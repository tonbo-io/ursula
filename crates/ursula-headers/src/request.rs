//! The headers a write carries: its producer key, digest and token.

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::value::BodyDigest;
use crate::value::Epoch;
use crate::value::Incarnation;
use crate::value::ProducerId;
use crate::value::Seq;

/// A producer write's key `(Producer-Id, Producer-Epoch, Producer-Seq)` and
/// its optional identity (`Producer-Digest`). Every field is valid by
/// construction, so every value renders and parses back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerWrite {
    pub id: ProducerId,
    pub epoch: Epoch,
    pub seq: Seq,
    pub digest: Option<BodyDigest>,
}

impl ProducerWrite {
    /// The write at seq 0, which opens its epoch.
    pub fn is_claim(&self) -> bool {
        self.seq == Seq::ZERO
    }
}

/// The `Stream-Incarnation` precondition of a request: the request applies
/// only to the stream incarnation it names (extensions Section 2.4). A
/// write that carries it is "tokened".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IncarnationPrecondition {
    pub incarnation: Incarnation,
}

impl IncarnationPrecondition {
    /// `Ok(None)` when the request carries no `Stream-Incarnation`.
    pub fn parse<'a, I>(headers: I) -> Result<Option<Self>, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [incarnation] = fields::collect(headers, [names::STREAM_INCARNATION])?;
        Self::from_field(incarnation)
    }

    fn from_field(value: Option<&[u8]>) -> Result<Option<Self>, HeaderError> {
        Ok(fields::incarnation(names::STREAM_INCARNATION, value)?
            .map(|incarnation| Self { incarnation }))
    }

    /// Emits `Stream-Incarnation`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        emit(names::STREAM_INCARNATION, &self.incarnation.to_string());
    }
}

/// The write-key headers of one request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerRequestHeaders {
    /// `None` for a write without a key.
    pub producer: Option<ProducerWrite>,
    pub precondition: Option<IncarnationPrecondition>,
}

impl ProducerRequestHeaders {
    /// Parses the write-key headers from `(name, value)` pairs. Producer
    /// integers may carry leading zeros, since base-protocol clients choose
    /// them.
    pub fn parse<'a, I>(headers: I) -> Result<Self, HeaderError>
    where I: IntoIterator<Item = (&'a str, &'a [u8])> {
        let [id, epoch, seq, digest, incarnation] = fields::collect(headers, [
            names::PRODUCER_ID,
            names::PRODUCER_EPOCH,
            names::PRODUCER_SEQ,
            names::PRODUCER_DIGEST,
            names::STREAM_INCARNATION,
        ])?;
        let id = fields::text(names::PRODUCER_ID, id)?;
        let epoch = fields::text(names::PRODUCER_EPOCH, epoch)?;
        let seq = fields::text(names::PRODUCER_SEQ, seq)?;
        let digest = fields::parsed(names::PRODUCER_DIGEST, digest, BodyDigest::parse)?;
        let precondition = IncarnationPrecondition::from_field(incarnation)?;
        let producer = match (id, epoch, seq) {
            (None, None, None) => {
                if digest.is_some() {
                    return Err(HeaderError::DigestWithoutProducer);
                }
                None
            }
            (Some(id), Some(epoch), Some(seq)) => Some(ProducerWrite {
                id: ProducerId::new(id)?,
                epoch: Epoch::parse_request(names::PRODUCER_EPOCH, epoch)?,
                seq: Seq::parse_request(names::PRODUCER_SEQ, seq)?,
                digest,
            }),
            _ => return Err(HeaderError::PartialProducer),
        };
        Ok(Self {
            producer,
            precondition,
        })
    }

    /// Emits every header this value carries, as `(lowercase name, value)`.
    /// Every value is visible ASCII, with inner spaces or tabs only in
    /// `Producer-Id`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        if let Some(producer) = &self.producer {
            emit(names::PRODUCER_ID, producer.id.as_str());
            emit(names::PRODUCER_EPOCH, &producer.epoch.to_string());
            emit(names::PRODUCER_SEQ, &producer.seq.to_string());
            if let Some(digest) = &producer.digest {
                emit(names::PRODUCER_DIGEST, &digest.to_string());
            }
        }
        if let Some(precondition) = &self.precondition {
            precondition.render(&mut emit);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IncarnationPrecondition;
    use super::ProducerRequestHeaders;
    use super::ProducerWrite;
    use crate::error::HeaderError;
    use crate::value::BodyDigest;
    use crate::value::Epoch;
    use crate::value::Incarnation;
    use crate::value::ProducerId;
    use crate::value::Seq;

    fn rendered(headers: &ProducerRequestHeaders) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        headers.render(|name, value| out.push((name, value.to_owned())));
        out
    }

    fn parse(pairs: &[(&str, &str)]) -> Result<ProducerRequestHeaders, HeaderError> {
        ProducerRequestHeaders::parse(pairs.iter().map(|(name, value)| (*name, value.as_bytes())))
    }

    fn round_trip(headers: &ProducerRequestHeaders) {
        let pairs = rendered(headers);
        let parsed = ProducerRequestHeaders::parse(
            pairs.iter().map(|(name, value)| (*name, value.as_bytes())),
        )
        .unwrap();
        assert_eq!(&parsed, headers);
    }

    fn precondition(token: u64) -> Option<IncarnationPrecondition> {
        Some(IncarnationPrecondition {
            incarnation: Incarnation::try_from(token).unwrap(),
        })
    }

    #[test]
    fn requests_round_trip() {
        let digest = BodyDigest::from_bytes([0xab; 32]);
        for headers in [
            ProducerRequestHeaders::default(),
            ProducerRequestHeaders {
                producer: None,
                precondition: precondition(4_503_599_627_370_497),
            },
            ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new("sqlite-ursula-vfs/2").unwrap(),
                    epoch: Epoch::ZERO,
                    seq: Seq::ZERO,
                    digest: None,
                }),
                precondition: None,
            },
            ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: ProducerId::new("w x").unwrap(),
                    epoch: Epoch::MAX,
                    seq: Seq::MAX,
                    digest: Some(digest),
                }),
                precondition: precondition(9_007_199_254_740_991),
            },
        ] {
            round_trip(&headers);
        }
    }

    #[test]
    fn names_compare_case_insensitively_and_values_are_trimmed() {
        let parsed = parse(&[
            ("Producer-Id", " w "),
            ("PRODUCER-EPOCH", "3"),
            ("producer-seq", "\t07"),
            ("Stream-Incarnation", "42"),
        ])
        .unwrap();
        assert_eq!(parsed.precondition, precondition(42));
        let producer = parsed.producer.unwrap();
        assert_eq!(
            (
                producer.id.as_str(),
                producer.epoch.get(),
                producer.seq.get()
            ),
            ("w", 3, 7)
        );
        assert!(!producer.is_claim());
    }

    #[test]
    fn a_precondition_parses_on_its_own() {
        let pairs = [("stream-incarnation", "42".as_bytes())];
        assert_eq!(IncarnationPrecondition::parse(pairs), Ok(precondition(42)));
        assert_eq!(
            IncarnationPrecondition::parse([("stream-incarnation", "0".as_bytes())]),
            Err(HeaderError::OutOfRange {
                name: "stream-incarnation",
                value: 0
            })
        );
    }

    #[test]
    fn malformed_requests_are_typed() {
        let cases: [(&[(&str, &str)], HeaderError); 14] = [
            (&[("producer-id", "w")], HeaderError::PartialProducer),
            (
                &[
                    ("producer-id", ""),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                ],
                HeaderError::EmptyProducerId,
            ),
            (
                &[
                    ("producer-id", "w"),
                    ("producer-epoch", "-1"),
                    ("producer-seq", "0"),
                ],
                HeaderError::InvalidInteger {
                    name: "producer-epoch",
                },
            ),
            (
                &[
                    ("producer-id", "w"),
                    ("producer-epoch", "+1"),
                    ("producer-seq", "0"),
                ],
                HeaderError::InvalidInteger {
                    name: "producer-epoch",
                },
            ),
            (
                &[
                    ("producer-id", "w"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "9007199254740992"),
                ],
                HeaderError::OutOfRange {
                    name: "producer-seq",
                    value: 9_007_199_254_740_992,
                },
            ),
            (
                &[("producer-digest", &"a".repeat(64))],
                HeaderError::DigestWithoutProducer,
            ),
            (
                &[
                    ("producer-id", "w"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                    ("producer-digest", &"A".repeat(64)),
                ],
                HeaderError::InvalidDigest {
                    name: "producer-digest",
                },
            ),
            (
                &[
                    ("producer-id", "w"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                    ("producer-digest", &"a".repeat(63)),
                ],
                HeaderError::InvalidDigest {
                    name: "producer-digest",
                },
            ),
            (
                &[("stream-incarnation", "042")],
                HeaderError::NonCanonicalInteger {
                    name: "stream-incarnation",
                },
            ),
            (
                &[("stream-incarnation", "1"), ("Stream-Incarnation", "1")],
                HeaderError::Duplicate {
                    name: "stream-incarnation",
                },
            ),
            (
                &[
                    ("producer-id", "é"),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                ],
                HeaderError::NotText {
                    name: "producer-id",
                },
            ),
            (
                &[
                    ("producer-id", &"x".repeat(257)),
                    ("producer-epoch", "0"),
                    ("producer-seq", "0"),
                ],
                HeaderError::ProducerIdTooLong { len: 257 },
            ),
            (
                &[("stream-incarnation", "+42")],
                HeaderError::InvalidInteger {
                    name: "stream-incarnation",
                },
            ),
            (
                &[("stream-incarnation", "18446744073709551615")],
                HeaderError::OutOfRange {
                    name: "stream-incarnation",
                    value: u64::MAX,
                },
            ),
        ];
        for (pairs, expected) in cases {
            assert_eq!(parse(pairs), Err(expected), "{pairs:?}");
        }
    }
}
