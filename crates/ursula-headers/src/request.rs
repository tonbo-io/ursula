//! The headers a write carries: its producer key, digest and token.

use crate::error::HeaderError;
use crate::fields;
use crate::names;
use crate::value::Digest;
use crate::value::PRODUCER_ID_MAX_BYTES;
use crate::value::parse_producer_integer;
use crate::value::render_decimal;

/// A producer write's key `(Producer-Id, Producer-Epoch, Producer-Seq)` and
/// its optional identity (`Producer-Digest`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerWrite {
    pub id: String,
    pub epoch: u64,
    pub seq: u64,
    pub digest: Option<Digest>,
}

impl ProducerWrite {
    /// The write at seq 0, which opens its epoch.
    pub fn is_claim(&self) -> bool {
        self.seq == 0
    }
}

/// The write-key headers of one request.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProducerRequestHeaders {
    /// `None` for a write without a key.
    pub producer: Option<ProducerWrite>,
    /// The `Stream-Incarnation` precondition. A write that carries it is
    /// "tokened".
    pub incarnation: Option<u64>,
}

impl ProducerRequestHeaders {
    /// Parses the write-key headers from `(name, value)` pairs.
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
        let digest = fields::digest(names::PRODUCER_DIGEST, digest)?;
        let incarnation = fields::token(names::STREAM_INCARNATION, incarnation)?;
        let producer = match (id, epoch, seq) {
            (None, None, None) => {
                if digest.is_some() {
                    return Err(HeaderError::DigestWithoutProducer);
                }
                None
            }
            (Some(id), Some(epoch), Some(seq)) => {
                if id.is_empty() {
                    return Err(HeaderError::EmptyProducerId);
                }
                if id.len() > PRODUCER_ID_MAX_BYTES {
                    return Err(HeaderError::ProducerIdTooLong { len: id.len() });
                }
                Some(ProducerWrite {
                    id: id.to_owned(),
                    epoch: parse_producer_integer(names::PRODUCER_EPOCH, epoch)?,
                    seq: parse_producer_integer(names::PRODUCER_SEQ, seq)?,
                    digest,
                })
            }
            _ => return Err(HeaderError::PartialProducer),
        };
        Ok(Self {
            producer,
            incarnation,
        })
    }

    /// Emits every header this value carries, as `(lowercase name, value)`.
    pub fn render(&self, mut emit: impl FnMut(&'static str, &str)) {
        if let Some(producer) = &self.producer {
            emit(names::PRODUCER_ID, &producer.id);
            emit(names::PRODUCER_EPOCH, &render_decimal(producer.epoch));
            emit(names::PRODUCER_SEQ, &render_decimal(producer.seq));
            if let Some(digest) = &producer.digest {
                emit(names::PRODUCER_DIGEST, &digest.to_string());
            }
        }
        if let Some(incarnation) = self.incarnation {
            emit(names::STREAM_INCARNATION, &render_decimal(incarnation));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ProducerRequestHeaders;
    use super::ProducerWrite;
    use crate::error::HeaderError;
    use crate::value::Digest;
    use crate::value::MAX_PRODUCER_INTEGER;

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

    #[test]
    fn requests_round_trip() {
        let digest = Digest::from_bytes([0xab; 32]);
        for headers in [
            ProducerRequestHeaders::default(),
            ProducerRequestHeaders {
                producer: None,
                incarnation: Some(4_503_599_627_370_497),
            },
            ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: "sqlite-ursula-vfs/2".to_owned(),
                    epoch: 0,
                    seq: 0,
                    digest: None,
                }),
                incarnation: None,
            },
            ProducerRequestHeaders {
                producer: Some(ProducerWrite {
                    id: "w".to_owned(),
                    epoch: MAX_PRODUCER_INTEGER,
                    seq: MAX_PRODUCER_INTEGER,
                    digest: Some(digest),
                }),
                incarnation: Some(u64::MAX),
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
            ("producer-seq", "\t7"),
            ("Stream-Incarnation", "42"),
        ])
        .unwrap();
        assert_eq!(parsed.incarnation, Some(42));
        let producer = parsed.producer.unwrap();
        assert_eq!(
            (producer.id.as_str(), producer.epoch, producer.seq),
            ("w", 3, 7)
        );
        assert!(!producer.is_claim());
    }

    #[test]
    fn malformed_requests_are_typed() {
        let cases: [(&[(&str, &str)], HeaderError); 13] = [
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
                HeaderError::IntegerTooLarge {
                    name: "producer-seq",
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
        ];
        for (pairs, expected) in cases {
            assert_eq!(parse(pairs), Err(expected), "{pairs:?}");
        }
    }
}
