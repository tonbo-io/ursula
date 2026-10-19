//! Ursula's one MessagePack codec. Everything Ursula replicates, persists,
//! sends between nodes or backs up as MessagePack goes through it: Raft log
//! entries and the WAL journal, the WAL state files, Raft RPC payloads,
//! forwarded reads and writes, snapshot records and pointers, and backup
//! group objects.
//!
//! The encoding is compact. A struct is an array of its fields in
//! declaration order, not a map keyed by field names, and an enum variant is
//! written by its name. Field order is therefore part of the format of every
//! type it encodes: reordering, adding or removing a field, or renaming a
//! variant, changes the format. Every field is required, so such a type has
//! no `skip_serializing_if`, `default`, `flatten` or untagged enum. A
//! document with an element more or less than its type has is refused, not
//! read in part.
//!
//! Clippy's `disallowed-methods` keeps every `rmp_serde` call inside this
//! module, and the workspace pins `rmp-serde` to one release: its
//! documentation claims integer variant indices, which it does not write, so
//! a release that "fixed" that would change the format silently.

/// Why a document does not decode.
pub use rmp_serde::decode::Error as DecodeError;
/// Why a value does not encode.
pub use rmp_serde::encode::Error as EncodeError;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Encodes `value` into a new buffer.
#[expect(clippy::disallowed_methods, reason = "the one MessagePack encoder")]
pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, EncodeError> {
    rmp_serde::to_vec(value)
}

/// Encodes `value` at the end of `out`.
#[expect(clippy::disallowed_methods, reason = "the one MessagePack encoder")]
pub fn encode_into<T: Serialize + ?Sized>(value: &T, out: &mut Vec<u8>) -> Result<(), EncodeError> {
    rmp_serde::encode::write(out, value)
}

/// Decodes one value of type `T` from `bytes`.
#[expect(clippy::disallowed_methods, reason = "the one MessagePack decoder")]
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    rmp_serde::from_slice(bytes)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::DecodeError;
    use super::decode;
    use super::encode;
    use super::encode_into;

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Pair {
        first: u8,
        second: Option<u8>,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Triple {
        first: u8,
        second: Option<u8>,
        third: u8,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    enum Shape {
        Unit,
        Struct { side: u8 },
    }

    /// The contract the formats rely on: a struct is an array of its fields
    /// in declaration order with an absent `Option` written as nil, and a
    /// variant is written by its name. A release of `rmp-serde` that changes
    /// either fails here.
    #[test]
    fn structs_are_arrays_and_variants_keep_their_names() {
        let pair = Pair {
            first: 1,
            second: None,
        };
        assert_eq!(encode(&pair).unwrap(), [0x92, 0x01, 0xc0]);
        let mut out = vec![0xff];
        encode_into(&pair, &mut out).unwrap();
        assert_eq!(out, [0xff, 0x92, 0x01, 0xc0]);
        assert_eq!(decode::<Pair>(&[0x92, 0x01, 0xc0]).unwrap(), pair);

        assert_eq!(encode(&Shape::Unit).unwrap(), b"\xa4Unit");
        assert_eq!(
            encode(&Shape::Struct { side: 3 }).unwrap(),
            b"\x81\xa6Struct\x91\x03"
        );
        for shape in [Shape::Unit, Shape::Struct { side: 3 }] {
            assert_eq!(decode::<Shape>(&encode(&shape).unwrap()).unwrap(), shape);
        }
    }

    /// A document with an element more or less than its type is refused,
    /// never read in part or filled with a default. An excess element names
    /// how many the type read.
    #[test]
    fn a_document_of_another_length_is_refused() {
        let triple = encode(&Triple {
            first: 1,
            second: Some(2),
            third: 3,
        })
        .unwrap();
        assert!(matches!(
            decode::<Pair>(&triple),
            Err(DecodeError::LengthMismatch(2))
        ));
        let pair = encode(&Pair {
            first: 1,
            second: Some(2),
        })
        .unwrap();
        assert!(matches!(
            decode::<Triple>(&pair),
            Err(DecodeError::Syntax(_))
        ));
    }
}
