//! Claiming a stream for an owner (a new producer epoch that fences every earlier one) and
//! taking it back after the server expired the producer.

use std::fs::{self};
use std::sync::atomic::Ordering;

use crate::client::Append;
use crate::client::advanced;
use crate::client::append;
use crate::client::is_recreated;
use crate::client::read_from;
use crate::client::recreated_error;
use crate::config::first_claim_epoch;
use crate::db::Db;
use crate::frame;
use crate::frame::Decoded;
use crate::frame::Record;

pub(crate) fn nonce() -> Result<[u8; 16], String> {
    let mut n = [0u8; 16];
    fs::File::open("/dev/urandom")
        .and_then(|mut f| std::io::Read::read_exact(&mut f, &mut n))
        .map_err(|e| format!("/dev/urandom: {e}"))?;
    Ok(n)
}

pub(crate) enum Claimed {
    /// Our claim ends at `next`: this owner alone writes at `epoch` from here on. `first`: it is
    /// the first frame after the position the claim was checked from.
    Won {
        first: bool,
        next: String,
    },
    /// Another owner's claim (or anything else) holds the answered position: a concurrent
    /// claim at the same epoch was answered as a duplicate of theirs.
    Lost,
    Fenced(Option<u64>),
    /// The stream is no longer the incarnation claimed (412).
    Recreated,
}

/// Appends a claim at (`epoch`, seq 0) and verifies our claim among the frames read back (see
/// `find_claim`). A 2xx alone proves nothing: two owners claiming the same epoch both get one, the
/// second as a duplicate of the first's receipt. The nonce makes our claim's bytes unique.
///
/// Offsets are opaque, so the claim's start is not computed from its end: `from` is a frame
/// boundary at or before the stream's tail before the claim (the tail catch-up recorded, or the
/// owner's own offset), and the frames from there to the answered offset are read and decoded.
pub(crate) fn claim_once(
    url: &str,
    incarnation: &str,
    producer: &str,
    epoch: u64,
    from: &str,
) -> Result<Claimed, String> {
    let nonce = nonce()?;
    let frame = frame::encode_claim(epoch, &nonce);
    let next = match append(url, incarnation, producer, &frame, epoch, 0) {
        Append::Acked {
            next: Some(next), ..
        } => next,
        Append::Acked { next: None, .. } => {
            return Err(format!("claim {url}: no Stream-Next-Offset"));
        }
        Append::Fenced { current } => return Ok(Claimed::Fenced(current)),
        Append::Recreated => return Ok(Claimed::Recreated),
        Append::ProducerExpired | Append::SeqConflict(_) => {
            unreachable!("a claim has sequence 0 and no Stream-Seq")
        }
        Append::Failed(e) => return Err(format!("claim {url}: {e}")),
    };
    let (mut buf, mut at) = (Vec::new(), from.to_owned());
    while at.as_str() < next.as_str() {
        let (bytes, n) = read_from(url, incarnation, &at).map_err(String::from)?;
        if bytes.is_empty() {
            return Err(format!(
                "claim {url}: the stream ends at {at}, before the claim's end {next}"
            ));
        }
        advanced(url, &at, bytes.len(), &n)?;
        buf.extend_from_slice(&bytes);
        at = n;
    }
    let exact = at == next;
    let found = find_claim(&buf, exact, epoch, &nonce).map_err(|e| format!("claim {url}: {e}"))?;
    Ok(match found {
        Some(first) => Claimed::Won { first, next },
        None => Claimed::Lost,
    })
}

/// Our claim (`epoch`, `nonce`) among the frames in `buf`, read from a frame boundary before it up
/// to the answered offset (`exact`) or past it (another owner appended meanwhile, and the answered
/// offset's place in the bytes is unknown): `Some(first)` when it is the frame ending at the
/// answered offset (`first`: no frame precedes it in `buf`), `None` (lost) otherwise. Past the
/// answered offset, our claim being there at all is enough: the server applies one append per
/// (producer, epoch, seq 0) and answers every other with that append's receipt, so our claim is in
/// the stream only if the answer was its own end.
pub(crate) fn find_claim(
    buf: &[u8],
    exact: bool,
    epoch: u64,
    nonce: &[u8; 16],
) -> Result<Option<bool>, String> {
    let (mut used, mut found, mut last_ours) = (0, None, false);
    while let Decoded::Frame { record, len } = frame::decode(&buf[used..])? {
        last_ours = record
            == Record::Claim {
                epoch,
                nonce: *nonce,
            };
        if last_ours {
            found = Some(used == 0);
        }
        used += len;
    }
    if exact && used != buf.len() {
        return Err("the answered offset is not a frame boundary".into());
    }
    Ok(found.filter(|_| !exact || last_ours))
}

/// Claims the stream with an epoch above every earlier owner's; returns it and the claim's end.
/// `from`: a frame boundary at or before the tail (see `claim_once`).
pub(crate) fn claim(
    url: &str,
    incarnation: &str,
    producer: &str,
    epoch: u64,
    from: &str,
) -> Result<(u64, String), String> {
    // Test hook URSULA_VFS_FIRST_CLAIM_EPOCH: the process's first claim uses this epoch.
    static HOOKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let mut epoch = match first_claim_epoch() {
        Some(e) if !HOOKED.swap(true, Ordering::Relaxed) => e,
        _ => epoch,
    };
    for _ in 0..16 {
        match claim_once(url, incarnation, producer, epoch, from)? {
            Claimed::Won { next, .. } => return Ok((epoch, next)),
            Claimed::Lost => epoch += 1,
            Claimed::Fenced(current) => epoch = current.unwrap_or(epoch).max(epoch) + 1,
            Claimed::Recreated => return Err(recreated_error(url, incarnation)),
        }
    }
    Err(format!("claim {url}: lost 16 claim races"))
}

/// The server does not know this owner's producer: it expired it (7 days without a write) and
/// forgot its epoch. Taking the stream back is safe only if nobody wrote since this owner's last
/// frame: the stream must end at our offset, and our new claim (one epoch up, fencing any later
/// owner's older epochs) must be ours (verified) and be the first frame after it. Otherwise
/// another owner wrote or claimed, and this one is fenced; so it is when the stream turns out to
/// be another incarnation (every request here carries ours as a precondition).
pub(crate) fn reclaim(db: &mut Db) -> Result<(), String> {
    let fenced = |db: &mut Db, e: String| {
        if is_recreated(&e) {
            db.fenced = true;
        }
        e
    };
    let (bytes, _) =
        read_from(&db.url, &db.incarnation, &db.offset).map_err(|e| fenced(db, String::from(e)))?;
    if !bytes.is_empty() {
        db.fenced = true;
        return Err(format!(
            "fenced: producer expired and the stream moved past {}",
            db.offset
        ));
    }
    let epoch = db.epoch + 1;
    let claimed = claim_once(&db.url, &db.incarnation, &db.producer, epoch, &db.offset)
        .map_err(|e| fenced(db, e))?;
    match claimed {
        Claimed::Won { first: true, next } => {
            db.epoch = epoch;
            db.seq = 0;
            db.offset = next;
            Ok(())
        }
        Claimed::Recreated => {
            db.fenced = true;
            Err(recreated_error(&db.url, &db.incarnation))
        }
        _ => {
            db.fenced = true;
            Err("fenced: another owner wrote or claimed while re-claiming".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::find_claim;
    use crate::frame;
    use crate::frame::PAGE;

    // A claim is ours only if it is the frame ending at the answered offset (the bytes read from a
    // frame boundary before it end there), or, when the read ran past that offset (another owner
    // appended meanwhile), if it is among the frames at all. A same-epoch claim with another nonce
    // (the answer was a duplicate of theirs) is lost; offsets are never computed.
    #[test]
    fn claims_are_found_by_their_nonce() {
        let (ours, theirs) = ([1u8; 16], [2u8; 16]);
        let commit = frame::encode_commit(1, &BTreeMap::from([(1, vec![0u8; PAGE])])).0;
        let (mine, other) = (
            frame::encode_claim(7, &ours),
            frame::encode_claim(7, &theirs),
        );
        let find = |frames: &[&[u8]], exact: bool| find_claim(&frames.concat(), exact, 7, &ours);
        assert_eq!(find(&[&mine[..]], true), Ok(Some(true)));
        assert_eq!(find(&[&commit[..], &mine[..]], true), Ok(Some(false)));
        assert_eq!(find(&[&commit[..], &other[..]], true), Ok(None));
        assert_eq!(find(&[&frame::encode_claim(6, &ours)[..]], true), Ok(None));
        // Read past the answered offset: ours, then another owner's claim and a partial frame.
        let past: [&[u8]; 3] = [&mine[..], &other[..], &commit[..9]];
        assert_eq!(find(&past, false), Ok(Some(true)));
        assert_eq!(find(&[&other[..], &commit[..]], false), Ok(None));
        // Read exactly to it, but not on a frame boundary.
        assert!(find(&past, true).is_err());
    }
}
