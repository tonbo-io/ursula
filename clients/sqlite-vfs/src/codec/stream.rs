//! Reading `USQ2` frames out of a stream range, read by read: the one implementation behind the
//! replicator's reconcile and the driver's builds.
//!
//! A [`FrameStream`] covers the range `from .. end`, where `end` is a frame boundary. Each read
//! hands it the bytes from [`FrameStream::at`] up to the read's next offset; bytes past `end` are
//! ignored. It returns every whole frame the bytes complete, a complete PART run as the COMMIT it
//! makes, each with its encoding and the stream offset just after it.

use std::sync::Arc;

use ursula_headers::Offset;

use crate::codec::frame;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::FrameError;
use crate::codec::frame::PartRun;

/// A frame read from the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamFrame {
    /// A CLAIM or COMMIT (a complete PART run joined).
    pub frame: Frame,
    /// Its encoding: the bytes as read, or the COMMIT a joined run encodes to.
    pub bytes: Arc<[u8]>,
    /// The stream offset just after it (after the run's last part).
    pub end: Offset,
}

/// What stops reading a range.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// The read named a range it does not hold, or made no progress short of the end.
    #[error("a read answered {got} bytes up to {next}, not the range it names from {at}")]
    Range {
        at: Offset,
        next: Offset,
        got: usize,
    },
    /// Bytes that are not `USQ2` frames, or a broken PART run.
    #[error(transparent)]
    Undecodable(#[from] FrameError),
    /// A frame runs past the end of the range.
    #[error("a frame runs past {end}")]
    PastEnd { end: Offset },
}

/// The reader of one range.
#[derive(Debug, Clone)]
pub struct FrameStream {
    at: Offset,
    end: Offset,
    buf: Vec<u8>,
    buf_start: Offset,
    parts: PartRun,
    max_body: u64,
}

impl FrameStream {
    /// Reads `from .. end`; no frame body may exceed `max_body`.
    pub fn new(from: Offset, end: Offset, max_body: u64) -> Self {
        Self {
            at: from,
            end,
            buf: Vec::new(),
            buf_start: from,
            parts: PartRun::default(),
            max_body,
        }
    }

    /// The next offset to read from.
    pub fn at(&self) -> Offset {
        self.at
    }

    pub fn end(&self) -> Offset {
        self.end
    }

    /// The whole range is read and decoded.
    pub fn done(&self) -> bool {
        self.at >= self.end && self.buf.is_empty()
    }

    /// Stops reading: nothing more is wanted.
    pub fn finish(&mut self) {
        self.at = self.end;
        self.buf.clear();
    }

    /// A read's bytes from [`FrameStream::at`] up to `next`: the frames they complete, then what
    /// stopped the reading, if anything. Frames before an undecodable byte come out first.
    pub fn feed(&mut self, body: &[u8], next: Offset) -> (Vec<StreamFrame>, Option<StreamError>) {
        let got = next.get().saturating_sub(self.at.get());
        if next <= self.at || u64::try_from(body.len()).ok() != Some(got) {
            let error = StreamError::Range {
                at: self.at,
                next,
                got: body.len(),
            };
            return (Vec::new(), Some(error));
        }
        let take = got.min(self.end.get().saturating_sub(self.at.get()));
        let wanted = body
            .get(..usize::try_from(take).unwrap_or(usize::MAX))
            .unwrap_or(body);
        self.buf.extend_from_slice(wanted);
        self.at = Offset::from_u64(self.at.get().saturating_add(take));
        let mut frames = Vec::new();
        loop {
            let (frame, len) = match frame::decode(&self.buf, self.max_body) {
                Ok(Decoded::Partial) => break,
                Ok(Decoded::Frame { frame, len }) => (frame, len),
                Err(e) => return (frames, Some(e.into())),
            };
            let raw: Arc<[u8]> = Arc::from(self.buf.get(..len).unwrap_or_default());
            self.buf.drain(..len.min(self.buf.len()));
            self.buf_start = Offset::from_u64(
                self.buf_start
                    .get()
                    .saturating_add(u64::try_from(len).unwrap_or(u64::MAX)),
            );
            let direct = !matches!(frame, Frame::Part { .. });
            let frame = match self.parts.push(frame) {
                Ok(Some(frame)) => frame,
                Ok(None) => continue,
                Err(e) => return (frames, Some(e.into())),
            };
            let bytes = if direct {
                raw
            } else {
                match frame::encode(&frame) {
                    Ok(bytes) => Arc::from(bytes),
                    Err(e) => return (frames, Some(e.into())),
                }
            };
            frames.push(StreamFrame {
                frame,
                bytes,
                end: self.buf_start,
            });
        }
        if self.at >= self.end && !self.buf.is_empty() {
            return (frames, Some(StreamError::PastEnd { end: self.end }));
        }
        (frames, None)
    }
}
