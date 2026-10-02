//! Deterministic payload generators (minified JSON, one record per line).

/// xorshift64 generator; deterministic for a seed.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform-ish value in `[0, n)`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

const ALPHA: &[u8] = b"abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.";

/// One minified JSON record of exactly `len` bytes including the trailing LF
/// (or the minimum the envelope needs). Looks like an agent-harness delta
/// event; text is pseudo-random so snapshot zstd ratios are not flattered.
pub fn json_record(rng: &mut Rng, seq: u64, len: usize) -> Vec<u8> {
    let head = format!(
        "{{\"seq\":{seq},\"ts\":{},\"type\":\"assistant.delta\",\"text\":\"",
        1_759_300_000_000u64.wrapping_add(seq.wrapping_mul(7))
    );
    let tail = b"\"}\n";
    let mut v = Vec::with_capacity(len.max(head.len() + tail.len()));
    v.extend_from_slice(head.as_bytes());
    let alphabet = ALPHA.len() as u64;
    while v.len() + tail.len() < len {
        let index = usize::try_from(rng.below(alphabet)).unwrap_or(0);
        v.push(ALPHA.get(index).copied().unwrap_or(b'a'));
    }
    v.extend_from_slice(tail);
    v
}

/// `n` records of about `len` bytes, concatenated as the HTTP layer stores a
/// flattened top-level JSON array.
pub fn json_records(rng: &mut Rng, first_seq: u64, n: usize, len: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(n.saturating_mul(len));
    for i in 0..n {
        payload.extend_from_slice(&json_record(rng, first_seq + i as u64, len));
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::Rng;
    use super::json_record;

    #[test]
    fn records_are_exact_length_single_line_json() {
        let mut rng = Rng::new(1);
        let record = json_record(&mut rng, 42, 200);
        assert_eq!(record.len(), 200);
        assert_eq!(record.iter().filter(|b| **b == b'\n').count(), 1);
        let value: serde_json::Value =
            serde_json::from_slice(&record).expect("record is valid JSON");
        assert_eq!(value["seq"], 42);
    }

    #[test]
    fn generator_is_deterministic() {
        let a = json_record(&mut Rng::new(7), 1, 300);
        let b = json_record(&mut Rng::new(7), 1, 300);
        assert_eq!(a, b);
    }
}
