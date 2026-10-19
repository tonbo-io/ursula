//! A small seeded generator (SplitMix64) for the code that must replay a
//! seed: the snapshot store harness draws every fault, delay and nonce from
//! it, and under madsim it draws the nonces of new cold object names. Neither
//! draws from a thread or process RNG or from madsim's own generator, so a
//! seed replays the same draws in the same order of requests.

#[derive(Debug, Clone)]
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) const fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ z.wrapping_shr(30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ z.wrapping_shr(27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ z.wrapping_shr(31)
    }

    /// A draw below `bound`, or 0 when `bound` is 0.
    pub(crate) fn below(&mut self, bound: u64) -> u64 {
        self.next_u64().checked_rem(bound).unwrap_or(0)
    }
}
