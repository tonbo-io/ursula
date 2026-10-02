//! Counting global allocator.
//!
//! Tracks requested live bytes and live block counts, plus a census of every
//! live allocation of at least [`BIG`] bytes, so the probe sees individual
//! large `Vec`/`VecDeque` buffers (record index, message records, hot-chunk
//! deque) and their capacity, not just their length. The binary installs it
//! with `#[global_allocator]`; library users (tests) may install it too.
//!
//! Figures are requested bytes; real RSS is somewhat higher.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::Ordering::SeqCst;

/// The counting allocator; wraps [`System`].
pub struct Counting;

static LIVE_BYTES: AtomicI64 = AtomicI64::new(0);
static LIVE_BLOCKS: AtomicI64 = AtomicI64::new(0);
static TOTAL_ALLOCS: AtomicUsize = AtomicUsize::new(0);

/// Allocations at least this large are tracked individually.
pub const BIG: usize = 64 * 1024;
const SLOTS: usize = 4096;
static BIG_PTRS: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];
static BIG_SIZES: [AtomicUsize; SLOTS] = [const { AtomicUsize::new(0) }; SLOTS];

fn signed(size: usize) -> i64 {
    i64::try_from(size).unwrap_or(i64::MAX)
}

fn on_alloc(ptr: *mut u8, size: usize) {
    LIVE_BYTES.fetch_add(signed(size), Relaxed);
    LIVE_BLOCKS.fetch_add(1, Relaxed);
    TOTAL_ALLOCS.fetch_add(1, Relaxed);
    if size >= BIG {
        let p = ptr as usize;
        for (slot, slot_size) in BIG_PTRS.iter().zip(BIG_SIZES.iter()) {
            if slot.compare_exchange(0, p, SeqCst, SeqCst).is_ok() {
                slot_size.store(size, SeqCst);
                break;
            }
        }
    }
}

fn on_dealloc(ptr: *mut u8, size: usize) {
    LIVE_BYTES.fetch_sub(signed(size), Relaxed);
    LIVE_BLOCKS.fetch_sub(1, Relaxed);
    if size >= BIG {
        let p = ptr as usize;
        for (slot, slot_size) in BIG_PTRS.iter().zip(BIG_SIZES.iter()) {
            if slot.load(SeqCst) == p {
                slot_size.store(0, SeqCst);
                slot.store(0, SeqCst);
                break;
            }
        }
    }
}

// SAFETY: every method forwards to `System` with the caller's layout and only
// updates atomic counters around the call, so `System`'s contract carries over.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded verbatim; the caller upholds `GlobalAlloc::alloc`.
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            on_alloc(p, layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded verbatim; the caller upholds `alloc_zeroed`'s contract.
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            on_alloc(p, layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        on_dealloc(ptr, layout.size());
        // SAFETY: `ptr` was returned by this allocator (i.e. by `System`) with `layout`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded verbatim; the caller upholds `realloc`'s contract.
        let q = unsafe { System.realloc(ptr, layout, new_size) };
        if !q.is_null() {
            on_dealloc(ptr, layout.size());
            on_alloc(q, new_size);
        }
        q
    }
}

/// Live requested heap.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Heap {
    pub bytes: i64,
    pub blocks: i64,
}

impl std::ops::Sub for Heap {
    type Output = Heap;

    fn sub(self, rhs: Heap) -> Heap {
        Heap {
            bytes: self.bytes.saturating_sub(rhs.bytes),
            blocks: self.blocks.saturating_sub(rhs.blocks),
        }
    }
}

/// Current live heap. Zero unless [`Counting`] is the global allocator.
pub fn heap() -> Heap {
    Heap {
        bytes: LIVE_BYTES.load(Relaxed),
        blocks: LIVE_BLOCKS.load(Relaxed),
    }
}

/// Allocations made since process start.
pub fn total_allocs() -> usize {
    TOTAL_ALLOCS.load(Relaxed)
}

/// Live heap of a deep clone: every `Vec`/`VecDeque`/`BinaryHeap` clone
/// allocates exactly `len` elements, so `heap(original) - tight_size(original)`
/// is the capacity slack the original holds.
pub fn tight_size<T: Clone>(value: &T) -> Heap {
    let before = heap();
    let clone = value.clone();
    let after = heap();
    drop(clone);
    after - before
}

/// Sizes of every live allocation of at least [`BIG`] bytes, largest first.
pub fn big_allocs() -> Vec<usize> {
    let mut out: Vec<usize> = BIG_PTRS
        .iter()
        .zip(BIG_SIZES.iter())
        .filter(|(ptr, _)| ptr.load(SeqCst) != 0)
        .map(|(_, size)| size.load(SeqCst))
        .filter(|size| *size > 0)
        .collect();
    out.sort_unstable_by(|a, b| b.cmp(a));
    out
}

/// Big allocations present in `after` but not in `before` (multiset diff).
pub fn big_allocs_diff(before: &[usize], after: &[usize]) -> Vec<usize> {
    let mut remaining = before.to_vec();
    let mut out = Vec::new();
    for size in after {
        if let Some(pos) = remaining.iter().position(|x| x == size) {
            remaining.swap_remove(pos);
        } else {
            out.push(*size);
        }
    }
    out.sort_unstable_by(|a, b| b.cmp(a));
    out
}
