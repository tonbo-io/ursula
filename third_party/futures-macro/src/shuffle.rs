//! Ursula's deterministic-simulation patch; not part of upstream
//! futures-macro 0.3.32.
//!
//! `select!` and `stream_select!` shuffle their branches with futures-util's
//! thread-local RNG, which is seeded from a process-wide counter. Under
//! `cfg(madsim)` two runs of one simulation seed in the same process (as
//! `madsim::runtime::Runtime::check_determinism` runs them) then poll ready
//! branches in different orders and diverge. There the expansion draws from a
//! hash of madsim's simulated clock and of how often this call site already
//! shuffled at that instant, which replays with the seed. The count keeps a
//! loop that never yields from drawing the same order forever: OpenRaft's
//! loops rely on the shuffle to reach their exit branch when another branch
//! is always ready. Without `cfg(madsim)` the expansion is upstream's.

use proc_macro2::{Ident, TokenStream};
use quote::quote;

/// Shuffles the array bound to `array` in place.
pub(crate) fn shuffle(array: &Ident) -> TokenStream {
    quote! {
        #[cfg(not(madsim))]
        __futures_crate::async_await::shuffle(&mut #array);
        #[cfg(madsim)]
        {
            ::std::thread_local! {
                static __FUTURES_SHUFFLES: ::std::cell::Cell<(::std::time::Duration, u64)> =
                    const { ::std::cell::Cell::new((::std::time::Duration::ZERO, 0)) };
            }
            let __clock = ::std::time::SystemTime::now()
                .duration_since(::std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let __shuffle = __FUTURES_SHUFFLES.with(|__shuffles| {
                let (__last_clock, __last_shuffle) = __shuffles.get();
                let __shuffle = if __last_clock == __clock {
                    __last_shuffle.wrapping_add(1)
                } else {
                    0
                };
                __shuffles.set((__clock, __shuffle));
                __shuffle
            });
            let __hasher = ::std::hash::BuildHasherDefault::<
                ::std::collections::hash_map::DefaultHasher,
            >::default();
            let mut __index = #array.len();
            while __index > 1 {
                __index -= 1;
                let __draw =
                    ::std::hash::BuildHasher::hash_one(&__hasher, (__clock, __shuffle, __index));
                let __bound = (__index as u64).wrapping_add(1);
                #array.swap(__index, (__draw % __bound) as usize);
            }
        }
    }
}
