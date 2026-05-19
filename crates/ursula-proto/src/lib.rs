pub mod durable {
    include!(concat!(env!("OUT_DIR"), "/ursula.durable.v1.rs"));
}

pub use durable::*;
