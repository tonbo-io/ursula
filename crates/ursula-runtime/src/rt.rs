#[cfg(madsim)]
pub use sim_tokio::{spawn, sync, time};

#[cfg(not(madsim))]
pub use tokio::{spawn, sync, time};
