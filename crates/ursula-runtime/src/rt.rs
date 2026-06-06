#[cfg(madsim)]
pub use sim_tokio::spawn;
#[cfg(madsim)]
pub use sim_tokio::sync;
#[cfg(madsim)]
pub use sim_tokio::time;
#[cfg(not(madsim))]
pub use tokio::spawn;
#[cfg(not(madsim))]
pub use tokio::sync;
#[cfg(not(madsim))]
pub use tokio::time;
