//! The endpoint policy of every server-side gRPC channel to a peer node.
//!
//! A peer host that loses power resets nothing: its TCP connections stay
//! open, and a call written to one waits for the kernel's retransmission
//! timeout (`tcp_retries2`, about 16 minutes on Linux). Every channel a node
//! opens to a peer (Raft replication, recovery probes, leader forwarding)
//! therefore bounds its connect and probes an idle connection with HTTP/2
//! PINGs. A silent peer then fails the calls in flight within seconds, and
//! the channel reconnects on its next call.

use std::time::Duration;

use tonic::transport::Endpoint;

/// Bound on opening a TCP connection to a peer, the same as the bound on
/// opening a Raft append stream.
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// A connection that has read no frame for this long sends an HTTP/2 PING.
/// A busy connection (Raft heartbeats every 250 ms) sends none.
const PEER_HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// How long a PING may go unanswered before the connection closes and every
/// call on it fails. With the interval, a peer that goes silent is cut off
/// within 3 s, the data groups' maximum election timeout: by then its
/// followers have stopped treating it as the leader.
const PEER_HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(2);

/// TCP keepalive for NAT and conntrack state. The HTTP/2 PINGs detect a dead
/// peer long before the kernel's keepalive probes would.
const PEER_TCP_KEEPALIVE: Duration = Duration::from_secs(10);

/// The endpoint of a peer's gRPC address with the bounds above.
pub(crate) fn peer_endpoint(address: &str) -> Result<Endpoint, tonic::transport::Error> {
    Ok(Endpoint::from_shared(address.to_owned())?
        .connect_timeout(PEER_CONNECT_TIMEOUT)
        .http2_keep_alive_interval(PEER_HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(PEER_HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(PEER_TCP_KEEPALIVE)))
}
