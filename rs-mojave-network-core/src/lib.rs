//! # rs-mojave-network-core
//!
//! This crate provides foundational traits and types for network communication
//! within the Mojave project. It defines core abstractions for:
//!
//! - **`transport`**: Deals with the underlying mechanisms for sending and
//!   receiving data over a network (e.g., TCP, UDP, WebTransport).
//!   It provides the [`Transport`] trait, which abstracts over specific
//!   transport protocols, allowing for dialing and listening for connections.
//! - **`muxing`**: Handles the multiplexing of multiple logical substreams
//!   over a single underlying connection. This is managed by the
//!   [`StreamMuxer`] trait.
//! - **`protocol`**: Defines protocol identifiers and related utilities using
//!   the [`Protocol`] enum, which is based on `multiaddr` components.
//!
//! These components work together to enable flexible and extensible network
//! communication capabilities. For example, a `Transport` can be used to
//! establish a connection, and then a `StreamMuxer` can be used on top of
//! that connection to manage multiple independent data streams.

pub mod muxing;
mod protocol;
pub mod transport;

pub use muxing::StreamMuxer;
pub use protocol::*;
pub use transport::Transport;

pub mod util {
    use std::convert::Infallible;

    /// A safe version of [`std::intrinsics::unreachable`].
    #[inline(always)]
    pub fn unreachable(x: Infallible) -> ! {
        match x {}
    }
}
