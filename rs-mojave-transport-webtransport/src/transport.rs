use std::{
    collections::VecDeque,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{StreamExt, future::BoxFuture};

use moq_native::quic;
use multiaddr::{Multiaddr, PeerId};
use rs_mojave_network_core::{
    Protocol, Transport,
    transport::{TransportError, TransportEvent},
};

use crate::{
    Error,
    connection::{self, Connection},
    listener::Listener,
    platform,
};

type PendingEvent = TransportEvent<BoxFuture<'static, Result<(PeerId, Connection), Error>>, Error>;

/// A [`Transport`] implementation that uses WebTransport.
///
/// WebTransport is a protocol framework that enables clients constrained by the web
/// security model to communicate with a remote server using a secure, multiplexed
/// transport. It uses HTTP/3 as the underlying protocol for the control channel
/// and QUIC for the data channels.
pub struct WebTransport {
    /// QUIC configuration. This is only available on non-wasm32 targets
    /// as wasm32 targets rely on the browser's WebTransport implementation.
    #[cfg(not(target_arch = "wasm32"))]
    config: quic::Config,
    /// If true, allows dialing a TCP [`Multiaddr`] to retrieve a WebTransport
    /// fingerprint, which can then be used to establish a WebTransport connection.
    /// This is a workaround for discovery when direct WebTransport multiaddrs are not known.
    allow_tcp_fingerprint: bool,

    /// Queue of events to be reported by `poll`.
    /// This includes listen confirmations, incoming connections, etc.
    pending_events: VecDeque<PendingEvent>,

    /// The local cryptographic keypair. Used for authenticating the transport
    /// and deriving TLS certificates.
    keypair: libp2p_identity::Keypair,

    /// The active listener for incoming WebTransport connections.
    /// `None` if the transport is not currently listening.
    listener: Option<Listener>,
}

impl WebTransport {
    /// Creates a new `WebTransport` instance.
    ///
    /// This version is for non-wasm32 targets and requires a [`quic::Config`].
    ///
    /// # Parameters
    /// - `config`: The QUIC configuration to use for the transport. This dictates
    ///   parameters for the underlying QUIC connections.
    /// - `allow_tcp_fingerprint`: If true, allows dialing a TCP [`Multiaddr`] to
    ///   retrieve a WebTransport server certificate fingerprint.
    /// - `keypair`: The local [`libp2p_identity::Keypair`] for securing the transport.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(
        config: quic::Config,
        allow_tcp_fingerprint: bool,
        keypair: libp2p_identity::Keypair,
    ) -> Self {
        Self {
            config,
            allow_tcp_fingerprint,
            pending_events: VecDeque::new(),
            keypair,
            listener: None,
        }
    }

    /// Creates a new `WebTransport` instance.
    ///
    /// This version is for wasm32 targets and does not take a `quic::Config`
    /// as it relies on the browser's WebTransport implementation.
    ///
    /// # Parameters
    /// - `allow_tcp_fingerprint`: If true, allows dialing a TCP [`Multiaddr`] to
    ///   retrieve a WebTransport server certificate fingerprint. (Note: TCP dialing
    ///   capabilities might be restricted in browser environments).
    #[cfg(target_arch = "wasm32")]
    pub fn new(allow_tcp_fingerprint: bool) -> Self {
        Self {
            allow_tcp_fingerprint,
            // On wasm32, keypair and listener are not managed directly in the same way,
            // relying on browser APIs. These fields might need adjustment based on
            // actual wasm implementation details.
            pending_events: VecDeque::new(),
            keypair: libp2p_identity::Keypair::generate_ed25519(), // Placeholder, may need specific wasm handling
            listener: None,
        }
    }
}

impl Transport for WebTransport {
    type Output = (PeerId, Connection);
    type Error = Error;
    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type ListenerUpgrade = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    /// Returns the [`Protocol`] supported by this transport, which is [`Protocol::WebTransport`].
    fn supported_protocols_for_dialing(&self) -> Protocol {
        Protocol::WebTransport
    }

    /// Dials a remote peer using WebTransport.
    ///
    /// Expects a [`Multiaddr`] with the `/ip4/.../udp/.../quic-v1/webtransport/...` or
    /// `/ip6/.../udp/.../quic-v1/webtransport/...` pattern.
    /// An optional `/p2p/<peer-id>` component can be present at the end.
    ///
    /// If `allow_tcp_fingerprint` is true, it might also initially accept a TCP address
    /// to fetch a fingerprint, then proceed with WebTransport.
    fn dial(&mut self, ma: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        let (remote_socket_address, peer_id_from_ma) = remote_ma_to_socketaddr(&ma)
            .map_err(|e| TransportError::Other(e.into()))?;
        tracing::debug!(%remote_socket_address, ?peer_id_from_ma, "dial");

        let allow_tcp_fingerprint = self.allow_tcp_fingerprint;
        let local_keypair = self.keypair.clone();

        Ok(Box::pin(async move {
            // The `peer_id_from_ma` is extracted from the multiaddress but might not be
            // strictly enforced or used by the lower-level connection upgrade logic
            // if the WebTransport handshake itself handles peer authentication differently.
            // It's available if needed for higher-level logic.
            connection::upgrade_outbound(remote_socket_address, allow_tcp_fingerprint, local_keypair)
                .await
        }))
    }

    /// Starts listening for incoming WebTransport connections on the given [`Multiaddr`].
    ///
    /// Expects a [`Multiaddr`] with the `/ip4/.../udp/.../quic-v1/webtransport` or
    /// `/ip6/.../udp/.../quic-v1/webtransport` pattern.
    /// The actual listening interface and port are determined from the address.
    ///
    /// After calling this, [`TransportEvent::ListenAddress`] or
    /// [`TransportEvent::ListenerError`] will be emitted via `poll`.
    fn listen_on(&mut self, addr: Multiaddr) -> Result<(), TransportError<Self::Error>> {
        // Ensure the address is a WebTransport address.
        // multiaddr_to_socketaddr will validate the basic structure.
        // The actual listening setup in platform::listen_on will perform more specific checks.
        // It should not contain a /p2p component for listening.
        let (socket_addr, peer_id) = multiaddr_to_socketaddr(&addr)
            .ok_or_else(|| TransportError::MultiaddrNotSupported(addr.clone()))?;
        if peer_id.is_some() {
            return Err(TransportError::MultiaddrNotSupported(addr));
        }

        let listener = platform::listen_on(
            #[cfg(not(target_arch = "wasm32"))]
            &self.config,
            #[cfg(target_arch = "wasm32")] // This is a bit of a placeholder for wasm config if any
            (), // Wasm platform::listen_on might not need a quic::Config
            self.allow_tcp_fingerprint,
            socket_addr, // Pass SocketAddr directly
            self.keypair.clone(),
        )
        .map_err(TransportError::Other)?;

        self.pending_events
            .push_back(TransportEvent::ListenAddress { address: addr });
        self.listener = Some(listener);
        Ok(())
    }

    /// Polls for events from the WebTransport transport.
    ///
    /// This includes:
    /// - Previously queued events (e.g., `ListenAddress` from `listen_on`).
    /// - New incoming connections from the active listener.
    /// - Listener errors or closures.
    #[tracing::instrument(level = "trace", name = "Transport::poll", skip(self, cx))]
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>> {
        // Drain pending events first
        if let Some(event) = self.pending_events.pop_front() {
            return Poll::Ready(event);
        }

        // Poll the listener for new incoming connections or events
        if let Some(listener) = self.listener.as_mut() {
            match listener.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => return Poll::Ready(event),
                Poll::Ready(None) => {
                    // Listener closed, may need to emit ListenerClosed if not already handled
                    // For now, assume the listener itself emits appropriate error/closed events
                    // that get wrapped into PendingEvent.
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

/// Converts a [`Multiaddr`] to a [`SocketAddr`] and an optional [`PeerId`].
/// This function is specific to WebTransport multiaddresses.
/// Expected format: /ip4/.../udp/.../quic-v1/webtransport(/p2p/...)
/// or /ip6/.../udp/.../quic-v1/webtransport(/p2p/...).
fn remote_ma_to_socketaddr(ma: &Multiaddr) -> Result<(SocketAddr, Option<PeerId>), Error> {
    multiaddr_to_socketaddr(ma).ok_or_else(|| Error::InvalidMultiaddr(ma.clone()))
}

/// Parses a [`Multiaddr`] to extract components relevant for a WebTransport connection.
///
/// It expects the following structure:
/// - An IP component (`Ip4` or `Ip6`).
/// - A UDP port component.
/// - A `QuicV1` component (optional, but checked for correct placement if present).
/// - A `WebTransport` component.
/// - Optionally, a `P2p` component for the peer ID.
///
/// The order of these components is strictly enforced.
///
/// # Returns
/// `Some((socket_addr, Option<peer_id>))` if the address is valid and parsable.
/// `None` otherwise.
fn multiaddr_to_socketaddr(addr: &Multiaddr) -> Option<(SocketAddr, Option<PeerId>)> {
    let mut ip: Option<std::net::IpAddr> = None;
    let mut port: Option<u16> = None;
    let mut peer_id = None;
    let mut webtransport_seen = false;

    for proto in addr.iter() {
        match proto {
            multiaddr::Protocol::Ip4(ip_addr) => {
                if ip.is_some() || port.is_some() || webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order
                }
                ip = Some(ip_addr.into());
            }
            multiaddr::Protocol::Ip6(ip_addr) => {
                if ip.is_some() || port.is_some() || webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order
                }
                ip = Some(ip_addr.into());
            }
            multiaddr::Protocol::Udp(p) => {
                if ip.is_none() || port.is_some() || webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order or missing IP
                }
                port = Some(p);
            }
            multiaddr::Protocol::WebTransport => {
                // WebTransport can appear after QuicV1 or Udp.
                // If it's after Udp, then ip and port must be set.
                // If it's after QuicV1, that's also fine, QuicV1 is skipped.
                // For now, we simplify and assume it must appear after Udp.
                if ip.is_none() || port.is_none() || webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order or missing IP/Port
                }
                webtransport_seen = true;
            }
            multiaddr::Protocol::P2p(id) => {
                // P2p can only be the last component and requires WebTransport
                if !webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order or missing WebTransport
                }
                peer_id = Some(id);
            }
            multiaddr::Protocol::QuicV1 => {
                // QuicV1 is expected and skipped if present.
                // It must appear after UDP and before WebTransport.
                if ip.is_none() || port.is_none() || webtransport_seen || peer_id.is_some() {
                    return None; // Invalid order
                }
                // We don't explicitly set a flag for QuicV1 as its presence is optional
                // and its main role here is to be skipped.
            }
            _ => return None, // Unsupported protocol
        }
    }

    if ip.is_some() && port.is_some() && webtransport_seen {
        Some((SocketAddr::new(ip.unwrap(), port.unwrap()), peer_id))
    } else {
        None // Required components not found
    }
}
