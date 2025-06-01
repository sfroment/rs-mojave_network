use futures::FutureExt;
use futures::stream::FusedStream;
use multiaddr::{Multiaddr, PeerId, Protocol as MultiaddrProtocol};
use rs_mojave_network_core::muxing::StreamMuxerBox;
use rs_mojave_network_core::transport;
use rs_mojave_network_core::{Protocol, Transport, transport::Boxed, transport::TransportEvent};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::{error, info};

use crate::error::Error;
use crate::peer::manager;
use crate::{NodeEvent, peer};

pub struct Node {
	pub peer_id: PeerId,
	transports: HashMap<Protocol, Boxed<(PeerId, StreamMuxerBox)>>,
	peer_manager: peer::manager::Manager,
	pending_events: VecDeque<NodeEvent>,
}

impl Node {
	pub fn new(peer_id: PeerId, transports: HashMap<Protocol, Boxed<(PeerId, StreamMuxerBox)>>) -> Self {
		Self {
			peer_id,
			transports,
			pending_events: VecDeque::new(),
			peer_manager: manager::Manager::new(),
		}
	}

	pub async fn dial(&mut self, remote_peer_id: PeerId, remote_address: Multiaddr) -> Result<(), Error> {
		info!(peer_id = %self.peer_id, %remote_peer_id, %remote_address, "Attempting to dial");

		let protocol = extract_protocol_from_multiaddr(&remote_address)?;

		let transport = self.transports.get_mut(&protocol).ok_or_else(|| {
			error!(peer_id = %self.peer_id, %remote_peer_id, %remote_address, ?protocol, "Transport not found for protocol");
			Error::TransportNotFound(protocol)
		})?;

		let dial = transport
			.dial(remote_address.clone())
			.map_err(|e| Error::Transport(Box::new(e)))?
			.boxed();

		self.peer_manager.add_outgoing(dial, remote_address);

		Ok(())
	}

	pub async fn listen(&mut self, address: Multiaddr) -> Result<(), Error> {
		let protocol = extract_protocol_from_multiaddr(&address)?;

		let transport = self.transports.get_mut(&protocol).ok_or_else(|| {
			error!(peer_id = %self.peer_id, %address, ?protocol, "Transport not found for protocol");
			Error::TransportNotFound(protocol)
		})?;

		transport
			.listen_on(address.clone())
			.inspect_err(|e| {
				error!(peer_id = %self.peer_id, %address, ?e, "Failed to listen");
			})
			.map_err(|e| Error::Transport(Box::new(e)))?;

		Ok(())
	}

	fn poll_next_event(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<NodeEvent> {
		let this = &mut *self;

		'outer: loop {
			if let Some(event) = this.pending_events.pop_front() {
				return Poll::Ready(event);
			}

			match this.peer_manager.poll(cx) {
				Poll::Pending => {}
				Poll::Ready(event) => {
					this.handle_peer_event(event);
					continue 'outer;
				}
			}

			for v in this.transports.values_mut() {
				match Pin::new(v).poll(cx) {
					Poll::Ready(event) => {
						this.handle_transport_event(event);
						continue 'outer;
					}
					Poll::Pending => {}
				}
			}

			return Poll::Pending;
		}
	}

	fn handle_peer_event(&mut self, event: peer::manager::PeerEvent) {
		info!(peer_id = %self.peer_id, ?event, "Peer event");
	}

	fn handle_transport_event(
		&mut self,
		event: TransportEvent<<transport::Boxed<(PeerId, StreamMuxerBox)> as Transport>::ListenerUpgrade, io::Error>,
	) {
		match event {
			TransportEvent::Incoming {
				remote_addr,
				local_addr,
				upgrade,
			} => {
				self.peer_manager.add_incoming(upgrade, local_addr, remote_addr);
			}
			TransportEvent::ListenAddress { address } => {
				info!(peer_id = %self.peer_id, %address, "Listening on");
			}
			TransportEvent::AddressExpired { address } => {
				info!(peer_id = %self.peer_id, %address, "Listen address expired");
			}
			TransportEvent::ListenerError { error } => {
				info!(peer_id = %self.peer_id, ?error, "Failed to listen");
			}
			TransportEvent::ListenerClosed { reason: _ } => {
				info!(peer_id = %self.peer_id, "Listen closed");
			}
		}
	}
}

impl futures::Stream for Node {
	type Item = NodeEvent;

	fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.poll_next_event(cx).map(Some)
	}
}

impl FusedStream for Node {
	fn is_terminated(&self) -> bool {
		false
	}
}

fn extract_protocol_from_multiaddr(address: &Multiaddr) -> Result<Protocol, Error> {
	let components = address.iter();
	let mut p2p_protocol: Option<Protocol> = None;

	for component in components {
		if component == MultiaddrProtocol::WebTransport {
  				p2p_protocol = Some(Protocol::WebTransport);
  				break;
  			}
	}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use multiaddr::{Multiaddr, PeerId};
    use rs_mojave_network_core::Protocol;
    use std::collections::HashMap;
    use rs_mojave_network_core::{transport::Transport, connection::Connection, stream_muxer::StreamMuxerBox};
    use async_trait::async_trait;
    use std::io;

    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct MockTransport {
        remote_peer_id: PeerId,
        listen_on_called: Arc<Mutex<bool>>,
    }

    impl MockTransport {
        fn new(remote_peer_id: PeerId) -> Self {
            Self {
                remote_peer_id,
                listen_on_called: Arc::new(Mutex::new(false)),
            }
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        type Output = (PeerId, StreamMuxerBox);
        type Error = io::Error;
        type Listener = futures::stream::Pending<Result<Self::ListenerUpgrade, Self::Error>>;
        type ListenerUpgrade = futures::future::Pending<Result<Self::Output, Self::Error>>;
        type Dial = futures::future::BoxFuture<'static, Result<Self::Output, Self::Error>>;

        fn listen_on(&mut self, _addr: Multiaddr) -> Result<Self::Listener, Self::Error> {
            *self.listen_on_called.lock().unwrap() = true;
            Ok(futures::stream::pending())
        }

        fn dial(&mut self, _peer_id: PeerId, _addr: Multiaddr) -> Result<Self::Dial, Self::Error> {
            let remote_peer_id = self.remote_peer_id.clone();
            Ok(Box::pin(async move { Ok((remote_peer_id, StreamMuxerBox::new_null())) }))
        }
    }

    #[test]
    fn test_node_creation() {
        let peer_id = PeerId::random();
        let transports = HashMap::new();
        let node = Node::new(peer_id.clone(), transports);
        assert_eq!(node.peer_id, peer_id);
    }

    #[tokio::test]
    async fn test_dial_remote_peer() {
        let local_peer_id = PeerId::random();
        let remote_peer_id = PeerId::random();
        let remote_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();

        let mock_transport = MockTransport::new(remote_peer_id.clone());
        let mut transports = HashMap::new();
        transports.insert(Protocol::WebTransport, Box::new(mock_transport.clone()) as Box<dyn Transport<Output = (PeerId, StreamMuxerBox), Error = io::Error, Listener = futures::stream::Pending<Result<futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, io::Error>>, ListenerUpgrade = futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, Dial = futures::future::BoxFuture<'static, Result<(PeerId, StreamMuxerBox), io::Error>>> + Send>);

        let mut node = Node::new(local_peer_id, transports);

        let result = node.dial(remote_peer_id.clone(), remote_addr).await;
        assert!(result.is_ok());
        assert!(node.peer_manager.lock().await.has_pending_outgoing(&remote_peer_id));
    }

    #[tokio::test]
    async fn test_listen_on_address() {
        let local_peer_id = PeerId::random();
        let listen_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();

        let mock_transport = MockTransport::new(PeerId::random());
        let mut transports = HashMap::new();
        transports.insert(Protocol::WebTransport, Box::new(mock_transport.clone()) as Box<dyn Transport<Output = (PeerId, StreamMuxerBox), Error = io::Error, Listener = futures::stream::Pending<Result<futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, io::Error>>, ListenerUpgrade = futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, Dial = futures::future::BoxFuture<'static, Result<(PeerId, StreamMuxerBox), io::Error>>> + Send>);

        let mut node = Node::new(local_peer_id, transports);

        let result = node.listen(listen_addr).await;
        assert!(result.is_ok());
        assert!(*mock_transport.listen_on_called.lock().unwrap());
    }

    #[tokio::test]
    async fn test_handle_transport_event() {
        let local_peer_id = PeerId::random();
        let local_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();
        let remote_addr: Multiaddr = "/ip4/127.0.0.1/udp/9091/webtransport"
            .parse()
            .unwrap();

        let mut node = Node::new(local_peer_id.clone(), HashMap::new());

        let remote_peer_id_for_upgrade = PeerId::random();
        let upgrade = Box::pin(async move { Ok((remote_peer_id_for_upgrade, StreamMuxerBox::new_null())) });
        let event = TransportEvent::Incoming {
            remote_addr: remote_addr.clone(),
            local_addr: local_addr.clone(),
            upgrade,
        };

        node.handle_transport_event(event);

        // We need to give some time for the event to be processed by the spawned task in handle_transport_event
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(node.peer_manager.lock().await.has_pending_incoming(&remote_addr));
    }
}
	p2p_protocol.ok_or_else(|| Error::NoProtocolsInMultiaddr(address.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use multiaddr::{Multiaddr, PeerId};
    use rs_mojave_network_core::Protocol;
    use std::collections::HashMap;
    use rs_mojave_network_core::{transport::Transport, connection::Connection, stream_muxer::StreamMuxerBox};
    use async_trait::async_trait;
    use std::io;

    #[derive(Clone)]
    struct MockTransport {
        remote_peer_id: PeerId,
    }

    #[async_trait]
    impl Transport for MockTransport {
        type Output = (PeerId, StreamMuxerBox);
        type Error = io::Error;
        type Listener = futures::stream::Pending<Result<Self::ListenerUpgrade, Self::Error>>;
        type ListenerUpgrade = futures::future::Pending<Result<Self::Output, Self::Error>>;
        type Dial = futures::future::BoxFuture<'static, Result<Self::Output, Self::Error>>;

        fn listen_on(&mut self, _addr: Multiaddr) -> Result<Self::Listener, Self::Error> {
            unimplemented!()
        }

        fn dial(&mut self, _peer_id: PeerId, _addr: Multiaddr) -> Result<Self::Dial, Self::Error> {
            let remote_peer_id = self.remote_peer_id.clone();
            Ok(Box::pin(async move { Ok((remote_peer_id, StreamMuxerBox::new_null())) }))
        }
    }

    #[test]
    fn test_node_creation() {
        let peer_id = PeerId::random();
        let transports = HashMap::new();
        let node = Node::new(peer_id.clone(), transports);
        assert_eq!(node.peer_id, peer_id);
    }

    #[tokio::test]
    async fn test_dial_remote_peer() {
        let local_peer_id = PeerId::random();
        let remote_peer_id = PeerId::random();
        let remote_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();

        let mock_transport = MockTransport { remote_peer_id: remote_peer_id.clone() };
        let mut transports = HashMap::new();
        transports.insert(Protocol::WebTransport, Box::new(mock_transport) as Box<dyn Transport<Output = (PeerId, StreamMuxerBox), Error = io::Error, Listener = futures::stream::Pending<Result<futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, io::Error>>, ListenerUpgrade = futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, Dial = futures::future::BoxFuture<'static, Result<(PeerId, StreamMuxerBox), io::Error>>> + Send>);

        let mut node = Node::new(local_peer_id, transports);

        let result = node.dial(remote_peer_id.clone(), remote_addr).await;
        assert!(result.is_ok());
        assert!(node.peer_manager.lock().await.has_pending_outgoing(&remote_peer_id));
    }

    #[tokio::test]
    async fn test_listen_on_address() {
        let local_peer_id = PeerId::random();
        let listen_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();

        let mock_transport = MockTransport::new(PeerId::random());
        let mut transports = HashMap::new();
        transports.insert(Protocol::WebTransport, Box::new(mock_transport.clone()) as Box<dyn Transport<Output = (PeerId, StreamMuxerBox), Error = io::Error, Listener = futures::stream::Pending<Result<futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, io::Error>>, ListenerUpgrade = futures::future::Pending<Result<(PeerId, StreamMuxerBox), io::Error>>, Dial = futures::future::BoxFuture<'static, Result<(PeerId, StreamMuxerBox), io::Error>>> + Send>);

        let mut node = Node::new(local_peer_id, transports);

        let result = node.listen(listen_addr).await;
        assert!(result.is_ok());
        assert!(*mock_transport.listen_on_called.lock().unwrap());
    }

    #[tokio::test]
    async fn test_handle_transport_event() {
        let local_peer_id = PeerId::random();
        let local_addr: Multiaddr = "/ip4/127.0.0.1/udp/9090/webtransport"
            .parse()
            .unwrap();
        let remote_addr: Multiaddr = "/ip4/127.0.0.1/udp/9091/webtransport"
            .parse()
            .unwrap();

        let mut node = Node::new(local_peer_id.clone(), HashMap::new());

        let remote_peer_id_for_upgrade = PeerId::random();
        let upgrade = Box::pin(async move { Ok((remote_peer_id_for_upgrade, StreamMuxerBox::new_null())) });
        let event = TransportEvent::Incoming {
            remote_addr: remote_addr.clone(),
            local_addr: local_addr.clone(),
            upgrade,
        };

        node.handle_transport_event(event);

        // We need to give some time for the event to be processed by the spawned task in handle_transport_event
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(node.peer_manager.lock().await.has_pending_incoming(&remote_addr));
    }
}
