use futures::prelude::*;
use multiaddr::Multiaddr;
use std::{
    error::Error,
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

pub mod map;
pub mod map_err;

mod boxed;

pub use boxed::*;

use crate::Protocol;

#[derive(Debug, Clone)]
pub enum TransportError<TErr> {
    /// The [`Multiaddr`] passed as parameter is not supported.
    ///
    /// Contains back the same address.
    MultiaddrNotSupported(Multiaddr),

    /// Any other error that a [`Transport`] may produce.
    Other(TErr),
}

impl<TErr> TransportError<TErr> {
    /// Applies a function to the contained error if this is a [`TransportError::Other`],
    /// leaving a [`TransportError::MultiaddrNotSupported`] variant untouched.
    ///
    /// This is useful for changing the type of the error.
    pub fn map<TNewErr>(self, map: impl FnOnce(TErr) -> TNewErr) -> TransportError<TNewErr> {
        match self {
            TransportError::MultiaddrNotSupported(addr) => {
                TransportError::MultiaddrNotSupported(addr)
            }
            TransportError::Other(err) => TransportError::Other(map(err)),
        }
    }
}

impl<TErr> fmt::Display for TransportError<TErr>
where
    TErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::MultiaddrNotSupported(addr) => {
                write!(f, "Multiaddr is not supported: {addr}")
            }
            TransportError::Other(err) => write!(f, "Transport error other error: {err}"),
        }
    }
}

impl<TErr> Error for TransportError<TErr>
where
    TErr: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TransportError::MultiaddrNotSupported(_) => None,
            TransportError::Other(err) => Some(err),
        }
    }
}

/// Represents events that can occur on a [`Transport`].
#[derive(Debug)]
pub enum TransportEvent<TUpgrade, TError> {
    /// A new incoming connection is pending.
    ///
    /// The `upgrade` is a [`Future`] that must be polled to complete the connection setup.
    /// This future resolves to the connection object (`Self::Output` of the [`Transport`])
    /// or an error if the setup fails.
    Incoming {
        /// The address of the remote peer that initiated the connection.
        remote_addr: Multiaddr,
        /// The local address on which the connection was received.
        local_addr: Multiaddr,
        /// A future representing the pending connection upgrade.
        upgrade: TUpgrade,
    },

    /// The transport is now listening on the given [`Multiaddr`].
    ListenAddress {
        /// The address the transport is now listening on.
        address: Multiaddr,
    },

    /// A previously established listen address has expired or is no longer valid.
    AddressExpired {
        /// The address that has expired.
        address: Multiaddr,
    },

    /// An error occurred on one of the listeners.
    ListenerError {
        /// The error that occurred.
        error: TError,
    },

    /// A listener has been closed.
    ///
    /// This can be due to an error or a manual closing operation.
    /// The `reason` provides details: `Ok(())` for a clean close, or `Err(TError)` if closed due to an error.
    ListenerClosed {
        /// The reason for the listener closing.
        reason: Result<(), TError>,
    },
}

/// Represents a network transport, an abstraction over a specific
/// network communication mechanism (e.g., TCP, UDP, WebRTC, WebTransport).
///
/// A `Transport` is responsible for:
/// - Establishing outgoing connections to remote peers (dialing).
/// - Accepting incoming connections from remote peers (listening).
/// - Producing a stream of [`TransportEvent`]s indicating connection attempts,
///   successful listening, errors, etc.
///
/// The connections established by a `Transport` typically yield an object
/// (defined by `Self::Output`) that can be used for communication, often
/// a type that implements [`AsyncRead`](futures::io::AsyncRead) and
/// [`AsyncWrite`](futures::io::AsyncWrite), or a [`StreamMuxer`].
pub trait Transport {
    /// The type of the successfully established connection, output by `Dial` and `ListenerUpgrade` futures.
    ///
    /// This is often a connection that implements [`AsyncRead`](futures::io::AsyncRead) and
    /// [`AsyncWrite`](futures::io::AsyncWrite), or a [`StreamMuxer`].
    type Output;

    /// The type of error that can occur during dialing or listening.
    type Error: Error;

    /// A [`Future`] that resolves to a successfully established outbound connection (`Self::Output`)
    /// or an error (`Self::Error`).
    type Dial: Future<Output = Result<Self::Output, Self::Error>>;

    /// A [`Future`] that resolves to a successfully established inbound connection (`Self::Output`)
    /// or an error (`Self::Error`). This is part of the [`TransportEvent::Incoming`] event.
    type ListenerUpgrade: Future<Output = Result<Self::Output, Self::Error>>;

    /// Returns the [`Protocol`] or set of protocols that this transport can dial.
    ///
    /// This is used to determine if the transport can handle a given [`Multiaddr`].
    fn supported_protocols_for_dialing(&self) -> Protocol;

    /// Dials a remote peer at the given [`Multiaddr`].
    ///
    /// # Parameters
    /// - `address`: The [`Multiaddr`] of the remote peer to connect to.
    ///
    /// # Returns
    /// - `Ok(Self::Dial)`: If the `address` is supported, returns a [`Future`] (`Self::Dial`)
    ///   that will resolve to either a connection (`Self::Output`) or an error (`Self::Error`).
    /// - `Err(TransportError)`: If the `address` is not supported by this transport
    ///   (e.g., wrong protocol like trying to dial a TCP address with a UDP transport),
    ///   or if another preliminary error occurs.
    ///
    /// # Behavior
    /// This method initiates the dialing process. The actual connection establishment
    /// happens when the returned `Self::Dial` future is polled.
    fn dial(&mut self, address: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>;

    /// Instructs the transport to start listening on the given [`Multiaddr`].
    ///
    /// # Parameters
    /// - `address`: The [`Multiaddr`] to listen on for incoming connections.
    ///
    /// # Returns
    /// - `Ok(())`: If the transport can attempt to listen on this `address`.
    ///   Note that this does not mean listening has started successfully yet.
    ///   A [`TransportEvent::ListenAddress`] or [`TransportEvent::ListenerError`]
    ///   will be emitted later via `poll` to indicate the outcome.
    /// - `Err(TransportError)`: If the `address` is not supported or another
    ///   error prevents listening from being attempted.
    ///
    /// # Behavior
    /// This method typically queues an operation to start listening. The actual outcome
    /// (success or failure) will be reported through events from the `poll` method.
    /// Multiple `listen_on` calls can be made to listen on several addresses.
    fn listen_on(&mut self, address: Multiaddr) -> Result<(), TransportError<Self::Error>>;

    /// Polls the transport for new events.
    ///
    /// This method is the central point for driving the transport's state and receiving
    /// notifications about its activities (new connections, listener events, etc.).
    /// It should be called repeatedly as part of an event loop.
    ///
    /// # Parameters
    /// - `cx`: The [`Context`] for the current task, used for waking up the task
    ///   when new events are ready.
    ///
    /// # Returns
    /// - `Poll::Ready(TransportEvent)`: An event has occurred. The specific variant of
    ///   [`TransportEvent`] provides details about the event (e.g., an incoming connection,
    ///   a listener error).
    /// - `Poll::Pending`: No new events are ready at this time. The transport will
    ///   wake up the current task via `cx.waker()` when an event becomes available.
    ///
    /// # Lifecycle
    /// This method drives the lifecycle of the transport. For example, after calling
    /// `listen_on`, you would call `poll` to receive a `TransportEvent::ListenAddress`
    /// if successful, or `TransportEvent::ListenerError` if not. Similarly, incoming
    /// connections are reported as `TransportEvent::Incoming`, which contains a
    /// `Self::ListenerUpgrade` future that must then be polled to completion.
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<TransportEvent<Self::ListenerUpgrade, Self::Error>>;

    /// Boxes the transport, including custom transport errors.
    fn boxed(self) -> boxed::Boxed<Self::Output>
    where
        Self: Sized + Send + Unpin + 'static,
        Self::Dial: Send + 'static,
        Self::ListenerUpgrade: Send + 'static,
        Self::Error: Send + Sync,
    {
        boxed::boxed(self)
    }

    /// Applies a function on the connections created by the transport.
    fn map<F, O>(self, f: F) -> map::Map<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Output) -> O,
    {
        map::Map::new(self, f)
    }

    /// Applies a function on the errors generated by the futures of the transport.
    fn map_err<F, E>(self, f: F) -> map_err::MapErr<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Error) -> E,
    {
        map_err::MapErr::new(self, f)
    }
}

impl<TUpgr, TErr> TransportEvent<TUpgr, TErr> {
    /// In case this [`TransportEvent`] is an upgrade, apply the given function
    /// to the upgrade and produce another transport event based the function's result.
    pub fn map_upgrade<U>(self, map: impl FnOnce(TUpgr) -> U) -> TransportEvent<U, TErr> {
        match self {
            TransportEvent::Incoming {
                upgrade,
                local_addr,
                remote_addr,
            } => TransportEvent::Incoming {
                upgrade: map(upgrade),
                local_addr,
                remote_addr,
            },
            TransportEvent::ListenAddress { address } => TransportEvent::ListenAddress { address },
            TransportEvent::AddressExpired { address } => {
                TransportEvent::AddressExpired { address }
            }
            TransportEvent::ListenerError { error } => TransportEvent::ListenerError { error },
            TransportEvent::ListenerClosed { reason } => TransportEvent::ListenerClosed { reason },
        }
    }

    /// In case this [`TransportEvent`] is an [`ListenerError`](TransportEvent::ListenerError),
    /// or [`ListenerClosed`](TransportEvent::ListenerClosed) apply the given function to the
    /// error and produce another transport event based on the function's result.
    pub fn map_err<E>(self, map_err: impl FnOnce(TErr) -> E) -> TransportEvent<TUpgr, E> {
        match self {
            TransportEvent::Incoming {
                upgrade,
                local_addr,
                remote_addr,
            } => TransportEvent::Incoming {
                upgrade,
                local_addr,
                remote_addr,
            },
            TransportEvent::ListenAddress { address } => TransportEvent::ListenAddress { address },
            TransportEvent::AddressExpired { address } => {
                TransportEvent::AddressExpired { address }
            }
            TransportEvent::ListenerError { error } => TransportEvent::ListenerError {
                error: map_err(error),
            },
            TransportEvent::ListenerClosed { reason } => TransportEvent::ListenerClosed {
                reason: reason.map_err(map_err),
            },
        }
    }
}
