//! The byte stream under a connection: plain TCP or TLS.
//!
//! Kept as a concrete enum rather than `Box<dyn AsyncRead + AsyncWrite>`
//! because the connection actor splits it, and splitting a trait object costs
//! an allocation and a vtable hop on every frame for no benefit — there are
//! exactly two variants and there will not be a third.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

/// A connected byte stream.
#[derive(Debug)]
pub enum Transport {
    /// An unencrypted TCP connection.
    Plain(TcpStream),
    /// A TLS session over TCP.
    ///
    /// Boxed: `TlsStream` carries rustls' session state and is an order of
    /// magnitude larger than a `TcpStream`, so an unboxed enum would make every
    /// plaintext connection pay for it.
    Tls(Box<TlsStream<TcpStream>>),
}

impl Transport {
    /// Whether this stream is encrypted. Used by the SASL layer, which must
    /// refuse to send a password in the clear unless asked to explicitly.
    pub fn is_encrypted(&self) -> bool {
        matches!(self, Transport::Tls(_))
    }

    /// Turn off Nagle's algorithm.
    ///
    /// Kafka is request/response over long-lived sockets; batching a small
    /// request into the next write adds up to 40ms to every round trip.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        match self {
            Transport::Plain(s) => s.set_nodelay(nodelay),
            Transport::Tls(s) => s.get_ref().0.set_nodelay(nodelay),
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_flush(cx),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Transport::Plain(s) => s.is_write_vectored(),
            Transport::Tls(s) => s.is_write_vectored(),
        }
    }
}
