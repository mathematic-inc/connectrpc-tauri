//! The request body fed by `connect_rpc_send`.

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use connectrpc::error::ConnectError;
use http_body::{Body, Frame};
use tokio::sync::mpsc;

/// How many chunks may sit in the channel before `connect_rpc_send` resolves
/// slowly, applying backpressure to the webview.
///
/// Matches the depth `connectrpc`'s own bidi client uses for its request body.
pub(crate) const CHANNEL_DEPTH: usize = 32;

/// An `http_body::Body` fed incrementally from the IPC side.
///
/// Client-streaming and bidi calls push chunks here as they arrive rather than
/// buffering the whole request, so a long stream costs `CHANNEL_DEPTH` chunks
/// of memory rather than its full length.
pub(crate) struct IpcRequestBody {
    rx: mpsc::Receiver<Bytes>,
}

impl IpcRequestBody {
    /// Create a body plus the sender that feeds it.
    pub(crate) fn channel() -> (mpsc::Sender<Bytes>, Self) {
        let (tx, rx) = mpsc::channel(CHANNEL_DEPTH);
        (tx, Self { rx })
    }

    /// A body that is already complete, for unary and server-streaming calls
    /// where the whole request arrived in the `connect_rpc` payload.
    pub(crate) fn complete(bytes: Bytes) -> Self {
        let (tx, rx) = mpsc::channel(1);
        if !bytes.is_empty() {
            // Capacity is 1 and the channel is empty, so this cannot fail.
            let _ = tx.try_send(bytes);
        }
        drop(tx);
        Self { rx }
    }
}

impl Body for IpcRequestBody {
    type Data = Bytes;
    type Error = ConnectError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        // Dropping every sender ends the body: that is how `connect_rpc_send`
        // signals `endOfStream`, and how a cancel unblocks a stalled handler.
        self.rx
            .poll_recv(cx)
            .map(|opt| opt.map(|bytes| Ok(Frame::data(bytes))))
    }
}
