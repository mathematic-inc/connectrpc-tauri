//! A harness for driving the transport without a webview.
//!
//! Tauri's `Channel` is constructible from a plain closure, so the real call
//! path — framing, the tower service, the response pump, the registry — runs
//! unchanged with frames collected into a queue.

use std::sync::Arc;

use buffa::Message;
use bytes::Bytes;
use connectrpc::{ConnectRpcService, dispatcher::Dispatcher};
use tauri::ipc::{Channel, InvokeResponseBody};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

use crate::{call, registry::CallRegistry, wire};

/// The in-flight `call::start` task for a streaming-request call.
type PendingHead = Mutex<Option<JoinHandle<Result<Vec<u8>, String>>>>;

/// One in-flight call under test.
pub struct TestCall<D: Dispatcher> {
    service: ConnectRpcService<D>,
    calls: Arc<CallRegistry>,
    frames: Mutex<mpsc::UnboundedReceiver<wire::ResponseFrame>>,
    channel: Channel<InvokeResponseBody>,
    call_id: u64,
    /// Set for streaming-request calls, whose head only resolves once the
    /// handler responds — which for client-streaming is after the request ends.
    pending_head: PendingHead,
}

impl<D: Dispatcher> TestCall<D> {
    /// Wrap a service, wiring a channel that collects frames.
    pub fn new(service: ConnectRpcService<D>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let channel = Channel::new(move |body: InvokeResponseBody| {
            let InvokeResponseBody::Raw(bytes) = body else {
                panic!("transport sent a JSON frame; it must always send raw bytes");
            };
            let frame = wire::ResponseFrame::decode(&mut bytes.as_slice())
                .expect("transport sent a malformed frame");
            // A closed receiver just means the test stopped reading.
            let _ = tx.send(frame);
            Ok(())
        });

        Self {
            service,
            calls: Arc::new(CallRegistry::default()),
            frames: Mutex::new(rx),
            channel,
            call_id: 1,
            pending_head: Mutex::new(None),
        }
    }

    /// Start a unary or server-streaming call with a complete request body.
    pub async fn start(&self, url: &str, body: &[u8]) -> Result<wire::ResponseHead, String> {
        self.start_inner(url, body.to_vec(), false).await
    }

    /// Start a call that asks for its response inline, as unary does.
    ///
    /// No channel is passed, so the head comes back carrying the whole body —
    /// the path every unary call takes in production.
    pub async fn start_buffered(
        &self,
        url: &str,
        body: &[u8],
    ) -> Result<wire::ResponseHead, String> {
        let start = self.start_message(url, body.to_vec(), false);
        let head_bytes =
            call::start(self.service.clone(), Arc::clone(&self.calls), start, None).await?;
        Ok(wire::ResponseHead::decode(&mut head_bytes.as_slice()).expect("malformed head"))
    }

    /// Start a server-streaming call, encoding `request` as one Connect frame.
    pub async fn start_streaming_response<M: Message>(
        &self,
        url: &str,
        request: &M,
    ) -> Result<wire::ResponseHead, String> {
        self.start_inner(url, enveloped(request), false).await
    }

    /// Start a client-streaming or bidi call; messages follow via [`Self::send`].
    ///
    /// Returns once the call is registered, without waiting for the response
    /// head: a client-streaming handler does not respond until it has read the
    /// whole request, so waiting here would deadlock against [`Self::send`].
    pub async fn start_streaming_request(&self, url: &str) -> Result<(), String> {
        let start = self.start_message(url, Vec::new(), true);
        let service = self.service.clone();
        let calls = Arc::clone(&self.calls);
        let channel = self.channel.clone();

        // Register synchronously here rather than inside the spawned task, so
        // a `send` issued right after this returns always finds the call.
        let handle =
            tokio::spawn(async move { call::start(service, calls, start, Some(channel)).await });
        *self.pending_head.lock().await = Some(handle);

        // `call::start` registers before its first await, but the task still
        // has to be polled once to get there.
        while self.calls.body_sender(self.call_id).is_none() {
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// Await the head of a streaming-request call started earlier.
    pub async fn head(&self) -> Result<wire::ResponseHead, String> {
        let handle = self
            .pending_head
            .lock()
            .await
            .take()
            .expect("no streaming-request call in flight");
        let bytes = handle.await.expect("call task panicked")?;
        Ok(wire::ResponseHead::decode(&mut bytes.as_slice()).expect("malformed head"))
    }

    async fn start_inner(
        &self,
        url: &str,
        body: Vec<u8>,
        streaming_request: bool,
    ) -> Result<wire::ResponseHead, String> {
        let start = self.start_message(url, body, streaming_request);

        let head_bytes = call::start(
            self.service.clone(),
            Arc::clone(&self.calls),
            start,
            Some(self.channel.clone()),
        )
        .await?;

        Ok(wire::ResponseHead::decode(&mut head_bytes.as_slice()).expect("malformed head"))
    }

    fn start_message(
        &self,
        url: &str,
        body: Vec<u8>,
        streaming_request: bool,
    ) -> wire::StartRequest {
        wire::StartRequest {
            call_id: self.call_id,
            url: url.to_string(),
            method: "POST".to_string(),
            headers: vec![wire::Header {
                name: "content-type".to_string(),
                value: if streaming_request || !body.is_empty() && is_enveloped(&body) {
                    "application/connect+proto".to_string()
                } else {
                    "application/proto".to_string()
                },
                ..Default::default()
            }],
            body,
            streaming_request,
            ..Default::default()
        }
    }

    /// Push one request body chunk, as `connect_rpc_send` would.
    pub async fn send(&self, chunk: &[u8]) {
        let tx = self
            .calls
            .body_sender(self.call_id)
            .expect("call is not accepting a request body");
        tx.send(Bytes::copy_from_slice(chunk))
            .await
            .expect("request body closed");
    }

    /// Signal end of the request body.
    pub async fn end_request(&self) {
        self.calls.close_request_body(self.call_id);
    }

    /// Cancel the call, as `connect_rpc_cancel` would.
    pub fn cancel(&self) {
        self.calls.remove(self.call_id);
    }

    /// How many calls the registry still tracks.
    pub fn in_flight(&self) -> usize {
        self.calls.len()
    }

    /// Next frame, or `None` once the response ends.
    async fn next_frame(&self) -> Option<wire::ResponseFrame> {
        let mut frames = self.frames.lock().await;
        match frames.recv().await?.frame? {
            wire::response_frame::Frame::End(_) => None,
            other => Some(wire::ResponseFrame {
                frame: Some(other),
                ..Default::default()
            }),
        }
    }

    /// All message bytes until the response ends.
    pub async fn collect_message_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(frame) = self.next_frame().await {
            if let Some(wire::response_frame::Frame::Message(bytes)) = frame.frame {
                out.extend_from_slice(&bytes);
            }
        }
        out
    }

    /// Decode the single message of a unary response.
    pub async fn expect_unary_message<M: Message + Default>(&self) -> M {
        let bytes = self.collect_message_bytes().await;
        M::decode(&mut bytes.as_slice()).expect("malformed unary response")
    }

    /// Decode every enveloped message of a streaming response.
    ///
    /// Connect's end-of-stream envelope (flag bit 1) carries trailers as JSON,
    /// not a message, so it is skipped.
    pub async fn collect_stream_messages<M: Message + Default>(&self) -> Vec<M> {
        let bytes = self.collect_message_bytes().await;
        decode_envelopes(&bytes)
    }

    /// Decode the next enveloped message without waiting for the stream to end.
    ///
    /// Needed for bidi, where the test must read a response before sending the
    /// next request.
    pub async fn next_message<M: Message + Default>(&self) -> Option<M> {
        while let Some(frame) = self.next_frame().await {
            if let Some(wire::response_frame::Frame::Message(bytes)) = frame.frame
                && let Some(message) = decode_envelopes(&bytes).into_iter().next()
            {
                return Some(message);
            }
        }
        None
    }
}

/// Split a buffer of Connect envelopes into decoded messages.
fn decode_envelopes<M: Message + Default>(bytes: &[u8]) -> Vec<M> {
    let mut out = Vec::new();
    let mut offset = 0;
    while offset + 5 <= bytes.len() {
        let flags = bytes[offset];
        let len = u32::from_be_bytes([
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
        ]) as usize;
        let start = offset + 5;
        let end = start + len;
        assert!(end <= bytes.len(), "truncated envelope");

        // Bit 1 marks Connect's EndStreamResponse, which is JSON trailers.
        if flags & 0x02 == 0 {
            out.push(M::decode(&mut &bytes[start..end]).expect("malformed stream message"));
        }
        offset = end;
    }
    out
}

/// Wrap a message in a Connect envelope: flags byte, big-endian length, payload.
fn enveloped<M: Message>(message: &M) -> Vec<u8> {
    let bytes = message.encode_to_vec();
    let mut framed = Vec::with_capacity(bytes.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    framed
}

/// Whether a body looks enveloped, used only to pick a test content type.
fn is_enveloped(body: &[u8]) -> bool {
    body.len() >= 5 && body[0] & 0xfc == 0
}
