//! Driving one RPC: IPC frame in, tower service, channel frames out.

use bytes::Bytes;
use connectrpc::{ConnectRpcService, dispatcher::Dispatcher};
use http::{HeaderMap, HeaderName, HeaderValue, Request};
use http_body_util::BodyExt;
use tauri::ipc::{Channel, InvokeResponseBody};
use tower::ServiceExt;

use crate::{body::IpcRequestBody, codec, registry::CallRegistry, wire};

/// Base URI for the synthetic HTTP request.
///
/// Nothing resolves this host: the Connect runtime routes on the path alone.
/// It exists because `http::Request` requires an absolute URI to carry an
/// authority, and handlers reading the URI see a stable, obviously-local value.
const IPC_ORIGIN: &str = "tauri://ipc";

/// Run one RPC to completion.
///
/// Returns the encoded [`wire::ResponseHead`] once status and headers are
/// known; the body streams onward over `channel` from a spawned task.
///
/// With no `channel` the call cannot stream, so there is nothing to gain from
/// answering early: the head is withheld until the body is complete and
/// carries it, which costs one webview crossing instead of three.
pub(crate) async fn start<D: Dispatcher>(
    service: ConnectRpcService<D>,
    calls: std::sync::Arc<CallRegistry>,
    start: wire::StartRequest,
    channel: Option<Channel<InvokeResponseBody>>,
) -> Result<Vec<u8>, String> {
    let call_id = start.call_id;

    let (body, body_tx, first_chunk) = if start.streaming_request {
        let (tx, body) = IpcRequestBody::channel();
        // The first chunk rides with the head; later ones arrive via
        // `connect_rpc_send`. It is queued below, after registration, so this
        // function reaches the registry without an intervening await.
        let first = (!start.body.is_empty()).then(|| Bytes::from(start.body));
        (body, Some(tx), first)
    } else {
        (
            IpcRequestBody::complete(Bytes::from(start.body)),
            None,
            None,
        )
    };

    let request = build_request(&start.url, &start.method, &start.headers, body)?;

    // No channel means no streaming, and a non-streaming call has no reason to
    // be registered: nothing can send to it, and a cancel has nothing to abort
    // beyond dropping this future.
    let Some(channel) = channel else {
        return buffered(service, request).await;
    };

    // Dispatch on a separate task and return as soon as the head is known.
    // A client-streaming or bidi handler parks on its first request message,
    // which cannot arrive until this function returns and `connect_rpc_send`
    // can run, so awaiting the service inline would deadlock.
    let (head_tx, head_rx) = tokio::sync::oneshot::channel();
    // Registered before this function's first await so a `connect_rpc_send`
    // issued right after the invoke resolves can never miss the call id, and so
    // the pump's completion cleanup cannot race ahead of the insert.
    let registration = calls.begin(call_id, body_tx.clone());

    let pump = tokio::spawn({
        let calls = std::sync::Arc::clone(&calls);
        async move {
            // `Infallible` error type, so this only fails if a handler panics.
            let response = match service.oneshot(request).await {
                Ok(response) => response,
                Err(e) => {
                    let _ = head_tx.send(Err(format!("connect service failed: {e}")));
                    return;
                }
            };

            let (parts, mut response_body) = response.into_parts();
            let head = wire::ResponseHead {
                status: u32::from(parts.status.as_u16()),
                headers: to_wire_headers(&parts.headers),
                ..Default::default()
            };

            // Resolving the head lets the client start reading; body frames
            // follow on the channel.
            if head_tx.send(Ok(codec::encode(&head))).is_err() {
                return;
            }

            while let Some(frame) = response_body.frame().await {
                // A handler that yields items without awaiting (an in-memory
                // stream) never suspends this loop, so an abort could not land
                // and one call would monopolise the worker. This inserts a
                // yield point only once the task's coop budget is spent.
                tokio::task::coop::consume_budget().await;

                let frame = match frame {
                    Ok(frame) => frame,
                    Err(e) => {
                        send_frame(&channel, wire::response_frame::Frame::Error(e.to_string()));
                        break;
                    }
                };

                let wire_frame = if let Some(data) = frame.data_ref() {
                    wire::response_frame::Frame::Message(data.to_vec())
                } else if let Some(trailers) = frame.trailers_ref() {
                    wire::response_frame::Frame::Trailers(Box::new(wire::Trailers {
                        headers: to_wire_headers(trailers),
                        ..Default::default()
                    }))
                } else {
                    continue;
                };

                // A send failure means the webview is gone; nothing left to serve.
                if !send_frame(&channel, wire_frame) {
                    break;
                }
            }
            // Dropping the channel ends the JS-side stream. Forgetting the call
            // here is what keeps the registry from growing without bound; a
            // client that never cancels still leaves nothing behind.
            //
            // The end marker is explicit because the JS `Channel` exposes no
            // hook for Tauri's own channel teardown.
            send_frame(&channel, wire::response_frame::Frame::End(Box::default()));
            calls.forget(call_id);
        }
    });

    registration.attach(pump.abort_handle());

    // Safe to await now: the call is registered, and the body channel has room
    // for the first chunk.
    if let (Some(tx), Some(chunk)) = (body_tx, first_chunk) {
        tx.send(chunk)
            .await
            .map_err(|_| "request body closed".to_string())?;
    }

    // A dropped sender means the pump was cancelled before producing a head.
    head_rx
        .await
        .unwrap_or_else(|_| Err("call cancelled".to_string()))
}

/// Send one frame, reporting whether the webview is still listening.
fn send_frame(channel: &Channel<InvokeResponseBody>, frame: wire::response_frame::Frame) -> bool {
    let encoded = codec::encode(&wire::ResponseFrame {
        frame: Some(frame),
        ..Default::default()
    });
    channel.send(InvokeResponseBody::Raw(encoded)).is_ok()
}

/// Run a call whose response cannot stream, answering once with the whole body.
///
/// Tauri delivers a channel frame by evaluating JavaScript in the webview, so
/// a channelled unary response costs three crossings: the invoke itself, the
/// message frame, and the end marker. A unary body is a single message that is
/// ready when the head is, so returning it inline collapses those to one.
///
/// Nothing is registered or spawned here. The request body is already complete
/// and the response is not observable until this returns, so a cancel is just
/// the caller dropping this future.
async fn buffered<D: Dispatcher>(
    service: ConnectRpcService<D>,
    request: Request<IpcRequestBody>,
) -> Result<Vec<u8>, String> {
    // `Infallible` error type, so this only fails if a handler panics.
    let response = service
        .oneshot(request)
        .await
        .map_err(|e| format!("connect service failed: {e}"))?;

    let (parts, body) = response.into_parts();
    let collected = body
        .collect()
        .await
        .map_err(|e| format!("response body failed: {e}"))?;

    // Trailers are dropped deliberately: Connect carries its own in-band, as
    // `trailer-` prefixed headers for unary, so the HTTP ones are unread.
    Ok(codec::encode(&wire::ResponseHead {
        status: u32::from(parts.status.as_u16()),
        headers: to_wire_headers(&parts.headers),
        body: collected.to_bytes().to_vec(),
        ..Default::default()
    }))
}

/// Build the synthetic HTTP request the Connect runtime expects.
fn build_request(
    url: &str,
    method: &str,
    headers: &[wire::Header],
    body: IpcRequestBody,
) -> Result<Request<IpcRequestBody>, String> {
    let mut builder = Request::builder()
        .method(http::Method::from_bytes(method.as_bytes()).map_err(|_| "invalid HTTP method")?)
        .uri(format!("{IPC_ORIGIN}{url}"));

    // Headers are not filtered: the peer is the app's own webview, and Connect
    // carries arbitrary user metadata as headers. The service still applies
    // its own protocol validation.
    for header in headers {
        let name =
            HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| "invalid header name")?;
        let value = HeaderValue::from_str(&header.value).map_err(|_| "invalid header value")?;
        builder = builder.header(name, value);
    }

    builder.body(body).map_err(|e| e.to_string())
}

/// Flatten an `http::HeaderMap` into repeated wire entries.
///
/// Multi-value headers become repeated entries rather than a joined string, so
/// the JS `Headers` object reconstructs them exactly.
fn to_wire_headers(headers: &HeaderMap) -> Vec<wire::Header> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            // Non-UTF8 header values cannot cross into JS. Connect only emits
            // ASCII values, so this drops nothing in practice.
            let value = value.to_str().ok()?;
            Some(wire::Header {
                name: name.as_str().to_string(),
                value: value.to_string(),
                ..Default::default()
            })
        })
        .collect()
}
