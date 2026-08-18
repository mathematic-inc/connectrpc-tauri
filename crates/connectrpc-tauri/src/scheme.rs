//! The `ipc-connect://` custom protocol: a direct path for unary calls.
//!
//! Tauri's own IPC is a command router: a request crosses as an `InvokeBody`,
//! is matched against a command name, checked against the ACL, then answered.
//! A custom URI scheme skips all of it. `ConnectRpcService` is already a
//! `tower::Service<http::Request>`, and the webview's `fetch` already speaks
//! HTTP, so the handler here is close to an identity mapping: no envelope, no
//! call registry, no command dispatch.
//!
//! # Why unary only
//!
//! A scheme responder is one-shot on every platform: [`UriSchemeResponder`]
//! takes a complete `Response` by value, and underneath it WKWebView does
//! `didReceiveResponse` then a single `didReceiveData`, while WebKitGTK builds
//! its stream from a finished buffer. Nothing can push a second frame, so a
//! server-streaming response would only arrive once it had been buffered
//! whole. Streaming keeps the channel transport, which really does stream.

use std::borrow::Cow;

use bytes::Bytes;
use connectrpc::{ConnectRpcService, dispatcher::Dispatcher};
use http::{Response, header};
use http_body_util::BodyExt;
use tauri::Runtime;
use tower::ServiceExt;

use crate::body::IpcRequestBody;

/// The URI scheme unary calls travel over.
///
/// Not `ipc`: that is Tauri's own, and overriding it would break every stock
/// plugin. The name doubles as a hostname label on Windows and Android, where
/// wry rewrites `scheme://localhost` to `http://scheme.localhost`, so it is
/// restricted to characters legal in both a scheme and a host: `+` would be
/// valid in a URI scheme but not in that hostname.
pub const SCHEME: &str = "ipc-connect";

/// Register the unary fast path on a Tauri plugin builder.
pub(crate) fn register<R: Runtime, D: Dispatcher>(
    builder: tauri::plugin::Builder<R>,
    service: ConnectRpcService<D>,
) -> tauri::plugin::Builder<R> {
    builder.register_asynchronous_uri_scheme_protocol(SCHEME, move |_ctx, request, responder| {
        // Cloning is cheap: every field behind `ConnectRpcService` is an `Arc`
        // or a `Copy` policy.
        let service = service.clone();
        tauri::async_runtime::spawn(async move {
            responder.respond(handle(service, request).await);
        });
    })
}

/// Answer one request off the scheme handler.
async fn handle<D: Dispatcher>(
    service: ConnectRpcService<D>,
    request: http::Request<Vec<u8>>,
) -> Response<Cow<'static, [u8]>> {
    // A preflight never reaches the service: the webview sends one for the
    // `content-type` and `connect-protocol-version` headers Connect sets, and
    // it must be answered before the POST is allowed to leave.
    if request.method() == http::Method::OPTIONS {
        return cors(
            Response::builder()
                .header(header::ACCESS_CONTROL_ALLOW_HEADERS, "*")
                .header(header::ACCESS_CONTROL_ALLOW_METHODS, "POST, OPTIONS")
                .body(Cow::Borrowed(&[][..]))
                .expect("static preflight response"),
        );
    }

    let request = request.map(|body| IpcRequestBody::complete(Bytes::from(body)));

    // Both of these are infallible: the service's error type is `Infallible`,
    // and so is that of the body it returns. An irrefutable `let` says so
    // without inventing an unreachable failure path.
    let Ok(response) = service.oneshot(request).await;
    let (parts, body) = response.into_parts();
    let Ok(collected) = body.collect().await;

    // Trailers are dropped deliberately: Connect carries its own in-band, as
    // `trailer-` prefixed headers for unary, so the HTTP ones are unread.
    cors(Response::from_parts(
        parts,
        Cow::Owned(collected.to_bytes().to_vec()),
    ))
}

/// Permit the webview's origin to read the response.
///
/// The page runs on `tauri://localhost` (or `http://tauri.localhost`), so every
/// request here is cross-origin and the response is unreadable without this.
/// `*` matches what Tauri's own IPC protocol sends: the only client that can
/// reach this scheme is a webview inside this process.
fn cors(mut response: Response<Cow<'static, [u8]>>) -> Response<Cow<'static, [u8]>> {
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        header::HeaderValue::from_static("*"),
    );
    // Connect reads its status and metadata off response headers, and a
    // cross-origin response exposes none by default.
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        header::HeaderValue::from_static("*"),
    );
    response
}
