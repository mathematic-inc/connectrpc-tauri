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
//!
//! # Who may call
//!
//! Only the app's own pages, as Tauri serves them (`tauri://localhost`, or the
//! `devUrl` during development). Every frame in the webview can reach this
//! scheme, including iframes at other origins: a remote page, or content an app
//! serves from a custom protocol of its own, which Tauri counts as local. Tauri's
//! own IPC keeps those out with an invoke key it gives the main frame only; a
//! scheme has no such key, so it checks the request's `Origin` instead, which a
//! page can't set.

use std::borrow::Cow;

use bytes::Bytes;
use connectrpc::{ConnectRpcService, dispatcher::Dispatcher};
use http::{Response, header};
use http_body_util::BodyExt;
use tauri::{Runtime, utils::config::BuildConfig, utils::config::FrontendDist};
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
    builder.register_asynchronous_uri_scheme_protocol(SCHEME, move |ctx, request, responder| {
        let origins = app_origins(&ctx.app_handle().config().build, tauri::is_dev());
        // Cloning is cheap: every field behind `ConnectRpcService` is an `Arc`
        // or a `Copy` policy.
        let service = service.clone();
        tauri::async_runtime::spawn(async move {
            responder.respond(handle(service, request, &origins).await);
        });
    })
}

/// The origins the app's own pages are served from, which Tauri's
/// `WebviewUrl::App` resolves against: the `devUrl` during development, a
/// `frontendDist` URL, and Tauri's own protocol.
fn app_origins(build: &BuildConfig, dev: bool) -> Vec<String> {
    let mut origins = if cfg!(windows) || cfg!(target_os = "android") {
        vec![
            "http://tauri.localhost".to_owned(),
            "https://tauri.localhost".to_owned(),
        ]
    } else {
        vec!["tauri://localhost".to_owned()]
    };
    let base = if dev {
        build.dev_url.as_ref()
    } else {
        match &build.frontend_dist {
            Some(FrontendDist::Url(url)) => Some(url),
            _ => None,
        }
    };
    origins.extend(base.map(|url| url.origin().ascii_serialization()));
    origins
}

/// Answer one request off the scheme handler, if it comes from one of
/// `origins`.
async fn handle<D: Dispatcher>(
    service: ConnectRpcService<D>,
    request: http::Request<Vec<u8>>,
    origins: &[String],
) -> Response<Cow<'static, [u8]>> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|origin| origin.to_str().ok())
        .filter(|origin| origins.iter().any(|allowed| allowed == origin))
        .map(str::to_owned);
    // Without CORS headers, a page that sent this can't read even the refusal.
    let Some(origin) = origin else {
        return Response::builder()
            .status(http::StatusCode::FORBIDDEN)
            .body(Cow::Borrowed(&[][..]))
            .expect("static refusal");
    };

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
            &origin,
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
    cors(
        Response::from_parts(parts, Cow::Owned(collected.to_bytes().to_vec())),
        &origin,
    )
}

/// Permit the app's page to read the response.
///
/// The page runs on `tauri://localhost` (or `http://tauri.localhost`), so every
/// request here is cross-origin and the response is unreadable without this.
/// It names the page's origin rather than `*`, so no other page can read it.
fn cors(mut response: Response<Cow<'static, [u8]>>, origin: &str) -> Response<Cow<'static, [u8]>> {
    let headers = response.headers_mut();
    if let Ok(origin) = header::HeaderValue::from_str(origin) {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin);
    }
    headers.insert(header::VARY, header::HeaderValue::from_static("Origin"));
    // Connect reads its status and metadata off response headers, and a
    // cross-origin response exposes none by default.
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        header::HeaderValue::from_static("*"),
    );
    response
}

#[cfg(test)]
mod tests {
    use connectrpc::Router;

    use super::*;

    const APP: &str = "tauri://localhost";

    async fn call(method: http::Method, origin: Option<&str>) -> Response<Cow<'static, [u8]>> {
        let mut request = http::Request::builder()
            .method(method)
            .uri(format!("{SCHEME}://localhost/greet.v1.GreetService/Greet"))
            .header(header::CONTENT_TYPE, "application/proto");
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        let request = request.body(Vec::new()).expect("a valid request");
        handle(
            ConnectRpcService::new(Router::new()),
            request,
            &[APP.to_owned()],
        )
        .await
    }

    fn allowed_origin<'a>(response: &'a Response<Cow<'static, [u8]>>) -> Option<&'a str> {
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|origin| origin.to_str().ok())
    }

    #[tokio::test]
    async fn the_apps_page_is_answered_and_named_as_the_only_reader() {
        let response = call(http::Method::POST, Some(APP)).await;
        assert_ne!(response.status(), http::StatusCode::FORBIDDEN);
        assert_eq!(allowed_origin(&response), Some(APP));

        let preflight = call(http::Method::OPTIONS, Some(APP)).await;
        assert_eq!(preflight.status(), http::StatusCode::OK);
        assert_eq!(allowed_origin(&preflight), Some(APP));
    }

    #[tokio::test]
    async fn a_frame_at_another_origin_is_refused() {
        for origin in ["myapp://widget", "https://example.com", "null"] {
            for method in [http::Method::POST, http::Method::OPTIONS] {
                let response = call(method, Some(origin)).await;
                assert_eq!(response.status(), http::StatusCode::FORBIDDEN, "{origin}");
                assert_eq!(allowed_origin(&response), None, "{origin}");
            }
        }
    }

    #[tokio::test]
    async fn a_request_without_an_origin_is_refused() {
        let response = call(http::Method::POST, None).await;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn the_dev_url_is_the_apps_during_development_only() {
        let build = BuildConfig {
            dev_url: Some("http://localhost:1420/".parse().expect("a URL")),
            ..Default::default()
        };
        let dev = "http://localhost:1420".to_owned();
        assert!(app_origins(&build, true).contains(&dev));
        assert!(!app_origins(&build, false).contains(&dev));
        let own = if cfg!(windows) || cfg!(target_os = "android") {
            "http://tauri.localhost"
        } else {
            APP
        };
        assert!(app_origins(&build, false).contains(&own.to_owned()));
    }
}
