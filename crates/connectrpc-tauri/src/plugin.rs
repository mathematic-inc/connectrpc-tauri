//! The Tauri plugin exposing a `ConnectRpcService` over IPC.

use std::{future::Future, pin::Pin, str::FromStr, sync::Arc};

use bytes::Bytes;
use connectrpc::{ConnectRpcService, dispatcher::Dispatcher};
use tauri::{
    Manager, Runtime,
    ipc::{InvokeResponseBody, JavaScriptChannelId},
    plugin::TauriPlugin,
};

use crate::{call, codec, registry::CallRegistry, scheme, wire};

/// Name under which the plugin registers. The webview addresses commands as
/// `plugin:connectrpc-tauri|connect_rpc`.
///
/// This must equal the crate name: Tauri's ACL keys a plugin's permissions on
/// `CARGO_PKG_NAME`, so a different runtime name makes every command fail the
/// permission check with "Plugin not found" even though the capability lists
/// it.
pub const PLUGIN_NAME: &str = "connectrpc-tauri";

/// Starts one RPC against the erased service.
///
/// `#[tauri::command]` functions cannot be generic, so the dispatcher type is
/// erased behind this closure at plugin-construction time.
type StartFn = Box<
    dyn Fn(
            wire::StartRequest,
            Option<tauri::ipc::Channel<InvokeResponseBody>>,
            Arc<CallRegistry>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
        + Send
        + Sync,
>;

/// Transport state managed by Tauri.
pub(crate) struct TransportState {
    pub(crate) calls: Arc<CallRegistry>,
    start: StartFn,
}

/// Serve a [`ConnectRpcService`] over Tauri IPC.
///
/// ```rust,ignore
/// let router = Arc::new(MyService).register(connectrpc::Router::new());
/// tauri::Builder::default()
///     .plugin(connectrpc_tauri::serve(ConnectRpcService::new(router)))
///     .run(tauri::generate_context!())?;
/// ```
pub fn serve<R: Runtime, D: Dispatcher>(service: ConnectRpcService<D>) -> TauriPlugin<R> {
    let builder = tauri::plugin::Builder::new(PLUGIN_NAME);
    // Unary calls take the `ipc-connect://` scheme instead of a command; the
    // commands below still carry every streaming kind.
    let builder = scheme::register(builder, service.clone());
    builder
        .setup(move |app, _api| {
            let state = TransportState {
                calls: Arc::new(CallRegistry::default()),
                start: Box::new(move |start, channel, calls| {
                    // Cloning is cheap: every field behind `ConnectRpcService`
                    // is an `Arc` or a `Copy` policy.
                    let service = service.clone();
                    Box::pin(async move { call::start(service, calls, start, channel).await })
                }),
            };
            app.manage(state);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            connect_rpc,
            connect_rpc_send,
            connect_rpc_cancel
        ])
        .build()
}

/// Start an RPC.
///
/// Resolves with an encoded [`wire::ResponseHead`] as soon as the service
/// produces status and headers; body frames follow on the channel.
#[tauri::command]
async fn connect_rpc<R: Runtime>(
    webview: tauri::Webview<R>,
    request: tauri::ipc::Request<'_>,
) -> Result<tauri::ipc::Response, String> {
    let start: wire::StartRequest = codec::decode(request.body())?;

    // The channel id rides inside the protobuf rather than arriving as a JSON
    // command argument: a raw IPC payload leaves no room for JSON args.
    //
    // An empty id means the client wants the body inline: a response that
    // cannot stream is cheaper returned from this invoke than pushed over a
    // channel, which Tauri delivers by evaluating JavaScript per frame.
    let channel = if start.channel.is_empty() {
        None
    } else {
        Some(
            JavaScriptChannelId::from_str(&start.channel)
                .map_err(|e| format!("invalid response channel: {e}"))?
                .channel_on(webview.clone()),
        )
    };

    let state = webview.state::<TransportState>();
    let calls = Arc::clone(&state.calls);
    let bytes = (state.start)(start, channel, calls).await?;
    Ok(tauri::ipc::Response::new(bytes))
}

/// Push another request body chunk into an in-flight call.
#[tauri::command]
async fn connect_rpc_send<R: Runtime>(
    webview: tauri::Webview<R>,
    request: tauri::ipc::Request<'_>,
) -> Result<(), String> {
    let send: wire::SendRequest = codec::decode(request.body())?;
    let calls = Arc::clone(&webview.state::<TransportState>().calls);

    if send.end_of_stream {
        calls.close_request_body(send.call_id);
        return Ok(());
    }

    let Some(tx) = calls.body_sender(send.call_id) else {
        // The call already finished or was cancelled; the handler is gone and
        // has nowhere to put this chunk.
        return Ok(());
    };

    // Awaiting is the backpressure: while the handler is not draining, this
    // invoke stays pending and the client stops writing.
    tx.send(Bytes::from(send.chunk))
        .await
        .map_err(|_| "request body closed".to_string())
}

/// Abandon an in-flight call.
#[tauri::command]
async fn connect_rpc_cancel<R: Runtime>(
    webview: tauri::Webview<R>,
    request: tauri::ipc::Request<'_>,
) -> Result<(), String> {
    let cancel: wire::CancelRequest = codec::decode(request.body())?;
    webview
        .state::<TransportState>()
        .calls
        .remove(cancel.call_id);
    Ok(())
}
