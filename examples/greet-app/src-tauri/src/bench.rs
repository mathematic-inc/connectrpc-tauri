//! Raw Tauri IPC baselines, so the transport's cost can be read against the
//! floor of what Tauri itself can do.
//!
//! Each command here does the least work that still moves the same bytes as
//! the equivalent Connect call, so the difference between the two is the
//! transport's overhead and nothing else.

use buffa::Message;
use connectrpc_tauri::greet::{GreetRequest, GreetResponse};
use tauri::ipc::{Channel, InvokeBody, InvokeResponseBody, Request, Response};

/// Unary floor: protobuf in, protobuf out, one invoke, no Connect involved.
///
/// Mirrors `GreetService.Greet` exactly, so the delta against the real client
/// is the price of the Connect protocol plus this crate's framing.
#[tauri::command]
pub async fn bench_raw_unary(request: Request<'_>) -> Result<Response, String> {
    let InvokeBody::Raw(bytes) = request.body() else {
        return Err("expected a raw IPC payload".to_string());
    };
    let decoded = GreetRequest::decode(&mut bytes.as_slice()).map_err(|e| e.to_string())?;
    let response = GreetResponse {
        greeting: format!("Hello, {}!", decoded.name),
        ..Default::default()
    };
    Ok(Response::new(response.encode_to_vec()))
}

/// The same work through Tauri's default JSON arguments.
///
/// This is what an app would write without any of this crate, and it is the
/// comparison a reader actually cares about.
#[tauri::command]
pub async fn bench_raw_json(name: String) -> String {
    format!("Hello, {name}!")
}

/// The same work, but with a *binary* payload carried as JSON arguments.
///
/// This is the honest comparison for a transport: `bench_raw_json` moves a
/// string, which JSON represents natively, while any Connect message is bytes.
/// Tauri encodes bytes in a JSON payload as one JSON number per byte, and this
/// is what that costs.
#[tauri::command]
pub async fn bench_raw_json_bytes(payload: Vec<u8>) -> Vec<u8> {
    let decoded = GreetRequest::decode(&mut payload.as_slice()).unwrap_or_default();
    let response = GreetResponse {
        greeting: format!("Hello, {}!", decoded.name),
        ..Default::default()
    };
    response.encode_to_vec()
}

/// Server-streaming floor: `count` frames pushed over a bare channel.
///
/// No Connect envelopes and no per-frame protobuf, so the gap against
/// `GreetMany` isolates what the transport adds per streamed message.
#[tauri::command]
pub async fn bench_raw_stream(
    count: u32,
    size: u32,
    channel: Channel<InvokeResponseBody>,
) -> Result<(), String> {
    let payload = vec![0u8; size as usize];
    for _ in 0..count {
        channel
            .send(InvokeResponseBody::Raw(payload.clone()))
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Whether the app was started to run the benchmark rather than the demo.
#[tauri::command]
pub fn bench_mode() -> bool {
    std::env::var_os("GREET_APP_BENCH").is_some()
}

/// Print the benchmark table and, in benchmark mode, exit.
///
/// The webview is the only place the transport can be timed end to end, but a
/// browser console is not readable from a terminal, so the table comes back
/// here to be printed.
#[tauri::command]
pub fn bench_report(app: tauri::AppHandle, report: String) {
    println!("{report}");
    if std::env::var_os("GREET_APP_BENCH").is_some() {
        app.exit(0);
    }
}
