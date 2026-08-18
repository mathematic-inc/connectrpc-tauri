// Demo app: serves `greet.v1.GreetService` to the webview over Tauri IPC.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod bench;

use std::{sync::Arc, time::Duration};

use connectrpc::{
    ConnectError, ConnectRpcService, InboundStream, RequestContext, Router, ServiceRequest,
    ServiceResult, ServiceStream,
};
use connectrpc_tauri::greet::{
    GreetManyRequest, GreetRequest, GreetResponse, GreetService, GreetServiceExt,
};
use futures::StreamExt;

struct Greeter;

// Naming concrete return types rather than the trait's `impl Encodable` keeps
// these readable; it is a refinement, not a mismatch.
#[allow(refining_impl_trait)]
impl GreetService for Greeter {
    async fn greet(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        let name = request.name;
        if name.is_empty() {
            return Err(ConnectError::invalid_argument("name is required"));
        }
        Ok(GreetResponse {
            greeting: format!("Hello, {name}!"),
            ..Default::default()
        }
        .into())
    }

    async fn greet_many(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GreetManyRequest>,
    ) -> ServiceResult<ServiceStream<GreetResponse>> {
        // Copy out: the stream outlives the borrowed request.
        let name = request.name.to_string();
        let count = request.count.clamp(1, 100);

        // Pacing makes the demo visibly stream, but it would swamp the
        // benchmark's own measurements, so it is off under `GREET_APP_BENCH`.
        let paced = std::env::var_os("GREET_APP_BENCH").is_none();

        let stream: ServiceStream<GreetResponse> =
            Box::pin(futures::stream::iter(0..count).then(move |i| {
                let name = name.clone();
                async move {
                    // Paced so the UI visibly streams rather than arriving at once.
                    if paced {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    Ok(GreetResponse {
                        greeting: format!("Hello, {name}! ({}/{count})", i + 1),
                        ..Default::default()
                    })
                }
            }));
        Ok(stream.into())
    }

    async fn greet_all(
        &self,
        _ctx: RequestContext,
        mut requests: InboundStream<GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        let mut names = Vec::new();
        while let Some(item) = requests.next().await {
            names.push(item?.name().to_string());
        }
        if names.is_empty() {
            return Err(ConnectError::invalid_argument(
                "at least one name is required",
            ));
        }
        Ok(GreetResponse {
            greeting: format!("Hello, {}!", names.join(" and ")),
            ..Default::default()
        }
        .into())
    }

    async fn greet_chat(
        &self,
        _ctx: RequestContext,
        requests: InboundStream<GreetRequest>,
    ) -> ServiceResult<ServiceStream<GreetResponse>> {
        // One response per request, emitted as each arrives.
        let stream: ServiceStream<GreetResponse> = Box::pin(requests.map(|item| {
            item.map(|msg| GreetResponse {
                greeting: format!("Hello, {}!", msg.name()),
                ..Default::default()
            })
        }));
        Ok(stream.into())
    }
}

/// Mirror one webview log line to stdout.
#[tauri::command]
fn selftest_log(line: String) {
    println!("app: {line}");
}

/// Receive the webview self-test result and print it.
///
/// The webview is the only place the transport can be exercised end to end,
/// but a browser console is awkward to assert on, so the verdict comes back
/// here. With `GREET_APP_SELFTEST=1` the process also exits with a status,
/// which makes the whole stack checkable from one command.
#[tauri::command]
fn selftest_report(passed: bool, failures: Vec<String>) {
    if passed {
        println!("selftest: PASS");
    } else {
        println!("selftest: FAIL");
        for failure in &failures {
            println!("  {failure}");
        }
    }

    if std::env::var_os("GREET_APP_SELFTEST").is_some() {
        // Exiting the process directly rather than via `AppHandle::exit`: the
        // runtime does not surface that code as the process status here, which
        // would report a failing self-test as success.
        use std::io::Write as _;
        let _ = std::io::stdout().flush();
        std::process::exit(i32::from(!passed));
    }
}

fn main() {
    let router = Arc::new(Greeter).register(Router::new());

    tauri::Builder::default()
        .plugin(connectrpc_tauri::serve(ConnectRpcService::new(router)))
        .invoke_handler(tauri::generate_handler![
            selftest_log,
            selftest_report,
            bench::bench_raw_unary,
            bench::bench_raw_json,
            bench::bench_raw_json_bytes,
            bench::bench_raw_stream,
            bench::bench_mode,
            bench::bench_report,
        ])
        .run(tauri::generate_context!())
        .expect("failed to run app");
}
