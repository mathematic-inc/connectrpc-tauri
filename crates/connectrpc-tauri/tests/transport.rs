//! Tests for the IPC-to-Connect bridge.
//!
//! These drive `call::start` directly with a `Channel` built from a plain
//! closure, so they exercise the real Connect runtime and the real framing
//! without needing a webview.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use buffa::Message;
use connectrpc::{
    ConnectRpcService, InboundStream, RequestContext, Router, ServiceRequest, ServiceResult,
    ServiceStream,
};
use connectrpc_tauri::{
    greet::{GreetManyRequest, GreetRequest, GreetResponse, GreetService, GreetServiceExt},
    testing::TestCall,
    wire,
};
use futures::StreamExt;

/// Demo service. Counts streamed items so cancellation can be observed
/// directly rather than inferred from timing.
#[derive(Default)]
struct Greeter {
    produced: Arc<AtomicUsize>,
}

// Naming concrete return types instead of the trait's `impl Encodable` is a
// refinement; it is what makes these handlers readable.
#[allow(refining_impl_trait)]
impl GreetService for Greeter {
    async fn greet(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, GreetRequest>,
    ) -> ServiceResult<GreetResponse> {
        // Fields are zero-copy views into the request buffer.
        let name = request.name;
        if name.is_empty() {
            return Err(connectrpc::ConnectError::invalid_argument(
                "name is required",
            ));
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
        // The stream outlives the request, so copy out before returning.
        let name = request.name.to_string();
        let count = request.count;
        let produced = Arc::clone(&self.produced);

        let stream: ServiceStream<GreetResponse> =
            Box::pin(futures::stream::iter(0..count).map(move |i| {
                produced.fetch_add(1, Ordering::Relaxed);
                Ok(GreetResponse {
                    greeting: format!("Hello, {name} #{i}!"),
                    ..Default::default()
                })
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
        let stream: ServiceStream<GreetResponse> = Box::pin(requests.map(|item| {
            item.map(|msg| GreetResponse {
                greeting: format!("Hello, {}!", msg.name()),
                ..Default::default()
            })
        }));
        Ok(stream.into())
    }
}

fn service(greeter: Greeter) -> ConnectRpcService<Router> {
    ConnectRpcService::new(Arc::new(greeter).register(Router::new()))
}

/// Encode a Connect enveloped frame: 5-byte header then payload.
fn envelope(message: &GreetRequest) -> Vec<u8> {
    let bytes = message.encode_to_vec();
    let mut framed = Vec::with_capacity(bytes.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    framed
}

#[tokio::test]
async fn unary_returns_the_encoded_response() {
    let call = TestCall::new(service(Greeter::default()));
    let head = call
        .start(
            "/greet.v1.GreetService/Greet",
            &GreetRequest {
                name: "World".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .await
        .expect("call failed");

    assert_eq!(head.status, 200);
    let response: GreetResponse = call.expect_unary_message().await;
    assert_eq!(response.greeting, "Hello, World!");
}

#[tokio::test]
async fn unary_error_keeps_its_connect_code_and_message() {
    let call = TestCall::new(service(Greeter::default()));
    let head = call
        .start(
            "/greet.v1.GreetService/Greet",
            &GreetRequest::default().encode_to_vec(),
        )
        .await
        .expect("call failed");

    // Connect maps invalid_argument to HTTP 400 and puts the error in the body.
    assert_eq!(head.status, 400);
    let body = call.collect_message_bytes().await;
    let json = String::from_utf8(body).expect("error body is JSON");
    assert!(json.contains("invalid_argument"), "got {json}");
    assert!(json.contains("name is required"), "got {json}");
}

/// The path every unary call takes: no channel, body inline on the head.
///
/// This is the optimisation that makes unary cost one webview crossing instead
/// of three, so it needs to produce byte-identical results to the channelled
/// path, errors included.
#[tokio::test]
async fn buffered_unary_returns_the_body_on_the_head() {
    let call = TestCall::new(service(Greeter::default()));
    let head = call
        .start_buffered(
            "/greet.v1.GreetService/Greet",
            &GreetRequest {
                name: "World".into(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .await
        .expect("call failed");

    assert_eq!(head.status, 200);
    let response = GreetResponse::decode(&mut head.body.as_slice()).expect("malformed response");
    assert_eq!(response.greeting, "Hello, World!");

    // No channel, no registry entry: nothing can outlive the invoke.
    assert_eq!(call.in_flight(), 0, "buffered call leaked in the registry");
}

/// A failing buffered call still carries Connect's error body.
#[tokio::test]
async fn buffered_unary_error_keeps_its_connect_code() {
    let call = TestCall::new(service(Greeter::default()));
    let head = call
        .start_buffered(
            "/greet.v1.GreetService/Greet",
            &GreetRequest::default().encode_to_vec(),
        )
        .await
        .expect("call failed");

    assert_eq!(head.status, 400);
    let json = String::from_utf8(head.body).expect("error body is JSON");
    assert!(json.contains("invalid_argument"), "got {json}");
    assert!(json.contains("name is required"), "got {json}");
}

#[tokio::test]
async fn unknown_method_is_not_found() {
    let call = TestCall::new(service(Greeter::default()));
    let head = call
        .start("/greet.v1.GreetService/Nope", &[])
        .await
        .expect("call failed");

    assert_eq!(head.status, 404);
}

#[tokio::test]
async fn server_stream_preserves_order() {
    let call = TestCall::new(service(Greeter::default()));
    call.start_streaming_response(
        "/greet.v1.GreetService/GreetMany",
        &GreetManyRequest {
            name: "World".into(),
            count: 3,
            ..Default::default()
        },
    )
    .await
    .expect("call failed");

    let greetings: Vec<String> = call
        .collect_stream_messages::<GreetResponse>()
        .await
        .into_iter()
        .map(|r| r.greeting)
        .collect();
    assert_eq!(
        greetings,
        vec![
            "Hello, World #0!".to_string(),
            "Hello, World #1!".to_string(),
            "Hello, World #2!".to_string(),
        ]
    );
}

#[tokio::test]
async fn client_stream_aggregates_every_message() {
    let call = TestCall::new(service(Greeter::default()));
    call.start_streaming_request("/greet.v1.GreetService/GreetAll")
        .await
        .expect("call failed");

    for name in ["Alice", "Bob"] {
        call.send(&envelope(&GreetRequest {
            name: name.into(),
            ..Default::default()
        }))
        .await;
    }
    call.end_request().await;

    let greetings: Vec<String> = call
        .collect_stream_messages::<GreetResponse>()
        .await
        .into_iter()
        .map(|r| r.greeting)
        .collect();
    assert_eq!(greetings, vec!["Hello, Alice and Bob!".to_string()]);
}

/// Several messages arriving in one chunk must frame exactly as if they had
/// arrived separately.
///
/// This is what the webview sends: the request pump coalesces whatever the
/// producer already had into a single `connect_rpc_send`, so a chunk boundary
/// no longer matches a message boundary. Connect's envelopes are
/// self-delimiting, so the body reader splits them; this pins that, since a
/// regression here would silently drop or merge messages.
#[tokio::test]
async fn client_stream_splits_coalesced_messages() {
    let call = TestCall::new(service(Greeter::default()));
    call.start_streaming_request("/greet.v1.GreetService/GreetAll")
        .await
        .expect("call failed");

    let mut batch = Vec::new();
    for name in ["Alice", "Bob", "Carol"] {
        batch.extend_from_slice(&envelope(&GreetRequest {
            name: name.into(),
            ..Default::default()
        }));
    }
    // One send carrying three messages, which is the point.
    call.send(&batch).await;
    call.end_request().await;

    let greetings: Vec<String> = call
        .collect_stream_messages::<GreetResponse>()
        .await
        .into_iter()
        .map(|r| r.greeting)
        .collect();
    assert_eq!(
        greetings,
        vec!["Hello, Alice and Bob and Carol!".to_string()]
    );
}

#[tokio::test]
async fn bidi_interleaves_responses_with_requests() {
    let call = TestCall::new(service(Greeter::default()));
    call.start_streaming_request("/greet.v1.GreetService/GreetChat")
        .await
        .expect("call failed");

    // Send one, read one: a response must arrive before the request stream
    // ends, which is what distinguishes bidi from client-streaming.
    call.send(&envelope(&GreetRequest {
        name: "Alice".into(),
        ..Default::default()
    }))
    .await;
    assert_eq!(
        call.next_message::<GreetResponse>()
            .await
            .map(|r| r.greeting),
        Some("Hello, Alice!".to_string())
    );

    call.send(&envelope(&GreetRequest {
        name: "Bob".into(),
        ..Default::default()
    }))
    .await;
    assert_eq!(
        call.next_message::<GreetResponse>()
            .await
            .map(|r| r.greeting),
        Some("Hello, Bob!".to_string())
    );

    call.end_request().await;
}

#[tokio::test]
async fn cancel_stops_the_handler_producing() {
    let produced = Arc::new(AtomicUsize::new(0));
    let call = TestCall::new(service(Greeter {
        produced: Arc::clone(&produced),
    }));

    // Far more messages than the channel can hold, so the handler is still
    // producing when the cancel lands.
    call.start_streaming_response(
        "/greet.v1.GreetService/GreetMany",
        &GreetManyRequest {
            name: "World".into(),
            count: 100_000,
            ..Default::default()
        },
    )
    .await
    .expect("call failed");

    call.cancel();

    // Let any in-flight production settle, then confirm it stopped rather than
    // ran to completion.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let after_cancel = produced.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        produced.load(Ordering::Relaxed),
        after_cancel,
        "handler kept producing after cancel"
    );
    assert!(
        after_cancel < 100_000,
        "cancel did not interrupt production ({after_cancel} of 100000)"
    );
}

#[tokio::test]
async fn a_finished_call_is_removed_from_the_registry() {
    let call = TestCall::new(service(Greeter::default()));
    call.start(
        "/greet.v1.GreetService/Greet",
        &GreetRequest {
            name: "World".into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await
    .expect("call failed");

    call.expect_unary_message::<GreetResponse>().await;
    assert_eq!(call.in_flight(), 0, "completed call leaked in the registry");
}

/// A client-streaming request must not be buffered whole.
///
/// The claim is that the request body applies backpressure: the webview can
/// only get `CHANNEL_DEPTH` chunks ahead of a handler that is not reading.
/// Without a bound, this send loop would run to completion against a stalled
/// handler, so the timeout failing is what proves the bound exists.
#[tokio::test]
async fn a_stalled_handler_stops_accepting_request_chunks() {
    let call = TestCall::new(service(Greeter::default()));

    // `GreetChat` responds per message, but nothing here reads the responses,
    // so the handler stalls on a full response path and stops draining.
    call.start_streaming_request("/greet.v1.GreetService/GreetChat")
        .await
        .expect("call failed");

    let chunk = envelope(&GreetRequest {
        name: "World".into(),
        ..Default::default()
    });

    let mut sent = 0usize;
    let stalled = tokio::time::timeout(Duration::from_millis(500), async {
        // Far more than any internal buffer; if every send resolves, nothing is
        // bounding the queue.
        for _ in 0..100_000 {
            call.send(&chunk).await;
            sent += 1;
        }
    })
    .await
    .is_err();

    assert!(
        stalled,
        "every one of {sent} chunks was accepted; the request body is unbounded"
    );
}

#[tokio::test]
async fn frames_are_protobuf_encoded() {
    // Guards the wire contract the TypeScript side decodes.
    let frame = wire::ResponseFrame {
        frame: Some(wire::response_frame::Frame::Message(vec![1, 2, 3])),
        ..Default::default()
    };
    let bytes = frame.encode_to_vec();
    let decoded = wire::ResponseFrame::decode(&mut bytes.as_slice()).expect("round trip");
    assert!(matches!(
        decoded.frame,
        Some(wire::response_frame::Frame::Message(ref b)) if b == &[1, 2, 3]
    ));
}

/// Silences an unused-import warning when the mutex helper is not needed.
#[allow(dead_code)]
type _Unused = Mutex<()>;
