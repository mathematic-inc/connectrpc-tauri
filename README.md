# ConnectRPC over Tauri IPC

A [ConnectRPC](https://connectrpc.com) transport that carries the Connect
protocol over Tauri's IPC instead of HTTP. The webview is the client, Rust
hosts the services, and no HTTP server or port is involved.

All four method kinds work: unary, server streaming, client streaming, and
bidi.

## Why this works without reimplementing Connect

Both sides already expose a byte-level seam, so this crate is only a shuttle
between them:

- Rust's `ConnectRpcService` is a `tower::Service<http::Request>` returning
  `http::Response`, so it accepts a synthetic request that never touched a
  socket.
- TypeScript's `@connectrpc/connect/protocol-connect` exports `createTransport`,
  which takes an `httpClient: UniversalClientFn` — a plain
  `(request) => Promise<response>` over async-iterable bodies.

Message serialization, envelope framing, compression negotiation, trailers,
error mapping, and Connect's idempotent-GET support all come from the existing
runtimes.

This wraps the _protocol-level_ `createTransport`, not `connect-web`'s
`createConnectTransport`. That distinction is what makes client-streaming and
bidi work: the web transport rejects them because the fetch API cannot stream a
request body, and there is no fetch here to be limited by.

## Usage

Rust:

```rust
let router = Arc::new(Greeter).register(connectrpc::Router::new());

tauri::Builder::default()
    .plugin(connectrpc_tauri::serve(ConnectRpcService::new(router)))
    .run(tauri::generate_context!())
    .expect("failed to run app");
```

TypeScript:

```ts
import { createClient } from "@connectrpc/connect";
import { createTauriTransport } from "@connectrpc-tauri/transport";

const client = createClient(GreetService, createTauriTransport());
const { greeting } = await client.greet({ name: "World" });
```

The capability file must grant the plugin's permission, or every call fails
with `Plugin not found`:

```json
{
  "identifier": "default",
  "windows": ["main"],
  "permissions": ["core:default", "connectrpc-tauri:default"]
}
```

The permission prefix is the Rust crate name. Tauri's ACL keys a plugin's
permissions on `CARGO_PKG_NAME`, so `PLUGIN_NAME` must equal the crate name;
a mismatch is rejected at the ACL layer before any command runs.

## How it maps onto IPC

Three commands and one channel per call:

| Connect concept                     | Tauri mechanism                                          |
| ----------------------------------- | -------------------------------------------------------- |
| Request head + first body chunk     | `invoke("connect_rpc", …)` with a protobuf `ArrayBuffer` |
| Response head (status + headers)    | Resolved value of that `invoke`                          |
| Unary response body                 | Resolved value of that `invoke`, no channel              |
| Streaming response body frames      | `Channel<InvokeResponseBody>` carrying raw protobuf      |
| Client-stream / bidi request frames | `invoke("connect_rpc_send", …)`                          |
| Cancellation                        | `invoke("connect_rpc_cancel", …)`, from `AbortSignal`    |
| Trailers                            | In-band, via Connect's own EndStreamResponse envelope    |

The envelopes are protobuf because Tauri's IPC payload is _either_ raw bytes or
JSON, never both, and a JSON payload encodes bytes as one number per byte.
Protobuf keeps every frame a single raw buffer, so Connect's bytes stay binary
end to end. For the same reason the response channel id travels inside
`StartRequest` rather than as a JSON command argument.

Channels are used rather than events: Tauri's event system is broadcast and
unordered relative to a call, so it would need correlation ids and dedup that a
channel provides for free.

## Streaming and backpressure

Streaming is incremental in both directions, not buffered. The request body is
an mpsc-backed `http_body::Body` that `connect_rpc_send` feeds, so a webview
writing faster than the handler reads is held at `CHANNEL_DEPTH` chunks — the
pending `invoke` is the backpressure signal.

On the webview side that signal is a send in flight. Messages produced while
one is outstanding are coalesced into the next send rather than each taking a
round trip, and a producer that outruns the hop by more than
`MAX_PENDING_BYTES` waits for it instead of buffering the stream.

Everything on the IPC path is async: all three commands are `async fn`, the
service is driven on its own task so the response head can return before the
body completes, and the response pump yields via the cooperative-scheduling
budget so an in-memory stream cannot monopolise a worker or outrun a cancel.
The one synchronous lock guards a hash map and is never held across an await,
which `#![deny(clippy::await_holding_lock)]` enforces.

## Layout

```text
crates/connectrpc-tauri      Rust plugin
packages/transport           @connectrpc-tauri/transport
examples/greet-app           demo app exercising all four kinds
proto/                       transport envelopes + demo service
```

## Verifying

```text
cargo test -p connectrpc-tauri     # 12 tests over the real Connect runtime
npx vitest run                     # transport unit tests

# End-to-end, in a real webview. Exits nonzero on failure.
npm run build --workspace @connectrpc-tauri/transport
npm run build --workspace greet-app
GREET_APP_SELFTEST=1 cargo run --release -p greet-app --features custom-protocol
```

The first two stub the IPC bridge on their own side. The third is the one that
proves Tauri's real commands and channels carry the protocol: it runs all four
method kinds plus a mid-stream cancel in a real webview, asserts the greetings
each RPC returned, prints a transcript, and exits with a status.

## Benchmarking

```text
npm run build --workspace @connectrpc-tauri/transport
npm run build --workspace greet-app
GREET_APP_BENCH=1 cargo run --release -p greet-app --features custom-protocol
```

This times the transport against the floor of what Tauri IPC can do for the
same bytes: a raw `invoke` carrying protobuf, a raw `invoke` with Tauri's
default JSON arguments, a bare `Channel` for streamed responses, and one
`invoke` per message for streamed requests. It runs inside the webview because
that is where the cost is — the expensive part of Tauri IPC is crossing the
webview boundary, which a Rust-only benchmark never pays.

Times are batched rather than measured per call: WebKit clamps
`performance.now()` to 1ms, which is coarser than an entire RPC.

On an M-series mac, release build:

| Case                          | This transport | Best raw baseline |
| ----------------------------- | -------------- | ----------------- |
| unary, 16-byte request        | 0.238ms        | 0.199ms (1.20x)   |
| unary, 4KiB request           | 0.262ms        | 0.227ms (1.16x)   |
| unary, 64KiB request          | 0.445ms        | 0.375ms (1.19x)   |
| server stream, 100 x 16 bytes | 0.781ms        | 1.297ms (0.60x)   |
| server stream, 100 x 4KiB     | 3.000ms        | 7.250ms (0.41x)   |
| client stream, 100 x 16 bytes | 1.156ms        | 19.250ms (0.06x)  |
| client stream, 100 x 4KiB     | 2.281ms        | 21.250ms (0.11x)  |

The raw baseline above is a protobuf `invoke`, the fastest way to move the
same bytes by hand — one per message for the client-streaming rows, which is
what a hand-written client sending a sequence would do.

A unary call costs ~15-20% over a hand-written `invoke`, which buys the Connect
protocol, generated clients, interceptors, and error mapping.

Both streaming directions come out ahead of hand-written IPC because neither
pays a webview crossing per message: responses arrive as `http_body` chunks
carrying many envelopes, and requests are coalesced on the way out. See
[Streaming costs less than a crossing per message](#streaming-costs-less-than-a-crossing-per-message).

### Why the IPC payload stays binary

Tauri's IPC payload is _either_ raw bytes or JSON, never both, and a JSON
payload encodes bytes as one JSON number per byte. Sending Connect's bytes as
JSON command arguments costs, at 64KiB:

| Payload carried as     | 64KiB request |
| ---------------------- | ------------- |
| raw bytes (what we do) | 0.375ms       |
| bytes inside JSON args | 5.063ms       |

That is 13x, and it grows with size: the same comparison is 1.04x at 16 bytes
and 2.4x at 4KiB, because the blow-up is in the encoding, not a fixed cost.
A JSON `invoke` moving a plain _string_ looks fast (0.375ms at 64KiB) — but a
Connect message is bytes, and that is the row that applies.

### Binary vs JSON Connect codec

A separate question from the one above: `useBinaryFormat` picks how a _message_
is encoded, while the IPC payload stays raw bytes either way. It is close to
free at these sizes, and binary stays the default:

| Codec  | 16B     | 4KiB    | 64KiB   |
| ------ | ------- | ------- | ------- |
| binary | 0.258ms | 0.313ms | 0.430ms |
| JSON   | 0.258ms | 0.309ms | 0.461ms |

JSON is worth it only for readable traffic in a devtools or proxy view; on an
in-process hop there is no such view to gain, and it costs re-encoding on
larger messages.

### Streaming costs less than a crossing per message

A webview crossing costs far more than the bytes it carries, so both streaming
directions are built to send fewer of them than there are messages.

Responses come free from the protocol: Connect's envelopes arrive as
`http_body` chunks, so many messages ride in one frame. Measured, 100 streamed
messages cross the IPC boundary as 2 frames, not 100 — where a naive `Channel`
loop pays one crossing each.

Requests need help, because Connect hands the client one message at a time.
Awaiting an `invoke` per message made a client-streaming call cost a full round
trip per message — 100 messages took ~19ms, exactly tracking a hand-written
loop of serial invokes. The pump now coalesces whatever the producer has
_already_ coalesced into one send, which takes the same 100 messages to
~1.2ms.

"Already produced" is the entire subtlety. The pump never waits for a chunk
that has not been generated, so a message whose reply the client is waiting on
still leaves immediately and alone. That is what keeps bidi interactive: a
batching window measured in time would deadlock a conversation where the next
request depends on the last response.

### Why unary skips the channel

Tauri delivers each channel frame by evaluating JavaScript in the webview, and
a payload under 1KiB is serialized as a JSON number array to do it — one JSON
number per byte. Routing a unary response over a channel therefore cost three
webview crossings (the invoke, the message frame, the end marker) plus that
re-encoding.

A unary response is a single message that is complete at the moment the head
is, so it now rides back on the `connect_rpc` invoke itself, in
`ResponseHead.body`, and no channel is created. That is one crossing instead of
three, and it took unary from ~1.9x the raw baseline to ~1.15x. Streaming calls
still use the channel, because their whole point is that the body is not ready
yet.

### Where the rest of a unary call goes

Measured on a quiet machine, a 16-byte unary call splits roughly:

| Cost                              | Time  |
| --------------------------------- | ----- |
| Tauri IPC crossing (sync command) | 154µs |
| Tauri's async-runtime hop         | +45µs |
| Connect protocol + this transport | +43µs |
| All webview-side per-call JS work | ~1µs  |

The JS side is already spent: building the URL, flattening headers, and
encoding the envelope sum to about a microsecond, so there is nothing there
worth optimizing. The 154µs floor belongs to Tauri.

That leaves the async hop, which is deliberately not taken. A synchronous
command answers on the IPC thread, but `connect_rpc` awaits a tower service
running arbitrary handler code — blocking that thread would let one slow
handler stall every other RPC and the UI with it. Trading a fixed 45µs for an
unbounded worst case is not a good trade, so unary stays async.

`custom-protocol` is what makes Tauri serve the bundled assets. Without it,
every build — release included — loads `devUrl` and needs `npm run dev`
running, since Tauri picks dev-vs-bundled from that feature rather than from
the cargo profile.

## Limitations

- Webview → Rust only. Rust-initiated calls into webview-hosted services are
  out of scope.
- Compression is off by default. It spends CPU to shrink bytes on an in-process
  hop that never touches a network; the knob is exposed on both sides.
- Android's IPC lacks `InvokeBody::Raw` and falls back to JSON number arrays. A
  base64 fallback is not implemented.
