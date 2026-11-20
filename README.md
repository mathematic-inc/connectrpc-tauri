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

If the router depends on state created during Tauri setup, register a deferred
dispatcher first and initialize it in the setup hook:

```rust
let deferred = connectrpc_tauri::DeferredDispatcher::new();

tauri::Builder::default()
    .plugin(connectrpc_tauri::serve(ConnectRpcService::new(deferred.clone())))
    .setup(move |app| {
        let router = router_from_app(app.handle());
        assert!(deferred.set(router).is_ok(), "ConnectRPC router initialized twice");
        Ok(())
    })
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

An app that also shows pages it doesn't trust, in webviews of their own, names
the webviews it serves:

```rust
.plugin(connectrpc_tauri::serve_to(ConnectRpcService::new(router), ["main"]))
```

Unary calls and commands alike then answer only webviews with those labels,
which Tauri sets and no page can change.

## How it maps onto IPC

Unary calls bypass Tauri's IPC entirely. Everything else uses three commands
and one channel per call:

| Connect concept                     | Tauri mechanism                                          |
| ----------------------------------- | -------------------------------------------------------- |
| Unary request and response          | `fetch("ipc-connect://…")`, a custom URI scheme          |
| Request head + first body chunk     | `invoke("connect_rpc", …)` with a protobuf `ArrayBuffer` |
| Response head (status + headers)    | Resolved value of that `invoke`                          |
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

Streaming is incremental in both directions, not buffered.
`connect_rpc_send` feeds the request body through a bounded channel, so a
webview that writes faster than the handler reads is held at `CHANNEL_DEPTH`
chunks — the pending `invoke` is the backpressure signal.

On the webview side that signal is a send in flight. Messages produced while
one is outstanding are coalesced into the next send rather than each taking a
round trip, and a producer that outruns the hop by more than
`MAX_PENDING_BYTES` waits for it instead of buffering the stream.

Everything on the IPC path is async. All three commands are `async fn`. The
service runs on its own task, so the response head can return before the body
completes. The response pump yields through the cooperative-scheduling budget,
so an in-memory stream cannot monopolise a worker or outrun a cancel. The one
synchronous lock guards a `HashMap` and is never held across an await, which
Clippy's `await_holding_lock` lint enforces.

## Layout

```text
crates/connectrpc-tauri      Rust plugin
packages/transport           @connectrpc-tauri/transport
examples/greet-app           demo app exercising all four kinds
proto/                       transport envelopes + demo service
tools/protobuf-bindings      regenerates the crate's checked-in bindings
```

## Protobuf bindings

The generated bindings for both packages are checked in, so neither building
the crate nor installing the npm package needs `protoc` or `buf`. After
changing a schema, regenerate them:

```text
npm run generate                                       # TypeScript, with buf
mise exec -- cargo run --locked -p protobuf-bindings   # Rust, with the pinned protoc
```

`hk check` fails if either needs regenerating.

## Verifying

```text
cargo test -p connectrpc-tauri     # 12 tests over the real Connect runtime
npx vitest run                     # transport unit tests

# End-to-end, in a real webview. Exits nonzero on failure.
npm run build --workspace @connectrpc-tauri/transport
npm run build --workspace greet-app
GREET_APP_SELFTEST=1 cargo run --release -p greet-app --features custom-protocol
```

The first two stub the IPC bridge on their own side. The third proves that
Tauri's real commands and channels carry the protocol. It runs all four method
kinds and a mid-stream cancel in a real webview, asserts the greetings that each
RPC returned, prints a transcript and exits with a status.

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
`performance.now()` to 1 millisecond, which is coarser than an entire RPC.

On an M-series mac, release build, in milliseconds per operation:

| Case                          | This transport | Best raw baseline |
| ----------------------------- | -------------- | ----------------- |
| unary, 16-byte request        | 0.203          | 0.195 (1.04x)     |
| unary, 4KiB request           | 0.227          | 0.223 (1.02x)     |
| unary, 64KiB request          | 0.363          | 0.340 (1.07x)     |
| server stream, 100 x 16 bytes | 0.797          | 1.453 (0.55x)     |
| server stream, 100 x 4KiB     | 2.938          | 7.875 (0.37x)     |
| client stream, 100 x 16 bytes | 1.156          | 19.250 (0.06x)    |
| client stream, 100 x 4KiB     | 2.250          | 21.500 (0.10x)    |

The raw baseline above is a protobuf `invoke`, the fastest way to move the
same bytes by hand — one per message for the client-streaming rows, which is
what a hand-written client sending a sequence would do.

Unary calls are within a few percent of a hand-written `invoke` — they no longer
go through Tauri's IPC at all. Unary requests are a plain `fetch` against the
`ipc-connect://` scheme the plugin registers, which skips the command router,
the ACL check, and the protobuf envelope. See
[Unary skips IPC entirely](#unary-skips-ipc-entirely).

Both streaming directions come out ahead of hand-written IPC because neither
pays a webview crossing per message: responses arrive as `http_body` chunks
carrying many envelopes, and requests are coalesced on the way out. See
[Streaming costs less than a crossing per message](#streaming-costs-less-than-a-crossing-per-message).

### Why the IPC payload stays binary

Tauri's IPC payload is _either_ raw bytes or JSON, never both, and a JSON
payload encodes bytes as one JSON number per byte. Sending Connect's bytes as
JSON command arguments costs, at 64KiB, in milliseconds:

| Payload carried as          | 64KiB request |
| --------------------------- | ------------- |
| raw bytes (what we do)      | 0.375         |
| bytes inside JSON arguments | 5.063         |

That is 13x, and it grows with size: the same comparison is 1.04x at 16 bytes
and 2.4x at 4KiB, because the blow-up is in the encoding, not a fixed cost.
A JSON `invoke` moving a plain _string_ looks fast (0.375 milliseconds at
64KiB) — but a Connect message is bytes, and that is the row that applies.

### Binary vs JSON Connect codec

A separate question from the one above: `useBinaryFormat` picks how a _message_
is encoded, while the IPC payload stays raw bytes either way. It is close to
free at these sizes, and binary stays the default. The values are
milliseconds per call:

| Codec  | 16B   | 4KiB  | 64KiB |
| ------ | ----- | ----- | ----- |
| binary | 0.258 | 0.313 | 0.430 |
| JSON   | 0.258 | 0.309 | 0.461 |

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
trip for each message — 100 messages took about 19 milliseconds, exactly
tracking a hand-written loop of serial invokes. The pump now coalesces whatever
the producer has _already_ coalesced into one send, which takes the same 100
messages to about 1.2 milliseconds.

"Already produced" is the entire subtlety. The pump never waits for a chunk
that has not been generated, so a message whose reply the client is waiting on
still leaves immediately and alone. That is what keeps bidi interactive: a
batching window measured in time would deadlock a conversation where the next
request depends on the last response.

### Unary skips IPC entirely

Tauri's IPC is a command router: a request crosses the webview boundary as an
`InvokeBody`, is matched against a command name, checked against the ACL, then
answered. Unary Connect calls need none of that. It is already an HTTP
request against a `tower::Service`, and the webview already speaks HTTP.

So the plugin registers its own URI scheme, `ipc-connect://`, and unary calls
go out as a plain `fetch`. Tauri's webview manager checks whether a scheme is
already registered before installing its own handler, so this coexists with
`ipc://` rather than replacing it — every stock plugin keeps working.

Skipping the command router also skips what keeps other pages out of it. Every
frame in the webview can reach a custom scheme, including an iframe at another
origin: a remote page, or content the app serves from a scheme of its own.
Tauri's IPC refuses those with an invoke key it gives only to the top-level
frame. The scheme has no key, so it checks the `Origin` of the request, which a
page cannot set. It answers only the pages of the app (`tauri://localhost`,
`http://tauri.localhost` on Windows and Android, or the `devUrl` during
development). It names that origin, not `*`, as the only one that may read the
response. Under `serve_to` it also checks the webview's label, which Tauri sets
and no page can change: another webview that loads the app's pages sends their
`Origin` too.

What that removes from unary calls: the protobuf envelope and its
encode/decode, the command-name dispatch, the ACL permission lookup, the call
registry, and the channel. What is left is close to an identity mapping —
`http::Request` in, service, `http::Response` out — which is why unary now
lands within a few percent of a hand-written `invoke` rather than 15-20% above
it.

Streaming keeps the command-and-channel path. A scheme responder is one-shot on
every platform: it takes a complete response by value, and underneath it
WKWebView does `didReceiveResponse` then a single `didReceiveData`, while
WebKitGTK builds its stream from a finished buffer. Nothing can push a second
frame, so routing a server-streaming response through the scheme would only
deliver it once buffered whole — which is exactly what streaming exists to
avoid.

The scheme is named `ipc-connect` rather than something like `ipc+connect`
because on Windows and Android wry rewrites `scheme://localhost` to
`http://scheme.localhost`, making the scheme name a hostname label. `+` is
legal in a URI scheme but not in a hostname, so it would work on macOS and
Linux and break on Windows.

`custom-protocol` is what makes Tauri serve the bundled assets. Without it,
every build — release included — loads `devUrl` and needs `npm run dev`
running, since Tauri picks dev-vs-bundled from that feature rather than from
the cargo profile.

## Limitations

- Webview → Rust only. Rust-initiated calls into webview-hosted services are
  out of scope.
- Compression is off by default. It spends CPU to shrink bytes on an in-process
  call path that never touches a network; the knob is exposed on both sides.
- Android's IPC lacks `InvokeBody::Raw` and falls back to JSON number arrays. A
  base64 fallback is not implemented. This affects streaming only; unary goes
  over the `ipc-connect://` scheme and is unaffected.
- The unary path has been verified on macOS. The scheme registration and the
  Windows/Android hostname rewrite are handled, but are not yet exercised in
  CI on those platforms.

## Contributing

Start with a
[Discussion](https://github.com/mathematic-inc/connectrpc-tauri/discussions/new),
not a pull request. A Mathematic maintainer will review the proposal. If we
decide to implement it, a maintainer or one of our AI agents will open the pull
request. GitHub restricts pull request creation to Mathematic maintainers and
repository collaborators with write, maintain, or admin access, plus authorized
maintenance agents.

When Mathematic implements a proposal, the implementation pull request will
link to the Discussion and credit the proposal's original author.

See [CONTRIBUTING.md](https://github.com/mathematic-inc/connectrpc-tauri/blob/main/CONTRIBUTING.md) for the full policy.
