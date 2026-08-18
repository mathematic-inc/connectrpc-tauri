// The Tauri IPC side of the transport: one `UniversalClientFn` that speaks
// commands and channels instead of HTTP.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { Code, ConnectError } from "@connectrpc/connect";
import type {
  UniversalClientFn,
  UniversalClientRequest,
  UniversalClientResponse,
} from "@connectrpc/connect/protocol";
import { createWritableIterable } from "@connectrpc/connect/protocol";
import { Channel, invoke } from "@tauri-apps/api/core";

import {
  CancelRequestSchema,
  type Header,
  ResponseFrameSchema,
  ResponseHeadSchema,
  SendRequestSchema,
  StartRequestSchema,
} from "./gen/connectrpc/tauri/v1/transport_pb.js";

/**
 * Commands registered by the `connectrpc-tauri` Tauri plugin.
 *
 * The prefix is the Rust crate name, which is what Tauri's ACL keys
 * permissions on; it has to match `PLUGIN_NAME` on the Rust side exactly.
 */
const START = "plugin:connectrpc-tauri|connect_rpc";
const SEND = "plugin:connectrpc-tauri|connect_rpc_send";
const CANCEL = "plugin:connectrpc-tauri|connect_rpc_cancel";

/** Monotonic per-webview call id. */
let nextCallId = 1n;

/**
 * How many request bytes may pile up while a send is in flight.
 *
 * Reached only when the producer outruns the IPC hop; until then the cap never
 * binds and the pump coalesces freely. It is what keeps a fast generator from
 * buffering an entire stream in the webview.
 */
const MAX_PENDING_BYTES = 1024 * 1024;

/**
 * Marks a request whose body streams incrementally.
 *
 * Connect hands the byte-level client an opaque body iterable and never says
 * which method kind produced it. Detecting it by pulling a second chunk would
 * deadlock bidi, where the client's next message can depend on a server
 * response that cannot arrive until the request is already in flight. The
 * transport wrapper does know the kind, so it tags the header instead; the tag
 * is stripped here and never reaches the service.
 */
export const STREAMING_REQUEST_HEADER = "x-connectrpc-tauri-streaming-request";

/**
 * Marks a call whose response arrives whole, on the invoke, with no channel.
 *
 * Set for unary methods, whose response is one message that is ready when the
 * head is. Tauri sends each channel frame by evaluating JavaScript in the
 * webview, so a channelled unary response pays three webview crossings — the
 * invoke, the message, the end marker — where this pays one.
 *
 * Like the streaming marker, this is stripped here and never reaches the
 * service.
 */
export const BUFFERED_RESPONSE_HEADER = "x-connectrpc-tauri-buffered-response";

/**
 * Build the byte-level client that `createTransport` drives.
 *
 * Connect gives us a `UniversalClientRequest` with an async-iterable body and
 * expects a `UniversalClientResponse` with one back. The mapping is:
 * the head plus first chunk go out on `connect_rpc`, remaining request chunks
 * on `connect_rpc_send`, and response bytes arrive on a per-call `Channel`.
 */
export function createTauriIpcClient(): UniversalClientFn {
  return async (request) => {
    const callId = nextCallId++;
    const streamingRequest = request.header.has(STREAMING_REQUEST_HEADER);
    request.header.delete(STREAMING_REQUEST_HEADER);
    const buffered = request.header.has(BUFFERED_RESPONSE_HEADER);
    request.header.delete(BUFFERED_RESPONSE_HEADER);

    if (buffered) {
      return await unaryCall(callId, request);
    }

    // The response body is written from the channel callback and read by
    // Connect; `createWritableIterable` bridges push to pull with
    // backpressure, so a slow reader stops us buffering without bound.
    const body = createWritableIterable<Uint8Array>();

    const channel = new Channel<ArrayBuffer>();
    let bodyClosed = false;
    // Set when the Rust side reports a transport failure mid-body. The reader
    // sees it on the next pull, since `WritableIterable` has no error channel.
    let bodyError: ConnectError | undefined;
    const closeBody = () => {
      if (!bodyClosed) {
        bodyClosed = true;
        body.close();
      }
    };

    // Channel callbacks are synchronous, but `write` is async and rejects once
    // the iterable is closed. Chaining keeps frames in order and keeps a
    // post-close write from becoming an unhandled rejection.
    let writes = Promise.resolve();
    const enqueue = (bytes: Uint8Array) => {
      writes = writes.then(async () => {
        if (bodyClosed) {
          return;
        }
        await body.write(bytes);
      });
    };

    channel.onmessage = (raw) => {
      const frame = fromBinary(ResponseFrameSchema, new Uint8Array(raw));
      switch (frame.frame.case) {
        case "message":
          enqueue(frame.frame.value);
          break;
        case "error":
          // The head already resolved, so the failure has to surface through
          // the body stream, which is where Connect is reading.
          bodyError = new ConnectError(frame.frame.value, Code.Internal);
          closeBody();
          break;
        case "trailers":
          // Connect carries trailers in-band (EndStreamResponse for streams,
          // `trailer-` headers for unary), so HTTP trailers are informational
          // here and the protocol layer never reads them.
          break;
        case "end":
          // Close only after queued writes land, or the reader loses the tail
          // of the response.
          void writes.then(closeBody, closeBody);
          break;
      }
    };

    // Unary and server-streaming send their whole body up front, so the first
    // chunk is the entire request and one round trip completes the call.
    const firstChunk = streamingRequest ? undefined : await readFirstChunk(request.body);

    const start = create(StartRequestSchema, {
      callId,
      url: new URL(request.url).pathname,
      method: request.method,
      headers: toWireHeaders(request.header),
      body: firstChunk ?? new Uint8Array(0),
      streamingRequest,
      channel: channel.toJSON(),
    });

    const abort = () => {
      void invoke(CANCEL, toBinary(CancelRequestSchema, create(CancelRequestSchema, { callId })));
      closeBody();
    };
    request.signal?.addEventListener("abort", abort, { once: true });

    const headPromise = invoke<ArrayBuffer>(START, toBinary(StartRequestSchema, start));

    // Pump concurrently with the head, never after it. A client-streaming
    // handler produces no response until it has read the whole request, so
    // awaiting the head first would deadlock; bidi would deadlock the other
    // way for the same reason.
    if (streamingRequest && request.body !== undefined) {
      void pumpRequestBody(callId, request.body, request.signal).catch(() => {
        // A failed pump surfaces as the call's own error; the head promise
        // below rejects or the body stream ends.
      });
    }

    const head = fromBinary(ResponseHeadSchema, new Uint8Array(await headPromise));

    return {
      status: head.status,
      header: fromWireHeaders(head.headers),
      body: raising(body, () => bodyError),
      trailer: new Headers(),
    };
  };
}

/**
 * Re-yield a body, then throw if the transport failed partway through.
 *
 * `WritableIterable` can only be closed, not failed, so a mid-stream error is
 * recorded and raised here once the buffered frames have been delivered.
 */
async function* once(bytes: Uint8Array): AsyncIterable<Uint8Array> {
  yield bytes;
}

/**
 * Run a call whose whole response comes back on the invoke.
 *
 * One webview crossing instead of three: no channel is created, so Rust never
 * evaluates JavaScript to deliver the message or an end marker.
 *
 * Cancellation still works because `invoke` is a promise the caller can stop
 * awaiting; the Rust side drops the call when this future is dropped, and
 * there is no stream left half-open to tidy up.
 */
async function unaryCall(
  callId: bigint,
  request: UniversalClientRequest,
): Promise<UniversalClientResponse> {
  const firstChunk = await readFirstChunk(request.body);

  const start = create(StartRequestSchema, {
    callId,
    url: new URL(request.url).pathname,
    method: request.method,
    headers: toWireHeaders(request.header),
    body: firstChunk ?? new Uint8Array(0),
    streamingRequest: false,
    // Empty: the Rust side reads this as "return the body inline".
    channel: "",
  });

  if (request.signal?.aborted === true) {
    throw new ConnectError("call cancelled", Code.Canceled);
  }

  const head = fromBinary(
    ResponseHeadSchema,
    new Uint8Array(await invoke<ArrayBuffer>(START, toBinary(StartRequestSchema, start))),
  );

  return {
    status: head.status,
    header: fromWireHeaders(head.headers),
    body: once(head.body),
    trailer: new Headers(),
  };
}

/**
 * Re-yield a body, then throw if the transport failed partway through.
 *
 * `WritableIterable` can only be closed, not failed, so a mid-stream error is
 * recorded and raised here once the buffered frames have been delivered.
 */
async function* raising(
  body: AsyncIterable<Uint8Array>,
  error: () => ConnectError | undefined,
): AsyncIterable<Uint8Array> {
  yield* body;
  const failure = error();
  if (failure !== undefined) {
    throw failure;
  }
}

/**
 * Read the whole body of a non-streaming request.
 *
 * Unary and server-streaming bodies are exactly one enveloped message, so this
 * takes the single chunk and the call completes in one round trip.
 */
async function readFirstChunk(
  body: AsyncIterable<Uint8Array> | undefined,
): Promise<Uint8Array | undefined> {
  if (body === undefined) {
    return undefined;
  }
  const first = await body[Symbol.asyncIterator]().next();
  return first.done === true ? undefined : first.value;
}

/**
 * Forward a streaming request body, coalescing chunks that are ready together.
 *
 * Each invoke is a webview crossing, and awaiting one per message makes a
 * client-streaming call cost a full round trip per message. Connect's envelopes
 * are self-delimiting and the Rust side feeds them to an `http_body`, so
 * several may ride in one send and be framed identically on arrival.
 *
 * Only chunks the producer already had are merged: the pump never waits for a
 * chunk that has not been produced. That distinction is what keeps bidi
 * responsive — a message whose reply the client is waiting on still leaves
 * immediately, alone, rather than being held back for a partner that may
 * depend on the response.
 */
async function pumpRequestBody(
  callId: bigint,
  body: AsyncIterable<Uint8Array>,
  signal: AbortSignal | undefined,
): Promise<void> {
  const iterator = body[Symbol.asyncIterator]();
  // Chunks produced while the previous send was in flight, awaiting a ride.
  let pending: Uint8Array[] = [];
  let pendingBytes = 0;
  // Sends stay chained so they arrive in order and a failed one surfaces at
  // the next await rather than becoming an unhandled rejection.
  let inFlight: Promise<void> = Promise.resolve();
  let sending = false;

  const flush = (): void => {
    const chunk = pending.length === 1 ? pending[0]! : concat(pending, pendingBytes);
    pending = [];
    pendingBytes = 0;
    sending = true;
    inFlight = inFlight
      .then(async () => {
        await invoke(
          SEND,
          toBinary(SendRequestSchema, create(SendRequestSchema, { callId, chunk })),
        );
      })
      .then(() => {
        sending = false;
      });
  };

  for (;;) {
    const next = await iterator.next();
    if (next.done === true) {
      break;
    }
    if (signal?.aborted === true) {
      return;
    }
    pending.push(next.value);
    pendingBytes += next.value.length;

    if (!sending) {
      // Nothing in flight, so this goes out on its own: the first message of a
      // bidi exchange must not wait for a partner that may depend on its reply.
      flush();
    } else if (pendingBytes >= MAX_PENDING_BYTES) {
      // The producer has outrun the IPC hop. Wait for the hop rather than keep
      // buffering; this is the backpressure.
      await inFlight;
      flush();
    }
  }

  if (pending.length > 0) {
    await inFlight;
    flush();
  }
  await inFlight;

  if (signal?.aborted !== true) {
    await invoke(
      SEND,
      toBinary(SendRequestSchema, create(SendRequestSchema, { callId, endOfStream: true })),
    );
  }
}

/** Join buffered chunks into the single body slice one send carries. */
function concat(chunks: Uint8Array[], total: number): Uint8Array {
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

/** Flatten `Headers` into repeated wire entries, preserving multi-values. */
function toWireHeaders(header: Headers): Header[] {
  const out: Header[] = [];
  header.forEach((value, name) => {
    out.push({ name, value } as Header);
  });
  return out;
}

function fromWireHeaders(headers: Header[]): Headers {
  const out = new Headers();
  for (const { name, value } of headers) {
    out.append(name, value);
  }
  return out;
}
