// ConnectRPC transport over Tauri IPC.

import type {
  BinaryReadOptions,
  BinaryWriteOptions,
  JsonReadOptions,
  JsonWriteOptions,
} from "@bufbuild/protobuf";
import type { Interceptor, Transport } from "@connectrpc/connect";
import { createTransport } from "@connectrpc/connect/protocol-connect";
import { convertFileSrc } from "@tauri-apps/api/core";

import { BUFFERED_RESPONSE_HEADER, STREAMING_REQUEST_HEADER, createTauriIpcClient } from "./ipc.js";

/**
 * Origin of the `ipc-connect://` scheme the Rust plugin registers.
 *
 * Unlike the old placeholder, this URL is really fetched: unary calls go
 * straight at the scheme handler. The origin differs by platform — Windows and
 * Android rewrite `scheme://localhost` to `http://scheme.localhost` — and
 * `convertFileSrc` is the mapping Tauri itself uses, so asking it for the
 * empty path yields the right origin everywhere without duplicating the rule.
 *
 * Streaming calls still travel over IPC commands, where only the path is read.
 */
const IPC_BASE_URL = convertFileSrc("", "ipc-connect").replace(/\/$/v, "");

export interface TauriTransportOptions {
  /**
   * Use the binary wire format. Defaults to `true`.
   *
   * Unlike a browser transport there is no devtools view of the traffic to
   * gain from JSON, and binary avoids re-encoding on a hop that never leaves
   * the process.
   */
  useBinaryFormat?: boolean;

  /** Interceptors applied to every call through this transport. */
  interceptors?: Interceptor[];

  /** Options for the JSON format. */
  jsonOptions?: Partial<JsonReadOptions & JsonWriteOptions>;

  /** Options for the binary wire format. */
  binaryOptions?: Partial<BinaryReadOptions & BinaryWriteOptions>;

  /** Timeout in milliseconds applied to all requests. */
  defaultTimeoutMs?: number;

  /**
   * Reject responses whose individual messages exceed this many bytes.
   * Defaults to Connect's ~4GiB maximum.
   */
  readMaxBytes?: number;

  /** Reject requests whose messages exceed this many bytes. */
  writeMaxBytes?: number;
}

/**
 * Create a `Transport` that carries Connect over Tauri IPC.
 *
 * Requires the `connectrpc` Tauri plugin on the Rust side.
 *
 * ```ts
 * const transport = createTauriTransport();
 * const client = createClient(GreetService, transport);
 * ```
 *
 * All four method kinds work. This wraps Connect's protocol-level
 * `createTransport` rather than the fetch-based web transport, which refuses
 * client-streaming and bidi because the fetch API cannot stream a request body.
 */
export function createTauriTransport(options: TauriTransportOptions = {}): Transport {
  return createTransport({
    httpClient: createTauriIpcClient(),
    baseUrl: IPC_BASE_URL,
    useBinaryFormat: options.useBinaryFormat ?? true,
    interceptors: [...(options.interceptors ?? []), markStreamingRequests],
    // Spread rather than assign: `exactOptionalPropertyTypes` distinguishes an
    // absent option from one explicitly set to `undefined`.
    ...(options.jsonOptions === undefined ? {} : { jsonOptions: options.jsonOptions }),
    ...(options.binaryOptions === undefined ? {} : { binaryOptions: options.binaryOptions }),
    ...(options.defaultTimeoutMs === undefined
      ? {}
      : { defaultTimeoutMs: options.defaultTimeoutMs }),
    readMaxBytes: options.readMaxBytes ?? 0xffffffff,
    writeMaxBytes: options.writeMaxBytes ?? 0xffffffff,
    // Compression is off: it spends CPU to shrink bytes on an in-process hop
    // that never touches a network.
    acceptCompression: [],
    sendCompression: null,
    compressMinBytes: 0,
  });
}
/**
 * Tag calls whose request body streams incrementally.
 *
 * Runs innermost so user interceptors never see the marker, which the IPC
 * client strips before the request leaves the webview.
 */
const markStreamingRequests: Interceptor = (next) => (req) => {
  if (
    req.stream &&
    (req.method.methodKind === "client_streaming" || req.method.methodKind === "bidi_streaming")
  ) {
    req.header.set(STREAMING_REQUEST_HEADER, "1");
  }
  // A unary response is a single message, complete at the moment the head is,
  // so it can ride back on the invoke instead of over a channel. Tauri
  // delivers each channel frame by evaluating JavaScript in the webview, so
  // this takes a unary call from three webview crossings down to one.
  if (req.method.methodKind === "unary") {
    req.header.set(BUFFERED_RESPONSE_HEADER, "1");
  }
  return next(req);
};
