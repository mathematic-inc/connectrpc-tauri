// Exercises the IPC client against a fake Tauri bridge.
//
// The fake stands in for the Rust plugin: it decodes the same protobuf
// envelopes and replies on the same channel, so ordering, backpressure,
// streaming, and cancellation are covered without a webview.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { beforeEach, describe, expect, it } from "vitest";

import {
  CancelRequestSchema,
  ResponseFrameSchema,
  ResponseHeadSchema,
  SendRequestSchema,
  StartRequestSchema,
} from "./gen/connectrpc/tauri/v1/transport_pb.js";

/** A call captured by the fake bridge. */
interface FakeCall {
  callId: bigint;
  url: string;
  streamingRequest: boolean;
  chunks: Uint8Array[];
  ended: boolean;
  cancelled: boolean;
  send: (frame: Uint8Array) => void;
}

const calls = new Map<bigint, FakeCall>();
/** Resolves the next `connect_rpc` invoke; set per test to control timing. */
let respondToStart: (call: FakeCall) => Promise<Uint8Array>;

// `@tauri-apps/api/core` reads this global; the real `invoke` and `Channel`
// are thin wrappers over it, so faking here keeps the module under test intact.
beforeEach(() => {
  calls.clear();
  const callbacks = new Map<number, (payload: unknown) => void>();
  let nextCallbackId = 1;

  respondToStart = async () =>
    toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));

  // The API reads `window.__TAURI_INTERNALS__`, so the fake has to live there.
  const globals = globalThis as Record<string, unknown>;
  globals.window ??= globals;
  (globals.window as Record<string, unknown>).__TAURI_INTERNALS__ = {
    transformCallback(callback: (payload: unknown) => void) {
      const id = nextCallbackId++;
      callbacks.set(id, callback);
      return id;
    },
    unregisterCallback(id: number) {
      callbacks.delete(id);
    },
    async invoke(cmd: string, args: unknown) {
      const payload = new Uint8Array(args as ArrayBuffer);

      if (cmd.endsWith("connect_rpc")) {
        const start = fromBinary(StartRequestSchema, payload);
        if (start.channel === "") {
          // The buffered path: no channel, so the fake answers inline exactly
          // as the Rust side does.
          const call: FakeCall = {
            callId: start.callId,
            url: start.url,
            streamingRequest: start.streamingRequest,
            chunks: start.body.length > 0 ? [start.body] : [],
            ended: true,
            cancelled: false,
            send() {},
          };
          calls.set(start.callId, call);
          return await respondToStart(call);
        }
        const channelId = Number(start.channel.replace("__CHANNEL__:", ""));
        let index = 0;
        const call: FakeCall = {
          callId: start.callId,
          url: start.url,
          streamingRequest: start.streamingRequest,
          chunks: start.body.length > 0 ? [start.body] : [],
          ended: false,
          cancelled: false,
          send(frame) {
            callbacks.get(channelId)?.({ message: frame.buffer, index: index++ });
          },
        };
        calls.set(start.callId, call);
        return await respondToStart(call);
      }

      if (cmd.endsWith("connect_rpc_send")) {
        const send = fromBinary(SendRequestSchema, payload);
        const call = calls.get(send.callId);
        if (call === undefined) {
          return null;
        }
        if (send.endOfStream) {
          call.ended = true;
        } else {
          call.chunks.push(send.chunk);
        }
        return null;
      }

      if (cmd.endsWith("connect_rpc_cancel")) {
        const cancel = fromBinary(CancelRequestSchema, payload);
        const call = calls.get(cancel.callId);
        if (call !== undefined) {
          call.cancelled = true;
        }
        return null;
      }

      throw new Error(`unexpected command ${cmd}`);
    },
  };
});

/** Encode a response frame the way the Rust side does. */
function messageFrame(bytes: Uint8Array): Uint8Array {
  return toBinary(
    ResponseFrameSchema,
    create(ResponseFrameSchema, { frame: { case: "message", value: bytes } }),
  );
}

function endFrame(): Uint8Array {
  return toBinary(
    ResponseFrameSchema,
    create(ResponseFrameSchema, { frame: { case: "end", value: {} } }),
  );
}

describe("tauri ipc client", () => {
  it("delivers response frames in order and ends the body", async () => {
    const { createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    respondToStart = async (call) => {
      // Push the whole response before the head resolves, the worst case for
      // ordering: frames must still arrive in sequence.
      call.send(messageFrame(new Uint8Array([1])));
      call.send(messageFrame(new Uint8Array([2])));
      call.send(endFrame());
      return toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));
    };

    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header: new Headers(),
    });

    const received: number[] = [];
    for await (const chunk of response.body) {
      received.push(...chunk);
    }

    expect(response.status).toBe(200);
    expect(received).toEqual([1, 2]);
  });

  it("sends a non-streaming request body in the start frame", async () => {
    const { createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    respondToStart = async (call) => {
      call.send(endFrame());
      return toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));
    };

    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header: new Headers(),
      body: (async function* () {
        yield new Uint8Array([7, 8, 9]);
      })(),
    });
    for await (const _ of response.body) {
      // drain
    }

    const call = [...calls.values()][0];
    expect(call?.streamingRequest).toBe(false);
    expect(call?.chunks).toEqual([new Uint8Array([7, 8, 9])]);
  });

  it("streams a client-streaming body without waiting for the head", async () => {
    const { STREAMING_REQUEST_HEADER, createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    // Hold the head until the request body has fully arrived. This is exactly
    // how a client-streaming handler behaves, and it deadlocks any client that
    // waits for the head before pumping.
    respondToStart = async (call) => {
      while (!call.ended) {
        await new Promise((resolve) => {
          setTimeout(resolve, 1);
        });
      }
      call.send(endFrame());
      return toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));
    };

    const header = new Headers();
    header.set(STREAMING_REQUEST_HEADER, "1");

    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header,
      body: (async function* () {
        yield new Uint8Array([1]);
        yield new Uint8Array([2]);
      })(),
    });
    for await (const _ of response.body) {
      // drain
    }

    const call = [...calls.values()][0];
    expect(call?.streamingRequest).toBe(true);
    expect(call?.chunks).toEqual([new Uint8Array([1]), new Uint8Array([2])]);
    expect(call?.ended).toBe(true);
  });

  it("strips the streaming marker before the request leaves the webview", async () => {
    const { STREAMING_REQUEST_HEADER, createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    respondToStart = async (call) => {
      call.send(endFrame());
      return toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));
    };

    const header = new Headers();
    header.set(STREAMING_REQUEST_HEADER, "1");
    header.set("content-type", "application/proto");

    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header,
      body: (async function* () {
        yield new Uint8Array([1]);
      })(),
    });
    for await (const _ of response.body) {
      // drain
    }

    expect(header.has(STREAMING_REQUEST_HEADER)).toBe(false);
    // The real headers still made it through.
    expect(header.get("content-type")).toBe("application/proto");
  });

  it("takes the response inline, with no channel, when marked buffered", async () => {
    const { BUFFERED_RESPONSE_HEADER, createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    // Fails the test if the client subscribes a channel: the whole point of
    // this path is that no channel exists to send on.
    respondToStart = async (_call) =>
      toBinary(
        ResponseHeadSchema,
        create(ResponseHeadSchema, {
          status: 200,
          body: new Uint8Array([4, 5, 6]),
          headers: [{ name: "content-type", value: "application/proto" } as never],
        }),
      );

    const header = new Headers();
    header.set(BUFFERED_RESPONSE_HEADER, "1");

    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header,
      body: (async function* () {
        yield new Uint8Array([1, 2, 3]);
      })(),
    });

    const received: number[] = [];
    for await (const chunk of response.body) {
      received.push(...chunk);
    }

    expect(response.status).toBe(200);
    expect(received).toEqual([4, 5, 6]);
    expect(response.header.get("content-type")).toBe("application/proto");
    // The marker must not reach the service.
    expect(header.has(BUFFERED_RESPONSE_HEADER)).toBe(false);

    const call = [...calls.values()][0];
    expect(call?.chunks).toEqual([new Uint8Array([1, 2, 3])]);
  });

  it("cancels the call when the signal aborts", async () => {
    const { createTauriIpcClient } = await import("./ipc.js");
    const client = createTauriIpcClient();

    respondToStart = async (call) => {
      call.send(messageFrame(new Uint8Array([1])));
      return toBinary(ResponseHeadSchema, create(ResponseHeadSchema, { status: 200 }));
    };

    const controller = new AbortController();
    const response = await client({
      url: "tauri://ipc/pkg.Service/Method",
      method: "POST",
      header: new Headers(),
      signal: controller.signal,
    });

    controller.abort();
    // Let the cancel invoke settle.
    await new Promise((resolve) => {
      setTimeout(resolve, 5);
    });

    const call = [...calls.values()][0];
    expect(call?.cancelled).toBe(true);

    // The body must terminate rather than hang after a cancel.
    const drained: number[] = [];
    for await (const chunk of response.body) {
      drained.push(...chunk);
    }
    expect(drained.length).toBeLessThanOrEqual(1);
  });
});
