// Times the Connect-over-IPC transport against the floor of what raw Tauri IPC
// can do for the same bytes.
//
// This runs in the real webview because that is the only place the cost is
// real: the expensive parts of Tauri's IPC are the webview boundary crossings,
// which a Rust-only benchmark never pays.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { createClient } from "@connectrpc/connect";
import { createTauriTransport } from "@connectrpc-tauri/transport";
import { Channel, invoke } from "@tauri-apps/api/core";

import { GreetRequestSchema, GreetResponseSchema, GreetService } from "./gen/greet/v1/greet_pb.js";

const client = createClient(GreetService, createTauriTransport());

/**
 * The same transport in Connect's JSON codec.
 *
 * Separate from the IPC question: this changes how a *message* is encoded,
 * while the payload still crosses IPC as raw bytes either way.
 */
const jsonClient = createClient(GreetService, createTauriTransport({ useBinaryFormat: false }));

/** One timed case. Times are milliseconds per operation. */
interface Result {
  group: string;
  name: string;
  median: number;
  p95: number;
  /** Operations per timed round, chosen by calibration. */
  batch: number;
}

/** Target duration of one timed round. */
const ROUND_TARGET_MS = 50;

/** Rounds timed per case, after calibration. */
const ROUNDS = 15;

/** Run `fn` `count` times in sequence and return the elapsed milliseconds. */
async function timeBatch(fn: () => Promise<unknown>, count: number): Promise<number> {
  const started = performance.now();
  for (let i = 0; i < count; i++) {
    await fn();
  }
  return performance.now() - started;
}

/**
 * Time `fn` and report the median cost of one operation.
 *
 * Operations are timed in batches rather than individually because WebKit
 * clamps `performance.now()` to 1ms: a call that takes 80µs and one that takes
 * 900µs both read as either 0ms or 1ms, which is not a measurement. The batch
 * is calibrated to run for `ROUND_TARGET_MS`, so the clamp becomes noise on a
 * quantity ~50x larger than itself.
 *
 * The median rather than the mean: a webview shares a thread with layout and
 * GC, so a handful of runs are always far off the typical cost and would drag
 * a mean away from what a call actually costs.
 */
async function measure(group: string, name: string, fn: () => Promise<unknown>): Promise<Result> {
  // Warm up: the first calls pay for JIT, the Connect client's per-method
  // setup, and Tauri's first custom-protocol request.
  await timeBatch(fn, 5);

  // Calibrate: double the batch until a round is long enough to measure. The
  // cap keeps a slow case (a 100-message stream) from running for minutes.
  let batch = 1;
  while (batch < 4096 && (await timeBatch(fn, batch)) < ROUND_TARGET_MS) {
    batch *= 2;
  }

  const perOp: number[] = [];
  for (let round = 0; round < ROUNDS; round++) {
    perOp.push((await timeBatch(fn, batch)) / batch);
  }
  perOp.sort((a, b) => a - b);

  return {
    group,
    name,
    batch,
    median: perOp[Math.floor(perOp.length / 2)] ?? 0,
    p95: perOp[Math.min(perOp.length - 1, Math.floor(perOp.length * 0.95))] ?? 0,
  };
}

/** A name of exactly `bytes` characters, to drive the payload sweep. */
function nameOfSize(bytes: number): string {
  return "x".repeat(bytes);
}

/** Unary through the full Connect transport. */
async function connectUnary(name: string): Promise<void> {
  await client.greet({ name });
}

/** Unary through one raw invoke carrying protobuf both ways. */
async function rawUnary(name: string): Promise<void> {
  const payload = toBinary(GreetRequestSchema, create(GreetRequestSchema, { name }));
  const raw = await invoke<ArrayBuffer>("bench_raw_unary", payload);
  fromBinary(GreetResponseSchema, new Uint8Array(raw));
}

/** Unary through Tauri's default JSON command arguments. */
async function rawJson(name: string): Promise<void> {
  await invoke<string>("bench_raw_json", { name });
}

/** Unary through the transport, with Connect's JSON codec instead of binary. */
async function connectJsonUnary(name: string): Promise<void> {
  await jsonClient.greet({ name });
}

/**
 * Unary carrying protobuf bytes as a JSON command argument.
 *
 * The comparison `bench_raw_json` cannot make: a Connect message is bytes, and
 * Tauri serializes bytes inside a JSON payload as one number per byte.
 */
async function rawJsonBytes(name: string): Promise<void> {
  const payload = toBinary(GreetRequestSchema, create(GreetRequestSchema, { name }));
  const raw = await invoke<number[]>("bench_raw_json_bytes", { payload: Array.from(payload) });
  fromBinary(GreetResponseSchema, new Uint8Array(raw));
}

/** Server streaming through the full Connect transport. */
async function connectStream(name: string, count: number): Promise<number> {
  let seen = 0;
  for await (const _ of client.greetMany({ name, count })) {
    seen++;
  }
  return seen;
}

/** Server streaming through a bare Tauri channel, no Connect framing. */
async function rawStream(count: number, size: number): Promise<number> {
  let seen = 0;
  const channel = new Channel<ArrayBuffer>();
  const done = new Promise<number>((resolve) => {
    channel.onmessage = () => {
      if (++seen === count) {
        resolve(seen);
      }
    };
  });
  await invoke("bench_raw_stream", { count, size, channel });
  return done;
}

/** Render the results as a table, grouped, with each group's floor as 1.00x. */
function format(results: Result[]): string {
  const lines: string[] = ["", "ConnectRPC over Tauri IPC — benchmark", "=".repeat(64)];

  const groups = [...new Set(results.map((r) => r.group))];
  for (const group of groups) {
    const rows = results.filter((r) => r.group === group);
    // The fastest case in a group is the floor; the ratio is what says whether
    // raw IPC beats the transport and by how much.
    const floor = Math.min(...rows.map((r) => r.median));

    lines.push(
      "",
      group,
      `  ${"case".padEnd(30)}${"median".padStart(12)}${"p95".padStart(12)}${"vs best".padStart(10)}`,
    );
    for (const row of rows) {
      lines.push(
        `  ${row.name.padEnd(30)}${`${row.median.toFixed(3)}ms`.padStart(12)}${`${row.p95.toFixed(3)}ms`.padStart(12)}${`${(row.median / floor).toFixed(2)}x`.padStart(10)}`,
      );
    }
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * Run every case and hand the table to Rust to print.
 *
 * Sizes straddle 1024 bytes deliberately: that is Tauri's threshold for
 * sending a channel payload by `eval` rather than by fetch, so a transport
 * whose responses ride the channel changes cost sharply there.
 */
export async function runBenchmark(): Promise<void> {
  const results: Result[] = [];
  const sizes = [16, 512, 4096, 65536];

  for (const size of sizes) {
    const name = nameOfSize(size);
    const group = `unary, ${size}-byte request`;

    results.push(
      await measure(group, "connect (this transport)", () => connectUnary(name)),
      await measure(group, "connect, JSON codec", () => connectJsonUnary(name)),
      await measure(group, "raw invoke, protobuf", () => rawUnary(name)),
      await measure(group, "raw invoke, bytes as JSON", () => rawJsonBytes(name)),
      await measure(group, "raw invoke, JSON args", () => rawJson(name)),
    );
  }

  for (const size of [16, 4096]) {
    const count = 100;
    const group = `server stream, ${count} x ${size}-byte messages`;
    results.push(
      await measure(group, "connect (this transport)", () =>
        connectStream(nameOfSize(size), count),
      ),
      await measure(group, "raw channel", () => rawStream(count, size)),
    );
  }

  await invoke("bench_report", { report: format(results) });
}
