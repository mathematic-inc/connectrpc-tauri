// Exercises all four ConnectRPC method kinds over the Tauri IPC transport.

import { createClient } from "@connectrpc/connect";
import { createTauriTransport } from "@connectrpc-tauri/transport";
import { invoke } from "@tauri-apps/api/core";

import { GreetService } from "./gen/greet/v1/greet_pb.js";

const client = createClient(GreetService, createTauriTransport());

const log = document.querySelector<HTMLElement>("#log")!;
const nameInput = document.querySelector<HTMLInputElement>("#name")!;
const cancelButton = document.querySelector<HTMLButtonElement>("#cancel")!;

/** Controller for the call in flight, so Cancel has something to abort. */
let inFlight: AbortController | undefined;

/**
 * Mirror writes to Rust in order.
 *
 * `invoke` resolves asynchronously, so firing one per line would let the
 * printed transcript interleave out of order.
 */
let mirrored = Promise.resolve();

function write(line: string, kind: "info" | "error" = "info"): void {
  const entry = document.createElement("div");
  entry.className = `line ${kind}`;
  entry.textContent = line;
  log.append(entry);
  log.scrollTop = log.scrollHeight;
  // Mirrored to the Rust side so a run is followable from a terminal; a
  // webview console is not readable from one.
  mirrored = mirrored
    .then(async () => {
      await invoke("selftest_log", { line });
    })
    .catch(() => {});
}

/**
 * Run a call with a fresh abort controller and uniform error reporting.
 *
 * Resolves to the greetings received, or `undefined` if the call failed.
 */
async function run(
  label: string,
  call: (signal: AbortSignal) => Promise<string[]>,
): Promise<string[] | undefined> {
  write(`▶ ${label}`);

  inFlight = new AbortController();
  cancelButton.disabled = false;
  try {
    const greetings = await call(inFlight.signal);
    write("✓ done");
    return greetings;
  } catch (e) {
    write(`✗ ${String(e)}`, "error");
    return undefined;
  } finally {
    cancelButton.disabled = true;
    inFlight = undefined;
  }
}

/**
 * The four method kinds.
 *
 * Each returns the greetings it received and states what it expects, so the
 * self-test asserts on the RPC's own results rather than on scraped log text.
 */
const kinds: ReadonlyArray<{
  id: string;
  label: string;
  call: (signal: AbortSignal) => Promise<string[]>;
  expected: string[];
}> = [
  {
    id: "unary",
    label: "unary",
    expected: ["Hello, World!"],
    call: async (signal) => {
      const response = await client.greet({ name: nameInput.value }, { signal });
      write(response.greeting);
      return [response.greeting];
    },
  },
  {
    id: "server-stream",
    label: "server stream",
    expected: [
      "Hello, World! (1/5)",
      "Hello, World! (2/5)",
      "Hello, World! (3/5)",
      "Hello, World! (4/5)",
      "Hello, World! (5/5)",
    ],
    call: async (signal) => {
      // Responses arrive one at a time; the Rust side paces them so the
      // streaming is visible rather than instantaneous.
      const greetings: string[] = [];
      for await (const response of client.greetMany(
        { name: nameInput.value, count: 5 },
        { signal },
      )) {
        write(response.greeting);
        greetings.push(response.greeting);
      }
      return greetings;
    },
  },
  {
    id: "client-stream",
    label: "client stream",
    expected: ["Hello, World!"],
    call: async (signal) => {
      const names = nameInput.value
        .split(",")
        .map((n) => n.trim())
        .filter((n) => n.length > 0);

      async function* requests() {
        for (const name of names) {
          write(`→ ${name}`);
          yield { name };
        }
      }

      // One response after the whole request stream is consumed.
      const response = await client.greetAll(requests(), { signal });
      write(response.greeting);
      return [response.greeting];
    },
  },
  {
    id: "bidi",
    label: "bidi",
    expected: ["Hello, Ada!", "Hello, Grace!", "Hello, Alan!"],
    call: async (signal) => {
      const names = ["Ada", "Grace", "Alan"];

      async function* requests() {
        for (const name of names) {
          write(`→ ${name}`);
          yield { name };
          // Spaced out so responses visibly interleave with requests instead
          // of arriving in one burst at the end.
          await new Promise((resolve) => {
            setTimeout(resolve, 300);
          });
        }
      }

      const greetings: string[] = [];
      for await (const response of client.greetChat(requests(), { signal })) {
        write(`← ${response.greeting}`);
        greetings.push(response.greeting);
      }
      return greetings;
    },
  },
];

for (const { id, label, call } of kinds) {
  document.querySelector(`#${id}`)!.addEventListener("click", () => {
    log.replaceChildren();
    void run(label, call);
  });
}

/**
 * Exercise every method kind, then cancellation, asserting the results.
 *
 * This is the only check that covers the full stack in a real webview: the
 * unit tests on both sides stub the IPC bridge, so nothing else proves that
 * Tauri's own commands and channels carry the protocol correctly. Results go
 * to the Rust side, which prints them, so the run is verifiable from a
 * terminal instead of from a screenshot.
 */
async function runAll(): Promise<void> {
  log.replaceChildren();
  const failures: string[] = [];

  // The transport must negotiate the binary codec: JSON would re-encode every
  // message on a hop that has no devtools view to gain from being readable.
  // Asserted at the wire rather than by reading the default back.
  const negotiated = await new Promise<string>((resolve) => {
    void createClient(
      GreetService,
      createTauriTransport({
        interceptors: [
          (next) => (req) => {
            resolve(req.header.get("content-type") ?? "missing");
            return next(req);
          },
        ],
      }),
    ).greet({ name: "World" });
  });
  if (!negotiated.includes("proto")) {
    failures.push(`codec: expected a binary content-type, got ${negotiated}`);
  }
  write(`codec: ${negotiated}`);

  for (const { label, call, expected } of kinds) {
    const greetings = await run(label, call);

    if (greetings === undefined) {
      failures.push(`${label}: call failed`);
    } else if (JSON.stringify(greetings) !== JSON.stringify(expected)) {
      // Compared in order: a streaming transport that delivered the right
      // messages in the wrong sequence is still broken.
      failures.push(
        `${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(greetings)}`,
      );
    }
  }

  // Cancellation is a separate case: the call must reject rather than run to
  // completion, so success here means the opposite of the checks above.
  write("▶ cancel");
  const controller = new AbortController();
  try {
    let seen = 0;
    for await (const response of client.greetMany(
      { name: "World", count: 50 },
      { signal: controller.signal },
    )) {
      write(response.greeting);
      if (++seen === 2) {
        controller.abort();
      }
    }
    failures.push("cancel: stream finished instead of aborting");
  } catch {
    write("✓ cancelled");
  }

  const passed = failures.length === 0;
  write(passed ? "✓ self-test passed" : "✗ self-test failed", passed ? "info" : "error");
  for (const failure of failures) {
    write(failure, "error");
  }
  // Drain the mirror first, or the verdict can be printed before the lines
  // that explain it.
  await mirrored;
  await invoke("selftest_report", { passed, failures });
}

document.querySelector("#run-all")!.addEventListener("click", () => void runAll());

// Guarded because Vite's dev client can evaluate the module more than once;
// two concurrent runs interleave their logs and corrupt the verdict.
if (!("__selftestStarted" in window)) {
  Object.defineProperty(window, "__selftestStarted", { value: true });
  // Under `GREET_APP_BENCH` the app is being run to measure the transport, not
  // to demo it; the self-test's paced streams would dominate the timings.
  // Not top-level await: that would block the rest of this module, leaving the
  // cancel button unwired until the whole self-test or benchmark finishes.
  // oxlint-disable-next-line unicorn/prefer-top-level-await
  void invoke<boolean>("bench_mode").then(async (benching) => {
    if (benching) {
      const { runBenchmark } = await import("./bench.js");
      await runBenchmark();
      return;
    }
    await runAll();
  });
}

cancelButton.addEventListener("click", () => {
  inFlight?.abort();
  write("cancelled", "error");
});
