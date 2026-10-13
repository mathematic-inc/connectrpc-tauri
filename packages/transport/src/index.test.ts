// Apps import the transport in code that also runs outside a Tauri webview, such
// as a browser build, so only creating a transport may need Tauri.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const globals = globalThis as Record<string, unknown>;

beforeEach(() => {
  vi.resetModules();
});

afterEach(() => {
  delete globals.__TAURI_INTERNALS__;
});

describe("createTauriTransport", () => {
  it("can be imported outside a Tauri webview", async () => {
    expect(globals.__TAURI_INTERNALS__).toBeUndefined();

    const transport = await import("./index.js");

    expect(transport.createTauriTransport).toBeTypeOf("function");
  });

  it("asks Tauri for the scheme's origin when a transport is created", async () => {
    const convertFileSrc = vi.fn(() => "ipc-connect://localhost/");
    // `@tauri-apps/api/core` reads `window.__TAURI_INTERNALS__`; in Node, `window` is `globalThis`.
    globals.window ??= globals;
    globals.__TAURI_INTERNALS__ = { convertFileSrc };

    const { createTauriTransport } = await import("./index.js");
    expect(convertFileSrc).not.toHaveBeenCalled();

    expect(createTauriTransport()).toHaveProperty("unary");
    expect(convertFileSrc).toHaveBeenCalledWith("", "ipc-connect");
  });
});
