import { defineConfig } from "vite";

export default defineConfig({
  // Tauri serves the built assets from here.
  build: { outDir: "dist", target: "esnext" },
  // Not 5173: that is Vite's default and collides with any other dev server
  // already running on this machine.
  server: { port: 5183, strictPort: true },
  clearScreen: false,
});
