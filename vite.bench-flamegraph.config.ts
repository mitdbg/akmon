import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const outDir = process.env.OKG_BENCH_FLAMEGRAPH_OUT
  || fileURLToPath(new URL("./.tmp/bench-flamegraph-viewer", import.meta.url));

export default defineConfig({
  plugins: [react()],
  define: {
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
  // No alias: the viewer lives in src/okg/bench/web/ and every module it needs
  // is a relative sibling there. The `@operator-ui` alias went away with the
  // last import out of src/okg/operator_ui/.
  build: {
    outDir,
    emptyOutDir: false,
    sourcemap: false,
    cssCodeSplit: false,
    lib: {
      entry: fileURLToPath(
        new URL("./src/okg/bench/web/bench-flamegraph.tsx", import.meta.url),
      ),
      name: "OKGBenchFlamegraphViewer",
      formats: ["iife"],
      fileName: () => "bench-flamegraph.js",
      cssFileName: "bench-flamegraph",
    },
  },
});
