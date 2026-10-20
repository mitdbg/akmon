import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "node",
    // Both extensions: a `.ts` test under a `.tsx`-only glob is not skipped,
    // it is absent from collection -- it reports nowhere and gates nothing.
    include: ["src/okg/bench/web/**/*.test.{ts,tsx}"],
    globals: true,
    // The viewer renders timestamps in the reader's local zone, so an assertion
    // on a rendered date is only reproducible against a fixed zone. Pin the
    // suite to UTC.
    //
    // `env` sets a *process* environment variable. Node re-reads TZ on
    // assignment only on the main thread; inside a worker_thread the assignment
    // does not reach the timezone, so under the `threads` pool this pin is
    // silently ineffective and there is no way to pin TZ there at all. Pin the
    // pool to `forks`, where each worker is a fresh process that starts with
    // TZ=UTC. These two lines are one mechanism: do not remove either alone.
    // Passing `--pool=threads` on the command line overrides this and breaks
    // the pin; the "runs under a suite timezone pinned to UTC" test in
    // src/okg/bench/web/flamegraph.test.tsx fails by name when that happens.
    pool: "forks",
    env: { TZ: "UTC" },
  },
});
