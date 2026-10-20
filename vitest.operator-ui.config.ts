import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// Worker count on a CI pod. Vitest sizes its `forks` pool from
// os.availableParallelism(), which inside a Kubernetes pod reports the NODE's
// cores, not the pod's 2-CPU quota: on the light pool that is a dozen forks
// time-slicing two CPUs. Measured on PR #2329 run 35815733973 (light pod
// okg-arc-light-8p7pn-runner-ztfdd): the suite took 193 s against 17-19 s on
// the VMs (dev runs 35798999680, 35804471874, 35809898150), and
// Sources.test.tsx "shows the first hundred of scale-200 and says so" took
// 6.0 s against 0.22-0.24 s, past the 5 s default timeout. The executor already
// exports the pod's size as OKG_QA_XDIST_MAX_WORKERS (2 on a light pod, 4 on a
// heavy one) for the Python lanes, and the reviewed executor cannot grow a new
// variable without a re-pin, so the suite reads the same one. Unset -- a
// developer's machine -- keeps Vitest's own default. A value that is not a
// positive integer is refused rather than ignored.
function ciMaxWorkers(): number | undefined {
  const raw = process.env.OKG_QA_XDIST_MAX_WORKERS;
  if (raw === undefined || raw === "") {
    return undefined;
  }
  const workers = Number(raw);
  if (!Number.isInteger(workers) || workers < 1) {
    throw new Error(
      `OKG_QA_XDIST_MAX_WORKERS must be a positive integer, got ${JSON.stringify(raw)}`,
    );
  }
  return workers;
}

const maxWorkers = ciMaxWorkers();

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "node",
    // Both extensions: a `.ts` test under a `.tsx`-only glob is not skipped,
    // it is absent from collection -- it reports nowhere and gates nothing.
    // tests/ci/test_operator_ui_test_collection.py holds this to the files on
    // disk so a future edit here cannot silently orphan tests again.
    include: ["src/okg/operator_ui/web/src/**/*.test.{ts,tsx}"],
    globals: true,
    // The UI renders timestamps in the viewer's local zone, so an assertion on a
    // rendered date is only reproducible against a fixed zone. Pin the suite to
    // UTC; the local-time contract itself is asserted with an explicit timeZone
    // argument instead of the host zone.
    //
    // `env` sets a *process* environment variable. Node re-reads TZ on assignment
    // only on the main thread; inside a worker_thread the assignment does not
    // reach the timezone, so under the `threads` pool this pin is silently
    // ineffective and there is no way to pin TZ there at all. Pin the pool to
    // `forks`, where each worker is a fresh process that starts with TZ=UTC.
    // These two lines are one mechanism: do not remove either alone. Passing
    // `--pool=threads` on the command line overrides this and breaks the pin;
    // the "runs under a suite timezone pinned to UTC" test in
    // app/suiteTimezone.test.tsx fails by name when that happens.
    pool: "forks",
    env: { TZ: "UTC" },
    ...(maxWorkers === undefined ? {} : { maxWorkers }),
  },
  resolve: {
    alias: {
      "@operator-ui": fileURLToPath(
        new URL("./src/okg/operator_ui/web/src/operator-ui", import.meta.url),
      ),
      // The six mock deployments live with the design tooling, outside the
      // application tree; only design-harness/fixtures.ts imports them.
      "@operator-ui-design-fixtures": fileURLToPath(
        new URL("./scripts/operator-ui-design/fixtures", import.meta.url),
      ),
    },
  },
});
