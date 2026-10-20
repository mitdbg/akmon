/**
 * The suite's timezone pin, asserted by name.
 *
 * `vitest.operator-ui.config.ts` pins TZ=UTC with `pool: "forks"`, because the
 * assignment does not reach the timezone inside a worker thread. If that pin
 * stops working — a `--pool=threads` override, or one of the two lines going
 * away — every assertion on a rendered timestamp starts depending on the host
 * zone and drifts by machine. This test fails first, and says why.
 */
import { describe, expect, it } from "vitest";

describe("operator UI test suite", () => {
  it("runs under a suite timezone pinned to UTC", () => {
    expect(process.env.TZ).toBe("UTC");
    expect(Intl.DateTimeFormat().resolvedOptions().timeZone).toBe("UTC");
    expect(new Date("2026-09-16T00:30:00Z").getHours()).toBe(0);
  });
});
