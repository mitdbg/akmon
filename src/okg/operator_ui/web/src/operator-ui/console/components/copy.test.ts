// The clipboard helper: stubbable, and loud when there is no clipboard.
import { afterEach, describe, expect, it } from "vitest";

import { COPIED_MS, copy, setClipboardWriter } from "@operator-ui/console/components/copy";

afterEach(() => setClipboardWriter(null));

describe("copy()", () => {
  it("hands the exact text to the writer a test installs", async () => {
    const written: string[] = [];
    setClipboardWriter(async (text) => { written.push(text); });
    await copy("okg:archiv3-small:branch:default:gen:20260913T090000000000Z:eea4e0d4e88d");
    expect(written).toEqual([
      "okg:archiv3-small:branch:default:gen:20260913T090000000000Z:eea4e0d4e88d",
    ]);
  });

  it("rejects by name where there is no clipboard, rather than pretending", async () => {
    // Node has no `navigator.clipboard`; a browser off HTTPS has none either.
    await expect(copy("anything")).rejects.toThrow(/clipboard is not available/);
    await expect(copy("anything")).rejects.toThrow(/Nothing was copied/);
  });

  it("propagates the writer's own failure", async () => {
    setClipboardWriter(async () => { throw new Error("denied by the browser"); });
    await expect(copy("x")).rejects.toThrow("denied by the browser");
  });

  it("says copied for a second and a half", () => {
    expect(COPIED_MS).toBe(1500);
  });
});
