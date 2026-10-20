// The console never reads a server-path or raw-error field (issue #2130).
// PACT console-reads-mask-paths-and-exception-text.
//
// The server no longer serves `read_authority.registry` as a path, a worktree's
// `cwd`, or an exception's text. This guard holds the other side: no console
// source reads those fields, and the only places that read an `error` field
// are the ones that print what the server already reduced to a code and a
// fixed sentence (the failure-body parsers) or to the kind of failure (a view
// run's error, filtered by `_safe_view_run_error`). A new read of any of them
// fails here by file name, so it is a decision, not an accident.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = resolve(__dirname, "..");

function sources(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) {
      if (name === "__fixtures__") continue;
      out.push(...sources(path));
    } else if (/\.(ts|tsx)$/.test(name) && !/\.test\.(ts|tsx)$/.test(name)) {
      out.push(path);
    }
  }
  return out;
}

/** The source with comments removed, so a sentence ABOUT a field is not a read. */
function code(path: string): string {
  return readFileSync(path, "utf8")
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
}

const FILES = sources(ROOT).map((path) => ({ rel: relative(ROOT, path), text: code(path) }));

function readers(pattern: RegExp): string[] {
  return FILES.filter((file) => pattern.test(file.text)).map((file) => file.rel).sort();
}

/** Every `interface Name { ... }` whose name matches, with its body. */
function interfaces(pattern: RegExp): { file: string; name: string; body: string }[] {
  const found: { file: string; name: string; body: string }[] = [];
  for (const file of FILES) {
    const head = /(?:export\s+)?interface\s+(\w+)[^{]*\{/g;
    let match: RegExpExecArray | null;
    while ((match = head.exec(file.text)) !== null) {
      if (!pattern.test(match[1])) continue;
      let depth = 1;
      let index = head.lastIndex;
      while (depth > 0 && index < file.text.length) {
        const char = file.text[index];
        if (char === "{") depth += 1;
        if (char === "}") depth -= 1;
        index += 1;
      }
      found.push({ file: file.rel, name: match[1], body: file.text.slice(head.lastIndex, index - 1) });
    }
  }
  return found;
}

describe("console reads of server-path and raw-error fields", () => {
  it("scans the console source tree", () => {
    expect(FILES.length).toBeGreaterThan(50);
  });

  it("never reads the views registry location", () => {
    expect(readers(/\.registry\b|\[\s*["']registry["']\s*\]|(?<![\w.])registry\??\s*:/)).toEqual([]);
  });

  it("never reads a worktree's working directory", () => {
    expect(readers(/(?<!process)\.cwd\b|\bcwd\??\s*:|\[\s*["']cwd["']\s*\]/)).toEqual([]);
  });

  it("declares a view run's `error` only where the server filters it", () => {
    const views = interfaces(/View(Run|Row|Diagnostics)/);
    // Not vacuous: the view-run and view-diagnostics shapes are all found.
    // `ViewDiagnosticsRow` and `ViewRunPayload` were the old Health page's,
    // and went with it (PACT console-default-remove-old-views).
    expect(views.map((shape) => shape.name)).toEqual(expect.arrayContaining([
      "RawViewRun", "RawViewRow", "ViewRunRow",
    ]));
    const withError = views
      .filter((shape) => /(^|[\s;,{])error\??\s*:/.test(shape.body))
      .map((shape) => `${shape.file}:${shape.name}`);
    // `RawViewRun` is the generations read's per-attempt run, whose `error` is
    // served through `_safe_view_run_error`: the kind of failure, never its
    // message or traceback. Every other view shape carries no error at all.
    expect(withError).toEqual(["console/model/shared/reads.ts:RawViewRun"]);
  });
});
