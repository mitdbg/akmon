// The console chunk carries no Ant Design.
//
// Six components and eight icons cost 142,464 gzip bytes of the console chunk
// -- about three quarters of it -- because importing any one of them carries
// antd, a dozen @rc-component packages and the cssinjs runtime. Every one is
// the browser's own element now. This test is the guard: it fails on the
// import, in the file, rather than at `npm run operator-ui:bundle-budget`
// after the chunk has already grown past its ceiling.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
/** Everything the console chunk pulls in. The design harness is NOT here: it
 *  is resolved to a stub in a production build and may use whatever it likes. */
const TREES = ["console", "routes/console"];

function sources(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const path = join(dir, entry);
    if (statSync(path).isDirectory()) { out.push(...sources(path)); continue; }
    if (/\.tsx?$/.test(entry) && !/\.test\.tsx?$/.test(entry)) out.push(path);
  }
  return out;
}

/**
 * Whether this source pulls in Ant Design, by any import form.
 *
 * The guard used to be `/from\s+"(antd|@ant-design\/[^"]+)"/`, which sees one
 * spelling of one form. `import "antd/es/button"` and `antd/lib/...` are the
 * deep imports a reader reaches for precisely when they are trying to avoid
 * the whole library -- and they carry the cssinjs runtime anyway -- while a
 * single quote or an `import("antd")` inside a lazy boundary walked past a
 * guard whose whole job is to stop the chunk growing.
 */
export function antdImports(source: string): string[] {
  const pattern = /(?:\bfrom|\bimport|\brequire)\s*\(?\s*(['"])(antd(?:\/[^'"]*)?|@ant-design\/[^'"]*)\1/g;
  return [...source.matchAll(pattern)].map((match) => match[2]);
}

describe("the console chunk", () => {
  it("imports no Ant Design package", () => {
    const offenders = TREES
      .flatMap((tree) => sources(join(ROOT, tree)))
      .filter((path) => antdImports(readFileSync(path, "utf8")).length > 0)
      .map((path) => path.slice(ROOT.length));
    expect(offenders).toEqual([]);
  });

  it("sees every import form, not just a double-quoted `from \"antd\"`", () => {
    // The six the review probed the old guard with. It caught the first and
    // walked past the other five.
    const probes = [
      'import { Table } from "antd";',
      "import { Table } from 'antd';",
      'import Button from "antd/es/button";',
      "import Button from 'antd/lib/button';",
      'const m = await import("antd");',
      'import { SearchOutlined } from "@ant-design/icons";',
    ];
    for (const probe of probes) {
      expect(antdImports(probe), probe).not.toEqual([]);
    }
  });

  it("leaves a package that merely reads like one alone", () => {
    // `antdesign-x` and `not-antd` are not antd, and a string in prose is
    // not an import: a guard that fires on those gets turned off.
    expect(antdImports('import x from "antdesign-x";')).toEqual([]);
    expect(antdImports('import x from "not-antd";')).toEqual([]);
    expect(antdImports('// this file imports no antd at all')).toEqual([]);
    expect(antdImports('const name = "antd";')).toEqual([]);
  });

  it("styles no Ant Design class name", () => {
    const dir = join(ROOT, "console", "styles");
    const offenders = readdirSync(dir)
      .filter((name) => name.endsWith(".css"))
      .filter((name) => /\.ant-[a-z]/.test(readFileSync(join(dir, name), "utf8")));
    expect(offenders).toEqual([]);
  });
});
