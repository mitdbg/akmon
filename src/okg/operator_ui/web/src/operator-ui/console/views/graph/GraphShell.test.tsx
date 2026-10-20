// @vitest-environment jsdom
//
// The Graph view's shell, Table and Grid, mounted against the harness's
// fixtures and driven through their URL: the default drawing written into
// the URL, the Map / Table / Grid links, the ignored-parameter line, the
// version named in the head, the Table's open row and empty-type list, the
// Grid's open cell and its panel, the keyboard, and the failures reading
// again cannot change.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@operator-ui/console/views/graph/typeMapDraw", () => ({
  createTypeMap: () => ({
    setSelection: () => {}, setHidden: () => {}, setText: () => {}, zoomBy: () => {}, fit: () => {},
    focusNode: () => {}, transform: () => ({ x: 0, y: 0, k: 1 }), destroy: () => {},
  }),
}));
vi.mock("@operator-ui/console/views/graph/recordTreeDraw", () => ({
  createRecordTree: (input: { onStatus: (s: string) => void }) => {
    input.onStatus("drawn");
    return { setSelection: () => {}, zoomBy: () => {}, fit: () => {}, destroy: () => {} };
  },
}));

import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import { ConsoleGraphRoute } from "@operator-ui/console/views/graph/Graph";
import { type ApiClient, ApiRequestError } from "@operator-ui/shared/api/apiClient";

const NODE_DETAIL = "file:okg:src/okg/substrate/console_views/node_detail.py";

let host: HTMLDivElement;
let root: Root;
let current: { fixture: string; detail: string | null; api: ApiClient };

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  window.location.hash = "";
  Object.defineProperty(window, "innerWidth", { configurable: true, value: 1440 });
  (globalThis as unknown as { CSS: { escape: (s: string) => string } }).CSS ??= { escape: (s: string) => s.replace(/["\\]/g, "\\$&") };
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
});

/** The okg-workspace producers the graph fixture's ids belong to, as the
 *  producers read lists them. */
const PRODUCERS = {
  payload: {
    deployment: "okg-workspace", generation_id: null, producer_count: 4,
    producers: [
      { name: "code-structure", ownership_id: "okg-workspace.code-structure", kind: "source", configured: true },
      { name: "pact", ownership_id: "okg-workspace.pact", kind: "source", configured: true },
      { name: "code_reference", ownership_id: "_enricher:code_reference", kind: "enricher", configured: true },
      { name: "idle", ownership_id: "okg-workspace.idle", kind: "source", configured: true },
    ],
  },
};

/** The fixture's client, with some routes answered or refused here. */
function client(fixture: string, overrides: Record<string, unknown | (() => never)> = {}): ApiClient {
  const f = fixtureByName(fixture);
  if (!f) throw new Error(`no fixture ${fixture}`);
  const base = createFixtureApiClient(f);
  return {
    ...base,
    async get<T>(route: `/viz/api/${string}`, params?: Record<string, string | number | boolean | null | undefined>, opts?: { signal?: AbortSignal }): Promise<T> {
      if (route in overrides) {
        const o = overrides[route];
        if (typeof o === "function") return (o as () => never)();
        return o as T;
      }
      return base.get<T>(route, params, opts);
    },
  } as ApiClient;
}

const hashParams = (): Record<string, string> => {
  const hash = window.location.hash.replace(/^#/, "");
  const at = hash.indexOf("?");
  const out = Object.fromEntries(new URLSearchParams(at < 0 ? "" : hash.slice(at + 1)));
  delete out.fixture;
  return out;
};

async function flush(): Promise<void> {
  for (let i = 0; i < 6; i += 1) await act(async () => { await Promise.resolve(); });
}

async function mount(
  fixture: string, params: Record<string, string>, detail: string | null = null, api?: ApiClient,
): Promise<void> {
  current = { fixture, detail, api: api ?? client(fixture, { [PRODUCERS_ROUTE]: PRODUCERS }) };
  await act(async () => {
    root.render(
      <ConsoleGraphRoute api={current.api} link={designLink(fixture)} params={params} detail={detail} readingVersion={null} nowMs={0} />,
    );
  });
  await flush();
}

/** Render again from the URL, as the route does on a hash change. */
async function follow(): Promise<void> {
  await act(async () => {
    root.render(
      <ConsoleGraphRoute api={current.api} link={designLink(current.fixture)} params={hashParams()} detail={current.detail} readingVersion={null} nowMs={0} />,
    );
  });
  await flush();
}

const q = (selector: string) => host.querySelector(selector);
const qa = (selector: string) => [...host.querySelectorAll(selector)];
const key = (el: Element, k: string) => act(async () => {
  el.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true }));
});

describe("the Graph shell", () => {
  it("writes the drawing it opened on into the URL: the map on a wide window", async () => {
    await mount("workspace-mixed", {});
    expect(hashParams().view).toBe("map");
  });

  it("opens on the Table on a phone, and says so in the URL", async () => {
    Object.defineProperty(window, "innerWidth", { configurable: true, value: 390 });
    await mount("workspace-mixed", {});
    expect(hashParams().view).toBe("table");
    expect(q('[data-testid="graph-table"]')).not.toBeNull();
  });

  it("switches drawing with links, and from a record page goes to the whole graph without the record", async () => {
    await mount("workspace-mixed", { id: NODE_DETAIL }, "record");
    const hrefs = qa('[aria-label="Drawing"] a').map((a) => a.getAttribute("href"));
    expect(hrefs).toEqual([null, "#graph?fixture=workspace-mixed&view=table", "#graph?fixture=workspace-mixed&view=grid"]);
    expect(q('[data-testid="graph-mode-map"]')?.getAttribute("aria-current")).toBe("page");
  });

  it("names the version it shows in the head", async () => {
    await mount("workspace-mixed", { view: "table" });
    expect(q('[data-testid="graph-version"]')?.textContent).toMatch(/^Version [0-9a-f]{12}/);
  });

  it("names every parameter it could not honour on one line", async () => {
    await mount("workspace-mixed", { view: "tabel", sort: "new", version: "postgres://u:secret@h/db" });
    const line = q('[data-testid="graph-ignored-params"]')?.textContent ?? "";
    expect(line).toContain("view=tabel");
    expect(line).toContain("sort=new");
    expect(line).toContain("version, which this page cannot honour: it is not a published version id");
    expect(line).not.toContain("secret");
  });

  it("says nothing is published in one sentence, with no Read again, on every drawing", async () => {
    for (const view of ["table", "grid"]) {
      await mount("empty", { view });
      expect(q('[data-testid="graph-settled-nothing_published"]')?.textContent).toContain("No version has been published");
      expect(q('[data-testid="read-again"]')).toBeNull();
    }
  });
});

describe("the Table", () => {
  it("lists the types with links in, out and within, and the sources by their Sources names", async () => {
    await mount("workspace-mixed", { view: "table" });
    expect(qa(".tt.types tr.row")).toHaveLength(25);
    const head = qa(".tt.types th").map((th) => th.textContent);
    expect(head).toEqual(["Type", "Records", "Links in", "Links out", "Within type", "Made by", "Declared in"]);
    const symbol = qa('tr.row[data-t="code_symbol"] td').map((td) => td.textContent);
    expect(symbol.slice(1, 5)).toEqual(["74,681", "262,789", "none", "148,652"]);
    const pact = q('.tt.srcs a.pn[href="#sources/pact?fixture=workspace-mixed"]');
    expect(pact?.textContent).toBe("pact");
    const coverage = q('[data-testid="graph-sources-coverage"]')?.textContent ?? "";
    expect(coverage).toContain("2 of 3 sources made records in this version; 1 of 1 enrichers added relationships.");
    // The fixture's other producers are not in this producers read: named by id, and said so.
    expect(coverage).toContain("8 producers here are not in the producers list and are named by their ids.");
    expect(q(".tt.srcs code.pn")?.textContent).toMatch(/^okg-workspace\./);
    expect(q(".tt.enrs")?.textContent).toContain("code_reference");
    expect(q('[data-testid="graph-table-footer"]')?.textContent).toContain("822,142 records, 1,571,775 relationships");
  });

  it("opens a row from the URL, with its type to copy, its Explore link and its system fields unflagged", async () => {
    await mount("workspace-mixed", { view: "table", open: "acceptance_criterion" });
    const detail = q("tr.detail");
    expect(detail?.textContent).toContain("1,000 of 5,091 records sampled");
    expect(detail?.querySelector('a[href="#explore?fixture=workspace-mixed&type=acceptance_criterion"]')?.textContent)
      .toBe("Open 5,091 acceptance_criterion records in Explore");
    expect(detail?.querySelector('tr[data-origin="system"]')?.textContent).toContain("nomos_source_lineage");
    expect(detail?.querySelector('tr[data-origin="system"] .tag')).toBeNull();
  });

  it("puts the open row and the empty-type list in the URL", async () => {
    await mount("workspace-mixed", { view: "table" });
    await act(async () => { (q('tr.row[data-t="git_commit"]') as HTMLElement).click(); });
    expect(hashParams()).toMatchObject({ view: "table", open: "git_commit" });
    await follow();
    expect(q("tr.detail")).not.toBeNull();
    await act(async () => { (q('.tnote input[type="checkbox"]') as HTMLInputElement).click(); });
    expect(hashParams()).toMatchObject({ open: "git_commit", empty: "1" });
    await follow();
    expect(q(".tt.types tr.sec")?.textContent).toBe("Declared, no records");
    expect(qa(".tt.types tr.row")).toHaveLength(25 + 115);
  });

  it("is one tab stop; the arrows walk the rows and Escape closes the open one", async () => {
    await mount("workspace-mixed", { view: "table", open: "code_symbol" });
    expect(qa('.tt.types tr.row[tabindex="0"]').map((r) => r.getAttribute("data-t"))).toEqual(["code_symbol"]);
    const row = q('tr.row[data-t="code_symbol"]') as HTMLElement;
    row.focus();
    await key(row, "ArrowDown");
    expect(document.activeElement?.getAttribute("data-t")).toBe("document_chunk");
    await key(document.activeElement as Element, "Escape");
    expect(hashParams().open).toBeUndefined();
  });

  it("says it is reading the declared types, then hides nothing it has", async () => {
    const stalled = client("workspace-mixed", {
      [PRODUCERS_ROUTE]: PRODUCERS,
      "/viz/api/console/graph/ontology": () => new Promise<never>(() => {}) as never,
    });
    await mount("workspace-mixed", { view: "table" }, null, stalled);
    expect(q('[data-testid="graph-ontology-reading"]')?.textContent).toContain("Reading the declared types");
    expect(qa(".tt.types th").map((th) => th.textContent)).not.toContain("Declared in");
  });
});

describe("the Grid", () => {
  it("draws two grids, one tab stop each, and cuts the source-by-type grid the Table already says", async () => {
    await mount("workspace-mixed", { view: "grid" });
    expect(qa("table.grid").map((t) => t.getAttribute("data-grid"))).toEqual(["tt", "ss"]);
    for (const grid of qa("table.grid")) expect(grid.querySelectorAll('td[tabindex="0"]')).toHaveLength(1);
    const total = q('table[data-grid="tt"] tfoot td.all');
    expect(total?.getAttribute("title")).toBe("1,571,775");
  });

  it("opens a cell from the URL in a panel beside it that says what it counts and links out", async () => {
    await mount("workspace-mixed", { view: "grid", cell: "tt:code_reference|code_symbol" });
    const panel = document.querySelector('[data-testid="graph-cellinfo-tt"]');
    expect(panel?.textContent).toContain("references 187,897");
    expect(panel?.querySelector('a[href="#explore?fixture=workspace-mixed&type=code_reference"]')).not.toBeNull();
    expect(q('td[aria-expanded="true"]')?.getAttribute("data-r")).toBe("code_reference");
  });

  it("puts the open cell in the URL, and Escape closes it", async () => {
    await mount("workspace-mixed", { view: "grid" });
    await act(async () => { (q('table[data-grid="tt"] td[data-r="code_reference"][data-c="code_symbol"]') as HTMLElement).click(); });
    expect(hashParams().cell).toBe("tt:code_reference|code_symbol");
    await follow();
    const panel = document.querySelector('[data-testid="graph-cellinfo-tt"]') as HTMLElement;
    await key(panel, "Escape");
    expect(hashParams().cell).toBeUndefined();
  });

  it("walks the cells with the arrow keys", async () => {
    await mount("workspace-mixed", { view: "grid" });
    const first = q('table[data-grid="tt"] td[tabindex="0"]') as HTMLElement;
    first.focus();
    const [i, j] = [first.getAttribute("data-i"), first.getAttribute("data-j")];
    await key(first, "ArrowRight");
    expect(document.activeElement?.getAttribute("data-i")).toBe(i);
    expect(document.activeElement?.getAttribute("data-j")).toBe(String(Number(j) + 1));
    await key(document.activeElement as Element, "ArrowDown");
    expect(document.activeElement?.getAttribute("data-i")).toBe(String(Number(i) + 1));
  });

  it("says in place of the source grid that its computation timed out, and draws the type grid", async () => {
    await mount("scale-200", { view: "grid" });
    expect(qa("table.grid").map((t) => t.getAttribute("data-grid"))).toEqual(["tt"]);
    expect(q('[data-testid="graph-source-links-timeout"]')?.textContent)
      .toBe("This grid is computed when it is asked for, and the computation stopped at its 18 s limit. Nothing is missing from the graph.");
    const failed = q('[data-testid="graph-source-links"] [role="alert"]')?.textContent ?? "";
    expect(failed).toContain("a quieter database may finish in time");
    expect(failed).not.toContain("maintained tables exist");
  });

  it("names a policy refusal of the source links plainly, with no Read again", async () => {
    const refused = client("workspace-mixed", {
      [PRODUCERS_ROUTE]: PRODUCERS,
      "/viz/api/console/graph/source-links": () => {
        throw new ApiRequestError({
          route: "/viz/api/console/graph/source-links", status: 403,
          responseText: JSON.stringify({ detail: { reason: "policy_denied" } }),
        });
      },
    });
    await mount("workspace-mixed", { view: "grid" }, null, refused);
    expect(q('[data-testid="graph-settled-refused"]')?.textContent).toContain("was refused by the read policy (policy_denied)");
    expect(q('[data-testid="graph-source-links"] [data-testid="read-again"]')).toBeNull();
  });
});
