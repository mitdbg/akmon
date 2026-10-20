// @vitest-environment jsdom
//
// The top bar's box on Graph: matching records listed as the reader types, a
// pick opening the record's neighbourhood, a failed search said in the list,
// and the box on every other view still opening Explore.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createFixtureApiClient } from "@operator-ui/design-harness/fixtureClient";
import { fixtureByName } from "@operator-ui/design-harness/fixtures";
import { designLink } from "@operator-ui/design-harness/DesignHarness";
import { PICKER_DEBOUNCE_MS } from "@operator-ui/console/components/RecordPicker";
import { ConsoleTopBar } from "@operator-ui/console/views/TopBar";

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  window.location.hash = "";
  vi.stubGlobal("matchMedia", (query: string) => ({ matches: false, media: query, addEventListener: () => {}, removeEventListener: () => {} }));
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
  vi.unstubAllGlobals();
});

const api = createFixtureApiClient(fixtureByName("workspace-mixed")!);

async function mountBar(pickRecord: ((id: string) => void) | null): Promise<HTMLInputElement> {
  await act(async () => {
    root.render(<ConsoleTopBar api={api} view={pickRecord ? "Graph" : "Home"} link={designLink("workspace-mixed")} pickRecord={pickRecord} />);
  });
  for (let i = 0; i < 3; i += 1) await act(async () => { await Promise.resolve(); });
  return host.querySelector('[data-testid="console-search"] input') as HTMLInputElement;
}

async function type(input: HTMLInputElement, text: string): Promise<void> {
  await act(async () => {
    input.focus();
    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
    setter?.call(input, text);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await act(async () => { await new Promise((resolve) => setTimeout(resolve, PICKER_DEBOUNCE_MS + 20)); });
  for (let i = 0; i < 3; i += 1) await act(async () => { await Promise.resolve(); });
}

describe("the top bar's box on Graph", () => {
  it("lists the matching records and opens the one picked", async () => {
    const picked: string[] = [];
    const input = await mountBar((id) => picked.push(id));
    expect(input.getAttribute("role")).toBe("combobox");
    await type(input, "node_detail");
    const options = [...host.querySelectorAll('[data-testid="console-record-picker"] li[id]')];
    expect(options.length).toBeGreaterThan(1);
    expect(options[0].textContent).toContain("src/okg/substrate/console_views/node_detail.py");
    expect(input.getAttribute("aria-activedescendant")).toBe(options[0].id);
    await act(async () => { input.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true })); });
    await act(async () => { input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true })); });
    expect(picked).toHaveLength(1);
    expect(picked[0]).not.toBe("file:okg:src/okg/substrate/console_views/node_detail.py");
    expect(window.location.hash).toBe("");
    expect(input.value).toBe("");
  });

  it("says in the list when the search could not be read", async () => {
    const input = await mountBar(() => {});
    await type(input, "zzz-not-a-defined-search");
    const failed = host.querySelector('[data-testid="console-record-picker"]')?.textContent ?? "";
    expect(failed).toContain("The search could not be read.");
    expect(failed).toContain("/viz/api/console/records/search");
  });

  it("honours Enter pressed before the search answered, with the first match", async () => {
    const picked: string[] = [];
    const input = await mountBar((id) => picked.push(id));
    await act(async () => {
      input.focus();
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
      setter?.call(input, "node_detail");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    });
    // Inside the debounce: nothing has been read yet.
    await act(async () => { input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true })); });
    expect(picked).toHaveLength(0);
    await act(async () => { await new Promise((resolve) => setTimeout(resolve, PICKER_DEBOUNCE_MS + 20)); });
    for (let i = 0; i < 3; i += 1) await act(async () => { await Promise.resolve(); });
    expect(picked).toEqual(["file:okg:src/okg/substrate/console_views/node_detail.py"]);
    expect(input.value).toBe("");
  });

  it("searches the version the page reads, not the latest", async () => {
    const asked: unknown[] = [];
    const spy = { ...api, get: (route: string, params?: Record<string, unknown>, opts?: unknown) => {
      if (route.endsWith("/records/search")) asked.push(params?.version);
      return (api.get as (r: string, p?: unknown, o?: unknown) => Promise<unknown>)(route, params, opts);
    } } as typeof api;
    await act(async () => {
      root.render(<ConsoleTopBar api={spy} view="Graph" link={designLink("workspace-mixed")} version="okg:older:gen:1" pickRecord={() => {}} />);
    });
    for (let i = 0; i < 3; i += 1) await act(async () => { await Promise.resolve(); });
    await type(host.querySelector('[data-testid="console-search"] input') as HTMLInputElement, "node");
    expect(asked).toEqual(["okg:older:gen:1"]);
  });

  it("searches the version the page shows when the URL names none (the read API's pinned one, not the latest)", async () => {
    const asked: unknown[] = [];
    const spy = { ...api, get: (route: string, params?: Record<string, unknown>, opts?: unknown) => {
      if (route.endsWith("/records/search")) asked.push(params?.version);
      return (api.get as (r: string, p?: unknown, o?: unknown) => Promise<unknown>)(route, params, opts);
    } } as typeof api;
    await act(async () => {
      root.render(<ConsoleTopBar api={spy} view="Graph" link={designLink("workspace-mixed")} pickVersion="okg:pinned:gen:7" pickRecord={() => {}} />);
    });
    for (let i = 0; i < 3; i += 1) await act(async () => { await Promise.resolve(); });
    await type(host.querySelector('[data-testid="console-search"] input') as HTMLInputElement, "node");
    expect(asked).toEqual(["okg:pinned:gen:7"]);
  });

  it("opens Explore's results on Enter everywhere else", async () => {
    const input = await mountBar(null);
    expect(input.getAttribute("role")).toBeNull();
    await type(input, "handbook");
    expect(host.querySelector('[data-testid="console-record-picker"]')).toBeNull();
    await act(async () => { input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true })); });
    expect(window.location.hash).toBe("#explore?fixture=workspace-mixed&q=handbook");
  });
});
