// @vitest-environment jsdom
// The top bar's CHAT control: where it sits, and what it says in each of the
// four states the deployments read can leave it in.
//
// These cannot be asserted from static markup, which is what most of this
// suite renders: the bar's reads run in an effect, so "the read has not
// answered yet" and "the read was refused" only exist in a DOM with a clock.
//
// What is guarded here, all of it failure-shaped:
//
//   1. the control is in the RIGHT control group, after the theme picker, and
//      not in the left view nav, which is where it used to read as a fifth
//      view (the approved reference lists the right side as deployment,
//      version, theme, Open chat);
//   2. each of the four states says which one it is. Three of them are
//      different kinds of absence, and the bar used to render NOTHING for all
//      three -- one silence standing for "declares no chat site", "nobody
//      identified this deployment" and "the read has not answered";
//   3. only `declared` is a link, and no state prints a host.
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  CHAT_LABEL_DECLARED, CHAT_LABEL_UNDECLARED, CHAT_LABEL_UNRESOLVED,
  ConsoleTopBar, DEPLOYMENTS_ROUTE,
} from "@operator-ui/console/views/TopBar";
import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";

const DECLARED = "https://chat.okg.example.org";

const VERSIONS = {
  payload: {
    versions: [{ generation_id: "abcdef012345", published_at: "2026-09-21T02:48:09Z" }],
    latest: "abcdef012345",
  },
};

type Entry = { name: string; chat_site_base: string | null; is_current: boolean };

/** A client answering the bar's two reads, with the deployments read under
 *  the test's control: a list, a never-settling promise, or a refusal. */
function barClient(deployments: Entry[] | "stall" | "refuse"): ApiClient {
  return {
    get: <T,>(route: string) => {
      if (route !== DEPLOYMENTS_ROUTE) return Promise.resolve(VERSIONS as T);
      if (deployments === "stall") return new Promise<T>(() => {});
      if (deployments === "refuse") {
        return Promise.reject(new ApiRequestError({
          route, status: 403,
          responseText: JSON.stringify({
            detail: { reason: "policy_denied", error: "This read is withheld." },
          }),
        })) as Promise<T>;
      }
      return Promise.resolve({ entries: deployments } as T);
    },
  } as unknown as ApiClient;
}

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  window.location.hash = "";
  // The bar asks the window whether it is narrow; jsdom has no matchMedia.
  vi.stubGlobal("matchMedia", (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
  vi.unstubAllGlobals();
});

async function mountBar(deployments: Entry[] | "stall" | "refuse"): Promise<void> {
  await act(async () => {
    root.render(
      <ConsoleTopBar
        api={barClient(deployments)}
        view="Home"
        link={(route) => `#console/${route}`}
        search={null}
      />,
    );
  });
  await act(async () => { await Promise.resolve(); });
  await act(async () => { await Promise.resolve(); });
}

function chat(): HTMLElement | null {
  return host.querySelector<HTMLElement>('[data-testid="console-bar-chat"]');
}

describe("the bar's chat control says which of the four states it is in", () => {
  it("links out, labelled Open chat, when the current deployment declares one", async () => {
    await mountBar([{ name: "okg-workspace", chat_site_base: DECLARED, is_current: true }]);

    const control = chat();
    expect(control).not.toBeNull();
    expect(control?.tagName).toBe("A");
    expect(control?.dataset.chatState).toBe("declared");
    expect(control?.getAttribute("href")).toBe(DECLARED);
    expect(control?.getAttribute("target")).toBe("_blank");
    expect(control?.getAttribute("rel")).toContain("noopener");
    expect(control?.textContent).toContain(CHAT_LABEL_DECLARED);
    // The href is the declared URL; the WORDS never print a host.
    expect(control?.textContent).not.toContain("http");
    expect(control?.textContent).not.toContain("chat.okg.example.org");
  });

  it("states the absence, as text and not a link, when none is declared", async () => {
    await mountBar([{ name: "archiv3", chat_site_base: null, is_current: true }]);

    const control = chat();
    expect(control?.dataset.chatState).toBe("undeclared");
    expect(control?.tagName).toBe("SPAN");
    expect(control?.textContent).toBe(CHAT_LABEL_UNDECLARED);
    // Not a control of any kind: there is no action to offer.
    expect(control?.querySelector("button")).toBeNull();
    expect(control?.getAttribute("href")).toBeNull();
    // No guessed host, and not the OTHER absence's words.
    expect(control?.textContent).not.toContain("http");
    expect(host.textContent).not.toContain(CHAT_LABEL_UNRESOLVED);
  });

  it("says UNKNOWN, not undeclared, when no entry came back current", async () => {
    // No manifest was read at all, so "declares no chat site" would be a claim
    // about a file nobody opened.
    await mountBar([
      { name: "a", chat_site_base: null, is_current: false },
      { name: "b", chat_site_base: DECLARED, is_current: false },
    ]);

    const control = chat();
    expect(control?.dataset.chatState).toBe("unresolved");
    expect(control?.textContent).toBe(CHAT_LABEL_UNRESOLVED);
    expect(host.textContent).not.toContain(CHAT_LABEL_UNDECLARED);
    // A non-current entry's declared site must never become this bar's link.
    expect(host.innerHTML).not.toContain(DECLARED);
  });

  it("says UNKNOWN when the deployments read was refused, and names the failure", async () => {
    // A refusal is an ANSWER, not a pending read -- but it read no manifest,
    // so the chat state is the unresolved one, and the refusal itself is
    // named in the bar's own read-failure line.
    await mountBar("refuse");

    expect(chat()?.dataset.chatState).toBe("unresolved");
    expect(host.querySelector('[data-testid="console-bar-read-failed"]')?.textContent)
      .toContain(DEPLOYMENTS_ROUTE);
  });

  it("prints nothing at all while the deployments read is still outstanding", async () => {
    await mountBar("stall");

    expect(chat()).toBeNull();
    // Neither absence is claimed one moment before the read answers.
    expect(host.textContent).not.toContain(CHAT_LABEL_UNDECLARED);
    expect(host.textContent).not.toContain(CHAT_LABEL_UNRESOLVED);
    expect(host.textContent).not.toContain(CHAT_LABEL_DECLARED);
  });
});

describe("the chat control's place in the bar", () => {
  it("sits in the right control group, after the theme picker", async () => {
    await mountBar([{ name: "okg-workspace", chat_site_base: DECLARED, is_current: true }]);

    const controls = host.querySelector(".okg-console-bar__controls");
    const control = chat();
    expect(controls).not.toBeNull();
    expect(controls?.contains(control as Node)).toBe(true);

    // The reference's right-hand order: deployment, version, theme, Open chat.
    const order = [...(controls?.children ?? [])].map((node) => {
      if (node.querySelector(".okg-console-bar__select--firing")) return "theme";
      if (node.matches('[data-testid="console-bar-chat"]')) return "chat";
      if (node.matches('[data-testid="console-bar-deployment"]')) return "deployment";
      if (node.matches('[data-testid="console-bar-version"]')) return "version";
      return "other";
    });
    expect(order).toEqual(["deployment", "version", "theme", "chat"]);
  });

  it("is NOT one of the view links", async () => {
    await mountBar([{ name: "okg-workspace", chat_site_base: DECLARED, is_current: true }]);

    const nav = host.querySelector(".okg-console-bar__nav");
    expect(nav).not.toBeNull();
    expect(nav?.contains(chat() as Node)).toBe(false);
    // The five views and nothing else.
    expect([...(nav?.querySelectorAll("a, span") ?? [])].map((node) => node.textContent))
      .toEqual(["Home", "Updates", "Sources", "Explore", "Graph"]);
  });
});

describe("the bar on Explore's landing, where the page shows its own search box", () => {
  // Audit B1: the bar's box was removed with `hidden`, which took a cell out
  // of the three-column grid. The controls then fell into the 400 px middle
  // column and spilled left over the nav at 1440.
  it("keeps three cells in the row, the search cell hidden but present", async () => {
    await act(async () => {
      root.render(
        <ConsoleTopBar
          api={barClient([{ name: "archiv3", chat_site_base: null, is_current: true }])}
          view="Explore"
          link={(route) => `#console/${route}`}
          search={null}
          hideSearch
        />,
      );
    });
    const row = host.querySelector<HTMLElement>(".okg-console-bar__row");
    const cells = Array.from(row?.children ?? []).map((cell) => cell.className);
    const left = cells.indexOf("okg-console-bar__left");
    const search = cells.indexOf("okg-console-bar__search");
    const controls = cells.indexOf("okg-console-bar__controls");
    expect([left, search, controls].every((index) => index >= 0)).toBe(true);
    expect(left < search && search < controls).toBe(true);

    const form = host.querySelector<HTMLElement>('[data-testid="console-search"]');
    expect(form?.hidden).toBe(false);
    expect(form?.dataset.hidden).toBe("true");
    // Out of the tab order and the accessibility tree while hidden.
    expect(form?.hasAttribute("inert")).toBe(true);
  });

  it("shows the box, with no hidden marker, on every other view", async () => {
    await mountBar([{ name: "archiv3", chat_site_base: null, is_current: true }]);
    const form = host.querySelector<HTMLElement>('[data-testid="console-search"]');
    expect(form?.dataset.hidden).toBeUndefined();
    expect(form?.hasAttribute("inert")).toBe(false);
  });
});
