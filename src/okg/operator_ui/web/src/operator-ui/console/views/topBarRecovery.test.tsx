// @vitest-environment jsdom
// The top bar after a read failure, and the version picker's words.
//
// PACT console-archiv3-view-fixes. The Archiv3 review of 2026-09-22 found the
// bar read `/viz/api/deployments` and `/viz/api/console/versions` ONCE per
// client: after a 502 it stayed "not read", with an empty picker, "Chat site
// unknown" and two failure lines, long after the read API was answering and
// the page body under it had reloaded. It also found the deployments route
// requested twice per load -- once by the shell, once by the bar -- and a
// picker whose three options all read "· 3 d ago".
import { act, type ComponentProps, type ReactElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { BAR_VERSIONS_ROUTE, ConsoleTopBar, DEPLOYMENTS_ROUTE } from "@operator-ui/console/views/TopBar";
import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";
import { ChatSiteContext, type ChatSiteState } from "@operator-ui/shared/hooks/chatSiteContext";

const VERSIONS = {
  payload: {
    versions: [
      { generation_id: "okg:cern-r3final:branch:default:gen:20260920T211645636603Z:efc517cac053", published_at: "2026-09-20T21:16:46.382690+00:00" },
      { generation_id: "okg:cern-r3final:branch:default:gen:20260920T211632661281Z:4a941be354ad", published_at: "2026-09-20T21:16:33.173487+00:00" },
      { generation_id: "okg:cern-r3final:branch:default:gen:20260920T211542342458Z:fdf6da58cfda", published_at: "2026-09-20T21:15:45.266012+00:00" },
    ],
    latest: "okg:cern-r3final:branch:default:gen:20260920T211645636603Z:efc517cac053",
  },
};
const DEPLOYMENTS = { entries: [{ name: "cern-r3final", chat_site_base: null, is_current: true }] };

/** A client that answers 502 until `up` is set, and counts every request. */
function flakyClient(): { api: ApiClient; calls: string[]; setUp: (value: boolean) => void } {
  const calls: string[] = [];
  let up = false;
  const api = {
    get: <T,>(route: string) => {
      calls.push(route);
      if (!up) return Promise.reject(new ApiRequestError({ route, status: 502, responseText: "" })) as Promise<T>;
      return Promise.resolve((route === DEPLOYMENTS_ROUTE ? DEPLOYMENTS : VERSIONS) as T);
    },
  } as unknown as ApiClient;
  return { api, calls, setUp: (value) => { up = value; } };
}

(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean }).IS_REACT_ACT_ENVIRONMENT = true;

let host: HTMLDivElement;
let root: Root;

beforeEach(() => {
  host = document.createElement("div");
  document.body.appendChild(host);
  root = createRoot(host);
  vi.stubGlobal("matchMedia", (query: string) => ({
    matches: false, media: query, addEventListener: () => {}, removeEventListener: () => {},
  }));
});

afterEach(() => {
  act(() => root.unmount());
  host.remove();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

async function settle(): Promise<void> {
  for (let i = 0; i < 4; i += 1) await act(async () => { await Promise.resolve(); });
}

async function render(element: ReactElement): Promise<void> {
  await act(async () => { root.render(element); });
  await settle();
}

function bar(api: ApiClient, extra: Partial<ComponentProps<typeof ConsoleTopBar>> = {}) {
  return (
    <ConsoleTopBar api={api} view="Home" link={(route) => `#console/${route}`} search={null} onVersion={() => {}} {...extra} />
  );
}

const failed = () => host.querySelector('[data-testid="console-bar-read-failed"]');
const deployment = () => host.querySelector('[data-testid="console-bar-deployment"]')?.textContent;

describe("the bar recovers once the read API answers again", () => {
  it("re-reads on a view change after a 502 and drops the failure lines", async () => {
    const client = flakyClient();
    await render(bar(client.api));
    expect(deployment()).toBe("not read");
    expect(failed()).not.toBeNull();

    client.setUp(true);
    await render(bar(client.api, { view: "Sources" }));
    expect(deployment()).toBe("cern-r3final");
    expect(failed()).toBeNull();
    expect(host.querySelector<HTMLSelectElement>('[data-testid="console-bar-version"]')?.options.length).toBe(3);
  });

  it("re-reads when the page's own read succeeds after a failure", async () => {
    const client = flakyClient();
    await render(bar(client.api, { pageReadKey: null }));
    expect(failed()).not.toBeNull();
    client.setUp(true);
    await render(bar(client.api, { pageReadKey: VERSIONS.payload.latest }));
    expect(failed()).toBeNull();
    expect(deployment()).toBe("cern-r3final");
  });

  it("retries a failed read on its own, a bounded number of times", async () => {
    vi.useFakeTimers();
    const client = flakyClient();
    await render(bar(client.api));
    const before = client.calls.length;
    for (let i = 0; i < 10; i += 1) {
      await act(async () => { vi.advanceTimersByTime(120_000); });
      await settle();
    }
    const retried = client.calls.length - before;
    // Some retries, and then it stops: a bar that polled a dead API forever
    // would be a load on it, not a recovery.
    expect(retried).toBeGreaterThan(0);
    expect(retried).toBeLessThanOrEqual(6);
    client.setUp(true);
    await render(bar(client.api, { view: "Updates" }));
    expect(failed()).toBeNull();
  });

  it("names the failure once, in words, not the route and the status three times", async () => {
    const client = flakyClient();
    await render(bar(client.api));
    const text = failed()?.textContent ?? "";
    expect(text).toContain("The data server is not answering (502).");
    expect(text.split("502").length - 1).toBe(2);
    expect(text.split(DEPLOYMENTS_ROUTE).length - 1).toBe(1);
    expect(text.split(BAR_VERSIONS_ROUTE).length - 1).toBe(1);
  });
});

describe("the deployments read is made once per load", () => {
  it("takes the name and the chat site from the shell when the shell already read them", async () => {
    const client = flakyClient();
    client.setUp(true);
    const shell: ChatSiteState = { status: "undeclared", href: null, deploymentName: "cern-r3final" };
    await render(
      <ChatSiteContext.Provider value={shell}>{bar(client.api)}</ChatSiteContext.Provider>,
    );
    expect(client.calls.filter((route) => route === DEPLOYMENTS_ROUTE)).toHaveLength(0);
    expect(deployment()).toBe("cern-r3final");
    expect(host.querySelector('[data-testid="console-bar-chat"]')?.getAttribute("data-chat-state")).toBe("undeclared");
  });
});

describe("the version picker", () => {
  it("prints each option's UTC publish time to the minute, not a shared relative age", async () => {
    const client = flakyClient();
    client.setUp(true);
    await render(bar(client.api, { view: "Explore", versionReading: true, version: VERSIONS.payload.versions[2].generation_id }));
    const options = [...host.querySelectorAll<HTMLOptionElement>('[data-testid="console-bar-version"] option')]
      .map((option) => option.textContent);
    expect(options).toEqual([
      "efc517cac053 · 2026-09-20 21:16 UTC",
      "4a941be354ad · 2026-09-20 21:16 UTC",
      "fdf6da58cfda · 2026-09-20 21:15 UTC",
    ]);
    expect(host.querySelector<HTMLSelectElement>('[data-testid="console-bar-version"]')?.value)
      .toBe(VERSIONS.payload.versions[2].generation_id);
  });

  it("shows latest on a view whose reads take no version, whatever was chosen", async () => {
    const client = flakyClient();
    client.setUp(true);
    await render(bar(client.api, {
      view: "Home", versionReading: false,
      version: VERSIONS.payload.versions[2].generation_id,
      readVersion: VERSIONS.payload.latest,
    }));
    const select = host.querySelector<HTMLSelectElement>('[data-testid="console-bar-version"]');
    expect(select?.selectedOptions[0]?.textContent).toBe("latest");
    expect(host.textContent).not.toContain("ago");
  });
});
