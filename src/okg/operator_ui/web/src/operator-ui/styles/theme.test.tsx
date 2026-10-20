import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  applyTheme,
  DEFAULT_FIRING_FOR_POLARITY,
  DEFAULT_THEME,
  getStoredTheme,
  initTheme,
  readStoredTheme,
  readTheme,
  setFiring,
  subscribeTheme,
  THEME_IS_DARK,
  THEME_LABELS,
  THEMES,
} from "@operator-ui/styles/theme";

const KEY = "okg-operator-theme";

// The operator-ui suite runs in the `node` vitest environment (no jsdom), so
// stub the two browser globals theme.ts touches: window.localStorage and
// document.documentElement.
function makeStorage() {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => (map.has(k) ? map.get(k)! : null),
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  };
}

function makeElement() {
  const attrs: Record<string, string> = {};
  return {
    setAttribute: (k: string, v: string) => void (attrs[k] = v),
    removeAttribute: (k: string) => void delete attrs[k],
    getAttribute: (k: string) => attrs[k] ?? null,
  };
}

let storage: ReturnType<typeof makeStorage>;
let docEl: ReturnType<typeof makeElement>;

beforeEach(() => {
  storage = makeStorage();
  docEl = makeElement();
  vi.stubGlobal("window", { localStorage: storage });
  vi.stubGlobal("document", { documentElement: docEl });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("theme", () => {
  it("defaults to Cyclades (cool light) when nothing is stored", () => {
    expect(DEFAULT_THEME).toBe("cyclades");
    expect(getStoredTheme()).toBe("cyclades");
  });

  it("returns a valid persisted choice", () => {
    storage.setItem(KEY, "melanomorpha");
    expect(getStoredTheme()).toBe("melanomorpha");
  });

  it("falls back to the default for an unrecognized stored value", () => {
    storage.setItem(KEY, "midnight");
    expect(getStoredTheme()).toBe(DEFAULT_THEME);
  });

  it("applyTheme sets the document attribute and persists both firings", () => {
    applyTheme("melanomorpha");
    expect(docEl.getAttribute("data-theme")).toBe("melanomorpha");
    expect(storage.getItem(KEY)).toBe("melanomorpha");

    applyTheme("lyceaum");
    expect(docEl.getAttribute("data-theme")).toBe("lyceaum");
    expect(storage.getItem(KEY)).toBe("lyceaum");
  });

  it("initTheme applies the stored theme to the document root", () => {
    storage.setItem(KEY, "melanomorpha");
    expect(initTheme()).toBe("melanomorpha");
    expect(docEl.getAttribute("data-theme")).toBe("melanomorpha");
  });

  it("readStoredTheme distinguishes 'unset' from 'chose the default'", () => {
    // The distinction the deployment base-firing rule rests on: an unset
    // browser may be moved onto a deployment's polarity-matched firing, a
    // browser where Cyclades was chosen on purpose may not. Collapsing the
    // two would make the base never apply.
    expect(readStoredTheme()).toBeNull();
    expect(getStoredTheme()).toBe(DEFAULT_THEME);

    storage.setItem(KEY, DEFAULT_THEME);
    expect(readStoredTheme()).toBe(DEFAULT_THEME);
  });

  it("readStoredTheme reports an unrecognized stored value as no choice", () => {
    storage.setItem(KEY, "midnight");
    expect(readStoredTheme()).toBeNull();
  });

  it("readStoredTheme is safe when storage throws", () => {
    vi.stubGlobal("window", {
      get localStorage(): Storage {
        throw new Error("blocked");
      },
    });
    expect(readStoredTheme()).toBeNull();
  });

  it("setFiring renders a firing WITHOUT recording it as the operator's choice", () => {
    // A deployment-supplied base must not become a stored preference: it
    // would follow the operator to every other deployment they open in this
    // browser, and the console would have chosen for them.
    setFiring("hephaestus");

    expect(docEl.getAttribute("data-theme")).toBe("hephaestus");
    expect(storage.getItem(KEY)).toBeNull();
    expect(readStoredTheme()).toBeNull();
  });

  it("setFiring leaves the attribute untouched when it already matches", () => {
    // Re-asserting the same value would fire the data-theme MutationObserver
    // behind useTheme, which re-runs the effect that called setFiring.
    setFiring("hephaestus");
    let writes = 0;
    docEl.setAttribute = (() => {
      writes += 1;
    }) as typeof docEl.setAttribute;

    setFiring("hephaestus");

    expect(writes).toBe(0);
  });

  it("pairs each polarity with a built-in firing that actually has it", () => {
    expect(DEFAULT_FIRING_FOR_POLARITY.light).toBe(DEFAULT_THEME);
    expect(DEFAULT_FIRING_FOR_POLARITY.dark).toBe("hephaestus");
    expect(THEME_IS_DARK[DEFAULT_FIRING_FOR_POLARITY.light]).toBe(false);
    expect(THEME_IS_DARK[DEFAULT_FIRING_FOR_POLARITY.dark]).toBe(true);
  });

  it("getStoredTheme is safe when storage throws", () => {
    vi.stubGlobal("window", {
      get localStorage(): Storage {
        throw new Error("blocked");
      },
    });
    expect(getStoredTheme()).toBe(DEFAULT_THEME);
  });

  it("readTheme reflects the data-theme attribute, defaulting when unset/invalid", () => {
    expect(readTheme()).toBe(DEFAULT_THEME);
    docEl.setAttribute("data-theme", "melanomorpha");
    expect(readTheme()).toBe("melanomorpha");
    docEl.setAttribute("data-theme", "not-a-theme");
    expect(readTheme()).toBe(DEFAULT_THEME);
  });

  it("subscribeTheme is a safe no-op without a MutationObserver (node)", () => {
    // The node vitest env has no MutationObserver, so subscribe returns a no-op
    // unsubscribe rather than throwing.
    const unsubscribe = subscribeTheme(() => {});
    expect(typeof unsubscribe).toBe("function");
    expect(() => unsubscribe()).not.toThrow();
  });

  it("subscribeTheme observes data-theme, fires onChange, and disconnects", () => {
    // Exercise the real observer branch (not the node no-op) with a stubbed
    // MutationObserver, so the recolor wiring is regression-guarded: watching
    // the wrong attribute or dropping the disconnect would fail here.
    const observeCalls: Array<{ target: unknown; options: unknown }> = [];
    const disconnect = vi.fn();
    let captured: (() => void) | null = null;
    class FakeObserver {
      constructor(cb: () => void) {
        captured = cb;
      }
      observe(target: unknown, options: unknown) {
        observeCalls.push({ target, options });
      }
      disconnect() {
        disconnect();
      }
    }
    vi.stubGlobal("MutationObserver", FakeObserver);

    const onChange = vi.fn();
    const unsubscribe = subscribeTheme(onChange);

    // Observes the document root, filtered to just the data-theme attribute.
    expect(observeCalls).toHaveLength(1);
    expect(observeCalls[0].target).toBe(docEl);
    expect(observeCalls[0].options).toEqual({
      attributes: true,
      attributeFilter: ["data-theme"],
    });

    // A mutation notification propagates to the subscriber (drives the recolor).
    expect(typeof captured).toBe("function");
    captured!();
    expect(onChange).toHaveBeenCalledTimes(1);

    // Unsubscribe tears the observer down — guards against an observer leak.
    unsubscribe();
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it("exposes a label for every theme", () => {
    for (const theme of THEMES) {
      expect(THEME_LABELS[theme]).toBeTruthy();
    }
  });

  it("offers the four firings and round-trips each through storage", () => {
    expect([...THEMES]).toEqual([
      "lyceaum",
      "cyclades",
      "melanomorpha",
      "hephaestus",
    ]);
    for (const theme of THEMES) {
      storage.setItem(KEY, theme);
      expect(getStoredTheme()).toBe(theme);
    }
  });

  it("classifies light vs dark firings", () => {
    expect(THEME_IS_DARK.lyceaum).toBe(false);
    expect(THEME_IS_DARK.cyclades).toBe(false);
    expect(THEME_IS_DARK.melanomorpha).toBe(true);
    expect(THEME_IS_DARK.hephaestus).toBe(true);
  });
});
