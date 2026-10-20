// Back and Forward return to where the reader was.
//
// The browser restores a scroll position the moment the hash changes, before
// the view's reads have answered, so the page is still short and the
// position is clamped: Back from a record to its results landed at 148 px
// instead of 435 (audit 2026-09-23). This keeps each hash's last position
// and, on a Back or Forward, puts it back once the page is tall enough -- or
// gives up after a few seconds, or as soon as the reader scrolls.
import { useEffect } from "react";

const GIVE_UP_MS = 4_000;

export function useScrollMemory(hash: string): void {
  useEffect(() => {
    if (typeof window === "undefined") return undefined;
    const positions = scrollPositions();
    const save = () => { positions.set(window.location.hash, window.scrollY); };
    window.addEventListener("scroll", save, { passive: true });
    return () => window.removeEventListener("scroll", save);
  }, []);

  useEffect(() => {
    if (typeof window === "undefined" || traversed === null) return undefined;
    // Read from the copy taken at `popstate`: the browser's own clamped
    // restore fires a scroll event that would overwrite the live entry.
    const target = traversed.get(hash);
    traversed = null;
    if (target === undefined || target <= 0) return undefined;
    let stopped = false;
    const stop = () => { stopped = true; };
    const place = () => {
      if (stopped) return;
      const room = document.documentElement.scrollHeight - window.innerHeight;
      if (room >= target) {
        window.scrollTo(0, target);
        stopped = true;
      }
    };
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(place);
    observer?.observe(document.body);
    const timer = setTimeout(stop, GIVE_UP_MS);
    window.addEventListener("wheel", stop, { passive: true, once: true });
    window.addEventListener("touchstart", stop, { passive: true, once: true });
    window.addEventListener("keydown", stop, { once: true });
    place();
    return () => {
      stopped = true;
      observer?.disconnect();
      clearTimeout(timer);
      window.removeEventListener("wheel", stop);
      window.removeEventListener("touchstart", stop);
      window.removeEventListener("keydown", stop);
    };
  }, [hash]);
}

// Back and Forward fire `popstate` before `hashchange`; a link click fires
// only the second. Only the first is a return to somewhere already seen.
let traversed: Map<string, number> | null = null;
let listening = false;
let memory: Map<string, number> | null = null;

function scrollPositions(): Map<string, number> {
  if (!listening && typeof window !== "undefined") {
    listening = true;
    window.addEventListener("popstate", () => { traversed = new Map(memory ?? []); });
  }
  memory ??= new Map();
  return memory;
}
