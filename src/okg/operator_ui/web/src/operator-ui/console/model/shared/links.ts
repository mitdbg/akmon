// How a model names a route without knowing the router: the harness and the
// shipping app each supply their own LinkFn.

export type LinkFn = (route: string, params?: Record<string, string>) => string;

/**
 * Whether this build serves a route, so a model can decide between a link and
 * a plain fact.
 *
 * The harness serves all four views and answers true to everything. The live
 * console of slice 1 serves Home alone: a link to `#console/updates` there
 * changes the hash and leaves the reader on Home, which reads as a page that
 * failed rather than as a view that does not exist yet. The count still gets
 * printed; only the promise of navigation is withheld.
 */
export type ServesRouteFn = (route: string) => boolean;

/** The harness's answer, and the default wherever a build does not say. */
export const SERVES_EVERY_ROUTE: ServesRouteFn = () => true;

/**
 * Where a submitted search box sends the reader.
 *
 * Both boxes -- the landing's 640 px one and the top bar's -- do the same
 * thing with the draft: open Explore's results for it. Whitespace alone is
 * not a query, so an emptied box goes back to the landing rather than
 * searching for nothing. The version rides along because `link` carries it.
 *
 * Shared so the two boxes cannot drift apart again. They each used to hand
 * Enter to the other -- the page's box acted "only when the shell does not
 * own the draft", the bar's "only when the page does not" -- and once the
 * shell owned one draft for the whole console, both conditions were false and
 * Enter did nothing at all (reported 2026-09-22).
 */
export function searchHash(draft: string, link: LinkFn): string {
  const trimmed = draft.trim();
  return link("explore", trimmed ? { q: trimmed } : undefined);
}
