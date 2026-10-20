// Explore's record reads: the search and one record's detail.
//
// The strict parser runs INSIDE the read, as `artifacts.ts` beside this file
// already does. That placement is the whole point. A parser called in the
// `onFulfilled` of the route's `promise.then(onFulfilled, onRejected)` throws
// past its own error handler: the rejection is unhandled, no state is set,
// and the page keeps rendering the loading frame for an answer that already
// arrived and was refused. Parsed here, the refusal rejects the promise the
// route is waiting on, so it reaches the read-failure surface with its named
// reason. routes/console/exploreLiveRoute.test.tsx mounts the route and holds
// that behaviour.
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import type { Pinned } from "@operator-ui/console/api/searchData";
import { type ExploreDetail, parseExploreRecord } from "@operator-ui/console/model/explore/record";
import { type ExploreSearchPage, parseExploreSearch } from "@operator-ui/console/model/explore/search";

/** The routes these reads ask for. Exported so a view that has to name the
 *  read that failed names the same string the read asked for, rather than a
 *  second copy of it that can drift. */
export const SEARCH_ROUTE = "/viz/api/console/records/search" as const;
export const DETAIL_ROUTE = "/viz/api/console/records/detail" as const;

/** The search read, for a request the model built, parsed strictly. */
export function searchRecords(
  api: ApiClient,
  request: Record<string, string | number | boolean | null | undefined>,
): Promise<Pinned<ExploreSearchPage>> {
  return api.get(SEARCH_ROUTE, request).then(parseExploreSearch);
}

/** One record's detail envelope at a version, parsed strictly. */
export function readRecordDetail(api: ApiClient, version: string, id: string): Promise<Pinned<ExploreDetail>> {
  return api.get(DETAIL_ROUTE, { version, id }).then(parseExploreRecord);
}
