import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";
import { parseFailurePayload } from "@operator-ui/shared/data/failure";
import {
  parseListPage,
  parseRecordDetail,
  parseRecordTypes,
  parseSearchPage,
  parseVersionList,
  SearchResponseError,
  type ListPage,
  type Pinned,
  type RecordDetail,
  type RecordTypeCount,
  type SearchPage,
  type VersionList,
} from "@operator-ui/console/api/searchData";

/** The routes these reads ask for, for a view that has to name the one
 *  that failed. */
export const VERSIONS_ROUTE = "/viz/api/console/versions" as const;
export const RECORD_TYPES_ROUTE = "/viz/api/console/records/types" as const;
export const RECORD_LIST_ROUTE = "/viz/api/console/records/list" as const;

export const SEARCH_PAGE_SIZE = 20;
export const LIST_PAGE_SIZE = 50;

export async function loadVersions(api: ApiClient): Promise<VersionList> {
  return parseVersionList(await api.get(VERSIONS_ROUTE));
}

export async function loadRecordTypes(api: ApiClient, version: string): Promise<Pinned<RecordTypeCount[]>> {
  return parseRecordTypes(await api.get(RECORD_TYPES_ROUTE, { version }));
}

export async function searchRecords(
  api: ApiClient,
  request: { version: string; query: string; type: string | null; offset: number },
): Promise<Pinned<SearchPage>> {
  return parseSearchPage(await api.get("/viz/api/console/records/search", {
    version: request.version,
    q: request.query,
    type: request.type,
    offset: request.offset,
    limit: SEARCH_PAGE_SIZE,
  }));
}

export async function listRecords(
  api: ApiClient,
  request: { version: string; type: string; offset: number },
): Promise<Pinned<ListPage>> {
  return parseListPage(await api.get(RECORD_LIST_ROUTE, {
    version: request.version,
    type: request.type,
    offset: request.offset,
    limit: LIST_PAGE_SIZE,
  }));
}

export async function loadRecord(
  api: ApiClient,
  request: { version: string; id: string },
): Promise<Pinned<RecordDetail>> {
  return parseRecordDetail(await api.get("/viz/api/console/records/detail", {
    version: request.version,
    id: request.id,
  }));
}

export interface SearchFailure {
  title: string;
  detail: string;
  nextCheck: string | null;
}

/** An operator-readable account of a failed Search read. */
export function describeSearchFailure(error: unknown, version: string | null): SearchFailure {
  if (error instanceof SearchResponseError) {
    return {
      title: "The server's answer could not be read",
      detail: error.message,
      nextCheck: "Refresh the page. If this repeats, check the data server's log.",
    };
  }
  if (!(error instanceof ApiRequestError)) {
    return {
      title: "The read did not complete",
      detail: error instanceof Error ? error.message : String(error),
      nextCheck: "Check that the data server is running, then refresh.",
    };
  }
  const payload = parseFailurePayload(error.responseText);
  const reason = payload.reason ?? "";
  const nextCheck = payload.next_check ?? null;
  const shown = version ? `version ${version}` : "this version";
  if (reason === "policy_denied") {
    return {
      title: "The access policy refused this read",
      detail: payload.error ?? "The deployment's policy does not allow the operator UI to read these records.",
      nextCheck,
    };
  }
  if (reason === "version_not_published" || reason === "version_not_a_published_checkpoint") {
    return {
      title: `${capitalize(shown)} is not a published version of this deployment`,
      detail: "No records were read. Nothing was substituted for the version you asked for.",
      nextCheck: nextCheck ?? "Choose a published version from the version picker.",
    };
  }
  if (reason === "version_compacted") {
    return {
      title: `${capitalize(shown)} is no longer retained`,
      detail: "Older versions can be removed to reclaim space.",
      nextCheck,
    };
  }
  if (error.status === undefined) {
    return {
      title: "The data server could not be reached",
      detail: error.responseText ?? error.message,
      nextCheck: "Check that the data server is running, then refresh.",
    };
  }
  return {
    title: `The read failed (${error.status})`,
    detail: payload.error ?? error.message,
    nextCheck: nextCheck ?? "Refresh the page. If this repeats, check the data server's log.",
  };
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}
