// Explore's artifact reads: the record's original file and its raw layers,
// parsed strictly on the way in.
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { parseArtifactRead, parseRawRead, type ArtifactRead, type RawRead } from "@operator-ui/console/model/explore/raw";
import type { Pinned } from "@operator-ui/console/api/searchData";

/** The routes these reads ask for, for a view that has to name the one
 *  that failed. */
export const ARTIFACT_ROUTE = "/viz/api/console/records/artifact" as const;
export const RAW_ROUTE = "/viz/api/console/records/raw" as const;

/** The retained original file, or the reason it is absent. */
export function readArtifact(api: ApiClient, version: string, id: string): Promise<Pinned<ArtifactRead>> {
  return api.get(ARTIFACT_ROUTE, { version, id }).then(parseArtifactRead);
}

/** The stored row, fact log and retained payload or refusal. */
export function readRaw(api: ApiClient, version: string, id: string): Promise<Pinned<RawRead>> {
  return api.get(RAW_ROUTE, { version, id }).then(parseRawRead);
}
