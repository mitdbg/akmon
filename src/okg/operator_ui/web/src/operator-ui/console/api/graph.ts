// The Graph view's reads: the type map, the source links, one record's
// neighbourhood and the declared ontology. GET only.
//
// Each read is parsed INSIDE the promise (as records.ts does), so a refused
// answer rejects the promise the view waits on and reaches the read-failure
// surface by name. Each is its own read, so one failing leaves the others
// drawn: the type map without the ontology still draws; the Grid without the
// source links draws its first two grids and says why the third is missing.
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import { SearchResponseError } from "@operator-ui/console/api/searchData";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import type { ProducerRef } from "@operator-ui/console/model/graph/derive";
import {
  type GraphTypesRead, type OntologyRead, type RecordNeighbourhoodRead, type SourceLinksRead,
  parseGraphTypes, parseOntology, parseRecordNeighbourhood, parseSourceLinks,
} from "@operator-ui/console/model/graph/reads";
import type { RawVersions } from "@operator-ui/console/model/shared/reads";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";

/** The console's versions read (the top bar's picker reads it too). */
export const PUBLISHED_VERSIONS_ROUTE = "/viz/api/console/versions" as const;

export const GRAPH_TYPES_ROUTE = "/viz/api/console/graph/types" as const;
export const GRAPH_SOURCE_LINKS_ROUTE = "/viz/api/console/graph/source-links" as const;
export const GRAPH_RECORD_ROUTE = "/viz/api/console/graph/record" as const;
export const GRAPH_ONTOLOGY_ROUTE = "/viz/api/console/graph/ontology" as const;

type Opts = { signal?: AbortSignal };

function withVersion(version: string | null): Record<string, string> {
  return version ? { version } : {};
}

export function readGraphTypes(api: ApiClient, version: string | null, opts: Opts = {}): Promise<GraphTypesRead> {
  return api.get(GRAPH_TYPES_ROUTE, withVersion(version), opts).then(parseGraphTypes);
}

export function readSourceLinks(api: ApiClient, version: string | null, opts: Opts = {}): Promise<SourceLinksRead> {
  return api.get(GRAPH_SOURCE_LINKS_ROUTE, withVersion(version), opts).then(parseSourceLinks);
}

export function readRecordNeighbourhood(
  api: ApiClient, id: string, version: string | null, opts: Opts = {},
): Promise<RecordNeighbourhoodRead> {
  return api.get(GRAPH_RECORD_ROUTE, { ...withVersion(version), id }, opts).then(parseRecordNeighbourhood);
}

export function readOntology(api: ApiClient, version: string | null, opts: Opts = {}): Promise<OntologyRead> {
  return api.get(GRAPH_ONTOLOGY_ROUTE, withVersion(version), opts).then(parseOntology);
}

/**
 * The producers read Sources draws, for one thing: the name, kind and Sources
 * page of each ownership id the graph reads name. It takes no version (names
 * do not change with one). A row without an ownership id names nothing here
 * and is left out, as Explore does.
 */
export function readProducerNames(api: ApiClient, opts: Opts = {}): Promise<ProducerRef[]> {
  return api.get<RawProducersRead>(PRODUCERS_ROUTE, {}, opts).then((raw) => {
    const rows = raw?.payload?.producers;
    if (!Array.isArray(rows)) throw new SearchResponseError("producers list");
    return rows.map((row) => {
      if (typeof row?.name !== "string" || !["source", "enricher", "view"].includes(row.kind)) {
        throw new SearchResponseError("producer row");
      }
      return { name: row.name, ownershipId: row.ownership_id ?? row.name, kind: row.kind };
    });
  });
}

/** Every published version and its publish time, newest first: the page
 *  names the minute the version it shows was published. */
export function readPublishedVersions(api: ApiClient, opts: Opts = {}): Promise<{ generationId: string; publishedAt: string }[]> {
  return api.get<RawVersions>(PUBLISHED_VERSIONS_ROUTE, {}, opts).then((raw) => {
    const rows = raw?.payload?.versions;
    if (!Array.isArray(rows)) throw new SearchResponseError("published versions");
    return rows.map((row) => {
      if (typeof row?.generation_id !== "string") throw new SearchResponseError("published version");
      return { generationId: row.generation_id, publishedAt: typeof row.published_at === "string" ? row.published_at : "" };
    });
  });
}
