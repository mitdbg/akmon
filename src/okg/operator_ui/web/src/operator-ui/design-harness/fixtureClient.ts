// A design-time ApiClient that answers from a fixture instead of the network.
//
// It is the whole harness: every read the console makes goes through
// `ApiClient.get`, so substituting this one client renders all four views from
// one mock deployment with no server, no database and no network. Nothing here
// ships: `fixtures.ts` imports the data behind `import.meta.env.DEV`, and
// scripts/operator-ui-design/check-bundle-clean.mjs fails the build if a fixture name reaches the
// production bundle.
//
// It REFUSES an undefined read rather than returning an empty payload. A
// fixture client that answered `{}` would render a healthy-looking blank page
// for a read nobody wrote, and the mock would be approved on a lie. Rule 13 of
// the redesign ("loud failure") is enforced here, at the bottom, so no view has
// to remember it.
import { FixtureReadMissing } from "@operator-ui/console/api/readFailures";
import { ApiRequestError, type ApiClient } from "@operator-ui/shared/api/apiClient";
import { FIXTURE_FAILURE, graphReads } from "@operator-ui/design-harness/graphFixtures";

// The class is defined beside LoudFailure so shipping code never imports
// this module; it is re-exported here because the harness and its tests
// name it from the client that throws it.
export { FixtureReadMissing };

export interface DesignFixture {
  name: string;
  purpose: string;
  clock: string;
  stories: string[];
  edge_cases: string[];
  expect: Record<string, unknown>;
  reads: Record<string, unknown>;
}

/** The key a read becomes. Must match `readKey()` in
 *  scripts/operator-ui-design/generate-fixtures.mjs exactly, or every lookup misses. */
export function readKey(
  route: string,
  params: Record<string, string | number | boolean | null | undefined> = {},
): string {
  const pairs = Object.entries(params)
    .filter(([name, value]) => value !== null && value !== undefined && name !== "refresh")
    .map(([name, value]) => [name, String(value)] as const)
    .sort((a, b) => a[0].localeCompare(b[0]));
  if (pairs.length === 0) return route;
  return `${route}?${pairs.map(([n, v]) => `${n}=${encodeURIComponent(v)}`).join("&")}`;
}

/**
 * Wrap a fixture as an ApiClient.
 *
 * Lookup is exact key first, then the bare route. The fallback exists because
 * several loaders append a `generation` or `version` parameter whose value is
 * whatever the health read just returned, and pinning every such combination
 * into the fixture would triple its size to say the same thing. A read whose
 * ANSWER depends on its parameters -- the record reads, and a producer's detail
 * read -- is written under its full key, so the fallback cannot silently serve
 * one record's payload for another: the bare route is absent for those and the
 * lookup refuses.
 */
export function createFixtureApiClient(fixture: DesignFixture): ApiClient {
  // The Graph view's reads are defined beside the harness rather than in the
  // generated JSON (see graphFixtures.ts); they are merged in here.
  const versions = fixture.reads["/viz/api/console/versions"] as { payload?: { latest?: string | null } } | undefined;
  const reads: Record<string, unknown> = { ...fixture.reads, ...graphReads(fixture.name, versions?.payload?.latest ?? null) };
  const answer = <T>(route: string, params: Record<string, unknown>, value: unknown): T => {
    // A failure the fixture declares is thrown as the live client throws it.
    const declared = value && typeof value === "object" ? (value as Record<string, unknown>)[FIXTURE_FAILURE] : undefined;
    if (declared) {
      const { status, body } = declared as { status: number; body: unknown };
      throw new ApiRequestError({
        route, params: params as ApiRequestError["params"], status, responseText: JSON.stringify(body),
      });
    }
    return value as T;
  };
  return {
    async get<T>(
      route: `/viz/api/${string}`,
      params: Record<string, string | number | boolean | null | undefined> = {},
    ): Promise<T> {
      const key = readKey(route, params);
      if (Object.prototype.hasOwnProperty.call(reads, key)) {
        return answer<T>(route, params, reads[key]);
      }
      if (Object.prototype.hasOwnProperty.call(reads, route)) {
        return answer<T>(route, params, reads[route]);
      }
      const available = Object.keys(reads).filter((k) => k === route || k.startsWith(`${route}?`));
      throw new FixtureReadMissing(fixture.name, key, available);
    },
  };
}

/** A client that fails every read, for proving a view's failure state. */
export function createFailingApiClient(reason: string): ApiClient {
  return {
    async get<T>(route: `/viz/api/${string}`): Promise<T> {
      throw new ApiRequestError({ route, status: 503, responseText: reason });
    },
  };
}
