/**
 * Where the operator console is mounted on its host.
 *
 * The console used to assume it owned the origin root: Vite emitted
 * root-absolute asset URLs (`/assets/index-*.js`) and `buildApiUrl` emitted
 * root-absolute read paths (`/viz/api/...`). Both assumptions break the moment
 * the console is served under a path prefix, and they break DIFFERENTLY:
 *
 *   - the assets 404, so the page is blank (measured:
 *     `pact/changes/openwebui-chat-frontend/evidence/read-handshake-transcript.md:325-348`);
 *   - the reads do NOT 404 — they succeed against whatever owns the origin
 *     root, which in the one-hostname topology is the chat site. A console
 *     whose assets were fixed but whose reads were not would boot cleanly and
 *     silently read another application.
 *
 * One declared mount fixes both. It is a BUILD INPUT (`OKG_OPERATOR_UI_BASE`),
 * threaded into Vite's `base`, from which Vite injects `import.meta.env.BASE_URL`
 * for the runtime read path. This module is the single place that parses it, and
 * it is deliberately free of `import.meta` so the Vite config (plain Node) and
 * the browser bundle can share it.
 *
 * Failure policy: a MALFORMED mount throws. There is no coercion to `/`,
 * because silently reverting to the origin root is exactly the defect.
 */

/** Build-time environment variable carrying the console's declared mount. */
export const OPERATOR_UI_BASE_ENV = "OKG_OPERATOR_UI_BASE";

/** The mount of a console that really does own the origin root. */
export const ROOT_MOUNT_BASE = "/";

/**
 * A declared mount could not be understood. Thrown, never swallowed: an
 * unreadable mount must stop the build (or the first read) rather than fall
 * back to the origin root.
 */
export class MountBaseError extends Error {
  readonly raw: unknown;

  constructor(message: string, raw: unknown) {
    super(message);
    this.name = "MountBaseError";
    this.raw = raw;
  }
}

function reject(raw: unknown, why: string): never {
  throw new MountBaseError(
    `invalid operator console mount base ${JSON.stringify(raw)}: ${why}. `
      + `Expected an origin-relative path prefix such as "/console/" `
      + `(or "/" for a console that owns the origin root). `
      + `Set it with ${OPERATOR_UI_BASE_ENV}.`,
    raw,
  );
}

/**
 * Canonicalise a declared mount to a leading-and-trailing-slash path prefix.
 *
 * `"/console"` and `"/console/"` both normalise to `"/console/"`; `"/"` stays
 * `"/"`. Everything else — a relative prefix, a protocol-relative prefix, a
 * full URL, a prefix carrying a query or fragment, `..` traversal, stray
 * whitespace, or the empty string — throws.
 */
export function normalizeMountBase(raw: unknown): string {
  if (typeof raw !== "string") {
    reject(raw, "expected a string");
  }
  if (raw !== raw.trim()) {
    reject(raw, "has leading or trailing whitespace");
  }
  if (raw === "") {
    reject(raw, "is empty (an unset variable is the root build; an empty one is a misconfigured shell)");
  }
  if (/\s/.test(raw)) {
    reject(raw, "contains whitespace");
  }
  if (raw.includes("?") || raw.includes("#")) {
    reject(raw, "carries a query or fragment");
  }
  if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(raw)) {
    reject(raw, "is an absolute URL; the console addresses its API relative to its own origin, so a CDN base would send reads off-origin");
  }
  if (raw.startsWith("//")) {
    reject(raw, "is protocol-relative, which points at another host");
  }
  if (!raw.startsWith("/")) {
    reject(raw, "does not start with \"/\"");
  }
  if (raw.split("/").includes("..")) {
    reject(raw, "contains a \"..\" segment");
  }
  if (raw.includes("//")) {
    reject(raw, "contains an empty path segment");
  }
  return raw.endsWith("/") ? raw : `${raw}/`;
}

/**
 * Read the console's declared mount out of a build environment.
 *
 * UNSET is the root build: the console owns the origin root and every emitted
 * URL is byte-for-byte what it is today. That is a declared outcome of the
 * contract (`scn.operator-ui-base-aware-build.root-build-unbroken`), not a
 * fallback — which is why a variable that is SET but empty or malformed throws
 * instead of quietly producing the root build.
 */
export function resolveOperatorUiBase(
  env: Record<string, string | undefined> = {},
): string {
  const raw = env[OPERATOR_UI_BASE_ENV];
  if (raw === undefined) {
    return ROOT_MOUNT_BASE;
  }
  return normalizeMountBase(raw);
}

/**
 * Join the console's mount with one of its own origin-relative paths.
 *
 * `joinMountPath("/console/", "/viz/api/health")` -> `"/console/viz/api/health"`.
 * At the root mount the path is returned unchanged, so the root build's request
 * URLs do not move.
 */
export function joinMountPath(mountBase: string, path: `/${string}`): string {
  const mount = normalizeMountBase(mountBase);
  if (mount === ROOT_MOUNT_BASE) {
    return path;
  }
  return `${mount.slice(0, -1)}${path}`;
}
