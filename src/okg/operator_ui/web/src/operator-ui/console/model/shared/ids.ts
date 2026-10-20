// Identifiers as the operator reads them.

/** The 12 hex characters at the tail of a generation id — the part an operator
 *  reads aloud. */
export function shortVersion(full: string | null): string | null {
  if (!full) return null;
  return full.slice(-12);
}

/** The shape every generation id this console has seen has: the `okg:` prefix,
 *  a `:gen:` segment, and a 12-hex tail --
 *  `okg:<deployment>:branch:<branch>:gen:<stamp>:<12 hex>`. It is the SHAPE,
 *  not a list: a deployment's older versions are past the end of the capped
 *  versions read and are still real ids. */
const VERSION_ID = /^okg:\S+:gen:\S*[0-9a-f]{12}$/i;

export function looksLikeVersionId(value: string): boolean {
  return VERSION_ID.test(value.trim());
}

/**
 * The short form to PRINT for a `?version=` value, or null when there is
 * nothing this console may print.
 *
 * `published` is what the versions read returned; a value among them is named
 * outright, and otherwise the id shape is the test. Anything else is withheld
 * and the caller says so in words.
 *
 * It replaced `shortVersionOrRaw`, which echoed the parameter verbatim
 * whenever it did not contain `:gen:`. `?version=` is whatever a reader or an
 * earlier link put on the URL: on 2026-09-22 a PostgreSQL DSN — password
 * included — arrived there and was rendered into the page's own sentence.
 * Nothing unrecognised is echoed now, not even truncated: a fragment of a
 * secret is still a fragment of a secret.
 */
export function namedVersion(
  value: string,
  published: readonly string[] = [],
): string | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (published.includes(trimmed)) return trimmed.slice(-12);
  return looksLikeVersionId(trimmed) ? trimmed.slice(-12) : null;
}
