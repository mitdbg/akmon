// Strict readers for the record, artifact, raw and search envelopes: a field
// that is missing or of the wrong shape raises by name; nothing is defaulted.
import { exact } from "@operator-ui/console/model/shared/format";
import { SearchResponseError } from "@operator-ui/console/api/searchData";

export type Json = Record<string, unknown>;

export function asObject(value: unknown, what: string): Json {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new SearchResponseError(what);
  return value as Json;
}

export function asOptionalString(value: unknown, what: string): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string") throw new SearchResponseError(what);
  return value;
}

export function asString(value: unknown, what: string): string {
  if (typeof value !== "string") throw new SearchResponseError(what);
  return value;
}

export function asOptionalCount(value: unknown, what: string): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) throw new SearchResponseError(what);
  return value;
}

export function asCount(value: unknown, what: string): number {
  const count = asOptionalCount(value, what);
  if (count === null) throw new SearchResponseError(what);
  return count;
}

export function asBoolean(value: unknown, what: string): boolean {
  if (typeof value !== "boolean") throw new SearchResponseError(what);
  return value;
}

/** What a read rewrote before it answered. The read sends the KEYS it
 *  masked and the rule it applied, never the original value, so the notice
 *  can name how many and which keys and nothing more. */
export interface MaskedPathsNotice {
  count: number;
  /** The masked keys, exactly as `masked_paths` spelled them. */
  keys: string[];
  /** `masked_paths_rule`, verbatim: the rule names its own blind spot (a
   *  path inside free text is not detected), which a paraphrase would drop. */
  rule: string;
  sentence: string;
}

/** `masked_paths` / `masked_paths_rule` on the record and raw reads. Null
 *  when the read masked nothing -- a read that rewrote no value says
 *  nothing, rather than printing an empty notice. */
export function maskedPathsNotice(payload: Json, what: string): MaskedPathsNotice | null {
  const rule = asString(payload.masked_paths_rule, `${what} masking rule`);
  const listed = payload.masked_paths;
  if (!Array.isArray(listed)) throw new SearchResponseError(`${what} masked paths`);
  const keys = listed.map((key) => asString(key, `${what} masked path`));
  if (keys.length === 0) return null;
  const values = keys.length === 1 ? "value" : "values";
  return {
    count: keys.length,
    keys,
    rule,
    sentence: `This read shortened ${exact(keys.length)} ${values} that look like a `
      + `file path to the last segment (${keys.join(", ")}). The original ${values} `
      + `${keys.length === 1 ? "is" : "are"} not in this response.`,
  };
}

/** The masking keys are ABSENT, and that is what this read should carry.
 *
 *  `_not_found_raw` is built without touching the record -- "nothing in this
 *  value is computed from the record" -- so it sends no `masked_paths` and no
 *  `masked_paths_rule`. That is the one read allowed to omit them, and this
 *  says so out loud: a read that starts sending them here fails rather than
 *  passing through a branch that ignores them.
 */
export function noMaskedPaths(payload: Json, what: string): null {
  if (payload.masked_paths !== undefined || payload.masked_paths_rule !== undefined) {
    throw new SearchResponseError(`${what} masked paths`);
  }
  return null;
}
