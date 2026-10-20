// `source_record_id` and `source_revision`, as labelled fields.
//
// Both are the SOURCE's own identity for a record, free-form by design, and
// the Provenance tab printed them by running JSON.stringify over whatever came
// back: `Source record id {"path":"HcalIsoTrackValidationSim.txt",
// "chunk_index":0}`. Every fact in there is worth a line, and a reader should
// not have to parse JSON to get at it.
//
// Nothing here invents a name. A key this build has never seen is printed as
// its own key with the underscores opened out, which is what the source called
// it; only the shape of the VALUE decides how it is drawn.
import type { Json } from "@operator-ui/console/model/explore/json";

export type IdentityFieldKind = "text" | "hash" | "count" | "position";

export interface IdentityField {
  /** The key as the source wrote it, which is what a copy or a bug report
   *  needs to name. */
  key: string;
  label: string;
  value: string;
  kind: IdentityFieldKind;
  /** The full value when `value` is shortened, so the row can copy it. */
  full: string;
  /** What the source actually recorded, when the row shows something else --
   *  a chunk's position is its stored index plus one. */
  asRecorded: string | null;
  /** True when the read shortened THIS value before answering (it looked
   *  like a file path). The row says so in place, which is what replaced a
   *  70-word policy paragraph at the top of every record page. */
  shortened: boolean;
}

/** Labels for the keys the live readers write. Anything else is its own key,
 *  opened out: `table_transcription_config_fingerprint` reads as
 *  "Table transcription config fingerprint". */
const LABELS: Record<string, string> = {
  path: "Path",
  repo: "Repository",
  chunk_index: "Chunk",
  run_id: "Run id",
  eos_root: "Root",
  n_records: "Records in the run",
  content_hash: "Content hash",
  content_sha256: "Content hash",
  snapshot_date: "Snapshot date",
};

const HEX64 = /^[0-9a-f]{64}$/i;

export function identityLabel(key: string): string {
  const known = LABELS[key];
  if (known) return known;
  const words = key.replace(/[_-]+/g, " ").trim();
  return words ? words[0].toUpperCase() + words.slice(1) : key;
}

/**
 * One identity value as rows.
 *
 * A string or a number is one unlabelled row -- the identity IS the value, and
 * a made-up label over it would be this build's word and not the source's. An
 * object is a row per key. Anything else (a list, a nested object) is one row
 * holding the JSON, because a shape no reader writes is better shown whole
 * than taken apart by guesswork.
 */
export function identityFields(
  value: unknown,
  /** The key this identity is served under ("source_revision"), so a
   *  `masked_paths` entry like `source_revision.eos_root` finds its row. */
  group = "",
  /** What the read said it shortened, exactly as it named it. */
  shortenedPaths: readonly string[] = [],
): IdentityField[] {
  const shortened = new Set(shortenedPaths);
  const isShort = (key: string) => shortened.has(key ? `${group}.${key}` : group) || shortened.has(key);
  if (value === null || value === undefined) return [];
  if (typeof value === "string" || typeof value === "number") {
    return [field("", String(value), isShort(""))];
  }
  if (typeof value !== "object" || Array.isArray(value)) {
    return [field("", JSON.stringify(value), isShort(""))];
  }
  const entries = Object.entries(value as Json)
    .filter(([, raw]) => raw !== null && raw !== undefined && raw !== "");
  const counts = runCounts(entries);
  return entries
    .filter(([key]) => !counts || !counts.keys.has(key) || key === "n_records")
    .map(([key, raw]) => (counts && key === "n_records"
      ? counts.row
      : field(key, raw, isShort(key))));
}

/**
 * A run's counts, as one row that says what each counts.
 *
 * cmssw_releases records `n_records: 2328` and `n_release_families: 87`, and
 * the tab printed "Records in the run 2328" beside a Sources count of 2,415:
 * the 87 families were a second row under their raw key. One row reads
 * "2,328 records and 87 release families", which is the 2,415. Only where a
 * source records `n_records` beside another `n_<thing>` count; a lone
 * `n_records` keeps its own row.
 */
function runCounts(entries: [string, unknown][]): { keys: Set<string>; row: IdentityField } | null {
  const counted = entries.filter(
    ([key, raw]) => /^n_[a-z0-9_]+$/.test(key) && typeof raw === "number" && Number.isFinite(raw),
  ) as [string, number][];
  if (counted.length < 2 || !counted.some(([key]) => key === "n_records")) return null;
  const ordered = [
    ...counted.filter(([key]) => key === "n_records"),
    ...counted.filter(([key]) => key !== "n_records"),
  ];
  const parts = ordered.map(([key, raw]) => `${raw.toLocaleString("en-US")} ${key.slice(2).replace(/_/g, " ")}`);
  const value = parts.length === 2
    ? parts.join(" and ")
    : `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
  return {
    keys: new Set(counted.map(([key]) => key)),
    row: {
      key: "n_records", label: "In the run", value, kind: "text", full: value,
      asRecorded: counted.map(([key, raw]) => `${key} ${raw}`).join(", "), shortened: false,
    },
  };
}

function field(key: string, raw: unknown, shortened = false): IdentityField {
  const label = key ? identityLabel(key) : "";
  if (key === "chunk_index" && typeof raw === "number" && Number.isInteger(raw) && raw >= 0) {
    // Counted from one, like the results page and "Part N of M". The stored
    // value is kept on the row rather than dropped, because it is what the
    // source recorded and what a query would use.
    return {
      key, label, kind: "position",
      value: String(raw + 1), full: String(raw + 1),
      asRecorded: `chunk_index ${raw}`, shortened,
    };
  }
  if (typeof raw === "number") {
    return { key, label, kind: "count", value: String(raw), full: String(raw), asRecorded: null, shortened };
  }
  const text = typeof raw === "string" ? raw : JSON.stringify(raw);
  if (typeof raw === "string" && HEX64.test(raw)) {
    return { key, label, kind: "hash", value: `${raw.slice(0, 12)}…`, full: raw, asRecorded: null, shortened };
  }
  return { key, label, kind: "text", value: text, full: text, asRecorded: null, shortened };
}
