import { describe, expect, it } from "vitest";

import { identityFields, identityLabel } from "@operator-ui/console/model/explore/identity";

describe("identityFields", () => {
  it("makes a row per key of the twiki reader's record id", () => {
    expect(identityFields({ path: "HcalIsoTrackValidationSim.txt", chunk_index: 0 })).toEqual([
      { key: "path", label: "Path", kind: "text", value: "HcalIsoTrackValidationSim.txt", full: "HcalIsoTrackValidationSim.txt", asRecorded: null, shortened: false },
      { key: "chunk_index", label: "Chunk", kind: "position", value: "1", full: "1", asRecorded: "chunk_index 0", shortened: false },
    ]);
  });

  it("shortens a 64-hex value and keeps the whole of it to copy", () => {
    const [row] = identityFields({ content_hash: "a".repeat(64) });
    expect(row).toMatchObject({ label: "Content hash", kind: "hash", value: `${"a".repeat(12)}…`, full: "a".repeat(64) });
  });

  it("counts a number and leaves an unknown key as its own words", () => {
    expect(identityFields({ n_records: 980, vision_config_fingerprint: "abc" })).toEqual([
      { key: "n_records", label: "Records in the run", kind: "count", value: "980", full: "980", asRecorded: null, shortened: false },
      { key: "vision_config_fingerprint", label: "Vision config fingerprint", kind: "text", value: "abc", full: "abc", asRecorded: null, shortened: false },
    ]);
  });

  it("marks the value the read shortened, by the path the read named", () => {
    const rows = identityFields(
      { run_id: "r", eos_root: "twiki-snapshot" },
      "source_revision",
      ["source_revision.eos_root"],
    );
    expect(rows.map((row) => [row.key, row.shortened])).toEqual([["run_id", false], ["eos_root", true]]);
  });

  it("drops a key the source left empty, and answers nothing for no identity", () => {
    expect(identityFields({ path: "a.txt", snapshot_date: null })).toHaveLength(1);
    expect(identityFields(null)).toEqual([]);
  });

  it("is one unlabelled row for a plain identity", () => {
    expect(identityFields("docs/operate/handbook.md")).toEqual([
      { key: "", label: "", kind: "text", value: "docs/operate/handbook.md", full: "docs/operate/handbook.md", asRecorded: null, shortened: false },
    ]);
  });

  it("opens out an unknown key rather than naming it something it is not", () => {
    expect(identityLabel("doc_corpus_emission_schema_version")).toBe("Doc corpus emission schema version");
  });
});
