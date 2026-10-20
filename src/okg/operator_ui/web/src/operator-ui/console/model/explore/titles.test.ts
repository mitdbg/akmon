import { describe, expect, it } from "vitest";

import { NO_ORIGIN, type RecordOrigin } from "@operator-ui/console/api/searchData";
import { chunkPosition, groupByDocument, partPosition, recordHeading, recordTitle, rowTitle, shortRecordId, titlesInList } from "@operator-ui/console/model/explore/titles";

const origin = (over: Partial<RecordOrigin>): RecordOrigin => ({ ...NO_ORIGIN, ...over });
const hit = (label: string, over: Partial<RecordOrigin> = {}) => ({ hit: { label, origin: origin(over) } });

describe("recordHeading", () => {
  it("prefers the source's own title, then its path, then the served label", () => {
    expect(recordHeading({ label: "Document Chunk · e734d", origin: origin({ title: "Hcal iso track", path: "a.txt" }) }))
      .toBe("Hcal iso track");
    expect(recordHeading({ label: "Document Chunk · e734d", origin: origin({ path: "HcalIsoTrackValidationSim.txt" }) }))
      .toBe("HcalIsoTrackValidationSim.txt");
  });

  it("keeps the served label when the source records no path", () => {
    expect(recordHeading({ label: "Document Chunk · e734d", origin: NO_ORIGIN })).toBe("Document Chunk · e734d");
  });
});

describe("chunkPosition", () => {
  it("counts from one, and says 'of' only when the count is served", () => {
    expect(chunkPosition(origin({ chunkIndex: 0 }))).toBe("chunk 1");
    expect(chunkPosition(origin({ chunkIndex: 2, chunkCount: 9 }))).toBe("chunk 3 of 9");
  });

  it("is null for a record that is not a chunk", () => {
    expect(chunkPosition(NO_ORIGIN)).toBeNull();
  });
});

describe("rowTitle", () => {
  it("is the position for a chunk and the heading for anything else", () => {
    expect(rowTitle({ label: "x", origin: origin({ path: "a.txt", chunkIndex: 1 }) })).toBe("chunk 2");
    expect(rowTitle({ label: "x", origin: origin({ path: "a.txt" }) })).toBe("a.txt");
  });
});

describe("groupByDocument", () => {
  it("puts consecutive chunks of one document under one heading", () => {
    const groups = groupByDocument([
      hit("Document Chunk · 1", { path: "a.txt", chunkIndex: 0 }),
      hit("Document Chunk · 2", { path: "a.txt", chunkIndex: 1 }),
      hit("Document Chunk · 3", { path: "b.txt", chunkIndex: 0 }),
    ]);
    expect(groups.map((group) => [group.heading, group.rows.length, group.chunks]))
      .toEqual([["a.txt", 2, true], ["b.txt", 1, true]]);
  });

  it("gathers a later chunk of a document into its first group, in first-seen order", () => {
    // Archiv3 review, 2026-09-22: one document drawn as five groups.
    const groups = groupByDocument([
      hit("1", { path: "a.txt", chunkIndex: 0 }),
      hit("2", { path: "b.txt", chunkIndex: 0 }),
      hit("3", { path: "a.txt", chunkIndex: 4 }),
    ]);
    expect(groups.map((group) => [group.heading, group.rows.length])).toEqual([["a.txt", 2], ["b.txt", 1]]);
  });

  it("leaves a record with no path as its own group, headed by its label", () => {
    const groups = groupByDocument([hit("Ontology node 7")]);
    expect(groups).toEqual([{ heading: "Ontology node 7", chunks: false, rows: [hit("Ontology node 7")] }]);
  });
});

describe("partPosition", () => {
  it("is the position in the word the record page already uses for it", () => {
    expect(partPosition(origin({ chunkIndex: 0 }))).toBe("Part 1");
    expect(partPosition(origin({ chunkIndex: 2, chunkCount: 9 }))).toBe("Part 3 of 9");
    expect(partPosition(NO_ORIGIN)).toBeNull();
  });
});

describe("recordTitle", () => {
  it("titles a chunk by its document and its place in it, not by its own hash", () => {
    // The row this came from is a live one: the twiki-eos reader records
    // `{path, chunk_index}` and the server labels the record by its hash.
    expect(recordTitle({
      label: "Document Chunk · 90a54b958922477c",
      origin: origin({ path: "HcalIsoTrackValidationSim.txt", chunkIndex: 1 }),
    })).toBe("HcalIsoTrackValidationSim.txt · Part 2");
  });

  it("is the document alone when the record is the document", () => {
    expect(recordTitle({ label: "Document Chunk · 1", origin: origin({ path: "a.txt" }) })).toBe("a.txt");
  });

  it("keeps the served label when the source records no path, and prints no bare position", () => {
    expect(recordTitle({ label: "CMSSW_14_0_0", origin: NO_ORIGIN })).toBe("CMSSW_14_0_0");
    // A position with no document to hang it on names nothing, so it is not
    // printed: "Part 3" on its own is worse than the label.
    expect(recordTitle({ label: "Ontology node 7", origin: origin({ chunkIndex: 2 }) })).toBe("Ontology node 7");
  });
});

describe("shortRecordId", () => {
  it("cuts a hash tail to eight characters and leaves a readable one whole", () => {
    expect(shortRecordId("chunk:001409c8556277f0")).toBe("001409c8");
    expect(shortRecordId("chunk:5fe6bef91614aa4d")).toBe("5fe6bef9");
    // `deployme` names nothing, so a tail that is not a hash is not cut.
    expect(shortRecordId("doc:deployment_configs:beaver-okg/deployment.yaml"))
      .toBe("beaver-okg/deployment.yaml");
  });
});

describe("titlesInList", () => {
  const read = (row: { title: string; id: string; scope?: string }) => row;

  it("leaves every title alone when no two of them read the same", () => {
    const rows = [
      { title: "a.txt", id: "doc:a" },
      { title: "b.txt", id: "doc:b" },
    ];
    expect(titlesInList(rows, read)).toEqual(["a.txt", "b.txt"]);
  });

  it("appends the short id to the rows that collide, and only to those", () => {
    const rows = [
      { title: "beaver-okg/deployment.yaml", id: "chunk:5fe6bef91614aa4d" },
      { title: "beaver-okg/deployment.yaml", id: "chunk:eb96b0ba40f4f8f8" },
      { title: "beaver-okg/source_registry.yaml", id: "chunk:c19dee76222743cb" },
    ];
    expect(titlesInList(rows, read)).toEqual([
      "beaver-okg/deployment.yaml · 5fe6bef9",
      "beaver-okg/deployment.yaml · eb96b0ba",
      "beaver-okg/source_registry.yaml",
    ]);
  });

  it("says nothing extra when the rows sharing a title are the SAME record", () => {
    // The Related tab lists a pair the graph recorded both ways: two rows,
    // one record. The id would repeat rather than tell them apart, and the
    // Relationship column already does.
    const rows = [
      { title: "beaver-okg/deployment.yaml", id: "doc:deployment_configs:beaver-okg/deployment.yaml" },
      { title: "beaver-okg/deployment.yaml", id: "doc:deployment_configs:beaver-okg/deployment.yaml" },
    ];
    expect(titlesInList(rows, read))
      .toEqual(["beaver-okg/deployment.yaml", "beaver-okg/deployment.yaml"]);
  });

  it("separates lists by scope, so a heading above the rows still counts", () => {
    // Two documents each matched at their own "chunk 1". The rows do not
    // read the same, because each sits under its document's heading.
    const rows = [
      { title: "chunk 1", id: "chunk:aaaaaaaaaaaa1111", scope: "A.txt" },
      { title: "chunk 1", id: "chunk:bbbbbbbbbbbb2222", scope: "B.txt" },
    ];
    expect(titlesInList(rows, read)).toEqual(["chunk 1", "chunk 1"]);
  });

  it("separates same-path rows by a digest, not by repeating the title or printing the id", () => {
    // One path recorded by two sources. The tail after the last colon IS the
    // title here, so the old rule appended the title to itself -- and when
    // that still collided it printed the whole internal id in a results
    // title, which is the one place this console has decided an id does not
    // belong. Neither row now repeats its title, and neither carries an id.
    const rows = [
      { title: "deployment.yaml", id: "doc:repo_a:deployment.yaml" },
      { title: "deployment.yaml", id: "doc:repo_b:deployment.yaml" },
    ];
    expect(titlesInList(rows, read)).toEqual([
      "deployment.yaml · doc:cb254221",
      "deployment.yaml · doc:c4d3b30b",
    ]);
  });

  it("names a path-tailed record by its type and a digest, never by its own title", () => {
    // The reviewer's case: `doc:...:kubernetes-smoke/deployment.yaml`, whose
    // tail after the last colon is exactly the title.
    const rows = [
      { title: "kubernetes-smoke/deployment.yaml", id: "doc:kubernetes_smoke:kubernetes-smoke/deployment.yaml" },
      { title: "kubernetes-smoke/deployment.yaml", id: "doc:repo_a:deployment.yaml" },
    ];
    const titles = titlesInList(rows, read);
    expect(titles[0]).toBe("kubernetes-smoke/deployment.yaml · doc:277c94db");
    expect(titles[0]).not.toContain("kubernetes-smoke/deployment.yaml · kubernetes-smoke");
    for (const title of titles) {
      expect(title).not.toContain("kubernetes_smoke:");
      expect(title.split(" · ")[1]).not.toBe(title.split(" · ")[0]);
    }
  });

  it("uses the record's own hash tail where the id has one", () => {
    // A chunk id ends in a hash, which is the record's own name: that is
    // what a reader would copy, so a digest of the id would be a second,
    // invented name for the same thing.
    const rows = [
      { title: "chunk 1", id: "chunk:aaaaaaaaaaaa1111" },
      { title: "chunk 1", id: "chunk:bbbbbbbbbbbb2222" },
    ];
    expect(titlesInList(rows, read)).toEqual([
      "chunk 1 · aaaaaaaa", "chunk 1 · bbbbbbbb",
    ]);
  });

  it("does not repeat a title that IS the hash tail", () => {
    // The server labels a chunk with its own hash. Appending the tail there
    // would draw "e734d84ffd5e9620 · e734d84f".
    const rows = [
      { title: "aaaaaaaaaaaa1111", id: "chunk:aaaaaaaaaaaa1111", scope: "A" },
      { title: "aaaaaaaaaaaa1111", id: "chunk:aaaaaaaaaaaa2222", scope: "A" },
    ];
    const titles = titlesInList(rows, read);
    expect(titles[0]).not.toBe("aaaaaaaaaaaa1111 · aaaaaaaa");
    expect(titles[0]).toBe("aaaaaaaaaaaa1111 · chunk:f5191f3d");
    expect(titles[1]).toBe("aaaaaaaaaaaa1111 · chunk:8c3937b5");
  });
});
