// What the console withholds from a recorded string, and what it prints.
//
// The rule is one requirement: a string that names a thing INSIDE the database
// is withheld, and everything else is printed. The first shape test read the
// rule as "any lowercase token with an internal dot or underscore", which is
// not what the requirement promises: it withheld a producer's own name
// (`cmssw_releases returned 412 rows`), a version (`v1.2.0`), a host
// (`api.github.com`) and a file (`config.yaml`), while missing a quoted
// relation, a possessive and anything written in mixed case.
//
// The table below is the rule. Each row is a string a read really can record.
import { describe, expect, it } from "vitest";

import {
  measurementWords, namesDatabaseObject, shareLabel,
} from "@operator-ui/console/model/shared/format";

/** [the recorded string, whether it names something inside the database]. */
const ROWS: [string, boolean][] = [
  // Schema-qualified, in whatever case the writer used.
  ["dbos.workflow_status missing", true],
  ["okg.v_node_current is empty", true],
  ["contributor-graph: relation okg.contributor_edge does not exist", true],
  ["okg.contributor_edge's row count fell to 0", true],
  ["OKG.Contributor_Edge has no rows", true],
  ["public.workflowStatus was dropped", true],
  ["information_schema.columns is unreadable", true],
  // A quoted relation name, whatever it is called.
  ['relation "public.workflowStatus" does not exist', true],
  ['relation "run_ledger" does not exist', true],
  // An object suffix with no schema in front of it.
  ["workflow_status missing", true],
  ["node_facts is stale", true],
  ["contributor_edge has no rows", true],
  ["document_node missing", true],
  ["paper_refs is empty", true],
  // A producer's own name is on the page anyway, so the sentence stays.
  ["cmssw_releases returned 412 rows of an expected 2,414", false],
  ["git_files produced nothing", false],
  // A version, a host, a file and an abbreviation are not database objects.
  ["the runtime reports v1.2.0", false],
  ["api.github.com refused the connection", false],
  ["config.yaml declares no cadence", false],
  ["no cadence is declared, e.g. daily", false],
  // Measurements made of words.
  ["0 workers reporting, last heartbeat 5h 49m ago", false],
  ["37 dead-letter jobs, oldest 2d 4h", false],
  ["3 of 412 invariants failed", false],
  ["repository not found (404)", false],
  // A fix hint naming the DBOS runtime is not a table name: bare, three of
  // the four schemas are ordinary words.
  ["inspect DBOS history volume and the runtime queue authority", false],
  ["the public API refused the read", false],
  ["okg publish is still running", false],
  ["the remote refused the connection after 3 retries", false],
];

describe("what the console withholds from a recorded string", () => {
  it.each(ROWS)("%s -> %s", (text, withheld) => {
    expect(namesDatabaseObject(text)).toBe(withheld);
  });

  it("says nothing about a string that is not there", () => {
    expect(namesDatabaseObject(null)).toBe(false);
    expect(namesDatabaseObject(undefined)).toBe(false);
    expect(namesDatabaseObject("")).toBe(false);
  });
});

describe("a recorded measurement, as a person reads it", () => {
  // Live okg-workspace's `dlq_nonzero` check writes
  // `actual: "12281 in dead_letter"`, and Home printed exactly that: an
  // unseparated five-digit number and a queue named by its column.
  it("groups the count and names the queue in words", () => {
    expect(measurementWords("12281 in dead_letter"))
      .toBe("12,281 in the dead-letter queue");
  });

  it("leaves a database object's name alone, so the row can still withhold it", () => {
    // `namesDatabaseObject` decides what the row does with the whole string,
    // and rewriting the name first would hide it from that test.
    expect(measurementWords("dbos.workflow_status missing"))
      .toBe("dbos.workflow_status missing");
    expect(namesDatabaseObject(measurementWords("dbos.workflow_status missing")))
      .toBe(true);
  });

  it("turns a code this console has no word for into its own words", () => {
    expect(measurementWords("4 in publish_backlog")).toBe("4 in publish backlog");
  });

  it("changes nothing else", () => {
    expect(measurementWords("0")).toBe("0");
    expect(measurementWords("412 rows of an expected 2,414"))
      .toBe("412 rows of an expected 2,414");
    expect(measurementWords(null)).toBe("");
  });
});

describe("a share of a total", () => {
  // okg-workspace holds 2,050 `blocks` against 1,037,093 relationships. The
  // Contents panel rounded that to 0 and printed "0% of relationships" beside
  // a count of two thousand.
  it("never prints a zero for a count that is not zero", () => {
    expect(shareLabel(2050, 1037093)).toBe("<1%");
    expect(shareLabel(1, 1000000)).toBe("<1%");
  });

  it("prints a zero for a count that is zero", () => {
    expect(shareLabel(0, 1000)).toBe("0%");
    expect(shareLabel(5, 0)).toBe("0%");
  });

  it("rounds to whole percents everywhere else", () => {
    expect(shareLabel(2414, 2605)).toBe("93%");
    expect(shareLabel(1, 200)).toBe("1%");
    expect(shareLabel(1, 201)).toBe("<1%");
    expect(shareLabel(1000, 1000)).toBe("100%");
  });
});
