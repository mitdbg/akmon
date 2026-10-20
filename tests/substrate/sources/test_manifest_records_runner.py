"""The source-manifest reader driven through the real SourceRunner.

PACT no-code-source-manifest, review of 44cc17c646. The reader returns an
exact applied token, so the runner manages its change identity; these tests
prove against a real database that:

- a read the runner must refuse names its real cause to the operator and
  leaves the stored cursor unchanged;
- a read with bad rows applies the valid records and removes nothing,
  under `deletion: missing_from_full_read`;
- the next clean read applies normally, including a real deletion.
"""
from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.manifest_records import ManifestRecordSource
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres

SOURCE = "test.manifest-runner"

MANIFEST = """
manifest: okg.source/v1
read:
  files: {files}
  records: issues
types:
  tracker_ticket:
    key: "{{key}}"
    title: summary
    fields:
      votes: {{ from: votes, type: integer }}
    links:
      blocks: {{ to: tracker_ticket, each: blocks, key: "{{.}}" }}
deletion: {deletion}
"""


def _ticket(key: str, *, summary: str = "", votes: Any = 1, blocks=()) -> dict:
    return {"key": key, "summary": summary or f"Ticket {key}", "votes": votes, "blocks": list(blocks)}


class _Fixture:
    def __init__(self, tmp_path: Path, dsn: str, deletion: str = "missing_from_full_read") -> None:
        self.dsn = dsn
        self.data = tmp_path / "data"
        self.data.mkdir()
        self.manifest = tmp_path / "tickets.source.yaml"
        self.manifest.write_text(textwrap.dedent(MANIFEST.format(
            files=str(self.data / "*.json"), deletion=deletion,
        )))
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog_version(conn)

    def write(self, tickets: list[dict]) -> None:
        (self.data / "a.json").write_text(json.dumps({"issues": tickets}))

    def run(self):
        runner = SourceRunner(self.dsn, source_name=SOURCE, write_fact_log=True)
        return runner.run(ManifestRecordSource(str(self.manifest), "tickets"), mode="scope_complete")

    def watermark(self) -> Any:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks WHERE source = %s", (SOURCE,),
            ).fetchone()
        return None if row is None else row[0]

    def retracted(self) -> list[str]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT node_id FROM okg.node_facts WHERE source = %s AND op = 'R' ORDER BY node_id",
                (SOURCE,),
            ).fetchall()
        return [row[0] for row in rows]

    def edge_retractions(self) -> list[tuple[str, str, str]]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            return conn.execute(
                "SELECT src, edge_type, dst FROM okg.edge_facts WHERE source = %s AND op = 'R'",
                (SOURCE,),
            ).fetchall()


def test_a_refused_read_names_its_cause_and_keeps_the_cursor(clean_runtime: str, tmp_path: Path) -> None:
    fx = _Fixture(tmp_path, clean_runtime)
    fx.write([_ticket("T-1"), _ticket("T-2")])
    first = fx.run()
    assert first.nodes_appended == 2
    before = fx.watermark()
    fx.write([_ticket("T-1"), _ticket("T-1", summary="again")])
    with pytest.raises(SubstrateError) as caught:
        fx.run()
    message = str(caught.value)
    assert "appears twice in one file" in message and "nothing was applied or removed" in message
    assert "cannot acknowledge change identity" not in message
    assert fx.watermark() == before


def test_bad_rows_apply_the_valid_records_and_remove_nothing(clean_runtime: str, tmp_path: Path) -> None:
    fx = _Fixture(tmp_path, clean_runtime)
    fx.write([_ticket("T-1", blocks=("T-2",)), _ticket("T-2"), _ticket("T-3")])
    fx.run()
    # T-1 edited; T-2 now has an unconvertible value; T-3 left the export.
    fx.write([_ticket("T-1", summary="Edited", blocks=("T-2",)), _ticket("T-2", votes="many")])
    result = fx.run()
    assert result.completed_scope
    assert result.nodes_appended == 1  # the edited T-1
    assert result.retractions_node == 0 and result.retractions_edge == 0
    assert fx.retracted() == []
    problems = result.next_cursor["problems"]
    assert any("tracker_ticket:T-2" in p and "votes" in p for p in problems)
    # The link to the withheld T-2 is kept, not dropped.
    emission = result.next_cursor["emissions"]["tracker_ticket:T-1"]
    assert [e["dst"] for e in emission["edges"]] == ["tickets:tracker_ticket:T-2"]
    assert {"tracker_ticket:T-2", "tracker_ticket:T-3"} <= set(result.next_cursor["emissions"])


def test_the_next_clean_read_applies_normally_with_a_real_deletion(clean_runtime: str, tmp_path: Path) -> None:
    fx = _Fixture(tmp_path, clean_runtime)
    fx.write([_ticket("T-1", blocks=("T-2",)), _ticket("T-2"), _ticket("T-3")])
    fx.run()
    fx.write([_ticket("T-1", blocks=("T-2",)), _ticket("T-2", votes="many")])
    withheld = fx.run()
    assert withheld.retractions_node == 0
    fx.write([_ticket("T-1"), _ticket("T-2", votes=5)])  # fixed; T-3 still gone
    clean = fx.run()
    assert clean.completed_scope
    assert "problems" not in clean.next_cursor
    assert clean.retractions_node == 1
    assert fx.retracted() == ["tickets:tracker_ticket:T-3"]
    assert ("tickets:tracker_ticket:T-1", "blocks", "tickets:tracker_ticket:T-2") in fx.edge_retractions()


def test_never_mode_treats_bad_rows_as_warnings(clean_runtime: str, tmp_path: Path) -> None:
    fx = _Fixture(tmp_path, clean_runtime, deletion="never")
    fx.write([_ticket("T-1"), _ticket("T-2")])
    fx.run()
    fx.write([_ticket("T-1", summary="Edited"), _ticket("T-9", votes="many")])
    result = fx.run()
    assert result.completed_scope and result.nodes_appended == 1
    assert result.retractions_node == 0
    assert "tracker_ticket:T-2" in result.next_cursor["emissions"]


# --------------------------------------------------------------------------
# PACT no-code-source-manifest-http
# --------------------------------------------------------------------------


def test_reset_cursor_under_never_refuses_instead_of_dropping_kept_records(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Phase 1 review N3: a `--reset-cursor` run hands the reader an empty
    earlier record set, and the runner's stray pass would retract every
    record `deletion: never` keeps. The runner now says the earlier
    footprint is unknown and the reader refuses."""
    fx = _Fixture(tmp_path, clean_runtime, deletion="never")
    fx.write([_ticket("T-1"), _ticket("T-2")])
    fx.run()
    fx.write([_ticket("T-1")])  # T-2 left the export; `never` keeps it
    before = fx.watermark()
    runner = SourceRunner(fx.dsn, source_name=SOURCE, write_fact_log=True)
    with pytest.raises(SubstrateError) as caught:
        runner.run(
            ManifestRecordSource(str(fx.manifest), "tickets"),
            mode="scope_complete", reset_cursor=True,
        )
    assert "earlier record set is unknown" in str(caught.value)
    assert "--reset-cursor" in str(caught.value)
    assert fx.retracted() == []
    assert fx.watermark() == before
    # Without the reset the source keeps working and still keeps T-2.
    result = fx.run()
    assert result.retractions_node == 0
    assert "tracker_ticket:T-2" in result.next_cursor["emissions"]


def test_reset_cursor_on_a_fresh_source_still_runs(clean_runtime: str, tmp_path: Path) -> None:
    """With no earlier rows there is nothing to drop: the reset run applies."""
    fx = _Fixture(tmp_path, clean_runtime, deletion="never")
    fx.write([_ticket("T-1")])
    runner = SourceRunner(fx.dsn, source_name=SOURCE, write_fact_log=True)
    result = runner.run(
        ManifestRecordSource(str(fx.manifest), "tickets"),
        mode="scope_complete", reset_cursor=True,
    )
    assert result.nodes_appended == 1


def test_http_read_unchanged_on_304_is_skipped_by_the_runner(clean_runtime: str, tmp_path: Path) -> None:
    from tests.substrate.sources._manifest_http_server import LocalServer, json_response

    state = {"etag": '"v1"', "rows": [_ticket("T-1"), _ticket("T-2")]}

    def route(request):
        if request.headers.get("If-None-Match") == state["etag"]:
            return 304, {"ETag": state["etag"]}, b""
        return json_response({"issues": state["rows"]}, headers={"ETag": state["etag"]})

    server = LocalServer({"/tickets": route})
    try:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            seed_catalog_version(conn)
        manifest = tmp_path / "api.source.yaml"
        manifest.write_text(textwrap.dedent(f"""
            manifest: okg.source/v1
            types:
              tracker_ticket:
                read:
                  url: {server.base}/tickets
                  records: issues
                key: "{{key}}"
                title: summary
        """))

        def run():
            runner = SourceRunner(clean_runtime, source_name=SOURCE, write_fact_log=True)
            return runner.run(ManifestRecordSource(str(manifest), "tickets"), mode="scope_complete")

        first = run()
        assert first.nodes_appended == 2
        fetches = sum(1 for r in server.requests if "If-None-Match" not in r.headers)
        second = run()
        assert second.mode == "no_change"
        assert second.nodes_appended == 0
        assert sum(1 for r in server.requests if "If-None-Match" not in r.headers) == fetches
        state.update(etag='"v2"', rows=[_ticket("T-1", summary="Edited"), _ticket("T-2")])
        third = run()
        assert third.mode != "no_change" and third.nodes_appended == 1
    finally:
        server.close()
