"""Issue #3592: undoing a killed run must not leave a later run's checkpoint
claiming records the graph no longer has.

The source here is shaped like the issue's external connectors: it keeps a
content hash per record in its checkpoint, emits facts only for records whose
hash changed, and declares its whole record set every run. The issue's steps:

1. publish;
2. change the input, start an ingest, and kill it after it has committed a
   chunk;
3. rerun -- every fact matches a row the killed run left;
4. ``okg runtime finish-undo --apply`` undoes the killed run;
5. rerun -- the checkpoint says nothing changed, so nothing is emitted.

Before the fix, step 5 published with the killed run's new records missing
and its changed records back at their old text.
"""
from __future__ import annotations

import json
import sys
from typing import Any

import pytest
import yaml

from tests.substrate.ingest.test_run_base_undo import (
    AAA,
    DEPLOYMENT,
    _Fixture,
    _Kill,
    _pending,
    _records,
)
from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

pytestmark = pytest.mark.postgres

from okg.substrate.ingest.runner import SourceRunner as _Runner  # noqa: E402

_REAL_ADVANCE = _Runner._advance_cursor_watermark_boundary

RECORDS = 300
CHANGED = 60
ADDED = 40

_ADAPTER = '''
import hashlib
import json
from pathlib import Path

from okg.substrate.library.sources.base import (
    NodeFact,
    ProgressMarker,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


class HashCheckpointSource:
    """Emits facts only for records whose content hash differs from the one
    in its checkpoint; declares its whole record set every run."""

    name = "hash_checkpoint"
    profile = "discovery_crawl"
    last_run_stats: dict = {}

    def __init__(self, spec_path, prefix, **kwargs):
        self.spec_path = Path(spec_path)
        self.prefix = prefix

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, **kwargs):
        spec = json.loads(self.spec_path.read_text())
        prior = dict((cursor or {}).get("hashes") or {})
        hashes = {
            str(key): hashlib.sha256(text.encode()).hexdigest()[:16]
            for key, text in spec["records"]
        }
        changed = [
            (key, text) for key, text in spec["records"]
            if prior.get(str(key)) != hashes[str(key)]
        ]

        def node_id(key):
            return "%s:%s" % (self.prefix, key)

        def facts():
            for key, text in changed:
                yield NodeFact(
                    node_id=node_id(key),
                    subtype="document",
                    attrs={"text": text},
                    source_record_id={"id": str(key)},
                    source_revision={"h": hashes[str(key)]},
                )

        record_set = RecordSet(records={
            str(key): RecordEmission(primary=RecordNode(
                node_id=node_id(key),
                subtype="document",
                source_record_id={"id": str(key)},
                source_revision={"h": hashes[str(key)]},
            ))
            for key, _text in spec["records"]
        })
        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={"hashes": hashes},
            run_mode=mode,
            record_set=record_set,
        )


class ProgressMarkerSource(HashCheckpointSource):
    """The same records, but a record's hash is kept as a per-record
    progress marker (saved with the chunk that wrote it) instead of in the
    checkpoint."""

    name = "progress_marker"

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, since_progress=None, **kwargs):
        spec = json.loads(self.spec_path.read_text())
        prior = dict(since_progress or {})
        hashes = {
            str(key): hashlib.sha256(text.encode()).hexdigest()[:16]
            for key, text in spec["records"]
        }

        def facts():
            for key, text in spec["records"]:
                if prior.get("record:%s" % key) == hashes[str(key)]:
                    continue
                yield NodeFact(
                    node_id="%s:%s" % (self.prefix, key),
                    subtype="document",
                    attrs={"text": text},
                    source_record_id={"id": str(key)},
                    source_revision={"h": hashes[str(key)]},
                )
                yield ProgressMarker(record_id=str(key), fingerprint=hashes[str(key)])

        run = super().run(run_id, mode=mode, cursor=None)
        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={},
            run_mode=mode,
            record_set=run.record_set,
        )
'''


def _fixture(tmp_path, dsn, monkeypatch, adapter_class: str) -> _Fixture:
    """The run-base fixture with source aaa swapped for ``adapter_class``,
    holding RECORDS records, published once."""
    fixture = _Fixture(tmp_path, dsn, monkeypatch)
    (tmp_path / "adapters" / "hash_checkpoint_adapter.py").write_text(
        _ADAPTER, encoding="utf-8",
    )
    sys.modules.pop("hash_checkpoint_adapter", None)
    registry_path = fixture.dep / "source_registry.yaml"
    data = yaml.safe_load(registry_path.read_text())
    data["sources"]["aaa"]["module"] = "hash_checkpoint_adapter"
    data["sources"]["aaa"]["class"] = adapter_class
    registry_path.write_text(yaml.safe_dump(data))
    _claim_layered_fixture_catalog(dsn, fixture.dep)
    fixture.write("aaa", {"records": _records(RECORDS)})
    first = fixture.ingest()
    assert first.any_failed is False, first.report
    assert fixture.published()["count"] == RECORDS
    return fixture


@pytest.fixture
def hx(tmp_path, clean_runtime, monkeypatch) -> _Fixture:
    return _fixture(tmp_path, clean_runtime, monkeypatch, "HashCheckpointSource")


@pytest.fixture
def px(tmp_path, clean_runtime, monkeypatch) -> _Fixture:
    return _fixture(tmp_path, clean_runtime, monkeypatch, "ProgressMarkerSource")


def _changed_input() -> list:
    """Step 2's input: CHANGED records rewritten and ADDED records new."""
    changed = {i: f"document {i}, edited" for i in range(CHANGED)}
    return _records(RECORDS + ADDED, changed=changed)


def _expected(records: list) -> dict[str, str]:
    return {f"aaa:{key}": text for key, text in records}


def _kill_after_staging(monkeypatch) -> None:
    """Kill the run after every chunk is on the branch, before its admission
    and checkpoint are final (the issue's "kill -9 after staging begins")."""
    def advance(self, **kwargs):
        raise _Kill()

    monkeypatch.setattr(_Runner, "_advance_cursor_watermark_boundary", advance)


def _finish_undo(dsn: str, run_id: str) -> dict[str, Any]:
    from okg.substrate.ingest.run_base import finish_pending_undos

    return finish_pending_undos(
        dsn=dsn, deployment=DEPLOYMENT, apply=True, run_id=run_id,
    )


def _ingest(fx: _Fixture, *, reset_cursor: bool = False) -> Any:
    from okg.substrate.ingest.service import IngestOptions, run_ingest

    return run_ingest(IngestOptions(
        dsn=fx.dsn,
        deployment=DEPLOYMENT,
        deployment_dir=fx.dep,
        include={"aaa"},
        include_predecessors=False,
        include_dependents=False,
        publish=True,
        reset_cursor=reset_cursor,
    ))


def _aaa_result(outcome: Any) -> dict[str, Any]:
    [result] = [r for r in outcome.report["results"] if r["name"] == "aaa"]
    return result


def _kill_then_rerun(hx: _Fixture, monkeypatch) -> tuple[list, str, Any]:
    """Steps 2 and 3: returns the input, the killed run's id and the
    rerun's outcome."""
    records = _changed_input()
    hx.write("aaa", {"records": records})
    _kill_after_staging(monkeypatch)
    with pytest.raises(_Kill):
        _ingest(hx)
    monkeypatch.setattr(
        _Runner, "_advance_cursor_watermark_boundary", _REAL_ADVANCE,
    )
    assert hx.head_count() == RECORDS + ADDED
    pending = _pending(hx.dsn)
    assert [item["source"] for item in pending] == [AAA], pending
    return records, pending[0]["run_id"], _ingest(hx)


def _undo(hx: _Fixture, killed: str) -> dict[str, Any]:
    """Step 4."""
    done = _finish_undo(hx.dsn, killed)
    assert [r["status"] for r in done["results"]] == ["undone"], done
    assert done["still_pending"] == [], done
    assert hx.head_count() == RECORDS
    return done


def test_a_rerun_waits_for_the_killed_runs_undo_and_publishes_every_record(
    hx: _Fixture, monkeypatch,
) -> None:
    """The issue's five steps. Before the fix, step 3 wrote nothing and saved
    its checkpoint, and step 5 left readers 300 records with old text while
    the checkpoint claimed 340."""
    records, killed, rerun = _kill_then_rerun(hx, monkeypatch)

    # Step 3: refused before it reads, naming the command that unblocks it.
    result = _aaa_result(rerun)
    assert result["status"] == "FAIL", result
    assert "unfinished undo" in result["error"], result
    hint = json.dumps(rerun.report, default=str)
    assert (
        f"okg runtime finish-undo --deployment {DEPLOYMENT} --run-id {killed} "
        "--apply"
    ) in hint
    assert hx.head_count() == RECORDS + ADDED

    done = _undo(hx, killed)
    # No run saved a checkpoint on top of the killed run's rows.
    assert done["results"][0]["stats"]["checkpoint_invalidated"] is False

    # Step 5: the checkpoint is still the one from before the killed run, so
    # the connector emits the 100 changed records again.
    final = _ingest(hx)
    assert final.any_failed is False, final.report
    assert _aaa_result(final)["nodes_appended"] == CHANGED + ADDED
    published = hx.published()
    assert published["count"] == RECORDS + ADDED
    assert published["texts"] == _expected(records)
    assert _pending(hx.dsn) == []


def test_an_undo_under_a_later_checkpoint_makes_the_next_run_refuse_loudly(
    hx: _Fixture, monkeypatch,
) -> None:
    """The gate. If a rerun saves its checkpoint on top of the killed run's
    rows anyway (here the runner's new guard is switched off to reach the
    old order), the undo marks that checkpoint unverified: the next run
    proves its record set against the graph and refuses instead of
    publishing 300 of 340 records, and --reset-cursor repairs it."""
    monkeypatch.setattr(
        _Runner, "_refuse_rerun_while_undo_unfinished",
        lambda self, run_id: None, raising=False,
    )
    records, killed, rerun = _kill_then_rerun(hx, monkeypatch)
    assert _aaa_result(rerun)["status"] == "OK", rerun.report
    assert _aaa_result(rerun)["nodes_appended"] == 0

    done = _undo(hx, killed)

    before = hx.published()
    assert before["count"] == RECORDS
    refused = _ingest(hx)
    result = _aaa_result(refused)
    assert result["status"] == "FAIL", result
    assert "record_set coverage check failed" in result["error"], result
    after = hx.published()
    assert (after["count"], after["texts"]) == (before["count"], before["texts"])
    assert done["results"][0]["stats"]["checkpoint_invalidated"] is True

    repaired = _ingest(hx, reset_cursor=True)
    assert _aaa_result(repaired)["status"] == "OK", repaired.report
    published = hx.published()
    assert published["count"] == RECORDS + ADDED
    assert published["texts"] == _expected(records)


def test_progress_markers_of_the_undone_run_are_forgotten(
    px: _Fixture, monkeypatch,
) -> None:
    """The same steps for a source that keeps per-record progress markers
    (saved with each chunk, so a killed run leaves them). Before the undo
    forgot them, step 5 skipped every record the killed run had marked and
    the undo had removed."""
    import psycopg

    kept = [
        (DEPLOYMENT, AAA, "sync.k1_probe"),
        ("other", AAA, "record:0"),
    ]

    def progress(keys) -> int:
        with psycopg.connect(px.dsn, autocommit=True) as conn:
            return sum(
                conn.execute(
                    "SELECT count(*) FROM okg.source_progress WHERE "
                    "deployment = %s AND source_name = %s AND key = %s",
                    key,
                ).fetchone()[0]
                for key in keys
            )

    def write_kept() -> None:
        with psycopg.connect(px.dsn, autocommit=True) as conn:
            for key in kept:
                conn.execute(
                    "INSERT INTO okg.source_progress "
                    "(deployment, source_name, key, value, updated_at) "
                    "VALUES (%s, %s, %s, '\"x\"', now())",
                    key,
                )

    records, killed, rerun = _kill_then_rerun(px, monkeypatch)
    assert _aaa_result(rerun)["status"] == "FAIL", rerun.report
    write_kept()  # saved after the cut, like the killed run's markers
    assert progress(kept) == 2

    done = _undo(px, killed)
    assert done["results"][0]["stats"]["progress_keys_forgotten"] == (
        CHANGED + ADDED
    )
    # Runner-owned sync keys and other deployments' progress stay.
    assert progress(kept) == 2

    final = _ingest(px)
    assert final.any_failed is False, final.report
    assert _aaa_result(final)["nodes_appended"] == CHANGED + ADDED
    published = px.published()
    assert published["count"] == RECORDS + ADDED
    assert published["texts"] == _expected(records)
