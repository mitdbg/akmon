"""Issue #2993: a failed source run is undone to its run-base checkpoint.

Before this change, compensation of a failed run deleted every row stamped
with the run's id. It never put back a row the run deleted, and it deleted a
pre-existing row the run had only updated (an update re-stamps the row). The
next publish of ANOTHER source then showed readers the damage.

Now every source run cuts a private run-base checkpoint first, and the undo
writes each key the run touched back to its run-base version. These tests
drive the real ingest service and publisher on PostgreSQL (the issue's
case, through the main loop and through the tail flush, and for updated
records), the undo itself at the graph-store level (within-run sequences,
shared-node hand-offs, conflicts, paging and rollback), the publish guard
and its resolve command, the run base's lifecycle, and every checkpoint
lookup that had to learn to ignore run bases.
"""
from __future__ import annotations

import json
import sys
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.service import IngestOptions, run_ingest
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.source_jobs_inline import use_inline_source_jobs
from tests.substrate.test_ingest_service import (
    _claim_layered_fixture_catalog,
    _fixture_source_privacy_policy,
    _source,
    _write_layered_fixture_deployment,
)

pytestmark = pytest.mark.postgres

from okg.substrate.ingest.compensation import (  # noqa: E402
    undo_run_to_run_base as _REAL_UNDO,
)
from okg.substrate.ingest.runner import SourceRunner as _Runner  # noqa: E402

_REAL_FLUSH = _Runner._flush_chunk

DEPLOYMENT = "svc"
AAA = "svc.aaa"
BBB = "svc.bbb"
AAA_RECORDS = 65
LIMIT = 20

# ---------------------------------------------------------------------------
# The ingest-service fixture: two sources on one deployment
# ---------------------------------------------------------------------------

_ADAPTER = '''
import json
from pathlib import Path

from okg.substrate.library.sources.base import NodeFact, SourceRun


class SpecSource:
    """Streams exactly what a JSON spec lists: retractions, then records."""

    name = "spec"
    profile = "discovery_crawl"
    last_run_stats: dict = {}

    def __init__(self, spec_path, prefix, **kwargs):
        self.spec_path = Path(spec_path)
        self.prefix = prefix

    def run(self, run_id, *, mode="scope_complete", cursor=None,
            sync_scope=None, **kwargs):
        spec = json.loads(self.spec_path.read_text())

        def facts():
            for key in spec.get("retract", []):
                yield NodeFact(
                    node_id="%s:%s" % (self.prefix, key),
                    subtype="document",
                    attrs={},
                    source_record_id={"id": str(key)},
                    source_revision={},
                    op="R",
                )
            for key, text in spec["records"]:
                yield NodeFact(
                    node_id="%s:%s" % (self.prefix, key),
                    subtype="document",
                    attrs={"text": text},
                    source_record_id={"id": str(key)},
                    source_revision={"v": text},
                )

        return SourceRun(
            facts=facts(),
            completed_scope=True,
            next_cursor={"n": len(spec["records"])},
            run_mode=mode,
        )
'''


def _records(count: int, *, changed: dict[int, str] | None = None) -> list:
    changed = changed or {}
    return [[i, changed.get(i, f"document {i}")] for i in range(count)]


class _Fixture:
    def __init__(self, tmp_path: Path, dsn: str, monkeypatch) -> None:
        module_dir = tmp_path / "adapters"
        module_dir.mkdir()
        (module_dir / "spec_source_adapter.py").write_text(
            _ADAPTER, encoding="utf-8",
        )
        monkeypatch.syspath_prepend(str(module_dir))
        sys.modules.pop("spec_source_adapter", None)
        # Sources run in this process, so the injected flush failures and
        # kills reach the runner (production spawns a child per source; the
        # real CLI run in the evidence covers that path).
        use_inline_source_jobs(monkeypatch)
        with psycopg.connect(dsn, autocommit=True) as conn:
            seed_catalog(conn, subtypes=["document"])
        self.dsn = dsn
        self.bbb_edits = 0
        self.dep = _write_layered_fixture_deployment(tmp_path)
        # Outside the deployment repository: the publisher refuses
        # uncommitted provenance inputs, and the tests rewrite these.
        self.spec = {
            "aaa": tmp_path / "aaa.json",
            "bbb": tmp_path / "bbb.json",
        }
        self.write("aaa", {"records": _records(AAA_RECORDS)})
        self.write("bbb", {"records": _records(3)})
        registry_path = self.dep / "source_registry.yaml"
        data = yaml.safe_load(registry_path.read_text())
        data["sources"] = {
            name: {
                **_source(ownership),
                "module": "spec_source_adapter",
                "class": "SpecSource",
                "params": {"spec_path": str(self.spec[name]), "prefix": name},
                "source_policy": _fixture_source_privacy_policy(),
            }
            for name, ownership in (("aaa", AAA), ("bbb", BBB))
        }
        registry_path.write_text(yaml.safe_dump(data))
        _claim_layered_fixture_catalog(dsn, self.dep)
        # Lower the runner's chunk and the FactChunk bound together, so a
        # few dozen records cross several committed chunks.
        import okg.substrate.fact_batch as fact_batch_module
        import okg.substrate.ingest.runner as runner_module

        monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", LIMIT)
        monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", LIMIT)

    def write(self, name: str, spec: dict) -> None:
        self.spec[name].write_text(json.dumps(spec))

    def ingest(self, include: set[str] | None = None):
        return run_ingest(IngestOptions(
            dsn=self.dsn,
            deployment=DEPLOYMENT,
            deployment_dir=self.dep,
            include=include,
            include_predecessors=False,
            include_dependents=False,
            publish=True,
        ))

    def published(self) -> dict[str, Any]:
        """What readers see: the latest published generation's aaa rows."""
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            latest = store.latest_generation(deployment_name=DEPLOYMENT)
            rows = store.checkout_generation(latest.checkpoint_id).query(
                "SELECT node_id, attrs FROM okg.graph_nodes WHERE source = :s",
                {"s": AAA},
            )
            return {
                "generation": latest.checkpoint_id,
                "count": len(rows),
                "texts": {r["node_id"]: r["attrs"].get("text") for r in rows},
            }

    def head_count(self) -> int:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            return int(store.checkout().query(
                "SELECT count(*) AS n FROM okg.graph_nodes WHERE source = :s",
                {"s": AAA},
            )[0]["n"])

    def run_bases(self) -> list[str]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT checkpoint_id FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id LIKE 'okg-run-base:%%'"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def receipts(self) -> dict[str, int]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT state, count(*) FROM okg.fact_batch_receipts GROUP BY state"
            ).fetchall()
        return {row[0]: row[1] for row in rows}


def _fail_flush(monkeypatch, *, when) -> list[str]:
    """Fail the first ``_flush_chunk`` call for which ``when`` says so."""
    from okg.substrate.ingest.runner import SourceRunner

    seen: list[str] = []

    def flush(self, facts, markers, **kwargs):
        kind = (
            "tail" if facts and all(getattr(f, "op", "I") == "R" for f in facts)
            and len(seen) > 0 else "main"
        )
        seen.append(kind)
        if when(seen):
            raise RuntimeError(f"injected failure on flush {len(seen)}")
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(SourceRunner, "_flush_chunk", flush)
    return seen


@pytest.fixture
def fx(tmp_path, clean_runtime, monkeypatch) -> _Fixture:
    fixture = _Fixture(tmp_path, clean_runtime, monkeypatch)
    first = fixture.ingest()
    assert first.any_failed is False, first.report
    published = fixture.published()
    assert published["count"] == AAA_RECORDS, published
    assert fixture.run_bases() == [], "a successful run left its run base"
    return fixture


def _publish_bbb_alone(fx: _Fixture, monkeypatch) -> dict[str, Any]:
    """Publish ANOTHER source alone, with every injected fault removed."""
    import okg.substrate.ingest.compensation as compensation
    from okg.substrate.ingest.runner import SourceRunner

    monkeypatch.setattr(SourceRunner, "_flush_chunk", _REAL_FLUSH)
    monkeypatch.setattr(compensation, "undo_run_to_run_base", _REAL_UNDO)
    fx.bbb_edits += 1
    fx.write("bbb", {
        "records": _records(3, changed={0: f"bbb moved on {fx.bbb_edits}"}),
    })
    outcome = fx.ingest({"bbb"})
    return outcome.report


def _published_only(report: dict[str, Any]) -> bool:
    """At least one publish, and every one that ran published (a layer may
    add a no-work skip)."""
    statuses = [
        str(p["status"]) for p in report["publishes"]
        if p.get("status") != "skipped"
    ]
    return bool(statuses) and set(statuses) == {"published"}


# ---------------------------------------------------------------------------
# (a) the issue's case, (b) updated records, through the real service
# ---------------------------------------------------------------------------


def test_removals_committed_by_a_failed_run_never_reach_readers(
    fx: _Fixture, monkeypatch,
) -> None:
    """The issue: 20 removals commit in chunk 1, chunk 2 fails, then bbb
    publishes alone. Readers must still see all 65 of aaa's records."""
    fx.write("aaa", {
        "retract": list(range(20)),
        "records": _records(AAA_RECORDS)[20:],
    })
    seen = _fail_flush(monkeypatch, when=lambda s: len(s) == 2)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert seen == ["main", "main"]
    assert fx.receipts().get("compensating", 0) == 0, fx.receipts()
    assert fx.head_count() == AAA_RECORDS
    assert fx.run_bases() == [], "the completed undo left its run base"

    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    after = fx.published()
    assert after["count"] == AAA_RECORDS, after


def test_a_tail_flush_failure_restores_what_earlier_chunks_changed(
    fx: _Fixture, monkeypatch,
) -> None:
    """Through the tail flush: chunk 1 updates 5 records and commits, the
    end-of-run removals fail. Readers keep 65 records with their text."""
    changed = {i: f"rewritten {i}" for i in range(5)}
    fx.write("aaa", {"records": _records(45, changed=changed)})
    seen = _fail_flush(monkeypatch, when=lambda s: s[-1] == "tail")
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert seen[-1] == "tail" and len(seen) >= 3, seen
    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    after = fx.published()
    assert after["count"] == AAA_RECORDS, after
    assert after["texts"]["aaa:0"] == "document 0", after["texts"]["aaa:0"]


def test_records_a_failed_run_updated_come_back_as_they_were(
    fx: _Fixture, monkeypatch,
) -> None:
    """Updated records: the old compensation deleted them (the update
    re-stamped them with the failed run's id); the undo restores them."""
    changed = {i: f"rewritten {i}" for i in range(5)}
    fx.write("aaa", {"records": _records(AAA_RECORDS, changed=changed)})
    _fail_flush(monkeypatch, when=lambda s: len(s) == 2)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert fx.head_count() == AAA_RECORDS
    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    after = fx.published()
    assert after["count"] == AAA_RECORDS, after
    assert {
        key: after["texts"][f"aaa:{key}"] for key in range(5)
    } == {key: f"document {key}" for key in range(5)}


def test_two_writing_runs_keep_the_fast_dirty_scope_hint(
    fx: _Fixture,
) -> None:
    """(a) Both sources write, so two run bases split the head before the
    publish. The sources' changed-row hint still lists every row written
    since the previous generation, so the publish uses it (no
    segments_since_previous_exceed_hint) and its counts match."""
    fx.write("aaa", {"records": _records(AAA_RECORDS, changed={1: "a new"})})
    fx.write("bbb", {"records": _records(3, changed={2: "b new"})})
    outcome = fx.ingest()
    assert outcome.any_failed is False, outcome.report
    published = [
        p for p in outcome.report["publishes"] if p.get("status") == "published"
    ]
    assert published, outcome.report["publishes"]
    decision = published[-1]["dirty_scope_hint"]
    assert decision["used"] is True, decision
    assert len(decision.get("segment_ids") or []) >= 2, decision
    assert fx.published()["texts"]["aaa:1"] == "a new"


def test_a_run_that_writes_nothing_leaves_the_redrive_token_alone(
    fx: _Fixture,
) -> None:
    """(b) The guarded-redrive token (segment id plus the head segment's
    rows) changes only when a run writes: the run base is cut at the first
    graph write, so an unchanged source run splits nothing."""
    from okg.substrate.publisher import Publisher

    def token() -> str:
        with psycopg.connect(fx.dsn, autocommit=True) as conn:
            return Publisher.chronos_graph_branch_mutation_token(
                conn, branch_name="default",
            )

    before = token()
    fx.write("aaa", {"records": _records(AAA_RECORDS)})
    unchanged = fx.ingest({"aaa"})
    assert unchanged.any_failed is False, unchanged.report
    assert token() == before
    fx.write("aaa", {"records": _records(AAA_RECORDS, changed={3: "moved"})})
    wrote = fx.ingest({"aaa"})
    assert wrote.any_failed is False, wrote.report
    assert fx.run_bases() == []


def test_a_run_base_exists_only_while_its_run_does(
    fx: _Fixture, monkeypatch,
) -> None:
    from okg.substrate.ingest.runner import SourceRunner

    during: list[list[str]] = []
    real = SourceRunner._flush_chunk

    def flush(self, facts, markers, **kwargs):
        during.append(fx.run_bases())
        return real(self, facts, markers, **kwargs)

    monkeypatch.setattr(SourceRunner, "_flush_chunk", flush)
    fx.write("aaa", {"records": _records(AAA_RECORDS, changed={1: "new"})})
    ok = fx.ingest({"aaa"})
    assert ok.any_failed is False, ok.report
    # Cut just before the run's first graph write (inside the first flush),
    # then present for every later flush.
    assert during[0] == [], during
    assert len(during) > 1 and all(len(names) == 1 for names in during[1:])
    assert during[1][0].startswith("okg-run-base:default:")
    assert fx.run_bases() == []


# ---------------------------------------------------------------------------
# (e) the publish guard, through the real publisher, and its way out
# ---------------------------------------------------------------------------


def test_an_unfinished_undo_refuses_every_publish_until_resolved(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    import okg.substrate.ingest.compensation as compensation
    from okg.substrate.cli import runtime as cli_runtime
    from okg.substrate.cli.main import _build_parser

    before = fx.published()
    fx.write("aaa", {
        "retract": list(range(20)),
        "records": _records(AAA_RECORDS)[20:],
    })
    _fail_flush(monkeypatch, when=lambda s: len(s) == 2)

    def crashed(*_args, **_kwargs):
        raise RuntimeError("injected crash inside the undo")

    monkeypatch.setattr(compensation, "undo_run_to_run_base", crashed)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert fx.receipts().get("compensating", 0) >= 1, fx.receipts()
    assert len(fx.run_bases()) == 1

    # A new process would see the same durable state: publish bbb alone.
    report = _publish_bbb_alone(fx, monkeypatch)  # also drops the crash
    publish = report["publishes"][0]
    assert publish["status"] == "fail", publish
    assert publish["code"] == "source_run_undo_unfinished", publish
    assert (
        "okg runtime finish-undo --deployment svc --apply" in publish["fix_hint"]
    ), publish["fix_hint"]
    assert "okg ingest --deployment svc --include svc.aaa" in publish["fix_hint"]
    assert fx.published()["generation"] == before["generation"]

    # The resolve command: retry the undo.
    args = _build_parser().parse_args([
        "runtime", "finish-undo", "--deployment", DEPLOYMENT, "--apply",
        "--json",
    ])
    args.dsn = fx.dsn
    # The fixture deployment has no runtime; finish-undo must not need one.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    capsys.readouterr()  # drop what the ingests above printed
    rc = cli_runtime._cmd_runtime(args, as_json=True)
    printed = capsys.readouterr()
    assert rc == 0, (printed.out, printed.err)
    payload = json.loads(printed.out)
    assert [r["status"] for r in payload["results"]] == ["undone"], payload
    assert payload["still_pending"] == []
    assert fx.run_bases() == []
    assert fx.head_count() == AAA_RECORDS

    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    assert fx.published()["count"] == AAA_RECORDS


# ---------------------------------------------------------------------------
# The undo at the graph-store level
# ---------------------------------------------------------------------------

SOURCE = "repo.gamma"
OTHER = "repo.alpha"
BAD_RUN = "run-gamma-bad"
GOOD_RUN = "run-gamma-good"
SHARED_OWNED = "author:owned-by-gamma@example.org"
SHARED_FOREIGN = "author:owned-by-alpha@example.org"


def _node(node_id: str, subtype: str = "claim", *, op: str = "I", **attrs):
    return NodeFact(
        node_id, subtype,
        {} if op == "R" else {"text": f"text of {node_id}", **attrs},
        {"id": node_id}, {"rev": attrs.get("rev", 1)}, op=op,
    )


def _author(node_id: str, source: str, *, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id, "git_author",
        {} if op == "R" else {"label": node_id, "seen_in": source},
        {"author": node_id, "repo": source}, {"commit": f"{source}-head"},
        ownership_mode="substitute", op=op,
    )


def _edge(src: str, dst: str, edge_type: str = "supported_by", *, op="I"):
    return EdgeFact(
        src, dst, edge_type,
        attrs={} if op == "R" else {"note": f"{src}->{dst}"},
        confidence=0.75,
        source_record_id={"id": f"{src}->{dst}"},
        source_revision={"rev": 1},
        op=op,
    )


def _apply(store, facts, *, source, run_id, branch="default") -> None:
    store.apply_facts(
        facts, source=source, run_id=run_id, strict_endpoints=False,
        branch=branch,
    )


def _state(store: ChronosGraphStore, branch: str) -> dict[str, list[dict]]:
    return {
        table: [
            dict(row) for row in store.query(
                f"SELECT * FROM okg.{table} ORDER BY {order}", branch=branch,
            )
        ]
        for table, order in (
            ("graph_nodes", "node_id"),
            ("graph_edges", "edge_id"),
            ("graph_node_declarers", "node_id, source"),
        )
    }


#: Liveness connections of the runs `_take` starts, closed after each test.
_LIVE_RUNS: dict[str, Any] = {}
_DSN: list[str] = []


@pytest.fixture(autouse=True)
def _end_live_runs(clean_runtime: str):
    _DSN[:] = [clean_runtime]
    yield
    for conn in _LIVE_RUNS.values():
        conn.close()
    _LIVE_RUNS.clear()


def _take(store, run_id: str = BAD_RUN) -> str:
    """Cut a run base as a runner does: under the run's liveness lock, which
    stays held (the run is "still going") until `_end_run` or the test ends."""
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    checkpoint_id = run_base_checkpoint_id("default", run_id)
    _LIVE_RUNS[checkpoint_id] = hold_run_liveness(_DSN[0], checkpoint_id)
    checkpoint = take_run_base(
        store, deployment=DEPLOYMENT, branch="default", source=SOURCE,
        run_id=run_id,
    )
    assert checkpoint is not None
    return checkpoint


def _end_run(run_id: str = BAD_RUN) -> None:
    """The run's process goes away: its liveness lock is released."""
    from okg.substrate.ingest.run_base import run_base_checkpoint_id

    _LIVE_RUNS.pop(run_base_checkpoint_id("default", run_id)).close()


def _seed_before_run(store: ChronosGraphStore) -> None:
    _apply(
        store,
        [_node(f"a:{i}") for i in range(5)] + [_author(SHARED_FOREIGN, OTHER)],
        source=OTHER, run_id="run-alpha-1",
    )
    good: list = [_node(f"g:{i}", "evidence") for i in range(10)]
    good += [_edge(f"g:{i}", f"g:{i + 1}") for i in range(9)]
    good += [_edge(f"g:{i}", f"a:{i % 5}", "cites") for i in range(10)]
    good += [_author(SHARED_OWNED, SOURCE)]
    _apply(store, good, source=SOURCE, run_id=GOOD_RUN)
    # The other source also declares the shared node gamma owns, and has an
    # edge into a gamma node the failed run will delete.
    _apply(
        store,
        [_author(SHARED_OWNED, OTHER), _edge("a:0", "g:3", "mentions")],
        source=OTHER, run_id="run-alpha-2",
    )
    # A run base is cut only once the deployment has published.
    store.checkpoint_generation(1, deployment_name=DEPLOYMENT)


def _failed_run(store: ChronosGraphStore, *, new_nodes: int) -> None:
    """A failed run touching every kind of key, committed chunk by chunk."""
    chunks: list[list] = []
    fresh: list = [_node(f"b:{i:04d}") for i in range(new_nodes)]
    fresh += [_edge(f"b:{i:04d}", f"a:{i % 5}", "cites") for i in range(new_nodes)]
    fresh += [_edge(f"b:{i:04d}", f"g:{i % 10}", "cites")
              for i in range(0, new_nodes, 3)]
    chunks += [fresh[i:i + 7] for i in range(0, len(fresh), 7)]
    chunks.append([_node(f"g:{i}", "evidence", rev=2, note="updated")
                   for i in range(3)])                         # updates
    chunks.append([_node("g:3", "evidence", op="R")])           # delete
    chunks.append([_node("g:4", "evidence", op="R")])           # delete ...
    chunks.append([_node("g:4", "evidence", rev=3, note="back")])  # re-insert
    chunks.append([_node("b:tmp")])                             # insert ...
    chunks.append([_node("b:tmp", op="R")])                     # ... delete
    chunks.append([_edge("g:5", "g:6", op="R")])                # edge delete
    chunks.append([_author(SHARED_FOREIGN, SOURCE)])            # declare
    chunks.append([_author(SHARED_OWNED, SOURCE, op="R")])      # hand-off
    for facts in chunks:
        _apply(store, facts, source=SOURCE, run_id=BAD_RUN)


def _parallel_other_writes(store, branch: str) -> None:
    _apply(
        store,
        [_node("o:new"), _node("a:1", rev=9, note="alpha moved on"),
         _edge("o:new", "a:2", "cites")],
        source=OTHER, run_id="run-alpha-3", branch=branch,
    )


@pytest.mark.parametrize("page_rows", [1, 4, 10_000])
def test_undo_restores_exactly_the_run_base_for_the_failed_run(
    clean_runtime: str, page_rows: int,
) -> None:
    """(c) delete-then-reinsert and insert-then-delete, (d) the run's own
    shared-node hand-off, updates, deletes with their incident edges, and a
    parallel source's writes -- at page sizes 1, 4 and 10,000."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        # "expected" is the pre-run state plus the other source's writes.
        store.create_branch("expected", from_branch="default")
        before = _state(store, "default")
        run_base = _take(store)
        _failed_run(store, new_nodes=23)
        damaged = _state(store, "default")
        _parallel_other_writes(store, "default")
        _parallel_other_writes(store, "expected")

        # Not vacuous: the run changed every kind of key it is meant to.
        by_id = {row["node_id"]: row for row in damaged["graph_nodes"]}
        assert "g:3" not in by_id and "b:0000" in by_id
        assert by_id["g:4"]["run_id"] == BAD_RUN
        assert by_id[SHARED_OWNED]["source"] == OTHER  # handed off
        assert damaged != before

        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
            page_rows=page_rows,
        )
        assert stats["mode"] == "undo_to_run_base", stats
        assert stats["run_base"] == run_base
        assert _state(store, "default") == _state(store, "expected")
        restored = {
            row["node_id"]: row for row in _state(store, "default")["graph_nodes"]
        }
        assert restored[SHARED_OWNED]["source"] == SOURCE
        assert restored["g:4"]["run_id"] == GOOD_RUN
        assert "b:tmp" not in restored and "b:0000" not in restored
        declarers = {
            (row["node_id"], row["source"])
            for row in _state(store, "default")["graph_node_declarers"]
        }
        assert (SHARED_OWNED, SOURCE) in declarers
        assert (SHARED_FOREIGN, SOURCE) not in declarers
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert conn.execute(
                "SELECT count(*) FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = %s", (run_base,),
            ).fetchone()[0] == 0
    finally:
        store.close()


def test_a_failure_mid_undo_rolls_back_every_page(
    clean_runtime: str, monkeypatch,
) -> None:
    """(f) The undo is one transaction: a page that fails after earlier
    pages were written leaves the damaged state exactly as it was, and the
    run base marked undo-pending, which the publisher refuses on."""
    import okg.substrate.ingest.compensation as compensation
    from okg.substrate.errors import SourceRunUndoUnfinished
    from okg.substrate.ingest.run_base import (
        pending_undos,
        refuse_publish_while_undo_unfinished,
    )

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        run_base = _take(store)
        _failed_run(store, new_nodes=23)
        damaged = _state(store, "default")
        real = compensation._apply_undo_page
        pages: list[str] = []

        def failing(session, table, **kwargs):
            pages.append(table)
            if len(pages) == 3:
                raise RuntimeError("undo page failed")
            return real(session, table, **kwargs)

        monkeypatch.setattr(compensation, "_apply_undo_page", failing)
        with pytest.raises(RuntimeError, match="undo page failed"):
            compensation.retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN, page_rows=4,
            )
        assert len(pages) == 3
        assert _state(store, "default") == damaged
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            pending = pending_undos(conn, deployment=DEPLOYMENT)
            assert [item["run_base"] for item in pending] == [run_base]
            with pytest.raises(SourceRunUndoUnfinished) as refused:
                refuse_publish_while_undo_unfinished(
                    conn, deployment=DEPLOYMENT,
                )
        assert refused.value.code == "source_run_undo_unfinished"
    finally:
        store.close()


def test_a_key_another_writer_changed_is_refused_not_overwritten(
    clean_runtime: str,
) -> None:
    """(e) The conflict rule. The failed run declared a node another source
    owns; that source then withdrew, handing the node to the failed run.
    Restoring the run base would undo the other source's retraction, so the
    undo refuses, writes nothing, and every publish is refused until an
    operator keeps the current rows."""
    from okg.substrate.errors import (
        CompensationRowSetChanged,
        SourceRunUndoUnfinished,
    )
    from okg.substrate.ingest.compensation import retract_chronos_run
    from okg.substrate.ingest.run_base import (
        finish_pending_undos,
        refuse_publish_while_undo_unfinished,
    )

    shared = "author:x@example.org"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_author(shared, OTHER)], source=OTHER, run_id="o1")
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        _take(store)
        _apply(store, [_author(shared, SOURCE)], source=SOURCE, run_id=BAD_RUN)
        _apply(
            store, [_author(shared, OTHER, op="R")], source=OTHER, run_id="o2",
        )
        current = _state(store, "default")
        handed = [r for r in current["graph_nodes"] if r["node_id"] == shared]
        assert [r["source"] for r in handed] == [SOURCE]

        with pytest.raises(CompensationRowSetChanged) as refused:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
            )
        assert refused.value.data["reason"] == "changed_after_run_base"
        assert refused.value.data["missing_sample"] == [f"{shared}"] or (
            shared in str(refused.value.data["missing_sample"])
        )
        assert _state(store, "default") == current

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with pytest.raises(SourceRunUndoUnfinished):
                refuse_publish_while_undo_unfinished(
                    conn, deployment=DEPLOYMENT,
                )
        retry = finish_pending_undos(
            dsn=clean_runtime, deployment=DEPLOYMENT, apply=True,
        )
        assert [r["status"] for r in retry["results"]] == ["refused"]
        assert len(retry["still_pending"]) == 1
        with pytest.raises(Exception, match="--reason"):
            finish_pending_undos(
                dsn=clean_runtime, deployment=DEPLOYMENT, accept_current=True,
                apply=True,
            )
        kept = finish_pending_undos(
            dsn=clean_runtime, deployment=DEPLOYMENT, accept_current=True,
            reason="alpha handed the node over on purpose", apply=True,
        )
        assert [r["status"] for r in kept["results"]] == ["accepted_current"]
        assert kept["still_pending"] == []
        # Option 2: the kept rows are still the failed run's partial state,
        # so the block stays, now as "rerun required" for that source.
        from okg.substrate.errors import SourceRerunRequired

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            with pytest.raises(SourceRerunRequired):
                refuse_publish_while_undo_unfinished(conn, deployment=DEPLOYMENT)
        assert _state(store, "default") == current
    finally:
        store.close()


def test_a_shared_node_the_run_inserted_passes_to_a_later_declarer(
    clean_runtime: str,
) -> None:
    """Issue #3382, gap 1. The failed run created a shared node absent at the
    run base; another source declared it afterwards (the substitute rule left
    the row with the run). Restoring "absent" would delete a node that source
    still declares, so the undo hands the row to it instead, keeps that
    source's edge onto it, and removes only the failed source's claim."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    shared = "author:shared-later@example.org"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        _take(store)
        _apply(store, [_author(shared, SOURCE)], source=SOURCE, run_id=BAD_RUN)
        _apply(
            store, [_author(shared, OTHER), _edge("a:0", shared, "authored_by")],
            source=OTHER, run_id="run-alpha-later",
        )
        before = {
            row["node_id"]: row for row in _state(store, "default")["graph_nodes"]
        }
        assert before[shared]["source"] == SOURCE  # the run owns the row

        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        )
        assert stats["mode"] == "undo_to_run_base", stats
        assert stats["nodes_handed_off"] == 1, stats
        assert stats["handoffs_refused_subtype"] == 0, stats
        after = _state(store, "default")
        nodes = {row["node_id"]: row for row in after["graph_nodes"]}
        assert shared in nodes, "the undo deleted a node another source declares"
        assert nodes[shared]["source"] == OTHER
        assert nodes[shared]["attrs"]["seen_in"] == OTHER
        assert any(
            row["src"] == "a:0" and row["dst"] == shared
            for row in after["graph_edges"]
        ), "the other source's edge onto the node went with the undo"
        declarers = {
            (row["node_id"], row["source"]) for row in after["graph_node_declarers"]
        }
        assert (shared, OTHER) in declarers
        assert (shared, SOURCE) not in declarers
    finally:
        store.close()


def test_the_undo_needs_no_run_base_rows_it_did_not_touch(
    clean_runtime: str,
) -> None:
    """A run that wrote nothing is undone to nothing, and its run base goes."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        before = _state(store, "default")
        run_base = _take(store)
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        )
        assert stats["rows_restored"] == 0 and stats["rows_deleted"] == 0
        assert stats["run_base"] == run_base
        assert _state(store, "default") == before
    finally:
        store.close()


# ---------------------------------------------------------------------------
# (g) Every checkpoint lookup that does not filter on the generation kind
# ---------------------------------------------------------------------------


@pytest.fixture
def generation_then_run_base(clean_runtime: str):
    """A published generation, then a newer run base on the same branch."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        generation = store.checkpoint_generation(
            1, deployment_name=DEPLOYMENT,
        )
        _apply(store, [_node("a:1")], source=OTHER, run_id="o2")
        run_base = _take(store)
        yield clean_runtime, str(generation), run_base
    finally:
        store.close()


def test_generation_lookups_by_kind_never_return_a_run_base(
    generation_then_run_base,
) -> None:
    dsn, generation, run_base = generation_then_run_base
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for status in ("published", None):
            latest = store.latest_generation(
                deployment_name=DEPLOYMENT, status=status,
            )
            assert latest.checkpoint_id == generation
        ids = [
            cp.checkpoint_id
            for cp in store.list_generations(
                deployment_name=None, status=None,
            )
        ]
        assert run_base not in ids and generation in ids
        views = [
            row[0] for row in conn.execute("SELECT id FROM okg.v_generations")
        ]
        assert run_base not in views


def test_staged_redrive_latest_checkpoint_ignores_a_run_base(
    generation_then_run_base,
) -> None:
    from okg.substrate.ingest.incomplete_recovery import (
        staged_source_redrive_state,
    )

    dsn, generation, _run_base = generation_then_run_base
    state = staged_source_redrive_state(dsn=dsn, deployment=DEPLOYMENT)
    assert state["latest_checkpoint_id"] == generation


def test_installer_stage_latest_checkpoint_ignores_a_run_base(
    generation_then_run_base,
) -> None:
    from okg.substrate.distributions.install_stage import (
        latest_default_checkpoint_id,
    )

    dsn, generation, _run_base = generation_then_run_base
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert latest_default_checkpoint_id(conn, DEPLOYMENT) == generation


def _feedback_generation(conn, generation: str) -> dict[str, Any]:
    segment = conn.execute(
        "SELECT segment_id FROM _chronos_branch_interval_checkpoints "
        "WHERE checkpoint_id = %s", (generation,),
    ).fetchone()[0]
    return {
        "checkpoint_id": generation,
        "checkpoint_segment_id": segment,
        "branch_name": "default",
        "deployment_name": DEPLOYMENT,
        "generation_id": generation,
    }


def test_admission_feedback_accepts_a_run_base_split_with_no_writes(
    clean_runtime: str,
) -> None:
    """(b) A run base that split the head and wrote nothing leaves delayed
    admission feedback valid: not "newer checkpoint", not "structure
    changed", not "intervening write"."""
    from okg.substrate.admission import _assert_chronos_feedback_head_unchanged
    from okg.substrate.ingest.run_base import release_run_base

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        generation = str(store.checkpoint_generation(
            1, deployment_name=DEPLOYMENT,
        ))
        _take(store)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _assert_chronos_feedback_head_unchanged(
                conn, generation=_feedback_generation(conn, generation),
            )
        assert release_run_base(
            clean_runtime, branch="default", run_id=BAD_RUN,
        ) is None
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            _assert_chronos_feedback_head_unchanged(
                conn, generation=_feedback_generation(conn, generation),
            )
    finally:
        store.close()


def test_admission_feedback_after_a_writing_run_names_the_write(
    generation_then_run_base,
) -> None:
    """(b) After a run base and a write, the refusal is the real one -- a
    graph write after the generation -- not "branch structure changed"."""
    from okg.substrate.admission import _assert_chronos_feedback_head_unchanged
    from okg.substrate.errors import SubstrateError

    dsn, generation, _run_base = generation_then_run_base
    store = ChronosGraphStore.connect(dsn)
    try:
        _apply(store, [_node("a:2")], source=SOURCE, run_id=BAD_RUN)
    finally:
        store.close()
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as refused:
            _assert_chronos_feedback_head_unchanged(
                conn, generation=_feedback_generation(conn, generation),
            )
    assert refused.value.data["reason"] == "intervening_graph_write", (
        refused.value.data
    )


def test_publish_history_checkpoint_count_ignores_a_run_base(
    generation_then_run_base,
) -> None:
    from okg.substrate.publish_recovery import _read_publish_history

    dsn, generation, _run_base = generation_then_run_base
    history = _read_publish_history(dsn, DEPLOYMENT)
    assert history["checkpoint_count_any_branch"] == 1
    assert history["last_published"]["generation_id"] == generation


def test_history_compaction_refuses_to_delete_what_a_run_base_reads(
    generation_then_run_base,
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.history_compaction import _refuse_run_bases_below

    dsn, _generation, run_base = generation_then_run_base
    with psycopg.connect(dsn, autocommit=True) as conn:
        point = int(conn.execute(
            "SELECT s.branch_point FROM _chronos_branch_interval_checkpoints c "
            "JOIN _chronos_branch_interval_segments s "
            "ON s.segment_id = c.segment_id WHERE c.checkpoint_id = %s",
            (run_base,),
        ).fetchone()[0])
        _refuse_run_bases_below(conn, branch="default", retirement_point=None)
        _refuse_run_bases_below(conn, branch="default", retirement_point=point)
        with pytest.raises(SubstrateError, match="run-base checkpoint"):
            _refuse_run_bases_below(
                conn, branch="default", retirement_point=point + 1,
            )


def test_the_graph_effect_sample_sees_writes_from_before_a_run_base(
    clean_runtime: str,
) -> None:
    """(c) The publish's graph-effect sample reads every segment written
    since the previous generation, so a row written before a run base is in
    it next to the run's own."""
    from okg.substrate.publisher import Publisher

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        _apply(store, [_node("a:1")], source=OTHER, run_id="o2")
        _take(store)
        _apply(store, [_node("b:1")], source=SOURCE, run_id=BAD_RUN)
        previous = store.latest_generation(deployment_name=DEPLOYMENT)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            manifest = Publisher._chronos_graph_effect_manifest(
                conn, branch_name="default", previous=previous,
            )
        added = {
            item["object_id"] for item in manifest["items"]
            if item["change_kind"] == "added"
        }
        assert {"a:1", "b:1"} <= added, manifest
        assert manifest["coverage"] == "exact", manifest
    finally:
        store.close()


def test_the_publish_write_probe_sees_writes_from_before_a_run_base(
    clean_runtime: str,
) -> None:
    """A run base splits the branch segment. The publisher's "did the branch
    write since its last checkpoint" probe must look through that split, or a
    run that writes nothing after another run's writes makes the next publish
    reuse the previous generation and drop those writes."""
    from okg.substrate.ingest.run_base import release_run_base
    from okg.substrate.publisher import Publisher

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        _apply(store, [_node("a:1")], source=OTHER, run_id="o2")
        _take(store)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert Publisher._chronos_current_segment_has_graph_writes(
                conn, branch_name="default",
            ) is True
        assert release_run_base(
            clean_runtime, branch="default", run_id=BAD_RUN,
        ) is None
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert Publisher._chronos_current_segment_has_graph_writes(
                conn, branch_name="default",
            ) is True
    finally:
        store.close()


def test_no_run_base_before_the_first_published_generation(
    clean_runtime: str,
) -> None:
    from okg.substrate.ingest.run_base import take_run_base

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        assert take_run_base(
            store, deployment=DEPLOYMENT, branch="default", source=SOURCE,
            run_id=BAD_RUN,
        ) is None
    finally:
        store.close()


def test_finish_undo_without_apply_changes_nothing(clean_runtime: str) -> None:
    from okg.substrate.ingest.run_base import finish_pending_undos

    report = finish_pending_undos(dsn=clean_runtime, deployment=DEPLOYMENT)
    assert report["applied"] is False and report["pending"] == []
    assert report["next_command"] == (
        "okg runtime finish-undo --deployment svc --apply"
    )


# ---------------------------------------------------------------------------
# Review round 2: the run base of a run that is still going, or that ended
# without its undo; a missing run base; the undo bound; two conflict rules
# ---------------------------------------------------------------------------


class _Kill(BaseException):
    """Stands in for a killed process: no ``except Exception`` handler runs,
    only ``finally`` blocks (which release the run's liveness lock, as the
    death of its connection would)."""


def _pending(dsn: str) -> list[dict[str, Any]]:
    from okg.substrate.ingest.run_base import pending_undos

    with psycopg.connect(dsn, autocommit=True) as conn:
        return pending_undos(conn, deployment=DEPLOYMENT)


def test_a_second_run_of_the_same_source_keeps_the_first_runs_run_base(
    fx: _Fixture, monkeypatch,
) -> None:
    """Review finding 1. Run 1 of aaa commits 20 removals; run 2 of aaa
    starts beside it and cuts its own run base. Run 1's run base must
    survive (its run is still going), so run 1's failure is undone exactly.
    Before the fix, run 2 deleted it as stale and run 1 fell back to
    retracting its own rows: readers then saw 45."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    second = "00000000-0000-4000-8000-000000000002"
    second_base = run_base_checkpoint_id("default", second)
    fx.write("aaa", {
        "retract": list(range(20)),
        "records": _records(AAA_RECORDS)[20:],
    })
    seen: list[list[str]] = []
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            _LIVE_RUNS[second_base] = hold_run_liveness(fx.dsn, second_base)
            with psycopg.connect(fx.dsn) as conn:
                store = ChronosGraphStore.from_connection(
                    conn, ensure_schema=False,
                )
                take_run_base(
                    store, deployment=DEPLOYMENT, branch="default",
                    source=AAA, run_id=second,
                )
                conn.commit()
            seen.append(sorted(fx.run_bases()))
            raise RuntimeError("injected failure on flush 2")
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert len(seen[0]) == 2, seen  # both runs' run bases, side by side
    assert fx.head_count() == AAA_RECORDS
    assert fx.run_bases() == [second_base]  # run 1's went with its undo
    assert fx.receipts().get("compensating", 0) == 0, fx.receipts()
    # Round 6: run 2 is live and still writing, so bbb's publish waits for
    # it (bounded here to 1 s) and is then refused; readers keep 65.
    monkeypatch.setenv("OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS", "1")
    report = _publish_bbb_alone(fx, monkeypatch)
    assert _refused_publish(report)["code"] == "source_run_in_progress"
    assert fx.published()["count"] == AAA_RECORDS
    # Run 2's process goes away without an admission: now it is an
    # unfinished undo, and the guard counts it.
    _end_run(second)
    assert [item["run_base"] for item in _pending(fx.dsn)] == [second_base]


def test_a_killed_run_is_an_unfinished_undo_until_finished(
    fx: _Fixture, monkeypatch,
) -> None:
    """Review findings 3 and 4. A run killed after committing 20 removals
    runs no compensation. Its run base, with no live process and no final
    admission, is an unfinished undo: the next publish is refused, history
    compaction names the command, finish-undo lists and finishes it, and
    readers never see 45."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.history_compaction import _refuse_run_bases_below
    from okg.substrate.ingest.run_base import finish_pending_undos

    before = fx.published()
    fx.write("aaa", {
        "retract": list(range(20)),
        "records": _records(AAA_RECORDS)[20:],
    })
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            raise _Kill()
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    with pytest.raises(_Kill):
        fx.ingest({"aaa"})
    assert fx.head_count() == AAA_RECORDS - 20
    assert len(fx.run_bases()) == 1
    assert fx.receipts().get("compensating", 0) == 0, fx.receipts()
    pending = _pending(fx.dsn)
    assert [item["signal"] for item in pending] == ["run_ended_without_undo"]
    assert pending[0]["admission_status"] == "pending"

    report = _publish_bbb_alone(fx, monkeypatch)
    publish = [p for p in report["publishes"] if p.get("status") == "fail"]
    assert publish and publish[0]["code"] == "source_run_undo_unfinished"
    assert fx.published() == before

    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        with pytest.raises(SubstrateError) as refused:
            _refuse_run_bases_below(
                conn, branch="default", retirement_point=10 ** 30,
            )
    assert "okg runtime finish-undo --deployment <deployment>" in (
        refused.value.fix_hint
    )
    listed = finish_pending_undos(dsn=fx.dsn, deployment=DEPLOYMENT)
    assert [item["run_id"] for item in listed["pending"]] == [
        pending[0]["run_id"],
    ]
    # Just ended: it may be a live run whose liveness session was lost, so
    # finish-undo leaves it unless the operator names it.
    waited = finish_pending_undos(dsn=fx.dsn, deployment=DEPLOYMENT, apply=True)
    assert waited["results"] == [], waited
    assert "was active" in waited["skipped"][0]["why"], waited
    assert fx.head_count() == AAA_RECORDS - 20
    done = finish_pending_undos(
        dsn=fx.dsn, deployment=DEPLOYMENT, apply=True,
        run_id=pending[0]["run_id"],
    )
    assert [r["status"] for r in done["results"]] == ["undone"], done
    assert done["results"][0]["admission"] == "failed"
    assert done["still_pending"] == []
    assert fx.run_bases() == []
    assert fx.head_count() == AAA_RECORDS

    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    assert fx.published()["count"] == AAA_RECORDS


def test_a_run_that_lost_its_run_base_is_refused_not_retracted(
    fx: _Fixture, monkeypatch,
) -> None:
    """Review finding 1: a failed run that applied chunks where its
    deployment has published, with no run base, must not fall back to
    retracting its own rows (which leaves its removals). The compensation
    refuses, the receipts stay compensating, and publishing is refused."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    fx.write("aaa", {
        "retract": list(range(20)),
        "records": _records(AAA_RECORDS)[20:],
    })
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            with psycopg.connect(fx.dsn, autocommit=True) as conn:
                conn.execute(
                    "DELETE FROM _chronos_branch_interval_checkpoints "
                    "WHERE checkpoint_id LIKE 'okg-run-base:%%'"
                )
            raise RuntimeError("injected failure on flush 2")
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert "SourceRunBaseMissing" in json.dumps(
        failed.report["results"][0], default=str,
    ), failed.report["results"][0]
    assert fx.receipts().get("compensating", 0) >= 1, fx.receipts()
    assert fx.head_count() == AAA_RECORDS - 20  # nothing retracted
    report = _publish_bbb_alone(fx, monkeypatch)
    publish = [p for p in report["publishes"] if p.get("status") == "fail"]
    assert publish and publish[0]["code"] == "source_run_undo_unfinished"
    # Its own refusal names the only way out: a retry cannot succeed.
    from okg.substrate.errors import SourceRunBaseMissing
    from okg.substrate.ingest.compensation import retract_chronos_run

    stuck = _pending(fx.dsn)[0]
    with pytest.raises(SourceRunBaseMissing) as refused:
        retract_chronos_run(
            fx.dsn, admission_id=stuck["admission_id"], source=AAA,
            run_id=stuck["run_id"],
        )
    assert "--accept-current" in refused.value.fix_hint
    assert "Retry" not in refused.value.fix_hint
    assert "finish-undo --deployment svc --apply" not in refused.value.fix_hint
    kept = finish_pending_undos(
        dsn=fx.dsn, deployment=DEPLOYMENT, accept_current=True,
        reason="the run base was lost", apply=True,
    )
    assert kept["still_pending"] == []
    # Option 2: keeping the partial rows does not unblock publishing; the
    # source must rerun first (the rerun-required tests below). bbb's work
    # staged by the refused publish above cannot drain either.
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as still_refused:
        _publish_bbb_alone(fx, monkeypatch)
    assert still_refused.value.data["publish_code"] == "source_rerun_required"
    assert "okg ingest --deployment svc --include svc.aaa" in (
        still_refused.value.fix_hint
    )


def test_an_undo_above_the_key_bound_is_refused_before_writing(
    clean_runtime: str, monkeypatch,
) -> None:
    """Review finding 6: the undo holds the branch write lock throughout,
    so above the bound it refuses before writing anything."""
    from okg.substrate.errors import SourceRunUndoTooLarge
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        _take(store)
        _failed_run(store, new_nodes=23)
        damaged = _state(store, "default")
        monkeypatch.setenv("OKG_RUN_BASE_UNDO_MAX_KEYS", "5")
        with pytest.raises(SourceRunUndoTooLarge) as refused:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN,
            )
        assert refused.value.data["keys_since_run_base"] > 5
        assert "OKG_RUN_BASE_UNDO_MAX_KEYS" in refused.value.fix_hint
        assert _state(store, "default") == damaged
        assert [item["signal"] for item in _pending(clean_runtime)] == [
            "run_base_pending",
        ]
        monkeypatch.delenv("OKG_RUN_BASE_UNDO_MAX_KEYS")
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        )
        assert stats["mode"] == "undo_to_run_base"
        assert _pending(clean_runtime) == []
    finally:
        store.close()


def test_a_row_of_the_source_another_writer_changed_is_refused(
    clean_runtime: str,
) -> None:
    """Review finding 5 (node rule): a row the source owned at the run base
    that another writer rewrote after it is not overwritten."""
    from okg.substrate.errors import CompensationRowSetChanged
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        _take(store)
        _apply(store, [_node("b:1")], source=SOURCE, run_id=BAD_RUN)
        _apply(
            store, [_node("g:0", "evidence", note="alpha took it")],
            source=OTHER, run_id="run-alpha-9",
        )
        current = _state(store, "default")
        with pytest.raises(CompensationRowSetChanged) as refused:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN,
            )
        assert refused.value.data["table"] == "okg.graph_nodes"
        assert "g:0" in refused.value.data["missing_sample"]
        assert refused.value.data["reasons"] == [
            "another writer changed this row after the run started",
        ]
        assert _state(store, "default") == current
    finally:
        store.close()


def test_an_edge_onto_a_node_another_writer_removed_is_refused(
    clean_runtime: str,
) -> None:
    """Review finding 5 (edge rule): the run deleted its edge g:1 -> a:1,
    then the other source removed a:1. Restoring the edge would point at a
    node that no longer exists, so the undo refuses."""
    from okg.substrate.errors import CompensationRowSetChanged
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        _take(store)
        _apply(
            store, [_edge("g:1", "a:1", "cites", op="R")],
            source=SOURCE, run_id=BAD_RUN,
        )
        _apply(store, [_node("a:1", op="R")], source=OTHER, run_id="o-r")
        current = _state(store, "default")
        with pytest.raises(CompensationRowSetChanged) as refused:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN,
            )
        assert refused.value.data["table"] == "okg.graph_edges"
        assert refused.value.data["reasons"] == [
            "the undo would restore an edge onto a node another writer "
            "removed",
        ]
        assert _state(store, "default") == current
    finally:
        store.close()


def test_a_live_run_is_not_an_unfinished_undo_until_it_ends(
    clean_runtime: str,
) -> None:
    """A run base of a run that is still going is not an unfinished undo;
    once its process ends without a final admission it is."""
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        run_base = _take(store)
        assert _pending(clean_runtime) == []
        _end_run()
        assert [
            (item["run_base"], item["signal"])
            for item in _pending(clean_runtime)
        ] == [(run_base, "run_ended_without_undo")]
    finally:
        store.close()


def test_the_hint_is_still_refused_after_a_blocked_generation(
    clean_runtime: str,
) -> None:
    """Review finding 9: only run-base splits let the hint cover several
    segments; a blocked generation since the previous one still refuses."""
    from okg.substrate.publisher import Publisher

    publisher = Publisher.__new__(Publisher)
    publisher.branch_name = "default"
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o1")
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        _apply(store, [_node("a:1")], source=OTHER, run_id="o2")
        _take(store)
        _apply(store, [_node("b:1")], source=SOURCE, run_id=BAD_RUN)
        previous = store.latest_generation(deployment_name=DEPLOYMENT)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            segments = publisher._chronos_hint_segments(conn, previous=previous)
        assert isinstance(segments, list) and len(segments) == 2
        store.checkpoint_generation(
            2, deployment_name=DEPLOYMENT, status="blocked",
        )
        _apply(store, [_node("b:2")], source=SOURCE, run_id=BAD_RUN)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            assert publisher._chronos_hint_segments(
                conn, previous=previous,
            ) is False
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Review round 3
# ---------------------------------------------------------------------------


def _removal_spec() -> dict[str, Any]:
    return {"retract": list(range(20)), "records": _records(AAA_RECORDS)[20:]}


def _fail_on_second_flush(monkeypatch, error: BaseException) -> None:
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            raise error
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)


def test_a_recovered_attempt_of_the_same_run_reuses_its_run_base(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 3, finding 1. A worker's DBOS recovery reruns a workflow with
    the same run id. Attempt 1 is killed after its first chunk; attempt 2
    must reuse attempt 1's run base (still the state to undo to) instead of
    failing on a checkpoint that already exists, and then finish."""
    from datetime import datetime, timezone

    observed = datetime(2026, 9, 30, 1, 2, 3, tzinfo=timezone.utc)

    def attempt():
        return run_ingest(IngestOptions(
            dsn=fx.dsn, deployment=DEPLOYMENT, deployment_dir=fx.dep,
            include={"aaa"}, include_predecessors=False,
            include_dependents=False, publish=True,
            admission_observed_at=observed,
        ))

    fx.write("aaa", _removal_spec())
    _fail_on_second_flush(monkeypatch, _Kill())
    with pytest.raises(_Kill):
        attempt()
    left = fx.run_bases()
    assert len(left) == 1
    monkeypatch.setattr(_Runner, "_flush_chunk", _REAL_FLUSH)
    second = attempt()
    result = second.report["results"][0]
    assert result["status"] == "OK", result
    assert str(result["run_id"]) in left[0]  # the same run, resumed
    assert fx.run_bases() == []
    assert _pending(fx.dsn) == []
    assert fx.published()["count"] == AAA_RECORDS - 20  # the run's own intent


def test_a_repeat_compensation_of_an_undone_run_does_nothing(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 3, finding 2, on a published deployment: once a run's undo has
    completed, a second compensation of it (a racing or repeated cleanup,
    #2244) finds every receipt compensated and changes nothing."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    fx.write("aaa", _removal_spec())
    _fail_on_second_flush(monkeypatch, RuntimeError("injected"))
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert fx.run_bases() == []
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        admission_id, run_id = conn.execute(
            "SELECT admission_id::text, run_id::text "
            "FROM okg.fact_batch_receipts WHERE state = 'compensated' LIMIT 1"
        ).fetchone()
    receipts = fx.receipts()
    stats = retract_chronos_run(
        fx.dsn, admission_id=admission_id, source=AAA, run_id=run_id,
    )
    assert stats.get("rows_retracted", 0) == 0, stats
    assert fx.receipts() == receipts
    assert fx.head_count() == AAA_RECORDS
    assert _pending(fx.dsn) == []


def _terminate_liveness_sessions(dsn: str) -> int:
    """Kill the backend holding a run's liveness lock (a lost connection).

    Only in this database: pg_locks lists every database on the server, and
    parallel test workers use the same run ids in their own databases.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        return int(conn.execute(
            "SELECT count(pg_terminate_backend(l.pid)) FROM pg_locks l "
            "JOIN _chronos_branch_interval_checkpoints c "
            "  ON c.checkpoint_id LIKE 'okg-run-base:%%' "
            "WHERE l.locktype = 'advisory' AND l.granted "
            "AND l.database = (SELECT oid FROM pg_database "
            "                  WHERE datname = current_database()) "
            "AND l.objsubid = 1 AND l.pid <> pg_backend_pid() "
            "AND l.classid::bigint = "
            "  ((hashtextextended(c.checkpoint_id, 0) >> 32) "
            "   & x'FFFFFFFF'::bigint) "
            "AND l.objid::bigint = "
            "  (hashtextextended(c.checkpoint_id, 0) & x'FFFFFFFF'::bigint)"
        ).fetchone()[0])


@contextmanager
def _another_database_on_this_server(dsn: str):
    """An empty database on the same server as ``dsn``, dropped afterwards."""
    from psycopg import sql

    options = psycopg.conninfo.conninfo_to_dict(dsn)
    name = f"okg_test_run_base_other_{uuid.uuid4().hex[:12]}"
    maintenance = psycopg.conninfo.make_conninfo(
        **{**options, "dbname": "postgres"},
    )
    with psycopg.connect(maintenance, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
            sql.Identifier(name),
        ))
    try:
        yield psycopg.conninfo.make_conninfo(**{**options, "dbname": name})
    finally:
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                sql.Identifier(name),
            ))


def _own_liveness_holders(dsn: str, checkpoint_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return int(conn.execute(
            "SELECT count(*) FROM pg_locks l "
            "WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 "
            "AND l.database = (SELECT oid FROM pg_database "
            "                  WHERE datname = current_database()) "
            "AND l.objid::bigint = "
            "  (hashtextextended(%s, 0) & x'FFFFFFFF'::bigint)",
            (checkpoint_id,),
        ).fetchone()[0])


def test_a_liveness_lock_in_another_database_on_the_server_is_not_this_runs(
    generation_then_run_base,
) -> None:
    """pg_locks lists every database on the server, and parallel test
    workers (like two deployments sharing a server) use the same run ids,
    so the same lock key, in their own databases. A session in another
    database holding this run's key is not this run's owner: the helper
    neither counts nor ends it, and the product reads the run as not live
    here once its own owner is gone."""
    from okg.substrate.ingest.run_base import run_is_live, runs_in_progress

    dsn, _generation, run_base = generation_then_run_base
    with _another_database_on_this_server(dsn) as other_dsn:
        with psycopg.connect(other_dsn, autocommit=True) as other:
            other.execute(
                "SELECT pg_advisory_lock(hashtextextended(%s, 0))", (run_base,),
            )
            other_pid = other.info.backend_pid

            # This run's own owner is alive: the helper ends exactly it.
            assert _terminate_liveness_sessions(dsn) == 1
            _LIVE_RUNS.pop(run_base, None)
            deadline = time.monotonic() + 10
            while _own_liveness_holders(dsn, run_base) and (
                time.monotonic() < deadline
            ):
                time.sleep(0.05)
            assert _own_liveness_holders(dsn, run_base) == 0

            # Only the other database's session holds the key now.
            assert _terminate_liveness_sessions(dsn) == 0
            with psycopg.connect(dsn, autocommit=True) as conn:
                assert run_is_live(conn, run_base) is False
                assert runs_in_progress(conn, deployment=DEPLOYMENT) == []
            assert other.execute(
                "SELECT pg_backend_pid()",
            ).fetchone()[0] == other_pid


def test_finish_undo_leaves_a_run_whose_liveness_session_was_lost(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 3, finding 4. The run's liveness connection is lost while the
    run goes on, so it reads as ended. finish-undo --apply must not undo it
    while it was active moments ago; the run then finishes normally."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    fx.write("aaa", _removal_spec())
    seen: dict[str, Any] = {}
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            seen["terminated"] = _terminate_liveness_sessions(fx.dsn)
            done = finish_pending_undos(
                dsn=fx.dsn, deployment=DEPLOYMENT, apply=True,
            )
            seen["results"] = done["results"]
            seen["skipped"] = [item["why"] for item in done["skipped"]]
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    run = fx.ingest({"aaa"})
    assert seen["terminated"] == 1, seen
    assert seen["results"] == [], seen
    assert len(seen["skipped"]) == 1 and "was active" in seen["skipped"][0]
    assert run.report["results"][0]["status"] == "OK", run.report["results"]
    assert fx.head_count() == AAA_RECORDS - 20
    assert fx.run_bases() == []


def test_finish_undo_rechecks_the_lock_just_before_undoing(
    clean_runtime: str, monkeypatch,
) -> None:
    """Round 3, finding 4: a run listed as ended whose lock is held again by
    the time finish-undo reaches it is skipped, even when named."""
    import okg.substrate.ingest.run_base as run_base

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        checkpoint = _take(store)
        _apply(store, [_node("b:1")], source=SOURCE, run_id=BAD_RUN)
        _end_run()
        listed = _pending(clean_runtime)
        assert [item["run_base"] for item in listed] == [checkpoint]
        _LIVE_RUNS[checkpoint] = run_base.hold_run_liveness(
            clean_runtime, checkpoint,
        )
        monkeypatch.setattr(
            run_base, "pending_undos", lambda _conn, **_kw: listed,
        )
        done = run_base.finish_pending_undos(
            dsn=clean_runtime, deployment=DEPLOYMENT, apply=True,
            run_id=BAD_RUN,
        )
        assert done["results"] == [], done
        assert "liveness lock again" in done["skipped"][0]["why"]
        heads = {row["node_id"] for row in _state(store, "default")["graph_nodes"]}
        assert "b:1" in heads  # not undone
    finally:
        store.close()


def test_finish_undo_reports_a_prepared_receipt_instead_of_crashing(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 3: a process killed between writing a chunk and recording it
    leaves the chunk's receipt 'prepared'. finish-undo reports that run as
    refused, with the way out, rather than raising."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    fx.write("aaa", _removal_spec())
    _fail_on_second_flush(monkeypatch, _Kill())
    with pytest.raises(_Kill):
        fx.ingest({"aaa"})
    stuck = _pending(fx.dsn)[0]
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.fact_batch_receipts SET state = 'prepared' "
            "WHERE admission_id = %s::uuid",
            (stuck["admission_id"],),
        )
    done = finish_pending_undos(
        dsn=fx.dsn, deployment=DEPLOYMENT, apply=True, run_id=stuck["run_id"],
    )
    assert [r["status"] for r in done["results"]] == ["refused"], done
    error = done["results"][0]["error"]
    assert error["code"] == "fact_batch_recovery_blocked"
    assert "--accept-current" in error["fix_hint"]
    assert len(done["still_pending"]) == 1


def test_a_blocked_ingest_names_finish_undo_first(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 3, finding 5: after a killed run, every other source's ingest
    is refused as well (its staged-work drain cannot publish), and the
    refusal leads with finish-undo, not with a drain that is refused too."""
    from okg.substrate.errors import SubstrateError

    fx.write("aaa", _removal_spec())
    _fail_on_second_flush(monkeypatch, _Kill())
    with pytest.raises(_Kill):
        fx.ingest({"aaa"})
    _publish_bbb_alone(fx, monkeypatch)  # refused; leaves bbb staged
    with pytest.raises(SubstrateError) as refused:
        _publish_bbb_alone(fx, monkeypatch)
    hint = refused.value.fix_hint
    assert "okg runtime finish-undo --deployment svc --apply" in hint, hint
    assert hint.index("okg runtime finish-undo") < hint.index("okg run --once")


# ---------------------------------------------------------------------------
# Round 3, option 2: the undo scans and counts only the failed run's
# own records.
# ---------------------------------------------------------------------------


def _run_versions(dsn: str, node_id: str, run_id: str) -> int:
    """Physical versions of ``node_id`` the run wrote that still exist."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE table_name = 'okg.graph_nodes' AND backend = 'interval'"
        ).fetchone()[0]
        return int(conn.execute(
            f'SELECT count(*) FROM public."{physical}" '
            "WHERE node_id = %s AND run_id = %s",
            (node_id, run_id),
        ).fetchone()[0])


def test_an_in_place_overwrite_of_a_run_row_is_never_clobbered(
    clean_runtime: str,
) -> None:
    """Option 2, test (a). The run writes a key, then another writer writes
    the same key in the same segment, which replaces the run's version in
    place: no version of the run is left to find. The undo must neither
    restore the run-base value over the other writer's version nor miss the
    conflict the rules name:
    * a row the source owned at the run base -> refused (another writer
      changed the source's row), the other writer's version kept;
    * a row the run inserted -> the other writer's version kept, the rest of
      the run undone."""
    from okg.substrate.errors import CompensationRowSetChanged
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        _take(store)
        _apply(
            store, [_node("g:0", "evidence", note="run edit")],
            source=SOURCE, run_id=BAD_RUN,
        )
        _apply(
            store, [_node("g:0", "evidence", note="alpha edit")],
            source=OTHER, run_id="run-alpha-9",
        )
        assert _run_versions(clean_runtime, "g:0", BAD_RUN) == 0  # in place
        current = _state(store, "default")
        with pytest.raises(CompensationRowSetChanged) as refused:
            retract_chronos_run(
                clean_runtime, admission_id=None, source=SOURCE,
                run_id=BAD_RUN,
            )
        assert "g:0" in refused.value.data["missing_sample"]
        assert _state(store, "default") == current
        head = {r["node_id"]: r for r in current["graph_nodes"]}
        assert head["g:0"]["run_id"] == "run-alpha-9"
    finally:
        store.close()

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        run = "run-gamma-bad-2"
        _take(store, run)
        _apply(store, [_node("b:new"), _node("b:mine")], source=SOURCE, run_id=run)
        _apply(
            store, [_node("b:new", note="alpha took it")],
            source=OTHER, run_id="run-alpha-10",
        )
        assert _run_versions(clean_runtime, "b:new", run) == 0  # in place
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=run,
        )
        assert stats["mode"] == "undo_to_run_base", stats
        head = {
            r["node_id"]: r for r in _state(store, "default")["graph_nodes"]
        }
        assert head["b:new"]["run_id"] == "run-alpha-10"  # kept, not deleted
        assert "b:mine" not in head  # the run's own insert undone
    finally:
        store.close()


def test_other_sources_writes_do_not_count_toward_the_undo_bound(
    fx: _Fixture, monkeypatch,
) -> None:
    """Option 2, test (b): the reviewer's low-bound reproduction inverted.
    With the bound at 30, aaa's failed run touches 20 keys while bbb writes
    40 after aaa's run base. aaa is undone, and bbb then publishes."""
    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setenv("OKG_RUN_BASE_UNDO_MAX_KEYS", "30")
    fx.write("aaa", _removal_spec())
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            busy = ChronosGraphStore.connect(fx.dsn)
            try:
                busy.apply_facts(
                    [
                        NodeFact(
                            node_id=f"bbb:busy{i}", subtype="document",
                            attrs={"text": f"busy {i}"},
                            source_record_id={"id": f"busy{i}"},
                            source_revision={"v": "1"},
                        )
                        for i in range(40)
                    ],
                    source=BBB, run_id="bbb-busy",
                )
            finally:
                busy.close()
            raise RuntimeError("injected failure after bbb's writes")
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    failed = fx.ingest({"aaa"})
    assert failed.any_failed is True
    assert "source_run_undo_too_large" not in json.dumps(
        failed.report, default=str,
    )
    assert fx.receipts().get("compensating", 0) == 0, fx.receipts()
    assert fx.head_count() == AAA_RECORDS
    assert _pending(fx.dsn) == []
    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    assert fx.published()["count"] == AAA_RECORDS


# ---------------------------------------------------------------------------
# Review round 4
# ---------------------------------------------------------------------------


def _dangling(state: dict[str, list[dict]]) -> list[tuple[str, str]]:
    nodes = {row["node_id"] for row in state["graph_nodes"]}
    return [
        (row["src"], row["dst"]) for row in state["graph_edges"]
        if row["src"] not in nodes or row["dst"] not in nodes
    ]


@pytest.mark.parametrize("when", ["before_undo", "while_undo_waits_for_lock"])
def test_an_edge_onto_a_node_the_undo_deletes_goes_with_it(
    clean_runtime: str, monkeypatch, when: str,
) -> None:
    """Round 4 (the reviewer's test_rv3_x). The failed run inserted g:new;
    another source adds an edge onto it before the undo starts, or while the
    undo waits for the branch write lock after its key scan. The edge was
    written after the run base onto a node the undo deletes, so it is deleted
    with the node: nothing is left dangling."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _apply(store, [_node("a:0")], source=OTHER, run_id="o0")
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
        _take(store)
        _apply(store, [_node("g:new")], source=SOURCE, run_id=BAD_RUN)

        def other_writes_edge() -> None:
            other = ChronosGraphStore.connect(clean_runtime)
            try:
                _apply(
                    other, [_edge("a:0", "g:new")], source=OTHER, run_id="o1",
                )
            finally:
                other.close()

        if when == "before_undo":
            other_writes_edge()
        else:
            real_lock = ChronosGraphStore._lock_branch_write
            fired: list[int] = []

            def lock(self, branch):
                if not fired:
                    fired.append(1)
                    other_writes_edge()
                return real_lock(self, branch)

            monkeypatch.setattr(ChronosGraphStore, "_lock_branch_write", lock)
        stats = retract_chronos_run(
            clean_runtime, admission_id=None, source=SOURCE, run_id=BAD_RUN,
        )
        assert stats["mode"] == "undo_to_run_base", stats
        state = _state(store, "default")
        assert "g:new" not in {row["node_id"] for row in state["graph_nodes"]}
        assert state["graph_edges"] == []
        assert _dangling(state) == []
    finally:
        store.close()


def test_a_kill_between_a_chunk_write_and_its_receipt_is_reported(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 4: drive the real path, not an edited row. The run's first
    chunk (its 20 removals; the rest of the stream is unchanged records and
    writes no chunk) commits its graph write, then the process dies before
    the chunk's receipt is carried to 'applied' (the transition raises a
    BaseException, as a kill would).
    finish-undo then meets that 'prepared' receipt and reports the run as
    refused, with the way out, instead of crashing."""
    from okg.substrate.fact_batch import PostgresApplicationReceiptStore
    from okg.substrate.ingest.run_base import finish_pending_undos

    fx.write("aaa", _removal_spec())
    real_transition = PostgresApplicationReceiptStore.transition
    real_with_evidence = PostgresApplicationReceiptStore.transition_with_evidence
    applied: list[int] = []

    def kill_second(state: str) -> None:
        if state == "applied":
            applied.append(1)
            if len(applied) == 1:
                raise _Kill()

    def transition(self, key, state, **kwargs):
        kill_second(state)
        return real_transition(self, key, state, **kwargs)

    def transition_with_evidence(self, key, state, **kwargs):
        kill_second(state)
        return real_with_evidence(self, key, state, **kwargs)

    monkeypatch.setattr(PostgresApplicationReceiptStore, "transition", transition)
    monkeypatch.setattr(
        PostgresApplicationReceiptStore, "transition_with_evidence",
        transition_with_evidence,
    )
    with pytest.raises(_Kill):
        fx.ingest({"aaa"})
    monkeypatch.setattr(
        PostgresApplicationReceiptStore, "transition", real_transition,
    )
    monkeypatch.setattr(
        PostgresApplicationReceiptStore, "transition_with_evidence",
        real_with_evidence,
    )
    assert len(applied) == 1
    assert fx.receipts().get("prepared") == 1, fx.receipts()
    assert fx.head_count() == AAA_RECORDS - 20  # the chunk's write committed
    stuck = _pending(fx.dsn)[0]
    done = finish_pending_undos(
        dsn=fx.dsn, deployment=DEPLOYMENT, apply=True, run_id=stuck["run_id"],
    )
    assert [r["status"] for r in done["results"]] == ["refused"], done
    error = done["results"][0]["error"]
    assert error["code"] == "fact_batch_recovery_blocked"
    assert "--accept-current" in error["fix_hint"]
    assert len(done["still_pending"]) == 1


# ---------------------------------------------------------------------------
# Option 2: after --accept-current the source must rerun
# ---------------------------------------------------------------------------

KEPT_REASON = "the run base was lost"
RERUN_COMMAND = "okg ingest --deployment svc --include svc.aaa"


def _keep_partial_aaa(fx: _Fixture, monkeypatch) -> dict[str, Any]:
    """A run of aaa removes 20 records in chunk 1 and fails in chunk 2 with
    its run base gone, so it cannot be undone; the operator keeps the rows
    (finish-undo --accept-current). The head then holds 45 of aaa's 65
    records: the failed run's partial state. Returns what readers saw."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    before = fx.published()
    fx.write("aaa", _removal_spec())
    calls: list[int] = []

    def flush(self, facts, markers, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            with psycopg.connect(fx.dsn, autocommit=True) as conn:
                conn.execute(
                    "DELETE FROM _chronos_branch_interval_checkpoints "
                    "WHERE checkpoint_id LIKE 'okg-run-base:%%'"
                )
            raise RuntimeError("injected failure on flush 2")
        return _REAL_FLUSH(self, facts, markers, **kwargs)

    monkeypatch.setattr(_Runner, "_flush_chunk", flush)
    assert fx.ingest({"aaa"}).any_failed is True
    monkeypatch.setattr(_Runner, "_flush_chunk", _REAL_FLUSH)
    kept = finish_pending_undos(
        dsn=fx.dsn, deployment=DEPLOYMENT, accept_current=True,
        reason=KEPT_REASON, apply=True,
    )
    assert [r["status"] for r in kept["results"]] == ["accepted_current"], kept
    assert kept["results"][0]["rerun_required"] is True
    assert RERUN_COMMAND in kept["results"][0]["fix_hint"]
    assert kept["still_pending"] == []
    assert fx.head_count() == AAA_RECORDS - 20
    return before


def _rerun_rows(dsn: str) -> list[dict[str, Any]]:
    from okg.substrate.ingest.run_base import rerun_required

    with psycopg.connect(dsn, autocommit=True) as conn:
        return rerun_required(conn, deployment=DEPLOYMENT)


def _refused_publish(report: dict[str, Any]) -> dict[str, Any]:
    failed = [p for p in report["publishes"] if p.get("status") == "fail"]
    assert failed, report["publishes"]
    return failed[0]


def _doctor(fx: _Fixture, monkeypatch, capsys) -> dict[str, Any]:
    import argparse

    from okg.substrate.cli.doctor import _cmd_doctor

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    monkeypatch.setenv("OKG_DSN", fx.dsn)
    capsys.readouterr()
    _cmd_doctor(
        argparse.Namespace(
            deployment=DEPLOYMENT, dsn=fx.dsn, check=None, json=True,
        ),
        as_json=True,
    )
    return json.loads(capsys.readouterr().out)


def _ready(fx: _Fixture) -> dict[str, Any]:
    from okg.substrate.cli import deployment as deployment_cli

    return deployment_cli._deployment_ready_live_checks(
        deployment=DEPLOYMENT,
        deployment_dir=fx.dep,
        dsn=fx.dsn,
        max_active_queue_depth=1_000,
        max_source_age_seconds=10**7,
        readiness_profile="general",
        skip_doctor=True,
    )


def _ready_check(payload: dict[str, Any]) -> dict[str, Any]:
    checks = [c for c in payload["checks"] if c["id"] == "publish.source_undo"]
    assert len(checks) == 1, [c["id"] for c in payload["checks"]]
    return checks[0]


def test_after_accept_current_every_publish_waits_for_the_rerun(
    fx: _Fixture, monkeypatch,
) -> None:
    """Option 2, test 1: after --accept-current another source's publish is
    refused as rerun-required, readers keep the last good generation, and the
    operator's reason is kept durably with the block."""
    from okg.substrate.errors import SubstrateError

    before = _keep_partial_aaa(fx, monkeypatch)
    rows = _rerun_rows(fx.dsn)
    assert [(r["source"], r["reason"]) for r in rows] == [(AAA, KEPT_REASON)]

    report = _publish_bbb_alone(fx, monkeypatch)
    publish = _refused_publish(report)
    assert publish["code"] == "source_rerun_required", publish
    assert RERUN_COMMAND in publish["fix_hint"], publish["fix_hint"]
    assert "okg remediate retire-source" in publish["fix_hint"]
    after = fx.published()
    assert after["generation"] == before["generation"]
    assert after["count"] == AAA_RECORDS, after

    # The next ingest of another source only is refused before it starts,
    # and its hint leads with the rerun, not with a drain that is refused too.
    with pytest.raises(SubstrateError) as refused:
        _publish_bbb_alone(fx, monkeypatch)
    hint = refused.value.fix_hint
    assert RERUN_COMMAND in hint, hint
    assert hint.index(RERUN_COMMAND) < hint.index("okg run --once")
    assert fx.published()["generation"] == before["generation"]


def test_a_successful_rerun_lifts_the_block_and_publishes(
    fx: _Fixture, monkeypatch,
) -> None:
    """Option 2, test 2: the failed source reruns successfully; its success
    lifts the block, its publish goes out with all 65 records, and other
    sources publish again."""
    _keep_partial_aaa(fx, monkeypatch)
    refused = _publish_bbb_alone(fx, monkeypatch)  # leaves bbb staged
    assert _refused_publish(refused)["code"] == "source_rerun_required"

    fx.write("aaa", {"records": _records(AAA_RECORDS)})
    rerun = fx.ingest({"aaa"})
    assert rerun.any_failed is False, rerun.report
    # Its staged-work publish is refused by the block this run lifts; the
    # run goes ahead and its own publish goes out.
    statuses = [
        (p.get("label"), p.get("status"), p.get("code"))
        for p in rerun.report["publishes"] if p.get("status") != "skipped"
    ]
    assert statuses[0] == (
        "staged-source-redrive", "fail", "source_rerun_required",
    ), statuses
    assert statuses[-1][1] == "published", statuses
    assert _rerun_rows(fx.dsn) == []
    after = fx.published()
    assert after["count"] == AAA_RECORDS, after

    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report


def test_a_rerun_that_fails_again_stays_blocked_and_doctor_shows_it(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Option 2, test 3: the rerun fails again. Its own undo puts the kept
    rows back, the block stays, and okg doctor names the source and the
    rerun command."""
    before = _keep_partial_aaa(fx, monkeypatch)
    fx.write("aaa", {"records": _records(AAA_RECORDS)})
    _fail_flush(monkeypatch, when=lambda s: len(s) == 2)
    again = fx.ingest({"aaa"})
    assert again.any_failed is True
    monkeypatch.setattr(_Runner, "_flush_chunk", _REAL_FLUSH)
    assert fx.head_count() == AAA_RECORDS - 20  # undone to the kept rows
    assert [r["source"] for r in _rerun_rows(fx.dsn)] == [AAA]

    publish = _refused_publish(_publish_bbb_alone(fx, monkeypatch))
    assert publish["code"] == "source_rerun_required", publish
    assert fx.published()["generation"] == before["generation"]

    report = _doctor(fx, monkeypatch, capsys)
    assert report["ok"] is False
    errors = [e for e in report["errors"] if e["code"] == "source_rerun_required"]
    assert len(errors) == 1, report["errors"]
    assert errors[0]["sources"] == [AAA]
    assert AAA in errors[0]["message"]
    assert RERUN_COMMAND in errors[0]["fix_hint"]


def test_retiring_the_source_lifts_the_block_without_partial_rows(
    fx: _Fixture, monkeypatch,
) -> None:
    """Option 2, test 4: the removal exit. `okg remediate retire-source`
    (dry run, then --apply with its plan digest) removes every aaa record,
    the partial ones too, and lifts the block in the same transaction; the
    next publish shows readers no aaa record at all, never the partial 45."""
    from okg.substrate.source_retirement import remediate_source_retirement

    before = _keep_partial_aaa(fx, monkeypatch)
    assert _refused_publish(
        _publish_bbb_alone(fx, monkeypatch)
    )["code"] == "source_rerun_required"

    import okg.substrate.fact_batch as fact_batch_module

    # The fixture lowers the chunk bound for the runner; retirement writes
    # its removals as one batch, as it does in production.
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", 10_000)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        preview = remediate_source_retirement(
            conn, source_name="aaa", deployment_name=DEPLOYMENT,
        )
        assert preview.applied is False
        assert _rerun_rows(fx.dsn) != []  # a dry run changes nothing
        applied = remediate_source_retirement(
            conn,
            source_name="aaa",
            deployment_name=DEPLOYMENT,
            expected_plan_digest=preview.details["plan"]["plan_digest"],
            confirm_source="aaa",
            apply=True,
        )
    assert applied.applied is True, applied.details
    assert _rerun_rows(fx.dsn) == []
    assert fx.head_count() == 0
    assert fx.published()["generation"] == before["generation"]

    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report
    after = fx.published()
    assert after["generation"] != before["generation"]
    assert after["count"] == 0, after


def test_doctor_and_readiness_name_the_source_that_must_rerun(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Option 2, test 5: okg doctor and okg deployment ready report the block
    with its code, the source and the commands; with no block both pass."""
    ready = _ready_check(_ready(fx))
    assert ready["status"] == "pass", ready
    report = _doctor(fx, monkeypatch, capsys)
    assert report["publish_block"] == {"blocked": False}

    _keep_partial_aaa(fx, monkeypatch)
    report = _doctor(fx, monkeypatch, capsys)
    assert report["ok"] is False
    assert report["publish_block"]["code"] == "source_rerun_required"
    errors = [e for e in report["errors"] if e["code"] == "source_rerun_required"]
    assert len(errors) == 1 and errors[0]["sources"] == [AAA], report["errors"]
    assert RERUN_COMMAND in errors[0]["fix_hint"]
    assert "okg remediate retire-source" in errors[0]["fix_hint"]

    ready = _ready_check(_ready(fx))
    assert ready["status"] == "blocker", ready
    assert AAA in ready["message"]
    assert ready["details"]["code"] == "source_rerun_required"
    assert ready["details"]["sources"] == [AAA]
    assert RERUN_COMMAND in ready["details"]["fix_hint"]


# ---------------------------------------------------------------------------
# Review round 5
# ---------------------------------------------------------------------------


def test_a_blocked_source_removed_from_the_registry_can_still_be_retired(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Round 5 (the reviewer's test_rv4_b_blocked_source_removed_from_the_
    registry): the operator deletes the blocked source's registry entry
    first. It can no longer rerun, so retire-source, named by the ownership
    id the block and its hint give, retires it anyway: every aaa record goes,
    the partial ones too, the block is lifted in the same transaction, and
    the next publish shows readers no aaa record."""
    import okg.substrate.fact_batch as fact_batch_module
    from okg.substrate.errors import SourceRetirementRefused
    from okg.substrate.source_retirement import remediate_source_retirement

    before = _keep_partial_aaa(fx, monkeypatch)
    assert _refused_publish(
        _publish_bbb_alone(fx, monkeypatch)
    )["code"] == "source_rerun_required"
    registry = fx.dep / "source_registry.yaml"
    data = yaml.safe_load(registry.read_text())
    data["sources"].pop("aaa")
    registry.write_text(yaml.safe_dump(data))

    # The hint names the exit that works in this state.
    report = _doctor(fx, monkeypatch, capsys)
    hint = [e for e in report["errors"]
            if e["code"] == "source_rerun_required"][0]["fix_hint"]
    assert f"okg remediate retire-source --source {AAA} " in hint, hint
    assert "whether or not the source is still in source_registry.yaml" in hint

    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", 10_000)
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        # A source that is neither registered nor blocked is still refused.
        with pytest.raises(SourceRetirementRefused, match="not registered"):
            remediate_source_retirement(
                conn, source_name="svc.nope", deployment_name=DEPLOYMENT,
            )
        preview = remediate_source_retirement(
            conn, source_name=AAA, deployment_name=DEPLOYMENT,
        )
        plan = preview.details["plan"]
        assert preview.applied is False
        assert plan["registry_source_id"] == AAA
        assert plan["counts"]["nodes"] == AAA_RECORDS - 20, plan["counts"]
        assert _rerun_rows(fx.dsn) != []
        applied = remediate_source_retirement(
            conn,
            source_name=AAA,
            deployment_name=DEPLOYMENT,
            expected_plan_digest=plan["plan_digest"],
            confirm_source=plan["registry_source_id"],
            apply=True,
        )
    assert applied.applied is True, applied.details
    assert _rerun_rows(fx.dsn) == []
    assert fx.head_count() == 0
    assert fx.published()["generation"] == before["generation"]

    # The documented follow-up: claim ownership for the registry without
    # aaa, then publish.
    from okg.substrate.errors import SubstrateError

    _claim_layered_fixture_catalog(fx.dsn, fx.dep)
    try:
        report = _publish_bbb_alone(fx, monkeypatch)
    except SubstrateError as exc:
        pytest.fail(f"publish after retirement refused: {exc} {exc.data}")
    assert _published_only(report), report
    after = fx.published()
    assert after["generation"] != before["generation"]
    assert after["count"] == 0, after
    doctor = _doctor(fx, monkeypatch, capsys)
    assert doctor["publish_block"] == {"blocked": False}


def test_approving_a_held_rerun_lifts_the_block(
    tmp_path, clean_runtime: str, monkeypatch,
) -> None:
    """Round 5: a held (review-mode) source's runs finish as `pending`, so
    its rerun cannot lift a rerun-required block itself. Approving the held
    rerun (okg admission decide) puts it onto `default` and lifts the block
    in the same transaction; until then the block stays."""
    from okg.substrate.ingest.run_base import mark_rerun_required
    from tests.substrate import test_held_resync_retractions as held

    use_inline_source_jobs(monkeypatch)
    dep, emission = held._deployment(tmp_path, monkeypatch, clean_runtime)
    held._accept(held._hold(clean_runtime, dep, emission, [0, 1, 2], []))
    with psycopg.connect(clean_runtime) as conn, conn.transaction():
        mark_rerun_required(
            conn, deployment="svc", source=held._SOURCE, run_id="kept-run",
            reason="kept a held source's partial rows", actor="operator",
        )

    def blocked() -> list[str]:
        from okg.substrate.ingest.run_base import rerun_required

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return [r["source"] for r in rerun_required(conn, deployment="svc")]

    # Round 6: a held run writes the quarantine branch, where no generation
    # is ever published, so it cuts no run base and never holds a publish.
    import okg.substrate.ingest.run_base as run_base_module

    real_take = run_base_module.take_run_base
    taken: list[Any] = []

    def take(store, **kwargs):
        taken.append((kwargs.get("branch"), real_take(store, **kwargs)))
        return taken[-1][1]

    monkeypatch.setattr(run_base_module, "take_run_base", take)
    rerun = held._hold(clean_runtime, dep, emission, [0, 1, 2, 3], [])
    assert taken and all(result is None for _branch, result in taken), taken
    assert rerun["status"] == "pending"
    assert blocked() == [held._SOURCE]  # a held run alone lifts nothing
    held._accept(rerun)
    assert blocked() == []
    assert held._graph(clean_runtime, "default") == held._expected(
        [0, 1, 2, 3], [],
    )


# ---------------------------------------------------------------------------
# Review round 6: no publish while another source run is still writing
# ---------------------------------------------------------------------------

FOREIGN_RUN = "foreign-run-a"


def _foreign_run(fx: _Fixture, *, live: bool = True, admission_id=None) -> str:
    """Another run of aaa that has cut its run base (as a runner does before
    its first write). ``live`` holds its liveness lock, as a running process
    does; without it the run base is what a crash or a database restore
    leaves: a row that no session owns."""
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    checkpoint_id = run_base_checkpoint_id("default", FOREIGN_RUN)
    if live:
        _LIVE_RUNS[checkpoint_id] = hold_run_liveness(fx.dsn, checkpoint_id)
    store = ChronosGraphStore.connect(fx.dsn)
    try:
        assert take_run_base(
            store, deployment=DEPLOYMENT, branch="default", source=AAA,
            run_id=FOREIGN_RUN, admission_id=admission_id,
        ) == checkpoint_id
    finally:
        store.close()
    return checkpoint_id


def _foreign_run_finishes(fx: _Fixture) -> None:
    """The foreign run ends as a successful run does: its run base is
    deleted once its admission is final, then its process lets go."""
    from okg.substrate.ingest.run_base import release_run_base

    assert release_run_base(fx.dsn, branch="default", run_id=FOREIGN_RUN) is None
    _end_run(FOREIGN_RUN)


def _scheduled_publisher(fx: _Fixture, **overrides):
    """The publisher `okg run --once` and the scheduled publisher build:
    no own source runs."""
    from okg.substrate.publisher.factory import build_deployment_publisher

    return build_deployment_publisher(
        dsn=fx.dsn,
        deployment_name=DEPLOYMENT,
        deployment_dir=fx.dep,
        command="okg publisher cycle",
        publisher_overrides=overrides or None,
    )


def _bbb_published_text(fx: _Fixture) -> str | None:
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(deployment_name=DEPLOYMENT)
        rows = store.checkout_generation(latest.checkpoint_id).query(
            "SELECT attrs FROM okg.graph_nodes WHERE node_id = 'bbb:0'",
        )
    return rows[0]["attrs"].get("text") if rows else None


def _bbb_refusal_code(fx: _Fixture, monkeypatch) -> str:
    """The code that holds an ingest of bbb back: from its refused run-end
    publish, or from the refused staged-work publish it runs first (then it
    starts no source writes)."""
    from okg.substrate.errors import SubstrateError

    try:
        report = _publish_bbb_alone(fx, monkeypatch)
    except SubstrateError as exc:
        return str(exc.data["publish_code"])
    return str(_refused_publish(report)["code"])


def test_a_foreign_live_run_refuses_a_publish_and_names_its_live_owner(
    fx: _Fixture,
) -> None:
    """Round 6: while another run's run base exists and its owner session
    is alive, the scheduled publisher (and `okg run --once`, which builds the
    same publisher) is refused with source_run_in_progress, naming the run,
    its source and its live owner. The publisher whose own run it is, named
    by run id, is not refused by it."""
    from okg.substrate.errors import SourceRunInProgress

    before = fx.published()
    _foreign_run(fx)
    with pytest.raises(SourceRunInProgress) as refused:
        _scheduled_publisher(fx).cycle()
    running = refused.value.data["in_progress"]
    assert [item["run_id"] for item in running] == [FOREIGN_RUN]
    owner = running[0]["owner"]
    assert owner["state"] == "live" and owner["pid"] and owner["backend_start"]
    assert FOREIGN_RUN in str(refused.value) and AAA in str(refused.value)
    assert "owner live" in str(refused.value)
    assert f"--run-id {FOREIGN_RUN}" in refused.value.fix_hint
    assert fx.published()["generation"] == before["generation"]

    own = _scheduled_publisher(fx, own_source_run_ids=[FOREIGN_RUN]).cycle()
    assert own.get("code") != "source_run_in_progress", own


def test_a_run_end_publish_waits_for_the_other_run_then_publishes(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 6, the choice for an ingest's own run-end publish: it waits,
    bounded, for another run in progress to finish, then publishes its own
    work. Nothing of the waiting ingest is failed or undone."""
    import threading

    monkeypatch.setenv("OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS", "60")
    _foreign_run(fx)
    finisher = threading.Timer(3.0, _foreign_run_finishes, args=(fx,))
    finisher.start()
    try:
        report = _publish_bbb_alone(fx, monkeypatch)
    finally:
        finisher.join()
    assert _published_only(report), report["publishes"]
    published = [p for p in report["publishes"] if p.get("status") == "published"]
    wait = published[-1]["source_run_wait"]
    assert wait["waited_for"] == [FOREIGN_RUN], wait
    assert wait["still_running"] == [] and wait["waited_seconds"] >= 2.0, wait
    assert _bbb_published_text(fx) == f"bbb moved on {fx.bbb_edits}"
    assert fx.published()["count"] == AAA_RECORDS


def test_a_run_end_publish_refused_at_the_bound_keeps_its_work(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 6: when the other run is still writing at the bound, the
    ingest's publish is refused with source_run_in_progress. The ingest's
    source run stays OK and accepted, its rows stay on the branch, and the
    next publish after the other run finishes publishes them."""
    monkeypatch.setenv("OKG_PUBLISH_WAIT_FOR_SOURCE_RUNS_SECONDS", "1")
    before = fx.published()
    _foreign_run(fx)
    report = _publish_bbb_alone(fx, monkeypatch)
    publish = _refused_publish(report)
    assert publish["code"] == "source_run_in_progress", publish
    assert publish["error_data"]["source_run_wait"]["still_running"] == [
        FOREIGN_RUN,
    ]
    assert [r["status"] for r in report["results"]] == ["OK"], report["results"]
    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        status = conn.execute(
            "SELECT status FROM okg.admission_envelopes WHERE admission_id = "
            "%s::uuid", (report["results"][0]["admission_id"],),
        ).fetchone()[0]
    assert status in {"accepted", "pending"}, status
    assert fx.published()["generation"] == before["generation"]
    assert fx.receipts().get("compensated", 0) == 0, fx.receipts()

    _foreign_run_finishes(fx)
    out = _scheduled_publisher(fx).cycle()
    assert out["status"] == "published", out
    assert _bbb_published_text(fx) == f"bbb moved on {fx.bbb_edits}"


def test_a_run_that_writes_between_the_guard_and_the_cut_is_not_published(
    fx: _Fixture, monkeypatch,
) -> None:
    """Round 6: the guard at the top of a cycle runs before the counts. A
    run that cuts its run base and writes a record in between is counted,
    so the segment check at the cut does not see it; the guard runs again
    under the branch write lock at the cut and refuses. Readers never get
    the record."""
    import okg.substrate.ingest.run_base as run_base_module
    from okg.substrate.errors import SourceRunInProgress

    before = fx.published()
    real_guard = run_base_module.refuse_publish_while_undo_unfinished
    calls: list[int] = []

    def guard(conn, **kwargs):
        real_guard(conn, **kwargs)
        calls.append(1)
        if len(calls) == 1:
            _foreign_run(fx)
            store = ChronosGraphStore.connect(fx.dsn)
            try:
                _apply(
                    store, [_node("aaa:half-written", "document")],
                    source=AAA, run_id=FOREIGN_RUN,
                )
            finally:
                store.close()

    monkeypatch.setattr(
        run_base_module, "refuse_publish_while_undo_unfinished", guard,
    )
    with pytest.raises(SourceRunInProgress):
        _scheduled_publisher(fx).cycle()
    assert len(calls) == 1  # the cut's call raised
    after = fx.published()
    assert after["generation"] == before["generation"]
    assert "aaa:half-written" not in after["texts"]


def test_a_run_base_nobody_owns_is_never_a_run_in_progress(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Round 6 addendum: a run base whose owner is provably gone -- its
    process crashed (its session was terminated), or the database came back
    from a restore (no session ever holds its lock) -- never reads as a run
    in progress. With no accepted admission it is an unfinished undo: every
    publish is refused with finish-undo as the way out, okg doctor names the
    run, its source and the command, and after the operator runs it the
    publish goes through."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    for case in ("crashed_process", "database_restore"):
        checkpoint_id = _foreign_run(fx, live=case == "crashed_process")
        if case == "crashed_process":
            assert _terminate_liveness_sessions(fx.dsn) == 1
            _LIVE_RUNS.pop(checkpoint_id, None)
        report = _doctor(fx, monkeypatch, capsys)
        assert report["source_runs"]["in_progress"] == [], case
        errors = [e for e in report["errors"]
                  if e["code"] == "source_run_undo_unfinished"]
        assert len(errors) == 1, (case, report["errors"])
        pending = errors[0]["details"]["pending"]
        assert [(p["run_id"], p["source"]) for p in pending] == [
            (FOREIGN_RUN, AAA),
        ], case
        assert "okg runtime finish-undo --deployment svc --apply" in (
            errors[0]["fix_hint"]
        )
        assert _bbb_refusal_code(fx, monkeypatch) == "source_run_undo_unfinished"

        done = finish_pending_undos(
            dsn=fx.dsn, deployment=DEPLOYMENT, apply=True, run_id=FOREIGN_RUN,
        )
        assert [r["status"] for r in done["results"]] == ["undone"], done
        assert done["still_pending"] == []
        report = _publish_bbb_alone(fx, monkeypatch)
        assert _published_only(report), (case, report["publishes"])
        assert _bbb_published_text(fx) == f"bbb moved on {fx.bbb_edits}"


def test_a_stale_run_base_is_named_and_cleared_but_blocks_nothing(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Round 6 addendum: a run base left by a run that ended with its writes
    accepted (its release failed) holds no unfinished work, so it blocks no
    publish. okg doctor and okg deployment ready name it, its run and source,
    and the command that deletes it; that command clears it."""
    from okg.substrate.ingest.run_base import finish_pending_undos

    with psycopg.connect(fx.dsn, autocommit=True) as conn:
        accepted = conn.execute(
            "SELECT admission_id::text FROM okg.admission_envelopes "
            "WHERE source = %s AND intent = 'source_sync' "
            "AND status IN ('accepted', 'published') "
            "LIMIT 1", (AAA,),
        ).fetchone()[0]
    checkpoint_id = _foreign_run(fx, live=False, admission_id=accepted)
    report = _doctor(fx, monkeypatch, capsys)
    stale = [w for w in report.get("warnings", [])
             if w["code"] == "stale_run_bases"]
    assert len(stale) == 1, report.get("warnings")
    assert [(d["run_id"], d["source"], d["run_base"]) for d in stale[0]["details"]] == [
        (FOREIGN_RUN, AAA, checkpoint_id),
    ]
    assert "okg runtime finish-undo --deployment svc --apply" in stale[0]["fix_hint"]
    assert report["publish_block"] == {"blocked": False}
    ready = [c for c in _ready(fx)["checks"] if c["id"] == "publish.stale_run_bases"]
    assert len(ready) == 1 and ready[0]["status"] == "warning", ready

    # It holds no publish back (the scheduled publisher is not refused)...
    _scheduled_publisher(fx).cycle()
    # ...and the named command deletes it.
    done = finish_pending_undos(dsn=fx.dsn, deployment=DEPLOYMENT, apply=True)
    assert done["stale_run_bases_deleted"] == 1, done
    assert fx.run_bases() == []
    report = _publish_bbb_alone(fx, monkeypatch)
    assert _published_only(report), report


def test_a_scheduled_publish_tick_yields_to_a_run_in_progress(
    fx: _Fixture, monkeypatch, capsys,
) -> None:
    """Round 6 fix: during another run, the scheduled publisher's tick (the
    DBOS publisher workflow's cycle) is a skip with the reason and the run
    named, not a failed publish, and okg doctor and okg deployment ready
    stay free of errors and blockers. (An operator's `okg run --once` still
    refuses loudly: see the foreign-run test above, which drives the same
    publisher it builds.)"""
    import okg.substrate.deployments as deployments
    from okg.substrate.runtime import publisher_workflow

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(fx.dep.parent))
    monkeypatch.setattr(
        deployments, "require_postgres_dsn",
        lambda _manifest, *, deployment: fx.dsn,
    )
    monkeypatch.setattr(
        publisher_workflow, "_active_source_sync_workflow_id_for_publisher",
        lambda *, deployment, dsn: None,
    )
    before = fx.published()
    _foreign_run(fx)
    summary = publisher_workflow._run_one_cycle(
        deployment=DEPLOYMENT, branch_name="default",
    )
    assert summary["status"] == "skipped", summary
    assert summary["publish_skipped"] is True
    assert summary["skipped"] == "source_run_in_progress"
    assert [r["run_id"] for r in summary["in_progress"]] == [FOREIGN_RUN]
    assert FOREIGN_RUN in summary["reason"] and AAA in summary["reason"]
    assert fx.published()["generation"] == before["generation"]

    report = _doctor(fx, monkeypatch, capsys)
    assert report["publish_block"] == {"blocked": False}
    assert [e["code"] for e in report["errors"]
            if "source_run" in e["code"] or "publish" in e["code"]] == []
    assert [w["code"] for w in report.get("warnings", [])
            if w["code"] == "source_run_in_progress"] == ["source_run_in_progress"]
    ready = _ready(fx)
    assert _ready_check(ready)["status"] == "pass"
    assert [c["status"] for c in ready["checks"]
            if c["id"] == "publish.source_run_in_progress"] == ["warning"]

    _foreign_run_finishes(fx)
    summary = publisher_workflow._run_one_cycle(
        deployment=DEPLOYMENT, branch_name="default",
    )
    assert summary.get("skipped") != "source_run_in_progress", summary


def test_the_undo_waits_for_an_in_flight_edge_ranking_change(
    clean_runtime: str,
) -> None:
    """Item 92a, review round 4, item 3: the undo to a run base rewrites
    ranked edges and their declarations, so it holds the edge ranking lock
    shared like every other such writer. While a ranking change holds that
    lock exclusively (simulated here by taking it on another connection),
    the undo waits; once the change ends, the undo completes."""
    import threading

    from okg.substrate.ingest.compensation import retract_chronos_run

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        _seed_before_run(store)
        run_base = _take(store)
        _failed_run(store, new_nodes=3)
    finally:
        store.close()
    ranking = psycopg.connect(clean_runtime)
    out: dict[str, Any] = {}
    try:
        ranking.execute(
            "SELECT pg_advisory_xact_lock(hashtext('okg.edge_precedence'), "
            "hashtext('ranking'))"
        )

        def undo() -> None:
            try:
                out["stats"] = retract_chronos_run(
                    clean_runtime, admission_id=None, source=SOURCE,
                    run_id=BAD_RUN,
                )
            except Exception as exc:  # noqa: BLE001 - asserted below
                out["stats"] = exc

        thread = threading.Thread(target=undo)
        thread.start()
        thread.join(3)
        waited = thread.is_alive()
    finally:
        ranking.rollback()
        ranking.close()
    thread.join(120)
    assert waited, out.get("stats")
    assert isinstance(out["stats"], dict), out["stats"]
    assert out["stats"]["mode"] == "undo_to_run_base"
    assert out["stats"]["run_base"] == run_base
