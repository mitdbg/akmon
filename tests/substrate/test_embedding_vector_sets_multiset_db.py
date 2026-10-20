"""Two active vector sets, and set widths on a real database.

PACT vector-set-pgvector-widths (from lane EH's lab checks of PR #3637,
2026-10-14). Free hash backend and a stand-in sentence-transformers model;
no paid call.

* Two active sets in one deployment: migrate, the real graph writer's links
  for both, a fill of each, publish, search reads only ``search_set``, and
  doctor reports both. Then two graph writers on two branches while a fill
  of one set runs.
* Widths follow pgvector's halfvec limits: a 2560- and a 3000-wide HNSW set
  are built, filled and searched (exact and approximate, through the index);
  a 4001-wide set refuses with ``hnsw`` and works with ``none``; 16001
  refuses; a 768-wide sentence-transformers set works end to end.
"""
from __future__ import annotations

import json
import sys
import threading
import time
import traceback
import types
from pathlib import Path

import psycopg
import pytest

from okg.substrate.embeddings import HashEmbedder
from okg.substrate.embeddings.config import (
    EmbeddingConfigError,
    build_embedding_execution_plan,
    parse_embedding_config,
)
from okg.substrate.embeddings.identity import reset_sentence_transformer_cache
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from tests.substrate.test_embedding_vector_sets_db import (
    SOURCE,
    _CountingHash,
    _deployment,
    _doctor,
    _issue,
    _note,
    _publish,
    _seed,
    _write,
)

pytestmark = pytest.mark.postgres


def _set(subtypes, *, backend="hash", max_chars=24_000,
         header=("subtype", "title"), kind="hnsw"):
    return {
        "backend": backend,
        "subtypes": list(subtypes),
        "max_chars": max_chars,
        "document": {"default": {"header": list(header), "body": "text"}},
        "index": {"kind": kind},
    }


def _multi(search_set: str = "main") -> dict:
    # alt differs from main by max_chars (identity) and renders another
    # header, so its documents (and their hash vectors) differ from main's.
    return {
        "enabled": True,
        "search_set": search_set,
        "sets": {
            "main": _set(["jira_issue", "note"]),
            "alt": _set(["note"], max_chars=12_000, header=("title",)),
        },
    }


def _config(block: dict):
    return parse_embedding_config(plural=block)


def _by_name(config) -> dict:
    return {item.name: item for item in config.vector_sets}


def _doc(subtype: str, title: str, text: str, *, with_type: bool = True) -> str:
    head = (f"Type: {subtype}\n" if with_type else "") + f"Title: {title}"
    return f"{head}\n\n{text}"


def _fill(dsn, deployment, config, set_name, builder=None, **kwargs) -> dict:
    """``builder`` None builds the set's own backend, as production does."""
    from okg.substrate.embeddings.backfill import run_set_backfill

    return run_set_backfill(
        dsn, deployment=deployment, config=config, set_name=set_name,
        max_usd=None, builder=builder, **kwargs,
    )


def _never(_spec):
    raise AssertionError("backend built although nothing was missing")


def _search(dsn, deployment, plan, query, **kwargs) -> dict:
    from okg.substrate.mcp import open_session, search

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="semantic", embedder=plan, **kwargs)


def _top(page: dict) -> tuple[str, float]:
    row = page["results"][0]
    return row["node_id"], float(row["distance"])


def _links_by_set(dsn: str) -> dict[str, set[str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        out: dict[str, set[str]] = {}
        for set_id, node_id in conn.execute(
            "SELECT set_id, node_id FROM okg.node_documents"
        ).fetchall():
            out.setdefault(str(set_id), set()).add(str(node_id))
        return out


def _rewrite_search_set(deployment: str, name: str) -> None:
    path = Path(deployment) / "deployment.yaml"
    manifest = json.loads(path.read_text())
    manifest["embeddings"]["search_set"] = name
    path.write_text(json.dumps(manifest))


def _vector_set_codes(report: dict) -> list[str]:
    return sorted(
        f["code"] for f in report["findings"] if f["code"].startswith("vector_set_")
    )


# --------------------------------------------------------------------------- #
# Two active sets                                                              #
# --------------------------------------------------------------------------- #

def test_two_active_sets_write_fill_publish_search_and_doctor(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets

    dsn = clean_runtime
    block = _multi("main")
    config = _config(block)
    sets = _by_name(config)
    main, alt = sets["main"], sets["alt"]
    assert main.set_id != alt.set_id
    assert config.search_vector_set.name == "main"

    deployment = _deployment(tmp_path, dsn, block)
    _seed(dsn, subtypes=("jira_issue", "note"))
    _write(dsn, _issue("jira:0", title="seed issue", text="seed body"))
    g0 = _publish(dsn, deployment)["generation_id"]

    reports = _ensure_vector_sets(dsn, deployment)
    by_id = {r["set_id"]: r for r in reports}
    assert set(by_id) == {main.set_id, alt.set_id}, reports
    for r in reports:
        assert r["created"] is True and r["watermark_generations"] == [g0]
        assert r["index"]["status"] == "present", r
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT set_id, name, active FROM okg.embedding_sets ORDER BY name"
        ).fetchall()
    assert {(str(s), n, a) for s, n, a in rows} == {
        (alt.set_id, "alt", True), (main.set_id, "main", True),
    }

    # The real graph writer links each node for every set that covers it.
    issues = [_issue(f"jira:{i}", title=f"issue {i}", text=f"issue body {i}")
              for i in range(1, 4)]
    notes = [_note(f"note:{i}", f"note body {i}") for i in range(1, 3)]
    _write(dsn, *issues, *notes)
    _publish(dsn, deployment)  # the fill reads the rows visible at the latest pin
    links = _links_by_set(dsn)
    assert links[main.set_id] >= {"jira:1", "jira:2", "jira:3", "note:1", "note:2"}
    assert links[alt.set_id] == {"note:1", "note:2"}

    fills = {name: _fill(dsn, deployment, config, name) for name in ("main", "alt")}
    for name, result in fills.items():
        assert result["status"] == "completed", (name, result)
    again = {name: _fill(dsn, deployment, config, name, _never) for name in ("main", "alt")}
    for name, result in again.items():
        assert result["status"] == "completed", (name, result)
        assert result["documents_missing"] == 0, (name, result)
        assert result["documents_embedded"] == 0, (name, result)
    assert fills["main"]["documents_embedded"] == 6, fills["main"]
    assert fills["alt"]["documents_embedded"] == 2, fills["alt"]
    _publish(dsn, deployment)

    # Search reads only the configured search_set (main).
    plan = build_embedding_execution_plan(config)
    page = _search(dsn, deployment, plan, _doc("note", "note note:1", "note body 1"),
                   limit=10)
    assert page["vector_source"] == "vector_set"
    assert page["vector_set"]["set_id"] == main.set_id
    assert page["ranking"] == "exact"
    assert _top(page) == ("note:1", pytest.approx(0.0, abs=1e-3))
    assert page["embedding_coverage"]["text_nodes"] == 6
    assert page["embedding_coverage"]["embedded_nodes"] == 6
    # alt's document text for note:2 has no exact match in main.
    alt_query = _doc("note", "note note:2", "note body 2", with_type=False)
    page = _search(dsn, deployment, plan, alt_query, limit=10)
    assert page["vector_set"]["set_id"] == main.set_id
    assert _top(page)[1] > 0.05, page["results"][:2]

    # search_set alt: the same query hits alt's own vector, notes only.
    _rewrite_search_set(deployment, "alt")
    alt_plan = build_embedding_execution_plan(_config(_multi("alt")))
    page = _search(dsn, deployment, alt_plan, alt_query, limit=10)
    assert page["vector_set"]["set_id"] == alt.set_id
    assert _top(page) == ("note:2", pytest.approx(0.0, abs=1e-3))
    assert {r["node_id"] for r in page["results"]} == {"note:1", "note:2"}
    _rewrite_search_set(deployment, "main")

    report = _doctor(dsn, deployment, config)
    entries = {e["set_id"]: e for e in report["sets"]}
    assert set(entries) == {main.set_id, alt.set_id}
    for set_id, text_nodes in ((main.set_id, 6), (alt.set_id, 2)):
        entry = entries[set_id]
        assert entry["active"] is True and entry["configured"] is True, entry
        assert entry["registered"] is True and entry["config_drift"] == []
        assert entry["index"]["status"] == "present"
        cov = entry["coverage"]
        assert (cov["text_nodes"], cov["linked_nodes"], cov["embedded_nodes"]) == (
            text_nodes, text_nodes, text_nodes,
        ), cov
        assert cov["unpublished_writes"] == 0
    assert _vector_set_codes(report) == [], report["findings"]


# --------------------------------------------------------------------------- #
# Two writers on two branches and a running fill                               #
# --------------------------------------------------------------------------- #

ROUNDS = 30
PAUSE_S = 0.05  # between rounds, so the writes span the whole fill
DEADLINE_S = 180.0


def _err(exc: BaseException) -> dict:
    return {
        "type": f"{type(exc).__module__}.{type(exc).__name__}",
        "sqlstate": getattr(exc, "sqlstate", None) or getattr(exc, "code", None),
        "message": str(exc)[:2000],
        "frames": [
            f"{f.filename.rsplit('/src/', 1)[-1]}:{f.lineno} {f.name}"
            for f in traceback.extract_tb(exc.__traceback__)[-6:]
        ],
    }


@pytest.mark.parametrize("iteration", range(2))
def test_two_branch_writers_and_a_fill_of_one_set(
    clean_runtime: str, tmp_path, iteration: int,
) -> None:
    """No hang; the only accepted failure is a PostgreSQL deadlock abort
    (40P01), raised; every successful writer round is linked in both sets."""
    from okg.substrate.db.migrate import _ensure_vector_sets

    dsn = clean_runtime
    block = _multi("main")
    config = _config(block)
    sets = _by_name(config)
    deployment = _deployment(tmp_path, dsn, block)
    _seed(dsn, subtypes=("jira_issue", "note"))
    # Written before migrate: the fill links these itself, in small windows,
    # while the writers run.
    _write(dsn, *[_issue(f"jira:{i}", title=f"i{i}", text=f"pre issue {i}") for i in range(120)],
           *[_note(f"note:{i}", f"pre note {i}") for i in range(60)])
    _publish(dsn, deployment)
    _ensure_vector_sets(dsn, deployment)
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        store.create_branch("wa", from_branch="default")
        store.create_branch("wb", from_branch="default")
    finally:
        store.close()

    class _Slow(_CountingHash):
        def embed_batch(self, texts):
            time.sleep(0.05)
            return super().embed_batch(texts)

    start = threading.Barrier(3)
    outcome: dict = {"iteration": iteration, "writers": {}, "fill": None}
    pids: dict[str, int] = {}

    def fill() -> None:
        start.wait()
        try:
            res = _fill(dsn, deployment, config, "main", lambda _s: _Slow(), window_rows=10)
            outcome["fill"] = {"status": res["status"]}
        except BaseException as exc:  # recorded and asserted below
            outcome["fill"] = {"error": _err(exc)}

    def writer(branch: str) -> None:
        rounds = []
        conn = psycopg.connect(dsn, autocommit=True)
        pids[branch] = conn.info.backend_pid
        try:
            g = ChronosGraphStore.from_connection(conn)
            start.wait()
            for r in range(ROUNDS):
                facts = [
                    _issue(f"jira:{branch}-{r}-{k}", title=f"{branch}{r}{k}",
                           text=f"{branch} issue {r} {k}") for k in range(3)
                ] + [
                    _note(f"note:{branch}-{r}-{k}", f"{branch} note {r} {k}")
                    for k in range(3)
                ] + [
                    _issue(f"jira:{r}", title=f"i{r}", text=f"{branch} edit {r}"),
                    _note(f"note:{r}", f"{branch} note edit {r}"),
                ]
                try:
                    g.apply_facts(facts, branch=branch, source=SOURCE,
                                  strict_endpoints=True)
                    rounds.append({"ok": True, "ids": [f.node_id for f in facts]})
                except BaseException as exc:  # recorded and asserted below
                    rounds.append({"ok": False, "error": _err(exc)})
                time.sleep(PAUSE_S)
        finally:
            outcome["writers"][branch] = rounds
            conn.close()

    threads = [threading.Thread(target=fill, daemon=True)] + [
        threading.Thread(target=writer, args=(b,), daemon=True) for b in ("wa", "wb")
    ]
    for t in threads:
        t.start()
    deadline = time.monotonic() + DEADLINE_S
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    hung = [t.name for t in threads if t.is_alive()]
    if hung:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for pid in pids.values():
                conn.execute("SELECT pg_cancel_backend(%s)", (pid,))
    assert not hung, outcome
    errors = [r["error"] for rs in outcome["writers"].values() for r in rs if not r["ok"]]
    if outcome["fill"].get("error"):
        errors.append(outcome["fill"]["error"])
    assert [e for e in errors if e["sqlstate"] != "40P01"] == [], errors
    links = _links_by_set(dsn)
    for rs in outcome["writers"].values():
        for r in rs:
            if r["ok"]:
                assert all(i in links[sets["main"].set_id] for i in r["ids"]), r
                assert all(
                    i in links[sets["alt"].set_id] for i in r["ids"] if i.startswith("note:")
                ), r
    if not outcome["fill"].get("error"):
        assert outcome["fill"]["status"] == "completed", outcome["fill"]


# --------------------------------------------------------------------------- #
# Widths: pgvector's halfvec limits                                            #
# --------------------------------------------------------------------------- #

def _index_scans(dsn: str, index: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT coalesce(sum(idx_scan), 0) FROM pg_stat_user_indexes "
            "WHERE schemaname = 'okg' AND indexrelname = %s", (index,),
        ).fetchone()
        return int(row[0])


def _stored_widths(dsn: str, set_id: str) -> list[tuple[int]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return conn.execute(
            "SELECT DISTINCT vector_dims(embedding::vector) FROM okg.text_embeddings "
            "WHERE set_id = %s", (set_id,),
        ).fetchall()


def _width_graph(dsn: str, deployment: str) -> None:
    _seed(dsn, subtypes=("jira_issue", "note"))
    nodes = [_issue(f"jira:{i}", title=f"issue {i}", text=f"issue body {i} alpha{i} beta{i}")
             for i in range(40)]
    nodes += [_note(f"note:{i}", f"note body {i}") for i in range(20)]
    _write(dsn, *nodes)
    _publish(dsn, deployment)


KNOWN = {
    "jira:7": _doc("jira_issue", "issue 7", "issue body 7 alpha7 beta7"),
    "jira:33": _doc("jira_issue", "issue 33", "issue body 33 alpha33 beta33"),
    "note:12": _doc("note", "note note:12", "note body 12"),
}


@pytest.mark.parametrize(("backend", "width"), [("hash", 2560), ("hash@3000", 3000)])
def test_an_hnsw_set_builds_fills_and_finds_the_nearest_exactly_and_through_the_index(
    clean_runtime: str, tmp_path, monkeypatch, backend: str, width: int,
) -> None:
    """The 3000-wide case is the must-fail of this change: before it,
    ``hash@3000`` was refused as an unknown spec, and no set could be wider
    than 2560."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings.backfill import dry_run_backfill, show_node_document
    from okg.substrate.embeddings.store import set_index_name

    dsn = clean_runtime
    block = {"enabled": True, "sets": {"main": _set(["jira_issue", "note"], backend=backend)}}
    config = _config(block)
    vs = config.search_vector_set
    assert vs.width == width and vs.index.kind == "hnsw"
    deployment = _deployment(tmp_path, dsn, block)
    _width_graph(dsn, deployment)
    (report,) = _ensure_vector_sets(dsn, deployment)
    assert report["index"]["status"] == "present", report
    index = set_index_name(vs)
    with psycopg.connect(dsn, autocommit=True) as conn:
        indexdef, registered_width = conn.execute(
            "SELECT i.indexdef, s.width FROM pg_indexes AS i, okg.embedding_sets AS s "
            "WHERE i.schemaname = 'okg' AND i.indexname = %s AND s.set_id = %s",
            (index, vs.set_id),
        ).fetchone()
    assert "hnsw" in indexdef and f"halfvec({width})" in indexdef, indexdef
    assert registered_width == width

    dry = dry_run_backfill(dsn, deployment=deployment, config=config, set_name="main")
    (dry_set,) = dry["sets"]
    assert dry_set["set_id"] == vs.set_id and dry_set["documents_missing"] == 60, dry_set
    res = _fill(dsn, deployment, config, "main")
    assert res["status"] == "completed" and res["documents_embedded"] == 60, res
    _publish(dsn, deployment)
    assert _stored_widths(dsn, vs.set_id) == [(width,)]
    shown = show_node_document(
        dsn, deployment=deployment, config=config, set_name="main", node_id="jira:7",
    )
    assert shown["has_vector"] is True and shown["readable_at_pin"] is True, shown

    plan = build_embedding_execution_plan(config)
    assert plan.observed_dimension == width
    assert plan.node_column is (width == 2560)
    scans: dict[str, int] = {}
    for ranking, exact_max in (("exact", 5_000), ("approximate", 2)):
        monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
        before = _index_scans(dsn, index)
        for node_id, query in KNOWN.items():
            page = _search(dsn, deployment, plan, query, limit=5)
            assert page["ranking"] == ranking, page.get("ranking")
            assert page["vector_set"]["set_id"] == vs.set_id
            assert _top(page) == (node_id, pytest.approx(0.0, abs=1e-3)), (
                ranking, [(r["node_id"], r["distance"]) for r in page["results"]],
            )
        # pg_stat is flushed asynchronously: wait up to 5 s.
        after = before
        for _ in range(50):
            after = _index_scans(dsn, index)
            if after > before:
                break
            time.sleep(0.1)
        scans[ranking] = after - before
    assert scans["approximate"] >= len(KNOWN), scans

    doctor = _doctor(dsn, deployment, config)
    (entry,) = doctor["sets"]
    assert entry["index"]["status"] == "present"
    cov = entry["coverage"]
    assert (cov["text_nodes"], cov["embedded_nodes"]) == (60, 60), cov
    assert _vector_set_codes(doctor) == [], doctor["findings"]


def test_a_set_above_4000_refuses_hnsw_and_works_with_kind_none(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.deployments import DeploymentConfigError

    dsn = clean_runtime
    hnsw = {"enabled": True, "sets": {"main": _set(["jira_issue", "note"], backend="hash@4001")}}
    with pytest.raises(EmbeddingConfigError) as caught:
        _config(hnsw)
    assert "pgvector's halfvec HNSW index takes at most 4000" in str(caught.value)
    refused = _deployment(tmp_path / "refused", dsn, hnsw)
    _seed(dsn, subtypes=("jira_issue", "note"))
    with pytest.raises((EmbeddingConfigError, SubstrateError, DeploymentConfigError)) as caught:
        _ensure_vector_sets(dsn, refused)
    assert "4000" in str(caught.value)
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM okg.embedding_sets").fetchone()[0] == 0

    too_wide = {"enabled": True, "sets": {"main": _set(
        ["note"], backend="hash@16001", kind="none",
    )}}
    with pytest.raises(EmbeddingConfigError) as caught:
        _config(too_wide)
    assert "pgvector's halfvec type stores at most 16000" in str(caught.value)

    none = {"enabled": True, "sets": {"main": _set(
        ["jira_issue", "note"], backend="hash@4001", kind="none",
    )}}
    config = _config(none)
    vs = config.search_vector_set
    deployment = _deployment(tmp_path / "none", dsn, none)
    _write(dsn, *[_issue(f"jira:{i}", title=f"issue {i}", text=f"issue body {i} alpha{i} beta{i}")
                  for i in range(40)],
           *[_note(f"note:{i}", f"note body {i}") for i in range(20)])
    _publish(dsn, deployment)
    (report,) = _ensure_vector_sets(dsn, deployment)
    assert report["index"]["status"] == "not_indexed", report
    res = _fill(dsn, deployment, config, "main")
    assert res["status"] == "completed", res
    _publish(dsn, deployment)
    assert _stored_widths(dsn, vs.set_id) == [(4001,)]
    page = _search(dsn, deployment, build_embedding_execution_plan(config),
                   KNOWN["jira:33"], limit=5)
    assert page["ranking"] == "exact" and page["vector_set"]["set_id"] == vs.set_id
    assert _top(page) == ("jira:33", pytest.approx(0.0, abs=1e-3))


# --------------------------------------------------------------------------- #
# A 768-wide sentence-transformers set (stand-in model)                        #
# --------------------------------------------------------------------------- #

class _StandInModel:
    """Reports 768 dimensions; its vectors are the hash embedder's."""

    def __init__(self, name: str, device=None) -> None:
        self.name = name

    def get_sentence_embedding_dimension(self) -> int:
        return 768

    def encode(self, texts, **_kwargs):
        return [HashEmbedder(768).embed(text) for text in texts]


@pytest.fixture
def stand_in_model(monkeypatch):
    module = types.ModuleType("sentence_transformers")
    module.SentenceTransformer = _StandInModel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", module)
    reset_sentence_transformer_cache()
    yield
    reset_sentence_transformer_cache()


def test_a_768_wide_sentence_transformers_set_works_end_to_end(
    clean_runtime: str, tmp_path, stand_in_model,
) -> None:
    """Must-fail of this change: before it, a sentence-transformers set was
    2560 wide whatever its model, and its plan refused a 768-wide model."""
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings.backfill import show_node_document

    dsn = clean_runtime
    block = {"enabled": True, "sets": {"main": _set(
        ["jira_issue", "note"], backend="sentence-transformers:okg-stand-in-768",
    )}}
    config = _config(block)
    vs = config.search_vector_set
    assert vs.width == 768
    deployment = _deployment(tmp_path, dsn, block)
    _width_graph(dsn, deployment)
    (report,) = _ensure_vector_sets(dsn, deployment)
    assert report["index"]["status"] == "present", report
    res = _fill(dsn, deployment, config, "main")
    assert res["status"] == "completed" and res["documents_embedded"] == 60, res
    _publish(dsn, deployment)
    assert _stored_widths(dsn, vs.set_id) == [(768,)]
    plan = build_embedding_execution_plan(config)
    assert plan.observed_dimension == 768 and plan.node_column is False
    page = _search(dsn, deployment, plan, KNOWN["note:12"], limit=5)
    assert page["vector_set"]["set_id"] == vs.set_id
    assert _top(page) == ("note:12", pytest.approx(0.0, abs=1e-3))
    shown = show_node_document(
        dsn, deployment=deployment, config=config, set_name="main", node_id="note:12",
    )
    assert shown["has_vector"] is True, shown
    assert _vector_set_codes(_doctor(dsn, deployment, config)) == []


def test_doctor_reports_a_sentence_transformers_model_that_cannot_load(
    clean_runtime: str, tmp_path, stand_in_model, monkeypatch,
) -> None:
    """Must-fail: before the fix, doctor raised from the set's width lookup
    when its model could not load, and reported no set at all."""
    from okg.substrate.db.migrate import _ensure_vector_sets

    dsn = clean_runtime
    block = {"enabled": True, "search_set": "main", "sets": {
        "main": _set(["jira_issue", "note"]),
        "local": _set(["note"], backend="sentence-transformers:okg-stand-in-768"),
    }}
    deployment = _deployment(tmp_path, dsn, block)
    _ensure_vector_sets(dsn, deployment)
    # The model is gone (sentence_transformers not installed on this host).
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    reset_sentence_transformer_cache()
    report = _doctor(dsn, deployment, _config(block))
    failed = [f for f in report["findings"] if f["code"] == "vector_set_probe_failed"]
    assert len(failed) == 1, report["findings"]
    assert failed[0]["set"] == "local"
    assert "could not load the model of vector set 'local'" in failed[0]["message"]
    assert "EmbedderModelMissing" in failed[0]["message"]
    assert "vector_set_not_configured" not in _vector_set_codes(report)
    assert [entry["set"] for entry in report["sets"]] == ["main"]


def test_doctor_does_not_report_a_hash_set_defect_as_a_missing_model(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Must-fail: before the catch was narrowed, any error from a set's id
    lookup became a finding telling the operator to install a
    sentence-transformers model, even for a hash set, which loads none."""
    dsn = clean_runtime
    block = {"enabled": True, "search_set": "main", "sets": {
        "main": _set(["jira_issue", "note"]),
        "aux": _set(["note"], max_chars=12_000),
    }}
    config = _config(block)
    deployment = _deployment(tmp_path, dsn, block)
    item_class = type(config.search_vector_set)
    real_identity = item_class.identity

    def broken_identity(self):
        if self.name == "aux":
            raise ValueError("identity defect in a hash set")
        return real_identity(self)

    monkeypatch.setattr(item_class, "identity", broken_identity)
    with pytest.raises(ValueError, match="identity defect in a hash set"):
        _doctor(dsn, deployment, config)


# --------------------------------------------------------------------------- #
# Storage: halfvec (default) or vector                                         #
# --------------------------------------------------------------------------- #

def test_a_vector_storage_set_builds_fills_and_searches_through_its_index(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """Must-fail of the storage choice: before it, ``storage`` was an
    unknown key and every set stored halfvec."""
    import okg.substrate.semantic_ann as semantic_ann
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.embeddings.backfill import show_node_document
    from okg.substrate.embeddings.store import set_index_name

    dsn = clean_runtime
    main = _set(["jira_issue", "note"], backend="hash@1536")
    halfvec_id = _config({"enabled": True, "sets": {"main": main}}).search_vector_set.set_id
    block = {"enabled": True, "sets": {"main": {**main, "storage": "vector"}}}
    config = _config(block)
    vs = config.search_vector_set
    assert vs.storage == "vector" and vs.width == 1536
    assert vs.set_id != halfvec_id, "storage is part of the identity"
    deployment = _deployment(tmp_path, dsn, block)
    _width_graph(dsn, deployment)
    (report,) = _ensure_vector_sets(dsn, deployment)
    assert report["index"]["status"] == "present", report
    index = set_index_name(vs)
    with psycopg.connect(dsn, autocommit=True) as conn:
        (indexdef,) = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'okg' AND indexname = %s",
            (index,),
        ).fetchone()
    assert "embedding_f32" in indexdef and "vector(1536)" in indexdef, indexdef
    assert "vector_cosine_ops" in indexdef and "halfvec" not in indexdef, indexdef
    res = _fill(dsn, deployment, config, "main")
    assert res["status"] == "completed" and res["documents_embedded"] == 60, res
    _publish(dsn, deployment)
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT count(*), count(embedding), "
            "       array_agg(DISTINCT vector_dims(embedding_f32)) "
            "  FROM okg.text_embeddings WHERE set_id = %s", (vs.set_id,),
        ).fetchone()
    assert rows == (60, 0, [1536]), rows

    plan = build_embedding_execution_plan(config)
    scans: dict[str, int] = {}
    for ranking, exact_max in (("exact", 5_000), ("approximate", 2)):
        monkeypatch.setattr(semantic_ann, "EXACT_SCOPE_MAX", exact_max)
        before = _index_scans(dsn, index)
        for node_id, query in KNOWN.items():
            page = _search(dsn, deployment, plan, query, limit=5)
            assert page["ranking"] == ranking, page.get("ranking")
            assert page["vector_set"]["set_id"] == vs.set_id
            assert _top(page) == (node_id, pytest.approx(0.0, abs=1e-5)), (
                ranking, [(r["node_id"], r["distance"]) for r in page["results"]],
            )
        after = before
        for _ in range(50):
            after = _index_scans(dsn, index)
            if after > before:
                break
            time.sleep(0.1)
        scans[ranking] = after - before
    assert scans["approximate"] >= len(KNOWN), scans
    shown = show_node_document(
        dsn, deployment=deployment, config=config, set_name="main", node_id="jira:7",
    )
    assert shown["has_vector"] is True and shown["readable_at_pin"] is True, shown
    doctor = _doctor(dsn, deployment, config)
    (entry,) = doctor["sets"]
    assert entry["index"]["status"] == "present"
    assert (entry["coverage"]["text_nodes"], entry["coverage"]["embedded_nodes"]) == (60, 60)
    assert _vector_set_codes(doctor) == [], doctor["findings"]


def test_a_vector_storage_set_refuses_hnsw_above_2000(clean_runtime: str, tmp_path) -> None:
    from okg.substrate.db.migrate import _ensure_vector_sets
    from okg.substrate.deployments import DeploymentConfigError

    block = {"enabled": True, "sets": {"main": {
        **_set(["note"], backend="hash@2001"), "storage": "vector",
    }}}
    with pytest.raises(EmbeddingConfigError) as caught:
        _config(block)
    assert "pgvector's vector HNSW index takes at most 2000" in str(caught.value)
    deployment = _deployment(tmp_path, clean_runtime, block)
    _seed(clean_runtime, subtypes=("note",))
    with pytest.raises((EmbeddingConfigError, SubstrateError, DeploymentConfigError)):
        _ensure_vector_sets(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute("SELECT count(*) FROM okg.embedding_sets").fetchone()[0] == 0


def _set_state(dsn: str, set_id: str, index: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        vectors = conn.execute(
            "SELECT doc_hash, embedding::text, seq FROM okg.text_embeddings "
            "WHERE set_id = %s ORDER BY doc_hash", (set_id,),
        ).fetchall()
        (indexdef,) = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = 'okg' AND indexname = %s",
            (index,),
        ).fetchone()
        registered = conn.execute(
            "SELECT set_id, width, identity_json FROM okg.embedding_sets"
        ).fetchall()
    return {"vectors": vectors, "indexdef": indexdef, "registered": registered}


def _table_shape(dsn: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        columns = conn.execute(
            "SELECT attname, attnotnull, format_type(atttypid, atttypmod) "
            "  FROM pg_attribute WHERE attrelid = 'okg.text_embeddings'::regclass "
            "   AND attnum > 0 AND NOT attisdropped ORDER BY attname"
        ).fetchall()
        check = conn.execute(
            "SELECT convalidated, pg_get_constraintdef(oid) FROM pg_constraint "
            " WHERE conrelid = 'okg.text_embeddings'::regclass "
            "   AND conname = 'text_embeddings_one_vector'"
        ).fetchone()
    return {"columns": columns, "check": check}


def test_migrate_upgrades_an_existing_halfvec_set_without_changing_it(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    """A database whose ``okg.text_embeddings`` predates the storage choice
    (``embedding halfvec NOT NULL``, no ``embedding_f32``) holds a filled
    halfvec set. Two migrations add the column and the one-vector check and
    leave the set's id, vectors and index exactly as they were."""
    import okg.substrate.db.management as management
    from okg.substrate.db.migrate import _ensure_vector_sets, migrate
    from okg.substrate.embeddings.store import set_index_name

    dsn = clean_runtime
    block = {"enabled": True, "sets": {"main": _set(["jira_issue", "note"])}}
    config = _config(block)
    vs = config.search_vector_set
    assert vs.storage == "halfvec" and "storage" not in vs.identity()
    deployment = _deployment(tmp_path, dsn, block)
    fresh_shape = _table_shape(dsn)
    assert fresh_shape["check"] == (
        True, "CHECK ((num_nonnulls(embedding, embedding_f32) = 1))",
    ), fresh_shape
    # Replay the schema body, as an upgrade does.
    monkeypatch.setattr(management, "applied_executable_schema_checksum", lambda conn: None)
    with psycopg.connect(dsn, autocommit=True) as conn:
        # The table as the previous release created it.
        conn.execute(
            "ALTER TABLE okg.text_embeddings "
            "DROP CONSTRAINT text_embeddings_one_vector, DROP COLUMN embedding_f32"
        )
        conn.execute("ALTER TABLE okg.text_embeddings ALTER COLUMN embedding SET NOT NULL")
    try:
        _width_graph(dsn, deployment)
        _ensure_vector_sets(dsn, deployment)
        assert _fill(dsn, deployment, config, "main")["status"] == "completed"
        _publish(dsn, deployment)
        index = set_index_name(vs)
        before = _set_state(dsn, vs.set_id, index)
        assert len(before["vectors"]) == 60
        assert _table_shape(dsn) != fresh_shape
        for _ in range(2):
            migrate(dsn)
            reports = _ensure_vector_sets(dsn, deployment)
            assert [(r["set_id"], r["created"]) for r in reports] == [(vs.set_id, False)]
            assert _table_shape(dsn) == fresh_shape
            assert _set_state(dsn, vs.set_id, index) == before
    finally:
        # Never leave the old shape behind for the next test on this database.
        if _table_shape(dsn) != fresh_shape:
            migrate(dsn)
    page = _search(dsn, deployment, build_embedding_execution_plan(config),
                   KNOWN["jira:7"], limit=5)
    assert page["vector_set"]["set_id"] == vs.set_id
    assert _top(page) == ("jira:7", pytest.approx(0.0, abs=1e-3))


def _schema_body_exclusive_locks(dsn: str) -> list[str]:
    """Run schema.sql's body in one transaction, as `okg migrate` does when
    the file changed, and return the ACCESS EXCLUSIVE locks this backend
    holds on ``okg.text_embeddings`` just before it commits."""
    import importlib.resources

    sql = importlib.resources.files("okg.substrate.db").joinpath("schema.sql").read_text(
        encoding="utf-8"
    )
    with psycopg.connect(dsn) as conn:
        conn.execute(sql)
        held = [mode for (mode,) in conn.execute(
            "SELECT mode FROM pg_locks WHERE pid = pg_backend_pid() "
            "   AND locktype = 'relation' "
            "   AND relation = 'okg.text_embeddings'::regclass "
            "   AND mode = 'AccessExclusiveLock'"
        ).fetchall()]
        conn.commit()
    return held


def test_a_schema_replay_on_an_upgraded_table_takes_no_exclusive_lock(
    clean_runtime: str, tmp_path,
) -> None:
    """Must-fail: before the catalog guard, ``ADD COLUMN IF NOT EXISTS``
    took ACCESS EXCLUSIVE on ``okg.text_embeddings`` on every replay of
    schema.sql (any later edit to the file replays it), and held it until
    migrate committed. The first replay below upgrades an old-shape table
    and must lock it; the second, on the upgraded table, must not."""
    from okg.substrate.db.migrate import migrate

    dsn = clean_runtime
    fresh_shape = _table_shape(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        # The table as the previous release created it.
        conn.execute(
            "ALTER TABLE okg.text_embeddings "
            "DROP CONSTRAINT text_embeddings_one_vector, DROP COLUMN embedding_f32"
        )
        conn.execute("ALTER TABLE okg.text_embeddings ALTER COLUMN embedding SET NOT NULL")
    try:
        assert _schema_body_exclusive_locks(dsn) == ["AccessExclusiveLock"]
        assert _table_shape(dsn) == fresh_shape
        assert _schema_body_exclusive_locks(dsn) == []
        assert _table_shape(dsn) == fresh_shape
    finally:
        # Never leave the old shape behind for the next test on this database.
        if _table_shape(dsn) != fresh_shape:
            migrate(dsn)
