"""Issue #1431 — a forbidden retraction must leave the whole run undone.

`SourceRunner` flushes durable chunks while it streams (every
`MARKER_BATCH_SIZE` progress markers or `COPY_CHUNK_ROWS` facts).  The
retraction-policy guard used to run lazily, on the first `op='R'` fact, so a
forbidden retraction arriving after earlier flushes left those chunks
committed.  These tests stream more than one flush-sized insertion prefix
before the forbidden retraction and assert the run leaves no fact rows, no
graph rows, no progress markers, and no application receipts.

The file also covers the opposite failure: a retraction the runner REFUSED
that it actually owed (issue #1454, below).

Issue #1714 shares this module because it asks the same question of the same
machinery from the other side: what a run is allowed to leave behind in the
physical Chronos interval tables.
"""
from __future__ import annotations

import base64
import hashlib
import json
import sys
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.runner import SourceRunContext, SourceRunner
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    ProgressMarker,
    RecordAuthorityStream,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    preserve_cursor_record_set_authority,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
)
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from okg.substrate.producer_authority import ProducerProfile
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres


# The runner flushes every 100 progress markers; 250 records therefore commit
# two whole chunks before the retraction is ever seen.
PREFIX_RECORDS = 250


class _LateRetractionAdapter:
    """Streams an insertion prefix, then (optionally) one `op='R'` fact."""

    name = "late-retraction"
    profile = "mutable_api"

    def __init__(self, *, records: int, retract: bool) -> None:
        self.records = records
        self.retract = retract

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
    ) -> SourceRun:
        del run_id, cursor, sync_scope

        def _facts():
            for index in range(self.records):
                yield NodeFact(
                    node_id=f"t:{index}",
                    subtype="ticket",
                    attrs={"text": f"ticket {index}"},
                    source_record_id={"id": f"r-{index}"},
                    source_revision={"v": 1},
                )
                yield ProgressMarker(
                    record_id=f"r-{index}",
                    fingerprint=f"fp-{index}",
                )
            if self.retract:
                yield NodeFact(
                    node_id="t:gone",
                    subtype="ticket",
                    attrs={"text": "gone"},
                    source_record_id={"id": "r-gone"},
                    source_revision={"v": 1},
                    op="R",
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=False,
            next_cursor={"cursor": str(self.records)},
            run_mode=mode,
        )


def _profile(producer_id: str) -> ProducerProfile:
    return ProducerProfile(
        deployment_name="default",
        producer_id=producer_id,
        producer_kind="source",
        trust_label="trusted",
        admission_mode="fast_track",
        authority_scope={},
        allowed_record_kinds=("node",),
        output_signature={},
        policy_clauses=(),
        output_scope_summary={"nodes": ["ticket"]},
        source_ref={"source": "fixture"},
        catalog_version_id=None,
        profile_fingerprint="sha256:" + "b" * 64,
        status="active",
    )


def _durable_state(dsn: str, source_name: str) -> dict[str, int]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE source = %s",
            (source_name,),
        )
        node_facts = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM okg.source_progress "
            "WHERE source_name = %s AND key LIKE 'record:%%'",
            (source_name,),
        )
        progress = cur.fetchone()[0]
        cur.execute(
            "SELECT state, count(*) FROM okg.fact_batch_receipts "
            "WHERE run_id = %s::uuid GROUP BY state",
            (_RUN_IDS[source_name],),
        )
        receipts = {row[0]: row[1] for row in cur.fetchall()}

    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = graph.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE source = :source",
            {"source": source_name},
        )
    finally:
        graph.close()
    return {
        "node_facts": node_facts,
        "progress": progress,
        "receipts": receipts,
        "graph_nodes": int(rows[0]["n"]) if rows else 0,
    }


_RUN_IDS: dict[str, str] = {}


def _run(
    dsn: str,
    source_name: str,
    adapter: _LateRetractionAdapter,
    *,
    mode: str,
):
    """Run `adapter` through the orchestrated (admission-bound) path."""
    from okg.substrate.admission import ensure_envelope

    profile = _profile(source_name)
    run_id = uuid.uuid4()
    _RUN_IDS[source_name] = str(run_id)
    idempotency_key = f"{source_name}-{run_id}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        envelope = ensure_envelope(
            conn,
            deployment_name="default",
            producer_id=profile.producer_id,
            producer_kind="source",
            intent="source_sync",
            admission_mode="fast_track",
            status="pending",
            idempotency_key=idempotency_key,
            source=profile.producer_id,
            run_id=run_id,
            authority={"profile_fingerprint": profile.profile_fingerprint},
        )
    runner = SourceRunner(
        dsn,
        source_name=source_name,
        deployment_name="default",
    )
    return runner.run(
        adapter,
        mode=mode,
        run_context=SourceRunContext(
            run_id=str(run_id),
            observed_at=datetime.now(timezone.utc),
            admission_id=envelope.admission_id,
            idempotency_key=idempotency_key,
            producer_profile=replace(profile),
        ),
    )


def test_late_forbidden_retraction_undoes_every_flushed_chunk(
    clean_runtime: str,
) -> None:
    """#1431: a forbidden retraction after two flushes leaves nothing durable."""
    source_name = "source:late-retraction-refused"
    # Baseline: the managed-probe contract promotes a source's first read to an
    # authoritative reconcile, which IS allowed to retract. Spend that first
    # read on an empty run so the tested run reaches the cursor-mode boundary.
    _run(
        clean_runtime,
        source_name,
        _LateRetractionAdapter(records=0, retract=False),
        mode="cursor",
    )
    before = _durable_state(clean_runtime, source_name)

    with pytest.raises(SubstrateError) as exc:
        _run(
            clean_runtime,
            source_name,
            _LateRetractionAdapter(records=PREFIX_RECORDS, retract=True),
            mode="cursor",
        )
    assert exc.value.code == "retraction_not_permitted"

    after = _durable_state(clean_runtime, source_name)
    assert after["node_facts"] == before["node_facts"], (
        "refused run left fact rows from earlier flushes"
    )
    assert after["graph_nodes"] == before["graph_nodes"], (
        "refused run left graph nodes from earlier flushes"
    )
    assert after["progress"] == before["progress"], (
        "refused run left progress markers from earlier flushes"
    )
    # The refused run's receipts survive as `compensated` evidence — that is
    # the proof the chunks were undone, and `assert_terminal_disposition_
    # allowed` requires it before the admission may go terminal. What must NOT
    # survive is a receipt still claiming its chunk is applied.
    assert set(after["receipts"]) == {"compensated"}, after["receipts"]
    assert after["receipts"]["compensated"] >= 2, (
        "the run must have applied more than one chunk before the refusal"
    )


def test_permitted_retraction_after_many_flushes_still_applies(
    clean_runtime: str,
) -> None:
    """Reconcile mode may retract; the multi-flush stream must still land."""
    source_name = "source:late-retraction-permitted"
    result = _run(
        clean_runtime,
        source_name,
        _LateRetractionAdapter(records=PREFIX_RECORDS, retract=True),
        mode="reconcile",
    )
    assert result.nodes_appended == PREFIX_RECORDS
    assert result.retractions_node == 1
    state = _durable_state(clean_runtime, source_name)
    assert state["graph_nodes"] >= PREFIX_RECORDS
    assert state["progress"] == PREFIX_RECORDS


# --- doc-corpus content revision (issue #1325) -------------------------------
#
# `source_revision` used to carry `run_id`, so the Chronos writer's semantic
# no-op filter (which compares `source_revision`) could never drop a
# doc-corpus row: every replay rewrote the entire footprint. These tests run
# the real adapter through the real runner twice over an unchanged corpus and
# assert the second pass writes no new interval rows.


@pytest.fixture
def _doc_corpus_extractors(tmp_path):
    import yaml

    extr = tmp_path / "extractors-cfg"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "markdown",
            "class": (
                "okg.substrate.library.extraction."
                "parse.markdown.MarkdownParser"
            ),
        }],
        "chunker": [{
            "class": (
                "okg.substrate.library.extraction.chunk."
                "window.WindowChunker"
            ),
            "config": {"max_chars": 200, "overlap": 0},
        }],
    }))
    return extr


@pytest.fixture
def _doc_corpus_dir(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    (root / "a.md").write_text("# Doc A\n\n## Intro\n\nLead paragraph A.\n")
    (root / "b.md").write_text("# Doc B\n\n## Intro\n\nLead paragraph B.\n")
    return root


def _doc_corpus_node_rows(dsn: str) -> dict[str, tuple[str, str, str]]:
    """Per node: (row version, stored source_revision, stored attrs).

    `xmin` is Postgres' row-version stamp, so a change to it means the
    physical interval row was rewritten -- the churn a semantic no-op is
    supposed to avoid.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        (physical,) = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()
        rows = conn.execute(
            "SELECT node_id, xmin::text, source_revision::text, attrs::text "
            f'FROM "{physical}" WHERE source = %s',
            ("test.doc_corpus",),
        ).fetchall()
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


def test_doc_corpus_replay_over_unchanged_corpus_rewrites_no_graph_rows(
    clean_runtime: str,
    _doc_corpus_dir,
    _doc_corpus_extractors,
) -> None:
    """An authoritative replay of doc-corpus over byte-identical files
    rewrites no graph row, `document` nodes included (issue #1754).

    Authoritative replay (`reset_cursor=True`) used to skip the runner's
    persisted comparison entirely, leaving the writer's semantic no-op
    filter, which compares `source_revision` and every attr, as the only
    guard. `document` nodes carry a wall-clock `parsed_at`, so every replay
    rewrote them. Replay now compares against the live branch, disregarding
    the declared volatile keys and the lineage revision, so an unchanged
    corpus writes nothing.
    """
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog_version_id = seed_catalog_version(conn)
        # The composed catalog declares `parsed_at` volatile on Document
        # (`library/ontologies/extraction/schema.yaml`, okg_volatile_fields);
        # stamp the same declaration the compose step writes.
        conn.execute(
            "INSERT INTO okg.subtypes (name, metadata, catalog_version_id) "
            "VALUES ('document', %s::jsonb, %s)",
            (json.dumps({"volatile_fields": ["parsed_at"]}), catalog_version_id),
        )

    def _run(*, reset_cursor: bool):
        adapter = DocCorpusSource(
            repo="test",
            repo_root=_doc_corpus_dir,
            non_git_walk=True,
            extractors_dir=_doc_corpus_extractors,
            include_globs=("**/*.md",),
        )
        result = SourceRunner(clean_runtime, source_name="test.doc_corpus").run(
            adapter, mode="reconcile", reset_cursor=reset_cursor,
        )
        return adapter, result

    _run(reset_cursor=False)
    before = _doc_corpus_node_rows(clean_runtime)
    assert before, "doc-corpus wrote no graph nodes"

    adapter, replay = _run(reset_cursor=True)
    assert adapter.last_run_stats["emitted_nodes"] > 0, (
        "the replay emitted nothing, so it exercised no filtering"
    )
    assert (replay.nodes_appended, replay.edges_appended) == (0, 0), (
        f"kept by key: {replay.dedupe_kept_by_key}"
    )
    assert replay.dedupe_reference == "live_branch", replay.as_dict()
    after = _doc_corpus_node_rows(clean_runtime)
    assert before.keys() == after.keys()

    # 1. No stored revision may move when the bytes did not.
    moved = {
        node_id for node_id in before
        if before[node_id][1] != after[node_id][1]
    }
    assert not moved, (
        f"{len(moved)} of {len(before)} doc-corpus rows changed "
        f"source_revision across a replay over an unchanged corpus: "
        f"{sorted(moved)[:3]}"
    )

    # 2. And none of them names the run.
    for node_id, (_xmin, revision, _attrs) in after.items():
        assert "run_id" not in json.loads(revision), (node_id, revision)

    # 3. No row is rewritten at all, `document` nodes (which carry the
    #    wall-clock `parsed_at`) included.
    assert any(
        json.loads(attrs).get("parsed_at") for _xmin, _rev, attrs in before.values()
    ), "no stored row carries parsed_at, so the document case is not exercised"
    churned = {
        node_id for node_id in before
        if before[node_id] != after[node_id]
    }
    assert not churned, (
        f"{len(churned)} of {len(before)} doc-corpus rows were rewritten by a "
        f"replay over an unchanged corpus: {sorted(churned)[:3]}"
    )


# ---------------------------------------------------------------------------
# Issue #1454 — a path-scoped run the adapter WIDENED still owns its deletions.
#
# A LaTeX caption's `caption_of` target is `visual:<image content hash>`, so
# editing a figure in place remints the target while the citing document's
# bytes never change. `DocCorpusSource` therefore abandons a narrow path scope
# and walks the whole selected corpus whenever the project manifest moved, and
# returns the complete footprint. The runner used to refuse deletion authority
# for ANY run carrying a `sync_scope`, so that complete record set was dropped
# and the caption kept its edge to the superseded figure forever. The adapter
# now declares the widening (`SourceRun.effective_scope_complete`) and the
# runner widens the effective scope before resolving deletion authority.
#
# `tests/substrate/library/sources/test_doc_corpus_figure_caption_publish.py`
# proves the same repair through a real publish, but no blocking pull-request
# lane owns that file, so the ingest-level proof lives here.
# ---------------------------------------------------------------------------

_CAPTION_EXTRACTORS = """\
parser:
  - format: latex
    class: okg.substrate.library.extraction.parse.latex.LatexParser
chunker:
  - format: latex
    class: okg.substrate.library.extraction.chunk.latex_structural.LatexStructuralChunker
ner: []
captioning:
  enabled: false
"""

_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)

_CAPTION_TEX = """\
\\documentclass{article}
\\begin{document}
\\begin{figure}
\\includegraphics{fig.png}
\\caption{One caption.}
\\end{figure}
\\end{document}
"""


def _visual_id(image_bytes: bytes) -> str:
    return f"visual:{hashlib.sha256(image_bytes).hexdigest()[:16]}"


def _caption_source(corpus: Path, extractors: Path) -> DocCorpusSource:
    return DocCorpusSource(
        repo="caption-currency",
        repo_root=corpus,
        extractors_dir=extractors,
        project_manifest={
            "repository_authority": "caption-currency",
            "projects": [{"project_dir": "paper", "entrypoints": ["main.tex"]}],
        },
    )


def _caption_of_edges(dsn: str, source_name: str) -> set[tuple[str, str]]:
    # `okg.graph_edges` is a Chronos view: a bare connection reads it as
    # EMPTY rather than raising, so it must be read through the store.
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.query(
            "SELECT src, dst FROM okg.graph_edges "
            "WHERE edge_type = 'caption_of' AND source = :source "
            "ORDER BY src, dst",
            {"source": source_name},
        )
    finally:
        store.close()
    return {(str(row["src"]), str(row["dst"])) for row in rows}


@pytest.fixture
def caption_corpus(tmp_path: Path) -> tuple[Path, Path]:
    corpus = tmp_path / "corpus"
    (corpus / "paper").mkdir(parents=True)
    (corpus / "paper" / "main.tex").write_text(_CAPTION_TEX, encoding="utf-8")
    (corpus / "paper" / "fig.png").write_bytes(_PNG + b"first")
    extractors = tmp_path / "extractors"
    extractors.mkdir()
    (extractors / "extractors.yaml").write_text(_CAPTION_EXTRACTORS)
    return corpus, extractors


def test_scoped_figure_edit_retracts_only_the_superseded_caption_edge(
    clean_runtime: str,
    caption_corpus: tuple[Path, Path],
) -> None:
    """A figure edited in place leaves EXACTLY ONE caption_of edge."""
    corpus, extractors = caption_corpus
    source_name = "proof.caption-currency-scoped"
    runner = SourceRunner(clean_runtime, source_name=source_name)

    runner.run(_caption_source(corpus, extractors), mode="scope_complete")
    first = _caption_of_edges(clean_runtime, source_name)
    assert len(first) == 1, first
    caption_id, first_target = next(iter(first))
    assert first_target == _visual_id(_PNG + b"first")

    (corpus / "paper" / "fig.png").write_bytes(_PNG + b"second")
    scope = {"scope_keys": ["paper/fig.png"]}
    widened = _caption_source(corpus, extractors)
    result = runner.run(
        widened,
        mode="scope_complete",
        sync_scope=scope,
        emit_delta_contract=True,
    )

    # The adapter abandoned the narrow scope and declared the widening, so the
    # runner had deletion authority and retracted the stale edge.
    assert widened.last_run_stats["scope_widened_for_project_manifest"] is True
    assert widened.last_run_stats["scope_status"] == "full"
    assert result.retractions_edge > 0
    # Exactly one edge: the caption points at the new figure and ONLY the new
    # figure. A retract-to-zero would be just as wrong as the stale duplicate.
    assert _caption_of_edges(clean_runtime, source_name) == {
        (caption_id, _visual_id(_PNG + b"second")),
    }
    # The delta contract keeps the two apart: the caller asked for one file,
    # the run claimed authority over the whole source.
    contract = result.source_delta_batch
    assert contract is not None
    assert contract.get("contract") != "invalid", contract
    assert contract["requested_scope"] == scope
    assert contract["effective_scope"] is None
    assert contract["fallback"] is not None
    assert contract["fallback"]["reason"] == (
        "adapter_declared_effective_scope_complete"
    )


def test_unchanged_caption_corpus_scoped_rerun_retracts_and_reprocesses_nothing(
    clean_runtime: str,
    caption_corpus: tuple[Path, Path],
) -> None:
    """A scoped rerun over an UNCHANGED corpus must not widen or retract.

    This is the negative control for the widening above, so it must take the
    same entry point: a narrow `sync_scope` over the same corpus, differing
    only in that no figure was edited and therefore the project manifest is
    unchanged. Without the scope the run never reaches the runner's widening
    block at all and the control would pass against a version of
    `DocCorpusSource` that declared `effective_scope_complete` for every run.
    """
    corpus, extractors = caption_corpus
    source_name = "proof.caption-currency-noop"
    runner = SourceRunner(clean_runtime, source_name=source_name)

    runner.run(_caption_source(corpus, extractors), mode="scope_complete")
    settled = _caption_of_edges(clean_runtime, source_name)
    assert len(settled) == 1, settled

    scope = {"scope_keys": ["paper/fig.png"]}
    repeat = _caption_source(corpus, extractors)
    result = runner.run(
        repeat,
        mode="scope_complete",
        sync_scope=scope,
        emit_delta_contract=True,
    )

    # The manifest did not move, so the adapter kept the narrow scope and the
    # runner kept refusing deletion authority: nothing retracted, nothing
    # reprocessed, and the settled edge survives untouched.
    assert repeat.last_run_stats["scope_widened_for_project_manifest"] is False
    assert repeat.last_run_stats["scope_status"] == "scoped"
    assert result.retractions_edge == 0
    assert result.retractions_node == 0
    assert repeat.last_run_stats["files_changed"] == 0
    assert repeat.last_run_stats["emitted_edges"] == 0
    assert _caption_of_edges(clean_runtime, source_name) == settled
    # The run stayed inside the scope it was given: the delta contract
    # records the same scope as requested AND effective, with no widening
    # fallback. This is what fails if `effective_scope_complete` stops
    # discriminating (e.g. is declared unconditionally) — the runner would
    # widen the effective scope of a run that read one file.
    contract = result.source_delta_batch
    assert contract is not None
    assert contract["requested_scope"] == scope
    assert contract["effective_scope"] == scope
    assert contract["fallback"] is None


class _StaticProbe:
    """A change probe that always reports the same token, unchanged."""

    def __init__(self, token: dict) -> None:
        self._token = token

    def probe(
        self,
        *,
        cursor: dict | None,
        sync_scope: dict | None,
    ) -> ProbeVerdict:
        del cursor, sync_scope
        return ProbeVerdict(changed=False, token=self._token)


class _WidenedScopeAdapter:
    """Declares `effective_scope_complete` whenever it is handed a scope."""

    name = "widened-scope"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self, token: dict) -> None:
        self.change_probe = _StaticProbe(token)
        self._token = token
        self.run_calls = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        since_progress: dict | None = None,
        sync_scope: dict | None = None,
    ) -> SourceRun:
        del run_id, cursor, since_progress
        self.run_calls += 1
        scoped = bool(sync_scope)
        return SourceRun(
            facts=[
                NodeFact(
                    node_id="t:widened",
                    subtype="concept",
                    attrs={"text": "widened"},
                    source_record_id={"id": "r-widened"},
                    source_revision={"v": self.run_calls},
                ),
            ],
            completed_scope=True,
            next_cursor={"runs": self.run_calls},
            run_mode=mode,
            # A run that STARTED scoped cannot claim global change
            # identity, whatever it went on to read -- exactly the
            # position a real path-scoped adapter is in.
            applied_raw_token=None if scoped else self._token,
            applied_token_scope="unavailable" if scoped else "global",
            applied_token_authority="unavailable" if scoped else "exact",
            effective_scope_complete=scoped,
        )


def test_widened_scope_run_reports_requested_and_effective_scope(
    clean_runtime: str,
) -> None:
    """A widened run records what was ASKED and what actually ran.

    `requested_scope` must stay the caller's narrow scope even though the
    runner cleared the effective one, and the widening must carry fallback
    evidence (the contract rejects an unexplained widening). This also pins
    that `path_scoped` is NOT recomputed at the widening site: the run began
    scoped, so managed change identity still takes the scoped branch and
    carries the prior probe token forward rather than acknowledging a new
    one from a run the global probe never covered.
    """
    source_name = "proof.widened-scope-contract"
    token = compose_token(
        content={"kind": "content_hash", "value": "a" * 64},
        config="b" * 64,
        emit_version="c" * 64,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _WidenedScopeAdapter(token)
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.next_cursor is not None
    prior_token = first.next_cursor.get(CURSOR_TOKEN_KEY)
    assert prior_token is not None, first.next_cursor

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["ticket/widened"]},
        emit_delta_contract=True,
    )

    assert adapter.run_calls == 2
    contract = second.source_delta_batch
    assert contract is not None
    assert contract.get("contract") != "invalid", contract
    assert contract["requested_scope"] == {"scope_keys": ["ticket/widened"]}
    assert contract["effective_scope"] is None
    fallback = contract["fallback"]
    assert fallback is not None
    assert fallback["reason"] == "adapter_declared_effective_scope_complete"
    assert fallback["affected_scopes"] == ["ticket/widened"]

    # `path_scoped` is still True, so the prior probe token is carried
    # forward unchanged instead of being re-acknowledged by this run.
    assert second.next_cursor is not None
    assert second.next_cursor[CURSOR_TOKEN_KEY] == prior_token


@pytest.fixture
def fixture_paths(tmp_path):
    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    if str(fixtures_dir) not in sys.path:
        sys.path.insert(0, str(fixtures_dir))
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def test_agent_sessions_rerun_rewrites_only_the_records_that_changed(
    clean_runtime: str,
    fixture_paths,
) -> None:
    """Acceptance for #1714: a per-run stamp must not be part of record identity.

    The adapter re-emits its whole footprint every run, so the only thing that
    keeps Chronos from closing and reopening every node and edge interval is
    the graph writer's semantic no-op filter
    (``ChronosGraphWriteMixin._node_upsert_semantic_noop``). That filter
    compares ``source_revision`` and ``attrs`` -- and ``attrs`` carries the
    Nomos lineage envelope, which embeds ``source_revision``. A revision
    derived from the whole emission rather than from the record defeated the
    filter on EVERY record as soon as ANY one session changed, which is why
    92% of the live graph's stored history was tombstones.

    Phase 2 is the acceptance stated on the issue (two runs, nothing changed,
    zero interval rows). Phase 3 is the discriminating one: a whole-emission
    digest is also stable when nothing changed at all, so only a run where one
    session changes separates a per-record revision from a per-run one.

    Assertions are on the physical Chronos interval table, not on live node
    counts: an unchanged rewrite leaves the live count identical while
    doubling the stored history.
    """
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from okg.substrate.publisher import Publisher
    from tests.substrate.library.sources.test_session_source_sync_db import (
        _seed_catalog_and_external_nodes,
    )

    projects_root = fixture_paths.sessions_root.parent

    def _source() -> AgentSessionsSource:
        return AgentSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
            since="all",
            cost_estimation=False,
        )

    def _publish() -> dict:
        return Publisher(
            clean_runtime,
            "test",
            catalog_identity=bind_test_catalog_writer(clean_runtime, "test"),
        ).cycle()

    _seed_catalog_and_external_nodes(clean_runtime, _source().run("preview").facts)

    runner = SourceRunner(clean_runtime, source_name="test.agent-sessions")
    first = runner.run(_source(), mode="scope_complete")
    assert first.completed_scope is True
    assert _publish()["status"] == "published"

    baseline_totals = _graph_interval_row_counts(clean_runtime)
    assert baseline_totals["okg.graph_nodes"] > 0
    assert baseline_totals["okg.graph_edges"] > 0

    # Phase 2 -- nothing on disk changed between the two runs.
    unchanged_run = runner.run(_source(), mode="scope_complete")
    assert unchanged_run.completed_scope is True
    assert _publish()["status"] in {"published", "no_changes"}
    assert _graph_interval_row_counts(clean_runtime) == baseline_totals

    # Phase 3 -- exactly one of the two fixture sessions changes. Take the
    # emitted node content before and after the edit so the set of records
    # that genuinely changed is derived from the adapter, not hand-listed.
    content_before = _emitted_node_content(_source())
    baseline_rows = {
        node_id: _node_interval_row_count(clean_runtime, node_id)
        for node_id in content_before
    }

    alpha = fixture_paths.sessions_root / "alpha-001.jsonl"
    with alpha.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({
            "type": "user",
            "timestamp": "2026-05-05T12:00:00Z",
            "message": {"content": "only alpha changed"},
        }) + "\n")

    content_after = _emitted_node_content(_source())
    unchanged_ids = sorted(
        node_id
        for node_id, content in content_after.items()
        if content_before.get(node_id) == content
    )
    changed_ids = sorted(set(content_before) - set(unchanged_ids))
    assert changed_ids, "the fixture edit must change at least one record"
    assert unchanged_ids, "the fixture edit must leave other records untouched"

    changed_run = runner.run(_source(), mode="scope_complete")
    assert changed_run.completed_scope is True
    assert _publish()["status"] == "published"

    rewritten = {
        node_id: (
            baseline_rows[node_id],
            _node_interval_row_count(clean_runtime, node_id),
        )
        for node_id in unchanged_ids
        if _node_interval_row_count(clean_runtime, node_id)
        != baseline_rows[node_id]
    }
    assert not rewritten, (
        "records whose emitted content did not change were rewritten anyway: "
        f"{rewritten}"
    )


def _emitted_node_content(source) -> dict[str, str]:
    """Return each emitted node's content, ignoring its source_revision.

    Content is what identity should be derived from, so this is the reference
    the test compares the stored history against.
    """
    content: dict[str, str] = {}
    for fact in source.run("content-probe").facts:
        if not isinstance(fact, NodeFact) or fact.op != "I":
            continue
        content[str(fact.node_id)] = json.dumps(
            {"subtype": str(fact.subtype), "attrs": fact.attrs},
            sort_keys=True,
            default=str,
        )
    return content


def _graph_interval_row_counts(dsn: str) -> dict[str, int]:
    """Return stored Chronos interval rows for the two graph tables.

    A rewrite closes the inherited interval and inserts a byte-new physical
    row, so churn shows up as a strictly larger count here even when the
    branch-visible row set is unchanged.
    """
    counts: dict[str, int] = {}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for table in ("okg.graph_nodes", "okg.graph_edges"):
            physical = _chronos_physical_table(cur, table)
            cur.execute(f'SELECT count(*) FROM "{physical}"')
            counts[table] = int(cur.fetchone()[0])
    return counts


def _node_interval_row_count(dsn: str, node_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        physical = _chronos_physical_table(cur, "okg.graph_nodes")
        cur.execute(
            f'SELECT count(*) FROM "{physical}" WHERE node_id = %s',
            (node_id,),
        )
        return int(cur.fetchone()[0])


def _chronos_physical_table(cur, table: str) -> str:
    cur.execute(
        "SELECT physical_table FROM _chronos_branch_tables WHERE table_name = %s",
        (table,),
    )
    row = cur.fetchone()
    assert row is not None, f"{table} is not registered with Chronos"
    physical = str(row[0])
    assert physical.startswith("_chronos_b_interval_"), physical
    return physical


# --- issue #2122 — an unchanged repeat run must not empty the graph ----------
#
# A `missing_from_completed_scope` source that short-circuits an unchanged
# full scan returns an empty fact stream and `record_set=None`. The runner
# read that pair as "infer this source's whole footprint from the stream",
# derived an EMPTY footprint, and retracted every record the source had
# published; the next run found an empty prior set, rebuilt, and the
# published count alternated 6 / 0 / 6 / 0 while every run reported success.
#
# `SourceRun.preserves_prior_record_set` separates the two meanings: the
# no-change run says the PRIOR authority (carried in its cursor) is still
# exact, and the runner leaves it alone. These proofs are at the runner
# boundary because no adapter-level assertion can see the retraction: the
# adapter's own `last_run_stats` reported `record_set_authority: preserved`
# on exactly the run that emptied the graph.


def _live_node_count(dsn: str, source_name: str) -> int:
    """Nodes visible on the working branch — what a read would return.

    The physical Chronos interval row survives a retraction (that is what a
    deletion marker is), so a count taken straight from the physical table
    cannot see this defect at all.
    """
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout().query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE source = :source",
            {"source": source_name},
        )
    finally:
        store.close()
    return int(rows[0]["n"]) if rows else 0


_NOOP_SCOPE_STATUSES = frozenset({"full_unchanged_noop", "git_frontier_no_change"})


def _full_retraction_override(source_name: str):
    """`okg ingest --allow-full-retraction <source>` for one runner.

    Issue #2818 backstop: a completed run that would retract every live row
    of a source while emitting nothing is refused without it.
    """
    from okg.substrate.ingest.full_retraction_guard import (
        FullRetractionOverride,
    )

    return FullRetractionOverride(
        source=source_name,
        requested_by="retraction-atomicity-test",
        requested_at=datetime.now(timezone.utc),
    )


def _doc_corpus_ingest(
    dsn: str,
    source_name: str,
    corpus: Path,
    extractors: Path,
    **runner_kwargs,
):
    """One `okg ingest` of a doc corpus: the shipped registry settings.

    `deletion_semantics` and `scope_complete` are what the shipped profiles
    and `IngestOptions.run_mode` actually use. Every earlier repeat-run test
    left `deletion_semantics` at the constructor default of `None`, which is
    precisely the condition that switches the inferred footprint off -- so
    those tests could not have caught this.
    """
    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus,
        non_git_walk=True,
        extractors_dir=extractors,
        include_globs=("**/*.md",),
    )
    runner = SourceRunner(
        dsn,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
        **runner_kwargs,
    )
    return adapter, runner.run(adapter, mode="scope_complete")


def test_unchanged_repeat_runs_of_a_completed_scope_source_retract_nothing(
    clean_runtime: str,
    _doc_corpus_dir,
    _doc_corpus_extractors,
) -> None:
    """#2122: six identical ingests leave one published footprint.

    Five repeats, not one, because the defect alternated: a single repeat
    proves the graph emptied, and the runs after it prove the rebuild that
    made every `okg ingest` look successful.
    """
    source_name = "proof.noop-retraction"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    _adapter, first = _doc_corpus_ingest(
        clean_runtime, source_name, _doc_corpus_dir, _doc_corpus_extractors,
    )
    assert first.nodes_appended > 0, "the first ingest published nothing"
    published = _live_node_count(clean_runtime, source_name)
    assert published > 0, "the first ingest left no readable nodes"

    counts = [published]
    statuses: list[str] = []
    for run_index in range(2, 7):
        adapter, result = _doc_corpus_ingest(
            clean_runtime, source_name, _doc_corpus_dir, _doc_corpus_extractors,
        )
        statuses.append(adapter.last_run_stats["scope_status"])
        assert result.retractions_node == 0, (
            f"run {run_index} retracted {result.retractions_node} nodes "
            "although nothing changed"
        )
        assert result.retractions_edge == 0, (
            f"run {run_index} retracted {result.retractions_edge} edges "
            "although nothing changed"
        )
        counts.append(_live_node_count(clean_runtime, source_name))

    assert counts == [published] * 6, (
        f"published node count moved across six unchanged ingests: {counts}"
    )
    # Only SOME repeats short-circuit inside the reader: its no-change cursor
    # omits `doc_corpus_emission_schema_version`, so the run after a
    # short-circuit declines its own and reprocesses the corpus. That is a
    # separate (and now visible) churn defect, reported as a follow-up; what
    # matters here is that the preserved-authority path really was taken, or
    # this test would pass without exercising the fix at all.
    assert any(status in _NOOP_SCOPE_STATUSES for status in statuses), (
        f"no repeat run short-circuited ({statuses}), so none of them "
        "exercised the preserved-authority path this test guards"
    )


def test_emptied_corpus_still_retracts_the_whole_footprint(
    clean_runtime: str,
    _doc_corpus_dir,
    _doc_corpus_extractors,
) -> None:
    """The other half of #2122: real deletion must still be reported.

    Suppressing the inferred footprint for a no-change run must not disarm
    deletion for a corpus that genuinely lost its files. The reader declines
    its own short-circuit here (its file set moved), so the run reaches the
    ordinary declared-record-set path and owes the retractions.
    """
    source_name = "proof.noop-retraction-emptied"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    _adapter, first = _doc_corpus_ingest(
        clean_runtime, source_name, _doc_corpus_dir, _doc_corpus_extractors,
    )
    published = _live_node_count(clean_runtime, source_name)
    assert first.nodes_appended > 0 and published > 0

    for path in sorted(_doc_corpus_dir.glob("*.md")):
        path.unlink()

    # Issue #2818 backstop: emptying the whole corpus is refused unless the
    # operator allows it for the run.
    with pytest.raises(SubstrateError) as refused:
        _doc_corpus_ingest(
            clean_runtime, source_name, _doc_corpus_dir, _doc_corpus_extractors,
        )
    assert refused.value.code == "source_run_full_retraction_refused"
    assert _live_node_count(clean_runtime, source_name) == published

    adapter, result = _doc_corpus_ingest(
        clean_runtime, source_name, _doc_corpus_dir, _doc_corpus_extractors,
        full_retraction_override=_full_retraction_override(source_name),
    )
    assert adapter.last_run_stats["scope_status"] not in _NOOP_SCOPE_STATUSES, (
        "an emptied corpus took the unchanged short-circuit"
    )
    assert result.retractions_node > 0, "the emptied corpus retracted nothing"
    assert _live_node_count(clean_runtime, source_name) == 0, (
        "the emptied corpus left readable nodes behind"
    )


class _PreservedAuthorityAdapter:
    """Any reader with a no-change short-circuit, reduced to its contract.

    Run 1 declares a record set. Run 2 onwards emits nothing and declares
    nothing, carrying the prior authority forward in its cursor -- the shape
    `doc_corpus`, `code_graph_structure` and the git readers all return from
    their `*_unchanged_noop` / `*_no_change` paths. `preserve_authority=False`
    is the pre-fix shape and the control arm: it must still retract.
    """

    name = "preserved-authority"
    # `discovery_crawl` is the profile every folder/tree reader ships with,
    # and the only one whose retraction policy admits `scope_complete`.
    profile = "discovery_crawl"

    def __init__(self, *, preserve_authority: bool) -> None:
        self.preserve_authority = preserve_authority
        self.runs = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
        **_runner_kwargs,
    ) -> SourceRun:
        del run_id, sync_scope, _runner_kwargs
        self.runs += 1
        if self.runs == 1:
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id="t:kept",
                        subtype="ticket",
                        attrs={"text": "kept"},
                        source_record_id={"id": "r-kept"},
                        source_revision={"v": 1},
                    ),
                ],
                completed_scope=True,
                next_cursor={"runs": 1},
                run_mode=mode,
                record_set=RecordSet(
                    records={
                        "r-kept": RecordEmission(
                            primary=RecordNode(
                                node_id="t:kept", subtype="ticket",
                            ),
                        ),
                    },
                ),
            )
        return SourceRun(
            facts=iter(()),
            completed_scope=True,
            next_cursor=preserve_cursor_record_set_authority(
                {"runs": self.runs}, cursor,
            ),
            run_mode=mode,
            record_set=None,
            preserves_prior_record_set=self.preserve_authority,
        )


@pytest.mark.parametrize(
    ("preserve_authority", "expected_retractions"),
    # #1859 rule (b), operator decision 2026-09-22: the `False` arm used to
    # retract the whole footprint. An EMPTY completed stream with no record
    # set now keeps the prior state whether or not the reader set the flag;
    # only `SourceRun.asserts_empty_scope` retracts it. The deletion paths
    # that remain live are pinned by `test_empty_completed_stream_rule_b`.
    [(True, 0), (False, 0)],
)
def test_preserved_authority_decides_whether_an_empty_noop_run_retracts(
    clean_runtime: str,
    preserve_authority: bool,
    expected_retractions: int,
) -> None:
    """The contract itself, independent of any one reader.

    Both arms are the SAME empty completed-scope run with `record_set=None`;
    only the declared meaning differs. Since #1859 rule (b) neither arm
    retracts: the flag still matters for a reader whose cursor must carry the
    authority by reference, but an empty stream alone no longer reads as an
    emptied source.
    """
    source_name = f"proof.preserved-authority-{preserve_authority}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _PreservedAuthorityAdapter(preserve_authority=preserve_authority)
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended == 1
    assert _live_node_count(clean_runtime, source_name) == 1

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == expected_retractions
    assert _live_node_count(clean_runtime, source_name) == (
        1 - expected_retractions
    )


def test_preserved_authority_run_may_not_drop_the_prior_cursor_authority(
    clean_runtime: str,
) -> None:
    """A reader cannot claim preservation and then orphan the footprint.

    Claiming preservation while returning a cursor with no record set would
    leave the next run rebuilding from nothing -- the same alternation by
    another route -- so the runner refuses the run instead.
    """

    class _DroppingAdapter(_PreservedAuthorityAdapter):
        def run(self, run_id: str, **kwargs) -> SourceRun:
            source_run = super().run(run_id, **kwargs)
            if self.runs > 1:
                source_run.next_cursor = {"runs": self.runs}
            return source_run

    source_name = "proof.preserved-authority-dropped"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _DroppingAdapter(preserve_authority=True)
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
    )
    runner.run(adapter, mode="scope_complete")

    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="scope_complete")
    assert (
        exc.value.data["reason_code"]
        == "preserved_record_set_authority_dropped"
    )
    assert _live_node_count(clean_runtime, source_name) == 1


def test_preserved_authority_cannot_be_claimed_alongside_a_record_set() -> None:
    """The two meanings are mutually exclusive by construction.

    All three authority-declaring fields are covered, not just the inline
    record set: a replacement-key slice and a private authority stream each
    declare an authority OF THIS RUN, which is the thing a preserved-authority
    run says it does not have. An adversarial review deleted each of those two
    arms with the rest of this file still green.
    """
    with pytest.raises(ValueError, match="declares no authority of its own"):
        SourceRun(
            facts=iter(()),
            completed_scope=True,
            record_set=RecordSet(),
            preserves_prior_record_set=True,
        )
    with pytest.raises(ValueError, match="declares no authority of its own"):
        SourceRun(
            facts=iter(()),
            completed_scope=True,
            record_set_replacement_keys={"r-changed"},
            preserves_prior_record_set=True,
        )
    with pytest.raises(ValueError, match="declares no authority of its own"):
        SourceRun(
            facts=iter(()),
            completed_scope=True,
            record_authority_stream=RecordAuthorityStream(
                operations=iter(()),
                logical_sha256="0" * 64,
                record_count=0,
                operation_count=0,
                component_count=0,
            ),
            preserves_prior_record_set=True,
        )
    with pytest.raises(ValueError, match="record-set authority without"):
        SourceRun(facts=iter(()), preserves_prior_record_set=True)


# ---------------------------------------------------------------------------
# Issue #2122, second pass. The proofs above show what the flag DOES: set, an
# empty no-change run retracts nothing; clear, it retracts the whole
# footprint. They do not show that any SHIPPED reader sets it on its own
# no-change path. An adversarial review deleted the flag from four of the five
# sites with that whole selection still green, including
# `code_graph_structure`'s `git_frontier_no_change` -- the path the shipped
# codebase-index template runs on every ingest. These pin each site at the
# reader boundary, which is the cheapest place a deleted flag is visible, and
# the contract proofs above carry the consequence from there.


def _init_git_repo(path: Path) -> None:
    """A real work tree: the git-frontier fast paths fail closed without one."""
    from okg.substrate.library.sources._git_helpers import run_git

    run_git(path, ["init"])
    run_git(path, ["config", "user.email", "fixture@example.test"])
    run_git(path, ["config", "user.name", "OKG Fixture"])
    run_git(path, ["add", "."])
    run_git(path, ["commit", "-m", "initial"])


def test_code_structure_git_frontier_no_change_declares_preserved_authority(
    tmp_path: Path,
) -> None:
    """`code_graph_structure.py` git-frontier path: the shipped hot path.

    `git_frontier_skip: true` is set for two sources in the codebase-index
    template, so this branch is what a repeat ingest of an indexed repository
    actually takes. Without the flag the runner reads its empty stream as an
    emptied repository and retracts the whole index.
    """
    pytest.importorskip("tree_sitter_language_pack")
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    repo = tmp_path / "structure-repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "app.js").write_text(
        "function app() { return 1; }\n", encoding="utf-8",
    )
    _init_git_repo(repo)

    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
        git_frontier_skip=True,
    )
    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert source.last_run_stats["scope_status"] == "git_frontier_no_change"
    assert second.record_set is None
    assert second.preserves_prior_record_set is True, (
        "the git-frontier no-change run declared no record set and emitted "
        "nothing, so the runner will infer a footprint from its empty "
        "stream and retract the whole index"
    )


def test_doc_corpus_git_frontier_no_change_declares_preserved_authority(
    tmp_path: Path,
    _doc_corpus_extractors,
) -> None:
    """`doc_corpus.py` git-frontier path, the sibling of the one above.

    The corpus fixture is rebuilt here rather than reused because the shared
    one is not a git work tree, and outside one this branch fails closed --
    which is exactly why the earlier repeat-run proof never entered it.
    """
    corpus = tmp_path / "git-corpus"
    corpus.mkdir()
    (corpus / "a.md").write_text(
        "# Doc A\n\n## Intro\n\nLead paragraph A.\n", encoding="utf-8",
    )
    (corpus / "b.md").write_text(
        "# Doc B\n\n## Intro\n\nLead paragraph B.\n", encoding="utf-8",
    )
    _init_git_repo(corpus)

    adapter = DocCorpusSource(
        repo="test",
        repo_root=corpus,
        extractors_dir=_doc_corpus_extractors,
        include_globs=("**/*.md",),
        git_frontier_skip=True,
    )
    first = adapter.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    second = adapter.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert adapter.last_run_stats["scope_status"] == "git_frontier_no_change"
    assert second.record_set is None
    assert second.preserves_prior_record_set is True


def _lsp_fixture(tmp_path: Path):
    """The hermetic language server the LSP reader tests already ship."""
    from tests.substrate.library.extraction.code.test_lsp_semantic_resolution import (  # noqa: E501
        _hermetic_lsp_server,
    )

    repo = tmp_path / "lsp-repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "a.py").write_text(
        "from .b import helper\n\ndef caller():\n    return helper()\n",
        encoding="utf-8",
    )
    (repo / "src" / "b.py").write_text(
        "def helper():\n    return 1\n", encoding="utf-8",
    )
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)
    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    definition = {
        "uri": target_uri,
        "range": {
            "start": {"line": 0, "character": 0},
            "end": {"line": 1, "character": 12},
        },
    }
    from okg.substrate.library.sources.code_graph import CodeLspResolutionSource

    source = CodeLspResolutionSource(
        repo="demo",
        repo_root=repo,
        server_command=[sys.executable, str(server)],
        server_name="hermetic-python-lsp",
        server_version="0.1",
        initialization_options={
            "definitions": {
                "src/a.py:4:11": definition,
                "src/a.py:5:11": definition,
            },
        },
    )
    return source


@pytest.mark.parametrize("carry_authority", [True, False])
def test_code_lsp_no_change_run_declares_preserved_authority(
    tmp_path: Path,
    carry_authority: bool,
) -> None:
    """`code_graph_lsp.py` no-change path, both arms of its condition.

    The flag there is `completed_scope and has_record_set_authority`, not a
    constant. The `False` arm pins that half: with no prior authority in the
    cursor there is nothing to preserve, and claiming otherwise would tell
    the runner to skip inference with no footprint to fall back on.
    """
    from okg.substrate.library.extraction.code.lsp import (
        _close_persistent_clients,
    )

    source = _lsp_fixture(tmp_path)
    try:
        first = source.run("run-1", mode="scope_complete")
        assert len(list(first.facts)) == 2
        cursor = dict(first.next_cursor or {})
        if carry_authority:
            assert first.record_set is not None
            cursor["record_set"] = first.record_set.to_cursor()
        else:
            cursor.pop("record_set", None)
            cursor.pop("record_set_ref", None)

        second = source.run("run-2", mode="scope_complete", cursor=cursor)
        assert list(second.facts) == []
        assert source.last_run_stats["status"] == "no_change"
        assert second.record_set is None
        assert second.preserves_prior_record_set is carry_authority
    finally:
        _close_persistent_clients()


def test_pact_unchanged_noop_declares_preserved_authority(
    tmp_path: Path,
) -> None:
    """`pact.py` unchanged-noop path, at its one reachable flag setting.

    The flag is `completed_scope and has_record_set_authority and record_set
    is None`, and `record_set` is None on this path only when the prior
    authority is external by reference -- so the cursor carries a
    `record_set_ref`, which is also what the shipped externalizing runner
    writes for a record set of this size.
    """
    import yaml

    from okg.substrate.library.sources.pact import PACTSource

    repo = tmp_path / "pact-repo"
    pact_root = repo / "pact"
    manifest = pact_root / "changes" / "alpha" / "pact.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        yaml.safe_dump({
            "pact_version": 0,
            "change": {
                "id": "alpha",
                "title": "alpha",
                "rationale": "test pact",
            },
            "requirements": [
                {"id": "req.alpha", "statement": "test requirement"},
            ],
            "tasks": [{
                "id": "task.alpha",
                "title": "test task",
                "verification": "test verification",
            }],
        }),
        encoding="utf-8",
    )
    _init_git_repo(repo)

    source = PACTSource(repo="demo", pact_root=pact_root)
    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.next_cursor is not None

    cursor = dict(first.next_cursor)
    cursor.pop("record_set", None)
    cursor["record_set_ref"] = {
        "storage": "okg.source_record_set_blobs",
        "sha256": "abc",
    }
    cursor["record_set_summary"] = {"record_count": 2}

    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert source.last_run_stats["scope_status"] == "unchanged_noop"
    assert second.record_set is None
    assert second.preserves_prior_record_set is True


def test_upload_passes_the_delegate_preserved_authority_flag_through(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`upload.py` wraps the doc-corpus reader and rebuilds its `SourceRun`.

    Every other authority field is passed through explicitly; dropping this
    one would strip the protection off the delegate's own no-change
    short-circuit and hand the runner an empty stream that looks like an
    emptied corpus.
    """
    from okg.substrate.library.sources.upload import UploadSource

    staging = tmp_path / "staging"
    (staging / "content").mkdir(parents=True)
    (staging / "manifest").mkdir(parents=True)
    blob = staging / "content" / "u1-note.md"
    blob.write_text("# Note\n\nbody\n", encoding="utf-8")
    (staging / "manifest" / "u1.json").write_text(
        json.dumps({
            "schema": 1,
            "upload_id": "u1",
            "origin_kind": "file",
            "origin": "note.md",
            "staged_name": "u1-note.md",
            "content_sha256": hashlib.sha256(blob.read_bytes()).hexdigest(),
            "byte_count": blob.stat().st_size,
            "uploaded_by": "fixture",
            "uploaded_at": "2026-09-21T00:00:00Z",
            "media_type": "text/markdown",
        }),
        encoding="utf-8",
    )

    class _StubDelegate:
        last_run_stats: dict = {"scope_status": "full_unchanged_noop"}

        def run(self, run_id, **kwargs):
            del run_id, kwargs
            return SourceRun(
                facts=iter(()),
                completed_scope=True,
                next_cursor={"record_set": {}, "marker": "prior"},
                run_mode="scope_complete",
                record_set=None,
                preserves_prior_record_set=True,
            )

    source = UploadSource(
        staging_dir=staging,
        extractors_dir=tmp_path / "extractors-unused",
    )
    monkeypatch.setattr(source, "_delegate", lambda _names: _StubDelegate())

    wrapped = source.run("run-1", mode="scope_complete")

    assert wrapped.preserves_prior_record_set is True, (
        "the upload wrapper dropped the delegate preserved-authority flag"
    )


def test_discovery_crawl_forced_full_accepts_preserved_prior_authority(
    clean_runtime: str,
) -> None:
    """The second runner gate, which a preserved run must also satisfy.

    A `discovery_crawl` source run under an executable profile has to hold
    complete deletion authority in forced-full mode. A preserved-authority
    run holds it BY REFERENCE in its cursor, so the gate must accept it;
    before the fix it refused, which is a second way the same no-change run
    fails -- loudly this time, but still a broken repeat ingest.
    """
    source_name = "proof.preserved-authority-discovery-crawl"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _PreservedAuthorityAdapter(preserve_authority=True)
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
        executable_profile="discovery_crawl",
        registry_name="preserved-authority-discovery-crawl",
        ownership_id=source_name,
        registry_binding_fingerprint="binding-v1",
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended == 1

    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert _live_node_count(clean_runtime, source_name) == 1


def test_preserved_authority_run_that_emits_facts_is_refused(
    clean_runtime: str,
) -> None:
    """The other half of the contract: preservation requires an EMPTY stream.

    Suppressing footprint inference is only sound when the run emits nothing,
    because nothing rechecked the preserved footprint. A reader that claims
    preservation and still streams a partial slice would get those facts
    written on top of a footprint no run verified -- silent divergence from
    the other direction. The runner refuses on the first identity fact.
    """

    class _PartialStreamAdapter(_PreservedAuthorityAdapter):
        def run(self, run_id: str, **kwargs) -> SourceRun:
            source_run = super().run(run_id, **kwargs)
            if self.runs > 1:
                source_run.facts = iter((
                    NodeFact(
                        node_id="t:late",
                        subtype="ticket",
                        attrs={"text": "late"},
                        source_record_id={"id": "r-late"},
                        source_revision={"v": 1},
                    ),
                ))
            return source_run

    source_name = "proof.preserved-authority-partial-stream"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _PartialStreamAdapter(preserve_authority=True)
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
    )
    runner.run(adapter, mode="scope_complete")
    assert _live_node_count(clean_runtime, source_name) == 1

    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="scope_complete")

    assert (
        exc.value.data["reason_code"]
        == "preserved_record_set_authority_stream_not_empty"
    )
    assert exc.value.data["fact_identity"] == "t:late"
    assert _live_node_count(clean_runtime, source_name) == 1, (
        "the refused run left its partial slice behind"
    )


# --- #1859 rule (b): an empty completed stream keeps the prior state --------
#
# Operator decision on #1859 (2026-09-22). A COMPLETED run that declares no
# record set and streams no positive fact used to be read as "this source is
# now empty": the runner inferred an empty footprint and retracted everything
# the source had published -- the #2122 failure mode, reachable by any reader
# that silently read nothing, with or without the no-change flag. It now
# keeps the prior state unless the reader sets `SourceRun.asserts_empty_scope`.
# A non-empty stream and an explicit record set of any size still diff exactly
# as before; those arms are the guards that this rule removed no deletion path.

_RULE_B_TICKETS = 5


def _ticket(index: int, *, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id=f"t:{index}",
        subtype="ticket",
        attrs={"text": f"ticket {index}"},
        source_record_id={"id": f"r-{index}"},
        source_revision={"v": 1},
        op=op,
    )


def _ticket_record_set(indexes) -> RecordSet:
    return RecordSet(
        records={
            f"r-{index}": RecordEmission(
                primary=RecordNode(node_id=f"t:{index}", subtype="ticket"),
            )
            for index in indexes
        },
    )


class _ScriptedStreamAdapter:
    """A streaming-authority reader: its fact stream IS its footprint.

    Each script step is one run. Steps name their facts, whether the scope
    completed, an optional explicit record set, and whether the run asserts
    an empty scope. The assertion is passed only when a step sets it, so the
    steps that do not use the new field still construct on the parent commit.
    """

    name = "scripted-stream"
    profile = "discovery_crawl"

    def __init__(self, script: list[dict]) -> None:
        self.script = script
        self.runs = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
        **_runner_kwargs,
    ) -> SourceRun:
        del run_id, cursor, sync_scope, _runner_kwargs
        step = self.script[self.runs]
        self.runs += 1
        extra = {"asserts_empty_scope": True} if step.get("asserts") else {}
        return SourceRun(
            facts=iter(list(step.get("facts", ()))),
            completed_scope=step.get("completed", True),
            next_cursor={"runs": self.runs},
            run_mode=mode,
            record_set=step.get("record_set"),
            **extra,
        )


def _rule_b_runner(
    dsn: str,
    source_name: str,
    *,
    allow_full_retraction: bool = False,
) -> SourceRunner:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
    extra = {}
    if allow_full_retraction:
        # Issue #2818 backstop: asserting an empty scope over live rows now
        # needs the operator's `okg ingest --allow-full-retraction`.
        extra["full_retraction_override"] = _full_retraction_override(
            source_name,
        )
    return SourceRunner(
        dsn,
        source_name=source_name,
        deletion_semantics="missing_from_completed_scope",
        **extra,
    )


def _watermark_record_set_node_count(dsn: str, source_name: str) -> int | None:
    """Nodes in the record-set authority the source's watermark now carries."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks"
            " WHERE source = %s",
            (source_name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    summary = (row[0] or {}).get("record_set_summary")
    if not isinstance(summary, dict):
        return None
    return int(summary["node_count"])


_ALL = range(_RULE_B_TICKETS)
_KEPT = range(_RULE_B_TICKETS - 2)


@pytest.mark.parametrize(
    ("step", "retracted", "live", "authority_label", "authority_nodes"),
    [
        # The rule itself: nothing retracted, the prior footprint carried.
        pytest.param(
            {"facts": ()},
            0, _RULE_B_TICKETS, "preserved_empty_stream", _RULE_B_TICKETS,
            id="empty-stream-keeps-prior",
        ),
        # The only way back to the old reading (with the operator's
        # `--allow-full-retraction`, issue #2818 backstop).
        pytest.param(
            {"facts": (), "asserts": True},
            _RULE_B_TICKETS, 0, None, 0,
            id="asserted-empty-scope-retracts-all",
        ),
        # Guards: explicit and streamed deletion are unchanged.
        pytest.param(
            {
                "facts": [_ticket(index) for index in _KEPT],
                "record_set": _ticket_record_set(_KEPT),
            },
            2, _RULE_B_TICKETS - 2, None, _RULE_B_TICKETS - 2,
            id="explicit-set-missing-two-retracts-two",
        ),
        pytest.param(
            {"facts": [_ticket(index) for index in _KEPT]},
            2, _RULE_B_TICKETS - 2, None, _RULE_B_TICKETS - 2,
            id="stream-missing-two-retracts-two",
        ),
        # A stream of ONLY explicit retractions: exactly those go, from the
        # graph AND from the carried authority; nothing else is inferred.
        pytest.param(
            {
                "facts": [
                    _ticket(index, op="R")
                    for index in range(_RULE_B_TICKETS - 2, _RULE_B_TICKETS)
                ],
            },
            2, _RULE_B_TICKETS - 2, "preserved_retraction_only_stream",
            _RULE_B_TICKETS - 2,
            id="retraction-only-stream-retracts-only-those",
        ),
    ],
)
def test_empty_completed_stream_rule_b(
    clean_runtime: str,
    request: pytest.FixtureRequest,
    step: dict,
    retracted: int,
    live: int,
    authority_label: str | None,
    authority_nodes: int,
) -> None:
    source_name = "proof.rule-b-" + request.node.callspec.id
    adapter = _ScriptedStreamAdapter([
        {"facts": [_ticket(index) for index in _ALL]},
        step,
        # A follow-up full stream of what should be live. Any identity the
        # carried authority still lists wrongly would be retracted here.
        {"facts": [_ticket(index) for index in range(live)]},
    ])
    runner = _rule_b_runner(
        clean_runtime,
        source_name,
        allow_full_retraction=bool(step.get("asserts")),
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.nodes_appended == _RULE_B_TICKETS
    assert _live_node_count(clean_runtime, source_name) == _RULE_B_TICKETS
    assert _watermark_record_set_node_count(
        clean_runtime, source_name,
    ) == _RULE_B_TICKETS

    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == retracted
    assert _live_node_count(clean_runtime, source_name) == live
    assert second.as_dict().get("record_set_authority") == authority_label
    assert _watermark_record_set_node_count(
        clean_runtime, source_name,
    ) == authority_nodes

    third = runner.run(adapter, mode="scope_complete")

    if live:
        assert third.retractions_node == 0, (
            "the authority carried out of run 2 still named identities the "
            "graph no longer holds"
        )
    assert _live_node_count(clean_runtime, source_name) == live


def test_empty_completed_stream_over_unverified_prior_retracts_no_strays(
    clean_runtime: str,
) -> None:
    """An empty run must not fall through to the stray-retraction pass.

    With no verified prior authority (here: every earlier run was
    incomplete, so none left a record set), the inferred-footprint path also
    ran `_record_set_stray_retractions`, which reads the WHOLE live graph as
    what the empty footprint omits and retracted every row the source owned.
    """
    source_name = "proof.rule-b-unverified-prior"
    adapter = _ScriptedStreamAdapter([
        {"facts": [_ticket(index) for index in _ALL], "completed": False},
        {"facts": ()},
    ])
    runner = _rule_b_runner(clean_runtime, source_name)

    runner.run(adapter, mode="cursor")
    assert _live_node_count(clean_runtime, source_name) == _RULE_B_TICKETS
    assert _watermark_record_set_node_count(clean_runtime, source_name) is None

    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert second.as_dict().get("record_set_authority") == (
        "preserved_empty_stream"
    )
    assert _live_node_count(clean_runtime, source_name) == _RULE_B_TICKETS


def test_empty_scope_assertion_with_positive_facts_is_refused(
    clean_runtime: str,
) -> None:
    """Asserting an empty scope and streaming a fact contradict each other.

    The runner refuses on the first positive fact, before its chunk
    persists, so the refused run changes nothing.
    """
    source_name = "proof.rule-b-assertion-with-facts"
    # A NEW identity, so a write that slipped past the refusal is visible.
    adapter = _ScriptedStreamAdapter([
        {"facts": [_ticket(index) for index in _ALL]},
        {"facts": [_ticket(99)], "asserts": True},
    ])
    # The override lets the run past the #2818 full-retraction guard to the
    # contract refusal this test pins.
    runner = _rule_b_runner(
        clean_runtime, source_name, allow_full_retraction=True,
    )
    runner.run(adapter, mode="scope_complete")

    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="scope_complete")

    assert exc.value.data["reason_code"] == (
        "empty_scope_assertion_stream_not_empty"
    )
    assert exc.value.data["fact_identity"] == "t:99"
    assert _live_node_ids(clean_runtime, source_name) == {
        f"t:{index}" for index in _ALL
    }
    assert _watermark_record_set_node_count(
        clean_runtime, source_name,
    ) == _RULE_B_TICKETS


def _live_node_ids(dsn: str, source_name: str) -> set[str]:
    """Node ids visible on the working branch for `source_name`."""
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        rows = store.checkout().query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = :source",
            {"source": source_name},
        )
    finally:
        store.close()
    return {str(row["node_id"]) for row in rows}


def test_empty_scope_assertion_refuses_retractions_before_they_are_saved(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retraction streamed under the assertion must not reach the graph.

    With one fact per chunk, each retraction used to be saved before the
    positive fact refused the run. Compensation only withdraws rows a run
    inserted, so t:3 and t:4 stayed deleted while the carried authority
    still listed all five. The runner now refuses the first fact of any
    kind, so nothing is saved.
    """
    monkeypatch.setattr("okg.substrate.ingest.runner.COPY_CHUNK_ROWS", 1)
    source_name = "proof.rule-b-assertion-after-retractions"
    adapter = _ScriptedStreamAdapter([
        {"facts": [_ticket(index) for index in _ALL]},
        {
            "facts": [
                _ticket(4, op="R"),
                _ticket(3, op="R"),
                _ticket(99),
            ],
            "asserts": True,
        },
    ])
    # The override lets the run past the #2818 full-retraction guard to the
    # contract refusal this test pins.
    runner = _rule_b_runner(
        clean_runtime, source_name, allow_full_retraction=True,
    )
    runner.run(adapter, mode="scope_complete")

    with pytest.raises(SubstrateError) as exc:
        runner.run(adapter, mode="scope_complete")

    assert _live_node_ids(clean_runtime, source_name) == {
        f"t:{index}" for index in _ALL
    }
    assert _watermark_record_set_node_count(
        clean_runtime, source_name,
    ) == _RULE_B_TICKETS
    assert exc.value.data["reason_code"] == (
        "empty_scope_assertion_stream_not_empty"
    )
    assert (exc.value.data["fact_identity"], exc.value.data["fact_op"]) == (
        "t:4", "R",
    )


def test_empty_scope_assertion_without_deletion_authority_is_logged(
    clean_runtime: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An assertion on an incremental run retracts nothing, and says so."""
    source_name = "proof.rule-b-assertion-incremental"
    adapter = _ScriptedStreamAdapter([
        {"facts": [_ticket(index) for index in _ALL]},
        {"facts": (), "asserts": True},
    ])
    runner = _rule_b_runner(clean_runtime, source_name)
    runner.run(adapter, mode="scope_complete")

    with caplog.at_level("WARNING", logger="okg.substrate.ingest.runner"):
        second = runner.run(adapter, mode="cursor")

    assert second.retractions_node == 0
    assert second.as_dict().get("record_set_authority") == (
        "empty_scope_assertion_ignored"
    )
    assert _live_node_count(clean_runtime, source_name) == _RULE_B_TICKETS
    assert any(
        "asserts an empty scope" in record.getMessage()
        and source_name in record.getMessage()
        for record in caplog.records
    )


# #2074 (a) through the real admission wiring. A HEALTHY zero-output run that
# did not finish its scan is an accepted no-op for a source that already has
# accepted terminal state -- and it must change nothing but the admission
# record. Gmail records its next history id before it reads and swallows
# per-message fetch errors, so an all-fetches-failed run is empty, incomplete
# and healthy; saving its proposed cursor skipped the unread mail for good.

_KNOWN_CURSOR = {"gmail_history_id": "100"}
_PROPOSED_CURSOR = {"gmail_history_id": "200"}


def _finalize_incomplete_empty_run(tmp_path: Path, dsn: str, *, known: bool):
    from okg.substrate.ingest import service as ingest_service
    from okg.substrate.ingest.service import IngestOptions
    from okg.substrate.sources.registry import load_source_registry
    from tests.substrate.test_ingest_service import (
        _write_layered_fixture_deployment,
    )

    dep = _write_layered_fixture_deployment(tmp_path)
    entry = load_source_registry(dep / "source_registry.yaml")["first"]
    source = entry.ownership_id
    options = IngestOptions(
        dsn=dsn,
        deployment="svc",
        deployment_dir=dep,
        include={"first"},
        include_predecessors=False,
        publish=False,
        admission_observed_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    reconciled_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    if known:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO okg.source_watermarks"
                " (source, last_applied_rev, last_reconcile_at)"
                " VALUES (%s, %s::jsonb, %s)"
                " ON CONFLICT (source) DO UPDATE SET"
                " last_applied_rev = EXCLUDED.last_applied_rev,"
                " last_reconcile_at = EXCLUDED.last_reconcile_at",
                (source, json.dumps(_KNOWN_CURSOR), reconciled_at),
            )
    run_context, workflow_ref, envelope_status = (
        ingest_service._prepare_source_admission(
            options=options,
            entry=entry,
            source_registry_name="first",
            run_mode="scope_incomplete",
            sync_scope=None,
        )
    )
    result = ingest_service._finalize_source_admission(
        options=options,
        admission_id=run_context.admission_id,
        source=source,
        run_id=run_context.run_id,
        workflow_ref=workflow_ref,
        existing_status=envelope_status,
        admission_mode="fast_track",
        observed_output_count=0,
        completed_scope=False,
        source_health_status="ok",
        run_mode="scope_incomplete",
        next_cursor=_PROPOSED_CURSOR,
        observed_at=datetime.now(timezone.utc),
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        watermark = conn.execute(
            "SELECT last_applied_rev, last_reconcile_at"
            " FROM okg.source_watermarks WHERE source = %s",
            (source,),
        ).fetchone()
        envelope = conn.execute(
            "SELECT status FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (run_context.admission_id,),
        ).fetchone()
    return result, source, watermark, envelope, reconciled_at


def test_incomplete_empty_run_of_a_known_source_is_accepted_and_keeps_its_cursor(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    result, source, watermark, envelope, reconciled_at = (
        _finalize_incomplete_empty_run(tmp_path, clean_runtime, known=True)
    )

    assert result.status == "accepted", result.verdict
    assert result.verdict["producer_policy"]["reason"] == (
        "incomplete_scope_noop"
    )
    assert envelope == ("accepted",)
    assert watermark[0] == _KNOWN_CURSOR, (
        "the accepted no-op saved the reader's proposed cursor"
    )
    assert watermark[1] == reconciled_at
    # Review-mode approval replays this key, so it must be absent too.
    assert "source_terminal_state" not in result.verdict
    next_runner = SourceRunner(clean_runtime, source_name=source)
    try:
        assert next_runner._load_prior_cursor() == _KNOWN_CURSOR
    finally:
        next_runner._release_conn()


def test_incomplete_empty_first_run_of_a_new_source_stays_refused(
    tmp_path: Path,
    clean_runtime: str,
) -> None:
    result, _source, watermark, envelope, _reconciled_at = (
        _finalize_incomplete_empty_run(tmp_path, clean_runtime, known=False)
    )

    assert result.status == "rejected"
    assert result.verdict["producer_policy"]["reason"] == "partial"
    assert envelope == ("rejected",)
    assert watermark is None or watermark[0] in ({}, None)
    assert watermark is None or watermark[1] is None


class _MarkerOnlyIncompleteSource:
    """Streams two progress markers, no facts, and does not finish its scope.

    The scholarly readers skip any candidate that already has a marker, so a
    marker kept from an empty incomplete run would hide that record forever.
    """

    name = "marker-only-incomplete"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor", **_kwargs) -> SourceRun:
        del run_id
        return SourceRun(
            facts=iter([
                ProgressMarker(record_id="candidate:a", fingerprint="fa"),
                ProgressMarker(record_id="candidate:b", fingerprint="fb"),
            ]),
            completed_scope=False,
            next_cursor=_PROPOSED_CURSOR,
            run_mode=mode,
        )


def test_incomplete_empty_run_rolls_back_the_progress_markers_it_streamed(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import yaml

    from okg.substrate.ingest import IngestOptions, run_ingest
    from tests.pytest.catalog_fixture_builders import seed_catalog
    from tests.substrate.test_ingest_service import _write_stub_deployment

    # The registry names this module itself, so the spawned source child
    # imports the real class: the run takes the production child path.
    module_name = __name__
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["document"])
    dep = _write_stub_deployment(tmp_path)
    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["first"]["module"] = module_name
    registry["sources"]["first"]["class"] = "_MarkerOnlyIncompleteSource"
    registry_path.write_text(yaml.safe_dump(registry))

    # A source that has published before: a saved cursor, a reconcile time
    # and one progress marker from an earlier run.
    reconciled_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_watermarks"
            " (source, last_applied_rev, last_reconcile_at)"
            " VALUES ('svc.first', %s::jsonb, %s)",
            (json.dumps(_KNOWN_CURSOR), reconciled_at),
        )
        conn.execute(
            "INSERT INTO okg.source_progress (deployment, source_name, key, value)"
            " VALUES ('svc', 'svc.first', 'record:prior', 'p0')"
        )

    outcome = run_ingest(
        IngestOptions(
            dsn=clean_runtime,
            deployment="svc",
            deployment_dir=dep,
            include={"first"},
            include_predecessors=False,
            include_dependents=False,
            publish=False,
        )
    )

    assert outcome.any_failed is False, outcome.report
    record = outcome.report["results"][0]
    assert record["admission_status"] == "accepted", record
    assert record["admission_verdict"]["producer_policy"]["reason"] == (
        "incomplete_scope_noop"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        progress = dict(
            conn.execute(
                "SELECT key, value FROM okg.source_progress"
                " WHERE deployment = 'svc' AND source_name = 'svc.first'"
                " AND key NOT LIKE 'sync.%%'"
            ).fetchall()
        )
        watermark = conn.execute(
            "SELECT last_applied_rev, last_reconcile_at"
            " FROM okg.source_watermarks WHERE source = 'svc.first'"
        ).fetchone()
        envelope = conn.execute(
            "SELECT status FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (record["admission_id"],),
        ).fetchone()
    assert envelope == ("accepted",)
    assert progress == {"record:prior": "p0"}, (
        "the accepted no-op kept the progress markers it streamed"
    )
    assert watermark == (_KNOWN_CURSOR, reconciled_at)


def test_a_whole_scope_request_for_an_older_contract_mutable_api_source_retracts(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """#2367: `okg provision` asks every source for `scope_complete`. A
    `mutable_api` source on the older registry contract (no executable
    profile) kept that mode, which the retraction policy has no row for, so
    the first run that had to delete a row failed. Resolved per profile, the
    same run retracts it."""
    from types import SimpleNamespace

    from okg.substrate.ingest.runner import profile_native_mode
    from okg.substrate.library.sources.jira import JiraSource

    entry = SimpleNamespace(source_class="mutable_api", admission_contract="legacy_v1")
    mode = profile_native_mode(entry, "scope_complete")
    assert mode == "reconcile"

    fixture = tmp_path / "fixture.json"
    fixture.write_text(json.dumps({"nodes": [], "edges": []}))
    adapter = JiraSource(fixture_path=str(fixture))
    runner = SourceRunner(clean_runtime, source_name="jira_whole_scope")
    runner.run(adapter, mode="cursor")
    fixture.write_text(json.dumps({
        "nodes": [{"node_id": "t:x", "subtype": "ticket", "attrs": {}, "op": "R"}],
        "edges": [],
    }))

    # The unresolved request is refused at the retraction, as before.
    with pytest.raises(SubstrateError, match="no policy mapping"):
        runner.run(adapter, mode="scope_complete")
    # The resolved mode retracts.
    result = runner.run(adapter, mode=mode)
    assert result.retractions_node == 1


# --- Issue #2988: stream-footprint tail retractions in slices ----------------
#
# A completed-scope run with no declared record set retracts what it stopped
# streaming at the END of the run. Those retractions used to go out as one
# chunk, which `FactChunk` refuses above MAX_FACTS_PER_CHUNK. Here both limits
# are lowered to 7 so a node and its own edge fall in different slices, and
# the graph is read after every slice for an edge that outlives an endpoint.

_SLICE_LIMIT = 7
_SLICE_TICKETS = 20
_SLICE_KEPT = 3


class _TicketsToHubAdapter:
    """Streams `t:<i>` tickets, each with an edge to a kept hub node."""

    name = "tickets-to-hub"
    profile = "discovery_crawl"

    def __init__(self) -> None:
        self.tickets = _SLICE_TICKETS

    def run(
        self,
        run_id: str,
        *,
        mode: str = "scope_complete",
        cursor: dict | None = None,
        sync_scope: dict | None = None,
        **_runner_kwargs,
    ) -> SourceRun:
        del run_id, cursor, sync_scope, _runner_kwargs

        def _facts():
            yield NodeFact(
                node_id="t:hub",
                subtype="ticket",
                attrs={"text": "hub"},
                source_record_id={"id": "hub"},
                source_revision={"v": 1},
            )
            for index in range(self.tickets):
                yield _ticket(index)
                yield EdgeFact(
                    src=f"t:{index}",
                    dst="t:hub",
                    edge_type="links_to",
                    attrs={},
                    provenance="explicit",
                    confidence=1.0,
                    source_record_id={"id": f"r-{index}"},
                    source_revision={"v": 1},
                )

        return SourceRun(
            facts=_facts(),
            completed_scope=True,
            next_cursor={"tickets": self.tickets},
            run_mode=mode,
        )


def _live_source_graph(dsn: str, source_name: str) -> dict[str, object]:
    graph = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        session = graph.checkout()
        nodes = {
            str(row["node_id"])
            for row in session.query(
                "SELECT node_id FROM okg.graph_nodes WHERE source = :source",
                {"source": source_name},
            )
        }
        edges = [
            (str(row["src"]), str(row["dst"]))
            for row in session.query(
                "SELECT src, dst FROM okg.graph_edges WHERE source = :source",
                {"source": source_name},
            )
        ]
    finally:
        graph.close()
    return {
        "nodes": nodes,
        "edges": edges,
        "dangling": [
            edge for edge in edges
            if edge[0] not in nodes or edge[1] not in nodes
        ],
    }


def test_stream_footprint_tail_slices_node_and_edge_retractions(
    clean_runtime: str, monkeypatch,
) -> None:
    """#2988: more tail retractions than one chunk holds all apply."""
    import okg.substrate.fact_batch as fact_batch_module
    import okg.substrate.ingest.runner as runner_module
    from okg.substrate.admission import ensure_envelope

    monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", _SLICE_LIMIT)
    monkeypatch.setattr(fact_batch_module, "MAX_FACTS_PER_CHUNK", _SLICE_LIMIT)
    source_name = "source:footprint-tail-slices"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    profile = replace(
        _profile(source_name),
        allowed_record_kinds=("node", "edge"),
        output_scope_summary={"nodes": ["ticket"], "edges": ["links_to"]},
    )
    runner = SourceRunner(
        clean_runtime,
        source_name=source_name,
        deployment_name="default",
        deletion_semantics="missing_from_completed_scope",
    )
    adapter = _TicketsToHubAdapter()

    def admitted_run():
        run_id = uuid.uuid4()
        _RUN_IDS[source_name] = str(run_id)
        idempotency_key = f"{source_name}-{run_id}"
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            envelope = ensure_envelope(
                conn,
                deployment_name="default",
                producer_id=profile.producer_id,
                producer_kind="source",
                intent="source_sync",
                admission_mode="fast_track",
                status="pending",
                idempotency_key=idempotency_key,
                source=profile.producer_id,
                run_id=run_id,
                authority={"profile_fingerprint": profile.profile_fingerprint},
            )
        return runner.run(
            adapter,
            mode="scope_complete",
            run_context=SourceRunContext(
                run_id=str(run_id),
                observed_at=datetime.now(timezone.utc),
                admission_id=envelope.admission_id,
                idempotency_key=idempotency_key,
                producer_profile=replace(profile),
            ),
        )

    first = admitted_run()
    assert first.nodes_appended == _SLICE_TICKETS + 1
    assert first.edges_appended == _SLICE_TICKETS

    tail_slices: list[list[str]] = []
    after_slice: list[dict[str, object]] = []
    real_flush = SourceRunner._flush_chunk

    def observing_flush(self, facts, markers, **kwargs):
        result = real_flush(self, facts, markers, **kwargs)
        if facts and all(getattr(f, "op", "I") == "R" for f in facts):
            tail_slices.append([
                "edge" if isinstance(f, EdgeFact) else "node" for f in facts
            ])
            after_slice.append(_live_source_graph(clean_runtime, source_name))
        return result

    monkeypatch.setattr(SourceRunner, "_flush_chunk", observing_flush)
    adapter.tickets = _SLICE_KEPT
    second = admitted_run()

    retracted = _SLICE_TICKETS - _SLICE_KEPT
    # 17 node + 17 edge retractions = 34 facts: slices of 7,7,7,7,6.
    assert [len(kinds) for kinds in tail_slices] == [7, 7, 7, 7, 6]
    # The retraction order is per record, node before its own edge, so at
    # least one ticket's node and edge land in different slices.
    assert any(kinds[-1] == "node" for kinds in tail_slices[:-1])
    # Receipt counts are the sum of the slices: nothing dropped or doubled.
    assert second.retractions_node == retracted
    assert second.retractions_edge == retracted
    # A node retraction deletes this source's incident edges in the same
    # chunk, so no slice boundary leaves an edge pointing at a gone node.
    for state in after_slice:
        assert state["dangling"] == [], state["dangling"]
    final = _live_source_graph(clean_runtime, source_name)
    assert final["nodes"] == {"t:hub"} | {
        f"t:{index}" for index in range(_SLICE_KEPT)
    }
    assert len(final["edges"]) == _SLICE_KEPT
