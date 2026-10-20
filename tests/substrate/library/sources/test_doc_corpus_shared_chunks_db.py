"""Issue #3382: doc-corpus sources that share text share nodes and lose none.

Doc-corpus chunk ids are content-addressed, so the same text in two
repositories is ONE `document_chunk` node; `visual:` nodes (image bytes) and
`mention:` nodes (seeded on the chunk id) are shared the same way. Before this
change each was declared `authoritative`, so the stored row carried its last
writer: two sources ingesting together each failed record-set coverage, and
the compensation or a later retraction by the owner deleted a node the other
source still declared.

These tests drive real `DocCorpusSource` runs through `SourceRunner` on
PostgreSQL: the concurrent and the sequential pair, retraction by the owner
and by the other source, compensation without and with a run base, shared
visual and mention nodes, and a deployment whose chunks were stored before
the change.
"""
from __future__ import annotations

import io
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
import yaml
from PIL import Image as PILImage

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.captioning.service import CaptionResult
from okg.substrate.library.extraction.chunk.window import WindowChunker
from okg.substrate.library.extraction.parse.text import TextParser
from okg.substrate.library.extraction.registry import ExtractorBundle, NerRoute
from okg.substrate.library.sources import doc_corpus as doc_corpus_module
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

ALPHA = "alpha.doc-corpus"
BETA = "beta.doc-corpus"
DEPLOYMENT = "shared-chunks"

#: The text both repositories carry, as the CRAB trees carry the same
#: `if __name__ == "__main__":` tail and the same Oracle DDL.
SHARED_TEXT = (
    "if __name__ == '__main__':\n"
    "    main()  # shared tail present in both repositories\n"
)


def _extractors(path: Path, *, captioning: bool = False) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    base = "okg.substrate.library.extraction"
    config: dict[str, Any] = {
        "parser": [{"format": "text", "class": f"{base}.parse.text.TextParser"}],
        "chunker": [{
            "format": "default",
            "class": f"{base}.chunk.window.WindowChunker",
            "config": {"max_chars": 1500, "overlap": 0},
        }],
        "ner": [],
    }
    config["captioning"] = (
        {"enabled": True, "mode": "coarse", "model": "chatgpt/gpt-4o",
         "min_image_px": 10_000}
        if captioning else {"enabled": False}
    )
    (path / "extractors.yaml").write_text(yaml.safe_dump(config))
    return path


def _corpus(root: Path, own: str, *, shared: bool = True) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{own}.txt").write_text(f"text only {own} carries\n")
    if shared:
        (root / f"{own}_tail.txt").write_text(SHARED_TEXT)
    else:
        (root / f"{own}_tail.txt").unlink(missing_ok=True)
    return root


def _source(
    tmp_path: Path, name: str, *, captioning: bool = False,
    caption_service: Any = None,
) -> DocCorpusSource:
    repo = name.split(".")[0]
    return DocCorpusSource(
        repo=repo,
        repo_root=tmp_path / f"corpus-{repo}",
        non_git_walk=True,
        extractors_dir=_extractors(
            tmp_path / f"extractors-{repo}", captioning=captioning,
        ),
        include_globs=("**/*",),
        caption_service=caption_service,
    )


def _run(dsn: str, tmp_path: Path, name: str, **kwargs: Any):
    runner_kwargs = {
        key: kwargs.pop(key)
        for key in ("deployment_name", "compensate_on_failure")
        if key in kwargs
    }
    return SourceRunner(dsn, source_name=name, **runner_kwargs).run(
        _source(tmp_path, name, **kwargs), mode="scope_complete",
    )


def _query(dsn: str, sql: str, params: dict[str, Any] | None = None) -> list:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return [dict(row) for row in store.query(sql, params or {})]


def _members(dsn: str, source: str, subtype: str = "document_chunk") -> set[str]:
    """Nodes of `subtype` linked by a `member_of` edge this source wrote."""
    return {
        str(row["src"]) for row in _query(dsn, """
            SELECT e.src FROM okg.graph_edges e
              JOIN okg.graph_nodes n ON n.node_id = e.src
             WHERE e.edge_type = 'member_of' AND e.source = :source
               AND n.subtype = :subtype
        """, {"source": source, "subtype": subtype})
    }




def _shared(dsn: str, subtype: str = "document_chunk") -> set[str]:
    return _members(dsn, ALPHA, subtype) & _members(dsn, BETA, subtype)


def _rows(dsn: str, node_ids: set[str]) -> dict[str, dict[str, Any]]:
    return {
        str(row["node_id"]): row for row in _query(dsn, """
            SELECT node_id, source, subtype FROM okg.graph_nodes
             WHERE node_id = ANY(:ids)
        """, {"ids": sorted(node_ids)})
    }


def _claims(dsn: str, node_ids: set[str]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {node_id: set() for node_id in node_ids}
    for row in _query(dsn, """
        SELECT node_id, source FROM okg.graph_node_declarers
         WHERE node_id = ANY(:ids)
    """, {"ids": sorted(node_ids)}):
        out[str(row["node_id"])].add(str(row["source"]))
    return out




@pytest.fixture
def dsn(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    return clean_runtime


@pytest.fixture
def corpora(tmp_path: Path) -> Path:
    _corpus(tmp_path / "corpus-alpha", "alpha")
    _corpus(tmp_path / "corpus-beta", "beta")
    return tmp_path




def _assert_both_hold(dsn: str, shared: set[str]) -> None:
    assert shared, "the corpora share no chunk; the test would be vacuous"
    rows = _rows(dsn, shared)
    assert set(rows) == shared, f"shared chunks lost: {sorted(shared - set(rows))}"
    claims = _claims(dsn, shared)
    assert all(claims[node] == {ALPHA, BETA} for node in shared), claims


# ---------------------------------------------------------------------------
# Coverage: concurrent and sequential pairs
# ---------------------------------------------------------------------------


def test_a_concurrent_pair_passes_coverage_and_keeps_the_shared_chunks(
    dsn: str, corpora: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both runs write every fact before either checks coverage, as two
    sources that start together do on a fresh CRAB database."""
    barrier = threading.Barrier(2, timeout=180)
    real = SourceRunner._verify_record_set_chronos_coverage

    def after_both_wrote(self, record_set):
        barrier.wait()
        return real(self, record_set)

    monkeypatch.setattr(
        SourceRunner, "_verify_record_set_chronos_coverage", after_both_wrote,
    )
    errors: dict[str, BaseException] = {}

    def run(name: str) -> None:
        try:
            _run(dsn, corpora, name)
        except BaseException as exc:  # noqa: BLE001 - reported below
            errors[name] = exc
            barrier.abort()

    threads = [threading.Thread(target=run, args=(name,)) for name in (ALPHA, BETA)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=600)
    assert not errors, {name: repr(exc) for name, exc in errors.items()}
    _assert_both_hold(dsn, _shared(dsn))


def test_a_sequential_pair_passes_coverage_and_both_sources_claim_the_chunks(
    dsn: str, corpora: Path,
) -> None:
    _run(dsn, corpora, ALPHA)
    _run(dsn, corpora, BETA)
    _assert_both_hold(dsn, _shared(dsn))


# ---------------------------------------------------------------------------
# Retraction
# ---------------------------------------------------------------------------


def test_the_owner_retracting_hands_the_shared_chunk_to_the_other_source(
    dsn: str, corpora: Path,
) -> None:
    _run(dsn, corpora, ALPHA)
    _run(dsn, corpora, BETA)
    shared = _shared(dsn)
    assert shared
    owner = {row["source"] for row in _rows(dsn, shared).values()}
    assert len(owner) == 1
    (owner_name,) = owner
    other = BETA if owner_name == ALPHA else ALPHA
    owner_repo = owner_name.split(".")[0]

    _corpus(corpora / f"corpus-{owner_repo}", owner_repo, shared=False)
    _run(dsn, corpora, owner_name)

    rows = _rows(dsn, shared)
    assert set(rows) == shared, "the owner's retraction deleted a shared chunk"
    assert {row["source"] for row in rows.values()} == {other}
    assert _members(dsn, other) >= shared, "the other source's edges went"
    assert all(c == {other} for c in _claims(dsn, shared).values())
    # The other source's next run still proves its coverage.
    _run(dsn, corpora, other)


def test_the_other_source_retracting_keeps_the_owner_row(
    dsn: str, corpora: Path,
) -> None:
    _run(dsn, corpora, ALPHA)
    _run(dsn, corpora, BETA)
    shared = _shared(dsn)
    (owner_name,) = {row["source"] for row in _rows(dsn, shared).values()}
    other = BETA if owner_name == ALPHA else ALPHA
    other_repo = other.split(".")[0]

    _corpus(corpora / f"corpus-{other_repo}", other_repo, shared=False)
    _run(dsn, corpora, other)

    rows = _rows(dsn, shared)
    assert set(rows) == shared
    assert {row["source"] for row in rows.values()} == {owner_name}
    assert all(c == {owner_name} for c in _claims(dsn, shared).values())
    _run(dsn, corpora, owner_name)


# ---------------------------------------------------------------------------
# Compensation of a rejected run
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rejected", [ALPHA, BETA])
def test_compensation_without_a_run_base_keeps_a_chunk_another_source_declares(
    dsn: str, corpora: Path, rejected: str,
) -> None:
    """A fresh deployment has no published generation, so no run base: the
    compensation of a rejected admission retracts the run's own rows. The
    rejected run is either the first or the second writer of the chunks."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    results = {
        name: _run(dsn, corpora, name, compensate_on_failure=False)
        for name in (ALPHA, BETA)
    }
    shared = _shared(dsn)
    assert shared
    stats = retract_chronos_run(
        dsn, admission_id=None, source=rejected,
        run_id=results[rejected].run_id,
    )
    assert stats["mode"] == "retract_run_rows", stats
    kept = BETA if rejected == ALPHA else ALPHA
    rows = _rows(dsn, shared)
    assert set(rows) == shared, "compensation deleted a chunk another source declares"
    assert {row["source"] for row in rows.values()} == {kept}
    assert _members(dsn, kept) >= shared


@pytest.mark.parametrize("rejected_first", [True, False])
def test_the_undo_after_a_publish_keeps_a_chunk_another_source_declares(
    dsn: str, corpora: Path, rejected_first: bool,
) -> None:
    """After a publish every run cuts a run base, and compensation is the undo
    to it. The rejected run inserts the shared chunks either before or after
    the other source declares them; either way the other source keeps them."""
    from okg.substrate.ingest.compensation import retract_chronos_run

    # Something is published, but not the shared chunks.
    _corpus(corpora / "corpus-alpha", "alpha", shared=False)
    _corpus(corpora / "corpus-beta", "beta", shared=False)
    for name in (ALPHA, BETA):
        _run(dsn, corpora, name)
    store = ChronosGraphStore.connect(dsn)
    try:
        store.checkpoint_generation(1, deployment_name=DEPLOYMENT)
    finally:
        store.close()
    _corpus(corpora / "corpus-alpha", "alpha")
    _corpus(corpora / "corpus-beta", "beta")

    rejected, kept = (BETA, ALPHA) if rejected_first else (ALPHA, BETA)
    order = (rejected, kept) if rejected_first else (kept, rejected)
    results = {
        name: _run(
            dsn, corpora, name, deployment_name=DEPLOYMENT,
            compensate_on_failure=False,
        )
        for name in order
    }
    shared = _shared(dsn)
    assert shared
    stats = retract_chronos_run(
        dsn, admission_id=None, source=rejected,
        run_id=results[rejected].run_id,
    )
    assert stats["mode"] == "undo_to_run_base", stats
    rows = _rows(dsn, shared)
    assert set(rows) == shared, "the undo deleted a chunk another source declares"
    assert {row["source"] for row in rows.values()} == {kept}
    assert _members(dsn, kept) >= shared
    assert all(c == {kept} for c in _claims(dsn, shared).values())


# ---------------------------------------------------------------------------
# Shared visual and mention nodes
# ---------------------------------------------------------------------------


class _StubCaptions:
    def caption(self, image: object) -> list[CaptionResult]:
        return [CaptionResult(
            text="a stub caption",
            chunk_source="vision_caption",
            caption_model="chatgpt/gpt-4o",
            caption_provider="chatgpt",
        )]


def _png() -> bytes:
    image = PILImage.new("RGB", (105, 105), color=(40, 80, 120))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def test_a_shared_image_is_one_visual_node_both_sources_keep(
    dsn: str, corpora: Path,
) -> None:
    png = _png()
    for repo in ("alpha", "beta"):
        (corpora / f"corpus-{repo}" / "figure.png").write_bytes(png)
    kwargs = {"captioning": True, "caption_service": _StubCaptions()}
    _run(dsn, corpora, ALPHA, **kwargs)
    _run(dsn, corpora, BETA, **kwargs)
    visuals = _shared(dsn, "visual_artifact")
    assert len(visuals) == 1
    assert all(c == {ALPHA, BETA} for c in _claims(dsn, visuals).values())

    (owner,) = {row["source"] for row in _rows(dsn, visuals).values()}
    other = BETA if owner == ALPHA else ALPHA
    (corpora / f"corpus-{owner.split('.')[0]}" / "figure.png").unlink()
    _run(dsn, corpora, owner, **kwargs)
    rows = _rows(dsn, visuals)
    assert set(rows) == visuals, "the owner's retraction deleted a shared visual"
    assert {row["source"] for row in rows.values()} == {other}


class _MarkerNer:
    """Finds the word `shared` as an entity; enough to mint mention nodes."""

    def extract(self, text: str) -> list[Any]:
        start = text.find("shared")
        if start < 0:
            return []
        return [SimpleNamespace(
            type="marker", value="shared", start=start, end=start + 6,
            confidence=1.0,
        )]


def test_a_mention_in_a_shared_chunk_is_one_node_both_sources_keep(
    dsn: str, corpora: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = ExtractorBundle(
        parsers={"text": TextParser()},
        chunker=WindowChunker(max_chars=1500, overlap=0),
        ner=_MarkerNer(),
        chunkers_by_format={"default": WindowChunker(max_chars=1500, overlap=0)},
        ner_routes=[NerRoute(formats=("*",), extractor=_MarkerNer())],
    )
    monkeypatch.setattr(doc_corpus_module, "load_extractors", lambda _path: bundle)
    _run(dsn, corpora, ALPHA)
    _run(dsn, corpora, BETA)
    mentions = {
        str(row["node_id"]) for row in _query(dsn, """
            SELECT node_id FROM okg.graph_nodes WHERE subtype = 'entity_mention'
        """)
    }
    shared_chunks = _shared(dsn)
    assert shared_chunks and mentions
    shared_mentions = {
        str(row["src"]) for row in _query(dsn, """
            SELECT src FROM okg.graph_edges
             WHERE edge_type = 'member_of' AND dst = ANY(:chunks)
               AND src = ANY(:mentions)
        """, {"chunks": sorted(shared_chunks), "mentions": sorted(mentions)})
    }
    assert shared_mentions
    assert all(
        c == {ALPHA, BETA} for c in _claims(dsn, shared_mentions).values()
    ), _claims(dsn, shared_mentions)

    (owner,) = {row["source"] for row in _rows(dsn, shared_mentions).values()}
    other = BETA if owner == ALPHA else ALPHA
    owner_repo = owner.split(".")[0]
    _corpus(corpora / f"corpus-{owner_repo}", owner_repo, shared=False)
    _run(dsn, corpora, owner)
    rows = _rows(dsn, shared_mentions)
    assert set(rows) == shared_mentions, "the owner's retraction deleted a mention"
    assert {row["source"] for row in rows.values()} == {other}


# ---------------------------------------------------------------------------
# A deployment whose chunks were stored before this change
# ---------------------------------------------------------------------------


def test_chunks_stored_authoritative_gain_claims_from_every_source(
    dsn: str, corpora: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Before this change both sources stored the shared chunks authoritative
    under emission schema 6. The next run of each source under the changed
    code must record a claim from both, including the source that already
    owns the row (whose unchanged fact the live-branch dedupe would drop),
    so an owner's retraction afterwards keeps the chunk."""
    with monkeypatch.context() as legacy:
        legacy.setattr(
            doc_corpus_module, "SHARED_NODE_OWNERSHIP_MODE", "authoritative",
            raising=False,
        )
        legacy.setattr(
            doc_corpus_module, "DOC_CORPUS_EMISSION_SCHEMA_VERSION", 6,
        )
        _run(dsn, corpora, ALPHA)
        _run(dsn, corpora, BETA)
    shared = _shared(dsn)
    assert shared
    assert all(not c for c in _claims(dsn, shared).values()), (
        "the legacy runs already recorded claims; the test would be vacuous"
    )

    _run(dsn, corpora, ALPHA)
    _run(dsn, corpora, BETA)
    _assert_both_hold(dsn, shared)

    (owner,) = {row["source"] for row in _rows(dsn, shared).values()}
    other = BETA if owner == ALPHA else ALPHA
    owner_repo = owner.split(".")[0]
    _corpus(corpora / f"corpus-{owner_repo}", owner_repo, shared=False)
    _run(dsn, corpora, owner)
    rows = _rows(dsn, shared)
    assert set(rows) == shared
    assert {row["source"] for row in rows.values()} == {other}


def test_a_scoped_run_after_the_upgrade_is_not_refused_and_the_next_full_run_re_emits(
    dsn: str, corpora: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review finding: a scoped run right after the upgrade re-emits only its
    slice and merges the prior records of every other file, which still say
    `authoritative` (schema 6). Two files of one source carry the same chunk,
    one inside the scope and one outside it. The scoped run must not mix the
    two declarations (the runner refuses that), and it must not mark the
    corpus upgraded, so the next full run re-emits every file and records
    the claim."""
    alpha_root = corpora / "corpus-alpha"
    (alpha_root / "alpha_tail_copy.txt").write_text(SHARED_TEXT)
    with monkeypatch.context() as legacy:
        legacy.setattr(
            doc_corpus_module, "SHARED_NODE_OWNERSHIP_MODE", "authoritative",
            raising=False,
        )
        legacy.setattr(
            doc_corpus_module, "DOC_CORPUS_EMISSION_SCHEMA_VERSION", 6,
        )
        _run(dsn, corpora, ALPHA)
    # The chunk both files carry: one node, a member of two documents.
    shared = {
        str(row["src"]) for row in _query(dsn, """
            SELECT e.src FROM okg.graph_edges e
              JOIN okg.graph_nodes n ON n.node_id = e.src
             WHERE e.edge_type = 'member_of' AND e.source = :source
               AND n.subtype = 'document_chunk'
             GROUP BY e.src HAVING count(DISTINCT e.dst) >= 2
        """, {"source": ALPHA})
    }
    assert shared, "the two files share no chunk; the test would be vacuous"
    assert all(not c for c in _claims(dsn, shared).values())

    SourceRunner(dsn, source_name=ALPHA).run(
        _source(corpora, ALPHA), mode="scope_complete",
        sync_scope={"scope_keys": ["alpha_tail.txt"]},
    )
    assert set(_rows(dsn, shared)) == shared

    full = _source(corpora, ALPHA)
    SourceRunner(dsn, source_name=ALPHA).run(full, mode="scope_complete")
    # Every file is re-parsed under the new schema: none is skipped and no
    # schema-6 record is carried forward.
    stats = full.last_run_stats
    assert stats["files_seen"] == 3, stats
    assert stats["files_skipped_unchanged"] == 0, stats
    assert stats["files_reused_prior_record_set"] == 0, stats
    assert all(c == {ALPHA} for c in _claims(dsn, shared).values())
