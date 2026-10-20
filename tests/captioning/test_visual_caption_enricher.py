"""Tests for the VisualCaptionEnricher (Phase-B VLM figure captioning).

Since the caption-enricher split (PACT visual-caption-enricher), captioning is
NOT source-time — this file previously tested source-time captioning, which no
longer exists (the naming trap). It now tests the real enricher:

- captions published ``pdf_figure`` visuals, fanning VLM calls out via a
  ThreadPoolExecutor, emitting one ``image_caption`` ``document_chunk`` per
  figure with ``visual_ref`` + ``vision_signature`` + a ``derived_from`` edge;
- reuses a caption whose ``vision_signature`` already exists → zero VLM calls;
- ``max_images_per_doc`` caps the VLM CALL (not discovery);
- a per-figure VLM failure marks the run incomplete (watermark held), other
  figures still captioned.

Real-DB proofs: visuals are seeded into ``okg.graph_nodes`` via a Chronos
checkout and crops into ``okg.visual_crop_blobs``; the enricher's emitted facts
are read back from ``okg.node_facts`` / ``okg.edge_facts``.
"""
from __future__ import annotations

import hashlib
import io
import threading

import psycopg
import pytest
from PIL import Image as PILImage

from okg.substrate.library.captioning import caption_failures, crop_store
from okg.substrate.library.captioning.service import CaptionResult
from okg.substrate.library.enrichers.visual_caption_enricher import (
    VisualCaptionEnricher,
)

pytestmark = pytest.mark.postgres

_ENRICHER_SOURCE = "_enricher:okg_visual_caption_enricher"
_MODEL = "openrouter/gemma"


# ---------------------------------------------------------------------------
# Stub caption service (thread-safe call recorder)
# ---------------------------------------------------------------------------


class _StubCaptionSvc:
    def __init__(
        self, *, fail_on: set[str] | None = None, empty_on: set[str] | None = None,
        fixed_text: str | None = None,
    ) -> None:
        self.calls: list[str] = []
        self._lock = threading.Lock()
        self._fail_on = fail_on or set()
        self._empty_on = empty_on or set()
        # When set, EVERY figure gets this exact caption text — used to prove
        # two different figures with identical caption text don't collapse.
        self._fixed_text = fixed_text

    def caption(
        self, image: object, context: str = "", *,
        caller: str = "", fragment_id: str = "",
    ) -> list[CaptionResult]:
        with self._lock:
            self.calls.append(fragment_id)
        if fragment_id in self._fail_on:
            raise RuntimeError("VLM boom")
        if fragment_id in self._empty_on:
            text = ""
        elif self._fixed_text is not None:
            text = self._fixed_text
        else:
            text = f"caption for {fragment_id} :: {context}"
        return [CaptionResult(
            text=text,
            category="plot",
            chunk_source="vision_caption",
        )]


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


def _make_png(seed: int = 0) -> bytes:
    img = PILImage.new("RGB", (105, 105), color=(seed % 255, 90, 120))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _seed_nodes(conn: psycopg.Connection, rows: list[tuple[str, str, dict]]) -> None:
    from okg.substrate.graph_store import (
        DEFAULT_BRANCH,
        NODE_TABLE,
        ChronosGraphStore,
    )

    store = ChronosGraphStore.from_connection(conn)
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows(
            NODE_TABLE,
            [
                {
                    "node_id": node_id,
                    "subtype": subtype,
                    "attrs": dict(attrs),
                    "text_hash": None,
                    "embedding": None,
                    "embedding_model": None,
                    "source": "fixture.visual-caption-enricher",
                    "source_record_id": {"id": node_id},
                    "source_revision": {"v": 1},
                    "run_id": None,
                    "observed_at": None,
                }
                for node_id, subtype, attrs in rows
            ],
        )


def _checkpoint(conn: psycopg.Connection, generation_id: int) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    ChronosGraphStore.from_connection(conn).checkpoint_generation(generation_id)


def _stage_figure(
    conn: psycopg.Connection,
    *,
    visual_id: str,
    doc_id: str,
    context: str,
    png_seed: int,
) -> dict:
    """Seed one pdf_figure visual + stage its crop. Returns the attrs used."""
    png = _make_png(png_seed)
    render_sha = hashlib.sha256(png).hexdigest()
    content_sha = hashlib.sha256(f"{visual_id}:content".encode()).hexdigest()
    crop_store.ensure_table(conn)
    crop_store.put_crop(conn, sha256=render_sha, data=png)
    attrs = {
        "visual_id": visual_id,
        "source_kind": "pdf_figure",
        "mime_type": "image/png",
        "parent_document_ref": doc_id,
        "content_sha256": content_sha,
        "render_sha256": render_sha,
        "crop_blob_ref": render_sha,
        "caption_context": context,
        "page_number": 1,
    }
    return attrs


def _emitted_caption_chunks(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT node_id, attrs FROM okg.node_facts "
            "WHERE source = %s AND subtype = 'document_chunk' "
            "ORDER BY node_id",
            (_ENRICHER_SOURCE,),
        )
        return [{"node_id": r[0], **r[1]} for r in cur.fetchall()]


def _emitted_edges(conn: psycopg.Connection, edge_type: str) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src, dst FROM okg.edge_facts "
            "WHERE source = %s AND edge_type = %s ORDER BY src, dst",
            (_ENRICHER_SOURCE, edge_type),
        )
        return [(r[0], r[1]) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Constructor validation (no DB)
# ---------------------------------------------------------------------------


def test_ctor_requires_model() -> None:
    with pytest.raises(ValueError, match="model is required"):
        VisualCaptionEnricher(model="")


def test_ctor_rejects_bad_max_workers() -> None:
    with pytest.raises(ValueError, match="max_workers"):
        VisualCaptionEnricher(model=_MODEL, max_workers=0)


def test_ctor_rejects_bad_cap() -> None:
    with pytest.raises(ValueError, match="max_images_per_doc"):
        VisualCaptionEnricher(model=_MODEL, max_images_per_doc=0)


def test_ctor_rejects_bad_max_caption_attempts() -> None:
    with pytest.raises(ValueError, match="max_caption_attempts"):
        VisualCaptionEnricher(model=_MODEL, max_caption_attempts=0)


def test_declares_narrowings_and_subtypes() -> None:
    e = VisualCaptionEnricher(model=_MODEL, caption_service=object())
    assert ("document_chunk", "derived_from", "visual_artifact") in e.requires_narrowings
    assert "document_chunk" in e.emits_node_subtypes


# ---------------------------------------------------------------------------
# enrich() — captioning with parallel fan-out (task.enricher-module)
# ---------------------------------------------------------------------------


def test_enricher_captions_published_figures(clean_runtime: str) -> None:
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(
        model=_MODEL, max_workers=4, caption_service=svc,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1: alpha", png_seed=1)
        b = _stage_figure(conn, visual_id="visual:b", doc_id="doc:1",
                          context="Figure 2: beta", png_seed=2)
        c = _stage_figure(conn, visual_id="visual:c", doc_id="doc:2",
                          context="Figure 3: gamma", png_seed=3)
        _seed_nodes(conn, [
            ("visual:a", "visual_artifact", a),
            ("visual:b", "visual_artifact", b),
            ("visual:c", "visual_artifact", c),
        ])
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        # One VLM call per figure (fanned out through the ThreadPool).
        assert sorted(svc.calls) == ["visual:a", "visual:b", "visual:c"]
        assert not result.incomplete
        assert result.n_edges_emitted >= 3

        chunks = _emitted_caption_chunks(conn)
        assert len(chunks) == 3
        for ch in chunks:
            assert ch["chunk_source"] == "vision_caption"
            assert ch["chunk_kind"] == "image_caption"
            assert ch["visual_ref"] in {"visual:a", "visual:b", "visual:c"}
            assert ch["vision_signature"]
            assert ch["caption_model"] == _MODEL

        # Each caption chunk is derived_from its visual, and member_of its doc.
        derived = _emitted_edges(conn, "derived_from")
        dsts = {dst for _src, dst in derived}
        assert dsts == {"visual:a", "visual:b", "visual:c"}
        member = _emitted_edges(conn, "member_of")
        assert {dst for _s, dst in member} == {"doc:1", "doc:2"}


# ---------------------------------------------------------------------------
# reuse — a matching signature already present ⇒ zero VLM calls
# ---------------------------------------------------------------------------


def test_reuse_makes_zero_vlm_calls(clean_runtime: str) -> None:
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1: alpha", png_seed=1)
        # Predict the signature the enricher will compute for this visual and
        # seed a pre-existing caption chunk carrying it — simulating a prior
        # publish's caption still valid under the checkout.
        sig = enricher._vision_signature(
            content_sha256=a["content_sha256"],
            render_sha256=a["render_sha256"],
            context=a["caption_context"],
        )
        existing_chunk = {
            "chunk_id": "chunk:existing",
            "text": "already captioned",
            "chunk_source": "vision_caption",
            "visual_ref": "visual:a",
            "vision_signature": sig,
        }
        _seed_nodes(conn, [
            ("visual:a", "visual_artifact", a),
            ("chunk:existing", "document_chunk", existing_chunk),
        ])
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        assert svc.calls == [], "unchanged visual must not call the VLM"
        assert result.n_edges_emitted == 0
        assert not result.incomplete
        assert _emitted_caption_chunks(conn) == []


def test_changed_context_forces_fresh_vlm_call(clean_runtime: str) -> None:
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1: alpha", png_seed=1)
        # Existing caption carries a signature for a DIFFERENT context, so it
        # must NOT satisfy the reuse check.
        stale_sig = enricher._vision_signature(
            content_sha256=a["content_sha256"],
            render_sha256=a["render_sha256"],
            context="a different old context",
        )
        _seed_nodes(conn, [
            ("visual:a", "visual_artifact", a),
            ("chunk:stale", "document_chunk", {
                "chunk_id": "chunk:stale", "text": "old",
                "chunk_source": "vision_caption", "visual_ref": "visual:a",
                "vision_signature": stale_sig,
            }),
        ])
        _checkpoint(conn, 1)

        enricher.enrich(conn, generation_id=1)
        assert svc.calls == ["visual:a"], "a context change must force a call"


# ---------------------------------------------------------------------------
# max_images_per_doc caps the VLM CALL, not discovery
# ---------------------------------------------------------------------------


def test_cap_gates_vlm_calls_per_document(clean_runtime: str) -> None:
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(
        model=_MODEL, max_images_per_doc=1, caption_service=svc,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = []
        for i in range(3):
            attrs = _stage_figure(
                conn, visual_id=f"visual:{i}", doc_id="doc:1",
                context=f"Figure {i}", png_seed=i + 10,
            )
            rows.append((f"visual:{i}", "visual_artifact", attrs))
        _seed_nodes(conn, rows)
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        # Cap of 1 per document ⇒ at most one VLM call for doc:1's 3 figures.
        assert len(svc.calls) == 1
        # Only the captioned figure gets a chunk; the enricher never retracts
        # the other two discovery nodes (it only emits) — they remain
        # captionable next cycle. The cap gated the CALL, not discovery.
        assert len(_emitted_caption_chunks(conn)) == 1
        assert not result.incomplete


# ---------------------------------------------------------------------------
# a per-figure VLM failure marks the run incomplete (watermark held)
# ---------------------------------------------------------------------------


def test_empty_caption_is_recorded_not_silently_dropped(clean_runtime: str) -> None:
    """An empty VLM caption writes no chunk (so no signature lands, which would
    silently re-burn a call every publish). It must be recorded as a failure so
    the watermark holds and it is loud, not a silent per-cycle cost."""
    svc = _StubCaptionSvc(empty_on={"visual:b"})
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = []
        for name, seed in (("visual:a", 1), ("visual:b", 2)):
            attrs = _stage_figure(conn, visual_id=name, doc_id="doc:1",
                                  context=f"Figure {name}", png_seed=seed)
            rows.append((name, "visual_artifact", attrs))
        _seed_nodes(conn, rows)
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        assert result.incomplete
        assert result.n_input_failures == 1
        # Only the non-empty caption landed; the empty one produced no chunk.
        refs = {ch["visual_ref"] for ch in _emitted_caption_chunks(conn)}
        assert refs == {"visual:a"}


def test_incomplete_holds_watermark_on_vlm_failure(clean_runtime: str) -> None:
    svc = _StubCaptionSvc(fail_on={"visual:b"})
    enricher = VisualCaptionEnricher(
        model=_MODEL, max_workers=4, caption_service=svc,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = []
        for name, seed in (("visual:a", 1), ("visual:b", 2), ("visual:c", 3)):
            attrs = _stage_figure(
                conn, visual_id=name, doc_id="doc:1",
                context=f"Figure {name}", png_seed=seed,
            )
            rows.append((name, "visual_artifact", attrs))
        _seed_nodes(conn, rows)
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        # visual:b raised; the run is incomplete so the publisher holds the
        # watermark and retries it — but the other two still captioned.
        assert result.incomplete
        assert result.n_input_failures == 1
        captioned_refs = {ch["visual_ref"] for ch in _emitted_caption_chunks(conn)}
        assert captioned_refs == {"visual:a", "visual:c"}


# ---------------------------------------------------------------------------
# C1: identical caption text on two different figures ⇒ two distinct nodes
# ---------------------------------------------------------------------------


def test_identical_caption_text_two_figures_are_distinct_nodes(
    clean_runtime: str,
) -> None:
    """Two different figures whose VLM captions come back byte-identical must
    become TWO distinct caption chunks, each attributed to its own figure — not
    collapse into one node (which would leave one figure never matching the
    reuse check and re-captioned every cycle). Regression for C1: chunk identity
    must include the figure's visual_id, not caption text alone."""
    svc = _StubCaptionSvc(fixed_text="a bar chart")  # same text for every figure
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1", png_seed=1)
        b = _stage_figure(conn, visual_id="visual:b", doc_id="doc:1",
                          context="Figure 2", png_seed=2)
        _seed_nodes(conn, [
            ("visual:a", "visual_artifact", a),
            ("visual:b", "visual_artifact", b),
        ])
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)
        assert not result.incomplete

        chunks = _emitted_caption_chunks(conn)
        # Two distinct chunk nodes, one per figure — NOT one collapsed node.
        assert len({ch["node_id"] for ch in chunks}) == 2, chunks
        assert {ch["visual_ref"] for ch in chunks} == {"visual:a", "visual:b"}
        # Both carry the same caption text yet remain separate records.
        assert {ch["text"] for ch in chunks} == {"a bar chart"}


# ---------------------------------------------------------------------------
# C6 / C2: crop-fetch failure handling — missing crop vs. real DB error
# ---------------------------------------------------------------------------


def test_missing_crop_is_recorded_not_silently_dropped(clean_runtime: str) -> None:
    """A figure whose crop bytes are absent from okg.visual_crop_blobs is a
    per-figure input failure (held watermark, retried), never a silent skip —
    and it must NOT abort captioning of the other figures."""
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1", png_seed=1)
        # visual:b references a crop that was never staged.
        missing_sha = hashlib.sha256(b"never-staged").hexdigest()
        b = {
            "visual_id": "visual:b",
            "source_kind": "pdf_figure",
            "mime_type": "image/png",
            "parent_document_ref": "doc:1",
            "content_sha256": hashlib.sha256(b"b:content").hexdigest(),
            "render_sha256": missing_sha,
            "crop_blob_ref": missing_sha,
            "caption_context": "Figure 2",
            "page_number": 1,
        }
        _seed_nodes(conn, [
            ("visual:a", "visual_artifact", a),
            ("visual:b", "visual_artifact", b),
        ])
        _checkpoint(conn, 1)

        result = enricher.enrich(conn, generation_id=1)

        assert result.incomplete
        assert result.n_input_failures == 1
        # visual:b never reached the VLM (no crop); visual:a captioned fine.
        assert svc.calls == ["visual:a"]
        refs = {ch["visual_ref"] for ch in _emitted_caption_chunks(conn)}
        assert refs == {"visual:a"}


def test_real_db_error_during_crop_fetch_propagates(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A genuine DB error on crop fetch must NOT be swallowed as a per-figure
    failure: under the publisher the connection is in an open transaction, so a
    failed command poisons every later command. Swallowing it would defer the
    crash to the write phase and discard already-paid captions. Regression for
    C2: catch ONLY CropBlobMissing; let real DB errors surface."""
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)

    def _boom(conn: object, sha256: str) -> bytes:
        raise psycopg.OperationalError("connection reset by peer")

    monkeypatch.setattr(crop_store, "get_crop", _boom)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1", png_seed=1)
        _seed_nodes(conn, [("visual:a", "visual_artifact", a)])
        _checkpoint(conn, 1)

        with pytest.raises(psycopg.OperationalError):
            enricher.enrich(conn, generation_id=1)


def test_all_crops_missing_logs_dsn_mismatch_hint(
    clean_runtime: str, caplog: pytest.LogCaptureFixture,
) -> None:
    """When EVERY candidate's crop is missing (≥2 figures), the enricher logs an
    explicit DSN-mismatch hint (D2) rather than an opaque failure count — the
    likely cause is the source staging crops into a different database."""
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(model=_MODEL, caption_service=svc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        crop_store.ensure_table(conn)  # table exists but is empty
        rows = []
        for name in ("visual:a", "visual:b"):
            missing_sha = hashlib.sha256(f"unstaged:{name}".encode()).hexdigest()
            rows.append((name, "visual_artifact", {
                "visual_id": name,
                "source_kind": "pdf_figure",
                "mime_type": "image/png",
                "parent_document_ref": "doc:1",
                "content_sha256": hashlib.sha256(f"{name}:content".encode()).hexdigest(),
                "render_sha256": missing_sha,
                "crop_blob_ref": missing_sha,
                "caption_context": f"Figure {name}",
                "page_number": 1,
            }))
        _seed_nodes(conn, rows)
        _checkpoint(conn, 1)

        with caplog.at_level("ERROR"):
            result = enricher.enrich(conn, generation_id=1)

        assert result.incomplete
        assert svc.calls == []
        assert any("same okg database" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# D3: a permanently-failing figure is quarantined after max_caption_attempts
# ---------------------------------------------------------------------------


def _failure_rows(conn: psycopg.Connection) -> list[tuple[str, str, int]]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT visual_id, vision_signature, fail_count FROM {caption_failures.TABLE} "
            "ORDER BY visual_id"
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def test_permanently_failing_figure_is_quarantined(clean_runtime: str) -> None:
    """A figure the VLM ALWAYS empties on must be tried at most
    max_caption_attempts times, then quarantined — no VLM call (no cost) on
    later cycles, and the run stops being held incomplete on its account."""
    svc = _StubCaptionSvc(empty_on={"visual:a"})
    enricher = VisualCaptionEnricher(
        model=_MODEL, caption_service=svc, max_caption_attempts=2,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1", png_seed=1)
        _seed_nodes(conn, [("visual:a", "visual_artifact", a)])
        _checkpoint(conn, 1)

        # Two attempts: each calls the VLM (gets empty) and holds the watermark.
        r1 = enricher.enrich(conn, generation_id=1)
        r2 = enricher.enrich(conn, generation_id=1)
        assert svc.calls == ["visual:a", "visual:a"]
        assert r1.incomplete and r2.incomplete
        rows = _failure_rows(conn)
        assert len(rows) == 1 and rows[0][2] == 2  # fail_count == max_attempts

        # Third cycle: quarantined — NO further VLM call, and the run is no
        # longer held incomplete on this figure's account (watermark advances).
        r3 = enricher.enrich(conn, generation_id=1)
        assert svc.calls == ["visual:a", "visual:a"], "quarantined ⇒ no new call"
        assert not r3.incomplete
        assert r3.n_input_failures == 0
        # Quarantine is surfaced in the result (not only logged), so a caller
        # sees the figure was permanently given up on.
        assert r3.n_quarantined == 1


def test_successful_caption_clears_failure_budget(clean_runtime: str) -> None:
    """A transient failure that later succeeds must clear the counter, so a
    future re-caption starts with a fresh budget (the quarantine is for
    PERMANENT failures, not a figure that recovers)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        a = _stage_figure(conn, visual_id="visual:a", doc_id="doc:1",
                          context="Figure 1", png_seed=1)
        _seed_nodes(conn, [("visual:a", "visual_artifact", a)])
        _checkpoint(conn, 1)

        # Cycle 1: VLM returns empty ⇒ one recorded failure.
        fail_svc = _StubCaptionSvc(empty_on={"visual:a"})
        VisualCaptionEnricher(
            model=_MODEL, caption_service=fail_svc, max_caption_attempts=3,
        ).enrich(conn, generation_id=1)
        assert _failure_rows(conn)[0][2] == 1

        # Cycle 2: same signature, VLM now succeeds ⇒ caption lands, budget cleared.
        ok_svc = _StubCaptionSvc()
        result = VisualCaptionEnricher(
            model=_MODEL, caption_service=ok_svc, max_caption_attempts=3,
        ).enrich(conn, generation_id=1)
        assert not result.incomplete
        assert ok_svc.calls == ["visual:a"]
        assert _failure_rows(conn) == [], "success must clear the failure row"


def test_missing_crop_does_not_count_toward_quarantine(clean_runtime: str) -> None:
    """An infrastructure failure (missing crop) fails BEFORE the VLM call, so it
    must NOT accrue a quarantine budget — otherwise a crop restaged after a
    transient miss would stay wrongly quarantined."""
    svc = _StubCaptionSvc()
    enricher = VisualCaptionEnricher(
        model=_MODEL, caption_service=svc, max_caption_attempts=2,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        missing_sha = hashlib.sha256(b"never-staged-d3").hexdigest()
        a = {
            "visual_id": "visual:a",
            "source_kind": "pdf_figure",
            "mime_type": "image/png",
            "parent_document_ref": "doc:1",
            "content_sha256": hashlib.sha256(b"a:content").hexdigest(),
            "render_sha256": missing_sha,
            "crop_blob_ref": missing_sha,
            "caption_context": "Figure 1",
            "page_number": 1,
        }
        crop_store.ensure_table(conn)
        _seed_nodes(conn, [("visual:a", "visual_artifact", a)])
        _checkpoint(conn, 1)

        for _ in range(3):
            result = enricher.enrich(conn, generation_id=1)
            # Never quarantined ⇒ still held incomplete every cycle (must fix
            # the staging, not give up), and no VLM call ever billed.
            assert result.incomplete
        assert svc.calls == []
        assert _failure_rows(conn) == [], "infra failure must not accrue budget"
