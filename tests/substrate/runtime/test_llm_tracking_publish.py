"""Real Phase-B publish proof for inherited LLM tracking authority."""
from __future__ import annotations

import hashlib
import io
from uuid import uuid4

import psycopg
import pytest
from PIL import Image

from okg.substrate.library.captioning import crop_store
from okg.substrate.library.enrichers.visual_caption_enricher import (
    VisualCaptionEnricher,
)

pytestmark = pytest.mark.postgres


def _png(seed: int) -> bytes:
    image = Image.new("RGB", (96, 96), color=(seed, 80, 120))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _stage_figure(
    conn: psycopg.Connection,
    *,
    visual_id: str,
    document_id: str,
    seed: int,
) -> dict:
    payload = _png(seed)
    digest = hashlib.sha256(payload).hexdigest()
    crop_store.ensure_table(conn)
    crop_store.put_crop(conn, sha256=digest, data=payload)
    return {
        "source_kind": "pdf_figure",
        "mime_type": "image/png",
        "parent_document_ref": document_id,
        "content_sha256": hashlib.sha256(visual_id.encode()).hexdigest(),
        "render_sha256": digest,
        "crop_blob_ref": digest,
        "caption_context": f"Figure for {visual_id}",
        "page_number": 1,
    }


def _seed_figure(
    conn: psycopg.Connection,
    *,
    visual_id: str,
    document_id: str,
    seed: int,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore, DEFAULT_BRANCH, NODE_TABLE

    attrs = _stage_figure(
        conn,
        visual_id=visual_id,
        document_id=document_id,
        seed=seed,
    )
    store = ChronosGraphStore.from_connection(conn)
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows(
            NODE_TABLE,
            [
                {
                    "node_id": visual_id,
                    "subtype": "visual_artifact",
                    "attrs": attrs,
                    "text_hash": None,
                    "embedding": None,
                    "embedding_model": None,
                    "source": "fixture.llm-tracking-publish",
                    "source_record_id": {"id": visual_id},
                    "source_revision": {"v": 1},
                    "run_id": None,
                    "observed_at": None,
                }
            ],
        )


def _checkpoint(conn: psycopg.Connection, generation_id: int) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    ChronosGraphStore.from_connection(conn).checkpoint_generation(generation_id)


def _fake_response():
    from types import SimpleNamespace

    return SimpleNamespace(
        usage=SimpleNamespace(prompt_tokens=4, completion_tokens=5, total_tokens=9),
        _hidden_params={},
        model="fixture/model",
    )


def test_publish_cycle_records_inherited_tracking_and_failure(
    clean_runtime: str,
) -> None:
    deployment = f"fixture-publish-{uuid4().hex}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_figure(
            conn,
            visual_id="visual:tracking-success",
            document_id="doc:tracking",
            seed=1,
        )
        _checkpoint(conn, 41)

        success = VisualCaptionEnricher(
            model="fixture/model",
            tracking_mode="optional",
            runtime_dsn=clean_runtime,
            deployment=deployment,
            max_workers=1,
        )
        service = success._service()
        service._vlm._call_with_retry = (  # type: ignore[attr-defined,method-assign]
            lambda *_args, **_kwargs: ("runtime caption", _fake_response())
        )
        result = success.enrich(conn, generation_id=41)
        assert result.n_edges_emitted >= 1
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts "
                "WHERE source = %s AND subtype = 'document_chunk'",
                ("_enricher:okg_visual_caption_enricher",),
            )
            assert cur.fetchone()[0] == 1

        _seed_figure(
            conn,
            visual_id="visual:tracking-failure",
            document_id="doc:tracking",
            seed=2,
        )
        _checkpoint(conn, 42)

        class ProviderUnauthorizedError(RuntimeError):
            status_code = 401

        failure = VisualCaptionEnricher(
            model="fixture/model",
            tracking_mode="optional",
            runtime_dsn=clean_runtime,
            deployment=deployment,
            max_workers=1,
        )
        failure_service = failure._service()
        call_count = 0

        def unauthorized(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                return "runtime caption", _fake_response()
            raise ProviderUnauthorizedError("provider rejected request")

        failure_service._vlm._call_with_retry = unauthorized  # type: ignore[attr-defined,method-assign]
        failed = failure.enrich(conn, generation_id=42)
        assert failed.incomplete

        with conn.cursor() as cur:
            cur.execute(
                "SELECT caller, success, error_kind, deployment_name, "
                "generation_id, extras FROM okg.llm_calls "
                "WHERE deployment_name = %s ORDER BY caller",
                (deployment,),
            )
            rows = cur.fetchall()
    assert len(rows) == 3
    assert all(row[3] == deployment for row in rows)
    assert {row[4] for row in rows} == {"41", "42"}
    failed_row = next(row for row in rows if row[4] == "42" and not row[1])
    assert failed_row[1] is False
    assert failed_row[2] == "ProviderUnauthorizedError"
    assert failed_row[5]["provider_status_code"] == 401
