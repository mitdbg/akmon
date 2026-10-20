"""VisualCaptionEnricher — VLM figure captioning as a Phase-B enricher.

Since the caption-enricher split (PACT ``visual-caption-enricher``),
``DocCorpusSource`` is discovery-only: it emits ``pdf_figure``
``visual_artifact`` nodes (bbox + confidence + ``render_sha256`` +
``crop_blob_ref`` + ``caption_context``) and stages each crop into
``okg.visual_crop_blobs``, but makes NO VLM call. This enricher owns all
captioning. For each published figure it:

  1. reads the crop bytes from ``okg.visual_crop_blobs`` by the node's
     ``crop_blob_ref`` (never re-opening the PDF — no pymupdf in Phase B),
  2. captions it with the VLM using the node's persisted ``caption_context``
     (the anchored "Figure N" text, or the page-text fallback), and
  3. emits an ``image_caption`` ``document_chunk`` carrying ``visual_ref`` +
     ``vision_signature``, linked ``derived_from`` the visual (and
     ``member_of`` its parent document, mirroring the source's old shape).

Parallelism reuses the substrate's proven pattern
(``views/execution/recheck.py``): the VLM calls fan out through a
``ThreadPoolExecutor`` (parallel network I/O), while every DB read and every
fact emit stays single-threaded — the psycopg connection is never shared
across threads. So crops are fetched up front (Phase A, single-threaded), VLM
calls fan out (Phase B, no DB), and facts are emitted at the end (Phase C,
single-threaded).

Signature-based reuse: the enricher predicts each visual's ``vision_signature``
(content/render hash + model + provider + prompt + config + context) BEFORE
calling the VLM. When a caption chunk carrying that signature already exists for
the visual under the checkout (a prior publish), it is reused and NO VLM call is
made — so a re-publish over an unchanged corpus issues zero VLM calls, and a
model/prompt/context change forces a fresh call.

``max_images_per_doc`` gates the VLM CALL here (not discovery): beyond the
per-document cap, a figure's discovery node still exists but is left
uncaptioned this cycle. A per-figure crop-fetch or VLM failure is recorded and
marks the run ``incomplete`` (P0 no-silent-data-loss), so the publisher holds
the enricher watermark and the figure is retried next cycle rather than
silently losing its caption.

Retry is BOUNDED (D3): a failure that cost a VLM call increments a persistent
per-``(visual, signature)`` counter in ``okg.visual_caption_failures``; after
``max_caption_attempts`` such failures the figure is QUARANTINED — the enricher
stops calling the VLM for it and lets the watermark advance, so a
permanently-failing figure (e.g. one the VLM always empties on) can't re-bill a
call every publish forever. The quarantine is loud and queryable, and a
content/model/prompt change (new signature) resets the budget. Infrastructure
failures that fail *before* the VLM call (missing crop, malformed node) are NOT
counted — they hold the watermark and retry until the staging is fixed.
"""
from __future__ import annotations

import hashlib
import io
import inspect
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.derived_node_liveness import (
    resolve_declared_support_policy,
)
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.captioning import caption_failures, crop_store
from okg.substrate.library.captioning.service import _COARSE_PROMPT
from okg.substrate.library.captioning.vision_signature import (
    compute_vision_signature,
)
from okg.substrate.llm.tracking import resolve_tracking_dsn

__all__ = ["VisualCaptionEnricher"]

logger = logging.getLogger(__name__)


def _provider_from_model(model: str) -> str:
    """litellm provider prefix (mirrors CaptionService._provider_from_model)."""
    return model.split("/", 1)[0] if "/" in model else model


@dataclass(frozen=True)
class _CaptionWork:
    """One figure staged for a VLM call (crop bytes already fetched)."""

    visual_id: str
    parent_document_ref: Optional[str]
    image_bytes: bytes
    context: str
    signature: str
    content_sha256: str
    render_sha256: str
    # The crop's key into okg.visual_crop_blobs. Written by the source as a
    # distinct attr from render_sha256 (they are equal by construction today);
    # the enricher fetches bytes by THIS pointer, never by render_sha256, so
    # the "where the bytes live" ref and the "what feeds the signature" hash
    # stay independent.
    crop_blob_ref: str
    page_number: Optional[int]


class VisualCaptionEnricher:
    """Caption published figure ``visual_artifact`` nodes with a VLM in Phase B."""

    name: str = "okg_visual_caption_enricher"
    reads_subtypes: tuple[str, ...] = ("visual_artifact", "document_chunk")
    emits_edge_types: tuple[str, ...] = ("derived_from", "member_of")
    emits_node_subtypes: tuple[str, ...] = ("document_chunk",)
    # PACT bind-derived-node-provenance: this enricher writes document_chunk
    # nodes and supplies each one's `derived_from` (the visual it was captioned
    # from), so it declares node provenance. The publisher refuses a
    # node-writing enricher that does not.
    declares_node_provenance: bool = True
    # The caption chunk is derived from the one visual artifact it describes.
    support_policy: DerivedSupportPolicy = "all_members"
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("document_chunk", "derived_from", "visual_artifact"),
        ("document_chunk", "member_of", "document"),
    )

    def __init__(
        self,
        *,
        model: str,
        mode: str = "coarse",
        coarse_prompt: Optional[str] = None,
        max_workers: Optional[int] = None,
        max_images_per_doc: Optional[int] = None,
        min_image_px: int = 0,
        max_caption_attempts: int = 3,
        tracking_dsn: Optional[str] = None,
        tracking_mode: str | None = None,
        runtime_dsn: str | None = None,
        deployment: Optional[str] = None,
        caption_service: Optional[Any] = None,
    ) -> None:
        if not model or not isinstance(model, str):
            raise ValueError(
                "VisualCaptionEnricher: model is required and must be a "
                "non-empty litellm model string (no silent default; project "
                "P0 rule)."
            )
        if mode != "coarse":
            raise ValueError(
                f"VisualCaptionEnricher: mode must be 'coarse', got {mode!r}"
            )
        # Width comes from the `enrichment.model_calls` concurrency term unless
        # a deployment names one explicitly. It was a constructor default of 8,
        # which made this a THIRD parallel lane whose width nothing reconciled
        # against the others or against Postgres capacity.
        if max_workers is None:
            from okg.substrate.db.concurrency_budget import width

            max_workers = width("enrichment.model_calls")
        if not isinstance(max_workers, int) or max_workers < 1:
            raise ValueError(
                f"VisualCaptionEnricher: max_workers must be a positive int, "
                f"got {max_workers!r}"
            )
        if max_images_per_doc is not None and (
            not isinstance(max_images_per_doc, int) or max_images_per_doc < 1
        ):
            raise ValueError(
                f"VisualCaptionEnricher: max_images_per_doc must be a positive "
                f"int or None, got {max_images_per_doc!r}"
            )
        if not isinstance(min_image_px, int) or min_image_px < 0:
            raise ValueError(
                f"VisualCaptionEnricher: min_image_px must be a non-negative "
                f"int, got {min_image_px!r}"
            )
        if not isinstance(max_caption_attempts, int) or max_caption_attempts < 1:
            raise ValueError(
                f"VisualCaptionEnricher: max_caption_attempts must be a "
                f"positive int, got {max_caption_attempts!r}"
            )
        self.model = model
        self.mode = mode
        self.coarse_prompt = coarse_prompt
        self._resolved_prompt = coarse_prompt or _COARSE_PROMPT
        self.provider = _provider_from_model(model)
        self.max_workers = int(max_workers)
        self.max_images_per_doc = max_images_per_doc
        self.min_image_px = int(min_image_px)
        self.max_caption_attempts = int(max_caption_attempts)
        self._tracking_dsn = resolve_tracking_dsn(
            tracking_mode=tracking_mode,
            explicit_tracking_dsn=tracking_dsn,
            runtime_dsn=runtime_dsn,
            component="VisualCaptionEnricher",
        )
        self.tracking_mode = tracking_mode
        self.deployment = deployment
        self._caption_service = caption_service
        self._built_service: Any = None
        # Per-run accumulator of visuals whose crop-fetch or VLM call failed;
        # a non-empty list at run end marks the run incomplete (watermark held).
        self._input_failures: list[tuple[str, str]] = []

    # ------------------------------------------------------------------
    # Caption service (injected, or built from model + tracking_dsn)
    # ------------------------------------------------------------------

    def _service(self) -> Any:
        if self._caption_service is not None:
            return self._caption_service
        if self._built_service is None:
            from okg.substrate.library.captioning.service import CaptionService
            from okg.views.execution.llm import VLMClient

            vlm = VLMClient(
                model=self.model,
                tracking_dsn=self._tracking_dsn,
                tracking_mode=("optional" if self._tracking_dsn else "disabled"),
                deployment=self.deployment,
            )
            self._built_service = CaptionService(
                mode="coarse",
                vlm=vlm,
                model=self.model,
                coarse_prompt=self.coarse_prompt,
            )
        return self._built_service

    # ------------------------------------------------------------------
    # vision_signature
    # ------------------------------------------------------------------

    def _vision_signature(
        self,
        *,
        content_sha256: str,
        render_sha256: str,
        context: str,
    ) -> str:
        # Delegates to the ONE shared formula both this enricher and
        # DocCorpusSource use, so the source's predicted signature and the
        # enricher's recomputed one can never drift (which would silently
        # re-caption the whole corpus).
        return compute_vision_signature(
            content_sha256=content_sha256,
            render_sha256=render_sha256,
            model=self.model,
            provider=self.provider,
            prompt=self._resolved_prompt,
            mode=self.mode,
            context=context,
        )

    # ------------------------------------------------------------------
    # enrich
    # ------------------------------------------------------------------

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)
        self._input_failures = []

        # Caption-retry quarantine (D3): a permanently-failing figure must not
        # re-bill a VLM call every publish. Load the persistent per-(visual,
        # signature) failure counts up front so the candidate partition can skip
        # any figure that has already burned `max_caption_attempts` calls.
        # The table is schema-declared (schema.sql) and created by `migrate`; the
        # enricher deliberately does NOT issue `CREATE TABLE` at runtime here —
        # DDL on the publisher connection would abort the publish under a
        # least-privilege publisher role (app_rw has REVOKE CREATE ON SCHEMA okg).
        fail_counts = caption_failures.load_counts(conn)
        n_quarantined = 0

        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)

        # Figures published for captioning: visual_artifact nodes that carry a
        # crop_blob_ref (staged by the discovery-only source).
        visuals = graph.query(
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = 'visual_artifact'
               AND attrs ? 'crop_blob_ref'
             ORDER BY node_id
            """
        )
        if not visuals:
            logger.info("%s: no captionable figure visuals", self.name)
            return EnrichResult(enricher_name=self.name, n_edges_emitted=0)

        # Existing captions (prior publishes) keyed by (visual_ref, signature):
        # a match means this exact caption already exists → reuse, zero VLM.
        captions = graph.query(
            """
            SELECT node_id, source, attrs,
                   attrs->>'visual_ref' AS vref,
                   attrs->>'vision_signature' AS sig
              FROM okg.graph_nodes
             WHERE subtype = 'document_chunk'
               AND attrs->>'chunk_source' = 'vision_caption'
               AND attrs ? 'visual_ref'
               AND attrs ? 'vision_signature'
            """
        )
        existing: dict[tuple[str, str], set[str]] = {}
        for caption in captions:
            existing.setdefault((caption["vref"], caption["sig"]), set()).add(
                caption["node_id"]
            )
        current: dict[str, set[str]] = {}

        # Partition into reuse (skip) vs. candidates (need a VLM call),
        # computing each visual's predicted signature up front.
        candidates: list[_CaptionWork] = []
        n_reused = 0
        for row in visuals:
            visual_id = row["node_id"]
            attrs = row["attrs"] or {}
            crop_ref = attrs.get("crop_blob_ref")
            content_sha256 = str(attrs.get("content_sha256") or "")
            render_sha256 = str(attrs.get("render_sha256") or crop_ref or "")
            context = str(attrs.get("caption_context") or "")
            if not crop_ref:
                # The scan filters on `attrs ? 'crop_blob_ref'`, so the key is
                # present; a falsy value means a malformed discovery node. Hold
                # the watermark loudly rather than skip it silently (P0).
                self._record_failure(
                    visual_id, ValueError("empty crop_blob_ref on visual"),
                )
                continue
            if self.min_image_px:
                w = attrs.get("width")
                h = attrs.get("height")
                if isinstance(w, int) and isinstance(h, int) and w * h < self.min_image_px:
                    continue
            signature = self._vision_signature(
                content_sha256=content_sha256,
                render_sha256=render_sha256,
                context=context,
            )
            if (visual_id, signature) in existing:
                n_reused += 1
                current[visual_id] = existing[(visual_id, signature)]
                continue
            if fail_counts.get((visual_id, signature), 0) >= self.max_caption_attempts:
                # Quarantined: this exact (figure, model/prompt) already failed
                # max_caption_attempts VLM calls. Skip the call so the cost is
                # bounded — the figure stays recorded in okg.visual_caption_failures
                # (queryable, loud) and NOT as an input failure, so the watermark
                # can finally advance. A content/model/prompt change yields a new
                # signature and a fresh budget.
                n_quarantined += 1
                logger.warning(
                    "%s: visual %s QUARANTINED after %d failed caption attempts "
                    "(signature %s…); skipping VLM until its content/model/prompt "
                    "changes. Inspect: SELECT * FROM %s WHERE visual_id = %r.",
                    self.name, visual_id,
                    fail_counts[(visual_id, signature)], signature[:12],
                    caption_failures.TABLE, visual_id,
                )
                continue
            page_number = attrs.get("page_number")
            candidates.append(
                _CaptionWork(
                    visual_id=visual_id,
                    parent_document_ref=attrs.get("parent_document_ref"),
                    image_bytes=b"",  # filled in Phase A
                    context=context,
                    signature=signature,
                    content_sha256=content_sha256,
                    render_sha256=render_sha256,
                    crop_blob_ref=str(crop_ref),
                    page_number=page_number if isinstance(page_number, int) else None,
                )
            )

        # max_images_per_doc gates the VLM CALL (not discovery): cap the
        # candidates per parent document. Beyond-cap figures keep their
        # discovery node and are simply left uncaptioned this cycle.
        capped = self._apply_cap(candidates)

        # Phase A (single-threaded): fetch every crop's bytes from
        # okg.visual_crop_blobs by its crop_blob_ref. A missing crop is a
        # per-figure input failure (held watermark), never a silent skip.
        work: list[_CaptionWork] = []
        n_crop_missing = 0
        for c in capped:
            try:
                data = crop_store.get_crop(conn, c.crop_blob_ref)
            except crop_store.CropBlobMissing as exc:
                # A missing crop is raised in Python AFTER a successful SELECT
                # (the query ran; it just returned no row), so the connection is
                # NOT poisoned — isolating this one figure is safe. We catch
                # ONLY this: under the publisher the connection is inside an open
                # transaction, so a genuine DB error (table absent, permission,
                # dropped conn) poisons every later command on it. Swallowing
                # that would not isolate anything — it would defer the crash to
                # the write phase and throw away every already-paid caption. So
                # a real DB error is left to surface loudly and abort the run.
                self._record_failure(c.visual_id, exc)
                n_crop_missing += 1
                continue
            work.append(
                _CaptionWork(
                    visual_id=c.visual_id,
                    parent_document_ref=c.parent_document_ref,
                    image_bytes=data,
                    context=c.context,
                    signature=c.signature,
                    content_sha256=c.content_sha256,
                    render_sha256=c.render_sha256,
                    crop_blob_ref=c.crop_blob_ref,
                    page_number=c.page_number,
                )
            )

        # If EVERY candidate's crop is missing (and there are at least two — a
        # lone miss is more likely genuine per-figure loss), the likely cause is
        # not per-figure loss but that the source staged crops into a different
        # database than the enricher reads from (its dsn vs. the publisher's
        # connection). Surface that explicitly rather than as an opaque failure
        # count with no cause.
        if len(capped) >= 2 and n_crop_missing == len(capped):
            logger.error(
                "%s: ALL %d candidate crops are absent from %s on this "
                "connection. This usually means the discovery source staged "
                "crops into a DIFFERENT database than the enricher reads — "
                "verify the source dsn and the publisher connection point at "
                "the same okg database.",
                self.name, n_crop_missing, crop_store.TABLE,
            )

        # Phase B (parallel): fan out one VLM call per figure. Pure network
        # work — no DB, no shared psycopg connection.
        captioned: list[tuple[_CaptionWork, Any]] = []
        if work:
            svc = self._service()
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = {
                    pool.submit(self._caption_one, svc, w, generation_id): w
                    for w in work
                }
                for fut in as_completed(futures):
                    w = futures[fut]
                    try:
                        result = fut.result()
                    except Exception as exc:  # noqa: BLE001 — record & hold
                        # A VLM call was made and failed → counts toward
                        # quarantine (bounds the per-cycle cost).
                        self._record_vlm_failure(conn, w, exc)
                        continue
                    if result is None:
                        # A caption service that returned no result is a
                        # degenerate outcome, not a clean skip — hold it loud so
                        # the figure retries rather than silently vanishing.
                        self._record_vlm_failure(
                            conn, w,
                            ValueError("caption service returned no result"),
                        )
                        continue
                    captioned.append((w, result))

        # Phase C (single-threaded): emit the caption chunks + edges.
        n_edges = 0
        for w, result in captioned:
            edges = self._emit_caption(
                conn,
                work=w,
                result=result,
                generation_id=generation_id,
                run_id=run_id,
                observed_at=observed_at,
            )
            if edges > 0:
                current[w.visual_id] = {_caption_id(w.visual_id, result.text)}
                # A caption landed — clear any prior failure budget for this
                # (visual, signature) so a transient failure that later
                # succeeds does not count against a future re-caption.
                caption_failures.clear(
                    conn, visual_id=w.visual_id, vision_signature=w.signature,
                )
            n_edges += edges

        # Only retire our own old nodes after a usable replacement exists.
        # A changed signature can produce identical text (and thus the same
        # node id); that node was updated in place and must stay live.
        from okg.substrate.library.enrichers._fact_emitters import (
            emit_edge_fact,
            emit_node_fact,
        )
        from okg.substrate.nomos.source_lineage import LINEAGE_ATTR

        producer_id = f"_enricher:{self.name}"
        superseded: list[dict[str, Any]] = []
        foreign_sources: set[str] = set()
        for caption in captions:
            keep = current.get(caption["vref"])
            if not keep or caption["node_id"] in keep:
                continue
            if caption["source"] != producer_id:
                # Not ours to retract. Loud below, because a deployment whose
                # captions were written under a different producer name (a
                # rename, or a deployment-local captioner) otherwise keeps the
                # exact pre-fix behaviour with no signal at all.
                foreign_sources.add(str(caption["source"]))
                continue
            superseded.append(caption)

        # The caption node's OWN edges (derived_from visual, member_of
        # document) are retracted here, explicitly. The publisher's
        # `retract_orphaned_derived_edges` sweep would also remove them
        # (it matches `source LIKE '_enricher:%'` with a dead endpoint), but
        # relying on that makes correctness under the default `block`
        # dangling-edge policy an implicit dependency on another subsystem's
        # sweep. Retracting them at the producer keeps the enricher's output
        # self-consistent; the sweep remains a backstop for edges pointing at
        # a superseded *visual*, which is issue #1454 and NOT fixed here.
        stale_edges: list[dict[str, Any]] = []
        if superseded:
            stale_edges = graph.query(
                """
                SELECT src, dst, edge_type, attrs, provenance, confidence
                  FROM okg.graph_edges
                 WHERE source = :producer_id
                   AND src = ANY(:node_ids)
                 ORDER BY src, edge_type, dst
                """,
                {
                    "producer_id": producer_id,
                    "node_ids": [c["node_id"] for c in superseded],
                },
            )

        n_retracted = 0
        n_edges_retracted = 0
        # `observed_at` is one instant for the whole run and the retained
        # UNIQUE (dedupe_key, observed_at) is NOT scoped by op, so an I and an
        # R sharing a dedupe key at this instant would silently collapse. It
        # cannot happen: every node retracted here is absent from `keep`, and
        # only nodes in `keep` were asserted this run — an id is never both.
        for caption in superseded:
            # Minimal attrs: the retraction deletes the node, so its text is
            # inert weight in the fact log. Drop the publisher-stamped source
            # lineage in particular — re-emitting an envelope this enricher did
            # not author is the laundering shape nomos/source_lineage.py warns
            # about (#1534).
            attrs = {
                key: value
                for key, value in dict(caption["attrs"] or {}).items()
                if key != LINEAGE_ATTR
            }
            source_record_id = {
                "enricher": self.name,
                "visual_ref": caption["vref"],
                "chunk_id": caption["node_id"],
            }
            source_revision = {"run_id": run_id, "generation_id": generation_id}
            outcome = emit_node_fact(
                conn,
                enricher_name=self.name,
                node_id=caption["node_id"],
                subtype="document_chunk",
                attrs=attrs,
                dedupe_key=_dedupe("node", caption["node_id"], caption["sig"]),
                source_record_id=source_record_id,
                source_revision=source_revision,
                run_id=run_id,
                observed_at=observed_at,
                op="R",
                support_policy=resolve_declared_support_policy(self),
            )
            n_retracted += int(outcome.inserted)
        for edge in stale_edges:
            outcome = emit_edge_fact(
                conn,
                enricher_name=self.name,
                src=str(edge["src"]),
                dst=str(edge["dst"]),
                edge_type=str(edge["edge_type"]),
                attrs=dict(edge["attrs"] or {}),
                provenance=str(edge["provenance"] or "extracted_model"),
                confidence=float(edge["confidence"] or 1.0),
                dedupe_key=_dedupe(
                    "edge", edge["src"], edge["edge_type"], edge["dst"],
                ),
                source_record_id={
                    "enricher": self.name,
                    "chunk_id": str(edge["src"]),
                },
                source_revision={"run_id": run_id, "generation_id": generation_id},
                run_id=run_id,
                observed_at=observed_at,
                op="R",
            )
            n_edges_retracted += int(outcome.inserted)
        if n_retracted:
            logger.info(
                "%s: retracted %d superseded caption node(s) and %d edge(s) "
                "(deployment=%s, generation=%d)",
                self.name, n_retracted, n_edges_retracted,
                self.deployment or "<unnamed>", generation_id,
            )
        if foreign_sources:
            logger.warning(
                "%s: left superseded caption(s) live because another producer "
                "owns them (sources=%s, deployment=%s); this enricher only "
                "retracts its own nodes, so stale captions under a renamed or "
                "deployment-local captioner keep accumulating",
                self.name, ",".join(sorted(foreign_sources)),
                self.deployment or "<unnamed>",
            )

        n_input_failures = len(self._input_failures)
        incomplete = n_input_failures > 0
        error_msg: Optional[str] = None
        if incomplete:
            error_msg = (
                f"{n_input_failures} figure(s) failed crop-fetch/VLM; watermark "
                f"held so they retry next cycle"
            )
        log_at = logger.warning if (incomplete or n_quarantined) else logger.info
        log_at(
            "%s: captioned %d figure(s) (%d reused, %d input failures, "
            "%d quarantined, %d superseded caption(s) retracted, "
            "generation=%d, incomplete=%s)",
            self.name, len(captioned), n_reused, n_input_failures,
            n_quarantined, n_retracted, generation_id, incomplete,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_edges,
            error=error_msg,
            incomplete=incomplete,
            n_input_failures=n_input_failures,
            n_quarantined=n_quarantined,
            n_retracted=n_retracted,
        )

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _apply_cap(self, candidates: list[_CaptionWork]) -> list[_CaptionWork]:
        if self.max_images_per_doc is None:
            return candidates
        per_doc: dict[Optional[str], int] = {}
        out: list[_CaptionWork] = []
        for c in candidates:
            k = c.parent_document_ref
            if per_doc.get(k, 0) >= self.max_images_per_doc:
                continue
            per_doc[k] = per_doc.get(k, 0) + 1
            out.append(c)
        return out

    def _record_failure(self, visual_id: str, exc: Exception) -> None:
        """An INFRASTRUCTURE failure that did NOT cost a VLM call (missing crop,
        malformed discovery node). Holds the watermark so it retries, but does
        NOT count toward quarantine — it signals a staging problem to fix
        (re-ingest restages the crop), not a figure to give up on, and counting
        it would falsely quarantine a figure whose crop is later restaged."""
        self._input_failures.append((visual_id, repr(exc)))
        logger.warning(
            "%s: captioning failed for visual %s: %s; recording it as an "
            "unprocessed input so the watermark holds and it retries next cycle",
            self.name, visual_id, exc,
        )

    def _record_vlm_failure(
        self, conn: psycopg.Connection, work: _CaptionWork, exc: Exception,
    ) -> None:
        """A failure at the CAPTIONING stage — the crop decode, the VLM call, an
        empty reply, or a degenerate no-result. Holds the watermark like any
        input failure AND increments the persistent per-(visual, signature)
        counter, so a permanently-failing figure is quarantined after
        ``max_caption_attempts`` instead of re-billing every publish forever
        (D3). These failures recur IDENTICALLY for the same (visual, signature)
        — the crop is content-addressed, so an undecodable crop stays
        undecodable and a VLM that empties on it keeps emptying — which is why
        bounding them is correct. Contrast :meth:`_record_failure`, for failures
        a re-ingest can fix (a missing crop), which retry unbounded."""
        self._input_failures.append((work.visual_id, repr(exc)))
        count = caption_failures.record_failure(
            conn,
            visual_id=work.visual_id,
            vision_signature=work.signature,
            error=repr(exc),
        )
        will_quarantine = count >= self.max_caption_attempts
        logger.warning(
            "%s: VLM captioning failed for visual %s (attempt %d/%d): %s; "
            "watermark held so it retries%s",
            self.name, work.visual_id, count, self.max_caption_attempts, exc,
            " — QUARANTINED from next cycle (no further VLM calls until its "
            "content/model/prompt changes)" if will_quarantine else "",
        )

    def _caption_one(
        self,
        svc: Any,
        work: _CaptionWork,
        generation_id: int | str | None = None,
    ) -> Any:
        """Worker: caption one crop. Runs in a thread — NO DB access."""
        from PIL import Image as PILImage

        image = PILImage.open(io.BytesIO(work.image_bytes))
        kwargs = {
            "caller": f"enricher:{self.name}",
            "fragment_id": work.visual_id,
        }
        # Keep injected legacy test services usable while making the concrete
        # CaptionService carry the publisher checkout generation into the
        # llm_calls row.
        try:
            signature = inspect.signature(svc.caption)
            accepts_generation = (
                "generation_id" in signature.parameters
                or any(
                    parameter.kind is inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
            )
        except (TypeError, ValueError):
            accepts_generation = False
        if accepts_generation:
            kwargs["generation_id"] = generation_id
        results = svc.caption(image, work.context, **kwargs)
        return results[0] if results else None

    def _emit_caption(
        self,
        conn: psycopg.Connection,
        *,
        work: _CaptionWork,
        result: Any,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        from okg.substrate.library.enrichers._fact_emitters import (
            emit_edge_fact,
            emit_node_fact,
        )

        # A VLM reply can carry lone UTF-16 surrogates (bad glyph decode); they
        # are unencodable as UTF-8 and would crash the hash / DB write. Replace.
        cap_text = (result.text or "").encode("utf-8", "replace").decode("utf-8")
        if not cap_text:
            # An empty caption writes no chunk, so no signature lands for this
            # visual — which would silently re-burn a VLM call every publish
            # (defeating reuse). Record it as a VLM-cost failure: the watermark
            # holds and the figure retries, but the counter advances so a figure
            # the VLM *always* empties on is quarantined after
            # max_caption_attempts rather than re-billed forever (D3).
            self._record_vlm_failure(
                conn, work, ValueError("VLM returned an empty caption"),
            )
            return 0
        chunk_sha = hashlib.sha256(cap_text.encode("utf-8")).hexdigest()
        # Identity MUST include the figure being captioned. Keying on
        # ('image_caption', cap_text) alone collapses two different figures that
        # happen to get identical caption text into ONE node carrying two
        # conflicting visual_refs/signatures — only one survives, so the other
        # figure never matches the reuse check and is re-sent to the VLM every
        # cycle (the exact defect the reuse machinery exists to prevent). The
        # visual_id disambiguates them into distinct, correctly-attributed nodes.
        chunk_id = _caption_id(work.visual_id, cap_text)

        attrs: dict[str, Any] = {
            "chunk_id": chunk_id,
            "text": cap_text,
            "chunk_kind": "image_caption",
            "content_sha256": chunk_sha,
            "chunker_name": "vision_caption",
            "chunk_source": "vision_caption",
            "caption_model": self.model,
            "caption_provider": self.provider,
            "visual_ref": work.visual_id,
            "vision_signature": work.signature,
        }
        category = getattr(result, "category", "") or ""
        if category:
            attrs["category"] = category.encode("utf-8", "replace").decode("utf-8")
        if work.page_number is not None:
            attrs["heading_path"] = f"page {work.page_number}"

        source_record_id = {
            "enricher": self.name,
            "visual_ref": work.visual_id,
            "chunk_id": chunk_id,
        }
        source_revision = {"run_id": run_id, "generation_id": generation_id}

        outcome = emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id=chunk_id,
            subtype="document_chunk",
            attrs=attrs,
            dedupe_key=_dedupe("node", chunk_id, work.signature),
            source_record_id=source_record_id,
            source_revision=source_revision,
            run_id=run_id,
            observed_at=observed_at,
            derived_from=[work.visual_id],
            support_policy=resolve_declared_support_policy(self),
        )
        if not outcome.emitted:
            self._record_failure(work.visual_id, ValueError("caption fact was not accepted"))
            return 0
        n_edges = 0
        emit_edge_fact(
            conn,
            enricher_name=self.name,
            src=chunk_id,
            dst=work.visual_id,
            edge_type="derived_from",
            attrs={"enricher": self.name},
            provenance="extracted_model",
            confidence=1.0,
            dedupe_key=_dedupe("edge", chunk_id, "derived_from", work.visual_id),
            source_record_id=source_record_id,
            source_revision=source_revision,
            run_id=run_id,
            observed_at=observed_at,
        )
        n_edges += 1
        if work.parent_document_ref:
            emit_edge_fact(
                conn,
                enricher_name=self.name,
                src=chunk_id,
                dst=work.parent_document_ref,
                edge_type="member_of",
                attrs={"enricher": self.name},
                provenance="extracted_model",
                confidence=1.0,
                dedupe_key=_dedupe(
                    "edge", chunk_id, "member_of", work.parent_document_ref
                ),
                source_record_id=source_record_id,
                source_revision=source_revision,
                run_id=run_id,
                observed_at=observed_at,
            )
            n_edges += 1
        return n_edges


def _caption_id(visual_id: str, text: str) -> str:
    text = (text or "").encode("utf-8", "replace").decode("utf-8")
    return "chunk:" + hashlib.sha256(
        chr(0).join(("image_caption", visual_id, text)).encode("utf-8")
    ).hexdigest()[:16]


def _dedupe(*parts: object) -> str:
    seed = "okg_visual_caption_enricher:" + "|".join(str(p) for p in parts)
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()
