"""Load an existing GraphSnapshot (pre-substrate JSON) as a generation.

Chronos-native rewrite. The legacy loader (deleted in f567014f as
"dead bootstrap") streamed a snapshot into `okg.node_facts` /
`okg.edge_facts` and projected it into the now-dropped
`okg.nodes_live` / `okg.edges_live` tables. Those tables are gone:
under the chronos cutover the published graph IS a Chronos
checkpoint over `okg.graph_nodes` / `okg.graph_edges`.

This is the deployment-onboarding entry point — operators run it
once after `okg migrate --apply` (and, optionally, `okg catalog
load --apply`) to seed a fresh substrate DB from a known-good
legacy snapshot. It:

1. Streams the snapshot's `nodes` / `edges` arrays (via `ijson`)
   into `NodeFact` / `EdgeFact` and applies them to the Chronos
   `default` branch with
   `ChronosGraphStore.apply_facts(..., strict_endpoints=False)`.
2. Checkpoints the branch head as the deployment's first
   **published** generation via
   `ChronosGraphStore.checkpoint_generation(..., status="published")`,
   so it is readable through `latest_generation(...)` +
   `checkout_generation(...)`.

Snapshot shape (top-level object):

    {"nodes": [ {node_id|id, subtype|type, attrs, label?, text?}, ... ],
     "edges": [ {src, dst, edge_type|type, attrs, provenance?,
                 confidence?}, ... ]}

Both the legacy (`id`/`type`) and the substrate (`node_id`/
`subtype`/`edge_type`) key spellings are accepted.

Image values (#3046) are reduced to their bare reference (``png`` and
``error`` dropped) before they are applied: a snapshot taken from another
okg names that okg's blob rows, which this database does not hold. A loaded
``file`` reference reads as not ingested. The loader is operator-trusted and
does not otherwise check image values.
"""
from __future__ import annotations

import hashlib
import itertools
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import ijson

from okg.substrate.errors import SubstrateError
from okg.substrate.images.values import strip_ingest_fields
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.db.connection import connect as db_connect


@dataclass
class BootstrapResult:
    generation_id: str
    nodes_appended: int
    edges_appended: int
    elapsed_seconds: float


@dataclass(frozen=True)
class VerifiedSnapshotAuthority:
    principal: str
    reason: str
    verification_ref: str

    def __post_init__(self) -> None:
        if not self.principal or not self.reason or not self.verification_ref:
            raise ValueError(
                "verified snapshot authority requires principal, reason, "
                "and verification_ref"
            )


def _stream_items(path: Path, prefix: str) -> Iterator[dict]:
    with path.open("rb") as f:
        yield from ijson.items(f, prefix)


def _node_facts(path: Path) -> Iterator[NodeFact]:
    """Yield a NodeFact per snapshot node.

    Accepts both the legacy (`id`/`type`/`label`/`text`) and the
    substrate (`node_id`/`subtype`) key spellings.
    """
    for n in _stream_items(path, "nodes.item"):
        node_id = n.get("node_id") or n.get("id")
        if not node_id:
            raise SubstrateError(
                "snapshot node missing node_id/id",
                data={"node": n},
                fix_hint="every node needs a stable `node_id` (or legacy `id`)",
            )
        node_id = str(node_id)
        subtype = n.get("subtype") or n.get("type") or "Thing"
        attrs = dict(n.get("attrs") or {})
        if n.get("label") and "label" not in attrs:
            attrs["label"] = n["label"]
        if n.get("text") and "text" not in attrs:
            attrs["text"] = n["text"]
        yield NodeFact(
            node_id=node_id,
            subtype=str(subtype),
            attrs=strip_ingest_fields(attrs),
            source_record_id={"node_id": node_id},
            source_revision={"bootstrap": True, "revision": n.get("revision")},
        )


def _edge_facts(path: Path) -> Iterator[EdgeFact]:
    """Yield an EdgeFact per snapshot edge.

    The Chronos store mints the `edge_id` deterministically from
    `(src, edge_type, dst)`, so the loader does not assign one.
    """
    for e in _stream_items(path, "edges.item"):
        src = e.get("src")
        dst = e.get("dst")
        if not src or not dst:
            raise SubstrateError(
                "snapshot edge missing src/dst",
                data={"edge": e},
                fix_hint="every edge needs `src` and `dst` node ids",
            )
        etype = e.get("edge_type") or e.get("type") or "related"
        yield EdgeFact(
            src=str(src),
            dst=str(dst),
            edge_type=str(etype),
            attrs=strip_ingest_fields(dict(e.get("attrs") or {})),
            provenance=e.get("provenance") or "explicit",
            confidence=float(e.get("confidence") or 1.0),
            source_record_id={"src": str(src), "dst": str(dst),
                              "edge_type": str(etype)},
            source_revision={"bootstrap": True},
        )


def _resolve_catalog_version_id(conn) -> int | None:
    """Return the max applied catalog_version_id, or None.

    Onboarding may run before `okg catalog load`; a missing catalog
    is not fatal for snapshot-load (the generation is still
    published, with `catalog_version_id=None`).
    """
    with conn.cursor() as cur:
        if cur.execute(
            "SELECT to_regclass('okg.catalog_versions') AS rel"
        ).fetchone()[0] is None:
            return None
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def bootstrap_snapshot(
    dsn: str,
    snapshot_path: Path,
    deployment_name: str,
    *,
    verified_authority: VerifiedSnapshotAuthority | None = None,
) -> BootstrapResult:
    """Admit a legacy snapshot, then let Publisher make it visible."""
    t0 = time.monotonic()
    run_id = str(uuid.uuid4())
    observed_at = datetime.now(timezone.utc)

    from okg.substrate.admission import (
        ensure_envelope,
        stable_idempotency_key,
        transition_envelope,
    )
    from okg.substrate.fact_application import (
        FactBatchApplication,
        ZeroOutputEvidence,
        assert_zero_output_acceptance_allowed,
        chronos_apply_callback,
    )
    from okg.substrate.fact_batch import (
        FactBatchHeader,
        FactChunk,
        PostgresApplicationReceiptStore,
    )
    from okg.substrate.graph_store import DEFAULT_BRANCH, ChronosGraphStore
    from okg.substrate.producer_authority import (
        ProducerProfile,
        profile_fingerprint,
    )

    # Chronos's own `connect()` mis-detects a key=value conninfo as sqlite,
    # so this opens the psycopg connection itself and uses `from_connection`.
    #
    # `idle_in_transaction_session_timeout=0` disables the bound for THIS
    # session only. Everything from the `count(*)` below to `conn.commit()`
    # runs in one transaction, and most of that time is client-side: hashing
    # the snapshot file in 1 MB blocks and building fact batches from it. The
    # session is legitimately idle in transaction for as long as the snapshot
    # takes to read, which for a large one exceeds the 300-second default and
    # would be killed with SQLSTATE 25P03. The factory's other protections
    # (keepalives, connect_timeout) still apply, and libpq takes the last
    # `-c` for a setting, so this overrides the factory's own value.
    with db_connect(
        dsn, options="-c idle_in_transaction_session_timeout=0",
    ) as conn:
        store = ChronosGraphStore.from_connection(conn)

        branch = store.checkout(DEFAULT_BRANCH)
        existing = int(
            branch.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        if existing > 0 and verified_authority is None:
            err = SubstrateError(
                "okg.graph_nodes is non-empty; snapshot onboarding requires "
                "separately verified authority",
                data={
                    "snapshot": str(snapshot_path),
                    "deployment_name": deployment_name,
                    "existing_nodes": existing,
                },
                fix_hint=(
                    "use a fresh DB or supply VerifiedSnapshotAuthority from "
                    "an authenticated onboarding workflow"
                ),
            )
            err.code = "bootstrap_authority_required"
            raise err

        file_hash = hashlib.sha256()
        with snapshot_path.open("rb") as snapshot_file:
            for block in iter(lambda: snapshot_file.read(1024 * 1024), b""):
                file_hash.update(block)
        snapshot_digest = "sha256:" + file_hash.hexdigest()
        catalog_version_id = _resolve_catalog_version_id(conn)
        producer_id = f"system:snapshot-onboarding:{deployment_name}"
        authority_scope = {
            "snapshot_digest": snapshot_digest,
            "non_empty": existing > 0,
            "verification_ref": (
                verified_authority.verification_ref
                if verified_authority is not None
                else "fresh-database"
            ),
        }
        fingerprint = profile_fingerprint(
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind="system",
            trust_label="trusted",
            admission_mode="fast_track",
            authority_scope=authority_scope,
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            catalog_version_id=catalog_version_id,
        )
        profile = ProducerProfile(
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind="system",
            trust_label="trusted",
            admission_mode="fast_track",
            authority_scope=authority_scope,
            allowed_record_kinds=("node", "edge"),
            output_signature={},
            policy_clauses=(),
            output_scope_summary={"snapshot_digest": snapshot_digest},
            source_ref={
                "workflow": "snapshot-onboarding",
                "principal": (
                    verified_authority.principal
                    if verified_authority is not None
                    else "system:fresh-database"
                ),
            },
            catalog_version_id=catalog_version_id,
            profile_fingerprint=fingerprint,
            status="compat",
        )
        idempotency_key = stable_idempotency_key(
            "snapshot-onboarding",
            deployment_name,
            snapshot_digest,
        )
        envelope = ensure_envelope(
            conn,
            deployment_name=deployment_name,
            producer_id=producer_id,
            producer_kind="system",
            intent="snapshot_onboarding",
            admission_mode="fast_track",
            status="pending",
            idempotency_key=idempotency_key,
            catalog_version_id=catalog_version_id,
            source=producer_id,
            run_id=run_id,
            authority={
                **profile.evidence(),
                **authority_scope,
                "reason": (
                    verified_authority.reason
                    if verified_authority is not None
                    else "fresh database onboarding"
                ),
            },
        )
        header = FactBatchHeader(
            schema_version=1,
            run_id=run_id,
            admission_id=envelope.admission_id,
            producer_profile_fingerprint=fingerprint,
            deployment=deployment_name,
            target_branch=DEFAULT_BRANCH,
            catalog_version_id=catalog_version_id or 0,
            intent="snapshot_onboarding",
            idempotency_key=idempotency_key,
            requested_scope=authority_scope,
        )
        application = FactBatchApplication(
            header=header,
            profile=profile,
            receipt_store=PostgresApplicationReceiptStore(conn),
            apply_graph=chronos_apply_callback(
                store,
                source=producer_id,
                observed_at=observed_at,
                strict_endpoints=False,
            ),
        )
        node_count = edge_count = ordinal = 0
        chunk: list[NodeFact | EdgeFact] = []
        for fact in itertools.chain(
            _node_facts(snapshot_path),
            _edge_facts(snapshot_path),
        ):
            chunk.append(fact)
            node_count += isinstance(fact, NodeFact)
            edge_count += isinstance(fact, EdgeFact)
            if len(chunk) == 1000:
                application.apply_chunk(
                    FactChunk.create(
                        run_id=run_id,
                        ordinal=ordinal,
                        facts=chunk,
                    )
                )
                ordinal += 1
                chunk = []
        if chunk:
            application.apply_chunk(
                FactChunk.create(
                    run_id=run_id,
                    ordinal=ordinal,
                    facts=chunk,
                )
            )
        zero_output_evidence = None
        if node_count == 0 and edge_count == 0:
            zero_output_evidence = ZeroOutputEvidence(
                kind="authoritative_empty_snapshot",
                health_status="ok",
                completed_scope=True,
                terminal_state_authorized=True,
                reason="verified snapshot contains no graph facts",
            )
            assert_zero_output_acceptance_allowed(zero_output_evidence)
        application.finish_observation(
            completion="snapshot",
            observed_scope=authority_scope,
        )
        transition_envelope(
            conn,
            admission_id=envelope.admission_id,
            status="accepted",
            actor=producer_id,
            reason="snapshot_onboarding_candidate_applied",
            verdict={
                "snapshot_digest": snapshot_digest,
                "node_count": node_count,
                "edge_count": edge_count,
                "publisher_required": True,
            },
        )
        conn.commit()

    from okg.substrate.publisher.factory import build_deployment_publisher

    publish = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment_name,
        command="okg db bootstrap snapshot",
    ).cycle(force_enrichers=True)
    if publish.get("status") != "published":
        raise SubstrateError(
            "snapshot candidate did not publish",
            data={"publish": publish, "snapshot": str(snapshot_path)},
        )
    generation_id = str(publish["generation_id"])

    elapsed = time.monotonic() - t0
    return BootstrapResult(
        generation_id=generation_id,
        nodes_appended=node_count,
        edges_appended=edge_count,
        elapsed_seconds=elapsed,
    )
