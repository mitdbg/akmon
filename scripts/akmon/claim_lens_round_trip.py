#!/usr/bin/env python3
"""Claim: an edit made through a lens round-trips into the published graph.

A lens is an editable view: a view over graph rows plus a contract naming
which fields may be edited and how an edit maps back to graph facts.
Against the published reviewer deployment this script:

1. builds a view of the record nodes (row = record, field = title) at the
   current published generation and registers a lens that makes `title`
   editable and maps an edit to the record node's `title` attribute;
2. submits an edit whose expected value is stale: it is refused as a
   conflict and no overlay is written;
3. submits an edit against the value it read: the overlay commits, and
   reading the view back returns the new value (put, then get);
4. promotes the overlay (`okg admission decide --action promote`, which
   opens a lens admission envelope) and runs one publisher cycle
   (`okg run --once`), which applies the envelope: the next published
   generation carries the new title on the record node.

Disclosed gap: registering a lens does not create the producer profile its
promotion is checked against, and no command does. This script creates it
with okg.substrate.producer_authority.materialize_producer_profile and says
so in its output (issue #2860).

Usage:
    uv run python scripts/akmon/claim_lens_round_trip.py \
        --state <workdir>/reviewer-state.json --dsn <postgres-url>
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    run_main,
    DSN_ENV,
    activate_deployment,
    deployment_environment,
    fail,
    load_state,
    note,
    passed,
    require_keyless,
    resolve_dsn,
    run_okg,
)


SLOT = "records"
SCOPE = "record"
FIELD = "title"
AUTHORITY = "akmon-reviewer.records"
EDIT_SUFFIX = " (edited through a lens)"


def _record_rows(checkout: Any, subtype: str) -> list[dict[str, Any]]:
    rows = checkout.query(
        "SELECT node_id, attrs->>'title' AS title FROM okg.graph_nodes "
        "WHERE subtype = %(subtype)s ORDER BY node_id",
        {"subtype": subtype},
    )
    return [dict(row) for row in rows]


def _publish_view(conn, *, name: str, suffix: str, token: int, rows: list[dict[str, Any]]):
    """Publish the record-title relation and the view that exposes it."""
    from okg.substrate.derived_artifacts import (
        InlineViewOutput,
        RelationArtifactDefinition,
        RelationRow,
        ViewArtifactDefinition,
        publish_relation_artifact,
        publish_view_artifact,
        stable_hash,
    )

    base = f"akmon_reviewer.record_titles.{suffix}"
    dirty_rule = {"scope_key": SCOPE, "dependencies": ["nodes_live"]}
    relation_contract = {"logical_relation_id": f"{base}.rows", "columns": [FIELD]}
    relation_definition = RelationArtifactDefinition(
        definition_id=f"relation:{base}.rows",
        deployment_name=name,
        logical_relation_id=f"{base}.rows",
        compute_hash=stable_hash({"relation": base, "compute": "v1"}),
        output_schema_hash=stable_hash(relation_contract),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=relation_contract,
        dirty_rule=dirty_rule,
    )
    view_contract = {"slots": [SLOT]}
    view = ViewArtifactDefinition(
        definition_id=f"view:{base}",
        deployment_name=name,
        view_id=base,
        view_name="Record titles",
        compute_hash=stable_hash({"view": base, "compute": "v1"}),
        output_schema_hash=stable_hash(view_contract),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=view_contract,
        dirty_rule=dirty_rule,
    )
    relation = publish_relation_artifact(
        conn,
        relation_definition,
        target_generation_id=token,
        rows=[
            RelationRow(
                scope_key=SCOPE,
                row_key=row["node_id"],
                row_data={FIELD: row["title"], "source_revision": str(token)},
            )
            for row in rows
        ],
    )
    column = {"name": FIELD, "label": "Title", "data_type": "text", "required": True}
    published = publish_view_artifact(
        conn,
        view,
        target_generation_id=token,
        outputs=[
            InlineViewOutput.relation_ref(
                slot_name=SLOT,
                upstream_definition_id=relation_definition.definition_id,
                logical_relation_id=relation_definition.logical_relation_id,
                storage_ref={
                    "schema": relation.schema_name,
                    "relation": relation.relation_name,
                    "target_generation_id": token,
                    "upstream_manifest_id": str(relation.manifest_id),
                    "upstream_target_generation_id": token,
                    "bound_compute_hash": relation_definition.compute_hash,
                    "bound_output_schema_hash": relation_definition.output_schema_hash,
                    "bound_renderer_hash": relation_definition.renderer_hash,
                    "bound_dirty_rule_hash": relation_definition.dirty_rule_hash,
                    "columns": [column],
                    "column_names": [FIELD],
                    "scope": {"policy": "column", "column": "scope_key"},
                    "row_identity": {"policy": "column", "column": "row_key"},
                },
                row_count=relation.row_count,
            )
        ],
    )
    return view, published


def _view_rows(conn, *, name: str, view_id: str, token: int) -> dict[str, dict[str, Any]]:
    from okg.substrate.derived_artifacts import inspect_view_slot

    inspection = inspect_view_slot(
        conn,
        deployment_name=name,
        view_id=view_id,
        generation_id=token,
        slot_name=SLOT,
        preview_limit=50,
    )
    rows = ((inspection.get("relation") or {}).get("preview") or {}).get("rows") or []
    return {str(row.get("row_key")): row for row in rows}


def _overlay_value(row: dict[str, Any]) -> Any:
    active = [
        overlay for overlay in row.get("lens_overlays") or []
        if overlay.get("status") == "active" and list(overlay.get("field_path") or []) == [FIELD]
    ]
    return active[-1].get("new_value") if active else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--dsn", help=f"PostgreSQL URL (or set {DSN_ENV})")
    parser.add_argument(
        "--stale-value",
        default="A title this record never had",
        help="expected value for the edit that must be refused as stale",
    )
    args = parser.parse_args(argv)

    require_keyless("lens-round-trip")
    dsn = resolve_dsn(args.dsn)
    state = load_state(args.state)
    name = state["deployment_name"]
    env = deployment_environment(state, dsn)
    activate_deployment(state, dsn)

    import psycopg

    from okg.substrate.derived_artifacts import stable_hash
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.lenses import (
        LensDefinition,
        LensPutbackDelta,
        LensPutbackStateError,
        commit_lens_overlay,
        lens_producer_id,
        lens_promotion_mapping_checksum,
        prepare_lens_putback,
        register_lens_definition,
    )
    from okg.substrate.producer_authority import materialize_producer_profile

    subtype = f"{name.replace('-', '_')}_record"
    suffix = uuid.uuid4().hex[:8]
    target = f"{name}:records:record:rec-1"

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        head = store.latest_generation(deployment_name=name)
        if head is None:
            fail("read", "deployment has no published generation", deployment=name)
        token = int((head.metadata or {})["target_generation_id"])
        rows = _record_rows(store.checkout("default"), subtype)
        by_id = {row["node_id"]: row for row in rows}
        if target not in by_id or not by_id[target]["title"]:
            fail("read", "record rec-1 has no title in the published graph", node=target)
        original = by_id[target]["title"]
        edited = original + EDIT_SUFFIX
        note(f"published head={head.checkpoint_id} records={len(rows)}")
        note(f"{target} title={original!r}")

    with psycopg.connect(dsn) as conn:
        view, published_view = _publish_view(
            conn, name=name, suffix=suffix, token=token, rows=rows,
        )
        mapping = {
            "version": 1,
            "rules": [{
                "id": f"record.{FIELD}",
                "slot_name": SLOT,
                "field_path": [FIELD],
                "mutation": {
                    "kind": "node_attr",
                    "subtype": subtype,
                    "attr_field": FIELD,
                    "node_id_template": "{row_key}",
                },
            }],
        }
        contract = {"editable_slots": {SLOT: {"row_identity": "row_key", "editable_fields": [FIELD]}}}
        putback_policy = {"mode": "overlay", "validator": "expected_value_compare_v1"}
        lens = LensDefinition(
            definition_id=f"lens:{view.view_id}.editor",
            deployment_name=name,
            lens_id=f"{view.view_id}.editor",
            view_definition_id=view.definition_id,
            view_id=view.view_id,
            compute_hash=stable_hash({"lens": view.view_id, "compute": "v1"}),
            output_schema_hash=stable_hash(contract),
            renderer_hash=stable_hash({"renderer": "lens-overlay"}),
            dirty_rule_hash=stable_hash(putback_policy),
            lens_contract=contract,
            putback_policy=putback_policy,
            source_authority_policy={
                "allowed_authorities": [AUTHORITY],
                "commit_modes": ["overlay"],
            },
            lens_promotion_mapping=mapping,
        )
        register_lens_definition(conn, lens)
        conn.commit()
        note(f"view {view.view_id} published at token {token}; lens {lens.lens_id} registered")

        # Disclosed gap: no product path creates the lens producer profile.
        materialize_producer_profile(
            conn,
            deployment_name=name,
            producer_id=lens_producer_id(lens.definition_id),
            producer_kind="lens",
            trust_label="derived-deterministic-trusted",
            admission_mode="review",
            authority_scope={
                "lens_definition_id": lens.definition_id,
                "mapping_checksum": lens_promotion_mapping_checksum(mapping),
            },
            allowed_record_kinds=("node",),
            output_signature={"nodes": [{"subtype": subtype, "clause_id": f"lens.{subtype}.{FIELD}"}]},
            policy_clauses=[{
                "id": f"lens.{subtype}.{FIELD}",
                "effect": "allow",
                "record_kind": "node",
                "subtype": subtype,
            }],
            output_scope_summary={},
            source_ref={"materialized_by": "scripts/akmon/claim_lens_round_trip.py"},
            catalog_version_id=None,
        )
        conn.commit()
        note(
            "producer profile for the lens created by this script with "
            "materialize_producer_profile (no product command does this yet)"
        )

        def delta(expected: str) -> LensPutbackDelta:
            return LensPutbackDelta(
                slot_name=SLOT,
                scope_key=SCOPE,
                row_key=target,
                field_path=(FIELD,),
                expected_value=expected,
                new_value=edited,
                source_authority=AUTHORITY,
                source_record_id={"node_id": target},
                expected_source_revision={"revision": str(token)},
            )

        # Stale edit: refused, no overlay.
        stale = prepare_lens_putback(
            conn, lens,
            base_view_manifest_id=published_view.manifest_id,
            target_generation_id=token,
            actor_id="reviewer",
            idempotency_key=f"akmon-reviewer-stale-{suffix}",
            deltas=[delta(args.stale_value)],
        )
        stale_refused = False
        try:
            commit_lens_overlay(conn, request_id=stale.request_id)
        except LensPutbackStateError:
            stale_refused = True
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.lens_overlay_log WHERE request_id = %s",
                (str(stale.request_id),),
            )
            stale_overlays = int(cur.fetchone()[0])
        conn.commit()
        note(
            f"stale edit (expected {args.stale_value!r}): status={stale.status} "
            f"conflicts={stale.conflict_count} commit_refused={stale_refused} overlays={stale_overlays}"
        )
        if stale.status != "conflict" or not stale_refused or stale_overlays != 0:
            fail("lens-round-trip", "stale edit was not refused",
                 status=stale.status, commit_refused=stale_refused, overlays=stale_overlays)

        # Fresh edit: committed as an overlay and read back through the view.
        prepared = prepare_lens_putback(
            conn, lens,
            base_view_manifest_id=published_view.manifest_id,
            target_generation_id=token,
            actor_id="reviewer",
            idempotency_key=f"akmon-reviewer-edit-{suffix}",
            deltas=[delta(original)],
        )
        committed = commit_lens_overlay(conn, request_id=prepared.request_id)
        conn.commit()
        view_after_edit = _view_rows(conn, name=name, view_id=view.view_id, token=token)
        overlay_value = _overlay_value(view_after_edit.get(target, {}))
        note(
            f"edit: status={committed.status}; view reads {target} "
            f"title overlay={overlay_value!r}"
        )
        if committed.status != "committed_overlay" or overlay_value != edited:
            fail("lens-round-trip", "the view does not read back the committed edit",
                 status=committed.status, overlay_value=overlay_value)

    promoted = run_okg("promote", [
        "admission", "decide", "--deployment", name, "--dsn", dsn,
        "--action", "promote", "--request-id", str(prepared.request_id),
        "--actor", "reviewer", "--json",
    ], env=env)
    admission_id = promoted.get("admission_id")
    if not admission_id:
        fail("promote", "no promotion admission id returned", output=promoted)
    # The operator's promote decision is the review step: it opens a lens
    # admission envelope carrying the planned node mutation, and the next
    # publisher cycle applies it (the generic `--decision accept` path is
    # for payload submissions and refuses a lens envelope).
    note(f"promotion envelope {admission_id} status={promoted.get('status')}")
    cycle = run_okg("publish", [
        "run", "--once", "--deployment", name, "--dsn", dsn, "--apply", "--json",
    ], env=env)
    generation = cycle.get("generation_id")
    if cycle.get("status") != "published" or not generation:
        fail("publish", "publisher cycle did not publish", status=cycle.get("status"))
    note(f"publisher cycle -> {generation}")

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, target_generation_ref FROM okg.admission_envelopes "
                "WHERE admission_id = %s::uuid",
                (str(admission_id),),
            )
            envelope = cur.fetchone()
            cur.execute(
                "SELECT status FROM okg.lens_overlay_log WHERE request_id = %s",
                (str(prepared.request_id),),
            )
            overlay_status = [row[0] for row in cur.fetchall()]
        rows_after = _record_rows(store.checkout_generation(generation), subtype)
        graph_title = {row["node_id"]: row["title"] for row in rows_after}.get(target)
    note(f"envelope status={envelope and envelope[0]} generation_ref={envelope and envelope[1]}")
    note(f"overlay status={overlay_status}")
    note(f"graph at {generation}: {target} title={graph_title!r}")
    if not envelope or envelope[0] != "published" or envelope[1] != generation:
        fail("lens-round-trip", "promotion envelope was not published in this generation",
             envelope=list(envelope or []), generation=generation)
    if graph_title != edited:
        fail("lens-round-trip", "published graph does not carry the edit",
             title=graph_title, expected=edited)

    passed(
        "CLAIM lens-round-trip",
        putget="ok",
        stale="refused",
        promoted=generation,
        producer_profile="materialized_by_script",
    )
    return 0


if __name__ == "__main__":
    run_main("lens-round-trip", main)
