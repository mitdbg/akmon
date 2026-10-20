"""Does Nomos read masking cover the `query` tool's output? (PACT mcp-http-auth)

The umbrella's open item (design.md section 7 Plane 2). The answer decides
whether chat-sync-projection's exclusion of `query` from presets on
masking/PII-posture deployments stands or may be lifted.

The answer is TWO-dimensional -- enforcement mode x whether the SQL is
graph-backed -- because the tri-state branch at run_sql.py:298-324 guards only
graph-backed SQL. Operational ``okg.v_*`` relations (run_sql.py:292-295) never
enter that branch, in any mode.

Two rules govern how a result may be recorded, and they are the whole reason
this file asserts diffs rather than booleans:

* A REFUSAL IS NOT COVERAGE. "graph-backed SQL was refused under enforce" is
  evidence that query cannot return those rows, NOT evidence that masking was
  applied to rows it returned. Recording it as coverage is the
  absence-dressed-as-agreement defect. The refusal arm therefore asserts the
  refusal and deliberately produces NO diff.
* Only masking OBSERVED APPLIED to RETURNED rows is lift-eligible. Every other
  outcome leaves the exclusion standing for that cell.

Fixture shape follows the proven one in test_read_masking_subtype_class.py: a
composed catalog whose ``person`` subtype declares pii_classes, a policy bundle
that redacts for ``subject:unprivileged``, and the same node read twice -- once
through the masking-covered operator path
(read_filter.filter_node_rows_for_session -> read_masking.mask_attrs) and once
through the query path (run_sql).

RUNNING THIS FILE: the four measured arms need the shared Postgres runtime
fixture and must be run serially -- concurrent runs against okg_test corrupt
chronos branch state.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog, load_modules_root
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.mcp.run_sql import (
    _references_physical_graph_view,
    _rewrite_graph_views,
    run_sql,
)
from okg.substrate.mcp.sql_safety import RunSqlInvalid
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_filter import filter_node_rows_for_session
from okg.substrate.nomos.read_masking import MASK_TOKEN
from tests.substrate._db_helpers import ensure_published_generation


postgres = pytest.mark.postgres

_NODE_ID = "node:person-masking-probe"
_GRAPH_SQL = f"SELECT node_id, attrs FROM okg.v_nodes WHERE node_id = '{_NODE_ID}'"
_OPERATIONAL_SQL = "SELECT * FROM okg.v_source_run_diagnostics LIMIT 1"
_RAW_ATTRS = {
    "classification": "internal",
    "email": "alice@example.com",
    "display_name": "Alice",
    "secret_note": "confidential",
}


def _attr_diff(operator_attrs: dict, query_attrs: dict) -> dict[str, tuple]:
    """Key-by-key difference between the two row shapes.

    Returns ``{key: (operator_value, query_value)}`` for every key whose value
    differs or that only one side carries. An EMPTY diff means the two paths
    returned the same attributes -- which, for a subject the operator path
    redacts, is exactly the finding that query is not masked... only if the
    operator side is actually masked, which each arm asserts separately. An
    empty diff is never read as agreement on its own.
    """
    missing = object()
    diff: dict[str, tuple] = {}
    for key in sorted(set(operator_attrs) | set(query_attrs)):
        left = operator_attrs.get(key, missing)
        right = query_attrs.get(key, missing)
        if left != right:
            diff[key] = (
                None if left is missing else left,
                None if right is missing else right,
            )
    return diff


# ── hermetic: which SQL shapes reach the tri-state branch at all ──────


def test_branch_predicate_separates_graph_backed_from_operational_sql() -> None:
    """The (mode x SQL-shape) grid's second axis, measured on the real predicate.

    The tri-state Nomos check in run_sql runs only when the submitted SQL is
    graph-backed. This is the hermetic half of the fourth arm: it establishes
    that an operational okg.v_* relation does not enter the branch in ANY mode,
    which is why a separate measured arm is needed for it rather than assuming
    the enforce refusal covers everything.
    """
    graph_rewritten = _rewrite_graph_views(_GRAPH_SQL)
    assert (
        graph_rewritten != _GRAPH_SQL
        or _references_physical_graph_view(graph_rewritten)
    ), "graph-backed SQL must enter the Nomos branch"

    operational_rewritten = _rewrite_graph_views(_OPERATIONAL_SQL)
    assert operational_rewritten == _OPERATIONAL_SQL
    assert not _references_physical_graph_view(operational_rewritten), (
        "an operational okg.v_* relation must NOT enter the Nomos branch -- if "
        "this fails, the fourth measured arm's premise has changed"
    )


# ── fixture ──────────────────────────────────────────────────────────


def _activate_redact_policy(dsn: str) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:query-masking-coverage",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "redact_unprivileged",
                "effect": "redact",
                "actions": ["graph.read.mcp"],
                "subjects": ["subject:unprivileged"],
                "match": {
                    "deployment": "default",
                    "scope": "deployment:default",
                    "source_id": "pii_fixture",
                },
                "priority": 100,
                "reason_code": "nomos.test.redact_unprivileged",
                "obligations": [{"type": "redaction"}, {"type": "audit"}],
            },
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope="deployment:default",
            activated_by="test:operator",
        )
        conn.commit()


def _row() -> dict:
    return {
        "node_id": _NODE_ID,
        "subtype": "person",
        "attrs": dict(_RAW_ATTRS),
        "source": "pii_fixture",
        "source_record_id": {"id": "probe"},
        "source_revision": {"updated_at": "2026-05-01T00:00:00Z"},
    }


@pytest.fixture
def masking_deployment(clean_runtime: str, tmp_path: Path) -> str:
    """A deployment whose `person` subtype declares a masking pii_class."""
    deployment = tmp_path / "masking_probe"
    schemas = deployment / "schemas"
    schemas.mkdir(parents=True)
    (deployment / "deployment.yaml").write_text(
        "name: default\nschema_dir: schemas\nmodules:\n- person\n"
    )
    (schemas / "nodes.yaml").write_text("classes: {}\n")
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    composed = compose_catalog(
        deployment,
        modules_root=load_modules_root(),
        cross_links_root=cross_links,
    )
    assert composed.subtype_metadata["person"]["pii_classes"]["email"] == (
        "direct_identifier"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
        legacy_generation_id = ensure_published_generation(
            conn, catalog_version_id=cv, deployment_name="default"
        )
    # Seed through the graph store, NOT a raw INSERT into okg.graph_nodes:
    # under Chronos that base table is empty and the live rows come from the
    # branch tables, so a direct insert would leave okg.v_nodes without the
    # probe node and the query arms would measure an empty result set.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=True)
    try:
        store.apply_facts(
            [
                NodeFact(
                    _NODE_ID,
                    "person",
                    dict(_RAW_ATTRS),
                    {"id": "probe"},
                    {"updated_at": "2026-05-01T00:00:00Z"},
                )
            ],
            branch="default",
            source="pii_fixture",
        )
        # Stamp the compat pin on THIS checkpoint, not the one
        # ensure_published_generation made. The session resolves the LATEST
        # okg_generation checkpoint, which is this one; without
        # `target_generation_id` here the session carries
        # compat_generation_id=None and run_sql's operational path refuses,
        # because okg.current_pin() needs a legacy integer that a Chronos
        # string generation id cannot supply. Stamping the earlier checkpoint
        # instead has no effect -- this one supersedes it.
        # Name the catalog version this fixture applied. A checkpoint without
        # one is refused on every session read with GenerationPinInvalid
        # (issue #2248), before any masking arm runs.
        store.checkpoint_generation(
            deployment_name="default",
            branch="default",
            catalog_version_id=cv,
            metadata={"target_generation_id": legacy_generation_id},
        )
    finally:
        store.close()
    _activate_redact_policy(clean_runtime)
    return clean_runtime


def _operator_attrs(conn, session) -> dict:
    filtered = filter_node_rows_for_session(
        conn, session, [_row()], subject="subject:unprivileged"
    )
    return dict(filtered[0]["attrs"])


def _query_attrs(conn, session) -> dict:
    """Row attrs as the `query` tool returns them, or raise RunSqlInvalid."""
    result = run_sql(conn, session, _GRAPH_SQL)
    assert result["rows"], "the probe node must be visible to the query path"
    columns = list(result["columns"])
    return dict(result["rows"][0][columns.index("attrs")])


# ── measured arms (needs the shared Postgres runtime; run serially) ───


@postgres
def test_operator_path_redacts_declared_pii_attrs(
    masking_deployment: str, monkeypatch
) -> None:
    """Baseline: the masking-covered path really does mask this node.

    Without this the other arms are unmeasurable -- if the operator path did
    not mask, an identical query result would prove nothing about coverage.
    """
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(masking_deployment) as conn:
        session = open_session(conn, deployment_name="default")
        attrs = _operator_attrs(conn, session)
    assert attrs["email"] == MASK_TOKEN
    assert attrs["display_name"] == MASK_TOKEN
    assert attrs["secret_note"] == MASK_TOKEN
    assert attrs.get("nomos_redacted") is True
    assert _RAW_ATTRS["email"] not in str(attrs)


@postgres
def test_query_path_under_enforce_is_refused_not_masked(
    masking_deployment: str, monkeypatch
) -> None:
    """Enforce REFUSES graph-backed SQL. Recorded as a refusal, never coverage.

    This arm deliberately produces no attribute diff: there are no returned
    rows to diff. "Under enforce query cannot leak, so lift the exclusion" is
    the refusal-is-not-coverage error restated and is inadmissible.
    """
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    with psycopg.connect(masking_deployment) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(RunSqlInvalid) as exc:
            run_sql(conn, session, _GRAPH_SQL)
    assert "source-lineage filtered" in str(exc.value)
    # Recorded outcome for the (enforce, graph-backed) cell: REFUSED.
    # Consequence: the chat-sync-projection exclusion STANDS for this cell.


@postgres
def test_query_path_under_audit_is_recorded_and_measured(
    masking_deployment: str, monkeypatch
) -> None:
    """Audit records a bypass and ALLOWS. Measure what the allowed rows carry."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    with psycopg.connect(masking_deployment) as conn:
        session = open_session(conn, deployment_name="default")
        operator_attrs = _operator_attrs(conn, session)
        query_attrs = _query_attrs(conn, session)
        with conn.cursor() as cur:
            # The audit-mode bypass lands in okg.nomos_decisions under the
            # run_sql_graph_backed target (see run_sql._record_run_sql_audit_bypass
            # and tests/substrate/nomos/test_nomos_audit_bypass_persist.py).
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.nomos_decisions
                 WHERE deployment_name = %s
                   AND target->>'kind' = 'run_sql_graph_backed'
                """,
                ("default",),
            )
            bypasses = cur.fetchone()[0]

    diff = _attr_diff(operator_attrs, query_attrs)
    # The operator side must actually be masked, or the comparison is void.
    assert operator_attrs["email"] == MASK_TOKEN
    # The measured finding, as an attribute-level diff rather than a boolean.
    assert diff["email"] == (MASK_TOKEN, _RAW_ATTRS["email"]), diff
    assert query_attrs["email"] == _RAW_ATTRS["email"]
    assert "nomos_redacted" not in query_attrs
    assert bypasses >= 1, "audit mode must record the bypass observation"
    # Recorded outcome for the (audit, graph-backed) cell: masking NOT applied
    # to returned rows. NOT lift-eligible; the exclusion STANDS.


@postgres
def test_query_path_under_off_is_measured(
    masking_deployment: str, monkeypatch
) -> None:
    """Off skips the check entirely. Measure the returned rows anyway."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with psycopg.connect(masking_deployment) as conn:
        session = open_session(conn, deployment_name="default")
        query_attrs = _query_attrs(conn, session)
    # With enforcement off the operator path does not mask either, so the
    # comparison that matters is against the RAW attrs: query returns them.
    diff = _attr_diff(dict(_RAW_ATTRS), query_attrs)
    # Every declared-PII attribute comes back verbatim -- no key masked, none
    # dropped, no redaction marker.
    for key, raw_value in _RAW_ATTRS.items():
        assert query_attrs[key] == raw_value, (key, diff)
    assert "nomos_redacted" not in query_attrs
    # The ONLY difference is the at-rest source-lineage stamp, a governance
    # key (read_masking.is_governance_key) that the masking transform never
    # touches in either path. Asserting the diff's exact key set keeps this a
    # measurement rather than a subset check that would hide a masked value.
    assert set(diff) <= {"nomos_source_lineage"}, diff
    assert query_attrs["email"] == _RAW_ATTRS["email"]
    # Recorded outcome for the (off, graph-backed) cell: no masking applied.
    # NOT lift-eligible; the exclusion STANDS.


@postgres
def test_non_graph_backed_sql_bypasses_nomos_in_every_mode(
    masking_deployment: str, monkeypatch
) -> None:
    """The fourth arm: an operational okg.v_* relation under ENFORCE.

    The tri-state branch guards only graph-backed SQL, so this SQL is neither
    refused nor audited in any mode. Record what it returns rather than
    assuming the enforce refusal covers the whole query surface.
    """
    for mode in ("enforce", "audit", "off"):
        monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
        with psycopg.connect(masking_deployment) as conn:
            session = open_session(conn, deployment_name="default")
            result = run_sql(conn, session, _OPERATIONAL_SQL)
        # Not refused in ANY mode, enforce included: the query executed and
        # returned a real column list off the operational view.
        assert "rows" in result, mode
        assert isinstance(result["columns"], list), mode
        assert "deployment" in result["columns"], (mode, result["columns"])
    # Recorded outcome for the (any mode, operational SQL) cell: the tri-state
    # Nomos check is BYPASSED -- enforce does not refuse this shape.
    #
    # HONEST LIMIT of this arm: the fixture's diagnostics view holds no rows,
    # so this measures refused-vs-allowed only. It does NOT observe masking
    # applied to returned operational rows, and an empty result set must not
    # be read as "masking was applied" -- that is the absence-dressed-as-
    # agreement defect. Either way the cell is NOT lift-eligible, because
    # lift-eligible requires masking OBSERVED APPLIED to returned rows.
