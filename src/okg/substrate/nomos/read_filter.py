"""Nomos source-lineage filters for read-side graph rows."""

from __future__ import annotations

import getpass
import hashlib
import json
import os
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

import psycopg

from okg.substrate.attributes import apply_effective_attrs_to_rows
from okg.substrate.commit_scope import read_pass_scope
from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.core import _PROTECTED_IDENTITY_FACTS, DecisionInput, PolicyDecision
from okg.substrate.nomos.identity_store import batched_decision_persist
from okg.substrate.nomos.runtime_enforcement import (
    SURFACE_DECISION_MAPPINGS,
    _is_json_compatible,
    deployment_requires_nomos_runtime,
    surface_for_action,
)
from okg.substrate.nomos.source_lineage import (
    RECORD_DETAIL_KEY,
    RECORD_DETAIL_TRUNCATED,
    attrs_with_lineage,
    graph_object_ref,
    horizon_ref,
    lineage_for_row,
    named_set_ref,
    source_record_contributors_from_lineage,
    source_record_refs_from_lineage,
    source_ids_from_lineage,
)
from okg.substrate.nomos.pii_classification import resolve_pii_class
from okg.substrate.nomos.read_masking import (
    READ_DROP_CLASSES,
    REDACTION_MARKER,
    REVEAL_CLASSES,
    ReadRedactedAttrs,
    mask_attrs,
    safe_decision_payload,
    subtype_attr_classifier,
)
from okg.substrate.nomos import store as nomos_store
from okg.substrate.nomos.read_class import (
    ReadClassMismatch,
    stored_read_class,
    verify_stored_classes,
)
from okg.substrate.nomos.store import evaluate_with_active_policy, load_active_policy_bundle
from okg.substrate.nomos.vocabulary import PII_CLASSES

_HIDE_EFFECTS = {"deny", "review", "quarantine"}
#: The decision-context fact a counting pass carries (value ``"count"``). A
#: policy may name it to treat counts differently from reads.
READ_PURPOSE_FACT = "nomos_read_purpose"
#: Row fields that hold a digest of the record's stored ``text`` (an unsalted
#: MD5). A redacted row keeps them only when its ``text`` attribute is served
#: exactly as stored; otherwise a reader could confirm a guessed text offline.
TEXT_DIGEST_FIELDS: tuple[str, ...] = ("text_hash", "raw_text_hash", "dest_text_hash")


def _load_subtype_pii_classes(
    conn: psycopg.Connection, catalog_version_id: Any
) -> dict[str, dict[str, Any]]:
    """Load ``{subtype: {attr: pii_class}}`` from ``okg.subtypes.metadata`` for
    one catalog version -- PACT ``read-masking-from-catalog-pii-class``.

    Read ONCE per filter pass (not per row), so the redact transform can classify
    a node row's attrs by its subtype's declared pii_class. Only a subtype whose
    metadata omits ``pii_classes`` is absent from the map and falls back to the
    legacy behavior. A present empty map is retained as a fail-safe opt-in;
    malformed present metadata fails closed rather than weakening masking.
    """
    if catalog_version_id is None:
        return {}
    from psycopg.rows import dict_row

    out: dict[str, dict[str, Any]] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT name, metadata FROM okg.subtypes "
            "WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        for record in cur.fetchall():
            metadata = record.get("metadata")
            if not isinstance(metadata, Mapping):
                raise SubstrateError(
                    "catalog subtype metadata must be a mapping",
                    data={"subtype": str(record["name"])},
                )
            if "pii_classes" not in metadata:
                continue
            declared = metadata["pii_classes"]
            if not isinstance(declared, Mapping):
                raise SubstrateError(
                    "catalog subtype pii_classes must be a mapping",
                    data={"subtype": str(record["name"])},
                )
            normalized: dict[str, str] = {}
            for attr_name, pii_class in declared.items():
                if (
                    not isinstance(attr_name, str)
                    or not isinstance(pii_class, str)
                    or pii_class not in PII_CLASSES
                ):
                    raise SubstrateError(
                        "catalog subtype pii_classes contains an invalid declaration",
                        data={"subtype": str(record["name"])},
                    )
                normalized[attr_name] = pii_class
            out[str(record["name"])] = normalized
    return out


def read_attr_classifier_for_row(
    conn: psycopg.Connection,
    session: Any,
    row: Mapping[str, Any],
) -> Any:
    """The attribute classifier the read filter masks ``row`` with.

    The same lookup ``_filter_rows_evaluate`` makes when a redact decision
    fires: the row's subtype (``""`` for an edge row, which carries none)
    selects its declared ``pii_classes`` from the session's catalog, and an
    unannotated subtype falls back to the legacy classifier. A caller that
    returns values the filter did not itself mask -- attribute observation
    history (issue #3189) -- masks them with this, so a value reads the same
    through either path.
    """
    pii_classes = _load_subtype_pii_classes(
        conn, getattr(session, "catalog_version_id", None)
    )
    return subtype_attr_classifier(pii_classes.get(str(row.get("subtype") or "")))


def _assert_multi_user_subject(session: Any, subject: str | None) -> None:
    """Refuse a multi-user read that carries no request-scoped subject.

    PACT ``mcp-principal-mapping``. Called at the PUBLIC entries as well as
    inside the evaluation, because the entries return rows UNFILTERED when
    ``deployment_requires_nomos_runtime`` is false -- before the evaluation
    (and its guard) is ever reached. A deployment whose enforcement mode is
    flipped off under a running multi-user server would otherwise serve every
    row to a caller nobody identified. ``build_server`` already refuses that
    combination at start; this is the second lock on the same door.
    """
    if subject is not None:
        return
    if getattr(session, "nomos_subject", None) is not None:
        return
    if not getattr(session, "nomos_multi_user", False):
        return
    from okg.substrate.mcp.principal_mapping import unidentified_principal_error

    raise unidentified_principal_error(
        "a multi-user read reached the Nomos read filter with no "
        "request-scoped subject; refusing rather than falling back to the "
        "process environment subject",
        channel="read_filter_no_request_subject",
        data={"deployment_name": getattr(session, "deployment_name", None)},
    )


def nomos_subject_from_env(*, default_kind: str = "mcp_client") -> str:
    raw = (
        os.getenv("OKG_NOMOS_SUBJECT")
        or os.getenv("OKG_MCP_NOMOS_SUBJECT")
        or os.getenv("OKG_PRINCIPAL")
        or os.getenv("USER")
        or getpass.getuser()
        or default_kind
    )
    subject = str(raw).strip()
    return subject if subject.startswith("subject:") else f"subject:{subject}"


_DEFAULT_READ_ACTION = "graph.read.mcp"
_DEFAULT_READ_SURFACE = "mcp"


def session_read_identity(
    session: Any,
    *,
    action: str | None,
    surface: str | None,
) -> tuple[str, str]:
    """The (action, surface) a filter pass records its decisions under.

    An explicit argument wins. Otherwise the session names the surface its reads
    come through (PACT operator-ui-search-view), and a session that names none
    keeps the historical MCP identity, so no existing caller's decisions change.
    """
    resolved_action = (
        action
        or getattr(session, "nomos_read_action", None)
        or _DEFAULT_READ_ACTION
    )
    resolved_surface = (
        surface
        or getattr(session, "nomos_read_surface", None)
        or _DEFAULT_READ_SURFACE
    )
    return str(resolved_action), str(resolved_surface)


def filter_node_rows_for_session(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    action: str | None = None,
    subject: str | None = None,
    identity_claim: Mapping[str, Any] | None = None,
    audit: bool = False,
    surface: str | None = None,
    runtime_required: bool | None = None,
    count: bool = False,
) -> list[dict[str, Any]]:
    """Apply the session's Nomos read policy to node rows.

    ``count=True`` is for rows the caller scans to decide a page fact and
    does not return (issue #3208): the decisions are recorded as counts (a
    ``_count`` target kind and ``nomos_read_purpose: "count"``), as in
    :func:`visible_ids_by_decision_class`, and the rows are still redacted
    so the caller can test them on read values.

    ``runtime_required`` is the caller's own reading of
    :func:`deployment_requires_nomos_runtime` for this call, passed so one
    call decides its SQL narrowing and this filter from ONE resolution of
    the mode; None resolves it here.
    """
    action, surface = session_read_identity(session, action=action, surface=surface)
    rows = [dict(row) for row in rows]
    _assert_multi_user_subject(session, subject)
    if runtime_required is None:
        runtime_required = deployment_requires_nomos_runtime(session.deployment_name)
    if not runtime_required:
        return rows
    return _filter_rows(
        conn,
        session,
        _enriched_node_rows(conn, session, rows),
        action=action,
        subject=subject,
        identity_claim=identity_claim,
        audit=audit,
        surface=surface,
        row_kind="node",
        id_key="node_id",
        count=count,
    )


def filter_edge_rows_for_session(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    action: str | None = None,
    subject: str | None = None,
    identity_claim: Mapping[str, Any] | None = None,
    audit: bool = False,
    surface: str | None = None,
    runtime_required: bool | None = None,
    count: bool = False,
) -> list[dict[str, Any]]:
    """Apply the session's Nomos read policy to edge rows.

    ``count=True`` is for rows the caller scans to decide a page fact and
    does not return (issue #3208): the decisions are recorded as counts (a
    ``_count`` target kind and ``nomos_read_purpose: "count"``), as in
    :func:`visible_ids_by_decision_class`, and the rows are still redacted
    so the caller can test them on read values.

    ``runtime_required`` is the caller's own reading of
    :func:`deployment_requires_nomos_runtime` for this call, passed so one
    call decides its SQL narrowing and this filter from ONE resolution of
    the mode; None resolves it here.
    """
    action, surface = session_read_identity(session, action=action, surface=surface)
    rows = [dict(row) for row in rows]
    _assert_multi_user_subject(session, subject)
    if runtime_required is None:
        runtime_required = deployment_requires_nomos_runtime(session.deployment_name)
    if not runtime_required:
        return rows
    return _filter_rows(
        conn,
        session,
        _enriched_edge_rows(conn, session, rows),
        action=action,
        subject=subject,
        identity_claim=identity_claim,
        audit=audit,
        surface=surface,
        row_kind="edge",
        id_key="edge_id",
        count=count,
    )


def visible_ids_by_decision_class(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    row_kind: str,
    action: str | None = None,
    surface: str | None = None,
) -> set[str]:
    """The ids of the rows the reader may see, deciding one row per decision class.

    The answer equals ``{row id for row in filter_<kind>_rows_for_session(...)}``
    for the same rows, session, action and surface; it is cheaper when many rows
    share a decision class.

    Why it is exact. The per-row filter builds each row's policy facts (its
    sources, classification, lineage, graph reference) and evaluates them; the
    subject, action, bundle and context are the same for every row of one call.
    A rule can only read the facts its ``match`` names, and the protected
    identity check reads only ``_PROTECTED_IDENTITY_FACTS`` and the
    classification. The boundary sanitizes each fact by its own key and value
    before matching, so two rows whose raw facts agree on those keys -- in
    every evaluation the per-row filter would make for them, in the same order,
    with the same JSON-shape verdict and fail-closed flag -- get the same
    decisions. Rows are grouped by that projection, ONE row per group goes
    through the real per-row filter, and its answer is the group's answer. A
    rule that matches a row-specific fact (a record id, a graph reference, a
    source record) makes every row its own group: still exact, only no faster.
    The identity-claim and access-context paths evaluate facts outside this
    projection, so a session carrying either is filtered row by row.

    Audit semantics. This is for rows the caller COUNTS and does not return.
    Each class is represented by its first row in (id, content) order, so the
    choice does not depend on the order the caller passed. Only representatives
    get recorded policy decisions, under their own ids, and those decisions are
    marked as counts in two ways: the target kind ends in ``_count`` (for
    example ``local_operator_node_source_count``) where a read's ends in
    ``_read``, and the decision context carries ``READ_PURPOSE_FACT``
    (``nomos_read_purpose: "count"``). The context fact gives a count decision
    its own decision identity, so the insert-once trail keeps it beside a read
    decision for the same row instead of dropping one of the two. The other
    members of a class get no decision row of their own; they are decided by
    their representative's answer. A caller that returns rows to the reader
    must pass those rows through ``filter_node_rows_for_session`` /
    ``filter_edge_rows_for_session``, which records a ``_read`` decision for
    every returned row and applies redaction.

    Because a policy may name ``nomos_read_purpose``, the exact-answer claim
    above is for policies that do not; one that does is obeyed, and may then
    count differently from what it lets a reader read.
    Without the Nomos runtime nothing is hidden and every id is returned.

    No active bundle (PACT lane-h-nomos-a-part1, decision 123a). Every row
    then gets the evaluator's ``nomos.default_deny.no_active_bundle``, which
    audit mode allows and enforce mode applies, so the call records ONE count
    decision for the whole call (``_decide_no_active_bundle``) and returns the
    id of every row with source lineage under audit, none under enforce. No
    row is projected or evaluated, and no per-row decision is written.
    """
    if row_kind not in ("node", "edge"):
        raise ValueError(f"row_kind must be 'node' or 'edge', not {row_kind!r}")
    id_key = f"{row_kind}_id"
    action, surface = session_read_identity(session, action=action, surface=surface)
    rows = [dict(row) for row in rows]
    _assert_multi_user_subject(session, None)
    if not deployment_requires_nomos_runtime(session.deployment_name):
        return {str(row.get(id_key)) for row in rows}
    exact = filter_node_rows_for_session if row_kind == "node" else filter_edge_rows_for_session
    if (
        getattr(session, "nomos_identity_claim", None) is not None
        or getattr(session, "nomos_access_context", None)
    ):
        return {
            str(row.get(id_key))
            for row in exact(conn, session, rows, action=action, surface=surface)
        }
    enriched = (
        _enriched_node_rows(conn, session, rows)
        if row_kind == "node"
        else _enriched_edge_rows(conn, session, rows)
    )
    scope = f"deployment:{session.deployment_name}"
    # One pass scope around the bundle read and the representatives'
    # evaluation, so both see the same active bundle.
    with read_pass_scope():
        bundle = _load_read_bundle(conn, session, scope=scope)
        if bundle is None:
            # No active bundle (PACT lane-h-nomos-a-part1, decision 123a):
            # every row with lineage gets the same answer, so ONE decision
            # decides the request and no row is projected or evaluated.
            ids = [str(row.get(id_key)) for row in enriched if _row_source_ids(row)]
            if ids and _decide_no_active_bundle(
                conn, session, row_kind=row_kind, action=action, surface=surface,
                audit=False, count=True, rows_decided=len(ids),
            ):
                return set(ids)
            return set()
        keys = _decision_class_keys(bundle)
        members: dict[Any, list[str]] = {}
        representatives: list[dict[str, Any]] = []
        # Deterministic representatives: each class is represented by its
        # first row in (id, content) order, whatever order the caller passed.
        order_key = _stable_order_key(enriched)
        for position, index in enumerate(sorted(range(len(enriched)), key=order_key)):
            row = enriched[index]
            key = _decision_class_key(
                session, row, row_kind=row_kind, id_key=id_key, scope=scope,
                surface=surface, keys=keys, position=position,
            )
            if key is None:
                # No source lineage: the per-row filter hides it without a decision.
                continue
            if key not in members:
                members[key] = []
                # Tagged with its group's position: two rows may share an id.
                representatives.append({**row, _CLASS_TAG: len(representatives)})
            members[key].append(str(row.get(id_key)))
        groups = list(members.values())
        visible: set[str] = set()
        for row in _filter_rows(
            conn,
            session,
            representatives,
            action=action,
            subject=None,
            identity_claim=None,
            audit=False,
            surface=surface,
            row_kind=row_kind,
            id_key=id_key,
            count=True,
        ):
            visible.update(groups[row[_CLASS_TAG]])
    return visible


#: The key a representative row carries naming its decision class. The filter
#: copies unknown row keys through without reading them.
_CLASS_TAG = "_okg_decision_class"


# --------------------------------------------------------------------------- #
# No active bundle: one decision per request (PACT lane-h-nomos-a-part1)       #
# --------------------------------------------------------------------------- #

def _load_read_bundle(conn: psycopg.Connection, session: Any, *, scope: str) -> Any:
    """The active bundle for one read, or None. Call it inside the caller's
    ``read_pass_scope`` so every later evaluation of the read sees this bundle."""
    return load_active_policy_bundle(
        conn, deployment_name=session.deployment_name, scope=scope,
    )


def _row_source_ids(row: Mapping[str, Any]) -> tuple[str, ...]:
    """The row's source ids, as the per-row filter reads them; empty means the
    per-row filter hides the row without a decision."""
    return source_ids_from_lineage(_row_lineage(row))


def _row_lineage(row: Mapping[str, Any]) -> dict[str, Any]:
    return lineage_for_row(
        dict(row.get("attrs") or {}),
        source_id=row.get("source"),
        source_record_id=row.get("source_record_id"),
        source_revision=row.get("source_revision"),
    )


def _lineage_stamped_attrs(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """The attrs the per-row filter returns for a visible row before masking,
    or None when the row has no source lineage (the per-row filter hides it)."""
    lineage = _row_lineage(row)
    if not source_ids_from_lineage(lineage):
        return None
    return attrs_with_lineage(dict(row.get("attrs") or {}), lineage)


def _decide_no_active_bundle(
    conn: psycopg.Connection,
    session: Any,
    *,
    row_kind: str,
    action: str,
    surface: str,
    audit: bool,
    count: bool,
    rows_decided: int,
) -> bool:
    """Record ONE decision for a read made while no policy bundle is active,
    and return whether the read's rows (those with source lineage) are visible.

    With no bundle the evaluator answers every row alike: the default deny
    ``nomos.default_deny.no_active_bundle``, which audit mode allows and
    enforce mode applies; no row fact can change it. So the per-row filter's
    answer for every row with lineage is the answer of one evaluation through
    the same boundary, at the same enforcement point, for the same subject and
    action.

    One decision per call, each its own row. The trail is insert-once by
    decision identity, so the identity carries the deployment's enforcement
    mode and a fresh per-call id (``NO_BUNDLE_REQUEST_FACT``) in the decision
    context: two calls never share a row, and a row's mode is the mode its
    rows were shown or hidden under. No read pass, session or MCP request
    carries a request id today, so the id is minted here. The rows decided and
    the publish position are audit metadata in the target.

    Call it inside the ``read_pass_scope`` that loaded the bundle as None.
    That scope memoizes the bundle, so the activation state is re-read from
    the database, uncached, just before the decision: a bundle activated since
    the call began refuses the read (``SubstrateError``) instead of answering
    it from a policy that is no longer the active one. An activation committed
    between that re-read and the decision insert is not seen. A connection in
    REPEATABLE READ (no MCP or Nomos read path uses one) does not see a
    mid-call activation at all; its answer stays consistent with its snapshot.
    """
    scope = f"deployment:{session.deployment_name}"
    verb = "count" if count else "read"
    mode = nomos_store.deployment_nomos_enforcement_mode(session.deployment_name)
    request_id = f"read-request:{uuid.uuid4().hex}"
    subject = (
        getattr(session, "nomos_subject", None)
        or nomos_subject_from_env(default_kind="mcp_client")
    )
    activated = _active_policy_fingerprint_now(
        conn, deployment_name=session.deployment_name, scope=scope,
    )
    if activated is not None:
        raise SubstrateError(
            "read filter: no policy bundle was active when the read began, but "
            "one is active now; refusing the read",
            data={"active_policy_fingerprint": str(activated)},
        )
    context: dict[str, Any] = {
        NO_BUNDLE_REQUEST_FACT: request_id,
        NO_BUNDLE_MODE_FACT: mode,
    }
    if count:
        context[READ_PURPOSE_FACT] = "count"
    target = {
        "kind": f"{surface}_{row_kind}_no_active_bundle_{verb}",
        "decided_by": "no_active_bundle",
        "read_request": request_id,
        "rows_decided": rows_decided,
        "enforcement_mode": mode,
        "generation_id": session.generation_id,
        "branch_name": session.branch_name,
        "catalog_version_id": session.catalog_version_id,
    }
    with read_pass_scope(), batched_decision_persist(conn):
        decision = evaluate_with_active_policy(
            conn,
            deployment_name=session.deployment_name,
            scope=scope,
            decision_input=DecisionInput(
                subject=subject,
                action=action,
                classification="internal",
                resource={
                    "deployment": session.deployment_name,
                    "scope": scope,
                    "kind": row_kind,
                    "audit": audit,
                },
                context=context,
            ),
            enforcement_point=f"{surface}.source_lineage_filter",
            target=target,
            performed_obligations=("audit", "redaction"),
        )
    conn.commit()
    return decision.effect not in _HIDE_EFFECTS


#: Decision-context facts of the one no-bundle decision: a per-call id and the
#: enforcement mode, so each call's decision is its own row in the
#: insert-once trail and names the mode its rows were decided under.
NO_BUNDLE_REQUEST_FACT = "nomos_read_request"
NO_BUNDLE_MODE_FACT = "nomos_read_enforcement_mode"


def _active_policy_fingerprint_now(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> str | None:
    """The active bundle's fingerprint read from the database now, bypassing
    the read pass memo (the same activation query as
    ``load_active_policy_bundle``, without loading the bundle)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.policy_fingerprint
              FROM okg.nomos_policy_activations a
              JOIN okg.nomos_policy_bundles b
                ON b.deployment_name = a.deployment_name
               AND b.policy_fingerprint = a.policy_fingerprint
             WHERE a.deployment_name = %s
               AND a.scope = %s
               AND a.status = 'active'
               AND b.status = 'active'
             ORDER BY a.activated_at DESC
             LIMIT 1
            """,
            (deployment_name, scope),
        )
        row = cur.fetchone()
    return None if row is None else str(row[0])


# --------------------------------------------------------------------------- #
# Read-class path: decide once per decision class, return every visible row   #
# (PACT lane-h-nomos-sql-read-filter, slice 0; Refs #3002)                     #
# --------------------------------------------------------------------------- #

#: Rule ``match`` keys a read may meet and still take the read-class path.
#: Each is either constant for one call (deployment, scope, kind, audit,
#: subject, action and the facts the boundary adds) or a fact the class key
#: always carries (source_ids, source_id, resource_kind, classification). An
#: active rule that can match the read and names any other key -- a row id, a
#: graph or source-record reference, the lineage, an access-context fact, a
#: key nobody knows yet -- sends the whole read down the per-row path. This is
#: an allow-list on purpose: a new resource key cannot silently join a class.
READ_CLASS_SAFE_RULE_KEYS = frozenset({
    "deployment",
    "scope",
    "kind",
    "audit",
    "subject",
    "action",
    "principal_kind",
    "nomos_surface",
    "nomos_boundary",
    READ_PURPOSE_FACT,
    "source_ids",
    "source_id",
    "resource_kind",
    "classification",
})

#: Facts every read-class key projects on, whether or not a rule names them,
#: so the decision recorded for a class names every member's sources: the
#: row's source set and each target's source id (each contributor's included).
_READ_CLASS_ALWAYS_KEYS = frozenset({"source_ids", "source_id"})

#: The key a row sent down the per-row path carries naming its position in the
#: caller's list, so its answer is merged back in order. Positions, never ids:
#: two rows may share an id.
_ROW_POSITION_TAG = "_okg_read_class_position"


class ReadClassPath:
    """How one ``read_class_filter`` call decided its rows (for tests and the
    benchmark; the returned rows do not depend on it)."""

    __slots__ = (
        "path", "reason", "classes", "class_rows", "per_row_rows",
        "stored_class_rows", "verified_rows",
    )

    def __init__(
        self,
        *,
        path: str,
        reason: str | None = None,
        classes: int = 0,
        class_rows: int = 0,
        per_row_rows: int = 0,
        stored_class_rows: int = 0,
        verified_rows: int = 0,
    ) -> None:
        self.path = path
        self.reason = reason
        self.classes = classes
        self.class_rows = class_rows
        self.per_row_rows = per_row_rows
        # Of ``class_rows``: rows keyed by their stored class (slice 1); the
        # rest were keyed by the read-time projection.
        self.stored_class_rows = stored_class_rows
        # Rows whose stored class the verification sample recomputed.
        self.verified_rows = verified_rows

    def __repr__(self) -> str:
        return (
            f"ReadClassPath(path={self.path!r}, reason={self.reason!r}, "
            f"classes={self.classes}, class_rows={self.class_rows}, "
            f"per_row_rows={self.per_row_rows}, "
            f"stored_class_rows={self.stored_class_rows}, "
            f"verified_rows={self.verified_rows})"
        )


def filter_rows_by_read_class(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    row_kind: str,
    action: str | None = None,
    subject: str | None = None,
    identity_claim: Mapping[str, Any] | None = None,
    audit: bool = False,
    surface: str | None = None,
) -> list[dict[str, Any]]:
    """The rows ``filter_<row_kind>_rows_for_session`` returns, decided per class.

    Same arguments, same rows in the same order, same attrs (redaction
    included). See ``read_class_filter``.
    """
    return read_class_filter(
        conn, session, rows, row_kind=row_kind, action=action, subject=subject,
        identity_claim=identity_claim, audit=audit, surface=surface,
    )[0]


def read_class_filter(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    row_kind: str,
    action: str | None = None,
    subject: str | None = None,
    identity_claim: Mapping[str, Any] | None = None,
    audit: bool = False,
    surface: str | None = None,
    verify_sample: int | None = None,
) -> tuple[list[dict[str, Any]], ReadClassPath]:
    """Filter returned rows by deciding one representative per decision class.

    The class key is the projection ``visible_ids_by_decision_class`` uses
    (``_decision_class_key``): the row's classification from its effective
    attrs, and for every evaluation the per-row filter would make for the row,
    in order, the facts the active rules and the protected-identity check can
    read, the JSON-shape verdict and the fail-closed flag. The source set is
    always part of the key. Rows with equal keys get equal decisions, so ONE
    representative per class goes through the real per-row evaluator
    (``_filter_rows``, recording ``_read`` decisions, not counts), and every
    other member follows its answer: hidden with it, or returned with the
    per-row filter's attrs (lineage stamped), masked with the member's own
    subtype classifier when the representative was redacted.

    Audit trail: each class writes its decisions under the representative's
    row id (insert-once, as every read decision), with the target tagged
    ``read_class``, ``class_member_count`` and ``decided_by: "read_class"``.
    Members get no decision row of their own.

    The whole call takes the per-row path when: the caller passes a subject or
    an identity claim, the session carries an identity claim or an access
    context, or an active rule that can match this read names a key outside
    ``READ_CLASS_SAFE_RULE_KEYS``. Rows with no source lineage, and rows whose
    facts cannot be written as canonical JSON, go through the per-row filter
    themselves. The keys and the representatives' decisions are computed inside
    one ``read_pass_scope`` (one active bundle); a representative decided under
    any other policy fingerprint refuses the read instead of deciding a class.

    With no active bundle (decision 123a) the call records ONE ``read``
    decision (``_decide_no_active_bundle``) and returns every row with source
    lineage, with the per-row filter's lineage-stamped attrs, under audit, and
    no row under enforce; the path is ``no_active_bundle``. The per-row
    refusals above (subject, identity claim, access context) are checked
    first and still win.

    Stored classes (PACT lane-h-nomos-read-class-stored): a row that carries a
    stored ``nomos_read_class`` whose ``nomos_read_class_version`` is the
    running ``CLASS_VOCABULARY_VERSION`` is keyed by (stored class, its source
    set, its effective classification) instead of the read-time projection;
    any other row (column absent from the caller's SELECT, NULL, an older
    version) is keyed by the projection, exactly as before. Every guard above
    applies to both. ``verify_sample`` (default: the
    ``OKG_NOMOS_READ_CLASS_VERIFY_SAMPLE`` environment value, else 0)
    recomputes the class of that many random stored-class rows from their own
    content and refuses the read (``SubstrateError``) on a mismatch.
    """
    if row_kind not in ("node", "edge"):
        raise ValueError(f"row_kind must be 'node' or 'edge', not {row_kind!r}")
    id_key = f"{row_kind}_id"
    action, surface = session_read_identity(session, action=action, surface=surface)
    rows = [dict(row) for row in rows]
    _assert_multi_user_subject(session, subject)
    if not deployment_requires_nomos_runtime(session.deployment_name):
        return rows, ReadClassPath(path="off")
    exact = filter_node_rows_for_session if row_kind == "node" else filter_edge_rows_for_session
    reason = _read_class_session_refusal(session, subject=subject, identity_claim=identity_claim)
    if reason is not None:
        return (
            exact(
                conn, session, rows, action=action, subject=subject,
                identity_claim=identity_claim, audit=audit, surface=surface,
            ),
            ReadClassPath(path="per_row", reason=reason, per_row_rows=len(rows)),
        )
    enriched = (
        _enriched_node_rows(conn, session, rows)
        if row_kind == "node"
        else _enriched_edge_rows(conn, session, rows)
    )
    scope = f"deployment:{session.deployment_name}"
    evaluate: dict[str, Any] = {
        "action": action,
        "subject": None,
        "identity_claim": None,
        "audit": audit,
        "surface": surface,
        "row_kind": row_kind,
        "id_key": id_key,
    }
    # ONE pass scope around the bundle read, the class keys and every
    # evaluation below: the memoized bundle is the one the keys came from.
    with read_pass_scope():
        bundle = _load_read_bundle(conn, session, scope=scope)
        if bundle is None:
            # No active bundle (decision 123a): one decision for the request;
            # each returned row carries the per-row filter's stamped attrs.
            stamped = [
                (row, attrs_out)
                for row in enriched
                if (attrs_out := _lineage_stamped_attrs(row)) is not None
            ]
            visible_rows: list[dict[str, Any]] = []
            if stamped and _decide_no_active_bundle(
                conn, session, row_kind=row_kind, action=action, surface=surface,
                audit=audit, count=False, rows_decided=len(stamped),
            ):
                visible_rows = [
                    _read_class_member_row(row, attrs_out, redact_with=None)
                    for row, attrs_out in stamped
                ]
            return visible_rows, ReadClassPath(
                path="no_active_bundle", class_rows=len(stamped),
            )
        blocking = _read_class_blocking_rule_keys(
            bundle,
            actions=_evaluated_read_actions(action, surface),
            subjects=_evaluated_read_subjects(session),
        )
        if blocking:
            return (
                _filter_rows(conn, session, enriched, **evaluate),
                ReadClassPath(
                    path="per_row",
                    reason="rule_names_key:" + ",".join(blocking),
                    per_row_rows=len(enriched),
                ),
            )
        expected_fingerprint = bundle.policy_fingerprint if bundle is not None else None
        keys = _read_class_keys(bundle)

        class_of: dict[int, int] = {}
        member_attrs: dict[int, dict[str, Any]] = {}
        class_index: dict[Any, int] = {}
        class_keys: list[Any] = []
        class_members: list[list[int]] = []
        per_row_positions: list[int] = []
        stored_positions: list[int] = []
        for position, row in enumerate(enriched):
            stored_class = stored_read_class(row)
            if stored_class is not None:
                key, attrs_out = _stored_class_key_and_attrs(row, stored_class)
                if key is not None:
                    stored_positions.append(position)
            else:
                key, attrs_out = _decision_class_key_and_attrs(
                    session, row, row_kind=row_kind, id_key=id_key, scope=scope,
                    surface=surface, keys=keys, position=position, audit=audit,
                )
            if key is None or attrs_out is None or key[0] == _UNCLASSIFIABLE:
                # Missing class facts: the per-row filter decides this row.
                per_row_positions.append(position)
                continue
            index = class_index.get(key)
            if index is None:
                index = class_index[key] = len(class_keys)
                class_keys.append(key)
                class_members.append([])
            class_of[position] = index
            class_members[index].append(position)
            member_attrs[position] = attrs_out

        # Deterministic representative: the first member in (id, content)
        # order, whatever order the caller passed.
        representatives: list[dict[str, Any]] = []
        tags: dict[int, dict[str, Any]] = {}
        order_key = _stable_order_key(enriched)
        for index, positions in enumerate(class_members):
            first = min(positions, key=order_key)
            representatives.append({**enriched[first], _CLASS_TAG: index})
            tags[index] = {
                "read_class": _read_class_id(class_keys[index]),
                "class_member_count": len(positions),
                "decided_by": "read_class",
            }
        fingerprints: dict[int, set[Any]] = {}

        def observe(row: Mapping[str, Any], decision: PolicyDecision) -> None:
            fingerprints.setdefault(row[_CLASS_TAG], set()).add(decision.policy_fingerprint)

        redacted_class: dict[int, bool] = {}
        if representatives:
            for row in _filter_rows(
                conn, session, representatives, count=False,
                class_target_tags=tags, decision_observer=observe, **evaluate,
            ):
                redacted_class[row[_CLASS_TAG]] = isinstance(row["attrs"], ReadRedactedAttrs)
        _assert_class_decisions_current(
            fingerprints, expected_fingerprint, classes=len(representatives),
            visible=redacted_class,
        )

        per_row_out: dict[int, list[dict[str, Any]]] = {}
        if per_row_positions:
            for row in _filter_rows(
                conn, session,
                [{**enriched[p], _ROW_POSITION_TAG: p} for p in per_row_positions],
                **evaluate,
            ):
                position = row.pop(_ROW_POSITION_TAG)
                per_row_out.setdefault(position, []).append(row)

    classifiers: dict[str, Any] = {}
    subtype_pii_classes: dict[str, dict[str, Any]] | None = None
    visible: list[dict[str, Any]] = []
    for position, row in enumerate(enriched):
        if position not in class_of:
            visible.extend(per_row_out.get(position, ()))
            continue
        index = class_of[position]
        if index not in redacted_class:
            continue
        classifier = None
        if redacted_class[index]:
            # The member's own subtype classifier, as the per-row filter's.
            if subtype_pii_classes is None:
                subtype_pii_classes = _load_subtype_pii_classes(
                    conn, getattr(session, "catalog_version_id", None)
                )
            subtype_key = str(row.get("subtype") or "")
            classifier = classifiers.get(subtype_key)
            if classifier is None:
                classifier = classifiers[subtype_key] = subtype_attr_classifier(
                    subtype_pii_classes.get(subtype_key)
                )
        visible.append(
            _read_class_member_row(row, member_attrs[position], redact_with=classifier)
        )
    verified = _verify_stored_class_sample(
        [enriched[p] for p in stored_positions if class_of.get(p) in redacted_class],
        row_kind=row_kind,
        sample=_read_class_verify_sample(verify_sample),
    )
    return visible, ReadClassPath(
        path="read_class",
        classes=len(representatives),
        class_rows=len(class_of),
        per_row_rows=len(per_row_positions),
        stored_class_rows=len(stored_positions),
        verified_rows=verified,
    )


#: A stored-class key's first element. A tuple, so it can never equal the
#: projection key's first element (the classification, a string).
_STORED_CLASS = ("stored_read_class",)

READ_CLASS_VERIFY_SAMPLE_ENV = "OKG_NOMOS_READ_CLASS_VERIFY_SAMPLE"


def _stored_class_key_and_attrs(
    raw_row: Mapping[str, Any],
    stored_class: Any,
) -> tuple[tuple[Any, ...] | None, dict[str, Any] | None]:
    """A row's class key from its stored class, and the per-row filter's attrs.

    The key is (stored class, the row's source set, its effective
    classification). The source set is recomputed from the row, as it must be
    for the returned attrs anyway, so it is always in the key (lane C's
    condition 5) whatever the stamp says. A row with no source lineage gets
    ``(None, None)``: the per-row filter hides it, as for the projection.
    """
    attrs = dict(raw_row.get("attrs") or {})
    lineage = lineage_for_row(
        attrs,
        source_id=raw_row.get("source"),
        source_record_id=raw_row.get("source_record_id"),
        source_revision=raw_row.get("source_revision"),
    )
    source_ids = source_ids_from_lineage(lineage)
    if not source_ids:
        return None, None
    classification = str(attrs.get("classification") or "internal")
    return (
        (_STORED_CLASS, stored_class, source_ids, classification),
        attrs_with_lineage(attrs, lineage),
    )


def _read_class_verify_sample(verify_sample: int | None) -> int:
    if verify_sample is not None:
        return int(verify_sample)
    raw = os.environ.get(READ_CLASS_VERIFY_SAMPLE_ENV, "").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{READ_CLASS_VERIFY_SAMPLE_ENV} must be an integer, not {raw!r}",
            data={"value": raw},
        ) from exc


def _verify_stored_class_sample(
    rows: list[Mapping[str, Any]],
    *,
    row_kind: str,
    sample: int,
) -> int:
    """Refuse the read when a sampled stored class disagrees with its row."""
    try:
        return verify_stored_classes(rows, row_kind, sample=sample)
    except ReadClassMismatch as exc:
        raise SubstrateError(
            "read-class filter: a row's stored Nomos read class does not match "
            "its content; refusing the read",
            data={
                "row_kind": exc.kind,
                "row_id": exc.row_id,
                "stored_read_class": str(exc.stored),
                "computed_read_class": None if exc.computed is None else str(exc.computed),
            },
            fix_hint=(
                "a graph writer stamped this row wrongly; find the writer that "
                "produced it (okg.graph_*.run_id) and re-stamp the row"
            ),
        ) from exc


def _withhold_text_digests(
    row: dict[str, Any],
    attrs_before: Mapping[str, Any],
    classifier: Any,
) -> None:
    """Drop the text digests of a redacted row unless its text is revealed.

    Called wherever a ``redact`` decision masks a row's attrs, so every
    operator that serves a filtered row inherits it. The digest stays only
    when the row carries its stored ``text`` and the classifier reveals it
    (the reader then reads the text itself). A dropped, mask-tokened or
    pseudonymized text, or a row that did not carry its text so the filter
    cannot tell, loses every field in :data:`TEXT_DIGEST_FIELDS`: the key is
    removed, as a withheld attribute value loses its ``value_hash``.
    """
    if "text" in attrs_before and resolve_pii_class(classifier("text")) in REVEAL_CLASSES:
        return
    for field in TEXT_DIGEST_FIELDS:
        row.pop(field, None)


def _read_class_member_row(
    row: Mapping[str, Any],
    attrs_out: dict[str, Any],
    *,
    redact_with: Any,
) -> dict[str, Any]:
    """One returned member row, exactly as ``_filter_rows_evaluate`` builds it.

    ``attrs_out`` is the member's own lineage-stamped attrs. ``redact_with`` is
    the member's subtype classifier when its class was redacted, else None.
    """
    out = dict(row)
    if redact_with is None:
        out["attrs"] = attrs_out
        return out
    masked = mask_attrs(attrs_out, classifier=redact_with, drop_classes=READ_DROP_CLASSES)
    # As the per-row filter: the marker even when nothing matched.
    masked[REDACTION_MARKER] = True
    out["attrs"] = ReadRedactedAttrs(masked)
    _withhold_text_digests(out, attrs_out, redact_with)
    return out


def _read_class_session_refusal(
    session: Any,
    *,
    subject: str | None,
    identity_claim: Mapping[str, Any] | None,
) -> str | None:
    """Why this call must take the per-row path before any row is read, if so."""
    if subject is not None:
        return "subject_argument"
    if identity_claim is not None:
        return "identity_claim_argument"
    if getattr(session, "nomos_identity_claim", None) is not None:
        return "identity_claim_session"
    if getattr(session, "nomos_access_context", None) is not None:
        return "access_context_session"
    return None


def _evaluated_read_actions(action: str, surface: str) -> frozenset[str]:
    """Every action the evaluator can record for this read's decisions.

    The boundary evaluates the action of the surface mapping it resolves for
    each enforcement point, which may differ from the caller's action; both
    count, so the rule-key gate never skips a rule that can match.
    """
    actions = {action}
    for point in ("source_lineage_filter", "fine_grained_lineage_filter"):
        mapping = SURFACE_DECISION_MAPPINGS.get(
            surface_for_action(action, fallback_surface=f"{surface}.{point}")
        )
        if mapping is not None:
            actions.add(mapping.action)
    return frozenset(actions)


def _evaluated_read_subjects(session: Any) -> frozenset[str]:
    subject = str(
        getattr(session, "nomos_subject", None)
        or nomos_subject_from_env(default_kind="mcp_client")
    )
    return frozenset({subject, subject.strip()})


def _read_class_blocking_rule_keys(
    bundle: Any,
    *,
    actions: frozenset[str],
    subjects: frozenset[str],
) -> list[str]:
    """Keys outside the allow-list named by an active rule that can match."""
    if bundle is None:
        return []
    blocking: set[str] = set()
    for rule in bundle.rules:
        if "*" not in rule.actions and not actions.intersection(rule.actions):
            continue
        if "*" not in rule.subjects and not subjects.intersection(rule.subjects):
            continue
        blocking.update(
            str(key) for key in rule.match.keys() if key not in READ_CLASS_SAFE_RULE_KEYS
        )
    return sorted(blocking)


def _decision_class_keys(bundle: Any) -> frozenset[str]:
    """The facts a decision-class key projects on: those the active rules'
    ``match`` names and the protected-identity check reads. No other fact of a
    resource can change a decision, so no other fact is written."""
    return frozenset(_PROTECTED_IDENTITY_FACTS).union(
        *(rule.match.keys() for rule in (bundle.rules if bundle is not None else ()))
    )


def _read_class_keys(bundle: Any) -> frozenset[str]:
    """The facts a read-class key projects on (see ``_decision_class_key``):
    the decision-class keys plus the source facts every class decision names."""
    return _decision_class_keys(bundle) | _READ_CLASS_ALWAYS_KEYS


def _read_class_id(key: Any) -> str:
    return hashlib.sha256(repr(key).encode("utf-8")).hexdigest()[:32]


def _assert_class_decisions_current(
    fingerprints: Mapping[int, set[Any]],
    expected: Any,
    *,
    classes: int,
    visible: Mapping[int, bool],
) -> None:
    """Refuse the read unless every class was decided under the key's policy.

    A class key is exact only for the bundle it was projected from. A decision
    made under any other fingerprint (a policy activated mid-read, a decision
    reused from an older bundle) could differ for some member, so the read is
    refused, never answered from it. A visible class with no decision at all is
    refused too: nothing decided it.
    """
    for index in range(classes):
        seen = fingerprints.get(index, set())
        if index in visible and not seen:
            raise SubstrateError(
                "read-class filter: a class was returned with no recorded decision",
                data={"class_index": index},
            )
        if seen and seen != {expected}:
            raise SubstrateError(
                "read-class filter: a class representative was decided under a "
                "different policy than its class key; refusing the read",
                data={
                    "class_index": index,
                    "expected_policy_fingerprint": expected,
                    "decided_policy_fingerprints": sorted(str(f) for f in seen),
                },
            )


_ABSENT_FACT = ("absent",)


def _fact_text(value: Any) -> str:
    """A fact's canonical JSON text: ``json.dumps(value, sort_keys=True,
    default=_fact_default)``.

    Strings, ``None``, booleans and integers -- nearly every projected fact --
    are written the way ``json.JSONEncoder.encode`` writes them, without
    building an encoder per call (PACT lane-h-nomos-a-part1: the per-call
    encoder was most of the class projection's JSON cost). The text is the
    same; anything else goes through ``json.dumps``.
    """
    if isinstance(value, str):
        return _encode_json_string(value)
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if type(value) is int:
        return int.__repr__(value)
    return json.dumps(value, sort_keys=True, default=_fact_default)


_encode_json_string = json.encoder.encode_basestring_ascii


def _stable_row_order(row: Mapping[str, Any]) -> tuple[str, str]:
    """A row's (id, content) order key: its id, then its canonical JSON text."""
    try:
        text = json.dumps(row, sort_keys=True, default=repr)
    except (TypeError, ValueError, RecursionError):
        text = repr(sorted((str(k), repr(v)) for k, v in row.items()))
    return (_row_order_id(row), text)


def _row_order_id(row: Mapping[str, Any]) -> str:
    return str(row.get("node_id", row.get("edge_id")))


def _stable_order_key(rows: list[Mapping[str, Any]]) -> Callable[[int], tuple[str, str]]:
    """A key over positions in ``rows`` that orders them as ``_stable_row_order``.

    The content text only breaks ties between rows that share an id, so it is
    written only for those rows: rows with distinct ids are ordered by id
    alone under both keys. (PACT lane-h-nomos-a-part1: writing every whole row
    as JSON was the order's whole cost.)
    """
    ids = [_row_order_id(row) for row in rows]
    shared = {row_id for row_id, n in Counter(ids).items() if n > 1}

    def key(position: int) -> tuple[str, str]:
        row_id = ids[position]
        if row_id in shared:
            return _stable_row_order(rows[position])
        return (row_id, "")

    return key


def _fact_default(value: Any) -> str:
    return f"<{type(value).__qualname__}:{value!r}>"


def _decision_class_key(
    session: Any,
    raw_row: Mapping[str, Any],
    *,
    row_kind: str,
    id_key: str,
    scope: str,
    surface: str,
    keys: frozenset[str],
    position: int,
) -> tuple[Any, ...] | None:
    """Everything the per-row filter's decisions for this row can depend on.

    Mirrors ``_filter_rows_evaluate`` (without the identity-claim path): the
    same lineage, the same resources in the same order, each projected on the
    facts a rule or the protected-identity check can read, with its JSON shape
    check and fail-closed flag.

    A fact that cannot be written as canonical JSON (a mapping with mixed key
    types, a cycle) makes the row its own class, keyed by its ``position``:
    the real per-row filter then decides it exactly as a read would (the
    boundary refuses such a resource as malformed and hides the row).
    """
    return _decision_class_key_and_attrs(
        session, raw_row, row_kind=row_kind, id_key=id_key, scope=scope,
        surface=surface, keys=keys, position=position, stamp_attrs=False,
    )[0]


def _decision_class_key_and_attrs(
    session: Any,
    raw_row: Mapping[str, Any],
    *,
    row_kind: str,
    id_key: str,
    scope: str,
    surface: str,
    keys: frozenset[str],
    position: int,
    audit: bool = False,
    stamp_attrs: bool = True,
) -> tuple[tuple[Any, ...] | None, dict[str, Any] | None]:
    """``_decision_class_key`` plus the attrs the per-row filter returns.

    The second value is ``attrs_with_lineage(attrs, lineage)``, exactly what
    ``_filter_rows_evaluate`` puts in a visible row before any masking, or
    ``None`` when the row has no source lineage (the per-row filter hides it).
    ``audit`` is the caller's audit flag, a constant fact of every resource.
    With ``stamp_attrs=False`` (a caller that only needs the key) the attrs
    are not built and the second value is None: no fact of the key reads them.
    """
    row = dict(raw_row)
    attrs = dict(row.get("attrs") or {})
    lineage = lineage_for_row(
        attrs,
        source_id=row.get("source"),
        source_record_id=row.get("source_record_id"),
        source_revision=row.get("source_revision"),
    )
    source_ids = source_ids_from_lineage(lineage)
    if not source_ids:
        return None, None
    if stamp_attrs:
        row["attrs"] = attrs_with_lineage(attrs, lineage)
    base_resource = _row_resource(
        session,
        scope=scope,
        row=row,
        row_kind=row_kind,
        id_key=id_key,
        lineage=lineage,
        source_ids=source_ids,
        audit=audit,
    )

    try:
        return _projected_facts(
            base_resource, row=row, attrs=attrs, lineage=lineage, source_ids=source_ids,
            row_kind=row_kind, id_key=id_key, surface=surface, keys=keys,
        ), (row["attrs"] if stamp_attrs else None)
    except (TypeError, ValueError, RecursionError):
        return (_UNCLASSIFIABLE, position), (row["attrs"] if stamp_attrs else None)


_UNCLASSIFIABLE = ("unclassifiable",)


def _projected_facts(
    base_resource: dict[str, Any],
    *,
    row: dict[str, Any],
    attrs: dict[str, Any],
    lineage: Mapping[str, Any],
    source_ids: tuple[str, ...],
    row_kind: str,
    id_key: str,
    surface: str,
    keys: frozenset[str],
) -> tuple[Any, ...]:
    # The boundary refuses a resource that is not JSON-shaped. Each resource
    # below is the base plus a few keys, so the base's values are checked once.
    base_ok = {
        key: isinstance(key, str) and _is_json_compatible(value)
        for key, value in base_resource.items()
    }

    def json_ok(resource: Mapping[str, Any]) -> bool:
        return all(
            base_ok[key]
            if key in base_ok and value is base_resource[key]
            else isinstance(key, str) and _is_json_compatible(value)
            for key, value in resource.items()
        )

    # The facts a rule can read, as canonical JSON (absent is not null).
    ordered_keys = sorted(keys)
    base_text = {
        key: _fact_text(base_resource[key])
        for key in ordered_keys if key in base_resource
    }

    def projection(resource: Mapping[str, Any]) -> tuple[Any, ...]:
        return tuple(
            (key, _ABSENT_FACT) if key not in resource
            else (key, base_text[key])
            if key in base_text and resource[key] is base_resource[key]
            else (key, _fact_text(resource[key]))
            for key in ordered_keys
        )

    def facts(resource: Mapping[str, Any]) -> tuple[Any, ...]:
        return (projection(resource), json_ok(resource))

    by_source = tuple(
        facts({**base_resource, "source_id": source_id}) for source_id in source_ids
    )
    fine_grained = tuple(
        (facts(resource), fail_closed)
        for resource, _target, fail_closed in _fine_grained_targets(
            base_resource,
            row=row,
            row_kind=row_kind,
            id_key=id_key,
            lineage=lineage,
            surface=surface,
        )
    )
    return (str(attrs.get("classification") or "internal"), by_source, fine_grained)


def _enriched_node_rows(
    conn: psycopg.Connection,
    session: Any,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Node rows as the filter decides them: stored lineage, then accepted overlays."""
    lookup = _node_source_lookup(conn, session, rows)
    enriched = [{**lookup.get(str(row.get("node_id")), {}), **row} for row in rows]
    # Hydrate projection-only rows before adding accepted overlays. Applying the
    # overlay helper first would synthesize attrs={} and let that empty mapping
    # override the stored source-lineage attrs above, incorrectly hiding derived
    # nodes. Nomos still decides and redacts the same final effective attributes
    # the caller will return, so a downstream overlay cannot restore raw PII.
    return apply_effective_attrs_to_rows(
        conn,
        enriched,
        target_kind="node",
        target_id_field="node_id",
        generation_id=session.generation_id,
    )


def _enriched_edge_rows(
    conn: psycopg.Connection,
    session: Any,
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Edge rows as the filter decides them: with their stored lineage."""
    lookup = _edge_source_lookup(conn, session, rows)
    return [{**lookup.get(str(row.get("edge_id")), {}), **row} for row in rows]


def _filter_rows(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    action: str,
    subject: str | None,
    identity_claim: Mapping[str, Any] | None,
    audit: bool,
    surface: str,
    row_kind: str,
    id_key: str,
    count: bool = False,
    class_target_tags: Mapping[Any, Mapping[str, Any]] | None = None,
    decision_observer: Callable[[Mapping[str, Any], PolicyDecision], None] | None = None,
) -> list[dict[str, Any]]:
    # One filter pass = one batched decision flush (issue #635 overhead fix):
    # per-row persists buffer in the sink and land as a single statement
    # before the pass commits, instead of one INSERT round trip per
    # (row x source x fine-grained target) evaluation.
    #
    # The read-pass memo scope is the same idea one layer down (PACT
    # mcp-principal-mapping): per-request identity resolution re-reads the
    # providers, subject bindings, permission grants and active policy
    # bundle once per row per source, none of which can change mid-pass. The
    # scope dies with the pass, so a policy-bundle activation between two
    # requests is picked up by the next one -- there is no cache to
    # invalidate.
    rows = _without_link_endpoints(conn, session, rows, row_kind=row_kind)
    with read_pass_scope(), batched_decision_persist(conn):
        visible, decisions_written = _filter_rows_evaluate(
            conn,
            session,
            rows,
            action=action,
            subject=subject,
            identity_claim=identity_claim,
            audit=audit,
            surface=surface,
            row_kind=row_kind,
            id_key=id_key,
            count=count,
            class_target_tags=class_target_tags,
            decision_observer=decision_observer,
        )
    if decisions_written:
        conn.commit()
    return visible


def _without_link_endpoints(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    row_kind: str,
) -> list[Mapping[str, Any]]:
    """Hide "known by key, not loaded" link endpoints (issue #3666).

    An endpoint has no source record: the core producer wrote it from a key
    a citing record names. No access rule reads such a row, so under audit
    and enforce it is withheld, and so is every edge that reaches it: the
    edge would name the key. Showing endpoints there needs an explicit read
    class, which this change does not add.
    """
    from okg.substrate.catalog.linker_config import (
        LINK_ENDPOINT_SOURCE,
        is_link_endpoint_row,
    )

    rows = list(rows)
    if row_kind == "node":
        return [row for row in rows if not is_link_endpoint_row(row)]
    if row_kind != "edge" or not rows:
        return rows
    ends: dict[str, tuple[str, str]] = {}
    unknown_edge_ids: list[int] = []
    for row in rows:
        # Only the extractor links to an endpoint; no other edge is checked,
        # so a read with no extractor edge costs no query here.
        if not str(row.get("source") or "").startswith("_extractor:"):
            continue
        if row.get("src") is not None and row.get("dst") is not None:
            ends[str(row.get("edge_id"))] = (str(row["src"]), str(row["dst"]))
        elif row.get("edge_id") is not None:
            unknown_edge_ids.append(int(row["edge_id"]))
    if not ends and not unknown_edge_ids:
        return rows
    graph = _checkout(conn, session)
    if unknown_edge_ids:
        for edge in graph.query(
            "SELECT edge_id, src, dst FROM okg.graph_edges "
            "WHERE edge_id = ANY(:ids)",
            {"ids": sorted(set(unknown_edge_ids))},
        ):
            ends[str(edge["edge_id"])] = (str(edge["src"]), str(edge["dst"]))
    node_ids = sorted({node_id for pair in ends.values() for node_id in pair})
    if not node_ids:
        return rows
    endpoint_ids = {
        str(node["node_id"])
        for node in graph.query(
            "SELECT node_id, source, attrs FROM okg.graph_nodes "
            "WHERE node_id = ANY(:ids) AND source = :src",
            {"ids": node_ids, "src": LINK_ENDPOINT_SOURCE},
        )
        if is_link_endpoint_row(dict(node))
    }
    if not endpoint_ids:
        return rows
    return [
        row for row in rows
        if not set(ends.get(str(row.get("edge_id")), ())) & endpoint_ids
    ]


def _filter_rows_evaluate(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    action: str,
    subject: str | None,
    identity_claim: Mapping[str, Any] | None,
    audit: bool,
    surface: str,
    row_kind: str,
    id_key: str,
    count: bool = False,
    class_target_tags: Mapping[Any, Mapping[str, Any]] | None = None,
    decision_observer: Callable[[Mapping[str, Any], PolicyDecision], None] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Decide each row with the Python evaluator; return the visible rows.

    ``class_target_tags`` and ``decision_observer`` serve the read-class path
    (``filter_rows_by_read_class``) only. The tags, looked up by a row's
    ``_CLASS_TAG``, are added to every decision target of that row; the target
    is audit metadata outside the decision identity. The observer sees every
    decision this pass makes for a row, before the row's outcome is known.
    Neither changes any decision or any row's outcome.
    """
    visible: list[dict[str, Any]] = []
    # A counting pass (``visible_ids_by_decision_class``) records its decisions
    # as counts: the purpose is a context fact, so the decision identity differs
    # from a read of the same row and both are kept in the insert-once trail,
    # and each target kind ends in ``_count`` instead of ``_read``.
    purpose_context: dict[str, Any] = {READ_PURPOSE_FACT: "count"} if count else {}
    verb = "count" if count else "read"
    # PACT mcp-principal-mapping (req.fail-closed-principal): the or-chain
    # below ends on the PROCESS environment subject, which is exactly the
    # silent downgrade multi-user mode must not have -- a request whose
    # principal never resolved would be answered as whoever started the
    # server. In multi-user mode the request-scoped subject is mandatory, so
    # the coalesce is made unreachable by refusing before it, rather than by
    # trusting that every caller remembered to pass one.
    _assert_multi_user_subject(session, subject)
    subject = (
        subject
        or getattr(session, "nomos_subject", None)
        or nomos_subject_from_env(default_kind="mcp_client")
    )
    if identity_claim is None:
        identity_claim = getattr(session, "nomos_identity_claim", None)
    scope = f"deployment:{session.deployment_name}"
    # Audit-only session coordinates: recorded on every decision target so the
    # trail keeps the publish position, without entering decision identity.
    audit_stamp = {
        "generation_id": session.generation_id,
        "branch_name": session.branch_name,
        "catalog_version_id": session.catalog_version_id,
    }
    decisions_written = False
    # PACT read-masking-from-catalog-pii-class: load the subtype -> pii_class map
    # once per pass so the redact transform classifies each node row by its
    # subtype's declared pii_class (unannotated subtypes fall back to legacy).
    subtype_pii_classes = _load_subtype_pii_classes(
        conn, getattr(session, "catalog_version_id", None)
    )
    # Memoize the per-subtype classifier so a hot read of many rows of one
    # subtype builds it once, not per row.
    classifier_by_subtype: dict[str, Any] = {}
    for raw_row in rows:
        row = dict(raw_row)
        attrs = dict(row.get("attrs") or {})
        lineage = lineage_for_row(
            attrs,
            source_id=row.get("source"),
            source_record_id=row.get("source_record_id"),
            source_revision=row.get("source_revision"),
        )
        source_ids = source_ids_from_lineage(lineage)
        if not source_ids:
            continue
        row["attrs"] = attrs_with_lineage(attrs, lineage)
        base_resource = _row_resource(
            session,
            scope=scope,
            row=row,
            row_kind=row_kind,
            id_key=id_key,
            lineage=lineage,
            source_ids=source_ids,
            audit=audit,
        )
        target_tags: Mapping[str, Any] = (
            class_target_tags.get(row.get(_CLASS_TAG), {})
            if class_target_tags is not None else {}
        )
        row_subject = subject
        if identity_claim is not None:
            from okg.substrate.nomos.identity_store import (
                evaluate_identity_claim_from_postgres,
            )

            resolved_subject: str | None = None
            identity_decision_ids: list[str] = []
            identity_hidden = False
            for source_id in source_ids:
                authorization = evaluate_identity_claim_from_postgres(
                    conn,
                    deployment_name=session.deployment_name,
                    bundle=None,
                    claim=identity_claim,
                    action=action,
                    resource={
                        **base_resource,
                        "audit": audit,
                        "source_id": source_id,
                    },
                    scope=scope,
                    classification=str(attrs.get("classification") or "internal"),
                    enforcement_point=f"{surface}.source_lineage_identity_filter",
                    # A counting pass (#3208 N4) records the identity
                    # decision as a count too: `_count` kind and the purpose
                    # fact, as on the source and fine-grained targets.
                    extra_context=purpose_context,
                    target={
                        "kind": f"{surface}_{row_kind}_identity_source_{verb}",
                        id_key: row.get(id_key),
                        "source_id": source_id,
                        **audit_stamp,
                    },
                )
                decisions_written = True
                if (
                    authorization.decision.effect in _HIDE_EFFECTS
                    or not authorization.resolved_subject
                ):
                    identity_hidden = True
                    break
                if resolved_subject is None:
                    resolved_subject = authorization.resolved_subject
                elif resolved_subject != authorization.resolved_subject:
                    identity_hidden = True
                    break
                identity_decision_ids.append(
                    authorization.decision.decision_id,
                )
            if identity_hidden or not resolved_subject:
                continue
            row_subject = resolved_subject
            row["nomos_identity_source_decision_ids"] = identity_decision_ids
        decisions: list[PolicyDecision] = []
        hidden = False
        for source_id in source_ids:
            decision = evaluate_with_active_policy(
                conn,
                deployment_name=session.deployment_name,
                scope=scope,
                decision_input=DecisionInput(
                    subject=row_subject,
                    action=action,
                    classification=str(attrs.get("classification") or "internal"),
                    resource={
                        **base_resource,
                        "source_id": source_id,
                    },
                    context=purpose_context,
                ),
                enforcement_point=f"{surface}.source_lineage_filter",
                target={
                    "kind": f"{surface}_{row_kind}_source_{verb}",
                    id_key: row.get(id_key),
                    "source_id": source_id,
                    **audit_stamp,
                    **target_tags,
                },
                performed_obligations=("audit", "redaction"),
                payload=safe_decision_payload(row["attrs"]),
            )
            decisions_written = True
            if decision_observer is not None:
                decision_observer(row, decision)
            if decision.effect in _HIDE_EFFECTS:
                hidden = True
                break
            decisions.append(decision)
        if hidden:
            continue
        for resource, target, fail_closed in _fine_grained_targets(
            base_resource,
            row=row,
            row_kind=row_kind,
            id_key=id_key,
            lineage=lineage,
            surface=surface,
            audit_stamp=audit_stamp,
        ):
            decision = evaluate_with_active_policy(
                conn,
                deployment_name=session.deployment_name,
                scope=scope,
                decision_input=DecisionInput(
                    subject=row_subject,
                    action=action,
                    classification=str(attrs.get("classification") or "internal"),
                    resource=resource,
                    context=purpose_context,
                ),
                enforcement_point=f"{surface}.fine_grained_lineage_filter",
                target={
                    **(
                        {**target, "kind": str(target["kind"]).removesuffix("_read") + "_count"}
                        if count else target
                    ),
                    **target_tags,
                },
                performed_obligations=("audit", "redaction"),
                payload=safe_decision_payload(row["attrs"]),
            )
            decisions_written = True
            if decision_observer is not None:
                decision_observer(row, decision)
            if fail_closed or (
                decision.effect in _HIDE_EFFECTS
                and decision.matched_rule_ids
            ):
                hidden = True
                break
            if decision.matched_rule_ids:
                decisions.append(decision)
        if hidden:
            continue
        if any(decision.effect == "redact" for decision in decisions):
            subtype_key = str(row.get("subtype") or "")
            classifier = classifier_by_subtype.get(subtype_key)
            if classifier is None:
                classifier = subtype_attr_classifier(
                    subtype_pii_classes.get(subtype_key)
                )
                classifier_by_subtype[subtype_key] = classifier
            masked = mask_attrs(
                row["attrs"],
                classifier=classifier,
                drop_classes=READ_DROP_CLASSES,
            )
            # A redact decision fired: keep the marker present even when nothing
            # matched the classifier, preserving the prior unconditional stamp.
            masked[REDACTION_MARKER] = True
            _withhold_text_digests(row, row["attrs"], classifier)
            row["attrs"] = ReadRedactedAttrs(masked)
        visible.append(row)
    return visible, decisions_written


def _row_resource(
    session: Any,
    *,
    scope: str,
    row: Mapping[str, Any],
    row_kind: str,
    id_key: str,
    lineage: Mapping[str, Any],
    source_ids: tuple[str, ...],
    audit: bool,
) -> dict[str, Any]:
    row_id = row.get(id_key)
    object_ref = graph_object_ref(row_kind, row_id)
    # generation_id / branch_name / catalog_version_id are deliberately NOT in
    # resource: resource is identity-bearing (core.evaluate hashes it into
    # input_hash -> decision_id), and a monotonic publish counter in identity
    # defeats the ON CONFLICT dedup — every publish re-minted every decision
    # (issue #635). They are audit metadata and live in the decision target;
    # policy-version discrimination is preserved because decision_id already
    # hashes policy_fingerprint.
    resource: dict[str, Any] = {
        "deployment": session.deployment_name,
        "scope": scope,
        "kind": row_kind,
        "audit": audit,
        id_key: row_id,
        "source_ids": list(source_ids),
        "source_lineage": dict(lineage),
        "source_record_refs": list(source_record_refs_from_lineage(lineage)),
    }
    if object_ref is not None:
        resource["graph_ref"] = object_ref
        resource[f"{row_kind}_ref"] = object_ref
    access_context = getattr(session, "nomos_access_context", None)
    if isinstance(access_context, Mapping):
        resource.update(_access_context_resource(access_context))
        resource["_nomos_access_context"] = dict(access_context)
    return resource


def _access_context_resource(access_context: Mapping[str, Any]) -> dict[str, Any]:
    context_id = access_context.get("context_id") or access_context.get("id")
    facts: dict[str, Any] = {}
    if context_id is not None:
        facts["context_id"] = str(context_id)
        facts["access_context_id"] = str(context_id)
    for key in (
        "run_id",
        "question_id",
        "as_of",
        "holdout_set_id",
        "policy_context",
    ):
        if access_context.get(key) is not None:
            facts[key] = access_context[key]
    for key in (
        "target_refs",
        "holdout_refs",
        "holdout_set_ids",
        "set_ids",
        "hidden_refs",
    ):
        value = access_context.get(key)
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
            facts[key] = list(value)
    return facts


def _fine_grained_targets(
    base_resource: Mapping[str, Any],
    *,
    row: Mapping[str, Any],
    row_kind: str,
    id_key: str,
    lineage: Mapping[str, Any],
    surface: str,
    audit_stamp: Mapping[str, Any] | None = None,
) -> list[tuple[dict[str, Any], dict[str, Any], bool]]:
    row_id = row.get(id_key)
    audit_stamp = dict(audit_stamp or {})
    targets: list[tuple[dict[str, Any], dict[str, Any], bool]] = []
    raw_access_context = base_resource.get("_nomos_access_context")
    access_context = (
        raw_access_context if isinstance(raw_access_context, Mapping) else {}
    )
    graph_ref = base_resource.get("graph_ref")
    if graph_ref is not None:
        targets.append(
            (
                {
                    **base_resource,
                    "resource_kind": "graph_object",
                    "graph_ref": graph_ref,
                    f"{row_kind}_ref": graph_ref,
                    **_access_context_match_facts(str(graph_ref), access_context),
                },
                {
                    "kind": f"{surface}_{row_kind}_graph_object_read",
                    id_key: row_id,
                    "graph_ref": graph_ref,
                    **audit_stamp,
                },
                False,
            )
        )
    contributors = source_record_contributors_from_lineage(lineage)
    # PACT bound-derived-lineage-envelope: a truncated envelope carries only a
    # capped subset of its contributing records, so the contributor list below
    # is INCOMPLETE. Deciding a record-level policy on it would allow a read on
    # evidence that was discarded, so a truncated envelope is unaddressable for
    # fine-grained access and fails closed — exactly as an absent one does.
    # Source-level policy is unaffected: source_ids are never truncated.
    record_detail_truncated = (
        lineage.get(RECORD_DETAIL_KEY) == RECORD_DETAIL_TRUNCATED
    )
    if _requires_source_record_context(access_context) and (
        not contributors or record_detail_truncated
    ):
        targets.append(
            (
                {
                    **base_resource,
                    "resource_kind": "source_record",
                    "is_unaddressable_fine_grained_lineage": True,
                    **(
                        {"record_detail_truncated": True}
                        if record_detail_truncated else {}
                    ),
                },
                {
                    "kind": f"{surface}_{row_kind}_source_record_read",
                    id_key: row_id,
                    "unaddressable": True,
                    **(
                        {"record_detail_truncated": True}
                        if record_detail_truncated else {}
                    ),
                    **audit_stamp,
                },
                True,
            )
        )
    for contributor in contributors:
        ref = contributor["source_record_ref"]
        horizon_facts = _horizon_facts(contributor, access_context)
        fail_closed = bool(
            access_context.get("as_of") is not None
            and "source_record_observed_at" not in horizon_facts
        )
        targets.append(
            (
                {
                    **base_resource,
                    **contributor,
                    "resource_kind": "source_record",
                    "source_record_refs": [ref],
                    **_access_context_match_facts(ref, access_context),
                    **horizon_facts,
                    **(
                        {"is_unaddressable_fine_grained_lineage": True}
                        if fail_closed
                        else {}
                    ),
                },
                {
                    "kind": f"{surface}_{row_kind}_source_record_read",
                    id_key: row_id,
                    "source_id": contributor.get("source_id"),
                    "source_record_ref": ref,
                    **audit_stamp,
                },
                fail_closed,
            )
        )
    return targets


def _requires_source_record_context(access_context: Mapping[str, Any]) -> bool:
    if access_context.get("as_of") is not None:
        return True
    for key in ("target_refs", "hidden_refs", "holdout_refs"):
        if _has_source_record_ref(access_context.get(key)):
            return True
    named_sets = access_context.get("named_sets")
    if isinstance(named_sets, Mapping):
        for raw_set in named_sets.values():
            refs = raw_set.get("refs") if isinstance(raw_set, Mapping) else raw_set
            if _has_source_record_ref(refs):
                return True
    return False


def _has_source_record_ref(refs: Any) -> bool:
    if isinstance(refs, (str, bytes)):
        return str(refs).startswith("source_record:")
    if not isinstance(refs, Iterable):
        return False
    return any(str(ref).startswith("source_record:") for ref in refs)


def _access_context_match_facts(
    resource_ref: str,
    access_context: Mapping[str, Any],
) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    set_ids: list[str] = []
    if _ref_in_context(resource_ref, access_context.get("target_refs")):
        facts["is_context_target"] = True
        facts["context_target_ref"] = resource_ref
    if _ref_in_context(resource_ref, access_context.get("hidden_refs")):
        facts["is_hidden_ref"] = True
        facts["hidden_ref"] = resource_ref
    holdout_refs = access_context.get("holdout_refs")
    holdout_set_id = access_context.get("holdout_set_id")
    if _ref_in_context(resource_ref, holdout_refs):
        facts["is_holdout"] = True
        if holdout_set_id is not None:
            set_ids.append(str(holdout_set_id))
    named_sets = access_context.get("named_sets")
    if isinstance(named_sets, Mapping):
        for raw_set_id, raw_set in named_sets.items():
            refs = raw_set.get("refs") if isinstance(raw_set, Mapping) else raw_set
            if _ref_in_context(resource_ref, refs):
                set_ids.append(str(raw_set_id))
    if set_ids:
        unique_set_ids = sorted(dict.fromkeys(set_ids))
        facts["set_ids"] = unique_set_ids
        facts["set_refs"] = [
            ref for ref in (named_set_ref(set_id) for set_id in unique_set_ids)
            if ref is not None
        ]
        if len(unique_set_ids) == 1:
            facts["set_id"] = unique_set_ids[0]
    return facts


def _ref_in_context(resource_ref: str, refs: Any) -> bool:
    if isinstance(refs, (str, bytes)):
        return resource_ref == str(refs)
    if not isinstance(refs, Iterable):
        return False
    return resource_ref in {str(ref) for ref in refs}


def _horizon_facts(
    contributor: Mapping[str, Any],
    access_context: Mapping[str, Any],
) -> dict[str, Any]:
    as_of = access_context.get("as_of")
    if as_of is None:
        return {}
    facts: dict[str, Any] = {"as_of": as_of}
    ref = horizon_ref(as_of)
    if ref is not None:
        facts["horizon_ref"] = ref
    as_of_dt = _parse_timestamp(as_of)
    observed_at = _contributor_timestamp(contributor)
    if observed_at is not None:
        facts["source_record_observed_at"] = observed_at.isoformat()
    if as_of_dt is not None and observed_at is not None:
        facts["is_after_as_of"] = observed_at > as_of_dt
    return facts


def _contributor_timestamp(contributor: Mapping[str, Any]) -> datetime | None:
    for key in ("source_revision", "source_revisions"):
        raw = contributor.get(key)
        if raw is None:
            continue
        values = raw if isinstance(raw, list) else [raw]
        for value in values:
            timestamp = _timestamp_from_revision(value)
            if timestamp is not None:
                return timestamp
    return None


def _timestamp_from_revision(value: Any) -> datetime | None:
    if isinstance(value, Mapping):
        for key in ("updated_at", "observed_at", "created_at", "timestamp", "ts"):
            if key in value:
                parsed = _parse_timestamp(value[key])
                if parsed is not None:
                    return parsed
        return None
    return _parse_timestamp(value)


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _node_source_lookup(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    ids = sorted(
        {
            str(row["node_id"])
            for row in rows
            if row.get("node_id") and "source" not in row
        }
    )
    if not ids:
        return {}
    graph = _checkout(conn, session)
    return {
        str(row["node_id"]): dict(row)
        for row in graph.query(
            """
            SELECT node_id, attrs, source, source_record_id, source_revision
              FROM okg.graph_nodes
             WHERE node_id = ANY(:ids)
            """,
            {"ids": ids},
        )
    }


def _edge_source_lookup(
    conn: psycopg.Connection,
    session: Any,
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    ids = sorted(
        {
            str(row["edge_id"])
            for row in rows
            if row.get("edge_id") and "source" not in row
        }
    )
    if not ids:
        return {}
    graph = _checkout(conn, session)
    return {
        str(row["edge_id"]): dict(row)
        for row in graph.query(
            """
            SELECT edge_id, attrs, source, source_record_id, source_revision
              FROM okg.graph_edges
             WHERE edge_id = ANY(:ids)
            """,
            {"ids": ids},
        )
    }


def _checkout(conn: psycopg.Connection, session: Any) -> Any:
    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
    ).checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )
