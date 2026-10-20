"""The reader's view: decide every catalogued read class once, filter in SQL.

PACT ``lane-h-nomos-read-class-sql`` (Nomos SQL read filter, slice 2; design
``pact/changes/lane-h-nomos-sql-read-filter/design.md`` sections 4.1, 5.4, 6,
7 and test T3b). Interface agreed with lane B's read model (design #3191,
section 3.1, decisions D3, D5, D13, D14).

One call::

    view = reader_view.open(conn, session, row_kind="node", action=..., surface=...)
    rows = query(... WHERE <view.predicate("n")> ...)
    served = view.finish(rows)

``open`` decides every class in ``okg.nomos_read_classes`` once, in one
``read_pass_scope`` against the loaded bundle, from a resource built from the
class's stored facts (the same facts the class id is the digest of). A class
is decided under every classification that can behave differently: each value
an active rule names, ``internal``, one unnamed value and one blank value. The
evaluation is the real one (the runtime boundary behind
``evaluate_with_active_policy``, the same enforcement points, subject, action
and enforcement mode as the per-row filter), so no rule is translated into
SQL; SQL only receives the answer. These class decisions are NOT recorded
(Jason, 2026-10-02): the boundary is called through its storage-adapter seam
with a no-op persist.

``predicate(alias)`` keeps a row when its class and classification are
visible, and keeps every row the view cannot place (no class, another
vocabulary version, a class missing from the catalog, an unusual
classification); it never drops a row the per-row filter would serve.
Callers pass ``finish`` rows as stored (full ``attrs``, or none so the
filter loads them, and ``subtype`` for nodes); a node row without ``subtype``
is refused. ``open`` records nothing and does not commit; ``finish`` commits
``conn`` when it records, as the per-row filter does.

``finish(rows)`` is the per-row filter itself (``filter_<kind>_rows_for_session``)
over the rows the caller serves: the same rows, order and attrs byte for byte,
and exactly today's per-row read decisions for every row given to it, nothing more
(Jason, 2026-10-02: no scan-site decisions; this supersedes D13's scan-site
part). ``sql_exact`` says when the predicate alone already equals what
``finish`` serves; decided without a query, it is true only with Nomos off
(see ``ReaderView.exactness``).

The whole call takes the per-row path (``mode == "per_row"``, predicate
``TRUE``) when the session carries an identity claim or an access context,
when an active rule that can match names a key outside
``READ_CLASS_SAFE_RULE_KEYS``, when the graph tables lack the class columns,
when the catalog table is absent, or when the caller forces it.

No active bundle (``mode == "no_bundle"``, read-model slice 7a): the per-row
filter then answers every row alike with the evaluator's
``nomos.default_deny.no_active_bundle`` (audit allows it, enforce applies
it), hides a row with no source lineage, and redacts nothing
(``read_filter._read_class_member_row(..., redact_with=None)`` on the class
path; every per-row decision is the same allow or deny). ``open`` evaluates
that one decision unrecorded. When it serves (audit), ``predicate`` is TRUE
and every key is readable (``redacts`` FALSE): SQL on the effective value
is exact for every row the per-row filter serves (served unredacted, with
overlays applied after the filter), and ``finish`` drops a row
with no source lineage; such a row's value may still decide whether it is
scanned (the disclosed limitation for rows without lineage). When it does
not serve (enforce), ``predicate`` is FALSE; MCP refuses that call earlier
at ``mcp.tool.call``, so this is a backstop. ``check_bundle_unchanged``
refuses the read when a bundle was activated after ``open``.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from psycopg import sql

from okg.substrate.commit_scope import read_pass_scope
from okg.substrate.errors import SubstrateError
from okg.substrate.nomos import read_class as rc
from okg.substrate.nomos import read_filter as rf
from okg.substrate.nomos import store as nomos_store
from okg.substrate.nomos.core import DecisionInput, PolicyDecision
from okg.substrate.nomos.pii_classification import resolve_pii_class
from okg.substrate.nomos.read_masking import (
    MASK_TOKEN,
    READ_DROP_CLASSES,
    REVEAL_CLASSES,
    ReadRedactedAttrs,
    is_governance_key,
    safe_decision_payload,
    subtype_attr_classifier,
)

MODES = ("off", "class", "no_bundle", "per_row")
FORCES = (None, "per_row", "shadow")
ROW_KINDS = ("node", "edge")

#: The key a non-JSON-shaped target carries in a resource built from class
#: facts: a value the boundary refuses, so the decision is the same fail-closed
#: decision the per-row filter gets for the row's malformed resource.
_NOT_JSON_KEY = "_okg_read_class_not_json"

#: A classification is "other" (behaves like any value no rule names and that
#: is not blank) when it is made only of these characters. Spelled out, not a
#: range: a bracket range in a PostgreSQL regex depends on the collation.
_SAFE_CHARS = (
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
)
_SAFE_RE = re.compile("[" + re.escape(_SAFE_CHARS) + "]+")
_SAFE_SQL_REGEX = "^[" + _SAFE_CHARS.replace("-", "") + "-]+$"

OTHER_LABEL = "other"
BLANK_LABEL = "blank"
_OTHER_BASE = "nomos-read-class-unnamed"
_BLANK_CLASSIFICATION = " "

#: Values ``str(attrs.get("classification") or "internal")`` turns into
#: ``internal``: every falsy JSON value.
_FALSY_JSON = ("null", "false", '""', "0", "[]", "{}")

_POSITION_TAG = "_okg_reader_view_position"


class ReaderViewShadowMismatch(SubstrateError):
    """``force="shadow"``: the class path and the per-row path disagree."""


class ReaderViewRowShapeError(SubstrateError):
    """``finish`` received a row it cannot mask as ``readable`` predicted."""


# --------------------------------------------------------------------------- #
# Classification labels                                                        #
# --------------------------------------------------------------------------- #
def _md5(text: str) -> str:
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def named_classifications(bundle: Any) -> frozenset[str]:
    """Every classification string a rule of ``bundle`` names, plus
    ``internal``. A classification no rule names and that is not blank
    behaves like every other such value: a rule compares it for equality only
    (``core._value_matches``) and the protected-identity check reads only
    whether it is a non-blank string."""
    names = {"internal"}
    for rule in getattr(bundle, "rules", ()) or ():
        expected = rule.match.get("classification") if "classification" in rule.match else None
        values = expected if isinstance(expected, list) else [expected]
        names.update(value for value in values if isinstance(value, str))
    return frozenset(names)


def classification_label(classification: str, named_labels: frozenset[str]) -> str | None:
    """The label the predicate's SQL gives a row's effective classification:
    its md5 when a rule names it (or it is ``internal``), ``other`` when it is
    made only of safe characters, else None (unusual: the view cannot place
    the row, and keeps it for the per-row filter)."""
    digest = _md5(classification)
    if digest in named_labels:
        return digest
    if _SAFE_RE.fullmatch(classification):
        return OTHER_LABEL
    return None


@dataclass(frozen=True)
class _Probe:
    label: str
    classification: str


def _probes(named: frozenset[str]) -> tuple[_Probe, ...]:
    other = _OTHER_BASE
    while other in named:
        other += "-x"
    probes = [_Probe(_md5(name), name) for name in sorted(named)]
    probes.append(_Probe(OTHER_LABEL, other))
    if _BLANK_CLASSIFICATION not in named:
        probes.append(_Probe(BLANK_LABEL, _BLANK_CLASSIFICATION))
    return tuple(probes)


# --------------------------------------------------------------------------- #
# Masks                                                                        #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AttrMask:
    """What the read filter's redact transform does to one subtype's attrs.

    ``action(key)`` is ``reveal``, ``drop`` (the key is removed), ``mask``
    (replaced by ``MASK_TOKEN``) or ``pseudonymize``. The read path passes no
    pseudonym service to ``mask_attrs``, so identifier classes are masked, and
    ``pseudonymize`` never occurs on reads today. ``declared`` is whether the
    subtype declares ``pii_classes``; for a declared subtype every key it does
    not list is masked (fail-safe), for an undeclared one only the legacy
    content keys are dropped.
    """

    subtype: str
    declared: bool
    classes: Mapping[str, str] | None
    #: An "off" view masks nothing.
    reveal_all: bool = False

    def action(self, key: str) -> str:
        if self.reveal_all or is_governance_key(key):
            return "reveal"
        classifier = subtype_attr_classifier(self.classes if self.declared else None)
        pii_class = resolve_pii_class(classifier(key))
        if pii_class in REVEAL_CLASSES:
            return "reveal"
        if pii_class in READ_DROP_CLASSES:
            return "drop"
        # The read filter calls mask_attrs with no pseudonym service.
        return "mask"

    def masked(self, key: str) -> bool:
        return self.action(key) != "reveal"


# --------------------------------------------------------------------------- #
# Class decisions                                                              #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ClassOutcome:
    visible: bool
    redacted: bool


def _parse_target(text: str) -> tuple[dict[str, Any], bool, bool]:
    fail_closed = text.endswith(":fail_closed")
    if fail_closed:
        text = text[: -len(":fail_closed")]
    pairs, json_ok = json.loads(text)
    return {key: json.loads(value) for key, value in pairs}, bool(json_ok), fail_closed


def class_resources(
    facts: Mapping[str, Any],
    *,
    deployment: str,
    scope: str,
    kind: str,
    audit: bool = False,
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], bool]]]:
    """The resources the per-row filter would evaluate for a row of this
    class, rebuilt from the class facts (design T3b): one per source id, and
    one per fine-grained target with its fail-closed flag.

    Each carries the call constants and every class-fact key of the real
    resource; the keys it lacks (row ids, references, the lineage) are ones no
    rule may name on the class path and the protected-identity check does not
    read. A target whose real resource was not JSON-shaped gets a value the
    boundary refuses, so it is refused the same way.
    """
    base = {"deployment": deployment, "scope": scope, "kind": kind, "audit": audit}

    def build(text: str) -> tuple[dict[str, Any], bool]:
        values, json_ok, fail_closed = _parse_target(text)
        resource = {**base, **values}
        if not json_ok:
            resource[_NOT_JSON_KEY] = float("nan")
        return resource, fail_closed

    by_source = [build(text)[0] for text in facts.get("by_source") or ()]
    fine_grained = [build(text) for text in facts.get("fine_grained") or ()]
    return by_source, fine_grained


class _NoRecordStorage:
    """The runtime boundary's storage-adapter seam, used to decide without
    recording: the bundle comes from the same active-policy loader the
    per-row filter uses (memoized by the caller's ``read_pass_scope``), and
    the decision is not persisted. Persistence is audit only; the decision
    itself is computed by the same boundary code."""

    @staticmethod
    def load_active_policy_bundle(conn: Any, *, deployment_name: str, scope: str) -> Any:
        return nomos_store.load_active_policy_bundle(
            conn, deployment_name=deployment_name, scope=scope,
        )

    @staticmethod
    def persist_nomos_decision(conn: Any, **_: Any) -> None:
        return None


def _evaluate_unrecorded(
    conn: Any,
    *,
    deployment_name: str,
    scope: str,
    decision_input: DecisionInput,
    enforcement_point: str,
    performed_obligations: Sequence[str],
    payload: Any,
) -> PolicyDecision:
    """``store.evaluate_with_active_policy`` without persisting the decision:
    the same surface resolution, trusted facts, enforcement mode and
    audit-mode caller effect."""
    from okg.substrate.nomos.runtime_enforcement import NomosRuntimeRequest

    surface = rf.surface_for_action(decision_input.action, fallback_surface=enforcement_point)
    outcome = nomos_store.evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=scope,
        request=NomosRuntimeRequest(
            surface=surface,
            subject=decision_input.subject,
            resource=decision_input.resource,
            classification=decision_input.classification,
            context=decision_input.context,
        ),
        target={"kind": "reader_view_class_decision", "scope": scope},
        performed_obligations=performed_obligations,
        payload=payload,
        storage_adapter=_NoRecordStorage(),
    )
    return nomos_store._effective_decision_for_callers(outcome)


def decide_classes(
    conn: Any,
    session: Any,
    catalog: Mapping[uuid.UUID, Mapping[str, Any]],
    *,
    kind: str,
    action: str,
    surface: str,
    probes: Sequence[_Probe],
    expected_fingerprint: Any,
) -> dict[uuid.UUID, dict[str, ClassOutcome]]:
    """Decide every class under every probe classification. Nothing is
    recorded (Jason, 2026-10-02: no scan-site decisions); every row given to
    ``finish`` keeps today's per-row read decisions. Call inside the read pass
    scope that loaded the bundle."""
    scope = f"deployment:{session.deployment_name}"
    subject = (
        getattr(session, "nomos_subject", None)
        or rf.nomos_subject_from_env(default_kind="mcp_client")
    )
    payload = safe_decision_payload({})
    seen: set[Any] = set()

    def evaluate(
        resource: Mapping[str, Any], *, class_id: uuid.UUID, probe: _Probe, by_source: bool,
    ) -> PolicyDecision:
        point = "source_lineage_filter" if by_source else "fine_grained_lineage_filter"
        decision = _evaluate_unrecorded(
            conn,
            deployment_name=session.deployment_name,
            scope=scope,
            decision_input=DecisionInput(
                subject=subject,
                action=action,
                classification=probe.classification,
                resource=resource,
                context={},
            ),
            enforcement_point=f"{surface}.{point}",
            performed_obligations=("audit", "redaction"),
            payload=payload,
        )
        seen.add(decision.policy_fingerprint)
        return decision

    out: dict[uuid.UUID, dict[str, ClassOutcome]] = {}
    for class_id in sorted(catalog, key=str):
        by_source, fine_grained = class_resources(
            catalog[class_id], deployment=session.deployment_name, scope=scope, kind=kind,
        )
        per_probe: dict[str, ClassOutcome] = {}
        for probe in probes:
            kept: list[PolicyDecision] = []
            hidden = False
            for resource in by_source:
                decision = evaluate(resource, class_id=class_id, probe=probe, by_source=True)
                if decision.effect in rf._HIDE_EFFECTS:
                    hidden = True
                    break
                kept.append(decision)
            if not hidden:
                for resource, fail_closed in fine_grained:
                    decision = evaluate(resource, class_id=class_id, probe=probe, by_source=False)
                    if fail_closed or (
                        decision.effect in rf._HIDE_EFFECTS and decision.matched_rule_ids
                    ):
                        hidden = True
                        break
                    if decision.matched_rule_ids:
                        kept.append(decision)
            per_probe[probe.label] = ClassOutcome(
                visible=not hidden,
                redacted=not hidden and any(d.effect == "redact" for d in kept),
            )
        out[class_id] = per_probe
    if seen and seen != {expected_fingerprint}:
        raise SubstrateError(
            "reader view: a class was decided under a different policy than the "
            "one the view loaded; refusing the read",
            data={
                "expected_policy_fingerprint": expected_fingerprint,
                "decided_policy_fingerprints": sorted(str(f) for f in seen),
            },
        )
    return out


# --------------------------------------------------------------------------- #
# Database lookups (each one statement; patched in the in-memory tests)        #
# --------------------------------------------------------------------------- #
def _regclass(conn: Any, name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
        row = cur.fetchone()
    return bool(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))


def _columns_present(conn: Any, kind: str) -> bool:
    """Whether the Chronos registration of the kind's graph table carries the
    two class columns (what a checkout's visible-row subquery selects)."""
    table = "okg.graph_nodes" if kind == "node" else "okg.graph_edges"
    if not _regclass(conn, "public._chronos_branch_tables"):
        return False
    with conn.cursor() as cur:
        cur.execute(
            "SELECT columns FROM _chronos_branch_tables WHERE table_name = %s", (table,),
        )
        row = cur.fetchone()
    if row is None:
        return False
    raw = row[0] if not isinstance(row, Mapping) else row["columns"]
    columns = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    return all(column in (columns or ()) for column in rc.READ_CLASS_COLUMNS)


def _catalog_present(conn: Any) -> bool:
    return _regclass(conn, rc.READ_CLASS_CATALOG_TABLE)


def _load_catalog(conn: Any) -> dict[str, dict[uuid.UUID, dict[str, Any]]]:
    """Every catalogued class of the running vocabulary, by kind. Each row's
    id is checked against the digest of its facts: a row that does not match
    refuses the read."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT CAST(read_class AS text), class_facts FROM {rc.READ_CLASS_CATALOG_TABLE}"
            " WHERE vocabulary_version = %s",
            (rc.CLASS_VOCABULARY_VERSION,),
        )
        rows = cur.fetchall()
    out: dict[str, dict[uuid.UUID, dict[str, Any]]] = {kind: {} for kind in ROW_KINDS}
    for row in rows:
        class_text, facts = (tuple(row.values()) if isinstance(row, Mapping) else tuple(row))
        if isinstance(facts, (str, bytes)):
            facts = json.loads(facts)
        class_id = uuid.UUID(str(class_text))
        if rc.class_id_of_facts(facts) != class_id:
            raise SubstrateError(
                "reader view: a read-class catalog row does not match its facts; "
                "refusing the read",
                data={"read_class": str(class_id)},
                fix_hint="the catalog is insert-once; find what rewrote this row",
            )
        kind = facts.get("kind")
        if kind in out:
            out[kind][class_id] = facts
    return out


def clear_reader_view_cache() -> None:
    """Nothing is cached across calls: ``_overlay_present`` reads the
    database on every ``open`` (a cached answer went stale when a
    classification overlay was materialized at an already-read generation).
    Kept so existing callers need no change."""


def _published(session: Any) -> bool:
    return session.generation_id is not None and str(session.generation_id) != "0"


def _overlay_present(conn: Any, session: Any, kind: str) -> bool:
    """Whether an accepted ``classification`` overlay exists for this kind at
    the session's generation. The working generation is always treated as
    having one (its overlays can change under the read). Read on every call,
    never cached: an overlay can be materialized at a generation already
    read."""
    if not _published(session):
        return True
    from okg.substrate.attributes import ATTRIBUTE_EFFECTIVE_TABLE, DEFAULT_NAMESPACE

    present = False
    if _regclass(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE}"
                " WHERE target_kind = %s AND generation_id = %s AND namespace = %s"
                " AND attr_key = 'classification')",
                (kind, str(session.generation_id), DEFAULT_NAMESPACE),
            )
            row = cur.fetchone()
        present = bool(row[0] if not isinstance(row, Mapping) else next(iter(row.values())))
    return present


def _generation_query(conn: Any, session: Any, text: str, params: Mapping[str, Any]) -> list[dict]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    return store.query_generation_once(
        session.generation_id, text, dict(params), branch=session.branch_name,
    )


# --------------------------------------------------------------------------- #
# SQL pieces                                                                   #
# --------------------------------------------------------------------------- #
def _alias(alias: str | sql.Composable) -> sql.Composable:
    return alias if isinstance(alias, sql.Composable) else sql.Identifier(alias)


def _col(alias: str | sql.Composable, column: str) -> sql.Composable:
    return sql.SQL("{}.{}").format(_alias(alias), sql.Identifier(column))


def _array(values: Iterable[str], pg_type: str) -> sql.Composable:
    return sql.SQL("CAST(ARRAY[{}] AS {})").format(
        sql.SQL(", ").join(sql.Literal(str(v)) for v in values), sql.SQL(pg_type),
    )


def _label_sql(alias: str | sql.Composable, named_labels: Iterable[str]) -> sql.Composable:
    """The classification label of a row from its STORED attrs, mirroring
    ``str(attrs.get("classification") or "internal")`` then
    ``classification_label``; NULL where the SQL cannot be sure (a truthy
    non-string value, a string with characters outside the safe set that no
    rule names)."""
    j = sql.SQL("({} -> 'classification')").format(_col(alias, "attrs"))
    falsy = sql.SQL(", ").join(sql.SQL("CAST({} AS jsonb)").format(sql.Literal(v)) for v in _FALSY_JSON)
    text = sql.SQL("({} #>> '{{}}')").format(j)
    return sql.SQL(
        "(CASE WHEN {j} IS NULL OR {j} IN ({falsy}) THEN {internal}"
        " WHEN jsonb_typeof({j}) = 'string' THEN"
        " CASE WHEN md5({text}) = ANY({named}) THEN md5({text})"
        " WHEN {text} ~ {safe} THEN {other} ELSE NULL END"
        " ELSE NULL END)"
    ).format(
        j=j, falsy=falsy, internal=sql.Literal(_md5("internal")), text=text,
        named=_array(sorted(named_labels), "text[]"), safe=sql.Literal(_SAFE_SQL_REGEX),
        other=sql.Literal(OTHER_LABEL),
    )


# --------------------------------------------------------------------------- #
# The view                                                                     #
# --------------------------------------------------------------------------- #
@dataclass
class _KindState:
    kind: str
    mode: str
    reason: str
    decisions: dict[uuid.UUID, dict[str, ClassOutcome]]
    overlay: bool
    #: ``no_bundle`` mode: whether the no-bundle default decision serves rows.
    serves: bool = False


class ReaderView:
    """One reader's view of one row kind for one call. See the module doc."""

    def __init__(
        self,
        *,
        conn: Any,
        session: Any,
        state: _KindState,
        action: str,
        surface: str,
        force: str | None,
        bundle_fingerprint: Any,
        named: frozenset[str],
        subtype_pii: Mapping[str, Mapping[str, Any]] | None,
        fingerprint_inputs: Mapping[str, Any],
    ) -> None:
        self._conn = conn
        self._session = session
        self._state = state
        self.row_kind = state.kind
        self.mode = state.mode
        self.reason = state.reason
        self.action = action
        self.surface = surface
        self.force = force
        self.bundle_fingerprint = bundle_fingerprint
        self._named = named
        self._named_labels = frozenset(_md5(name) for name in named)
        self._subtype_pii = dict(subtype_pii or {})
        self._fingerprint_inputs = dict(fingerprint_inputs)
        self.fingerprint = fingerprint_of(self._fingerprint_inputs)
        decisions = state.decisions
        self.class_version = rc.CLASS_VOCABULARY_VERSION if self.mode == "class" else None
        self.decided_classes = frozenset(decisions)
        self.visible_classes = frozenset(
            c for c, outcomes in decisions.items() if any(o.visible for o in outcomes.values())
        )
        self._visible_pairs = sorted(
            f"{c}|{label}" for c, outcomes in decisions.items()
            for label, o in outcomes.items() if o.visible and label != BLANK_LABEL
        )
        self._redacted_pairs = sorted(
            f"{c}|{label}" for c, outcomes in decisions.items()
            for label, o in outcomes.items() if o.visible and o.redacted and label != BLANK_LABEL
        )
        # Overlay mode: a class's redaction is known only when every visible
        # classification of it agrees.
        self._always_redacted = frozenset(
            c for c, outcomes in decisions.items()
            if any(o.visible for o in outcomes.values())
            and all(o.redacted for o in outcomes.values() if o.visible)
        )
        # Overlay mode places a row by its class alone, but the predicate
        # keeps a class visible under SOME classification, so a kept row may
        # be hidden under the classification an overlay gave it. A key is
        # therefore decided in SQL only for a class that every probed
        # classification serves (and, for a masked key, serves unredacted);
        # any other class is undecided (fail closed).
        self._all_visible = frozenset(
            c for c, outcomes in decisions.items()
            if outcomes and all(o.visible for o in outcomes.values())
        )
        self._never_redacted = frozenset(
            c for c in self._all_visible
            if not any(o.redacted for o in decisions[c].values())
        )

    # ---------------------------------------------------------------- SQL --
    def _known_class(self, alias: str | sql.Composable) -> sql.Composable:
        cls = _col(alias, rc.READ_CLASS_COLUMN)
        # Strictly boolean: a NULL here must never turn the predicate NULL.
        return sql.SQL(
            "COALESCE({c} IS NOT NULL AND {v} IS NOT DISTINCT FROM {ver}"
            " AND {c} = ANY({decided}), FALSE)"
        ).format(
            c=cls, v=_col(alias, rc.READ_CLASS_VERSION_COLUMN),
            ver=sql.Literal(rc.CLASS_VOCABULARY_VERSION),
            decided=_array(sorted(map(str, self.decided_classes)), "uuid[]"),
        )

    def predicate(self, alias: str | sql.Composable) -> sql.Composable:
        """A boolean SQL expression over the row ``alias`` (an
        ``okg.graph_nodes`` / ``okg.graph_edges`` row with its ``attrs`` and
        the two class columns): TRUE for every row the per-row filter could
        serve. ``TRUE`` when the view is not in class mode, except ``FALSE``
        with no active bundle when its default decision serves nothing."""
        if self.mode == "no_bundle":
            return sql.SQL("TRUE" if self._state.serves else "FALSE")
        if self.mode != "class":
            return sql.SQL("TRUE")
        cls = _col(alias, rc.READ_CLASS_COLUMN)
        any_visible = sql.SQL("{} = ANY({})").format(
            cls, _array(sorted(map(str, self.visible_classes)), "uuid[]"),
        )
        if self._state.overlay:
            visible = any_visible
        else:
            label = _label_sql(alias, self._named_labels)
            visible = sql.SQL(
                "(CASE WHEN {label} IS NULL THEN {any_visible}"
                " ELSE (CAST({c} AS text) || '|' || {label}) = ANY({pairs}) END)"
            ).format(
                label=label, any_visible=any_visible, c=cls,
                pairs=_array(self._visible_pairs, "text[]"),
            )
        # COALESCE: any NULL keeps the row (the predicate is a superset).
        return sql.SQL("COALESCE(NOT {known} OR {visible}, TRUE)").format(
            known=self._known_class(alias), visible=visible,
        )

    def _revealed(self, alias: str | sql.Composable, attr_key: str) -> sql.Composable:
        """TRUE where the row's subtype reveals ``attr_key`` even when its class
        is redacted."""
        if self.row_kind == "edge":
            # The per-row filter reads ``row.get("subtype")``; edge rows have
            # none, so every edge masks with the "" subtype's classifier.
            return sql.Literal(not self.masked_attrs("").masked(attr_key))
        declared = sorted(self._subtype_pii)
        revealed = [s for s in declared if not self.masked_attrs(s).masked(attr_key)]
        subtype = sql.SQL("COALESCE({}, '')").format(_col(alias, "subtype"))
        legacy_reveals = not AttrMask("", False, None).masked(attr_key)
        if legacy_reveals:
            hiding = [s for s in declared if s not in revealed]
            return sql.SQL("NOT ({} = ANY({}))").format(subtype, _array(hiding, "text[]"))
        return sql.SQL("{} = ANY({})").format(subtype, _array(revealed, "text[]"))

    def readable(self, alias: str | sql.Composable, attr_key: str) -> sql.Composable:
        """A boolean SQL expression: TRUE when a served row's ``attr_key`` is
        returned as stored (its class is not redacted for its classification,
        or its subtype reveals the key), FALSE when it is masked or dropped,
        NULL (unknown) when the view cannot tell before ``finish``: a row with
        no current catalogued class, an unusual classification, or (with a
        classification overlay in scope) a class that some probed
        classification hides (or, for a masked key, redacts). D5: a clause on an unknown value is unknown, so a caller
        composes ``CASE WHEN readable THEN <clause> END``. ``"text"`` also
        governs the searchable-text copy (``mask_attrs`` gives it ``text``'s
        class); ``attrs.path`` and grep's path term use ``readable(a, "path")``.
        """
        if self.mode == "off":
            return sql.SQL("TRUE")
        if self.mode == "no_bundle":
            # Nothing is redacted with no bundle: a served row is read as
            # stored on every key. A row without source lineage is not
            # served, but its stored value may still decide whether it is
            # scanned (disclosed limitation case 1, as for unplaced rows in
            # class mode). With nothing served the predicate drops every row.
            return sql.SQL("TRUE" if self._state.serves else "CAST(NULL AS boolean)")
        revealed = self._revealed(alias, attr_key)
        if self.mode != "class":
            return sql.SQL("(CASE WHEN {} THEN TRUE ELSE NULL END)").format(revealed)
        cls = _col(alias, rc.READ_CLASS_COLUMN)
        if self._state.overlay:
            # A revealed key is served as stored whenever the row is served,
            # so it is decided only where no probed classification hides the
            # class; a masked key only where none hides or redacts it.
            return sql.SQL(
                "(CASE WHEN {revealed} THEN"
                " (CASE WHEN {known} AND {c} = ANY({all_visible}) THEN TRUE ELSE NULL END)"
                " WHEN {known} AND {c} = ANY({never}) THEN TRUE"
                " WHEN {known} AND {c} = ANY({always}) THEN FALSE"
                " ELSE NULL END)"
            ).format(
                revealed=revealed, known=self._known_class(alias), c=cls,
                all_visible=_array(sorted(map(str, self._all_visible)), "uuid[]"),
                never=_array(sorted(map(str, self._never_redacted)), "uuid[]"),
                always=_array(sorted(map(str, self._always_redacted)), "uuid[]"),
            )
        label = _label_sql(alias, self._named_labels)
        return sql.SQL(
            "(CASE WHEN {revealed} THEN TRUE"
            " WHEN {known} AND {label} IS NOT NULL THEN"
            " NOT ((CAST({c} AS text) || '|' || {label}) = ANY({redacted}))"
            " ELSE NULL END)"
        ).format(
            revealed=revealed, known=self._known_class(alias), label=label, c=cls,
            redacted=_array(self._redacted_pairs, "text[]"),
        )

    def redacts(self, alias: str | sql.Composable) -> sql.Composable:
        """A boolean SQL expression: TRUE when a served row comes back
        redacted (the per-row filter serves its STORED attrs, masked, and no
        later overlay applies to them), FALSE when it is served unredacted,
        NULL when the view cannot tell before ``finish`` (the same unknown
        cases as ``readable``, and every row outside class mode). Decided from
        the stored class and classification only, never a value. Read-model
        slice 7a uses it on edges: a redacted edge is served its stored
        attrs, an unredacted one its overlaid attrs, so an overlaid value may
        decide a clause in SQL only where this is FALSE."""
        if self.mode == "off":
            return sql.SQL("FALSE")
        if self.mode == "no_bundle":
            return sql.SQL("FALSE" if self._state.serves else "CAST(NULL AS boolean)")
        if self.mode != "class":
            return sql.SQL("CAST(NULL AS boolean)")
        cls = _col(alias, rc.READ_CLASS_COLUMN)
        if self._state.overlay:
            return sql.SQL(
                "(CASE WHEN {known} AND {c} = ANY({never}) THEN FALSE"
                " WHEN {known} AND {c} = ANY({always}) THEN TRUE"
                " ELSE NULL END)"
            ).format(
                known=self._known_class(alias), c=cls,
                never=_array(sorted(map(str, self._never_redacted)), "uuid[]"),
                always=_array(sorted(map(str, self._always_redacted)), "uuid[]"),
            )
        label = _label_sql(alias, self._named_labels)
        return sql.SQL(
            "(CASE WHEN {known} AND {label} IS NOT NULL THEN"
            " (CAST({c} AS text) || '|' || {label}) = ANY({redacted})"
            " ELSE NULL END)"
        ).format(
            known=self._known_class(alias), label=label, c=cls,
            redacted=_array(self._redacted_pairs, "text[]"),
        )

    def label_sql(self, alias: str | sql.Composable) -> sql.Composable:
        """The classification label the predicate gives a row (for tests and
        the shadow mode)."""
        return _label_sql(alias, self._named_labels)

    # -------------------------------------------------------------- masks --
    def masked_attrs(self, subtype: str | None) -> AttrMask:
        """The redact transform of rows of ``subtype`` (``""`` for edges), as
        the per-row filter applies it when a row's class is redacted."""
        key = str(subtype or "")
        if self.mode == "off":
            return AttrMask(key, False, None, reveal_all=True)
        declared = key in self._subtype_pii
        return AttrMask(key, declared, self._subtype_pii.get(key) if declared else None)

    def pseudonymized(self, subtype: str | None) -> frozenset[str]:
        """Attribute keys of ``subtype`` a redacted read returns as a pseudonym.
        Always empty today: the read path passes no pseudonym service to
        ``mask_attrs``, so identifier classes are masked (``MASK_TOKEN``)."""
        return frozenset()

    @property
    def serves_unredacted(self) -> bool:
        """True only in ``no_bundle`` mode when the no-bundle default
        decision serves rows (audit): every key is then readable in SQL as
        with Nomos off, so a caller may narrow candidates by value exactly as
        with Nomos off. The rows ``finish`` drops (no source lineage) can then
        decide only whether they are scanned toward the scan bound (the
        disclosed limitation for rows without lineage)."""
        return self.mode == "no_bundle" and self._state.serves

    # ------------------------------------------------------------- finish --
    def _per_row(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        exact = (
            rf.filter_node_rows_for_session if self.row_kind == "node"
            else rf.filter_edge_rows_for_session
        )
        return exact(self._conn, self._session, rows, action=self.action, surface=self.surface)

    def finish(self, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """The rows the reader is served, in the caller's order, exactly as
        the per-row filter returns them (the per-row filter decides them, and
        records today's read decisions for every row given to ``finish``,
        hidden ones included). Off: the rows, unchanged, unless Nomos has been
        turned on since ``open``: then the per-row path (fail closed). ``force="shadow"``: also checks the class path against the
        per-row path and raises ``ReaderViewShadowMismatch`` on a difference.

        Callers pass rows as stored: full ``attrs`` (at least the stored
        ``classification``) or no ``attrs`` key at all (the filter then loads
        the stored row), and, for nodes, the ``subtype`` column. The masks
        ``finish`` applies key on the row's own subtype and classification, so
        a projected row would be masked differently from what ``predicate``
        and ``readable`` computed from the stored row. A node row without
        ``subtype`` is refused (``ReaderViewRowShapeError``), never guessed."""
        rows = [dict(row) for row in rows]
        if self.mode == "off":
            if not rf.deployment_requires_nomos_runtime(self._session.deployment_name):
                return rows
            # Nomos was turned on after open(): fail closed, per row.
            self.mode, self.reason = "per_row", "nomos_turned_on_after_open"
        if self.row_kind == "node":
            missing = [row.get("node_id") for row in rows if "subtype" not in row]
            if missing:
                raise ReaderViewRowShapeError(
                    "reader view: node rows passed to finish() must carry their "
                    "subtype; the masks key on it",
                    data={"node_ids": [str(m) for m in missing[:20]],
                          "missing_rows": len(missing)},
                    fix_hint="select the subtype column with the row",
                )
        if self.force == "shadow" and self.mode == "class":
            return self._shadow_finish(rows)
        if self.force == "shadow" and self.mode == "no_bundle":
            return self._shadow_no_bundle(rows)
        return self._per_row(rows)

    def check_bundle_unchanged(self) -> None:
        """Refuse the read (``SubstrateError``, "reader view: ...") when the
        active policy bundle is not the one the view opened with: in
        ``no_bundle`` mode any active bundle, in ``class`` mode a different
        fingerprint or none. The view's SQL decided and ordered keys on the
        bundle it opened with, while the per-row filter reloads the bundle on
        every call, so a caller runs this after its LAST per-row read. The
        MCP page path never reaches ``read_filter._decide_no_active_bundle``
        once a bundle is active (``_filter_rows`` loads the new bundle), so
        this check is the only guard there. ``off`` and ``per_row`` decide
        nothing in SQL from the bundle and are not checked."""
        if self.mode not in ("no_bundle", "class"):
            return
        scope = f"deployment:{self._session.deployment_name}"
        active = rf._active_policy_fingerprint_now(
            self._conn, deployment_name=self._session.deployment_name, scope=scope,
        )
        if self.mode == "no_bundle":
            if active is not None:
                raise SubstrateError(
                    "reader view: no policy bundle was active when the read began, "
                    "but one is active now; refusing the read",
                    data={"active_policy_fingerprint": str(active)},
                )
            return
        opened = None if self.bundle_fingerprint is None else str(self.bundle_fingerprint)
        if (None if active is None else str(active)) != opened:
            raise SubstrateError(
                "reader view: the active policy bundle changed while the read ran; "
                "refusing the read",
                data={"opened_policy_fingerprint": opened,
                      "active_policy_fingerprint": None if active is None else str(active)},
            )

    # ---------------------------------------------------------- exactness --
    @property
    def sql_exact(self) -> bool:
        """Whether the predicate alone equals what ``finish`` serves, so a
        scanner may charge counts and bounds in SQL: ``finish`` would remove
        no row and mask nothing. Decided from the view's own state, with no
        query (Jason, 2026-10-02: no table scan). See ``exactness``."""
        return bool(self.exactness()["sql_exact"])

    def exactness(self) -> dict[str, Any]:
        """``{"sql_exact", "reason"}``.

        True only with Nomos off (``finish`` returns the rows unchanged).
        Otherwise False, because the per-row filter can remove or mask rows
        whatever the bundle says, and only reading the rows could rule that
        out:

        * it hides every row with no source lineage, with no decision, in
          audit and enforce alike (``_filter_rows_evaluate``: ``if not
          source_ids: continue``); such rows hold a NULL class, so the
          predicate keeps them and ``finish`` removes them;
        * in enforce it denies a row whose classification is blank (an
          invalid protected identity fact,
          ``core._has_invalid_protected_identity_fact``), and the predicate
          cannot recognize every blank string;
        * a ``redact`` effect masks in audit too: audit mode turns only
          deny/review/quarantine into allow
          (``store._effective_decision_for_callers``).

        ``reason`` is ``nomos_off``, ``per_row:<reason>`` or, in class mode,
        the constant ``nomos_on``. ``nomos_off`` and every per-row reason come
        from the deployment's mode, the session, the caller's ``force``, the
        loaded bundle or the database schema (catalog table, class columns),
        never from rows. A finer class-mode cause is NOT exposed on the view:
        it would be read off the catalog, which holds the classes of hidden
        rows in every generation, so it could tell a reader who knows the
        policy that a hidden row exists. ``exactness_cause`` gives it to
        tests and operator tools.
        """
        if self.mode == "off":
            return {"sql_exact": True, "reason": "nomos_off"}
        if self.mode != "class":
            return {"sql_exact": False, "reason": f"per_row:{self.reason}"}
        return {"sql_exact": False, "reason": "nomos_on"}

    # ------------------------------------------------------------- shadow --
    def _shadow_finish(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run the per-row path and the class path on the same rows and refuse
        any difference: in the rows served, their order and their attrs; in
        each placed row's class decision against its per-row outcome; and in
        ``readable`` and ``redacts`` against the attrs the per-row filter
        masked."""
        id_key = f"{self.row_kind}_id"
        tagged = [{**row, _POSITION_TAG: position} for position, row in enumerate(rows)]
        exact_out = self._per_row([dict(row) for row in tagged])
        probe = self._probe_rows(rows, id_key=id_key)
        passing = [row for row in tagged if probe[str(row.get(id_key))]["passes"]]
        class_out = self._per_row([dict(row) for row in passing])
        exact_rows = [_untagged(row) for row in exact_out]
        class_rows = [_untagged(row) for row in class_out]
        if _canonical(exact_rows) != _canonical(class_rows):
            dropped = sorted(
                {str(r.get(id_key)) for r in exact_rows} - {str(r.get(id_key)) for r in class_rows}
            )
            raise ReaderViewShadowMismatch(
                "reader view shadow: the class path served different rows than the "
                "per-row filter",
                data={"row_kind": self.row_kind, "dropped_by_predicate": dropped[:20],
                      "per_row_count": len(exact_rows), "class_count": len(class_rows)},
            )
        served = {row[_POSITION_TAG]: row for row in exact_out}
        for position, row in enumerate(rows):
            facts = probe[str(row.get(id_key))]
            out = served.get(position)
            if facts["class"] is not None and facts["label"] is not None and not self._state.overlay:
                outcome = self._state.decisions[facts["class"]][facts["label"]]
                per_row = ClassOutcome(
                    visible=out is not None,
                    redacted=out is not None and isinstance(out["attrs"], ReadRedactedAttrs),
                )
                if outcome != per_row:
                    raise ReaderViewShadowMismatch(
                        "reader view shadow: a class decision differs from the row's "
                        "per-row decision",
                        data={"row_kind": self.row_kind, id_key: row.get(id_key),
                              "read_class": str(facts["class"]),
                              "class_outcome": outcome.__dict__,
                              "per_row_outcome": per_row.__dict__},
                    )
            if out is None:
                continue
            redacted = isinstance(out["attrs"], ReadRedactedAttrs)
            if facts["redacts"] is not None and bool(facts["redacts"]) != redacted:
                raise ReaderViewShadowMismatch(
                    "reader view shadow: redacts() disagrees with the per-row filter",
                    data={"row_kind": self.row_kind, id_key: row.get(id_key),
                          "redacts": facts["redacts"], "redacted": redacted},
                )
            mask = self.masked_attrs(str(row.get("subtype") or ""))
            for key, readable in facts["readable"].items():
                masked = redacted and mask.masked(key)
                if readable is not None and readable == masked:
                    raise ReaderViewShadowMismatch(
                        "reader view shadow: readable() disagrees with the per-row mask",
                        data={"row_kind": self.row_kind, id_key: row.get(id_key),
                              "attr_key": key, "readable": readable, "masked": masked},
                    )
                if redacted:
                    _check_mask(out["attrs"], key, mask.action(key), row_id=row.get(id_key))
        return exact_rows

    def _shadow_no_bundle(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """``no_bundle`` shadow: the per-row filter's answer against the
        view's. Refused: a served row when the view serves nothing; a
        redacted row (the view says nothing is redacted)."""
        id_key = f"{self.row_kind}_id"
        out = self._per_row([dict(row) for row in rows])
        if not self._state.serves:
            if out:
                raise ReaderViewShadowMismatch(
                    "reader view shadow: the no-bundle view serves nothing, the "
                    "per-row filter served rows",
                    data={"row_kind": self.row_kind, "served": len(out)},
                )
            return out
        for row in out:
            if isinstance(row.get("attrs"), ReadRedactedAttrs):
                raise ReaderViewShadowMismatch(
                    "reader view shadow: the per-row filter redacted a row with no "
                    "active bundle",
                    data={"row_kind": self.row_kind, id_key: row.get(id_key)},
                )
        return out

    def _probe_rows(self, rows: list[dict[str, Any]], *, id_key: str) -> dict[str, dict[str, Any]]:
        """Each row's stored class, label, predicate value and readable()
        values for its attrs keys, from the database at the session's
        generation."""
        ids = sorted({str(row.get(id_key)) for row in rows})
        keys = sorted({
            str(k) for row in rows for k in dict(row.get("attrs") or {})
        })[:64]
        table = "okg.graph_nodes" if self.row_kind == "node" else "okg.graph_edges"
        alias = "r"
        conn = self._conn
        columns = [
            f"CAST({alias}.{id_key} AS text) AS row_id",
            f"CAST({alias}.{rc.READ_CLASS_COLUMN} AS text) AS read_class",
            f"{alias}.{rc.READ_CLASS_VERSION_COLUMN} AS read_class_version",
            f"{self.label_sql(alias).as_string(conn)} AS label",
            f"({self.predicate(alias).as_string(conn)}) AS passes",
            f"({self.redacts(alias).as_string(conn)}) AS redacts",
            *(
                f"({self.readable(alias, key).as_string(conn)}) AS readable_{i}"
                for i, key in enumerate(keys)
            ),
        ]
        text = (
            f"SELECT {', '.join(columns)} FROM {table} {alias}"
            f" WHERE CAST({alias}.{id_key} AS text) = ANY(:ids)"
        )
        found = {
            str(r["row_id"]): r for r in _generation_query(conn, self._session, text, {"ids": ids})
        }
        missing = [row_id for row_id in ids if row_id not in found]
        if missing:
            raise ReaderViewShadowMismatch(
                "reader view shadow: rows passed to finish are not in the session's "
                "generation, so the class path cannot be checked",
                data={"row_kind": self.row_kind, "missing": missing[:20]},
            )
        out: dict[str, dict[str, Any]] = {}
        for row_id, r in found.items():
            known = (
                r["read_class"] is not None
                and r["read_class_version"] == rc.CLASS_VOCABULARY_VERSION
                and uuid.UUID(r["read_class"]) in self.decided_classes
            )
            out[row_id] = {
                "class": uuid.UUID(r["read_class"]) if known else None,
                "label": r["label"],
                "passes": bool(r["passes"]),
                "redacts": r["redacts"],
                "readable": {key: r[f"readable_{i}"] for i, key in enumerate(keys)},
            }
        return out


def _check_mask(attrs: Mapping[str, Any], key: str, action: str, *, row_id: Any) -> None:
    present = key in attrs
    ok = (
        (action == "drop" and not present)
        or (action == "mask" and present and attrs[key] == MASK_TOKEN)
        or (action == "reveal" and present)
    )
    if not ok:
        raise ReaderViewShadowMismatch(
            "reader view shadow: masked_attrs() disagrees with the per-row mask",
            data={"row_id": row_id, "attr_key": key, "predicted": action,
                  "present": present},
        )


def _untagged(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.pop(_POSITION_TAG, None)
    return out


def _canonical(rows: list[Mapping[str, Any]]) -> list[tuple[str, str]]:
    out = []
    for row in rows:
        try:
            text = json.dumps(row, sort_keys=True, default=repr)
        except (TypeError, ValueError):
            text = repr(sorted((str(k), repr(v)) for k, v in row.items()))
        out.append((type(row.get("attrs")).__name__, text))
    return out


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def fingerprint_of(inputs: Mapping[str, Any]) -> str:
    """The view's fingerprint: sha256 of the canonical JSON of its inputs.
    Classes are serialized as sorted lowercase hyphenated UUID strings."""
    return "reader-view:" + _digest(inputs)


def exactness_cause(view: ReaderView) -> str:
    """For tests and operator tools only: why a class-mode view is not
    exact. Read off the catalog decisions, which cover hidden rows' classes,
    so it must not reach a reader (``ReaderView.exactness`` says only
    ``nomos_on``)."""
    if view.mode != "class":
        return view.exactness()["reason"]

    def hides_or_masks(blank: bool) -> bool:
        return any(
            not outcome.visible or outcome.redacted
            for outcomes in view._state.decisions.values()
            for label, outcome in outcomes.items()
            if (label == BLANK_LABEL) is blank
        )

    if hides_or_masks(blank=False):
        return "rule_can_hide_or_mask"
    if hides_or_masks(blank=True):
        return "blank_classification_denied"
    return "per_row_filter_hides_rows_without_lineage"


# --------------------------------------------------------------------------- #
# Entry points                                                                 #
# --------------------------------------------------------------------------- #
class ReaderViews(Mapping[str, ReaderView]):
    """The views ``open_many`` returns, one per kind, sharing one fingerprint."""

    def __init__(self, views: Mapping[str, ReaderView], fingerprint: str) -> None:
        self._views = dict(views)
        self.fingerprint = fingerprint

    def __getitem__(self, kind: str) -> ReaderView:
        return self._views[kind]

    def __iter__(self):
        return iter(self._views)

    def __len__(self) -> int:
        return len(self._views)


def _no_bundle_serves(
    conn: Any, session: Any, *, subject: str, action: str, surface: str, scope: str,
) -> bool | None:
    """The no-bundle default decision, unrecorded, as
    ``read_filter._decide_no_active_bundle`` evaluates it (same subject,
    action, enforcement point, classification and resource): True when it
    serves rows unredacted (audit), False when it hides them (enforce), None
    for any other effect (the view then stays per-row)."""
    decision = _evaluate_unrecorded(
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
                "kind": "node",
                "audit": False,
            },
            context={},
        ),
        enforcement_point=f"{surface}.source_lineage_filter",
        performed_obligations=("audit", "redaction"),
        payload=safe_decision_payload({}),
    )
    if decision.effect in rf._HIDE_EFFECTS:
        return False
    if decision.effect == "allow":
        return True
    return None


def open(  # noqa: A001 - the agreed name
    conn: Any,
    session: Any,
    *,
    row_kind: str,
    action: str | None,
    surface: str | None,
    force: str | None = None,
    nomos_runtime: bool | None = None,
) -> ReaderView:
    """The reader's view of ``row_kind`` rows for one call (module doc).

    It records no decision and does not commit ``conn``. Its catalog,
    column and overlay SELECTs run on ``conn``, so with autocommit off they
    leave a transaction open until the caller commits or ``finish`` commits
    (through the per-row filter, after it records)."""
    if row_kind not in ROW_KINDS:
        raise ValueError(f"row_kind must be 'node' or 'edge', not {row_kind!r}")
    return open_many(conn, session, action=action, surface=surface, force=force,
                     kinds=(row_kind,), nomos_runtime=nomos_runtime)[row_kind]


def open_many(
    conn: Any,
    session: Any,
    *,
    action: str | None,
    surface: str | None,
    force: str | None = None,
    kinds: Sequence[str] = ("node", "edge"),
    nomos_runtime: bool | None = None,
) -> ReaderViews:
    """The views of several row kinds, decided in ONE read pass against one
    bundle, with one fingerprint.

    ``nomos_runtime`` is the caller's one reading of whether the deployment
    runs Nomos (``audit``/``enforce``); None reads it here. A caller that
    already decided its read path on that reading passes it, so the view and
    the caller cannot disagree within a call.

    It records no decision and does not commit ``conn``. Its catalog,
    column and overlay SELECTs run on ``conn``, so with autocommit off they
    leave a transaction open until the caller commits or ``finish`` commits
    (through the per-row filter, after it records the read decisions of every
    row given to it)."""
    kinds = tuple(dict.fromkeys(kinds))
    if not kinds or any(kind not in ROW_KINDS for kind in kinds):
        raise ValueError(f"kinds must be drawn from {ROW_KINDS}, not {kinds!r}")
    if force not in FORCES:
        raise ValueError(f"force must be one of {FORCES}, not {force!r}")
    action, surface = rf.session_read_identity(session, action=action, surface=surface)
    rf._assert_multi_user_subject(session, None)
    subject = str(
        getattr(session, "nomos_subject", None)
        or rf.nomos_subject_from_env(default_kind="mcp_client")
    )
    states: dict[str, _KindState] = {}
    bundle_fingerprint: Any = None
    named: frozenset[str] = frozenset({"internal"})
    subtype_pii: Mapping[str, Mapping[str, Any]] | None = None
    enforcement = None

    def all_kinds(mode: str, reason: str) -> None:
        for kind in kinds:
            states[kind] = _KindState(kind, mode, reason, {}, overlay=False)

    if nomos_runtime is None:
        nomos_runtime = rf.deployment_requires_nomos_runtime(session.deployment_name)
    if not nomos_runtime:
        all_kinds("off", "nomos_off")
    else:
        enforcement = nomos_store.deployment_nomos_enforcement_mode(session.deployment_name)
        subtype_pii = rf._load_subtype_pii_classes(
            conn, getattr(session, "catalog_version_id", None)
        )
        refusal = rf._read_class_session_refusal(session, subject=None, identity_claim=None)
        if refusal is not None:
            all_kinds("per_row", refusal)
        elif force == "per_row":
            all_kinds("per_row", "forced_per_row")
        else:
            scope = f"deployment:{session.deployment_name}"
            with read_pass_scope():
                bundle = rf._load_read_bundle(conn, session, scope=scope)
                if bundle is None:
                    serves = _no_bundle_serves(
                        conn, session, subject=subject, action=action, surface=surface,
                        scope=scope,
                    )
                    if serves is None:
                        all_kinds("per_row", "no_active_bundle")
                    else:
                        for kind in kinds:
                            states[kind] = _KindState(
                                kind, "no_bundle", "no_active_bundle", {}, False,
                                serves=serves,
                            )
                else:
                    bundle_fingerprint = bundle.policy_fingerprint
                    named = named_classifications(bundle)
                    blocking = rf._read_class_blocking_rule_keys(
                        bundle,
                        actions=rf._evaluated_read_actions(action, surface),
                        subjects=rf._evaluated_read_subjects(session),
                    )
                    if blocking:
                        all_kinds("per_row", "rule_names_key:" + ",".join(blocking))
                    elif not _catalog_present(conn):
                        all_kinds("per_row", "catalog_absent")
                    else:
                        catalog = _load_catalog(conn)
                        probes = _probes(named)
                        # Class decisions are not recorded: no persist batch.
                        for kind in kinds:
                            if not _columns_present(conn, kind):
                                states[kind] = _KindState(
                                    kind, "per_row", "read_class_columns_absent", {}, False,
                                )
                                continue
                            decided = decide_classes(
                                conn, session, catalog[kind], kind=kind, action=action,
                                surface=surface, probes=probes,
                                expected_fingerprint=bundle_fingerprint,
                            )
                            states[kind] = _KindState(
                                kind, "class", "read_class_catalog", decided,
                                overlay=_overlay_present(conn, session, kind),
                            )

    inputs = {
        "kinds": {
            kind: {
                "mode": states[kind].mode,
                "reason": states[kind].reason,
                "overlay": states[kind].overlay,
                "serves": states[kind].serves,
                "visible_classes": sorted(
                    str(c) for c, o in states[kind].decisions.items()
                    if any(x.visible for x in o.values())
                ),
                "decisions": sorted(
                    f"{c}|{label}|{int(o.visible)}{int(o.redacted)}"
                    for c, outcomes in states[kind].decisions.items()
                    for label, o in outcomes.items()
                ),
                "catalog": _digest(sorted(str(c) for c in states[kind].decisions)),
            }
            for kind in kinds
        },
        "policy_fingerprint": bundle_fingerprint,
        "enforcement_mode": enforcement,
        "subject": subject,
        "action": action,
        "surface": surface,
        "force": force,
        "class_version": rc.CLASS_VOCABULARY_VERSION,
        "generation_pin": {
            "deployment": session.deployment_name,
            "branch": session.branch_name,
            "generation_id": str(session.generation_id),
            "catalog_version_id": (
                None if session.catalog_version_id is None else str(session.catalog_version_id)
            ),
        },
        "subtype_pii": _digest(subtype_pii or {}),
    }
    fingerprint = fingerprint_of(inputs)
    views = {
        kind: ReaderView(
            conn=conn, session=session, state=states[kind], action=action, surface=surface,
            force=force, bundle_fingerprint=bundle_fingerprint, named=named,
            subtype_pii=subtype_pii, fingerprint_inputs=inputs,
        )
        for kind in kinds
    }
    return ReaderViews(views, fingerprint)


__all__ = [
    "AttrMask",
    "ClassOutcome",
    "FORCES",
    "MODES",
    "ReaderView",
    "ReaderViewRowShapeError",
    "ReaderViewShadowMismatch",
    "ReaderViews",
    "class_resources",
    "classification_label",
    "clear_reader_view_cache",
    "decide_classes",
    "exactness_cause",
    "fingerprint_of",
    "named_classifications",
    "open",
    "open_many",
]
