"""`inspect` history reads: one node's versions, and one subtype's changes.

PACT okg-attribute-history-read.

* ``inspect(target={kind: "node_history", node_id, attr_keys?, from?, to?,
  limit?, cursor?})`` lists one node's versions, newest first, from the
  Chronos interval rows of the session's branch
  (``ChronosGraphStore.node_version_history``).
* ``inspect(target={kind: "subtype_diff", subtype, from_generation,
  to_generation?, attr_keys?, limit?, cursor?})`` lists the nodes of one
  subtype added, retracted or changed between two generations, from two
  pinned checkouts that project ``(node_id, subtype, content hash)``, as
  ``generation_delta`` does for the whole graph.

What a reader is shown, and why it cannot learn a hidden value:

* Visibility and masking are the session pin's Nomos read filter
  (``filter_node_rows_for_session``), the one ``inspect(kind node)`` uses,
  applied to each version's own row (its attrs and lineage). A version the
  reader cannot see is left out; a hidden or absent node reads the same.
* Attributes are the graph layer (``attrs_layer: "graph"``): as the source
  projected them, before the reviewer and proposal overlay. A redacted
  version's attrs are masked by the classifier the filter masks with
  (``read_attr_classifier_for_row``), so a value reads the same here as in
  ``inspect(kind attr)``.
* Changes are computed on the masked attrs, against the next older version
  the reader is shown, under ``fact_equivalence.compare_attrs`` with the
  subtype's volatile fields (from ``okg.subtypes`` at the session pin's
  catalog version) and a lineage revision that moved with no content change
  ignored. Adjacent shown versions equal under that rule merge into one, so
  a hidden value's change shows neither as a changed key nor as a version
  boundary, and a run of hidden versions leaves no marker.
* Every version and entry carries ``provenance`` (its producer and trust
  label, as a node read labels a node). Values are fenced by that label, and
  so are the names in ``added_keys``, ``removed_keys`` and ``changed_keys``:
  an attribute name is the producer's text, so a name read from an
  untrusted row on either side of a change is fenced like a value.
* A multi-user session with no request subject is refused before any read,
  under every enforcement mode, as the read filter refuses it.
* ``subtype_diff`` counts follow ``generation_delta``: under ``audit`` or
  ``enforce`` the exact counts are given while the delta is at most
  ``_PREVIEW_ADVANCE_EXACT_ROW_BUDGET`` rows; above it the counts and the
  entries are withheld (``truncated: true`` and a note), so the cardinality
  of rows the reader cannot see does not leak through them.

Bounds: ``limit`` defaults to 50 and is at most 500; a page is also cut to
the call's byte budget. A cursor binds the resolved ``from`` and ``to``
generation ids and the request's other arguments, so a page read after a
new publish reads the same range, and a cursor sent with other arguments is
refused. Over a row-scan budget is refused (``history_row_budget_exceeded``),
never answered with a shortened list. A ``from`` or ``to`` that is not a
published generation, or was compacted, is refused with its typed code;
nothing falls back to the latest generation.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Sequence

import psycopg

from okg.substrate.errors import GenerationOutOfScope, SubstrateError
from okg.substrate.fact_equivalence import (
    LINEAGE_ATTR,
    compare_attrs,
    volatile_fields_for,
)
from okg.substrate.graph_store_internal.node_history import (
    NODE_HISTORY_ROW_BUDGET,
    HistoryGenerationUnordered,
    HistoryRangeInvalid,
    HistoryRowBudgetExceeded,
    LineageGeneration,
    NodeRowVersion,
)
from okg.substrate.mcp import continuation as _continuation
from okg.substrate.mcp.operator_models import (
    HISTORY_DEFAULT_LIMIT,
    HISTORY_MAX_ATTR_KEYS,
    HISTORY_MAX_LIMIT,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.session import Session, assert_session_matches


NODE_HISTORY_KIND = "node_history"
SUBTYPE_DIFF_KIND = "subtype_diff"

#: Continuation-token kinds. Neither is a `next` route: these reads take
#: their cursor in `target.cursor`, beside the arguments it is bound to.
_NODE_HISTORY_TOKEN = "inspect_node_history"
_SUBTYPE_DIFF_TOKEN = "inspect_subtype_diff"

#: Rows of the subtype each `subtype_diff` checkout may scan.
SUBTYPE_DIFF_SCAN_ROW_BUDGET = 200_000
#: Changed rows (content hash differs) whose attributes `subtype_diff`
#: compares when no Nomos runtime is configured. Under `audit` or `enforce`
#: the delta bound is `_PREVIEW_ADVANCE_EXACT_ROW_BUDGET`, as for
#: `generation_delta`.
SUBTYPE_DIFF_COMPARE_ROW_BUDGET = 20_000
#: Ids per statement when reading delta rows from a checkout.
_ID_CHUNK = 1_000

_ROW_KEY = "_okg_history_row"

_WITHHELD_NOTE = (
    "the subtype's delta between these generations exceeds the exact-count "
    "budget under Nomos audit or enforce; counts and entries are withheld so "
    "the number of rows you cannot see does not leak; diff two closer "
    "generations, or read the subtype with filter at each generation"
)


# ── arguments ──────────────────────────────────────────────────────────


def _invalid(message: str, field: str, fix_hint: str) -> InvalidOperatorArguments:
    return InvalidOperatorArguments(
        message,
        data={
            "tool": "inspect",
            "invalid_fields": [f"target.{field}"],
            "validation_kinds": ["argument_check"],
        },
        fix_hint=fix_hint,
    )


def _limit(target: Mapping[str, Any]) -> int:
    value = target.get("limit")
    if value is None:
        return HISTORY_DEFAULT_LIMIT
    if isinstance(value, bool) or not isinstance(value, int) or not (
        1 <= value <= HISTORY_MAX_LIMIT
    ):
        raise _invalid(
            f"limit must be an integer from 1 to {HISTORY_MAX_LIMIT}",
            "limit",
            f"pass limit between 1 and {HISTORY_MAX_LIMIT}, or omit it for "
            f"{HISTORY_DEFAULT_LIMIT}",
        )
    return value


def _attr_keys(target: Mapping[str, Any]) -> Optional[list[str]]:
    value = target.get("attr_keys")
    if value is None:
        return None
    if (
        not isinstance(value, list)
        or not value
        or len(value) > HISTORY_MAX_ATTR_KEYS
        or not all(isinstance(key, str) and key for key in value)
    ):
        raise _invalid(
            f"attr_keys must be a list of 1 to {HISTORY_MAX_ATTR_KEYS} "
            "attribute names",
            "attr_keys",
            "name the attributes whose values you want, or omit attr_keys "
            "for changed keys only",
        )
    return list(dict.fromkeys(value))


def _generation_argument(value: Any, field: str) -> Any:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, str)) or (
        isinstance(value, str) and not value.strip()
    ):
        raise _invalid(
            f"{field} must be a generation id",
            field,
            f"pass {field} as a generation id from `okg generation list` or a "
            "reply's generation_id",
        )
    return value


def _cursor_argument(target: Mapping[str, Any]) -> Optional[str]:
    value = target.get("cursor")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise _invalid(
            "cursor must be the token a previous page returned",
            "cursor",
            "pass the cursor exactly as the reply printed it, or omit it",
        )
    return value


# ── generations ────────────────────────────────────────────────────────


def resolve_history_generation(
    conn: psycopg.Connection,
    session: Session,
    requested: Any,
) -> str:
    """The checkpoint id ``requested`` names, by every form `generation_id`
    accepts; a compacted one raises ``GenerationCompacted`` and one that is
    no published generation of this deployment ``GenerationNotFound``.

    Branch membership is not decided here: the store places the id on the
    session branch's lineage and refuses one outside it.
    """
    from okg.substrate.generation_resolver import resolve_okg_generation
    from okg.substrate.mcp.session import _unpublished_generation

    resolved = resolve_okg_generation(
        conn,
        deployment_name=session.deployment_name,
        requested=requested,
        branch=None,
        status="published",
    )
    if resolved is None:
        raise _unpublished_generation(
            conn, session.deployment_name, requested, session.branch_name,
        )
    return str(resolved.generation_id)


def _generation_ref(generation: LineageGeneration) -> dict[str, Any]:
    return {
        "generation_id": generation.generation_id,
        "branch": generation.branch,
        "created_at": generation.created_at,
    }


# ── catalog ────────────────────────────────────────────────────────────


def volatile_fields_at_pin(
    conn: psycopg.Connection, session: Session,
) -> dict[str, frozenset[str]]:
    """Each subtype's volatile fields in the session pin's catalog version,
    the catalog every other read of the session uses."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT name, metadata -> 'volatile_fields' FROM okg.subtypes "
            "WHERE catalog_version_id = %s",
            (session.catalog_version_id,),
        )
        rows = cur.fetchall()
    fields: dict[str, frozenset[str]] = {}
    for name, declared in rows:
        if not isinstance(declared, list):
            continue
        keys = frozenset(str(key) for key in declared if isinstance(key, str) and key)
        if keys:
            fields[str(name)] = keys
    return fields


# ── Nomos ──────────────────────────────────────────────────────────────


class ShownRow:
    """A row the reader is shown: its subtype, masked graph attrs, source."""

    __slots__ = ("subtype", "attrs", "source")

    def __init__(self, subtype: Optional[str], attrs: dict[str, Any], source: Optional[str]):
        self.subtype = subtype
        self.attrs = attrs
        self.source = source


def decide_rows(
    conn: psycopg.Connection,
    session: Session,
    rows: Sequence[Mapping[str, Any]],
    *,
    runtime_required: Optional[bool] = None,
) -> dict[Any, ShownRow]:
    """The rows the reader is shown, by ``rows[i][_ROW_KEY]``.

    Each row is decided by the session's Nomos read filter on its own attrs
    and lineage (with the pin's accepted overlay, as ``inspect(kind node)``
    decides it). A shown row keeps its GRAPH attrs; when the decision
    redacted the row, they are masked by the classifier the filter masks
    with, and carry the redaction marker. Under ``off`` every row is shown
    as stored.
    """
    from okg.substrate.nomos.read_filter import (
        filter_node_rows_for_session,
        read_attr_classifier_for_row,
    )
    from okg.substrate.nomos.read_masking import (
        READ_DROP_CLASSES,
        REDACTION_MARKER,
        ReadRedactedAttrs,
        mask_attrs,
    )

    if not rows:
        return {}
    stored = {row[_ROW_KEY]: row for row in rows}
    visible = filter_node_rows_for_session(
        conn,
        session,
        [dict(row) for row in rows],
        runtime_required=runtime_required,
    )
    classifiers: dict[str, Any] = {}
    shown: dict[Any, ShownRow] = {}
    for row in visible:
        key = row[_ROW_KEY]
        original = stored[key]
        attrs = dict(original.get("attrs") or {})
        if isinstance(row.get("attrs"), ReadRedactedAttrs):
            subtype = str(original.get("subtype") or "")
            classifier = classifiers.get(subtype)
            if classifier is None:
                classifier = read_attr_classifier_for_row(
                    conn, session, {"subtype": subtype},
                )
                classifiers[subtype] = classifier
            attrs = mask_attrs(
                attrs, classifier=classifier, drop_classes=READ_DROP_CLASSES,
            )
            # A redact decision fired: the marker is present even when no
            # key matched the classifier, as the read filter stamps it.
            attrs[REDACTION_MARKER] = True
        shown[key] = ShownRow(original.get("subtype"), attrs, original.get("source"))
    return shown


def _version_row(node_id: str, version: NodeRowVersion) -> dict[str, Any]:
    return {
        "node_id": node_id,
        "subtype": version.subtype,
        "attrs": dict(version.attrs),
        "source": version.source,
        "source_record_id": version.source_record_id,
        "source_revision": version.source_revision,
        _ROW_KEY: version.live_lo,
    }


# ── changes ────────────────────────────────────────────────────────────


def classify_changes(
    new_attrs: Mapping[str, Any],
    old_attrs: Mapping[str, Any],
    *,
    ignore: frozenset[str],
) -> Optional[dict[str, list[str]]]:
    """``{added_keys, removed_keys, changed_keys}`` between two attribute
    maps under ``compare_attrs``, or None when they are equal under it.

    A lineage whose revision alone moved alongside a real change is named
    ``nomos_source_lineage.source_revision``, as ``compare_attrs`` names it.
    """
    comparison = compare_attrs(new_attrs, old_attrs, ignore=ignore)
    if comparison.unchanged:
        return None
    added: list[str] = []
    removed: list[str] = []
    changed: list[str] = []
    for key in comparison.differing:
        base = LINEAGE_ATTR if key.startswith(LINEAGE_ATTR + ".") else key
        if base not in old_attrs:
            added.append(key)
        elif base not in new_attrs:
            removed.append(key)
        else:
            changed.append(key)
    return {"added_keys": added, "removed_keys": removed, "changed_keys": changed}


class _Version:
    __slots__ = (
        "first", "last", "subtype", "first_attrs", "first_source",
        "last_attrs", "last_source", "against", "change",
    )

    def __init__(self, generation: LineageGeneration, shown: ShownRow):
        self.first = generation
        self.last = generation
        self.subtype = shown.subtype
        self.first_attrs = shown.attrs
        self.first_source = shown.source
        self.last_attrs = shown.attrs
        self.last_source = shown.source
        #: The older shown row (source, attrs) the change was computed
        #: against; None for the oldest shown version.
        self.against: Optional[tuple[Optional[str], Mapping[str, Any]]] = None
        self.change: dict[str, Any] = {"change": "oldest_shown"}

    def sides(self) -> list[tuple[Optional[str], Mapping[str, Any]]]:
        """The rows this version's item is read from: the version as it
        last stood (its label), then the rows its key names came from."""
        sides = [(self.last_source, self.last_attrs), (self.first_source, self.first_attrs)]
        if self.against is not None:
            sides.append(self.against)
        return sides


def merge_versions(
    generations: Sequence[LineageGeneration],
    states: Sequence[Optional[ShownRow]],
    volatile: Mapping[str, frozenset[str]],
) -> list[_Version]:
    """Shown versions, oldest first.

    ``states[i]`` is what the reader is shown at ``generations[i]`` (None:
    absent or hidden, which read the same). A shown state equal to the
    previous shown version under ``compare_attrs`` (same subtype, its
    volatile fields and a revision-only lineage move ignored) extends that
    version; any other starts a new one, whose changes are computed against
    the previous shown version as it last stood. A subtype change is the old
    subtype retracted and the new one added.
    """
    versions: list[_Version] = []
    for generation, shown in zip(generations, states):
        if shown is None:
            continue
        current = versions[-1] if versions else None
        ignore = volatile_fields_for(volatile, shown.subtype)
        if current is not None and current.subtype == shown.subtype:
            changes = classify_changes(shown.attrs, current.last_attrs, ignore=ignore)
            if changes is None:
                current.last = generation
                current.last_attrs = shown.attrs
                current.last_source = shown.source
                continue
            version = _Version(generation, shown)
            version.against = (current.last_source, current.last_attrs)
            version.change = {"change": "changed", **changes}
            versions.append(version)
            continue
        version = _Version(generation, shown)
        if current is not None:
            version.against = (current.last_source, current.last_attrs)
            changes = classify_changes(
                shown.attrs, current.last_attrs, ignore=ignore,
            ) or {"added_keys": [], "removed_keys": [], "changed_keys": []}
            version.change = {
                "change": "subtype_changed",
                "subtype_retracted": current.subtype,
                "subtype_added": shown.subtype,
                **changes,
            }
        versions.append(version)
    return versions


# ── shaping and paging ─────────────────────────────────────────────────


def _selected_values(
    conn: psycopg.Connection,
    session: Session,
    entries: list[tuple[dict[str, Any], str, Optional[str], Mapping[str, Any]]],
    attr_keys: Optional[list[str]],
) -> None:
    """Attach ``attr_keys`` values to each entry, fenced by producer trust.

    ``entries`` holds ``(item, field, source, attrs)``: the item gets
    ``item[field]`` (the selected values, fenced as ``inspect(kind node)``
    fences an untrusted producer's strings) and ``item[field +
    "_provenance"]``. The full ``attrs`` decide the trust marker, as in a
    node read whose `select` dropped attributes.
    """
    if not attr_keys or not entries:
        return
    from okg.substrate.mcp.provenance import serialize_node_read_rows

    rows = [
        {"attrs": {key: attrs[key] for key in attr_keys if key in attrs}, "source": source}
        for _, _, source, attrs in entries
    ]
    served = serialize_node_read_rows(
        conn,
        rows,
        deployment_name=session.deployment_name,
        marker_attrs=[attrs for _, _, _, attrs in entries],
    )
    for (item, field, _, _), out in zip(entries, served):
        item[field] = out.get("attrs") or {}
        item[f"{field}_provenance"] = out.get("provenance")


#: Item fields that list attribute names read from the producer's rows.
_KEY_FIELDS = ("added_keys", "removed_keys", "changed_keys")


def label_items(
    conn: psycopg.Connection,
    session: Session,
    labels: Sequence[tuple[dict[str, Any], Sequence[tuple[Optional[str], Mapping[str, Any]]]]],
) -> None:
    """Stamp each item's trust label, and fence its key names by trust.

    ``labels`` holds ``(item, sides)``. ``sides[0]`` is the row the item
    reads as, given as its source and FULL shown attrs (the transcript
    marker lives in attrs); its label becomes ``item["provenance"]``, as
    ``inspect(kind node)`` labels a node. Every side is a row the item's key
    names were read from. A key name is the producer's text, so when any side
    is untrusted each name in ``added_keys``, ``removed_keys`` and
    ``changed_keys`` is fenced as an untrusted value is, under that side's
    source. One producer lookup covers the whole page.
    """
    from okg.substrate.mcp.provenance import _fence_value, serialize_node_read_rows

    flat = [side for _, sides in labels for side in sides]
    if not flat:
        return
    served = serialize_node_read_rows(
        conn,
        [{"source": source} for source, _ in flat],
        deployment_name=session.deployment_name,
        marker_attrs=[dict(attrs) for _, attrs in flat],
    )
    provenances = iter(out["provenance"] for out in served)
    for item, sides in labels:
        labelled = [next(provenances) for _ in sides]
        item["provenance"] = labelled[0]
        untrusted = next(
            (label for label in labelled if label.get("trust") != "trusted"), None,
        )
        if untrusted is None:
            continue
        for field in _KEY_FIELDS:
            if field in item:
                item[field] = [
                    _fence_value(key, source=untrusted.get("source"))
                    for key in item[field]
                ]


def fit_page(
    build: Callable[[int], dict[str, Any]],
    count: int,
    *,
    max_bytes: Optional[int],
    measure: Optional[Callable[[dict[str, Any]], int]],
) -> int:
    """The most of ``count`` items whose page fits ``max_bytes``, at least 1.

    ``build(n)`` is the page holding the first ``n`` items. With no measure
    the page holds every item; a single item over the budget is served and
    left to the reply's byte bound, which refuses it with its remedy.
    """
    if count <= 1 or measure is None or max_bytes is None:
        return count
    if measure(build(count)) <= max_bytes:
        return count
    low, high = 1, count - 1
    while low < high:
        middle = (low + high + 1) // 2
        if measure(build(middle)) <= max_bytes:
            low = middle
        else:
            high = middle - 1
    return low


#: Bytes added to a measured cursor's length when a page is sized before its
#: real cursor exists: a signed token's compressed length moves by a few
#: bytes with the position it records.
_CURSOR_SLACK = 64


def paged_payload(
    build: Callable[[int], dict[str, Any]],
    page_len: int,
    *,
    target: Mapping[str, Any],
    issue_for: Callable[[int], str],
    max_bytes: Optional[int],
    measure: Optional[Callable[[dict[str, Any]], int]],
) -> dict[str, Any]:
    """The page of the first ``n <= page_len`` items that fits the budget,
    with its cursor when items remain.

    ``build(n)`` sets ``truncated`` when items remain after the first ``n``.
    The page is sized with a stand-in cursor at least as long as the real
    one, then the real cursor is issued for the cut it ended at, so a cut
    page never outgrows the budget it was sized for.
    """
    echo = {
        key: value for key, value in target.items()
        if key not in {"cursor", "from_"} and value is not None
    }
    if target.get("from_") is not None and "from" not in echo:
        echo["from"] = target["from_"]
    stand_in: Optional[str] = None

    def with_cursor(payload: dict[str, Any], cursor: str) -> dict[str, Any]:
        payload["cursor"] = cursor
        payload["next_target"] = {**echo, "cursor": cursor}
        return payload

    def sized(count: int) -> dict[str, Any]:
        payload = build(count)
        if payload.get("truncated") and stand_in is not None:
            with_cursor(payload, stand_in)
        return payload

    if page_len >= 1:
        whole = build(page_len)
        may_cut = (
            page_len > 1 and measure is not None and max_bytes is not None
            and measure(whole) > max_bytes
        )
        if whole.get("truncated") or may_cut:
            stand_in = "x" * (len(issue_for(page_len)) + _CURSOR_SLACK)
    count = fit_page(sized, page_len, max_bytes=max_bytes, measure=measure)
    payload = build(count)
    if payload.get("truncated") and count >= 1:
        with_cursor(payload, issue_for(count))
    return payload


def _binding(
    conn: psycopg.Connection,
    session: Session,
    *,
    kind: str,
    generations: Mapping[str, Any],
    subtype: Optional[str],
    projection: Mapping[str, Any],
) -> dict[str, Any]:
    """What a history cursor is bound to.

    The resolved generation range, not the session pin: a page read after a
    new publish reads the same range. The catalog version (the volatile
    fields), the reader and the Nomos policy are bound as for every other
    continuation token, so a page is never read under another policy or by
    another reader.
    """
    authority_digest, policy_fingerprint = _continuation.authority_fingerprint(
        conn, session,
    )
    return {
        "kind": kind,
        "deployment": session.deployment_name,
        "database": session.db_fingerprint,
        "branch": session.branch_name,
        "generation": dict(generations),
        "catalog_version": session.catalog_version_id,
        "subtype": subtype,
        "path": None,
        "sort": "newest_first" if kind == _NODE_HISTORY_TOKEN else "node_id",
        "projection": dict(projection),
        "authority": authority_digest,
        "policy": policy_fingerprint,
        "key_id": _continuation.signing_key_id(),
    }


def _cursor_range(cursor: str) -> dict[str, str]:
    """The generation range a cursor carries, from its verified token.

    The signature is checked here; the whole binding is compared with the
    current call by ``consume_continuation`` before the cursor is used.
    """
    body = _continuation.open_continuation(cursor)
    generations = body["binding"].get("generation")
    if (
        not isinstance(generations, dict)
        or not isinstance(generations.get("from"), str)
        or not isinstance(generations.get("to"), str)
    ):
        raise _continuation.ContinuationTokenInvalid(
            "the cursor does not belong to a history read",
            data={"reason": "binding_mismatch", "mismatched": ["kind"]},
            fix_hint="restart the read without a cursor",
        )
    return {"from": generations["from"], "to": generations["to"]}


# ── node_history ───────────────────────────────────────────────────────


def read_node_history(
    conn: psycopg.Connection,
    session: Session,
    target: Mapping[str, Any],
    *,
    max_bytes: Optional[int] = None,
    measure: Optional[Callable[[dict[str, Any]], int]] = None,
    row_budget: Optional[int] = None,
) -> dict[str, Any]:
    """``inspect(target={kind: "node_history", ...})``."""
    if row_budget is None:
        row_budget = NODE_HISTORY_ROW_BUDGET
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.nomos.read_filter import _assert_multi_user_subject
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_requires_nomos_runtime,
    )

    assert_session_matches(conn, session)
    # Before any read and under every mode: with Nomos off the read filter
    # returns rows unfiltered, and an empty history never reaches it.
    _assert_multi_user_subject(session, None)
    node_id = target.get("node_id")
    if not isinstance(node_id, str) or not node_id:
        raise _invalid(
            "node_history needs node_id", "node_id",
            "pass target.node_id, the id of the record whose versions you want",
        )
    attr_keys = _attr_keys(target)
    limit = _limit(target)
    cursor = _cursor_argument(target)
    from_arg = _generation_argument(
        target.get("from", target.get("from_")), "from",
    )
    to_arg = _generation_argument(target.get("to"), "to")
    projection = {
        "node_id": node_id, "attr_keys": attr_keys, "limit": limit,
        "from": from_arg, "to": to_arg,
    }

    before: Optional[str] = None
    if cursor is not None:
        bound = _cursor_range(cursor)
        state = _continuation.consume_continuation(
            cursor,
            expected_binding=_binding(
                conn, session, kind=_NODE_HISTORY_TOKEN, generations=bound,
                subtype=None, projection=projection,
            ),
        )
        before = state.get("before")
        if not isinstance(before, str):
            raise _continuation.ContinuationTokenInvalid(
                "the cursor has no usable position",
                data={"reason": "cursor_invalid"},
                fix_hint="restart the read without a cursor",
            )
        # Still published and retained: a generation compacted since the
        # first page is refused, never read from what is left.
        from_id = resolve_history_generation(conn, session, bound["from"])
        to_id = resolve_history_generation(conn, session, bound["to"])
        explicit_from: Optional[str] = from_id
    else:
        to_id = (
            resolve_history_generation(conn, session, to_arg)
            if to_arg is not None else str(session.generation_id)
        )
        explicit_from = (
            resolve_history_generation(conn, session, from_arg)
            if from_arg is not None else None
        )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    runtime_required = deployment_requires_nomos_runtime(session.deployment_name)
    try:
        history = store.node_version_history(
            node_id,
            branch=session.branch_name,
            deployment_name=session.deployment_name,
            from_generation=explicit_from,
            to_generation=to_id,
            row_budget=row_budget,
        )
    except HistoryRowBudgetExceeded:
        if runtime_required and not _visible_at_pin(conn, session, node_id):
            # A node the reader cannot see is refused as absent, never with
            # the budget refusal that would say it has many stored versions.
            raise _not_visible(conn, session, node_id) from None
        raise
    if not history.generations:
        raise _not_visible(conn, session, node_id)
    from_id = history.generations[0].generation_id
    to_id = history.generations[-1].generation_id

    distinct: dict[int, NodeRowVersion] = {}
    for state in history.states:
        if state is not None:
            distinct.setdefault(state.live_lo, state)
    decided = decide_rows(
        conn, session,
        [_version_row(node_id, version) for version in distinct.values()],
        runtime_required=runtime_required,
    )
    states = [
        None if state is None else decided.get(state.live_lo)
        for state in history.states
    ]
    volatile = volatile_fields_at_pin(conn, session)
    versions = merge_versions(history.generations, states, volatile)
    if not versions:
        # Absent or hidden at every generation of the range: the refusal
        # `inspect(kind node)` gives, so the two cannot be told apart.
        raise _not_visible(conn, session, node_id)
    newest_first = list(reversed(versions))
    if before is not None:
        positions = {g.generation_id: i for i, g in enumerate(history.generations)}
        boundary = positions.get(before)
        if boundary is None:
            raise _continuation.ContinuationTokenInvalid(
                "the cursor's position is not in this history",
                data={"reason": "cursor_invalid"},
                fix_hint="restart the read without a cursor",
            )
        newest_first = [
            v for v in newest_first if positions[v.first.generation_id] < boundary
        ]

    page = newest_first[:limit]
    items = [_version_item(version) for version in page]
    label_items(
        conn, session,
        [(item, version.sides()) for item, version in zip(items, page)],
    )
    _selected_values(
        conn, session,
        [
            (item, "values", version.last_source, version.last_attrs)
            for item, version in zip(items, page)
        ],
        attr_keys,
    )
    generations = {"from": from_id, "to": to_id}

    def build(count: int) -> dict[str, Any]:
        return {
            "status": "ok",
            "result_kind": NODE_HISTORY_KIND,
            "node_id": node_id,
            "attrs_layer": "graph",
            "from_generation_id": from_id,
            "to_generation_id": to_id,
            "generations_read": len(history.generations),
            "versions": items[:count],
            "returned_count": count,
            "total_count": len(versions),
            "total_count_basis": "exact",
            "truncated": count < len(newest_first),
            "generation_id": session.generation_id,
            "pinned_generation_id": session.generation_id,
        }

    binding = _binding(
        conn, session, kind=_NODE_HISTORY_TOKEN, generations=generations,
        subtype=None, projection=projection,
    )
    return paged_payload(
        build,
        len(items),
        target=target,
        issue_for=lambda count: _continuation.issue_continuation(
            binding, cursor={"before": page[count - 1].first.generation_id},
        ),
        max_bytes=max_bytes,
        measure=measure,
    )


def _version_item(version: _Version) -> dict[str, Any]:
    return {
        "first": _generation_ref(version.first),
        "last": _generation_ref(version.last),
        "subtype": version.subtype,
        **version.change,
    }


def _visible_at_pin(conn: psycopg.Connection, session: Session, node_id: str) -> bool:
    """Whether the reader sees ``node_id`` at the session pin, decided as
    ``inspect(kind node)`` decides it."""
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout_generation(session.generation_id, branch=session.branch_name)
    rows = [
        {**dict(row), _ROW_KEY: str(row["node_id"])}
        for row in graph.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, "
            "source_revision FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": node_id},
        )
    ]
    return bool(decide_rows(conn, session, rows, runtime_required=True))


def _not_visible(conn: psycopg.Connection, session: Session, node_id: str) -> SubstrateError:
    from okg.substrate.mcp.tools import (
        close_node_ids, node_not_visible, reader_sees_every_row,
    )

    return node_not_visible(
        node_id, session.generation_id,
        close=close_node_ids(conn, session, node_id),
        every_row_readable=reader_sees_every_row(session),
    )


# ── subtype_diff ───────────────────────────────────────────────────────


def _diff_endpoints(
    store: Any, session: Session, from_id: str, to_id: str,
) -> None:
    """Both ends on the session branch's lineage, ordered, from <= to."""
    _, lineage = store.lineage_generations(
        branch=session.branch_name, deployment_name=session.deployment_name,
    )
    index = {g.generation_id: (i, g) for i, g in enumerate(lineage)}
    placed = []
    for requested, end in ((from_id, "from_generation"), (to_id, "to_generation")):
        found = index.get(requested)
        if found is None:
            raise GenerationOutOfScope(
                f"generation {requested!r} is not in the history of branch "
                f"{session.branch_name!r} for deployment {session.deployment_name!r}",
                data={
                    "requested_generation": requested,
                    "deployment_name": session.deployment_name,
                    "branch_name": session.branch_name,
                    "generation_deployment_name": None,
                    "generation_branch": None,
                    "history_end": end,
                },
                fix_hint=(
                    "pass a generation published on this session's branch "
                    "or before its fork point"
                ),
            )
        position, generation = found
        if generation.ordinal is None:
            raise HistoryGenerationUnordered(
                "a diff end has no ordinal on its branch, so its order cannot be read",
                data={
                    "generation_ids": [requested],
                    "unordered_count": 1,
                    "branch": session.branch_name,
                    "deployment_name": session.deployment_name,
                },
                fix_hint="backfill the generation-ordinal registry for this branch",
            )
        placed.append((position, generation))
    (from_pos, from_gen), (to_pos, to_gen) = placed
    if from_pos > to_pos or int(from_gen.ordinal) > int(to_gen.ordinal):  # type: ignore[arg-type]
        raise HistoryRangeInvalid(
            f"subtype_diff from_generation {from_id!r} is newer than "
            f"to_generation {to_id!r}",
            data={"from_generation": from_id, "to_generation": to_id},
            fix_hint="swap from_generation and to_generation",
        )


def _scan_subtype(graph: Any, subtype: str, budget: int) -> dict[str, str]:
    from okg.substrate.mcp.session import _ROW_CONTENT_HASH_SQL

    rows = graph.query(
        "SELECT node_id, subtype, "
        f"{_ROW_CONTENT_HASH_SQL} AS content_hash "
        "FROM okg.graph_nodes WHERE subtype = :subtype LIMIT :cap",
        {"subtype": subtype, "cap": int(budget) + 1},
    )
    if len(rows) > budget:
        raise HistoryRowBudgetExceeded(
            f"subtype_diff scan exceeds its budget of {budget} rows per generation",
            data={
                "budget": int(budget),
                "budget_name": "subtype_diff_scan_row_budget",
                "target_kind": SUBTYPE_DIFF_KIND,
            },
            fix_hint="this subtype is too large to diff in one read",
        )
    return {str(row["node_id"]): str(row["content_hash"]) for row in rows}


def _full_rows(graph: Any, ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    ordered = sorted(ids)
    for start in range(0, len(ordered), _ID_CHUNK):
        chunk = ordered[start: start + _ID_CHUNK]
        for row in graph.query(
            "SELECT node_id, subtype, attrs, source, source_record_id, "
            "source_revision FROM okg.graph_nodes WHERE node_id = ANY(:ids)",
            {"ids": chunk},
        ):
            record = dict(row)
            record["node_id"] = str(record["node_id"])
            record[_ROW_KEY] = record["node_id"]
            out[record["node_id"]] = record
    return out


def read_subtype_diff(
    conn: psycopg.Connection,
    session: Session,
    target: Mapping[str, Any],
    *,
    max_bytes: Optional[int] = None,
    measure: Optional[Callable[[dict[str, Any]], int]] = None,
    scan_budget: Optional[int] = None,
    compare_budget: Optional[int] = None,
    exact_budget: Optional[int] = None,
) -> dict[str, Any]:
    """``inspect(target={kind: "subtype_diff", ...})``."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.session import (
        _PREVIEW_ADVANCE_EXACT_ROW_BUDGET,
        _delta_candidates,
    )
    from okg.substrate.nomos.read_filter import _assert_multi_user_subject
    from okg.substrate.nomos.runtime_enforcement import (
        deployment_nomos_enforcement_mode,
    )

    assert_session_matches(conn, session)
    # Before any read and under every mode: the Nomos-off branch below never
    # calls the read filter that refuses an unidentified multi-user reader.
    _assert_multi_user_subject(session, None)
    if exact_budget is None:
        exact_budget = _PREVIEW_ADVANCE_EXACT_ROW_BUDGET
    if scan_budget is None:
        scan_budget = SUBTYPE_DIFF_SCAN_ROW_BUDGET
    if compare_budget is None:
        compare_budget = SUBTYPE_DIFF_COMPARE_ROW_BUDGET
    subtype = target.get("subtype")
    if not isinstance(subtype, str) or not subtype:
        raise _invalid(
            "subtype_diff needs subtype", "subtype",
            "pass target.subtype, the record type to compare",
        )
    attr_keys = _attr_keys(target)
    limit = _limit(target)
    cursor = _cursor_argument(target)
    from_arg = _generation_argument(target.get("from_generation"), "from_generation")
    if from_arg is None:
        raise _invalid(
            "subtype_diff needs from_generation", "from_generation",
            "pass target.from_generation, the older generation to compare from",
        )
    to_arg = _generation_argument(target.get("to_generation"), "to_generation")
    projection = {
        "attr_keys": attr_keys, "limit": limit,
        "from_generation": from_arg, "to_generation": to_arg,
    }

    after: Optional[str] = None
    if cursor is not None:
        bound = _cursor_range(cursor)
        state = _continuation.consume_continuation(
            cursor,
            expected_binding=_binding(
                conn, session, kind=_SUBTYPE_DIFF_TOKEN, generations=bound,
                subtype=subtype, projection=projection,
            ),
        )
        after = state.get("after")
        if not isinstance(after, str):
            raise _continuation.ContinuationTokenInvalid(
                "the cursor has no usable position",
                data={"reason": "cursor_invalid"},
                fix_hint="restart the read without a cursor",
            )
        from_id = resolve_history_generation(conn, session, bound["from"])
        to_id = resolve_history_generation(conn, session, bound["to"])
    else:
        from_id = resolve_history_generation(conn, session, from_arg)
        to_id = (
            resolve_history_generation(conn, session, to_arg)
            if to_arg is not None else str(session.generation_id)
        )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    _diff_endpoints(store, session, from_id, to_id)
    from_graph = store.checkout_generation(from_id, branch=session.branch_name)
    to_graph = store.checkout_generation(to_id, branch=session.branch_name)
    from_map = _scan_subtype(from_graph, subtype, scan_budget)
    to_map = _scan_subtype(to_graph, subtype, scan_budget)
    from_only, to_only, changed_hash = _delta_candidates(from_map, to_map)
    mode = deployment_nomos_enforcement_mode(session.deployment_name)
    runtime_required = mode != "off"
    volatile = volatile_fields_at_pin(conn, session)
    ignore = volatile_fields_for(volatile, subtype)
    generations = {"from": from_id, "to": to_id}

    base: dict[str, Any] = {
        "status": "ok",
        "result_kind": SUBTYPE_DIFF_KIND,
        "subtype": subtype,
        "attrs_layer": "graph",
        "from_generation_id": from_id,
        "to_generation_id": to_id,
        "generation_id": session.generation_id,
        "pinned_generation_id": session.generation_id,
    }

    entries: dict[str, dict[str, Any]] = {}
    values_from: dict[str, tuple[Optional[str], dict[str, Any]]] = {}
    values_to: dict[str, tuple[Optional[str], dict[str, Any]]] = {}
    if runtime_required:
        delta_size = len(from_only) + len(to_only) + len(changed_hash)
        if delta_size > exact_budget:
            return {
                **base,
                "added_count": None,
                "retracted_count": None,
                "changed_count": None,
                "total_count": None,
                "total_count_basis": "unknown",
                "entries": None,
                "truncated": True,
                "note": _WITHHELD_NOTE,
            }
        from_rows = _full_rows(from_graph, sorted(from_only | changed_hash))
        to_rows = _full_rows(to_graph, sorted(to_only | changed_hash))
        shown_from = decide_rows(
            conn, session, list(from_rows.values()), runtime_required=True,
        )
        shown_to = decide_rows(
            conn, session, list(to_rows.values()), runtime_required=True,
        )
        for node_id in to_only:
            if node_id in shown_to:
                entries[node_id] = {"node_id": node_id, "change": "added"}
        for node_id in from_only:
            if node_id in shown_from:
                entries[node_id] = {"node_id": node_id, "change": "retracted"}
        for node_id in changed_hash:
            before, after_row = shown_from.get(node_id), shown_to.get(node_id)
            if before is None and after_row is None:
                continue
            if before is None:
                entries[node_id] = {"node_id": node_id, "change": "added"}
            elif after_row is None:
                entries[node_id] = {"node_id": node_id, "change": "retracted"}
            else:
                changes = classify_changes(after_row.attrs, before.attrs, ignore=ignore)
                if changes is not None:
                    entries[node_id] = {"node_id": node_id, "change": "changed", **changes}
        values_from = {k: (v.source, v.attrs) for k, v in shown_from.items()}
        values_to = {k: (v.source, v.attrs) for k, v in shown_to.items()}
    else:
        if len(changed_hash) > compare_budget:
            raise HistoryRowBudgetExceeded(
                f"subtype_diff would compare {len(changed_hash)} changed rows, "
                f"over its budget of {compare_budget}",
                data={
                    "budget": int(compare_budget),
                    "budget_name": "subtype_diff_compare_row_budget",
                    "target_kind": SUBTYPE_DIFF_KIND,
                },
                fix_hint="diff two closer generations",
            )
        for node_id in to_only:
            entries[node_id] = {"node_id": node_id, "change": "added"}
        for node_id in from_only:
            entries[node_id] = {"node_id": node_id, "change": "retracted"}
        if changed_hash:
            from_rows = _full_rows(from_graph, sorted(changed_hash))
            to_rows = _full_rows(to_graph, sorted(changed_hash))
            for node_id in changed_hash:
                old_attrs = dict(from_rows[node_id].get("attrs") or {})
                new_attrs = dict(to_rows[node_id].get("attrs") or {})
                changes = classify_changes(new_attrs, old_attrs, ignore=ignore)
                if changes is not None:
                    entries[node_id] = {"node_id": node_id, "change": "changed", **changes}
                values_from[node_id] = (from_rows[node_id].get("source"), old_attrs)
                values_to[node_id] = (to_rows[node_id].get("source"), new_attrs)

    counts = {"added": 0, "retracted": 0, "changed": 0}
    for entry in entries.values():
        counts[entry["change"]] += 1
    ordered = [entries[node_id] for node_id in sorted(entries)]
    if after is not None:
        ordered = [entry for entry in ordered if entry["node_id"] > after]
    page = [dict(entry) for entry in ordered[:limit]]

    if page and not runtime_required:
        # The page's added and retracted rows, for their trust label (and
        # their values when attr_keys names some); at most `limit` rows.
        need_from = [e["node_id"] for e in page if e["change"] == "retracted"]
        need_to = [e["node_id"] for e in page if e["change"] == "added"]
        for node_id, row in _full_rows(from_graph, need_from).items():
            values_from[node_id] = (row.get("source"), dict(row.get("attrs") or {}))
        for node_id, row in _full_rows(to_graph, need_to).items():
            values_to[node_id] = (row.get("source"), dict(row.get("attrs") or {}))
    selections: list[tuple[dict[str, Any], str, Optional[str], Mapping[str, Any]]] = []
    labels: list[tuple[dict[str, Any], list[tuple[Optional[str], Mapping[str, Any]]]]] = []
    for entry in page:
        node_id = entry["node_id"]
        # The entry reads as its newer row (its older one when retracted).
        sides: list[tuple[Optional[str], Mapping[str, Any]]] = []
        if entry["change"] in {"retracted", "changed"}:
            sides.append(values_from[node_id])
            selections.append((entry, "from_values", *values_from[node_id]))
        if entry["change"] in {"added", "changed"}:
            sides.insert(0, values_to[node_id])
            selections.append((entry, "to_values", *values_to[node_id]))
        labels.append((entry, sides))
    label_items(conn, session, labels)
    _selected_values(conn, session, selections, attr_keys)

    total = sum(counts.values())

    def build(count: int) -> dict[str, Any]:
        return {
            **base,
            "added_count": counts["added"],
            "retracted_count": counts["retracted"],
            "changed_count": counts["changed"],
            "total_count": total,
            "total_count_basis": "exact",
            "entries": page[:count],
            "returned_count": count,
            "truncated": count < len(ordered),
        }

    binding = _binding(
        conn, session, kind=_SUBTYPE_DIFF_TOKEN, generations=generations,
        subtype=subtype, projection=projection,
    )
    return paged_payload(
        build,
        len(page),
        target=target,
        issue_for=lambda count: _continuation.issue_continuation(
            binding, cursor={"after": page[count - 1]["node_id"]},
        ),
        max_bytes=max_bytes,
        measure=measure,
    )


__all__ = [
    "NODE_HISTORY_KIND",
    "SUBTYPE_DIFF_COMPARE_ROW_BUDGET",
    "ShownRow",
    "SUBTYPE_DIFF_KIND",
    "SUBTYPE_DIFF_SCAN_ROW_BUDGET",
    "classify_changes",
    "decide_rows",
    "fit_page",
    "merge_versions",
    "paged_payload",
    "read_node_history",
    "read_subtype_diff",
    "resolve_history_generation",
    "volatile_fields_at_pin",
]
