"""DeclarativeLinker — generic substrate-shipped Enricher
driven by `CrossLinkRule` declarations. Subsumes the bespoke
CommitChange / SessionChange / IssueChange / EntityMention
linkers via five named matchers (exact, subject_prefix,
word_boundary, derive_from_path, alias_table).

Per `declarative-cross-source-linker` (2026-04-26).
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
    stable_edge_dedupe_key,
)
from okg.substrate.enrichers.input_quarantine import (
    InputQuarantineKey,
    claim_inputs,
    record_deterministic_failure,
    record_success,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers.rules import (
    CrossLinkRule,
    UniqueFieldFallback,
    load_rules_file,
)


__all__ = ["CrossLinkFieldShapeError", "DeclarativeLinker"]

log = logging.getLogger(__name__)

_MAX_DIRTY_NODE_SCOPE = 10_000
_SAFE_ATTR_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EndpointCache = dict[tuple[Any, ...], list[tuple[Any, ...]]]


def _timing_label_segment(value: Any) -> str:
    normalized = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in str(value or "").strip()
    ).strip("_")
    return normalized or "unnamed"


def _record_timing(timings_ms: dict[str, float], name: str, started: float) -> None:
    elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
    timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)


def _field_values(value: Any, *, expand_string_arrays: bool) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,) if value else ()
    if expand_string_arrays and isinstance(value, list):
        return tuple(item for item in value if isinstance(item, str) and item)
    if value is None:
        return ()
    text = str(value)
    return (text,) if text else ()


class CrossLinkFieldShapeError(SubstrateError):
    """A `derive_from_path` rule read a field whose value it cannot match.

    The matcher reads a scalar, or a list of scalars (one match per item).
    An object, or a list holding an object or a nested list, has no single
    unambiguous string reading (issue #2921). The linker does not raise
    this: it skips that node for that rule, logs the diagnostic, and
    quarantines the node for that rule in `okg.enricher_input_quarantines`
    (see `DeclarativeLinker.enrich`). The class carries the registered code
    and the payload; `value_digest` identifies the unreadable value.
    """

    code = "cross_link_field_shape_unsupported"
    value_digest: str = ""


def _value_digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode("utf-8"),
    ).hexdigest()


def _shape_error(
    message: str,
    *,
    value: Any,
    node_id: str,
    field: str,
    value_type: str,
    item_type: str,
) -> CrossLinkFieldShapeError:
    err = CrossLinkFieldShapeError(
        message,
        data={
            "node_id": node_id,
            "field": field,
            "value_type": value_type,
            "item_type": item_type,
        },
        fix_hint=_SHAPE_FIX_HINT,
    )
    err.value_digest = _value_digest(value)
    return err


# A shape defect is deterministic: the same value fails the same way every
# run, so one failure quarantines the node for that rule.
_SHAPE_QUARANTINE_ATTEMPT_LIMIT = 1
_SHAPE_QUARANTINE_OPERATOR = "declarative_linker.derive_from_path.shape.v1"
_SHAPE_QUARANTINE_MODEL = "deterministic"
_SHAPE_FAILURE_CLASS = "cross_link_field_shape_unsupported"
# Per rule per run, log this many skipped nodes, then one summary line.
_SHAPE_WARNING_LIMIT = 20

_SHAPE_FIX_HINT = (
    "point the rule at a string field or a list of strings, "
    "or emit one string attribute per value"
)


def _path_field_values(
    value: Any,
    *,
    node_id: str,
    field: str,
    shape_skips: list[CrossLinkFieldShapeError],
) -> tuple[str, ...]:
    """Values a `derive_from_path` rule reads from one node's field.

    A string gives itself; a number or boolean gives its text, as it always
    has for a scalar; a list gives each such item (nulls and empty strings
    left out). An object, or a list holding an object or a nested list,
    gives nothing and appends a `CrossLinkFieldShapeError` naming the node
    and field to `shape_skips`; the node's other values are not read.
    """
    if isinstance(value, dict):
        shape_skips.append(_shape_error(
            f"node {node_id!r} field {field!r} is an object; "
            "derive_from_path reads only scalars or lists of scalars",
            value=value,
            node_id=node_id,
            field=field,
            value_type="dict",
            item_type="",
        ))
        return ()
    if not isinstance(value, list):
        return _field_values(value, expand_string_arrays=False)
    out: list[str] = []
    for item in value:
        if isinstance(item, (dict, list)):
            shape_skips.append(_shape_error(
                f"node {node_id!r} field {field!r} is a list holding "
                f"a {type(item).__name__}; derive_from_path reads only "
                "scalars or lists of scalars",
                value=value,
                node_id=node_id,
                field=field,
                value_type="list",
                item_type=type(item).__name__,
            ))
            return ()
        out.extend(_field_values(item, expand_string_arrays=False))
    return tuple(out)


def _index_by_value(
    pairs: list[tuple[str, str]],
) -> dict[str, tuple[str, ...]]:
    """Map each value to every node carrying it, in node-id order.

    Two destinations sharing a value both stay: a capture links to each,
    as the `exact` matcher does, instead of to whichever row came last.
    """
    index: dict[str, set[str]] = {}
    for node_id, value in pairs:
        index.setdefault(value, set()).add(node_id)
    return {value: tuple(sorted(ids)) for value, ids in index.items()}


def _cached_skips(
    endpoint_cache: _EndpointCache | None,
    cache_key: tuple[Any, ...],
) -> list[CrossLinkFieldShapeError]:
    """Skips recorded when `cache_key` was filled (a cache hit replays them,
    so a second rule reading the same field still reports the bad node)."""
    if endpoint_cache is None:
        return []
    return list(endpoint_cache.get(("shape_skips", cache_key), ()))  # type: ignore[arg-type]


def _store_skips(
    endpoint_cache: _EndpointCache | None,
    cache_key: tuple[Any, ...],
    skips: list[CrossLinkFieldShapeError],
) -> None:
    if endpoint_cache is not None:
        endpoint_cache[("shape_skips", cache_key)] = list(skips)  # type: ignore[assignment]


def _dedupe_skips(
    shape_skips: list[CrossLinkFieldShapeError],
) -> list[CrossLinkFieldShapeError]:
    """One skip per (node, field): a node read by several fetches in one
    rule counts once."""
    seen: set[tuple[str, str]] = set()
    unique: list[CrossLinkFieldShapeError] = []
    for skip in shape_skips:
        key = (str(skip.data["node_id"]), str(skip.data["field"]))
        if key in seen:
            continue
        seen.add(key)
        unique.append(skip)
    return unique


def _log_shape_skips(
    rule: CrossLinkRule,
    skips: list[CrossLinkFieldShapeError],
    *,
    counted: str,
) -> None:
    """Warn about the first `_SHAPE_WARNING_LIMIT` nodes in `skips`, then
    one summary line with the total. `counted` says what the total counts:
    "skipped" for a direct run, "newly quarantined" under the publisher
    (nodes already quarantined are not logged again)."""
    for skip in skips[:_SHAPE_WARNING_LIMIT]:
        log.warning(
            "%s: cross-link rule %r skipped %s (%s)",
            CrossLinkFieldShapeError.code,
            rule.id,
            skip.message,
            skip.fix_hint,
        )
    suppressed = len(skips) - _SHAPE_WARNING_LIMIT
    if suppressed > 0:
        log.warning(
            "%s: cross-link rule %r: %d node(s) %s in total; "
            "%d more suppressed",
            CrossLinkFieldShapeError.code,
            rule.id,
            len(skips),
            counted,
            suppressed,
        )


def _shape_error_line(
    rule: CrossLinkRule,
    skips: list[CrossLinkFieldShapeError],
) -> str:
    return (
        f"{CrossLinkFieldShapeError.code}: cross-link rule {rule.id!r} "
        f"skipped {len(skips)} node(s) whose field it cannot read; "
        f"first: {skips[0].message}"
    )


_ITEM_REF_KIND = "derive_from_path"


def _shape_item_ref(rule_id: str, node_id: str, field: str) -> str:
    """A JSON array `["derive_from_path", rule, field, node]`: any character
    in any part round-trips exactly through `_parse_shape_item_ref`."""
    return json.dumps(
        [_ITEM_REF_KIND, rule_id, field, node_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _parse_shape_item_ref(item_ref: str) -> tuple[str, str, str] | None:
    """Return (rule_id, node_id, field), or None for another item kind."""
    try:
        parts = json.loads(item_ref)
    except ValueError:
        return None
    if (
        not isinstance(parts, list)
        or len(parts) != 4
        or parts[0] != _ITEM_REF_KIND
        or not all(isinstance(part, str) for part in parts)
    ):
        return None
    return parts[1], parts[3], parts[2]


def _active_catalog_version(cur) -> Optional[int]:
    cur.execute(
        "SELECT max(catalog_version_id) FROM okg.catalog_versions "
        " WHERE status = 'applied'"
    )
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def _candidate_alias_target_subtypes(
    cur, *, cv: int,
) -> tuple[str, ...]:
    """Subtypes whose ontology declared `okg_alias_fields` —
    i.e. the universe of subtypes that an alias_table dst can
    legitimately resolve to. Read from `okg.subtypes.metadata`
    at the active cv (populated by modules_compose)."""
    cur.execute(
        "SELECT name FROM okg.subtypes "
        " WHERE catalog_version_id = %s "
        "   AND metadata ? 'alias_fields'",
        (cv,),
    )
    return tuple(name for (name,) in cur.fetchall())


class DeclarativeLinker:
    """Run a list of cross-link rules against the Chronos graph
    (`okg.graph_nodes` / `okg.graph_edges`), emit `references`
    edge_facts.

    `enrich()` walks each rule, dispatches to the named matcher,
    and emits forward (+ optional reverse) edges with
    `provenance='derived_deterministic'` and a stable derived-edge
    dedupe key. Rules whose subtypes are not in the active
    catalog are silently skipped (a deployment that doesn't
    load the forge module won't fire forge rules)."""

    name: str = "declarative"
    emits_edge_types: tuple[str, ...] = ("references",)

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        rules: Optional[list[CrossLinkRule]] = None,
        rules_files: Optional[list[str]] = None,
        rules_dir: Optional[str] = None,
        include_rule_ids: Optional[list[str]] = None,
        exclude_rule_ids: Optional[list[str]] = None,
        publisher_skip_when_clean: Optional[bool] = None,
        publisher_skip_alias_table_when_clean: Optional[bool] = None,
    ) -> None:
        """Construct a DeclarativeLinker. Sources of rules
        (in priority order; first non-empty wins):

          1. `rules=[CrossLinkRule, ...]` — programmatic.
          2. `rules_files=["path/a.yaml", ...]` — explicit list.
          3. `rules_dir="path/"` — every `*.yaml` in dir.
          4. Default: every substrate-shipped cross-link YAML
             under `okg/substrate/library/ontologies/<m>/
             cross_links/*.yaml`. Per
             `add-module-manifests-and-composition`
             (2026-05-19), rules now live next to their owning
             module instead of in a flat `cross_links/`
             directory.
        """
        if rules is not None:
            collected = list(rules)
        elif rules_files:
            collected = []
            for p in rules_files:
                collected.extend(load_rules_file(Path(p)))
        else:
            collected = []
            if rules_dir is not None:
                root = Path(rules_dir)
                if root.is_dir():
                    for path in sorted(root.glob("*.yaml")):
                        collected.extend(load_rules_file(path))
            else:
                ontologies_root = (
                    Path(__file__).resolve().parents[1]
                    / "ontologies"
                )
                if ontologies_root.is_dir():
                    for path in sorted(
                        ontologies_root.glob("*/cross_links/*.yaml"),
                    ):
                        collected.extend(load_rules_file(path))
        include = set(include_rule_ids or ())
        exclude = set(exclude_rule_ids or ())
        if include:
            collected = [rule for rule in collected if rule.id in include]
        if exclude:
            collected = [rule for rule in collected if rule.id not in exclude]
        if name is not None:
            self.name = name
        self.publisher_skip_when_clean = (
            True if publisher_skip_when_clean is None
            else bool(publisher_skip_when_clean)
        )
        self.publisher_skip_alias_table_when_clean = bool(
            publisher_skip_alias_table_when_clean
        )
        # Set by the publisher before each run (`_set_publisher_context`);
        # the input-quarantine ledger is keyed by deployment and branch.
        self._publisher_deployment_name: str | None = None
        self._publisher_branch_name: str | None = None
        self.rules = collected
        # Aggregate every read subtype across all rules; the
        # publisher-side validator uses this to confirm subtypes
        # are loaded.
        reads: set[str] = set()
        edge_reads: set[str] = set()
        emits: set[str] = set()
        narrowings: set[tuple[str, str, str]] = set()
        self._has_alias_table_rules = False
        for r in self.rules:
            reads.update(r.src.subtypes)
            reads.update(r.dst.subtypes)
            if r.src.via_edge is not None:
                reads.add(r.src.via_edge.dst_subtype)
                edge_reads.add(r.src.via_edge.edge_type)
            if r.dst.via == "alias_table":
                self._has_alias_table_rules = True
            if r.dst.unique_field_fallback is not None:
                # The fallback links a mention to a node of this subtype.
                # The publisher trims each linker's changed nodes to its
                # read subtypes, so without this a newly published node of
                # the fallback subtype would never reach the linker.
                reads.add(r.dst.unique_field_fallback.subtype)
            emits.add(r.edge)
            # Narrowings for forward direction. Rules whose dst
            # is `via: alias_table` resolve to an unbounded set
            # at runtime; the deployment composer auto-widens
            # narrowings to every alias-target subtype, but the
            # linker doesn't pre-declare those triples here
            # (they'd be redundant with the auto-widened ones).
            if r.dst.via != "alias_table":
                for s in r.src.subtypes:
                    for d in r.dst.subtypes:
                        narrowings.add((s, r.edge, d))
                        if r.bidirectional:
                            narrowings.add((d, r.edge, s))
        self.reads_subtypes: tuple[str, ...] = tuple(sorted(reads))
        self.reads_edge_types: tuple[str, ...] = tuple(sorted(edge_reads))
        self.emits_edge_types = tuple(sorted(emits)) or ("references",)
        self.requires_narrowings: tuple[
            tuple[str, str, str], ...,
        ] = tuple(sorted(narrowings))
        # Alias-table destinations are resolved from the active catalog at
        # runtime, so their edge triples cannot be enumerated honestly at
        # construction time.  Declare that open dimension explicitly: the
        # producer-envelope validator must never mistake the intentionally
        # empty ``requires_narrowings`` of an alias-only linker for an
        # undeclared write surface.
        self.unbounded_write_dimensions: tuple[str, ...] = (
            ("edges",) if self._has_alias_table_rules else ()
        )

    def can_skip_clean(
        self,
        conn: psycopg.Connection,
        ctx: IncrementalContext,
    ) -> bool:
        del conn, ctx
        # Alias-table rules depend on okg.aliases. Deployments that can
        # prove aliases only move with the catalog, or have a separate
        # alias-change trigger, may opt into clean skips; the publisher's
        # catalog guard still disables skips across catalog changes.
        return (
            not self._has_alias_table_rules
            or self.publisher_skip_alias_table_when_clean
        )

    def _set_publisher_context(
        self,
        *,
        deployment_name: str,
        branch_name: str,
    ) -> None:
        """Receive the publisher's deployment and branch (optional hook)."""
        self._publisher_deployment_name = str(deployment_name)
        self._publisher_branch_name = str(branch_name)

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        """Run every active rule and emit its edges.

        A `derive_from_path` rule skips a node whose field it cannot read
        (`CrossLinkFieldShapeError`). With publisher context, the node is
        quarantined for that rule in `okg.enricher_input_quarantines`: the
        run stays complete so the watermark advances, `n_quarantined`
        counts new quarantines, and `okg doctor` reports each active row.
        A quarantine resolves when a later run reads the node's field
        cleanly or the node is gone. Without publisher context (a direct
        call) there is no ledger to write, so the run is marked incomplete
        with an error naming the rule and the skip count instead."""
        timings_ms: dict[str, float] = {}
        dirty_node_ids = self._dirty_node_scope(incremental)
        if (
            incremental is not None
            and not incremental.is_first_run
            and dirty_node_ids is not None
            and not dirty_node_ids
            and not incremental.dirty_edge_ids
            and not self._has_alias_table_rules
        ):
            return EnrichResult(
                enricher_name=self.name,
                n_edges_emitted=0,
                timings_ms=timings_ms,
            )

        n_total_emitted = 0
        n_total_skipped = 0
        endpoint_cache: _EndpointCache = {}
        # Without publisher context: one entry per rule that skipped a node,
        # and the run is marked incomplete (see the docstring).
        shape_errors: list[str] = []
        n_quarantined = 0
        quarantine_scope = self._quarantine_scope()

        with conn.cursor() as cur:
            started = time.perf_counter()
            cv = _active_catalog_version(cur)
            _record_timing(timings_ms, "active_catalog_version", started)
            if cv is None:
                return EnrichResult(
                    enricher_name=self.name,
                    n_edges_emitted=0,
                    timings_ms=timings_ms,
                )
            # Set of subtypes loaded at the active cv. Rules
            # referencing unloaded subtypes are silently skipped.
            started = time.perf_counter()
            cur.execute(
                "SELECT name FROM okg.subtypes "
                " WHERE catalog_version_id = %s",
                (cv,),
                )
            loaded = {n for (n,) in cur.fetchall()}
            _record_timing(timings_ms, "load_subtypes", started)
            started = time.perf_counter()
            dirty_node_subtypes = self._dirty_node_subtypes(
                cur,
                dirty_node_ids=dirty_node_ids,
                endpoint_cache=endpoint_cache,
            )
            _record_timing(timings_ms, "dirty_node_subtypes", started)

            for rule in self.rules:
                if not self._rule_active(rule, loaded):
                    continue
                rule_label = _timing_label_segment(rule.id)
                started = time.perf_counter()
                shape_skips: list[CrossLinkFieldShapeError] = []
                pairs = self._dispatch_matcher(
                    cur,
                    rule=rule,
                    cv=cv,
                    dirty_node_ids=dirty_node_ids,
                    dirty_edge_ids=(
                        frozenset()
                        if incremental is None else incremental.dirty_edge_ids
                    ),
                    dirty_node_subtypes=dirty_node_subtypes,
                    dirty_node_attr_changes_by_subtype=(
                        None if incremental is None
                        else incremental.dirty_node_attr_changes_by_subtype
                    ),
                    endpoint_cache=endpoint_cache,
                    shape_skips=shape_skips,
                )
                _record_timing(
                    timings_ms,
                    f"rule.{rule_label}.dispatch_matcher",
                    started,
                )
                if shape_skips:
                    unique = _dedupe_skips(shape_skips)
                    if quarantine_scope is None:
                        _log_shape_skips(rule, unique, counted="skipped")
                        shape_errors.append(_shape_error_line(rule, unique))
                    else:
                        fresh = self._quarantine_shape_skips(
                            conn,
                            rule,
                            unique,
                            scope=quarantine_scope,
                            generation_id=generation_id,
                        )
                        _log_shape_skips(
                            rule, fresh, counted="newly quarantined",
                        )
                        n_quarantined += len(fresh)
                if not pairs:
                    continue
                started = time.perf_counter()
                emitted, skipped = self._emit_edges(
                    cur,
                    rule=rule,
                    pairs=pairs,
                    generation_id=generation_id,
                )
                _record_timing(
                    timings_ms,
                    f"rule.{rule_label}.emit_edges",
                    started,
                )
                n_total_emitted += emitted
                n_total_skipped += skipped

            if quarantine_scope is not None:
                started = time.perf_counter()
                self._resolve_shape_quarantines(
                    conn,
                    cur,
                    scope=quarantine_scope,
                    generation_id=generation_id,
                )
                _record_timing(timings_ms, "resolve_shape_quarantines", started)

        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_total_emitted,
            n_edges_skipped=n_total_skipped,
            timings_ms=timings_ms,
            incomplete=bool(shape_errors),
            error="; ".join(shape_errors) or None,
            n_quarantined=n_quarantined,
        )

    def _quarantine_scope(self) -> tuple[str, str] | None:
        if self._publisher_deployment_name and self._publisher_branch_name:
            return (self._publisher_deployment_name, self._publisher_branch_name)
        return None

    def _shape_quarantine_key(
        self,
        *,
        scope: tuple[str, str],
        rule: CrossLinkRule,
        node_id: str,
        field: str,
        value_digest: str,
    ) -> InputQuarantineKey:
        """Ledger key: the item is (rule, node, field); the signature is the
        value, so a changed value is a new episode."""
        rule_hash = hashlib.sha256(
            json.dumps(
                [rule.id, field, rule.dst.derive_from_path.regex
                 if rule.dst.derive_from_path else ""],
            ).encode("utf-8"),
        ).hexdigest()
        signature = hashlib.sha256(
            "|".join((
                _SHAPE_QUARANTINE_OPERATOR, rule_hash, value_digest,
            )).encode("utf-8"),
        ).hexdigest()
        return InputQuarantineKey(
            deployment_name=scope[0],
            branch_name=scope[1],
            enricher_name=self.name,
            item_ref=_shape_item_ref(rule.id, node_id, field),
            input_signature=signature,
            operator_version=_SHAPE_QUARANTINE_OPERATOR,
            model_id=_SHAPE_QUARANTINE_MODEL,
            prompt_hash=rule_hash,
            input_hash=value_digest,
        )

    def _quarantine_shape_skips(
        self,
        conn: psycopg.Connection,
        rule: CrossLinkRule,
        skips: list[CrossLinkFieldShapeError],
        *,
        scope: tuple[str, str],
        generation_id: int,
    ) -> list[CrossLinkFieldShapeError]:
        """Quarantine each skipped node for `rule`; return the new ones.

        A node already quarantined with the same value is skipped quietly:
        its finding is already in the ledger."""
        keyed = [
            (
                self._shape_quarantine_key(
                    scope=scope,
                    rule=rule,
                    node_id=str(skip.data["node_id"]),
                    field=str(skip.data["field"]),
                    value_digest=skip.value_digest,
                ),
                skip,
            )
            for skip in skips
        ]
        claims = claim_inputs(
            conn,
            [key for key, _skip in keyed],
            attempt_limit=_SHAPE_QUARANTINE_ATTEMPT_LIMIT,
            generation_id=generation_id,
        )
        fresh: list[CrossLinkFieldShapeError] = []
        for key, skip in keyed:
            claim = claims[key.identity]
            if not claim.dispatch:
                continue
            record_deterministic_failure(
                conn,
                claim,
                failure_class=_SHAPE_FAILURE_CLASS,
                error=f"cross-link rule {rule.id!r}: {skip.message}",
                generation_id=generation_id,
            )
            fresh.append(skip)
        return fresh

    def _resolve_shape_quarantines(
        self,
        conn: psycopg.Connection,
        cur,
        *,
        scope: tuple[str, str],
        generation_id: int,
    ) -> None:
        """Resolve this linker's active shape quarantines whose node now
        reads cleanly or is gone. Rows naming a rule this linker no longer
        runs are left for the operator."""
        cur.execute(
            "SELECT item_ref FROM okg.enricher_input_quarantines "
            " WHERE deployment_name = %s AND branch_name = %s "
            "   AND enricher_name = %s AND status = 'active' "
            "   AND operator_version = %s",
            (scope[0], scope[1], self.name, _SHAPE_QUARANTINE_OPERATOR),
        )
        rules_by_id = {rule.id: rule for rule in self.rules}
        pending: list[tuple[CrossLinkRule, str, str]] = []
        for (item_ref,) in cur.fetchall():
            parsed = _parse_shape_item_ref(str(item_ref))
            if parsed is None or parsed[0] not in rules_by_id:
                continue
            pending.append((rules_by_id[parsed[0]], parsed[1], parsed[2]))
        if not pending:
            return
        # Strict: a failed read must not look like "every node is gone",
        # which would resolve every active quarantine. It raises instead,
        # so the run fails and the rows stay active.
        rows = _chronos.query_strict(
            cur,
            "SELECT node_id, attrs FROM okg.graph_nodes "
            " WHERE node_id = ANY(:node_ids)",
            {"node_ids": sorted({node_id for _r, node_id, _f in pending})},
        )
        attrs_by_node = {
            str(row["node_id"]): (row["attrs"] or {}) for row in rows
        }
        keys: list[InputQuarantineKey] = []
        for rule, node_id, field in pending:
            attrs = attrs_by_node.get(node_id)
            value = None if attrs is None else attrs.get(field)
            if attrs is not None:
                still_bad: list[CrossLinkFieldShapeError] = []
                _path_field_values(
                    value, node_id=node_id, field=field, shape_skips=still_bad,
                )
                if still_bad:
                    continue
            keys.append(self._shape_quarantine_key(
                scope=scope,
                rule=rule,
                node_id=node_id,
                field=field,
                value_digest=_value_digest(
                    value if attrs is not None else {"__node_gone__": node_id},
                ),
            ))
        if not keys:
            return
        claims = claim_inputs(
            conn,
            keys,
            attempt_limit=_SHAPE_QUARANTINE_ATTEMPT_LIMIT,
            generation_id=generation_id,
        )
        for claim in claims.values():
            if claim.dispatch:
                # Resolves this clean signature and supersedes the active
                # finding for the same rule, node and field.
                record_success(conn, claim)

    @staticmethod
    def _dirty_node_scope(
        incremental: IncrementalContext | None,
    ) -> frozenset[str] | None:
        if incremental is None or incremental.is_first_run:
            return None
        dirty = incremental.dirty_node_ids
        if len(dirty) > _MAX_DIRTY_NODE_SCOPE:
            return None
        return dirty

    @staticmethod
    def _dirty_node_subtypes(
        cur,
        *,
        dirty_node_ids: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> frozenset[str] | None:
        if dirty_node_ids is None:
            return None
        if not dirty_node_ids:
            return frozenset()
        cache_key = ("dirty_node_subtypes", tuple(sorted(dirty_node_ids)))
        if cache_key in endpoint_cache:
            cached = endpoint_cache[cache_key]
            return frozenset(subtype for _node_id, subtype in cached)
        rows = _chronos.query(
            cur,
            """
            SELECT node_id, subtype
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": list(dirty_node_ids)},
        )
        if not rows:
            return None
        pairs = [
            (str(row["node_id"]), str(row["subtype"]))
            for row in rows
        ]
        endpoint_cache[cache_key] = pairs
        return frozenset(subtype for _node_id, subtype in pairs)

    @staticmethod
    def _any_live_node(
        cur,
        *,
        subtypes: tuple[str, ...],
        endpoint_cache: _EndpointCache | None = None,
    ) -> bool:
        """Whether any live node has one of ``subtypes``.

        An exact match pairs a dirty side with the other side's values, so
        when the other side has no node at all there is no pair, and the
        dirty side's fields need not be read (issue #2941: a rule whose
        source subtypes are absent read every dirty code node under the
        publish lock for no output)."""
        cache_key = ("any_live", tuple(subtypes))
        if endpoint_cache is not None and cache_key in endpoint_cache:
            return bool(endpoint_cache[cache_key])
        rows = _chronos.query(
            cur,
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
             LIMIT 1
            """,
            {"subtypes": list(subtypes)},
        )
        found = bool(rows)
        if endpoint_cache is not None:
            endpoint_cache[cache_key] = found  # type: ignore[assignment]
        return found

    @staticmethod
    def _dirty_intersects(
        dirty_node_subtypes: frozenset[str] | None,
        subtypes: tuple[str, ...],
    ) -> bool:
        return (
            dirty_node_subtypes is None
            or bool(dirty_node_subtypes.intersection(subtypes))
        )

    @staticmethod
    def _rule_active(
        rule: CrossLinkRule, loaded: set[str],
    ) -> bool:
        for s in rule.src.subtypes:
            if s not in loaded:
                return False
        if rule.dst.via != "alias_table":
            for d in rule.dst.subtypes:
                if d not in loaded:
                    return False
        if rule.src.via_edge is not None:
            if rule.src.via_edge.dst_subtype not in loaded:
                return False
        return True

    def _dispatch_matcher(
        self,
        cur,
        *,
        rule: CrossLinkRule,
        cv: int,
        dirty_node_ids: frozenset[str] | None,
        dirty_edge_ids: frozenset[int],
        dirty_node_subtypes: frozenset[str] | None,
        dirty_node_attr_changes_by_subtype: (
            dict[str, dict[str, frozenset[str]]] | None
        ),
        endpoint_cache: _EndpointCache,
        shape_skips: list[CrossLinkFieldShapeError],
    ) -> set[tuple[str, str]]:
        m = rule.matcher
        if m == "exact":
            return self._match_exact(
                cur,
                rule,
                dirty_node_ids=dirty_node_ids,
                dirty_node_subtypes=dirty_node_subtypes,
                endpoint_cache=endpoint_cache,
            )
        if m == "subject_prefix":
            return self._match_subject_prefix(
                cur,
                rule,
                dirty_node_ids=dirty_node_ids,
                dirty_node_subtypes=dirty_node_subtypes,
                endpoint_cache=endpoint_cache,
            )
        if m == "word_boundary":
            return self._match_word_boundary(
                cur,
                rule,
                dirty_node_ids=dirty_node_ids,
                dirty_node_subtypes=dirty_node_subtypes,
                endpoint_cache=endpoint_cache,
            )
        if m == "derive_from_path":
            return self._match_derive_from_path(
                cur,
                rule,
                dirty_node_ids=dirty_node_ids,
                dirty_edge_ids=dirty_edge_ids,
                dirty_node_subtypes=dirty_node_subtypes,
                dirty_node_attr_changes_by_subtype=(
                    dirty_node_attr_changes_by_subtype
                ),
                endpoint_cache=endpoint_cache,
                shape_skips=shape_skips,
            )
        if m == "alias_table":
            return self._match_alias_table(
                cur,
                rule,
                cv=cv,
                dirty_node_ids=dirty_node_ids,
                endpoint_cache=endpoint_cache,
            )
        return set()

    @staticmethod
    def _fetch_subtype_field(
        cur,
        *,
        subtypes: tuple[str, ...],
        field: str,
        only_node_ids: frozenset[str] | None = None,
        endpoint_cache: _EndpointCache | None = None,
        expand_string_arrays: bool = False,
        shape_skips: list[CrossLinkFieldShapeError] | None = None,
    ) -> list[tuple[str, str]]:
        """Return [(node_id, value)] for every live row of any
        of `subtypes` whose `attrs->>field` is non-empty.

        Passing `shape_skips` reads values the way `derive_from_path`
        does (`_path_field_values`): a list of scalars gives one row per
        item, and a node holding an object or nested list gives no row
        and is appended to `shape_skips`.

        Extract the field in Python rather than via `attrs->>:field`:
        the Chronos query rewriter turns a bind-param JSON key into
        `json_extract_path_text(attrs, $1)`, which Postgres cannot
        type-resolve for an `unknown` param."""
        if only_node_ids is not None and not only_node_ids:
            return []
        cache_key = (
            "field",
            tuple(subtypes),
            field,
            expand_string_arrays,
            shape_skips is not None,
            None if only_node_ids is None else tuple(sorted(only_node_ids)),
        )
        if endpoint_cache is not None and cache_key in endpoint_cache:
            if shape_skips is not None:
                shape_skips.extend(_cached_skips(endpoint_cache, cache_key))
            return endpoint_cache[cache_key]
        node_filter = (
            "AND node_id = ANY(:node_ids)"
            if only_node_ids is not None else ""
        )
        params: dict[str, Any] = {"subtypes": list(subtypes)}
        if only_node_ids is not None:
            params["node_ids"] = list(only_node_ids)
        rows = _chronos.query(
            cur,
            f"""
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
                   {node_filter}
            """,
            params,
        )
        if only_node_ids is not None and not rows:
            rows = _chronos.query(
                cur,
                """
                SELECT node_id, subtype, attrs
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": list(only_node_ids)},
            )
            subtype_set = set(subtypes)
            rows = [
                row for row in rows
                if str(row.get("subtype") or "") in subtype_set
            ]
        out: list[tuple[str, str]] = []
        skips: list[CrossLinkFieldShapeError] = []
        for row in rows:
            raw = (row["attrs"] or {}).get(field)
            values = (
                _path_field_values(
                    raw,
                    node_id=str(row["node_id"]),
                    field=field,
                    shape_skips=skips,
                )
                if shape_skips is not None
                else _field_values(raw, expand_string_arrays=expand_string_arrays)
            )
            out.extend((str(row["node_id"]), value) for value in values)
        if shape_skips is not None:
            shape_skips.extend(skips)
            _store_skips(endpoint_cache, cache_key, skips)
        if endpoint_cache is not None:
            endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _fetch_subtype_fields_concat(
        cur,
        *,
        subtypes: tuple[str, ...],
        fields: tuple[str, ...],
        only_node_ids: frozenset[str] | None = None,
        endpoint_cache: _EndpointCache | None = None,
    ) -> list[tuple[str, str]]:
        """Return [(node_id, concatenated_text)]; missing fields
        contribute the empty string. Used by word_boundary when
        src.fields=[title, body] etc."""
        if only_node_ids is not None and not only_node_ids:
            return []
        cache_key = (
            "fields",
            tuple(subtypes),
            tuple(fields),
            None if only_node_ids is None else tuple(sorted(only_node_ids)),
        )
        if endpoint_cache is not None and cache_key in endpoint_cache:
            return endpoint_cache[cache_key]
        node_filter = (
            "AND node_id = ANY(:node_ids)"
            if only_node_ids is not None else ""
        )
        params: dict[str, Any] = {"subtypes": list(subtypes)}
        if only_node_ids is not None:
            params["node_ids"] = list(only_node_ids)
        rows = _chronos.query(
            cur,
            f"""
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
                   {node_filter}
            """,
            params,
        )
        if only_node_ids is not None and not rows:
            rows = _chronos.query(
                cur,
                """
                SELECT node_id, subtype, attrs
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": list(only_node_ids)},
            )
            subtype_set = set(subtypes)
            rows = [
                row for row in rows
                if str(row.get("subtype") or "") in subtype_set
            ]
        out: list[tuple[str, str]] = []
        for row in rows:
            attrs = row["attrs"]
            parts = []
            for f in fields:
                v = (attrs or {}).get(f)
                if isinstance(v, str) and v:
                    parts.append(v)
            if parts:
                out.append((str(row["node_id"]), "\n".join(parts)))
        if endpoint_cache is not None:
            endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _fetch_subtype_field_matching_values(
        cur,
        *,
        subtypes: tuple[str, ...],
        field: str,
        values: set[str],
        endpoint_cache: _EndpointCache | None = None,
        strict: bool = False,
        shape_skips: list[CrossLinkFieldShapeError] | None = None,
    ) -> list[tuple[str, str]]:
        """Return `(node_id, value)` for nodes whose `field` holds a value.

        The values bind as one array, so any number of them fits in one
        query. With `strict=True` a failed query raises instead of reading
        as "no node matches"; callers that must not mistake an error for an
        empty answer pass it. Passing `shape_skips` reads a matched node's
        field as `_fetch_subtype_field` does with it.
        """
        if not values:
            return []
        if not _SAFE_ATTR_FIELD.match(field):
            rows = DeclarativeLinker._fetch_subtype_field(
                cur,
                subtypes=subtypes,
                field=field,
                endpoint_cache=endpoint_cache,
                expand_string_arrays=True,
                shape_skips=shape_skips,
            )
            return [row for row in rows if row[1] in values]
        cache_key = (
            "field_values",
            tuple(subtypes),
            field,
            shape_skips is not None,
            tuple(sorted(values)),
        )
        if endpoint_cache is not None and cache_key in endpoint_cache:
            if shape_skips is not None:
                shape_skips.extend(_cached_skips(endpoint_cache, cache_key))
            return endpoint_cache[cache_key]
        read = _chronos.query_strict if strict else _chronos.query
        rows = read(
            cur,
            f"""
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
               AND (
                    attrs->>'{field}' = ANY(:field_value_array)
                    OR EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements_text(
                              CASE
                                WHEN jsonb_typeof(attrs->'{field}') = 'array'
                                THEN attrs->'{field}'
                                ELSE '[]'::jsonb
                              END
                          ) AS item(value)
                         WHERE item.value = ANY(:field_value_array)
                    )
               )
            """,
            {"subtypes": list(subtypes), "field_value_array": sorted(values)},
        )
        out: list[tuple[str, str]] = []
        skips: list[CrossLinkFieldShapeError] = []
        for row in rows:
            raw = (row["attrs"] or {}).get(field)
            field_values = (
                _path_field_values(
                    raw,
                    node_id=str(row["node_id"]),
                    field=field,
                    shape_skips=skips,
                )
                if shape_skips is not None
                else _field_values(raw, expand_string_arrays=True)
            )
            out.extend(
                (str(row["node_id"]), value)
                for value in field_values
                if value in values
            )
        if shape_skips is not None:
            shape_skips.extend(skips)
            _store_skips(endpoint_cache, cache_key, skips)
        if endpoint_cache is not None:
            endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _fetch_subtype_field_containing_values(
        cur,
        *,
        subtypes: tuple[str, ...],
        field: str,
        values: set[str],
        shape_skips: list[CrossLinkFieldShapeError],
        endpoint_cache: _EndpointCache | None = None,
    ) -> list[tuple[str, str]]:
        """Return `(node_id, value)` for nodes whose `field` may contain
        one of `values`, read the way `derive_from_path` reads a field:
        one row per scalar, or per item of a list of scalars; a node it
        cannot read is appended to `shape_skips`. Only `derive_from_path`
        calls this; the SQL filter is a superset and the caller's regex
        decides."""
        if not values:
            return []
        if not _SAFE_ATTR_FIELD.match(field):
            rows = DeclarativeLinker._fetch_subtype_field(
                cur,
                subtypes=subtypes,
                field=field,
                endpoint_cache=endpoint_cache,
                shape_skips=shape_skips,
            )
            return [
                row for row in rows
                if any(value and value in row[1] for value in values)
            ]
        cache_key = (
            "field_contains_values",
            tuple(subtypes),
            field,
            tuple(sorted(values)),
        )
        if endpoint_cache is not None and cache_key in endpoint_cache:
            shape_skips.extend(_cached_skips(endpoint_cache, cache_key))
            return endpoint_cache[cache_key]
        patterns = [f"%{value}%" for value in sorted(values) if value]
        if not patterns:
            return []
        rows = _chronos.query(
            cur,
            f"""
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = ANY(:subtypes)
               AND (
                    attrs->>'{field}' LIKE ANY(:patterns)
                    OR EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements_text(
                              CASE
                                WHEN jsonb_typeof(attrs->'{field}') = 'array'
                                THEN attrs->'{field}'
                                ELSE '[]'::jsonb
                              END
                          ) AS item(value)
                         WHERE item.value LIKE ANY(:patterns)
                    )
               )
            """,
            {"subtypes": list(subtypes), "patterns": patterns},
        )
        out: list[tuple[str, str]] = []
        skips: list[CrossLinkFieldShapeError] = []
        for row in rows:
            node_id = str(row["node_id"])
            out.extend(
                (node_id, text)
                for text in _path_field_values(
                    (row["attrs"] or {}).get(field),
                    node_id=node_id,
                    field=field,
                    shape_skips=skips,
                )
            )
        shape_skips.extend(skips)
        _store_skips(endpoint_cache, cache_key, skips)
        if endpoint_cache is not None:
            endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _dirty_field_may_have_changed(
        dirty_node_attr_changes_by_subtype: (
            dict[str, dict[str, frozenset[str]]] | None
        ),
        *,
        dirty_node_ids: frozenset[str] | None,
        subtypes: tuple[str, ...],
        field: str | None,
    ) -> bool:
        if dirty_node_attr_changes_by_subtype is None or dirty_node_ids is None:
            return True
        if not field:
            return True
        for subtype in subtypes:
            changes_by_node = dirty_node_attr_changes_by_subtype.get(subtype, {})
            for node_id in dirty_node_ids:
                fields = changes_by_node.get(node_id)
                if fields is None:
                    continue
                if (
                    field in fields
                    or "__new__" in fields
                    or "__deleted__" in fields
                ):
                    return True
        return False

    @classmethod
    def _match_exact(
        cls,
        cur,
        rule: CrossLinkRule,
        *,
        dirty_node_ids: frozenset[str] | None,
        dirty_node_subtypes: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> set[tuple[str, str]]:
        def _match(
            src_scope: frozenset[str] | None,
            dst_scope: frozenset[str] | None,
        ) -> set[tuple[str, str]]:
            srcs = cls._fetch_subtype_field(
                cur, subtypes=rule.src.subtypes,
                field=rule.src.field or "",
                only_node_ids=src_scope,
                endpoint_cache=endpoint_cache,
                expand_string_arrays=True,
            )
            if not srcs:
                # No source value, no pair: the destination side is not
                # read (issue #2941; a first run reads the whole graph).
                return set()
            dsts = cls._fetch_subtype_field(
                cur, subtypes=rule.dst.subtypes,
                field=rule.dst.field or "",
                only_node_ids=dst_scope,
                endpoint_cache=endpoint_cache,
                expand_string_arrays=True,
            )
            return cls._pairs_exact(srcs, dsts)

        if dirty_node_ids is not None and dirty_node_subtypes is not None:
            out: set[tuple[str, str]] = set()
            if cls._dirty_intersects(
                dirty_node_subtypes, rule.src.subtypes,
            ) and cls._any_live_node(
                cur, subtypes=rule.dst.subtypes, endpoint_cache=endpoint_cache,
            ):
                srcs = cls._fetch_subtype_field(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=rule.src.field or "",
                    only_node_ids=dirty_node_ids,
                    endpoint_cache=endpoint_cache,
                    expand_string_arrays=True,
                )
                dsts = cls._fetch_subtype_field_matching_values(
                    cur,
                    subtypes=rule.dst.subtypes,
                    field=rule.dst.field or "",
                    values={value for _node_id, value in srcs},
                    endpoint_cache=endpoint_cache,
                )
                out |= cls._pairs_exact(srcs, dsts)
            if cls._dirty_intersects(
                dirty_node_subtypes, rule.dst.subtypes,
            ) and cls._any_live_node(
                cur, subtypes=rule.src.subtypes, endpoint_cache=endpoint_cache,
            ):
                dsts = cls._fetch_subtype_field(
                    cur,
                    subtypes=rule.dst.subtypes,
                    field=rule.dst.field or "",
                    only_node_ids=dirty_node_ids,
                    endpoint_cache=endpoint_cache,
                    expand_string_arrays=True,
                )
                srcs = cls._fetch_subtype_field_matching_values(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=rule.src.field or "",
                    values={value for _node_id, value in dsts},
                    endpoint_cache=endpoint_cache,
                )
                out |= cls._pairs_exact(srcs, dsts)
            return out
        return _match(None, None)

    @staticmethod
    def _pairs_exact(
        srcs: list[tuple[str, str]],
        dsts: list[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        dst_index: dict[str, list[str]] = {}
        for nid, val in dsts:
            dst_index.setdefault(val, []).append(nid)
        out: set[tuple[str, str]] = set()
        for src_id, val in srcs:
            for d in dst_index.get(val, ()):
                out.add((src_id, d))
        return out

    @classmethod
    def _match_subject_prefix(
        cls,
        cur,
        rule: CrossLinkRule,
        *,
        dirty_node_ids: frozenset[str] | None,
        dirty_node_subtypes: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> set[tuple[str, str]]:
        def _match(
            src_scope: frozenset[str] | None,
            dst_scope: frozenset[str] | None,
        ) -> set[tuple[str, str]]:
            srcs = cls._fetch_subtype_field(
                cur, subtypes=rule.src.subtypes,
                field=rule.src.field or "",
                only_node_ids=src_scope,
                endpoint_cache=endpoint_cache,
            )
            dsts = cls._fetch_subtype_field(
                cur, subtypes=rule.dst.subtypes,
                field=rule.dst.field or "",
                only_node_ids=dst_scope,
                endpoint_cache=endpoint_cache,
            )
            return cls._pairs_subject_prefix(srcs, dsts)

        if dirty_node_ids is not None and dirty_node_subtypes is not None:
            out: set[tuple[str, str]] = set()
            if cls._dirty_intersects(dirty_node_subtypes, rule.src.subtypes):
                out |= _match(dirty_node_ids, None)
            if cls._dirty_intersects(dirty_node_subtypes, rule.dst.subtypes):
                out |= _match(None, dirty_node_ids)
            return out
        return _match(None, None)

    @staticmethod
    def _pairs_subject_prefix(
        srcs: list[tuple[str, str]],
        dsts: list[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        out: set[tuple[str, str]] = set()
        for src_id, subj in srcs:
            for dst_id, slug in dsts:
                if not slug:
                    continue
                if (
                    subj == slug
                    or subj.startswith(slug + ":")
                    or subj.startswith(slug + " ")
                ):
                    out.add((src_id, dst_id))
        return out

    @classmethod
    def _match_word_boundary(
        cls,
        cur,
        rule: CrossLinkRule,
        *,
        dirty_node_ids: frozenset[str] | None,
        dirty_node_subtypes: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> set[tuple[str, str]]:
        def _srcs(
            scope: frozenset[str] | None,
        ) -> list[tuple[str, str]]:
            if rule.src.fields:
                return cls._fetch_subtype_fields_concat(
                    cur, subtypes=rule.src.subtypes,
                    fields=rule.src.fields,
                    only_node_ids=scope,
                    endpoint_cache=endpoint_cache,
                )
            return cls._fetch_subtype_field(
                cur, subtypes=rule.src.subtypes,
                field=rule.src.field or "",
                only_node_ids=scope,
                endpoint_cache=endpoint_cache,
            )

        def _match(
            src_scope: frozenset[str] | None,
            dst_scope: frozenset[str] | None,
        ) -> set[tuple[str, str]]:
            srcs = _srcs(src_scope)
            dsts = cls._fetch_subtype_field(
                cur, subtypes=rule.dst.subtypes,
                field=rule.dst.field or "",
                only_node_ids=dst_scope,
                endpoint_cache=endpoint_cache,
            )
            return cls._pairs_word_boundary(srcs, dsts)

        if dirty_node_ids is not None and dirty_node_subtypes is not None:
            out: set[tuple[str, str]] = set()
            if cls._dirty_intersects(dirty_node_subtypes, rule.src.subtypes):
                out |= _match(dirty_node_ids, None)
            if cls._dirty_intersects(dirty_node_subtypes, rule.dst.subtypes):
                out |= _match(None, dirty_node_ids)
            return out
        return _match(None, None)

    @staticmethod
    def _pairs_word_boundary(
        srcs: list[tuple[str, str]],
        dsts: list[tuple[str, str]],
    ) -> set[tuple[str, str]]:
        pat_cache: dict[str, re.Pattern] = {}
        out: set[tuple[str, str]] = set()
        for src_id, text in srcs:
            for dst_id, slug in dsts:
                if not slug:
                    continue
                pat = pat_cache.get(slug)
                if pat is None:
                    pat = re.compile(
                        r"(?:^|[^a-zA-Z0-9-])"
                        + re.escape(slug)
                        + r"(?:[^a-zA-Z0-9-]|$)"
                    )
                    pat_cache[slug] = pat
                if pat.search(text):
                    out.add((src_id, dst_id))
        return out

    @classmethod
    def _match_derive_from_path(
        cls,
        cur,
        rule: CrossLinkRule,
        *,
        dirty_node_ids: frozenset[str] | None,
        dirty_edge_ids: frozenset[int],
        dirty_node_subtypes: frozenset[str] | None,
        dirty_node_attr_changes_by_subtype: (
            dict[str, dict[str, frozenset[str]]] | None
        ),
        endpoint_cache: _EndpointCache,
        shape_skips: list[CrossLinkFieldShapeError],
    ) -> set[tuple[str, str]]:
        """For each src node (or via_edge.dst_subtype node), run
        the regex on `derive_from_path.attr`; the capture group
        is matched against dst.field via exact lookup.

        A capture links to every destination carrying that value, as
        `exact` does; two destinations sharing a value both link.
        A node whose field this matcher cannot read is left out and
        recorded in `shape_skips`."""
        if rule.dst.derive_from_path is None or rule.dst.field is None:
            raise ValueError("derive_from_path rules require dst field metadata")
        d = rule.dst.derive_from_path
        pat = re.compile(d.regex)
        # `multi_capture: true` enables `re.findall` semantics so a
        # single source value can produce multiple edges (e.g. a
        # docstring with multiple `@implements_concept:` lines).
        # Default is single-capture `re.match` for backward compat.
        multi = bool(getattr(d, "multi_capture", False))

        def _captures(val: str) -> list[str]:
            if multi:
                return [c for c in pat.findall(val) if c]
            m = pat.match(val)
            return [m.group(1)] if m else []

        def _capture_to_dst(
            dst_scope: frozenset[str] | None,
        ) -> dict[str, tuple[str, ...]]:
            dsts = cls._fetch_subtype_field(
                cur,
                subtypes=rule.dst.subtypes,
                field=rule.dst.field,
                only_node_ids=dst_scope,
                endpoint_cache=endpoint_cache,
                shape_skips=shape_skips,
            )
            return _index_by_value(dsts)

        if rule.src.via_edge is not None:
            ve = rule.src.via_edge
            candidate_edges = cls._derive_from_path_candidate_edges(
                cur,
                rule=rule,
                dirty_node_ids=dirty_node_ids,
                dirty_edge_ids=dirty_edge_ids,
                dirty_node_subtypes=dirty_node_subtypes,
                endpoint_cache=endpoint_cache,
            )
            out: set[tuple[str, str]] = set()

            dst_backfill_ids: frozenset[str] | None = None
            if (
                dirty_node_ids is not None
                and dirty_node_subtypes is not None
                and dirty_node_subtypes.intersection(rule.dst.subtypes)
                and cls._dirty_field_may_have_changed(
                    dirty_node_attr_changes_by_subtype,
                    dirty_node_ids=dirty_node_ids,
                    subtypes=rule.dst.subtypes,
                    field=rule.dst.field,
                )
            ):
                dst_backfill_ids = cls._dirty_ids_for_subtypes(
                    cur,
                    dirty_node_ids=dirty_node_ids,
                    subtypes=rule.dst.subtypes,
                    endpoint_cache=endpoint_cache,
                )

            if dst_backfill_ids:
                scoped_capture_to_dst = _capture_to_dst(dst_backfill_ids)
                via_rows_for_dst = cls._fetch_subtype_field_containing_values(
                    cur,
                    subtypes=(ve.dst_subtype,),
                    field=d.attr,
                    values=set(scoped_capture_to_dst),
                    endpoint_cache=endpoint_cache,
                    shape_skips=shape_skips,
                )
                via_to_captures_for_dst: dict[str, list[str]] = {}
                for via_id, val in via_rows_for_dst:
                    caps = [
                        c for c in _captures(val)
                        if c in scoped_capture_to_dst
                    ]
                    if caps:
                        # A list field yields one row per item, so
                        # one via node can arrive more than once.
                        via_to_captures_for_dst.setdefault(
                            via_id, [],
                        ).extend(caps)
                if via_to_captures_for_dst:
                    edge_rows_for_dst = cls._via_edges_to_nodes(
                        cur,
                        via_ids=frozenset(via_to_captures_for_dst),
                        edge_type=ve.edge_type,
                        src_subtypes=rule.src.subtypes,
                        endpoint_cache=endpoint_cache,
                    )
                    for src_id, via_id, _edge_id in edge_rows_for_dst:
                        for cap in via_to_captures_for_dst.get(via_id, ()):
                            for dst_id in scoped_capture_to_dst.get(cap, ()):
                                out.add((src_id, dst_id))

            if candidate_edges == []:
                return out
            capture_to_dst: dict[str, tuple[str, ...]] = {}
            # Get path values for via-dst nodes that match the regex. When
            # candidate via-edges are already bounded, fetch only dst nodes
            # whose field value appears in those scoped captures.
            node_filter = ""
            params: dict[str, Any] = {"dst_subtype": ve.dst_subtype}
            if candidate_edges is not None:
                node_scope = frozenset(dst for _src, dst, _edge_id in candidate_edges)
                if not node_scope:
                    return set()
                node_filter = "AND node_id = ANY(:node_ids)"
                params["node_ids"] = sorted(node_scope)
            else:
                capture_to_dst = _capture_to_dst(None)
                if not capture_to_dst:
                    return out
            via_rows = _chronos.query(
                cur,
                f"""
                SELECT node_id, attrs
                  FROM okg.graph_nodes
                 WHERE subtype = :dst_subtype
                       {node_filter}
                """,
                params,
            )
            # `multi_capture` produces possibly multiple captures per
            # via-dst row, but the via->src join below uses node_id as
            # the key — so multi-capture maps to a list per via_id.
            via_to_captures: dict[str, list[str]] = {}
            candidate_capture_values: set[str] = set()
            for row in via_rows:
                via_id = str(row["node_id"])
                raw_caps = [
                    cap
                    for val in _path_field_values(
                        (row["attrs"] or {}).get(d.attr),
                        node_id=via_id,
                        field=d.attr,
                        shape_skips=shape_skips,
                    )
                    for cap in _captures(val)
                ]
                if candidate_edges is not None:
                    caps = raw_caps
                    candidate_capture_values.update(raw_caps)
                else:
                    caps = [c for c in raw_caps if c in capture_to_dst]
                if caps:
                    via_to_captures[via_id] = caps
            if candidate_edges is not None:
                if not candidate_capture_values:
                    return out
                dsts = cls._fetch_subtype_field_matching_values(
                    cur,
                    subtypes=rule.dst.subtypes,
                    field=rule.dst.field,
                    values=candidate_capture_values,
                    endpoint_cache=endpoint_cache,
                    shape_skips=shape_skips,
                )
                capture_to_dst = _index_by_value(dsts)
                if not capture_to_dst:
                    return out
                via_to_captures = {
                    via_id: [cap for cap in caps if cap in capture_to_dst]
                    for via_id, caps in via_to_captures.items()
                }
                via_to_captures = {
                    via_id: caps
                    for via_id, caps in via_to_captures.items()
                    if caps
                }
            if not via_to_captures:
                return set()
            # Edges: src -> via_edge.edge_type -> via_dst.
            if candidate_edges is None:
                edge_rows = _chronos.query(
                    cur,
                    """
                    SELECT e.src, e.dst
                      FROM okg.graph_edges e
                      JOIN okg.graph_nodes s
                        ON s.node_id = e.src
                     WHERE e.edge_type = :edge_type
                       AND s.subtype = ANY(:src_subtypes)
                       AND e.dst = ANY(:via_ids)
                    """,
                    {
                        "edge_type": ve.edge_type,
                        "src_subtypes": list(rule.src.subtypes),
                        "via_ids": list(via_to_captures.keys()),
                    },
                )
            else:
                edge_rows = [
                    {"src": src, "dst": dst}
                    for src, dst, _edge_id in candidate_edges
                    if dst in via_to_captures
                ]
            for row in edge_rows:
                src_id = str(row["src"])
                via_id = str(row["dst"])
                for cap in via_to_captures.get(via_id, ()):
                    for dst_id in capture_to_dst.get(cap, ()):
                        out.add((src_id, dst_id))
            return out

        # Direct path: regex applied to src.field.
        def _match_direct(
            src_scope: frozenset[str] | None,
            dst_scope: frozenset[str] | None,
        ) -> set[tuple[str, str]]:
            scoped_dsts = cls._fetch_subtype_field(
                cur,
                subtypes=rule.dst.subtypes,
                field=rule.dst.field,
                only_node_ids=dst_scope,
                endpoint_cache=endpoint_cache,
                shape_skips=shape_skips,
            )
            scoped_capture_to_dst = _index_by_value(scoped_dsts)
            if not scoped_capture_to_dst:
                return set()
            src_field = rule.src.field or d.attr
            if src_scope is None and dst_scope is not None:
                srcs = cls._fetch_subtype_field_containing_values(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=src_field,
                    values=set(scoped_capture_to_dst),
                    endpoint_cache=endpoint_cache,
                    shape_skips=shape_skips,
                )
            else:
                srcs = cls._fetch_subtype_field(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=src_field,
                    only_node_ids=src_scope,
                    endpoint_cache=endpoint_cache,
                    shape_skips=shape_skips,
                )
            out2: set[tuple[str, str]] = set()
            for src_id, val in srcs:
                for cap in _captures(val):
                    for dst_id in scoped_capture_to_dst.get(cap, ()):
                        out2.add((src_id, dst_id))
            return out2

        if dirty_node_ids is not None and dirty_node_subtypes is not None:
            out: set[tuple[str, str]] = set()
            if (
                cls._dirty_intersects(dirty_node_subtypes, rule.src.subtypes)
                and cls._dirty_field_may_have_changed(
                    dirty_node_attr_changes_by_subtype,
                    dirty_node_ids=dirty_node_ids,
                    subtypes=rule.src.subtypes,
                    field=rule.src.field or d.attr,
                )
            ):
                out |= _match_direct(dirty_node_ids, None)
            if (
                cls._dirty_intersects(dirty_node_subtypes, rule.dst.subtypes)
                and cls._dirty_field_may_have_changed(
                    dirty_node_attr_changes_by_subtype,
                    dirty_node_ids=dirty_node_ids,
                    subtypes=rule.dst.subtypes,
                    field=rule.dst.field,
                )
            ):
                out |= _match_direct(None, dirty_node_ids)
            return out
        return _match_direct(None, None)

    @classmethod
    def _derive_from_path_candidate_edges(
        cls,
        cur,
        *,
        rule: CrossLinkRule,
        dirty_node_ids: frozenset[str] | None,
        dirty_edge_ids: frozenset[int],
        dirty_node_subtypes: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> list[tuple[str, str, int]] | None:
        """Return bounded candidate via edges for via-edge path rules.

        `None` means full scope. An empty list means the incremental delta is
        irrelevant to source/via/edge candidates. Destination subtype changes
        are handled by value-bounded backfill in `_match_derive_from_path`.
        """
        if dirty_node_ids is None:
            return None
        ve = rule.src.via_edge
        if ve is None:
            return None
        if dirty_node_subtypes is None:
            return None
        relevant = (
            *rule.src.subtypes,
            ve.dst_subtype,
            *rule.dst.subtypes,
        )
        if not dirty_node_subtypes.intersection(relevant) and not dirty_edge_ids:
            return []
        candidate_rows: dict[tuple[str, str, int], tuple[str, str, int]] = {}

        if dirty_node_subtypes.intersection(rule.src.subtypes):
            dirty_src_ids = cls._dirty_ids_for_subtypes(
                cur,
                dirty_node_ids=dirty_node_ids,
                subtypes=rule.src.subtypes,
                endpoint_cache=endpoint_cache,
            )
            if not dirty_src_ids:
                return []
            for row in cls._via_edges_from_sources(
                cur,
                src_ids=dirty_src_ids,
                edge_type=ve.edge_type,
                dst_subtype=ve.dst_subtype,
                endpoint_cache=endpoint_cache,
            ):
                candidate_rows[row] = row

        if dirty_node_subtypes.intersection((ve.dst_subtype,)):
            dirty_via_ids = cls._dirty_ids_for_subtypes(
                cur,
                dirty_node_ids=dirty_node_ids,
                subtypes=(ve.dst_subtype,),
                endpoint_cache=endpoint_cache,
            )
            if not dirty_via_ids:
                return []
            for row in cls._via_edges_to_nodes(
                cur,
                via_ids=dirty_via_ids,
                edge_type=ve.edge_type,
                src_subtypes=rule.src.subtypes,
                endpoint_cache=endpoint_cache,
            ):
                candidate_rows[row] = row

        if dirty_edge_ids:
            for row in cls._dirty_via_edges(
                cur,
                dirty_edge_ids=dirty_edge_ids,
                edge_type=ve.edge_type,
                src_subtypes=rule.src.subtypes,
                dst_subtype=ve.dst_subtype,
                endpoint_cache=endpoint_cache,
            ):
                candidate_rows[row] = row

        return list(candidate_rows.values())

    @staticmethod
    def _dirty_ids_for_subtypes(
        cur,
        *,
        dirty_node_ids: frozenset[str] | None,
        subtypes: tuple[str, ...],
        endpoint_cache: _EndpointCache,
    ) -> frozenset[str] | None:
        if dirty_node_ids is None:
            return None
        if not dirty_node_ids:
            return frozenset()
        cache_key = (
            "dirty_ids_for_subtypes",
            tuple(sorted(dirty_node_ids)),
            tuple(subtypes),
        )
        if cache_key in endpoint_cache:
            cached = endpoint_cache[cache_key]
            return frozenset(node_id for node_id, _subtype in cached)
        rows = _chronos.query(
            cur,
            """
            SELECT node_id, subtype
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
               AND subtype = ANY(:subtypes)
            """,
            {
                "node_ids": sorted(dirty_node_ids),
                "subtypes": list(subtypes),
            },
        )
        if not rows:
            rows = _chronos.query(
                cur,
                """
                SELECT node_id, subtype
                  FROM okg.graph_nodes
                 WHERE node_id = ANY(:node_ids)
                """,
                {"node_ids": sorted(dirty_node_ids)},
            )
            subtype_set = set(subtypes)
            rows = [
                row for row in rows
                if str(row.get("subtype") or "") in subtype_set
            ]
        pairs = [
            (str(row["node_id"]), str(row["subtype"]))
            for row in rows
        ]
        endpoint_cache[cache_key] = pairs
        return frozenset(node_id for node_id, _subtype in pairs)

    @staticmethod
    def _via_edges_from_sources(
        cur,
        *,
        src_ids: frozenset[str],
        edge_type: str,
        dst_subtype: str,
        endpoint_cache: _EndpointCache,
    ) -> list[tuple[str, str, int]]:
        if not src_ids:
            return []
        cache_key = (
            "via_edges_from_sources",
            tuple(sorted(src_ids)),
            edge_type,
            dst_subtype,
        )
        if cache_key in endpoint_cache:
            return [
                (str(src), str(dst), int(edge_id))
                for src, dst, edge_id in endpoint_cache[cache_key]
            ]
        rows = _chronos.query(
            cur,
            """
            SELECT e.src, e.dst, e.edge_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes d
                ON d.node_id = e.dst
               AND d.subtype = :dst_subtype
             WHERE e.edge_type = :edge_type
               AND e.src = ANY(:src_ids)
            """,
            {
                "edge_type": edge_type,
                "src_ids": sorted(src_ids),
                "dst_subtype": dst_subtype,
            },
        )
        out = [
            (str(row["src"]), str(row["dst"]), int(row["edge_id"]))
            for row in rows
        ]
        endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _via_edges_to_nodes(
        cur,
        *,
        via_ids: frozenset[str],
        edge_type: str,
        src_subtypes: tuple[str, ...],
        endpoint_cache: _EndpointCache,
    ) -> list[tuple[str, str, int]]:
        if not via_ids:
            return []
        cache_key = (
            "via_edges_to_nodes",
            tuple(sorted(via_ids)),
            edge_type,
            tuple(src_subtypes),
        )
        if cache_key in endpoint_cache:
            return [
                (str(src), str(dst), int(edge_id))
                for src, dst, edge_id in endpoint_cache[cache_key]
            ]
        rows = _chronos.query(
            cur,
            """
            SELECT e.src, e.dst, e.edge_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes s
                ON s.node_id = e.src
               AND s.subtype = ANY(:src_subtypes)
             WHERE e.edge_type = :edge_type
               AND e.dst = ANY(:via_ids)
            """,
            {
                "edge_type": edge_type,
                "via_ids": sorted(via_ids),
                "src_subtypes": list(src_subtypes),
            },
        )
        out = [
            (str(row["src"]), str(row["dst"]), int(row["edge_id"]))
            for row in rows
        ]
        endpoint_cache[cache_key] = out
        return out

    @staticmethod
    def _dirty_via_edges(
        cur,
        *,
        dirty_edge_ids: frozenset[int],
        edge_type: str,
        src_subtypes: tuple[str, ...],
        dst_subtype: str,
        endpoint_cache: _EndpointCache,
    ) -> list[tuple[str, str, int]]:
        if not dirty_edge_ids:
            return []
        cache_key = (
            "dirty_via_edges",
            tuple(sorted(dirty_edge_ids)),
            edge_type,
            tuple(src_subtypes),
            dst_subtype,
        )
        if cache_key in endpoint_cache:
            return [
                (str(src), str(dst), int(edge_id))
                for src, dst, edge_id in endpoint_cache[cache_key]
            ]
        rows = _chronos.query(
            cur,
            """
            SELECT e.src, e.dst, e.edge_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes s
                ON s.node_id = e.src
               AND s.subtype = ANY(:src_subtypes)
              JOIN okg.graph_nodes d
                ON d.node_id = e.dst
               AND d.subtype = :dst_subtype
             WHERE e.edge_id = ANY(:edge_ids)
               AND e.edge_type = :edge_type
            """,
            {
                "edge_ids": sorted(dirty_edge_ids),
                "edge_type": edge_type,
                "src_subtypes": list(src_subtypes),
                "dst_subtype": dst_subtype,
            },
        )
        if not rows:
            endpoint_cache[cache_key] = []
            return []
        out = [
            (str(row["src"]), str(row["dst"]), int(row["edge_id"]))
            for row in rows
        ]
        endpoint_cache[cache_key] = out
        return out

    @classmethod
    def _match_alias_table(
        cls,
        cur,
        rule: CrossLinkRule,
        *,
        cv: int,
        dirty_node_ids: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> set[tuple[str, str]]:
        srcs = cls._fetch_subtype_field(
            cur,
            subtypes=rule.src.subtypes,
            field=rule.src.field or "",
            only_node_ids=dirty_node_ids,
            endpoint_cache=endpoint_cache,
        )
        if not srcs and dirty_node_ids is None:
            return set()
        unique = sorted({v for _, v in srcs})
        alias_to_canon: dict[str, str] = {}
        if unique:
            cur.execute(
                "SELECT alias, canonical FROM okg.aliases "
                " WHERE catalog_version_id = %s "
                "   AND alias = ANY(%s)",
                (cv, unique),
            )
            alias_to_canon.update(dict(cur.fetchall()))
        if dirty_node_ids is not None:
            cur.execute(
                "SELECT alias, canonical FROM okg.aliases "
                " WHERE catalog_version_id = %s "
                "   AND canonical = ANY(%s)",
                (cv, list(dirty_node_ids)),
            )
            aliases_for_dirty_canonicals = dict(cur.fetchall())
            if aliases_for_dirty_canonicals:
                values = set(aliases_for_dirty_canonicals)
                all_srcs = cls._fetch_subtype_field(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=rule.src.field or "",
                    endpoint_cache=endpoint_cache,
                )
                srcs = list(srcs) + [
                    item for item in all_srcs if item[1] in values
                ]
                alias_to_canon.update(aliases_for_dirty_canonicals)
        fallback = rule.dst.unique_field_fallback
        if fallback is not None:
            srcs = cls._add_unique_fallback_targets(
                cur,
                rule,
                fallback,
                cv=cv,
                srcs=srcs,
                alias_to_canon=alias_to_canon,
                dirty_node_ids=dirty_node_ids,
                endpoint_cache=endpoint_cache,
            )
        if not alias_to_canon:
            return set()
        canonicals = list(set(alias_to_canon.values()))
        live_rows = _chronos.query(
            cur,
            "SELECT node_id FROM okg.graph_nodes "
            " WHERE node_id = ANY(:canonicals)",
            {"canonicals": canonicals},
        )
        live = {str(row["node_id"]) for row in live_rows}
        out: set[tuple[str, str]] = set()
        for src_id, val in srcs:
            canon = alias_to_canon.get(val)
            if canon is None or canon not in live:
                continue
            if canon == src_id:
                continue
            out.add((src_id, canon))
        return out

    @classmethod
    def _add_unique_fallback_targets(
        cls,
        cur,
        rule: CrossLinkRule,
        fallback: UniqueFieldFallback,
        *,
        cv: int,
        srcs: list[tuple[str, str]],
        alias_to_canon: dict[str, str],
        dirty_node_ids: frozenset[str] | None,
        endpoint_cache: _EndpointCache,
    ) -> list[tuple[str, str]]:
        """Resolve alias misses through `fallback`; never pick among several.

        Adds `value -> node_id` to `alias_to_canon` for each source value
        that no alias matches and exactly one live `fallback.subtype` node
        carries in `fallback.field`. A value two or more nodes carry resolves
        to nothing. An alias always wins over the fallback. In a dirty run,
        the source rows naming a dirty fallback node's value join `srcs`, so a
        newly published node links to mentions that already existed. Returns
        the (possibly widened) source rows.
        """
        srcs = list(srcs)
        if dirty_node_ids is not None:
            dirty_values = {
                value for _node_id, value in cls._fetch_subtype_field(
                    cur,
                    subtypes=(fallback.subtype,),
                    field=fallback.field,
                    only_node_ids=dirty_node_ids,
                    endpoint_cache=endpoint_cache,
                )
            }
            if dirty_values:
                all_srcs = cls._fetch_subtype_field(
                    cur,
                    subtypes=rule.src.subtypes,
                    field=rule.src.field or "",
                    endpoint_cache=endpoint_cache,
                )
                srcs.extend(
                    item for item in all_srcs if item[1] in dirty_values
                )
        unresolved = sorted(
            {value for _src, value in srcs if value not in alias_to_canon}
        )
        if not unresolved:
            return srcs
        # Values that joined in a dirty run were never looked up in the alias
        # table; an alias must still win over the fallback for them.
        cur.execute(
            "SELECT alias, canonical FROM okg.aliases "
            " WHERE catalog_version_id = %s "
            "   AND alias = ANY(%s)",
            (cv, unresolved),
        )
        alias_to_canon.update(dict(cur.fetchall()))
        remaining = {v for v in unresolved if v not in alias_to_canon}
        rows = cls._fetch_subtype_field_matching_values(
            cur,
            subtypes=(fallback.subtype,),
            field=fallback.field,
            values=remaining,
            endpoint_cache=endpoint_cache,
            strict=True,
        )
        targets: dict[str, set[str]] = {}
        for node_id, value in rows:
            targets.setdefault(value, set()).add(node_id)
        for value, node_ids in targets.items():
            if len(node_ids) == 1:
                alias_to_canon[value] = next(iter(node_ids))
        return srcs

    def _emit_edges(
        self,
        cur,
        *,
        rule: CrossLinkRule,
        pairs: set[tuple[str, str]],
        generation_id: int,
    ) -> tuple[int, int]:
        candidates: list[DerivedEdgeCandidate] = []
        for src_id, dst_id in pairs:
            directions: list[tuple[str, str]] = [(src_id, dst_id)]
            if rule.bidirectional:
                directions.append((dst_id, src_id))
            for s, d in directions:
                candidates.append(
                    DerivedEdgeCandidate(
                        src=s,
                        edge_type=rule.edge,
                        dst=d,
                        source_record_id={
                            "src_node_id": s,
                            "dst_node_id": d,
                            "linker": self.name,
                            "rule_id": rule.id,
                        },
                        source_revision={
                            "generation_id": generation_id,
                        },
                        dedupe_key=stable_edge_dedupe_key(
                            source=f"_enricher:{self.name}",
                            src=s,
                            edge_type=rule.edge,
                            dst=d,
                        ),
                    )
                )
        if not candidates:
            return 0, 0
        result = insert_deterministic_edges(
            cur,
            source=f"_enricher:{self.name}:{rule.id}",
            candidates=candidates,
        )
        return result.inserted, result.skipped
