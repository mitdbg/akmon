"""Read-only exposure reporting for derived-node support liveness.

The report in this module is deliberately separate from the cleanup path.  It
reads one immutable published Chronos checkpoint, classifies every node, and
returns enough before/after state to prove that the inspection did not move
the branch or its published pointer.  It does not infer support from edge
counts: only the exact versioned ``derived_support`` declaration is trusted
for ``_enricher:*`` rows.
"""

from __future__ import annotations

import json
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from okg.substrate.errors import SubstrateError

EXPOSURE_SCHEMA = "okg.derived_node_exposure.v1"
DERIVED_SUPPORT_SCHEMA = "okg.derived_support.v1"
_DERIVED_SUPPORT_KEYS = frozenset({"schema", "producer", "members", "policy"})
#: The only liveness policies a derived node may declare. There is no
#: default: the policy decides whether a derived node survives its inputs
#: being retracted, so guessing it silently risks unrecoverable loss.
DerivedSupportPolicy: TypeAlias = Literal["any_member", "all_members"]
DERIVED_SUPPORT_POLICIES: frozenset[str] = frozenset(
    {"any_member", "all_members"}
)
_POLICIES = DERIVED_SUPPORT_POLICIES
_MANAGED_FAMILIES = {
    "_extractor": "_extractor",
    "_lens": "_lens",
    "derived_artifact": "derived_artifact",
    "_system": "_system",
}

__all__ = [
    "DERIVED_SUPPORT_POLICIES",
    "DERIVED_SUPPORT_SCHEMA",
    "EXPOSURE_SCHEMA",
    "DerivedSupportDeclaration",
    "DerivedSupportPolicy",
    "canonical_derived_support",
    "classify_derived_node",
    "derived_support_state",
    "frontier_digest",
    "plan_derived_node_cleanup",
    "read_frontier_summary",
    "resolve_declared_support_policy",
    "report_derived_node_exposure",
    "validate_derived_node_row",
]


@dataclass(frozen=True)
class DerivedSupportDeclaration:
    """One admitted current-node support declaration."""

    node_id: str
    producer: str
    members: tuple[str, ...]
    policy: str


def resolve_declared_support_policy(producer: Any) -> DerivedSupportPolicy:
    """The producer class-declared node liveness policy, or a named refusal.

    Every node-writing enricher declares ``support_policy`` on its class;
    :func:`okg.substrate.publisher.enricher_loader.validate_enricher_envelopes`
    refuses the omission at worker boot and at Publisher construction. This
    resolution is the backstop for a producer built outside that path (a
    direct unit-test instantiation, say): it names the class instead of
    failing with a bare ``AttributeError`` or a guessed policy, because no
    default is safe (issue #1256).
    """
    policy = getattr(producer, "support_policy", None)
    if isinstance(policy, str) and policy in DERIVED_SUPPORT_POLICIES:
        return policy  # type: ignore[return-value]
    name = getattr(producer, "name", type(producer).__name__)
    allowed = ", ".join(sorted(DERIVED_SUPPORT_POLICIES))
    raise ValueError(
        f"{type(producer).__name__} (name={name!r}) has no valid "
        f"derived-node support_policy declaration (found {policy!r}); "
        f"declare `support_policy: DerivedSupportPolicy = \"all_members\"` "
        f"(or one of: {allowed}) on the class"
    )


def canonical_derived_support(
    *,
    producer: str,
    members: Any,
    policy: DerivedSupportPolicy | None,
) -> tuple[dict[str, Any], list[str]]:
    """Validate and canonicalize a version-1 support declaration.

    Duplicate member ids are intentionally canonicalized. Every other malformed
    value is refused rather than guessed or defaulted.
    """
    if not isinstance(producer, str) or not producer.strip() or "\x00" in producer:
        raise ValueError("derived support producer must be a non-empty NUL-free string")
    if policy not in _POLICIES:
        raise ValueError("derived support policy must be any_member or all_members")
    if isinstance(members, (str, bytes)) or not isinstance(members, (list, tuple)):
        raise ValueError("derived support members must be a non-empty sequence")
    canonical: list[str] = []
    for member in members:
        if not isinstance(member, str) or not member.strip() or "\x00" in member:
            raise ValueError(
                "derived support members must be non-empty NUL-free node-id strings"
            )
        canonical.append(member)
    canonical = sorted(set(canonical))
    if not canonical:
        raise ValueError("derived support members must not be empty")
    return (
        {
            "schema": DERIVED_SUPPORT_SCHEMA,
            "producer": producer,
            "members": canonical,
            "policy": policy,
        },
        canonical,
    )


def _json_value(value: Any) -> Any:
    """Decode JSON returned by lightweight/fake adapters as well as psycopg."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    return value


def _mapping(value: Any) -> dict[str, Any]:
    value = _json_value(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _producer_family(source: Any) -> tuple[str, str]:
    producer = str(source or "")
    for prefix, family in _MANAGED_FAMILIES.items():
        if producer.startswith(prefix + ":"):
            return family, producer
    if producer.startswith("_enricher:"):
        return "_enricher", producer
    if producer == "curated" or producer.startswith("curated:"):
        return "curated", producer
    if producer == "_curated" or producer.startswith("_curated:"):
        return "curated", producer
    # A non-substrate producer is source-owned by default.  This is an
    # intentional protection boundary: an unknown source is never promoted to
    # an automatically retractable derived family merely because it has a
    # lineage-looking attribute.
    return "source_owned", producer or "<missing-source>"


def _valid_members(value: Any) -> tuple[list[str] | None, str | None]:
    if not isinstance(value, list) or not value:
        return None, "members_missing_or_empty"
    if any(
        not isinstance(item, str) or not item.strip() or "\x00" in item
        for item in value
    ):
        return None, "members_malformed"
    members = [str(item) for item in value]
    canonical = sorted(set(members))
    if members != canonical:
        return None, "members_not_canonical"
    return canonical, None


def _support_state(
    row: Mapping[str, Any],
    *,
    source: str,
    live_node_ids: set[str],
) -> tuple[str, str, str, list[str]]:
    """Return ``(policy_state, verdict, cause, members)`` for an enricher row."""
    record = _mapping(row.get("source_record_id"))
    support = _json_value(record.get("derived_support"))

    if support is None:
        legacy_members, legacy_error = _valid_members(record.get("derived_from"))
        if legacy_error is None and legacy_members is not None:
            if set(legacy_members).isdisjoint(live_node_ids):
                # Both supported policies fail when no declared member is live,
                # so this is truly unsupported without guessing a policy.
                return (
                    "legacy_policy_missing",
                    "unsupported",
                    "legacy_no_live_support_members",
                    legacy_members,
                )
            return (
                "legacy_policy_missing",
                "unresolved",
                "legacy_support_policy_missing",
                legacy_members,
            )
        return (
            "missing",
            "unresolved",
            "missing_derived_support",
            [],
        )
    if not isinstance(support, Mapping):
        return "malformed", "unresolved", "derived_support_not_object", []
    support = dict(support)
    if set(support) != _DERIVED_SUPPORT_KEYS:
        return "malformed", "unresolved", "derived_support_fields_invalid", []
    if support.get("schema") != DERIVED_SUPPORT_SCHEMA:
        return "unknown_schema", "unresolved", "unknown_support_schema", []
    policy = support.get("policy")
    if policy not in _POLICIES:
        return "unknown_policy", "unresolved", "unknown_support_policy", []
    producer = support.get("producer")
    if producer != source:
        return "producer_mismatch", "unresolved", "support_producer_mismatch", []
    if not source.startswith("_enricher:") or not source.removeprefix(
        "_enricher:"
    ).strip():
        return "producer_mismatch", "unresolved", "support_producer_invalid", []

    members, member_error = _valid_members(support.get("members"))
    if member_error is not None or members is None:
        return "malformed", "unresolved", member_error or "members_malformed", []

    legacy_members, legacy_error = _valid_members(record.get("derived_from"))
    if legacy_error is not None or legacy_members is None:
        return (
            "legacy_mismatch",
            "unresolved",
            "derived_from_missing_or_malformed",
            members,
        )
    if legacy_members != members:
        return (
            "legacy_mismatch",
            "unresolved",
            "derived_support_derived_from_mismatch",
            members,
        )

    live_members = set(members) & live_node_ids
    if policy == "any_member":
        supported = bool(live_members)
        cause = "live_support_member" if supported else "no_live_support_members"
    else:
        supported = set(members).issubset(live_node_ids)
        cause = (
            "all_support_members_live"
            if supported
            else "all_support_members_not_live"
        )
    return (
        f"valid_{policy}",
        "supported" if supported else "unsupported",
        cause,
        members,
    )


def _declaration_state(row: Mapping[str, Any]) -> dict[str, Any]:
    """Parse one row for write admission and fixed-point evaluation."""
    node_id = str(row.get("node_id") or "")
    source = str(row.get("source") or "")
    family, _producer = _producer_family(source)
    protected = family in {"source_owned", "curated"}
    record = _mapping(row.get("source_record_id"))
    raw_support = _json_value(record.get("derived_support"))
    managed_by_family = family == "_enricher"
    explicitly_managed = raw_support is not None and not protected
    managed = managed_by_family or explicitly_managed
    base = {
        "node_id": node_id,
        "source": source,
        "producer_family": family,
        "protected": protected,
        "managed": managed,
        "members": [],
        "policy": None,
        "declaration": None,
    }
    if not managed:
        return {**base, "status": "not_managed", "cause": "independent_lifecycle"}
    if raw_support is None:
        legacy_members, legacy_error = _valid_members(record.get("derived_from"))
        return {
            **base,
            "status": "legacy_policy_missing" if legacy_error is None else "missing",
            "cause": (
                "legacy_support_policy_missing"
                if legacy_error is None
                else "missing_derived_support"
            ),
            "members": legacy_members or [],
        }
    if not isinstance(raw_support, Mapping):
        return {**base, "status": "malformed", "cause": "derived_support_not_object"}
    support = dict(raw_support)
    if set(support) != _DERIVED_SUPPORT_KEYS:
        return {
            **base,
            "status": "malformed",
            "cause": "derived_support_fields_invalid",
        }
    if support.get("schema") != DERIVED_SUPPORT_SCHEMA:
        return {**base, "status": "unknown_schema", "cause": "unknown_support_schema"}
    if support.get("policy") not in _POLICIES:
        return {**base, "status": "unknown_policy", "cause": "unknown_support_policy"}
    if support.get("producer") != source:
        return {
            **base,
            "status": "producer_mismatch",
            "cause": "support_producer_mismatch",
        }
    if managed_by_family and (
        not source.startswith("_enricher:")
        or not source.removeprefix("_enricher:").strip()
    ):
        return {
            **base,
            "status": "producer_mismatch",
            "cause": "support_producer_invalid",
        }
    members, member_error = _valid_members(support.get("members"))
    if member_error is not None or members is None:
        return {
            **base,
            "status": "malformed",
            "cause": member_error or "members_malformed",
        }
    legacy_members, legacy_error = _valid_members(record.get("derived_from"))
    if legacy_error is not None or legacy_members is None:
        return {
            **base,
            "status": "legacy_mismatch",
            "cause": "derived_from_missing_or_malformed",
            "members": members,
        }
    if members != legacy_members:
        return {
            **base,
            "status": "legacy_mismatch",
            "cause": "derived_support_derived_from_mismatch",
            "members": members,
        }
    declaration = DerivedSupportDeclaration(
        node_id=node_id,
        producer=source,
        members=tuple(members),
        policy=str(support["policy"]),
    )
    return {
        **base,
        "status": "valid",
        "cause": "valid_declaration",
        "members": members,
        "policy": declaration.policy,
        "declaration": declaration,
    }


def validate_derived_node_row(row: Mapping[str, Any]) -> DerivedSupportDeclaration | None:
    """Refuse malformed managed assertions before a graph write.

    Source-owned/curated rows and independently managed rows without an explicit
    declaration are outside this lifecycle. ``_enricher:*`` rows are always in
    scope and therefore cannot omit the declaration.
    """
    state = _declaration_state(row)
    if not state["managed"]:
        return None
    declaration = state.get("declaration")
    if state["status"] != "valid" or not isinstance(
        declaration, DerivedSupportDeclaration
    ):
        raise ValueError(
            "invalid derived support declaration for "
            f"{state['node_id']!r}: {state['cause']}"
        )
    return declaration


def derived_support_state(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return the JSON-safe declaration state used by planning surfaces."""
    state = dict(_declaration_state(row))
    state.pop("declaration", None)
    return state


def _declaration_fingerprint(state: Mapping[str, Any] | None) -> tuple[Any, ...] | None:
    if state is None or not state.get("managed"):
        return None
    return (
        state.get("source"),
        state.get("status"),
        state.get("policy"),
        tuple(state.get("members") or ()),
        state.get("cause"),
    )


def frontier_digest(node_ids: Iterable[str]) -> str:
    """Return the content digest of a cleanup frontier (issue #2718).

    The cleanup record keeps this digest and a count instead of the id list:
    on okg-workspace the list reached about 788,000 ids (62 MB) and every
    checkpoint-metadata reader paid for it. The digest is the gate's
    canonical scope digest (``observations.scope_digest``) over the
    de-duplicated ids, so it is comparable with the gate's
    ``scope_identity`` digests. Order and repeats do not change it.
    """
    from okg.substrate.consistency.observations import scope_digest

    return scope_digest(sorted({str(node_id) for node_id in node_ids if node_id}))


def _frontier_record(node_ids: set[str]) -> dict[str, Any]:
    from okg.substrate.consistency.observations import SCOPE_DIGEST_ALGORITHM

    return {
        "frontier_node_count": len(node_ids),
        "frontier_node_digest_sha256": frontier_digest(node_ids),
        "frontier_node_digest_algorithm": SCOPE_DIGEST_ALGORITHM,
    }


def _is_sha256_hex(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


def read_frontier_summary(cleanup: Mapping[str, Any]) -> dict[str, Any] | None:
    """Read a cleanup record's frontier as a count and a digest.

    A record written since issue #2718 carries ``frontier_node_count`` and
    ``frontier_node_digest_sha256``. An older checkpoint carries the whole
    ``frontier_node_ids`` list instead; its count and digest are computed
    from that list, so old and new records compare on the same terms.
    ``basis`` says which of the two the answer came from. A record with
    neither returns ``None``. A record whose fields are the wrong type, or
    whose count disagrees with its own legacy list, raises: a wrong
    frontier must not read as a plausible one.
    """
    count = cleanup.get("frontier_node_count")
    digest = cleanup.get("frontier_node_digest_sha256")
    legacy = cleanup.get("frontier_node_ids")
    if count is None and digest is None and legacy is None:
        return None
    malformed = {"reason": "derived_cleanup_frontier_malformed"}
    if legacy is not None:
        if not isinstance(legacy, list):
            raise SubstrateError("frontier_node_ids is not a list", data=malformed)
        derived = _frontier_record({str(node_id) for node_id in legacy if node_id})
        if (count is not None and count != derived["frontier_node_count"]) or (
            digest is not None and digest != derived["frontier_node_digest_sha256"]
        ):
            raise SubstrateError(
                "frontier count or digest disagrees with frontier_node_ids",
                data=malformed,
            )
        return {
            "frontier_node_count": derived["frontier_node_count"],
            "frontier_node_digest_sha256": derived["frontier_node_digest_sha256"],
            "basis": "legacy_id_list",
        }
    if (
        not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        or not _is_sha256_hex(digest)
    ):
        raise SubstrateError(
            "frontier_node_count or frontier_node_digest_sha256 is missing or "
            "malformed",
            data=malformed,
        )
    return {
        "frontier_node_count": count,
        "frontier_node_digest_sha256": digest,
        "basis": "recorded",
    }


def plan_derived_node_cleanup(
    candidate_rows: list[Mapping[str, Any]],
    prior_rows: list[Mapping[str, Any]],
    *,
    frontier_node_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Plan closure-scoped fixed-point cleanup without mutating the graph.

    Candidate and prior rows are frozen by the caller. The returned node ids may
    be tombstoned only while the same branch fence remains held.
    """
    candidate = {str(row.get("node_id") or ""): dict(row) for row in candidate_rows}
    prior = {str(row.get("node_id") or ""): dict(row) for row in prior_rows}
    candidate_states = {node_id: _declaration_state(row) for node_id, row in candidate.items()}
    prior_states = {node_id: _declaration_state(row) for node_id, row in prior.items()}

    initial = {str(node_id) for node_id in (frontier_node_ids or set()) if node_id}
    initial.update(set(candidate) ^ set(prior))
    relevant_ids = set(candidate_states) | set(prior_states)
    changed_declarations: set[str] = set()
    for node_id in relevant_ids:
        current = candidate_states.get(node_id)
        previous = prior_states.get(node_id)
        if _declaration_fingerprint(current) == _declaration_fingerprint(previous):
            continue
        if not ((current and current.get("managed")) or (previous and previous.get("managed"))):
            continue
        changed_declarations.add(node_id)
        initial.add(node_id)
        initial.update(str(member) for member in (current or {}).get("members", ()))
        initial.update(str(member) for member in (previous or {}).get("members", ()))

    reverse: dict[str, set[str]] = defaultdict(set)
    for node_id, state in candidate_states.items():
        if not state.get("managed"):
            continue
        for member in state.get("members") or ():
            reverse[str(member)].add(node_id)

    evaluated: set[str] = {
        node_id
        for node_id in initial
        if candidate_states.get(node_id, {}).get("managed")
    }
    # A deque, not a list: the frontier is every node on a first publish, and
    # list.pop(0) shifts the whole remaining list each call, which made this
    # walk quadratic -- about 30 s of the publish lock at 400k nodes (#2941).
    # popleft visits the same members in the same order.
    queue = deque(sorted(initial))
    seen_frontier = set(initial)
    while queue:
        member = queue.popleft()
        for dependent in sorted(reverse.get(member, ())):
            if dependent not in evaluated:
                evaluated.add(dependent)
            if dependent not in seen_frontier:
                seen_frontier.add(dependent)
                queue.append(dependent)

    invalid = {
        node_id: candidate_states[node_id]
        for node_id in sorted(evaluated)
        if candidate_states[node_id].get("status") != "valid"
    }
    declarations = {
        node_id: candidate_states[node_id]["declaration"]
        for node_id in sorted(evaluated)
        if candidate_states[node_id].get("status") == "valid"
    }
    unresolved_members: dict[str, list[str]] = {}
    candidate_ids = set(candidate)
    prior_ids = set(prior)
    for node_id, declaration in declarations.items():
        unknown = sorted(
            member
            for member in declaration.members
            if member not in candidate_ids and member not in prior_ids
        )
        if unknown:
            unresolved_members[node_id] = unknown

    # Induction permits declarations outside the changed closure to remain a
    # live base. Inside the closure, managed nodes earn liveness only through
    # this least fixed point; self-only and unsupported cycles therefore stay
    # false unless a live external member seeds them.
    support = candidate_ids - evaluated
    support.update(
        node_id
        for node_id in evaluated
        if candidate_states[node_id].get("protected")
    )
    pending = dict(declarations)
    while pending:
        ready: list[str] = []
        for node_id, declaration in pending.items():
            if declaration.policy == "any_member":
                # One known-live member proves an any-member declaration even
                # when another member predates both frozen snapshots. We do
                # not need to guess whether that other member is dead.
                holds = any(member in support for member in declaration.members)
            else:
                if node_id in unresolved_members:
                    continue
                holds = all(member in support for member in declaration.members)
            if holds:
                ready.append(node_id)
        if not ready:
            break
        for node_id in ready:
            pending.pop(node_id)
            support.add(node_id)

    unresolved_members = {
        node_id: members
        for node_id, members in unresolved_members.items()
        if node_id not in support
    }
    unsupported = sorted(
        node_id
        for node_id in declarations
        if node_id not in support and node_id not in unresolved_members
    )
    blockers = [
        {
            "node_id": node_id,
            "status": str(state.get("status")),
            "cause": str(state.get("cause")),
        }
        for node_id, state in invalid.items()
    ]
    blockers.extend(
        {
            "node_id": node_id,
            "status": "unresolved_member",
            "cause": "member_absent_from_candidate_and_prior",
            "member_ids": members,
        }
        for node_id, members in sorted(unresolved_members.items())
    )
    unevaluated_legacy = sorted(
        node_id
        for node_id, state in candidate_states.items()
        if state.get("managed")
        and state.get("status") != "valid"
        and node_id not in evaluated
    )
    return {
        "schema": "okg.derived_node_cleanup_plan.v2",
        # Count and digest, never the ids (#2718): the frontier can be every
        # node in the graph, and this plan lands in checkpoint metadata.
        **_frontier_record(initial),
        "changed_declaration_ids": sorted(changed_declarations),
        "evaluated_node_ids": sorted(evaluated),
        "retract_node_ids": [] if blockers else unsupported,
        "unsupported_node_ids": unsupported,
        "blocking_declarations": blockers,
        "unevaluated_legacy_node_ids": unevaluated_legacy,
        "support_fixed_point_node_ids": sorted(set(declarations) & support),
    }


def classify_derived_node(
    row: Mapping[str, Any],
    *,
    live_node_ids: set[str],
) -> dict[str, Any]:
    """Classify one pinned graph node without consulting mutable graph state."""
    node_id = str(row.get("node_id") or "")
    subtype = str(row.get("subtype") or "")
    source = str(row.get("source") or "")
    family, producer = _producer_family(source)

    if family == "_enricher":
        policy_state, verdict, cause, members = _support_state(
            row,
            source=source,
            live_node_ids=live_node_ids,
        )
    elif family == "curated":
        policy_state, verdict, cause, members = (
            "not_applicable",
            "curated",
            "protected_curated_content",
            [],
        )
    elif family == "source_owned":
        policy_state, verdict, cause, members = (
            "not_applicable",
            "source_owned",
            "protected_source_owned_content",
            [],
        )
    else:
        policy_state, verdict, cause, members = (
            "separately_managed",
            "separately_managed",
            "independent_lifecycle",
            [],
        )

    return {
        "node_id": node_id,
        "producer_family": family,
        "producer": producer,
        "subtype": subtype,
        "support_policy_state": policy_state,
        "support_verdict": verdict,
        # ``verdict`` is retained as the short contract name used by the
        # report grouping; ``support_verdict`` makes the row self-documenting.
        "verdict": verdict,
        "cause": cause,
        "members": members,
    }


def _row_value(row: Any, key: str, index: int | None = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if index is not None:
        try:
            return row[index]
        except (IndexError, KeyError, TypeError):
            return None
    return None


def _count_from_query(graph: Any, sql: str) -> int:
    rows = graph.query(sql)
    if not rows:
        raise SubstrateError(
            "derived-node exposure count query returned no row",
            data={"query": sql},
        )
    raw = _row_value(rows[0], "n", 0)
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise SubstrateError(
            "derived-node exposure count query returned a non-integer",
            data={"query": sql, "value": raw},
        ) from exc


def _latest_id(store: Any, *, deployment_name: str, branch: str) -> str | None:
    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch,
        status="published",
    )
    return str(latest.checkpoint_id) if latest is not None else None


def _proof_state(
    conn: Any,
    store: Any,
    *,
    branch: str,
    deployment_name: str,
) -> dict[str, Any]:
    from okg.substrate.publisher import Publisher

    token = Publisher.chronos_graph_branch_mutation_token(
        conn,
        branch_name=branch,
    )
    return {
        "branch_mutation_token": str(token),
        "latest_published_generation": _latest_id(
            store,
            deployment_name=deployment_name,
            branch=branch,
        ),
    }


def _proof_state_with_counts(
    mutable_state: Mapping[str, Any],
    *,
    node_count: int,
    edge_count: int,
) -> dict[str, Any]:
    return {
        **dict(mutable_state),
        "pinned_graph_counts": {
            "nodes": int(node_count),
            "edges": int(edge_count),
        },
        "node_count": int(node_count),
        "edge_count": int(edge_count),
    }


def _close_checkout(graph: Any) -> None:
    close = getattr(graph, "close", None)
    if callable(close):
        close()


def _checkout_immutable_generation(store: Any, checkpoint_id: str, *, branch: str) -> Any:
    """Open a historical checkout without acquiring a writable lease row.

    ``ChronosGraphStore.checkout_generation`` is the right choice for long-lived
    readers because it acquires a database-backed compaction lease.  This
    report is intentionally a zero-write inspection, so it uses the Chronos
    context's immutable checkpoint checkout directly.  Minimal test adapters
    may expose only the public store method; that fallback keeps the helper
    usable without weakening the production path.
    """
    context = getattr(store, "context", None)
    checkout_checkpoint = getattr(context, "checkout_checkpoint", None)
    if callable(checkout_checkpoint):
        return checkout_checkpoint(checkpoint_id)
    return store.checkout_generation(checkpoint_id, branch=branch)


def report_derived_node_exposure(
    conn: Any,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    generation: int | str | None = None,
    example_limit: int = 20,
) -> dict[str, Any]:
    """Return a bounded, immutable-generation derived-node exposure report.

    ``generation`` is optional only to select the latest published checkpoint.
    If supplied, it must resolve to a published checkpoint belonging to both
    ``deployment_name`` and ``branch``.  The function never calls a graph write
    or a publisher; all state is obtained from read-only Chronos checkouts.
    """
    if example_limit < 0:
        raise ValueError("--example-limit must be zero or greater")

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch,
        status="published",
    )
    if latest is None:
        raise SubstrateError(
            "derived-node exposure requires a published checkpoint",
            data={"deployment_name": deployment_name, "branch": branch},
        )

    if generation is None:
        checkpoint = latest
    else:
        checkpoint = store.resolve_generation(
            generation,
            deployment_name=deployment_name,
            branch=branch,
            status="published",
        )
        if checkpoint is None:
            raise SubstrateError(
                "requested generation is not a published checkpoint for the deployment branch",
                data={
                    "generation": str(generation),
                    "deployment_name": deployment_name,
                    "branch": branch,
                },
            )

    metadata = dict(getattr(checkpoint, "metadata", None) or {})
    checkpoint_branch = metadata.get("branch")
    if (
        metadata.get("deployment_name") != deployment_name
        or metadata.get("status") != "published"
        or checkpoint_branch != branch
    ):
        raise SubstrateError(
            "requested generation does not belong to the deployment branch",
            data={
                "generation": str(getattr(checkpoint, "checkpoint_id", generation)),
                "checkpoint_deployment": metadata.get("deployment_name"),
                "checkpoint_branch": checkpoint_branch,
                "deployment_name": deployment_name,
                "branch": branch,
            },
        )

    checkpoint_id = str(checkpoint.checkpoint_id)
    mutable_before = _proof_state(
        conn,
        store,
        branch=branch,
        deployment_name=deployment_name,
    )
    graph = _checkout_immutable_generation(store, checkpoint_id, branch=branch)
    try:
        node_rows = [
            dict(row)
            for row in graph.query(
                "SELECT node_id, subtype, source, source_record_id "
                "FROM okg.graph_nodes ORDER BY node_id"
            )
        ]
        live_node_ids = {str(row.get("node_id") or "") for row in node_rows}
        node_count = _count_from_query(
            graph,
            "SELECT COUNT(*) AS n FROM okg.graph_nodes",
        )
        edge_count = _count_from_query(
            graph,
            "SELECT COUNT(*) AS n FROM okg.graph_edges",
        )

        before = _proof_state_with_counts(
            mutable_before,
            node_count=node_count,
            edge_count=edge_count,
        )
        classifications = [
            classify_derived_node(row, live_node_ids=live_node_ids)
            for row in node_rows
        ]

        groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
        for item in classifications:
            key = (
                item["producer_family"],
                item["producer"],
                item["subtype"],
                item["support_policy_state"],
                item["verdict"],
                item["cause"],
            )
            group = groups.setdefault(
                key,
                {
                    "producer_family": item["producer_family"],
                    "producer": item["producer"],
                    "subtype": item["subtype"],
                    "support_policy_state": item["support_policy_state"],
                    "verdict": item["verdict"],
                    "cause": item["cause"],
                    "count": 0,
                    "examples": [],
                },
            )
            group["count"] += 1
            if len(group["examples"]) < example_limit:
                group["examples"].append(item["node_id"])

        ordered_groups = [groups[key] for key in sorted(groups)]
        unresolved_ids = sorted(
            item["node_id"]
            for item in classifications
            if item["verdict"] == "unresolved"
        )
        unresolved_examples = unresolved_ids[:example_limit]
        unresolved = {
            "ids": unresolved_examples,
            "total": len(unresolved_ids),
            "limit": int(example_limit),
            "truncated": len(unresolved_ids) > example_limit,
        }
        by_verdict: dict[str, int] = defaultdict(int)
        by_family: dict[str, int] = defaultdict(int)
        for item in classifications:
            by_verdict[item["verdict"]] += 1
            by_family[item["producer_family"]] += 1

        mutable_after = _proof_state(
            conn,
            store,
            branch=branch,
            deployment_name=deployment_name,
        )
        after = _proof_state_with_counts(
            mutable_after,
            node_count=node_count,
            edge_count=edge_count,
        )
        proof_ok = before == after
        return {
            "schema": EXPOSURE_SCHEMA,
            "deployment_name": deployment_name,
            "branch": branch,
            "generation": checkpoint_id,
            "pinned_generation": {
                "checkpoint_id": checkpoint_id,
                "status": metadata.get("status"),
                "deployment_name": metadata.get("deployment_name"),
                "branch": metadata.get("branch"),
            },
            "summary": {
                "node_count": node_count,
                "edge_count": edge_count,
                "derived_node_count": by_family.get("_enricher", 0),
                "by_verdict": dict(sorted(by_verdict.items())),
                "by_producer_family": dict(sorted(by_family.items())),
                "classification_count": len(ordered_groups),
            },
            # Both names are intentional: ``classifications`` is the natural
            # report surface, while ``counts`` is convenient for graph-report
            # consumers that expect a count-oriented field.
            "classifications": ordered_groups,
            "counts": ordered_groups,
            "examples": [
                item
                for item in classifications
                if item["node_id"] in {
                    example
                    for group in ordered_groups
                    for example in group["examples"]
                }
            ][:example_limit],
            "unresolved_legacy": unresolved,
            "unresolved_legacy_ids": unresolved["ids"],
            "mutation_proof": {
                "ok": proof_ok,
                "before": before,
                "after": after,
                "compared_fields": [
                    "branch_mutation_token",
                    "latest_published_generation",
                    "pinned_graph_counts.nodes",
                    "pinned_graph_counts.edges",
                    "node_count",
                    "edge_count",
                ],
            },
        }
    finally:
        _close_checkout(graph)
