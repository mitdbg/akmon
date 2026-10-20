"""The pre-grouping source-run signature read, kept only as a test oracle.

PACT ``lane-h-admission-readback`` replaced the path that loaded every node
and edge of a source run into Python. The functions below are the removed
code, copied verbatim from ``src/okg/substrate/producer_authority.py`` at
``origin/dev`` 535e261fac (only renamed), so the tests can prove the grouped
evaluation returns exactly what the old one did. Do not use them anywhere
else: the chronos reader swallows every error, which is the defect this
change removed.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence

import psycopg

from okg.substrate.producer_authority import (
    EDGE_ENDPOINT_MISSING_CODE,
    ProducerProfile,
    _OFFENDING_SAMPLE_LIMIT,
    _batch_violation_message,
    _candidate_edge_clause_ids,
    _candidate_node_clause_ids,
    _match_edge_rule,
    _match_node_rule,
    _missing_endpoint_message,
    _missing_endpoints,
    _rule_clause_id,
    _sample_records,
    _violation,
    profile_write_envelope,
)


def legacy_evaluate_records(
    *,
    profile: ProducerProfile,
    node_records: Sequence[Mapping[str, Any]],
    edge_records: Sequence[Mapping[str, Any]],
    endpoint_scope: str,
    ancestors: Mapping[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Evaluate one emission against a producer profile.

    ``endpoint_scope`` names WHERE this caller looked for endpoint subtypes,
    in plain words. It is required rather than defaulted: an unresolved
    endpoint means only "not found in the place this path searched", and the
    paths search very different places -- one reads the nodes declared in the
    payload itself, another the default-branch graph, another the full
    node-fact history. A message that says the node does not exist would be
    a claim the caller cannot support (issue #1323 review of #1330).
    """
    if profile.status == "compat":
        return {
            "producer_policy": {
                **profile.evidence(),
                "status": "compat",
                "matched_clause_ids": [],
                "failed_clause_ids": [],
                "record_counts": {
                    "node": sum(1 for node in node_records
                                if node.get("op") == "I"),
                    "edge": sum(1 for edge in edge_records
                                if edge.get("op") == "I"),
                },
                "warnings": ["producer_signature_missing"],
            },
            "catalog": {
                "catalog_version_id": profile.catalog_version_id,
                "narrowings": [],
            },
        }
    if not profile.output_signature:
        raise _violation(
            profile=profile,
            code="producer_signature_missing",
            message="producer profile has no output_signature",
            failed_clause_ids=[],
            offending_records=[],
        )

    allowed_kinds = set(profile.allowed_record_kinds)
    if node_records and "node" not in allowed_kinds:
        raise _violation(
            profile=profile,
            code="producer_record_kind_not_allowed",
            message="producer is not allowed to emit node records",
            failed_clause_ids=[],
            offending_records=_sample_records("node", node_records),
        )
    if edge_records and "edge" not in allowed_kinds:
        raise _violation(
            profile=profile,
            code="producer_record_kind_not_allowed",
            message="producer is not allowed to emit edge records",
            failed_clause_ids=[],
            offending_records=_sample_records("edge", edge_records),
        )

    matched_clause_ids: set[str] = set()
    failed_clause_ids: set[str] = set()
    catalog_narrowings: list[dict[str, Any]] = []
    node_rules = list(profile.output_signature.get("nodes") or [])
    edge_rules = list(profile.output_signature.get("edges") or [])

    # Envelope dimensions beyond the signature (spec §9.3): an unbounded
    # dimension is exempt from producer-signature matching (the catalog's
    # graph-shape law still applies downstream — unbounded bounds the
    # PRODUCER's declaration, and forfeits skippability, not admission);
    # closed value vocabularies reject well-typed out-of-vocabulary values
    # as envelope violations, never widening the vocabulary at runtime.
    envelope = profile_write_envelope(profile) or {}
    unbounded_dims = set(envelope.get("unbounded") or ())
    vocabularies = {
        str(field): {str(v) for v in values}
        for field, values in (envelope.get("value_vocabularies") or {}).items()
        if isinstance(values, (list, tuple, set, frozenset))
    }

    def _vocabulary_failure(
        record: Mapping[str, Any], kind: str,
    ) -> tuple[str, str] | None:
        """The closed-vocabulary check as a verdict, not an exception.

        Returning the failure instead of raising is what lets the caller
        finish the batch and report an exact offending count (issue #1330).
        """
        if not vocabularies or "values" in unbounded_dims:
            return None
        attrs = record.get("attrs") or {}
        if not isinstance(attrs, Mapping):
            return None
        for field, allowed_values in vocabularies.items():
            if field not in attrs:
                continue
            value = attrs.get(field)
            if str(value) not in allowed_values:
                return (
                    "envelope_vocabulary_violation",
                    (
                        f"{kind} record carries {field}={value!r}, outside "
                        f"the producer's declared closed vocabulary "
                        f"(spec §9.3: a well-typed out-of-envelope value "
                        f"is recorded and rejected, never widened into the "
                        f"envelope)"
                    ),
                )
        return None

    # Failures are COLLECTED, not raised on sight. The refusal is still
    # whole-batch and fail-closed (the derived-fact loader gates edges by the
    # envelope's fact-range allow-list and nodes by a per-run deny-list, so the
    # envelope is the unit of admission), but a batch refused whole must be
    # able to say how much it discarded and how much of that was admissible.
    # Raising on the first offender made both counts unknowable (issue #1330).
    failures: list[dict[str, Any]] = []
    node_inserts = [
        record for record in node_records if record.get("op") == "I"
    ]
    edge_inserts = [
        record for record in edge_records if record.get("op") == "I"
    ]

    for record in node_inserts:
        vocabulary_failure = _vocabulary_failure(record, "node")
        if vocabulary_failure is not None:
            failures.append({
                "code": vocabulary_failure[0],
                "message": vocabulary_failure[1],
                "kind": "node",
                "record": record,
                "clause_ids": (),
            })
            continue
        if "nodes" in unbounded_dims:
            continue
        rule_index, rule = _match_node_rule(
            node_rules, record, ancestors=ancestors,
        )
        if rule is None:
            failures.append({
                "code": "producer_node_subtype_not_allowed",
                "message": "node record does not match producer signature",
                "kind": "node",
                "record": record,
                "clause_ids": tuple(_candidate_node_clause_ids(
                    profile, str(record["subtype"]),
                )),
            })
            continue
        matched_clause_ids.add(_rule_clause_id(
            rule,
            fallback=f"signature:nodes:{rule_index}",
        ))

    for record in edge_inserts:
        vocabulary_failure = _vocabulary_failure(record, "edge")
        if vocabulary_failure is not None:
            failures.append({
                "code": vocabulary_failure[0],
                "message": vocabulary_failure[1],
                "kind": "edge",
                "record": record,
                "clause_ids": (),
            })
            continue
        if "edges" in unbounded_dims:
            continue
        rule_index, rule = _match_edge_rule(
            edge_rules, record, ancestors=ancestors,
        )
        if rule is None:
            # Which defect is this? An endpoint whose subtype did not resolve
            # is a node that does not exist, not a signature the producer got
            # wrong. The endpoint diagnosis is claimed ONLY when the missing
            # endpoint is the sole blocker: re-match with the unresolved
            # endpoint treated as a wildcard, and if the record would then
            # satisfy a declared rule, the signature is genuinely not at
            # issue. An edge that ALSO carries an undeclared edge_type or a
            # wrong resolved endpoint keeps the signature code -- reporting
            # that one as a missing endpoint would be the mirror image of the
            # defect this change exists to fix. The record is refused either
            # way; only the reported cause differs.
            missing = _missing_endpoints(record)
            _, wildcard_rule = (
                _match_edge_rule(
                    edge_rules,
                    record,
                    ancestors=ancestors,
                    unresolved_endpoint_wildcard=True,
                )
                if missing else (-1, None)
            )
            if missing and wildcard_rule is not None:
                failures.append({
                    "code": EDGE_ENDPOINT_MISSING_CODE,
                    "message": _missing_endpoint_message(
                        record, missing, endpoint_scope=endpoint_scope,
                    ),
                    "kind": "edge",
                    "record": record,
                    "clause_ids": (),
                    "missing_endpoints": missing,
                })
                continue
            failures.append({
                "code": "producer_edge_triple_not_allowed",
                "message": "edge record does not match producer signature",
                "kind": "edge",
                "record": record,
                "clause_ids": tuple(_candidate_edge_clause_ids(
                    profile,
                    str(record.get("src_subtype") or ""),
                    str(record["edge_type"]),
                    str(record.get("dst_subtype") or ""),
                )),
            })
            continue
        matched_clause_ids.add(_rule_clause_id(
            rule,
            fallback=f"signature:edges:{rule_index}",
        ))
        catalog_narrowings.append({
            "src_subtype": record.get("src_subtype"),
            "edge_type": record["edge_type"],
            "dst_subtype": record.get("dst_subtype"),
            "status": "matched",
        })

    if failures:
        # The first failure decides the code, exactly as the raise-on-sight
        # order did; every other record failing the SAME way is then counted,
        # so `offending_record_count` is measured rather than assumed.
        code = str(failures[0]["code"])
        group = [failure for failure in failures if failure["code"] == code]
        for failure in group:
            failed_clause_ids.update(failure["clause_ids"])
        offending_records: list[dict[str, Any]] = []
        for failure in group[:_OFFENDING_SAMPLE_LIMIT]:
            offending_records.extend(
                _sample_records(str(failure["kind"]), [failure["record"]]),
            )
        missing_endpoints = [
            endpoint
            for failure in group[:_OFFENDING_SAMPLE_LIMIT]
            for endpoint in failure.get("missing_endpoints", ())
        ]
        by_code: dict[str, int] = {}
        for failure in failures:
            by_code[str(failure["code"])] = (
                by_code.get(str(failure["code"]), 0) + 1
            )
        evaluated = len(node_inserts) + len(edge_inserts)
        # Collateral excludes EVERY failing record, not merely the ones
        # sharing the reported code: a record refused under a second code was
        # never admissible, and counting it as collateral would overstate what
        # the refusal cost.
        raise _violation(
            profile=profile,
            code=code,
            message=_batch_violation_message(
                str(group[0]["message"]),
                len(group),
                evaluated,
                collateral=evaluated - len(failures),
            ),
            failed_clause_ids=sorted(failed_clause_ids),
            offending_records=offending_records,
            record_counts={
                "node": len(node_inserts),
                "edge": len(edge_inserts),
                # Retractions travel in the same emission and are discarded
                # with it, but they are not signature-evaluated; name them
                # rather than let the totals quietly disagree.
                "non_insert": (
                    len(node_records) + len(edge_records) - evaluated
                ),
            },
            offending_record_count=len(group),
            offending_record_counts_by_code=by_code,
            collateral_record_count=evaluated - len(failures),
            missing_endpoints=missing_endpoints or None,
            sample_truncated=len(group) > _OFFENDING_SAMPLE_LIMIT,
        )

    return {
        "producer_policy": {
            **profile.evidence(),
            "status": "matched",
            "matched_clause_ids": sorted(matched_clause_ids),
            "failed_clause_ids": sorted(failed_clause_ids),
            "record_counts": {
                "node": sum(1 for node in node_records
                            if node.get("op") == "I"),
                "edge": sum(1 for edge in edge_records
                            if edge.get("op") == "I"),
            },
        },
        "catalog": {
            "catalog_version_id": profile.catalog_version_id,
            "narrowings": _dedupe_dicts(catalog_narrowings),
        },
    }


def legacy_source_run_records_from_chronos(
    conn: psycopg.Connection,
    *,
    source: str,
    run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]] | None:
    try:
        with conn.transaction():
            node_rows = _chronos_interval_rows(
                conn,
                table_name="okg.graph_nodes",
                columns=("node_id", "subtype", "attrs"),
                source=source,
                run_id=run_id,
            )
            edge_rows = _chronos_interval_rows(
                conn,
                table_name="okg.graph_edges",
                columns=("src", "edge_type", "dst", "attrs"),
                source=source,
                run_id=run_id,
            )
            endpoint_ids = {
                str(row["src"]) for row in edge_rows
            } | {
                str(row["dst"]) for row in edge_rows
            }
            endpoint_subtypes = {
                str(row["node_id"]): str(row["subtype"])
                for row in node_rows
            }
            missing = sorted(endpoint_ids - set(endpoint_subtypes))
            if missing:
                for row in _chronos_interval_endpoint_rows(
                    conn,
                    node_ids=missing,
                ):
                    endpoint_subtypes[str(row["node_id"])] = str(row["subtype"])
    except Exception:  # noqa: BLE001 — best-effort authority read; failure yields no record
        return None
    if not node_rows and not edge_rows:
        return None
    node_records = [
        {
            "fact_id": idx,
            "node_id": str(row["node_id"]),
            "subtype": str(row["subtype"]),
            "attrs": dict(row["attrs"] or {}),
            "op": "I",
        }
        for idx, row in enumerate(node_rows, start=1)
    ]
    edge_records = [
        {
            "fact_id": idx,
            "src": str(row["src"]),
            "src_subtype": endpoint_subtypes.get(str(row["src"])),
            "edge_type": str(row["edge_type"]),
            "dst": str(row["dst"]),
            "dst_subtype": endpoint_subtypes.get(str(row["dst"])),
            "attrs": dict(row["attrs"] or {}),
            "op": "I",
        }
        for idx, row in enumerate(edge_rows, start=1)
    ]
    return node_records, edge_records


def _chronos_interval_rows(
    conn: psycopg.Connection,
    *,
    table_name: str,
    columns: tuple[str, ...],
    source: str,
    run_id: str,
    branch: str = "default",
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    ).checkout(branch)
    select_cols = ", ".join(columns)
    order_by = ", ".join(
        column for column in columns if column != "attrs"
    ) or columns[0]
    return graph.query(
        f"""
        SELECT {select_cols}
          FROM {table_name}
         WHERE source = :source
           AND run_id = :run_id
         ORDER BY {order_by}
        """,
        {"source": source, "run_id": run_id},
    )


def _chronos_interval_endpoint_rows(
    conn: psycopg.Connection,
    *,
    node_ids: list[str],
    branch: str = "default",
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    ).checkout(branch)
    return graph.query(
        """
        SELECT node_id, subtype
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": node_ids},
    )


def _dedupe_dicts(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    # Removed from producer_authority with the grouped evaluator (which
    # dedupes narrowings as it goes); kept here verbatim for the oracle.
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for record in records:
        encoded = json.dumps(record, sort_keys=True, default=str)
        if encoded in seen:
            continue
        seen.add(encoded)
        out.append(dict(record))
    return out


def legacy_evaluate_source_run_from_chronos(
    conn: psycopg.Connection,
    *,
    profile: ProducerProfile,
    source: str,
    run_id: str,
    ancestors: Mapping[str, set[str]] | None,
) -> dict[str, Any] | None:
    """The old chronos branch of ``evaluate_source_run_signature``."""
    records = legacy_source_run_records_from_chronos(
        conn, source=source, run_id=run_id,
    )
    if records is None:
        return None
    node_records, edge_records = records
    return legacy_evaluate_records(
        profile=profile,
        node_records=node_records,
        edge_records=edge_records,
        endpoint_scope="the default-branch graph nodes for this source run",
        ancestors=ancestors,
    )


__all__ = [
    "legacy_evaluate_records",
    "legacy_evaluate_source_run_from_chronos",
    "legacy_source_run_records_from_chronos",
]
