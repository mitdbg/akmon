"""Review-mode admission workflows.

Trusted source admission is integrated into ``okg_source_sync``. These
workflows are only for human/agent review-mode payloads: submit a
proposal, then record an accept/reject/quarantine decision.
"""
from __future__ import annotations

import hashlib
import json
import logging
from contextlib import nullcontext
from pathlib import Path
from typing import Any
import uuid

from okg.substrate.errors import SubstrateError
from okg.substrate.images.values import (
    ImageRefInvalid,
    ImageValueForged,
    check_incoming,
    check_record_write,
)
from okg.substrate.transform.commit import persist_transform_program
from okg.substrate.transform.programs import admission_program


__all__ = [
    "ADMISSION_SUBMIT_WORKFLOW_NAME",
    "ADMISSION_DECIDE_WORKFLOW_NAME",
    "evaluate_runtime_approver",
    "register_admission_workflows",
    "admission_submit_workflow",
    "admission_decide_workflow",
]


log = logging.getLogger(__name__)

# Commit-witness (PACT route-all-writers, task 2): the admission path stamps the
# facts it emits with this program's checksum, via the GUC-default on
# okg.{node,edge}_facts.source_program_checksum.
_ADMISSION_PROGRAM = admission_program()
_ADMISSION_CHECKSUM = _ADMISSION_PROGRAM.checksum()


ADMISSION_SUBMIT_WORKFLOW_NAME = "okg_admission_submit"
ADMISSION_DECIDE_WORKFLOW_NAME = "okg_admission_decide"


_DECORATED: dict[str, Any] = {}


def register_admission_workflows(
    *,
    deployment: str,
    approver_config: dict[str, Any] | None = None,
) -> None:
    """Decorate the review-mode admission workflows."""
    from dbos import DBOS

    _DECORATED.clear()

    @DBOS.step(name="okg_admission_submit_write")
    def _submit_step(
        payload: dict,
        producer_id: str,
        producer_kind: str,
        intent: str,
        idempotency_key: str | None,
        authority: dict | None,
        check_summary: dict | None,
    ) -> dict:
        return _run_submit_with_runtime_approver(
            deployment=deployment,
            payload=payload,
            producer_id=producer_id,
            producer_kind=producer_kind,
            intent=intent,
            idempotency_key=idempotency_key,
            authority=authority,
            check_summary=check_summary,
            approver_config=approver_config,
        )

    @DBOS.workflow(
        name=ADMISSION_SUBMIT_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def admission_submit_workflow_impl(
        payload: dict,
        producer_id: str,
        producer_kind: str = "agent",
        intent: str = "review_proposal",
        idempotency_key: str | None = None,
        authority: dict | None = None,
        check_summary: dict | None = None,
    ) -> dict:
        return _submit_step(
            payload,
            producer_id,
            producer_kind,
            intent,
            idempotency_key,
            authority,
            check_summary,
        )

    @DBOS.step(name="okg_admission_decide_write")
    def _decide_step(
        admission_id: str,
        decision: str,
        actor: str,
        reason: str | None,
        verdict: dict | None,
    ) -> dict:
        return _run_decide(
            deployment=deployment,
            admission_id=admission_id,
            decision=decision,
            actor=actor,
            reason=reason,
            verdict=verdict,
        )

    @DBOS.workflow(
        name=ADMISSION_DECIDE_WORKFLOW_NAME,
        max_recovery_attempts=10,
    )
    def admission_decide_workflow_impl(
        admission_id: str,
        decision: str,
        actor: str = "operator",
        reason: str | None = None,
        verdict: dict | None = None,
    ) -> dict:
        return _decide_step(
            admission_id,
            decision,
            actor,
            reason,
            verdict,
        )

    _DECORATED[ADMISSION_SUBMIT_WORKFLOW_NAME] = (
        admission_submit_workflow_impl
    )
    _DECORATED[ADMISSION_DECIDE_WORKFLOW_NAME] = (
        admission_decide_workflow_impl
    )
    log.info(
        "registered admission workflows: deployment=%s submit=%s decide=%s",
        deployment,
        ADMISSION_SUBMIT_WORKFLOW_NAME,
        ADMISSION_DECIDE_WORKFLOW_NAME,
    )


def admission_submit_workflow() -> Any:
    if ADMISSION_SUBMIT_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "admission-submit workflow not registered; call "
            "register_admission_workflows() before DBOS.launch()"
        )
    return _DECORATED[ADMISSION_SUBMIT_WORKFLOW_NAME]


def admission_decide_workflow() -> Any:
    if ADMISSION_DECIDE_WORKFLOW_NAME not in _DECORATED:
        raise RuntimeError(
            "admission-decide workflow not registered; call "
            "register_admission_workflows() before DBOS.launch()"
        )
    return _DECORATED[ADMISSION_DECIDE_WORKFLOW_NAME]


def _tolerant_fact_dicts(
    payload: dict[str, Any],
    *,
    list_key: str,
    single_key: str,
    kind: str,
) -> list[tuple[str, dict[str, Any]]]:
    """Extract (label, dict) node/edge items from a raw submit payload
    WITHOUT enforcing shape.

    Tolerant on purpose: malformed shapes must keep rejecting at apply
    time with the existing ``invalid_payload`` verdicts — this helper
    only needs to find identity-bearing fields for the NUL admission
    check at submit.
    """
    items: list[tuple[str, dict[str, Any]]] = []
    value = payload.get(list_key)
    if isinstance(value, list):
        items.extend(
            (f"{list_key}[{index}]", item)
            for index, item in enumerate(value)
            if isinstance(item, dict)
        )
    single = payload.get(single_key)
    if isinstance(single, dict):
        items.append((single_key, single))
    if payload.get("kind") == kind:
        items.append((
            "payload",
            {key: v for key, v in payload.items() if key != "kind"},
        ))
    return items


_NODE_PAYLOAD_IDENTITY_KEYS = (
    "node_id", "subtype", "source_record_id", "source_revision", "op",
)
_EDGE_PAYLOAD_IDENTITY_KEYS = (
    "src", "dst", "edge_type", "source_record_id", "source_revision",
    "op", "src_subtype", "dst_subtype",
    # Closed vocabulary (edge_facts_provenance_check): a scrubbed
    # provenance could never be a valid member, so NUL rejects.
    "provenance",
)


def _reject_submit_identity_nul(
    *,
    payload: dict[str, Any],
    producer_id: str,
    producer_kind: str,
    idempotency_key: str | None,
    authority: dict | None,
) -> None:
    """Reject an admission submit whose identity-bearing strings carry
    NUL (U+0000).

    Per PACT sanitize-fact-text-at-admission: identity is never
    silently rewritten — scrubbing a node_id / dedupe identity /
    branch name would alias two distinct identities. Everything else
    in the submission scrubs to U+FFFD (see ``_run_submit``).
    """
    from okg.substrate.ingest.sanitize import value_contains_nul

    offenders: list[str] = []
    if value_contains_nul(producer_id):
        offenders.append("producer_id")
    if value_contains_nul(producer_kind):
        offenders.append("producer_kind")
    if idempotency_key is not None and value_contains_nul(idempotency_key):
        offenders.append("idempotency_key")
    target_branch = (authority or {}).get("target_branch_name")
    if isinstance(target_branch, str) and value_contains_nul(target_branch):
        offenders.append("authority.target_branch_name")
    for label, item in _tolerant_fact_dicts(
        payload, list_key="nodes", single_key="node", kind="node",
    ):
        offenders.extend(
            f"{label}.{key}"
            for key in _NODE_PAYLOAD_IDENTITY_KEYS
            if value_contains_nul(item.get(key))
        )
    for label, item in _tolerant_fact_dicts(
        payload, list_key="edges", single_key="edge", kind="edge",
    ):
        offenders.extend(
            f"{label}.{key}"
            for key in _EDGE_PAYLOAD_IDENTITY_KEYS
            if value_contains_nul(item.get(key))
        )
    if offenders:
        from okg.substrate.errors import SubstrateError

        raise SubstrateError(
            "admission payload contains NUL (U+0000) in "
            "identity-bearing field(s); identity is never silently "
            "rewritten — remove the NUL and resubmit "
            "(PACT sanitize-fact-text-at-admission)",
            data={"fields": offenders},
        )


def _runtime_approver_payload_summary(
    payload: Any,
) -> tuple[dict[str, Any] | None, str | None]:
    """Return a strict, bounded payload summary for auto-approval."""
    if not isinstance(payload, dict):
        return None, "runtime_approver_payload_malformed"
    try:
        nodes = _raw_node_payloads(payload)
        edges = _raw_edge_payloads(payload)
    except _ReviewPayloadRejected as exc:
        if exc.reason == "retraction_not_permitted":
            return None, "runtime_approver_retraction_forbidden"
        return None, "runtime_approver_payload_malformed"
    if not nodes and not edges:
        return None, "runtime_approver_payload_malformed"
    node_subtypes: list[str] = []
    edge_types: list[str] = []
    for node in nodes:
        node_id = node.get("node_id")
        subtype = node.get("subtype")
        op = node.get("op", "I")
        if op == "R":
            return None, "runtime_approver_retraction_forbidden"
        if (
            op != "I"
            or not isinstance(node_id, str)
            or not node_id.strip()
            or not isinstance(subtype, str)
            or not subtype.strip()
            or ("attrs" in node and not isinstance(node["attrs"], dict))
            or (
                "source_record_id" in node
                and not isinstance(node["source_record_id"], dict)
            )
            or (
                "source_revision" in node
                and not isinstance(node["source_revision"], dict)
            )
        ):
            return None, "runtime_approver_payload_malformed"
        node_subtypes.append(subtype)
    for edge in edges:
        src = edge.get("src")
        dst = edge.get("dst")
        edge_type = edge.get("edge_type")
        op = edge.get("op", "I")
        if op == "R":
            return None, "runtime_approver_retraction_forbidden"
        try:
            confidence = float(edge.get("confidence", 1.0))
        except (TypeError, ValueError):
            return None, "runtime_approver_payload_malformed"
        provenance = edge.get("provenance", "explicit")
        from okg.substrate.library.sources.source_specs import (
            VALID_PROVENANCE_CLASSES,
        )

        if (
            op != "I"
            or not isinstance(src, str)
            or not src.strip()
            or not isinstance(dst, str)
            or not dst.strip()
            or not isinstance(edge_type, str)
            or not edge_type.strip()
            or not 0 <= confidence <= 1
            or provenance not in VALID_PROVENANCE_CLASSES
            or ("attrs" in edge and not isinstance(edge["attrs"], dict))
            or (
                "source_record_id" in edge
                and not isinstance(edge["source_record_id"], dict)
            )
            or (
                "source_revision" in edge
                and not isinstance(edge["source_revision"], dict)
            )
            or (
                "src_subtype" in edge
                and (
                    not isinstance(edge["src_subtype"], str)
                    or not edge["src_subtype"].strip()
                )
            )
            or (
                "dst_subtype" in edge
                and (
                    not isinstance(edge["dst_subtype"], str)
                    or not edge["dst_subtype"].strip()
                )
            )
        ):
            return None, "runtime_approver_payload_malformed"
        edge_types.append(edge_type)
    return {
        "node_count": len(nodes),
        "edge_count": len(edges),
        "node_subtypes": sorted(set(node_subtypes)),
        "edge_types": sorted(set(edge_types)),
    }, None


def evaluate_runtime_approver(
    *,
    approver_config: dict[str, Any] | None,
    payload: Any,
    producer_id: str,
    producer_kind: str,
    intent: str,
    authority: dict[str, Any] | None,
) -> dict[str, Any]:
    """Evaluate one review payload against the closed approver rules.

    The result is safe to persist in ``check_summary.runtime_approver``.
    Auto-approval is eligible only when exactly one complete rule matches.
    """
    from okg.substrate.runtime.config import (
        validate_admission_intent,
        validate_runtime_admission_config,
    )

    validate_admission_intent(intent)
    normalized = validate_runtime_admission_config(approver_config or {})
    approver = normalized.get("approver")
    base: dict[str, Any] = {
        "eligible": False,
        "reason_code": "runtime_approver_disabled",
        "matched_rule_ids": [],
    }
    if not approver or not approver["enabled"]:
        return base
    policy = dict((authority or {}).get("producer_policy") or {})
    admission_mode = str(policy.get("admission_mode") or "review")
    if admission_mode != "review":
        return {
            **base,
            "reason_code": "runtime_approver_mode_not_review",
            "admission_mode": admission_mode,
        }
    summary, refusal = _runtime_approver_payload_summary(payload)
    if refusal is not None:
        return {**base, "reason_code": refusal}
    assert summary is not None
    base.update(summary)
    rules = list(approver["rules"])
    identity_rules = [
        rule for rule in rules
        if producer_id in rule["producer_ids"]
        and producer_kind in rule["producer_kinds"]
        and intent in rule["intents"]
    ]
    if not identity_rules:
        return {
            **base,
            "reason_code": "runtime_approver_no_identity_match",
        }
    within_budget = [
        rule for rule in identity_rules
        if summary["node_count"] <= rule["max_nodes"]
        and summary["edge_count"] <= rule["max_edges"]
    ]
    if not within_budget:
        return {**base, "reason_code": "runtime_approver_over_budget"}
    content_rules = [
        rule for rule in within_budget
        if set(summary["node_subtypes"]).issubset(rule["node_subtypes"])
        and set(summary["edge_types"]).issubset(rule["edge_types"])
    ]
    if not content_rules:
        return {
            **base,
            "reason_code": "runtime_approver_record_type_not_allowed",
        }
    matched_rule_ids = sorted(rule["id"] for rule in content_rules)
    if len(matched_rule_ids) != 1:
        return {
            **base,
            "reason_code": "runtime_approver_ambiguous_match",
            "matched_rule_ids": matched_rule_ids,
        }
    return {
        **base,
        "eligible": True,
        "reason_code": "runtime_approver_rule_match",
        "matched_rule_ids": matched_rule_ids,
        "actor": approver["actor"],
    }


def _run_submit_with_runtime_approver(
    *,
    deployment: str,
    payload: dict,
    producer_id: str,
    producer_kind: str,
    intent: str,
    idempotency_key: str | None,
    authority: dict | None,
    check_summary: dict | None,
    approver_config: dict[str, Any] | None,
    dsn: str | None = None,
) -> dict[str, Any]:
    """Submit and, for one exact rule match, accept the new envelope.

    The DBOS submit step calls this with the manifest DSN. The inline CLI
    path (runtime disabled) calls it with the operator's ``--dsn`` and no
    approver config, so both paths share the idempotency lock and the
    replay short-circuit below.
    """
    evaluation = evaluate_runtime_approver(
        approver_config=approver_config,
        payload=payload,
        producer_id=producer_id,
        producer_kind=producer_kind,
        intent=intent,
        authority=authority,
    )
    from okg.substrate.admission import stable_idempotency_key

    key = idempotency_key or stable_idempotency_key(
        "review",
        deployment,
        producer_id,
        intent,
        payload,
    )
    from okg.substrate.db.connection import connect as db_connect

    dsn = dsn or _resolve_dsn(deployment)
    with db_connect(dsn, autocommit=True) as conn:
        # Bootstrap the Chronos schema BEFORE the transaction opens (#2639).
        # In a process that has not bootstrapped this database yet, the
        # bootstrap ends with an explicit commit, which psycopg refuses inside
        # `conn.transaction()`. `_run_submit` and `_run_decide` bootstrap the
        # same way on their own connections; their calls below then find the
        # schema ready and do not commit. Autocommit keeps the bootstrap's
        # own reads out of the block below, so `conn.transaction()` is still
        # the one top-level transaction rather than a savepoint.
        from okg.substrate.graph_store import ChronosGraphStore

        ChronosGraphStore.from_connection(conn)
        with conn.transaction():
            # Serialize the exact idempotency scope before the existence read.
            # The lock and the submit+decision share one transaction: a crash
            # rolls back both, and a concurrent replay waits then observes the
            # committed envelope without reaching ensure_envelope's duplicate
            # event path.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{deployment}\x1f{producer_id}\x1f{key}",),
                )
            existing = _existing_runtime_approver_submission(
                deployment=deployment,
                producer_id=producer_id,
                idempotency_key=key,
                connection=conn,
            )
            if existing is not None:
                return {
                    **existing,
                    "runtime_approver": {
                        **evaluation,
                        "eligible": False,
                        "reason_code": "runtime_approver_duplicate_existing",
                    },
                }
            submitted = _run_submit(
                deployment=deployment,
                payload=payload,
                producer_id=producer_id,
                producer_kind=producer_kind,
                intent=intent,
                idempotency_key=key,
                authority=authority,
                check_summary={
                    **dict(check_summary or {}),
                    "runtime_approver": evaluation,
                },
                connection=conn,
            )
            if not evaluation["eligible"] or submitted["status"] != "pending":
                return {**submitted, "runtime_approver": evaluation}
            decided = _run_decide(
                deployment=deployment,
                admission_id=submitted["admission_id"],
                decision="accept",
                actor=str(evaluation["actor"]),
                reason=str(evaluation["reason_code"]),
                verdict={"runtime_approver": evaluation},
                connection=conn,
            )
            return {
                **submitted,
                **decided,
                "inserted": True,
                "runtime_approver": evaluation,
            }


def _existing_runtime_approver_submission(
    *,
    deployment: str,
    producer_id: str,
    idempotency_key: str,
    connection: Any | None = None,
) -> dict[str, Any] | None:
    """Read an existing envelope before any replay-side mutation."""
    from okg.substrate.db.connection import connect as db_connect

    scope = (
        nullcontext(connection)
        if connection is not None
        else db_connect(_resolve_dsn(deployment))
    )
    with scope as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, status, admission_mode
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND producer_id = %s
               AND idempotency_key = %s
            """,
            (deployment, producer_id, idempotency_key),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "deployment": deployment,
        "admission_id": str(row[0]),
        "status": str(row[1]),
        "admission_mode": str(row[2]),
        "inserted": False,
        "payload_ref_inserted": False,
        "idempotency_key": idempotency_key,
    }


class AdmissionProducerIdentityReserved(SubstrateError):
    """A submit named a producer identity only the server may assign.

    An accepted submission is written under its producer id as the fact
    ``source`` and judged by Nomos on that value, so a caller-chosen id must
    not be blank, carry a server prefix, or equal a registered source, and a
    caller may not claim the ``source`` producer kind.
    """

    code = "admission_producer_identity_reserved"


#: Producer-id prefixes the server assigns to its own writers: every
#: underscore-led id (lenses, extractors, enrichers, catalog jobs,
#: `_publisher_derived`), admission's fallback source, Poros senders, catalog
#: and system jobs, and Nomos source subjects. Matched case-insensitively.
_RESERVED_PRODUCER_ID_PREFIXES = (
    "_",
    "admission:",
    "poros:",
    "catalog:",
    "system:",
    "source:",
)

#: Producer kinds a caller may submit as. Every other kind (source,
#: extractor, enricher, lens) is assigned by a server-side writer.
_CALLER_PRODUCER_KINDS = frozenset({"agent", "human", "workflow", "system"})


def _refuse_reserved_producer_identity(
    *,
    deployment: str,
    producer_id: Any,
    producer_kind: Any,
) -> None:
    """Refuse a caller-chosen producer identity the server reserves.

    Checks that need no I/O: blank or padded ids, server prefixes, and
    server-only kinds. Registered sources are checked by
    ``_refuse_registered_source_producer`` inside the submit transaction.
    """
    def refuse(reason: str, fix_hint: str) -> None:
        raise AdmissionProducerIdentityReserved(
            f"admission refused producer identity: {reason}",
            data={
                "deployment": deployment,
                "producer_id": producer_id,
                "producer_kind": producer_kind,
                "reason": reason,
            },
            fix_hint=fix_hint,
        )

    if not isinstance(producer_id, str) or not producer_id.strip():
        refuse(
            "producer_id_blank",
            "pass a non-empty producer id such as agent:<name>",
        )
    if producer_id != producer_id.strip():
        refuse(
            "producer_id_padded",
            "remove leading and trailing whitespace from the producer id",
        )
    folded = producer_id.casefold()
    for prefix in _RESERVED_PRODUCER_ID_PREFIXES:
        if folded.startswith(prefix):
            refuse(
                "producer_id_reserved_prefix",
                f"the {prefix!r} prefix is assigned by the server; use "
                "agent:<name>",
            )
    if producer_kind not in _CALLER_PRODUCER_KINDS:
        refuse(
            "producer_kind_server_only",
            "submit as agent, human, workflow or system; sources, "
            "extractors, enrichers and lenses are admitted by the server",
        )


def _registered_source_identities(deployment: str) -> frozenset[str]:
    """Every id a registered source writes or is named by, from the manifest.

    Registry names, `<deployment>.<name>`, ownership ids, declared policy
    producer ids, and declared derived source families (every
    ``authority_scope.source_family`` in the manifest or the registry). The
    registry path is resolved as ``okg deployment apply`` and the Nomos
    baseline resolve it: through the typed manifest, so placeholders such as
    ``${manifest_dir}`` are expanded. A deployment with no deployment
    directory has no registry. A declared registry that does not exist, or
    one that cannot be read, raises, so the caller fails closed.
    """
    from okg.substrate import fastyaml
    from okg.substrate.deployments import (
        DeploymentManifest,
        find_deployment_dir,
        resolve_source_registry_path,
    )
    from okg.substrate.nomos.store import deployment_derived_source_families
    from okg.substrate.sources.registry import load_source_registry

    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        return frozenset()
    manifest_path = Path(deployment_dir) / "deployment.yaml"
    raw_manifest = fastyaml.safe_load(
        manifest_path.read_text(encoding="utf-8"),
    ) or {}
    if not isinstance(raw_manifest, dict):
        raise SubstrateError(
            "deployment manifest must be a mapping",
            data={"deployment": deployment, "path": str(manifest_path)},
        )
    declared = raw_manifest.get("source_registry")
    registry_path = Path(
        getattr(
            DeploymentManifest.from_yaml_lenient(manifest_path),
            "source_registry",
            None,
        )
        or resolve_source_registry_path(deployment_dir, raw_manifest)
    )
    identities: set[str] = set(deployment_derived_source_families(raw_manifest))
    if not registry_path.is_file():
        if declared:
            raise SubstrateError(
                "declared source registry does not exist; admission cannot "
                "tell which producer ids belong to sources",
                data={
                    "deployment": deployment,
                    "path": str(registry_path),
                    "declared": str(declared),
                },
                fix_hint="fix `source_registry` in deployment.yaml",
            )
        return frozenset(identity for identity in identities if identity)
    for name, entry in load_source_registry(registry_path).items():
        identities.update({name, f"{deployment}.{name}", entry.ownership_id})
        policy = entry.admission_policy
        if policy is not None:
            identities.add(policy.producer_id)
            family = (policy.authority_scope or {}).get("source_family")
            if isinstance(family, str) and family.strip():
                identities.add(family.strip())
    return frozenset(identity for identity in identities if identity)


def _refuse_registered_source_producer(
    conn,
    *,
    deployment: str,
    producer_id: str,
    producer_kind: str,
) -> None:
    """Refuse a producer id that names a source of this deployment.

    Two lists: the manifest's source registry (no database work) and the
    sources this database already knows -- a watermark row or a source
    producer profile -- in one indexed query on the submit transaction.
    """
    registered = producer_id in _registered_source_identities(deployment)
    if not registered:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                         SELECT 1 FROM okg.source_watermarks
                          WHERE source = %(producer_id)s)
                    OR EXISTS (
                         SELECT 1 FROM okg.producer_profiles
                          WHERE deployment_name = %(deployment)s
                            AND producer_id = %(producer_id)s
                            AND producer_kind = 'source')
                """,
                {"deployment": deployment, "producer_id": producer_id},
            )
            (registered,) = cur.fetchone()
    if registered:
        raise AdmissionProducerIdentityReserved(
            "admission refused producer identity: "
            "producer_id_is_registered_source",
            data={
                "deployment": deployment,
                "producer_id": producer_id,
                "producer_kind": producer_kind,
                "reason": "producer_id_is_registered_source",
            },
            fix_hint=(
                "a source's name is its identity in the graph; submit under "
                "your own producer id such as agent:<name>"
            ),
        )


def _decide_producer_identity_refusal(
    conn,
    *,
    deployment: str,
    admission_id: str,
    status: str,
    current: dict[str, Any],
) -> dict[str, Any] | None:
    """Re-run the submit identity rules when an accept writes the producer id.

    A pending envelope may predate this rule, or its producer id may have
    become a source after submit. Only an accept writes facts, and only an
    envelope with no stored source outside ``source_sync`` writes them under
    its producer id, so only those are checked. Lens promotions are the one
    server writer of such envelopes and are exempt. A refusal leaves the
    envelope ``pending`` (no transition, no event, no fact) so an operator
    can still reject it.
    """
    if status != "accepted" or current.get("status") != "pending":
        return None
    if current.get("intent") == "source_sync" or current.get("source"):
        return None
    producer_id = current.get("producer_id")
    producer_kind = current.get("producer_kind")
    if (
        producer_kind == "lens"
        and current.get("intent") == "promote_lens_overlay"
        and isinstance(producer_id, str)
        and producer_id.startswith("_lens:")
    ):
        return None
    try:
        _refuse_reserved_producer_identity(
            deployment=deployment,
            producer_id=producer_id,
            producer_kind=producer_kind,
        )
        _refuse_registered_source_producer(
            conn,
            deployment=deployment,
            producer_id=producer_id,
            producer_kind=producer_kind,
        )
    except AdmissionProducerIdentityReserved as exc:
        # The reader here is the operator deciding, not the submitter: the
        # envelope stays pending, so tell them how to clear it.
        fix_hint = (
            "reject this envelope (okg admission decide --deployment "
            f"{deployment} {admission_id} --decision reject) and have the "
            "producer resubmit under its own id such as agent:<name>"
        )
        return {
            "admission_id": admission_id,
            "status": "refused",
            "envelope_status": "pending",
            "target_generation_ref": current.get("target_generation_ref"),
            "reason": exc.code,
            "fix_hint": fix_hint,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "fix_hint": fix_hint,
                "data": exc.data,
            },
        }
    return None


def _run_submit(
    *,
    deployment: str,
    payload: dict,
    producer_id: str,
    producer_kind: str,
    intent: str,
    idempotency_key: str | None,
    authority: dict | None,
    check_summary: dict | None,
    connection: Any | None = None,
) -> dict:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.admission import (
        attach_fact_range_refs_for_run,
        attach_payload_ref,
        ensure_envelope,
        record_event,
        stable_idempotency_key,
        transition_envelope,
        workflow_ref_from_dbos,
    )
    from okg.substrate.producer_authority import (
        materialize_producer_profile,
        merge_policy_evidence,
        policy_from_authority,
    )

    if not isinstance(payload, dict):
        raise TypeError("admission submit payload must be a dict")

    from okg.substrate.runtime.config import validate_admission_intent

    intent = validate_admission_intent(intent)
    # Every caller submit path (MCP record/propose, the CLI, the DBOS submit
    # step) reaches this function; server-side writers use ensure_envelope.
    _refuse_reserved_producer_identity(
        deployment=deployment,
        producer_id=producer_id,
        producer_kind=producer_kind,
    )

    # NUL (U+0000) admission for the agent write-back (PACT
    # sanitize-fact-text-at-admission): agent JSON legally encodes NUL
    # as \u0000 and json decoding turns it into a real NUL, which
    # Postgres text/jsonb cannot store — pre-fix, `okg record` aborted
    # on the payload-ref insert (or, if stored, on the node_facts /
    # edge_facts insert at apply). Normalize BEFORE deriving the
    # idempotency key and BEFORE attach_payload_ref stores the
    # payload: identity-bearing fields reject loudly (never
    # rewritten); every other string value scrubs to U+FFFD with a
    # surfaced count.
    from okg.substrate.ingest.sanitize import sanitize_value

    _reject_submit_identity_nul(
        payload=payload,
        producer_id=producer_id,
        producer_kind=producer_kind,
        idempotency_key=idempotency_key,
        authority=authority,
    )
    # Image values (#3046): only okg's ingest image step adds `png` or
    # `error`, and only a connector's ingest run renders a `file`
    # reference. Refuse a forged or malformed value, or a `file` reference,
    # before anything is stored.
    check_review_payload_images(payload)
    payload, sanitized_value_count = sanitize_value(payload)
    intent, _n_intent = sanitize_value(intent)
    sanitized_value_count += _n_intent
    if authority is not None:
        authority, _n_authority = sanitize_value(authority)
        sanitized_value_count += _n_authority
    if check_summary is not None:
        check_summary, _n_check = sanitize_value(check_summary)
        sanitized_value_count += _n_check
    if sanitized_value_count:
        log.warning(
            "admission submit scrubbed %d NUL (U+0000) occurrence(s) "
            "(each replaced with U+FFFD) for producer=%s intent=%s "
            "(PACT sanitize-fact-text-at-admission)",
            sanitized_value_count,
            producer_id,
            intent,
        )

    # A caller-supplied connection already names its database; resolving
    # the manifest DSN here would refuse a deployment whose DSN comes from
    # the caller (the inline CLI path passes --dsn).
    dsn = None if connection is not None else _resolve_dsn(deployment)
    workflow_ref = workflow_ref_from_dbos(ADMISSION_SUBMIT_WORKFLOW_NAME)
    key = idempotency_key or stable_idempotency_key(
        "review",
        deployment,
        producer_id,
        intent,
        payload,
    )

    scope = (
        nullcontext(connection)
        if connection is not None
        else db_connect(dsn, autocommit=True)
    )
    with scope as conn:
        from okg.substrate.graph_store import ChronosGraphStore

        ChronosGraphStore.from_connection(conn)
        catalog_version_id = _current_catalog_version_id(conn)
        with conn.transaction():
            _refuse_registered_source_producer(
                conn,
                deployment=deployment,
                producer_id=producer_id,
                producer_kind=producer_kind,
            )
            policy_payload = policy_from_authority(
                authority,
                producer_id=producer_id,
                producer_kind=producer_kind,
            )
            if (
                str(policy_payload["producer_id"]) != producer_id
                or str(policy_payload["producer_kind"]) != producer_kind
            ):
                from okg.substrate.producer_authority import (
                    ProducerPolicyViolation,
                )

                raise ProducerPolicyViolation(
                    code="producer_policy_invalid",
                    message=(
                        "authority.producer_policy producer identity "
                        "must match the admission producer"
                    ),
                    verdict={"code": "producer_policy_invalid"},
                )
            profile = materialize_producer_profile(
                conn,
                deployment_name=deployment,
                producer_id=producer_id,
                producer_kind=producer_kind,
                trust_label=str(policy_payload["trust_label"]),
                admission_mode=str(policy_payload["admission_mode"]),
                authority_scope=policy_payload["authority_scope"],
                allowed_record_kinds=policy_payload["allowed_record_kinds"],
                output_signature=policy_payload["output_signature"],
                policy_clauses=policy_payload["policy_clauses"],
                output_scope_summary=policy_payload["output_scope_summary"],
                source_ref={"intent": intent, "workflow": "review"},
                catalog_version_id=catalog_version_id,
            )
            producer_policy_summary = {
                **profile.evidence(),
                **(
                    {"warnings": ["producer_signature_missing"]}
                    if profile.status == "compat"
                    else {}
                ),
            }
            authority_payload = {
                **dict(authority or {}),
                "producer_policy": producer_policy_summary,
            }
            check_summary_payload = {
                **dict(check_summary or {}),
                "producer_policy": producer_policy_summary,
            }
            if sanitized_value_count:
                # Durable loud-count: the envelope records how many
                # NUL occurrences were scrubbed from this submission
                # (PACT sanitize-fact-text-at-admission).
                check_summary_payload["sanitize"] = {
                    "sanitized_value_count": sanitized_value_count,
                }
            admission_mode = str(policy_payload["admission_mode"])
            initial_status = (
                "accepted" if admission_mode == "fast_track" else "pending"
            )
            envelope = ensure_envelope(
                conn,
                deployment_name=deployment,
                producer_id=producer_id,
                producer_kind=producer_kind,
                intent=intent,
                admission_mode=admission_mode,
                status=initial_status,
                idempotency_key=key,
                catalog_version_id=catalog_version_id,
                authority=authority_payload,
                check_summary=check_summary_payload,
                workflow_ref=workflow_ref,
                # Route the write onto the server-pinned session branch
                # (worktree-fork-writes P2). None for default writes -> column
                # stays NULL -> default admission path unchanged.
                target_branch_name=(authority or {}).get("target_branch_name"),
            )
            payload_ref_inserted = attach_payload_ref(
                conn,
                admission_id=envelope.admission_id,
                payload=payload,
                workflow_ref=workflow_ref,
            )
            if envelope.inserted:
                record_event(
                    conn,
                    admission_id=envelope.admission_id,
                    event_type="submitted",
                    actor=producer_id,
                    payload={"intent": intent},
                    workflow_ref=workflow_ref,
                )
            fast_track_result: dict[str, Any] | None = None
            if admission_mode == "fast_track" and envelope.inserted:
                current = _load_any_envelope(
                    conn,
                    deployment=deployment,
                    admission_id=envelope.admission_id,
                )
                try:
                    emitted = _emit_review_payload_facts(
                        conn,
                        envelope=current,
                        workflow_ref=workflow_ref,
                    )
                except _ReviewPayloadRejected as exc:
                    fast_track_result = transition_envelope(
                        conn,
                        admission_id=envelope.admission_id,
                        status="rejected",
                        actor=producer_id,
                        reason=exc.reason,
                        verdict=exc.verdict,
                        workflow_ref=workflow_ref,
                    )
                    fast_track_result["payload_rejected"] = True
                else:
                    refs = attach_fact_range_refs_for_run(
                        conn,
                        admission_id=envelope.admission_id,
                        source=emitted["source"],
                        run_id=emitted["run_id"],
                        workflow_ref=workflow_ref,
                    )
                    fast_track_result = transition_envelope(
                        conn,
                        admission_id=envelope.admission_id,
                        status="accepted",
                        actor=producer_id,
                        reason="fast_track",
                        verdict=merge_policy_evidence(
                            {},
                            {
                                "source": emitted["source"],
                                "run_id": emitted["run_id"],
                                "node_facts_inserted": (
                                    emitted["node_facts_inserted"]
                                ),
                                "edge_facts_inserted": (
                                    emitted["edge_facts_inserted"]
                                ),
                                "fact_ref_count": len(refs),
                            },
                            emitted.get("policy_evidence"),
                        ),
                        workflow_ref=workflow_ref,
                    )
                    fast_track_result["emitted"] = emitted
                    fast_track_result["fact_ref_count"] = len(refs)

    return {
        "deployment": deployment,
        "admission_id": envelope.admission_id,
        "status": (
            fast_track_result["status"]
            if fast_track_result is not None
            else envelope.status
        ),
        "admission_mode": admission_mode,
        "inserted": envelope.inserted,
        "payload_ref_inserted": payload_ref_inserted,
        "idempotency_key": key,
        **(
            {"sanitized_value_count": sanitized_value_count}
            if sanitized_value_count
            else {}
        ),
        **(
            {
                "emitted": fast_track_result["emitted"],
                "fact_ref_count": fast_track_result["fact_ref_count"],
            }
            if fast_track_result is not None
            and "emitted" in fast_track_result
            else {}
        ),
        **(
            {"payload_rejected": True}
            if fast_track_result is not None
            and fast_track_result.get("payload_rejected")
            else {}
        ),
    }


def _run_decide(
    *,
    deployment: str,
    admission_id: str,
    decision: str,
    actor: str,
    reason: str | None,
    verdict: dict | None,
    dsn: str | None = None,
    connection: Any | None = None,
) -> dict:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.admission import (
        transition_envelope,
        workflow_ref_from_dbos,
    )
    from okg.substrate.errors import SubstrateError
    from okg.substrate.producer_authority import merge_policy_evidence

    status_by_decision = {
        "accept": "accepted",
        "accepted": "accepted",
        "approve": "accepted",
        "approved": "accepted",
        "reject": "rejected",
        "rejected": "rejected",
        "quarantine": "quarantined",
        "quarantined": "quarantined",
    }
    status = status_by_decision.get(decision)
    if status is None:
        raise SubstrateError(
            f"unknown admission decision {decision!r}",
            data={"decision": decision},
            fix_hint="use accept, reject, or quarantine",
        )

    dsn = dsn or _resolve_dsn(deployment)
    workflow_ref = workflow_ref_from_dbos(ADMISSION_DECIDE_WORKFLOW_NAME)
    scope = (
        nullcontext(connection)
        if connection is not None
        else db_connect(dsn, autocommit=True)
    )
    with scope as conn:
        from okg.substrate.graph_store import ChronosGraphStore

        ChronosGraphStore.from_connection(conn)
        with conn.transaction():
            current = _load_review_envelope(
                conn,
                deployment=deployment,
                admission_id=admission_id,
            )
            if current.get("intent") == "source_sync" and current.get("source"):
                # Decisions on one source's held runs read each other's
                # status and markers (_retractions_still_standing), so they
                # run one at a time. Re-read the envelope once the lock is
                # held: a decision that committed while this one waited is
                # visible only to statements issued after it.
                _lock_source_sync_decisions(
                    conn,
                    deployment=deployment,
                    source=str(current["source"]),
                )
                current = _load_review_envelope(
                    conn,
                    deployment=deployment,
                    admission_id=admission_id,
                )
            nomos_refusal = _decide_producer_identity_refusal(
                conn,
                deployment=deployment,
                admission_id=admission_id,
                status=status,
                current=current,
            ) or _authorize_nomos_admission_decision(
                conn,
                deployment=deployment,
                admission_id=admission_id,
                actor=actor,
                decision=decision,
                status=status,
                current=current,
            )
            if nomos_refusal is not None:
                result = nomos_refusal
            elif status != "accepted" and current["status"] != "pending":
                result = {
                    "admission_id": admission_id,
                    "status": current["status"],
                    "target_generation_ref": (
                        current["target_generation_ref"]
                    ),
                    "already_decided": True,
                }
            elif status == "accepted":
                if current["status"] in {
                    "published", "blocked", "quarantined", "failed",
                }:
                    result = {
                        "admission_id": admission_id,
                        "status": current["status"],
                        "target_generation_ref": (
                            current["target_generation_ref"]
                        ),
                        "already_decided": True,
                    }
                elif current["status"] == "rejected":
                    result = {
                        "admission_id": admission_id,
                        "status": "rejected",
                        "target_generation_ref": (
                            current["target_generation_ref"]
                        ),
                        "already_decided": True,
                    }
                elif (
                    current["status"] == "accepted"
                    and current.get("intent") == "source_sync"
                ):
                    # Re-approve of an already-applied source run: the
                    # apply completed before the envelope flipped to
                    # accepted, so there is nothing to re-apply.
                    result = {
                        "admission_id": admission_id,
                        "status": "accepted",
                        "target_generation_ref": (
                            current["target_generation_ref"]
                        ),
                        "already_decided": True,
                    }
                else:
                    superseded_removals: dict[str, Any] | None = None
                    try:
                        if current.get("intent") == "source_sync":
                            # Chronos-native source runs: the facts
                            # were already written by the runner onto
                            # the quarantine branch. Approval applies
                            # them onto `default`
                            # (make-admission-review-load-bearing).
                            _refuse_if_run_retractions_may_have_expired(
                                conn,
                                envelope=current,
                            )
                            emitted = _apply_quarantined_source_run(
                                conn,
                                envelope=current,
                            )
                            # Kept out of the returned result: it is
                            # recorded only in the accepted verdict.
                            superseded_removals = emitted.pop(
                                "superseded_removals",
                                None,
                            )
                        else:
                            emitted = _emit_review_payload_facts(
                                conn,
                                envelope=current,
                                workflow_ref=workflow_ref,
                            )
                    except _ReviewPayloadRejected as exc:
                        result = transition_envelope(
                            conn,
                            admission_id=admission_id,
                            status="rejected",
                            actor=actor,
                            reason=exc.reason,
                            verdict=exc.verdict,
                            workflow_ref=workflow_ref,
                        )
                        result["payload_rejected"] = True
                    else:
                        from okg.substrate.admission import (
                            attach_fact_range_refs_for_run,
                        )

                        refs = attach_fact_range_refs_for_run(
                            conn,
                            admission_id=admission_id,
                            source=emitted["source"],
                            run_id=emitted["run_id"],
                            workflow_ref=workflow_ref,
                        )
                        if current.get("intent") == "source_sync":
                            pending = conn.execute(
                                '''SELECT authority_id::text,source,incarnation,attempt,
                                          expected_sha256,expected_records,
                                          expected_operations,expected_components
                                   FROM okg.source_record_authority_pending_admissions
                                   WHERE admission_id=%s::uuid FOR UPDATE''',
                                (admission_id,),
                            ).fetchone()
                            if pending is not None:
                                from okg.substrate.ingest.record_authority import (
                                    PrivateAuthorityCandidate,
                                    ProductionAuthorityStore,
                                )

                                candidate = PrivateAuthorityCandidate(*pending)
                                ProductionAuthorityStore(conn).seal_and_bind(candidate)
                                conn.execute(
                                    '''DELETE FROM okg.source_record_authority_pending_admissions
                                       WHERE admission_id=%s::uuid''',
                                    (admission_id,),
                                )
                            from okg.substrate.ingest.service import (
                                _persist_accepted_source_terminal_state,
                            )

                            _persist_accepted_source_terminal_state(
                                conn,
                                source=emitted["source"],
                                terminal_state=(
                                    current.get("verdict") or {}
                                ).get("source_terminal_state"),
                            )
                            # Issue #2993: a held source's run finishes as
                            # `pending`, so the run itself cannot lift a
                            # rerun-required block (kept partial rows after
                            # finish-undo --accept-current). Approving it
                            # puts the complete run onto `default`, which is
                            # what the block waits for; lift it in the same
                            # transaction.
                            from okg.substrate.ingest.run_base import (
                                clear_rerun_required,
                            )

                            clear_rerun_required(
                                conn,
                                deployment=deployment,
                                sources=sorted({
                                    str(emitted["source"]),
                                    str(current.get("source") or emitted["source"]),
                                }),
                            )
                        if current["status"] == "accepted":
                            result = {
                                "admission_id": admission_id,
                                "status": "accepted",
                                "target_generation_ref": (
                                    current["target_generation_ref"]
                                ),
                                "already_decided": True,
                            }
                        else:
                            result = transition_envelope(
                                conn,
                                admission_id=admission_id,
                                status="accepted",
                                actor=actor,
                                reason=reason,
                                verdict=merge_policy_evidence(
                                    dict(verdict or {}),
                                    {
                                        "source": emitted["source"],
                                        "run_id": emitted["run_id"],
                                        "node_facts_inserted": (
                                            emitted["node_facts_inserted"]
                                        ),
                                        "edge_facts_inserted": (
                                            emitted["edge_facts_inserted"]
                                        ),
                                        "fact_ref_count": len(refs),
                                        **(
                                            {
                                                "superseded_removals": (
                                                    superseded_removals
                                                ),
                                            }
                                            if superseded_removals
                                            else {}
                                        ),
                                    },
                                    emitted.get("policy_evidence"),
                                ),
                                workflow_ref=workflow_ref,
                            )
                        result["emitted"] = emitted
                        result["fact_ref_count"] = len(refs)
            else:
                if (
                    status == "rejected"
                    and current.get("intent") == "source_sync"
                ):
                    # Drop the held run from the quarantine branch so
                    # rejected content does not linger in the holding
                    # pen (it never touched `default`).
                    dropped = _drop_quarantined_source_run(
                        conn,
                        envelope=current,
                    )
                    # The private indexed-authority handoff dies with the
                    # decision. Leaving it would let a later admission with the
                    # same id bind an authority this decision refused. The
                    # staged authority itself is kept: retention and deletion
                    # belong to issues 1308 and 1309, not to this decision.
                    conn.execute(
                        "DELETE FROM "
                        "okg.source_record_authority_pending_admissions "
                        "WHERE admission_id=%s::uuid",
                        (admission_id,),
                    )
                else:
                    dropped = None
                result = transition_envelope(
                    conn,
                    admission_id=admission_id,
                    status=status,
                    actor=actor,
                    reason=reason,
                    verdict=verdict,
                    workflow_ref=workflow_ref,
                )
                if dropped is not None:
                    result["dropped"] = dropped
    result["deployment"] = deployment
    result["decision"] = decision
    return result


def _review_payload_write_source(envelope: dict[str, Any]) -> str:
    """The ``source`` that ``_emit_review_payload_facts`` stamps on its facts.

    The envelope's own source when it has one, else the producer id, else a
    per-admission name. The emitter and the Nomos admission check both call
    this, so the policy decides on exactly the value the accept writes.
    """
    return (
        envelope["source"]
        or envelope.get("producer_id")
        or f"admission:{envelope['admission_id']}"
    )


def _admission_write_source(envelope: dict[str, Any]) -> str | None:
    """The ``source`` an accept of this envelope writes, as Nomos sees it.

    A ``source_sync`` accept applies the held run under the envelope's stored
    source (``_apply_quarantined_source_run``), so that value is returned
    as-is, ``None`` included; every other accept goes through
    ``_emit_review_payload_facts``. ``source`` is a protected identity fact,
    so a ``None``, empty or blank result is refused by Nomos as
    ``nomos.invalid_identity_fact`` before any rule is read.
    """
    if envelope.get("intent") == "source_sync":
        return envelope.get("source")
    return _review_payload_write_source(envelope)


def _authorize_nomos_admission_decision(
    conn,
    *,
    deployment: str,
    admission_id: str,
    actor: str,
    decision: str,
    status: str,
    current: dict[str, Any],
) -> dict[str, Any] | None:
    if current.get("status") != "pending":
        return None
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_requires_nomos_runtime,
        evaluate_runtime_boundary_with_active_policy,
    )

    if not deployment_requires_nomos_runtime(deployment):
        return None
    base_resource = {
        "admission_id": admission_id,
        "decision": decision,
        "target_status": status,
        "current_status": current.get("status"),
        "intent": current.get("intent"),
        # The source the accept will write, not the stored column: an agent
        # submission has no stored source and is written under its producer
        # id, which the caller chooses, so a source-scoped rule must see it.
        "source": _admission_write_source(current),
        "producer_id": current.get("producer_id"),
        "producer_kind": current.get("producer_kind"),
        "run_id": current.get("run_id"),
        "admission_mode": current.get("admission_mode"),
        "target_generation_ref": current.get("target_generation_ref"),
    }
    boundaries = [("admin.override", "admission_admin_override")]
    if status == "accepted":
        boundaries.append(("quarantine.release", "admission_quarantine_release"))
    for surface, target_kind in boundaries:
        outcome = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name=deployment,
            scope=f"deployment:{deployment}",
            request=NomosRuntimeRequest(
                surface=surface,
                subject=actor,
                classification="restricted",
                resource=base_resource,
            ),
            target={
                "kind": target_kind,
                "admission_id": admission_id,
                "decision": decision,
            },
            performed_obligations=("audit",),
        )
        if outcome is not None and not outcome.allowed:
            return {
                "admission_id": admission_id,
                "status": "refused",
                "target_generation_ref": current.get("target_generation_ref"),
                "reason": f"nomos_{surface.replace('.', '_')}_denied",
                "nomos_decision": outcome.decision.to_dict(),
            }
    return None


class _ReviewPayloadRejected(Exception):
    def __init__(self, *, reason: str, verdict: dict[str, Any]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.verdict = verdict


def _review_payload_fact_candidates(payload: Any) -> list[Any]:
    """Every node or edge mapping a review payload names, in the three
    shapes `_raw_node_payloads` / `_raw_edge_payloads` read. Lenient: a
    malformed shape is left for normalization to reject."""
    if not isinstance(payload, dict):
        return []
    found: list[Any] = []
    for many, one, kind in (("nodes", "node", "node"), ("edges", "edge", "edge")):
        items = payload.get(many)
        if isinstance(items, list):
            found.extend(items)
        found.append(payload.get(one))
        if payload.get("kind") == kind:
            found.append(payload)
    return [item for item in found if isinstance(item, dict)]


def check_review_payload_images(payload: Any) -> None:
    """Refuse a review payload whose node or edge attrs carry a forged or
    malformed image value, or a ``file`` image reference (#3046).

    Raises `ImageValueForged` or `ImageRefInvalid`, the errors
    `okg.substrate.images.values.check_record_write` raises. Every submit
    path (MCP record and propose, the CLI, the DBOS submit step) is an agent
    record write and runs this before the payload is stored. A ``file``
    reference is refused because file images come only from a connector's
    ingest run; a ``url`` reference is accepted.

    The apply-time check (`_refuse_forged_image_values`) stays
    `check_incoming`: a payload attached by another writer -- the Poros
    bridge, whose export reduced each image value to its bare reference --
    may carry a bare ``file`` reference, which reads as not ingested.
    """
    for fact in _review_payload_fact_candidates(payload):
        check_record_write(fact.get("attrs"))


def _refuse_forged_image_values(
    fact: dict[str, Any],
    *,
    kind: str,
    payload_index: int,
    ordinal: int,
) -> None:
    """Apply-time image check on one normalized node or edge payload.

    Submit already refuses these; this covers a payload ref stored by
    another writer (Poros exchange) or before the check existed. The
    admission is rejected with the image error's code, and nothing is
    written.
    """
    try:
        check_incoming(fact["attrs"])
    except (ImageValueForged, ImageRefInvalid) as exc:
        raise _ReviewPayloadRejected(
            reason=exc.code,
            verdict={
                "code": exc.code,
                "kind": kind,
                "payload_index": payload_index,
                "ordinal": ordinal,
                "message": exc.message,
                "data": dict(exc.data),
            },
        ) from exc


def _load_review_envelope(
    conn,
    *,
    deployment: str,
    admission_id: str,
) -> dict[str, Any]:
    envelope = _load_any_envelope(
        conn,
        deployment=deployment,
        admission_id=admission_id,
    )
    if envelope["admission_mode"] not in {"review", "quarantine"}:
        from okg.substrate.errors import SubstrateError

        raise SubstrateError(
            "admission_decide only handles review/quarantine-mode "
            "envelopes",
            data={
                "admission_id": admission_id,
                "admission_mode": envelope["admission_mode"],
            },
        )
    return envelope


def _load_any_envelope(
    conn,
    *,
    deployment: str,
    admission_id: str,
) -> dict[str, Any]:
    from okg.substrate.errors import SubstrateError

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT admission_id::text, deployment_name, producer_id,
                   producer_kind, admission_mode, status,
                   catalog_version_id, source, run_id::text,
                   target_generation_ref, idempotency_key, created_at,
                   authority, check_summary, intent, target_branch_name,
                   verdict
              FROM okg.admission_envelopes
             WHERE admission_id = %s::uuid
            """,
            (admission_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise SubstrateError(
            "admission envelope not found",
            data={"admission_id": admission_id},
        )
    if str(row[1]) != deployment:
        raise SubstrateError(
            "admission envelope belongs to a different deployment",
            data={
                "admission_id": admission_id,
                "expected_deployment": deployment,
                "actual_deployment": str(row[1]),
            },
        )
    return {
        "admission_id": str(row[0]),
        "deployment_name": str(row[1]),
        "producer_id": str(row[2]),
        "producer_kind": str(row[3]),
        "admission_mode": str(row[4]),
        "status": str(row[5]),
        "catalog_version_id": int(row[6]) if row[6] is not None else None,
        "source": str(row[7]) if row[7] is not None else None,
        "run_id": str(row[8]) if row[8] is not None else None,
        "target_generation_ref": row[9],
        "idempotency_key": str(row[10]),
        "created_at": row[11],
        "authority": dict(row[12] or {}),
        "check_summary": dict(row[13] or {}),
        "intent": str(row[14]) if row[14] is not None else None,
        "target_branch_name": str(row[15]) if row[15] is not None else None,
        "verdict": dict(row[16] or {}),
    }


def _load_quarantined_run_rows(
    graph_conn,
    *,
    source: str,
    run_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Read one held source run's rows from the quarantine branch.

    Returns ``(node_rows, edge_rows)``; both empty when the
    quarantine branch does not exist (nothing was ever held).
    """
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(
        graph_conn,
        ensure_schema=False,
        autocommit=False,
    )
    try:
        graph = store.checkout(QUARANTINE_BRANCH)
    except Exception:  # noqa: BLE001 - no quarantine branch -> no held facts
        return [], []
    node_rows = graph.query(
        """
        SELECT node_id, subtype, attrs, source_record_id,
               source_revision, observed_at
          FROM okg.graph_nodes
         WHERE source = :source
           AND run_id = :run_id
        """,
        {"source": source, "run_id": run_id},
    )
    edge_rows = graph.query(
        """
        SELECT src, dst, edge_type, attrs, provenance, confidence,
               source_record_id, source_revision, observed_at
          FROM okg.graph_edges
         WHERE source = :source
           AND run_id = :run_id
        """,
        {"source": source, "run_id": run_id},
    )
    return list(node_rows), list(edge_rows)


def _load_run_retraction_facts(
    graph_conn,
    *,
    deployment: str,
    source: str,
    run_id: str,
) -> list[Any]:
    """Reconstruct a run's retraction facts (op='R') from the fact log.

    The quarantine branch no longer holds rows the run retracted (the
    runner already deleted them there), so the fact log is the only
    record of deletions to propagate onto `default` when a later run
    of an already-approved source is approved. The runner logs a held
    run's retractions even when the rest of the fact log is off
    (`SourceRunner._fact_log_rows_to_persist`, issue #2999). Returns []
    when the fact tables are absent.
    """
    import psycopg as _psycopg

    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    facts: list[Any] = []
    try:
        with graph_conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (node_id)
                       node_id, subtype, source_record_id, source_revision
                  FROM okg.node_facts
                 WHERE source = %s
                   AND run_id = %s::uuid
                   AND op = 'R'
                 ORDER BY node_id, fact_id DESC
                """,
                (source, run_id),
            )
            for node_id, subtype, record_id, revision in cur.fetchall():
                facts.append(NodeFact(
                    node_id=str(node_id),
                    subtype=str(subtype),
                    attrs={},
                    source_record_id=dict(record_id or {}),
                    source_revision=dict(revision or {}),
                    op="R",
                ))
            cur.execute(
                """
                SELECT DISTINCT ON (edge_id)
                       src, dst, edge_type, source_record_id,
                       source_revision
                  FROM okg.edge_facts
                 WHERE source = %s
                   AND run_id = %s::uuid
                   AND op = 'R'
                 ORDER BY edge_id, fact_id DESC
                """,
                (source, run_id),
            )
            for src, dst, edge_type, record_id, revision in cur.fetchall():
                facts.append(EdgeFact(
                    src=str(src),
                    dst=str(dst),
                    edge_type=str(edge_type),
                    attrs={},
                    source_record_id=dict(record_id or {}),
                    source_revision=dict(revision or {}),
                    op="R",
                ))
    except _psycopg.errors.UndefinedTable:
        graph_conn.rollback()
        return []
    return _retractions_still_standing(
        graph_conn,
        deployment=deployment,
        source=source,
        run_id=run_id,
        facts=facts,
    )


# Whether an envelope's rows are on `default`: accepted or published, or
# blocked after an acceptance. A blocked publish moves accepted envelopes to
# `blocked` (admission._mark_generation_blocked and the recovery variant) and
# a recovery can publish them later; the approval wrote their rows to
# `default` in its own transaction, so they stay there throughout. The
# `accepted` event is required for `blocked` because the recovery UPDATE also
# accepts `pending` envelopes; no current path hands it a pending held one
# (every candidate query excludes pending review/quarantine envelopes), but a
# held run blocked without an acceptance has its rows only on the quarantine
# branch. `failed` never counts: that status requires compensated receipts
# (admission._assert_fact_batch_disposition). Review round 5.
_APPLIED_TO_DEFAULT_SQL = """(
    e.status IN ('accepted', 'published')
    OR (
      e.status = 'blocked'
      AND EXISTS (
        SELECT 1 FROM okg.admission_events ev
         WHERE ev.admission_id = e.admission_id
           AND ev.event_type = 'accepted'
      )
    )
)"""


def _source_sync_decision_lock_key(deployment: str, source: str) -> str:
    """Advisory-lock key serializing decisions on one source's held runs."""
    return f"okg.admission.source_sync_decision\x1f{deployment}\x1f{source}"


def _lock_source_sync_decisions(
    conn,
    *,
    deployment: str,
    source: str,
) -> None:
    """Hold the per-source decision lock until the decision's transaction ends.

    Accepting one held run reads the status of the source's other held runs
    and the removals their approvals superseded. Under read committed, two
    decisions running at once each read the other as still pending: accepting
    run 2 could then delete a record that run 3's concurrent approval had just
    put back (issue #2999 review, round 4). Only decisions take this key, and
    they take it before any graph or branch lock, so it cannot deadlock with a
    source sync or the publisher.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (_source_sync_decision_lock_key(deployment, source),),
        )


def _removals_superseded_by_run(
    conn,
    *,
    deployment: str,
    source: str,
    run_id: str,
    node_ids: set[str],
    edge_keys: set[tuple[str, str, str]],
) -> dict[str, dict[str, list[Any]]]:
    """The pending removals that accepting ``run_id`` supersedes.

    ``node_ids`` / ``edge_keys`` are the run's live rows on the quarantine
    branch, the ones its approval writes to `default`. A still-pending held
    run of the same source that logged a removal of one of them removed it
    BEFORE this run wrote it: a removal after this run's write would have
    taken the row off the branch, and a later re-add would carry the re-adder's
    run_id, not this one. So the order comes from the branch itself, not from
    a clock.

    The result, ``{removing run_id: {"nodes": [...], "edges": [[src,
    edge_type, dst], ...]}}``, is stored in this run's accepted verdict, and
    accepting the removing run later skips exactly those removals
    (``_retractions_still_standing``). Without it the evidence is lost as soon
    as a later held run rewrites the row on the branch, and the default row
    cannot stand in for it: an unchanged upsert keeps the older run's run_id
    there (``_node_upsert_semantic_noop``).
    """
    import psycopg as _psycopg

    if not node_ids and not edge_keys:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT run_id::text
              FROM okg.admission_envelopes
             WHERE deployment_name = %s
               AND source = %s
               AND intent = 'source_sync'
               AND status = 'pending'
               AND run_id IS NOT NULL
               AND run_id <> %s::uuid
            """,
            (deployment, source, run_id),
        )
        pending_runs = sorted(str(row[0]) for row in cur.fetchall())
    if not pending_runs:
        return {}
    superseded: dict[str, dict[str, set[Any]]] = {}
    try:
        with conn.transaction(), conn.cursor() as cur:
            if node_ids:
                cur.execute(
                    """
                    SELECT DISTINCT run_id::text, node_id
                      FROM okg.node_facts
                     WHERE source = %s
                       AND op = 'R'
                       AND run_id = ANY(%s::uuid[])
                    """,
                    (source, pending_runs),
                )
                for removing_run, node_id in cur.fetchall():
                    if str(node_id) in node_ids:
                        superseded.setdefault(
                            str(removing_run), {"nodes": set(), "edges": set()},
                        )["nodes"].add(str(node_id))
            if edge_keys:
                cur.execute(
                    """
                    SELECT DISTINCT run_id::text, src, edge_type, dst
                      FROM okg.edge_facts
                     WHERE source = %s
                       AND op = 'R'
                       AND run_id = ANY(%s::uuid[])
                    """,
                    (source, pending_runs),
                )
                for removing_run, src, edge_type, dst in cur.fetchall():
                    key = (str(src), str(edge_type), str(dst))
                    if key in edge_keys:
                        superseded.setdefault(
                            str(removing_run), {"nodes": set(), "edges": set()},
                        )["edges"].add(key)
    except _psycopg.errors.UndefinedTable:
        # No fact log: no removal was logged, so none can be superseded.
        return {}
    return {
        removing_run: {
            "nodes": sorted(found["nodes"]),
            "edges": [list(key) for key in sorted(found["edges"])],
        }
        for removing_run, found in sorted(superseded.items())
    }


def _retractions_still_standing(
    graph_conn,
    *,
    deployment: str,
    source: str,
    run_id: str,
    facts: list[Any],
) -> list[Any]:
    """Drop the logged retractions of ``run_id`` that a newer accepted run undid.

    Held runs of one source apply to the quarantine branch in the order they
    ran, and operators may accept them in any order. When run 2 removed record
    4, run 3 added it back, and run 3 was accepted first, applying run 2's
    logged removal would delete a record the source still has (issue #2999
    review, round 2).

    A removal is skipped when either holds. "Accepted" here means the run's
    rows are on `default`: accepted, published, or blocked after an
    acceptance (``_APPLIED_TO_DEFAULT_SQL``).

    * An accepted (or published) run's verdict lists it under
      ``superseded_removals`` for ``run_id``: that run wrote the record to
      `default` after this run removed it (``_removals_superseded_by_run``,
      round 4). This still holds after a later held run rewrote the row on
      the branch; e.g. run 3 re-adds record 4 and is accepted, run 4 changes
      it and waits, and run 2 is accepted last.
    * ``source``'s live row for it on the quarantine branch was written by a
      run whose envelope (of any intent, for this source) is accepted or
      published. That row is on `default` and must stay. This also covers
      runs accepted before the verdict carried the markers.

    In every other case the removal applies (round 3):

    * The live row's writer is still PENDING and no accepted run superseded
      the removal. Accepting that run later re-upserts the row, so applying
      the removal now loses nothing; if that run is rejected instead,
      `default` is already without the record.
    * The writer is rejected or failed: its row does not belong on `default`.
    * The writer cannot be identified (no envelope for the row's run_id). The
      removal applies, and a warning names the rows. Applying keeps a record
      the source deleted from staying published, which is the failure this
      change exists to prevent; refusing would block the source's approvals
      with nothing the operator could do about it.

    Statuses and markers are read in the approval's own transaction, which
    holds the source's decision lock (``_lock_source_sync_decisions``). A row
    another source owns never holds the removal back.
    """
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    if not facts:
        return facts

    def _identity(fact: Any) -> Any:
        if isinstance(fact, NodeFact):
            return fact.node_id
        return (fact.src, fact.edge_type, fact.dst)

    superseded: set[Any] = set()
    with graph_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT e.verdict -> 'superseded_removals' -> %(run)s
              FROM okg.admission_envelopes e
             WHERE e.deployment_name = %(deployment)s
               AND e.source = %(source)s
               AND e.intent = 'source_sync'
               AND {_APPLIED_TO_DEFAULT_SQL}
               AND e.verdict -> 'superseded_removals' ? %(run)s
            """,
            {
                "run": str(run_id),
                "deployment": deployment,
                "source": source,
            },
        )
        for (marker,) in cur.fetchall():
            marker = dict(marker or {})
            superseded.update(str(node) for node in marker.get("nodes") or [])
            superseded.update(
                (str(key[0]), str(key[1]), str(key[2]))
                for key in marker.get("edges") or []
            )
    remaining = [fact for fact in facts if _identity(fact) not in superseded]
    if not remaining:
        return remaining

    node_ids = sorted({
        fact.node_id for fact in remaining if isinstance(fact, NodeFact)
    })
    edge_keys = {
        (fact.src, fact.edge_type, fact.dst)
        for fact in remaining
        if not isinstance(fact, NodeFact)
    }
    graph = ChronosGraphStore.from_connection(
        graph_conn,
        ensure_schema=False,
        autocommit=False,
    ).checkout(QUARANTINE_BRANCH)
    # Identity -> run_id of this source's live row on the quarantine branch.
    live_writers: dict[Any, str | None] = {}
    if node_ids:
        for row in graph.query(
            """
            SELECT node_id, run_id FROM okg.graph_nodes
             WHERE source = :source AND node_id = ANY(:node_ids)
            """,
            {"source": source, "node_ids": node_ids},
        ):
            live_writers[str(row["node_id"])] = (
                str(row["run_id"]) if row["run_id"] is not None else None
            )
    if edge_keys:
        for row in graph.query(
            """
            SELECT src, edge_type, dst, run_id FROM okg.graph_edges
             WHERE source = :source AND src = ANY(:srcs)
            """,
            {
                "source": source,
                "srcs": sorted({key[0] for key in edge_keys}),
            },
        ):
            key = (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
            if key in edge_keys:
                live_writers[key] = (
                    str(row["run_id"]) if row["run_id"] is not None else None
                )
    if not live_writers:
        return remaining
    writer_runs = sorted({run for run in live_writers.values() if run})
    # run_id -> whether any of its envelopes has its rows on `default`.
    applied_by_run: dict[str, bool] = {}
    if writer_runs:
        with graph_conn.cursor() as cur:
            # Any intent: an accepted operator or agent write under this
            # source name put its row on `default` too (round 4, note 3).
            cur.execute(
                f"""
                SELECT e.run_id::text, {_APPLIED_TO_DEFAULT_SQL}
                  FROM okg.admission_envelopes e
                 WHERE e.deployment_name = %s
                   AND e.source = %s
                   AND e.run_id::text = ANY(%s)
                """,
                (deployment, source, writer_runs),
            )
            for writer_run, applied in cur.fetchall():
                # Several envelopes for one run: an applied one wins.
                applied_by_run[str(writer_run)] = bool(
                    applied_by_run.get(str(writer_run)) or applied
                )
    unidentified = sorted(
        str(key)
        for key, run in live_writers.items()
        if run is None or run not in applied_by_run
    )
    if unidentified:
        log.warning(
            "held-run approval for source %s: %d live quarantine row(s) have "
            "no admission envelope for their run_id; their logged removals "
            "apply (first: %s)",
            source,
            len(unidentified),
            unidentified[0],
        )

    def _kept_by_accepted_writer(key: Any) -> bool:
        run = live_writers.get(key)
        return run is not None and applied_by_run.get(run, False)

    return [
        fact
        for fact in remaining
        if not _kept_by_accepted_writer(_identity(fact))
    ]


def _refuse_if_run_retractions_may_have_expired(
    conn,
    *,
    envelope: dict[str, Any],
) -> None:
    """Refuse to accept a held source run whose logged removals may be gone.

    Approval rebuilds a held run's removals from its op='R' fact-log rows,
    and the fact-log hypertables drop rows after their retention interval
    (180 days in schema.sql). Past that, the approval would find no removals
    and apply the run's upserts alone, so records the source deleted would
    stay published with nothing said. It refuses instead. A deployment
    without a retention policy on the fact log (no TimescaleDB, or the
    policy removed) keeps its rows, and nothing is refused.
    """
    import psycopg as _psycopg

    from okg.substrate.errors import SubstrateError

    created_at = envelope.get("created_at")
    if created_at is None:
        return
    try:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT min((config ->> 'drop_after')::interval)
                  FROM timescaledb_information.jobs
                 WHERE proc_name = 'policy_retention'
                   AND hypertable_schema = 'okg'
                   AND hypertable_name IN ('node_facts', 'edge_facts')
                """
            )
            drop_after = cur.fetchone()[0]
            if drop_after is None:
                return
            cur.execute(
                "SELECT %s::timestamptz < now() - %s::interval",
                (created_at, drop_after),
            )
            expired = bool(cur.fetchone()[0])
    except _psycopg.errors.UndefinedTable:
        return
    if expired:
        raise SubstrateError(
            "held source run is older than the fact-log retention "
            f"({drop_after}); the removals it recorded may have been "
            "dropped, so accepting it could leave deleted records published",
            data={
                "reason": "held_run_retractions_may_have_expired",
                "admission_id": envelope.get("admission_id"),
                "source": envelope.get("source"),
                "run_id": envelope.get("run_id"),
                "created_at": str(created_at),
                "retention": str(drop_after),
            },
            fix_hint=(
                "reject this run (okg admission decide <id> reject) and "
                "re-sync the source, then review the new run"
            ),
        )


def _apply_quarantined_source_run(
    conn,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Approve a held source_sync run: apply its quarantine-branch
    facts onto `default` (make-admission-review-load-bearing).

    Upserts come from the run's current rows on the quarantine
    branch; retractions are reconstructed from the run's op='R' fact
    log entries (so deletions in a re-synced, already-approved source
    propagate). Idempotent: `apply_facts` upserts by node/edge
    identity and retracting an absent row is a no-op.
    """
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.errors import SubstrateError
    from okg.substrate.fact_application import FactBatchApplication
    from okg.substrate.fact_batch import (
        FactBatchHeader,
        PostgresApplicationReceiptStore,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.producer_authority import load_profile_by_fingerprint

    source = envelope.get("source")
    run_id = envelope.get("run_id")
    if not source or not run_id:
        raise SubstrateError(
            "source_sync review envelope lacks source/run_id; "
            "cannot locate its quarantined facts",
            data={"admission_id": envelope.get("admission_id")},
        )
    node_rows, edge_rows = _load_quarantined_run_rows(
        conn,
        source=source,
        run_id=run_id,
    )
    facts: list[Any] = []
    observed_at = None
    for row in node_rows:
        observed_at = row.get("observed_at") or observed_at
        facts.append(NodeFact(
            node_id=str(row["node_id"]),
            subtype=str(row["subtype"]),
            attrs=dict(row["attrs"] or {}),
            source_record_id=dict(row["source_record_id"] or {}),
            source_revision=dict(row["source_revision"] or {}),
            op="I",
        ))
    for row in edge_rows:
        observed_at = row.get("observed_at") or observed_at
        facts.append(EdgeFact(
            src=str(row["src"]),
            dst=str(row["dst"]),
            edge_type=str(row["edge_type"]),
            attrs=dict(row["attrs"] or {}),
            provenance=str(row["provenance"] or "explicit"),
            confidence=float(row["confidence"] or 1.0),
            source_record_id=dict(row["source_record_id"] or {}),
            source_revision=dict(row["source_revision"] or {}),
            op="I",
        ))
    # Propagate the run's deletions. Skip any identity the run also
    # (re-)upserted — the current row wins over a stale R.
    held_node_ids = {str(row["node_id"]) for row in node_rows}
    held_edge_keys = {
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in edge_rows
    }
    # Removals of still-pending older runs that this run's rows undo; stored
    # in this run's accepted verdict so their later approval skips them.
    superseded_removals = _removals_superseded_by_run(
        conn,
        deployment=envelope["deployment_name"],
        source=source,
        run_id=run_id,
        node_ids=held_node_ids,
        edge_keys=held_edge_keys,
    )
    retractions = [
        fact
        for fact in _load_run_retraction_facts(
            conn,
            deployment=envelope["deployment_name"],
            source=source,
            run_id=run_id,
        )
        if (
            fact.node_id not in held_node_ids
            if isinstance(fact, NodeFact)
            else (fact.src, fact.edge_type, fact.dst) not in held_edge_keys
        )
    ]
    facts.extend(retractions)
    receipts: list[dict[str, Any]] = []
    if facts:
        policy_summary = dict(
            (envelope.get("check_summary") or {}).get("producer_policy") or {}
        )
        profile = load_profile_by_fingerprint(
            conn,
            deployment_name=envelope["deployment_name"],
            producer_id=envelope["producer_id"],
            profile_fingerprint=policy_summary.get("profile_fingerprint"),
        )
        promotion_run_id = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"okg:review-source-promotion:{envelope['admission_id']}",
        )
        header = FactBatchHeader(
            schema_version=1,
            run_id=promotion_run_id,
            admission_id=envelope["admission_id"],
            producer_profile_fingerprint=profile.profile_fingerprint,
            deployment=envelope["deployment_name"],
            target_branch="default",
            catalog_version_id=profile.catalog_version_id or 0,
            intent="source_review_promotion",
            idempotency_key=(
                f"source-review-promotion:{envelope['admission_id']}"
            ),
            requested_scope=profile.authority_scope or None,
        )
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        )

        def apply_review_facts(values, _header):
            return store.apply_facts(
                list(values),
                branch="default",
                source=source,
                run_id=run_id,
                observed_at=observed_at,
                strict_endpoints=False,
            )

        application = FactBatchApplication(
            header=header,
            profile=profile,
            receipt_store=PostgresApplicationReceiptStore(conn),
            apply_graph=apply_review_facts,
            retractions_allowed=True,
        )
        receipts = _apply_facts_in_bounded_chunks(
            application,
            run_id=promotion_run_id,
            facts=facts,
        )
        application.finish_observation(
            completion="delta",
            observed_scope=profile.output_scope_summary or None,
        )
    return {
        "source": source,
        "run_id": run_id,
        "node_facts_inserted": len(node_rows),
        "edge_facts_inserted": len(edge_rows),
        "retractions_applied": len(retractions),
        "superseded_removals": superseded_removals,
        "applied_from_branch": QUARANTINE_BRANCH,
        **_fact_batch_receipt_fields(receipts),
    }


def _apply_facts_in_bounded_chunks(
    application: Any,
    *,
    run_id: Any,
    facts: list[Any],
) -> list[dict[str, Any]]:
    """Apply ``facts`` as consecutive chunks of at most MAX_FACTS_PER_CHUNK.

    Issue #2994: an approval used to send every fact as ONE chunk, and
    ``FactChunk`` refuses a chunk over the limit, so a held run above it could
    never be approved. The slices keep the facts' order (node upserts, edge
    upserts, then retractions) and number their ordinals from 0.

    Nothing here commits. Both callers run inside one ``conn.transaction()``
    that also holds the receipts and the envelope transition: ``_run_decide``
    (an operator accepting a held source run or a review payload) and
    ``_run_submit``'s fast-track branch (a fast-track payload applied at
    submit). The receipt store's own transactions nest inside it as
    savepoints. A failure on any slice therefore rolls back the earlier
    slices too: the write stays all or nothing, as the single chunk was.
    Returns each slice's receipt, in ordinal order.
    """
    from okg.substrate.fact_application import _bounded_chunks
    from okg.substrate.fact_batch import MAX_FACTS_PER_CHUNK, FactChunk

    receipts: list[dict[str, Any]] = []
    for ordinal, chunk_facts in enumerate(
        _bounded_chunks(facts, MAX_FACTS_PER_CHUNK)
    ):
        applied = application.apply_chunk(
            FactChunk.create(
                run_id=run_id,
                ordinal=ordinal,
                facts=chunk_facts,
            )
        )
        receipts.append(applied.receipt.to_dict())
    return receipts


def _fact_batch_receipt_fields(
    receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    """The receipt fields of an apply result.

    One chunk (every approval up to MAX_FACTS_PER_CHUNK facts) keeps the
    shape it always had: ``fact_batch_receipt`` holds that chunk's receipt,
    or ``None`` when nothing was applied. Several chunks have no single
    receipt, so ``fact_batch_receipt`` is ``None`` and
    ``fact_batch_receipts`` lists every slice's receipt in ordinal order.
    """
    if len(receipts) <= 1:
        return {"fact_batch_receipt": receipts[0] if receipts else None}
    return {
        "fact_batch_receipt": None,
        "fact_batch_receipts": receipts,
        "fact_batch_chunk_count": len(receipts),
    }


def _drop_quarantined_source_run(
    conn,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Reject a held source_sync run: retract its rows from the
    quarantine branch. The run never touched `default`."""
    from okg.substrate.admission import QUARANTINE_BRANCH
    from okg.substrate.fact_batch import PostgresApplicationReceiptStore
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    source = envelope.get("source")
    run_id = envelope.get("run_id")
    if not source or not run_id:
        return {"nodes_dropped": 0, "edges_dropped": 0}
    node_rows, edge_rows = _load_quarantined_run_rows(
        conn,
        source=source,
        run_id=run_id,
    )
    facts: list[Any] = []
    for row in edge_rows:
        facts.append(EdgeFact(
            src=str(row["src"]),
            dst=str(row["dst"]),
            edge_type=str(row["edge_type"]),
            attrs=dict(row["attrs"] or {}),
            provenance=str(row["provenance"] or "explicit"),
            confidence=float(row["confidence"] or 1.0),
            source_record_id=dict(row["source_record_id"] or {}),
            source_revision=dict(row["source_revision"] or {}),
            op="R",
        ))
    for row in node_rows:
        facts.append(NodeFact(
            node_id=str(row["node_id"]),
            subtype=str(row["subtype"]),
            attrs={},
            source_record_id=dict(row["source_record_id"] or {}),
            source_revision=dict(row["source_revision"] or {}),
            op="R",
        ))
    receipt_store = PostgresApplicationReceiptStore(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id, chunk_ordinal
              FROM okg.fact_batch_receipts
             WHERE admission_id = %s::uuid
             ORDER BY run_id, chunk_ordinal
             FOR UPDATE
            """,
            (envelope["admission_id"],),
        )
        receipt_keys = list(cur.fetchall())
    for receipt_run_id, ordinal in receipt_keys:
        receipt = receipt_store.get(
            envelope["admission_id"],
            receipt_run_id,
            ordinal,
        )
        if receipt is not None and receipt.state == "applied":
            receipt_store.transition(receipt.key, "compensating")
    if facts:
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        )
        store.apply_facts(
            facts,
            branch=QUARANTINE_BRANCH,
            source=source,
            run_id=run_id,
        )
    # The rejected run's declarations of shared nodes it did not own have no
    # graph row to retract; delete them too (issue #1901).
    ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    ).withdraw_node_declarations(
        branch=QUARANTINE_BRANCH,
        sources=[source],
        run_id=run_id,
    )
    for receipt_run_id, ordinal in receipt_keys:
        receipt = receipt_store.get(
            envelope["admission_id"],
            receipt_run_id,
            ordinal,
        )
        if receipt is not None and receipt.state == "compensating":
            receipt_store.transition(receipt.key, "compensated")
    return {
        "nodes_dropped": len(node_rows),
        "edges_dropped": len(edge_rows),
        "branch": QUARANTINE_BRANCH,
    }


# Transcript node subtypes and id namespaces are populated ONLY by the
# agent-sessions ingest source (which writes via apply_facts and never reaches
# this admission path). An admission-path write (record / propose / `okg
# admission decide`) that authored one of these would collide with -- but NOT
# dedup against -- the source-ingested node: the admission emitter stamps a
# different `source` (the producer_id), so the ON CONFLICT (dedupe_key,
# observed_at) DO NOTHING guard does not collapse the pair, double-counting the
# transcript. `record` already re-mints a forbidden-prefix caller node_id, but
# `propose` passes its payload verbatim -- so reject here, the one choke point
# every admission write flows through.
_TRANSCRIPT_NODE_SUBTYPES = frozenset({
    "agent_session",
    "sub_agent_session",
    "agent_message",
    "tool_use_event",
    "agent_chapter",
})
_TRANSCRIPT_ID_PREFIXES = ("message:", "tool_use:", "session:", "subsession:")


def _reject_agent_authored_transcripts(nodes: list[dict[str, Any]]) -> None:
    """Refuse an admission write that hand-mints a transcript-typed node.

    Transcript subtypes / id namespaces are source-only; authoring one on the
    admission path double-counts the corresponding source-ingested node. Raises
    _ReviewPayloadRejected naming the offenders.
    """
    offenders = [
        {
            "node_id": str(node.get("node_id") or ""),
            "subtype": str(node.get("subtype") or ""),
        }
        for node in nodes
        if str(node.get("subtype") or "") in _TRANSCRIPT_NODE_SUBTYPES
        or str(node.get("node_id") or "").startswith(_TRANSCRIPT_ID_PREFIXES)
    ]
    if offenders:
        raise _ReviewPayloadRejected(
            reason="agent_authored_transcript_subtype",
            verdict={
                "code": "agent_authored_transcript_subtype",
                "message": (
                    "transcript nodes (agent_session / agent_message / "
                    "tool_use_event / session: / message: / tool_use: ...) are "
                    "populated only by the agent-sessions ingest source; an "
                    "admission-path write may not author them (double-count "
                    "guard)"
                ),
                "offending_nodes": offenders[:10],
            },
        )


def _emit_review_payload_facts(
    conn,
    *,
    envelope: dict[str, Any],
    workflow_ref: dict[str, Any],
) -> dict[str, Any]:
    from psycopg.types.json import Jsonb
    from okg.substrate.fact_application import (
        FactBatchApplication,
        chronos_apply_callback,
    )
    from okg.substrate.fact_batch import (
        FactBatchHeader,
        PostgresApplicationReceiptStore,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.producer_authority import (
        ProducerPolicyViolation,
        evaluate_payload_signature,
        load_profile_by_fingerprint,
    )

    admission_id = envelope["admission_id"]
    source = _review_payload_write_source(envelope)
    run_id = envelope["run_id"] or str(uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"okg:admission-review:{admission_id}:"
        f"{envelope['idempotency_key']}",
    ))
    observed_at = envelope["created_at"]
    payloads = _load_payload_refs(conn, admission_id=admission_id)
    normalized, sanitized_value_count = _normalize_review_payloads(
        payloads,
        admission_id=admission_id,
    )
    if sanitized_value_count:
        # Normally zero: submit scrubs before storing the payload ref.
        # Nonzero means a pre-fix or externally written payload ref
        # carried NUL — scrubbed here so the fact inserts cannot abort
        # (PACT sanitize-fact-text-at-admission).
        log.warning(
            "admission apply scrubbed %d NUL (U+0000) occurrence(s) "
            "(each replaced with U+FFFD) for admission_id=%s "
            "source=%s",
            sanitized_value_count,
            admission_id,
            source,
        )
    policy_summary = dict(
        (envelope.get("check_summary") or {}).get("producer_policy") or {},
    )
    try:
        profile = load_profile_by_fingerprint(
            conn,
            deployment_name=envelope["deployment_name"],
            producer_id=envelope["producer_id"],
            profile_fingerprint=policy_summary.get("profile_fingerprint"),
        )
        policy_evidence = evaluate_payload_signature(
            profile=profile,
            nodes=normalized["nodes"],
            edges=normalized["edges"],
        )
    except ProducerPolicyViolation as exc:
        raise _ReviewPayloadRejected(
            reason=exc.code,
            verdict=exc.verdict,
        ) from exc
    _reject_agent_authored_transcripts(normalized["nodes"])
    _validate_review_edge_narrowings(
        conn,
        catalog_version_id=envelope["catalog_version_id"],
        nodes=normalized["nodes"],
        edges=normalized["edges"],
    )

    from okg.substrate.admission import source_mutation_transaction

    with source_mutation_transaction(conn, admission_id=admission_id):
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.admission_envelopes
                   SET source = COALESCE(source, %(source)s),
                       run_id = COALESCE(run_id, %(run_id)s::uuid),
                       workflow_ref = CASE
                         WHEN %(workflow_ref)s::jsonb = '{}'::jsonb
                         THEN workflow_ref
                         ELSE %(workflow_ref)s::jsonb
                       END,
                       updated_at = now()
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {
                    "admission_id": admission_id,
                    "source": source,
                    "run_id": run_id,
                    "workflow_ref": Jsonb(dict(workflow_ref or {})),
                },
            )

    # Commit-witness (PACT route-all-writers, task 2): ledger the admission program
    # and pin its checksum into the per-tx GUC the fact-table column default reads,
    # so the node_facts/edge_facts rows below auto-stamp source_program_checksum.
    persist_transform_program(conn, _ADMISSION_PROGRAM)
    with conn.cursor() as _wcur:
        _wcur.execute(
            "SELECT set_config('okg.source_program_checksum', %s, true)",
            (_ADMISSION_CHECKSUM,),
        )

    node_inserted = 0
    edge_inserted = 0
    for node in normalized["nodes"]:
        dedupe = _node_dedupe_key(
            source,
            node["node_id"],
            node["source_revision"],
            node["op"],
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES
                  (%(node_id)s, %(subtype)s, %(attrs)s, %(source)s,
                   %(source_record_id)s, %(source_revision)s,
                   %(observed_at)s, %(dedupe_key)s, %(run_id)s::uuid,
                   %(op)s)
                ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                RETURNING fact_id
                """,
                {
                    "node_id": node["node_id"],
                    "subtype": node["subtype"],
                    "attrs": Jsonb(node["attrs"]),
                    "source": source,
                    "source_record_id": Jsonb(node["source_record_id"]),
                    "source_revision": Jsonb(node["source_revision"]),
                    "observed_at": observed_at,
                    "dedupe_key": dedupe,
                    "run_id": run_id,
                    "op": node["op"],
                },
            )
            if cur.fetchone() is not None:
                node_inserted += 1

    for edge in normalized["edges"]:
        dedupe = _edge_dedupe_key(
            source,
            edge["src"],
            edge["edge_type"],
            edge["dst"],
            edge["source_revision"],
            edge["op"],
        )
        edge_id = _mint_edge_id(edge["src"], edge["edge_type"], edge["dst"])
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs, provenance,
                   confidence, source, source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES
                  (%(edge_id)s, %(src)s, %(dst)s, %(edge_type)s,
                   %(attrs)s, %(provenance)s, %(confidence)s, %(source)s,
                   %(source_record_id)s, %(source_revision)s,
                   %(observed_at)s, %(dedupe_key)s, %(run_id)s::uuid,
                   %(op)s)
                ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                RETURNING fact_id
                """,
                {
                    "edge_id": edge_id,
                    "src": edge["src"],
                    "dst": edge["dst"],
                    "edge_type": edge["edge_type"],
                    "attrs": Jsonb(edge["attrs"]),
                    "provenance": edge["provenance"],
                    "confidence": edge["confidence"],
                    "source": source,
                    "source_record_id": Jsonb(edge["source_record_id"]),
                    "source_revision": Jsonb(edge["source_revision"]),
                    "observed_at": observed_at,
                    "dedupe_key": dedupe,
                    "run_id": run_id,
                    "op": edge["op"],
                },
            )
            if cur.fetchone() is not None:
                edge_inserted += 1

    # The graph effect, optional audit rows, receipt, and envelope disposition
    # are one admission transaction. Chronos must therefore share ``conn`` and
    # must not autocommit its primitive operations. A fresh/autocommit graph
    # connection would allow a rollback of the receipt/envelope to strand a
    # durable, publishable graph effect.
    target_branch = envelope.get("target_branch_name") or "default"
    # Promote-intent (worktree-fork-writes req.promote-intent): stamp
    # promote_intent="durable" onto a FORK write the agent explicitly marked
    # durable (the server normalized it onto the envelope authority, fork-only).
    # Deny-by-default: scratch facts (the default, and every default-branch write)
    # carry NO marker, so the P3 merge-back scopes to attrs->>'promote_intent'
    # ='durable' only. Stamped before apply_facts so it lands on the fork's chronos
    # graph (what merge-back reads), not just the admission ledger.
    promote_intent = str(
        (envelope.get("authority") or {}).get("promote_intent") or ""
    ).strip().lower()
    stamp_durable = target_branch != "default" and promote_intent == "durable"

    def _with_intent(attrs: dict[str, Any] | None) -> dict[str, Any]:
        return {**(attrs or {}), "promote_intent": "durable"}

    graph_store = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=False,
    )
    batch_facts = [
            *[
                NodeFact(
                    node["node_id"],
                    node["subtype"],
                    (_with_intent(node["attrs"]) if stamp_durable else node["attrs"]),
                    node["source_record_id"],
                    node["source_revision"],
                    op=node["op"],
                )
                for node in normalized["nodes"]
            ],
            *[
                EdgeFact(
                    edge["src"],
                    edge["dst"],
                    edge["edge_type"],
                    attrs=(
                        _with_intent(edge["attrs"]) if stamp_durable else edge["attrs"]
                    ),
                    provenance=edge["provenance"],
                    confidence=edge["confidence"],
                    source_record_id=edge["source_record_id"],
                    source_revision=edge["source_revision"],
                    op=edge["op"],
                )
                for edge in normalized["edges"]
            ],
    ]
    header = FactBatchHeader(
        schema_version=1,
        run_id=run_id,
        admission_id=admission_id,
        producer_profile_fingerprint=profile.profile_fingerprint,
        deployment=envelope["deployment_name"],
        target_branch=target_branch,
        catalog_version_id=profile.catalog_version_id or 0,
        intent=envelope["intent"],
        idempotency_key=envelope["idempotency_key"],
        requested_scope=profile.authority_scope or None,
    )
    application = FactBatchApplication(
        header=header,
        profile=profile,
        receipt_store=PostgresApplicationReceiptStore(conn),
        apply_graph=chronos_apply_callback(
            graph_store,
            source=source,
            observed_at=observed_at,
        ),
        # Generic review payloads never own source retraction authority.
        # SourceRunner is the server-resolved path for retractions.
        retractions_allowed=False,
    )
    # Issue #2994: sliced like a held source run. Two callers reach here: an
    # operator accepting a review payload (bounded only by an auto-approver's
    # max_nodes/max_edges, so any size) and `_run_submit`'s fast-track
    # branch, which on dev refused a payload above MAX_FACTS_PER_CHUNK and
    # now applies it in slices, still inside the submit transaction.
    batch_receipts = _apply_facts_in_bounded_chunks(
        application,
        run_id=run_id,
        facts=batch_facts,
    )
    application.finish_observation(
        completion="delta",
        observed_scope=profile.output_scope_summary or None,
    )

    return {
        "source": source,
        "run_id": run_id,
        "node_count": len(normalized["nodes"]),
        "edge_count": len(normalized["edges"]),
        "node_facts_inserted": node_inserted,
        "edge_facts_inserted": edge_inserted,
        **_fact_batch_receipt_fields(batch_receipts),
        "policy_evidence": policy_evidence,
        **(
            {"sanitized_value_count": sanitized_value_count}
            if sanitized_value_count
            else {}
        ),
    }


def _load_payload_refs(conn, *, admission_id: str) -> list[dict[str, Any]]:
    from okg.substrate.errors import SubstrateError

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT payload
              FROM okg.admission_record_refs
             WHERE admission_id = %s::uuid
               AND ref_kind = 'payload'
             ORDER BY ref_id
            """,
            (admission_id,),
        )
        payloads = [dict(row[0]) for row in cur.fetchall()]
    if not payloads:
        raise SubstrateError(
            "review admission has no payload ref to accept",
            data={"admission_id": admission_id},
        )
    return payloads


def _normalize_review_payloads(
    payloads: list[dict[str, Any]],
    *,
    admission_id: str,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    """Normalize raw payloads into insertable node/edge dicts.

    Returns ``(normalized, sanitized_value_count)``. Every normalized
    fact passes the shared NUL admission contract
    (``_sanitize_normalized_fact``) before it can reach the
    node_facts/edge_facts inserts or the chronos apply — submit-time
    scrubbing normally makes this a no-op, but payload refs stored
    before that fix (or by other writers) are still defended here.
    """
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    sanitized_value_count = 0
    for payload_index, payload in enumerate(payloads):
        payload_hash = _json_sha256(payload)
        for ordinal, raw in enumerate(_raw_node_payloads(payload)):
            node = _normalize_node_payload(
                raw,
                admission_id=admission_id,
                payload_hash=payload_hash,
                payload_index=payload_index,
                ordinal=ordinal,
            )
            _refuse_forged_image_values(
                node,
                kind="node",
                payload_index=payload_index,
                ordinal=ordinal,
            )
            sanitized_value_count += _sanitize_normalized_fact(
                node,
                kind="node",
                payload_index=payload_index,
                ordinal=ordinal,
            )
            nodes.append(node)
        for ordinal, raw in enumerate(_raw_edge_payloads(payload)):
            edge = _normalize_edge_payload(
                raw,
                admission_id=admission_id,
                payload_hash=payload_hash,
                payload_index=payload_index,
                ordinal=ordinal,
            )
            _refuse_forged_image_values(
                edge,
                kind="edge",
                payload_index=payload_index,
                ordinal=ordinal,
            )
            sanitized_value_count += _sanitize_normalized_fact(
                edge,
                kind="edge",
                payload_index=payload_index,
                ordinal=ordinal,
            )
            edges.append(edge)
    if not nodes and not edges:
        raise _ReviewPayloadRejected(
            reason="empty_payload",
            verdict={
                "code": "empty_payload",
                "message": (
                    "review admission payload must contain at least "
                    "one node or edge"
                ),
            },
        )
    return {"nodes": nodes, "edges": edges}, sanitized_value_count


def _sanitize_normalized_fact(
    fact: dict[str, Any],
    *,
    kind: str,
    payload_index: int,
    ordinal: int,
) -> int:
    """NUL (U+0000) admission on one normalized node/edge payload.

    Per PACT sanitize-fact-text-at-admission: a NUL in an
    identity-bearing field — or in edge ``provenance``, a closed
    vocabulary where a scrubbed value could never be valid — rejects
    the payload loudly (never silently rewritten); attr values scrub
    to U+FFFD. Mutates ``fact`` in place and returns the replacement
    count.
    """
    from okg.substrate.ingest.sanitize import (
        identity_nul_fields_from,
        sanitize_value,
    )

    if kind == "node":
        bad = identity_nul_fields_from(
            node_id=fact["node_id"],
            subtype=fact["subtype"],
            source_record_id=fact["source_record_id"],
            source_revision=fact["source_revision"],
            op=fact["op"],
        )
    else:
        bad = identity_nul_fields_from(
            src=fact["src"],
            dst=fact["dst"],
            edge_type=fact["edge_type"],
            source_record_id=fact["source_record_id"],
            source_revision=fact["source_revision"],
            op=fact["op"],
            src_subtype=fact.get("src_subtype"),
            dst_subtype=fact.get("dst_subtype"),
            provenance=fact["provenance"],
        )
    if bad:
        raise _ReviewPayloadRejected(
            reason="nul_in_identity",
            verdict={
                "code": "nul_in_identity",
                "kind": kind,
                "payload_index": payload_index,
                "ordinal": ordinal,
                "fields": list(bad),
                "message": (
                    "identity-bearing fields are never rewritten; "
                    "remove the NUL (U+0000) and resubmit (PACT "
                    "sanitize-fact-text-at-admission)"
                ),
            },
        )
    fact["attrs"], count = sanitize_value(fact["attrs"])
    return count


def _raw_node_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_nodes: list[dict[str, Any]] = []
    if "nodes" in payload:
        value = payload["nodes"]
        if not isinstance(value, list):
            raise _ReviewPayloadRejected(
                reason="invalid_payload",
                verdict={"code": "invalid_payload", "field": "nodes"},
            )
        raw_nodes.extend(_as_dict_items(value, field="nodes"))
    if "node" in payload:
        node = payload["node"]
        if not isinstance(node, dict):
            raise _ReviewPayloadRejected(
                reason="invalid_payload",
                verdict={"code": "invalid_payload", "field": "node"},
            )
        raw_nodes.append(dict(node))
    if payload.get("kind") == "node":
        raw_nodes.append({
            key: value for key, value in payload.items()
            if key != "kind"
        })
    return raw_nodes


def _raw_edge_payloads(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_edges: list[dict[str, Any]] = []
    if "edges" in payload:
        value = payload["edges"]
        if not isinstance(value, list):
            raise _ReviewPayloadRejected(
                reason="invalid_payload",
                verdict={"code": "invalid_payload", "field": "edges"},
            )
        raw_edges.extend(_as_dict_items(value, field="edges"))
    if "edge" in payload:
        edge = payload["edge"]
        if not isinstance(edge, dict):
            raise _ReviewPayloadRejected(
                reason="invalid_payload",
                verdict={"code": "invalid_payload", "field": "edge"},
            )
        raw_edges.append(dict(edge))
    if payload.get("kind") == "edge":
        raw_edges.append({
            key: value for key, value in payload.items()
            if key != "kind"
        })
    return raw_edges


def _as_dict_items(value: list[Any], *, field: str) -> list[dict[str, Any]]:
    out = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise _ReviewPayloadRejected(
                reason="invalid_payload",
                verdict={
                    "code": "invalid_payload",
                    "field": f"{field}[{idx}]",
                },
            )
        out.append(dict(item))
    return out


def _normalize_node_payload(
    raw: dict[str, Any],
    *,
    admission_id: str,
    payload_hash: str,
    payload_index: int,
    ordinal: int,
) -> dict[str, Any]:
    node_id = _required_str(raw, "node_id")
    subtype = _required_str(raw, "subtype")
    attrs = _optional_dict(raw, "attrs")
    attrs.pop("promote_intent", None)  # server-only marker; strip caller value (req.promote-intent)
    op = str(raw.get("op") or "I")
    if op == "R":
        raise _ReviewPayloadRejected(
            reason="retraction_not_permitted",
            verdict={
                "code": "retraction_not_permitted",
                "message": (
                    "generic admission payloads cannot self-authorize "
                    "retractions; use a server-resolved source workflow"
                ),
            },
        )
    if op != "I":
        raise _ReviewPayloadRejected(
            reason="invalid_payload",
            verdict={"code": "invalid_payload", "field": "op", "op": op},
        )
    source_record_id = _optional_dict(raw, "source_record_id")
    source_record_id.setdefault("admission_id", admission_id)
    source_record_id.setdefault("node_id", node_id)
    source_record_id.setdefault("payload_index", payload_index)
    source_record_id.setdefault("ordinal", ordinal)
    source_revision = _optional_dict(raw, "source_revision")
    source_revision["admission_id"] = admission_id
    source_revision["payload_sha256"] = payload_hash
    source_revision["ordinal"] = ordinal
    return {
        "node_id": node_id,
        "subtype": subtype,
        "attrs": attrs,
        "source_record_id": source_record_id,
        "source_revision": source_revision,
        "op": op,
    }


def _normalize_edge_payload(
    raw: dict[str, Any],
    *,
    admission_id: str,
    payload_hash: str,
    payload_index: int,
    ordinal: int,
) -> dict[str, Any]:
    src = _required_str(raw, "src")
    dst = _required_str(raw, "dst")
    edge_type = _required_str(raw, "edge_type")
    attrs = _optional_dict(raw, "attrs")
    attrs.pop("promote_intent", None)  # server-only marker; strip caller value (req.promote-intent)
    provenance = str(raw.get("provenance") or "explicit")
    confidence = float(raw.get("confidence", 1.0))
    if not 0 <= confidence <= 1:
        raise _ReviewPayloadRejected(
            reason="invalid_payload",
            verdict={
                "code": "invalid_payload",
                "field": "confidence",
                "confidence": confidence,
            },
        )
    op = str(raw.get("op") or "I")
    if op == "R":
        raise _ReviewPayloadRejected(
            reason="retraction_not_permitted",
            verdict={
                "code": "retraction_not_permitted",
                "message": (
                    "generic admission payloads cannot self-authorize "
                    "retractions; use a server-resolved source workflow"
                ),
            },
        )
    if op != "I":
        raise _ReviewPayloadRejected(
            reason="invalid_payload",
            verdict={"code": "invalid_payload", "field": "op", "op": op},
        )
    source_record_id = _optional_dict(raw, "source_record_id")
    source_record_id.setdefault("admission_id", admission_id)
    source_record_id.setdefault("src", src)
    source_record_id.setdefault("dst", dst)
    source_record_id.setdefault("edge_type", edge_type)
    source_record_id.setdefault("payload_index", payload_index)
    source_record_id.setdefault("ordinal", ordinal)
    source_revision = _optional_dict(raw, "source_revision")
    source_revision["admission_id"] = admission_id
    source_revision["payload_sha256"] = payload_hash
    source_revision["ordinal"] = ordinal
    return {
        "src": src,
        "dst": dst,
        "edge_type": edge_type,
        "attrs": attrs,
        "provenance": provenance,
        "confidence": confidence,
        "source_record_id": source_record_id,
        "source_revision": source_revision,
        "op": op,
        "src_subtype": raw.get("src_subtype"),
        "dst_subtype": raw.get("dst_subtype"),
    }


def _required_str(raw: dict[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _ReviewPayloadRejected(
            reason="invalid_payload",
            verdict={"code": "invalid_payload", "field": field},
        )
    return value


def _optional_dict(raw: dict[str, Any], field: str) -> dict[str, Any]:
    value = raw.get(field) or {}
    if not isinstance(value, dict):
        raise _ReviewPayloadRejected(
            reason="invalid_payload",
            verdict={"code": "invalid_payload", "field": field},
        )
    return dict(value)


def _validate_review_edge_narrowings(
    conn,
    *,
    catalog_version_id: int | None,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> None:
    if not edges:
        return
    cv = catalog_version_id or _current_catalog_version_id(conn)
    if cv is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.narrowings
             WHERE catalog_version_id = %s
            """,
            (cv,),
        )
        narrowing_count = int(cur.fetchone()[0] or 0)
    if narrowing_count == 0:
        return

    endpoint_subtypes = {
        str(node["node_id"]): str(node["subtype"])
        for node in nodes
        if node["op"] == "I"
    }
    endpoint_ids = sorted({
        edge["src"] for edge in edges
    } | {
        edge["dst"] for edge in edges
    })
    missing = [
        node_id for node_id in endpoint_ids
        if node_id not in endpoint_subtypes
    ]
    if missing:
        from okg.substrate.db.connection import connect as db_connect
        from psycopg.conninfo import make_conninfo

        from okg.substrate.graph_store import ChronosGraphStore

        # Chronos cutover: the checkout's `.query(...)` runs in autocommit
        # mode and calls `conn.commit()`, which psycopg forbids while `conn`
        # is inside the open `_run_decide` transaction. Resolve the missing
        # endpoint subtypes over a separate, short-lived read connection so
        # the outer transaction is untouched. `conn.info.dsn` redacts the
        # password, so reattach it from `conn.info.password` to reconnect.
        read_dsn = make_conninfo(conn.info.dsn, password=conn.info.password)
        with db_connect(read_dsn, autocommit=True) as read_conn:
            graph = ChronosGraphStore.from_connection(
                read_conn, ensure_schema=False
            ).checkout()
            endpoint_subtypes.update({
                str(row["node_id"]): str(row["subtype"])
                for row in graph.query(
                    """
                    SELECT node_id, subtype
                      FROM okg.graph_nodes
                     WHERE node_id = ANY(:node_ids)
                    """,
                    {"node_ids": missing},
                )
            })

    for edge in edges:
        if edge["op"] != "I":
            continue
        src_subtype = edge.get("src_subtype") or endpoint_subtypes.get(
            edge["src"],
        )
        dst_subtype = edge.get("dst_subtype") or endpoint_subtypes.get(
            edge["dst"],
        )
        if not src_subtype or not dst_subtype:
            raise _ReviewPayloadRejected(
                reason="narrowing_violation",
                verdict={
                    "code": "narrowing_violation",
                    "message": (
                        "edge endpoint subtype is unknown; include "
                        "the endpoint node in the payload or provide "
                        "src_subtype/dst_subtype"
                    ),
                    "edge": {
                        "src": edge["src"],
                        "edge_type": edge["edge_type"],
                        "dst": edge["dst"],
                    },
                    "catalog_version_id": cv,
                },
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                      FROM okg.narrowings n
                      JOIN okg.subtype_ancestors sa_src
                        ON sa_src.catalog_version_id = n.catalog_version_id
                       AND sa_src.subtype_name = %s
                       AND sa_src.ancestor_name = n.parent_subtype
                      JOIN okg.subtype_ancestors sa_dst
                        ON sa_dst.catalog_version_id = n.catalog_version_id
                       AND sa_dst.subtype_name = %s
                       AND sa_dst.ancestor_name = n.child_subtype
                     WHERE n.catalog_version_id = %s
                       AND n.edge_type = %s
                )
                """,
                (src_subtype, dst_subtype, cv, edge["edge_type"]),
            )
            allowed = bool(cur.fetchone()[0])
        if not allowed:
            raise _ReviewPayloadRejected(
                reason="narrowing_violation",
                verdict={
                    "code": "narrowing_violation",
                    "edge": {
                        "src": edge["src"],
                        "src_subtype": src_subtype,
                        "edge_type": edge["edge_type"],
                        "dst": edge["dst"],
                        "dst_subtype": dst_subtype,
                    },
                    "catalog_version_id": cv,
                },
            )


def _node_dedupe_key(
    source: str,
    node_id: str,
    source_revision: dict[str, Any],
    op: str,
) -> str:
    revision_bytes = json.dumps(
        source_revision, sort_keys=True, default=str,
    ).encode("utf-8")
    return (
        f"{source}:n:"
        f"{uuid.uuid5(uuid.NAMESPACE_URL, node_id + '|' + revision_bytes.hex()).hex}"
        f":{op}"
    )


def _edge_dedupe_key(
    source: str,
    src: str,
    edge_type: str,
    dst: str,
    source_revision: dict[str, Any],
    op: str,
) -> str:
    revision_bytes = json.dumps(
        source_revision, sort_keys=True, default=str,
    ).encode("utf-8")
    return (
        f"{source}:e:"
        f"{uuid.uuid5(uuid.NAMESPACE_URL, f'{src}|{edge_type}|{dst}|' + revision_bytes.hex()).hex}"
        f":{op}"
    )


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    edge_uuid = uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"{src}|{edge_type}|{dst}",
    ).int
    return edge_uuid & ((1 << 63) - 1)


def _json_sha256(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _resolve_dsn(deployment: str) -> str:
    from okg.substrate.deployments import (
        require_postgres_dsn,
        resolve_deployment,
    )

    manifest = resolve_deployment(deployment)
    return require_postgres_dsn(manifest, deployment=deployment)


def _current_catalog_version_id(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])
