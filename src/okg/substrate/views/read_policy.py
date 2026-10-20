"""Admission and execution contract for deployment-product SQL read models.

The module owns no scheduler, policy engine, lineage store, or registry. SQL
read models remain canonical ``TypedAsset`` records; accepted outputs publish
through the existing derived-artifact machinery and carry the existing
``MaterializedLineageEnvelope``.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql as psycopg_sql

from okg.substrate.contracts.deployment_product.read_assets import (
    ReadAssetContract,
    ReadAssetExecutionToken,
    build_read_asset,
    resolve_read_assets,
)
from okg.substrate.derived_artifacts.lineage import (
    MaterializedLineageEnvelope,
    SourceRecordLineage,
)
from okg.substrate.derived_artifacts.output_lineage import (
    build_output_lineage_envelope,
)
from okg.substrate.derived_artifacts.relations import (
    PublishedArtifact,
    RelationArtifactDefinition,
    RelationRow,
    publish_relation_artifact,
    stable_hash,
)


SQL_READ_MODEL_SCHEMA = "okg.deployment_product.sql_read_model/v1"
_QUALIFIED_RELATION_RE = re.compile(
    r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$"
)
_NAMED_OR_TEXT_SUBSTITUTION_RE = re.compile(
    r"(?:%\([^)]+\)s|\{\{|\$\{|(?<!:):[A-Za-z_][A-Za-z0-9_]*)"
)
_SECRET_LITERAL_RE = re.compile(
    r"(?:postgres(?:ql)?://|password\s*=|secret\s*=|token\s*=|api[_-]?key\s*=|sk-[A-Za-z0-9]{12,})",
    re.IGNORECASE,
)

# sqlglot node classes a read model may contain. Functions, operators,
# casts, IS NULL, unary minus, ``*`` and subqueries have their own rules in
# `_check_read_model_tree`; every other class is refused.
_ALLOWED_NODE_TYPES = frozenset({
    "Alias",
    "And",
    "Boolean",
    "CTE",
    "Case",
    "Coalesce",
    "Column",
    "DataType",
    "DataTypeParam",
    "Distinct",
    "From",
    "Group",
    "Having",
    "Identifier",
    "If",
    "Join",
    "Limit",
    "Literal",
    "Not",
    "Null",
    "Offset",
    "Or",
    "Order",
    "Ordered",
    "Parameter",
    "Paren",
    "Select",
    "Subquery",
    "Table",
    "TableAlias",
    "Where",
    "With",
})
_ALLOWED_OPERATORS = frozenset({
    "=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%",
    "~~", "!~~", "~~*", "!~~*", "->", "->>", "@>", "<@", "?",
})
_ALLOWED_FUNCTIONS = frozenset({
    "abs",
    "avg",
    "count",
    "date_trunc",
    "jsonb_array_length",
    "jsonb_extract_path_text",
    "length",
    "lower",
    "max",
    "min",
    "round",
    "sum",
    "upper",
})
_ALLOWED_CAST_TYPES = frozenset({
    "bigint",
    "bool",
    "boolean",
    "date",
    "float4",
    "float8",
    "int2",
    "int4",
    "int8",
    "integer",
    "json",
    "jsonb",
    "numeric",
    "text",
    "timestamp",
    "timestamptz",
    "uuid",
})
_PARAMETER_KINDS = frozenset({
    "boolean", "date", "integer", "json", "number", "text", "timestamptz", "uuid",
})


class ReadSurfacePolicyError(ValueError):
    """Stable pre-mutation SQL or policy refusal."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        data: Mapping[str, Any] | None = None,
        fix_hint: str | None = None,
    ) -> None:
        self.code = code
        self.data = dict(data or {})
        self.fix_hint = fix_hint
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ReadPolicyRequirements:
    """Declarative requirements consumed by the existing Nomos read path."""

    audiences: frozenset[str]
    tenants: frozenset[str]
    visible_fields: frozenset[str]
    decision_ids: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        for field_name in ("audiences", "tenants", "visible_fields", "decision_ids"):
            values = getattr(self, field_name)
            if not all(isinstance(item, str) and item for item in values):
                raise ValueError(f"policy {field_name} must contain non-empty strings")

    def to_dict(self) -> dict[str, Any]:
        return {
            "audiences": sorted(self.audiences),
            "tenants": sorted(self.tenants),
            "visible_fields": sorted(self.visible_fields),
            "decision_ids": sorted(self.decision_ids),
        }

    @classmethod
    def from_value(cls, value: object) -> "ReadPolicyRequirements":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("policy requirements must be a mapping")
        unknown = sorted(set(value) - {"audiences", "tenants", "visible_fields", "decision_ids"})
        if unknown:
            raise ValueError(f"policy requirements contain unknown fields: {', '.join(unknown)}")
        return cls(
            audiences=frozenset(value.get("audiences") or ()),
            tenants=frozenset(value.get("tenants") or ()),
            visible_fields=frozenset(value.get("visible_fields") or ()),
            decision_ids=frozenset(value.get("decision_ids") or ()),
        )


def compose_non_widening_policy(
    policies: Sequence[ReadPolicyRequirements],
) -> ReadPolicyRequirements:
    """Meet policies in the finite access lattice (set intersection)."""

    if not policies:
        raise ValueError("at least one source policy is required")
    return ReadPolicyRequirements(
        audiences=frozenset.intersection(*(item.audiences for item in policies)),
        tenants=frozenset.intersection(*(item.tenants for item in policies)),
        visible_fields=frozenset.intersection(*(item.visible_fields for item in policies)),
        decision_ids=frozenset().union(*(item.decision_ids for item in policies)),
    )


def require_non_widening_policy(
    sources: Sequence[ReadPolicyRequirements],
    output: ReadPolicyRequirements,
) -> None:
    expected = compose_non_widening_policy(sources)
    widened: dict[str, list[str]] = {}
    for field_name in ("audiences", "tenants", "visible_fields"):
        extra = sorted(getattr(output, field_name) - getattr(expected, field_name))
        if extra:
            widened[field_name] = extra
    missing_decisions = sorted(expected.decision_ids - output.decision_ids)
    if widened or missing_decisions:
        raise ReadSurfacePolicyError(
            "read_policy_widening",
            "read-model output policy would widen or detach source policy",
            data={"widened": widened, "missing_decision_ids": missing_decisions},
            fix_hint="intersect audience, tenant, and field grants and preserve every source decision ID",
        )


@dataclass(frozen=True, slots=True)
class SqlValueParameter:
    position: int
    name: str
    value_type: str
    nullable: bool = False

    def __post_init__(self) -> None:
        if self.position < 1:
            raise ValueError("parameter position must be positive")
        if not self.name or not isinstance(self.name, str):
            raise ValueError("parameter name is required")
        if self.value_type not in _PARAMETER_KINDS:
            raise ValueError(
                "SQL parameters are data-value binds only; identifier, SQL-text, clause, and object-name parameters are forbidden"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "position": self.position,
            "name": self.name,
            "value_type": self.value_type,
            "nullable": self.nullable,
        }

    @classmethod
    def from_value(cls, value: object) -> "SqlValueParameter":
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError("SQL parameter declarations must be mappings")
        unknown = sorted(set(value) - {"position", "name", "value_type", "nullable"})
        if unknown:
            raise ValueError(f"SQL parameter contains unknown fields: {', '.join(unknown)}")
        return cls(
            position=int(value["position"]),
            name=str(value["name"]),
            value_type=str(value["value_type"]),
            nullable=bool(value.get("nullable", False)),
        )


@dataclass(frozen=True, slots=True)
class SqlReadModelPlan:
    asset_id: str
    asset_digest: str
    sql_digest: str
    normalized_sql: str
    distribution_namespace: str
    output_object: str
    source_relations: tuple[str, ...]
    source_asset_ids: tuple[str, ...]
    parameters: tuple[SqlValueParameter, ...]
    result_columns: tuple[tuple[str, str], ...]
    policy: ReadPolicyRequirements
    source_policies: tuple[tuple[str, ReadPolicyRequirements], ...]
    source_policy_enforcements: tuple[tuple[str, str, str], ...]
    migration_id: str | None
    maintenance_role: str
    application_role: str


@dataclass(frozen=True, slots=True)
class SqlReadResult:
    columns: tuple[str, ...]
    rows: tuple[Mapping[str, Any], ...]
    execution_role: str
    asset_id: str
    asset_digest: str


@dataclass(frozen=True, slots=True)
class SqlViewMaterializationReceipt:
    asset_id: str
    asset_digest: str
    sql_digest: str
    database_object: str
    maintenance_role: str
    view_definition_digest: str


@dataclass(frozen=True, slots=True)
class NativeViewReadResult:
    asset_id: str
    asset_digest: str
    generation_id: int
    payload: object
    visible_fields: tuple[str, ...]
    lineage_sources: tuple[str, ...]
    decision_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SqlReadPublicationReceipt:
    materialization: SqlViewMaterializationReceipt
    read_result: SqlReadResult
    lineage: MaterializedLineageEnvelope
    publication: PublishedArtifact


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _policy_digest(policy: ReadPolicyRequirements) -> str:
    return _sha256(json.dumps(
        policy.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ))


def read_governed_native_view(
    conn: psycopg.Connection[Any],
    *,
    contract: ReadAssetContract,
    execution_token: ReadAssetExecutionToken,
    deployment_name: str,
    view_id: str,
    slot_name: str,
    audience: str,
    tenant: str,
    generation_id: int | None = None,
) -> NativeViewReadResult:
    """Read one selected native view through its digest-bound policy boundary."""

    if contract.asset.kind != "view":
        raise ReadSurfacePolicyError(
            "native_read_asset_kind_invalid",
            "governed native reads require a canonical view asset",
        )
    if contract.asset.provenance.get("adapter") == "views_registry.yaml/v1":
        raise ReadSurfacePolicyError(
            "legacy_view_policy_migration_required",
            "legacy views_registry provenance cannot authorize a governed native read",
            fix_hint=(
                "replace the compatibility row with a canonical view asset "
                "bound to an attested Nomos policy decision"
            ),
        )
    policy = ReadPolicyRequirements.from_value(contract.policy_requirements)
    expected_view_id = (
        contract.asset.asset_id
        if contract.asset.provenance.get("adapter") == "views_registry.yaml/v1"
        else contract.config.get("view_id") or contract.config.get("view_name")
    )
    expected_slots = tuple(sorted(
        contract.config.get("result_slots") or ("json", "markdown")
    ))
    if (
        execution_token.asset_id != contract.qualified_id
        or execution_token.asset_digest != contract.asset.digest
        or execution_token.policy_digest != _policy_digest(policy)
        or execution_token.lineage_sources != contract.lineage_sources
        or execution_token.runtime_identity != expected_view_id
        or execution_token.allowed_slots != expected_slots
    ):
        raise ReadSurfacePolicyError(
            "native_read_resolution_mismatch",
            "native read authority does not match the selected asset/policy/lineage digest",
        )
    expected_deployment = contract.config.get("deployment_name")
    if not isinstance(expected_deployment, str) or deployment_name != expected_deployment:
        raise ReadSurfacePolicyError(
            "native_read_deployment_mismatch",
            "native read deployment does not match the selected view contract",
        )
    if view_id != expected_view_id:
        raise ReadSurfacePolicyError(
            "native_read_view_mismatch",
            "native read view identity does not match the selected view contract",
        )
    if slot_name not in expected_slots:
        raise ReadSurfacePolicyError(
            "native_read_slot_not_allowed",
            "native read slot is not admitted by the selected result contract",
        )
    if audience not in policy.audiences or tenant not in policy.tenants:
        raise ReadSurfacePolicyError(
            "native_read_policy_denied",
            "Nomos-bound native read audience or tenant is not admitted",
            data={"audience": audience, "tenant": tenant},
        )
    from okg.substrate.derived_artifacts.read_views import read_view_slot

    read = read_view_slot(
        conn,
        deployment_name=deployment_name,
        view_id=view_id,
        slot_name=slot_name,
        generation_id=generation_id,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metrics::jsonb -> 'read_asset_authority' "
            "FROM okg.derived_artifact_runs "
            "WHERE output_manifest_id = %s AND status = 'completed'",
            (read.artifact.manifest_id,),
        )
        authority_rows = cur.fetchall()
        cur.execute(
            "SELECT envelope::jsonb FROM okg.derived_output_lineage_envelopes "
            "WHERE manifest_id = %s AND output_kind = 'native_view'",
            (read.artifact.manifest_id,),
        )
        lineage_rows = cur.fetchall()
    expected_authority = {
        "asset_id": contract.qualified_id,
        "asset_digest": contract.asset.digest,
        "policy_digest": execution_token.policy_digest,
        "runtime_identity": expected_view_id,
        "allowed_slots": list(expected_slots),
        "lineage_sources": list(contract.lineage_sources),
    }
    if len(authority_rows) != 1 or authority_rows[0][0] != expected_authority:
        raise ReadSurfacePolicyError(
            "native_read_publication_authority_invalid",
            "native view manifest is not bound to the selected policy/lineage authority",
        )
    if len(lineage_rows) != 1:
        raise ReadSurfacePolicyError(
            "native_read_lineage_missing",
            "native view manifest lacks its exact persisted lineage envelope",
        )
    lineage_envelope = lineage_rows[0][0]
    lineage_metadata = lineage_envelope.get("metadata") or {}
    if (
        tuple(lineage_metadata.get("declared_lineage_sources") or ())
        != contract.lineage_sources
        or lineage_metadata.get("policy_digest") != execution_token.policy_digest
        or tuple(sorted(lineage_envelope.get("decision_ids") or ()))
        != tuple(sorted(policy.decision_ids))
    ):
        raise ReadSurfacePolicyError(
            "native_read_lineage_mismatch",
            "native view persisted lineage does not match the selected policy contract",
        )
    raw_payload = read.slot.payload_value()
    if isinstance(raw_payload, Mapping):
        payload: object = MappingProxyType({
            key: value
            for key, value in raw_payload.items()
            if key in policy.visible_fields
        })
    elif "content" in policy.visible_fields:
        payload = raw_payload
    else:
        raise ReadSurfacePolicyError(
            "native_read_field_policy_invalid",
            "scalar native output requires the explicit content field grant",
        )
    return NativeViewReadResult(
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        generation_id=read.artifact.target_generation_id,
        payload=payload,
        visible_fields=tuple(sorted(policy.visible_fields)),
        lineage_sources=contract.lineage_sources,
        decision_ids=tuple(sorted(policy.decision_ids)),
    )


def _outside_quoted_literals(statement: str) -> str:
    """Blank quoted content so interpolation markers inside data stay data."""

    out: list[str] = []
    quote: str | None = None
    index = 0
    while index < len(statement):
        char = statement[index]
        if quote is None:
            if char in {"'", '"'}:
                quote = char
                out.append(char)
            else:
                out.append(char)
            index += 1
            continue
        out.append(" ")
        if char == quote:
            if index + 1 < len(statement) and statement[index + 1] == quote:
                out.append(" ")
                index += 2
                continue
            quote = None
        index += 1
    return "".join(out)


def _canonical_relation(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not _QUALIFIED_RELATION_RE.fullmatch(value):
        raise ValueError(f"{field_name} must be a lower-case schema.relation identifier")
    return value


def build_sql_read_model_asset(
    *,
    owner: str,
    asset_id: str,
    sql: str,
    distribution_namespace: str,
    object_name: str,
    source_assets: Mapping[str, ReadAssetContract],
    source_policies: Mapping[str, ReadPolicyRequirements | Mapping[str, Any]],
    source_policy_enforcements: Mapping[str, str],
    parameters: Sequence[SqlValueParameter | Mapping[str, Any]] = (),
    result_columns: Mapping[str, str],
    policy: ReadPolicyRequirements | Mapping[str, Any],
    maintenance_role: str,
    application_role: str,
    migration_id: str | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> ReadAssetContract:
    """Build a SQL read model whose SQL and source closure are digest-bound."""

    if not isinstance(sql, str) or not sql.strip():
        raise ValueError("SQL read model requires non-empty SQL")
    namespace = _canonical_relation(f"{distribution_namespace}.x", field_name="distribution_namespace").split(".", 1)[0]
    output_object = _canonical_relation(
        f"{namespace}.{object_name}", field_name="output_object"
    )
    checked_parameters = tuple(SqlValueParameter.from_value(item) for item in parameters)
    checked_policy = ReadPolicyRequirements.from_value(policy)
    source_config: dict[str, dict[str, str]] = {}
    references = []
    if set(source_policies) != set(source_assets) or set(source_policy_enforcements) != set(source_assets):
        raise ValueError("every typed source relation requires exactly one policy and enforcement")
    for relation, contract in sorted(source_assets.items()):
        canonical = _canonical_relation(relation, field_name="source relation")
        source_policy = ReadPolicyRequirements.from_value(source_policies[relation])
        enforcement = source_policy_enforcements[relation]
        if enforcement != "row-security":
            raise ValueError("Wave 4 SQL sources require database-attested row-security enforcement")
        enforcement_digest = _sha256(json.dumps({
            "relation": canonical,
            "asset_id": contract.asset.qualified_id,
            "asset_digest": contract.asset.digest,
            "policy_digest": _policy_digest(source_policy),
            "enforcement": enforcement,
        }, sort_keys=True, separators=(",", ":")))
        source_config[canonical] = {
            "asset_id": contract.asset.qualified_id,
            "asset_digest": contract.asset.digest,
            "kind": contract.asset.kind,
            "policy": source_policy.to_dict(),
            "policy_digest": _policy_digest(source_policy),
            "policy_enforcement": enforcement,
            "policy_enforcement_digest": enforcement_digest,
        }
        references.append({
            "owner": contract.asset.owner,
            "id": contract.asset.asset_id,
            "kind": contract.asset.kind,
            "version": contract.asset.version,
            "digest": contract.asset.digest,
        })
    checked_columns = tuple((str(name), str(type_name)) for name, type_name in result_columns.items())
    if not checked_columns or any(not name or not type_name for name, type_name in checked_columns):
        raise ValueError("result_columns must declare at least one named typed column")
    config = {
        "schema": SQL_READ_MODEL_SCHEMA,
        "sql": sql,
        "sql_digest": _sha256(sql),
        "distribution_namespace": namespace,
        "output_object": output_object,
        "source_relations": source_config,
        "parameters": [item.to_dict() for item in checked_parameters],
        "result_columns": [
            {"name": name, "type": type_name} for name, type_name in checked_columns
        ],
        "policy": checked_policy.to_dict(),
        "maintenance_role": maintenance_role,
        "application_role": application_role,
        "migration_id": migration_id,
    }
    return build_read_asset(
        owner=owner,
        asset_id=asset_id,
        kind="read-model",
        config_schema=SQL_READ_MODEL_SCHEMA,
        result_schema="okg.deployment_product.sql_read_model.result/v1",
        capability_schema="okg.deployment_product.sql_read_model.capability/v1",
        output_identity=f"sql-read-model:{output_object}",
        database_object=output_object,
        refresh_policy={"mode": "generation"},
        policy_requirements=checked_policy.to_dict(),
        lineage_sources=tuple(
            sorted(contract.asset.qualified_id for contract in source_assets.values())
        ),
        config=config,
        references=references,
        provenance=provenance or {"source": "definition"},
    )


def _dotted(node: Any) -> tuple[str, ...]:
    """Canonical name parts of an identifier, a dotted name, or a function."""
    from sqlglot import exp

    from okg.substrate.postgres_sql import identifier_name

    if isinstance(node, exp.Dot):
        return _dotted(node.this) + _dotted(node.expression)
    if isinstance(node, exp.Anonymous):
        return (identifier_name(node.this),)
    if isinstance(node, (exp.Identifier, str)):
        return (identifier_name(node),)
    if isinstance(node, exp.Column):
        return tuple(identifier_name(part) for part in node.parts)
    return ()


# sqlglot's names for the built-in cast targets the allowlist admits.
_SQLGLOT_CAST_TYPES = {
    "BIGINT": "int8",
    "BOOLEAN": "bool",
    "DATE": "date",
    "DECIMAL": "numeric",
    "DOUBLE": "float8",
    "FLOAT": "float4",
    "INT": "int4",
    "JSON": "json",
    "JSONB": "jsonb",
    "SMALLINT": "int2",
    "TEXT": "text",
    "TIMESTAMP": "timestamp",
    "TIMESTAMPTZ": "timestamptz",
    "UUID": "uuid",
}


def _cast_target(to: Any) -> tuple[str, str]:
    """``(type_name, rendered)`` for a cast target; ``""`` when not admissible.

    A built-in type written with a schema must name ``pg_catalog``; arrays and
    object-identifier types (``regproc``, ``regclass``) never qualify.
    """
    from sqlglot import exp

    rendered = to.sql(dialect="postgres") if to is not None else ""
    if not isinstance(to, exp.DataType):
        return "", rendered
    dtype = to.this
    if not isinstance(dtype, exp.DataType.Type):
        # e.g. ObjectIdentifier (regclass, regproc), which carries a string.
        return "", rendered
    if dtype == exp.DataType.Type.USERDEFINED:
        names = _dotted(to.args.get("kind"))
        if not names or (len(names) > 1 and names[-2] != "pg_catalog") or len(names) > 2:
            return "", rendered
        return names[-1], rendered
    if any(not isinstance(item, exp.DataTypeParam) for item in to.expressions):
        return "", rendered
    return _SQLGLOT_CAST_TYPES.get(dtype.name, ""), rendered


# sqlglot parses some PostgreSQL operators into function nodes; they were
# written as operators and are refused as operators.
_OPERATOR_SHAPED_FUNCTIONS = frozenset({
    "ArrayContainsAll",
    "ArrayContainedBy",
    "ArrayOverlaps",
    "JSONBContains",
    "JSONBExtract",
    "JSONBExtractScalar",
    "JSONExtract",
    "JSONExtractScalar",
    "MatchAgainst",
})


@dataclass(frozen=True)
class _ReadModelTree:
    relations: frozenset[str]
    parameter_positions: frozenset[int]
    referenced_columns: frozenset[str]


def _check_read_model_tree(stmt: Any) -> _ReadModelTree:
    """Walk a parsed read model against the closed allowlist.

    Refusal codes are the ones the pglast checker used. The walk is an
    allowlist: a sqlglot node class not named in ``_ALLOWED_NODE_TYPES`` is
    refused, so a construct sqlglot adds later cannot slip through.
    """
    from sqlglot import exp

    from okg.substrate.postgres_sql import (
        canonical_identifier,
        cte_names_visible_at,
        identifier_is_canonical_lower,
        identifier_name,
        iter_nodes,
        table_name_parts,
    )

    relations: set[str] = set()
    positions: set[int] = set()
    columns: set[str] = set()
    nodes = list(iter_nodes(stmt))
    # Statement-shape refusals first, anywhere in the tree, so their specific
    # codes win over a node-level refusal met earlier in the walk.
    for node in nodes:
        node_name = type(node).__name__
        if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
            raise ReadSurfacePolicyError(
                "sql_data_modifying_cte",
                f"SQL AST node {node_name} is not allowlisted",
                data={"node_type": node_name},
            )
        if isinstance(node, exp.Into):
            raise ReadSurfacePolicyError("sql_select_into_forbidden", "SELECT INTO is forbidden")
        if isinstance(node, exp.Lock):
            raise ReadSurfacePolicyError("sql_row_lock_forbidden", "row-locking clauses are forbidden")
        if isinstance(node, exp.Values):
            raise ReadSurfacePolicyError("sql_values_source_forbidden", "VALUES sources are not allowed in read models")
        if isinstance(node, exp.SetOperation):
            raise ReadSurfacePolicyError("sql_set_operation_forbidden", "UNION/INTERSECT/EXCEPT are not allowlisted")

    for node in nodes:
        node_name = type(node).__name__
        if isinstance(node, exp.Anonymous) or (
            isinstance(node, exp.Dot) and isinstance(node.expression, exp.Anonymous)
        ):
            function_node = node if isinstance(node, exp.Anonymous) else node.expression
            parent = function_node.parent
            names = (
                _dotted(parent)
                if isinstance(parent, exp.Dot) and parent.expression is function_node
                else _dotted(function_node)
            )
            function = names[-1] if names else ""
            schema = names[-2] if len(names) == 2 else None
            if schema != "pg_catalog" or function not in _ALLOWED_FUNCTIONS:
                raise ReadSurfacePolicyError(
                    "sql_function_forbidden",
                    f"function {'.'.join(names) or '<dynamic>'!r} is not on the immutable/aggregate allowlist",
                    data={"function": ".".join(names)},
                )
            continue
        if isinstance(node, exp.Dot) and isinstance(node.parent, exp.DataType):
            # A schema-qualified cast target; the Cast check judged it.
            continue
        if isinstance(node, exp.If) and not isinstance(node.parent, exp.Case):
            raise ReadSurfacePolicyError(
                "sql_function_forbidden",
                "function 'if' is not on the immutable/aggregate allowlist",
                data={"function": "if"},
            )
        if isinstance(node, exp.Dot):
            raise ReadSurfacePolicyError(
                "sql_ast_node_forbidden",
                f"SQL AST node {node_name} is not allowlisted",
                data={"node_type": node_name},
            )
        if isinstance(node, exp.Operator):
            schema_text, _, operator = str(node.args.get("operator") or "").rpartition(".")
            schema_text = schema_text.strip()
            operator = operator.strip()
            if not schema_text:
                schema = None
            elif len(schema_text) > 1 and schema_text[0] == schema_text[-1] == '"':
                schema = canonical_identifier(
                    schema_text[1:-1].replace('""', '"'), quoted=True
                )
            else:
                schema = canonical_identifier(schema_text, quoted=False)
            if schema != "pg_catalog" or operator not in _ALLOWED_OPERATORS:
                raise ReadSurfacePolicyError(
                    "sql_operator_forbidden",
                    "SQL expression/operator is not allowlisted",
                    data={"kind": "AEXPR_OP", "operators": [schema, operator] if schema else [operator]},
                )
            continue
        if isinstance(node, exp.Cast):
            target_type, rendered = _cast_target(node.args.get("to"))
            if target_type not in _ALLOWED_CAST_TYPES:
                raise ReadSurfacePolicyError(
                    "sql_cast_forbidden",
                    "SQL cast target is not an allowlisted built-in type",
                    data={"target_type": rendered},
                )
            continue
        if isinstance(node, exp.Is):
            if not isinstance(node.expression, exp.Null):
                raise ReadSurfacePolicyError(
                    "sql_operator_forbidden",
                    "SQL expression/operator is not allowlisted",
                    data={"kind": "IS", "operators": []},
                )
            continue
        if isinstance(node, exp.Neg):
            if not (isinstance(node.this, exp.Literal) and not node.this.is_string):
                raise ReadSurfacePolicyError(
                    "sql_operator_forbidden",
                    "SQL expression/operator is not allowlisted",
                    data={"kind": "AEXPR_OP", "operators": ["-"]},
                )
            continue
        if isinstance(node, exp.Star):
            parent = node.parent
            if not (
                isinstance(parent, exp.Anonymous)
                and identifier_name(parent.this) == "count"
                and list(parent.expressions) == [node]
            ):
                raise ReadSurfacePolicyError(
                    "sql_ast_node_forbidden",
                    "SQL AST node Star is not allowlisted outside count(*)",
                    data={"node_type": node_name},
                )
            continue
        if isinstance(node, exp.Subquery) and not isinstance(node.parent, (exp.From, exp.Join)):
            raise ReadSurfacePolicyError(
                "sql_ast_node_forbidden",
                "a subquery is allowlisted only as a FROM / JOIN source",
                data={"node_type": node_name},
            )
        if node_name not in _ALLOWED_NODE_TYPES:
            if type(node).__name__ in _OPERATOR_SHAPED_FUNCTIONS or (
                isinstance(node, (exp.Binary, exp.Unary, exp.Predicate))
                and not isinstance(node, exp.Func)
            ):
                raise ReadSurfacePolicyError(
                    "sql_operator_forbidden",
                    "SQL expression/operator is not allowlisted",
                    data={"kind": node_name, "operators": []},
                )
            if isinstance(node, exp.Func):
                raise ReadSurfacePolicyError(
                    "sql_function_forbidden",
                    f"function {node.sql_name().lower()!r} is not on the immutable/aggregate allowlist",
                    data={"function": node.sql_name().lower()},
                )
            raise ReadSurfacePolicyError(
                "sql_ast_node_forbidden",
                f"SQL AST node {node_name} is not allowlisted",
                data={"node_type": node_name},
            )

        if isinstance(node, exp.Table):
            parts = table_name_parts(node)
            if parts is None:
                raise ReadSurfacePolicyError(
                    "sql_ast_node_forbidden",
                    "table functions are not allowlisted",
                    data={"node_type": node_name},
                )
            catalog, schema, relation = parts
            if not all(
                identifier_is_canonical_lower(part)
                for part in (node.this, node.args.get("db"), node.args.get("catalog"))
                if part is not None
            ):
                raise ReadSurfacePolicyError(
                    "sql_quoted_identifier_forbidden",
                    "quoted or case-distinct relation identifiers are forbidden",
                )
            if catalog is not None:
                raise ReadSurfacePolicyError(
                    "sql_cross_namespace_relation",
                    "database-qualified relation names are forbidden",
                )
            if schema is None and relation in cte_names_visible_at(node):
                continue
            relations.add(f"{schema}.{relation}" if schema else relation)
        elif isinstance(node, exp.Parameter):
            literal = node.this
            if not (isinstance(literal, exp.Literal) and not literal.is_string):
                raise ReadSurfacePolicyError(
                    "sql_ast_node_forbidden",
                    "only positional $n parameters are allowlisted",
                    data={"node_type": node_name},
                )
            positions.add(int(str(literal.this)))
        elif isinstance(node, exp.Literal):
            if node.is_string and _SECRET_LITERAL_RE.search(str(node.this)):
                raise ReadSurfacePolicyError(
                    "sql_literal_secret",
                    "SQL contains a secret-shaped literal",
                    data={"literal_kind": "redacted"},
                    fix_hint="store secrets in governed connections; SQL may contain only distribution-owned non-secret constants",
                )
        elif isinstance(node, exp.Column):
            if not isinstance(node.this, exp.Star):
                columns.add(identifier_name(node.this).lower())
    return _ReadModelTree(
        relations=frozenset(relations),
        parameter_positions=frozenset(positions),
        referenced_columns=frozenset(columns),
    )


def _validate_parameter_value(spec: SqlValueParameter, value: object) -> None:
    if value is None:
        if spec.nullable:
            return
        raise ReadSurfacePolicyError(
            "sql_parameter_null",
            f"parameter ${spec.position} ({spec.name}) is not nullable",
            data={"position": spec.position, "name": spec.name},
        )
    valid = False
    if spec.value_type == "boolean":
        valid = isinstance(value, bool)
    elif spec.value_type == "integer":
        valid = isinstance(value, int) and not isinstance(value, bool)
    elif spec.value_type == "number":
        valid = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif spec.value_type == "text":
        valid = isinstance(value, str)
    elif spec.value_type == "uuid":
        try:
            UUID(str(value))
            valid = True
        except (TypeError, ValueError):
            valid = False
    elif spec.value_type == "date":
        valid = isinstance(value, date) and not isinstance(value, datetime)
    elif spec.value_type == "timestamptz":
        valid = isinstance(value, datetime) and value.tzinfo is not None
    elif spec.value_type == "json":
        try:
            json.dumps(value, allow_nan=False)
            valid = True
        except (TypeError, ValueError):
            valid = False
    if not valid:
        raise ReadSurfacePolicyError(
            "sql_parameter_type_mismatch",
            f"parameter ${spec.position} ({spec.name}) is not {spec.value_type}",
            data={"position": spec.position, "name": spec.name, "expected_type": spec.value_type},
        )


def validate_sql_read_model(
    contract: ReadAssetContract,
    *,
    source_assets: Mapping[str, ReadAssetContract],
    parameter_values: Sequence[object] | None = None,
    previous_plan: SqlReadModelPlan | None = None,
    mutation_observer=None,
) -> SqlReadModelPlan:
    """Parse and close the complete read model before any mutation."""

    if contract.asset.kind != "read-model":
        raise ReadSurfacePolicyError(
            "sql_read_model_kind_invalid",
            "SQL read-model admission requires a canonical read-model asset",
        )
    # This also verifies every source reference/digest and the output object's
    # single-owner rule through the Wave 1 registry.
    resolve_read_assets(
        [*source_assets.values(), contract],
        roots=[contract.qualified_id],
    )
    config = dict(contract.config)
    if config.get("schema") != SQL_READ_MODEL_SCHEMA:
        raise ReadSurfacePolicyError(
            "sql_read_model_schema_invalid",
            "SQL read model is missing the registered config schema",
        )
    statement = config.get("sql")
    if not isinstance(statement, str) or not statement.strip():
        raise ReadSurfacePolicyError("sql_invalid", "SQL read model has empty SQL")
    if _sha256(statement) != config.get("sql_digest"):
        raise ReadSurfacePolicyError(
            "sql_digest_mismatch",
            "SQL bytes do not match the digest-pinned read asset",
        )
    if _NAMED_OR_TEXT_SUBSTITUTION_RE.search(_outside_quoted_literals(statement)):
        raise ReadSurfacePolicyError(
            "sql_text_substitution_forbidden",
            "SQL text, identifier, clause, or object-name substitution is forbidden; use positional value binds",
        )
    from okg.substrate.postgres_sql import (
        COMMENT_KINDS,
        WORD,
        PostgresSqlError,
        lex,
        parse,
        statement_count,
        statement_text,
    )

    try:
        lexed = lex(statement)
    except PostgresSqlError as exc:
        raise ReadSurfacePolicyError(
            "sql_parse_invalid", f"PostgreSQL parser rejected the SQL: {exc}"
        ) from exc
    if statement_count(lexed) != 1:
        raise ReadSurfacePolicyError(
            "sql_multi_statement",
            "SQL read models must contain exactly one statement",
        )
    leading = next((token for token in lexed if token.kind not in COMMENT_KINDS), None)
    if leading is None or not (
        (leading.kind == WORD and leading.value.lower() in {"select", "with"})
        or leading.text == "("
    ):
        raise ReadSurfacePolicyError(
            "sql_statement_forbidden",
            f"SQL read model refuses {(leading.text if leading else '').upper()!r}; only SELECT is allowed",
        )
    try:
        parsed = parse(statement, strict=True)
    except (PostgresSqlError, RecursionError) as exc:
        raise ReadSurfacePolicyError(
            "sql_parse_invalid", f"PostgreSQL parser rejected the SQL: {exc}"
        ) from exc
    if len(parsed.statements) != 1:
        raise ReadSurfacePolicyError(
            "sql_multi_statement",
            "SQL read models must contain exactly one statement",
        )
    from sqlglot import exp

    stmt = parsed.statements[0]
    if not isinstance(stmt, exp.Select):
        if isinstance(stmt, exp.SetOperation):
            raise ReadSurfacePolicyError("sql_set_operation_forbidden", "UNION/INTERSECT/EXCEPT are not allowlisted")
        raise ReadSurfacePolicyError(
            "sql_statement_forbidden",
            f"SQL read model refuses {type(stmt).__name__}; only SELECT is allowed",
        )
    tree = _check_read_model_tree(stmt)
    actual_relations = set(tree.relations)
    parameter_positions = set(tree.parameter_positions)

    declared_source_config = config.get("source_relations")
    if not isinstance(declared_source_config, Mapping):
        raise ReadSurfacePolicyError("sql_source_declaration_invalid", "source_relations must be a mapping")
    declared_relations = set(declared_source_config)
    namespace = str(config.get("distribution_namespace") or "")
    if actual_relations != declared_relations:
        raise ReadSurfacePolicyError(
            "sql_relation_lineage_mismatch",
            "SQL source relations must exactly match the typed source closure",
            data={
                "undeclared": sorted(actual_relations - declared_relations),
                "unused_declared": sorted(declared_relations - actual_relations),
            },
        )
    bound_source_policies: list[ReadPolicyRequirements] = []
    source_policy_enforcements: list[tuple[str, str, str]] = []
    for relation in sorted(actual_relations):
        if not relation.startswith(f"{namespace}."):
            raise ReadSurfacePolicyError(
                "sql_cross_namespace_relation",
                f"relation {relation!r} is outside distribution namespace {namespace!r}",
            )
        source = source_assets.get(relation)
        declaration = declared_source_config.get(relation)
        if source is None or not isinstance(declaration, Mapping):
            raise ReadSurfacePolicyError(
                "sql_source_asset_missing",
                f"relation {relation!r} has no typed source asset",
            )
        if (
            declaration.get("asset_id") != source.qualified_id
            or declaration.get("asset_digest") != source.asset.digest
            or source.database_object != relation
        ):
            raise ReadSurfacePolicyError(
                "sql_source_asset_mismatch",
                f"relation {relation!r} does not match its digest-pinned typed source asset",
            )
        try:
            source_policy = ReadPolicyRequirements.from_value(declaration.get("policy"))
        except (TypeError, ValueError) as exc:
            raise ReadSurfacePolicyError(
                "read_source_policy_invalid",
                f"relation {relation!r} is missing a valid digest-bound source policy",
            ) from exc
        if declaration.get("policy_digest") != _policy_digest(source_policy):
            raise ReadSurfacePolicyError(
                "read_source_policy_digest_mismatch",
                f"relation {relation!r} source policy changed after asset composition",
            )
        bound_source_policies.append(source_policy)
        enforcement = declaration.get("policy_enforcement")
        expected_enforcement_digest = _sha256(json.dumps({
            "relation": relation,
            "asset_id": source.asset.qualified_id,
            "asset_digest": source.asset.digest,
            "policy_digest": _policy_digest(source_policy),
            "enforcement": enforcement,
        }, sort_keys=True, separators=(",", ":")))
        if (
            enforcement != "row-security"
            or declaration.get("policy_enforcement_digest") != expected_enforcement_digest
        ):
            raise ReadSurfacePolicyError(
                "read_source_policy_enforcement_invalid",
                f"relation {relation!r} lacks digest-bound policy enforcement",
            )
        source_policy_enforcements.append(
            (relation, str(enforcement), expected_enforcement_digest)
        )

    try:
        parameters = tuple(SqlValueParameter.from_value(item) for item in config.get("parameters") or ())
    except (KeyError, TypeError, ValueError) as exc:
        raise ReadSurfacePolicyError("sql_parameter_declaration_invalid", str(exc)) from exc
    declared_positions = {item.position for item in parameters}
    expected_positions = set(range(1, len(parameters) + 1))
    if declared_positions != expected_positions or parameter_positions != declared_positions:
        raise ReadSurfacePolicyError(
            "sql_parameter_binding_mismatch",
            "SQL positional binds and typed parameter declarations must be contiguous and exact",
            data={
                "sql_positions": sorted(parameter_positions),
                "declared_positions": sorted(declared_positions),
            },
        )
    if tuple(item.position for item in parameters) != tuple(range(1, len(parameters) + 1)):
        raise ReadSurfacePolicyError(
            "sql_parameter_order_invalid",
            "typed SQL parameter declarations must be ordered by position",
        )
    if parameter_values is not None:
        if len(parameter_values) != len(parameters):
            raise ReadSurfacePolicyError(
                "sql_parameter_count_mismatch",
                "provided values do not match the typed bind count",
                data={"expected": len(parameters), "observed": len(parameter_values)},
            )
        for spec, value in zip(parameters, parameter_values, strict=True):
            _validate_parameter_value(spec, value)

    try:
        output_policy = ReadPolicyRequirements.from_value(config.get("policy"))
    except (TypeError, ValueError) as exc:
        raise ReadSurfacePolicyError("read_policy_invalid", str(exc)) from exc
    require_non_widening_policy(bound_source_policies, output_policy)

    result_columns_raw = config.get("result_columns")
    if not isinstance(result_columns_raw, list) or not result_columns_raw:
        raise ReadSurfacePolicyError("sql_result_contract_invalid", "result_columns must be a non-empty list")
    result_columns = tuple(
        (str(item["name"]), str(item["type"])) for item in result_columns_raw
    )
    result_names = tuple(name for name, _type_name in result_columns)
    if len(result_names) != len(set(result_names)):
        raise ReadSurfacePolicyError(
            "sql_result_contract_invalid",
            "result column names must be unique",
        )
    undeclared_visible = sorted(set(result_names) - output_policy.visible_fields)
    if undeclared_visible:
        raise ReadSurfacePolicyError(
            "sql_result_policy_mismatch",
            "SQL result columns exceed the output policy visible-field grant",
            data={"undeclared_visible_fields": undeclared_visible},
        )
    referenced_columns = set(tree.referenced_columns)
    forbidden_source_columns = sorted(
        referenced_columns - output_policy.visible_fields
    )
    if forbidden_source_columns:
        raise ReadSurfacePolicyError(
            "sql_source_field_policy_mismatch",
            "SQL references source fields outside the output policy",
            data={"forbidden_fields": forbidden_source_columns},
        )
    if not output_policy.audiences or not output_policy.tenants:
        raise ReadSurfacePolicyError(
            "read_policy_empty_grant",
            "SQL read models require at least one admitted audience and tenant",
        )
    output_object = _canonical_relation(config.get("output_object"), field_name="output_object")
    if not output_object.startswith(f"{namespace}.") or contract.database_object != output_object:
        raise ReadSurfacePolicyError(
            "sql_output_ownership_invalid",
            "SQL output object is outside its distribution namespace or canonical asset ownership",
        )
    maintenance_role = str(config.get("maintenance_role") or "")
    application_role = str(config.get("application_role") or "")
    if not maintenance_role or not application_role or maintenance_role == application_role:
        raise ReadSurfacePolicyError(
            "sql_role_separation_invalid",
            "maintenance and application roles must be non-empty and distinct",
        )
    if application_role not in output_policy.audiences:
        raise ReadSurfacePolicyError(
            "sql_application_role_not_admitted",
            "application role must be an admitted output-policy audience",
        )

    migration_id = config.get("migration_id")
    if migration_id is not None and (not isinstance(migration_id, str) or not migration_id):
        raise ReadSurfacePolicyError(
            "sql_result_migration_invalid",
            "migration_id must be a stable non-empty identifier when present",
        )
    if previous_plan is not None:
        if previous_plan.asset_id != contract.qualified_id:
            raise ReadSurfacePolicyError(
                "sql_result_migration_asset_mismatch",
                "previous result contract belongs to a different read asset",
            )
        require_result_shape_migration(
            previous_plan.result_columns,
            result_columns,
            migration_id=migration_id,
        )

    plan = SqlReadModelPlan(
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        sql_digest=config["sql_digest"],
        # The checked text itself (U& decoded, outer comments and `;`
        # trimmed), not sqlglot's re-rendering, which can change meaning.
        normalized_sql=statement_text(parsed),
        distribution_namespace=namespace,
        output_object=output_object,
        source_relations=tuple(sorted(actual_relations)),
        source_asset_ids=tuple(
            sorted(source_assets[relation].qualified_id for relation in actual_relations)
        ),
        parameters=parameters,
        result_columns=result_columns,
        policy=output_policy,
        source_policies=tuple(
            (relation, ReadPolicyRequirements.from_value(
                declared_source_config[relation]["policy"]
            ))
            for relation in sorted(actual_relations)
        ),
        source_policy_enforcements=tuple(sorted(source_policy_enforcements)),
        migration_id=migration_id,
        maintenance_role=maintenance_role,
        application_role=application_role,
    )
    if mutation_observer is not None:
        mutation_observer(plan)
    return plan


def require_result_shape_migration(
    previous_columns: Sequence[tuple[str, str]],
    next_columns: Sequence[tuple[str, str]],
    *,
    migration_id: str | None,
) -> None:
    if not previous_columns or tuple(previous_columns) == tuple(next_columns):
        return
    if not migration_id:
        raise ReadSurfacePolicyError(
            "sql_result_migration_required",
            "incompatible SQL result shape requires an explicit migration ID",
            data={"previous": list(previous_columns), "next": list(next_columns)},
        )


def _attest_limited_role(
    cur: Any,
    role: str,
    *,
    allowed_select_relations: Sequence[str],
    create_schema: str | None,
    require_select_relations: Sequence[str] = (),
    allowed_write_relations: Sequence[str] = (),
) -> None:
    cur.execute(
        "SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
        "FROM pg_catalog.pg_roles WHERE rolname = %s",
        (role,),
    )
    row = cur.fetchone()
    if row is None or any(bool(value) for value in row):
        raise ReadSurfacePolicyError(
            "sql_role_authority_invalid",
            f"role {role!r} is missing or has privileged/bypass authority",
        )
    cur.execute(
        "SELECT 1 FROM pg_catalog.pg_auth_members m "
        "JOIN pg_catalog.pg_roles member ON member.oid = m.member "
        "JOIN pg_catalog.pg_roles granted ON granted.oid = m.roleid "
        "WHERE member.rolname = %s OR granted.rolname = %s LIMIT 1",
        (role, role),
    )
    if cur.fetchone() is not None:
        raise ReadSurfacePolicyError(
            "sql_role_authority_invalid",
            f"role {role!r} must not inherit or delegate another role",
        )
    allowed_select = set(allowed_select_relations)
    required_select = set(require_select_relations)
    allowed_write = set(allowed_write_relations)
    cur.execute(
        "SELECT n.nspname, "
        "pg_catalog.has_schema_privilege(%s, n.oid, 'USAGE'), "
        "pg_catalog.has_schema_privilege(%s, n.oid, 'CREATE') "
        "FROM pg_catalog.pg_namespace n "
        "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'",
        (role, role),
    )
    for schema_name, has_usage, has_create in cur.fetchall():
        if has_create and schema_name != create_schema:
            raise ReadSurfacePolicyError(
                "sql_role_authority_invalid",
                f"role {role!r} has CREATE outside its owned namespace",
                data={"schema": schema_name},
            )
        if schema_name == create_schema and not has_usage:
            raise ReadSurfacePolicyError(
                "sql_role_authority_invalid",
                f"role {role!r} lacks USAGE on its admitted namespace",
            )
    cur.execute(
        "SELECT n.nspname || '.' || c.relname, "
        "pg_catalog.has_table_privilege(%s, c.oid, 'SELECT'), "
        "pg_catalog.has_table_privilege(%s, c.oid, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'), "
        "pg_catalog.pg_get_userbyid(c.relowner) "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind IN ('r','p','v','m','f') "
        "AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM pg_catalog.pg_depend d "
        "  WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e'"
        ")",
        (role, role),
    )
    observed_select: set[str] = set()
    for relation, can_select, can_mutate, owner in cur.fetchall():
        if can_mutate and (relation not in allowed_write or owner != role):
            raise ReadSurfacePolicyError(
                "sql_role_authority_invalid",
                f"role {role!r} has write authority on {relation!r}",
            )
        if relation in allowed_write and owner != role:
            raise ReadSurfacePolicyError(
                "sql_role_authority_invalid",
                f"role {role!r} does not own its admitted output {relation!r}",
            )
        if can_select:
            observed_select.add(str(relation))
            if relation not in allowed_select:
                raise ReadSurfacePolicyError(
                    "sql_role_authority_invalid",
                    f"role {role!r} can SELECT undeclared relation {relation!r}",
                )
    missing = sorted(required_select - observed_select)
    if missing:
        raise ReadSurfacePolicyError(
            "sql_role_authority_invalid",
            f"role {role!r} lacks required SELECT authority",
            data={"missing_relations": missing},
        )


def _relation_columns(cur: Any, schema_name: str, object_name: str) -> tuple[tuple[str, str], ...]:
    cur.execute(
        "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        (schema_name, object_name),
    )
    return tuple((str(name), str(type_name)) for name, type_name in cur.fetchall())


def _attest_builtin_relation_types(cur: Any, relation: str) -> None:
    schema_name, object_name = relation.split(".", 1)
    cur.execute(
        "SELECT a.attname, tn.nspname, t.typname "
        "FROM pg_catalog.pg_attribute a "
        "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
        "JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace "
        "WHERE n.nspname = %s AND c.relname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped",
        (schema_name, object_name),
    )
    rows = cur.fetchall()
    if not rows or any(type_schema != "pg_catalog" for _name, type_schema, _type in rows):
        raise ReadSurfacePolicyError(
            "sql_source_type_forbidden",
            f"source relation {relation!r} contains a non-built-in or unknown type",
        )


def _policy_tenant_values(expression: str) -> frozenset[str]:
    from sqlglot import exp

    from okg.substrate.postgres_sql import canonical_identifier, parse

    try:
        parsed = parse(f"SELECT 1 WHERE {expression}")
        if len(parsed.statements) != 1:
            raise ValueError("expected one statement")
        where = parsed.statements[0].args.get("where")
        if where is None:
            raise ValueError("no WHERE clause")
        condition = where.this
    except Exception as exc:
        raise ReadSurfacePolicyError(
            "sql_source_policy_expression_invalid",
            "database row-security expression could not be parsed",
        ) from exc

    def strip_parens(node: Any) -> Any:
        while isinstance(node, exp.Paren):
            node = node.this
        return node

    def unwrap(node: Any) -> Any:
        node = strip_parens(node)
        if isinstance(node, exp.Cast):
            target, _rendered = _cast_target(node.args.get("to"))
            if target != "text":
                raise ValueError("policy casts must resolve to pg_catalog.text")
            node = strip_parens(node.this)
            if isinstance(node, exp.Cast):
                raise ValueError("nested policy casts are forbidden")
        return node

    def is_equality(node: Any) -> bool:
        if isinstance(node, exp.EQ):
            return True
        if isinstance(node, exp.Operator):
            schema_text, _, operator = str(node.args.get("operator") or "").rpartition(".")
            schema_text = schema_text.strip()
            if len(schema_text) > 1 and schema_text[0] == schema_text[-1] == '"':
                schema = canonical_identifier(
                    schema_text[1:-1].replace('""', '"'), quoted=True
                )
            else:
                schema = canonical_identifier(schema_text, quoted=False)
            return schema == "pg_catalog" and operator.strip() == "="
        return False

    def values(node: Any) -> frozenset[str]:
        node = strip_parens(node)
        if isinstance(node, exp.Or):
            return values(node.this) | values(node.expression)
        if isinstance(node, exp.Connector) or isinstance(node, exp.Not):
            raise ValueError("only OR between closed tenant equalities is admitted")
        if not is_equality(node):
            raise ValueError("policy term is not an equality")
        left = unwrap(node.this)
        right = unwrap(node.expression)
        if (
            not isinstance(left, exp.Column)
            or len(left.parts) != 1
            or _dotted(left) != ("tenant_id",)
            or not isinstance(right, exp.Literal)
            or not right.is_string
        ):
            raise ValueError("policy must compare tenant_id to a string literal")
        return frozenset({str(right.this)})

    try:
        return values(condition)
    except (IndexError, TypeError, ValueError) as exc:
        raise ReadSurfacePolicyError(
            "sql_source_policy_expression_invalid",
            "row security must be a closed OR of tenant_id literal equalities",
        ) from exc


def sql_source_policy_name(plan: SqlReadModelPlan, relation: str) -> str:
    digest = next(
        (item[2] for item in plan.source_policy_enforcements if item[0] == relation),
        None,
    )
    if digest is None:
        raise ValueError(f"relation {relation!r} is not in the admitted plan")
    return "okg_read_" + digest.removeprefix("sha256:")[:24]


def _attest_policy_dependencies(cur: Any, policy_oid: int) -> None:
    cur.execute(
        "SELECT n.nspname FROM pg_catalog.pg_depend d "
        "JOIN pg_catalog.pg_operator o ON d.refclassid = 'pg_operator'::regclass "
        " AND d.refobjid = o.oid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = o.oprnamespace "
        "WHERE d.classid = 'pg_policy'::regclass AND d.objid = %s",
        (policy_oid,),
    )
    if any(namespace != "pg_catalog" for (namespace,) in cur.fetchall()):
        raise ReadSurfacePolicyError(
            "sql_source_policy_expression_invalid",
            "row-security policy resolved a non-catalog operator",
        )
    cur.execute(
        "SELECT n.nspname, p.provolatile, p.prosecdef FROM pg_catalog.pg_depend d "
        "JOIN pg_catalog.pg_proc p ON d.refclassid = 'pg_proc'::regclass "
        " AND d.refobjid = p.oid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
        "WHERE d.classid = 'pg_policy'::regclass AND d.objid = %s",
        (policy_oid,),
    )
    if any(
        namespace != "pg_catalog" or volatility == "v" or security_definer
        for namespace, volatility, security_definer in cur.fetchall()
    ):
        raise ReadSurfacePolicyError(
            "sql_source_policy_expression_invalid",
            "row-security policy resolved a non-catalog or privileged routine",
        )


def _attest_source_policy_enforcement(cur: Any, plan: SqlReadModelPlan) -> None:
    policies = dict(plan.source_policies)
    for relation, enforcement, _digest in plan.source_policy_enforcements:
        schema_name, object_name = relation.split(".", 1)
        cur.execute(
            "SELECT c.oid, c.relrowsecurity, c.relforcerowsecurity, c.relkind "
            "FROM pg_catalog.pg_class c "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema_name, object_name),
        )
        row = cur.fetchone()
        if row is None:
            raise ReadSurfacePolicyError(
                "sql_source_policy_enforcement_missing",
                f"policy-enforcing source relation {relation!r} does not exist",
            )
        relation_oid, row_security, force_row_security, relkind = row
        if (
            enforcement != "row-security"
            or row_security is not True
            or force_row_security is not True
            or relkind not in {"r", "p"}
        ):
            raise ReadSurfacePolicyError(
                "sql_source_policy_enforcement_missing",
                f"source relation {relation!r} does not force row security",
            )
        _attest_builtin_relation_types(cur, relation)
        cur.execute(
            "SELECT p.oid, p.polname, p.polcmd, p.polpermissive, "
            "ARRAY(SELECT r.rolname FROM pg_catalog.pg_roles r "
            "      WHERE r.oid = ANY(p.polroles) ORDER BY r.rolname), "
            "pg_catalog.pg_get_expr(p.polqual, p.polrelid), "
            "pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) "
            "FROM pg_catalog.pg_policy p WHERE p.polrelid = %s",
            (relation_oid,),
        )
        policy_rows = cur.fetchall()
        expected_policy = policies[relation]
        if len(policy_rows) != 1:
            raise ReadSurfacePolicyError(
                "sql_source_policy_attestation_invalid",
                f"source relation {relation!r} must have exactly one closed read policy",
            )
        policy_oid, name, command, permissive, roles, qualifier, with_check = policy_rows[0]
        _attest_policy_dependencies(cur, int(policy_oid))
        if (
            name != sql_source_policy_name(plan, relation)
            or command != "r"
            or permissive is not True
            or tuple(roles or ()) != (plan.maintenance_role,)
            or with_check is not None
            or _policy_tenant_values(str(qualifier)) != expected_policy.tenants
        ):
            raise ReadSurfacePolicyError(
                "sql_source_policy_attestation_invalid",
                f"source relation {relation!r} policy does not match the digest-bound tenant/role grant",
            )


def _attest_view_dependencies(cur: Any, plan: SqlReadModelPlan) -> None:
    schema_name, object_name = plan.output_object.split(".", 1)
    cur.execute(
        "SELECT DISTINCT pn.nspname, p.provolatile, p.prosecdef "
        "FROM pg_catalog.pg_rewrite rw "
        "JOIN pg_catalog.pg_class c ON c.oid = rw.ev_class "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "JOIN pg_catalog.pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = rw.oid "
        "JOIN pg_catalog.pg_proc p ON d.refclassid = 'pg_proc'::regclass AND d.refobjid = p.oid "
        "JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema_name, object_name),
    )
    if any(namespace != "pg_catalog" or volatility == "v" or security_definer
           for namespace, volatility, security_definer in cur.fetchall()):
        raise ReadSurfacePolicyError(
            "sql_resolved_routine_forbidden",
            "materialized view resolved a non-catalog, volatile, or security-definer routine",
        )


def _attest_output_acl(cur: Any, plan: SqlReadModelPlan) -> None:
    schema_name, object_name = plan.output_object.split(".", 1)
    cur.execute(
        "SELECT COALESCE(r.rolname, 'PUBLIC'), acl.privilege_type "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE("
        "c.relacl, pg_catalog.acldefault('r', c.relowner))) acl "
        "LEFT JOIN pg_catalog.pg_roles r ON r.oid = acl.grantee "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema_name, object_name),
    )
    unexpected = [
        (str(grantee), str(privilege))
        for grantee, privilege in cur.fetchall()
        if grantee != plan.maintenance_role
        and (grantee != plan.application_role or privilege != "SELECT")
    ]
    if unexpected:
        raise ReadSurfacePolicyError(
            "sql_output_acl_invalid",
            "materialized output has grants outside the restricted application reader",
            data={"unexpected_grants": unexpected},
        )


def _attest_output_identity(cur: Any, plan: SqlReadModelPlan) -> str:
    """Bind the live view object and resolved dependency closure to its plan."""

    schema_name, object_name = plan.output_object.split(".", 1)
    cur.execute(
        "SELECT c.oid, c.relkind, pg_catalog.pg_get_userbyid(c.relowner), "
        "       COALESCE(c.reloptions, ARRAY[]::text[]), "
        "       pg_catalog.pg_get_viewdef(c.oid, true) "
        "FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema_name, object_name),
    )
    row = cur.fetchone()
    if row is None:
        raise ReadSurfacePolicyError(
            "sql_output_identity_invalid",
            "materialized output view is missing",
        )
    view_oid, relation_kind, owner, options, view_definition = row
    if (
        relation_kind != "v"
        or owner != plan.maintenance_role
        or "security_barrier=true" not in set(options or ())
        or not isinstance(view_definition, str)
    ):
        raise ReadSurfacePolicyError(
            "sql_output_identity_invalid",
            "materialized output kind, owner, or security barrier does not match the admitted plan",
        )
    cur.execute(
        "SELECT DISTINCT source_ns.nspname || '.' || source.relname "
        "FROM pg_catalog.pg_rewrite rw "
        "JOIN pg_catalog.pg_depend d "
        "  ON d.classid = 'pg_rewrite'::regclass AND d.objid = rw.oid "
        "JOIN pg_catalog.pg_class source "
        "  ON d.refclassid = 'pg_class'::regclass AND d.refobjid = source.oid "
        "JOIN pg_catalog.pg_namespace source_ns ON source_ns.oid = source.relnamespace "
        "WHERE rw.ev_class = %s AND source.oid <> %s "
        "  AND source.relkind IN ('r','p','v','m','f')",
        (view_oid, view_oid),
    )
    observed_sources = {str(value) for (value,) in cur.fetchall()}
    if observed_sources != set(plan.source_relations):
        raise ReadSurfacePolicyError(
            "sql_output_identity_invalid",
            "materialized output dependencies no longer match the admitted source closure",
            data={
                "expected": list(plan.source_relations),
                "observed": sorted(observed_sources),
            },
        )
    _attest_view_dependencies(cur, plan)
    return _sha256(view_definition)


def execute_sql_read_query(
    conn: psycopg.Connection[Any],
    plan: SqlReadModelPlan,
    materialization: SqlViewMaterializationReceipt,
    *,
    statement_timeout_ms: int = 5_000,
) -> SqlReadResult:
    """Read the validated materialized output under the application role."""

    if plan.parameters:
        raise ReadSurfacePolicyError(
            "sql_application_parameters_require_broker",
            "parameterized application reads require a policy-enforcing read broker",
        )
    if (
        materialization.asset_id != plan.asset_id
        or materialization.asset_digest != plan.asset_digest
        or materialization.sql_digest != plan.sql_digest
        or materialization.database_object != plan.output_object
        or materialization.maintenance_role != plan.maintenance_role
    ):
        raise ReadSurfacePolicyError(
            "sql_output_identity_invalid",
            "materialization receipt does not match the admitted SQL plan",
        )
    with conn.transaction():
        with conn.cursor() as cur:
            _attest_limited_role(
                cur,
                plan.application_role,
                allowed_select_relations=(plan.output_object,),
                create_schema=None,
                require_select_relations=(plan.output_object,),
            )
            schema_name, object_name = plan.output_object.split(".", 1)
            actual_columns = _relation_columns(cur, schema_name, object_name)
            if actual_columns != plan.result_columns:
                raise ReadSurfacePolicyError(
                    "sql_result_shape_mismatch",
                    "materialized result names/types do not match the admitted contract",
                    data={"expected": list(plan.result_columns), "observed": list(actual_columns)},
                )
            _attest_output_acl(cur, plan)
            if _attest_output_identity(cur, plan) != materialization.view_definition_digest:
                raise ReadSurfacePolicyError(
                    "sql_output_identity_invalid",
                    "materialized view definition changed after its admitted receipt",
                )
            cur.execute(
                psycopg_sql.SQL("SET LOCAL ROLE {}").format(
                    psycopg_sql.Identifier(plan.application_role)
                )
            )
            cur.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{int(statement_timeout_ms)}ms",),
            )
            cur.execute(
                psycopg_sql.SQL("SET LOCAL search_path TO pg_catalog, {}").format(
                    psycopg_sql.Identifier(plan.distribution_namespace)
                )
            )
        with conn.cursor() as cur:
            schema_name, object_name = plan.output_object.split(".", 1)
            selected_columns = psycopg_sql.SQL(", ").join(
                psycopg_sql.Identifier(name) for name, _type_name in plan.result_columns
            )
            cur.execute(
                psycopg_sql.SQL("SELECT {} FROM {}.{}").format(
                    selected_columns,
                    psycopg_sql.Identifier(schema_name),
                    psycopg_sql.Identifier(object_name),
                )
            )
            columns = tuple(item.name for item in (cur.description or ()))
            expected_columns = tuple(name for name, _type_name in plan.result_columns)
            if columns != expected_columns:
                raise ReadSurfacePolicyError(
                    "sql_result_shape_mismatch",
                    "database result columns do not match the declared result contract",
                    data={"expected": list(expected_columns), "observed": list(columns)},
                )
            rows = tuple(
                MappingProxyType(dict(zip(columns, values, strict=True)))
                for values in cur.fetchall()
            )
            cur.execute("RESET ROLE")
    return SqlReadResult(
        columns=columns,
        rows=rows,
        execution_role=plan.application_role,
        asset_id=plan.asset_id,
        asset_digest=plan.asset_digest,
    )


def materialize_sql_view(
    conn: psycopg.Connection[Any],
    plan: SqlReadModelPlan,
) -> SqlViewMaterializationReceipt:
    """Create/replace exactly the validated owned view; no grants or other DDL."""

    if plan.parameters:
        raise ReadSurfacePolicyError(
            "sql_materialized_parameters_forbidden",
            "a persistent SQL view cannot contain instance bind values; execute the parameterized read instead",
        )
    schema_name, object_name = plan.output_object.split(".", 1)
    with conn.transaction():
        with conn.cursor() as cur:
            _attest_limited_role(
                cur,
                plan.maintenance_role,
                allowed_select_relations=(*plan.source_relations, plan.output_object),
                create_schema=plan.distribution_namespace,
                require_select_relations=plan.source_relations,
                allowed_write_relations=(plan.output_object,),
            )
            _attest_limited_role(
                cur,
                plan.application_role,
                allowed_select_relations=(plan.output_object,),
                create_schema=None,
            )
            _attest_source_policy_enforcement(cur, plan)
            previous_columns = _relation_columns(cur, schema_name, object_name)
            require_result_shape_migration(
                previous_columns,
                plan.result_columns,
                migration_id=plan.migration_id,
            )
            cur.execute(
                psycopg_sql.SQL("SET LOCAL ROLE {}").format(
                    psycopg_sql.Identifier(plan.maintenance_role)
                )
            )
            cur.execute(
                psycopg_sql.SQL("SET LOCAL search_path TO pg_catalog, {}").format(
                    psycopg_sql.Identifier(plan.distribution_namespace)
                )
            )
            cur.execute(
                psycopg_sql.SQL(
                    "CREATE OR REPLACE VIEW {}.{} WITH (security_barrier=true) AS "
                ).format(
                    psycopg_sql.Identifier(schema_name),
                    psycopg_sql.Identifier(object_name),
                )
                + psycopg_sql.SQL(plan.normalized_sql)
            )
            actual_columns = _relation_columns(cur, schema_name, object_name)
            if actual_columns != plan.result_columns:
                raise ReadSurfacePolicyError(
                    "sql_result_shape_mismatch",
                    "created view names/types do not match the admitted result contract",
                    data={"expected": list(plan.result_columns), "observed": list(actual_columns)},
                )
            view_definition_digest = _attest_output_identity(cur, plan)
            _attest_output_acl(cur, plan)
    return SqlViewMaterializationReceipt(
        asset_id=plan.asset_id,
        asset_digest=plan.asset_digest,
        sql_digest=plan.sql_digest,
        database_object=plan.output_object,
        maintenance_role=plan.maintenance_role,
        view_definition_digest=view_definition_digest,
    )


def build_sql_lineage_envelope(
    plan: SqlReadModelPlan,
    *,
    source_records: Sequence[SourceRecordLineage],
    graph_generation_id: str | int,
    catalog_version_id: str | int,
    output_digest: str,
) -> MaterializedLineageEnvelope:
    envelope = build_output_lineage_envelope(
        output_id=plan.output_object,
        output_kind="derived_relation",
        source_records=source_records,
        component_id=plan.asset_id,
        component_hash=plan.asset_digest,
        component_kind="transform",
        output_digest=output_digest,
        catalog_version_id=catalog_version_id,
        graph_generation_id=graph_generation_id,
        decision_ids=plan.policy.decision_ids,
        metadata={
            "asset_id": plan.asset_id,
            "asset_digest": plan.asset_digest,
            "sql_digest": plan.sql_digest,
            "source_relations": list(plan.source_relations),
            "source_asset_ids": list(plan.source_asset_ids),
            "policy": plan.policy.to_dict(),
            "maintenance_role": plan.maintenance_role,
            "application_role": plan.application_role,
        },
    )
    require_sql_lineage_closure(plan, envelope)
    return envelope


def _materialize_and_publish_sql_read_model(
    conn: psycopg.Connection[Any],
    plan: SqlReadModelPlan,
    *,
    deployment_name: str,
    source_records: Sequence[SourceRecordLineage],
    graph_generation_id: int,
    catalog_version_id: str | int,
) -> SqlReadPublicationReceipt:
    """Couple admission output, DDL, restricted read, lineage, and publication."""

    if not source_records:
        raise ReadSurfacePolicyError(
            "sql_lineage_source_records_missing",
            "SQL read publication requires authoritative source-record lineage",
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata::jsonb ->> 'status',
                   metadata::jsonb ->> 'catalog_version_id'
              FROM public._chronos_branch_interval_checkpoints
             WHERE metadata::jsonb ->> 'kind' = 'okg_generation'
               AND metadata::jsonb ->> 'deployment_name' = %s
               AND metadata::jsonb ->> 'branch' = 'default'
               AND metadata::jsonb ->> 'target_generation_id' = %s
             ORDER BY created_at DESC
             LIMIT 1
             FOR SHARE
            """,
            (deployment_name, str(graph_generation_id)),
        )
        generation = cur.fetchone()
    if generation is None or generation[0] != "published":
        raise ReadSurfacePolicyError(
            "sql_generation_not_published",
            "SQL read publication requires an existing published generation for the deployment",
            data={
                "deployment_name": deployment_name,
                "generation_id": graph_generation_id,
                "observed_status": None if generation is None else generation[0],
            },
        )
    if str(generation[1]) != str(catalog_version_id):
        raise ReadSurfacePolicyError(
            "sql_generation_catalog_mismatch",
            "SQL read publication catalog version does not match the published generation",
            data={
                "generation_id": graph_generation_id,
                "expected_catalog_version_id": str(generation[1]),
                "observed_catalog_version_id": str(catalog_version_id),
            },
        )
    materialization = materialize_sql_view(conn, plan)
    read_result = execute_sql_read_query(conn, plan, materialization)
    rows = tuple(dict(row) for row in read_result.rows)
    output_digest = stable_hash(rows)
    lineage = build_sql_lineage_envelope(
        plan,
        source_records=source_records,
        graph_generation_id=graph_generation_id,
        catalog_version_id=catalog_version_id,
        output_digest=output_digest,
    )
    definition = RelationArtifactDefinition(
        definition_id=f"sql-read-model:{plan.asset_id}",
        deployment_name=deployment_name,
        logical_relation_id=plan.output_object,
        compute_hash=stable_hash({"sql": plan.sql_digest}),
        output_schema_hash=stable_hash(plan.result_columns),
        renderer_hash=stable_hash("sql-read-model-jsonb-v1"),
        dirty_rule_hash=stable_hash({"sources": plan.source_relations}),
        output_contract={
            "columns": list(plan.result_columns),
            "asset_id": plan.asset_id,
            "asset_digest": plan.asset_digest,
        },
        dirty_rule={"source_relations": list(plan.source_relations)},
    )
    relation_rows = tuple(
        RelationRow(
            scope_key=str(index),
            row_key=str(index),
            row_data=row,
            dependency_hash=lineage.envelope_hash,
        )
        for index, row in enumerate(rows)
    )
    publication = publish_relation_artifact(
        conn,
        definition,
        target_generation_id=int(graph_generation_id),
        rows=relation_rows,
        trigger_kind="post_publish",
        lineage_envelopes=(lineage,),
        metrics={
            "asset_id": plan.asset_id,
            "asset_digest": plan.asset_digest,
            "maintenance_role": plan.maintenance_role,
            "application_role": plan.application_role,
        },
    )
    if publication.lineage_envelope_count != 1:
        raise ReadSurfacePolicyError(
            "sql_lineage_publication_incomplete",
            "derived publication did not persist the admitted lineage envelope",
        )
    return SqlReadPublicationReceipt(
        materialization=materialization,
        read_result=read_result,
        lineage=lineage,
        publication=publication,
    )


def materialize_and_publish_sql_read_model(
    conn: psycopg.Connection[Any],
    plan: SqlReadModelPlan,
    *,
    deployment_name: str,
    source_records: Sequence[SourceRecordLineage],
    graph_generation_id: int,
    catalog_version_id: str | int,
) -> SqlReadPublicationReceipt:
    """Atomically couple DDL, restricted read, lineage, and publication."""

    with conn.transaction():
        return _materialize_and_publish_sql_read_model(
            conn,
            plan,
            deployment_name=deployment_name,
            source_records=source_records,
            graph_generation_id=graph_generation_id,
            catalog_version_id=catalog_version_id,
        )


def require_sql_lineage_closure(
    plan: SqlReadModelPlan,
    envelope: MaterializedLineageEnvelope,
) -> None:
    metadata_relations = set(envelope.metadata.get("source_relations") or ())
    metadata_assets = set(envelope.metadata.get("source_asset_ids") or ())
    if metadata_relations != set(plan.source_relations) or metadata_assets != set(plan.source_asset_ids):
        raise ReadSurfacePolicyError(
            "sql_lineage_closure_incomplete",
            "materialized lineage does not close over every referenced relation asset",
            data={
                "missing_relations": sorted(set(plan.source_relations) - metadata_relations),
                "missing_assets": sorted(set(plan.source_asset_ids) - metadata_assets),
            },
        )


__all__ = [
    "SQL_READ_MODEL_SCHEMA",
    "ReadPolicyRequirements",
    "ReadSurfacePolicyError",
    "NativeViewReadResult",
    "SqlReadModelPlan",
    "SqlReadPublicationReceipt",
    "SqlReadResult",
    "SqlValueParameter",
    "SqlViewMaterializationReceipt",
    "build_sql_lineage_envelope",
    "build_sql_read_model_asset",
    "compose_non_widening_policy",
    "execute_sql_read_query",
    "materialize_sql_view",
    "materialize_and_publish_sql_read_model",
    "read_governed_native_view",
    "require_non_widening_policy",
    "require_result_shape_migration",
    "require_sql_lineage_closure",
    "sql_source_policy_name",
    "validate_sql_read_model",
]
