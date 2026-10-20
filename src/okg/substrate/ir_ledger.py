"""Graph-wide IR ledger persistence helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from okg.optimizer import IRBinding, IRFunctionRegistry, IRProgram


@dataclass(frozen=True)
class IRProgramLedgerRef:
    program_id: str
    binding_id: str | None = None


def persist_ir_program(
    conn: Any,
    program: IRProgram,
    *,
    program_kind: str,
    registry: IRFunctionRegistry | None = None,
    registry_id: str | None = None,
    registry_version: str | None = None,
    registry_checksum: str | None = None,
    payload_mode: str = "full",
    program_ref: str | None = None,
    cardinality_summary: Mapping[str, Any] | None = None,
) -> str:
    """Persist a content-addressed graph IR program and return program id."""

    summary = _program_summary(program)
    if registry is not None:
        registry_id = registry.registry_id
        registry_version = registry.version
        registry_checksum = registry.checksum()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.ir_programs (
              program_checksum, ir_version, program_kind, registry_id,
              registry_version, registry_checksum, effect_summary,
              determinism, op_counts, function_counts,
              cardinality_summary, payload_mode, program_json, program_ref
            )
            VALUES (
              %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
              %s::jsonb, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s
            )
            ON CONFLICT (program_checksum) DO UPDATE
              SET program_checksum = EXCLUDED.program_checksum
            RETURNING program_id
            """,
            (
                program.checksum(),
                str(program.attrs.get("ir_version") or "okg-ir-v1"),
                program_kind,
                registry_id,
                registry_version,
                registry_checksum,
                Jsonb(summary["effect_summary"]),
                summary["determinism"],
                Jsonb(summary["op_counts"]),
                Jsonb(summary["function_counts"]),
                Jsonb(dict(cardinality_summary or summary["cardinality_summary"])),
                payload_mode,
                Jsonb(program.to_dict() if payload_mode == "full" else {}),
                program_ref,
            ),
        )
        return str(cur.fetchone()[0])


def bind_ir_program(
    conn: Any,
    *,
    program_id: str,
    deployment_name: str,
    owner_kind: str,
    owner_id: str,
    program_role: str,
    generation_id: str | None = None,
    branch_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    """Bind a content-addressed IR program to one graph owner and role."""

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.ir_program_bindings (
              program_id, deployment_name, owner_kind, owner_id,
              program_role, generation_id, branch_name, metadata
            )
            VALUES (
              %s::uuid, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            ON CONFLICT DO NOTHING
            RETURNING binding_id
            """,
            (
                program_id,
                deployment_name,
                owner_kind,
                owner_id,
                program_role,
                generation_id,
                branch_name,
                Jsonb(dict(metadata or {})),
            ),
        )
        row = cur.fetchone()
        if row is not None:
            return str(row[0])
        cur.execute(
            """
            SELECT binding_id
              FROM okg.ir_program_bindings
             WHERE program_id = %s::uuid
               AND deployment_name = %s
               AND owner_kind = %s
               AND owner_id = %s
               AND program_role = %s
               -- Mirrors ir_program_bindings_dedupe_idx exactly (D2: the
               -- generation ref is TEXT, so the sentinel is '' not -1).
               AND COALESCE(generation_id::text, '') = COALESCE(%s::text, '')
               AND COALESCE(branch_name, '') = COALESCE(%s, '')
             ORDER BY created_at
             LIMIT 1
            """,
            (
                program_id,
                deployment_name,
                owner_kind,
                owner_id,
                program_role,
                generation_id,
                branch_name,
            ),
        )
        return str(cur.fetchone()[0])


def ledger_ir_program(
    conn: Any,
    program: IRProgram,
    *,
    program_kind: str,
    deployment_name: str,
    owner_kind: str,
    owner_id: str,
    program_role: str,
    registry: IRFunctionRegistry | None = None,
    generation_id: str | None = None,
    branch_name: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> IRProgramLedgerRef:
    """Persist and bind an IR program in one call."""

    program_id = persist_ir_program(
        conn,
        program,
        program_kind=program_kind,
        registry=registry,
    )
    binding_id = bind_ir_program(
        conn,
        program_id=program_id,
        deployment_name=deployment_name,
        owner_kind=owner_kind,
        owner_id=owner_id,
        program_role=program_role,
        generation_id=generation_id,
        branch_name=branch_name,
        metadata=metadata,
    )
    return IRProgramLedgerRef(program_id=program_id, binding_id=binding_id)


def load_ir_program(conn: Any, program_id: str) -> IRProgram:
    """Load a full-payload IR program from the graph-wide ledger."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT program_json
              FROM okg.ir_programs
             WHERE program_id = %s::uuid
               AND payload_mode = 'full'
            """,
            (program_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(f"IR program not found or not fully persisted: {program_id}")
    return IRProgram.from_dict(row[0])


def record_ir_invocation(
    conn: Any,
    *,
    program_id: str,
    deployment_name: str,
    invocation_kind: str,
    binding_id: str | None = None,
    branch_name: str | None = None,
    generation_id: str | None = None,
    status: str = "succeeded",
    verdict: str | None = None,
    reason: str | None = None,
    result_count: int = 0,
    node_delta_count: int = 0,
    edge_delta_count: int = 0,
    cost_proxy: float | None = None,
    quality_score: float | None = None,
    error_count: int = 0,
    truncation_count: int = 0,
    metrics: Mapping[str, Any] | None = None,
    completed: bool = True,
) -> str:
    """Record one graph-observable IR invocation attempt."""

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.ir_invocations (
              program_id, binding_id, deployment_name, invocation_kind,
              branch_name, generation_id, status, verdict, reason,
              completed_at, result_count, node_delta_count,
              edge_delta_count, cost_proxy, quality_score, error_count,
              truncation_count, metrics
            )
            VALUES (
              %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s,
              CASE WHEN %s THEN now() ELSE NULL END,
              %s, %s, %s, %s, %s, %s, %s, %s::jsonb
            )
            RETURNING invocation_id
            """,
            (
                program_id,
                binding_id,
                deployment_name,
                invocation_kind,
                branch_name,
                generation_id,
                status,
                verdict,
                reason,
                completed,
                result_count,
                node_delta_count,
                edge_delta_count,
                cost_proxy,
                quality_score,
                error_count,
                truncation_count,
                Jsonb(dict(metrics or {})),
            ),
        )
        return str(cur.fetchone()[0])


def _program_summary(program: IRProgram) -> dict[str, Any]:
    bindings = tuple(_iter_bindings(program))
    op_counts: dict[str, int] = {}
    function_counts: dict[str, int] = {}
    for binding in bindings:
        op_name = binding.op.kind.value
        op_counts[op_name] = op_counts.get(op_name, 0) + 1
        function_id = binding.op.args.get("function")
        if isinstance(function_id, str) and function_id:
            function_counts[function_id] = function_counts.get(function_id, 0) + 1
    return {
        "effect_summary": [effect.value for effect in program.effects],
        "determinism": program.determinism.value,
        "op_counts": op_counts,
        "function_counts": function_counts,
        "cardinality_summary": {
            "op_count": len(bindings),
            "block_count": len(program.blocks),
            "output_count": len(program.outputs),
        },
    }


def _iter_bindings(program: IRProgram) -> tuple[IRBinding, ...]:
    bindings = list(program.ops)
    for block in program.blocks:
        bindings.extend(block.bindings)
    return tuple(bindings)
