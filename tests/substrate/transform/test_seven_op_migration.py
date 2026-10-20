"""task.gtir.ledger-and-migration -- seven-op IR lowers to transform; both in the ledger.

Structural: a seven-op AKMON search program lowers to a transform program with
equivalent FROM/SEARCH structure (per the migration table). Ledger (graph): both
the seven-op and transform forms resolve in okg.v_ir_programs under their
program_kind, checksum dedupe holds, and a transform invocation shows in
okg.v_ir_invocations.
"""

from __future__ import annotations

import pytest

from okg.optimizer import lower_akmon_call
from okg.optimizer.akmon import AKMONCall, AKMONOperator
from okg.substrate.transform import OpKind, no_dangling_edges_program
from okg.substrate.transform.migrate import (
    lower_seven_op_to_transform,
    seven_op_kind_correspondence,
)


def _seven_op_search():
    return lower_akmon_call(
        AKMONCall(AKMONOperator.SEARCH, {"q": "hello", "k": 10}), program_id="akmon.search.mig"
    )


def test_seven_op_search_lowers_to_transform_with_from_and_search():
    seven = _seven_op_search()
    transform = lower_seven_op_to_transform(seven)
    kinds = [b.op.kind for b in transform.blocks[0].bindings]
    assert OpKind.FROM in kinds        # READ -> FROM
    assert OpKind.SEARCH in kinds      # APPLY(akmon.search) -> SEARCH
    corr = seven_op_kind_correspondence(seven, transform)
    assert ("read", "from") in corr
    assert ("apply", "search") in corr
    # The lowered program preserves the original's outputs and is content-addressed.
    assert transform.outputs == seven.outputs
    assert transform.checksum().startswith("sha256:")


@pytest.mark.postgres
def test_seven_op_and_transform_coreside_in_ledger(clean_runtime: str):
    import psycopg

    from okg.substrate.ir_ledger import persist_ir_program, record_ir_invocation
    from okg.substrate.transform.commit import persist_transform_program

    seven = _seven_op_search()
    transform = lower_seven_op_to_transform(seven)
    # A consistency program is expressible as a transform too.
    consistency = no_dangling_edges_program()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seven_id = persist_ir_program(conn, seven, program_kind="read")
        transform_id = persist_transform_program(conn, transform)
        consistency_id = persist_transform_program(conn, consistency)
        # Checksum dedupe: re-persisting the same transform returns the same row.
        transform_id_again = persist_transform_program(conn, transform)
        invocation_id = record_ir_invocation(
            conn, program_id=transform_id, deployment_name="test",
            invocation_kind="commit", result_count=1, node_delta_count=0,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT program_id, program_kind FROM okg.v_ir_programs "
                "WHERE program_id = ANY(%s::uuid[])",
                ([seven_id, transform_id, consistency_id],),
            )
            kinds = {str(pid): kind for pid, kind in cur.fetchall()}
            cur.execute(
                "SELECT count(*) FROM okg.v_ir_invocations WHERE invocation_id = %s::uuid",
                (invocation_id,),
            )
            invocation_visible = cur.fetchone()[0]

    assert transform_id_again == transform_id                  # checksum dedupe
    assert kinds[seven_id] == "read"                           # seven-op resolves
    assert kinds[transform_id] == "transform"                  # transform resolves
    assert kinds[consistency_id] == "transform"                # consistency as transform
    assert invocation_visible == 1                             # invocation in v_ir_invocations

    import json
    import pathlib

    rep = pathlib.Path(__file__).resolve().parents[3] / ".scratch" / "gtir_ledger_graph.json"
    rep.parent.mkdir(exist_ok=True)
    rep.write_text(json.dumps({"evidence": [{
        "kind": "graph",
        "status": "pass",
        "refs": ["req.gtir.ledger-reuse-and-migration"],
        "summary": (
            f"seven-op ({seven_id[:8]}, kind=read) + transform ({transform_id[:8]}, "
            "kind=transform) co-resident in okg.v_ir_programs; checksum dedupe holds; "
            "transform invocation visible in okg.v_ir_invocations"
        ),
        "command": "uv run --extra dev python -m pytest tests/substrate/transform/test_seven_op_migration.py",
        "data": {
            "seven_op_id": seven_id,
            "transform_id": transform_id,
            "dedupe": transform_id_again,
        },
    }]}, indent=2))
