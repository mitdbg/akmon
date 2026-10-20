"""proof_kind discriminator: report rows must say how strong the proof is.

The audit found every formal report row -- a TLC fixpoint proof, a Python
executable-model floor check, and a faked-TLC fixture -- emitted identical
status/fresh/proof_status with no way to tell them apart. These tests pin the
proof_kind field and its ordering so a consumer can require a minimum strength.
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.formal_tlc import (
    PROOF_KIND_EXECUTABLE_MODEL,
    PROOF_KIND_REPORT_SCHEMA,
    PROOF_KIND_TLC_FIXPOINT,
    TLCProperty,
    build_formal_report_rows,
    proof_kind_satisfies,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_tlc_rows_default_to_tlc_fixpoint(tmp_path: Path) -> None:
    artifact = tmp_path / "tlc.stdout.log"
    artifact.write_text("Model checking completed. No error has been found.\n")
    model = tmp_path / "M.tla"
    model.write_text("---- MODULE M ----\n====\n")
    cfg = tmp_path / "M.cfg"
    cfg.write_text("SPECIFICATION Spec\n")
    rows = build_formal_report_rows(
        properties=[TLCProperty("P", "summary")],
        status="pass",
        checker_command="x",
        command=("tlc",),
        artifact=artifact,
        model_path=model,
        config_path=cfg,
    )
    assert rows[0]["proof_kind"] == PROOF_KIND_TLC_FIXPOINT


def test_proof_kind_is_explicit_per_row(tmp_path: Path) -> None:
    artifact = tmp_path / "a.log"
    artifact.write_text("x")
    model = tmp_path / "M.tla"
    model.write_text("x")
    cfg = tmp_path / "M.cfg"
    cfg.write_text("x")
    rows = build_formal_report_rows(
        properties=[TLCProperty("P", "s")],
        status="pass",
        checker_command="x",
        command=("c",),
        artifact=artifact,
        model_path=model,
        config_path=cfg,
        proof_kind=PROOF_KIND_EXECUTABLE_MODEL,
    )
    assert rows[0]["proof_kind"] == PROOF_KIND_EXECUTABLE_MODEL


def test_proof_kind_ordering() -> None:
    # A stronger proof satisfies a weaker requirement, never the reverse.
    assert proof_kind_satisfies(PROOF_KIND_TLC_FIXPOINT, PROOF_KIND_EXECUTABLE_MODEL)
    assert proof_kind_satisfies(PROOF_KIND_TLC_FIXPOINT, PROOF_KIND_TLC_FIXPOINT)
    assert not proof_kind_satisfies(PROOF_KIND_EXECUTABLE_MODEL, PROOF_KIND_TLC_FIXPOINT)
    assert not proof_kind_satisfies(PROOF_KIND_REPORT_SCHEMA, PROOF_KIND_EXECUTABLE_MODEL)
    # A missing/unknown proof_kind never satisfies a real requirement.
    assert not proof_kind_satisfies(None, PROOF_KIND_TLC_FIXPOINT)
    assert not proof_kind_satisfies("bogus", PROOF_KIND_EXECUTABLE_MODEL)
    # No requirement is always satisfied.
    assert proof_kind_satisfies(None, None)
