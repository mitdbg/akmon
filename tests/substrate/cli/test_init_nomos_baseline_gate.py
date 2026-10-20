"""Issue #2281: init seeds the Nomos baseline under the rule doctor uses.

``okg doctor`` (through ``deployment apply``'s plan) calls the Nomos
baseline required whenever the mode is not ``off``. Init used to seed it only
in ``enforce`` mode, so a fresh install of the academic-researcher profile,
which then shipped ``runtime_enforcement: audit``, left the plan's
``nomos_baseline`` stage unconverged and doctor not ok.

These tests (Postgres-gated) build a deployment with the profile's own
``nomos`` block, run init's seed step against a migrated test database, and
read the result back through ``reconcile_deployment_baseline`` -- the same
call the apply plan makes for its ``nomos_baseline`` stage.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml

import okg.substrate.library as library
from okg.substrate.cli.init import _seed_init_nomos_baseline_if_required
from okg.substrate.nomos.baseline import reconcile_deployment_baseline

pytestmark = pytest.mark.postgres

DEPLOYMENT = "issue2281"
PROFILE_DEFAULTS = (
    Path(library.__file__).parent
    / "profiles"
    / "academic-researcher"
    / "deployment-defaults.yaml"
)


def _profile_nomos_block() -> dict[str, Any]:
    defaults = yaml.safe_load(PROFILE_DEFAULTS.read_text(encoding="utf-8"))
    nomos = dict(defaults["nomos"])
    # Guard the premise: the shipped profile is off since issue #3414 (no
    # access rules declared); each test sets the mode it exercises, as an
    # operator opting in to audit or enforce would.
    assert nomos["runtime_enforcement"] == "off"
    return nomos


def _write_deployment(tmp_path: Path, mode: str) -> Path:
    deployment_dir = tmp_path / DEPLOYMENT
    deployment_dir.mkdir()
    nomos = _profile_nomos_block()
    nomos["runtime_enforcement"] = mode
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": DEPLOYMENT,
                "source_registry": "source_registry.yaml",
                "nomos": nomos,
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n", encoding="utf-8",
    )
    return deployment_dir


def _active_policy_rows(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.nomos_policy_activations"
            " WHERE deployment_name = %s",
            (DEPLOYMENT,),
        )
        row = cur.fetchone()
    return int(row[0]) if row else 0


def _plan_stage_needed(state: dict[str, Any]) -> bool:
    # The apply plan's nomos_baseline row (deployment_apply.plan_deployment_apply).
    return bool(state.get("required") and not state.get("converged"))


@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_fresh_init_seeds_baseline_and_plan_converges(
    mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clean_runtime: str,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    dsn = clean_runtime
    deployment_dir = _write_deployment(tmp_path, mode)

    before = reconcile_deployment_baseline(
        deployment=DEPLOYMENT, deployment_dir=deployment_dir, dsn=dsn,
    )
    assert before["required"] is True
    assert _plan_stage_needed(before) is True
    assert _active_policy_rows(dsn) == 0

    seeded = _seed_init_nomos_baseline_if_required(
        deployment_name=DEPLOYMENT,
        deployment_dir=deployment_dir,
        dsn=dsn,
    )

    assert seeded is not None, f"init did not seed the baseline in {mode} mode"
    after = reconcile_deployment_baseline(
        deployment=DEPLOYMENT, deployment_dir=deployment_dir, dsn=dsn,
    )
    assert after["converged"] is True, after
    assert _plan_stage_needed(after) is False
    assert _active_policy_rows(dsn) >= 1


def test_fresh_init_in_off_mode_seeds_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    clean_runtime: str,
) -> None:
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    dsn = clean_runtime
    deployment_dir = _write_deployment(tmp_path, "off")

    seeded = _seed_init_nomos_baseline_if_required(
        deployment_name=DEPLOYMENT,
        deployment_dir=deployment_dir,
        dsn=dsn,
    )

    assert seeded is None
    state = reconcile_deployment_baseline(
        deployment=DEPLOYMENT, deployment_dir=deployment_dir, dsn=dsn,
    )
    assert state["required"] is False
    assert _plan_stage_needed(state) is False
    assert _active_policy_rows(dsn) == 0
