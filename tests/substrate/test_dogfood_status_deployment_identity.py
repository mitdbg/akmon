"""Deployment identifiers are typed at the CLI boundary (issue #1289).

`okg deployment ready <deployment>` and `okg dogfood status --deployment
<deployment>` take a GRAPH DEPLOYMENT NAME. An ENVIRONMENT ID is a
different kind: the environment `okg-workspace-production` hosts the graph
deployment `okg-workspace`, and every durable row — checkpoints,
ownership, catalog versions, Nomos activations — is keyed by the latter.

`okg dogfood status` forwarded the raw argument into the live checks. The
generation resolver matched none of the 1,329 checkpoints stored under
`okg-workspace`, reported "no latest published generation is available",
and `nomos.source_lineage` failed behind it on the empty generation id.
The command reported EIGHT blockers where the correct name reported
three; five described a read path that was in fact permitting every
standard read subject under enforce mode. That false diagnosis redirected
a recovery toward read denial and deployment corruption while the
published graph stayed readable.

The fix is a typed boundary, not a silent substitution: substituting the
manifest name would stop the fabrication but leave the operator unable to
tell which identity actually ran.

Acceptance, per the issue:
  * wrong-kind input fails immediately, before any health probe
  * the refusal names both identities and gives the exact correct command
  * the deployment name still succeeds
  * an unknown name stays DISTINCT from wrong-kind input
  * JSON never mixes the two identities
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.cli import deployment_identifier as identifier_mod
from okg.substrate.cli import dogfood as dogfood_cli
from okg.substrate.errors import SubstrateError

GRAPH_DEPLOYMENT = "okg-workspace"
ENVIRONMENT_ID = "okg-workspace-production"
UNKNOWN_NAME = "okg-workspace-typo"


@pytest.fixture()
def environments(tmp_path: Path, monkeypatch) -> Path:
    """One environment whose id differs from its graph deployment name."""
    root = tmp_path / "environments"
    descriptor_dir = root / ENVIRONMENT_ID
    descriptor_dir.mkdir(parents=True)
    (descriptor_dir / "environment.yaml").write_text(
        yaml.safe_dump({
            "api_version": "okg.environment/v1",
            "id": ENVIRONMENT_ID,
            "tier": "production",
            "graph_deployment_name": GRAPH_DEPLOYMENT,
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_ENVIRONMENTS_DIR", str(root))
    return root


@pytest.fixture()
def live_checks_ran(tmp_path: Path, monkeypatch) -> dict[str, Any]:
    """Stub the probes and record whether any of them ran at all."""
    deployment_dir = tmp_path / "deployments" / GRAPH_DEPLOYMENT
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": GRAPH_DEPLOYMENT}), encoding="utf-8",
    )
    seen: dict[str, Any] = {"probed": False}

    def _env_status(_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], str]:
        seen["probed"] = True
        return (
            {"ok": True, "deployment_dir": str(deployment_dir)},
            "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
        )

    def _live_checks(**kwargs: Any) -> dict[str, Any]:
        seen["probed"] = True
        seen["deployment"] = kwargs["deployment"]
        return {"checks": [], "blockers": [], "warnings": []}

    monkeypatch.setattr(
        dogfood_cli, "_deployment_ready_env_status", _env_status,
    )
    monkeypatch.setattr(
        dogfood_cli, "_deployment_ready_live_checks", _live_checks,
    )
    return seen


def _run(deployment: str) -> int:
    args = argparse.Namespace(
        deployment=deployment,
        dogfood_subcmd="status",
        details=False,
        skip_doctor=True,
        show_values=False,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    return dogfood_cli._cmd_dogfood_status(args, as_json=True)


# --- the identity boundary itself -----------------------------------


def test_environment_id_is_identified_as_the_wrong_kind(
    environments: Path,
) -> None:
    identified = identifier_mod.environment_identifier_for(ENVIRONMENT_ID)

    assert identified is not None
    assert identified.graph_deployment_name == GRAPH_DEPLOYMENT
    assert identified.supplied == ENVIRONMENT_ID


def test_a_graph_deployment_name_is_not_a_mismatch(
    environments: Path,
) -> None:
    assert identifier_mod.environment_identifier_for(GRAPH_DEPLOYMENT) is None


def test_an_unknown_name_is_not_a_kind_mismatch(environments: Path) -> None:
    """Wrong-kind and no-such-thing must stay distinct causes."""
    assert identifier_mod.environment_identifier_for(UNKNOWN_NAME) is None


def test_an_environment_whose_id_equals_its_deployment_is_not_a_mismatch(
    tmp_path: Path, monkeypatch,
) -> None:
    """Nothing to confuse when the two identities coincide."""
    root = tmp_path / "environments"
    descriptor_dir = root / GRAPH_DEPLOYMENT
    descriptor_dir.mkdir(parents=True)
    (descriptor_dir / "environment.yaml").write_text(
        yaml.safe_dump({
            "api_version": "okg.environment/v1",
            "id": GRAPH_DEPLOYMENT,
            "tier": "production",
            "graph_deployment_name": GRAPH_DEPLOYMENT,
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_ENVIRONMENTS_DIR", str(root))

    assert identifier_mod.environment_identifier_for(GRAPH_DEPLOYMENT) is None


def test_an_unreadable_descriptor_does_not_fail_closed(
    tmp_path: Path, monkeypatch,
) -> None:
    """A broken descriptor must not block readiness for a healthy name."""
    root = tmp_path / "environments"
    descriptor_dir = root / ENVIRONMENT_ID
    descriptor_dir.mkdir(parents=True)
    (descriptor_dir / "environment.yaml").write_text(
        "{{ not: valid: yaml", encoding="utf-8",
    )
    monkeypatch.setenv("OKG_ENVIRONMENTS_DIR", str(root))

    assert identifier_mod.environment_identifier_for(ENVIRONMENT_ID) is None


def test_the_refusal_names_both_identities_and_the_command(
    environments: Path,
) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        identifier_mod.require_graph_deployment_name(
            ENVIRONMENT_ID,
            correct_command="uv run okg dogfood status --deployment {deployment}",
        )

    error = excinfo.value
    assert ENVIRONMENT_ID in str(error)
    assert GRAPH_DEPLOYMENT in str(error)
    # The exact command, rendered — not a description of one.
    assert (
        f"uv run okg dogfood status --deployment {GRAPH_DEPLOYMENT}"
        in str(error.fix_hint)
    )

    data = error.data
    assert data["reason_code"] == identifier_mod.IDENTIFIER_KIND_MISMATCH
    # The two identities are bound under SEPARATE keys.
    assert data["environment_id"] == ENVIRONMENT_ID
    assert data["graph_deployment_name"] == GRAPH_DEPLOYMENT
    assert data["supplied_identifier_kind"] == "environment_id"
    assert data["expected_identifier_kind"] == "graph_deployment_name"


# --- the command surface --------------------------------------------


def test_dogfood_status_refuses_before_any_probe_runs(
    environments: Path, live_checks_ran: dict[str, Any], capsys,
) -> None:
    """No source, policy, worker or generation blocker after the boundary."""
    exit_code = _run(ENVIRONMENT_ID)

    assert exit_code == 1
    assert live_checks_ran["probed"] is False, (
        "a health probe ran after the identity boundary should have failed"
    )

    payload = capsys.readouterr().out
    for fabricated in (
        "generation.latest_published",
        "nomos.source_lineage",
        "nomos.read_path",
        "mcp.graph_read",
        "sources.optimization_budget",
    ):
        assert fabricated not in payload, (
            f"emitted {fabricated} for an identifier that never resolved"
        )
    assert identifier_mod.IDENTIFIER_KIND_MISMATCH in payload
    assert GRAPH_DEPLOYMENT in payload


def test_the_refusal_hands_back_only_correctly_identified_commands(
    environments: Path, live_checks_ran: dict[str, Any], capsys,
) -> None:
    """Every command offered must use the graph deployment name.

    The recovery block is normally rendered from the supplied identifier.
    On this path that identifier is the wrong kind, so rendering it would
    hand the operator a page of commands repeating the mistake.
    """
    _run(ENVIRONMENT_ID)

    import json as _json

    payload = _json.loads(capsys.readouterr().out)

    for key, command in (payload.get("recovery_commands") or {}).items():
        assert ENVIRONMENT_ID not in command, (
            f"recovery command {key!r} carries the environment id"
        )
        assert GRAPH_DEPLOYMENT in command

    next_action = payload.get("next_action") or {}
    assert next_action.get("code") == identifier_mod.IDENTIFIER_KIND_MISMATCH
    assert GRAPH_DEPLOYMENT in str(next_action.get("command"))
    assert ENVIRONMENT_ID not in str(next_action.get("command"))


def test_dogfood_status_still_accepts_the_deployment_name(
    environments: Path, live_checks_ran: dict[str, Any],
) -> None:
    """The correct name behaves exactly as before."""
    _run(GRAPH_DEPLOYMENT)

    assert live_checks_ran["probed"] is True
    assert live_checks_ran["deployment"] == GRAPH_DEPLOYMENT


def test_dogfood_status_still_probes_an_unknown_name(
    environments: Path, live_checks_ran: dict[str, Any],
) -> None:
    """An unknown name is not a kind mismatch; it takes the normal path."""
    _run(UNKNOWN_NAME)

    assert live_checks_ran["probed"] is True, (
        "an unknown name was refused as a kind mismatch, collapsing two "
        "distinct causes that need different repairs"
    )


def test_mutating_control_verbs_also_refuse_the_wrong_kind(
    environments: Path, capsys,
) -> None:
    """up / down / restart must refuse before acting.

    A wrong-kind identifier on the read-only status surface produces a
    misleading report. On these it would ACT — rendering and running
    service commands built from a name that owns nothing — so the
    boundary matters more here, not less.
    """
    for action in ("up", "down", "restart"):
        args = argparse.Namespace(
            deployment=ENVIRONMENT_ID,
            dogfood_subcmd=action,
            apply=True,
            adapter="auto",
            launchd_plist=None,
            systemd_scope="system",
        )

        exit_code = dogfood_cli._cmd_dogfood_control(args, as_json=True)

        assert exit_code == 1, f"{action} did not refuse a wrong-kind name"
        payload = capsys.readouterr().out
        assert identifier_mod.IDENTIFIER_KIND_MISMATCH in payload
        assert GRAPH_DEPLOYMENT in payload
        # It must not report having done anything.
        assert '"applied": false' in payload.lower()


# --- `okg deployment ready`: the surface the issue is filed against ------
#
# `_cmd_deployment_ready` calls the same boundary, but nothing pinned it.
# The acceptance criteria name this command specifically, so measure it
# here rather than inferring it from the sibling.


@pytest.fixture()
def ready_probes_ran(tmp_path: Path, monkeypatch) -> dict[str, Any]:
    """Stub `deployment ready`'s probes and record whether any ran."""
    from okg.substrate.cli import deployment as deployment_cli

    deployment_dir = tmp_path / "deployments" / GRAPH_DEPLOYMENT
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({"name": GRAPH_DEPLOYMENT}), encoding="utf-8",
    )
    seen: dict[str, Any] = {"probed": False}

    def _env_status(_args: Any, **_kwargs: Any) -> tuple[dict[str, Any], str]:
        seen["probed"] = True
        return (
            {"ok": True, "deployment_dir": str(deployment_dir)},
            "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
        )

    def _live_checks(**kwargs: Any) -> dict[str, Any]:
        seen["probed"] = True
        seen["deployment"] = kwargs["deployment"]
        return {"checks": [], "blockers": [], "warnings": [], "unknowns": []}

    monkeypatch.setattr(
        deployment_cli, "_deployment_ready_env_status", _env_status,
    )
    monkeypatch.setattr(
        deployment_cli, "_deployment_ready_live_checks", _live_checks,
    )
    return seen


def _run_ready(deployment: str) -> int:
    from okg.substrate.cli import deployment as deployment_cli

    args = argparse.Namespace(
        deployment=deployment,
        profile="general",
        show_values=False,
        skip_doctor=True,
        max_active_queue_depth=0,
        max_source_age_seconds=None,
        max_producer_age_seconds=None,
    )
    return deployment_cli._cmd_deployment_ready(args, as_json=True)


def test_deployment_ready_refuses_an_environment_id_before_any_probe(
    environments: Path, ready_probes_ran: dict[str, Any], capsys,
) -> None:
    rc = _run_ready(ENVIRONMENT_ID)

    assert rc != 0
    assert ready_probes_ran["probed"] is False, (
        "readiness probed the database after the identity boundary failed"
    )
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    data = payload["error"]["data"]
    assert data["reason_code"] == identifier_mod.IDENTIFIER_KIND_MISMATCH
    # Both identities, bound under separate keys — never merged.
    assert data["environment_id"] == ENVIRONMENT_ID
    assert data["graph_deployment_name"] == GRAPH_DEPLOYMENT
    assert "deployment" not in data
    # The exact command, not a description of one.
    assert GRAPH_DEPLOYMENT in payload["error"]["fix_hint"]
    assert ENVIRONMENT_ID not in payload["error"]["fix_hint"]


def test_deployment_ready_emits_no_downstream_blocker_on_a_kind_mismatch(
    environments: Path, ready_probes_ran: dict[str, Any], capsys,
) -> None:
    """The five fabricated blockers came from probing past this point."""
    _run_ready(ENVIRONMENT_ID)

    rendered = capsys.readouterr().err
    for fabricated in (
        "generation.latest_published",
        "nomos.source_lineage",
        "nomos.read_path",
        "mcp.graph_read",
        "sources.optimization_budget",
    ):
        assert fabricated not in rendered


def test_deployment_ready_still_runs_for_the_graph_deployment_name(
    environments: Path, ready_probes_ran: dict[str, Any],
) -> None:
    _run_ready(GRAPH_DEPLOYMENT)

    assert ready_probes_ran["probed"] is True
    assert ready_probes_ran["deployment"] == GRAPH_DEPLOYMENT


def test_deployment_ready_keeps_an_unknown_name_distinct_from_wrong_kind(
    environments: Path, ready_probes_ran: dict[str, Any], capsys,
) -> None:
    """"No such thing" and "wrong kind" have different repairs."""
    _run_ready(UNKNOWN_NAME)

    rendered = capsys.readouterr()
    assert identifier_mod.IDENTIFIER_KIND_MISMATCH not in (
        rendered.out + rendered.err
    )
