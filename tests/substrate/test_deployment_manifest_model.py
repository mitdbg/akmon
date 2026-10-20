"""Golden + validator tests for the typed DeploymentManifest model.

`deployment-manifest-model` PACT. The golden test is the safety net for the
non-breaking superset change: it serializes the BASELINE captured fields of
`DeploymentManifest` for all six in-repo deployments and asserts the model
produces byte-identical values after the typed-superset extension. New fields
added by the change are intentionally excluded from the golden comparison —
the contract is "existing captured fields are unchanged".
"""
from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path

import pytest

from okg.substrate.deployments import (
    DeploymentConfigError,
    DeploymentManifest,
)
from tests.mirror_scope import is_public_copy
from tests.private_deployments import names

# Repo root: tests/substrate/test_deployment_manifest_model.py -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "deployment_manifest_golden.json"
)
# The golden entries of deployments whose names the public copy must not
# carry. The export leaves this file out (release/public-mirror/allowlist.yaml).
_PRIVATE_FIXTURE = _FIXTURE.with_name("deployment_manifest_golden_private.json")

# The five in-repo deployments under test.
IN_REPO_DEPLOYMENTS = [
    "beaver-okg",
    *names("retired_prototype"),
    "okg-ci",
    "okg-workspace",
    "synth-bench",
]

# The ORIGINAL (pre-change) captured field names. The golden test asserts the
# post-change model still serializes these to byte-identical values. Fields
# the typed-superset change adds (consistency, extractor, enrichers, viz typed
# attrs, nomos, sync_root, ...) are deliberately NOT in this list.
BASELINE_FIELDS = [
    "name",
    "description",
    "schema_dir",
    "source_registry",
    "enricher_registry",
    "taxonomy",
    "aliases",
    "link_config",
    "mcp_instructions",
    "skills_dir",
    "mcp_trace_path",
    "audit_questions",
    "tool_description_overrides",
    "skill_triggers",
    "refreshers",
    "postgres_dsn",
    "snapshot_path",
    "modules",
    "modules_config",
    "record_prefixes",
    "session_start_context",
    "agent_tools",
    "mcp_pre_join_tools",
    "pre_join_resolvers",
    "viz",
    "viz_path",
    "profiling",
    "chronos",
    "release",
    "runtime",
]

# These runtime fields were added after the baseline was captured. Keep them
# out of this legacy-field compatibility assertion; their parsing and
# round-trip behavior are covered by the runtime-config tests.
NON_BASELINE_RUNTIME_FIELDS = {
    "admission",
    "python_extras",
    "smoke_reset_cursor",
    "smoke_sources",
    # coherent-parallelism: manifest-expressible intra-stage widths
    # (runtime.concurrency), added after the baseline was captured.
    "concurrency",
}
NON_BASELINE_RUNTIME_SCHEDULES = {"okg_ci_git_history_reconcile"}
NON_BASELINE_RUNTIME_WORKFLOWS = {"workflows.git_history_reconcile"}


def _norm_str(s: str) -> str:
    """Normalize machine-specific absolute paths to stable sentinels so the
    golden snapshot is portable across machines/worktrees."""
    s = s.replace(str(_REPO_ROOT), "<REPO_ROOT>")
    s = s.replace(str(Path.home()), "<HOME>")
    return s


def _serialize(value, dep_dir: Path):
    """Stable, comparable serialization of a captured manifest field."""
    if isinstance(value, Path):
        try:
            return "<dep>/" + str(value.relative_to(dep_dir))
        except ValueError:
            return _norm_str(str(value))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "__dataclass__": type(value).__name__,
            **{
                f.name: _serialize(getattr(value, f.name), dep_dir)
                for f in dataclasses.fields(value)
            },
        }
    if isinstance(value, dict):
        return {k: _serialize(v, dep_dir) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(v, dep_dir) for v in value]
    if isinstance(value, str):
        return _norm_str(value)
    return value


# DSN env vars the in-repo manifests interpolate into `postgres.dsn`
# (`${OKG_DSN}`, `${OKG_CI_DSN}`). The golden snapshot was pinned with
# these UNSET, so the env-derived `postgres_dsn` baseline field resolves
# to null. The persistent self-hosted CI runners export these (they host
# live deployments), which would otherwise drift `postgres_dsn` to a real
# DSN string and fail this byte-identity test. Clear them for the capture
# so it guards the manifest MODEL's baseline contract, not the runner's
# ambient environment. Parse via `_from_yaml_uncached` to bypass the
# (path, mtime)-keyed _MANIFEST_CACHE, which would otherwise serve a
# non-null value cached by a sibling test that parsed with OKG_DSN set.
_DSN_ENV_VARS = ("OKG_DSN", "OKG_CI_DSN")


def _capture(name: str) -> dict:
    manifest_path = _REPO_ROOT / "deployments" / name / "deployment.yaml"
    saved = {k: os.environ.pop(k, None) for k in _DSN_ENV_VARS}
    try:
        manifest = DeploymentManifest._from_yaml_uncached(manifest_path)
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v
    dep_dir = manifest.deployment_dir
    captured = {
        fn: _serialize(getattr(manifest, fn), dep_dir)
        for fn in BASELINE_FIELDS
    }
    runtime = captured.get("runtime")
    if isinstance(runtime, dict):
        for field in NON_BASELINE_RUNTIME_FIELDS:
            runtime.pop(field, None)
        schedules = runtime.get("schedules")
        if isinstance(schedules, dict):
            for schedule in NON_BASELINE_RUNTIME_SCHEDULES:
                schedules.pop(schedule, None)
        workflows = runtime.get("workflows")
        if isinstance(workflows, list):
            runtime["workflows"] = [
                workflow
                for workflow in workflows
                if workflow not in NON_BASELINE_RUNTIME_WORKFLOWS
            ]
    return captured


@pytest.fixture(scope="module")
def golden_snapshot() -> dict:
    snapshot = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    # Here the private entries must be present; only the public copy lacks them.
    if _PRIVATE_FIXTURE.is_file() or not is_public_copy():
        snapshot.update(json.loads(_PRIVATE_FIXTURE.read_text(encoding="utf-8")))
    return snapshot


@pytest.mark.parametrize("deployment", IN_REPO_DEPLOYMENTS)
def test_manifest_baseline_fields_byte_identical(
    deployment: str, golden_snapshot: dict,
) -> None:
    """Each in-repo deployment's BASELINE captured fields are byte-identical
    to the committed golden snapshot — the non-breaking-superset guard."""
    assert deployment in golden_snapshot, (
        f"{deployment} missing from golden fixture; regenerate it"
    )
    captured = _capture(deployment)
    # ``embedding`` deliberately left the raw baseline surface in #1229;
    # its replacement ``embedding_config`` has a closed decision-table test.
    expected = {
        key: value for key, value in golden_snapshot[deployment].items()
        if key in BASELINE_FIELDS
    }
    # Compare as canonical JSON so the failure diff is a byte diff.
    assert json.dumps(captured, indent=2, sort_keys=True) == json.dumps(
        expected, indent=2, sort_keys=True,
    ), f"baseline captured fields drifted for {deployment}"


@pytest.mark.parametrize("deployment", IN_REPO_DEPLOYMENTS)
def test_all_in_repo_manifests_load_without_error(deployment: str) -> None:
    """The typed model + unknown-key validator must still load every in-repo
    deployment cleanly."""
    manifest_path = _REPO_ROOT / "deployments" / deployment / "deployment.yaml"
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.name == deployment


def _cached_external_manifests() -> list[Path]:
    cache_root = Path.home() / ".cache" / "okg" / "deployments"
    if not cache_root.is_dir():
        return []
    return sorted(cache_root.glob("*/*/deployment.yaml"))


#: A checked-in manifest that lives outside `deployments/`, so the parameter
#: set below is never empty.
#:
#: It used to be exactly `_cached_external_manifests()`. pytest turns an empty
#: parameter set into one SKIPPED `[NOTSET]` case, and nothing is cached under
#: `~/.cache/okg/deployments` on a CI runner -- so on every CI run this test
#: checked no manifest at all while reading, in a green report, as coverage of
#: the known-key census. A skip proves nothing.
_FIXTURE_EXTERNAL_MANIFEST = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "deployments"
    / "lifecycle-exec"
    / "deployment.yaml"
)


def _external_shaped_manifests() -> list[Path]:
    """Cached external manifests, plus a checked-in stand-in for them."""
    candidates = [_FIXTURE_EXTERNAL_MANIFEST, *_cached_external_manifests()]
    return [path for path in candidates if path.is_file()]


def test_the_external_manifest_sweep_is_never_empty() -> None:
    """Guard the guard: an empty sweep is what made the case below vacuous."""
    found = _external_shaped_manifests()
    assert found, (
        f"no external-shaped manifest to check. {_FIXTURE_EXTERNAL_MANIFEST} "
        f"must exist so the parametrized case below cannot resolve to a "
        f"single skipped [NOTSET] node"
    )
    assert _FIXTURE_EXTERNAL_MANIFEST in found


@pytest.mark.parametrize(
    "manifest_path",
    _external_shaped_manifests(),
    ids=lambda p: f"{p.parent.parent.name}/{p.parent.name}",
)
def test_cached_external_manifests_load_without_error(
    manifest_path: Path,
) -> None:
    """External-shaped manifests must not be falsely rejected by the
    unknown-key validator (the known-key set was censused over them)."""
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.name


# ---------------------------------------------------------------------------
# Unknown-key rejection (Task 2)
# ---------------------------------------------------------------------------


def _write_min_manifest(tmp_path: Path, extra: str = "") -> Path:
    dep = tmp_path / "demo"
    dep.mkdir()
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    body = (
        "name: demo\n"
        "description: test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        + extra
    )
    (dep / "deployment.yaml").write_text(body, encoding="utf-8")
    return dep / "deployment.yaml"


def test_unknown_top_level_key_is_rejected(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(
        tmp_path, "totally_bogus_key: 1\n",
    )
    with pytest.raises(DeploymentConfigError) as exc:
        DeploymentManifest.from_yaml(manifest_path)
    assert "totally_bogus_key" in str(exc.value)


def test_contract_top_level_key_is_captured(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(
        tmp_path,
        "contract:\n"
        "  version: 1\n"
        "  lifecycle: prototype\n"
        "  owner: okg-operators\n"
        "  intended_usage: contract fixture\n"
        "  evidence:\n"
        "    readiness:\n"
        "      status: not_required\n"
        "      collected_at: null\n"
        "      ttl_seconds: null\n"
        "      provenance: unit-test\n",
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.contract is not None
    assert manifest.contract["version"] == 1
    assert manifest.contract["lifecycle"] == "prototype"


def test_known_keys_still_load(tmp_path: Path) -> None:
    # A grab-bag of legitimate top-level keys must NOT be rejected.
    manifest_path = _write_min_manifest(
        tmp_path,
        "consistency:\n  dangling_edges: prune\n"
        "extractor:\n  class: x.Y\n"
        "nomos: {}\n"
        "viz: {}\n"
        "release:\n  channel: dev\n"
        "sync_root: /tmp/x\n"
        "profile:\n  name: dev\n"
        "doctor_checks: []\n",
    )
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.name == "demo"
    assert manifest.sync_root == Path("/tmp/x").resolve(strict=False)


def test_deployment_up_policy_is_captured(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(
        tmp_path,
        "deployment_up:\n"
        "  backup:\n"
        "    verification_policy: restore_drill\n",
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.deployment_up == {
        "backup": {"verification_policy": "restore_drill"}
    }


def test_malformed_deployment_up_block_is_rejected(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(tmp_path, "deployment_up: false\n")

    with pytest.raises(DeploymentConfigError, match="must be a mapping"):
        DeploymentManifest.from_yaml(manifest_path)


# ---------------------------------------------------------------------------
# Declared-path existence (Task 2)
# ---------------------------------------------------------------------------


def test_missing_schema_dir_is_rejected(tmp_path: Path) -> None:
    dep = tmp_path / "demo"
    dep.mkdir()
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        "name: demo\n"
        "schema_dir: does_not_exist\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    with pytest.raises(DeploymentConfigError) as exc:
        DeploymentManifest.from_yaml(dep / "deployment.yaml")
    assert "schema_dir" in str(exc.value)


def test_missing_enricher_registry_is_rejected(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(
        tmp_path, "enricher_registry: missing_registry.yaml\n",
    )
    with pytest.raises(DeploymentConfigError) as exc:
        DeploymentManifest.from_yaml(manifest_path)
    assert "enricher_registry" in str(exc.value)


def test_present_enricher_registry_loads(tmp_path: Path) -> None:
    dep = tmp_path / "demo"
    dep.mkdir()
    (dep / "schemas").mkdir()
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    (dep / "enrichers.yaml").write_text("enrichers: []\n", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        "name: demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "enricher_registry: enrichers.yaml\n",
        encoding="utf-8",
    )
    manifest = DeploymentManifest.from_yaml(dep / "deployment.yaml")
    assert manifest.enricher_registry is not None
    assert manifest.enricher_registry.name == "enrichers.yaml"


def test_absent_optional_paths_load_fine(tmp_path: Path) -> None:
    # No optional registry keys declared at all — must load with None fields.
    manifest_path = _write_min_manifest(tmp_path)
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.enricher_registry is None
    assert manifest.taxonomy is None
    assert manifest.aliases is None



# ---------------------------------------------------------------------------
# Issue #1310 -- `postgres.idle_in_transaction_session_timeout_ms`.
# ---------------------------------------------------------------------------


def test_idle_in_transaction_timeout_is_captured(tmp_path: Path) -> None:
    manifest_path = _write_min_manifest(
        tmp_path,
        "postgres:\n  idle_in_transaction_session_timeout_ms: 900000\n",
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.postgres_idle_in_transaction_timeout_ms == 900000


def test_idle_in_transaction_timeout_absent_means_the_built_in_default(
    tmp_path: Path,
) -> None:
    manifest_path = _write_min_manifest(tmp_path, "postgres:\n  database: d\n")

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.postgres_idle_in_transaction_timeout_ms is None


def test_idle_in_transaction_timeout_zero_disables_the_bound(
    tmp_path: Path,
) -> None:
    """0 is a legitimate value -- "no bound" -- not a missing one."""
    manifest_path = _write_min_manifest(
        tmp_path,
        "postgres:\n  idle_in_transaction_session_timeout_ms: 0\n",
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)

    assert manifest.postgres_idle_in_transaction_timeout_ms == 0


@pytest.mark.parametrize(
    "value", ["'5min'", "-1", "true", "3.5"],
)
def test_malformed_idle_in_transaction_timeout_is_rejected(
    tmp_path: Path, value: str,
) -> None:
    """Refuse at load: an ignored knob reads to the operator as a bound."""
    manifest_path = _write_min_manifest(
        tmp_path,
        f"postgres:\n  idle_in_transaction_session_timeout_ms: {value}\n",
    )

    with pytest.raises(DeploymentConfigError) as exc:
        DeploymentManifest.from_yaml(manifest_path)

    assert "idle_in_transaction_session_timeout_ms" in str(exc.value)
