from __future__ import annotations

import importlib
import json
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.deployment_release import (
    DatabaseReleaseState,
    ReleaseArtifact,
    ReleaseContract,
    assert_release_compatible,
    current_release_artifact,
    evaluate_release_contract,
    load_database_release_state,
    load_release_contract,
)
from okg.substrate.deployments import DeploymentManifest
from okg.substrate.errors import SubstrateError


def _write_manifest(
    tmp_path: Path,
    *,
    name: str = "dep",
    release: dict | None = None,
) -> DeploymentManifest:
    deployment = tmp_path / name
    deployment.mkdir()
    (deployment / "schemas").mkdir()
    (deployment / "source_registry.yaml").write_text("sources: []\n")
    manifest: dict = {
        "name": name,
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "postgres": {"dsn": "postgres://user@host/db"},
    }
    if release is not None:
        manifest["release"] = release
    (deployment / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    return DeploymentManifest.from_yaml(deployment / "deployment.yaml")


def _artifact(**overrides: str | None) -> ReleaseArtifact:
    values = {
        "code_revision": "code-new",
        "schema_checksum": "schema-new",
        "catalog_fingerprint": "catalog-new",
        "skill_bundle_hash": "skills-new",
    }
    values.update(overrides)
    return ReleaseArtifact(**values)


def _db_state(**overrides: object) -> DatabaseReleaseState:
    values = {
        "ledger_exists": True,
        "schema_checksum": "schema-new",
        "schema_ok": True,
        "pending_migrations": (),
        "blocking_pending_migrations": (),
        "checksum_mismatches": (),
        "checksum_compatibilities": (),
        "latest_catalog_version_id": 42,
        "latest_catalog_fingerprint": "catalog-new",
    }
    values.update(overrides)
    return DatabaseReleaseState(**values)


_PLANNER_PATH = Path("src/okg/substrate/deployment_upgrade.py")
_RELEASE_PATH = Path("src/okg/substrate/deployment_release.py")


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _init_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")


def _write_framework_checkout(repo: Path) -> None:
    planner = repo / _PLANNER_PATH
    release = repo / _RELEASE_PATH
    planner.parent.mkdir(parents=True, exist_ok=True)
    planner.write_text("# fixture deployment upgrade module\n")
    release.write_text("# fixture deployment release module\n")


def _release_layout(
    root: Path,
    *,
    vendored: bool,
    release: dict[str, Any] | None = None,
) -> tuple[Path, Path, SimpleNamespace]:
    outer = root / "deployment-repo"
    deployment_dir = outer / "deployments/dep"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text("name: dep\n")

    if vendored:
        _init_git_repo(outer)
        _git(outer, "add", ".")
        _git(outer, "commit", "-m", "deployment")
        framework = outer / "vendor/okg"
        _init_git_repo(framework)
        _write_framework_checkout(framework)
        _git(framework, "add", ".")
        _git(framework, "commit", "-m", "framework")
    else:
        framework = outer
        _write_framework_checkout(framework)
        _init_git_repo(outer)
        _git(outer, "add", ".")
        _git(outer, "commit", "-m", "deployment and framework")

    manifest = SimpleNamespace(
        name="dep",
        deployment_dir=deployment_dir,
        release=release or {"channel": "dev"},
        runtime=SimpleNamespace(enabled=True),
        skills_dir=None,
    )
    return outer, framework, manifest


def _patch_release_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    manifest: SimpleNamespace,
    framework: Path,
    worker_calls: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    deployment_release = importlib.import_module(
        "okg.substrate.deployment_release"
    )
    deployment_upgrade = importlib.import_module(
        "okg.substrate.deployment_upgrade"
    )
    deployments = importlib.import_module("okg.substrate.deployments")
    worker = importlib.import_module("okg.substrate.runtime.worker")
    db_readiness = importlib.import_module("okg.substrate.db.readiness")

    calls = worker_calls if worker_calls is not None else []
    monkeypatch.setenv("OKG_DSN", "postgres://example")
    # Issue #2128 answers database readiness before the worker branch
    # does any release work, and nothing listens on `postgres://example`.
    # These cases are about release and dispatch, not readiness, so the
    # probe is answered with a migrated database. The gate on this branch
    # is pinned against a real server by
    # `test_runtime_worker_refuses_an_unmigrated_database_by_name` in
    # tests/substrate/test_migrate.py.
    monkeypatch.setattr(
        db_readiness,
        "require_database_ready",
        lambda dsn, **kwargs: db_readiness.DatabaseReadiness(
            schema_present=True, schema_applied=True, ledger_present=True
        ),
    )
    monkeypatch.setattr(deployments, "resolve_deployment", lambda _name: manifest)
    monkeypatch.setattr(
        deployment_release,
        "resolve_deployment",
        lambda _name: manifest,
    )
    monkeypatch.setattr(
        deployment_upgrade,
        "__file__",
        str(framework / _PLANNER_PATH),
    )
    monkeypatch.setattr(
        deployment_release,
        "__file__",
        str(framework / _RELEASE_PATH),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _manifest: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.catalog_identity_for",
        lambda **_kwargs: SimpleNamespace(catalog_fingerprint="catalog"),
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.current_known_migrations",
        lambda: [SimpleNamespace(checksum="schema")],
    )
    monkeypatch.setattr(deployment_release, "_hash_tree", lambda _path: "skills")
    monkeypatch.setattr(
        deployment_release,
        "load_database_release_state",
        lambda _dsn: _db_state(schema_checksum="schema"),
    )

    def fake_run_worker(**kwargs: Any) -> int:
        calls.append(dict(kwargs))
        return 0

    monkeypatch.setattr(worker, "run_worker", fake_run_worker)
    return calls


def _worker_start_attempts(
    *,
    capsys: pytest.CaptureFixture[str],
) -> list[dict[str, Any]]:
    from okg.substrate.cli.runtime import _cmd_runtime

    args = SimpleNamespace(
        rt_subcmd="worker",
        deployment="dep",
        dsn="postgres://example",
        operator=True,
        max_wait_seconds=1.0,
    )
    attempts: list[dict[str, Any]] = []
    for attempt in (1, 2):
        returncode = _cmd_runtime(args, as_json=True)
        captured = capsys.readouterr()
        output = (captured.out or captured.err).strip()
        attempts.append({
            "attempt": attempt,
            "returncode": returncode,
            "output": json.loads(output) if output else None,
        })
    return attempts


def test_release_pin_preserves_split_repository_authorities() -> None:
    artifact = _artifact(
        code_repository="../framework-repo",
        deployment_revision="deployment-commit",
    )

    encoded = artifact.to_dict()
    decoded = ReleaseArtifact.from_mapping(encoded)

    assert encoded["code_repository"] == "../framework-repo"
    assert encoded["deployment_revision"] == "deployment-commit"
    assert decoded == artifact


def test_current_release_artifact_uses_framework_and_deployment_revisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        release={
            "channel": "prod",
            "pin": {
                "code_revision": "framework-commit",
                "schema_checksum": "schema",
                "catalog_fingerprint": "catalog",
                "skill_bundle_hash": "skills",
                "code_repository": "framework-repo",
                "deployment_revision": "deployment-commit",
            },
        },
    )
    deployment_root = deployment.deployment_dir.parent
    framework_root = tmp_path / "framework-repo"
    framework_root.mkdir()

    def git(root: Path, *args: str) -> str:
        return subprocess.run(
            ["git", *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    for root, filename, message in (
        (deployment_root, "deployment.txt", "deployment"),
        (framework_root, "framework.txt", "framework"),
    ):
        git(root, "init")
        git(root, "config", "user.email", "test@example.com")
        git(root, "config", "user.name", "Test")
        (root / filename).write_text(message)
        git(root, "add", ".")
        git(root, "commit", "-m", message)

    shutil.copytree(
        Path(__file__).resolve().parents[2] / "src/okg",
        framework_root / "src/okg",
    )
    git(framework_root, "add", ".")
    git(framework_root, "commit", "-m", "add framework package")

    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _manifest: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.catalog.ownership.catalog_identity_for",
        lambda **kwargs: type("CatalogIdentity", (), {"catalog_fingerprint": "catalog"})(),
    )
    monkeypatch.setattr(
        "okg.substrate.db.management.current_known_migrations",
        lambda: [type("Migration", (), {"checksum": "schema"})()],
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_release._hash_tree",
        lambda _path: "skills",
    )
    current = current_release_artifact(deployment)

    assert current.code_revision == git(framework_root, "rev-parse", "HEAD")
    assert current.code_repository == "framework-repo"
    assert current.deployment_revision == git(deployment_root, "rev-parse", "HEAD")
    deployment.release["pin"].update(
        {
            "code_revision": current.code_revision,
            "deployment_revision": current.deployment_revision,
        }
    )
    report = evaluate_release_contract(
        deployment="dep",
        contract=load_release_contract(deployment),
        current=current,
        database=_db_state(schema_checksum="schema"),
        process_role="cli",
    )
    assert report["ok"] is True, json.dumps(report, sort_keys=True)

    (framework_root / "src/okg/substrate/deployment_upgrade.py").write_text(
        "# mismatched planner\n"
    )
    git(framework_root, "add", ".")
    git(framework_root, "commit", "-m", "mismatch framework package")
    with pytest.raises(SubstrateError, match="provenance is not proven") as raised:
        current_release_artifact(deployment)
    assert raised.value.data["authority_role"] == "framework"
    assert set(raised.value.data) == {
        "authority_role",
        "repository",
        "requested_revision",
        "resolved_revision",
        "operation",
        "reason",
        "evaluation",
    }


def test_vendored_worker_restarts_reach_worker_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _outer, framework, manifest = _release_layout(
        tmp_path,
        vendored=True,
    )
    worker_calls = _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=framework,
    )

    attempts = _worker_start_attempts(capsys=capsys)
    final_code_revision = (
        current_release_artifact(manifest).code_revision
        if all(attempt["returncode"] == 0 for attempt in attempts)
        else None
    )
    framework_head = _git(framework, "rev-parse", "HEAD")
    loop_record = {
        "attempts": attempts,
        "worker_dispatches": len(worker_calls),
        "framework_head": framework_head,
        "resolved_framework_revision": final_code_revision,
    }

    assert [attempt["returncode"] for attempt in attempts] == [0, 0], (
        json.dumps(loop_record, indent=2, sort_keys=True)
    )
    assert len(worker_calls) == 2, json.dumps(
        loop_record,
        indent=2,
        sort_keys=True,
    )
    assert final_code_revision == framework_head, json.dumps(
        loop_record,
        indent=2,
        sort_keys=True,
    )


def test_invalid_explicit_framework_repository_never_falls_back_or_dispatches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    outer, framework, manifest = _release_layout(
        tmp_path,
        vendored=True,
        release={
            "channel": "dev",
            "pin": {"code_repository": "vendor/missing-okg"},
        },
    )
    worker_calls = _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=framework,
    )

    attempts = _worker_start_attempts(capsys=capsys)

    assert [attempt["returncode"] for attempt in attempts] == [1, 1]
    assert worker_calls == []
    for attempt in attempts:
        error = attempt["output"]["error"]
        assert error["data"]["authority_role"] == "framework"
        assert error["data"]["repository"] == str(
            outer / "vendor/missing-okg"
        )
        assert "release.pin.code_repository" in error["fix_hint"]
        assert "okg up" not in error["fix_hint"]


def test_in_repo_worker_release_check_keeps_repository_and_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    outer, framework, manifest = _release_layout(
        tmp_path,
        vendored=False,
    )
    worker_calls = _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=framework,
    )

    current = current_release_artifact(manifest)
    attempts = _worker_start_attempts(capsys=capsys)

    assert current.code_revision == _git(outer, "rev-parse", "HEAD")
    assert [attempt["returncode"] for attempt in attempts] == [0, 0]
    assert len(worker_calls) == 2


def test_framework_authority_refusals_give_operation_specific_fix_hints(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases: dict[str, SubstrateError] = {}

    _outer, _framework, manifest = _release_layout(
        tmp_path / "undiscoverable",
        vendored=True,
    )
    installed = tmp_path / "undiscoverable/site-packages/okg-checkout"
    _write_framework_checkout(installed)
    _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=installed,
    )
    with pytest.raises(SubstrateError) as raised:
        current_release_artifact(manifest)
    cases["discover_framework_repository"] = raised.value

    _outer, framework, manifest = _release_layout(
        tmp_path / "invalid-explicit",
        vendored=True,
        release={
            "channel": "dev",
            "pin": {"code_repository": "vendor/missing-okg"},
        },
    )
    _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=framework,
    )
    with pytest.raises(SubstrateError) as raised:
        current_release_artifact(manifest)
    cases["resolve_framework_repository"] = raised.value

    _outer, _framework, manifest = _release_layout(
        tmp_path / "headless",
        vendored=True,
    )
    headless = tmp_path / "headless/framework-without-head"
    _init_git_repo(headless)
    _write_framework_checkout(headless)
    _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=headless,
    )
    with pytest.raises(SubstrateError) as raised:
        current_release_artifact(manifest)
    cases["resolve_framework_revision"] = raised.value

    _outer, framework, manifest = _release_layout(
        tmp_path / "provenance",
        vendored=True,
    )
    (framework / _RELEASE_PATH).write_text("# changed after framework commit\n")
    _patch_release_runtime(
        monkeypatch,
        manifest=manifest,
        framework=framework,
    )
    with pytest.raises(SubstrateError) as raised:
        current_release_artifact(manifest)
    cases["verify_installed_framework_provenance"] = raised.value

    for operation, error in cases.items():
        assert error.data["authority_role"] == "framework"
        assert error.data["operation"] == operation
        assert "okg up" not in (error.fix_hint or "")

    for operation in (
        "discover_framework_repository",
        "resolve_framework_repository",
    ):
        assert "release.pin.code_repository" in (
            cases[operation].fix_hint or ""
        )
        assert "framework checkout" in (cases[operation].fix_hint or "")

    revision_hint = cases["resolve_framework_revision"].fix_hint or ""
    assert "repair" in revision_hint
    assert "framework Git checkout" in revision_hint

    provenance_hint = (
        cases["verify_installed_framework_provenance"].fix_hint or ""
    )
    assert cases["verify_installed_framework_provenance"].data["reason"] == (
        "installed_file_bytes_differ"
    )
    assert "run or reinstall okg" in provenance_hint
    assert "framework checkout" in provenance_hint


def test_release_channel_is_required(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path)

    with pytest.raises(SubstrateError) as exc:
        load_release_contract(manifest)

    assert "release.channel" in str(exc.value)
    assert "okg up dep" in (exc.value.fix_hint or "")


def test_prod_release_requires_complete_pin(tmp_path: Path) -> None:
    manifest = _write_manifest(
        tmp_path,
        release={
            "channel": "prod",
            "pin": {
                "code_revision": "code-new",
                "schema_checksum": "schema-new",
            },
        },
    )

    with pytest.raises(SubstrateError) as exc:
        load_release_contract(manifest)

    assert exc.value.data["missing"] == [
        "catalog_fingerprint",
        "skill_bundle_hash",
    ]
    assert exc.value.fix_hint == "run `okg up dep`"


def test_dev_channel_is_exempt_from_pin_but_not_schema_skew() -> None:
    contract = ReleaseContract(
        channel="dev",
        pin=_artifact(code_revision="old-code"),
    )

    report = assert_release_compatible(
        deployment="dep",
        contract=contract,
        current=_artifact(),
        database=_db_state(),
        process_role="cli",
    )
    assert report["ok"] is True

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=contract,
            current=_artifact(),
            database=_db_state(schema_checksum="schema-old"),
            process_role="cli",
        )
    assert exc.value.fix_hint == "run `okg up dep`"
    assert exc.value.data["problems"][0]["kind"] == "schema_checksum_mismatch"


def test_destructive_pending_migrations_do_not_block_release() -> None:
    contract = ReleaseContract(channel="dev", pin=None)
    destructive = {
        "migration_id": "retire_legacy_runtime",
        "destructive": True,
    }

    report = assert_release_compatible(
        deployment="dep",
        contract=contract,
        current=_artifact(),
        database=_db_state(
            pending_migrations=(destructive,),
            blocking_pending_migrations=(),
        ),
        process_role="cli",
    )

    assert report["ok"] is True
    assert report["database"]["pending_migrations"] == [destructive]
    assert report["database"]["blocking_pending_migrations"] == []


def test_release_report_surfaces_checksum_compatibilities() -> None:
    compatibility = {
        "migration_id": "0003_deployment_promotion_fence",
        "recorded_checksum": "legacy",
        "expected_checksum": "canonical",
    }
    report = assert_release_compatible(
        deployment="dep",
        contract=ReleaseContract(channel="dev", pin=None),
        current=_artifact(),
        database=_db_state(checksum_compatibilities=(compatibility,)),
        process_role="cli",
    )

    assert report["ok"] is True
    assert report["database"]["checksum_compatibilities"] == [compatibility]


def test_non_destructive_pending_migrations_still_block_release() -> None:
    contract = ReleaseContract(channel="dev", pin=None)
    pending = {
        "migration_id": "0000_schema_snapshot",
        "destructive": False,
    }

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=contract,
            current=_artifact(),
            database=_db_state(
                pending_migrations=(pending,),
                blocking_pending_migrations=(pending,),
            ),
            process_role="cli",
        )

    assert exc.value.data["problems"][0] == {
        "kind": "schema_migrations_pending",
        "pending": [pending],
    }


def test_prod_pin_mismatch_refuses_with_up_fix_hint() -> None:
    contract = ReleaseContract(
        channel="prod",
        pin=_artifact(code_revision="code-old"),
    )

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=contract,
            current=_artifact(),
            database=_db_state(),
            process_role="runtime-worker",
        )

    assert exc.value.fix_hint == "run `okg up dep`"
    assert {
        "kind": "release_pin_mismatch",
        "field": "code_revision",
        "expected": "code-old",
        "observed": "code-new",
    } in exc.value.data["problems"]


def test_deployment_release_cli_reports_verdict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest = _write_manifest(
        tmp_path,
        release={"channel": "dev"},
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: manifest.deployment_dir if name == "dep" else None,
    )

    def _fake_report(*, deployment: str, dsn: str, process_role: str) -> dict:
        return {
            "ok": False,
            "deployment": deployment,
            "dsn_seen": dsn,
            "process_role": process_role,
            "fix_hint": "run `okg up dep`",
        }

    monkeypatch.setattr(
        "okg.substrate.deployment_release.build_release_report",
        _fake_report,
    )

    rc = main([
        "deployment",
        "release",
        "dep",
        "--process-role",
        "runtime-worker",
        "--json",
    ])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["process_role"] == "runtime-worker"
    assert payload["fix_hint"] == "run `okg up dep`"


# ---------------------------------------------------------------------------
# The standalone release identity in a pin and a contract evaluation (#1962)
# ---------------------------------------------------------------------------
#
# `ReleaseArtifact` gained an explicit `release_kind`, so a reader can tell
# "no repository revision, by declaration" from "revision unknown". These
# tests pin the two places that distinction is load-bearing: the pin an
# operator writes, and the prod contract evaluation that compares against it.


_STANDALONE = "standalone"


def _standalone_current() -> ReleaseArtifact:
    return ReleaseArtifact.standalone(
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-new",
        skill_bundle_hash="sha256:skills",
    )


def test_standalone_artifact_states_why_it_has_no_revision() -> None:
    artifact = _standalone_current()

    assert artifact.code_revision is None
    assert artifact.release_kind == _STANDALONE
    assert artifact.records_repository_revision is False
    # A packaged artifact also carries `code_revision: None`. The two must not
    # be confusable by a reader of the serialized report.
    packaged = ReleaseArtifact(
        code_revision=None,
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-new",
        skill_bundle_hash="sha256:skills",
        packaged_release={"name": "okg", "version": "1.0"},
        release_kind="packaged",
    )
    # Both carry a null revision, and `records_repository_revision` is False
    # for both. `release_kind` is the only field that tells them apart.
    assert packaged.records_repository_revision is False
    assert packaged.to_dict()["release_kind"] == "packaged"
    assert artifact.to_dict()["release_kind"] == _STANDALONE
    assert "packaged_release" not in artifact.to_dict()


def test_standalone_pin_cannot_also_pin_a_repository_identity() -> None:
    """Two contradictory authorities in one pin is refused at load time.

    Not left to surface later as a `release_pin_mismatch` on a field the
    deployment could never satisfy, which reads as drift the operator could
    fix by syncing code.
    """
    from okg.substrate.deployment_release import StandaloneReleaseConflict

    with pytest.raises(StandaloneReleaseConflict) as raised:
        ReleaseArtifact.from_mapping({
            "release_kind": _STANDALONE,
            "code_revision": "a" * 40,
            "schema_checksum": "schema-new",
            "catalog_fingerprint": "catalog-new",
            "skill_bundle_hash": "sha256:skills",
        })

    assert raised.value.code == "standalone_release_conflict"
    assert "code_revision" in raised.value.data["conflicting_authority"]


def test_pin_release_kind_typo_is_refused_not_ignored() -> None:
    from okg.substrate.deployment_release import ReleaseKindInvalid

    with pytest.raises(ReleaseKindInvalid) as raised:
        ReleaseArtifact.from_mapping({"release_kind": "standlaone"})

    assert raised.value.code == "release_kind_invalid"
    assert raised.value.data["observed"] == "standlaone"


def test_standalone_pin_round_trips_through_from_mapping() -> None:
    pin = ReleaseArtifact.from_mapping({
        "release_kind": _STANDALONE,
        "code_revision": None,
        "schema_checksum": "schema-new",
        "catalog_fingerprint": "catalog-new",
        "skill_bundle_hash": "sha256:skills",
    })

    assert pin.release_kind == _STANDALONE
    assert pin.code_revision is None
    assert pin == _standalone_current()


def test_prod_pin_naming_a_revision_reports_the_real_cause() -> None:
    """A standalone deployment pinned to a Git revision is unsatisfiable.

    The generic `release_pin_mismatch` this used to produce told the operator
    a revision had drifted, which invites `okg up` and a code sync. Neither
    can help: this deployment records no revision at all. The distinct kind
    must fire, and the generic one must not also fire on the same field.
    """
    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(
            channel="prod",
            pin=ReleaseArtifact(
                code_revision="b" * 40,
                schema_checksum="schema-new",
                catalog_fingerprint="catalog-new",
                skill_bundle_hash="sha256:skills",
            ),
        ),
        current=_standalone_current(),
        database=_db_state(),
        process_role="cli",
    )

    assert report["ok"] is False
    kinds = [problem["kind"] for problem in report["problems"]]
    assert "standalone_release_pin_unsatisfiable" in kinds
    assert not [
        problem
        for problem in report["problems"]
        if problem["kind"] == "release_pin_mismatch"
        and problem["field"] == "code_revision"
    ], report["problems"]
    assert report["current"]["release_kind"] == _STANDALONE


def test_prod_standalone_pin_matches_a_standalone_deployment() -> None:
    """A standalone deployment can still be pinned for prod, honestly.

    The pin declares `release_kind: standalone` and no revision, so the four
    fields that ARE measurable still gate the release and the contract passes.
    """
    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="prod", pin=_standalone_current()),
        current=_standalone_current(),
        database=_db_state(),
        process_role="cli",
    )

    assert report["ok"] is True, report["problems"]


def test_prod_standalone_pin_still_catches_catalog_drift() -> None:
    """Waiving the repository half waives nothing else."""
    drifted = ReleaseArtifact.standalone(
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-DRIFTED",
        skill_bundle_hash="sha256:skills",
    )

    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="prod", pin=_standalone_current()),
        current=drifted,
        database=_db_state(),
        process_role="cli",
    )

    assert report["ok"] is False
    assert [
        problem["field"]
        for problem in report["problems"]
        if problem["kind"] == "release_pin_mismatch"
    ] == ["catalog_fingerprint"]


def test_prod_pin_declaring_standalone_catches_a_tracking_deployment() -> None:
    """The kind is itself compared: a pin cannot silently change meaning."""
    tracking = ReleaseArtifact(
        code_revision="c" * 40,
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-new",
        skill_bundle_hash="sha256:skills",
    )

    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="prod", pin=_standalone_current()),
        current=tracking,
        database=_db_state(),
        process_role="cli",
    )

    assert report["ok"] is False
    mismatched = {
        problem["field"]
        for problem in report["problems"]
        if problem["kind"] == "release_pin_mismatch"
    }
    assert "release_kind" in mismatched


def test_a_packaged_pin_is_satisfiable(tmp_path: Path) -> None:
    """A pin may declare `release_kind: packaged`; it must be meetable.

    `RELEASE_KINDS` admits `packaged` in a pin, but the packaged artifact did
    not set `release_kind`, so such a pin compared `packaged` against None and
    refused EVERY startup of a correctly packaged deployment with
    `release_pin_mismatch field=release_kind`. The deployment was fine; the
    contract could not be met by any deployment at all.
    """
    packaged_current = ReleaseArtifact(
        code_revision=None,
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-new",
        skill_bundle_hash="sha256:skills",
        packaged_release={"name": "okg", "version": "1.0"},
        release_kind="packaged",
    )
    pin = ReleaseArtifact.from_mapping({
        "release_kind": "packaged",
        "schema_checksum": "schema-new",
        "catalog_fingerprint": "catalog-new",
        "skill_bundle_hash": "sha256:skills",
    })

    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="prod", pin=pin),
        current=packaged_current,
        database=_db_state(),
        process_role="cli",
    )

    assert report["ok"] is True, report["problems"]


def test_release_kind_refusal_hint_agrees_with_allowed() -> None:
    """The refusal must not name fewer kinds than it says it allows."""
    from okg.substrate.deployment_release import ReleaseKindInvalid

    with pytest.raises(ReleaseKindInvalid) as raised:
        ReleaseArtifact.from_mapping({"release_kind": "nonsense"})

    hint = raised.value.fix_hint or ""
    for kind in raised.value.data["allowed"]:
        assert kind in hint, (kind, hint)


# ---------------------------------------------------------------------------
# The fix hint a fresh install actually needs (issue #2127)
# ---------------------------------------------------------------------------


def test_an_unmigrated_database_is_told_to_migrate_not_to_run_okg_up() -> None:
    """`okg up` does not create the schema ledger; `okg migrate` does.

    Before issue #2127 the installer wrote no `release` block, so
    `enforce_release_contract_if_declared` short-circuited and this refusal
    never reached a fresh install. Now that a block is always written, the
    first command an operator points at a brand-new database refuses with
    `schema_ledger_missing` -- and the generic hint sent them to a command
    that is not the cure.
    """
    contract = ReleaseContract(channel="dev", pin=None)

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=contract,
            current=_artifact(),
            database=_db_state(ledger_exists=False, schema_ok=False),
            process_role="mcp",
        )

    hint = exc.value.fix_hint or ""
    assert "okg migrate --deployment dep --apply" in hint
    assert "okg up" not in hint
    assert exc.value.data["problems"][0]["kind"] == "schema_ledger_missing"
    assert exc.value.data["fix_hint"] == hint


def test_migration_and_release_skew_together_name_migrate_first() -> None:
    """Mixed problems: migrate first, then the general recovery."""
    contract = ReleaseContract(channel="dev", pin=None)

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=contract,
            current=_artifact(),
            database=_db_state(
                ledger_exists=False,
                schema_ok=False,
                schema_checksum="schema-old",
            ),
            process_role="mcp",
        )

    hint = exc.value.fix_hint or ""
    assert (
        hint.index("okg migrate --deployment dep --apply")
        < hint.index("okg up dep")
    )


def _hinted_commands(hint: str) -> list[list[str]]:
    """Every `okg ...` command a fix hint tells the operator to paste."""
    import re
    import shlex

    return [
        shlex.split(found)[1:]
        for found in re.findall(r"`(okg [^`]+)`", hint)
    ]


@pytest.mark.parametrize(
    ("database", "expected_kind"),
    [
        (
            _db_state(ledger_exists=False, schema_ok=False),
            "schema_ledger_missing",
        ),
        (
            _db_state(
                ledger_exists=False,
                schema_ok=False,
                schema_checksum="schema-old",
            ),
            "schema_ledger_missing",
        ),
        (
            _db_state(schema_checksum="schema-old"),
            "schema_checksum_mismatch",
        ),
    ],
)
def test_every_hinted_command_parses_through_the_real_cli(
    database: DatabaseReleaseState, expected_kind: str,
) -> None:
    """A fix hint an operator cannot paste is worse than no hint.

    The first form of the migration hint was
    `okg migrate --apply <deployment>`. `okg migrate` takes the
    deployment in `--deployment` and no positional argument, so that
    command exited 2 with `unrecognized arguments` -- the refusal
    handed a fresh installer a command that does not run. Asserting on
    the hint substring cannot see that; only the real parser can. This
    parses every command the hint names, for every shape of problem
    that reaches `_problems_fix_hint`.
    """
    from okg.substrate.cli.main import _build_parser

    with pytest.raises(SubstrateError) as exc:
        assert_release_compatible(
            deployment="dep",
            contract=ReleaseContract(channel="dev", pin=None),
            current=_artifact(),
            database=database,
            process_role="mcp",
        )

    assert exc.value.data["problems"][0]["kind"] == expected_kind
    commands = _hinted_commands(exc.value.fix_hint or "")
    assert commands, "the refusal named no command at all"
    for argv in commands:
        _build_parser(argv).parse_args(argv)


def test_the_old_positional_migrate_form_does_not_parse() -> None:
    """The guard above only bites because this form really is rejected."""
    from okg.substrate.cli.main import _build_parser

    argv = ["migrate", "--apply", "dep"]
    with pytest.raises(SystemExit) as exc:
        _build_parser(argv).parse_args(argv)
    assert exc.value.code == 2


def test_an_unmigrated_database_is_read_without_touching_the_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No ledger means no tables, so nothing queries them.

    `okg mcp-serve` against a brand-new database raised
    `psycopg.errors.UndefinedTable` on `okg.catalog_versions` before the
    `schema_ledger_missing` refusal could be built. The refusal was
    unreachable exactly when it was needed.
    """
    from okg.substrate.db import management

    monkeypatch.setattr(
        management, "migration_status",
        lambda _dsn: {"ledger_exists": False, "applied": (), "pending": ()},
    )

    def _no_connection(*_a, **_kw):
        raise AssertionError(
            "an unmigrated database must not be queried for okg tables"
        )

    from okg.substrate.db import connection as db_connection

    monkeypatch.setattr(db_connection, "connect", _no_connection)

    state = load_database_release_state("postgresql://unused/db")

    assert state.ledger_exists is False
    assert state.schema_ok is False
    assert state.latest_catalog_version_id is None
