"""A plain install's worker must be checked against the okg that runs, not paths.

On a plain install the deployment folder lives under ~/.local/share and is not
in a Git checkout. The host adapters derived the worker's expected working
directory, interpreter and code revision from that folder, so the revision was
always missing and doctor stayed red on a healthy worker. The `process`
authority names the okg installation the checking command runs instead.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from okg.substrate import deployment_apply as da

DSN = "postgresql://u@h:5432/d"
TARGET = da._target_identity(DSN)

FINGERPRINTS = {
    "desired_state_digest": "d",
    "source_registry_fingerprint": "s",
    "runtime_fingerprint": "r",
    "worker_config_fingerprint": "w",
    "manifest_fingerprint": "m",
    "catalog_fingerprint": "c",
    "schema_fingerprint": "sc",
    "writer_fingerprint": "wr",
    "schedule_fingerprint": "sh",
}


@pytest.fixture
def plain_folder(tmp_path: Path) -> Path:
    root = (tmp_path / "deployments" / "d").resolve()
    root.mkdir(parents=True)
    if da._enclosing_checkout(root) is not None:
        pytest.skip("the temporary folder is inside a Git checkout")
    return root


def _authority(folder: Path, **over: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "deployment": "d",
        "deployment_dir": folder,
        "sync_root": None,
        "target": TARGET,
        "supervisor": None,
    }
    kwargs.update(over)
    return da._tracking_service_authority(**kwargs)


def _desired(authority: dict[str, Any]) -> dict[str, Any]:
    return {
        **FINGERPRINTS,
        "source_fingerprints": {},
        "source_names": [],
        "source_schedule_ids": [],
        "runtime_enabled": True,
        "service_authority": authority,
    }


def _ack(observed: dict[str, Any]) -> dict[str, Any]:
    return {
        **FINGERPRINTS,
        "source_fingerprints": {},
        "active_sources": [],
        "active_schedules": [],
        "observed_runtime_authority": observed,
    }


def _observed(folder: Path) -> dict[str, Any]:
    return da._observed_runtime_authority(
        deployment="d",
        deployment_dir=folder,
        dsn=DSN,
    )


def test_folder_outside_a_checkout_expects_the_running_okg(
    plain_folder: Path,
) -> None:
    authority = _authority(plain_folder)
    assert authority["adapter"] == da.PROCESS_ADAPTER
    assert authority["python_prefix"] == str(Path(sys.prefix).resolve())
    assert authority["import_root"] == str(da._imported_package_root())
    assert authority["database"] == {
        key: TARGET.get(key) for key in ("host", "port", "dbname")
    }
    assert "--deployment d" in authority["command"]
    # Nothing in it is derived from the deployment folder.
    assert str(plain_folder) not in repr(authority)


@pytest.mark.parametrize("variant", ["git_folder", "sync_root", "declared"])
def test_checkouts_and_declared_supervisors_keep_their_adapter(
    plain_folder: Path, variant: str,
) -> None:
    over: dict[str, Any] = {}
    if variant == "git_folder":
        subprocess.run(["git", "init", "-q"], cwd=plain_folder, check=True)
    elif variant == "sync_root":
        over["sync_root"] = plain_folder
    else:
        over["supervisor"] = "systemd"
    assert _authority(plain_folder, **over)["adapter"] in {"launchd", "systemd"}


def test_worker_from_the_same_okg_matches_from_any_directory(
    plain_folder: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority(plain_folder)
    if not authority["installed_code_revision"]:
        pytest.skip(authority["installed_code_revision_reason"])
    # A plain-install worker may be started from any directory.
    monkeypatch.chdir(plain_folder)
    observed = _observed(plain_folder)
    assert da._worker_ack_mismatches(_desired(authority), _ack(observed)) == []


@pytest.mark.parametrize(
    "field", ["python_prefix", "import_root", "installed_code_revision"],
)
def test_worker_from_another_okg_is_refused_by_name(
    plain_folder: Path, field: str,
) -> None:
    authority = _authority(plain_folder)
    authority["installed_code_revision"] = "a" * 40
    observed = {**_observed(plain_folder), "installed_code_revision": "a" * 40}
    observed[field] = "/somewhere/else" if field != "installed_code_revision" else "b" * 40
    reasons = da._worker_ack_mismatches(_desired(authority), _ack(observed))
    assert len(reasons) == 1
    assert reasons[0].startswith(f"identity {field} differs")


def test_unknown_revision_is_refused_with_the_reason(plain_folder: Path) -> None:
    authority = _authority(plain_folder)
    authority["installed_code_revision"] = None
    authority["installed_code_revision_reason"] = "no build record"
    reasons = da._worker_ack_mismatches(
        _desired(authority), _ack(_observed(plain_folder)),
    )
    assert reasons == [
        "identity installed_code_revision missing from desired authority "
        "(no build record)"
    ]


def test_restart_hint_names_the_worker_command() -> None:
    for started in (True, False):
        hint = da._process_worker_restart_hint("d", started=started)
        assert "runtime worker --deployment d" in hint
        assert "deployment apply" not in hint
    assert da._process_worker_restart_hint("d", started=False).startswith(
        "start the worker:"
    )


def _write_manifest(folder: Path, runtime: str = "  enabled: true\n") -> None:
    (folder / "deployment.yaml").write_text(
        "name: d\nmodules: []\nsource_registry: source_registry.yaml\n"
        f"runtime:\n{runtime}",
        encoding="utf-8",
    )
    (folder / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")


def test_okg_up_sees_no_service_for_a_hand_started_worker(
    plain_folder: Path,
) -> None:
    """`okg up` must not read a service definition a process worker lacks."""
    from okg.substrate.deployment_installed_upgrade import worker_service

    _write_manifest(plain_folder)

    def no_dsn() -> str:
        raise AssertionError("the probe must not need the database")

    service = worker_service(
        "d", deployment_dir=plain_folder, resolve_dsn=no_dsn,
    )
    assert service["adapter"] == "process"
    assert service["available"] is False
    assert service["self_installable"] is False


@pytest.mark.parametrize(
    "runtime", ["  enabled: true\n  supervisor: systemd\n", "  enabled: false\n"],
)
def test_process_worker_predicate_matches_the_authority(
    plain_folder: Path, runtime: str,
) -> None:
    _write_manifest(plain_folder, runtime)
    declared = "supervisor" in runtime
    assert da.deployment_uses_process_worker(plain_folder) is not declared
