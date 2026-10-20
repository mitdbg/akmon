"""`okg deployment install-receipt` through the real parser (#1179 s7).

The verb is a thin wrapper: the three receipts and every refusal belong to
`okg.substrate.distributions.lifecycle_receipts`, which
`tests/substrate/distributions/test_lifecycle_receipts.py` proves
directly. These arms prove the CLI seam itself -- that the parser
registers each kind, that install directories on disk resolve through
`main()`, and that a refusal reaches the JSON error envelope with its
stable code and writes nothing.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.distributions.install_plan import write_install_plan

from tests.substrate.catalog.test_module_providers import (
    _empty_cross_links,
    _write_module,
)
from tests.substrate.catalog.test_package_module_provider import (
    _SCHEMA_YAML,
    _verified_package,
)


_OKG_VERSION = "1.0.0"
_SECRET_ENV = {"ARCHI_TOKEN": "cli-never-recorded"}
# The receipt arms do not exercise credentials, and a deferred credential
# now needs a selected source or tool declaration this package lacks.
_INSTANCE = {"name": "demo"}
_SCHEMA_V2 = _SCHEMA_YAML + b"\n# revision two\n"
_BASELINE_RELEASE = "sha256:" + "a" * 64
_CANDIDATE_RELEASE = "sha256:" + "b" * 64


def _install(tmp_path: Path, name: str, *, schema: bytes = _SCHEMA_YAML) -> Path:
    """Build a package and plan it into `<tmp_path>/<name>/install`."""
    package = _verified_package(tmp_path / name, schema_payload=schema)
    package_path = Path(package.path)
    package.close_runtime_authority()
    builtin = tmp_path / "builtin"
    _write_module(builtin, "reasoning", {"Hypothesis": "entity"})
    output = tmp_path / name / "install"
    write_install_plan(
        package_path,
        "lab-alpha",
        _INSTANCE,
        output,
        okg_version=_OKG_VERSION,
        environ=_SECRET_ENV,
        builtin_modules_root=builtin,
        cross_links_root=_empty_cross_links(tmp_path),
    )
    return output


def _package_file(tmp_path: Path, name: str) -> Path:
    return next((tmp_path / name / "packages").glob("*.okgdist.tar"))


def _run(argv: list[str]) -> int:
    from okg.substrate.cli.main import main

    return main(argv)


def _argv(kind: str, baseline: Path, candidate: Path, output: Path) -> list[str]:
    return [
        "deployment",
        "install-receipt",
        "--kind",
        kind,
        "--baseline",
        str(baseline),
        "--candidate",
        str(candidate),
        "--baseline-release",
        _BASELINE_RELEASE,
        "--candidate-release",
        _CANDIDATE_RELEASE,
        "--output",
        str(output),
        "--json",
    ]


def test_the_upgrade_kind_writes_one_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    candidate = _install(tmp_path, "v2", schema=_SCHEMA_V2)
    output = tmp_path / "receipt"

    rc = _run(_argv("upgrade", baseline, candidate, output))
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert rc == 0
    assert payload["kind"] == "upgrade"
    receipt = json.loads(Path(payload["receipt_path"]).read_text())
    assert receipt["module_provider_changes"]["digest_changed"]
    assert sorted(item.name for item in output.iterdir()) == ["upgrade-receipt.json"]


def test_the_drift_kind_reports_drift_rather_than_refusing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    candidate = _install(tmp_path, "v2", schema=_SCHEMA_V2)
    output = tmp_path / "receipt"

    rc = _run(_argv("drift", baseline, candidate, output))
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert rc == 0
    receipt = json.loads(Path(payload["receipt_path"]).read_text())
    assert receipt["status"] == "drift"
    assert receipt["drift_code"] == "install_lock_drift"


def test_the_rollback_kind_refuses_a_missing_baseline_package(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    candidate = _install(tmp_path, "v2", schema=_SCHEMA_V2)
    package = _package_file(tmp_path, "v1")
    package.unlink()
    output = tmp_path / "receipt"

    rc = _run(
        [
            *_argv("rollback", baseline, candidate, output),
            "--baseline-package",
            str(package),
        ]
    )
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["code"] == "rollback_baseline_package_missing"
    assert payload["error"]["data"]["reason"] == "package_absent"
    assert not output.exists()


def test_rollback_corrupt_baseline_is_a_redacted_cli_refusal_without_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    candidate = _install(tmp_path, "v2", schema=_SCHEMA_V2)
    package = _package_file(tmp_path, "v1")
    package.write_bytes(b"private-corrupted-package-marker")
    output = tmp_path / "receipt"
    rc = _run(
        [
            *_argv("rollback", baseline, candidate, output),
            "--baseline-package",
            str(package),
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.err.strip().splitlines()[-1])
    assert rc != 0
    assert payload["error"]["code"] == "rollback_baseline_package_missing"
    assert payload["error"]["data"] == {
        "reason": "package_verification_failed",
        "package_error_code": "transport_digest_mismatch",
    }
    assert str(package) not in captured.err
    assert "private-corrupted-package-marker" not in captured.err
    assert not output.exists()


def test_a_rollback_without_a_baseline_package_is_refused_before_reading(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    output = tmp_path / "receipt"

    rc = _run(_argv("rollback", baseline, baseline, output))
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["data"]["kind"] == "rollback"
    assert not output.exists()


def test_an_unreadable_install_directory_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    baseline = _install(tmp_path, "v1")
    output = tmp_path / "receipt"

    rc = _run(_argv("upgrade", baseline, tmp_path / "absent", output))
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["message"] == (
        "could not read an install directory's plan and lock"
    )
    assert not output.exists()


def test_an_upgrade_without_release_ids_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """A receipt naming an empty release would bind nothing in #1185."""
    baseline = _install(tmp_path, "v1")
    output = tmp_path / "receipt"

    rc = _run(
        [
            "deployment",
            "install-receipt",
            "--kind",
            "upgrade",
            "--baseline",
            str(baseline),
            "--candidate",
            str(baseline),
            "--output",
            str(output),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["data"]["missing"] == [
        "--baseline-release",
        "--candidate-release",
    ]
    assert not output.exists()
