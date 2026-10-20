"""`okg deployment install-plan` end to end through the real parser (#1179 s5).

The verb is a thin wrapper: the plan, the lock and every refusal belong to
`okg.substrate.distributions.install_plan`, which
`tests/substrate/distributions/test_install_plan.py` proves directly. These
arms prove the CLI seam itself -- that the parser registers, that a real
package resolves through `main()`, that a refusal reaches the JSON error
envelope with its stable code, and that a secret value never reaches stdout.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.substrate.catalog.test_package_module_provider import (
    _MODULE_ID,
    _verified_package,
)


_SECRET_VALUE = "cli-secret-never-printed"
_SECRET_VAR = "OKG_TEST_INSTALL_PLAN_TOKEN"
# A deferred credential must name a selected source or tool declaration,
# so the fixture package carries one.
_DECLARATION_REF = "archi-test:lab-units-notes"


def _package(tmp_path: Path) -> Path:
    package = _verified_package(tmp_path / "dist", extra_kind="source")
    path = Path(package.path)
    package.close_runtime_authority()
    return path


def _instance(tmp_path: Path, **overrides: object) -> Path:
    body: dict[str, object] = {
        "name": "cli-demo",
        "secrets": {
            "token": {
                "reference": f"env:{_SECRET_VAR}",
                "declaration": _DECLARATION_REF,
            },
        },
    }
    body.update(overrides)
    path = tmp_path / "instance.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return path


def _run(argv: list[str]) -> int:
    from okg.substrate.cli.main import main

    return main(argv)


def _argv(package: Path, instance: Path, output: Path, *extra: str) -> list[str]:
    return [
        "deployment",
        "install-plan",
        "--package",
        str(package),
        "--bundle",
        "lab-alpha",
        "--instance",
        str(instance),
        "--output",
        str(output),
        "--json",
        *extra,
    ]


def test_install_plan_writes_a_plan_and_a_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(_SECRET_VAR, _SECRET_VALUE)
    output = tmp_path / "out"
    rc = _run(_argv(_package(tmp_path), _instance(tmp_path), output))
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])

    assert rc == 0
    assert payload["modules"] == [_MODULE_ID]
    assert payload["plan_digest"].startswith("sha256:")

    plan = json.loads((output / "plan.json").read_text())
    lock = json.loads((output / "lock.json").read_text())
    assert plan["bundle"]["bundle_id"] == "lab-alpha"
    assert lock["plan_digest"] == plan["plan_digest"]
    # The verb ran nothing: only the two documents exist.
    assert sorted(item.name for item in output.iterdir()) == ["lock.json", "plan.json"]
    # A secret value reaches neither the documents nor the operator's terminal.
    assert _SECRET_VALUE not in captured.out
    assert _SECRET_VALUE not in (output / "plan.json").read_text()
    assert _SECRET_VALUE not in (output / "lock.json").read_text()


def test_install_plan_is_idempotent_through_the_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(_SECRET_VAR, _SECRET_VALUE)
    output = tmp_path / "out"
    argv = _argv(_package(tmp_path), _instance(tmp_path), output)

    assert _run(argv) == 0
    first_plan = (output / "plan.json").read_bytes()
    first_lock = (output / "lock.json").read_bytes()
    capsys.readouterr()

    assert _run(argv) == 0
    assert (output / "plan.json").read_bytes() == first_plan
    assert (output / "lock.json").read_bytes() == first_lock


def test_install_plan_refuses_an_unknown_bundle_with_a_coded_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(_SECRET_VAR, _SECRET_VALUE)
    output = tmp_path / "out"
    argv = _argv(_package(tmp_path), _instance(tmp_path), output)
    argv[argv.index("lab-alpha")] = "lab-omega"

    rc = _run(argv)
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["code"] == "install_bundle_unknown"
    assert payload["error"]["data"]["declared_bundles"] == ["lab-alpha", "lab-beta"]
    assert not output.exists()


def test_install_plan_refuses_an_unresolved_secret_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv(_SECRET_VAR, raising=False)
    output = tmp_path / "out"

    rc = _run(_argv(_package(tmp_path), _instance(tmp_path), output))
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["code"] == "install_secret_reference_unresolved"
    assert payload["error"]["data"]["reason"] == "environment_variable_absent"
    assert not output.exists()


def test_install_plan_refuses_an_unreadable_instance_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv(_SECRET_VAR, _SECRET_VALUE)
    output = tmp_path / "out"

    rc = _run(
        _argv(_package(tmp_path), tmp_path / "absent.json", output)
    )
    payload = json.loads(capsys.readouterr().err.strip().splitlines()[-1])

    assert rc != 0
    assert payload["error"]["message"] == "could not read the instance config"
    assert not output.exists()
