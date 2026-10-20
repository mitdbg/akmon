from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import textwrap
from pathlib import Path


def _write_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "fixture"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        textwrap.dedent(
            """\
            name: fixture
            postgres:
              dsn: postgresql://unused
            doctor_checks:
              - name: fake-ok
                handler: fixture_doctor_checks:ok_check
              - name: fake-fail
                handler: fixture_doctor_checks:fail_check
            """
        )
    )
    (dep / "fixture_doctor_checks.py").write_text(
        textwrap.dedent(
            """\
            def ok_check(args, *, context, as_json):
                context.emit_ok(
                    {"ok": True, "status": "OK", "deployment": args.deployment},
                    as_json=as_json,
                )
                return 0


            def fail_check(args, *, context, as_json):
                context.emit_ok(
                    {
                        "ok": False,
                        "status": "FAIL",
                        "errors": [{"code": "fixture_failed"}],
                    },
                    as_json=as_json,
                )
                return 1
            """
        )
    )
    return dep


def _run_doctor_check(check: str, deployment: str = "fixture") -> tuple[int, dict]:
    from okg.substrate.cli.main import _cmd_doctor

    args = argparse.Namespace(
        check=check,
        deployment=deployment,
        dsn=None,
        warm=False,
        generation=None,
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = _cmd_doctor(args, as_json=True)
    return rc, json.loads(buf.getvalue())


def test_deployment_doctor_registry_dispatches_ok_and_fail(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dep = _write_deployment(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep if name == "fixture" else None,
    )
    sys.modules.pop("fixture_doctor_checks", None)

    rc, payload = _run_doctor_check("fake-ok")
    assert rc == 0
    assert payload == {
        "ok": True,
        "status": "OK",
        "deployment": "fixture",
    }

    rc, payload = _run_doctor_check("fake-fail")
    assert rc == 1
    assert payload["ok"] is False
    assert payload["errors"][0]["code"] == "fixture_failed"


def test_unknown_doctor_check_lists_builtins_and_deployment_checks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    dep = _write_deployment(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep if name == "fixture" else None,
    )
    sys.modules.pop("fixture_doctor_checks", None)

    rc, payload = _run_doctor_check("missing")
    assert rc == 1
    assert payload["ok"] is False
    available = payload["errors"][0]["available_checks"]
    assert "sources" in available
    assert "source-auth" in available
    assert "fake-ok" in available
    assert "fake-fail" in available
