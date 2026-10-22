"""`okg jobs` subcommands refuse a missing deployment with a named error."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.cli.jobs import _cmd_jobs


def _args(subcmd: str, **kwargs):
    base = {
        "jobs_subcmd": subcmd,
        "deployment": None,
        "dsn": None,
        "apply": True,
        "runtime_deployment": None,
        "source": "docs",
        "scope_key": None,
        "run_key": "k",
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


@pytest.mark.parametrize("subcmd", ["status", "enqueue-source", "find-source-run"])
@pytest.mark.parametrize("as_json", [False, True])
def test_jobs_without_deployment_prints_named_error_not_traceback(
    subcmd: str, as_json: bool, monkeypatch, capsys
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    rc = _cmd_jobs(_args(subcmd), as_json=as_json)

    err = capsys.readouterr().err
    assert rc == 1
    assert "--deployment" in err
    assert "Traceback" not in err


@pytest.mark.parametrize("as_json", [False, True])
def test_chat_source_retry_without_deployment_prints_named_error(
    as_json: bool, monkeypatch, capsys
) -> None:
    from okg.substrate.cli.chat import _cmd_source_retry

    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_AGENT", raising=False)

    rc = _cmd_source_retry(
        SimpleNamespace(deployment=None, dsn=None, source="docs"),
        as_json=as_json,
        runner=lambda *a, **k: None,
    )

    err = capsys.readouterr().err
    assert rc == 1
    assert "--deployment" in err
    assert "Traceback" not in err
