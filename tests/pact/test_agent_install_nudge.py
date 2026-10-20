"""task.pai.first-run-nudge: cmd_pact nudges toward `okg pact install` until
the bundle marker is present — human (non-JSON) mode only, and writing nothing.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact.agent_install import install_agent_setup, nudge_message


def _empty_pact_repo(tmp_path: Path) -> Path:
    (tmp_path / "pact" / "changes").mkdir(parents=True)
    return tmp_path


def test_first_run_nudges_then_silent(tmp_path: Path) -> None:
    # Absent marker -> a nudge pointing at `okg pact install`.
    msg = nudge_message(tmp_path)
    assert msg is not None and "okg pact install" in msg
    # Installing writes the marker -> silent thereafter.
    install_agent_setup("codex", tmp_path)
    assert nudge_message(tmp_path) is None


def test_nudge_writes_nothing(tmp_path: Path) -> None:
    before = sorted(p.name for p in tmp_path.iterdir())
    nudge_message(tmp_path)
    assert sorted(p.name for p in tmp_path.iterdir()) == before  # pure read


def test_cmd_pact_human_mode_emits_nudge(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _empty_pact_repo(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_PACT_QUIET_NUDGE", raising=False)
    main(["pact", "--root", str(tmp_path), "view", "--format=list"])
    assert "okg pact install" in capsys.readouterr().err


def test_cmd_pact_json_mode_is_silent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _empty_pact_repo(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    # JSON / agent mode: machine output stays clean — no nudge on stderr.
    main(["pact", "--root", str(tmp_path), "view", "--format=list", "--json"])
    assert "okg pact install" not in capsys.readouterr().err


def test_nudge_silent_once_installed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _empty_pact_repo(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.delenv("OKG_PACT_QUIET_NUDGE", raising=False)
    install_agent_setup("claude-code", tmp_path)
    main(["pact", "--root", str(tmp_path), "view", "--format=list"])
    assert "okg pact install" not in capsys.readouterr().err


def test_quiet_env_suppresses_nudge(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _empty_pact_repo(tmp_path)
    monkeypatch.delenv("OKG_AGENT", raising=False)
    monkeypatch.setenv("OKG_PACT_QUIET_NUDGE", "1")
    main(["pact", "--root", str(tmp_path), "view", "--format=list"])
    assert "okg pact install" not in capsys.readouterr().err
