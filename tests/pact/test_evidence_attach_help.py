"""`okg pact evidence attach --help` shows the form a v5 change takes."""

from __future__ import annotations

import pytest

from okg.substrate.cli.main import _pact_v5_reroute
from okg.substrate.cli.main import main as okg_main


@pytest.mark.parametrize("flag", ["--help", "-h"])
def test_attach_help_without_id_reroutes_to_v5(flag: str) -> None:
    assert _pact_v5_reroute(["evidence", "attach", flag]) == ["evidence", "attach", flag]


def test_attach_help_without_id_prints_v5_flags(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        okg_main(["pact", "evidence", "attach", "--help"])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    assert "--requirement" in out
    assert "executed_test" in out
    assert "--ref" not in out
    assert "--summary" not in out


def test_attach_without_id_or_help_still_refuses() -> None:
    # Only a bare help request reroutes; anything else keeps its old path.
    assert _pact_v5_reroute(["evidence", "attach"]) is None
