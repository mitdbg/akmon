"""The library CLI exposes exactly the resolver's supported ref kinds."""
from __future__ import annotations

import json

import pytest

import okg.substrate.library as library
from okg.substrate.cli.main import main
from okg.substrate.library.resolver import KINDS


@pytest.mark.parametrize("kind", KINDS)
def test_library_list_accepts_each_supported_kind(kind: str, capsys) -> None:
    assert main(["--json", "library", "list", "--kind", kind]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert set(payload) == {kind}


def test_library_list_help_uses_resolver_kinds(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["library", "list", "--help"])

    assert exc.value.code == 0
    help_text = capsys.readouterr().out
    assert "{" + ",".join(KINDS) + "}" in help_text
    assert "ontology" not in help_text
    assert "narrowings" not in help_text


@pytest.mark.parametrize("retired_kind", ("ontology", "narrowings"))
def test_library_list_refuses_retired_kinds_before_resolution(
    retired_kind: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    def unexpected_resolution(_kind: str) -> list[str]:
        raise AssertionError("retired kind reached library resolution")

    monkeypatch.setattr(library, "available_refs", unexpected_resolution)

    with pytest.raises(SystemExit) as exc:
        main(["--json", "library", "list", "--kind", retired_kind])

    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err
