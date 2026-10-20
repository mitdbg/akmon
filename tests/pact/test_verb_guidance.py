"""Per `polish-pact-agent-ux` req.polish.verb-guidance.

Deleted legacy pact verb shapes and typos return a one-line did-you-mean
correction (no argparse usage dump); `okg query` redirects to the real
graph-read surfaces; pact/AGENTS.md no longer claims deleted verbs forward.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.main import main

from tests.private_policy import private_policy_path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _seed_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 0\nproject: okg\n", encoding="utf-8"
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    return root


def test_deleted_verb_returns_did_you_mean_without_usage(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["pact", "--root", str(root), "validate", "demo"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "did you mean `okg pact gate post-edit <id>`" in err
    assert "usage:" not in err
    assert "invalid choice" not in err


def test_typo_returns_did_you_mean(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["pact", "--root", str(root), "nex", "demo"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "did you mean `okg pact view <id> --format=next`" in err
    assert "supported verbs:" in err


def test_flat_status_and_attach_guesses_name_canonical_shapes(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    with pytest.raises(SystemExit):
        main(["pact", "--root", str(root), "status", "demo"])
    assert "okg pact view <id> --format=status" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        main(["pact", "--root", str(root), "attach", "demo"])
    assert "okg pact evidence attach" in capsys.readouterr().err


# The full flat-verb deletion list from `complete-pact-surface-cut` (the
# enumeration the pre-fix pact/AGENTS.md carried). Every one of these must
# get a did-you-mean correction, never a usage dump.
CUT_VERBS = [
    "normalize", "render", "check-view", "validate", "attach-evidence",
    "loop", "advance", "change", "require", "dep", "waiver", "bind-tla",
    "recheck-alignment", "edit", "explain", "show", "status", "list",
    "next", "brief", "review", "version", "fast-path",
]


@pytest.mark.parametrize("verb", CUT_VERBS)
def test_every_cut_verb_gets_did_you_mean(verb: str, tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["pact", "--root", str(root), verb, "demo"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "did you mean" in err, f"`okg pact {verb}` fell through to argparse"
    assert "usage:" not in err
    assert "invalid choice" not in err


def test_require_does_not_suggest_review() -> None:
    from okg.substrate.pact.cli import _unknown_verb_hint

    hint = _unknown_verb_hint("require")
    assert hint is not None
    assert "chat-review" not in hint
    assert "pact.yaml" in hint


def test_gibberish_falls_back_to_argparse_error(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    with pytest.raises(SystemExit) as exc:
        main(["pact", "--root", str(root), "zzxqwv", "demo"])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "invalid choice" in err


def test_supported_verbs_unaffected(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    rc = main(["pact", "--root", str(root), "new", "demo", "--json"])
    assert rc == 0
    capsys.readouterr()
    rc = main(["pact", "--root", str(root), "view", "demo", "--format=status", "--json"])
    assert rc == 0


def _no_deployment_anywhere(monkeypatch, tmp_path) -> None:
    """No flag, no variable, and a working directory outside any checkout
    that configures a default deployment."""
    for name in ("OKG_DEPLOYMENT", "OKG_PACT_GRAPH_DEPLOYMENT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)


def test_okg_query_redirects_to_search_and_mcp(capsys, monkeypatch, tmp_path) -> None:
    _no_deployment_anywhere(monkeypatch, tmp_path)
    rc = main(["query", "recent pacts"])
    assert rc != 0
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    # A bare `okg query "<words>"` meant a text search: it still gets the
    # pointer, now naming the real read commands (`okg query` is the
    # read-only SQL operator since issue 3666).
    assert "`okg query` runs one read-only SQL statement" in combined
    assert "okg search" in combined
    assert "okg inspect" in combined


def test_okg_query_with_a_deployment_reaches_the_command(
    capsys, monkeypatch, tmp_path,
) -> None:
    from okg.substrate.cli.main import _query_redirect_error

    _no_deployment_anywhere(monkeypatch, tmp_path)
    assert _query_redirect_error(["query", "--deployment", "d", "select 1"]) is None
    assert _query_redirect_error(["--json", "query", "--help"]) is None
    assert _query_redirect_error(["query", "recent pacts"]) is not None


def test_bare_okg_query_reaches_the_command_in_a_checkout_with_a_configured_default(
    monkeypatch, tmp_path,
) -> None:
    """The order `okg inspect` resolves a deployment in, all of it."""
    from okg.substrate.cli.main import _query_redirect_error

    _no_deployment_anywhere(monkeypatch, tmp_path)
    assert _query_redirect_error(["query", "select 1"]) is not None
    config = tmp_path / ".okg" / "coordination.yaml"
    config.parent.mkdir()
    config.write_text("graph_projection:\n  deployment: configured\n", encoding="utf-8")
    assert _query_redirect_error(["query", "select 1"]) is None
    assert _query_redirect_error(["query", "recent pacts"]) is None


def test_bare_okg_query_reaches_the_command_with_a_deployment_in_the_environment(
    monkeypatch, tmp_path,
) -> None:
    """PACT read-commands-shell-3666: `OKG_DEPLOYMENT` stands in for the
    flag, so the call goes to the command and its tool refuses text that is
    not SQL."""
    from okg.substrate.cli.main import _query_redirect_error

    _no_deployment_anywhere(monkeypatch, tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENT", "d")
    assert _query_redirect_error(["query", "recent pacts"]) is None
    assert _query_redirect_error(["query", "select 1"]) is None
    monkeypatch.setenv("OKG_DEPLOYMENT", " ")
    assert _query_redirect_error(["query", "recent pacts"]) is not None


def test_okg_query_redirect_json_envelope(capsys, monkeypatch, tmp_path) -> None:
    _no_deployment_anywhere(monkeypatch, tmp_path)
    rc = main(["--json", "query", "recent pacts"])
    assert rc != 0
    captured = capsys.readouterr()
    payload = json.loads((captured.out + captured.err).strip().splitlines()[-1])
    assert payload["error"]["code"] == "substrate_error"
    assert "okg search" in payload["error"]["fix_hint"]


def test_agents_md_no_longer_claims_forwards() -> None:
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")
    assert "They forward to the six" not in text
    assert "did you mean" in text
