"""route-canvas-token-to-keychain: `okg pi set-token` CLI verb."""
from __future__ import annotations

import argparse
import importlib.util
import io
from pathlib import Path


def _install_module():
    import okg.substrate.library.profiles as profiles_pkg
    install_path = (
        Path(list(profiles_pkg.__path__)[0])
        / "academic-researcher" / "cli" / "install.py"
    )
    spec = importlib.util.spec_from_file_location(
        "academic_researcher_install_set_token", install_path,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_set_token_stores_in_keychain(monkeypatch) -> None:
    install = _install_module()
    captured: dict = {}

    def fake_set(*, deployment, provider, token):
        captured.update(
            deployment=deployment, provider=provider, token=token,
        )

    import okg.substrate.library.oauth.static_token as st
    monkeypatch.setattr(st, "set_static_token", fake_set)

    args = argparse.Namespace(
        deployment="madden_pi", provider="canvas", token="instr-tok",
    )
    rc = install._cmd_set_token(args, as_json=True)
    assert rc == 0
    assert captured == {
        "deployment": "madden_pi", "provider": "canvas",
        "token": "instr-tok",
    }


def test_set_token_reads_stdin_when_arg_omitted(monkeypatch) -> None:
    install = _install_module()
    captured: dict = {}
    import okg.substrate.library.oauth.static_token as st
    monkeypatch.setattr(
        st, "set_static_token",
        lambda **kw: captured.update(kw),
    )
    monkeypatch.setattr("sys.stdin", io.StringIO("stdin-tok\n"))

    args = argparse.Namespace(
        deployment="pi", provider="canvas", token=None,
    )
    rc = install._cmd_set_token(args, as_json=True)
    assert rc == 0
    assert captured["token"] == "stdin-tok"


def test_set_token_empty_fails(monkeypatch) -> None:
    install = _install_module()
    monkeypatch.setattr("sys.stdin", io.StringIO("\n"))
    args = argparse.Namespace(
        deployment="pi", provider="canvas", token=None,
    )
    rc = install._cmd_set_token(args, as_json=True)
    assert rc != 0


def test_set_token_verb_registered() -> None:
    install = _install_module()
    assert "set-token" in install.DISPATCH
    # Build just the set-token subparser in isolation (the full
    # build_parsers pulls in sibling parsers whose relative imports
    # don't resolve under a standalone-loaded module).
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers()
    install._build_set_token_parser(sub)
    ns = parser.parse_args(
        ["set-token", "--deployment", "pi", "--provider", "canvas"],
    )
    assert ns.provider == "canvas"
    assert ns.token is None  # optional → stdin path
