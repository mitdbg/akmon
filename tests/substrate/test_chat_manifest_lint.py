"""`okg lint deployments` states its chat-block coverage (PACT chat-manifest-block).

Coverage has to be STATED, not inferred from an empty findings list, because
"no chat findings" is produced equally by three very different portfolios: one
where every block is clean, one where nobody declares a block at all, and one
where lint could not read the manifests. Reporting all three as clean is the
failure mode this file exists to prevent — most sharply the third, since
`lint_deployments` historically caught a manifest parse error, warned, and
CONTINUED, so a portfolio of unparseable manifests exited 0.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.runtime.lint import (
    CHAT_CHECKED,
    CHAT_NOT_CHECKABLE,
    CHAT_NOT_DECLARED,
    CHAT_VIOLATION,
    chat_block_coverage,
)


def _valid_chat() -> dict:
    return {
        "enabled": True,
        "ui": {
            "site_name": "Fixture Chat",
            "banner": "Fixture banner",
            "features": {"web_search": False},
        },
        "models": {
            "builtin": ["anthropic/claude-sonnet-5"],
            "default": "anthropic/claude-sonnet-5",
        },
        "preset": {"display_name": "Fixture Chat"},
        "search_profile": "analyst",
        "mcp": {"port": 8101, "tools": ["inspect", "search"]},
    }


def _write(
    root: Path,
    name: str,
    *,
    chat: dict | None = None,
    search: dict | None = None,
    body: str | None = None,
) -> Path:
    dep = root / name
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    manifest_path = dep / "deployment.yaml"
    if body is not None:
        manifest_path.write_text(body, encoding="utf-8")
        return manifest_path
    document: dict = {
        "name": name,
        "schema_dir": "schema",
        "source_registry": "source_registry.yaml",
        "search": (
            search
            if search is not None
            else {"profiles": {"analyst": {"subtypes": ["chunk"]}}}
        ),
    }
    if chat is not None:
        document["chat"] = chat
    manifest_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return manifest_path


def _by_deployment(root: Path) -> dict[str, object]:
    return {
        record.deployment: record
        for record in chat_block_coverage(deployments_root=root)
    }


# ---------------------------------------------------------------------------
# The three coverage states
# ---------------------------------------------------------------------------

def test_lint_reports_missing_chat_block_as_not_declared(
    tmp_path: Path,
) -> None:
    """Not-declared is distinct from checked-and-clean.

    One deployment declares a block; two do not. The two must not be counted
    toward the passing chat checks — a portfolio in which nobody declares a
    chat block can never report as a portfolio of passing chat checks.
    """
    _write(tmp_path, "with-chat", chat=_valid_chat())
    _write(tmp_path, "without-chat-a")
    _write(tmp_path, "without-chat-b")

    records = _by_deployment(tmp_path)
    assert records["with-chat"].state == CHAT_CHECKED
    assert records["without-chat-a"].state == CHAT_NOT_DECLARED
    assert records["without-chat-b"].state == CHAT_NOT_DECLARED

    passing = [r for r in records.values() if r.state == CHAT_CHECKED]
    assert len(passing) == 1
    assert not records["without-chat-a"].blocks_exit


def test_lint_reports_declared_valid_chat_block_as_checked(
    tmp_path: Path,
) -> None:
    """The PASS state is reachable, not merely the absence states.

    Without this, every other test here would still pass if `checked` were
    unreachable — the lint would report a correct portfolio as broken and
    nothing would notice.
    """
    _write(tmp_path, "with-chat", chat=_valid_chat())
    record = _by_deployment(tmp_path)["with-chat"]

    assert record.state == CHAT_CHECKED
    assert record.rule is None
    assert not record.blocks_exit


def test_lint_reports_unparseable_manifest_as_not_checkable(
    tmp_path: Path,
) -> None:
    """A manifest lint cannot read is not-checkable, and exits non-zero.

    Never skipped into a clean report: an unreadable manifest says nothing
    about its chat block, and silence must not read as approval.
    """
    _write(tmp_path, "broken", body="name: [unclosed\n")
    _write(tmp_path, "not-a-mapping", body="- just\n- a\n- list\n")

    records = _by_deployment(tmp_path)
    for name in ("broken", "not-a-mapping"):
        record = records[name]
        assert record.state == CHAT_NOT_CHECKABLE, name
        assert record.blocks_exit, name
        assert record.state != CHAT_CHECKED

    passing = [r for r in records.values() if r.state == CHAT_CHECKED]
    assert passing == []


# ---------------------------------------------------------------------------
# Each cross-reference rule fails lint
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "rule, mutate, search, offending",
    [
        (
            "undeclared search profile",
            lambda c: c.__setitem__("search_profile", "ghost"),
            None,
            "ghost",
        ),
        (
            "search profile with no search block",
            lambda c: None,
            {},
            "analyst",
        ),
        (
            "tool outside the known universe",
            lambda c: c["mcp"].__setitem__("tools", ["inspect", "teleport"]),
            None,
            "teleport",
        ),
        (
            "default model outside builtin",
            lambda c: c["models"].__setitem__("default", "meta/llama-4"),
            None,
            "meta/llama-4",
        ),
        (
            "empty builtin list",
            lambda c: c["models"].__setitem__("builtin", []),
            None,
            "builtin",
        ),
        (
            "unknown nested key",
            lambda c: c["preset"].__setitem__("display_nam", "typo"),
            None,
            "display_nam",
        ),
    ],
)
def test_lint_fails_on_each_cross_reference_violation(
    tmp_path: Path, rule: str, mutate, search: dict | None, offending: str,
) -> None:
    """Each rule is reported by name, with the offending value, non-zero."""
    chat = _valid_chat()
    mutate(chat)
    _write(tmp_path, "offender", chat=chat, search=search)

    record = _by_deployment(tmp_path)["offender"]
    assert record.state == CHAT_VIOLATION, rule
    assert record.blocks_exit, rule
    assert record.rule, rule
    assert offending in record.message, rule
    assert record.deployment == "offender"


def test_lint_reports_an_inadmissible_chat_accent(tmp_path: Path) -> None:
    """A chat-side accent fails lint, with the measured reason.

    The census established that no accent can reach a chat site at the pinned
    version. Lint must surface that as a violation rather than letting the
    portfolio pass with a color nobody will ever see rendered.
    """
    dep = tmp_path / "themed"
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    chat = _valid_chat()
    chat["ui"]["theme"] = {"base": "light", "accent": "#10b981"}
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "themed",
            "schema_dir": "schema",
            "source_registry": "source_registry.yaml",
            "search": {"profiles": {"analyst": {"subtypes": ["chunk"]}}},
            "viz": {"theme": {"accent": "#ff8800"}},
            "chat": chat,
        }),
        encoding="utf-8",
    )

    record = _by_deployment(tmp_path)["themed"]
    assert record.state == CHAT_VIOLATION
    assert "theme" in record.message
    assert "not drivable" in record.message.lower()
    # The console's own accent is untouched by the refusal.
    assert "#ff8800" not in record.message


# ---------------------------------------------------------------------------
# The CLI envelope
# ---------------------------------------------------------------------------

def _run_lint(root: Path, capsys) -> tuple[int, dict]:
    import argparse

    from okg.substrate.cli.main import _cmd_lint

    args = argparse.Namespace(
        lint_target="deployments", deployments_root=str(root),
    )
    code = _cmd_lint(args, as_json=True)
    return code, json.loads(capsys.readouterr().out)


def test_cli_envelope_carries_per_deployment_chat_state(
    tmp_path: Path, capsys,
) -> None:
    """`chat_coverage` names every deployment's state, and a clean run exits 0."""
    _write(tmp_path, "with-chat", chat=_valid_chat())
    _write(tmp_path, "without-chat")

    code, payload = _run_lint(tmp_path, capsys)

    assert code == 0
    states = {
        entry["deployment"]: entry["state"]
        for entry in payload["chat_coverage"]
    }
    assert states == {
        "with-chat": CHAT_CHECKED, "without-chat": CHAT_NOT_DECLARED,
    }
    assert payload["chat_summary"] == {
        "checked": 1, "blocking": 0, "total": 2,
    }


def test_cli_exits_non_zero_on_a_chat_violation(
    tmp_path: Path, capsys,
) -> None:
    """A violating chat block fails the command, naming deployment and value."""
    chat = _valid_chat()
    chat["search_profile"] = "ghost"
    _write(tmp_path, "offender", chat=chat)

    code, payload = _run_lint(tmp_path, capsys)

    assert code == 1
    entry = payload["chat_coverage"][0]
    assert entry["deployment"] == "offender"
    assert entry["state"] == CHAT_VIOLATION
    assert "ghost" in entry["message"]
    assert entry["rule"].startswith("unknown_search_profile")
    assert payload["chat_summary"]["checked"] == 0


def test_cli_exits_non_zero_on_an_unparseable_manifest(
    tmp_path: Path, capsys,
) -> None:
    """Not-checkable exits non-zero rather than reporting a broken portfolio clean."""
    _write(tmp_path, "broken", body="name: [unclosed\n")

    code, payload = _run_lint(tmp_path, capsys)

    assert code == 1
    assert payload["chat_coverage"][0]["state"] == CHAT_NOT_CHECKABLE
    assert payload["chat_summary"]["blocking"] == 1
