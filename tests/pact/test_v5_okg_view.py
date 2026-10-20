"""`okg pact view` keeps every projection working on a v5 manifest.

CLAUDE.md, the `agent-workflow` and `dev-process` skills, and the agent
runbooks all mandate `okg pact view <id> --format=bootstrap --json`. The v5
CLI defines only `brief` and `yaml`, so rerouting those commands to it made
argparse exit 2 on the exact command the instructions require. The v4 reader
owns the other projections and reads a v5 manifest through
`okg.substrate.pact.v5_adapter.to_v4`, so the reroute must decline them.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from _v5_repo import MANIFEST, commit_all, make_repo, write_manifest
from okg.substrate.pact.store import load_manifest_bytes
from okg.substrate.pact.v5_adapter import is_archived_path, to_v4

#: The projections the agent instructions mandate; none is a v5 CLI format.
V4_ONLY_FORMATS = ["bootstrap", "next", "packet", "chat-review", "review-queue"]


@pytest.fixture
def v5_repo(tmp_path: Path, monkeypatch) -> Path:
    monkeypatch.setenv("PACT_USAGE_LOG", "off")
    # No ledger, no graph: these views must answer from git and the file alone.
    for name in (
        "OKG_PACT_GRAPH_DEPLOYMENT",
        "OKG_DEPLOYMENT",
        "OKG_ENVIRONMENT_ID",
        "OKG_DSN",
        "DATABASE_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    root = make_repo(tmp_path)
    write_manifest(root, MANIFEST)
    (root / "src").mkdir()
    (root / "src" / "a.py").write_text("x = 1\n", encoding="utf-8")
    commit_all(root, "feat")
    return root


@pytest.mark.parametrize("fmt", V4_ONLY_FORMATS)
def test_okg_pact_view_renders_v4_projections_of_a_v5_manifest(
    v5_repo: Path, monkeypatch, capsys, fmt: str
) -> None:
    from okg.substrate.cli.main import main as okg_main

    monkeypatch.chdir(v5_repo)
    assert okg_main(["pact", "view", "demo", "--format", fmt, "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["blocked_by"] == []


@pytest.mark.parametrize("fmt", V4_ONLY_FORMATS)
def test_reroute_declines_formats_the_v5_cli_does_not_define(
    v5_repo: Path, monkeypatch, fmt: str
) -> None:
    from okg.substrate.cli.main import _pact_v5_reroute

    monkeypatch.chdir(v5_repo)
    # Both spellings: `--format X` and `--format=X`.
    assert _pact_v5_reroute(["view", "demo", "--format", fmt, "--json"]) is None
    assert _pact_v5_reroute(["view", "demo", f"--format={fmt}", "--json"]) is None


@pytest.mark.parametrize("fmt", ["brief", "yaml"])
def test_reroute_keeps_the_v5_native_formats(v5_repo: Path, monkeypatch, fmt: str) -> None:
    from okg.substrate.cli.main import _pact_v5_reroute

    monkeypatch.chdir(v5_repo)
    assert _pact_v5_reroute(["view", "demo", "--format", fmt, "--json"]) == [
        "view", "demo", "--format", fmt, "--json",
    ]
    assert _pact_v5_reroute(["view", "demo", f"--format={fmt}", "--json"]) == [
        "view", "demo", f"--format={fmt}", "--json",
    ]


def test_reroute_without_a_format_still_belongs_to_v5(v5_repo: Path, monkeypatch) -> None:
    from okg.substrate.cli.main import _pact_v5_reroute

    monkeypatch.chdir(v5_repo)
    # `view` defaults to `brief` in the v5 CLI, which it does define.
    assert _pact_v5_reroute(["view", "demo", "--json"]) == ["view", "demo", "--json"]


def test_archived_v5_manifest_reads_as_terminal_not_as_a_fresh_proposal(
    tmp_path: Path,
) -> None:
    """A merged v5 change carries no evidence rows of its own here; without the
    archive signal the adapter derived `proposal` and the review surfaces
    listed settled work as in-flight."""
    payload = yaml.safe_dump(MANIFEST).encode("utf-8")
    active = tmp_path / "pact" / "changes" / "demo" / "pact.yaml"
    archived = tmp_path / "pact" / "archive" / "demo" / "pact.yaml"

    assert load_manifest_bytes(payload, source_path=active).change.status == "proposal"
    # v4 has no terminal member in the authored vocabulary: it projects every
    # terminal lifecycle state onto "verification", so parity is that value.
    assert load_manifest_bytes(payload, source_path=archived).change.status == "verification"
    assert to_v4(MANIFEST, archived=True)["change"]["method_metadata"]["archived"] is True


def test_is_archived_path_needs_the_adjacent_pact_archive_pair() -> None:
    assert is_archived_path(Path("/repo/pact/archive/demo/pact.yaml"))
    assert not is_archived_path(Path("/repo/pact/changes/demo/pact.yaml"))
    # A directory merely named `archive` must not retire a live change.
    assert not is_archived_path(Path("/repo/archive/pact/changes/demo/pact.yaml"))


#: Every verb v5 removed. A projection that recommends one sends the agent
#: reading it straight into `verb_removed`.
REMOVED_VERBS = (
    "pact decide",
    "pact gate",
    "pact hooks",
    "pact evidence ingest",
    "pact evidence status",
    "pact evidence waive",
    "pact migrate-version",
    "--format=status",
)


def test_the_v4_projections_never_recommend_a_removed_verb_for_a_v5_change(
    v5_repo: Path,
) -> None:
    """`--format=bootstrap` stays with the v4 reader; its advice must not.

    The bootstrap brief is the entry point CLAUDE.md and the agent-workflow
    skill send every agent to, and it derived its next command from the
    READER's version rather than the manifest's. So every v5 change was told
    to run `okg pact decide <id> --verdict approve`, a verb that answers
    `verb_removed`. The whole corpus is v5, so that was the advice for every
    change in the repository.
    """
    from okg.substrate.pact.cli import _lifecycle_successor

    for state in ("proposed", "awaiting_review", "draft", "retired", "rejected", "in_progress"):
        command, owner = _lifecycle_successor(v5_repo, "demo", state)
        assert owner in {"agent", "operator"}
        for removed in REMOVED_VERBS:
            assert removed not in command, (state, command)
        assert "pact digest demo" in command or "pact check demo" in command


def test_a_v4_change_still_gets_the_v4_successor(tmp_path: Path) -> None:
    """The switch reads the manifest, so it cannot silently retarget v4.

    Without this, "no removed verb is recommended" would also pass on a
    helper that had stopped reading the manifest at all.
    """
    from okg.substrate.pact.cli import _lifecycle_successor

    path = tmp_path / "pact" / "changes" / "legacy" / "pact.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        "pact_version: 4\ntarget_pact_version: 4\n"
        "change:\n  id: legacy\n  title: L\n  status: proposal\n",
        encoding="utf-8",
    )

    command, owner = _lifecycle_successor(tmp_path, "legacy", "proposed")

    assert "pact decide legacy --verdict approve" in command
    assert owner == "operator"
