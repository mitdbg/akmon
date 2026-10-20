"""The framework provenance proof must survive every path git can emit.

``_installed_framework_matches_revision`` compares the installed ``okg``
package against a git revision file by file. It had no direct test, and its
broad ``except`` turns any parsing accident into ``False`` -- which the caller
reports as "framework release provenance is not proven". So a proof that
silently stops proving looks exactly like a proof that failed.

The concrete escape: ``git ls-tree --name-only`` applies ``core.quotePath``,
so a path holding a non-ASCII byte comes back QUOTED and octal-escaped rather
than as a real path. One accented filename anywhere under ``src/okg`` was
enough to disable the check.
"""

from __future__ import annotations

import subprocess
import os
import sys
from pathlib import Path

import pytest

from okg.substrate import deployment_upgrade as upgrade
from okg.substrate.deployment_upgrade import _installed_framework_matches_revision

# A real Synthea fixture name from src/okg/bench/synth/world_prose/.
NON_ASCII_NAME = "Alfredo17_Marroquín624_4f80586c.json"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True, text=True, check=True,
    ).stdout


def _repo_with(tmp_path: Path, files: dict[str, bytes]) -> tuple[Path, str, Path]:
    """Build a git repo plus a matching 'installed' package copy."""
    repo = tmp_path / "repo"
    (repo / "src" / "okg" / "substrate").mkdir(parents=True)
    _git(repo.parent, "init", "-q", str(repo))
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")

    install = tmp_path / "install" / "okg"
    for rel, body in files.items():
        tracked = repo / "src" / "okg" / rel
        tracked.parent.mkdir(parents=True, exist_ok=True)
        tracked.write_bytes(body)
        installed = install / rel
        installed.parent.mkdir(parents=True, exist_ok=True)
        installed.write_bytes(body)

    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "seed")
    revision = _git(repo, "rev-parse", "HEAD").strip()
    return repo, revision, install / "substrate" / "deployment_upgrade.py"


def test_a_non_ascii_filename_does_not_silently_disable_the_proof(tmp_path) -> None:
    repo, revision, running = _repo_with(
        tmp_path,
        {
            "substrate/deployment_upgrade.py": b"x = 1\n",
            f"bench/synth/world_prose/{NON_ASCII_NAME}": b'{"name": "Alfredo"}\n',
        },
    )
    result = _installed_framework_matches_revision(repo, revision, running)
    assert result.status == "match"
    assert result.matched is True


def test_an_ascii_only_tree_still_proves(tmp_path) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )
    result = _installed_framework_matches_revision(repo, revision, running)
    assert result.status == "match"
    assert result.matched is True


def test_a_changed_byte_still_refuses_even_with_a_non_ascii_path(tmp_path) -> None:
    """The fix must not make the proof permissive -- only correct."""
    repo, revision, running = _repo_with(
        tmp_path,
        {
            "substrate/deployment_upgrade.py": b"x = 1\n",
            f"bench/synth/world_prose/{NON_ASCII_NAME}": b'{"name": "Alfredo"}\n',
        },
    )
    tampered = running.parent.parent / "bench" / "synth" / "world_prose" / NON_ASCII_NAME
    tampered.write_bytes(b'{"name": "someone else"}\n')
    result = _installed_framework_matches_revision(repo, revision, running)
    assert result.status == "mismatch"
    assert result.reason == "installed_file_bytes_differ"


def test_a_file_missing_from_the_install_refuses(tmp_path) -> None:
    repo, revision, running = _repo_with(
        tmp_path,
        {
            "substrate/deployment_upgrade.py": b"x = 1\n",
            f"bench/synth/world_prose/{NON_ASCII_NAME}": b'{"name": "Alfredo"}\n',
        },
    )
    (running.parent.parent / "bench" / "synth" / "world_prose" / NON_ASCII_NAME).unlink()
    result = _installed_framework_matches_revision(repo, revision, running)
    assert result.status == "mismatch"
    assert result.reason == "installed_file_missing"


def test_a_bad_revision_is_unevaluable_not_a_mismatch(tmp_path) -> None:
    repo, _revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    result = _installed_framework_matches_revision(repo, "not-a-revision", running)

    assert result.status == "unevaluable"
    assert result.reason == "invalid_revision"
    assert result.matched is False


def test_a_non_git_root_is_unevaluable_not_a_mismatch(tmp_path) -> None:
    root = tmp_path / "not-a-repo"
    root.mkdir()
    running = tmp_path / "install" / "okg" / "substrate" / "deployment_upgrade.py"
    running.parent.mkdir(parents=True)
    running.write_bytes(b"x = 1\n")

    result = _installed_framework_matches_revision(root, "HEAD", running)

    assert result.status == "unevaluable"
    assert result.reason == "non_git_repository"


def test_git_timeout_is_unevaluable_not_a_mismatch(tmp_path, monkeypatch) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    def timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("git", 30)

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._git_command", timeout
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "git_timeout"


def test_git_ls_tree_failure_is_not_mislabeled_as_invalid_revision(
    tmp_path, monkeypatch
) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    def command_failure(*_args, **_kwargs):
        raise ValueError("git ls-tree failed: permission denied")

    monkeypatch.setattr(upgrade, "_git_command", command_failure)
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "git_command_failed"


def test_decoding_failure_is_unevaluable_not_a_mismatch(tmp_path, monkeypatch) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    def decode_failure(*_args, **_kwargs):
        raise UnicodeDecodeError("utf-8", b"\xed", 0, 1, "invalid start byte")

    monkeypatch.setattr(
        "okg.substrate.deployment_upgrade._git_command", decode_failure
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "decoding_failed"


def test_raw_bytes_tree_entry_is_not_decoded_as_utf8(tmp_path, monkeypatch) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )
    raw_path = b"src/okg/raw-\xed.json"

    def raw_tree(_repo, args, **_kwargs):
        assert args[0] == "ls-tree"
        return b"100644 blob " + b"0" * 40 + b"\t" + raw_path + b"\0"

    monkeypatch.setattr(upgrade, "_git_command", raw_tree)
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "mismatch"
    assert result.reason == "installed_file_missing"
    assert result.path == os.fsdecode(raw_path)


def test_malformed_tree_path_is_unevaluable(tmp_path, monkeypatch) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    def malformed_tree(_repo, args, **_kwargs):
        assert args[0] == "ls-tree"
        return b"100644 blob " + b"0" * 40 + b"\tsrc/okg/../outside.py\0"

    monkeypatch.setattr(upgrade, "_git_command", malformed_tree)
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "invalid_tree_path"


def test_dot_tree_path_is_unevaluable(tmp_path, monkeypatch) -> None:
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    monkeypatch.setattr(
        upgrade,
        "_git_command",
        lambda _repo, args, **_kwargs: (
            b"100644 blob " + b"0" * 40 + b"\tsrc/okg/.\0"
        ),
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "invalid_tree_path"


@pytest.mark.skipif(
    sys.platform == "darwin",
    reason="macOS rejects the invalid pathname bytes accepted by ext4",
)
def test_raw_non_utf8_tree_path_is_compared_byte_safely(tmp_path) -> None:
    repo, _revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )
    raw_path = b"src/okg/bench/raw-\xed.json"
    contents = b"raw filename\n"
    blob = subprocess.run(
        ["git", "-C", str(repo), "hash-object", "-w", "--stdin"],
        input=contents,
        capture_output=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(repo), "update-index", "--add", "--index-info"],
        input=b"100644 " + blob + b"\t" + raw_path + b"\n",
        capture_output=True,
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-qm", "raw pathname"],
        check=True,
    )
    installed = running.parent.parent / os.fsdecode(raw_path[len(b"src/okg/") :])
    installed.parent.mkdir(parents=True, exist_ok=True)
    installed.write_bytes(contents)
    revision = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True,
        check=True,
        text=True,
    ).stdout.strip()

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "match"


def test_a_malformed_tree_entry_is_unevaluable_not_a_mismatch(
    tmp_path, monkeypatch
) -> None:
    """An entry that is not `<mode> <type> <id>TAB<path>` proves nothing."""
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    monkeypatch.setattr(
        upgrade,
        "_git_command",
        lambda _repo, args, **_kwargs: b"src/okg/substrate/deployment_upgrade.py\0",
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "malformed_tree_entry"


def test_a_non_blob_tree_entry_is_unevaluable_not_a_mismatch(
    tmp_path, monkeypatch
) -> None:
    """A gitlink cannot be byte-compared, so it stops the proof rather than
    reading as tampering."""
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    monkeypatch.setattr(
        upgrade,
        "_git_command",
        lambda _repo, args, **_kwargs: (
            b"160000 commit " + b"0" * 40 + b"\tsrc/okg/vendored\0"
        ),
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "unexpected_tree_entry"
    assert result.path == "src/okg/vendored"


def test_a_non_hex_object_id_is_unevaluable_not_a_mismatch(
    tmp_path, monkeypatch
) -> None:
    """An id this code cannot recompute is a read failure, not a difference."""
    repo, revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    monkeypatch.setattr(
        upgrade,
        "_git_command",
        lambda _repo, args, **_kwargs: (
            b"100644 blob " + b"z" * 40 + b"\tsrc/okg/substrate/deployment_upgrade.py\0"
        ),
    )
    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "unknown_git_object_format"


@pytest.mark.skipif(os.name != "posix", reason="symlink tree entries need POSIX")
def test_a_symlink_tree_entry_is_unevaluable_not_a_mismatch(tmp_path) -> None:
    """A `120000` entry is a blob whose CONTENT is the link target.

    Reading the installed path returns the bytes of whatever it points at, so
    comparing those against the entry's object id reports a mismatch on an
    untampered install. The mode has to decide, not the type.
    """
    repo, _revision, running = _repo_with(
        tmp_path, {"substrate/deployment_upgrade.py": b"x = 1\n"}
    )

    # A real symlink, in the tracked tree and in the installed copy alike.
    for root in (repo / "src" / "okg", running.parents[1]):
        (root / "substrate" / "alias.py").symlink_to("deployment_upgrade.py")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "symlink")
    revision = _git(repo, "rev-parse", "HEAD").strip()

    entries = _git(repo, "ls-tree", "-r", revision, "src/okg")
    assert "120000 blob" in entries

    result = _installed_framework_matches_revision(repo, revision, running)

    assert result.status == "unevaluable"
    assert result.reason == "unsupported_tree_entry_mode"
    assert result.path == "src/okg/substrate/alias.py"
