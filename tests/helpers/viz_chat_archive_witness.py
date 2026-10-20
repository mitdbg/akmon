"""Generate the retire-viz-chat orphan-sweep witness fixture from git.

PACT layout-history-openspec deleted ``archive/`` from the working tree, so
the archived copy of the retired viz chat surface lives only in git history.
The orphan sweep in ``tests/substrate/test_retire_viz_chat.py`` needs each of
its patterns witnessed in that archived copy, or a typo'd pattern contributes
a vacuous zero. CI checks out shallow history, so the sweep cannot read the
archive commit at test time; this script records one witness per pattern
into a small checked-in fixture instead, and the test reads the fixture.

Run from the repository root, with full history available::

    .venv/bin/python -m tests.helpers.viz_chat_archive_witness

It rewrites ``tests/substrate/fixtures/viz_chat_archive_witness.json``. The
test module's ``test_witness_fixture_matches_the_archive_commit`` regenerates
the same document in memory whenever the archive commit is present and fails
on any difference, so a stale fixture cannot survive a full-history run.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "substrate" / "fixtures" / "viz_chat_archive_witness.json"
COMMAND = ".venv/bin/python -m tests.helpers.viz_chat_archive_witness"

#: A recorded line longer than this is cut to a window around the pattern, so
#: a minified bundle line cannot bloat the fixture. The window still contains
#: the pattern, which is the property the test checks.
MAX_TEXT = 200
CONTEXT = 80


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def _excerpt(text: str, pattern: str) -> str:
    text = text.rstrip("\n")
    if len(text) <= MAX_TEXT:
        return text
    at = text.index(pattern)
    return text[max(0, at - CONTEXT): at + len(pattern) + CONTEXT]


def _first_witness(commit: str, path: str, pattern: str) -> dict[str, object]:
    """The first line under ``path`` at ``commit`` containing ``pattern``.

    ``git grep`` lists matches in path order, then line order, so "first" is
    deterministic. Binary files are skipped (``-I``). Raises if nothing
    matches: an unwitnessed pattern must not be recorded as witnessed.
    """
    proc = subprocess.run(
        [
            "git", "-C", str(REPO_ROOT), "grep", "-I", "-n", "-F",
            "-e", pattern, commit, "--", path,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 1:
        raise ValueError(f"pattern {pattern!r} has no witness under {path} at {commit}")
    if proc.returncode != 0:
        raise RuntimeError(f"git grep failed for {pattern!r}: {proc.stderr.strip()}")
    first = proc.stdout.splitlines()[0]
    prefix = f"{commit}:"
    if not first.startswith(prefix):
        raise RuntimeError(f"unexpected git grep line: {first!r}")
    file_path, line_no, text = first[len(prefix):].split(":", 2)
    return {"path": file_path, "line": int(line_no), "text": _excerpt(text, pattern)}


def generate(
    patterns: Iterable[str],
    *,
    chat_commit: str,
    chat_path: str,
    ui_commit: str,
    ui_path: str,
) -> dict[str, object]:
    """Build the fixture document. Pure apart from the git reads."""
    witnesses = {
        pattern: _first_witness(chat_commit, chat_path, pattern)
        for pattern in sorted(set(patterns))
    }
    return {
        "_generated_by": COMMAND,
        "_about": (
            "One witness per retire-viz-chat orphan-sweep pattern, read from "
            "the archived copy in git (archive/ is no longer in the tree). "
            "Do not edit by hand; rerun the command above."
        ),
        "chat_archive": {
            "commit": chat_commit,
            "path": chat_path,
            "index_blob": _git("rev-parse", f"{chat_commit}:{chat_path}/INDEX.md").strip(),
        },
        "ui_archive": {
            "commit": ui_commit,
            "path": ui_path,
            "index_blob": _git("rev-parse", f"{ui_commit}:{ui_path}/INDEX.md").strip(),
        },
        "witnesses": witnesses,
    }


def render(document: dict[str, object]) -> str:
    return json.dumps(document, indent=2, ensure_ascii=False) + "\n"


def current_document() -> dict[str, object]:
    """Generate from the constants the code and the sweep actually use."""
    from okg.substrate.deployments.chat_block import (
        VIZ_CHAT_ARCHIVE_COMMIT,
        VIZ_CHAT_ARCHIVE_PATH,
    )
    from okg.viz.app import LEGACY_VIZ_ARCHIVE_COMMIT, LEGACY_VIZ_ARCHIVE_PATH
    from tests.substrate.test_retire_viz_chat import SWEEP_PATTERNS

    return generate(
        SWEEP_PATTERNS,
        chat_commit=VIZ_CHAT_ARCHIVE_COMMIT,
        chat_path=VIZ_CHAT_ARCHIVE_PATH,
        ui_commit=LEGACY_VIZ_ARCHIVE_COMMIT,
        ui_path=LEGACY_VIZ_ARCHIVE_PATH,
    )


def main() -> int:
    FIXTURE.write_text(render(current_document()), encoding="utf-8")
    print(f"wrote {FIXTURE.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
