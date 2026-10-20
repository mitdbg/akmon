"""The only side-effect surface: read-only git queries on the checkout."""

from __future__ import annotations

import subprocess
from collections.abc import Iterable, Sequence
from pathlib import Path


def _run(root: Path, *args: str) -> str | None:
    try:
        out = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=False)
    except OSError:
        # No git, or it could not be started (a permission error, a missing
        # working directory): every one of these is an unknown answer.
        return None
    if out.returncode != 0:
        return None
    return out.stdout


def head(root: Path) -> str | None:
    out = _run(root, "rev-parse", "HEAD")
    return out.strip() if out else None


def rev_parse(root: Path, ref: str) -> str | None:
    out = _run(root, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    return out.strip() if out else None


def is_tracked(root: Path, relative: str) -> bool:
    out = _run(root, "ls-files", "--error-unmatch", "--", relative)
    return bool(out and out.strip())


def path_exists_at(root: Path, commit: str, relative: str) -> bool | None:
    """True when ``relative`` is a file in ``commit``'s tree, False when it is
    not, and None when the question cannot be answered (no git, unresolvable
    commit). None must fail closed at the call site."""
    if not commit or not relative:
        return None
    if rev_parse(root, commit) is None:
        return None
    out = _run(root, "cat-file", "-t", f"{commit}:{relative}")
    if out is None:
        # Exit status 128: the path is absent from that tree.
        return False
    return out.strip() == "blob"


def tree_of(root: Path, rev: str) -> str | None:
    """The tree object a commit points at, or None.

    Not :func:`rev_parse`, which peels to a COMMIT (``^{commit}``) and so can
    never answer a question about trees.
    """
    out = _run(root, "rev-parse", "--verify", "--quiet", f"{rev}^{{tree}}")
    return out.strip() if out else None


def parents(root: Path, commit: str) -> list[str] | None:
    """The commit's parents, or None when the question cannot be answered.

    An empty list is a root commit. The count matters: a merge commit must
    never act as an approver, because its own diff against either parent is
    other people's work.
    """
    out = _run(root, "rev-list", "--parents", "-n", "1", commit)
    if not out or not out.split():
        return None
    return out.split()[1:]


def file_at(root: Path, commit: str, relative: str) -> str | None:
    """The text of ``relative`` in ``commit``'s tree, or None.

    None is every unknown answer -- no git, an unresolvable commit, the path
    absent from that tree, or bytes that are not UTF-8 -- and the caller must
    fail closed on it.
    """
    if not commit or not relative:
        return None
    if rev_parse(root, commit) is None:
        return None
    return _run(root, "show", f"{commit}:{relative}")


def is_ancestor(root: Path, ancestor: str, descendant: str) -> bool | None:
    try:
        out = subprocess.run(
            ["git", "merge-base", "--is-ancestor", ancestor, descendant],
            cwd=root, capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return None
    if out.returncode == 0:
        return True
    if out.returncode == 1:
        return False
    return None


def merge_base(root: Path, base_ref: str, head_ref: str = "HEAD") -> str | None:
    out = _run(root, "merge-base", base_ref, head_ref)
    return out.strip() if out else None


#: Flags for the shared change-set reader. Renames and copies are detected so
#: a record keeps both of its paths. ``--find-copies-harder`` (copy sources
#: among unmodified files too) is on for commit ranges; see
#: :func:`diff_records` for the one caller that turns it off.
DIFF_RECORD_ARGS: tuple[str, ...] = (
    "diff",
    "--name-status",
    "-z",
    "--find-renames",
    "--find-copies",
)

#: Wall-clock bound on one change-set read. git caps rename and copy
#: detection itself (``diff.renameLimit``): a 916-file, two-day range of this
#: repository reads in under a second with ``--find-copies-harder``, and a
#: 6,630-file month in under 0.1 s because git skips exhaustive detection. Two
#: minutes is two orders of magnitude of headroom for a slow CI disk while
#: still ending a hung git (a lock, a blob fetch from an unreachable remote in
#: a partial clone) before the job's own timeout erases its report. The old
#: CI readers had no bound; scope_drift's had 10 s, too tight for copy
#: detection on a large range.
DIFF_TIMEOUT_SECONDS = 120


class DiffUnavailable(Exception):
    """A change set could not be read. The message carries git's own error.

    Raised, never returned as an empty list: an empty change set reads as
    "nothing changed", which would skip lanes and pass checks.
    """


def _diff_output(root: Path, args: tuple[str, ...]) -> str:
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
            timeout=DIFF_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise DiffUnavailable(
            f"git {' '.join(args)} timed out after {DIFF_TIMEOUT_SECONDS}s"
        ) from exc
    except (OSError, UnicodeError) as exc:
        raise DiffUnavailable(f"git {' '.join(args)} could not run: {exc}") from exc
    if out.returncode != 0:
        raise DiffUnavailable(
            f"git {' '.join(args)} exited {out.returncode}: {out.stderr.strip()}"
        )
    return out.stdout


def diff_records(
    root: Path, *range_args: str, find_copies_harder: bool = True
) -> list[tuple[str, tuple[str, ...]]]:
    """Every ``(status, paths)`` record of one ``git diff``.

    The shared change-set reader. ``scripts/ci`` loads this module by path
    (it runs where the ``pact`` package is not installed); lane_scope,
    lane_catalog, agent_preflight, regression_test_gate, verdict_policy and
    scope_drift build on it. The caller picks the range and passes it
    verbatim:

    * a pull request: ``"<merge-base>..<head>"``. Diffing from the merge base
      of the target branch and the head (not from the target tip) keeps
      commits that landed on the target after the branch point out, and a
      merge-from-target commit on the branch contributes only its conflict
      resolution;
    * a push with no pull request: ``"<before>..<head>"``;
    * one exact commit range, such as an approval commit to the head;
    * ``"--cached"`` for the index, or nothing for the working tree.

    ``find_copies_harder=False`` keeps copy detection to sources modified in
    the same diff. The local preflight uses it for the index and working
    tree, as it did before this reader was shared: there a copy of a file the
    branch already changed would put that file in two records, and the local
    affected-test plan treats a path in two records as a collision and falls
    back to every test.

    A rename (``R``) or copy (``C``) record carries two paths, old then new;
    every other record carries one. A deletion is a ``D`` record naming the
    deleted path. Paths are read with ``-z``, so non-ASCII names and names
    containing newlines come back verbatim, never quoted. Any failure -- no
    git, a bad range, a timeout, output that does not parse -- raises
    :class:`DiffUnavailable` with git's message.
    """
    args = (*DIFF_RECORD_ARGS, *(("--find-copies-harder",) if find_copies_harder else ()))
    out = _diff_output(root, (*args, *range_args))
    tokens = out.split("\0")
    if tokens and tokens[-1] == "":
        tokens.pop()
    records: list[tuple[str, tuple[str, ...]]] = []
    index = 0
    while index < len(tokens):
        status = tokens[index]
        index += 1
        count = 2 if status[:1] in {"R", "C"} else 1
        paths = tuple(tokens[index : index + count])
        index += count
        if not status or len(paths) != count or not all(paths):
            raise DiffUnavailable(
                f"git {' '.join(args)} {' '.join(range_args)}: incomplete "
                f"name-status record {status!r} {paths!r}"
            )
        records.append((status, paths))
    return records


def record_paths(records: Iterable[tuple[str, Sequence[str]]]) -> list[str]:
    """The changed paths of ``records``, sorted and without duplicates.

    Both paths of a rename count (the old one is deleted, the new one
    added), and a deleted path counts. The source of a copy does not: it is
    unchanged, and counting it made CI require ``affected_code`` coverage
    for a file the pull request never touched. On a range where git runs
    exhaustive rename detection, this set equals the paths of
    ``git diff --no-renames`` over the same range, which is what
    :func:`changed_paths` reads for its line counts.
    """
    paths: set[str] = set()
    for status, record in records:
        paths.update(record[1:] if status[:1] == "C" else record)
    return sorted(paths)


def changed_paths(root: Path, base: str, head_ref: str = "HEAD") -> list[tuple[str, int]] | None:
    """(path, added+deleted lines) between base and head_ref, or None.

    The line counts are what this adds over :func:`diff_records`; its paths
    are the :func:`record_paths` set over the same range. ``--no-renames`` is
    deliberate: a rename counts the whole file as deleted and added, so
    moving code cannot shrink the computed tier. ``-z`` keeps non-ASCII and
    newline-bearing names verbatim, as the other readers see them.
    """
    try:
        out = _diff_output(root, ("diff", "--numstat", "-z", "--no-renames", f"{base}..{head_ref}"))
    except DiffUnavailable:
        return None
    rows: list[tuple[str, int]] = []
    for record in out.split("\0"):
        if not record:
            continue
        parts = record.split("\t", 2)
        if len(parts) != 3 or not parts[2]:
            return None
        added, deleted, path = parts
        try:
            n = int(added) + int(deleted)
        except ValueError:
            # Binary rows print "-"; a binary change cannot be read as small.
            n = 10**6
        rows.append((path, n))
    return rows


def index_is_clean(root: Path) -> bool | None:
    """True when nothing is staged, False when something is, None when the
    question cannot be answered (no git). None must fail closed."""
    try:
        out = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=root, capture_output=True, text=True, check=False,
        )
    except FileNotFoundError:
        return None
    if out.returncode == 0:
        return True
    if out.returncode == 1:
        return False
    return None


def trailer_entries(root: Path, key: str, base: str | None, head_ref: str = "HEAD") -> list[tuple[str, str]]:
    """(commit, value) for every ``key:`` trailer in base..head, merges
    excluded.

    The commit matters: an approval trailer binds the tree it sits on, so the
    caller must know which commit carried it (see
    ``approval.code_rejection``). One commit can carry several values; each
    becomes its own row. Commits are newest first, as ``git log`` orders them.
    """
    rng = f"{base}..{head_ref}" if base else head_ref
    # ``--no-merges``: a merge commit's message is written by whoever ran the
    # merge and covers a whole side branch, so a trailer there would approve
    # code nobody read. Approvals come from single-parent commits only.
    out = _run(root, "log", "--no-merges", f"--format=%H%x1f%(trailers:key={key},valueonly,separator=%x1e)%x00", rng)
    if not out:
        return []
    rows: list[tuple[str, str]] = []
    for record in out.split("\x00"):
        record = record.strip("\n")
        if not record or "\x1f" not in record:
            continue
        commit, _, values = record.partition("\x1f")
        for value in values.split("\x1e"):
            value = value.strip()
            if value:
                rows.append((commit.strip(), value))
    return rows


def trailers(root: Path, key: str, base: str | None, head_ref: str = "HEAD") -> list[str]:
    """Values of ``key:`` trailers in commits base..head (or all reachable)."""
    return [value for _, value in trailer_entries(root, key, base, head_ref)]


def origin_url(root: Path) -> str | None:
    out = _run(root, "remote", "get-url", "origin")
    return out.strip() if out else None
