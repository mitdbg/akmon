"""Deployed-code drift probe for a deployment's registered sync root.

WHY THIS EXISTS (2026-07-18). okg-workspace ran six-day-old code without any
signal saying so. A 07-12 cutover incident moved the sync root onto a chain of
hotfix commits (detached HEAD) to get the live deployment working; the fix
branch was never merged to ``dev`` and the sync root was never returned to a
tracked ref. The deployment then sat 313 commits behind while every merge to
``dev`` -- including a storage fix authored specifically for it -- silently
never reached production. Nothing was broken, nothing was logged, and the drift
was only found by accident while debugging why a shipped change had no effect.

WHAT THIS PROBES. The sync root is the checkout a deployment is allowed to
project file-derived truth from (``deployment_authority.check_publish_provenance``);
it is therefore the thing whose revision IS the deployed code. Two conditions
are unambiguous defects and are classified:

* **Not an ancestor of the base ref.** The deployed revision carries commits
  that are not in ``dev``. Those are stranded fixes: real work that production
  depends on and that no reviewed branch is carrying forward. This is the
  condition that would have caught the 07-12 drift on day one.
* **Detached HEAD.** The deployed revision tracks no branch, so there is no ref
  to advance and nothing to notice when it falls behind.

Staleness (``commits_behind``) is REPORTED as a measured fact but is only
CLASSIFIED when the deployment declares a threshold. Being behind between
deploys is normal; inventing a universal "too old" number would be policy this
module has no standing to set. An *absent* threshold is therefore legitimate --
but an *invalid* one is a safety gate the operator tried to arm and this module
silently dropped, so it is a loud misconfiguration (adversarial review F2).

NEVER A SILENT PASS. Every failure mode yields a classified verdict:
unreadable manifest, missing deployment, undeclared/invalid config, missing or
non-git sync root, a sync root that is merely *inside* some other checkout, an
absent base ref, git being unavailable or timing out, and unparsable rev-list
output (which would otherwise leave a declared threshold unevaluated while the
verdict read "ok" -- adversarial review F1).

OFFLINE AND BUDGETED: the probe compares against the LOCAL ``origin/dev`` ref
and never fetches -- a doctor check must not depend on the network, and a stale
base ref still catches the ancestry and detached-HEAD defects. Git calls carry
a short timeout so the whole probe stays well inside the scheduled doctor's 60s
budget even on a contended or network-mounted sync root.

TWO MODES (``code_drift.mode``, issue #2987). The default, ``track_base``, is
everything above: the sync root is a branch checkout that should follow the
base ref. A consumer that embeds okg as a PINNED git submodule has no base ref
to follow -- its submodule sits on a detached commit by design -- so
``track_base`` could never read healthy there. ``mode: pinned`` asks the
questions that matter for a pin instead:

* Is the sync root at exactly the commit the INSTALLED okg package was built
  from? That build revision is the one the worker code-identity check reads
  (``code_identity.checkout_code_revision``: git HEAD of the checkout whose
  ``src/okg`` is the imported package). A package that cannot report one is
  an error, never a pass.
* Is it at the commit the pin RECORDS? When the sync root is a git submodule,
  the pin is the gitlink in the superproject's committed HEAD, and the
  submodule's HEAD must equal it. This is the check that catches an operator
  checking out another okg commit inside the submodule: with an editable
  install the build revision is re-read from that same checkout, so it moves
  with the HEAD and cannot catch the move by itself (lane A, kb76).
* Is the checkout clean?

A sync root that is the installed package's own checkout and NOT a submodule
has no pin to compare against -- its HEAD is the build revision by
definition -- so pinned mode reports that it cannot verify the pin (error)
rather than reading healthy. ``commits_behind`` is not measured in pinned mode
and a detached HEAD is expected.
"""
from __future__ import annotations

import contextvars
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_BASE_REF = "origin/dev"

MODE_TRACK_BASE = "track_base"
MODE_PINNED = "pinned"
_MODES = (MODE_TRACK_BASE, MODE_PINNED)

#: Every key a ``code_drift`` block may carry. Anything else is refused as
#: misconfigured, so a typo such as ``mdoe: pinned`` cannot silently run
#: track_base. ``deployments/keys.py`` closes the schema over this constant.
_CODE_DRIFT_KEYS = frozenset({"mode", "base_ref", "max_commits_behind"})

#: Dirty paths named in a pinned-mode warning; the count is always exact.
MAX_REPORTED_DIRTY_PATHS = 10

#: Per-git-call timeout. track_base makes at most six git calls; pinned mode
#: makes at most eight (three shared, three to read the superproject pin, two
#: working-tree status reads) plus the build-revision read, which has its own
#: 5s timeout in ``code_identity``. ``_PROBE_BUDGET_S`` caps the whole probe
#: so the worst case stays inside the scheduled doctor's 60s budget: 45s of git
#: plus the 5s build-revision read is 50s.
_GIT_TIMEOUT_S = 8

#: Wall-clock budget for all git calls of one probe. Each call's timeout is
#: the smaller of ``_GIT_TIMEOUT_S`` and what remains of this budget; a call
#: with nothing left is refused as ``_GitUnavailable``.
_PROBE_BUDGET_S = 45.0

#: Monotonic deadline of the probe in progress, or None outside a probe.
_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "okg_code_drift_deadline", default=None,
)

#: Global options on every git call. The probe only reads, so it must never
#: run a program the checked-out repository's config names, and never take the
#: index lock an operator's commit may need:
#: * ``core.fsmonitor=false``: ``git status`` and ``ls-files`` otherwise run
#:   the hook ``core.fsmonitor`` names in the sync root's or superproject's
#:   config.
#: * ``--no-optional-locks``: ``git status`` otherwise refreshes and rewrites
#:   the index under ``index.lock``, which can race an operator's commit.
_GIT_READ_ONLY_OPTIONS = ("--no-optional-locks", "-c", "core.fsmonitor=false")


class _GitUnavailable(RuntimeError):
    """git could not be RUN (missing binary, timeout, OS error).

    Distinct from git running and answering non-zero -- which is a legitimate
    answer for e.g. ``merge-base --is-ancestor``.
    """


@dataclass(frozen=True)
class DriftVerdict:
    """Structured result. ``ok`` is False whenever a classified defect fires."""

    deployment: str
    status: str  # ok | warning | misconfigured | error
    #: The ref compared against; None in pinned mode, which compares against
    #: the installed build revision instead.
    base_ref: str | None
    sync_root: str | None = None
    #: Where the sync root value came from: manifest_literal | process_env |
    #: local_profile (issue #1014). A local_profile value is untracked, so a
    #: reader must be able to tell it from a reviewed manifest literal.
    sync_root_source: str | None = None
    head: str | None = None
    branch: str | None = None
    detached: bool | None = None
    is_ancestor_of_base: bool | None = None
    commits_ahead: int | None = None
    commits_behind: int | None = None
    behind_threshold: int | None = None
    problems: list[str] = field(default_factory=list)
    mode: str = MODE_TRACK_BASE
    #: Pinned mode only: the commit the installed okg package was built from.
    installed_revision: str | None = None
    dirty_path_count: int | None = None
    dirty_paths: list[str] = field(default_factory=list)
    #: Pinned mode only: what the pin was read from -- superproject_gitlink
    #: (the sync root is a submodule) or none (nothing independent records it).
    pin_source: str | None = None
    #: The commit the superproject's committed HEAD records for the submodule.
    pinned_revision: str | None = None
    #: The gitlink staged in the superproject's index, when it differs.
    staged_revision: str | None = None
    superproject: str | None = None
    submodule_path: str | None = None
    #: The checkout the running package is imported from, when it is not the
    #: sync root; its uncommitted changes run too, so they are reported.
    package_checkout: str | None = None
    package_dirty_path_count: int | None = None
    package_dirty_paths: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == "ok"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": self.ok,
            "status": self.status,
            "deployment": self.deployment,
            "base_ref": self.base_ref,
            "sync_root": self.sync_root,
            "sync_root_source": self.sync_root_source,
            "head": self.head,
            "branch": self.branch,
            "detached": self.detached,
            "is_ancestor_of_base": self.is_ancestor_of_base,
            "commits_ahead": self.commits_ahead,
            "commits_behind": self.commits_behind,
            "behind_threshold": self.behind_threshold,
            "problems": list(self.problems),
        }
        if self.mode != MODE_TRACK_BASE:
            # Pinned-only keys. A track_base verdict serializes exactly as it
            # did before modes existed, so no existing reader changes.
            payload.update({
                "mode": self.mode,
                "installed_revision": self.installed_revision,
                "dirty_path_count": self.dirty_path_count,
                "dirty_paths": list(self.dirty_paths),
                "pin_source": self.pin_source,
                "pinned_revision": self.pinned_revision,
                "staged_revision": self.staged_revision,
                "superproject": self.superproject,
                "submodule_path": self.submodule_path,
                "package_checkout": self.package_checkout,
                "package_dirty_path_count": self.package_dirty_path_count,
                "package_dirty_paths": list(self.package_dirty_paths),
            })
        return payload


def _git_run(root: Path, *args: str) -> tuple[int, str, str]:
    """Run a read-only git command in ``root``: (rc, stdout, stderr), stripped.

    Raises ``_GitUnavailable`` when git could not be run at all, including
    when the probe's overall budget is spent. A non-zero return code is passed
    through -- callers decide whether it is an answer or a failure.
    """
    timeout: float = _GIT_TIMEOUT_S
    deadline = _DEADLINE.get()
    if deadline is not None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _GitUnavailable(
                f"code-drift probe spent its {_PROBE_BUDGET_S:g}s budget before "
                f"git {' '.join(args)} in {root}"
            )
        timeout = min(timeout, remaining)
    try:
        proc = subprocess.run(
            ["git", *_GIT_READ_ONLY_OPTIONS, "-C", str(root), *args],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise _GitUnavailable(
            f"git {' '.join(args)} timed out after {timeout:g}s in {root}"
        ) from exc
    except (OSError, ValueError) as exc:  # missing binary, bad path, ...
        raise _GitUnavailable(f"git {' '.join(args)} failed to run: {exc}") from exc
    return (
        proc.returncode,
        (proc.stdout or "").strip(),
        (proc.stderr or "").strip(),
    )


def _git(root: Path, *args: str) -> tuple[int, str]:
    """``_git_run`` without stderr: (returncode, stripped stdout)."""
    rc, out, _ = _git_run(root, *args)
    return rc, out


def _manifest(deployment: str) -> tuple[dict | None, bool, Path | None]:
    from okg.substrate.deployment_authority import _read_manifest_with_dir

    raw, readable, deployment_dir = _read_manifest_with_dir(deployment)
    return (
        (raw if isinstance(raw, dict) else None),
        readable,
        deployment_dir,
    )


def _declared_behind_threshold(raw: dict | None) -> tuple[int | None, str | None]:
    """(threshold, invalid_reason) from ``code_drift.max_commits_behind``.

    Absent -> (None, None): legitimate, staleness simply stays unclassified.
    Present but unusable -> (None, reason): the operator tried to arm a gate and
    we must NOT silently drop it. ``bool`` is rejected explicitly because it is
    an ``int`` subclass, so ``true`` would otherwise silently mean ``1``.
    """
    if raw is None:
        return None, None
    block = raw.get("code_drift")
    if block is None:
        return None, None
    if not isinstance(block, dict):
        return None, f"code_drift must be a mapping, got {type(block).__name__}"
    if "max_commits_behind" not in block:
        return None, None
    value = block.get("max_commits_behind")
    if isinstance(value, bool):
        return None, (
            "code_drift.max_commits_behind must be a non-negative integer; got "
            f"the boolean {value!r} (a bool would silently mean 0/1)"
        )
    if not isinstance(value, int):
        return None, (
            "code_drift.max_commits_behind must be a non-negative integer; got "
            f"{value!r} ({type(value).__name__}) -- quote-wrapped YAML numbers "
            "are strings"
        )
    if value < 0:
        return None, (
            f"code_drift.max_commits_behind must be >= 0; got {value}"
        )
    return value, None


def _declared_mode(raw: dict | None) -> tuple[str, str | None, str | None]:
    """(mode, declared_base_ref, refusal) from the ``code_drift`` block.

    Absent ``mode`` is ``track_base``, today's behaviour. A non-mapping block
    is left to ``_declared_behind_threshold``, which already names it. An
    unknown mode, an unusable ``base_ref``, or ``pinned`` combined with a
    track_base-only key is a refusal: those are settings the operator wrote
    and this probe would otherwise ignore.
    """
    if raw is None:
        return MODE_TRACK_BASE, None, None
    block = raw.get("code_drift")
    if not isinstance(block, dict):
        return MODE_TRACK_BASE, None, None
    unknown = sorted(
        (key for key in block if key not in _CODE_DRIFT_KEYS), key=repr,
    )
    if unknown:
        return MODE_TRACK_BASE, None, (
            "code_drift has unknown key(s): "
            + ", ".join(repr(key) for key in unknown)
            + f"; known keys are {', '.join(sorted(_CODE_DRIFT_KEYS))}. A "
            "misspelled key would otherwise be ignored and the check would "
            "silently run in track_base mode."
        )
    mode = block.get("mode", MODE_TRACK_BASE)
    if not isinstance(mode, str) or mode not in _MODES:
        return MODE_TRACK_BASE, None, (
            f"code_drift.mode must be one of {', '.join(_MODES)}; got {mode!r}"
        )
    if mode == MODE_PINNED:
        conflicting = [
            key for key in ("base_ref", "max_commits_behind") if key in block
        ]
        if conflicting:
            return MODE_PINNED, None, (
                "code_drift.mode: pinned compares the sync_root with the "
                "installed okg build, not with a base ref, so "
                + " and ".join(f"code_drift.{key}" for key in conflicting)
                + " cannot apply; remove "
                + ("them" if len(conflicting) > 1 else "it")
            )
        return MODE_PINNED, None, None
    if "base_ref" not in block:
        return MODE_TRACK_BASE, None, None
    declared = block.get("base_ref")
    if (
        not isinstance(declared, str)
        or not declared.strip()
        or declared != declared.strip()
        or any(ch.isspace() for ch in declared)
    ):
        return MODE_TRACK_BASE, None, (
            "code_drift.base_ref must be a non-empty git ref name without "
            f"whitespace; got {declared!r}"
        )
    return MODE_TRACK_BASE, declared, None


def _installed_package_root() -> Path:
    """The imported ``okg`` package directory -- the one code identity reads."""
    from okg.substrate.runtime import code_identity

    return Path(code_identity.__file__).resolve().parents[2]


def _installed_build_revision() -> tuple[str | None, str | None, str | None]:
    """(revision, checkout_root, unavailable_reason) for the installed okg.

    The revision is ``code_identity.checkout_code_revision`` -- the same build
    revision the worker code-identity check records -- read for the imported
    package. One guard is added: the enclosing checkout must be the package's
    own source tree (``<checkout>/src/okg``). Walking up from a wheel installed
    in a virtualenv inside some OTHER repository would otherwise find that
    repository and report its HEAD as okg's build revision.
    """
    from okg.substrate.runtime import code_identity

    package_root = _installed_package_root().resolve()
    checkout = code_identity._repo_root(package_root)
    if checkout is None:
        return None, None, (
            f"the installed okg package at {package_root} is not inside a git "
            "checkout, so it cannot report the commit it was built from (for "
            "example a wheel or git-archive install). Pinned mode cannot "
            "verify the pin without it; install okg from the pinned checkout."
        )
    if (checkout / "src" / "okg").resolve() != package_root:
        return None, str(checkout), (
            f"the installed okg package at {package_root} is not the src/okg "
            f"tree of the enclosing checkout {checkout}, so that checkout's "
            "HEAD is not the package's build revision. Pinned mode cannot "
            "verify the pin; install okg from the pinned checkout."
        )
    revision = code_identity.checkout_code_revision(package_root)
    if revision is None:
        return None, str(checkout), (
            f"git could not read HEAD of the okg checkout at {checkout}, so "
            "the installed package's build revision is unknown"
        )
    return revision, str(checkout), None


def _dirty_paths(root: Path) -> list[str] | None:
    """Uncommitted paths in ``root`` (tracked changes and untracked files).

    Porcelain v2 so no line starts with a space: ``_git`` strips its output,
    which would eat the leading status column of a v1 ``" M path"`` line.
    Returns None when git answers non-zero.
    """
    rc, out = _git(root, "status", "--porcelain=v2", "--untracked-files=normal")
    if rc != 0:
        return None
    paths: list[str] = []
    for line in out.splitlines():
        if line.startswith("1 "):
            parts = line.split(" ", 8)
        elif line.startswith("2 "):
            parts = line.split(" ", 9)
        elif line.startswith("u "):
            parts = line.split(" ", 10)
        elif line.startswith("? "):
            paths.append(line[2:])
            continue
        else:
            continue
        paths.append(parts[-1].split("\t", 1)[0])
    return paths


@dataclass(frozen=True)
class _SuperprojectPin:
    """The gitlink a superproject records for the sync root submodule."""

    superproject: str
    path: str
    #: Gitlink in the superproject's committed HEAD; None when HEAD has none.
    committed: str | None
    #: Gitlink in the superproject's index; None when the index has none.
    staged: str | None


class _PinUnreadable(RuntimeError):
    """git answered, but with an error, while reading the superproject pin.

    Distinct from "not a submodule" and from "no gitlink recorded": both of
    those are answers, this is a failure to get one, and it must never let
    pinned mode skip the gitlink check (review LOW-4).
    """


def _gitlink(line: str, *, sha_field: int) -> str | None:
    """The commit of a mode-160000 (gitlink) ls-tree / ls-files line."""
    head, _, _ = line.partition("\t")
    fields = head.split()
    if len(fields) <= sha_field or fields[0] != "160000":
        return None
    return fields[sha_field]


def _superproject_pin(root: Path) -> _SuperprojectPin | None:
    """Read the pin when ``root`` is a git submodule; None when it is not.

    The superproject's COMMITTED HEAD is the pin. Its index is read only to
    explain a mismatch: a gitlink bump that is staged but not committed is
    not yet the recorded pin (see ``_pinned_verdict``). Raises
    ``_PinUnreadable`` with git's own message when git fails to answer.
    """
    rc, superproject, err = _git_run(
        root, "rev-parse", "--show-superproject-working-tree",
    )
    if rc != 0:
        raise _PinUnreadable(
            "could not tell whether sync_root is a git submodule: git "
            f"rev-parse --show-superproject-working-tree exited {rc} in "
            f"{root}: {err or '(no message)'}"
        )
    if not superproject:
        return None
    super_root = Path(superproject)
    # relpath, not relative_to: it never raises, and an odd layout then shows
    # up as "no gitlink at <path>" instead of escaping as an exception.
    rel = Path(
        os.path.relpath(Path(root).resolve(), super_root.resolve())
    ).as_posix()
    rc_tree, tree, err_tree = _git_run(super_root, "ls-tree", "HEAD", "--", rel)
    if rc_tree != 0:
        raise _PinUnreadable(
            f"could not read the committed pin for {rel} from the superproject "
            f"{super_root}: git ls-tree HEAD exited {rc_tree}: "
            f"{err_tree or '(no message)'}"
        )
    committed = _gitlink(tree.splitlines()[0], sha_field=2) if tree else None
    rc_index, index, err_index = _git_run(
        super_root, "ls-files", "--stage", "--", rel,
    )
    if rc_index != 0:
        raise _PinUnreadable(
            f"could not read the staged pin for {rel} from the superproject "
            f"{super_root}: git ls-files --stage exited {rc_index}: "
            f"{err_index or '(no message)'}"
        )
    staged = _gitlink(index.splitlines()[0], sha_field=1) if index else None
    return _SuperprojectPin(
        superproject=str(super_root), path=rel,
        committed=committed, staged=staged,
    )


def _dirty_problem(
    what: str, paths: list[str], revision: str | None,
) -> str:
    shown = paths[:MAX_REPORTED_DIRTY_PATHS]
    more = len(paths) - len(shown)
    return (
        f"{what} has {len(paths)} uncommitted change(s), so the code there is "
        f"not exactly commit {revision}: "
        + ", ".join(shown)
        + (f" (and {more} more)" if more else "")
    )


def _pinned_verdict(
    *,
    deployment: str,
    root: Path,
    source: str | None,
    head: str,
    branch: str | None,
    detached: bool,
) -> DriftVerdict:
    """Classify a pinned sync root against its pin and the installed build."""
    installed, checkout, unavailable = _installed_build_revision()
    pin_error: str | None = None
    try:
        pin = _superproject_pin(root)
    except _PinUnreadable as exc:
        pin, pin_error = None, str(exc)
    dirty = _dirty_paths(root)
    # The checkout the running package is imported from, when it is not the
    # sync root itself: its edits run too, so it must be clean as well
    # (review MED-1: a separate checkout with src/okg edited read ok).
    separate_checkout = (
        Path(checkout)
        if unavailable is None and checkout is not None
        and Path(checkout).resolve() != Path(root).resolve()
        else None
    )
    package_dirty = (
        _dirty_paths(separate_checkout) if separate_checkout is not None
        else []
    )

    problems: list[str] = []
    status = "ok"

    def _raise_to(level: str) -> None:
        nonlocal status
        order = ("ok", "warning", "error")
        if order.index(level) > order.index(status):
            status = level

    if unavailable is not None:
        problems.append(unavailable)
        _raise_to("error")
    elif installed != head:
        problems.append(
            f"sync_root HEAD {head} is not the commit the installed okg "
            f"package was built from ({installed}, read from {checkout}); one "
            "was upgraded without the other. Check out the installed commit "
            "in the sync_root, or reinstall okg from the pinned checkout."
        )
        _raise_to("error")

    if pin_error is not None:
        pin_source = "unreadable"
        problems.append(pin_error)
        _raise_to("error")
    elif pin is not None:
        pin_source = "superproject_gitlink"
        where = f"{pin.superproject} at {pin.path}"
        if pin.committed is None:
            problems.append(
                f"sync_root is a submodule of {pin.superproject}, but the "
                f"superproject's committed HEAD records no gitlink at "
                f"{pin.path}, so there is no committed pin to verify. Commit "
                "the submodule in the superproject."
            )
            _raise_to("error")
        elif head != pin.committed:
            staged_note = (
                f" The superproject's index stages {head} there, but a staged "
                "gitlink is not the pin until it is committed."
                if pin.staged == head else ""
            )
            problems.append(
                f"submodule HEAD {head} is not the commit the superproject "
                f"{where} pins ({pin.committed}); the okg checkout was moved "
                "without updating the pin." + staged_note + " Check out the "
                "pinned commit (git submodule update), or commit the new pin "
                "in the superproject."
            )
            _raise_to("error")
        elif pin.staged is not None and pin.staged != pin.committed:
            # The deployed code IS the committed pin; the superproject has a
            # pending, uncommitted bump to some other commit.
            problems.append(
                f"the superproject {where} stages a pin change to "
                f"{pin.staged} that is not committed; the committed pin "
                f"{pin.committed} is what runs. Commit or unstage it."
            )
            _raise_to("warning")
    elif unavailable is not None or checkout is None:
        # No build revision: already an error above, and no pin to read.
        pin_source = "none"
    elif separate_checkout is not None:
        # A separate checkout provides the package: its HEAD and the sync
        # root's HEAD move independently, so the comparison above is real.
        pin_source = "installed_package_checkout"
    else:
        pin_source = "none"
        # An editable install from the sync root itself: the build revision is
        # re-read from this checkout's HEAD, so it moves with any checkout and
        # the comparison above can never fail. Nothing independent records
        # the pin, so a healthy verdict would verify nothing.
        problems.append(
            f"pinned mode cannot verify the pin: sync_root {root} is the "
            "installed okg package's own checkout and not a git submodule, so "
            "its HEAD is the build revision by definition and any checkout "
            "would read healthy. Point sync_root at a submodule whose "
            "superproject commits the pin, install okg from a separate "
            "checkout, or use code_drift.mode: track_base."
        )
        _raise_to("error")

    if dirty is None:
        problems.append(
            f"could not read the working-tree status of sync_root {root}"
        )
        _raise_to("error")
    elif dirty:
        problems.append(_dirty_problem("sync_root", dirty, head))
        _raise_to("warning")

    if package_dirty is None:
        problems.append(
            "could not read the working-tree status of the running okg "
            f"checkout {separate_checkout}"
        )
        _raise_to("error")
    elif package_dirty:
        problems.append(_dirty_problem(
            f"the running okg checkout {separate_checkout}",
            package_dirty, installed,
        ))
        _raise_to("warning")

    return DriftVerdict(
        deployment=deployment,
        status=status,
        base_ref=None,
        sync_root=str(root),
        sync_root_source=source,
        head=head,
        branch=branch,
        detached=detached,
        problems=problems,
        mode=MODE_PINNED,
        installed_revision=installed,
        dirty_path_count=None if dirty is None else len(dirty),
        dirty_paths=[] if dirty is None else dirty[:MAX_REPORTED_DIRTY_PATHS],
        pin_source=pin_source,
        pinned_revision=None if pin is None else pin.committed,
        staged_revision=(
            pin.staged
            if pin is not None and pin.staged != pin.committed else None
        ),
        superproject=None if pin is None else pin.superproject,
        submodule_path=None if pin is None else pin.path,
        package_checkout=(
            None if separate_checkout is None else str(separate_checkout)
        ),
        package_dirty_path_count=(
            None if separate_checkout is None or package_dirty is None
            else len(package_dirty)
        ),
        package_dirty_paths=(
            [] if package_dirty is None
            else package_dirty[:MAX_REPORTED_DIRTY_PATHS]
        ),
    )


def check_deployment_code_drift(
    *,
    deployment: str,
    base_ref: str | None = None,
) -> DriftVerdict:
    """Probe the deployment's sync root for deployed-code drift.

    ``base_ref`` (track_base mode only) overrides ``code_drift.base_ref``,
    which defaults to ``origin/dev``.

    Never raises for an expected condition -- every failure yields a LOUD
    classified verdict rather than an exception or a silent pass. All git
    calls share one ``_PROBE_BUDGET_S`` wall-clock budget.
    """
    token = _DEADLINE.set(time.monotonic() + _PROBE_BUDGET_S)
    try:
        return _check_deployment_code_drift(
            deployment=deployment, base_ref=base_ref,
        )
    finally:
        _DEADLINE.reset(token)


def _check_deployment_code_drift(
    *,
    deployment: str,
    base_ref: str | None,
) -> DriftVerdict:
    from okg.substrate.deployments.sync_root import (
        SyncRootResolverError,
        resolve_declared_sync_root,
    )

    raw, manifest_readable, deployment_dir = _manifest(deployment)
    threshold, threshold_invalid = _declared_behind_threshold(raw)
    mode, declared_base_ref, mode_refusal = _declared_mode(raw)
    # The ref a track_base probe compares against: the caller's, else the
    # manifest's, else origin/dev. A pinned verdict reports no base ref.
    track_ref: str = base_ref or declared_base_ref or DEFAULT_BASE_REF
    effective_base_ref = None if mode == MODE_PINNED else track_ref

    # Set once the sync root resolves; every verdict below reports it so a
    # reader can tell a reviewed manifest literal from an untracked local
    # profile value (issue #1014).
    resolved_source: list[str | None] = [None]

    def _bail(status: str, problem: str, **kw: Any) -> DriftVerdict:
        problems = [problem]
        if threshold_invalid:
            problems.append(threshold_invalid)
        kw.setdefault("sync_root_source", resolved_source[0])
        return DriftVerdict(
            deployment=deployment, status=status, base_ref=effective_base_ref,
            behind_threshold=threshold, problems=problems, mode=mode, **kw,
        )

    if not manifest_readable:
        return _bail(
            "error",
            f"deployment manifest for {deployment!r} is unreadable; cannot "
            "determine the deployed revision",
        )

    if raw is None:
        # _read_manifest reports (None, True) both for "no such deployment" and
        # for "parsed but empty" -- distinguish so a typo'd --deployment does
        # not tell the operator to edit a file that does not exist (review F7).
        return _bail(
            "error",
            f"no readable deployment manifest for {deployment!r} -- check the "
            "deployment name and that its deployment.yaml exists",
        )

    if mode_refusal is not None:
        return _bail("misconfigured", mode_refusal)

    try:
        resolution = resolve_declared_sync_root(
            raw, deployment_dir=deployment_dir,
        )
    except SyncRootResolverError as exc:
        # This probe promises never to raise for an expected condition: a
        # --json caller must get a classified envelope, not a traceback.
        return _bail("error", f"cannot resolve the declared sync_root: {exc}")
    if resolution.is_unresolved:
        # The manifest DOES name an authoritative checkout; this machine just
        # cannot say which one. Reporting that as "declares no sync_root" would
        # send the operator to add a key that is already there (issue #1014).
        return _bail(
            "misconfigured",
            f"deployment {deployment!r} declares a sync_root that cannot be "
            f"resolved here, so the deployed revision cannot be measured. "
            f"{resolution.detail}",
        )
    if resolution.is_undeclared:
        return _bail(
            "misconfigured",
            f"deployment {deployment!r} declares no sync_root; there is no "
            "registered checkout whose revision is the deployed code. Declare "
            "sync_root in deployment.yaml. When the path cannot be tracked -- "
            "a manifest shared by several developers, or one mirrored into a "
            "shipped package -- declare it per machine as "
            "sync_root: ${OKG_<NAME>_SYNC_ROOT} and set that variable in the "
            "environment or in the deployment checkout's .okg/local.env, "
            "which admits OKG_-prefixed keys.",
        )

    root = resolution.path
    if root is None:  # unreachable: a resolved declaration carries a path
        return _bail(
            "error",
            f"deployment {deployment!r} resolved a sync_root with no path; "
            "this is a resolver defect, not a deployment misconfiguration",
        )
    resolved_source[0] = resolution.source
    if not root.is_dir():
        return _bail(
            "error", f"declared sync_root does not exist: {root}",
            sync_root=str(root),
        )

    try:
        rc, toplevel = _git(root, "rev-parse", "--show-toplevel")
        if rc != 0 or not toplevel:
            return _bail(
                "error", f"declared sync_root is not a git checkout: {root}",
                sync_root=str(root),
            )
        # --show-toplevel walks UP, so a stale/typo'd sync_root pointing at a
        # subdirectory of some other checkout would otherwise read healthy
        # (review F6).
        if Path(toplevel).resolve() != root.resolve():
            return _bail(
                "error",
                f"declared sync_root {root} is not a checkout root; it lies "
                f"inside the checkout at {toplevel}. The deployed revision "
                "would be read from the wrong tree.",
                sync_root=str(root),
            )

        rc, head = _git(root, "rev-parse", "HEAD")
        if rc != 0 or not head:
            return _bail(
                "error", f"could not resolve HEAD in sync_root: {root}",
                sync_root=str(root),
            )

        rc_branch, branch = _git(root, "symbolic-ref", "--quiet", "--short", "HEAD")
        detached = rc_branch != 0 or not branch
        branch_name = None if detached else branch

        if mode == MODE_PINNED:
            return _pinned_verdict(
                deployment=deployment, root=root, source=resolution.source,
                head=head, branch=branch_name, detached=detached,
            )

        base_ref = track_ref
        rc_base, _ = _git(
            root, "rev-parse", "--verify", "--quiet", f"{base_ref}^{{commit}}"
        )
        if rc_base != 0:
            return _bail(
                "error",
                f"base ref {base_ref!r} is not present in the sync_root's "
                "object store; cannot compare the deployed revision",
                sync_root=str(root), head=head, branch=branch_name,
                detached=detached,
            )

        rc_anc, _ = _git(root, "merge-base", "--is-ancestor", head, base_ref)
        is_ancestor = rc_anc == 0

        rc_cnt, counts = _git(
            root, "rev-list", "--left-right", "--count", f"{base_ref}...HEAD"
        )
    except _GitUnavailable as exc:
        # git itself could not run; a --json caller must still get an envelope
        # rather than a traceback (review F4).
        return _bail("error", str(exc), sync_root=str(root))

    ahead = behind = None
    parts = counts.split() if rc_cnt == 0 else []
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        behind, ahead = int(parts[0]), int(parts[1])

    problems: list[str] = []
    if threshold_invalid:
        problems.append(threshold_invalid)
    if not is_ancestor:
        problems.append(
            f"deployed revision {head[:9]} is NOT an ancestor of {base_ref}"
            + (f" ({ahead} commit(s) ahead)" if ahead else "")
            + " — the running deployment carries commits that are not merged; "
            "those fixes are stranded outside review and will be lost on the "
            "next fast-forward. Merge them to the base branch."
        )
    if detached:
        problems.append(
            f"sync_root HEAD is DETACHED at {head[:9]} — the deployed revision "
            "tracks no branch, so nothing advances it and nothing notices it "
            "falling behind. Return it to a tracked ref."
        )
    if behind is None:
        # Never let an unparsable count leave a DECLARED threshold silently
        # unevaluated, and never report a "fact" we failed to measure (F1).
        problems.append(
            f"could not measure ahead/behind against {base_ref} "
            f"(git rev-list rc={rc_cnt}, output {counts!r})"
            + (
                f"; the declared code_drift.max_commits_behind={threshold} "
                "could NOT be evaluated"
                if threshold is not None else ""
            )
        )
    elif threshold is not None and behind > threshold:
        problems.append(
            f"deployed revision is {behind} commit(s) behind {base_ref}, over "
            f"the declared code_drift.max_commits_behind={threshold}"
        )

    status = "ok"
    if threshold_invalid:
        status = "misconfigured"
    elif behind is None:
        status = "error"
    elif problems:
        status = "warning"

    return DriftVerdict(
        deployment=deployment,
        status=status,
        base_ref=base_ref,
        sync_root=str(root),
        sync_root_source=resolution.source,
        head=head,
        branch=branch_name,
        detached=detached,
        is_ancestor_of_base=is_ancestor,
        commits_ahead=ahead,
        commits_behind=behind,
        behind_threshold=threshold,
        problems=problems,
    )
