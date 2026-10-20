"""What code is the running worker actually executing?

The runtime worker is a long-lived process. It imports the substrate and
each deployment's source-adapter modules once and holds those module
objects for its whole life. Editing a module in the checkout the worker
runs from does not change what the worker executes, and nothing else in
the system compares the two.

Issue #945: on ``cooper`` a source adapter gained a ``dsn`` parameter 84
minutes after the worker booted. Every subsequent cycle failed with
``TypeError: CooperAliasSource.__init__() got an unexpected keyword
argument 'dsn'`` — 177 times over ~36 hours — while the code on disk and
the source registry agreed with each other the entire time. Restarting
fixed it instantly.

This module records, at worker boot, what the worker loaded:

* :func:`record_worker_code_identity` freezes a snapshot — the checkout's
  git revision **at boot**, the boot timestamp, the pid, the package
  root. It is deliberately captured once. A revision recomputed at report
  time would read the checkout's *current* HEAD and therefore always
  agree with it, reporting a match it never verified.
* It also installs :class:`_ImportTimeRecorder`, a no-op ``sys.meta_path``
  observer that stamps the time each module is first imported. That is
  what makes the incident detectable: the ``cooper`` adapter was imported
  shortly *after* boot and edited later still, so "modified since boot"
  alone would have been both noisy (every lazily-imported module looks
  stale) and wrong.
* :func:`worker_code_identity_report` returns the snapshot plus every
  already-imported module whose file on disk has been modified since that
  module was imported. That set is direct evidence of stale loaded code,
  and unlike a git revision it also catches uncommitted edits.

:func:`evaluate_code_divergence` is the pure comparison used by the
read side. It reports four states, only one of which is a passing check,
and never lets a missing signal read as agreement — see the docstring
there.
"""
from __future__ import annotations

import functools
import importlib.abc
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

log = logging.getLogger(__name__)

__all__ = [
    "InstalledCodeRevision",
    "WorkerCodeIdentity",
    "clear_worker_code_identity",
    "evaluate_code_divergence",
    "forget_module_import_times",
    "get_worker_code_identity",
    "installed_code_revision",
    "record_worker_code_identity",
    "worker_code_identity_report",
]

#: Cap on the module list carried in the report. The count is always
#: exact; only the enumerated sample is bounded, so a worker that has
#: drifted wholesale does not produce a multi-megabyte doctor event.
MAX_REPORTED_MODULES = 25

#: How old the worker's reported identity may be and still describe the
#: process that is running now. The carrier is the scheduled doctor
#: (``*/5 * * * *`` by default), so this allows several missed fires
#: before the observation is declared unusable rather than reassuring.
DEFAULT_MAX_OBSERVATION_AGE_MINUTES = 20.0


@dataclass(frozen=True)
class WorkerCodeIdentity:
    """A frozen record of the code a worker process loaded at boot."""

    deployment: str
    pid: int
    started_at: str
    started_at_epoch: float
    code_revision: str | None
    revision_source: str
    package_root: str
    checkout_root: str | None
    extra_roots: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "pid": self.pid,
            "started_at": self.started_at,
            "started_at_epoch": self.started_at_epoch,
            "code_revision": self.code_revision,
            "revision_source": self.revision_source,
            "package_root": self.package_root,
            "checkout_root": self.checkout_root,
            "extra_roots": list(self.extra_roots),
        }


_LOCK = threading.Lock()
_IDENTITY: WorkerCodeIdentity | None = None
_IMPORT_TIMES: dict[str, float] = {}
_RECORDER: "_ImportTimeRecorder | None" = None


class _ImportTimeRecorder(importlib.abc.MetaPathFinder):
    """Stamp the time each module is first imported.

    Sits at the head of ``sys.meta_path`` and always returns ``None``, so
    it never participates in resolving an import — the real finders run
    exactly as before. ``find_spec`` is consulted only on a module's
    first import (later imports hit the ``sys.modules`` cache), which is
    precisely the timestamp we want.
    """

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: Any = None,
    ) -> None:
        try:
            _IMPORT_TIMES.setdefault(fullname, time.time())
        except Exception:  # noqa: BLE001 - never break an import.
            pass
        return None


def record_worker_code_identity(
    *,
    deployment: str,
    extra_roots: Sequence[Path | str] = (),
) -> WorkerCodeIdentity:
    """Freeze the code identity of this process and start import tracking.

    Call once, early in worker boot. ``extra_roots`` adds directories
    outside the ``okg`` package whose modules should be watched — a
    deployment directory whose source adapters live outside the checkout,
    for instance.
    """
    global _IDENTITY, _RECORDER

    package_root = Path(__file__).resolve().parents[2]
    checkout_root = _repo_root(package_root)
    now = time.time()
    identity = WorkerCodeIdentity(
        deployment=deployment,
        pid=os.getpid(),
        started_at=datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        started_at_epoch=now,
        code_revision=_git_head(checkout_root or package_root),
        revision_source="git" if checkout_root is not None else "unavailable",
        package_root=str(package_root),
        checkout_root=str(checkout_root) if checkout_root else None,
        extra_roots=tuple(
            str(Path(root).resolve()) for root in extra_roots
        ),
    )
    with _LOCK:
        _IDENTITY = identity
        if _RECORDER is None:
            _RECORDER = _ImportTimeRecorder()
            sys.meta_path.insert(0, _RECORDER)
    log.info(
        "worker code identity: deployment=%s revision=%s checkout=%s pid=%d",
        identity.deployment,
        identity.code_revision or "unavailable",
        identity.checkout_root or identity.package_root,
        identity.pid,
    )
    return identity


def get_worker_code_identity() -> WorkerCodeIdentity | None:
    """Return this process's frozen identity, or ``None`` if unrecorded."""
    with _LOCK:
        return _IDENTITY


def clear_worker_code_identity() -> None:
    """Drop the recorded identity and uninstall the import observer.

    Used by tests and by ``DBOS.destroy``-style teardown so a second
    worker boot in the same process re-records rather than inheriting.
    """
    global _IDENTITY, _RECORDER
    with _LOCK:
        _IDENTITY = None
        if _RECORDER is not None:
            try:
                sys.meta_path.remove(_RECORDER)
            except ValueError:
                # Recorder teardown is idempotent; absence is the target state.
                pass
            _RECORDER = None
        _IMPORT_TIMES.clear()


def module_import_time(name: str) -> float | None:
    """Return when ``name`` was first imported, if it was observed."""
    return _IMPORT_TIMES.get(name)


def forget_module_import_times(names: Iterable[str]) -> None:
    """Drop recorded import times for modules about to be re-imported.

    The observer stamps a module's FIRST import and never overwrites, so
    a module that is evicted and re-imported would otherwise keep the
    stamp of the copy that was thrown away — and be reported
    loaded-then-modified forever after being fixed. Callers that evict a
    module must call this so the re-import is stamped afresh.
    """
    for name in names:
        _IMPORT_TIMES.pop(name, None)


def module_is_stale(module_name: str) -> dict[str, Any]:
    """Is the loaded ``module_name`` older than its file on disk?

    Returns a mapping with ``status`` in ``{"stale", "current",
    "unknown"}``. ``unknown`` is a real answer: it means the module's
    file or its import time could not be read, and callers must not
    treat it as either staleness or freshness.
    """
    module = sys.modules.get(module_name)
    if module is None:
        return {"status": "unknown", "reason": "module_not_loaded"}
    path = _module_file(module)
    if path is None:
        return {"status": "unknown", "reason": "module_has_no_file"}
    try:
        modified_at = path.stat().st_mtime
    except OSError:
        return {"status": "unknown", "reason": "module_file_unreadable"}
    reference = _reference_time(module_name)
    if reference is None:
        return {"status": "unknown", "reason": "import_time_unknown"}
    return {
        "status": "stale" if modified_at > reference else "current",
        "module": module_name,
        "path": str(path),
        "modified_at": _iso(modified_at),
        "loaded_at": _iso(reference),
    }


def worker_code_identity_report() -> dict[str, Any]:
    """Build the payload the worker publishes for out-of-process readers.

    ``status`` is ``recorded`` only when an identity was frozen at boot.
    A process that never recorded one reports ``not_recorded`` — callers
    must not read that as "no drift".
    """
    identity = get_worker_code_identity()
    if identity is None:
        return {
            "status": "not_recorded",
            "reason": "record_worker_code_identity_was_not_called",
        }
    modified = _loaded_then_modified(identity)
    return {
        "status": "recorded",
        **identity.to_dict(),
        "modified_module_count": len(modified),
        "modified_modules": modified[:MAX_REPORTED_MODULES],
        "modified_modules_truncated": len(modified) > MAX_REPORTED_MODULES,
    }


def evaluate_code_divergence(
    *,
    observed: Mapping[str, Any] | None,
    checkout_revision: str | None = None,
    observed_age_minutes: float | None = None,
    max_age_minutes: float = DEFAULT_MAX_OBSERVATION_AGE_MINUTES,
    worker_root_count: int | None = None,
) -> dict[str, Any]:
    """Compare the worker's loaded code to the checkout it is serving.

    Four states, and a missing or stale signal is never one of the
    reassuring ones (the "absence dressed as agreement" defect):

    ``diverged``
        Positive evidence of drift: at least one loaded-then-modified
        module, or two known-and-unequal revisions.
    ``matched``
        Every one of these was affirmatively established: both revisions
        known and equal, no loaded module modified since it was
        imported, the observation's age KNOWN and within
        ``max_age_minutes``, and no evidence of more than one worker
        process root. Any of them being merely un-contradicted is not
        enough.
    ``stale_observation``
        An identity was reported, but longer ago than ``max_age_minutes``
        — it may describe a worker that has since died or restarted. The
        last surviving verdict of a dead worker must not read as
        agreement.
    ``not_observed``
        No identity was reported, a revision is unknown, the
        observation's age could not be measured, or more than one worker
        process root is live so "the worker" is not one thing. ``ok`` is
        ``None`` — the check did not pass, it did not run.

    ``checkout_revision`` must be read from the WORKER's own recorded
    root, not from the caller's working tree: multi-worktree operation is
    normal here, and comparing a worker's boot revision to some other
    checkout's HEAD is a permanent false mismatch. Callers get this by
    passing ``observed["checkout_root"]`` to
    :func:`checkout_code_revision`.

    On ``worker_root_count``: a count above one is used, because two
    workers at different revisions make "the worker's revision" a claim
    the data does not support. A count of zero or ``None`` is NOT read as
    "one worker" — but neither is it used to refuse an answer, because
    the caller's process-root count comes from a local ``ps`` scan that
    is structurally blind to a worker in a container or on another host.
    What establishes that a worker was alive is the measured age of the
    identity it published, which is why an unmeasurable age is
    ``not_observed`` rather than a passing check.
    """
    base: dict[str, Any] = {
        "status": "not_observed",
        "ok": None,
        "worker_code_revision": None,
        "checkout_code_revision": checkout_revision,
        "worker_started_at": None,
        "worker_pid": None,
        "compared_root": None,
        "modified_module_count": None,
        "modified_modules": [],
        "observed_age_minutes": observed_age_minutes,
        "reasons": [],
    }

    if worker_root_count is not None and worker_root_count > 1:
        base["reasons"] = ["multiple_worker_process_roots"]
        return base
    if not isinstance(observed, Mapping) or not observed:
        base["reasons"] = ["worker_code_identity_not_reported"]
        return base
    if str(observed.get("status") or "") != "recorded":
        base["reasons"] = [
            str(
                observed.get("reason")
                or "worker_code_identity_not_recorded"
            )
        ]
        return base

    base["compared_root"] = (
        observed.get("checkout_root") or observed.get("package_root")
    )
    # An observation older than the carrier's cadence supports NO
    # conclusion in either direction: it may describe a worker that has
    # since died (so `matched` would be agreement from a process that no
    # longer exists) or one that has since restarted (so `diverged`
    # would be a false blocker). Decide this before reading the verdict.
    if (
        observed_age_minutes is not None
        and observed_age_minutes > max_age_minutes
    ):
        base["status"] = "stale_observation"
        base["worker_code_revision"] = _clean(observed.get("code_revision"))
        base["worker_started_at"] = observed.get("started_at")
        base["worker_pid"] = observed.get("pid")
        base["reasons"] = ["worker_code_identity_observation_stale"]
        return base
    worker_revision = _clean(observed.get("code_revision"))
    modified = [
        row for row in observed.get("modified_modules") or []
        if isinstance(row, Mapping)
    ]
    modified_count = observed.get("modified_module_count")
    modified_count = (
        int(modified_count)
        if isinstance(modified_count, (int, float))
        else len(modified)
    )
    base.update({
        "worker_code_revision": worker_revision,
        "worker_started_at": observed.get("started_at"),
        "worker_pid": observed.get("pid"),
        "modified_module_count": modified_count,
        "modified_modules": [dict(row) for row in modified],
    })

    reasons: list[str] = []
    if modified_count > 0:
        reasons.append("loaded_modules_modified_since_import")
    checkout = _clean(checkout_revision)
    if worker_revision is not None and checkout is not None:
        if worker_revision != checkout:
            reasons.append("code_revision_mismatch")
    if reasons:
        base["status"] = "diverged"
        base["ok"] = False
        base["reasons"] = reasons
        return base

    # From here on nothing CONTRADICTS the worker. That is not the same
    # as having checked it: `matched` requires every input to have been
    # measured, so enumerate what is missing rather than passing by
    # default.
    unmeasured: list[str] = []
    if worker_revision is None:
        unmeasured.append("worker_code_revision_unavailable")
    if checkout is None:
        unmeasured.append("checkout_code_revision_unavailable")
    if observed_age_minutes is None:
        # Without an age we cannot tell a live worker's verdict from the
        # last one a dead worker ever wrote.
        unmeasured.append("observation_age_unknown")
    if unmeasured:
        base["reasons"] = unmeasured
        return base

    base["status"] = "matched"
    base["ok"] = True
    base["reasons"] = []
    return base


def checkout_code_revision(start: Path | str | None = None) -> str | None:
    """Git HEAD of the checkout that provides the ``okg`` package."""
    root = (
        Path(start).resolve()
        if start is not None
        else Path(__file__).resolve().parents[2]
    )
    return _git_head(_repo_root(root) or root)


@dataclass(frozen=True)
class InstalledCodeRevision:
    """The commit the imported ``okg`` package was built from, and how known.

    ``source`` is ``"checkout"`` (Git HEAD of the checkout whose ``src/okg``
    is the imported package), ``"build_info"`` (the ``source_revision`` a
    wheel's build recorded in ``okg/_build_info.json``), or None with
    ``reason`` saying why no revision is known. ``dirty`` is the wheel
    build's recorded uncommitted-change state; a dirty build reports no
    revision, since its code is not the commit's. The checkout path reads
    HEAD only, as ``checkout_code_revision`` always has, so ``dirty`` is None
    there: uncommitted edits in a checkout are not detected.
    """

    revision: str | None
    source: str | None
    dirty: bool | None = None
    reason: str | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "source": self.source,
            "dirty": self.dirty,
            "reason": self.reason,
        }


def installed_code_revision(
    package_root: Path | str | None = None,
    *,
    distribution: Any | None = None,
) -> InstalledCodeRevision:
    if package_root is None and distribution is None:
        # The imported code cannot change within a process, and reading a
        # wheel's file list costs ~45 ms.
        return _installed_code_revision_cached()
    return _installed_code_revision(package_root, distribution=distribution)


@functools.lru_cache(maxsize=1)
def _installed_code_revision_cached() -> InstalledCodeRevision:
    return _installed_code_revision(None, distribution=None)


def _installed_code_revision(
    package_root: Path | str | None,
    *,
    distribution: Any | None,
) -> InstalledCodeRevision:
    """What code this process runs: never the deployment folder's Git state.

    A wheel install has no ``.git``, so a revision read with ``git rev-parse``
    beside the package was always None there, and every check that compares
    revisions read a release user's install as unknown (#3585). The build
    already records the commit; this reads it.

    Order: the enclosing checkout, but only when its ``src/okg`` IS the
    imported package (a wheel in a virtualenv inside some other repository
    must not report that repository's HEAD); else the installed wheel's
    build record, but only when that wheel's file list contains the imported
    package (an editable or source install lists no module files); else None.
    The build record is the builder's claim, never verified Git authority;
    byte-level checks of the installed files belong to
    ``distributions.installed_framework``. ``package_root`` and
    ``distribution`` exist for tests.
    """
    root = (
        Path(package_root).resolve() if package_root is not None
        else Path(__file__).resolve().parents[2]
    )
    checkout = _repo_root(root)
    if checkout is not None and (checkout / "src" / "okg").resolve() == root:
        revision = _git_head(checkout)
        if revision:
            return InstalledCodeRevision(revision=revision, source="checkout")
        return InstalledCodeRevision(
            revision=None, source=None,
            reason=f"`git rev-parse HEAD` failed in the okg checkout {checkout}",
        )
    from okg.substrate.distributions import installed_framework

    try:
        dist = distribution or importlib_metadata.distribution(
            installed_framework.DISTRIBUTION_NAME,
        )
    except importlib_metadata.PackageNotFoundError:
        dist = None
    anchor = root / "substrate" / "runtime" / "code_identity.py"
    listed = False
    if dist is not None:
        for entry in dist.files or ():
            if str(entry).replace("\\", "/") != "okg/substrate/runtime/code_identity.py":
                continue
            try:
                listed = Path(str(dist.locate_file(entry))).resolve() == anchor
            except (OSError, ValueError):
                listed = False
            break
    if not listed:
        return InstalledCodeRevision(
            revision=None, source=None,
            reason=(
                f"the okg package at {root} is neither the src/okg tree of a "
                "Git checkout nor an installed okg wheel, so the commit it "
                "was built from is unknown"
            ),
        )
    info = installed_framework._read_build_info(dist)
    revision = info.get("source_revision")
    if not revision:
        return InstalledCodeRevision(
            revision=None, source=None,
            reason=(
                f"the installed okg wheel at {root} records no build revision "
                "(okg/_build_info.json has no source_revision: built outside "
                "a Git checkout without OKG_BUILD_SOURCE_REVISION, or before "
                "the build recorded it)"
            ),
        )
    dirty = info.get("source_dirty")
    if dirty is True:
        # A dirty build is not the commit it names: reporting the commit
        # would match a clean install of it and read as agreement.
        return InstalledCodeRevision(
            revision=None, source=None, dirty=True,
            reason=(
                f"the installed okg wheel at {root} was built from a work "
                f"tree with uncommitted changes on top of {revision}"
            ),
        )
    return InstalledCodeRevision(
        revision=revision, source="build_info", dirty=dirty,
    )


# ----------------------------------------------------------------- helpers


def _loaded_then_modified(
    identity: WorkerCodeIdentity,
) -> list[dict[str, Any]]:
    roots = [Path(identity.package_root)]
    if identity.checkout_root:
        roots.append(Path(identity.checkout_root))
    roots.extend(Path(root) for root in identity.extra_roots)

    rows: list[dict[str, Any]] = []
    for name, module in list(sys.modules.items()):
        if module is None:
            continue
        path = _module_file(module)
        if path is None or not _under_any(path, roots):
            continue
        try:
            modified_at = path.stat().st_mtime
        except OSError:
            continue
        # A module imported AFTER boot loaded whatever was on disk at
        # that moment, so its own import time — not the boot time — is
        # the correct reference. Modules imported before the observer
        # was installed fall back to the boot time, which is a safe
        # upper bound on when they were loaded.
        reference = _reference_time(name, default=identity.started_at_epoch)
        if reference is None or modified_at <= reference:
            continue
        rows.append({
            "module": name,
            "path": str(path),
            "loaded_at": _iso(reference),
            "modified_at": _iso(modified_at),
        })
    rows.sort(key=lambda row: (row["modified_at"], row["module"]))
    return rows


def _reference_time(
    name: str,
    default: float | None = None,
) -> float | None:
    observed = _IMPORT_TIMES.get(name)
    if observed is not None:
        return observed
    if default is not None:
        return default
    identity = get_worker_code_identity()
    return identity.started_at_epoch if identity else None


def _module_file(module: Any) -> Path | None:
    raw = getattr(module, "__file__", None)
    if not raw or not str(raw).endswith(".py"):
        return None
    try:
        return Path(raw).resolve()
    except OSError:
        return None


def _under_any(path: Path, roots: Sequence[Path]) -> bool:
    for root in roots:
        try:
            path.relative_to(root)
        except ValueError:
            continue
        return True
    return False


def _repo_root(start: Path) -> Path | None:
    for candidate in [start, *start.parents]:
        if (candidate / ".git").exists():
            return candidate
    return None


def _git_head(start: Path) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=start,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except Exception:  # noqa: BLE001 - identity stays inspectable.
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()
