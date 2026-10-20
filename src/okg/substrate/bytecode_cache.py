"""Keep Python's bytecode cache out of deployment and code repositories.

okg imports Python that lives in a deployment folder (doctor checks, sources,
enrichers, workflows): ``inject_deployment_path`` puts the folder on
``sys.path``. By default Python then writes ``__pycache__/*.pyc`` next to
each imported file, inside the deployment's Git repository. That dirties the
repository, and the publish's provenance check
(``publisher/repository_provenance.py``) refuses the untracked files:
"repository provenance inputs are not committed".

This is an ease-of-use fix, not a provenance one. With ``sys.pycache_prefix``
set, CPython reads and writes bytecode only under that directory, so okg no
longer writes into, or dirties, the deployment repository. Python still
trusts a cached ``.pyc`` after checking only its source's size and
modification time; that trust now sits in the per-user okg cache, the same
trust Python places in the bytecode of okg's own installed modules. The cache
is hardened: an absolute path, created owner-only (0700), refused unless it
is a real directory owned by this user with no group or other permission
bits, and a clear error when there is no home directory to put it in.

:func:`enter_user_bytecode_cache` is called by the process entry points only
(the ``okg`` console script and ``python -m okg.substrate.cli.main``, the
runtime worker, ``python -m okg.substrate.mcp.server``, a deployment's
``server.py`` through ``run_for_deployment``, and ``python -m okg.viz``),
never on ``import okg``, so a program that uses okg as a library keeps its
own interpreter settings. When bytecode writing is off (``-B`` or
``PYTHONDONTWRITEBYTECODE``), Python writes nothing anywhere, so the hook
leaves everything as it is and needs no cache directory (read-only homes,
CI pods).

POSIX only, like okg (macOS and Linux): the checks use ``O_NOFOLLOW``,
``geteuid`` and Unix permission bits.
"""
from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

PYCACHE_PREFIX_ENV = "PYTHONPYCACHEPREFIX"


class BytecodeCacheError(RuntimeError):
    """The per-user bytecode cache directory cannot be used safely."""


def user_bytecode_cache_dir() -> Path:
    """The per-user bytecode cache: ``<okg cache dir>/pycache``, absolute.

    The okg cache dir is ``${OKG_CACHE_HOME:-${XDG_CACHE_HOME:-~/.cache}/okg}``
    (``deployments._cache_dir``); everything in it can be regenerated. Only
    the okg cache dir is resolved; the ``pycache`` leaf is not, so a symlink
    there is refused rather than followed.
    """
    from okg.substrate.deployments import _cache_dir

    try:
        return _cache_dir().expanduser().resolve() / "pycache"
    except (RuntimeError, KeyError, OSError) as exc:
        # Path.home() raises when there is no HOME and no passwd entry.
        raise BytecodeCacheError(
            "cannot locate the per-user okg cache for Python bytecode "
            f"({exc}); set OKG_CACHE_HOME or XDG_CACHE_HOME to a private "
            "directory, or PYTHONPYCACHEPREFIX to choose the bytecode cache"
        ) from exc


def _private_directory(path: Path) -> None:
    """Create ``path`` owner-only, or refuse it unless it is owner-only.

    The same checks ``runtime/release_launcher._private_pycache_prefix``
    makes on its cache root: a real directory (not a symlink), owned by this
    user, with no group or other permission bits. Another user able to write
    here could plant bytecode okg would run. The parent must be owned by
    this user or root and must not be writable by others unless it has the
    sticky bit, or another user could replace the directory. A group-writable
    parent is accepted: a umask of 002 (user private groups, Ubuntu's
    default) creates ``~/.cache/okg`` as 0775.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        parent = os.stat(path.parent)
    except OSError as exc:
        raise BytecodeCacheError(
            f"the okg cache directory {path.parent} cannot be created ({exc}); "
            "set OKG_CACHE_HOME or PYTHONPYCACHEPREFIX elsewhere"
        ) from exc
    parent_mode = stat.S_IMODE(parent.st_mode)
    if parent.st_uid not in (os.geteuid(), 0):
        raise BytecodeCacheError(
            f"the okg cache directory {path.parent} is owned by uid "
            f"{parent.st_uid}, not by this user or root, so another user could "
            f"replace the bytecode cache; set OKG_CACHE_HOME or "
            f"PYTHONPYCACHEPREFIX to a directory you own"
        )
    if parent_mode & 0o002 and not parent_mode & stat.S_ISVTX:
        raise BytecodeCacheError(
            f"the okg cache directory {path.parent} is writable by others "
            f"(mode {parent_mode:04o}) without the sticky bit, so another user "
            f"could replace the bytecode cache; fix it with chmod o-w "
            f"{path.parent}, or set OKG_CACHE_HOME or PYTHONPYCACHEPREFIX"
        )
    try:
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError as exc:
        raise BytecodeCacheError(
            f"the okg bytecode cache {path} must be a real directory okg can "
            f"create ({exc}); set OKG_CACHE_HOME or PYTHONPYCACHEPREFIX elsewhere"
        ) from exc
    try:
        metadata = os.fstat(fd)
    finally:
        os.close(fd)
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o077:
        raise BytecodeCacheError(
            f"the okg bytecode cache {path} must be owned by this user with "
            f"mode 0700 (found uid {metadata.st_uid}, mode "
            f"{stat.S_IMODE(metadata.st_mode):04o}); fix it with "
            f"chmod 700 {path}, or set OKG_CACHE_HOME or PYTHONPYCACHEPREFIX"
        )


def use_user_bytecode_cache() -> str | None:
    """Point this process and its Python children at the per-user cache.

    An explicit prefix wins: when the interpreter already has one (``-X
    pycache_prefix=...`` or ``PYTHONPYCACHEPREFIX``, as the sealed-release
    launcher sets), it is left as it is. With bytecode writing off, nothing
    is changed and ``None`` is returned. Otherwise returns the prefix in
    effect. Raises :class:`BytecodeCacheError` when the cache cannot be used.
    """
    if sys.pycache_prefix:
        return sys.pycache_prefix
    if sys.dont_write_bytecode:
        return None
    path = user_bytecode_cache_dir()
    _private_directory(path)
    prefix = str(path)
    sys.pycache_prefix = prefix
    # Python children (okg re-entering itself, extension runners, tools the
    # sources start) read the variable at startup.
    os.environ[PYCACHE_PREFIX_ENV] = prefix
    return prefix


def enter_user_bytecode_cache() -> str | None:
    """Entry-point form: report a refusal on stderr and exit 2."""
    try:
        return use_user_bytecode_cache()
    except BytecodeCacheError as exc:
        print(f"okg: error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
