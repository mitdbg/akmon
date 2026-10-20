"""Run each ingest source in its own process (the source job, lane H stage 1).

Before this module every source of one source-sync step ran as a thread of
one Python process, so all of them shared one interpreter lock: one process
used 0.97 cores while 79% of source-thread samples waited on that lock. The
design (PR #2917, section 3) moves each source into a ``spawn`` child process
and changes nothing else: every child runs today's ``SourceRunner`` path.

What lives here:

* the coordinator (:func:`run_source_jobs`), which starts one child per source
  as soon as that source's predecessors finish -- not layer by layer -- and
  admits each start against a connection budget and a memory budget;
* the child entry point (:func:`child_main`) with its orphan guards: a
  watcher thread that exits the moment the parent's end of the lifeline pipe
  closes, and a per-source session advisory lock;
* the spill file that carries a source's write-set back to the parent (the
  pipe carries only a small summary);
* memory measurement (``proc_pid_rusage`` on macOS, ``/proc`` on Linux).

``ingest/service.py`` owns the per-source work itself (``_run_source``);
``ingest/source_job_runtime.py`` owns the admission preparation that must run
in the parent, the child's start payload, and the failure path a crashed
child needs. This module never imports either at module level, because both
import this module.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import errno
import json
import logging
import multiprocessing
import multiprocessing.connection
import os
import shutil
import signal
import socket
import stat
import sys
import threading
import time
import uuid
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Collection
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence

from okg.substrate.errors import (
    SourceRunAlreadyActive,
    SubstrateError,
)

log = logging.getLogger(__name__)

__all__ = [
    "HEAVY_SOURCES",
    "SOURCE_MEMORY_LIMIT_BYTES",
    "SOURCE_MEMORY_LIMIT_ENV",
    "SPILLED_RESULT_KEYS",
    "SourceJobHandle",
    "SourceJobOutcome",
    "SourceJobRuntime",
    "SpawnedSourceJob",
    "SpillUnreadable",
    "child_main",
    "connections_per_child",
    "memory_limit_bytes",
    "process_footprint_bytes",
    "read_spill",
    "run_lock_key",
    "run_source_jobs",
    "source_job_step_dir",
    "sweep_stale_step_dirs",
    "write_spill",
]

_GIB = 1024 ** 3

#: Memory limit of every source's child: a flat 16 GiB. The lane H rebuild
#: saw admission readback alone take 4 to 10+ GB per heavy source.
#: OKG_SOURCE_MEMORY_LIMIT_BYTES replaces it for all sources.
SOURCE_MEMORY_LIMIT_BYTES = 16 * _GIB
SOURCE_MEMORY_LIMIT_ENV = "OKG_SOURCE_MEMORY_LIMIT_BYTES"

#: Sources that run one at a time until the admission-readback fix lands.
HEAVY_SOURCES = frozenset({"code-structure", "agent-sessions", "codex-sessions"})

#: Result keys that can be arbitrarily large. They travel through the spill
#: file, never through the pipe.
SPILLED_RESULT_KEYS = ("graph_write_set", "source_delta_batch")

SPILL_SCHEMA = "okg.source_spill.v1"
_SPILL_CHUNK_ITEMS = 10_000

MEMORY_SAMPLE_INTERVAL_SECONDS = 5.0
_POLL_SECONDS = 1.0
_CONNECTION_HEADROOM_DEFAULT = 20
_CONNECTION_WAIT_SECONDS_DEFAULT = 300.0
_RUN_LOCK_WAIT_SECONDS_DEFAULT = 60.0
_CHILD_JOIN_SECONDS = 60.0

#: Exit status of a child whose parent died (the lifeline pipe closed).
LIFELINE_EXIT_STATUS = 70

#: `okg.source_progress` key holding the last recorded peak memory of a
#: source's child, read by the memory admission of the next run. It is in the
#: `sync.` diagnostic namespace, beside the stage timings, so it is never
#: mistaken for a source's own progress marker.
PEAK_MEMORY_PROGRESS_KEY = "sync.source_process.peak_memory_bytes"
#: The most database connections the source's child held at once.
PEAK_CONNECTIONS_PROGRESS_KEY = "sync.source_process.peak_connections"

_STEP_DIR_PREFIX = "step-"


# --------------------------------------------------------------------------
# Configuration read from the environment. Malformed values raise; they are
# never replaced by the default.
# --------------------------------------------------------------------------


def _env_number(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(
            f"{name}={raw!r} is not a number; refusing to fall back to "
            f"the default {default!r}"
        ) from None
    if value < minimum:
        raise ValueError(f"{name}={raw!r} must be at least {minimum!r}")
    return value


def connection_headroom() -> int:
    return int(_env_number("OKG_SOURCE_CONNECTION_HEADROOM", _CONNECTION_HEADROOM_DEFAULT))


def connection_wait_seconds() -> float:
    return _env_number("OKG_SOURCE_CONNECTION_WAIT_SECONDS", _CONNECTION_WAIT_SECONDS_DEFAULT)


def run_lock_wait_seconds() -> float:
    return _env_number("OKG_SOURCE_RUN_LOCK_WAIT_SECONDS", _RUN_LOCK_WAIT_SECONDS_DEFAULT)


def connections_per_child() -> int:
    """Connections one child may open: its pool, interval GC, compensation."""
    from okg.substrate.db.concurrency_budget import width

    return width("ingest.source_process") + width("chronos.interval_gc") + 1


def physical_memory_bytes() -> int:
    pages = os.sysconf("SC_PHYS_PAGES")
    page_size = os.sysconf("SC_PAGE_SIZE")
    if pages <= 0 or page_size <= 0:
        raise RuntimeError("cannot read the physical memory size of this host")
    return int(pages) * int(page_size)


def memory_budget_bytes() -> int:
    """Memory the running children may hold together: half of RAM by default."""
    raw = os.environ.get("OKG_SOURCE_MEMORY_BUDGET_BYTES")
    if raw is not None and raw.strip():
        return int(_env_number("OKG_SOURCE_MEMORY_BUDGET_BYTES", 0, minimum=1))
    return physical_memory_bytes() // 2


def memory_limit_bytes() -> int:
    """Memory one source's child may hold: the same for every source.

    OKG_SOURCE_MEMORY_LIMIT_BYTES (whole bytes) sets it for all sources. Any
    positive value is used as given, including one below the 16 GiB
    default, so an operator can also tighten it. A malformed, non-integer,
    zero or negative value raises; it is never replaced by the default.
    """
    raw = os.environ.get(SOURCE_MEMORY_LIMIT_ENV)
    if raw is None or not raw.strip():
        return SOURCE_MEMORY_LIMIT_BYTES
    try:
        value = int(raw.strip())
    except ValueError:
        raise ValueError(
            f"{SOURCE_MEMORY_LIMIT_ENV}={raw!r} is not a whole number of "
            "bytes; refusing to fall back to the default "
            f"{SOURCE_MEMORY_LIMIT_BYTES}"
        ) from None
    if value < 1:
        raise ValueError(
            f"{SOURCE_MEMORY_LIMIT_ENV}={raw!r} must be a positive number of bytes"
        )
    return value


def is_heavy_source(name: str, ownership_id: str | None = None) -> bool:
    return name in HEAVY_SOURCES or bool(ownership_id and ownership_id in HEAVY_SOURCES)


def run_lock_key(deployment: str, source: str) -> str:
    return f"okg.source.run:{deployment}:{source}"


# --------------------------------------------------------------------------
# Memory measurement
# --------------------------------------------------------------------------


class _RusageInfoV0(ctypes.Structure):
    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
    ]


_RUSAGE_INFO_V0 = 0
_LIBPROC: Any = None
_LIBPROC_LOCK = threading.Lock()


def _libproc() -> Any:
    global _LIBPROC
    with _LIBPROC_LOCK:
        if _LIBPROC is None:
            path = ctypes.util.find_library("proc") or "/usr/lib/libproc.dylib"
            lib = ctypes.CDLL(path, use_errno=True)
            lib.proc_pid_rusage.argtypes = [
                ctypes.c_int,
                ctypes.c_int,
                ctypes.POINTER(_RusageInfoV0),
            ]
            lib.proc_pid_rusage.restype = ctypes.c_int
            _LIBPROC = lib
        return _LIBPROC


def process_footprint_bytes(pid: int) -> int | None:
    """Memory a child holds now, or None when the process no longer exists.

    macOS: the physical footprint from ``proc_pid_rusage``. The resident size
    (``ps -o rss=``) undercounts compressed and swapped pages there. Linux:
    the resident size from ``/proc/<pid>/status``. Any other platform raises:
    the coordinator cannot enforce a limit it cannot measure.
    """
    if sys.platform == "darwin":
        info = _RusageInfoV0()
        rc = _libproc().proc_pid_rusage(int(pid), _RUSAGE_INFO_V0, ctypes.byref(info))
        if rc != 0:
            err = ctypes.get_errno()
            if err == errno.ESRCH:
                return None
            raise OSError(err, f"proc_pid_rusage({pid}) failed (errno {err})")
        return int(info.ri_phys_footprint)
    if sys.platform.startswith("linux"):
        try:
            with open(f"/proc/{int(pid)}/status", encoding="ascii") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) * 1024
        except (FileNotFoundError, ProcessLookupError):
            return None
        # A zombie has no VmRSS line: it holds no memory any more.
        return None
    raise RuntimeError(
        f"source process memory cannot be measured on {sys.platform}; "
        "only macOS and Linux are supported"
    )


# --------------------------------------------------------------------------
# Spill files
# --------------------------------------------------------------------------


class SpillUnreadable(RuntimeError):
    """A spill file is missing, truncated, or malformed: use no hint."""


def _chunks(items: Sequence[Any]) -> Iterator[list[Any]]:
    for start in range(0, len(items), _SPILL_CHUNK_ITEMS):
        yield list(items[start:start + _SPILL_CHUNK_ITEMS])


def _spill_lines(payload: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    """One JSON object per chunk.

    A value of the form ``{field: {group: [ids...]}}`` is split into chunks of
    at most 10,000 ids, and ``{field: {group: {id: [...]}}}`` into chunks of
    at most 10,000 entries; anything else is one line.
    """
    for key, value in payload.items():
        if not isinstance(value, Mapping):
            yield {"key": key, "path": [], "value": value}
            continue
        for field_name, field_value in value.items():
            if not isinstance(field_value, Mapping):
                yield {"key": key, "path": [field_name], "value": field_value}
                continue
            yield {"key": key, "path": [field_name], "value": {}}
            for group, group_value in field_value.items():
                if isinstance(group_value, list):
                    if not group_value:
                        yield {"key": key, "path": [field_name, group], "items": []}
                    for chunk in _chunks(group_value):
                        yield {"key": key, "path": [field_name, group], "items": chunk}
                elif isinstance(group_value, Mapping):
                    entries = list(group_value.items())
                    if not entries:
                        yield {"key": key, "path": [field_name, group], "entries": {}}
                    for chunk in _chunks(entries):
                        yield {
                            "key": key,
                            "path": [field_name, group],
                            "entries": dict(chunk),
                        }
                else:
                    yield {"key": key, "path": [field_name, group], "value": group_value}


def write_spill(path: Path, payload: Mapping[str, Any]) -> int:
    """Write ``payload`` to ``path`` one chunk per line; return the line count.

    The file is written under a temporary name and renamed into place, and it
    ends with a trailer line naming the line count, so a reader can tell a
    complete file from a truncated one.
    """
    tmp = path.with_name(path.name + ".partial")
    count = 0
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(json.dumps({"schema": SPILL_SCHEMA, "keys": list(payload)}) + "\n")
        for line in _spill_lines(payload):
            handle.write(json.dumps(line, allow_nan=False) + "\n")
            count += 1
        handle.write(json.dumps({"end": True, "lines": count}) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    return count


def read_spill(path: Path) -> dict[str, Any]:
    """Rebuild the spilled payload, or raise :class:`SpillUnreadable`."""
    try:
        handle = open(path, encoding="utf-8")
    except OSError as exc:
        raise SpillUnreadable(f"spill file {path} cannot be opened: {exc}") from exc
    out: dict[str, Any] = {}
    seen = 0
    ended = False
    with handle:
        try:
            header = json.loads(handle.readline() or "null")
        except json.JSONDecodeError as exc:
            raise SpillUnreadable(f"spill file {path} has a malformed header") from exc
        if not isinstance(header, dict) or header.get("schema") != SPILL_SCHEMA:
            raise SpillUnreadable(f"spill file {path} has no {SPILL_SCHEMA} header")
        for raw in handle:
            if ended:
                raise SpillUnreadable(f"spill file {path} has data after its trailer")
            try:
                line = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise SpillUnreadable(f"spill file {path} has a malformed line") from exc
            if line.get("end") is True:
                if line.get("lines") != seen:
                    raise SpillUnreadable(
                        f"spill file {path} trailer names {line.get('lines')} "
                        f"lines but {seen} were read"
                    )
                ended = True
                continue
            seen += 1
            key = line["key"]
            path_parts = line["path"]
            if not path_parts:
                out[key] = line["value"]
                continue
            target = out.setdefault(key, {})
            if len(path_parts) == 1:
                target[path_parts[0]] = line["value"]
                continue
            field_name, group = path_parts
            field_target = target.setdefault(field_name, {})
            if "items" in line:
                field_target.setdefault(group, []).extend(line["items"])
            elif "entries" in line:
                field_target.setdefault(group, {}).update(line["entries"])
            else:
                field_target[group] = line["value"]
    if not ended:
        raise SpillUnreadable(f"spill file {path} is truncated (no trailer)")
    missing = [key for key in header.get("keys") or () if key not in out]
    if missing:
        raise SpillUnreadable(f"spill file {path} lacks keys {missing}")
    return out


# --------------------------------------------------------------------------
# Step directories (mode 0700: node ids contain paths and author emails)
# --------------------------------------------------------------------------


def spill_base_dir() -> Path:
    """The per-user directory that holds step directories.

    Defaults to the user's cache directory, never a shared temporary
    directory: spill files hold node ids, which contain paths and author
    emails.
    """
    configured = os.environ.get("OKG_SOURCE_SPILL_DIR")
    if configured and configured.strip():
        return Path(configured)
    cache = os.environ.get("XDG_CACHE_HOME")
    root = Path(cache) if cache and cache.strip() else Path.home() / ".cache"
    return root / "okg" / "source-jobs"


def _require_private_dir(path: Path) -> None:
    """Refuse a directory that is a symlink, not ours, or open to others."""
    info = os.lstat(path)
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
        raise PermissionError(f"source-job directory {path} is not a real directory")
    if info.st_uid != os.getuid():
        raise PermissionError(
            f"source-job directory {path} is owned by uid {info.st_uid}, "
            f"not this user ({os.getuid()})"
        )
    if stat.S_IMODE(info.st_mode) & 0o077:
        os.chmod(path, 0o700, follow_symlinks=False)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def sweep_stale_step_dirs(base: Path | None = None) -> list[Path]:
    """Delete step directories whose owning coordinator process is gone."""
    base = base or spill_base_dir()
    removed: list[Path] = []
    if not base.is_dir():
        return removed
    for child in sorted(base.iterdir()):
        if not child.is_dir() or not child.name.startswith(_STEP_DIR_PREFIX):
            continue
        try:
            pid = int(child.name[len(_STEP_DIR_PREFIX):].split("-", 1)[0])
        except ValueError:
            continue
        if pid != os.getpid() and _pid_alive(pid):
            continue
        shutil.rmtree(child, ignore_errors=False)
        removed.append(child)
    return removed


@contextmanager
def source_job_step_dir(base: Path | None = None) -> Iterator[Path]:
    """Create a 0700 directory for one source-sync step; delete it afterwards.

    Leftovers from a coordinator that died are swept first.
    """
    base = base or spill_base_dir()
    base.mkdir(mode=0o700, parents=True, exist_ok=True)
    _require_private_dir(base)
    swept = sweep_stale_step_dirs(base)
    if swept:
        log.info("removed %d leftover source-job step directories", len(swept))
    step = base / f"{_STEP_DIR_PREFIX}{os.getpid()}-{uuid.uuid4().hex[:12]}"
    step.mkdir(mode=0o700)
    _require_private_dir(step)
    try:
        yield step
    finally:
        shutil.rmtree(step, ignore_errors=True)


# --------------------------------------------------------------------------
# The child process
# --------------------------------------------------------------------------


def _watch_lifeline(lifeline: multiprocessing.connection.Connection) -> None:
    """Exit the child the moment the parent's end of the lifeline closes.

    macOS has no parent-death signal. The parent never writes to this pipe; a
    read returns only at end-of-file, which happens when the parent process
    is gone, however it died.
    """
    while True:
        try:
            lifeline.recv()
        except (EOFError, OSError):
            os._exit(LIFELINE_EXIT_STATUS)


#: Serializes sends on the result pipe: the run thread and the connection
#: sampler thread both write to it.
_SEND_LOCK = threading.Lock()


def _send(result_conn: Any, kind: str, payload: Any) -> None:
    with _SEND_LOCK:
        result_conn.send((kind, payload))


def _database_port(dsn: str) -> int:
    """The port this child's database connections go to (libpq's 5432 when
    the DSN names none; the first one when it names several)."""
    from okg.substrate.db import dsn as dsn_module

    return int(str(dsn_module.parse(dsn).port or "5432").split(",")[0])


def count_database_sockets(port: int) -> int:
    """Open sockets of this process connected to the database port.

    TCP sockets whose peer port is ``port`` and Unix sockets whose peer path
    is PostgreSQL's ``.s.PGSQL.<port>``. It counts the connections the
    process really holds -- its pool, interval GC, compensation, anything a
    library opens -- without asking the server.

    It never duplicates or closes a descriptor it did not open. macOS lets a
    system library guard a descriptor; ``dup`` or ``close`` on a guarded one
    makes the kernel kill the whole process with SIGKILL, which no
    ``except`` can catch. A first tree-sitter grammar download opens such a
    descriptor, so a ``dup`` of every open descriptor killed the
    ``code_structure`` child on a fresh Mac. Only ``fstat`` (not guarded)
    touches every descriptor; a descriptor is wrapped only when it is a
    socket, and the wrapper is detached, never closed.
    """
    fd_dir = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"
    count = 0
    for name in os.listdir(fd_dir):
        try:
            fd = int(name)
        except ValueError:
            continue
        peer = _socket_peer(fd)
        if isinstance(peer, tuple) and len(peer) >= 2 and peer[1] == port:
            count += 1
        elif isinstance(peer, (str, bytes)) and f".s.PGSQL.{port}" in (
            peer.decode(errors="replace") if isinstance(peer, bytes) else peer
        ):
            count += 1
    return count


def _socket_peer(fd: int) -> Any:
    """The peer address of descriptor ``fd`` when it is a socket, else None.

    Borrows the descriptor without duplicating it: ``socket(fileno=fd)``
    wraps the same descriptor and ``detach`` hands it back unclosed.
    """
    try:
        mode = os.fstat(fd).st_mode
    except OSError:
        return None
    if not stat.S_ISSOCK(mode):
        return None
    # socket(fileno=) applies socket.getdefaulttimeout() to the descriptor,
    # as the old dup-and-wrap did; nothing in src sets a default timeout.
    try:
        sock = socket.socket(fileno=fd)
    except OSError:
        return None
    try:
        return sock.getpeername()
    except OSError:
        return None
    finally:
        sock.detach()


def _sample_connections(result_conn: Any, port: int, stop: threading.Event) -> None:
    """Report this child's current and peak database connections each second.

    Samples once at start, every second, and once more when stopped, so even
    a short run reports its peak before its result.
    """
    peak = 0
    last = -1
    while True:
        stopping = stop.is_set()
        try:
            current = count_database_sockets(port)
        except OSError:
            current = None
        if current is not None:
            peak = max(peak, current)
            if current != last or stopping:
                last = current
                try:
                    _send(result_conn, "connections", {"current": current, "peak": peak})
                except (OSError, ValueError):
                    return
        if stopping or stop.wait(1.0):
            if stopping:
                return


@dataclass
class ChildJobHooks:
    """What the child's ``_run_source`` calls at its two job boundaries."""

    dsn: str
    deployment: str
    result_conn: Any
    lock_wait_seconds: float
    _stack: ExitStack = field(default_factory=ExitStack)
    lock_key: str | None = None

    def acquire_run_lock(self, source: str) -> None:
        """Take the per-source session lock before the first write.

        The lock lives on a pool connection this child keeps for its whole
        life, so it covers the run and the admission finalization after it,
        and it is released when the connection closes -- including when the
        child dies.
        """
        from okg.substrate.db.pool import RESERVED, pooled_connection

        key = run_lock_key(self.deployment, source)
        # One of the ingest.source_process slots: the lock is counted
        # against the child's pool, never opened beside it.
        conn = self._stack.enter_context(
            pooled_connection(self.dsn, purpose=RESERVED)
        )
        deadline = time.monotonic() + self.lock_wait_seconds
        started = time.monotonic()
        while True:
            row = conn.execute(
                "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                (key,),
            ).fetchone()
            if row is not None and bool(row[0]):
                self.lock_key = key
                self._stack.callback(
                    lambda: conn.execute(
                        "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                        (key,),
                    )
                )
                return
            if time.monotonic() >= deadline:
                raise SourceRunAlreadyActive(
                    "another run of this source is still active; refusing to "
                    "write alongside it",
                    data={
                        "source": source,
                        "deployment": self.deployment,
                        "lock_key": key,
                        "waited_seconds": round(time.monotonic() - started, 3),
                    },
                    fix_hint=(
                        "find the process holding the lock in pg_locks and "
                        "let it finish or stop it, then re-run the source"
                    ),
                )
            time.sleep(0.5)

    def started(self, **payload: Any) -> None:
        _send(self.result_conn, "started", payload)

    def close(self) -> None:
        self._stack.close()


#: The deployment binding scope a source child opened; see
#: :func:`prepare_child_process`.
_CHILD_DEPLOYMENT_SCOPE: Any = None


def prepare_child_process(payload: Mapping[str, Any]) -> None:
    """Process setup a source child does before any other work.

    It configures the child's connection pool and binds the deployment
    folders the parent command bound. A folder-resolved deployment (#2951)
    is found by name only inside the command's binding scope, a context
    variable that does not reach a ``spawn`` child. ``None`` means the
    parent ran outside any scope, and the child then opens none either. The
    scope opened here stays open for the child's life (it ends with
    ``os._exit``); the module keeps a reference, because a collected scope
    would run its exit and drop the bindings. A folder that no longer binds
    is refused by name rather than left to a later "not found".
    """
    global _CHILD_DEPLOYMENT_SCOPE
    from okg.substrate.db.pool import configure_source_process_pool
    from okg.substrate.runtime.source_sync_priority import adopt_run_guard

    configure_source_process_pool()
    # #3003: stop at the next chunk if the parent's source-sync hold dies.
    if payload.get("run_guard"):
        adopt_run_guard(payload["run_guard"], dsn=str(payload["options"]["dsn"]))
    bindings = payload.get("deployment_bindings")
    if bindings is None:
        return
    from okg.substrate.deployments import (
        bind_resolved_deployment,
        deployment_binding_scope,
    )

    _CHILD_DEPLOYMENT_SCOPE = deployment_binding_scope()
    _CHILD_DEPLOYMENT_SCOPE.__enter__()
    for bound_name, bound_dir in bindings.items():
        if not bind_resolved_deployment(bound_name, Path(bound_dir)):
            raise SubstrateError(
                "source child could not bind the deployment folder its "
                "command resolved",
                data={"deployment": bound_name, "deployment_dir": str(bound_dir)},
            )


def child_main(
    result_conn: multiprocessing.connection.Connection,
    lifeline: multiprocessing.connection.Connection,
    payload: Mapping[str, Any],
) -> None:
    """Entry point of one source child (``spawn`` start method)."""
    # Index work left in a child runs inline; the parent drained its own
    # asynchronous schema-index jobs before this child started.
    os.environ["CHRONOS_ASYNC_SCHEMA_INDEXES"] = "0"
    threading.Thread(
        target=_watch_lifeline,
        args=(lifeline,),
        name="okg-source-lifeline",
        daemon=True,
    ).start()
    exit_status = 0
    hooks: ChildJobHooks | None = None
    sampler_stop = threading.Event()
    try:
        from okg.substrate.db.pool import close_all
        from okg.substrate.ingest import service
        from okg.substrate.profiling.context import (
            ProfileContext,
            _current as _current_profile,
            install_psycopg_patch,
        )
        from okg.substrate.profiling.policy import resolve_policy
        from okg.substrate.sources.registry import source_entry_from_description
        from okg.substrate.telemetry import use_trace

        prepare_child_process(payload)
        db_port = _database_port(str(payload["options"]["dsn"]))
        sampler = threading.Thread(
            target=_sample_connections,
            args=(result_conn, db_port, sampler_stop),
            name="okg-source-connections",
            daemon=True,
        )
        sampler.start()
        install_psycopg_patch()
        profile = ProfileContext(
            deployment=str(payload["options"]["deployment"]),
            target_kind="ingest_source_process",
            command="source job",
            policy=resolve_policy(None),
            collect_detail=False,
        )
        _current_profile.set(profile)
        options = service.IngestOptions(**payload["options"])
        hooks = ChildJobHooks(
            dsn=options.dsn,
            deployment=options.deployment,
            result_conn=result_conn,
            lock_wait_seconds=float(payload["lock_wait_seconds"]),
        )
        trace = payload.get("trace") or {}
        with use_trace(
            trace.get("trace_id"),
            span_id=trace.get("span_id"),
            parent_span_id=trace.get("parent_span_id"),
        ):
            try:
                result = service._run_source(
                    options,
                    # The parent checked that this rebuilds equal to the entry
                    # it admitted (`source_job_runtime.source_child_payload`).
                    source_entry_from_description(payload["entry"]),
                    payload["name"],
                    prepared=payload["prepared"],
                    job_hooks=hooks,
                )
            finally:
                hooks.close()
        spilled = {
            key: result.pop(key)
            for key in SPILLED_RESULT_KEYS
            if key in result
        }
        if spilled:
            spill_path = Path(payload["spill_path"])
            try:
                lines = write_spill(spill_path, spilled)
            except (OSError, TypeError, ValueError) as exc:
                # The parent then has no hint for this source and takes the
                # publisher's full-scope path. It never reads this as "no work".
                result["spill"] = {
                    "status": "write_failed",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            else:
                result["spill"] = {
                    "status": "written",
                    "path": str(spill_path),
                    "keys": list(spilled),
                    "lines": lines,
                }
        result["source_process_child"] = {
            "pid": os.getpid(),
            "query_count": profile.query_count,
        }
        sampler_stop.set()
        sampler.join(5.0)
        _send(result_conn, "result", result)
        close_all()
    except BaseException as exc:  # noqa: BLE001 - reported to the parent, then exit non-zero
        exit_status = 1
        try:
            _send(result_conn, "error", {"error": f"{type(exc).__name__}: {exc}"})
        except Exception:  # noqa: BLE001 - the parent sees the exit status instead
            pass
    finally:
        try:
            result_conn.close()
        except Exception:  # noqa: BLE001
            pass
    # Skip interpreter teardown: threads that adapters left behind must not
    # hold the child alive after its result is delivered.
    os._exit(exit_status)


# --------------------------------------------------------------------------
# Handles
# --------------------------------------------------------------------------


class SourceJobHandle(Protocol):
    """One started source job, as the coordinator sees it."""

    name: str
    conn: Any
    pid: int | None
    ready: bool
    memory_limit: int
    peak_bytes: int
    #: Database connections the child holds now (None until it reports).
    connections: int | None
    #: The child delivered its result; it is finishing, never to be killed.
    finished: bool

    def handle_message(self, kind: str, payload: Any) -> None: ...

    def kill_for_memory(self, observed_bytes: int) -> None: ...

    def abort(self, reason: str) -> None: ...

    def finish(self) -> dict[str, Any]: ...


class SpawnedSourceJob:
    """A source running in a ``spawn`` child process.

    ``on_crash`` builds the failed result, compensation included, when the
    child ends without delivering one.
    """

    def __init__(
        self,
        *,
        name: str,
        payload: Mapping[str, Any],
        memory_limit: int,
        on_crash: Callable[["SpawnedSourceJob"], dict[str, Any]],
        target: Callable[..., None] | None = None,
    ) -> None:
        ctx = multiprocessing.get_context("spawn")
        self.name = name
        self.memory_limit = int(memory_limit)
        self.peak_bytes = 0
        self.ready = False
        self.connections: int | None = None
        self.peak_connections = 0
        self.started: dict[str, Any] | None = None
        self.result: dict[str, Any] | None = None
        self.child_error: str | None = None
        self.memory_kill: dict[str, int] | None = None
        self.abort_reason: str | None = None
        self._on_crash = on_crash
        self.started_monotonic = time.monotonic()
        self.conn, result_w = ctx.Pipe(duplex=False)
        lifeline_r, self._lifeline_w = ctx.Pipe(duplex=False)
        self.process = ctx.Process(
            # `target` is for tests that drive the process mechanics with a
            # stand-in child; production always runs `child_main`.
            target=target or child_main,
            args=(result_w, lifeline_r, dict(payload)),
            name=f"okg-source-{name}",
        )
        self.process.start()
        # Drop the parent's copies of the child's ends: the parent must see
        # end-of-file on the result pipe when the child exits, and the child
        # must see end-of-file on the lifeline when the parent exits.
        result_w.close()
        lifeline_r.close()
        self.pid = self.process.pid

    @property
    def finished(self) -> bool:
        return self.result is not None

    def handle_message(self, kind: str, payload: Any) -> None:
        if kind == "started":
            self.ready = True
            self.started = dict(payload)
        elif kind == "connections":
            self.connections = int(payload.get("current") or 0)
            self.peak_connections = max(
                self.peak_connections,
                int(payload.get("peak") or 0),
                self.connections,
            )
        elif kind == "result":
            self.result = payload
        elif kind == "error":
            self.child_error = str(payload.get("error"))
        else:
            self.child_error = f"unknown message from the source child: {kind!r}"

    def kill_for_memory(self, observed_bytes: int) -> None:
        self.memory_kill = {
            "peak_bytes": int(max(observed_bytes, self.peak_bytes)),
            "limit_bytes": self.memory_limit,
        }
        self._kill()

    def abort(self, reason: str) -> None:
        """Stop the child because the coordinator itself failed."""
        self.abort_reason = reason
        self._kill()

    def _kill(self) -> None:
        try:
            self.process.kill()
        except (OSError, ValueError):
            pass

    @property
    def exitcode(self) -> int | None:
        return self.process.exitcode

    def finish(self) -> dict[str, Any]:
        """Join the child (its pipe is already drained) and build the result."""
        self.process.join(_CHILD_JOIN_SECONDS)
        if self.process.is_alive():
            log.error(
                "source child %s (pid %s) closed its pipe but did not exit in "
                "%ss; killing it", self.name, self.pid, _CHILD_JOIN_SECONDS,
            )
            self.process.kill()
            self.process.join()
        try:
            self._lifeline_w.close()
        except OSError:
            pass
        wall_s = round(time.monotonic() - self.started_monotonic, 3)
        if (
            self.result is not None
            and self.memory_kill is None
            and self.abort_reason is None
        ):
            result = self.result
        else:
            result = self._on_crash(self)
        child_info = result.pop("source_process_child", None)
        result["source_process"] = {
            "pid": self.pid,
            "exit_code": self.exitcode,
            "peak_memory_bytes": self.peak_bytes,
            "memory_limit_bytes": self.memory_limit,
            "peak_connections": self.peak_connections,
            "wall_s": wall_s,
            **(
                {"query_count": child_info.get("query_count")}
                if isinstance(child_info, dict)
                else {}
            ),
        }
        self.process.close()
        return result

    def describe_exit(self) -> dict[str, Any]:
        code = self.exitcode
        out: dict[str, Any] = {"exit_code": code}
        if code is not None and code < 0:
            try:
                out["signal"] = signal.Signals(-code).name
            except ValueError:
                out["signal"] = str(-code)
        if code == LIFELINE_EXIT_STATUS:
            out["lifeline_closed"] = True
        if self.child_error:
            out["child_error"] = self.child_error
        if self.abort_reason:
            out["coordinator_error"] = self.abort_reason
        return out


# --------------------------------------------------------------------------
# The coordinator
# --------------------------------------------------------------------------


class SourceJobRuntime(Protocol):
    """Everything the coordinator needs from its environment."""

    def launch(self, name: str) -> SourceJobHandle | dict[str, Any]:
        """Start one source; a dict is a result that needed no child."""

    def connection_state(self) -> tuple[int, int]:
        """Live client connections and the server's ``max_connections``."""

    def recorded_peak(self, name: str) -> int | None: ...

    def memory_limit(self, name: str) -> int: ...

    def is_heavy(self, name: str) -> bool: ...

    def sample_memory(self, pid: int) -> int | None: ...

    def memory_budget(self) -> int: ...

    def connection_budget_exhausted(
        self, name: str, *, live: int, needed: int, allowed: int, waited: float,
    ) -> dict[str, Any]: ...

    def start_failed(self, name: str, *, stage: str, error: str) -> dict[str, Any]:
        """The failed result of a source whose start check or launch raised."""


@dataclass
class SourceJobOutcome:
    results: dict[str, dict[str, Any]] = field(default_factory=dict)
    skipped: dict[str, list[str]] = field(default_factory=dict)
    start_order: list[str] = field(default_factory=list)
    started_at: dict[str, float] = field(default_factory=dict)
    finished_at: dict[str, float] = field(default_factory=dict)
    admission_waits: dict[str, dict[str, Any]] = field(default_factory=dict)


def run_source_jobs(
    *,
    ordered: Sequence[str],
    deps: Mapping[str, Collection[str]],
    runtime: SourceJobRuntime,
    max_concurrency: int,
    clock: Callable[[], float] = time.perf_counter,
) -> SourceJobOutcome:
    """Run every source once its predecessors finished, one child each.

    A source whose predecessor failed or was skipped is skipped with its
    blockers named. A start is admitted only when (1) fewer than
    ``max_concurrency`` children run, (2) no other heavy source runs when this
    one is heavy, (3) the recorded peaks of the running children plus this
    one's fit the memory budget (a source with no recorded peak counts at its
    limit; a child with nothing else running is always admitted), and (4) live
    connections, plus the connections this child and every running child may
    still open, fit ``max_connections`` minus the headroom. A running child
    that has not reported counts at its full allowance; one that has reported
    counts at its allowance minus what it already holds. A start that does not
    fit (4) within the wait limit fails with
    ``source_connection_budget_exhausted``.

    A start check or launch that raises is retried until the wait limit and
    then fails only that source. If the coordinator itself fails, every
    running child is stopped and gets its crash-failure record (compensation
    included) before the error propagates.
    """
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")
    planned = list(dict.fromkeys(ordered))
    planned_set = set(planned)
    state: dict[str, str] = {name: "pending" for name in planned}
    outcome = SourceJobOutcome()
    running: dict[str, SourceJobHandle] = {}
    drained: set[str] = set()
    wait_since: dict[str, float] = {}
    per_child = connections_per_child()
    headroom = connection_headroom()
    wait_limit = connection_wait_seconds()
    budget = runtime.memory_budget()
    last_sample = time.monotonic()  # first periodic sample one interval in

    def finish(name: str) -> None:
        handle = running.pop(name)
        drained.discard(name)
        result = handle.finish()
        outcome.results[name] = result
        outcome.finished_at[name] = clock()
        state[name] = "ok" if result.get("status") == "OK" else "failed"

    def record_immediate(name: str, result: dict[str, Any]) -> None:
        now = clock()
        outcome.results[name] = result
        outcome.started_at.setdefault(name, now)
        outcome.finished_at[name] = now
        if name not in outcome.start_order:
            outcome.start_order.append(name)
        state[name] = "ok" if result.get("status") == "OK" else "failed"

    def start_error(name: str, stage: str, exc: BaseException) -> bool:
        """Record a failed start attempt; True when the source now failed."""
        now = time.monotonic()
        waited = now - wait_since.setdefault(name, now)
        error = f"{type(exc).__name__}: {exc}"
        log.warning("source %s: %s failed (%s); waited %.1fs", name, stage, error, waited)
        if waited < wait_limit:
            return False
        record_immediate(name, runtime.start_failed(name, stage=stage, error=error))
        return True

    def still_to_open() -> int:
        pending = 0
        for handle in running.values():
            if handle.finished:
                continue
            held = handle.connections if handle.ready else None
            pending += per_child if held is None else max(0, per_child - held)
        return pending

    def sample(name: str, handle: SourceJobHandle) -> None:
        if handle.pid is None or name in drained:
            return
        observed = runtime.sample_memory(handle.pid)
        if observed is None:
            return
        handle.peak_bytes = max(handle.peak_bytes, observed)
        if (
            observed > handle.memory_limit
            and not handle.finished
            and getattr(handle, "memory_kill", None) is None
        ):
            log.error(
                "source %s child pid %s uses %d bytes, over its limit "
                "%d; killing it", name, handle.pid, observed,
                handle.memory_limit,
            )
            handle.kill_for_memory(observed)

    try:
        while any(value in ("pending", "running") for value in state.values()):
            connection_snapshot: tuple[int, int] | None = None
            waiting_on_admission = False
            for name in planned:
                if state[name] != "pending":
                    continue
                predecessors = set(deps.get(name, ())) & planned_set
                blockers = sorted(
                    dep for dep in predecessors if state[dep] in ("failed", "skipped")
                )
                if blockers:
                    state[name] = "skipped"
                    outcome.skipped[name] = blockers
                    continue
                if any(state[dep] != "ok" for dep in predecessors):
                    continue
                if len(running) >= max_concurrency:
                    continue
                if runtime.is_heavy(name) and any(
                    runtime.is_heavy(other) for other in running
                ):
                    continue
                if running:
                    reserved = sum(
                        max(
                            handle.peak_bytes,
                            runtime.recorded_peak(other) or handle.memory_limit,
                        )
                        for other, handle in running.items()
                        if not handle.finished
                    )
                    need = runtime.recorded_peak(name) or runtime.memory_limit(name)
                    if reserved + need > budget:
                        continue
                if connection_snapshot is None:
                    try:
                        connection_snapshot = runtime.connection_state()
                    except Exception as exc:  # noqa: BLE001 - retried, then this start fails
                        if not start_error(name, "connection_admission", exc):
                            waiting_on_admission = True
                        continue
                live, max_connections = connection_snapshot
                needed = per_child + still_to_open()
                allowed = max_connections - headroom
                if live + needed > allowed:
                    now = time.monotonic()
                    first = wait_since.setdefault(name, now)
                    waited = now - first
                    outcome.admission_waits[name] = {
                        "live": live, "needed": needed, "allowed": allowed,
                        "waited_seconds": round(waited, 3),
                    }
                    if waited >= wait_limit:
                        record_immediate(
                            name,
                            runtime.connection_budget_exhausted(
                                name, live=live, needed=needed, allowed=allowed,
                                waited=waited,
                            ),
                        )
                    else:
                        waiting_on_admission = True
                    continue
                try:
                    started = runtime.launch(name)
                except Exception as exc:  # noqa: BLE001 - retried, then this start fails
                    if not start_error(name, "child_start", exc):
                        waiting_on_admission = True
                    continue
                wait_since.pop(name, None)
                outcome.started_at[name] = clock()
                outcome.start_order.append(name)
                if isinstance(started, dict):
                    record_immediate(name, started)
                    continue
                state[name] = "running"
                running[name] = started
                # The new child's connections are not visible yet; count them
                # as still to open on the next admission in this pass.
                connection_snapshot = None

            if not running:
                if waiting_on_admission:
                    time.sleep(_POLL_SECONDS)
                    continue
                if any(value == "pending" for value in state.values()):
                    # Only possible when every pending source waits on another
                    # pending source that can never start: a planning defect.
                    stuck = sorted(
                        name for name, value in state.items() if value == "pending"
                    )
                    raise RuntimeError(f"source jobs cannot make progress: {stuck}")
                break

            live_conns = {
                handle.conn: name
                for name, handle in running.items()
                if name not in drained and handle.conn is not None
            }
            if live_conns:
                ready = multiprocessing.connection.wait(
                    list(live_conns), timeout=_POLL_SECONDS,
                )
                for conn in ready:
                    name = live_conns[conn]
                    # Read to end-of-file before waiting on the child, so a
                    # full pipe buffer can never block its exit.
                    try:
                        kind, message = conn.recv()
                    except (EOFError, OSError):
                        drained.add(name)
                        continue
                    running[name].handle_message(kind, message)
                    if kind in ("started", "result"):
                        # A child that finishes between two periodic samples
                        # still gets a measured peak.
                        sample(name, running[name])
            now = time.monotonic()
            if now - last_sample >= MEMORY_SAMPLE_INTERVAL_SECONDS:
                last_sample = now
                for name, handle in running.items():
                    sample(name, handle)
            for name in [
                name for name in running
                if name in drained or running[name].conn is None
            ]:
                finish(name)
    except BaseException as exc:
        # Never leave a child writing unmanaged: stop each one and run its
        # crash-failure path (retract, restore, fail admission) before the
        # error propagates.
        reason = f"{type(exc).__name__}: {exc}"
        for name in list(running):
            handle = running[name]
            try:
                # Read what the child already sent first: one that delivered
                # its result finished its run and admission, so it must be
                # collected as it is, never killed and compensated.
                conn = handle.conn
                if conn is not None and name not in drained:
                    try:
                        while conn.poll(0):
                            kind, message = conn.recv()
                            handle.handle_message(kind, message)
                    except (EOFError, OSError):
                        drained.add(name)
                if not handle.finished:
                    handle.abort(reason)
                finish(name)
            except Exception:  # noqa: BLE001 - keep stopping the others
                log.exception("source %s: stopping its child after a coordinator failure failed", name)
        raise
    return outcome
