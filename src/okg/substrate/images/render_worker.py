"""Run image renders in a separate, bounded process (PACT first-class-images, #3046).

Rendering untrusted bytes is not safe to do inside the ingest process. PDFium
runs in-process native code that Python cannot interrupt: a 2 KB PDF of nested
form XObjects can take seconds and gigabytes, and it would do so inside a chunk
flush that holds the run's locks. A ``MemoryError`` there would also look like
an ordinary render failure and be cached for good.

So everything after the file's bytes are read -- PDF page sizing and
rasterising, PNG/JPEG decoding, scaling and PNG encoding -- runs in a helper
process:

* One helper per ingest run, started on the first render and fed jobs over a
  pipe, so a run of many small images pays for one interpreter start, not one
  per image.
* Each job has a wall-clock limit (`RENDER_TIMEOUT_S`). On a timeout the
  helper is killed and the next job starts a fresh one.
* The helper has a memory limit (`RENDER_MEMORY_BYTES`). How it is enforced
  depends on the platform, below.
* A helper that dies (timeout, memory limit, a native crash) yields the
  closed reason ``image_render_limit_exceeded`` for that image. The caller
  does not cache it: whether a render fits a limit depends on the machine and
  the limit, so a later run tries again.
* The helper is started with ``close_fds`` (it holds only its two pipes and
  stderr -- no database connection or other descriptor of the parent), a
  minimal environment (no DSN, password or token variables), ``cwd = /`` and
  no core dumps.

What is bounded, by platform:

* Linux: ``RLIMIT_DATA`` caps the helper's private writable memory (heap and
  anonymous mappings, since Linux 4.7), so an allocation past the limit fails
  inside the helper, which then dies or reports the limit. ``RLIMIT_DATA``
  rather than ``RLIMIT_AS``: native allocators reserve large inaccessible
  address ranges that ``RLIMIT_AS`` would count. The RSS watchdog below runs
  too.
* macOS: the kernel does not enforce ``RLIMIT_AS``, ``RLIMIT_DATA`` or
  ``RLIMIT_RSS``. A watchdog thread in the helper reads its peak resident
  size every `_WATCHDOG_INTERVAL_S` and exits the helper once it passes the
  limit. PDFium is called through ctypes, which releases the GIL, so the
  thread runs while PDFium works. This bounds resident memory only, checked
  by sampling: memory macOS compresses or swaps out is not counted, and
  growth between two samples can overshoot the limit.
* Both: the wall-clock limit is enforced by the parent and always holds.

The parent kills a helper that misses its deadline; it never waits on one.
"""
from __future__ import annotations

import json
import logging
import os
import selectors
import signal
import struct
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

log = logging.getLogger(__name__)

#: Wall-clock limit for one render, in seconds. Real figures render in well
#: under a second; this bounds a hostile or pathological file.
RENDER_TIMEOUT_S: float = 30.0
#: Memory limit for the helper process, in bytes (1.5 GiB). The largest
#: legitimate render is a page at `MAX_DECODE_PIXELS` (50 Mpx): about 200 MB
#: for the RGBA bitmap plus a few copies while it is converted and scaled.
RENDER_MEMORY_BYTES: int = 1536 * 1024 * 1024
#: How long the helper may take to start and report ready. Separate from the
#: render limit: a helper that cannot start is a broken install, raised loudly.
_STARTUP_TIMEOUT_S: float = 60.0
_WATCHDOG_INTERVAL_S: float = 0.01

#: Exit status of a helper whose watchdog saw the memory limit passed.
_EXIT_MEMORY = 71

LIMIT_REASON = "image_render_limit_exceeded"

#: Environment variables the helper keeps. Everything else (DSNs, passwords,
#: tokens) stays in the parent.
_ENV_KEEP = (
    "PATH", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "HOME", "SYSTEMROOT",
)

_HEADER = struct.Struct(">I")
_READY = b"okg-render-worker-ready\n"


class RenderWorkerError(RuntimeError):
    """The helper failed in a way that is a bug, not a property of the image:
    it could not start, or the render raised an unexpected exception."""


@dataclass(frozen=True)
class WorkerResult:
    png: Optional[bytes] = None
    width: int = 0
    height: int = 0
    error: Optional[str] = None


# ── parent side ────────────────────────────────────────────────────────

def _child_env() -> dict[str, str]:
    env = {key: os.environ[key] for key in _ENV_KEEP if key in os.environ}
    # The helper imports the same okg the parent runs, whether it is installed
    # or on PYTHONPATH.
    import okg

    okg_parent = str(Path(okg.__file__).resolve().parents[1])
    paths = [okg_parent]
    if os.environ.get("PYTHONPATH"):
        paths.append(os.environ["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


class RenderWorker:
    """A lazily started helper process that renders one image at a time."""

    def __init__(
        self,
        *,
        timeout_s: Optional[float] = None,
        memory_bytes: Optional[int] = None,
    ) -> None:
        self.timeout_s = RENDER_TIMEOUT_S if timeout_s is None else timeout_s
        self.memory_bytes = RENDER_MEMORY_BYTES if memory_bytes is None else memory_bytes
        self._proc: Optional[subprocess.Popen[bytes]] = None
        #: Helpers started so far (for tests and diagnostics).
        self.starts = 0

    # lifecycle

    def _start(self) -> subprocess.Popen[bytes]:
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "okg.substrate.images.render_worker",
                str(self.memory_bytes),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=None,
            close_fds=True,
            cwd="/",
            env=_child_env(),
        )
        self.starts += 1
        assert proc.stdout is not None
        try:
            ready = _read_exact(proc.stdout.fileno(), len(_READY), time.monotonic() + _STARTUP_TIMEOUT_S)
        except (_Timeout, EOFError) as exc:
            _kill(proc)
            raise RenderWorkerError(
                f"the image render helper did not start ({type(exc).__name__}, "
                f"exit status {proc.returncode})"
            ) from None
        if ready != _READY:
            _kill(proc)
            raise RenderWorkerError("the image render helper sent an unexpected greeting")
        return proc

    def close(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            assert proc.stdin is not None
            proc.stdin.close()  # EOF: the helper exits
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001 - a helper that will not exit is killed
            _kill(proc)
        finally:
            if proc.stdout is not None:
                proc.stdout.close()

    def __enter__(self) -> "RenderWorker":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _job_timeout(self, timeout_s: Optional[float]) -> float:
        """This job's limit: the helper's own, or less when the caller has less."""
        if timeout_s is None:
            return self.timeout_s
        return max(0.0, min(self.timeout_s, timeout_s))

    # one job

    def render(
        self,
        content: bytes,
        *,
        source_format: str,
        dpi: int,
        max_side: int,
        renderer: str,
        label: str,
        timeout_s: Optional[float] = None,
    ) -> WorkerResult:
        """Render in the helper. A timeout, memory limit or crash returns
        `LIMIT_REASON`; the helper is then discarded and restarted on the
        next call. `renderer` is the renderer the render id was keyed by; the
        helper refuses to render with any other. `timeout_s` can only shorten
        this job's limit (a read call's remaining deadline), never lengthen
        it."""
        if self._proc is None or self._proc.poll() is not None:
            if self._proc is not None:
                self._discard()
            self._proc = self._start()
        proc = self._proc
        assert proc.stdin is not None and proc.stdout is not None
        header = json.dumps({
            "source_format": source_format,
            "dpi": dpi,
            "max_side": max_side,
            "renderer": renderer,
            "label": label,
            "length": len(content),
        }).encode("utf-8")
        limit = self._job_timeout(timeout_s)
        deadline = time.monotonic() + limit
        try:
            proc.stdin.write(_HEADER.pack(len(header)) + header)
            proc.stdin.write(content)
            proc.stdin.flush()
            (reply_len,) = _HEADER.unpack(_read_exact(proc.stdout.fileno(), _HEADER.size, deadline))
            reply = json.loads(_read_exact(proc.stdout.fileno(), reply_len, deadline))
            png = None
            if reply.get("png_length"):
                png = _read_exact(proc.stdout.fileno(), int(reply["png_length"]), deadline)
        except _Timeout:
            self._discard()
            log.warning(
                "image %s: render passed the %.1f s limit; the helper was killed",
                label, limit,
            )
            return WorkerResult(error=LIMIT_REASON)
        except (EOFError, BrokenPipeError, ConnectionError, OSError, ValueError):
            status = self._discard()
            cause = (
                "memory limit" if status == _EXIT_MEMORY
                else f"signal {-status}" if status is not None and status < 0
                else f"exit status {status}"
            )
            log.warning("image %s: the render helper died (%s)", label, cause)
            return WorkerResult(error=LIMIT_REASON)
        if reply.get("exception"):
            raise RenderWorkerError(
                f"image {label}: the render raised {reply['exception']}"
            )
        if reply.get("error"):
            return WorkerResult(error=str(reply["error"]))
        return WorkerResult(
            png=png, width=int(reply["width"]), height=int(reply["height"]),
        )

    def _discard(self) -> Optional[int]:
        proc, self._proc = self._proc, None
        if proc is None:
            return None
        _kill(proc)
        for stream in (proc.stdin, proc.stdout):
            try:
                if stream is not None:
                    stream.close()
            except OSError:
                pass
        return proc.returncode


class _Timeout(Exception):
    pass


def _kill(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    proc.wait()


def _read_exact(fd: int, length: int, deadline: float) -> bytes:
    """Read `length` bytes from `fd` by `deadline`, or raise."""
    chunks: list[bytes] = []
    remaining = length
    with selectors.DefaultSelector() as selector:
        selector.register(fd, selectors.EVENT_READ)
        while remaining > 0:
            left = deadline - time.monotonic()
            if left <= 0 or not selector.select(timeout=left):
                raise _Timeout()
            chunk = os.read(fd, min(remaining, 1 << 20))
            if not chunk:
                raise EOFError()
            chunks.append(chunk)
            remaining -= len(chunk)
    return b"".join(chunks)


# ── helper side ────────────────────────────────────────────────────────

def _peak_rss_bytes() -> int:
    if sys.platform.startswith("linux"):
        # Not ru_maxrss: on Linux it survives exec, so a helper forked from a
        # large parent would start "over" the limit and exit at once. VmHWM is
        # the peak of this process image's own memory, fresh after exec.
        with open("/proc/self/status", "rb") as status:
            for line in status:
                if line.startswith(b"VmHWM:"):
                    return int(line.split()[1]) * 1024
    import resource

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS bytes.
    return peak if sys.platform == "darwin" else peak * 1024


def _apply_limits(memory_bytes: int) -> None:
    import resource

    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if sys.platform.startswith("linux"):
        resource.setrlimit(resource.RLIMIT_DATA, (memory_bytes, memory_bytes))

    def watchdog() -> None:
        while True:
            if _peak_rss_bytes() > memory_bytes:
                os.write(2, b"okg image render helper: memory limit passed, exiting\n")
                os._exit(_EXIT_MEMORY)
            time.sleep(_WATCHDOG_INTERVAL_S)

    threading.Thread(target=watchdog, name="rss-watchdog", daemon=True).start()


def _read_stdin(stream: Any, length: int) -> Optional[bytes]:
    data = stream.read(length)
    if len(data) < length:
        return None
    return data


def main(argv: list[str]) -> int:
    memory_bytes = int(argv[1])
    # Keep the reply channel private: anything else that writes to fd 1 (a
    # print, a native library) goes to stderr instead.
    reply = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)
    _apply_limits(memory_bytes)
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent decides when to stop
    logging.basicConfig(
        level=logging.WARNING, stream=sys.stderr,
        format="okg image render helper: %(name)s: %(message)s",
    )
    logging.getLogger("okg.substrate.imaging").setLevel(logging.WARNING)

    from okg.substrate.images.render import render_bytes, renderer_for

    stdin = sys.stdin.buffer
    reply.write(_READY)
    reply.flush()
    while True:
        head = _read_stdin(stdin, _HEADER.size)
        if head is None:
            return 0
        (header_len,) = _HEADER.unpack(head)
        raw_header = _read_stdin(stdin, header_len)
        if raw_header is None:
            return 0
        job = json.loads(raw_header)
        content = _read_stdin(stdin, int(job["length"]))
        if content is None:
            return 0
        out: dict[str, Any] = {}
        png = b""
        try:
            helper_renderer = renderer_for(job["source_format"])
            if helper_renderer != job["renderer"]:
                raise RuntimeError(
                    f"the helper renders with {helper_renderer!r}, but the render "
                    f"id was keyed by {job['renderer']!r}"
                )
            result = render_bytes(
                content,
                source_format=job["source_format"],
                dpi=int(job["dpi"]),
                max_side=int(job["max_side"]),
                label=str(job["label"]),
            )
        except MemoryError:
            out["error"] = LIMIT_REASON
        except Exception as exc:  # noqa: BLE001 - reported to the parent, which raises
            out["exception"] = f"{type(exc).__name__}: {str(exc)[:500]}"
        else:
            if result.error is not None:
                out["error"] = result.error
            else:
                png = result.png or b""
                out.update(width=result.width, height=result.height, png_length=len(png))
        if _peak_rss_bytes() > memory_bytes:
            os._exit(_EXIT_MEMORY)
        encoded = json.dumps(out).encode("utf-8")
        reply.write(_HEADER.pack(len(encoded)) + encoded + png)
        reply.flush()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
