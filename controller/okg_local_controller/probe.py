"""Trusted launcher for candidate probes with no ambient lifecycle authority."""

from __future__ import annotations

import json
import os
import platform
import resource
import secrets
import shlex
import shutil
import signal
import stat
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Protocol

from .protocol import (
    PROBE_ATTESTATION_SCHEMA,
    PROBE_INPUT_SCHEMA,
    AuthoritySigner,
    ProbeAttestation,
    canonical_json,
    digest_bytes,
    digest_json,
)

# Reserved by the Linux sandbox supervisor for descendant refusal.
_DESCENDANT_EXIT_STATUS = 125


class ProbeError(RuntimeError):
    pass


@dataclass(frozen=True)
class SandboxLimits:
    timeout_seconds: int = 10
    cpu_seconds: int = 5
    memory_bytes: int = 256 * 1024 * 1024
    output_bytes: int = 1024 * 1024
    process_count: int = 8

    def __post_init__(self) -> None:
        if (
            min(
                self.timeout_seconds,
                self.cpu_seconds,
                self.memory_bytes,
                self.output_bytes,
                self.process_count,
            )
            <= 0
        ):
            raise ProbeError("sandbox limits must be positive")


class SandboxBackend(Protocol):
    name: str
    process_limit_in_sandbox: bool
    descendant_exit_status: int | None

    def wrap(
        self,
        command: Sequence[str],
        *,
        candidate_root: Path,
        input_file: Path,
        output_root: Path,
        environment: Mapping[str, str],
        limits: SandboxLimits,
    ) -> tuple[list[str], Mapping[str, Any]]:
        raise NotImplementedError


def _sandbox_string(value: Path | str) -> str:
    return json.dumps(str(value))


def _script_interpreter(executable: Path) -> Path | None:
    """Return an absolute, exact shebang interpreter when one is declared."""

    try:
        first_line = executable.open("rb").readline(4096)
    except OSError:
        return None
    if not first_line.startswith(b"#!"):
        return None
    try:
        fields = shlex.split(first_line[2:].decode("utf-8").strip())
    except (UnicodeDecodeError, ValueError):
        return None
    if not fields:
        return None
    interpreter = Path(fields[0])
    return interpreter if interpreter.is_absolute() else None


def _executable_variants(executable: Path) -> tuple[Path, ...]:
    """Keep every executable symlink hop and the resolved target visible."""

    variants: list[Path] = []
    current = Path(os.path.abspath(executable))
    seen: set[Path] = set()
    while current.is_symlink():
        if current in seen:
            raise ProbeError("candidate probe executable contains a symlink cycle")
        seen.add(current)
        variants.append(current)
        target = Path(os.readlink(current))
        if not target.is_absolute():
            target = current.parent / target
        current = Path(os.path.abspath(target))
    variants.append(current)
    variants.append(executable.resolve())
    return tuple(dict.fromkeys(variants))


class MacSandboxBackend:
    name = "macos-sandbox-exec-deny-all-v1"
    process_limit_in_sandbox = False
    descendant_exit_status = None

    def __init__(self, executable: str = "/usr/bin/sandbox-exec") -> None:
        if not Path(executable).is_file():
            raise ProbeError("sandbox-exec is unavailable; candidate probe refused")
        self.executable = executable

    def wrap(
        self,
        command: Sequence[str],
        *,
        candidate_root: Path,
        input_file: Path,
        output_root: Path,
        environment: Mapping[str, str],
        limits: SandboxLimits,
    ) -> tuple[list[str], Mapping[str, Any]]:
        requested_executable = Path(command[0])
        executable = requested_executable.resolve()
        declared_interpreter = _script_interpreter(executable)
        runtime_executables = [requested_executable, executable]
        if declared_interpreter is not None:
            runtime_executables.extend(
                (declared_interpreter, declared_interpreter.resolve())
            )
        system_roots = tuple(
            Path(root) for root in ("/System", "/usr", "/bin", "/Library")
        )
        runtime_roots: list[Path] = []
        for runtime_executable in runtime_executables:
            if any(runtime_executable.is_relative_to(root) for root in system_roots):
                continue
            runtime_root = (
                runtime_executable.parent.parent
                if runtime_executable.parent.name == "bin"
                else runtime_executable.parent
            )
            if runtime_root not in runtime_roots:
                runtime_roots.append(runtime_root)
        process_literals = " ".join(
            f"(literal {_sandbox_string(path)})"
            for path in sorted(
                set(runtime_executables), key=str
            )
        )
        runtime_reads = " ".join(
            f"(subpath {_sandbox_string(path)})" for path in runtime_roots
        )
        profile = "\n".join(
            (
                "(version 1)",
                "(deny default)",
                "(deny process-fork)",
                f"(allow process-exec {process_literals})",
                "(allow sysctl-read)",
                "(allow file-read-metadata)",
                '(allow file-read* (subpath "/System") (subpath "/usr") '
                '(subpath "/bin") (subpath "/Library") '
                '(literal "/") (literal "/dev/null") '
                f"(subpath {_sandbox_string(candidate_root)}) "
                f"(literal {_sandbox_string(input_file)}) "
                f"(subpath {_sandbox_string(output_root)}) {runtime_reads})",
                f"(allow file-write* (subpath {_sandbox_string(output_root)}))",
                "(deny network*)",
            )
        )
        metadata = {
            "backend": self.name,
            "executable": self.executable,
            "profile": profile,
            "probe_executable": str(executable),
            "declared_interpreter": (
                str(declared_interpreter)
                if declared_interpreter is not None
                else None
            ),
            "runtime_read_roots": [str(path) for path in runtime_roots],
            "process_isolation": "fork-denied",
        }
        return [self.executable, "-p", profile, "--", *command], metadata


class BubblewrapSandboxBackend:
    name = "linux-bubblewrap-deny-all-v1"
    process_limit_in_sandbox = True
    descendant_exit_status = _DESCENDANT_EXIT_STATUS

    def __init__(self, executable: str | None = None) -> None:
        self.executable = executable or shutil.which("bwrap") or ""
        if not self.executable:
            raise ProbeError("bubblewrap is unavailable; candidate probe refused")

    def wrap(
        self,
        command: Sequence[str],
        *,
        candidate_root: Path,
        input_file: Path,
        output_root: Path,
        environment: Mapping[str, str],
        limits: SandboxLimits,
    ) -> tuple[list[str], Mapping[str, Any]]:
        requested_executable = Path(command[0])
        executable = requested_executable.resolve()
        declared_interpreter = _script_interpreter(executable)
        runtime_executables = list(_executable_variants(requested_executable))
        if declared_interpreter is not None:
            runtime_executables.extend(_executable_variants(declared_interpreter))
        system_roots = tuple(Path(root) for root in ("/usr", "/bin", "/lib", "/lib64"))
        runtime_roots: list[Path] = []
        for runtime_executable in runtime_executables:
            if any(runtime_executable.is_relative_to(root) for root in system_roots):
                continue
            runtime_root = (
                runtime_executable.parent.parent
                if runtime_executable.parent.name == "bin"
                else runtime_executable.parent
            )
            if runtime_root not in runtime_roots:
                runtime_roots.append(runtime_root)
        args = [
            self.executable,
            "--die-with-parent",
            "--unshare-all",
            "--tmpfs",
            "/",
            "--clearenv",
        ]
        for name, value in sorted(environment.items()):
            args.extend(("--setenv", name, value))
        args.extend(("--proc", "/proc", "--dev", "/dev"))
        for system_root in ("/usr", "/bin", "/lib", "/lib64"):
            if Path(system_root).exists():
                args.extend(("--ro-bind", system_root, system_root))
        for runtime_root in runtime_roots:
            args.extend(("--ro-bind", str(runtime_root), str(runtime_root)))
        args.extend(
            (
                "--ro-bind",
                str(candidate_root),
                str(candidate_root),
                "--ro-bind",
                str(input_file),
                str(input_file),
                "--bind",
                str(output_root),
                str(output_root),
                "--remount-ro",
                "/",
                "--chdir",
                str(output_root),
                "--",
                "/bin/sh",
                "-c",
                (
                    'ulimit -u "$1"; shift; /usr/bin/env -i "$@"; status=$?; '
                    "for process in /proc/[0-9]*; do "
                    'pid=${process##*/}; if [ "$pid" != 1 ] && [ "$pid" != "$$" ]; '
                    f'then exit {_DESCENDANT_EXIT_STATUS}; fi; done; exit "$status"'
                ),
                "okg-probe-process-limit",
                str(limits.process_count),
                *(f"{name}={value}" for name, value in sorted(environment.items())),
                *command,
            )
        )
        return args, {
            "backend": self.name,
            "executable": self.executable,
            "probe_executable": str(executable),
            "declared_interpreter": (
                str(declared_interpreter)
                if declared_interpreter is not None
                else None
            ),
            "runtime_read_roots": [str(path) for path in runtime_roots],
            "process_isolation": "launcher-session-plus-pid-namespace-supervisor",
            "process_limit_enforcement": "candidate-namespace-rlimit-nproc",
            "reserved_exit_statuses": {
                str(_DESCENDANT_EXIT_STATUS): "descendant-processes"
            },
        }


def default_backend() -> SandboxBackend:
    system = platform.system()
    if system == "Darwin":
        return MacSandboxBackend()
    if system == "Linux":
        return BubblewrapSandboxBackend()
    raise ProbeError(
        f"unsupported sandbox platform {system!r}; candidate probe refused"
    )


def _tree_digest(path: Path) -> str:
    path = path.resolve()
    if path.is_file():
        return digest_bytes(path.read_bytes())
    if not path.is_dir():
        raise ProbeError(f"protected path is unavailable: {path}")
    entries: list[dict[str, Any]] = []
    for item in sorted(path.rglob("*")):
        relative = item.relative_to(path).as_posix()
        mode = stat.S_IMODE(item.lstat().st_mode)
        if item.is_symlink():
            entries.append({"path": relative, "mode": mode, "link": os.readlink(item)})
        elif item.is_file():
            entries.append(
                {
                    "path": relative,
                    "mode": mode,
                    "digest": digest_bytes(item.read_bytes()),
                }
            )
        elif item.is_dir():
            entries.append({"path": relative + "/", "mode": mode})
    return digest_json(entries)


def _combined_canary(paths: Sequence[Path]) -> str:
    return digest_json(
        [
            {"path": str(path.resolve()), "digest": _tree_digest(path)}
            for path in sorted(paths, key=lambda item: str(item.resolve()))
        ]
    )


def _limit_process(
    limits: SandboxLimits, *, include_process_count: bool = True
) -> None:
    def lower_limit(kind: int, requested: int) -> None:
        _soft, hard = resource.getrlimit(kind)
        bounded = requested if hard == resource.RLIM_INFINITY else min(requested, hard)
        resource.setrlimit(kind, (bounded, bounded))

    lower_limit(resource.RLIMIT_CPU, limits.cpu_seconds)
    # One sentinel byte lets the trusted reader distinguish a kernel-truncated
    # over-limit file from independently malformed JSON at the exact boundary.
    lower_limit(resource.RLIMIT_FSIZE, limits.output_bytes + 1)
    if include_process_count and hasattr(resource, "RLIMIT_NPROC"):
        lower_limit(resource.RLIMIT_NPROC, limits.process_count)


def _process_group_resident_bytes(process_group: int) -> int:
    if platform.system() == "Linux":
        resident_pages = 0
        found = False
        for stat_path in Path("/proc").glob("[0-9]*/stat"):
            try:
                fields = stat_path.read_text(encoding="utf-8").split()
                if int(fields[4]) != process_group:
                    continue
                statm = stat_path.with_name("statm").read_text(encoding="utf-8").split()
                resident_pages += int(statm[1])
                found = True
            except (FileNotFoundError, PermissionError, ValueError, IndexError):
                continue
        if not found:
            raise ProbeError("candidate process-group memory could not be observed")
        return resident_pages * os.sysconf("SC_PAGE_SIZE")

    if platform.system() == "Darwin":
        monitor = Path("/bin/ps")
        if not monitor.is_file():
            raise ProbeError("trusted process-group memory monitor is unavailable")
        observed = subprocess.run(
            [str(monitor), "-o", "rss=", "-g", str(process_group)],
            check=False,
            capture_output=True,
            text=True,
            env={"PATH": "/usr/bin:/bin"},
        )
        if observed.returncode != 0:
            raise ProbeError("candidate process-group memory could not be observed")
        try:
            rows = [int(value) for value in observed.stdout.split()]
        except ValueError as exc:
            raise ProbeError(
                "candidate process-group memory observation was malformed"
            ) from exc
        if not rows:
            raise ProbeError("candidate process-group memory could not be observed")
        return sum(rows) * 1024

    raise ProbeError("candidate process-group memory monitoring is unsupported")


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _process_group_has_executable_members(process_group: int) -> bool:
    """Return whether a group still contains a process that can execute.

    ``killpg(group, 0)`` also observes zombies.  A sandbox wrapper's orphaned
    child can remain a zombie until the platform reaper is scheduled, but it
    cannot execute, retain authority, or mutate probe output.  Cleanup must
    still fail closed for every non-zombie member.
    """

    system = platform.system()
    if system == "Darwin":
        monitor = Path("/bin/ps")
        if not monitor.is_file():
            raise ProbeError("candidate process-group state monitor is unavailable")
        observed = subprocess.run(
            [str(monitor), "-o", "stat=", "-g", str(process_group)],
            check=False,
            capture_output=True,
            text=True,
            env={"PATH": "/usr/bin:/bin"},
        )
        states = observed.stdout.split()
        if observed.returncode != 0 and _process_group_exists(process_group):
            raise ProbeError("candidate process-group state could not be observed")
        if not states:
            if _process_group_exists(process_group):
                raise ProbeError("candidate process-group state could not be observed")
            return False
        if any(
            state[0] not in {"D", "I", "R", "S", "T", "U", "Z"}
            for state in states
        ):
            raise ProbeError("candidate process-group state observation was malformed")
        return any(not state.startswith("Z") for state in states)

    if system == "Linux":
        proc = Path("/proc")
        if not proc.is_dir():
            raise ProbeError("candidate process-group state monitor is unavailable")
        try:
            entries = tuple(proc.iterdir())
        except OSError as exc:
            raise ProbeError(
                "candidate process-group state could not be observed"
            ) from exc
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            try:
                raw = (entry / "stat").read_text(encoding="utf-8")
                fields = raw[raw.rindex(")") + 2 :].split()
                state = fields[0]
                member_group = int(fields[2])
            except FileNotFoundError:
                continue
            except PermissionError as exc:
                raise ProbeError(
                    "candidate process-group state could not be observed"
                ) from exc
            except (ValueError, IndexError) as exc:
                raise ProbeError(
                    "candidate process-group state observation was malformed"
                ) from exc
            if state not in {
                "D",
                "I",
                "K",
                "P",
                "R",
                "S",
                "T",
                "W",
                "X",
                "Z",
                "t",
                "x",
            }:
                raise ProbeError(
                    "candidate process-group state observation was malformed"
                )
            if member_group == process_group and state not in {"X", "Z"}:
                return True
        return False

    raise ProbeError("candidate process-group state monitoring is unsupported")


def _kill_process_group(process_group: int) -> None:
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise ProbeError("candidate process group could not be terminated") from exc


def _drain_process_group(
    process_group: int, process: subprocess.Popen[Any], *, timeout: float = 1.0
) -> None:
    if process.poll() is not None and not _process_group_has_executable_members(
        process_group
    ):
        return
    _kill_process_group(process_group)
    with suppress(subprocess.TimeoutExpired):
        process.wait(timeout=timeout)
    deadline = time.monotonic() + timeout
    while _process_group_exists(process_group):
        if not _process_group_has_executable_members(process_group):
            return
        if time.monotonic() >= deadline:
            raise ProbeError("candidate process group could not be terminated")
        time.sleep(0.01)


def _bounded_output_tree_bytes(output_root: Path, limit: int) -> int:
    """Measure bytes plus entry names without following candidate symlinks."""

    total = 0
    pending = [output_root]
    while pending:
        directory = pending.pop()
        try:
            entries = os.scandir(directory)
        except OSError as exc:
            raise ProbeError("candidate probe output tree is not observable") from exc
        with entries:
            for entry in entries:
                total += len(os.fsencode(entry.name)) + 1
                if total > limit:
                    raise ProbeError(
                        "candidate probe exceeded its aggregate output limit"
                    )
                try:
                    if entry.is_symlink():
                        raise ProbeError(
                            "candidate probe output tree contains a symlink"
                        )
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False):
                        total += entry.stat(follow_symlinks=False).st_size
                    else:
                        raise ProbeError(
                            "candidate probe output tree contains a special file"
                        )
                except FileNotFoundError:
                    continue
                if total > limit:
                    raise ProbeError(
                        "candidate probe exceeded its aggregate output limit"
                    )
    return total


class TrustedProbeLauncher:
    """Launch and attest inert candidate output outside the candidate sandbox."""

    _ENV_ALLOWLIST = frozenset({"LANG", "LC_ALL", "TZ"})

    def __init__(
        self,
        *,
        signer: AuthoritySigner,
        backend: SandboxBackend | None = None,
        limits: SandboxLimits | None = None,
    ) -> None:
        self.signer = signer.signer_for("probe")
        self.backend = backend or default_backend()
        self.limits = limits or SandboxLimits()

    def launch(
        self,
        *,
        candidate_artifact: str | Path,
        command: Sequence[str],
        input_payload: Mapping[str, Any],
        protected_paths: Sequence[str | Path] = (),
        harmless_environment: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Any], ProbeAttestation]:
        candidate = Path(candidate_artifact).resolve()
        if not candidate.exists() or candidate.is_symlink():
            raise ProbeError("candidate artifact must be a real read-only path")
        if not command or not Path(command[0]).is_absolute():
            raise ProbeError("candidate probe command must use an absolute executable")
        candidate_digest = _tree_digest(candidate)
        protected = [Path(path).resolve() for path in protected_paths]
        canary_before = _combined_canary(protected)
        environment = dict(harmless_environment or {})
        forbidden = sorted(set(environment) - self._ENV_ALLOWLIST)
        if forbidden:
            raise ProbeError(f"probe environment contains forbidden names: {forbidden}")

        with tempfile.TemporaryDirectory(prefix="okg-controller-probe-") as scratch:
            scratch_path = Path(scratch).resolve()
            input_file = scratch_path / "input.json"
            output_root = scratch_path / "output"
            output_root.mkdir(mode=0o700)
            unsigned_input = {
                "schema": PROBE_INPUT_SCHEMA,
                "candidate_digest": candidate_digest,
                "payload": dict(input_payload),
            }
            envelope = {
                **unsigned_input,
                "signature": self.signer.sign(unsigned_input),
            }
            input_file.write_text(canonical_json(envelope), encoding="utf-8")
            input_file.chmod(0o400)
            process_environment = {
                **environment,
                "OKG_PROBE_INPUT": str(input_file),
                "OKG_PROBE_OUTPUT": str(output_root),
                "PYTHONCOERCECLOCALE": "0",
                "PYTHONDONTWRITEBYTECODE": "1",
            }
            wrapped, profile = self.backend.wrap(
                command,
                candidate_root=candidate,
                input_file=input_file,
                output_root=output_root,
                environment=process_environment,
                limits=self.limits,
            )
            profile_payload = {
                **dict(profile),
                "limits": asdict(self.limits),
                "memory_enforcement": "trusted-launcher-process-group-rss",
                "environment_names": sorted(environment),
            }
            sandbox_profile_digest = digest_json(profile_payload)
            started = time.monotonic()
            try:
                process = subprocess.Popen(
                    wrapped,
                    cwd=output_root,
                    env=process_environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    preexec_fn=lambda: _limit_process(
                        self.limits,
                        include_process_count=not self.backend.process_limit_in_sandbox,
                    ),
                    start_new_session=True,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise ProbeError("candidate sandbox process could not start") from exc
            process_group = process.pid
            try:
                while True:
                    returncode = process.poll()
                    group_exists = _process_group_exists(process_group)
                    if returncode is not None:
                        if group_exists and _process_group_has_executable_members(
                            process_group
                        ):
                            _drain_process_group(process_group, process)
                            raise ProbeError(
                                "candidate probe left descendant processes running"
                            )
                        break
                    if not group_exists:
                        process.wait()
                        break
                    if time.monotonic() - started > self.limits.timeout_seconds:
                        _drain_process_group(process_group, process)
                        raise ProbeError("candidate probe exceeded its time limit")
                    try:
                        resident_bytes = _process_group_resident_bytes(process_group)
                    except ProbeError:
                        if process.poll() is not None and not _process_group_exists(
                            process_group
                        ):
                            continue
                        _drain_process_group(process_group, process)
                        raise
                    if resident_bytes > self.limits.memory_bytes:
                        _drain_process_group(process_group, process)
                        raise ProbeError("candidate probe exceeded its memory limit")
                    try:
                        _bounded_output_tree_bytes(
                            output_root, self.limits.output_bytes
                        )
                    except ProbeError:
                        _drain_process_group(process_group, process)
                        raise
                    time.sleep(0.01)
            finally:
                if _process_group_exists(
                    process_group
                ) and _process_group_has_executable_members(process_group):
                    _drain_process_group(process_group, process)
                elif process.poll() is None:
                    process.wait(timeout=1)
            returncode = process.returncode
            if returncode is None:
                raise ProbeError("candidate probe completion was not observable")
            elapsed_millis = int((time.monotonic() - started) * 1000)
            canary_after = _combined_canary(protected)
            if canary_after != canary_before:
                raise ProbeError("candidate probe changed a protected mutation canary")
            if returncode == self.backend.descendant_exit_status:
                raise ProbeError("candidate probe left descendant processes running")
            if returncode != 0:
                raise ProbeError(
                    f"candidate probe refused with exit status {returncode}"
                )
            _bounded_output_tree_bytes(output_root, self.limits.output_bytes)
            result_file = output_root / "result.json"
            if not result_file.is_file() or result_file.is_symlink():
                raise ProbeError("candidate probe did not emit a regular result.json")
            result_bytes = result_file.read_bytes()
            if len(result_bytes) > self.limits.output_bytes:
                raise ProbeError("candidate probe output exceeded its size limit")
            try:
                output = json.loads(result_bytes)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ProbeError("candidate probe emitted malformed JSON") from exc
            if not isinstance(output, dict):
                raise ProbeError("candidate probe output must be one JSON object")
            if "signature" in output or "attestation" in output:
                raise ProbeError("candidate output cannot attest or sign itself")
            launcher_digest = digest_json(
                {
                    "module": digest_bytes(Path(__file__).read_bytes()),
                    "sandbox_executable": digest_bytes(
                        Path(wrapped[0]).resolve().read_bytes()
                    ),
                }
            )
            unsigned_attestation = {
                "schema": PROBE_ATTESTATION_SCHEMA,
                "nonce": secrets.token_hex(16),
                "launcher_key_id": self.signer.key_id,
                "issued_at": int(time.time()),
                "expires_at": int(time.time())
                + min(self.limits.timeout_seconds + 60, 300),
                "input_digest": digest_json(envelope),
                "input_payload_digest": digest_json(input_payload),
                "candidate_digest": candidate_digest,
                "sandbox_profile_digest": sandbox_profile_digest,
                "output_digest": digest_bytes(result_bytes),
                "output_payload_digest": digest_json(output),
                "launcher_digest": launcher_digest,
                "limits_digest": digest_json(asdict(self.limits)),
                "exit_status": returncode,
                "elapsed_millis": elapsed_millis,
                "cpu_limit_seconds": self.limits.cpu_seconds,
                "memory_limit_bytes": self.limits.memory_bytes,
                "output_limit_bytes": self.limits.output_bytes,
                "mutation_canary_before": canary_before,
                "mutation_canary_after": canary_after,
            }
            unsigned_attestation["attestation_id"] = digest_json(
                {
                    "nonce": unsigned_attestation["nonce"],
                    "launcher_key_id": unsigned_attestation["launcher_key_id"],
                    "candidate_digest": unsigned_attestation["candidate_digest"],
                    "output_payload_digest": unsigned_attestation[
                        "output_payload_digest"
                    ],
                }
            )
            attestation = ProbeAttestation(
                **unsigned_attestation,
                signature=self.signer.sign(unsigned_attestation),
            )
            return output, attestation
