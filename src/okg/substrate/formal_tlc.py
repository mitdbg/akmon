"""Shared TLC runner support for formal checker scripts."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import yaml


TLC_TIMEOUT_EXIT_CODE = 124

# Generic vacuity floor: a passing TLC run must have explored at least this
# many distinct states unless the checker declares a higher, measured floor.
# A constraint-collapsed model (or a config typo that empties the state space)
# must fail closed rather than pass as freshly_proven.
DEFAULT_MIN_DISTINCT_STATES = 2

# Proof-strength discriminator carried on every formal report row. A consumer
# can require a minimum proof_kind so an executable-model floor check or a
# report-schema fixture cannot be read as a real TLC fixpoint proof.
#   tlc_fixpoint    -- TLC explored the state space to a fixpoint (real proof)
#   executable_model-- a Python executable model asserted properties over an
#                      enumerated input space (weaker than a TLC fixpoint)
#   report_schema   -- a row produced only to validate report shape (no proof)
PROOF_KIND_TLC_FIXPOINT = "tlc_fixpoint"
PROOF_KIND_EXECUTABLE_MODEL = "executable_model"
PROOF_KIND_REPORT_SCHEMA = "report_schema"
PROOF_KIND_RANK = {
    PROOF_KIND_REPORT_SCHEMA: 0,
    PROOF_KIND_EXECUTABLE_MODEL: 1,
    PROOF_KIND_TLC_FIXPOINT: 2,
}

# --- TLC resource profile -------------------------------------------------
#
# Left to itself TLC takes the whole machine: `-workers auto` runs one worker
# per core, and with no -Xmx the JVM's default max heap (1/4 of physical RAM)
# makes TLC size its fingerprint set to match. Measured on a 12-core/96 GB
# host, formal/nomos NomosIamClaims.cfg peaks at 5.5 GB RSS while pinning ~6.8
# cores. A suite run is ~100 such JVM launches, so the untuned defaults
# saturate every core and evict the page cache for the whole run, which is
# what makes an interactive machine crawl.
#
# The whole profile is therefore INTERACTIVE-ONLY: when CI is set, every knob
# resolves to exactly what it was before this existed. The machine-usability
# problem only exists on a developer's host, and the gating `formal-full` lane
# runs profiles far larger than anything measured here (admission's full
# profile floors at 22.3M distinct states against nomos's 380K), so there is
# no measurement backing a cap on that lane and we do not impose one.
DEFAULT_TLC_HEAP = "4g"
INTERACTIVE_WORKER_DIVISOR = 3


def _in_ci() -> bool:
    return os.environ.get("CI", "").strip().lower() not in {"", "0", "false", "no"}


def tlc_workers() -> str:
    """Resolve TLC's ``-workers`` value.

    ``OKG_TLC_WORKERS`` wins when set (``auto`` restores one worker per core).
    Otherwise CI gets ``auto`` and an interactive machine gets a third of the
    cores, so a suite run leaves the box usable.
    """
    explicit = os.environ.get("OKG_TLC_WORKERS", "").strip()
    if explicit:
        return explicit
    if _in_ci():
        return "auto"
    return str(max(1, (os.cpu_count() or 1) // INTERACTIVE_WORKER_DIVISOR))


def tlc_jvm_args(workers: str) -> tuple[str, ...]:
    """JVM flags that bound what one TLC process takes from the machine.

    ``OKG_TLC_HEAP`` overrides the max-heap cap; ``off`` restores the JVM
    default. Under CI the cap is off unless explicitly requested, so the
    scheduled full-corpus lane keeps the heap headroom it was sized against.
    Requires JDK 8u191+ for ``-XX:ActiveProcessorCount``; older JVMs refuse to
    start rather than ignoring it, so set ``OKG_TLC_WORKERS=auto`` there.
    """
    args: list[str] = []
    explicit_heap = os.environ.get("OKG_TLC_HEAP", "").strip()
    if not explicit_heap and _in_ci():
        heap = "off"
    else:
        heap = explicit_heap or DEFAULT_TLC_HEAP
    if heap.lower() != "off":
        args.append(f"-Xmx{heap}")
    # -workers bounds only TLC's state-exploration threads. Without this the
    # JVM still sizes its GC and JIT compiler pools from the full core count,
    # so a 4-worker run keeps every core busy anyway.
    if workers.isdigit():
        args.append(f"-XX:ActiveProcessorCount={workers}")
    return tuple(args)


def proof_kind_satisfies(have: str | None, required: str | None) -> bool:
    """True when ``have`` is at least as strong as ``required``.

    Unknown proof kinds rank below everything, so an unrecognized or missing
    proof_kind never satisfies a tlc_fixpoint/executable_model requirement.
    """
    if not required:
        return True
    return PROOF_KIND_RANK.get(have or "", -1) >= PROOF_KIND_RANK.get(required, 99)


class TLCSetupError(RuntimeError):
    """A fail-loud setup error before TLC can produce useful evidence."""

    def __init__(self, message: str, *, exit_code: int = 2) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class TLCReportError(RuntimeError):
    """The runner could not build a strict formal report."""


@dataclass(frozen=True)
class TLCProperty:
    property_id: str
    summary: str
    refinement_tests: tuple[str, ...] = ()
    expected_invariants: tuple[str, ...] = ()
    expected_temporal_properties: tuple[str, ...] = ()

    def __iter__(self):
        # Preserve compatibility with existing tests that unpack PROPERTIES
        # as ``property_id, summary`` tuples.
        yield self.property_id
        yield self.summary

    @property
    def invariants(self) -> tuple[str, ...]:
        if self.expected_invariants:
            return self.expected_invariants
        if self.expected_temporal_properties:
            return ()
        return (self.property_id,)

    @property
    def temporal_properties(self) -> tuple[str, ...]:
        return self.expected_temporal_properties


@dataclass(frozen=True)
class TLCExecutionProfile:
    name: str
    description: str
    config: Path
    out_dir: Path
    timeout_seconds: int
    property_ids: tuple[str, ...]
    expected_property_ids: tuple[str, ...] = ()
    full_strength: bool = False
    ci_smoke: bool = False
    # Measured vacuity floor for this profile's state space; a passing run
    # exploring fewer distinct states fails closed (vacuous_state_space).
    min_distinct_states: int | None = None

    def as_dict(self, *, repo_root: Path | None = None) -> dict:
        def display(path: Path) -> str:
            if repo_root is None:
                return str(path)
            try:
                return str(path.resolve().relative_to(repo_root.resolve()))
            except ValueError:
                return str(path)

        return {
            "name": self.name,
            "description": self.description,
            "config": display(self.config),
            "out": display(self.out_dir),
            "timeout_seconds": self.timeout_seconds,
            "property_ids": list(self.property_ids),
            "expected_property_ids": list(
                self.expected_property_ids or self.property_ids
            ),
            "full_strength": self.full_strength,
            "ci_smoke": self.ci_smoke,
            "min_distinct_states": self.min_distinct_states,
        }


@dataclass(frozen=True)
class TLCReportResult:
    report_path: Path
    stdout_path: Path
    stderr_path: Path
    command: tuple[str, ...]
    rows: tuple[dict, ...]
    exit_code: int
    run_failed: bool
    timed_out: bool
    invariant_violated: bool
    missing_invariants: tuple[str, ...] = ()
    violated_invariants: tuple[str, ...] = ()
    unexpected_errors: tuple[str, ...] = ()
    distinct_states: int | None = None


def run_tlc_formal_report(
    *,
    repo_root: Path,
    model_dir: Path,
    model: Path,
    config: Path,
    out_dir: Path,
    properties: Sequence[TLCProperty],
    checker_command: str,
    expected_properties: Sequence[TLCProperty] | None = None,
    extra_refinement_tests: Sequence[str] = (),
    expect_counterexample: bool = False,
    timeout_seconds: int = 300,
    include_checker_output: bool = False,
    write_report: bool = True,
    execution_profile: str | None = None,
    profile_timeout_seconds: int | None = None,
    min_distinct_states: int | None = None,
) -> TLCReportResult:
    jar = find_tla2tools_jar(repo_root)
    if jar is None:
        raise TLCSetupError(
            "TLC is required but no tla2tools.jar was found. "
            "Set TLA2TOOLS_JAR=/absolute/path/to/tla2tools.jar, or run "
            "`uv run python scripts/bootstrap_formal_admission.py` to install "
            "the pinned jar at .tools/tla2tools.jar."
        )
    java = shutil.which("java")
    if java is None:
        raise TLCSetupError("Java is required to run TLC but was not found on PATH.")

    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = resolve_model_path(model_dir, model)
    config_path = resolve_model_path(model_dir, config)
    if not model_path.is_file():
        raise TLCSetupError(f"TLA+ model file not found: {model_path}")
    if not config_path.is_file():
        raise TLCSetupError(f"TLC config file not found: {config_path}")

    stdout_path = out_dir / "tlc.stdout.log"
    stderr_path = out_dir / "tlc.stderr.log"
    report_path = out_dir / "formal-report.yaml"
    metadir = out_dir / "tlc-meta"
    metadir.mkdir(parents=True, exist_ok=True)
    workers = tlc_workers()
    cmd = (
        java,
        *tlc_jvm_args(workers),
        "-cp",
        str(jar),
        "tlc2.TLC",
        "-config",
        str(config_path),
        "-deadlock",
        "-workers",
        workers,
        "-metadir",
        str(metadir),
        str(model_path),
    )

    missing_invariants = missing_expected_obligations(
        config_path,
        expected_properties or properties,
    )
    if missing_invariants:
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text(
            "TLC config omitted expected invariants/properties: "
            f"{', '.join(missing_invariants)}\n",
            encoding="utf-8",
        )
        rows = build_formal_report_rows(
            properties=properties,
            status="fail",
            checker_command=checker_command,
            command=cmd,
            artifact=stdout_path,
            model_path=model_path,
            config_path=config_path,
            extra_refinement_tests=extra_refinement_tests,
            summary_override=(
                "TLC config omitted expected invariants/properties: "
                f"{', '.join(missing_invariants)}"
            ),
            exit_code=1,
            timed_out=False,
            failure_class="missing_obligation",
            proof_status="not_freshly_proven",
            execution_profile=execution_profile,
            profile_timeout_seconds=profile_timeout_seconds,
            checker_output=stderr_path.read_text(encoding="utf-8")
            if include_checker_output
            else None,
        )
        if write_report:
            _write_report(report_path, rows)
        return TLCReportResult(
            report_path=report_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            command=cmd,
            rows=tuple(rows),
            exit_code=1,
            run_failed=True,
            timed_out=False,
            invariant_violated=False,
            missing_invariants=missing_invariants,
        )

    timed_out = False
    try:
        proc = subprocess.run(
            list(cmd),
            cwd=str(model_dir),
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return_code = proc.returncode
        stdout = proc.stdout
        stderr = proc.stderr
    except subprocess.TimeoutExpired as exc:
        timed_out = True
        return_code = TLC_TIMEOUT_EXIT_CODE
        stdout = _timeout_output_to_text(exc.stdout)
        stderr = _timeout_output_to_text(exc.stderr)
        stderr = (
            f"{stderr}\n" if stderr else ""
        ) + f"TLC timed out after {timeout_seconds} seconds.\n"

    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")
    if return_code != 0 and not timed_out and not expect_counterexample:
        _echo_tlc_failure_tail(return_code, stdout, stderr)

    violated_invariants = _tlc_violated_invariants(stdout)
    invariant_violated = bool(violated_invariants) or _tlc_invariant_violated(stdout)
    if timed_out and invariant_violated and not expect_counterexample:
        # A violation TLC already printed is a semantic failure, not "not
        # proven in time" -- the kill landed while it was dumping the error
        # trace, after the answer was known. Without this demotion the
        # classification below gives `timed_out` precedence and reports
        # `failure_class="timeout"` with exit code 124, which is exactly the
        # shape run_formal_suite.py's bounded not-proven bucket accepts. A
        # real invariant violation could then be recorded as an untimed proof
        # gap and stop reding its lane. Moving off TLC_TIMEOUT_EXIT_CODE as
        # well means no consumer keyed on the timeout code can mistake it
        # either. Counterexample runs are excluded: there a violation is the
        # expected result, and demoting the timeout would let a half-finished
        # witness run report success.
        timed_out = False
        return_code = 1
    tlc_failed = timed_out or return_code != 0 or invariant_violated
    distinct_states = _tlc_distinct_states(stdout)
    states_floor = (
        min_distinct_states
        if min_distinct_states is not None
        else DEFAULT_MIN_DISTINCT_STATES
    )
    # Vacuity floor applies only to fixpoint runs: a counterexample run halts
    # at the first violation, so its state count says nothing about coverage.
    below_states_floor = (
        not expect_counterexample
        and not tlc_failed
        and (distinct_states is None or distinct_states < states_floor)
    )
    if expect_counterexample:
        expected_counterexample_invariants = {
            invariant
            for prop in properties
            for invariant in prop.invariants
        }
        unexpected_errors = _tlc_unexpected_counterexample_error_lines(
            stdout,
            stderr,
            expected_counterexample_invariants,
        )
        if expected_counterexample_invariants:
            expected_counterexample_seen = expected_counterexample_invariants <= set(
                violated_invariants
            )
        else:
            expected_counterexample_seen = invariant_violated
        run_failed = (
            timed_out
            or not expected_counterexample_seen
            or bool(unexpected_errors)
        )
        summary_if_ok = "Counter-example produced as expected."
        if timed_out:
            summary_if_fail = "TLC timed out."
        elif not expected_counterexample_seen:
            summary_if_fail = (
                "Expected counter-example invariant(s) did NOT appear: "
                + ", ".join(sorted(expected_counterexample_invariants))
                + (
                    f"; saw: {', '.join(violated_invariants)}"
                    if violated_invariants
                    else ""
                )
            )
        else:
            summary_if_fail = (
                "Unexpected TLC error(s) appeared with counter-example output: "
                + "; ".join(unexpected_errors[:3])
            )
    else:
        unexpected_errors = ()
        run_failed = tlc_failed or below_states_floor
        summary_if_ok = None
        if timed_out:
            summary_if_fail = (
                "TLC timed out; property not freshly proven within timeout."
            )
        elif below_states_floor:
            summary_if_fail = (
                "TLC explored "
                f"{distinct_states if distinct_states is not None else 'an unparseable number of'} "
                f"distinct states, below the vacuity floor of {states_floor}; "
                "the state space is too small for the pass to mean anything."
            )
        else:
            summary_if_fail = "TLC failed."

    failure_class = None
    proof_status = "freshly_proven"
    if run_failed:
        if timed_out:
            failure_class = "timeout"
            proof_status = "not_freshly_proven"
            summary_if_fail = "TLC timed out; property not freshly proven within timeout."
        elif invariant_violated:
            failure_class = "invariant_violation"
            proof_status = "semantic_failure"
        elif below_states_floor:
            failure_class = "vacuous_state_space"
            proof_status = "not_freshly_proven"
        else:
            failure_class = "tlc_error"
            proof_status = "not_freshly_proven"

    rows = build_formal_report_rows(
        properties=properties,
        status="fail" if run_failed else "pass",
        checker_command=checker_command,
        command=cmd,
        artifact=stdout_path,
        model_path=model_path,
        config_path=config_path,
        extra_refinement_tests=extra_refinement_tests,
        summary_override=summary_if_fail if run_failed else summary_if_ok,
        exit_code=(return_code or 1) if run_failed else 0,
        timed_out=timed_out,
        failure_class=failure_class,
        proof_status=proof_status,
        execution_profile=execution_profile,
        profile_timeout_seconds=profile_timeout_seconds,
        checker_output=_tail(stdout) if include_checker_output else None,
        distinct_states=distinct_states,
    )
    if write_report:
        _write_report(report_path, rows)
    return TLCReportResult(
        report_path=report_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        command=cmd,
        rows=tuple(rows),
        exit_code=(return_code or 1) if run_failed else 0,
        run_failed=run_failed,
        timed_out=timed_out,
        invariant_violated=invariant_violated,
        violated_invariants=violated_invariants,
        unexpected_errors=unexpected_errors,
        distinct_states=distinct_states,
    )


def build_formal_report_rows(
    *,
    properties: Sequence[TLCProperty],
    status: str,
    checker_command: str,
    command: Sequence[str],
    artifact: Path,
    model_path: Path,
    config_path: Path,
    extra_refinement_tests: Sequence[str] = (),
    summary_override: str | None = None,
    checker_output: str | None = None,
    exit_code: int | None = None,
    timed_out: bool = False,
    failure_class: str | None = None,
    proof_status: str | None = None,
    execution_profile: str | None = None,
    profile_timeout_seconds: int | None = None,
    proof_kind: str = PROOF_KIND_TLC_FIXPOINT,
    distinct_states: int | None = None,
) -> list[dict]:
    if not artifact.is_file():
        raise TLCReportError(f"formal report artifact does not exist: {artifact}")
    command_text = " ".join(command)
    rows = []
    for prop in properties:
        row = {
            "property_id": prop.property_id,
            "kind": "formal_check",
            "status": status,
            "fresh": status == "pass",
            "checker_command": checker_command,
            "command": command_text,
            "artifact": str(artifact),
            "refinement_tests": [
                *prop.refinement_tests,
                *extra_refinement_tests,
            ],
            "module_digest": path_digest(model_path),
            "config_digest": path_digest(config_path),
            "summary": summary_override or prop.summary,
            "proof_status": proof_status
            or ("freshly_proven" if status == "pass" else "not_freshly_proven"),
            "proof_kind": proof_kind,
            "timed_out": timed_out,
        }
        if exit_code is not None:
            row["exit_code"] = exit_code
        if failure_class is not None:
            row["failure_class"] = failure_class
        if execution_profile is not None:
            row["execution_profile"] = execution_profile
        if profile_timeout_seconds is not None:
            row["profile_timeout_seconds"] = profile_timeout_seconds
        if checker_output is not None:
            row["checker_output"] = checker_output
        if distinct_states is not None:
            row["distinct_states"] = distinct_states
        rows.append(row)
    return rows


def select_properties(
    properties: Sequence[TLCProperty],
    selected_ids: Sequence[str] | None,
) -> list[TLCProperty]:
    if not selected_ids:
        return list(properties)
    by_id = {prop.property_id: prop for prop in properties}
    unknown = sorted(set(selected_ids) - set(by_id))
    if unknown:
        raise TLCSetupError(f"unknown formal property id(s): {', '.join(unknown)}")
    return [by_id[property_id] for property_id in selected_ids]


def resolve_model_path(model_dir: Path, path: Path) -> Path:
    if path.is_absolute():
        return path.resolve()
    return (model_dir / path).resolve()


def find_tla2tools_jar(repo_root: Path) -> Path | None:
    env = os.environ.get("TLA2TOOLS_JAR")
    candidates = []
    if env:
        candidates.append(Path(env).expanduser())
    candidates.extend([
        repo_root / ".tools" / "tla2tools.jar",
        repo_root / "tools" / "tla2tools.jar",
    ])
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    return None


def path_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def config_invariants(config_path: Path) -> set[str]:
    return _config_names(config_path, {"INVARIANT", "INVARIANTS"})


def config_temporal_properties(config_path: Path) -> set[str]:
    return _config_names(config_path, {"PROPERTY", "PROPERTIES"})


def _config_names(config_path: Path, section_directives: set[str]) -> set[str]:
    names: set[str] = set()
    in_block = False
    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("\\*", 1)[0].strip()
        if not line:
            continue
        parts = line.split()
        directive = parts[0]
        if directive in section_directives:
            in_block = True
            names.update(part.rstrip(",") for part in parts[1:])
            continue
        if directive in _TLC_CONFIG_DIRECTIVES:
            in_block = False
        if not in_block:
            continue
        names.add(directive.rstrip(","))
    return names


def missing_expected_invariants(
    config_path: Path,
    properties: Iterable[TLCProperty],
) -> tuple[str, ...]:
    configured = config_invariants(config_path)
    expected = {
        invariant
        for prop in properties
        for invariant in prop.invariants
    }
    return tuple(sorted(expected - configured))


def missing_expected_obligations(
    config_path: Path,
    properties: Iterable[TLCProperty],
) -> tuple[str, ...]:
    properties = tuple(properties)
    missing_invariants = missing_expected_invariants(config_path, properties)
    configured_temporal = config_temporal_properties(config_path)
    expected_temporal = {
        temporal_property
        for prop in properties
        for temporal_property in prop.temporal_properties
    }
    missing_temporal = tuple(
        f"PROPERTY {name}" for name in sorted(expected_temporal - configured_temporal)
    )
    return (*missing_invariants, *missing_temporal)


def _write_report(report_path: Path, rows: Sequence[dict]) -> None:
    report_path.write_text(
        yaml.safe_dump({"results": list(rows)}, sort_keys=False),
        encoding="utf-8",
    )


# TLC names a violated invariant two ways: "Invariant X is violated" for a
# reachable state, and "The invariant of X is equal to FALSE" when X is a
# constant-level formula (no state variables) that fails before the search.
_VIOLATED_INVARIANT = re.compile(
    r"\bInvariant\s+([A-Za-z_][A-Za-z0-9_!]*)\s+is violated\b"
    r"|\bThe invariant of\s+([A-Za-z_][A-Za-z0-9_!]*)\s+is equal to FALSE\b"
)


def _tlc_invariant_violated(stdout: str) -> bool:
    return "Error: Invariant" in stdout or (
        "Invariant" in stdout and "is violated" in stdout
    ) or _VIOLATED_INVARIANT.search(stdout) is not None


def _tlc_distinct_states(stdout: str) -> int | None:
    """Parse TLC's distinct-states count (e.g. '2797 states generated, 641
    distinct states found, 0 states left on queue'). None when the line is
    absent, which a floor check must treat as fail-closed."""
    matches = re.findall(r"(\d+)\s+distinct states found", stdout)
    if not matches:
        return None
    return int(matches[-1])


def _tlc_violated_invariants(stdout: str) -> tuple[str, ...]:
    matches = [a or b for a, b in _VIOLATED_INVARIANT.findall(stdout)]
    return tuple(dict.fromkeys(matches))


def _tlc_unexpected_counterexample_error_lines(
    stdout: str,
    stderr: str,
    expected_invariants: set[str],
) -> tuple[str, ...]:
    unexpected = []
    for stream_name, text in (("stdout", stdout), ("stderr", stderr)):
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if _tlc_expected_counterexample_line(line, expected_invariants):
                continue
            if line.startswith("Error: The behavior up to this point is"):
                continue
            if (
                line.startswith("Error:")
                or line.startswith("TLC threw")
                or "TLC threw an unexpected exception" in line
                or "Parsing or semantic analysis failed" in line
                or line.startswith("Semantic errors:")
            ):
                unexpected.append(f"{stream_name}: {line}")
    return tuple(unexpected)


def _tlc_expected_counterexample_line(
    line: str,
    expected_invariants: set[str],
) -> bool:
    match = _VIOLATED_INVARIANT.search(line)
    if match is None:
        return False
    if match.group(2) is not None:
        # A constant-level failure counts only when a witness names it.
        return match.group(2) in expected_invariants
    return not expected_invariants or match.group(1) in expected_invariants


TLC_FAILURE_TAIL_LINES = 40


def _echo_tlc_failure_tail(return_code: int, stdout: str, stderr: str) -> None:
    """Print why TLC failed to this process's stderr, not only to files.

    The stdout/stderr files reach a reader only through an artifact upload,
    and a full artifact quota drops them: the 2026-09-25 nightly lost the
    only record of two TLC kills (exit -9). A negative return code is a
    signal, named here because TLC itself prints nothing when it is killed.
    Not called for an expected-counterexample run, where a non-zero exit is
    the passing result.
    """
    for stream_name, text in (("stdout", stdout), ("stderr", stderr)):
        tail = text.splitlines()[-TLC_FAILURE_TAIL_LINES:]
        if tail:
            print(f"--- last {len(tail)} lines of TLC {stream_name} ---", file=sys.stderr)
            print("\n".join(tail), file=sys.stderr)
    # Last, so it survives scripts/run_formal_suite.py keeping only the final
    # few lines of a failed checker's output.
    if return_code < 0:
        print(f"TLC was killed by signal {-return_code}.", file=sys.stderr)
    else:
        print(f"TLC exited with code {return_code}.", file=sys.stderr)


def _timeout_output_to_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _tail(text: str, *, lines: int = 20) -> str:
    return "\n".join(text.splitlines()[-lines:])


_TLC_CONFIG_DIRECTIVES = {
    "CHECK_DEADLOCK",
    "CONSTANT",
    "CONSTANTS",
    "CONSTRAINT",
    "CONSTRAINTS",
    "INIT",
    "INVARIANT",
    "INVARIANTS",
    "NEXT",
    "POSTCONDITION",
    "PROPERTY",
    "PROPERTIES",
    "SPECIFICATION",
    "SYMMETRY",
    "VIEW",
}
