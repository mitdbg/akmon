"""One-benchmark-tree witnesses (PACT ship-okg-bench, task.ship.retire-benchmarks).

Executable form of req.ship.one-tree: the benchmarks/ skeleton is gone, the
repo-side bench/ directory carries only lab state plus thin launchers (no
harness logic outside the okg.bench package), and no live code references
the retired module paths (historical records — openspec archive, pact
records — are exempt by construction: the greps below scan live code trees
only).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Live code trees the retired-path greps must come back clean from. The
# frozen synth world corpora are excluded the same way the historical
# artifacts below are: they are DATA (minified FHIR bundles), not live code
# that could import anything - and their multi-hundred-KB single-line JSON
# is quadratic for glibc's ERE engine under `^[^#]*`, which turned this
# module's scan from seconds into minutes on the linux fast lane.
_LIVE_TREES = (
    "src",
    "tests",
    "deployments",
    "scripts",
    ".github",
    "docs",
    ":(exclude)src/okg/bench/synth/world_*/*.json",
)


# Historical graph-report artifacts (recorded symbol ids from past
# generations) legitimately mention retired paths; they are state, not code.
_HISTORICAL = re.compile(r"(self_play/reports|workflow_mining)/.*\.json:")


def _grep(pattern: str, *, trees: tuple[str, ...] = _LIVE_TREES) -> list[str]:
    out = subprocess.run(
        ["git", "grep", "-nE", pattern, "--", *trees],
        capture_output=True,
        text=True,
        cwd=_REPO_ROOT,
    )
    return [
        line
        for line in out.stdout.splitlines()
        if line.strip() and not _HISTORICAL.search(line)
    ]


def test_benchmarks_skeleton_is_retired():
    assert not (_REPO_ROOT / "benchmarks").exists()
    hits = _grep(r"benchmarks/(matrix|run_matrix|README)")
    assert hits == [], hits


def test_no_live_imports_of_the_old_module_paths():
    # `import bench.` / `from bench.` was the pre-package namespace accident;
    # `from okgmcp import` was the sys.path-hack shape.
    hits = _grep(r"^[^#]*(from bench\.|import bench\.|from okgmcp import)")
    assert hits == [], hits


def test_repo_bench_dir_is_lab_state_plus_thin_launchers():
    bench_dir = _REPO_ROOT / "bench"
    python_files = sorted(
        path
        for path in bench_dir.rglob("*.py")
        if "__pycache__" not in path.parts
    )
    assert [p.relative_to(bench_dir).as_posix() for p in python_files] == [
        "orchestrator.py"
    ]
    launcher = (bench_dir / "orchestrator.py").read_text(encoding="utf-8")
    assert "from okg.bench.orchestrator import main" in launcher
    assert len(launcher.splitlines()) < 25, "launcher must stay thin"
    # The state that must remain repo-side.
    for state in ("manifest.yaml", "sweep.lock.json", "engine_sensitivity.yaml"):
        assert (bench_dir / state).is_file(), state


def test_orchestrator_subprocess_seams_use_module_invocations():
    """The perf lab shells out to its own helpers; after the migration those
    must be -m okg.bench.* invocations, never repo-path scripts (a dead
    script path skipped extraction SILENTLY pre-fix - review finding F1)."""
    orchestrator = (
        _REPO_ROOT / "src/okg/bench/orchestrator.py"
    ).read_text(encoding="utf-8")
    for dead in ('BENCH_DIR / "extract.py"', 'BENCH_DIR / "plot.py"',
                 'BENCH_DIR / "orchestrator.py"'):
        assert dead not in orchestrator, dead
    for module in ("okg.bench.extract", "okg.bench.plot", "okg.bench.orchestrator"):
        assert f'"-m", "{module}"' in orchestrator, module


def test_run_sh_launcher_uses_package_paths():
    run_sh = (_REPO_ROOT / "bench/agent_memory/run.sh").read_text(encoding="utf-8")
    assert "okg.bench.agent_memory" in run_sh
    assert not re.search(r"python bench/agent_memory/\w+\.py", run_sh)


def test_no_live_code_invokes_retired_bench_script_paths():
    """A bare "bench/agent_memory/<module>.py" string inside packaged code is
    a subprocess/hint seam the import-shape greps above cannot see — one such
    string in run_control.py survived the ship-okg-bench migration and made
    every live scored run fail while FakeBackend tests stayed green. Only
    run.sh (lab-state launcher, genuinely at that path) is a legal referent.

    TWO BUGS FIXED HERE, 2026-08-05, both of which made this test lie.

    `\\w` is a GNU regex extension. `git grep -E` on macOS uses the platform
    regex library, where it matches NOTHING — so this test was a silent no-op
    on every developer machine and fired only on Linux CI. A test that cannot
    fail where it is written is not a test. Now a POSIX class.

    And the pattern matched the LEGITIMATE current path as a substring:
    `src/okg/bench/agent_memory/spec.py` contains `bench/agent_memory/spec.py`.
    Once `\\w` started working, four correct references would have been
    reported as retired ones. The prefix is excluded explicitly, so what
    remains is exactly the bare form this test exists to catch.
    """
    hits = [
        line for line in _grep(
            r"bench/agent_memory/[A-Za-z0-9_]+\.py", trees=("src",)
        )
        if "src/okg/bench/agent_memory/" not in line.split(":", 2)[-1]
    ]
    assert hits == [], hits


def test_inspect_backend_resolves_runner_from_the_package():
    """The subprocess seam must go through inspect_runner_path() (resolved
    from the installed package), and that path must exist."""
    from okg.bench.agent_memory.run_control import inspect_runner_path

    runner = inspect_runner_path()
    assert runner.is_file()
    source = (
        _REPO_ROOT / "src/okg/bench/agent_memory/run_control.py"
    ).read_text(encoding="utf-8")
    assert 'str(inspect_runner_path())' in source
