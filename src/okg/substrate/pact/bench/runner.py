"""Orchestrates the six metric families from one frozen fixture corpus."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from okg.substrate.pact.bench import (
    catch_rate,
    episodes,
    fixtures,
    honesty,
    quality,
    refusals,
    report,
)
from okg.substrate.pact.bench.calls import Recorder
from okg.substrate.pact.bench.isolation import (
    FIXED_NOW,
    FIXED_STAMP,
    FixedClock,
    RuntimeControls,
    isolated_env,
    isolated_runtime,
    require_clean_environment,
)


@dataclass
class BenchResult:
    metrics: dict[str, Any]
    fixture_root: Path
    subprocess_calls: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return report.emit(self.metrics)

    def to_table(self) -> str:
        return report.human_table(self.metrics)


def repo_root_from(start: Path | None = None) -> Path | None:
    """Locate the live checkout, used only for the corpus waiver sweep."""
    here = (start or Path(__file__)).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "pact" / "changes").is_dir():
            return candidate
    return None


def _harness_contract() -> dict[str, Any]:
    from okg.substrate.pact.bench import commands as cmd
    from okg.substrate.pact.bench.isolation import (
        DENIED_ENV_EXACT,
        DENIED_ENV_PREFIXES,
    )

    return {
        "in_process": True,
        "requires_dsn": False,
        "fixed_clock": {"stamp": FIXED_STAMP, "now": FIXED_NOW},
        "denied_env": sorted(DENIED_ENV_EXACT),
        "denied_env_prefixes": list(DENIED_ENV_PREFIXES),
        "command_guards": {
            "max_chars": cmd.MAX_COMMAND_CHARS,
            "max_statement_separators": cmd.MAX_STATEMENT_SEPARATORS,
        },
        "ledger_arm": "unreachable (no DSN is offered to the harness)",
        # INSTRUMENT COVERAGE. `known_verbs` is read off the real argparse
        # tree, so a verb the CLI gains appears here as an uncovered gap
        # instead of being scored as a PACT defect the moment it is
        # recommended. Published because a blind spot the report does not name
        # is a blind spot the reader cannot discount.
        "instrument_coverage": {
            "known_verbs": sorted(cmd.known_verbs()),
            "executable_verbs": sorted(cmd.EXECUTABLE_VERBS),
            "unimplemented_verbs": list(cmd.unimplemented_verbs()),
            "executable_view_formats": sorted(cmd.EXECUTABLE_VIEW_FORMATS),
            "verb_coverage_fraction": round(
                len(cmd.EXECUTABLE_VERBS) / len(cmd.known_verbs()), 4
            )
            if cmd.known_verbs()
            else 0.0,
        },
    }


def run_benchmark(
    *,
    workdir: Path | None = None,
    repo_root: Path | None = None,
    enforce_env: bool = True,
) -> BenchResult:
    """Compute all six metric families. No DSN, no graph, no subprocess CLI."""
    if enforce_env:
        require_clean_environment()
    if workdir is None:
        with TemporaryDirectory(prefix="pact-bench-") as tmp:
            return _run_in(Path(tmp), repo_root)
    workdir.mkdir(parents=True, exist_ok=True)
    return _run_in(workdir, repo_root)


def _run_in(workdir: Path, repo_root: Path | None) -> BenchResult:
    home = workdir / "home"
    home.mkdir(parents=True, exist_ok=True)
    root = workdir / "repo"
    sweep_root = repo_root if repo_root is not None else repo_root_from()

    with isolated_env(home):
        repo = fixtures.build_repo(root)
        honesty.seed(repo)
        mutants = catch_rate.seed(repo)
        episodes.seed(repo)
        fixtures.git_init(root)

        controls = RuntimeControls(fixture_root=root)
        # Normalize against the WHOLE workdir, not just the repo: bootstrap
        # payloads quote the isolated HOME too, and a longer tmp path would
        # otherwise change the measured byte counts run to run.
        recorder = Recorder(root=workdir)
        with isolated_runtime(controls, clock=FixedClock()):
            honesty_metrics = honesty.run(recorder, root, repo)
            catch_metrics = catch_rate.run(recorder, root, mutants, controls)
            specificity_metrics = catch_rate.run_specificity(
                recorder, root, repo_root=sweep_root
            )
            # Metric 3 BEFORE metric 4, and the order is load-bearing: metric 3
            # judges a refusal by REPLAYING it against the fixture, and metric 4
            # REPAIRS episode manifests in that same tree. Run the other way
            # round, every refusal an episode recorded was replayed against a
            # tree the episode had already fixed — the repair looked like it
            # changed nothing, and the descriptor it was judged on no longer
            # matched the file.
            refusal_metrics = refusals.run(recorder, root)
            episode_metrics = episodes.run(recorder, root)
            quality_metrics = quality.run(
                root,
                sorted(fixtures.clean_corpus())
                + [episode.change for episode in episodes.EPISODES],
            )
            size_metrics = report.payload_sizes(recorder)

    metrics = {
        "schema": report.SCHEMA,
        "harness": _harness_contract(),
        "metrics": {
            "envelope_honesty": honesty_metrics,
            "gate_catch_rate": catch_metrics,
            "gate_specificity": specificity_metrics,
            "refusal_self_sufficiency": refusal_metrics,
            "round_trips_to_green": episode_metrics,
            "manifest_quality": quality_metrics,
            "envelope_payload_size": size_metrics,
        },
    }
    return BenchResult(
        metrics=metrics,
        fixture_root=root,
        subprocess_calls=list(controls.subprocess_calls),
    )
