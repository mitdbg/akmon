from pathlib import Path

import pytest

from okg.substrate.deployment_lifecycle import (
    INPUT_INVALIDATIONS,
    PHASE_ORDER,
    LifecycleResourceState,
    mutation_allowed,
    phases_to_run,
    terminal_state_is_clean,
    valid_completed_phases,
)


ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "formal" / "deployment_lifecycle"
MODEL = MODEL_DIR / "DeploymentLifecycle.tla"
CONFIG = MODEL_DIR / "DeploymentLifecycle.cfg"
RUNNER = ROOT / "scripts" / "run_formal_suite.py"


def test_no_mutation_precedes_preflight() -> None:
    model = MODEL.read_text(encoding="utf-8")

    assert not mutation_allowed(())
    assert not mutation_allowed(("preflight", "backup"))
    assert mutation_allowed(("preflight", "backup", "quiesce"))
    assert 'p \\in MutationPhases /\\ "preflight" \\notin validCompleted' in model
    assert "NoMutationBeforePreflight == ~unsafeMutation" in model


def test_unchanged_completed_phase_never_replays() -> None:
    completed = PHASE_ORDER[:5]

    assert phases_to_run(completed) == ("smoke", "readiness")
    assert phases_to_run(completed, changed_inputs=("smoke",)) == (
        "smoke",
        "readiness",
    )
    model = MODEL.read_text(encoding="utf-8")
    assert "p \\notin validCompleted" in model
    assert "replayedCompleted \\subseteq invalidatedEver" in model


@pytest.mark.parametrize(
    ("changed_input", "expected"),
    [
        ("preflight", PHASE_ORDER),
        ("backup", PHASE_ORDER[1:]),
        ("migration", PHASE_ORDER[3:]),
        ("catalog", PHASE_ORDER[4:]),
        ("smoke", PHASE_ORDER[5:]),
        ("readiness", PHASE_ORDER[6:]),
    ],
)
def test_input_change_invalidates_only_dependents(
    changed_input: str,
    expected: tuple[str, ...],
) -> None:
    assert INPUT_INVALIDATIONS[changed_input] == frozenset(expected)
    assert valid_completed_phases(
        PHASE_ORDER,
        changed_inputs=(changed_input,),
    ) == frozenset(PHASE_ORDER) - frozenset(expected)

    model = MODEL.read_text(encoding="utf-8")
    assert "invalidatedEver \\subseteq allowedInvalidations" in model


@pytest.mark.parametrize("prior_running", [False, True])
def test_terminal_state_restores_prior_service_state(
    prior_running: bool,
) -> None:
    clean = LifecycleResourceState(
        prior_service_running=prior_running,
        service_running=prior_running,
    )
    changed = LifecycleResourceState(
        prior_service_running=prior_running,
        service_running=not prior_running,
    )

    assert terminal_state_is_clean("succeeded", clean)
    assert terminal_state_is_clean("failed", clean)
    assert not terminal_state_is_clean("failed", changed)


@pytest.mark.parametrize(
    "resources",
    [
        {"child_process_running": True},
        {"disposable_database_present": True},
        {"temporary_archive_present": True},
        {"cleanup_obligations": frozenset({"service"})},
    ],
)
def test_terminal_state_owns_no_disposable_resources(
    resources: dict[str, object],
) -> None:
    state = LifecycleResourceState(
        prior_service_running=True,
        service_running=True,
        **resources,
    )

    assert not terminal_state_is_clean("failed", state)


def test_unknown_effect_cannot_be_reported_clean() -> None:
    state = LifecycleResourceState(
        prior_service_running=True,
        service_running=True,
        uncertain_effects=frozenset({"migration"}),
    )

    assert not terminal_state_is_clean("succeeded", state)
    assert not terminal_state_is_clean("failed", state)
    assert not terminal_state_is_clean(
        "blocked",
        LifecycleResourceState(True, True),
    )


def test_every_named_invariant_runs_in_ci() -> None:
    model = MODEL.read_text(encoding="utf-8")
    config = CONFIG.read_text(encoding="utf-8")
    runner = RUNNER.read_text(encoding="utf-8")

    for invariant in (
        "NoDisruptiveEffectBeforePreflight",
        "NoMutationBeforePreflight",
        "CompletedPhaseReplayRequiresInvalidation",
        "InputChangeInvalidatesOnlyDependents",
        "CleanTerminalRestoresPriorServiceState",
        "CleanTerminalOwnsNoDisposableResources",
        "UnknownEffectCannotBeClean",
        "CleanupFailureCannotBeClean",
        "CancellationRequiresCompensation",
        "UncertainCompletionRequiresReceipt",
    ):
        assert invariant in config
    assert 'CheckerSpec("check_formal_deployment_lifecycle.py"' in runner
    assert "Cancel ==" in model
    assert "UncertainCompletionRequiresReceipt" in model
