"""Publish lock-hold headroom: receipt fields and the blocking gates.

Issue #1291. A corpus-scale publish held the publisher's global advisory lock
881.6s against the 900s hard statement timeout — 18.4s of margin — and the only
consequence was a log warning: readiness stayed clean and release proof passed.

These tests pin the three things that changed:
  * the publish receipt names lock wait, lock hold, the slowest phase, the hard
    timeout, and the configured headroom policy;
  * the shared publish proof and deployment readiness flag a measured hold
    that lacks the required safety factor (decisions 23a and 35a made both a
    capacity warning rather than a failure); and
  * a missing measurement is reported as unproven, never as healthy.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.publish_lock_headroom import (
    DEFAULT_LOCK_HOLD_HEADROOM_FACTOR,
    LOCK_HEADROOM_FACTOR_ENV,
    RAISED_TIMEOUT_OVERRIDE_ENV,
    STATEMENT_TIMEOUT_ENV,
    evaluate_lock_headroom,
    headroom_policy,
    lock_receipt,
    resolve_hard_timeout_ms,
    resolve_headroom_factor,
)

HARD_TIMEOUT_MS = 900_000


@pytest.fixture(autouse=True)
def _default_headroom_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the policy environment.

    Every verdict here is stated against the built-in policy. A shell that
    exports a statement-timeout or factor override would otherwise change what
    these tests assert, and they would fail for a reason that has nothing to do
    with the code under test.
    """
    for name in (
        STATEMENT_TIMEOUT_ENV,
        LOCK_HEADROOM_FACTOR_ENV,
        RAISED_TIMEOUT_OVERRIDE_ENV,
    ):
        monkeypatch.delenv(name, raising=False)


def _timing_details(
    *,
    lock_hold_ms: float | None,
    lock_acquire_ms: float | None = 12.0,
    hard_timeout_ms: int | None = HARD_TIMEOUT_MS,
) -> dict[str, Any]:
    from okg.substrate.publisher.publisher import Publisher

    return Publisher._chronos_timing_details(
        timings_ms={
            "locked_connection": lock_acquire_ms,
            "chronos_delta_scan": 700_000.0,
            "catalog_version": 25.0,
            "total": 880_000.0,
        },
        lock_acquire_ms=lock_acquire_ms,
        lock_hold_ms=lock_hold_ms,
        coordination_mode="dbos",
        hard_timeout_ms=hard_timeout_ms,
        enricher_runs=[],
        consistency_summaries=[],
        view_outcomes=[],
        native_view_refresh=None,
        lens_outcomes=[],
    )


def _publish_payload(details: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "published",
        "generation_id": "gen-1291",
        "nodes_added": 4,
        "edges_added": 6,
        "timing_details": details,
    }


def test_publish_receipt_exposes_lock_wait_hold_timeout_and_policy() -> None:
    """The receipt must answer 'how close was this publish to the bound?'."""
    details = _timing_details(lock_hold_ms=120_000.0, lock_acquire_ms=12.0)
    lock = details["lock"]

    assert lock["wait_ms"] == 12.0
    assert lock["acquire_ms"] == 12.0
    assert lock["hold_ms"] == 120_000.0
    assert lock["hard_timeout_ms"] == HARD_TIMEOUT_MS
    # Slowest phase identity: the phase to fix, not the lock bookkeeping and
    # not the whole-cycle total.
    assert lock["slowest_phase"]["name"] == "stage.chronos_delta_scan"
    assert lock["slowest_phase"]["elapsed_ms"] == 700_000.0

    headroom = lock["headroom"]
    assert headroom["status"] == "pass"
    assert headroom["ok"] is True
    assert headroom["required_factor"] == DEFAULT_LOCK_HOLD_HEADROOM_FACTOR
    assert headroom["max_hold_ms"] == 450_000.0
    assert headroom["hard_timeout_ms"] == HARD_TIMEOUT_MS
    assert headroom["remaining_ms"] == 780_000.0
    assert headroom["observed_factor"] == 7.5


def test_receipt_reports_the_881s_hold_as_blocked() -> None:
    """The measured staging hold from the issue must not read as healthy."""
    lock = _timing_details(lock_hold_ms=881_600.0)["lock"]
    headroom = lock["headroom"]

    assert headroom["status"] == "blocked"
    assert headroom["ok"] is False
    assert headroom["remaining_ms"] == pytest.approx(18_400.0)
    assert headroom["observed_factor"] == pytest.approx(1.021, abs=1e-3)
    assert "881.6s" in headroom["reason"]
    assert headroom["fix_hint"]


def test_receipt_without_a_measured_hold_is_unmeasured_not_a_pass() -> None:
    """Absence dressed as agreement: no hold measured is not 'healthy'."""
    headroom = _timing_details(lock_hold_ms=None)["lock"]["headroom"]

    assert headroom["status"] == "unmeasured"
    assert headroom["ok"] is False
    assert headroom["measured"] is False


def test_disabled_hard_timeout_is_unbounded_not_a_pass() -> None:
    headroom = _timing_details(
        lock_hold_ms=5_000.0, hard_timeout_ms=0,
    )["lock"]["headroom"]

    assert headroom["status"] == "unbounded"
    assert headroom["ok"] is False


def test_headroom_factor_is_configurable_and_rejects_a_weaker_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(LOCK_HEADROOM_FACTOR_ENV, "3")
    assert resolve_headroom_factor() == (3.0, "env")
    assert headroom_policy(hard_timeout_ms=HARD_TIMEOUT_MS)["max_hold_ms"] == (
        300_000.0
    )
    strict = evaluate_lock_headroom(
        hold_ms=400_000.0, hard_timeout_ms=HARD_TIMEOUT_MS,
    )
    assert strict["status"] == "blocked"

    # An unusable override never silently becomes a weaker bar.
    monkeypatch.setenv(LOCK_HEADROOM_FACTOR_ENV, "not-a-number")
    factor, source = resolve_headroom_factor()
    assert (factor, source) == (
        DEFAULT_LOCK_HOLD_HEADROOM_FACTOR, "default_after_invalid_env",
    )
    monkeypatch.setenv(LOCK_HEADROOM_FACTOR_ENV, "0.1")
    assert resolve_headroom_factor()[0] == DEFAULT_LOCK_HOLD_HEADROOM_FACTOR


def test_headroom_ratio_is_not_integer_floored() -> None:
    """A `//` floor would report every hold in the upper half as 1x."""
    verdict = evaluate_lock_headroom(
        hold_ms=600_000.0, hard_timeout_ms=HARD_TIMEOUT_MS,
    )
    assert verdict["observed_factor"] == 1.5
    assert verdict["status"] == "blocked"


# --------------------------------------------------------------- publish proof


@pytest.fixture()
def _no_dead_letters(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (0, {}),
    )


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_passes_a_bounded_hold() -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        _publish_payload(_timing_details(lock_hold_ms=90_000.0)),
        dsn="postgres://unused",
    )
    assert verdict["ok"] is True
    assert verdict["lock_headroom"]["status"] == "pass"


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_warns_on_a_committed_hold_over_the_bar() -> None:
    """Decision 23a: the 400k publish held the lock 800.4s against a 450.0s
    bar and committed. The proof passes with a capacity warning that keeps
    the hold, the bar, and how the bar was derived."""
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        _publish_payload(_timing_details(lock_hold_ms=800_400.0)),
        dsn="postgres://unused",
    )
    assert verdict["ok"] is True
    assert verdict["status"] == "published"
    assert verdict["generation_id"] == "gen-1291"
    assert verdict["lock_headroom"]["status"] == "blocked"
    warning = verdict["capacity_warning"]
    assert warning["status"] == "capacity_warning"
    assert warning["hold_ms"] == 800_400.0
    assert warning["max_hold_ms"] == 450_000.0
    assert warning["required_factor"] == 2.0
    assert warning["policy_timeout_ms"] == 900_000.0
    assert "held the lock 800.4s" in warning["message"]
    assert "(450.0s)" in warning["message"]
    assert "it held the lock 800.4s against a 450.0s bar" in warning["message"]
    assert warning["message"].startswith("publish held the lock")
    assert warning["bar_derivation"] == (
        "bar = the running statement timeout 900.0s / headroom factor 2.0 "
        "(default) = 450.0s"
    )
    assert warning["fix_hint"]


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_under_the_bar_carries_no_capacity_warning() -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        _publish_payload(_timing_details(lock_hold_ms=449_000.0)),
        dsn="postgres://unused",
    )
    assert verdict["ok"] is True
    assert "capacity_warning" not in verdict


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_still_fails_a_disabled_statement_timeout() -> None:
    """Decision 23a covers only a measured hold over a real bar. With the
    statement timeout disabled nothing bounds the hold, so the proof fails
    with no capacity warning, however long the hold."""
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        _publish_payload(
            _timing_details(lock_hold_ms=800_400.0, hard_timeout_ms=0),
        ),
        dsn="postgres://unused",
    )
    assert verdict["lock_headroom"]["status"] == "unbounded"
    assert verdict["ok"] is False
    assert "statement timeout is disabled" in verdict["reason"]
    assert "capacity_warning" not in verdict


def test_surface_capacity_warning_prints_one_line_and_returns_the_block(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from okg.substrate.publish_proof import surface_capacity_warning

    block = {"status": "capacity_warning", "message": "publish held the lock 800.4s"}
    assert surface_capacity_warning(
        {"ok": True, "capacity_warning": block}, surface="smoke publish",
    ) == block
    assert surface_capacity_warning({"ok": True}, surface="x") is None
    assert surface_capacity_warning(
        {"ok": False, "capacity_warning": block}, surface="x",
    ) is None
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == (
        "capacity warning (smoke publish): publish held the lock 800.4s\n"
    )


def test_publish_proof_over_the_bar_still_fails_a_real_proof_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The warning never masks another check: dead-letter work over a hold
    past the bar is still a failed proof, with its own reason."""
    from okg.substrate.publish_proof import assert_publish_proof

    monkeypatch.setattr(
        "okg.substrate.publish_proof.dlq_count",
        lambda dsn, **_kwargs: (2, {}),
    )
    verdict = assert_publish_proof(
        _publish_payload(_timing_details(lock_hold_ms=800_400.0)),
        dsn="postgres://unused",
    )
    assert verdict["ok"] is False
    assert verdict["reason"] == "publish verification found dead-letter runtime work"
    assert "capacity_warning" not in verdict


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_blocks_a_receipt_with_no_lock_evidence() -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        {
            "status": "published",
            "generation_id": "gen-1291",
            "nodes_added": 4,
            "edges_added": 6,
        },
        dsn="postgres://unused",
    )
    assert verdict["ok"] is False
    assert verdict["lock_headroom"]["status"] == "unmeasured"


@pytest.mark.usefixtures("_no_dead_letters")
def test_publish_proof_exemption_must_be_stated_not_inferred() -> None:
    from okg.substrate.publish_proof import assert_publish_proof

    verdict = assert_publish_proof(
        {
            "status": "published",
            "generation_id": "gen-1291",
            "nodes_added": 4,
            "edges_added": 6,
        },
        dsn="postgres://unused",
        lock_headroom_not_applicable=(
            "the already-current published generation was observed without "
            "republishing"
        ),
    )
    assert verdict["ok"] is True
    assert verdict["lock_headroom"]["status"] == "not_applicable"


# ------------------------------------------------------------------ readiness


def _ledger_row(name: str, hold_ms: float | None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "row_kind": "source",
        "name": name,
        "readiness_required": True,
        "latest_full_pipeline_scenario": "full_pipeline",
        "latest_full_pipeline_lock_wait_p95_ms": 8.0,
    }
    if hold_ms is not None:
        row["latest_full_pipeline_lock_hold_p95_ms"] = hold_ms
    return row


def test_readiness_warns_on_a_measured_hold_without_the_safety_factor() -> None:
    """Decision 35a: over the bar, this check warns instead of blocking.

    Proves only the publisher.lock_headroom check: it lands in warnings with
    its message, violations and next action, and on its own it leaves the
    readiness payload clean. Other checks (for example
    sources.optimization_budget) are not exercised here.
    """
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
        _ready_publisher_lock_headroom_status,
    )
    from okg.substrate.publish_proof import CAPACITY_WARNING_STATUS

    status = _ready_publisher_lock_headroom_status({
        "rows": [
            _ledger_row("git-history", 40_000.0),
            _ledger_row("code-structure", 881_600.0),
        ],
    })
    assert status["status"] == CAPACITY_WARNING_STATUS == "capacity_warning"
    assert status["ok"] is False
    assert status["representative"]["source_name"] == "code-structure"
    assert status["next_action"] == "reduce_publish_lock_hold"
    assert [v["source_name"] for v in status["violations"]] == [
        "code-structure",
    ]
    assert "exceeds the 2.0x headroom bound 450.0s" in status["message"]

    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status},
    )
    _ready_finalize_payload_state(payload)
    assert not payload.get("blockers")
    assert not payload.get("unknowns")
    warnings = payload.get("warnings") or []
    assert [row["id"] for row in warnings] == ["publisher.lock_headroom"]
    assert warnings[0]["status"] == "warning"
    assert warnings[0]["message"] == status["message"]
    assert warnings[0]["details"]["next_action"] == "reduce_publish_lock_hold"
    assert payload["publisher_lock_headroom"]["violations"]
    # The warning alone does not fail readiness.
    assert payload["ok"] is True
    assert payload["status"] == "clean"


def test_readiness_still_reports_unknown_for_a_disabled_timeout() -> None:
    """Only a measured over-the-bar hold became a warning; unbounded did not."""
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
    )

    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload,
        {"publisher_lock_headroom": {"ok": False, "status": "unbounded",
                                     "message": "no bound"}},
    )
    _ready_finalize_payload_state(payload)
    assert [row["id"] for row in payload["unknowns"]] == [
        "publisher.lock_headroom",
    ]
    assert not payload.get("warnings")
    assert payload["ok"] is False


def test_readiness_passes_a_comfortably_bounded_hold() -> None:
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_publisher_lock_headroom_status,
    )

    status = _ready_publisher_lock_headroom_status({
        "rows": [_ledger_row("git-history", 40_000.0)],
    })
    assert status["status"] == "pass"

    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status},
    )
    assert not payload.get("blockers")
    assert not payload.get("unknowns")
    assert [row["id"] for row in payload["checks"]] == [
        "publisher.lock_headroom",
    ]


def test_readiness_never_reads_an_unmeasured_hold_as_healthy() -> None:
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
        _ready_publisher_lock_headroom_status,
    )

    status = _ready_publisher_lock_headroom_status({
        "rows": [_ledger_row("git-history", None)],
    })
    assert status["status"] == "unmeasured"
    assert status["ok"] is False

    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status},
    )
    _ready_finalize_payload_state(payload)
    assert [row["id"] for row in payload["unknowns"]] == [
        "publisher.lock_headroom",
    ]
    assert payload["ok"] is False
    assert not payload.get("blockers")


def test_readiness_reports_unknown_when_the_ledger_was_not_read() -> None:
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
    )

    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(payload, None)
    assert [row["id"] for row in payload["unknowns"]] == [
        "publisher.lock_headroom",
    ]


def test_a_non_finite_factor_cannot_disable_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`nan` parses and compares False against every bound."""
    for raw in ("nan", "inf", "-inf"):
        monkeypatch.setenv(LOCK_HEADROOM_FACTOR_ENV, raw)
        assert resolve_headroom_factor() == (
            DEFAULT_LOCK_HOLD_HEADROOM_FACTOR, "default_after_invalid_env",
        ), raw
        verdict = evaluate_lock_headroom(
            hold_ms=881_600.0, hard_timeout_ms=HARD_TIMEOUT_MS,
        )
        assert verdict["status"] == "blocked", raw


def test_a_non_finite_or_negative_hold_is_not_a_measurement() -> None:
    for hold in (float("nan"), float("inf"), -5.0):
        verdict = evaluate_lock_headroom(
            hold_ms=hold, hard_timeout_ms=HARD_TIMEOUT_MS,
        )
        assert verdict["status"] == "unmeasured", hold
        assert verdict["ok"] is False
        assert verdict["measured"] is False


def test_raising_the_statement_timeout_cannot_relax_the_release_bar() -> None:
    """The bar is the smaller of the running timeout and the 900s default."""
    widened = evaluate_lock_headroom(
        hold_ms=881_600.0, hard_timeout_ms=3_600_000,
    )
    assert widened["status"] == "blocked"
    assert widened["policy"]["policy_timeout_ms"] == 900_000.0
    assert widened["policy"]["timeout_above_default"] is True
    assert widened["max_hold_ms"] == 450_000.0

    # A LOWER timeout still tightens the bar.
    tightened = evaluate_lock_headroom(
        hold_ms=200_000.0, hard_timeout_ms=300_000,
    )
    assert tightened["status"] == "blocked"
    assert tightened["max_hold_ms"] == 150_000.0


def test_the_worst_hold_in_a_multi_publish_report_is_the_one_judged() -> None:
    """A slow first publish must not be hidden by a fast last one."""
    from okg.substrate.publish_lock_headroom import lock_headroom_from_payload

    report = {
        "publishes": [
            {
                "status": "published",
                "timing_details": _timing_details(lock_hold_ms=881_600.0),
            },
            {
                "status": "published",
                "timing_details": _timing_details(lock_hold_ms=1_200.0),
            },
        ],
    }
    assert lock_receipt(report)["hold_ms"] == 881_600.0
    verdict = lock_headroom_from_payload(report)
    assert verdict["status"] == "blocked"


def test_an_unmeasured_publish_outranks_a_measured_one() -> None:
    from okg.substrate.publish_lock_headroom import lock_headroom_from_payload

    report = {
        "publishes": [
            {"timing_details": _timing_details(lock_hold_ms=1_200.0)},
            {"timing_details": _timing_details(lock_hold_ms=None)},
        ],
    }
    assert lock_headroom_from_payload(report)["status"] == "unmeasured"


def test_the_reader_says_where_the_bound_came_from(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.publish_lock_headroom import lock_headroom_from_payload

    monkeypatch.setenv(STATEMENT_TIMEOUT_ENV, "900")
    from_receipt = lock_headroom_from_payload(
        _publish_payload(_timing_details(lock_hold_ms=1_000.0)),
    )
    assert from_receipt["hard_timeout_source"] == "receipt"

    legacy = {"timing_details": {"lock": {"hold_ms": 1_000.0}}}
    assert lock_headroom_from_payload(legacy)["hard_timeout_source"] == (
        "environment"
    )


def test_the_readiness_check_is_actually_wired_into_the_ready_command() -> None:
    """The gate must not be silently unhooked from the readiness command.

    Every other readiness test here calls the check helper directly, so
    deleting its one call site would leave them all green. Bind the call site
    itself: it must run in `_deployment_ready_live_checks`, right where the
    source-optimization ledger it reads is evaluated.
    """
    import inspect

    from okg.substrate.cli import deployment as deployment_cli

    source = inspect.getsource(deployment_cli._deployment_ready_live_checks)
    assert "_ready_add_publisher_lock_headroom_check(" in source
    assert source.index("sources.optimization_budget") < source.index(
        "_ready_add_publisher_lock_headroom_check(",
    )


def test_the_ledger_readiness_payload_carries_the_headroom_summary() -> None:
    """The readiness check reads its status out of the ledger payload."""
    from okg.substrate.cli.deployment import (
        _ready_source_optimization_status_from_ledger,
    )

    payload = _ready_source_optimization_status_from_ledger(
        {
            "deployment": "okg-workspace",
            "rows": [
                dict(_ledger_row("code-structure", 881_600.0), blocker=None),
            ],
        },
        latency_budget_ms=1_000.0,
    )
    summary = payload["publisher_lock_headroom"]
    assert summary["status"] == "capacity_warning"
    assert summary["hard_timeout_source"] == "compatibility_default"


def test_a_raised_timeout_is_honored_only_when_the_operator_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cold rebuild can raise the bound, but never by accident.

    pact/changes/rebuild-af-pubs-interval-storage records a real rebuild run
    with the statement timeout raised to 7200s. Clamping with no escape would
    make that run's publish proof unpassable.
    """
    clamped = evaluate_lock_headroom(
        hold_ms=1_200_000.0, hard_timeout_ms=7_200_000,
    )
    assert clamped["status"] == "blocked"
    assert clamped["policy"]["policy_timeout_source"] == "clamped_to_default"
    assert RAISED_TIMEOUT_OVERRIDE_ENV in clamped["reason"]

    monkeypatch.setenv(RAISED_TIMEOUT_OVERRIDE_ENV, "1")
    widened = evaluate_lock_headroom(
        hold_ms=1_200_000.0, hard_timeout_ms=7_200_000,
    )
    assert widened["status"] == "pass"
    assert widened["policy"]["policy_timeout_source"] == (
        "raised_timeout_override"
    )
    assert widened["policy"]["timeout_above_default"] is True
    # The named escape widens the bound; it does not disable the bar.
    assert evaluate_lock_headroom(
        hold_ms=7_000_000.0, hard_timeout_ms=7_200_000,
    )["status"] == "blocked"


def test_a_receipt_cannot_author_its_own_passing_headroom_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The judged artifact must not be allowed to set the bar it is judged by.

    Issue #1479 moved the required factor and the raised-timeout authorization
    into the deployment manifest, and the publisher writes both into its own
    lock receipt. Reading them back verbatim would let a deployment declare
    ``lock_hold_headroom_factor: 1.0`` plus a two-hour statement timeout and
    pass a nearly two-hour lock hold -- the exact hold this module exists to
    refuse. A receipt may make the bar stricter; it may never relax it.
    """
    from okg.substrate.publish_lock_headroom import lock_headroom_from_payload

    def report(factor: float) -> dict[str, Any]:
        return {
            "timing_details": {
                "lock": {
                    "hold_ms": 7_000_000.0,
                    "hard_timeout_ms": 7_200_000,
                    "headroom": {
                        "policy": {
                            "required_factor": factor,
                            "factor_source": "deployment_manifest",
                            "allow_raised_timeout": True,
                            "allow_raised_timeout_source": (
                                "deployment_manifest"
                            ),
                            "hard_timeout_source": "deployment_manifest",
                        },
                    },
                },
            },
        }

    # The reader withholds its own raised-timeout authorization, so the bound
    # clamps back to the 900s default no matter what the receipt claims.
    monkeypatch.delenv(RAISED_TIMEOUT_OVERRIDE_ENV, raising=False)
    monkeypatch.delenv(LOCK_HEADROOM_FACTOR_ENV, raising=False)
    self_serving = lock_headroom_from_payload(report(1.0))
    assert self_serving["status"] == "blocked"
    assert self_serving["ok"] is False
    # The weakened factor is discarded in favour of the reader's own.
    assert self_serving["policy"]["required_factor"] == (
        DEFAULT_LOCK_HOLD_HEADROOM_FACTOR
    )
    assert self_serving["policy"]["policy_timeout_source"] == (
        "clamped_to_default"
    )

    # Even with the reader's own raised-timeout authorization, the relaxed
    # factor stays refused: the two authorities are ANDed, not ORed.
    monkeypatch.setenv(RAISED_TIMEOUT_OVERRIDE_ENV, "1")
    assert lock_headroom_from_payload(report(1.0))["status"] == "blocked"

    # A receipt that asks for a STRICTER bar than the reader's is honored.
    stricter = lock_headroom_from_payload(report(10.0))
    assert stricter["policy"]["required_factor"] == 10.0
    assert stricter["policy"]["factor_source"] == "deployment_manifest"


def test_the_margin_is_reported_against_the_bound_that_decided(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """"observed 4.08x, required 2.0x, blocked" would read as a gate bug."""
    verdict = evaluate_lock_headroom(
        hold_ms=881_600.0, hard_timeout_ms=3_600_000,
    )
    assert verdict["status"] == "blocked"
    assert verdict["policy_timeout_ms"] == 900_000.0
    assert verdict["observed_factor"] == pytest.approx(1.021, abs=1e-3)
    assert verdict["remaining_ms"] == pytest.approx(18_400.0)


def test_an_unusable_statement_timeout_does_not_crash_the_readers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """int(inf) and int(nan) raise; readiness and the proof now read this."""
    for raw in ("inf", "nan", "-900", "not-a-number"):
        monkeypatch.setenv(STATEMENT_TIMEOUT_ENV, raw)
        assert resolve_hard_timeout_ms() == 900_000, raw
    monkeypatch.setenv(STATEMENT_TIMEOUT_ENV, "0")
    assert resolve_hard_timeout_ms() == 0
    monkeypatch.setenv(STATEMENT_TIMEOUT_ENV, "300")
    assert resolve_hard_timeout_ms() == 300_000


def test_the_microbench_records_the_worst_single_lock_hold() -> None:
    """The ledger bound is per cycle, so the sample total is the wrong number.

    `flat_ms` sums each key across every publish in a sample. A two-publish
    scenario holding the lock 300s then 200s must not be judged as one 500s
    hold.
    """
    from okg.substrate.source_sync_microbench import _publish_timing_details

    details = _publish_timing_details(
        [
            {
                "label": "first",
                "status": "published",
                "timing_details": {"flat_ms": {"lock.hold": 300_000.0}},
            },
            {
                "label": "second",
                "status": "published",
                "timing_details": {"flat_ms": {"lock.hold": 200_000.0}},
            },
        ],
        graph_read_visibility=None,
    )
    assert details["flat_ms"]["lock.hold"] == 500_000.0
    assert details["flat_ms"]["lock.hold.max"] == 300_000.0


# --------------------------------------- readiness: latest publish (284a)


def _readiness_after(ledger_rows, latest_publish):
    """Run the lock-headroom readiness check as `okg deployment ready` does."""
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
        _ready_publisher_lock_headroom_status,
    )

    status = _ready_publisher_lock_headroom_status({"rows": ledger_rows})
    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status}, latest_publish,
    )
    _ready_finalize_payload_state(payload)
    return payload


def test_fresh_install_with_one_measured_publish_is_ready() -> None:
    """Decision 284a: provision's own publish measured the hold; no source
    benchmark exists yet. That publish is the evidence, so the check passes
    instead of staying unknown forever."""
    payload = _readiness_after(
        [_ledger_row("git-files", None)],
        {"generation_id": "gen-284a", "hold_ms": 11_600.0},
    )
    status = payload["publisher_lock_headroom"]
    assert status["status"] == "pass"
    assert status["representative"]["origin"] == "latest_publish"
    assert status["representative"]["generation_id"] == "gen-284a"
    assert status["measured_count"] == 1
    assert not payload.get("unknowns")
    assert not payload.get("blockers")
    assert payload["ok"] is True


def test_never_published_deployment_stays_unknown() -> None:
    payload = _readiness_after([_ledger_row("git-files", None)], None)
    assert payload["publisher_lock_headroom"]["status"] == "unmeasured"
    assert "no published generation recorded one" in (
        payload["publisher_lock_headroom"]["message"]
    )
    assert [row["id"] for row in payload["unknowns"]] == [
        "publisher.lock_headroom",
    ]
    assert payload["ok"] is False


@pytest.mark.parametrize("hold", [None, "fast", float("nan"), -1.0])
def test_a_publish_without_a_valid_measured_hold_is_not_evidence(hold) -> None:
    payload = _readiness_after(
        [_ledger_row("git-files", None)],
        {"generation_id": "gen-x", "hold_ms": hold},
    )
    assert payload["publisher_lock_headroom"]["status"] == "unmeasured"
    assert payload["ok"] is False


def test_a_latest_publish_over_the_bar_is_a_capacity_warning() -> None:
    """Decision 35a still holds: a measured hold over the bar warns, it does
    not block, whichever evidence measured it."""
    from okg.substrate.publish_proof import CAPACITY_WARNING_STATUS

    payload = _readiness_after(
        [_ledger_row("git-files", None)],
        {"generation_id": "gen-slow", "hold_ms": 600_000.0},
    )
    status = payload["publisher_lock_headroom"]
    assert status["status"] == CAPACITY_WARNING_STATUS
    assert "latest publish gen-slow" in status["message"]
    assert [row["id"] for row in payload["warnings"]] == [
        "publisher.lock_headroom",
    ]
    assert not payload.get("blockers")


def test_the_worst_hold_decides_across_ledger_and_latest_publish() -> None:
    """A fast latest publish cannot hide a slow benchmarked source, and a slow
    latest publish is not hidden by a fast benchmark."""
    from okg.substrate.publish_proof import CAPACITY_WARNING_STATUS

    slow_source = _readiness_after(
        [_ledger_row("code-structure", 881_600.0)],
        {"generation_id": "gen-fast", "hold_ms": 1_000.0},
    )["publisher_lock_headroom"]
    assert slow_source["status"] == CAPACITY_WARNING_STATUS
    assert slow_source["representative"]["source_name"] == "code-structure"

    slow_publish = _readiness_after(
        [_ledger_row("git-history", 40_000.0)],
        {"generation_id": "gen-slow", "hold_ms": 600_000.0},
    )["publisher_lock_headroom"]
    assert slow_publish["status"] == CAPACITY_WARNING_STATUS
    assert slow_publish["representative"]["origin"] == "latest_publish"
    assert slow_publish["measured_count"] == 2


def test_an_unbounded_timeout_stays_unknown_even_after_a_publish() -> None:
    from okg.substrate.cli.deployment import (
        _ready_add_publisher_lock_headroom_check,
        _ready_finalize_payload_state,
    )

    status = {
        "ok": False, "status": "unbounded", "message": "no bound",
        "policy": {"max_hold_ms": None, "hard_timeout_ms": None,
                   "required_factor": 2.0},
        "measurements": [],
    }
    payload: dict[str, Any] = {}
    _ready_add_publisher_lock_headroom_check(
        payload, {"publisher_lock_headroom": status},
        {"generation_id": "gen-1", "hold_ms": 1_000.0},
    )
    _ready_finalize_payload_state(payload)
    assert payload["publisher_lock_headroom"]["status"] == "unbounded"
    assert [row["id"] for row in payload["unknowns"]] == [
        "publisher.lock_headroom",
    ]


def test_generation_status_reads_the_checkpoint_lock_receipt() -> None:
    from okg.substrate.cli.deployment_readiness import (
        _ready_generation_publisher_lock,
    )

    metadata = {"publisher_lock": {
        "schema": "okg.publisher.checkpoint_lock.v1", "hold_ms": 11_634.2,
        "measured_until": "before_checkpoint_write",
        "hard_timeout_ms": 900_000, "hard_timeout_source": "manifest",
    }}
    assert _ready_generation_publisher_lock("gen-1", metadata) == {
        "generation_id": "gen-1", "hold_ms": 11_634.2,
        "measured_until": "before_checkpoint_write",
        "hard_timeout_ms": 900_000, "hard_timeout_source": "manifest",
    }
    assert _ready_generation_publisher_lock("gen-1", {}) is None
    # The returned-only timing_details is not checkpoint evidence.
    assert _ready_generation_publisher_lock(
        "gen-1", {"timing_details": {"lock": {"hold_ms": 1.0}}},
    ) is None
