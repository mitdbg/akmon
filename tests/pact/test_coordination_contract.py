"""Coordinator contract rules (issue #1477)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import coordination
from okg.substrate.pact import coordination_contract as contract
from okg.substrate.pact.coordination_contract import (
    COORDINATOR_ASSIGNMENT_KINDS,
    IDLE_HEARTBEAT_SECONDS,
    MUTATION_ASSIGNMENT_KINDS,
    REGISTER_ENV_VAR,
    REPORTED_AGENT_STATES,
    build_reported_state_event,
    claim_mutation_owner,
    coordinator_lease_refusal,
    is_coordinator_kind,
    is_idle_assignment,
    load_mutation_owners,
    mutation_owner,
    mutation_owner_register_path,
    release_mutation_owner,
)

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture()
def register_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(REGISTER_ENV_VAR, str(tmp_path / "mutation-owners.json"))
    return tmp_path


# --- a coordinator never holds a mutation lease -----------------------------


def test_coordinator_kinds_and_mutation_kinds_do_not_overlap() -> None:
    assert COORDINATOR_ASSIGNMENT_KINDS == {"chief", "sprint"}
    assert not COORDINATOR_ASSIGNMENT_KINDS & MUTATION_ASSIGNMENT_KINDS
    assert is_coordinator_kind("Chief") is True
    assert is_coordinator_kind("pact") is False


@pytest.mark.parametrize("kind", sorted(COORDINATOR_ASSIGNMENT_KINDS))
@pytest.mark.parametrize("event_type", ["claim", "renew"])
def test_coordinator_mutation_lease_events_are_refused(
    kind: str, event_type: str
) -> None:
    refusal = coordinator_lease_refusal(kind, event_type)
    assert refusal is not None
    code, reason = refusal
    assert code == "coordinator_lease_refused"
    assert kind in reason


@pytest.mark.parametrize("event_type", ["heartbeat", "release"])
def test_coordinator_liveness_and_cleanup_events_stay_allowed(
    event_type: str,
) -> None:
    assert coordinator_lease_refusal("chief", event_type) is None


@pytest.mark.parametrize("kind", sorted(MUTATION_ASSIGNMENT_KINDS))
def test_worker_kinds_may_claim_a_mutation_lease(kind: str) -> None:
    assert coordinator_lease_refusal(kind, "claim") is None


def test_tracked_policy_binds_to_the_selected_transport() -> None:
    # Private only: this repository's tracked policy loads into a binding,
    # which refuses any agent transport but the selected restricted route.
    private_policy_path(".okg/coordination.yaml")
    binding = coordination.load_coordination_binding(REPO_ROOT)
    assert binding.agent_transport == coordination.AGENT_TRANSPORT
    assert coordination.transport_status(binding)["agent_transport"] == (
        coordination.AGENT_TRANSPORT
    )


def test_transport_status_publishes_the_contract_vocabulary() -> None:
    # The vocabulary is code, not policy: a synthetic binding is enough, so
    # this runs where the tracked policy does not ship.
    binding = coordination.CoordinationPolicyBinding(
        agent_transport=coordination.AGENT_TRANSPORT,
        lease_ttl_seconds={"chief": 60, "sprint": 60, "pact": 60, "delegation": 60},
        policy_digest="sha256:" + "0" * 64,
        project_digest="sha256:" + "1" * 64,
        source="synthetic",
    )
    status = coordination.transport_status(binding)
    assert status["coordinator_assignment_kinds"] == ["chief", "sprint"]
    assert status["mutation_lease_event_types"] == ["claim", "renew"]
    assert status["reported_agent_states"] == ["blocked", "idle"]


# --- exactly one mutation owner per pull request ----------------------------


def test_register_path_follows_the_environment_override(
    register_root: Path,
) -> None:
    assert mutation_owner_register_path(REPO_ROOT) == (
        register_root / "mutation-owners.json"
    )


def test_register_path_defaults_to_the_shared_git_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sibling worktrees of one clone must resolve to one register."""
    monkeypatch.delenv(REGISTER_ENV_VAR, raising=False)
    common = Path(
        subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    )
    if not common.is_absolute():
        common = REPO_ROOT / common
    expected = common / "okg" / "pact" / "mutation-owners.json"
    assert mutation_owner_register_path(REPO_ROOT) == expected
    # `--git-common-dir` is the whole point: it is the SAME directory for every
    # linked worktree of a clone, so two worktrees resolve to one register.
    # In a plain clone it sits inside the checkout; in a linked worktree it
    # does not.  Either way it is shared, so assert the sharing property rather
    # than a layout-specific location.
    assert common.name == ".git" or common.is_dir()
    assert expected.parent == common / "okg" / "pact"


def test_a_write_refuses_when_the_shared_register_cannot_be_resolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Degrading to a worktree-local file would allow two sole owners."""
    monkeypatch.delenv(REGISTER_ENV_VAR, raising=False)
    monkeypatch.setattr(contract, "_git_common_dir", lambda _root: None)
    # A read may still fall back: it cannot invent an owner.
    assert mutation_owner_register_path(tmp_path).parent == tmp_path / ".pact"
    with pytest.raises(SubstrateError) as excinfo:
        claim_mutation_owner(
            tmp_path, pull_request="1500", owner_principal="agent:worker-a"
        )
    assert (excinfo.value.data or {})["cause"] == "shared_register_unavailable"


def test_first_claim_records_the_owner(register_root: Path) -> None:
    owner = claim_mutation_owner(
        REPO_ROOT,
        pull_request="1500",
        owner_principal="agent:worker-a",
        owner_session_id="session:a",
        assignment_id="assignment:a",
    )
    assert owner.owner_principal == "agent:worker-a"
    assert owner.assignment_kind == "pact"
    assert mutation_owner(REPO_ROOT, "1500") == owner
    assert list(load_mutation_owners(REPO_ROOT)) == ["1500"]


def test_second_owner_claim_is_refused(register_root: Path) -> None:
    claim_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    with pytest.raises(SubstrateError) as excinfo:
        claim_mutation_owner(
            REPO_ROOT, pull_request="1500", owner_principal="agent:worker-b"
        )
    data = excinfo.value.data or {}
    assert data["cause"] == "mutation_owner_conflict"
    assert data["recorded_owner"] == "agent:worker-a"
    assert data["claiming_owner"] == "agent:worker-b"
    # The refused claim must not have displaced the recorded owner.
    recorded = mutation_owner(REPO_ROOT, "1500")
    assert recorded is not None and recorded.owner_principal == "agent:worker-a"


def test_reclaim_by_the_same_owner_is_idempotent(register_root: Path) -> None:
    first = claim_mutation_owner(
        REPO_ROOT,
        pull_request="1500",
        owner_principal="agent:worker-a",
        now=datetime(2026, 9, 2, 1, 0, tzinfo=timezone.utc),
    )
    second = claim_mutation_owner(
        REPO_ROOT,
        pull_request="1500",
        owner_principal="agent:worker-a",
        now=datetime(2026, 9, 2, 2, 0, tzinfo=timezone.utc),
    )
    assert second.owner_principal == first.owner_principal
    assert second.claimed_at != first.claimed_at
    assert list(load_mutation_owners(REPO_ROOT)) == ["1500"]


def test_two_pull_requests_have_independent_owners(register_root: Path) -> None:
    claim_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    claim_mutation_owner(
        REPO_ROOT, pull_request="1501", owner_principal="agent:worker-b"
    )
    assert sorted(load_mutation_owners(REPO_ROOT)) == ["1500", "1501"]


@pytest.mark.parametrize("kind", sorted(COORDINATOR_ASSIGNMENT_KINDS))
def test_a_coordinator_cannot_own_a_pull_request(
    register_root: Path, kind: str
) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        claim_mutation_owner(
            REPO_ROOT,
            pull_request="1500",
            owner_principal="agent:coordinator",
            assignment_kind=kind,
        )
    assert (excinfo.value.data or {})["cause"] == "coordinator_lease_refused"
    assert load_mutation_owners(REPO_ROOT) == {}


def test_unknown_assignment_kind_is_refused(register_root: Path) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        claim_mutation_owner(
            REPO_ROOT,
            pull_request="1500",
            owner_principal="agent:worker-a",
            assignment_kind="operator",
        )
    assert (excinfo.value.data or {})["cause"] == "unknown_assignment_kind"


def test_claim_requires_a_pull_request_and_a_principal(
    register_root: Path,
) -> None:
    with pytest.raises(SubstrateError) as missing_pr:
        claim_mutation_owner(
            REPO_ROOT, pull_request="  ", owner_principal="agent:worker-a"
        )
    assert (missing_pr.value.data or {})["cause"] == "pull_request_required"
    with pytest.raises(SubstrateError) as missing_owner:
        claim_mutation_owner(REPO_ROOT, pull_request="1500", owner_principal="")
    assert (missing_owner.value.data or {})["cause"] == "owner_principal_required"


def test_only_the_recorded_owner_may_release(register_root: Path) -> None:
    claim_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    with pytest.raises(SubstrateError) as excinfo:
        release_mutation_owner(
            REPO_ROOT, pull_request="1500", owner_principal="agent:worker-b"
        )
    assert (excinfo.value.data or {})["cause"] == "mutation_owner_mismatch"
    assert release_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    assert mutation_owner(REPO_ROOT, "1500") is None


def test_release_of_an_unknown_pull_request_is_not_an_error(
    register_root: Path,
) -> None:
    assert (
        release_mutation_owner(
            REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
        )
        is False
    )


def test_release_lets_the_pull_request_be_re_dispatched(
    register_root: Path,
) -> None:
    claim_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    release_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-a"
    )
    successor = claim_mutation_owner(
        REPO_ROOT, pull_request="1500", owner_principal="agent:worker-b"
    )
    assert successor.owner_principal == "agent:worker-b"


@pytest.mark.parametrize(
    "body",
    [
        "{not json",
        '["not", "an", "object"]',
        '{"version": 1, "owners": null}',
        '{"version": 1, "owners": ["not-a-mapping"]}',
        '{"version": 1, "owners": {"1500": "agent:worker-a"}}',
    ],
)
def test_a_corrupt_register_is_refused_rather_than_read_as_empty(
    register_root: Path, body: str
) -> None:
    """Reading a damaged register as empty would hand out a second owner."""
    (register_root / "mutation-owners.json").write_text(body, encoding="utf-8")
    with pytest.raises(SubstrateError) as excinfo:
        load_mutation_owners(REPO_ROOT)
    assert (excinfo.value.data or {})["cause"] == "mutation_owner_register_invalid"


def test_a_register_without_owners_is_simply_empty(register_root: Path) -> None:
    (register_root / "mutation-owners.json").write_text(
        '{"version": 1}', encoding="utf-8"
    )
    assert load_mutation_owners(REPO_ROOT) == {}


def test_a_naive_claim_timestamp_is_refused(register_root: Path) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        claim_mutation_owner(
            REPO_ROOT,
            pull_request="1500",
            owner_principal="agent:worker-a",
            now=datetime(2026, 9, 2, 1, 0),
        )
    assert (excinfo.value.data or {})["cause"] == "naive_timestamp"


def test_concurrent_claims_resolve_to_one_owner(register_root: Path) -> None:
    """The advisory lock is the reason two worktrees cannot both win."""
    import threading

    winners: list[str] = []
    losers: list[str] = []
    barrier = threading.Barrier(6)

    def _claim(index: int) -> None:
        barrier.wait()
        try:
            owner = claim_mutation_owner(
                REPO_ROOT,
                pull_request="1500",
                owner_principal=f"agent:worker-{index}",
            )
        except SubstrateError as err:
            losers.append((err.data or {}).get("cause", ""))
        else:
            winners.append(owner.owner_principal)

    threads = [threading.Thread(target=_claim, args=(i,)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(winners) == 1
    assert losers == ["mutation_owner_conflict"] * 5
    recorded = mutation_owner(REPO_ROOT, "1500")
    assert recorded is not None and recorded.owner_principal == winners[0]


# --- idle and blocked are reported events with a reason ---------------------


@pytest.mark.parametrize("state", REPORTED_AGENT_STATES)
def test_reported_state_event_carries_state_reason_and_time(state: str) -> None:
    event = build_reported_state_event(
        state,
        reason="hosted checks are pending on head abc123",
        assignment_id="assignment:a",
        next_action="watch checks",
    )
    assert event["state"] == state
    assert event["reason"] == "hosted checks are pending on head abc123"
    assert event["assignment_id"] == "assignment:a"
    assert event["next_action"] == "watch checks"
    assert event["reported_at"].endswith("+00:00")


@pytest.mark.parametrize("state", REPORTED_AGENT_STATES)
@pytest.mark.parametrize("reason", ["", "   ", None])
def test_a_reported_state_without_a_reason_is_refused(
    state: str, reason: object
) -> None:
    with pytest.raises(SubstrateError) as excinfo:
        build_reported_state_event(state, reason=reason)
    assert (excinfo.value.data or {})["cause"] == "reported_state_reason_required"


def test_an_unsupported_reported_state_is_refused() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        build_reported_state_event("active", reason="looks busy")
    assert (excinfo.value.data or {})["cause"] == "reported_state_invalid"


# --- idle detection ---------------------------------------------------------


_NOW = datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc)
_LIVE_LEASE = _NOW + timedelta(minutes=30)
_SILENT = _NOW - timedelta(seconds=IDLE_HEARTBEAT_SECONDS + 60)


def test_a_silent_live_lease_is_idle() -> None:
    assert is_idle_assignment(
        lease_expires_at=_LIVE_LEASE, last_heartbeat_at=_SILENT, now=_NOW
    )


def test_a_recently_beating_lease_is_not_idle() -> None:
    assert not is_idle_assignment(
        lease_expires_at=_LIVE_LEASE,
        last_heartbeat_at=_NOW - timedelta(seconds=60),
        now=_NOW,
    )


def test_a_lease_that_never_beat_is_not_idle() -> None:
    """Idle measures silence, so it needs a heartbeat to measure against.

    A just-created assignment has no heartbeat yet; calling it idle would
    report every new assignment the moment it appears.
    """
    assert not is_idle_assignment(
        lease_expires_at=_LIVE_LEASE, last_heartbeat_at=None, now=_NOW
    )


def test_a_blocked_assignment_is_not_also_reported_idle() -> None:
    """A blocked agent already said why it stopped; do not double-report it."""
    assert not is_idle_assignment(
        lease_expires_at=_LIVE_LEASE,
        last_heartbeat_at=_SILENT,
        now=_NOW,
        blocked=True,
    )


def test_an_expired_or_absent_lease_is_a_lease_problem_not_idleness() -> None:
    assert not is_idle_assignment(
        lease_expires_at=_NOW - timedelta(minutes=1),
        last_heartbeat_at=_SILENT,
        now=_NOW,
    )
    assert not is_idle_assignment(
        lease_expires_at=None, last_heartbeat_at=_SILENT, now=_NOW
    )


def test_idle_is_exclusive_at_the_threshold() -> None:
    """Exactly at the threshold is not yet idle; one second past it is."""
    assert not is_idle_assignment(
        lease_expires_at=_LIVE_LEASE,
        last_heartbeat_at=_NOW - timedelta(seconds=IDLE_HEARTBEAT_SECONDS),
        now=_NOW,
    )
    assert is_idle_assignment(
        lease_expires_at=_LIVE_LEASE,
        last_heartbeat_at=_NOW - timedelta(seconds=IDLE_HEARTBEAT_SECONDS + 1),
        now=_NOW,
    )


def test_incomparable_timestamps_are_refused_not_read_as_healthy() -> None:
    """A silent False here would turn a safety signal into an all-clear."""
    with pytest.raises(SubstrateError) as excinfo:
        is_idle_assignment(
            lease_expires_at=_LIVE_LEASE,
            last_heartbeat_at=_SILENT,
            now=datetime(2026, 9, 2, 12, 0),
        )
    assert (excinfo.value.data or {})["cause"] == "idle_timestamps_incomparable"
