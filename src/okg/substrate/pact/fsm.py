"""PACT v4 lifecycle state machine.

State is DERIVED from exactly two inputs — the evidence ledger and recorded
decisions — never authored. The transition table ships as data
(``lifecycle_v4.yaml``, validated by :func:`validate_table`); this module is
the only interpreter. Per ``standardize-pact-review-interface`` design.md §1.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError

TABLE_PATH = Path(__file__).parent / "lifecycle_v4.yaml"

HUMAN_VERDICTS = ("approve", "revise", "reject", "supersede", "defer")
AGENT_ONLY_VERDICTS = ("escalate",)
OPERATOR_ONLY_VERDICTS = ("reopen",)
ALL_VERDICTS = HUMAN_VERDICTS + AGENT_ONLY_VERDICTS + OPERATOR_ONLY_VERDICTS

# States whose pending exit is a decision (they sit in someone's queue).
DECISION_STATES = ("proposed", "awaiting_review")


@dataclass(frozen=True)
class DecisionEvent:
    """One recorded decision, as read back from the ledger."""

    change_id: str
    verdict: str
    principal: str
    authority: str = "operator"
    note: str | None = None
    target: str | None = None
    from_state: str | None = None
    to_state: str | None = None
    intent_digest: str | None = None
    decided_at: str | None = None


@dataclass
class DerivedState:
    state: str
    pending_decision: str | None  # what kind of decision the queue shows
    reasons: list[str] = field(default_factory=list)


@lru_cache(maxsize=1)
def load_table() -> dict[str, Any]:
    raw = fastyaml.safe_load(TABLE_PATH.read_text(encoding="utf-8"))
    issues = validate_table(raw)
    if issues:
        raise SubstrateError(
            "lifecycle_v4.yaml failed table validation",
            data={"issues": issues},
        )
    return raw


def validate_table(raw: dict[str, Any]) -> list[str]:
    """Structural checks: no orphan states, every non-terminal state has an
    exit, terminal states exit only via operator-only verdicts."""
    issues: list[str] = []
    states = raw.get("states") or {}
    transitions = raw.get("transitions") or []
    names = set(states)
    reachable: set[str] = set()
    for t in transitions:
        for key in ("from", "to"):
            s = t.get(key)
            if s not in names:
                issues.append(f"transition references unknown state: {s}")
        reachable.add(t.get("to"))
    for name, spec in states.items():
        terminal = bool(spec.get("terminal"))
        exits = [t for t in transitions if t.get("from") == name]
        if not terminal and not exits:
            issues.append(f"non-terminal state has no exit: {name}")
        if terminal:
            for t in exits:
                if t.get("event") not in OPERATOR_ONLY_VERDICTS:
                    issues.append(
                        f"terminal state {name} exits via non-operator event "
                        f"{t.get('event')}"
                    )
        if name not in reachable and name != "draft":
            issues.append(f"orphan state (unreachable, no inbound transition): {name}")
    declared = set(raw.get("verdicts", {}).get("human", [])) | set(
        raw.get("verdicts", {}).get("agent_only", [])
    ) | set(raw.get("verdicts", {}).get("operator_only", []))
    if declared != set(ALL_VERDICTS):
        issues.append(
            f"verdict set mismatch: table declares {sorted(declared)}, "
            f"module expects {sorted(ALL_VERDICTS)}"
        )
    return issues


def terminal_states() -> set[str]:
    return {
        name
        for name, spec in (load_table().get("states") or {}).items()
        if spec.get("terminal")
    }


@dataclass
class GuardViolation(SubstrateError):
    pass


def legal_transition(
    state: str,
    verdict: str,
    *,
    is_operator: bool,
    target: str | None = None,
) -> str | None:
    """Return the next state for ``verdict`` taken in ``state``.

    Returns None for recorded-but-non-transitioning verdicts (defer,
    escalate). Raises SubstrateError naming the violated guard otherwise.
    """
    if verdict not in ALL_VERDICTS:
        raise SubstrateError(
            f"unknown verdict: {verdict}",
            data={"legal": list(ALL_VERDICTS)},
        )
    table = load_table()
    if verdict in table.get("non_transition_verdicts", []):
        return None
    if verdict in OPERATOR_ONLY_VERDICTS and not is_operator:
        raise SubstrateError(
            f"verdict {verdict} is operator-only",
            data={"violated_guard": "operator_only", "state": state},
        )
    if verdict == "supersede" and not target:
        raise SubstrateError(
            "supersede requires --target <change-id>",
            data={"violated_guard": "target_exists", "state": state},
        )
    for t in table["transitions"]:
        if t["from"] == state and t["event"] == verdict:
            return t["to"]
    legal = sorted(
        {t["event"] for t in table["transitions"] if t["from"] == state}
        & set(ALL_VERDICTS)
    )
    # The guard alone names the wall ("legal_verdicts_here: [supersede]"), and
    # the audit measured a caller answering it by picking a different verdict
    # rather than reaching the state they wanted. The ENABLING PATH — computed
    # from this same table — is carried alongside so the refusal can name the
    # door (pact-self-repairing-refusals req.refusal.executable).
    path = enabling_path(state, verdict, is_operator=is_operator)
    raise SubstrateError(
        f"verdict {verdict} is not legal in state {state}",
        data={
            "violated_guard": "legal_transition",
            "state": state,
            "legal_verdicts_here": legal,
            "enabling_path": path.to_dict(),
        },
    )


def legal_verdicts(state: str, *, is_operator: bool = False) -> list[str]:
    """Every verdict `decide` will accept in ``state``, operator-aware.

    Computed from the transition table, so it cannot drift from what
    :func:`legal_transition` enforces. The non-transition verdicts (`defer`,
    `escalate`) are legal everywhere by construction — they record without
    moving state.
    """
    table = load_table()
    verdicts = {
        transition["event"]
        for transition in table["transitions"]
        if transition["from"] == state
    } & set(ALL_VERDICTS)
    verdicts.update(table.get("non_transition_verdicts", []))
    if not is_operator:
        verdicts -= set(OPERATOR_ONLY_VERDICTS)
    return sorted(verdicts)


@dataclass(frozen=True)
class EnablingPath:
    """How to reach a state where a requested verdict becomes legal.

    The audited failure is a refusal that names the violated guard and stops:
    "verdict approve is not legal in state in_progress ... legal_verdicts_here:
    [supersede]". That names the wall, never the door. This is the door —
    computed from the same transition table the guard reads, so a refusal can
    never recommend a verdict the next call rejects.
    """

    state: str
    verdict: str
    legal_here: bool
    reachable: bool
    events: tuple[str, ...] = ()
    states: tuple[str, ...] = ()
    # The table guard on each step, in the same order. `draft --approve` is
    # only reachable through the `tier_2_plus` arm of `gate_post_edit_pass`,
    # so "the tier change that makes approve reachable" is data, not folklore.
    guards: tuple[str | None, ...] = ()
    blockers: tuple[str, ...] = ()
    unreachable_reason: str | None = None

    @property
    def target_state(self) -> str | None:
        return self.states[-1] if self.states else (self.state if self.legal_here else None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "verdict": self.verdict,
            "legal_here": self.legal_here,
            "reachable": self.reachable,
            "events": list(self.events),
            "states": list(self.states),
            "guards": list(self.guards),
            "target_state": self.target_state,
            "blockers": list(self.blockers),
            "unreachable_reason": self.unreachable_reason,
        }


# What each non-verdict transition event means for the caller, as a
# `blocked_by` glossary token. `tasks_terminal` is the audited case: the
# operator wanted `approve` in `in_progress`, and the door is "close the open
# tasks", not "pick a different verdict".
_EVENT_BLOCKERS: dict[str, str] = {
    "gate_post_edit_pass": "manifest_not_admitted",
    "tasks_terminal": "tasks_not_done",
    "evidence_stale": "evidence_stale",
}


def _transitions_from(state: str) -> list[dict[str, Any]]:
    return [t for t in load_table()["transitions"] if t["from"] == state]


def enabling_path(
    state: str,
    verdict: str,
    *,
    is_operator: bool = False,
) -> EnablingPath:
    """The shortest event sequence after which ``verdict`` becomes legal.

    Total over every (state, verdict) pair: either the verdict is legal here,
    or a path exists, or the result explicitly declares it UNREACHABLE with a
    reason. Swept exhaustively by `tests/pact/test_refusal_commands.py`.
    """
    if verdict not in ALL_VERDICTS:
        return EnablingPath(
            state=state,
            verdict=verdict,
            legal_here=False,
            reachable=False,
            unreachable_reason=f"unknown_verdict:{verdict}",
        )
    if verdict in OPERATOR_ONLY_VERDICTS and not is_operator:
        # Not a state problem: no transition sequence grants operator
        # authority, so naming one would be a path the next call still rejects.
        return EnablingPath(
            state=state,
            verdict=verdict,
            legal_here=False,
            reachable=False,
            blockers=(f"operator_only_verdict:{verdict}",),
            unreachable_reason="operator_only",
        )
    if verdict in legal_verdicts(state, is_operator=is_operator):
        return EnablingPath(
            state=state, verdict=verdict, legal_here=True, reachable=True
        )

    # Breadth-first over the transition table; the first arrival wins, so the
    # path is the shortest one the machine actually allows.
    frontier: list[
        tuple[str, tuple[str, ...], tuple[str, ...], tuple[str | None, ...]]
    ] = [(state, (), (), ())]
    seen = {state}
    while frontier:
        current, events, states, guards = frontier.pop(0)
        for transition in _transitions_from(current):
            nxt = transition["to"]
            if nxt in seen:
                continue
            event = transition["event"]
            if event in OPERATOR_ONLY_VERDICTS and not is_operator:
                continue
            next_events = (*events, event)
            next_states = (*states, nxt)
            next_guards = (*guards, transition.get("guard"))
            if verdict in legal_verdicts(nxt, is_operator=is_operator):
                return EnablingPath(
                    state=state,
                    verdict=verdict,
                    legal_here=False,
                    reachable=True,
                    events=next_events,
                    states=next_states,
                    guards=next_guards,
                    blockers=tuple(
                        dict.fromkeys(
                            _EVENT_BLOCKERS.get(name, f"awaiting_verdict:{name}")
                            for name in next_events
                        )
                    ),
                )
            seen.add(nxt)
            frontier.append((nxt, next_events, next_states, next_guards))
    return EnablingPath(
        state=state,
        verdict=verdict,
        legal_here=False,
        reachable=False,
        blockers=(f"terminal_state:{state}",) if state in terminal_states() else (),
        unreachable_reason=(
            "terminal_state" if state in terminal_states() else "no_transition_path"
        ),
    )


def derive_state(
    *,
    valid: bool,
    tier: int,
    tasks_terminal: bool,
    evidence_satisfied: bool,
    decisions: list[DecisionEvent],
    intent_digest: str | None = None,
) -> DerivedState:
    """Derive the lifecycle state from manifest condition + decision history.

    Pure function: callers supply the manifest-condition flags (validity,
    tier, task/evidence posture) and the decision events for this change in
    ledger order. The authored change.status is deliberately NOT an input.
    """
    reasons: list[str] = []
    # A v4 decision approves a specific intent digest, not a mutable path.
    # When a current digest is supplied, undigested legacy events cannot
    # approve a v4 boundary. Pure legacy callers omit ``intent_digest`` and
    # retain their historical in-memory behavior.
    applicable_decisions = [
        decision
        for decision in decisions
        if intent_digest is None
        or decision.intent_digest == intent_digest
    ]

    # Terminal outcomes come from the decision ledger, newest wins; an
    # operator reopen cancels the terminal outcome it follows.
    last_terminal: str | None = None
    for d in applicable_decisions:
        if d.verdict == "approve" and (
            d.to_state == "retired" or d.from_state == "retired"
        ):
            last_terminal = "retired"
        elif d.verdict == "reject":
            last_terminal = "rejected"
        elif d.verdict == "supersede":
            last_terminal = "superseded"
        elif d.verdict == "reopen":
            last_terminal = None
    if last_terminal:
        reasons.append(f"ledger records terminal outcome {last_terminal}")
        return DerivedState(last_terminal, None, reasons)

    if not valid:
        reasons.append("manifest invalid")
        return DerivedState("draft", None, reasons)

    # A proposal revision returns to authored draft until the author changes
    # the intent. That edit changes the digest, so this event then falls out
    # of ``applicable_decisions`` and the next post-edit can propose it again.
    if applicable_decisions:
        latest = applicable_decisions[-1]
        if latest.verdict == "revise" and latest.to_state == "draft":
            reasons.append("ledger requests proposal revision")
            return DerivedState("draft", None, reasons)

    needs_proposal_gate = tier >= 2
    proposal_approved = any(
        d.verdict == "approve" and d.from_state == "proposed"
        for d in applicable_decisions
    )
    if needs_proposal_gate and not proposal_approved:
        reasons.append("tier-2+ change awaiting go-ahead")
        return DerivedState("proposed", "go-ahead", reasons)

    if tasks_terminal and evidence_satisfied:
        reasons.append("all tasks terminal, evidence satisfied")
        return DerivedState("awaiting_review", "review", reasons)
    if tasks_terminal and not evidence_satisfied:
        reasons.append("tasks terminal but evidence gaps remain")
    return DerivedState("in_progress", None, reasons)


# Projection: derived state -> the legacy authored change.status vocabulary,
# kept only as a display cache for v0-3 readers.
STATUS_PROJECTION = {
    "draft": "proposal",
    "proposed": "proposal",
    "in_progress": "implementation",
    "awaiting_review": "verification",
    "retired": "verification",
    "superseded": "verification",
    "rejected": "verification",
}
