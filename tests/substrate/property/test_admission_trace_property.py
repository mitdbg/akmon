from __future__ import annotations

from hypothesis import given, strategies as st

from okg.substrate.admission_trace import TERMINAL_STATUSES, validate_event_sequence


EVENT_TYPES = [
    "created",
    "submitted",
    "duplicate_coalesced",
    "refs_attached",
    "accepted",
    "rejected",
    "quarantined",
    "published",
    "blocked",
    "failed",
]


@given(
    mode=st.sampled_from(["fast_track", "review", "quarantine", "direct"]),
    events=st.lists(st.sampled_from(EVENT_TYPES), min_size=0, max_size=8),
)
def test_admission_trace_relation_preserves_terminal_safety(
    mode: str,
    events: list[str],
) -> None:
    final_status = _derived_final_status(events)
    result = validate_event_sequence(
        mode=mode,
        final_status=final_status,
        event_types=events,
    )
    if not result.ok:
        return

    if "published" in events:
        assert "accepted" in events
        assert "refs_attached" in events
        assert events.index("accepted") < events.index("published")
    if "accepted" in events:
        assert "refs_attached" in events
    for terminal in TERMINAL_STATUSES:
        if terminal in events:
            index = events.index(terminal)
            later_terminals = [
                event for event in events[index + 1:]
                if event in TERMINAL_STATUSES
            ]
            assert later_terminals == []


def _derived_final_status(events: list[str]) -> str:
    status = "absent"
    for event in events:
        if event == "created":
            status = "pending"
        elif event == "accepted":
            status = "accepted"
        elif event == "rejected":
            status = "rejected"
        elif event == "quarantined":
            status = "quarantined"
        elif event == "published":
            status = "published"
        elif event == "blocked":
            status = "blocked"
        elif event == "failed":
            status = "failed"
    return status
