"""Do the recorded bytes equal the bytes the model API received?

This is the proof the whole PACT turns on. An audit hook that fires before
compaction records text the model never saw and would read as an audit while
auditing nothing — the absence-dressed-as-agreement failure mode, in the one
place where it would be invisible.

The comparison, stated once
---------------------------
IN-SCOPE SUBTREE: the message array plus the model identifier. Both sides
are re-serialized with ``json.dumps(sort_keys=True, separators=(",", ":"),
ensure_ascii=False)`` before digesting, and the raw recorded body is kept in
the evidence next to the digests. Canonicalization is the DEFINITION of the
comparison, not an escape hatch: a dict-vs-HTTP-bytes comparison with no
canonicalization fails on serialization form even for a correct filter, and
a difference confined to serialization form still fails here, because both
sides are re-serialized from parsed values.

Vacuity, twice guarded
----------------------
* The ON arm asserts compaction ACTUALLY FIRED before it compares — fewer
  messages than were sent, or a compaction summary message present — and
  fails as vacuous otherwise. A conversation too short to compact makes
  equality pass while proving nothing.
* Every arm asserts it found the captures and records it is comparing.
  Zero-vs-zero is a failed verification, not a pass.

Running it
----------
Operator-run, not collected by the CI fast lane (see ``conftest.py``). Two
arms, one command each::

    uv run python tests/live/chat/drive_instance.py --arm on  --out .scratch/live-on
    uv run python tests/live/chat/drive_instance.py --arm off --out .scratch/live-off
    OKG_CHAT_LIVE_ON=.scratch/live-on OKG_CHAT_LIVE_OFF=.scratch/live-off \\
      uv run --extra dev pytest tests/live/chat/test_post_compaction_audit_equality.py -q

The verdict is written by ``--write-verdict``; a proof run that does not
write the verdict leaves compaction disabled everywhere and has not
finished.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

ON_ENV = "OKG_CHAT_LIVE_ON"
OFF_ENV = "OKG_CHAT_LIVE_OFF"


def _arm(env: str) -> dict[str, Any]:
    root = os.environ.get(env)
    if not root:
        pytest.fail(
            f"{env} is unset. This proof compares artifacts a live run "
            f"produced; without them there is nothing to compare and a pass "
            f"would mean nothing."
        )
    out = Path(root)
    run = json.loads((out / "run.json").read_text("utf-8"))
    audit = Path(run["audit_artifact"])
    captures = Path(run["captures"])
    records = [
        json.loads(line)
        for line in audit.read_text("utf-8").splitlines()
        if line.strip()
    ]
    caps = [
        json.loads(line)
        for line in captures.read_text("utf-8").splitlines()
        if line.strip()
    ]
    return {"run": run, "records": records, "captures": caps, "root": out}


def _digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _user_turns(arm: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """The user-turn records and the user-turn captures, paired by order.

    Auxiliary calls are excluded on BOTH sides by their label, never by
    position: title generation, tag generation, and the compaction
    summarization call all reach the same endpoint, and comparing one of
    those as the user's turn is exactly the mistake the labeling exists to
    prevent.
    """
    records = [r for r in arm["records"] if r.get("task_kind") == "user_turn"]
    captures = [c for c in arm["captures"] if c.get("task_kind") == "user_turn"]
    return records, captures


def _assert_paired(records: list[dict], captures: list[dict]) -> None:
    assert records, "no audit records: the filter never ran"
    assert captures, "no captures: the model endpoint was never called"
    assert len(records) == len(captures), (
        f"{len(records)} audited user turns vs {len(captures)} captured — "
        "the two sides are not describing the same conversation"
    )


def test_audit_payload_equals_recorded_request_body() -> None:
    """ON arm: compaction fired, and the audit equals what was sent."""
    arm = _arm(ON_ENV)
    assert arm["run"]["compaction_enabled"] is True, "this is the ON arm"
    records, captures = _user_turns(arm)
    _assert_paired(records, captures)

    # (a) COMPACTION ACTUALLY FIRED. Without this the equality below can
    # pass on a conversation that was never compacted, which proves nothing
    # about the ordering this PACT exists to establish.
    sent_counts = [len(json.loads(r["payload"])["messages"] or []) for r in records]
    fired = False
    reasons: list[str] = []
    for index, capture in enumerate(captures):
        messages = json.loads(capture["raw_body"]).get("messages") or []
        summary_present = any(
            isinstance(m.get("content"), str)
            and "[CONVERSATION SUMMARY]" in m["content"]
            for m in messages
        )
        # The conversation grows by two messages per turn (user + assistant),
        # so turn i would carry 2i+1 messages uncompacted.
        uncompacted = 2 * index + 1
        if summary_present or len(messages) < uncompacted:
            fired = True
            reasons.append(
                f"turn {index}: {len(messages)} messages vs {uncompacted} "
                f"uncompacted, summary_present={summary_present}"
            )
    assert fired, (
        "compaction never fired in the ON arm, so this equality would be "
        f"VACUOUS. message counts per turn: {sent_counts}"
    )

    # (b) The canonical digests match, turn by turn.
    for index, (record, capture) in enumerate(zip(records, captures, strict=True)):
        recorded = record["payload"]
        sent = capture["canonical_payload"]
        assert _digest(recorded) == record["payload_sha256"], (
            "the record's own digest does not cover its own payload"
        )
        assert _digest(recorded) == _digest(sent), (
            f"turn {index}: the audited payload is not the payload sent.\n"
            f"audited: {recorded[:400]}\n"
            f"sent:    {sent[:400]}"
        )


def test_audit_payload_equals_request_body_compaction_disabled() -> None:
    """OFF arm: the fallback configuration is MEASURED, not assumed safe.

    Compaction is not the only post-inlet mutation Open WebUI can perform,
    and the off configuration is where every unprovable preset lands. No
    compaction-fired guard here — there is nothing to fire — but the same
    canonical equality has to hold.
    """
    arm = _arm(OFF_ENV)
    assert arm["run"]["compaction_enabled"] is False, "this is the OFF arm"
    records, captures = _user_turns(arm)
    _assert_paired(records, captures)

    # The negative control for the arm itself: nothing was compacted, so the
    # conversation must have grown monotonically. If it shrank, the arm was
    # mislabeled and the "no guard needed" reasoning does not apply.
    counts = [c["message_count"] for c in captures]
    assert counts == sorted(counts), f"the OFF arm shows compaction: {counts}"
    assert counts[-1] > counts[0], "the OFF arm conversation never grew"

    for index, (record, capture) in enumerate(zip(records, captures, strict=True)):
        assert _digest(record["payload"]) == _digest(capture["canonical_payload"]), (
            f"turn {index}: the audited payload is not the payload sent.\n"
            f"audited: {record['payload'][:400]}\n"
            f"sent:    {capture['canonical_payload'][:400]}"
        )


def test_the_audited_payload_is_missing_what_compaction_dropped() -> None:
    """The falsification control: a PRE-compaction record would not pass.

    Digest equality proves the audit matches what was sent. It does not, on
    its own, prove the audit is post-compaction — two identical records
    taken at two points that happened not to differ would also match. This
    test closes that: the driver's turn-0 text is verbatim in the
    conversation, and a payload recorded BEFORE compaction would still
    contain it. After compaction it is gone, replaced by the summary.

    Couples to ``drive_instance.py``'s message text on purpose; both halves
    of this harness are one artifact.
    """
    arm = _arm(ON_ENV)
    records, captures = _user_turns(arm)
    _assert_paired(records, captures)

    early_marker = "Turn 0: region-0-0"
    assert early_marker in records[0]["payload"], (
        "the marker this control depends on is not in the conversation at all"
    )

    compacted = [
        (index, record)
        for index, record in enumerate(records)
        if "[CONVERSATION SUMMARY]" in record["payload"]
    ]
    assert compacted, "no turn shows a compaction summary; nothing to falsify against"

    for index, record in compacted:
        assert early_marker not in record["payload"], (
            f"turn {index} was compacted, yet its audit record still carries "
            "turn 0 verbatim — the record was taken BEFORE compaction"
        )
        assert early_marker not in captures[index]["canonical_payload"], (
            "the outbound request still carries turn 0, so the arm is not "
            "actually compacted and this control is vacuous"
        )


def test_auxiliary_calls_are_labeled_and_never_compared_as_the_user_turn() -> None:
    """The labeling this proof depends on, asserted on live captures."""
    arm = _arm(ON_ENV)
    labels = {c["task_kind"] for c in arm["captures"]}
    assert "user_turn" in labels
    assert labels - {"user_turn"}, (
        "no auxiliary model call was captured, so the claim that the labels "
        "separate them is untested on this run"
    )

    # An auxiliary capture must never digest-match an audited user turn.
    audited = {
        _digest(r["payload"])
        for r in arm["records"]
        if r.get("task_kind") == "user_turn"
    }
    for capture in arm["captures"]:
        if capture["task_kind"] == "user_turn":
            continue
        assert _digest(capture["canonical_payload"]) not in audited


def test_auxiliary_model_calls_do_not_reach_the_filter_at_this_version() -> None:
    """A MEASURED SCOPE LIMIT, asserted so it cannot drift silently.

    Open WebUI's own model calls — title generation, tag generation, and the
    compaction summarizer — are issued through ``generate_chat_completion``
    directly, NOT through ``process_chat_payload``, so they never traverse
    the Filter inlet (measured at v0.11.0, and again on the 0.11.4
    compatibility image on 2026-09-30, where this test passed against the
    live arms of live-chat-drivers-0114). The audit therefore covers USER TURNS
    ONLY, and the auxiliary text this instance sent to a model is not in the
    artifact.

    That is a real gap in coverage and it is recorded here rather than left
    to be discovered. The assertion is two-sided on purpose: if a later
    version starts routing auxiliary calls through the inlet, the second
    half goes red and the scope limit gets revisited instead of quietly
    becoming false.
    """
    arm = _arm(ON_ENV)

    auxiliary_captures = [
        c for c in arm["captures"] if c["task_kind"] != "user_turn"
    ]
    assert auxiliary_captures, (
        "no auxiliary model call was captured, so this test measures nothing"
    )
    assert any(
        c["task_kind"] == "context_compaction" for c in auxiliary_captures
    ), "the compaction summarizer never ran; the ON arm is not what it claims"

    inlets = [r for r in arm["records"] if r["record_kind"] == "inlet"]
    assert inlets, "no inlet records at all"
    audited_kinds = {r["task_kind"] for r in inlets}
    assert audited_kinds == {"user_turn"}, (
        "auxiliary calls now reach the filter inlet — the recorded scope "
        f"limit is stale. Observed audited kinds: {sorted(audited_kinds)}"
    )

    # And the payloads confirm it rather than the labels alone: no audit
    # record matches any auxiliary capture.
    audited = {_digest(r["payload"]) for r in inlets}
    for capture in auxiliary_captures:
        assert _digest(capture["canonical_payload"]) not in audited


def test_every_user_turn_has_a_paired_completion_record() -> None:
    """One turn, TWO records — checked on the artifact a live run produced.

    A green equality over an artifact holding inlet records only would mean
    the observed generation — the entire reason the record is split in two —
    is produced by nothing on the path that actually runs.
    """
    for env in (ON_ENV, OFF_ENV):
        arm = _arm(env)
        inlets = [r for r in arm["records"] if r["record_kind"] == "inlet"]
        completions = [r for r in arm["records"] if r["record_kind"] == "completion"]
        assert inlets, f"{env}: no inlet records"
        assert len(completions) == len(inlets), (
            f"{env}: {len(inlets)} inlet records but {len(completions)} "
            "completion records"
        )
        inlet_ids = [r["record_id"] for r in inlets]
        assert len(set(inlet_ids)) == len(inlet_ids), f"{env}: duplicate record_id"
        assert {r["record_id"] for r in completions} == set(inlet_ids), (
            f"{env}: a completion record does not join to any inlet record"
        )
        for record in completions:
            assert "trust_level" not in record
            assert record["generation_agreement"] in {
                "no_graph_read",
                "single",
                "missing",
                "conflicting",
            }
