"""The generation stamp and the fail-closed verdict (PACT chat-context-audit-filter).

Two defect classes drive these tests, and both pass a naive check:

* **A stamp that reads fresh when nothing was stamped.** Omitting the line,
  substituting a default, reusing the previous turn's value, or inferring
  freshness from the clock all produce output that LOOKS stamped. All four
  are asserted against, and the reuse case is asserted across conversations
  too, so a cache cannot be excused as per-conversation state.
* **A refusal predicate proven on one side only.** "Unverified refuses" is
  green on a filter that refuses everything. Every refusal test here names
  its positive counterpart in the same run.
"""

from __future__ import annotations

import ast
import inspect
import json
import re
from pathlib import Path

import pytest

from okg.substrate.chat import audit_filter as audit_filter_module
from okg.substrate.chat.audit_filter import (
    REFUSAL_UNVERIFIED_OBSERVABILITY,
    STAMP_FIELD,
    STAMP_UNAVAILABLE,
    ContextAuditFilter,
    FilterSettings,
    RefusalSurface,
    TurnRefused,
    render_stamp,
)
from okg.substrate.chat.audit_record import (
    GenerationAgreement,
    observe_generation,
)
from okg.substrate.chat.observability_verdict import (
    VERDICT_SCHEMA_NAME,
    Configuration,
    Verdict,
    VerdictKey,
    is_verified,
    read_verdicts,
    resolve_verdict_path,
    write_verdict,
)

DEPLOYMENT = "cooper"
PRESET = "okg-cooper-analyst"
OWUI_VERSION = "0.11.0"
BASE = "https://okg.example.org/operator/"

G1 = "okg:cooper:branch:default:gen:20260804T035442195199Z:08b1448381a9"
G2 = "okg:cooper:branch:default:gen:20260804T041635485944Z:4eccdc957fb8"

REPO_ROOT = Path(__file__).resolve().parents[3]


def _settings(tmp_path: Path, **overrides: object) -> FilterSettings:
    base: dict[str, object] = {
        "deployment": DEPLOYMENT,
        "preset": PRESET,
        "openwebui_version": OWUI_VERSION,
        "compaction_enabled": False,
        "operator_ui_base": BASE,
        "state_home": tmp_path / "operator-state",
    }
    base.update(overrides)
    return FilterSettings(**base)  # type: ignore[arg-type]


def _key() -> VerdictKey:
    return VerdictKey(DEPLOYMENT, PRESET, OWUI_VERSION)


def _answer(text: str = "Three regions matched.") -> dict[str, object]:
    return {"messages": [{"role": "assistant", "content": text}]}


def _rendered(body: dict) -> str:
    return "\n".join(str(m.get("content", "")) for m in body["messages"])


# --------------------------------------------------------------------------
# The stamp
# --------------------------------------------------------------------------


def test_stamp_present_on_every_graph_answer(tmp_path: Path) -> None:
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(_answer(), envelopes=[{"pinned_generation_id": G1}])

    text = _rendered(out)
    assert f"{STAMP_FIELD}: {G1}" in text
    assert "Three regions matched." in text, "the answer itself survives"

    # Every graph-derived answer, not just the first: a second turn in the
    # same filter is stamped with its own generation.
    out2 = filt.outlet(_answer("Two regions."), envelopes=[{"pinned_generation_id": G2}])
    assert f"{STAMP_FIELD}: {G2}" in _rendered(out2)


def test_stamp_reads_the_compat_generation_field_when_pinned_is_absent(
    tmp_path: Path,
) -> None:
    """``generation_id`` is still emitted alongside the canonical name.

    Reading only the canonical field would render "unavailable" for an
    envelope that plainly carries the generation — an absence claim that is
    false.
    """
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(_answer(), envelopes=[{"generation_id": G1}])

    assert f"{STAMP_FIELD}: {G1}" in _rendered(out)


def test_missing_generation_renders_unavailable_not_default(tmp_path: Path) -> None:
    """All four prohibited behaviors, asserted one by one."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(_answer(), envelopes=[{"results": [{"node_id": "n1"}]}])
    text = _rendered(out)

    # 1. Not omitted: the stamp line is present and names the missing field.
    assert STAMP_FIELD in text
    assert STAMP_UNAVAILABLE in text
    assert "pinned_generation_id" in text

    # 2. No default substituted: nothing generation-shaped is emitted.
    assert not re.search(r"gen:\d{8}T", text)
    assert "unknown" not in text.lower()
    assert "latest" not in text.lower()

    # 3. Not inferred from the clock: no timestamp appears in the stamp.
    assert not re.search(r"\d{4}-\d{2}-\d{2}", text)

    # 4. No per-answer trust_level is claimed (decision B).
    assert "trust_level" not in text


def test_missing_generation_does_not_reuse_prior_turn_stamp(tmp_path: Path) -> None:
    """Turn 1 is stamped g1; turn 2's envelope omits the generation.

    The literal g1 value must appear NOWHERE in turn 2's rendered output.
    The cross-conversation variant follows in the same test so a cache
    cannot be excused as per-conversation state.
    """
    filt = ContextAuditFilter(_settings(tmp_path))

    turn1 = filt.outlet(
        _answer("first"),
        envelopes=[{"pinned_generation_id": G1}],
        conversation_id="conversation-A",
    )
    assert G1 in _rendered(turn1), "the reuse test needs a real prior stamp"

    turn2 = filt.outlet(
        _answer("second"),
        envelopes=[{"results": []}],
        conversation_id="conversation-A",
    )
    text2 = _rendered(turn2)
    assert G1 not in text2
    assert STAMP_UNAVAILABLE in text2

    # CROSS-CONVERSATION, in the SAME filter object and the same process: a
    # cache keyed by conversation would survive the same-conversation arm
    # above and only show up here. Turn 3 is a different conversation whose
    # envelope omits the generation; turn 4 goes back to conversation A.
    turn3 = filt.outlet(
        _answer("third"),
        envelopes=[{"results": []}],
        conversation_id="conversation-B",
    )
    assert G1 not in _rendered(turn3)
    assert STAMP_UNAVAILABLE in _rendered(turn3)

    filt.outlet(
        _answer("fourth"),
        envelopes=[{"pinned_generation_id": G2}],
        conversation_id="conversation-B",
    )
    back = filt.outlet(
        _answer("fifth"),
        envelopes=[{"results": []}],
        conversation_id="conversation-A",
    )
    assert G1 not in _rendered(back), "a per-conversation cache would resurface g1"
    assert G2 not in _rendered(back), "nor the other conversation's stamp"

    # And the structural reason it cannot happen: the stamp is a pure
    # function of one turn's envelopes, so there is no prior value to reuse.
    assert render_stamp(observe_generation([])) is None
    signature = inspect.signature(render_stamp)
    assert list(signature.parameters) == ["observation"]


def test_conflicting_generations_are_named_never_collapsed(tmp_path: Path) -> None:
    """Picking one of two observed generations would be a synthesized stamp."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(
        _answer(),
        envelopes=[{"pinned_generation_id": G1}, {"pinned_generation_id": G2}],
    )

    text = _rendered(out)
    assert "conflicting" in text
    assert G1 in text and G2 in text


def test_no_stamp_when_the_answer_used_no_graph_rows(tmp_path: Path) -> None:
    """"Every answer that consumed a graph read" — an answer that did not, is not stamped."""
    filt = ContextAuditFilter(_settings(tmp_path))

    out = filt.outlet(_answer("hello"), envelopes=[])

    assert STAMP_FIELD not in _rendered(out)
    assert observe_generation([]).agreement is GenerationAgreement.NO_GRAPH_READ


def test_stamp_uses_dogfood_status_vocabulary() -> None:
    """One vocabulary: the stamp spells the field as the status surfaces do.

    Pinned against the two producers by name. If either renames the field,
    this fails rather than leaving an operator to translate between an
    answer and ``okg dogfood status``.
    """
    dogfood = (REPO_ROOT / "src/okg/substrate/cli/dogfood.py").read_text("utf-8")
    # The console's read shapes. The old Health page's loader
    # (routes/health/healthApi.ts) was the reference until PACT
    # console-default-remove-old-views deleted it with the page.
    health = (
        REPO_ROOT
        / "src/okg/operator_ui/web/src/operator-ui/console/model/shared/reads.ts"
    ).read_text("utf-8")
    assert dogfood and health, "an empty reference file would pass every check"

    assert STAMP_FIELD == "generation_id"
    # The dogfood read-authority block, where the CLI prints the generation.
    assert re.search(
        r'"read_authority":\s*\{[^}]*"' + re.escape(STAMP_FIELD) + r'"',
        dogfood,
        re.S,
    ), "okg dogfood status no longer spells the field this way"
    assert f"{STAMP_FIELD}:" in health, "the console no longer spells it this way"


def _executable_mentions(source: str, needle: str) -> list[int]:
    """Line numbers where CODE — never a docstring — mentions ``needle``.

    Prose that explains why a per-answer trust_level is not rendered is not
    a violation of that rule; a string literal or identifier in executable
    code is. Scanning the raw text would conflate the two, so the check is
    over the parsed tree.
    """
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            body = getattr(node, "body", None)
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                docstrings.add(id(body[0].value))

    hits: list[int] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and needle in node.value
        ):
            hits.append(node.lineno)
        elif isinstance(node, ast.Name) and needle in node.id:
            hits.append(node.lineno)
        elif isinstance(node, ast.Attribute) and needle in node.attr:
            hits.append(node.lineno)
    return hits


def test_no_per_answer_trust_level_is_rendered_or_computed() -> None:
    """Decision B, asserted over the parsed module rather than its text."""
    # Positive control first: the scanner must actually find a violation.
    assert _executable_mentions('"""doc trust_level."""\nx = "trust_level"\n', "trust_level") == [2]

    source = inspect.getsource(audit_filter_module)
    assert _executable_mentions(source, "trust_level") == []


# --------------------------------------------------------------------------
# The refusal predicate: compaction enabled AND verdict not verified
# --------------------------------------------------------------------------


def test_unverified_verdict_with_compaction_enabled_refuses_the_turn(
    tmp_path: Path,
) -> None:
    """The refusal arm, with both positive counterparts in the same run."""
    settings = _settings(tmp_path, compaction_enabled=True)
    filt = ContextAuditFilter(settings)

    with pytest.raises(TurnRefused) as raised:
        filt.inlet({"model": "m", "messages": [{"role": "user", "content": "hi"}]})

    assert raised.value.reason == REFUSAL_UNVERIFIED_OBSERVABILITY
    assert "compaction" in str(raised.value)
    assert PRESET in str(raised.value)
    assert OWUI_VERSION in str(raised.value)
    assert not filt.artifact_path.exists(), "a refused turn writes no audit record"

    # Positive counterpart 1: same preset, verdict written -> the turn runs.
    write_verdict(filt.verdict_path, _key(), Configuration.COMPACTION_ON)
    record = ContextAuditFilter(settings).inlet(
        {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    )
    assert record["record_id"]

    # Positive counterpart 2: compaction off -> the turn runs regardless.
    off = ContextAuditFilter(_settings(tmp_path / "off", compaction_enabled=False))
    assert off.inlet({"model": "m", "messages": []})["record_id"]


def test_verified_verdict_allows_the_turn(tmp_path: Path) -> None:
    settings = _settings(tmp_path, compaction_enabled=True)
    filt = ContextAuditFilter(settings)
    write_verdict(filt.verdict_path, _key(), Configuration.COMPACTION_ON)

    record = filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})

    assert filt.refusal() is None
    assert record["compaction_enabled"] is True
    # And the fact that was actually knowable: this payload carries no
    # summary, so it was NOT compacted. One name for both would have said
    # it was.
    assert record["compaction_observed"] is False
    assert len(filt.artifact_path.read_text("utf-8").splitlines()) == 1


def test_compaction_disabled_answers_without_refusal(tmp_path: Path) -> None:
    """The off-configuration answers normally; it is not an outage.

    An unconditional refusal would contradict the design's stated
    consequence — compaction goes off for an unprovable preset, the
    deployment keeps answering.
    """
    filt = ContextAuditFilter(_settings(tmp_path, compaction_enabled=False))
    assert read_verdicts(filt.verdict_path) == (), "no verdict recorded at all"

    record = filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})

    assert filt.refusal() is None
    assert record["compaction_enabled"] is False

    # The OTHER preset, same instance, with compaction on, still refuses —
    # so this pass is not a filter that permits everything.
    on = ContextAuditFilter(_settings(tmp_path, compaction_enabled=True))
    with pytest.raises(TurnRefused):
        on.inlet({"model": "m", "messages": []})


def test_verified_off_configuration_does_not_verify_the_on_configuration(
    tmp_path: Path,
) -> None:
    """Two-valued per configuration: one arm never stands in for the other."""
    filt = ContextAuditFilter(_settings(tmp_path, compaction_enabled=True))
    write_verdict(filt.verdict_path, _key(), Configuration.COMPACTION_OFF)

    assert is_verified(filt.verdict_path, _key(), Configuration.COMPACTION_OFF)
    assert not is_verified(filt.verdict_path, _key(), Configuration.COMPACTION_ON)
    with pytest.raises(TurnRefused):
        filt.inlet({"model": "m", "messages": []})


# --------------------------------------------------------------------------
# The verdict artifact
# --------------------------------------------------------------------------


def test_missing_verdict_key_reads_unverified(tmp_path: Path) -> None:
    """Absence is the ENCODING of unverified — including a version bump."""
    path = resolve_verdict_path(DEPLOYMENT, state_home=tmp_path / "operator-state")

    # No file at all.
    assert not is_verified(path, _key(), Configuration.COMPACTION_ON)

    write_verdict(path, _key(), Configuration.COMPACTION_ON)
    assert is_verified(path, _key(), Configuration.COMPACTION_ON)

    # A different Open WebUI version is a DIFFERENT KEY with no record, so
    # every verdict is invalidated by construction on an upgrade.
    upgraded = VerdictKey(DEPLOYMENT, PRESET, "0.12.0")
    assert not is_verified(path, upgraded, Configuration.COMPACTION_ON)
    # ... and so is a different preset, and a different deployment.
    assert not is_verified(
        path, VerdictKey(DEPLOYMENT, "other-preset", OWUI_VERSION),
        Configuration.COMPACTION_ON,
    )
    assert not is_verified(
        path, VerdictKey("other", PRESET, OWUI_VERSION), Configuration.COMPACTION_ON
    )


def test_unrecognized_verdict_schema_version_reads_unverified(tmp_path: Path) -> None:
    """An unknown major reads unverified — never verified, never a parse guess."""
    path = tmp_path / "verdicts.jsonl"
    path.write_text(
        json.dumps(
            {
                "schema": VERDICT_SCHEMA_NAME,
                "schema_version": "99.0",
                "deployment": DEPLOYMENT,
                "preset": PRESET,
                "openwebui_version": OWUI_VERSION,
                "verdict": "verified_compaction_on",
                "measured_at": "2026-08-04T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    assert not is_verified(path, _key(), Configuration.COMPACTION_ON)
    assert read_verdicts(path) == ()

    # The same record at a recognized version IS verified, so the refusal
    # above is about the version and not about the record being unreadable
    # for some other reason.
    write_verdict(path, _key(), Configuration.COMPACTION_ON)
    assert is_verified(path, _key(), Configuration.COMPACTION_ON)


def test_filter_never_writes_a_positive_verdict(tmp_path: Path) -> None:
    """A runtime component cannot know whether the operator-run proof happened."""
    settings = _settings(tmp_path, compaction_enabled=True)
    filt = ContextAuditFilter(settings)
    write_verdict(filt.verdict_path, _key(), Configuration.COMPACTION_ON)
    before = filt.verdict_path.read_bytes()

    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})
    filt.outlet(_answer(), envelopes=[{"pinned_generation_id": G1}])

    assert filt.verdict_path.read_bytes() == before, "the filter wrote a verdict"

    # Structural, not just behavioral: the writer is not reachable from the
    # filter module at all.
    source = inspect.getsource(audit_filter_module)
    assert "write_verdict" not in source
    assert not hasattr(audit_filter_module, "write_verdict")

    # And a filter running against a state dir with NO verdict file leaves
    # none behind — it refuses instead of writing itself a permission slip.
    fresh = ContextAuditFilter(_settings(tmp_path / "fresh", compaction_enabled=True))
    with pytest.raises(TurnRefused):
        fresh.inlet({"model": "m", "messages": []})
    assert not fresh.verdict_path.exists()


def test_verdict_is_readable_by_chat_sync(tmp_path: Path) -> None:
    """The sibling reads the artifact by path, key tuple, and vocabulary.

    This test stands in for chat-sync-projection: it locates the artifact
    from the deployment name alone (no chat-context-audit-filter context),
    reads the pinned fields, and applies the gate the sync verb applies.
    """
    state_home = tmp_path / "operator-state"
    path = resolve_verdict_path(DEPLOYMENT, state_home=state_home)
    written = write_verdict(path, _key(), Configuration.COMPACTION_ON)

    # Located the same way the sibling will locate it.
    assert path == resolve_verdict_path(DEPLOYMENT, state_home=state_home)
    assert path.name == "verdicts.jsonl"

    records = read_verdicts(path)
    assert len(records) == 1
    record = records[0]
    assert record.key == _key()
    assert record.verdict.value == "verified_compaction_on"
    assert record.measured_at == written.measured_at and record.measured_at
    assert record.schema_version

    # The gate the sync verb applies, both ways.
    def compaction_allowed(preset: str, version: str) -> bool:
        return is_verified(
            path,
            VerdictKey(DEPLOYMENT, preset, version),
            Configuration.COMPACTION_ON,
        )

    assert compaction_allowed(PRESET, OWUI_VERSION) is True
    assert compaction_allowed(PRESET, "0.12.0") is False
    assert compaction_allowed("unproven-preset", OWUI_VERSION) is False

    # The on-disk record carries exactly the pinned field set.
    raw = json.loads(path.read_text("utf-8").splitlines()[0])
    assert set(raw) == {
        "schema",
        "schema_version",
        "deployment",
        "preset",
        "openwebui_version",
        "verdict",
        "measured_at",
    }


def test_refusal_surface_assistant_turn_returns_the_refusal_as_the_answer(
    tmp_path: Path,
) -> None:
    """The PACT's re-specification path is wired, not an unreachable enum.

    v0.11.0 measured the raised exception as user-visible, so RAISE is the
    default. A future version that swallows it needs the other surface to
    be a mechanism rather than a comment — and an unwired branch is
    untested code, not a mechanism held in reserve.
    """
    raising = ContextAuditFilter(_settings(tmp_path, compaction_enabled=True))
    with pytest.raises(TurnRefused):
        raising.inlet_or_refusal_message({"model": "m", "messages": []})

    answering = ContextAuditFilter(
        _settings(
            tmp_path,
            compaction_enabled=True,
            refusal_surface=RefusalSurface.ASSISTANT_TURN,
        )
    )
    record, message = answering.inlet_or_refusal_message(
        {"model": "m", "messages": []}
    )

    assert record is None
    assert message is not None, "never (None, None): a silent skip is the failure"
    assert message["role"] == "assistant"
    assert REFUSAL_UNVERIFIED_OBSERVABILITY in message["content"]
    assert not answering.artifact_path.exists()

    # The positive side, on the same surface: a verified verdict answers.
    write_verdict(answering.verdict_path, _key(), Configuration.COMPACTION_ON)
    record, message = answering.inlet_or_refusal_message(
        {"model": "m", "messages": [{"role": "user", "content": "q"}]}
    )
    assert message is None and record is not None


def test_unverified_is_expressed_by_absence_not_by_a_value(tmp_path: Path) -> None:
    """The PRODUCER side of "absent means unverified".

    The reader treating a missing key as unverified is only half the
    contract. The other half is that a failed arm leaves NOTHING behind:
    there is no negative value to write, so a proof run that could not prove
    an arm expresses that by the absence of the arm's record. A writer with
    an "unverified" value would give a later reader something to compare
    equal to, which is how absence gets dressed as agreement.
    """
    # There is no unverified value in the vocabulary, by construction.
    assert {v.value for v in Verdict} == {
        "verified_compaction_on",
        "verified_compaction_off",
    }
    assert not any("unverified" in v.value for v in Verdict)

    path = resolve_verdict_path(DEPLOYMENT, state_home=tmp_path / "operator-state")

    # An arm that did not pass writes nothing at all — not a file, not a row.
    assert not path.exists()
    assert not is_verified(path, _key(), Configuration.COMPACTION_ON)
    assert not is_verified(path, _key(), Configuration.COMPACTION_OFF)

    # One arm passes. The OTHER arm's absence still reads unverified, so a
    # half-proved key cannot be mistaken for a fully proved one.
    write_verdict(path, _key(), Configuration.COMPACTION_OFF)
    assert is_verified(path, _key(), Configuration.COMPACTION_OFF)
    assert not is_verified(path, _key(), Configuration.COMPACTION_ON)
    assert len(read_verdicts(path)) == 1

    # A truncated or malformed line is unverified too, not an exception a
    # caller could catch into a default.
    with path.open("a", encoding="utf-8") as handle:
        handle.write('{"schema": "okg.chat.observability_verdict", "sch\n')
    assert not is_verified(path, _key(), Configuration.COMPACTION_ON)
    assert len(read_verdicts(path)) == 1


def test_the_named_positive_writer_is_the_live_proof_task() -> None:
    """The only positive writer is the operator-run proof, by name."""
    proof_writer = REPO_ROOT / "tests/live/chat/write_proof_verdict.py"
    assert proof_writer.exists(), "the named writer does not exist"
    source = proof_writer.read_text("utf-8")
    assert "write_verdict(" in source, "the named writer does not write"
    # It refuses to write for an arm whose assertion did not pass.
    assert "_proof_passes" in source
    assert "no verdict written for it" in source

    # And no other module in the substrate writes one.
    substrate = REPO_ROOT / "src/okg/substrate/chat"
    writers = [
        path.name
        for path in substrate.glob("*.py")
        if "write_verdict" in path.read_text("utf-8")
    ]
    assert writers == ["observability_verdict.py"], (
        f"write_verdict is reachable from {writers}"
    )


def test_verdict_artifact_lives_under_operator_state(tmp_path: Path) -> None:
    path = resolve_verdict_path(DEPLOYMENT, state_home=tmp_path / "operator-state")

    assert path.is_relative_to(tmp_path / "operator-state")
    assert DEPLOYMENT in path.parts
    assert path.suffix == ".jsonl"
