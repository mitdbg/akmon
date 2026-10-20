"""The two canonicalizers must agree byte for byte.

The audit filter exists in two copies: the substrate module
(``okg.substrate.chat.audit_record``) and the file installed INTO Open WebUI
(``tests/live/chat/okg_audit_filter_function.py``), which runs where ``okg``
is not importable. The recording model endpoint carries a third copy for the
same reason.

Duplication is the price of the deployment boundary; an UNCHECKED
duplication is not. If the two serializations drift, the post-compaction
equality proof compares two canonical forms that were never the same
function — and it would fail, or pass, for a reason that has nothing to do
with what Open WebUI sent. These tests are the pin: same inputs, byte-equal
outputs, or red.

This is the pattern ``test_deep_links.py`` already uses against the
console's own hash emitter, applied to the other cross-boundary emitter in
this PACT.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from okg.substrate.chat.audit_record import (
    AUDIT_SCHEMA_NAME,
    AUDIT_SCHEMA_VERSION,
    PAYLOAD_CANONICALIZATION,
    TaskKind,
    canonical_payload,
    payload_sha256,
    task_kind_from_metadata,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
LIVE_FILTER = REPO_ROOT / "tests/live/chat/okg_audit_filter_function.py"
RECORDER = REPO_ROOT / "tests/live/chat/recording_model_endpoint.py"


def _load(path: Path, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def live_filter() -> ModuleType:
    assert LIVE_FILTER.exists(), "the installed-into-Open-WebUI half is missing"
    return _load(LIVE_FILTER, "okg_audit_filter_function")


@pytest.fixture(scope="module")
def recorder() -> ModuleType:
    assert RECORDER.exists(), "the recording endpoint is missing"
    return _load(RECORDER, "recording_model_endpoint")


#: Bodies chosen for the ways two serializers usually diverge: key order,
#: non-ASCII, embedded quotes and newlines, nested content parts, null and
#: numeric values, and an empty message list.
BODIES = [
    {"model": "m", "messages": [{"role": "user", "content": "hello"}]},
    {"messages": [{"content": "hello", "role": "user"}], "model": "m"},
    {"model": "qwen3:32b", "messages": [{"role": "user", "content": "région ⟨x⟩ 名前"}]},
    {"model": "m", "messages": [{"role": "user", "content": 'quote " and \\ and \n'}]},
    {
        "model": "m",
        "messages": [
            {"role": "system", "content": "[CONVERSATION SUMMARY]\nearlier turns"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "look"},
                    {"type": "image_url", "image_url": {"url": "data:,"}},
                ],
            },
        ],
    },
    {"model": None, "messages": []},
    {"model": "m", "messages": [{"role": "user", "content": "x", "n": 1, "z": None}]},
    # Instance internals that must NOT reach the canonical form.
    {
        "model": "m",
        "messages": [{"role": "user", "content": "x"}],
        "chat_id": "c",
        "session_id": "s",
        "features": {"web_search": True},
        "tools": [{"type": "function"}],
    },
]


def test_live_filter_canonicalization_agrees_with_the_module(
    live_filter: ModuleType,
) -> None:
    assert BODIES, "an empty body set would pass this check vacuously"
    for body in BODIES:
        mine = canonical_payload(body)
        theirs = live_filter.canonical_payload(body)
        assert mine == theirs, f"canonical form drifted for {body!r}"
        assert payload_sha256(mine) == __import__("hashlib").sha256(
            theirs.encode("utf-8")
        ).hexdigest()


def test_recording_endpoint_canonicalization_agrees_with_the_module(
    recorder: ModuleType,
) -> None:
    for body in BODIES:
        assert canonical_payload(body) == recorder.canonical_payload(body)


def test_canonical_form_drops_instance_internals(live_filter: ModuleType) -> None:
    """The negative that makes the agreement meaningful."""
    body = BODIES[-1]
    payload = json.loads(live_filter.canonical_payload(body))

    assert set(payload) == {"messages", "model"}
    for internal in ("chat_id", "session_id", "features", "tools"):
        assert internal not in live_filter.canonical_payload(body)


def test_live_filter_task_labels_agree_with_the_module(
    live_filter: ModuleType,
) -> None:
    cases: list[tuple[dict | None, TaskKind]] = [
        (None, TaskKind.USER_TURN),
        ({}, TaskKind.USER_TURN),
        ({"task": None}, TaskKind.USER_TURN),
        ({"task": "title_generation"}, TaskKind.TITLE_GENERATION),
        ({"task": "tags_generation"}, TaskKind.TAGS_GENERATION),
        ({"task": "context_compaction"}, TaskKind.CONTEXT_COMPACTION),
        ({"task": "a_task_added_next_version"}, TaskKind.OTHER_AUXILIARY),
    ]
    for metadata, expected in cases:
        assert task_kind_from_metadata(metadata) is expected
        assert live_filter.task_kind_from_metadata(metadata) == expected.value


def test_live_filter_writes_the_declared_schema(live_filter: ModuleType) -> None:
    """Same artifact kind, same version, same canonicalization statement."""
    assert live_filter.SCHEMA == AUDIT_SCHEMA_NAME
    assert live_filter.SCHEMA_VERSION == AUDIT_SCHEMA_VERSION

    source = LIVE_FILTER.read_text("utf-8")
    assert PAYLOAD_CANONICALIZATION.split("(", 1)[0] in source


def test_live_filter_record_is_readable_by_the_substrate_reader(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """The end of the chain: what the container writes, the substrate reads."""
    from okg.substrate.chat.audit_record import read_records

    artifact = tmp_path / "records-001.jsonl"
    filt = live_filter.Filter()
    filt.valves.deployment = "cooper"
    filt.valves.preset = "okg-cooper-analyst"
    filt.valves.openwebui_version = "0.11.0"
    filt.valves.artifact_path = str(artifact)

    body = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    filt.inlet(dict(body), __metadata__={"chat_id": "c1", "message_id": "m1"})
    filt.inlet(dict(body), __metadata__={"task": "title_generation"})

    records = read_records(artifact)
    assert len(records) == 2
    assert records[0]["task_kind"] == TaskKind.USER_TURN.value
    assert records[1]["task_kind"] == TaskKind.TITLE_GENERATION.value
    assert records[0]["record_id"] != records[1]["record_id"]
    assert records[1]["record_seq"] == records[0]["record_seq"] + 1
    for record in records:
        assert record["payload_sha256"] == payload_sha256(record["payload"])


def test_live_filter_verdict_reader_agrees_with_the_substrate_reader(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """The container reads the verdict the same way the substrate does.

    Both readers answer one question — "has this exact key been proved for
    compaction-on?" — and both must answer False in every absence case. A
    container-side reader that answered True where the substrate answered
    False would let a preset run with compaction on and an unverified
    audit, which is the single outcome this PACT forbids.
    """
    from okg.substrate.chat.observability_verdict import (
        Configuration,
        VerdictKey,
        is_verified,
        write_verdict,
    )

    path = tmp_path / "verdicts.jsonl"
    key = VerdictKey("cooper", "okg-cooper-analyst", "0.11.0")

    def both(k: VerdictKey) -> tuple[bool, bool]:
        return (
            is_verified(path, k, Configuration.COMPACTION_ON),
            live_filter.is_verified_compaction_on(
                str(path), k.deployment, k.preset, k.openwebui_version
            ),
        )

    # No file at all.
    assert both(key) == (False, False)

    # The other configuration only.
    write_verdict(path, key, Configuration.COMPACTION_OFF)
    assert both(key) == (False, False)

    # The right one.
    write_verdict(path, key, Configuration.COMPACTION_ON)
    assert both(key) == (True, True)

    # A different version, preset, and deployment are different KEYS.
    assert both(VerdictKey("cooper", "okg-cooper-analyst", "0.12.0")) == (False, False)
    assert both(VerdictKey("cooper", "other", "0.11.0")) == (False, False)
    assert both(VerdictKey("other", "okg-cooper-analyst", "0.11.0")) == (False, False)

    # An unrecognized major reads unverified on both sides.
    unknown = tmp_path / "unknown.jsonl"
    unknown.write_text(
        json.dumps(
            {
                "schema": "okg.chat.observability_verdict",
                "schema_version": "99.0",
                "deployment": key.deployment,
                "preset": key.preset,
                "openwebui_version": key.openwebui_version,
                "verdict": "verified_compaction_on",
                "measured_at": "2026-08-04T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    assert not is_verified(unknown, key, Configuration.COMPACTION_ON)
    assert not live_filter.is_verified_compaction_on(
        str(unknown), key.deployment, key.preset, key.openwebui_version
    )


def test_live_filter_refuses_a_turn_with_compaction_on_and_no_verdict(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """The refusal predicate, proven on BOTH sides in one test."""
    artifact = tmp_path / "records-001.jsonl"
    filt = live_filter.Filter()
    filt.valves.deployment = "cooper"
    filt.valves.preset = "okg-cooper-analyst"
    filt.valves.openwebui_version = "0.11.0"
    filt.valves.artifact_path = str(artifact)
    filt.valves.verdict_path = str(tmp_path / "verdicts.jsonl")
    filt.valves.compaction_enabled = True

    body = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    with pytest.raises(RuntimeError) as raised:
        filt.inlet(dict(body))
    assert "verified_compaction_on" in str(raised.value)
    assert not artifact.exists(), "a refused turn writes no audit record"

    # Positive side 1: verdict present -> the turn runs.
    from okg.substrate.chat.observability_verdict import (
        Configuration,
        VerdictKey,
        write_verdict,
    )

    write_verdict(
        Path(filt.valves.verdict_path),
        VerdictKey("cooper", "okg-cooper-analyst", "0.11.0"),
        Configuration.COMPACTION_ON,
    )
    filt.inlet(dict(body))
    assert artifact.exists()

    # Positive side 2: compaction off with NO verdict -> the turn runs.
    off = live_filter.Filter()
    off.valves.deployment = "cooper"
    off.valves.preset = "okg-cooper-analyst"
    off.valves.openwebui_version = "0.11.0"
    off.valves.artifact_path = str(tmp_path / "off.jsonl")
    off.valves.verdict_path = str(tmp_path / "nothing-here.jsonl")
    off.valves.compaction_enabled = False
    off.inlet(dict(body))
    assert (tmp_path / "off.jsonl").exists()


def _configured(live_filter: ModuleType, tmp_path: Path, **valves: object):
    filt = live_filter.Filter()
    filt.valves.deployment = "cooper"
    filt.valves.preset = "okg-cooper-analyst"
    filt.valves.openwebui_version = "0.11.0"
    filt.valves.artifact_path = str(tmp_path / "records-001.jsonl")
    filt.valves.verdict_path = str(tmp_path / "verdicts" / "verdicts.jsonl")
    for name, value in valves.items():
        setattr(filt.valves, name, value)
    return filt


def test_live_filter_writes_a_completion_record_per_turn(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """One turn, TWO records — in the code that actually runs.

    The two-record split is the whole justification for the observed
    generation living where it was knowable. An outlet that returns the
    body unchanged means the artifact holds inlet records only and the
    generation the turn observed is produced by nothing.
    """
    from okg.substrate.chat.audit_record import RecordKind, read_records

    filt = _configured(live_filter, tmp_path)
    meta = {"chat_id": "c1", "message_id": "m1"}
    body = {"model": "m", "messages": [{"role": "user", "content": "regions?"}]}

    filt.inlet(dict(body), __metadata__=dict(meta))
    answered = {
        "messages": [
            {"role": "user", "content": "regions?"},
            {
                "role": "tool",
                "content": json.dumps(
                    {"pinned_generation_id": "okg:cooper:gen:1", "results": []}
                ),
            },
            {"role": "assistant", "content": "Two regions."},
        ]
    }
    filt.outlet(answered, __metadata__=dict(meta))

    records = read_records(tmp_path / "records-001.jsonl")
    assert [r["record_kind"] for r in records] == [
        RecordKind.INLET.value,
        RecordKind.COMPLETION.value,
    ]
    assert records[1]["record_id"] == records[0]["record_id"], "the join key"
    assert records[1]["observed_generation_id"] == "okg:cooper:gen:1"
    assert records[1]["generation_agreement"] == "single"
    assert records[1]["graph_read"] is True
    assert "trust_level" not in records[1]


def test_live_filter_completion_states_a_turn_with_no_graph_read(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """No tool output at all is a different fact from an absent generation."""
    from okg.substrate.chat.audit_record import read_records

    filt = _configured(live_filter, tmp_path)
    meta = {"chat_id": "c1", "message_id": "m1"}
    filt.inlet(
        {"model": "m", "messages": [{"role": "user", "content": "hi"}]},
        __metadata__=dict(meta),
    )
    filt.outlet(
        {"messages": [{"role": "assistant", "content": "hello"}]},
        __metadata__=dict(meta),
    )

    completion = read_records(tmp_path / "records-001.jsonl")[1]
    assert completion["generation_agreement"] == "no_graph_read"
    assert completion["graph_read"] is False
    assert completion["observed_generation_id"] is None

    # ... and a turn that DID call a tool whose envelope carried no
    # generation reads "missing", not "no_graph_read".
    filt.inlet(
        {"model": "m", "messages": [{"role": "user", "content": "again"}]},
        __metadata__={"chat_id": "c1", "message_id": "m2"},
    )
    filt.outlet(
        {
            "messages": [
                {"role": "tool", "content": json.dumps({"results": []})},
                {"role": "assistant", "content": "nothing"},
            ]
        },
        __metadata__={"chat_id": "c1", "message_id": "m2"},
    )
    second = read_records(tmp_path / "records-001.jsonl")[3]
    assert second["generation_agreement"] == "missing"
    assert second["graph_read"] is True


def test_live_filter_joins_a_rewritten_turn_to_its_newest_inlet_record(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """An edited-and-resent message reuses (chat_id, message_id).

    The completion must join the NEWEST inlet record for that pair, which is
    the rule the record schema states for consumers. Joining the first would
    attach the new answer to the superseded payload.
    """
    from okg.substrate.chat.audit_record import read_records

    filt = _configured(live_filter, tmp_path)
    meta = {"chat_id": "c1", "message_id": "m1"}
    first = filt.inlet(
        {"model": "m", "messages": [{"role": "user", "content": "original"}]},
        __metadata__=dict(meta),
    )
    filt.inlet(
        {"model": "m", "messages": [{"role": "user", "content": "edited"}]},
        __metadata__=dict(meta),
    )
    filt.outlet(
        {"messages": [{"role": "assistant", "content": "answer"}]},
        __metadata__=dict(meta),
    )

    records = read_records(tmp_path / "records-001.jsonl")
    inlets = [r for r in records if r["record_kind"] == "inlet"]
    completion = records[-1]
    assert len(inlets) == 2
    assert completion["record_id"] == inlets[1]["record_id"]
    assert completion["record_id"] != inlets[0]["record_id"]
    assert first is not None


def test_live_filter_proof_suspension_needs_a_host_marker_not_a_valve(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """The proof bootstrap must not be reachable from the admin UI.

    The verdict cannot exist before the proof that writes it, so the
    refusal has to be suspendable for that run. If the suspension were a
    Valve, an Open WebUI administrator could flip it and run a preset with
    compaction on and an unverified audit — the one thing the PACT forbids
    outright.
    """
    filt = _configured(live_filter, tmp_path, compaction_enabled=True)
    verdict_dir = Path(filt.valves.verdict_path).parent

    # No valve can turn the refusal off: there is no such field.
    assert "proof_mode" not in type(filt.valves).model_fields
    with pytest.raises(RuntimeError) as raised:
        filt.inlet({"model": "m", "messages": []})
    assert "verified_compaction_on" in str(raised.value)

    # Only the host-side marker, in the read-only-mounted verdict dir.
    verdict_dir.mkdir(parents=True, exist_ok=True)
    (verdict_dir / live_filter.PROOF_MARKER_NAME).write_text("x", encoding="utf-8")
    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})

    # And every record written under the suspension says so, so a proof-run
    # record can never be read as a production audit.
    from okg.substrate.chat.audit_record import read_records

    written = read_records(Path(filt.valves.artifact_path))
    assert len(written) == 1
    assert written[0]["proof_mode"] is True

    # Remove the marker and the refusal is back.
    (verdict_dir / live_filter.PROOF_MARKER_NAME).unlink()
    with pytest.raises(RuntimeError):
        filt.inlet({"model": "m", "messages": []})


def test_live_filter_record_fields_match_the_substrate_producer(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """Two producers, one schema_version — so one field set.

    Different shapes under the same declared version is how a consumer ends
    up reading a field from one producer that the other never writes.
    """
    from okg.substrate.chat.audit_record import AuditArtifact, TaskKind, read_records

    filt = _configured(live_filter, tmp_path)
    body = {"model": "m", "messages": [{"role": "user", "content": "hi"}]}
    # The Open WebUI contract is that inlet returns the BODY, so the record
    # is read back off the artifact — which is also the shape a consumer
    # sees.
    filt.inlet(dict(body), __metadata__={"chat_id": "c", "message_id": "m"})
    live_record = read_records(Path(filt.valves.artifact_path))[0]

    substrate_record = AuditArtifact(
        path=tmp_path / "substrate.jsonl"
    ).append_inlet(
        deployment="cooper",
        preset="okg-cooper-analyst",
        conversation_id="c",
        message_id="m",
        task_kind=TaskKind.USER_TURN,
        task_metadata_raw=None,
        principal=None,
        model="m",
        provider=None,
        compaction_enabled=False,
        payload=canonical_payload(body),
        openwebui_version="0.11.0",
    )

    assert set(live_record) == set(substrate_record), (
        "the two producers write different inlet-record field sets under the "
        f"same schema_version: only-live={set(live_record) - set(substrate_record)}, "
        f"only-substrate={set(substrate_record) - set(live_record)}"
    )
    assert live_record["payload_sha256"] == substrate_record["payload_sha256"]


def test_live_filter_rotates_on_a_producer_version_change(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """A major bump must start a new artifact, not refuse forever.

    The installed producer is the one that runs. If only the substrate copy
    can rotate, a version bump bricks the deployment on a fixed path.
    """
    directory = tmp_path / "audit"
    directory.mkdir()
    (directory / "records-001.jsonl").write_text(
        json.dumps(
            {
                "schema": "okg.chat.audit_record",
                "schema_version": "0.9",
                "record_kind": "inlet",
                "record_seq": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    resolved = live_filter.resolve_artifact_path(str(directory))

    assert Path(resolved).name == "records-002.jsonl"
    filt = _configured(live_filter, tmp_path, artifact_path=str(directory))
    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})
    assert Path(resolved).exists()
    # The old major's file is untouched.
    assert "0.9" in (directory / "records-001.jsonl").read_text("utf-8")


def test_live_filter_repairs_an_uncommitted_torn_tail(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """Same rule as the substrate: a torn tail is uncommitted, not fatal."""
    from okg.substrate.chat.audit_record import read_records

    filt = _configured(live_filter, tmp_path)
    artifact = Path(filt.valves.artifact_path)
    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "one"}]})
    with artifact.open("a", encoding="utf-8") as handle:
        handle.write('{"schema": "okg.chat.audit_record", "record_ki')

    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "two"}]})

    records = read_records(artifact)
    assert len(records) == 2
    assert records[1]["record_seq"] == 2

    # A corrupt line that is NOT the tail refuses with a NAMED reason.
    lines = artifact.read_text("utf-8").splitlines(keepends=True)
    lines[0] = '{"schema": "okg.chat.audit_record", "broken\n'
    artifact.write_text("".join(lines), encoding="utf-8")
    with pytest.raises(RuntimeError) as raised:
        filt.inlet({"model": "m", "messages": [{"role": "user", "content": "three"}]})
    assert "audit-artifact-corrupt" in str(raised.value)


@pytest.mark.skipif(
    __import__("os").geteuid() == 0, reason="root ignores the mode bits this test sets"
)
def test_live_filter_names_the_reason_when_the_directory_is_unwritable(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """Measured on a live instance: the lock open fails before the append.

    The first live run after the lock was added surfaced a bare
    ``[Errno 30] Read-only file system: '.../records-001.jsonl.lock'`` in
    the chat — a refusal that names neither the filter nor what it refused.
    """
    import os as _os
    import stat as _stat

    audit_dir = tmp_path / "readonly"
    audit_dir.mkdir()
    filt = _configured(
        live_filter, tmp_path, artifact_path=str(audit_dir / "records-001.jsonl")
    )
    audit_dir.chmod(_stat.S_IRUSR | _stat.S_IXUSR)
    try:
        with pytest.raises(RuntimeError) as raised:
            filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]})
    finally:
        audit_dir.chmod(_stat.S_IRWXU)

    message = str(raised.value)
    assert message.startswith("okg audit filter:"), message
    assert "answered unaudited" in message
    assert _os.listdir(audit_dir) == []


def test_live_filter_refuses_when_no_artifact_path_is_configured(
    live_filter: ModuleType,
) -> None:
    """An unset path refuses; it does not pick somewhere to write."""
    filt = live_filter.Filter()
    filt.valves.artifact_path = ""

    with pytest.raises(RuntimeError) as raised:
        filt.inlet({"model": "m", "messages": []})

    assert "artifact_path" in str(raised.value)


# ---------------------------------------------------------------------------
# The OUTLET's rendering half.
#
# Same argument as the canonicalization pin above, applied to the other
# cross-boundary copy this PACT carries: the block the user actually reads is
# rendered by the installed file, while every hermetic test of the citation
# contract runs against the substrate module. Unpinned, the two could render
# different text and both suites would stay green.
# ---------------------------------------------------------------------------

#: Row/observation pairs chosen for the ways two renderers diverge: a linked
#: node, a lineage that is absent, a subtype outside the closed map, one
#: deliberately unroutable, an edge-shaped row, a missing generation, two
#: generations at once, and a turn that read nothing at all.
RENDER_CASES = [
    (
        "single",
        ["okg:gen:1"],
        [{"node_id": "okg:node:pact:x", "subtype": "pact_change", "source": "pact"}],
    ),
    (
        "single",
        ["okg:gen:1"],
        [{"node_id": "okg:node:file:a.py", "subtype": "source_file"}],
    ),
    (
        "single",
        ["okg:gen:1"],
        [
            {
                "node_id": "okg:node:s/1",
                "subtype": "agent_session",
                "provenance": {"source": "sessions", "trust": "high"},
            }
        ],
    ),
    ("single", ["okg:gen:1"], [{"node_id": "n1", "subtype": "not_a_real_subtype"}]),
    ("single", ["okg:gen:1"], [{"node_id": "n1", "subtype": "chat_message"}]),
    ("single", ["okg:gen:1"], [{"edge_id": "e1", "src": "a", "dst": "b"}]),
    ("single", ["okg:gen:1"], [{"node_id": "n1"}]),
    ("missing", [], [{"node_id": "okg:node:pact:x", "subtype": "pact_change"}]),
    (
        "conflicting",
        ["okg:gen:1", "okg:gen:2"],
        [{"node_id": "okg:node:pact:x", "subtype": "pact_change"}],
    ),
    ("no_graph_read", [], []),
    ("single", ["okg:gen:1"], []),
]

#: Both sides of the link contract: a declared console base, and none.
BASES = ["http://127.0.0.1:5199", None]


def _observation(agreement: str, generation_ids: list[str]):
    from okg.substrate.chat.audit_record import (
        GenerationAgreement,
        GenerationObservation,
    )

    return GenerationObservation(
        agreement=GenerationAgreement(agreement),
        generation_ids=tuple(generation_ids),
    )


def test_live_filter_subtype_table_equals_the_substrate_table(
    live_filter: ModuleType,
) -> None:
    """Entry for entry, not merely "the same size".

    The table IS the closed allow-list. One extra entry in the installed copy
    would create a live console link for a subtype the contract never
    reviewed, and every hermetic test would still pass.
    """
    from okg.substrate.chat.deep_links import SUBTYPE_TO_TARGET_KIND

    mine = {
        subtype: (None if kind is None else kind.value)
        for subtype, kind in SUBTYPE_TO_TARGET_KIND.items()
    }
    assert mine, "an empty table would pass this check vacuously"
    assert live_filter.SUBTYPE_TO_TARGET_KIND == mine


def test_live_filter_render_vocabulary_equals_the_substrate_vocabulary(
    live_filter: ModuleType,
) -> None:
    """The strings a reader sees, and the field name they compare to status."""
    from okg.substrate.chat import audit_filter as substrate

    assert live_filter.STAMP_FIELD == substrate.STAMP_FIELD
    assert live_filter.STAMP_UNAVAILABLE == substrate.STAMP_UNAVAILABLE
    assert live_filter.STAMP_CONFLICTING == substrate.STAMP_CONFLICTING
    assert live_filter.LINEAGE_UNAVAILABLE == substrate.LINEAGE_UNAVAILABLE
    assert live_filter.CITATION_HEADER == substrate.CITATION_HEADER
    assert live_filter.CITATION_SEPARATOR == substrate.CITATION_SEPARATOR


def test_live_filter_provenance_block_agrees_with_the_module(
    live_filter: ModuleType,
) -> None:
    """Same rows, same observation, same base — byte-equal blocks."""
    from okg.substrate.chat.audit_filter import render_provenance_block

    assert RENDER_CASES and BASES, "an empty case set would pass vacuously"
    for agreement, generation_ids, rows in RENDER_CASES:
        envelope = (
            {"pinned_generation_id": generation_ids[0]}
            if len(generation_ids) == 1
            else None
        )
        for base in BASES:
            mine = render_provenance_block(
                observation=_observation(agreement, generation_ids),
                rows=rows,
                envelope=envelope,
                operator_ui_base=base,
            )
            theirs = live_filter.render_provenance_block(
                agreement, generation_ids, rows, envelope, base
            )
            assert mine == theirs, (
                f"rendered block drifted for {agreement} / {rows!r} / base={base!r}"
            )


@pytest.mark.parametrize("base", [None, "http://127.0.0.1:5199"])
def test_live_filter_renders_no_url_with_or_without_a_console_base(
    live_filter: ModuleType, base: str | None,
) -> None:
    """The negative that makes the agreement meaningful.

    Since PACT operator-ui-remove-four-views no kind has a view, so the block
    carries no scheme, no host and no console hash whether or not a base is
    declared — only the id and the stated reason. Running BOTH bases is the
    point: with a base present this would catch a link creeping back in.
    """
    block = live_filter.render_provenance_block(
        "single",
        ["okg:gen:1"],
        [{"node_id": "okg:node:pact:x", "subtype": "pact_change"}],
        {"pinned_generation_id": "okg:gen:1"},
        base,
    )
    assert "okg:node:pact:x" in block
    assert "no console link" in block
    for token in ("http", "://", "#graph", "#trace", "]("):
        assert token not in block, f"a URL leaked into the refusal: {token}"


def test_live_filter_outlet_stamps_and_cites_the_answer(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """End to end through the code Open WebUI runs, not just the renderer."""
    filt = _configured(
        live_filter, tmp_path, operator_ui_base="http://127.0.0.1:5199"
    )
    meta = {"chat_id": "c1", "message_id": "m1"}
    filt.inlet({"model": "m", "messages": [{"role": "user", "content": "q"}]}, __metadata__=dict(meta))

    body = {
        "messages": [
            {"role": "user", "content": "q"},
            {
                "role": "tool",
                "content": json.dumps(
                    {
                        "pinned_generation_id": "okg:gen:1",
                        "results": [
                            {
                                "node_id": "okg:node:pact:x",
                                "subtype": "pact_change",
                                "source": "pact",
                            }
                        ],
                    }
                ),
            },
            {"role": "assistant", "content": "Two PACTs."},
        ]
    }
    rendered = filt.outlet(body, __metadata__=dict(meta))
    answer = rendered["messages"][-1]["content"]

    assert answer.startswith("Two PACTs.")
    assert "generation_id: okg:gen:1" in answer
    assert "Sources:" in answer
    # No view shows a record since PACT operator-ui-remove-four-views, so the
    # citation is the bare identifier plus the reason — never a hash.
    assert "okg:node:pact:x" in answer
    assert "no console link" in answer
    assert "#graph" not in answer and "127.0.0.1:5199#" not in answer
    assert answer.count("Sources:") == 1

    # A second outlet call on the same body must not append a second footer.
    again = filt.outlet(rendered, __metadata__=dict(meta))
    assert again["messages"][-1]["content"].count("Sources:") == 1


def test_live_filter_outlet_cites_rows_carried_in_output_blocks(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """The shape a NATIVE MCP tool loop actually produces at v0.11.0.

    Measured on the live instance: a native tool loop leaves `sources` empty
    and emits no `role == "tool"` message. The results sit in the assistant
    message's `output` blocks. A scope that missed them still found the
    generation — `observed_generations` walks the whole body — and rendered a
    stamped answer that cited NOTHING. This test is that regression.
    """
    filt = _configured(
        live_filter, tmp_path, operator_ui_base="http://127.0.0.1:5199"
    )
    meta = {"chat_id": "c1", "message_id": "m1"}
    filt.inlet({"model": "m", "messages": []}, __metadata__=dict(meta))

    body = {
        "messages": [
            {"id": "u1", "role": "user", "content": "which PACTs?"},
            {
                "id": "a1",
                "role": "assistant",
                "content": "Two of them.",
                "output": [
                    {"type": "reasoning", "content": [{"text": "thinking"}]},
                    {
                        "type": "function_call_output",
                        "output": json.dumps(
                            {
                                "pinned_generation_id": "okg:gen:9",
                                "results": [
                                    {
                                        "node_id": "pact-change:one",
                                        "subtype": "pact_change",
                                        "provenance": {"source": "okg.pact"},
                                    }
                                ],
                            }
                        ),
                    },
                ],
            },
        ]
    }
    answer = filt.outlet(body, __metadata__=dict(meta))["messages"][-1]["content"]

    assert "generation_id: okg:gen:9" in answer
    assert "Sources:" in answer
    assert "pact-change:one" in answer
    assert "okg.pact" in answer
    assert "no console link" in answer
    assert "#graph" not in answer


def test_live_filter_outlet_edits_the_text_the_frontend_renders(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """A footer only in `content` is persisted and never seen.

    Measured on the live instance at v0.11.0: when an assistant turn carries
    an `output` array the frontend renders the last `message` entry's
    `output_text`, not the message-level `content`. The first live run edited
    only `content`; the block was stored on the message and the screen showed
    nothing. Both are written now, with the same text.
    """
    filt = _configured(
        live_filter, tmp_path, operator_ui_base="http://127.0.0.1:5199"
    )
    meta = {"chat_id": "c1", "message_id": "m1"}
    filt.inlet({"model": "m", "messages": []}, __metadata__=dict(meta))

    body = {
        "messages": [
            {"id": "u1", "role": "user", "content": "which PACTs?"},
            {
                "id": "a1",
                "role": "assistant",
                "content": "Two of them.",
                "output": [
                    {"type": "reasoning", "content": [{"type": "output_text", "text": "think"}]},
                    {
                        "type": "function_call_output",
                        "output": [
                            {
                                "type": "input_text",
                                "text": json.dumps(
                                    {
                                        "generation_id": "okg:gen:9",
                                        "results": [
                                            {
                                                "node_id": "pact-change:one",
                                                "subtype": "pact_change",
                                                "provenance": {"source": "okg.pact"},
                                            }
                                        ],
                                    }
                                ),
                            }
                        ],
                    },
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "Two of them."}],
                    },
                ],
            },
        ]
    }
    message = filt.outlet(body, __metadata__=dict(meta))["messages"][-1]
    rendered = message["output"][-1]["content"][-1]["text"]

    assert "Sources:" in rendered, "the rendered answer body carries the footer"
    assert "generation_id: okg:gen:9" in rendered
    assert "pact-change:one" in rendered
    assert "no console link" in rendered
    assert "#graph" not in rendered
    # The two surfaces must not disagree.
    assert message["content"].endswith(rendered.split("Two of them.", 1)[1])
    # The reasoning block is left alone: a footer nobody reads is not a footer.
    assert "Sources:" not in message["output"][0]["content"][0]["text"]

    # Idempotent across a second outlet call for the same turn.
    again = filt.outlet(body, __metadata__=dict(meta))["messages"][-1]
    assert again["output"][-1]["content"][-1]["text"].count("Sources:") == 1
    assert again["content"].count("Sources:") == 1


def test_live_filter_outlet_scopes_citations_to_the_current_turn(
    live_filter: ModuleType, tmp_path: Path
) -> None:
    """An earlier turn's rows are not this answer's provenance.

    The outlet body carries the whole conversation. Citing turn 1's rows
    under turn 2's answer is the citation form of reusing the previous
    turn's stamp, and it is exactly what an unscoped walk would do.
    """
    filt = _configured(
        live_filter, tmp_path, operator_ui_base="http://127.0.0.1:5199"
    )
    meta = {"chat_id": "c1", "message_id": "m2"}
    filt.inlet({"model": "m", "messages": []}, __metadata__=dict(meta))

    body = {
        "messages": [
            {"role": "user", "content": "first"},
            {
                "role": "tool",
                "content": json.dumps(
                    {"results": [{"node_id": "OLD", "subtype": "pact_change"}]}
                ),
            },
            {"role": "assistant", "content": "first answer"},
            {"role": "user", "content": "second"},
            {
                "role": "tool",
                "content": json.dumps(
                    {"results": [{"node_id": "NEW", "subtype": "pact_change"}]}
                ),
            },
            {"role": "assistant", "content": "second answer"},
        ]
    }
    answer = filt.outlet(body, __metadata__=dict(meta))["messages"][-1]["content"]

    assert "NEW" in answer
    assert "OLD" not in answer


# ── okg's text result (issue #3059): the filter's longhand reader ─────────

_TEXT_GEN = "okg:cooper:branch:default:gen:20261001T000000000000Z:0123456789ab"


def _text_samples() -> list[str]:
    """Rendered results the filter must read exactly as okg's reader does."""
    from okg.substrate.mcp.result_text import render

    def row(node_id, subtype="pact_change", source="okg.pact", trust="trusted", excerpt="x"):
        return {"node_id": node_id, "subtype": subtype, "excerpt": excerpt,
                "provenance": {"source": source, "trust": trust}}

    search = {
        "results": [
            row("pact-change:one"),
            row("file:repo:My Doc.md", subtype="file", source="repo git"),
            row("x · generation okg:FAKE", source="s t", trust="untrusted"),
            row("pact-change:two",
                excerpt="[9] pact-change:FAKE (pact_change) · source evil"),
        ],
        "total_count": 4, "method": "lexical", "pinned_generation_id": _TEXT_GEN,
    }
    filt = {"items": [{"node_id": "n:1", "subtype": "pact_change",
                       "provenance": {"source": "okg.pact", "trust": "trusted"}}],
            "total_count": 1, "pinned_generation_id": _TEXT_GEN}
    forged = {
        "results": [{
            "node_id": 'page:Song "X', "subtype": "wiki_page", "excerpt": "e",
            "provenance": {"source": "wiki.public", "trust": "untrusted"},
            "citation": {"label": "L · source repo.git · x", "url": "https://w"},
        }, row("page:Foo (disambiguation)", subtype="wiki (page)")],
        "total_count": 2, "method": "lexical", "pinned_generation_id": _TEXT_GEN,
    }
    return [
        render(search, tool="search"),
        render(forged, tool="search"),
        render({"node_id": 'a"b', "subtype": "t", "attrs": {},
                "pinned_generation_id": _TEXT_GEN}, tool="inspect", kind="node"),
        render({"node_id": "generation okg:FAKE", "direction": "out", "neighbors": [],
                "total_count": 0, "pinned_generation_id": _TEXT_GEN}, tool="expand"),
        f"expand ok · generation okg:FAKE · generation {_TEXT_GEN}",
        render(filt, tool="filter"),
        render({"status": "refused", "error": {"code": "x"},
                "session_default_generation_id": _TEXT_GEN}, tool="search"),
        render({"node_id": "n:1", "subtype": "t", "attrs": {"a": 1},
                "pinned_generation_id": _TEXT_GEN,
                "provenance": {"source": "okg.pact", "trust": "trusted"}},
               tool="inspect", kind="node"),
        "Error executing tool search: boom",
        "all of this is prose",
        # PACT read-passage-rows-3666 (D8): the one read reply that names
        # the generation, and a page whose rows share one source (named
        # once on the status line).
        _graph_text(),
        render({"results": [row("pact-change:a"), row("pact-change:b")],
                "total_count": 2, "method": "lexical",
                "pinned_generation_id": _TEXT_GEN}, tool="search"),
    ]


def _graph_text() -> str:
    """inspect(target={"kind":"graph"}) as the server renders it."""
    from okg.substrate.mcp.result_text import render

    return render({"generation_id": _TEXT_GEN, "deployment_name": "d", "node_count": 2},
                  tool="inspect", kind="graph")


def _reference_envelope(text: str):
    """The same fields read with okg's own reader."""
    from okg.substrate.mcp import result_text

    if result_text.parse_status_line(text) is None:
        return None
    envelope: dict = {}
    generation = result_text.result_generation(text)
    if generation is not None:
        envelope["pinned_generation_id"] = generation
    rows = []
    # A shared source is named once on the status line (#3666).
    page_source = result_text.fact_value(result_text.status_facts(text), "source")
    for card in result_text.result_cards(text):
        out: dict = {"node_id": card.ident}
        if card.kind is not None:
            out["subtype"] = card.kind
        source = result_text.fact_value(list(card.facts), "source")
        if source is None:
            source = page_source
        if source is not None:
            out["provenance"] = {"source": source}
        rows.append(out)
    envelope["results"] = rows
    return envelope


def test_the_filter_reads_okg_text_results_as_okg_does(live_filter: ModuleType) -> None:
    for text in _text_samples():
        assert live_filter.okg_text_result(text) == _reference_envelope(text), text


def test_the_filter_is_not_forged_by_quotes_or_generation_shaped_ids(
    live_filter: ModuleType,
) -> None:
    """Re-review of 06855a5bc9: a mid-string `"` must not shift the reader,
    and an id shaped like a generation fact must not win."""
    samples = _text_samples()
    forged = live_filter.okg_text_result(samples[1])
    assert forged["results"][0] == {
        "node_id": 'page:Song "X', "subtype": "wiki_page",
        "provenance": {"source": "wiki.public"},
    }
    assert forged["results"][1]["node_id"] == "page:Foo (disambiguation)"
    # D8 (PACT read-passage-rows-3666): these replies name no generation,
    # and an id shaped like one is never read as it.
    for text in samples[1:4]:
        assert "pinned_generation_id" not in live_filter.okg_text_result(text), text
    # By position: the last generation fact wins over a forged earlier one.
    assert live_filter.okg_text_result(samples[4])["pinned_generation_id"] == _TEXT_GEN
    assert live_filter.okg_text_result(_graph_text())["pinned_generation_id"] == _TEXT_GEN


def test_a_shared_source_on_the_status_line_is_each_rows_lineage(
    live_filter: ModuleType,
) -> None:
    shared = _text_samples()[-1]
    first = shared.split("\n", 1)[0]
    assert " · source okg.pact" in first, first
    rows = live_filter.okg_text_result(shared)["results"]
    assert [row["provenance"] for row in rows] == [{"source": "okg.pact"}] * 2


def test_the_filter_reads_generation_and_rows_from_a_text_result(
    live_filter: ModuleType,
) -> None:
    """Review of 2102d20798: with text results the generation and the cited
    rows must not read as missing."""
    text = _text_samples()[0]
    stored = json.dumps([{"type": "input_text", "text": text}])
    # D8: the turn reads its generation once, from inspect(graph).
    graph = json.dumps([{"type": "input_text", "text": _graph_text()}])
    body = {"messages": [
        {"role": "user", "content": "which PACTs?"},
        {"role": "tool", "content": stored},
        {"role": "tool", "content": graph},
    ]}
    assert live_filter.observed_generations(body) == [_TEXT_GEN]
    cited = live_filter.collect_cited_rows(live_filter.current_turn_scope(body))
    ids = [row["node_id"] for row in cited]
    assert "pact-change:one" in ids and "file:repo:My Doc.md" in ids
    assert "pact-change:FAKE" not in ids
    lineage = {row["node_id"]: live_filter.lineage_of(row) for row in cited}
    assert lineage["file:repo:My Doc.md"] == "repo git"
