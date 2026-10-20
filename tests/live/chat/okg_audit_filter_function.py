"""
title: OKG Context Audit
author: okg
version: 1.0
description: Records the exact bytes each turn sends to the model into the
  append-only audit-record artifact the OKG substrate reads, and stamps each
  answer with the generation it came from plus one citation per cited node.
"""

# The Open WebUI-side half of PACT chat-context-audit-filter.
#
# This file is INSTALLED INTO Open WebUI, where `okg` is not importable, so
# the canonicalization, the record shape and the provenance rendering are
# written out longhand here. That duplication is the risk this file carries,
# and it is pinned rather than trusted:
# `tests/substrate/chat/test_live_filter_agreement.py` feeds the same bodies
# and the same rows through this module and through
# `okg.substrate.chat.audit_record` / `okg.substrate.chat.audit_filter` and
# asserts the canonical payloads, the digests, the subtype route table and
# the rendered provenance blocks are byte-identical. A drift fails that test.
#
# The inlet writes; it never reads a verdict and never writes one. The
# fail-closed refusal predicate lives in the substrate filter
# (`okg.substrate.chat.audit_filter`); what runs here is the recorder whose
# output the equality proof digests.
#
# The outlet does two things, in order: append the turn's completion record,
# then render the provenance the reader sees. On NATIVE-loop turns this
# outlet is the SOLE citation producer (chat-turn-trace-events' citation
# ownership seam); Pipe-mediated turns render citations from that change's
# events instead, and a turn showing two disagreeing citation sets is a
# defect by definition.

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

from pydantic import BaseModel, Field

SCHEMA = "okg.chat.audit_record"
SCHEMA_VERSION = "1.0"
COMPACTION_SUMMARY_MARKER = "[CONVERSATION SUMMARY]"

#: The proof run cannot wait for the verdict the proof writes, so the
#: refusal has to be suspended for exactly that run. The suspension is NOT
#: a valve an admin can flip: it requires this marker file, which lives in
#: the operator-state directory mounted READ-ONLY into the container. The
#: only writer is the host-side driver. An Open WebUI administrator with
#: full access to the admin UI cannot create it.
PROOF_MARKER_NAME = "PROOF_RUN_IN_PROGRESS"

VERDICT_SCHEMA = "okg.chat.observability_verdict"
VERDICT_SCHEMA_VERSION = "1.0"
VERIFIED_COMPACTION_ON = "verified_compaction_on"

TASK_KINDS = {
    "title_generation": "title_generation",
    "tags_generation": "tags_generation",
    "follow_up_generation": "follow_up_generation",
    "context_compaction": "context_compaction",
    "query_generation": "query_generation",
    "autocomplete_generation": "autocomplete_generation",
}


def canonical_payload(body: dict) -> str:
    """The in-scope subtree: the message array plus the model identifier."""
    return json.dumps(
        {"messages": body.get("messages"), "model": body.get("model")},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def task_kind_from_metadata(metadata: Optional[dict]) -> str:
    """Absent task = the user's own turn; present-but-unknown = auxiliary."""
    if not isinstance(metadata, dict):
        return "user_turn"
    raw = metadata.get("task")
    if raw is None:
        return "user_turn"
    name = str(raw).strip()
    if not name:
        return "user_turn"
    return TASK_KINDS.get(name, "other_auxiliary")


def is_verified_compaction_on(
    verdict_path: str, deployment: str, preset: str, openwebui_version: str
) -> bool:
    """Read the observability verdict. ABSENCE IS UNVERIFIED.

    False for: no file, no record for the key, a record proving only the
    other configuration, an unrecognized schema version, and an unparseable
    line. Nothing here raises and nothing here writes — a runtime component
    cannot know whether the operator-run proof happened, so it can only
    read what that proof left behind.
    """
    if not verdict_path or not os.path.exists(verdict_path):
        return False
    try:
        with open(verdict_path, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        return False
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if not isinstance(record, dict):
            continue
        if record.get("schema") != VERDICT_SCHEMA:
            continue
        version = record.get("schema_version")
        if not isinstance(version, str) or version.split(".", 1)[0] != (
            VERDICT_SCHEMA_VERSION.split(".", 1)[0]
        ):
            continue
        if (
            record.get("verdict") == VERIFIED_COMPACTION_ON
            and record.get("deployment") == deployment
            and record.get("preset") == preset
            and record.get("openwebui_version") == openwebui_version
        ):
            return True
    return False


def proof_run_in_progress(verdict_path: str) -> bool:
    """Is the host-side proof marker present next to the verdict artifact?

    This, not a valve, is what suspends the refusal. The verdict directory
    is mounted read-only into the container, so the marker can only be
    created by the operator-run driver on the host.
    """
    if not verdict_path:
        return False
    marker = os.path.join(os.path.dirname(verdict_path), PROOF_MARKER_NAME)
    return os.path.exists(marker)


def _read_lines(path: str) -> list:
    """Artifact lines, with an uncommitted torn final line dropped.

    A final line with no newline is an append that died between write and
    flush. It was never committed, so removing it is not a rewrite of an
    earlier record — and leaving it would make every later turn fail on a
    parse error with no stated recovery.
    """
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    if lines and not lines[-1].endswith("\n"):
        size = os.path.getsize(path)
        with open(path, "a", encoding="utf-8") as handle:
            handle.truncate(max(0, size - len(lines[-1])))
        lines = lines[:-1]
    return lines


def _parse(line: str, path: str) -> dict:
    try:
        return json.loads(line)
    except ValueError as exc:
        raise RuntimeError(
            "okg audit filter: audit-artifact-corrupt: %s contains a record "
            "that is not valid JSON (%s); the turn is refused" % (path, exc)
        )


def _scan(path: str) -> tuple:
    """Recover (artifact_id, next inlet sequence) from the artifact."""
    artifact_id = None
    highest = 0
    for line in _read_lines(path):
        if not line.strip():
            continue
        record = _parse(line, path)
        version = record.get("schema_version")
        if not isinstance(version, str) or version.split(".", 1)[0] != (
            SCHEMA_VERSION.split(".", 1)[0]
        ):
            raise RuntimeError(
                "audit-schema-version-conflict: %r in %s" % (version, path)
            )
        if artifact_id is None:
            artifact_id = record.get("artifact_id")
        if record.get("record_kind") == "inlet":
            highest = max(highest, int(record.get("record_seq") or 0))
    return artifact_id, highest + 1


def resolve_artifact_path(directory: str) -> str:
    """The records-NNN.jsonl file this producer version may append to.

    Same rule as the substrate's ``resolve_artifact_path``: reuse the newest
    file whose head record carries this major, otherwise open the next
    index. Without it, a producer version bump would refuse forever on a
    fixed path instead of starting a new artifact.
    """
    wanted = SCHEMA_VERSION.split(".", 1)[0]
    if not os.path.isdir(directory):
        return os.path.join(directory, "records-001.jsonl")
    names = sorted(
        name
        for name in os.listdir(directory)
        if name.startswith("records-") and name.endswith(".jsonl")
    )
    if not names:
        return os.path.join(directory, "records-001.jsonl")
    empty = None
    for name in reversed(names):
        candidate = os.path.join(directory, name)
        head = None
        for line in _read_lines(candidate):
            if line.strip():
                head = _parse(line, candidate)
                break
        if head is None:
            if empty is None:
                empty = candidate
            continue
        version = head.get("schema_version")
        if isinstance(version, str) and version.split(".", 1)[0] == wanted:
            return candidate
    if empty is not None:
        return empty
    last = int(names[-1][len("records-") : -len(".jsonl")] or 0)
    return os.path.join(directory, "records-%03d.jsonl" % (last + 1))


def _locked_append(path: str, record: dict) -> None:
    """Append one record under an exclusive lock, or leave the file alone.

    The lock spans the caller's read-then-append: two Open WebUI workers
    that each read the same next sequence would otherwise mint the SAME
    record_id, and two that each find an empty file would mint two
    artifact ids into one file.
    """
    line = json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    size_before = os.path.getsize(path) if os.path.exists(path) else 0
    try:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as exc:
        try:
            if os.path.exists(path) and os.path.getsize(path) > size_before:
                with open(path, "a", encoding="utf-8") as handle:
                    handle.truncate(size_before)
        except OSError:
            # Best effort only: preserve the original write failure below.
            pass
        raise RuntimeError(
            "okg audit filter: the audit record could not be written (%s); "
            "the turn is refused rather than answered unaudited" % exc
        )


class _Lock:
    """Exclusive advisory lock on a file beside the artifact."""

    def __init__(self, path: str) -> None:
        self.lock_path = path + ".lock"
        self.handle = None

    def __enter__(self):
        directory = os.path.dirname(self.lock_path)
        try:
            if directory:
                os.makedirs(directory, exist_ok=True)
            self.handle = open(self.lock_path, "a+")
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        except OSError as exc:
            # An unwritable audit directory surfaces HERE first, before the
            # record write. Letting the raw OSError escape would put
            # "[Errno 30] Read-only file system" in front of the user with
            # nothing naming the filter or the reason — a refusal that does
            # not say what it refused.
            raise RuntimeError(
                "okg audit filter: the audit record could not be written "
                "(%s); the turn is refused rather than answered unaudited"
                % exc
            )
        return self

    def __exit__(self, *exc) -> None:
        if self.handle is not None:
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.handle.close()
                self.handle = None


# ---------------------------------------------------------------------------
# The OUTLET's provenance rendering (PACT req.outlet-provenance-and-links).
#
# Two halves with a deliberate seam between them:
#
# * EXTRACTION (`collect_cited_rows`, `envelope_for_rows`) is live-only. The
#   substrate outlet takes `rows` and `envelope` as arguments; deciding which
#   of Open WebUI's tool results ARE the cited rows is a question only the
#   live surface has, so it is answered only here.
# * RENDERING (everything below the extraction) is a longhand copy of
#   `okg.substrate.chat.audit_filter` and `okg.substrate.chat.deep_links`,
#   for the same reason the record writer above is a copy: `okg` is not
#   importable inside Open WebUI. The copy is PINNED, not trusted --
#   `tests/substrate/chat/test_live_filter_agreement.py` feeds the same rows
#   through both and asserts the rendered blocks are byte-identical, and
#   asserts this module's subtype table equals the substrate's entry for
#   entry. A drift fails that test.
#
# No URL shape is defined here. Every link comes from the chat-deep-links
# grammar copied below, and where the deployment declares no console base the
# id renders as plain text with the stated reason -- never a guessed URL.
# ---------------------------------------------------------------------------

#: Spelled as `okg dogfood status` spells it, so an operator comparing an
#: answer against status output never has to translate.
STAMP_FIELD = "generation_id"

STAMP_UNAVAILABLE = (
    "unavailable — this answer's MCP envelope carried no pinned_generation_id"
)
STAMP_CONFLICTING = (
    "conflicting — this turn's MCP envelopes reported more than one "
    "pinned_generation_id"
)
LINEAGE_UNAVAILABLE = "source lineage unavailable — the row carried no source field"

CITATION_HEADER = "Sources:"
CITATION_SEPARATOR = " — "

EMPTY_ID_TEXT = "(empty identifier)"
ENVELOPE_KIND_FIELD = "target_kind"
ENVELOPE_GENERATION_FIELD = "pinned_generation_id"
PRODUCER_KEY_FIELD = "producer_key"

#: Which row field carries the id, per kind. Never used to guess a kind.
ID_FIELD_BY_KIND = {
    "node": "node_id",
    "agent_session": "session_id",
    "producer_run": "run_key",
    "mcp_trace": "trace_id",
}

#: Kinds the envelope still declares that no console view shows. Today that
#: is every kind: the Graph view showed records and the Trace view showed the
#: other three, and PACT operator-ui-remove-four-views deleted both.
KINDS_WITHOUT_A_VIEW = frozenset({"node", "agent_session", "producer_run", "mcp_trace"})

SUBTYPE_TO_TARGET_KIND = {
    "acceptance_criterion": 'node',
    "acceptance_scenario": 'node',
    "agent_claim": 'node',
    "agent_decision": 'node',
    "agent_experiment": 'node',
    "agent_observation": 'node',
    "agent_session": 'node',
    "agent": 'node',
    "agent_assignment": 'node',
    "agent_chapter": 'node',
    "agent_message": 'node',
    "alternative": 'node',
    "arena_preference": 'node',
    "benchmark_measurement": 'node',
    "blocker_event": 'node',
    "branch_archive_summary": 'node',
    "branch_disposition": 'node',
    "chat_context_record": 'node',
    "chat_message": None,
    "chat_rating": 'node',
    "chat_session": 'node',
    "citation": 'node',
    "code_import": 'node',
    "code_quality_finding": 'node',
    "code_reference": 'node',
    "code_symbol": 'node',
    "cooper_alias": 'node',
    "consistency_claim": 'node',
    "conference": 'node',
    "conference_edition": 'node',
    "config_key": 'node',
    "coordination_event": 'node',
    "coordination_lane": 'node',
    "coordination_org_unit": 'node',
    "coordination_work_area": 'node',
    "coordination_work_item": 'node',
    "data_asset": 'node',
    "data_flow_observation": 'node',
    "data_flow_step": 'node',
    "dependency_package": 'node',
    "dependency_vulnerability": 'node',
    "design_decision": 'node',
    "document": 'node',
    "document_chunk": 'node',
    "entity_mention": 'node',
    "gate_result": 'node',
    "git_branch": 'node',
    "git_commit": 'node',
    "git_author": 'node',
    "git_tag": 'node',
    "goal": 'node',
    "heartbeat_event": 'node',
    "hypothesis": 'node',
    "insight": 'node',
    "instrumentation_gap": 'node',
    "interface_endpoint": 'node',
    "issue": 'node',
    "machine": 'node',
    "merge_gate": 'node',
    "non_goal": 'node',
    "observation": 'node',
    "open_spec_change": 'node',
    "operator_report": 'node',
    "package_manifest": 'node',
    "pact_change": 'node',
    "pact_decision": 'node',
    "pact_task": 'node',
    "paper": 'node',
    "paper_section": 'node',
    "performance_budget": 'node',
    "permission_event": 'node',
    "person": 'node',
    "profile_hotspot": 'node',
    "profile_query": 'node',
    "profile_run": 'node',
    "profile_stage": 'node',
    "proof_event": 'node',
    "proof_of_work": 'node',
    "proposal_task": 'node',
    "pull_request": 'node',
    "remote_host": 'node',
    "repo": 'node',
    "region": 'node',
    "review": 'node',
    "review_finding": 'node',
    "review_gate": 'node',
    "risk": 'node',
    "role_agent": 'node',
    "runtime": 'node',
    "runtime_event": 'node',
    "security_finding": 'node',
    "self_play_finding": 'node',
    "self_play_run": 'node',
    "self_play_scenario": 'node',
    "source_file": 'node',
    "secret_finding": 'node',
    "skill": 'node',
    "spawned_task_chip": 'node',
    "spec_capability": 'node',
    "spec_delta": 'node',
    "stack_frame": 'node',
    "stakeholder": 'node',
    "sub_agent_session": 'node',
    "subagent_rollup": 'node',
    "supervisor_decision": 'node',
    "task_claim": 'node',
    "task_group": 'node',
    "test_case": 'node',
    "test_report": 'node',
    "test_run": 'node',
    "tool_use_event": 'node',
    "venue": 'node',
    "verification_result": 'node',
    "verification_step": 'node',
    "visual_artifact": 'node',
    "work_gate": 'node',
    "work_order": 'node',
    "worker_pool": 'node',
    "workspace_lease": 'node',
    "workspace_observation": 'node',
}

#: Operator-readable reason per refusal. Deliberately free of any URL, host,
#: or scheme text — a refusal must not hand anyone a base to copy.
REFUSAL_REASONS = {
    "undeclared-kind": (
        "the answer envelope declared no target kind for this identifier, "
        "so no console route can be chosen"
    ),
    "unknown-kind": (
        "the declared target kind is not one the operator console routes"
    ),
    "unknown-subtype": (
        "the MCP node subtype is not in the closed console-route mapping"
    ),
    "unroutable-subtype": (
        "the MCP node subtype is intentionally not routed to a console link"
    ),
    "missing-operator-ui-base": (
        "this deployment declares no operator console base "
        "(chat.deep_links.operator_ui_base), so no console link can be built"
    ),
    "no-console-view": (
        "the operator console has no view that shows this kind, so the "
        "identifier is left as text"
    ),
    "missing-producer-key": (
        "a producer run needs both its producer key and its run key; "
        "the producer key is missing"
    ),
    "empty-id": "the identifier is empty",
    "malformed-row": (
        "the MCP result is not a node-shaped row with an authoritative node_id and subtype"
    ),
}


def _clean(value):
    """A non-empty stripped string, or None."""
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def encode_id(value: str) -> str:
    """Percent-encode an id for a console hash, as the console does."""
    return quote(value, safe="")


def current_turn_scope(body) -> list:
    """The sub-trees of the outlet body that belong to THIS turn.

    LIVE-ONLY, and load-bearing. The outlet body carries the WHOLE
    conversation, so an unscoped search would cite rows an EARLIER turn read
    and present them as this answer's provenance — the citation equivalent of
    reusing the previous turn's stamp. Only the current turn is searched:
    ``body["sources"]``, plus every message AFTER the last user message,
    through the four places a tool result can sit.

    MEASURED, not assumed. At the pinned v0.11.0 the outlet body's messages
    are rebuilt field by field (utils/middleware.py:3475-3490) as
    ``id, role, content, info, timestamp`` plus ``output``, ``usage`` and
    ``sources`` where present. A native MCP tool loop leaves NOTHING in
    ``sources`` and emits no ``role == "tool"`` message: the tool results
    live in the assistant message's ``output`` blocks. A scope that omitted
    ``output`` found the generation (``observed_generations`` walks the whole
    body) and zero rows — a stamped answer citing nothing, which is why
    ``output`` is named here explicitly rather than left to a generic walk.

    An empty list is the honest answer for a turn that called no tools.
    """
    scope = []
    if not isinstance(body, dict):
        return scope
    if body.get("sources"):
        scope.append(body["sources"])

    messages = body.get("messages")
    if not isinstance(messages, list):
        return scope

    last_user = -1
    for index, message in enumerate(messages):
        if isinstance(message, dict) and message.get("role") == "user":
            last_user = index

    for message in messages[last_user + 1 :]:
        if not isinstance(message, dict):
            continue
        if message.get("role") == "tool":
            scope.append(message)
        for key in ("output", "sources", "tool_calls"):
            if message.get(key):
                scope.append(message[key])
    return scope


# ── okg's text result, read longhand ─────────────────────────────────────
#
# Since okg issue #3059 an okg MCP tool answers with ONE text block: a status
# line (`<tool> ok · ... · generation <id>`), then row headers
# (`[n] <node_id> (<subtype>) · ... · source <name>`). Open WebUI cannot ask
# for the JSON format, so this filter reads the text. `okg` is not importable
# here, so the rules of `okg.substrate.mcp.result_text` (`parse_status_line`,
# `result_generation`, `result_cards`, `fact_value`) are written out
# longhand; `tests/substrate/chat/test_live_filter_agreement.py` pins this
# copy to that module on rendered results. A value that holds the separator
# or a space-like character is a JSON string, so the split below honors
# quotes. Source text that imitates a row header is escaped by the server
# with a leading backslash, so it never matches a row header here.

_OKG_SEP = " \u00b7 "
_OKG_TOOL = re.compile(r"[A-Za-z0-9_]+")
_OKG_CARD = re.compile(r"\[(\d+(?:\.\d+)*)\] ")


def _okg_split_facts(line: str) -> list:
    parts, current, index, quoted = [], [], 0, False
    while index < len(line):
        c = line[index]
        if quoted:
            current.append(c)
            if c == "\\" and index + 1 < len(line):
                current.append(line[index + 1])
                index += 2
                continue
            if c == '"':
                quoted = False
        elif c == '"':
            quoted = True
            current.append(c)
        elif line.startswith(_OKG_SEP, index):
            parts.append("".join(current))
            current = []
            index += len(_OKG_SEP)
            continue
        else:
            current.append(c)
        index += 1
    parts.append("".join(current))
    return parts


def _okg_typed(text: str):
    try:
        return json.loads(text)
    except ValueError:
        return text


def _okg_value(text: str):
    """A fact's value: a JSON string, or the bare rest of the fact."""
    if text.startswith('"'):
        try:
            return json.JSONDecoder().raw_decode(text)[0]
        except ValueError:
            return None
    return _okg_typed(text)


def _okg_card_head(head: str):
    """``(id, subtype)`` of a row header after its ``[n] `` (subtype may be None)."""
    if head.startswith('"'):
        try:
            ident, end = json.JSONDecoder().raw_decode(head)
        except ValueError:
            return None, None
        remainder = head[end:]
    else:
        cut = head.rfind(" (") if head.endswith(")") else -1
        ident = _okg_typed(head[:cut] if cut >= 0 else head)
        remainder = head[cut:] if cut >= 0 else ""
    kind = None
    if remainder.startswith(" (") and remainder.endswith(")"):
        inner = remainder[2:-1]
        kind = _okg_value(inner) if inner.startswith('"') else _okg_typed(inner)
    return ident, kind


def okg_text_result(text) -> Optional[dict]:
    """An okg text result as the envelope fields this filter reads, or None.

    ``{"pinned_generation_id": <the generation fact>, "results": [{"node_id",
    "subtype", "provenance": {"source"}} per row header]}``. None when the
    text has no okg status line (an ordinary string, a vendor error).
    """
    if not isinstance(text, str) or not text:
        return None
    lines = text.split("\n")
    facts = _okg_split_facts(lines[0])
    tool, _, rest = facts[0].partition(" ")
    if not rest or not _OKG_TOOL.fullmatch(tool):
        return None
    word = rest.split(" ", 1)[0]
    if not (rest == "ok" or word in ("refused", "error") or rest.startswith("status=")):
        return None
    envelope: dict = {}
    # The server writes the generation as the LAST fact: read the last match.
    # Since okg PACT read-passage-rows-3666 (D8) only inspect(graph) writes
    # one; a turn reads its generation from that call's result.
    for fact in reversed(facts[1:]):
        if fact.startswith("generation "):
            value = _okg_value(fact[len("generation "):])
            if isinstance(value, str):
                envelope["pinned_generation_id"] = value
            break
    # A search page whose rows share one source names it once, as the
    # status line's first `source <name>` fact; its rows then carry none.
    page_source = None
    for fact in facts[1:]:
        if fact.startswith("source "):
            page_source = _okg_value(fact[len("source "):])
            break
    rows = []
    for line in lines[1:]:
        match = _OKG_CARD.match(line)
        if match is None:
            continue
        parts = _okg_split_facts(line)
        ident, kind = _okg_card_head(parts[0][match.end():])
        row: dict = {"node_id": ident}
        if kind is not None:
            row["subtype"] = kind
        for fact in parts[1:]:
            if fact.startswith("source "):
                row["provenance"] = {"source": _okg_value(fact[len("source "):])}
                break
        else:
            if page_source is not None:
                row["provenance"] = {"source": page_source}
        rows.append(row)
    envelope["results"] = rows
    return envelope


def collect_cited_rows(scope) -> list:
    """Node-shaped MCP rows in this turn's tool output, in order.

    LIVE-ONLY. Open WebUI nests tool results differently depending on how the
    model called them — under ``sources``, inside a ``tool`` message, or as a
    JSON STRING in message content — so the search descends through all
    three, exactly as ``observed_generations`` descends.

    A row is admitted only when it carries BOTH an authoritative ``node_id``
    and an authoritative ``subtype``. That is the same pair the substrate's
    ``link_target_from_row`` treats as node-shaped, so nothing is admitted
    here that the renderer would then have to refuse as malformed.

    De-duplicated on ``(node_id, subtype)``: one node cited twice in a turn is
    one citation. Order is first-seen.
    """
    rows = []
    seen = set()

    def admit(node) -> None:
        node_id = _clean(node.get("node_id"))
        subtype = _clean(node.get("subtype"))
        if node_id is None or subtype is None:
            return
        key = (node_id, subtype)
        if key in seen:
            return
        seen.add(key)
        rows.append(node)

    def walk(node) -> None:
        if isinstance(node, dict):
            admit(node)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, str):
            envelope = okg_text_result(node)
            if envelope is not None:
                walk(envelope)
            elif "node_id" in node or node.lstrip().startswith("["):
                # A JSON STRING: a serialized result set, or Open WebUI's
                # stored content list holding okg's text result.
                try:
                    walk(json.loads(node))
                except ValueError:
                    # Ordinary tool prose, not a serialized result set.
                    pass

    walk(scope)
    return rows


def envelope_for_rows(generations: list):
    """The envelope the links read, or None.

    LIVE-ONLY. The only envelope field the link grammar needs for node rows is
    the pinned generation, and it is supplied ONLY when the turn observed
    exactly one. Two generations in one turn is the CONFLICTING state: pinning
    the links to either one would be a synthesized stamp, so the links carry
    no generation at all and the stamp says so out loud.
    """
    if len(generations) != 1:
        return None
    return {ENVELOPE_GENERATION_FIELD: generations[0]}


def link_target_from_row(row, envelope) -> dict:
    """Build a link target from an MCP row, typed by its subtype.

    Mirrors ``okg.substrate.chat.deep_links.link_target_from_row`` field for
    field. Every malformed row yields an explicit refused target, so a cited
    result can never be silently dropped.
    """
    subtype = _clean(row.get("subtype"))
    node_id = _clean(row.get("node_id"))
    # Presence of any edge field makes the row edge-shaped even when the value
    # is malformed. A node id hidden beside an edge marker is never a route.
    has_edge_markers = any(
        name in row for name in ("edge_id", "src", "dst", "edge_type")
    )
    if has_edge_markers:
        return _target(
            _clean(row.get("edge_id")) or node_id or "(malformed MCP row)",
            None,
            refusal="malformed-row",
        )
    if subtype is not None or node_id is not None:
        if subtype is None or node_id is None:
            return _target(
                node_id or subtype or "(malformed MCP row)",
                None,
                refusal="malformed-row",
            )
        kind = SUBTYPE_TO_TARGET_KIND.get(subtype)
        subtype_refusal = None
        if subtype not in SUBTYPE_TO_TARGET_KIND:
            subtype_refusal = "unknown-subtype"
        elif kind is None:
            subtype_refusal = "unroutable-subtype"
    else:
        subtype_refusal = None
        declared = (
            _clean(envelope.get(ENVELOPE_KIND_FIELD))
            if isinstance(envelope, dict)
            else None
        )
        kind = declared if declared in ID_FIELD_BY_KIND else None

    generation = None
    if isinstance(envelope, dict):
        generation = _clean(envelope.get(ENVELOPE_GENERATION_FIELD))

    if kind is None:
        # No route to choose: name whatever id the row carries so the refusal
        # can quote it, but never pick a route for it.
        for name in ID_FIELD_BY_KIND.values():
            value = _clean(row.get(name))
            if value is not None:
                return _target(
                    value, None, generation=generation, refusal=subtype_refusal
                )
        return _target(
            _clean(row.get("edge_id")) or "(malformed MCP row)",
            None,
            generation=generation,
            refusal="malformed-row",
        )

    identifier = _clean(row.get(ID_FIELD_BY_KIND[kind]))
    if identifier is None:
        return _target(
            _clean(row.get("edge_id")) or "(malformed MCP row)",
            None,
            generation=generation,
            refusal="malformed-row",
        )
    return _target(
        identifier,
        kind,
        generation=generation,
        producer_key=_clean(row.get(PRODUCER_KEY_FIELD)),
    )


def _target(identifier, kind, generation=None, producer_key=None, refusal=None):
    return {
        "id": identifier,
        "kind": kind,
        "generation": generation,
        "producer_key": producer_key,
        "refusal": refusal,
        "label": None,
    }


def _console_hash(target):
    """The console hash for a target, or the refusal that blocked it.

    No kind has a hash today. The console showed records in the Graph view
    and sessions, producer runs and traces in the Trace view, and PACT
    operator-ui-remove-four-views deleted both. Plain text is the honest
    outcome: the console routes an unrecognized hash to Home, so a link to a
    deleted view would land the reader on the wrong page with no error.

    This is a COPY of ``okg.substrate.chat.deep_links`` that runs inside the
    live chat filter, and ``test_live_filter_agreement`` asserts the two
    render byte-identical blocks. Change one and the other fails.
    """
    identifier = _clean(target.get("id"))
    if identifier is None:
        return None, "empty-id"
    if target.get("refusal") is not None:
        return None, target["refusal"]
    kind = target.get("kind")
    if kind is None:
        return None, "undeclared-kind"

    if kind in KINDS_WITHOUT_A_VIEW:
        return None, "no-console-view"
    return None, "unknown-kind"


def deep_link(target, operator_ui_base) -> dict:
    """One target as a console link, or as plain text with a reason.

    There is no default base. An absent or empty ``operator_ui_base`` refuses,
    because a plausible-looking dead link is worse than plain text.
    """
    text = (
        _clean(target.get("label")) or _clean(target.get("id")) or EMPTY_ID_TEXT
    )
    fragment, refusal = _console_hash(target)
    if refusal is not None:
        return {
            "text": text,
            "url": None,
            "refusal": refusal,
            "reason": REFUSAL_REASONS[refusal],
        }
    base = _clean(operator_ui_base)
    if base is None:
        return {
            "text": text,
            "url": None,
            "refusal": "missing-operator-ui-base",
            "reason": REFUSAL_REASONS["missing-operator-ui-base"],
        }
    return {"text": text, "url": base + fragment, "refusal": None, "reason": None}


def link_markdown(link) -> str:
    """The renderable string: a markdown link, or bare plain text."""
    if link.get("url") is not None:
        return "[%s](%s)" % (link["text"], link["url"])
    return link["text"]


def lineage_of(row) -> str:
    """The row's source lineage, from the shapes MCP rows actually use.

    ``provenance.trust`` is deliberately NOT rendered: it is a per-SOURCE
    marker, and next to the generation stamp it would read as the per-answer
    trust_level that nothing produces.
    """
    if not isinstance(row, dict):
        return LINEAGE_UNAVAILABLE
    provenance = row.get("provenance")
    if isinstance(provenance, dict):
        name = _clean(provenance.get("source"))
        if name is not None:
            return name
    source = row.get("source")
    if isinstance(source, str) and source.strip():
        return source.strip()
    if isinstance(source, dict):
        name = _clean(source.get("source_id")) or _clean(source.get("name"))
        if name is not None:
            return name
    return LINEAGE_UNAVAILABLE


def render_stamp(agreement: str, generation_ids: list):
    """The freshness stamp line, or None when the turn read no graph rows.

    Four observations, four renderings, none producible by accident from
    another. Absence is a STATE, never a value: a missing generation renders
    the named unavailable marker and never a default, and never the previous
    turn's id — there is no cross-turn state here to reuse.
    """
    if agreement == "no_graph_read":
        return None
    if agreement == "single":
        return "%s: %s" % (STAMP_FIELD, generation_ids[0])
    if agreement == "conflicting":
        return "%s: %s (%s)" % (
            STAMP_FIELD,
            STAMP_CONFLICTING,
            ", ".join(generation_ids),
        )
    return "%s: %s" % (STAMP_FIELD, STAMP_UNAVAILABLE)


def render_citations(rows, envelope, operator_ui_base) -> list:
    """One citation line per cited row: the id, its lineage, and a link.

    No URL is constructed here. There is no string concatenation in this
    function that could produce one; the link is whatever the deep-link
    grammar returned.
    """
    lines = []
    for row in rows:
        if isinstance(row, dict):
            link = deep_link(link_target_from_row(row, envelope), operator_ui_base)
        else:
            # A malformed result must stay visible; dropping it would turn a
            # refusal into a silent omission.
            link = deep_link(
                _target("(malformed MCP row)", None, refusal="malformed-row"),
                operator_ui_base,
            )
        rendered = link_markdown(link)
        lineage = lineage_of(row)
        if link.get("url") is not None:
            lines.append("- %s%s%s" % (rendered, CITATION_SEPARATOR, lineage))
        else:
            lines.append(
                "- %s%s%s (no console link: %s)"
                % (rendered, CITATION_SEPARATOR, lineage, link["reason"])
            )
    return lines


def render_provenance_block(agreement, generation_ids, rows, envelope, operator_ui_base):
    """The whole footer the outlet appends: stamp first, then citations."""
    parts = []
    stamp = render_stamp(agreement, generation_ids)
    if stamp is not None:
        parts.append(stamp)
    citations = render_citations(rows, envelope, operator_ui_base)
    if citations:
        parts.append(CITATION_HEADER)
        parts.extend(citations)
    return "\n".join(parts)


class Filter:
    class Valves(BaseModel):
        deployment: str = Field(default="", description="Deployment name.")
        preset: str = Field(default="", description="Preset (model) name.")
        openwebui_version: str = Field(default="", description="Pinned version.")
        artifact_path: str = Field(
            default="",
            description="Absolute path of the append-only audit artifact.",
        )
        compaction_enabled: bool = Field(default=False)
        verdict_path: str = Field(
            default="",
            description="Absolute path of the observability verdict artifact.",
        )
        operator_ui_base: str = Field(
            default="",
            # NO DEFAULT and no same-origin guess. The value is the
            # deployment's declared `chat.deep_links.operator_ui_base`; empty
            # means the deployment declared none, and the outlet renders ids
            # as plain text with that reason rather than a guessed URL.
            description=(
                "Absolute base URL of the operator console, from the "
                "deployment's chat.deep_links.operator_ui_base. Empty means "
                "no console link is built."
            ),
        )

    def __init__(self) -> None:
        self.valves = self.Valves()

    # -- paths -----------------------------------------------------------

    def _artifact_path(self) -> str:
        """The artifact to append to, rotating on a producer version bump."""
        configured = (self.valves.artifact_path or "").strip()
        if not configured:
            # No default path. A filter that quietly wrote somewhere else
            # would produce an audit nobody reads, which is worse than a
            # refusal an operator sees.
            raise RuntimeError(
                "okg audit filter: artifact_path valve is unset; refusing the "
                "turn rather than answering unaudited"
            )
        if configured.endswith(".jsonl"):
            return configured
        return resolve_artifact_path(configured)

    # -- the refusal predicate -------------------------------------------

    def _refusal(self) -> Optional[str]:
        """``compaction enabled AND verdict not verified``, or None.

        Suspended ONLY while the host-side proof marker exists, because the
        verdict cannot exist before the proof that writes it. The
        suspension is not settable from inside Open WebUI.
        """
        if not self.valves.compaction_enabled:
            return None
        verdict_path = (self.valves.verdict_path or "").strip()
        if is_verified_compaction_on(
            verdict_path,
            self.valves.deployment,
            self.valves.preset,
            self.valves.openwebui_version,
        ):
            return None
        if proof_run_in_progress(verdict_path):
            return None
        return (
            "okg audit filter: compaction is enabled for preset %r but no "
            "observability verdict records verified_compaction_on for Open "
            "WebUI %s — the audit cannot show the bytes the model receives, "
            "so the turn is refused rather than answered with an audit that "
            "proves nothing" % (self.valves.preset, self.valves.openwebui_version)
        )

    # -- hooks -----------------------------------------------------------

    def inlet(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        refusal = self._refusal()
        if refusal is not None:
            raise RuntimeError(refusal)

        path = self._artifact_path()
        metadata = __metadata__ if isinstance(__metadata__, dict) else {}
        payload = canonical_payload(body if isinstance(body, dict) else {})
        if not payload:
            raise RuntimeError("okg audit filter: empty payload; refusing the turn")

        raw_task = metadata.get("task")
        with _Lock(path):
            artifact_id, seq = _scan(path)
            if artifact_id is None:
                artifact_id = uuid.uuid4().hex
            record = {
                "schema": SCHEMA,
                "schema_version": SCHEMA_VERSION,
                "record_kind": "inlet",
                "artifact_id": artifact_id,
                "record_seq": seq,
                "record_id": "%s:%012d" % (artifact_id, seq),
                "source_revision": 1,
                "deployment": self.valves.deployment or None,
                "preset": self.valves.preset or None,
                "openwebui_version": self.valves.openwebui_version or None,
                "conversation_id": metadata.get("chat_id"),
                "message_id": metadata.get("message_id"),
                "task_kind": task_kind_from_metadata(metadata),
                "task_metadata_raw": str(raw_task) if raw_task is not None else None,
                "principal": (
                    (__user__ or {}).get("email")
                    if isinstance(__user__, dict)
                    else None
                ),
                "model": body.get("model") if isinstance(body, dict) else None,
                "provider": None,
                "timestamp": datetime.now(timezone.utc).isoformat(
                    timespec="microseconds"
                ),
                # The CONFIGURATION vs what THIS payload shows. One name for
                # both would say turn 1 was compacted on a preset where
                # compaction had not yet fired.
                "compaction_enabled": bool(self.valves.compaction_enabled),
                "compaction_observed": COMPACTION_SUMMARY_MARKER in payload,
                "proof_mode": proof_run_in_progress(
                    (self.valves.verdict_path or "").strip()
                ),
                "payload_canonicalization": (
                    "json.dumps({'messages': ..., 'model': ...}, sort_keys=True, "
                    "separators=(',', ':'), ensure_ascii=False)"
                ),
                "payload": payload,
                "payload_sha256": hashlib.sha256(
                    payload.encode("utf-8")
                ).hexdigest(),
            }
            _locked_append(path, record)
        return body

    def outlet(
        self,
        body: dict,
        __user__: Optional[dict] = None,
        __metadata__: Optional[dict] = None,
    ) -> dict:
        """Append the turn COMPLETION record, then RENDER the provenance.

        The join key is re-derived from the artifact rather than held in
        memory: the newest inlet record for this ``(conversation_id,
        message_id)``. An in-memory map would be lost on a restart and
        wrong across workers, and a turn whose completion record went
        missing would look like a turn that never finished.

        The observed generation comes from whatever MCP envelopes the turn's
        tool results carried. When the turn made no graph read, that is
        RECORDED as such rather than left as a null someone could read as a
        missing stamp.

        ONE DERIVATION, TWO CONSUMERS. The ``generations``/``agreement`` pair
        computed here is written into the completion record AND rendered as
        the answer's stamp — the same values, not two computations that could
        disagree. That is what the record schema means by "the same quantity
        the outlet stamp renders".
        """
        path = self._artifact_path()
        metadata = __metadata__ if isinstance(__metadata__, dict) else {}
        conversation_id = metadata.get("chat_id")
        message_id = metadata.get("message_id")

        generations = observed_generations(body)
        if not generations:
            agreement = "no_graph_read" if not _has_tool_results(body) else "missing"
        elif len(generations) == 1:
            agreement = "single"
        else:
            agreement = "conflicting"

        with _Lock(path):
            record_id = _latest_inlet_record_id(path, conversation_id, message_id)
            if record_id is not None:
                _locked_append(
                    path,
                    {
                        "schema": SCHEMA,
                        "schema_version": SCHEMA_VERSION,
                        "record_kind": "completion",
                        "record_id": record_id,
                        "graph_read": agreement != "no_graph_read",
                        "generation_agreement": agreement,
                        "observed_generation_id": (
                            generations[0] if agreement == "single" else None
                        ),
                        "observed_generation_ids": generations,
                        "timestamp": datetime.now(timezone.utc).isoformat(
                            timespec="microseconds"
                        ),
                    },
                )
            # No inlet record to join to: no completion record is written,
            # because a completion carrying a made-up id would put an
            # unjoinable row in the interface chat-transcript-source reads.
            # The ANSWER is still stamped and cited — provenance is what the
            # reader needs, and withholding it on the one turn whose audit is
            # already in doubt is exactly backwards.
        return self._render_provenance(body, agreement, generations)

    def _render_provenance(self, body: dict, agreement: str, generations: list) -> dict:
        """Append the stamp and citation block to the answer.

        The block goes on the LAST assistant message, after a horizontal
        rule, which is where the substrate outlet puts it.

        TWO PLACES, MEASURED not guessed. An assistant turn that used tools
        carries an ``output`` array, and at the pinned v0.11.0 the frontend
        renders THAT — an edit confined to the message-level ``content`` was
        persisted (middleware.py:3528-3556 upserts it, keeping the pre-edit
        text as ``originalContent``) and never appeared on screen. The vendor
        checks ``output_changed`` beside ``content_changed`` in the same
        block, so editing both is the supported path. They are written with
        the SAME text: a reader and a downstream consumer must not be shown
        two different answers.

        Idempotent by construction: Open WebUI can call a filter's outlet
        more than once for one assistant turn, and a second footer would read
        as a second answer.
        """
        rows = collect_cited_rows(current_turn_scope(body))
        block = render_provenance_block(
            agreement,
            generations,
            rows,
            envelope_for_rows(generations),
            # Empty valve means the deployment declared no console base. It is
            # passed through as None so the link renderer REFUSES with that
            # reason; it is never a hostname to guess from.
            (self.valves.operator_ui_base or "").strip() or None,
        )
        if not block or not isinstance(body, dict):
            return body
        messages = body.get("messages")
        if not isinstance(messages, list):
            return body
        for message in reversed(messages):
            if not isinstance(message, dict) or message.get("role") != "assistant":
                continue
            _append_block(message, "content", block)
            _append_to_rendered_output(message, block)
            return body
        messages.append({"role": "assistant", "content": block})
        return body


def _append_block(holder: dict, key: str, block: str) -> bool:
    """Append the footer to ``holder[key]``, once. True if it is there now."""
    text = holder.get(key)
    if isinstance(text, str) and block in text:
        return True
    holder[key] = "%s\n\n---\n%s" % (text, block) if text else block
    return True


def _append_to_rendered_output(message: dict, block: str) -> bool:
    """Append the footer to the text the pinned frontend actually renders.

    The last ``type == "message"`` entry of the ``output`` array, and within
    it the last ``output_text`` part: that is the answer body. Reasoning and
    function-call entries are left alone — a footer inside a collapsed
    reasoning block is a footer nobody reads.

    False when the turn carried no output array at all, which is the ordinary
    shape for an answer that used no tools; the message-level ``content``
    edit is the whole rendering in that case.
    """
    output = message.get("output")
    if not isinstance(output, list):
        return False
    for entry in reversed(output):
        if not isinstance(entry, dict) or entry.get("type") != "message":
            continue
        parts = entry.get("content")
        if not isinstance(parts, list):
            continue
        for part in reversed(parts):
            if not isinstance(part, dict) or part.get("type") != "output_text":
                continue
            return _append_block(part, "text", block)
    return False


def _has_tool_results(body: dict) -> bool:
    """Did this turn carry any tool output at all?

    "The turn read no graph rows" and "the turn read rows whose envelope
    carried no generation" are different facts; this is what keeps them
    apart.
    """
    if not isinstance(body, dict):
        return False
    if body.get("sources"):
        return True
    for message in body.get("messages") or []:
        if not isinstance(message, dict):
            continue
        if message.get("role") == "tool" or message.get("tool_calls"):
            return True
        if message.get("sources"):
            return True
    return False


def observed_generations(body: dict) -> list:
    """Distinct pinned generation ids anywhere in the turn's tool output.

    ``pinned_generation_id`` is the canonical MCP envelope field and
    ``generation_id`` the compat name; both are searched, and the search is
    over the WHOLE body because Open WebUI nests tool results differently
    depending on how the model called them.
    """
    found = []

    def walk(node) -> None:
        if isinstance(node, dict):
            for field in ("pinned_generation_id", "generation_id"):
                value = node.get(field)
                if isinstance(value, str) and value.strip():
                    if value.strip() not in found:
                        found.append(value.strip())
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif isinstance(node, str):
            # okg's text result (issue #3059): the generation is a fact on
            # its status line.
            envelope = okg_text_result(node)
            if envelope is not None:
                walk(envelope)
            elif "generation_id" in node or node.lstrip().startswith("["):
                # Tool results often arrive as a JSON STRING in message
                # content, including Open WebUI's stored content list.
                try:
                    walk(json.loads(node))
                except ValueError:
                    # Non-JSON strings are ordinary tool output, not generation data.
                    pass

    walk(body)
    return found


def _latest_inlet_record_id(path, conversation_id, message_id):
    """The newest inlet record for this (conversation, message), or None.

    NEWEST, not first: an edited-and-resent message reuses the pair, and the
    live turn is the most recent one. This is the join rule the record
    schema states for consumers.
    """
    latest_seq = -1
    latest_id = None
    for line in _read_lines(path):
        if not line.strip():
            continue
        record = _parse(line, path)
        if record.get("record_kind") != "inlet":
            continue
        if (
            record.get("conversation_id") == conversation_id
            and record.get("message_id") == message_id
        ):
            seq = int(record.get("record_seq") or 0)
            if seq > latest_seq:
                latest_seq = seq
                latest_id = record.get("record_id")
    return latest_id
