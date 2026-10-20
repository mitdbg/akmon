"""The chat answer says PENDING, names the envelope, and never says published.

PACT ``chat-record-affordance``, req.honest-affordance.

What is asserted here is the TEXT a user is given, for two arms produced by the
same tool on the same server in the same run: a write that landed as a pending
admission envelope, and a write the tool refused. The success arm's envelope
identifier is pulled back OUT of the rendered sentence and looked up in
``okg.admission_envelopes`` -- so an answer naming an identifier we composed,
or an identifier for a row that does not exist, fails here.

Both arms come from real calls. Neither answer is hand-built in this file: a
test that rendered its own result dict would prove the renderer's arithmetic
and nothing about what the tool actually hands a chat client.

This file stops at the tool result's ``answer`` field. That a MODEL relays it
is the preset instruction's job, asserted in
``test_write_connection_projection.py`` where the write preset's system prompt
is composed; that a USER sees it is the live transcript
(task.chat-record-affordance.live-write-proof), which pytest cannot supply.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.deployment.testing import mock_dbos_context
from okg.substrate.mcp.operators import (
    FORBIDDEN_WRITE_ANSWER_CLAIMS,
    PENDING_REVIEW_PHRASE,
    REFUSED_PHRASE,
)
from okg.substrate.mcp.server import build_server

pytestmark = [
    pytest.mark.postgres,
]

DEPLOYMENT = "chat-answer-fixture"

#: Fresh per run, for the same reason as in ``test_chat_write_admission``: a
#: record node id is content-derived, so a fixed summary would submit the same
#: idempotency key twice against a shared database and the second run would be
#: answered from the first run's envelope.
RUN_TOKEN = uuid.uuid4().hex
SUMMARY = f"The chat user observed an honest answer contract [{RUN_TOKEN}]."

AUTHORITY: dict[str, Any] = {
    "producer_id": "agent:chat-answer-fixture",
    "producer_kind": "agent",
}

#: Any RFC-4122-shaped token. Used two ways: to pull the envelope identifier
#: out of the success answer for a database lookup, and to prove the refusal
#: answer contains no such token at all.
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def _forbidden_claims(answer: str) -> list[str]:
    """Which false claims a piece of answer text makes.

    One predicate, used on the real answer AND on a deliberately dishonest
    sentence, so "the real answer makes none of them" is paired with proof
    that the check can find one.
    """
    lowered = answer.lower()
    return [claim for claim in FORBIDDEN_WRITE_ANSWER_CLAIMS if claim in lowered]


def _seed_agent_memory_catalog(dsn: str, tmp_path: Path) -> int:
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "chat-answer-agent-memory-dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "chat-answer-agent-memory-dep",
                "description": "chat record affordance answer fixture",
                "modules": ["agent_memory"],
            }
        ),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(dsn) as conn:
        return int(apply_composed_catalog(conn, composed))


def _envelope_count(dsn: str) -> int:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.admission_envelopes")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _envelope_exists(dsn: str, admission_id: str) -> dict[str, Any] | None:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT admission_mode, status FROM okg.admission_envelopes "
            "WHERE admission_id = %s",
            (admission_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {"admission_mode": row[0], "status": row[1]}


@pytest.fixture(scope="module")
def answers(okg_migrated: str, tmp_path_factory: Any) -> Iterator[dict[str, Any]]:
    """One server, one success, one refusal, and the envelope count around each."""
    dsn = okg_migrated
    tmp_path = tmp_path_factory.mktemp("chat-answer")
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(dsn, tmp_path)
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\n"
        "description: chat record affordance answer fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )

    observed: dict[str, Any] = {"dsn": dsn}
    # A fresh `okg mcp-serve` process has not bootstrapped the Chronos schema
    # yet. Start from that state rather than from whatever an earlier test in
    # this process left in the cache (#2639).
    from okg.substrate.graph_store_internal.common import clear_schema_ensured_cache

    clear_schema_ensured_cache()
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
        with mock_dbos_context():
            mp.setattr(admission_workflows, "_resolve_dsn", lambda _deployment: dsn)
            admission_workflows.register_admission_workflows(deployment=DEPLOYMENT)

            mcp = build_server(dsn=dsn, deployment_name=DEPLOYMENT, allow_writes=True)
            # The read-only sibling, built from the same code with the one
            # operator decision flipped. Nothing is called on it: what it is
            # here to show is that a chat client on a read connection has no
            # `record` to call, which is the transport-boundary half of "a
            # refused write is distinguishable from a queued one".
            read_only = build_server(
                dsn=dsn, deployment_name=DEPLOYMENT, allow_writes=False,
            )
            tools = mcp._tool_manager._tools
            try:
                observed["read_only_tool_names"] = sorted(
                    read_only._tool_manager._tools
                )

                observed["envelopes_before_success"] = _envelope_count(dsn)
                observed["success"] = tools["record"].fn(
                    kind="observation",
                    payload={
                        "summary": SUMMARY,
                        "confidence_kind": "deterministic",
                        "occurred_at": "2026-08-11T11:00:00Z",
                    },
                    authority=dict(AUTHORITY),
                )
                observed["envelopes_after_success"] = _envelope_count(dsn)

                # A REAL refusal from the same tool: the payload does not
                # satisfy the record template, so the write never reaches
                # admission. Refusing at the tool boundary is the ordinary
                # case a chat user hits by asking for something the schema
                # does not accept.
                observed["envelopes_before_refusal"] = _envelope_count(dsn)
                observed["refusal"] = tools["record"].fn(
                    kind="observation",
                    payload={"not_a_field": "no summary, no confidence_kind"},
                    authority=dict(AUTHORITY),
                )
                observed["envelopes_after_refusal"] = _envelope_count(dsn)
                yield observed
            finally:
                for server in (mcp, read_only):
                    server._okg_pool.close()
                    if server._okg_write_pool:
                        server._okg_write_pool.close()


def _success_answer(answers: dict[str, Any]) -> str:
    answer = answers["success"].get("answer")
    assert isinstance(answer, str) and answer, answers["success"]
    return answer


def _refusal_answer(answers: dict[str, Any]) -> str:
    answer = answers["refusal"].get("answer")
    assert isinstance(answer, str) and answer, answers["refusal"]
    return answer


# ---------------------------------------------------------------------------
# The success arm
# ---------------------------------------------------------------------------

def test_success_answer_states_pending_review(answers: dict[str, Any]) -> None:
    """The write landed, and the sentence says what "landed" means here."""
    # The arm is about a write that DID land -- otherwise "pending review" is
    # trivially the right thing to say about nothing.
    assert answers["success"]["status"] == "pending"
    assert (
        answers["envelopes_after_success"]
        == answers["envelopes_before_success"] + 1
    )
    answer = _success_answer(answers)
    assert PENDING_REVIEW_PHRASE in answer.lower()
    # And it says where, so the user is not told to wait on an inbox they
    # cannot find.
    assert "okg admission list" in answer


def test_success_answer_names_the_envelope_id_that_exists_in_the_database(
    answers: dict[str, Any],
) -> None:
    """The identifier is read OUT of the sentence and looked UP in the table."""
    answer = _success_answer(answers)
    named = _UUID_RE.findall(answer)
    assert named, f"the answer names no envelope identifier: {answer!r}"
    assert len(set(named)) == 1, named
    row = _envelope_exists(answers["dsn"], named[0])
    assert row is not None, (
        f"the answer names envelope {named[0]!r}, which is not a row in "
        "okg.admission_envelopes -- an invented identifier"
    )
    assert row["admission_mode"] == "review"
    assert row["status"] == "pending"
    # ... and it is THE envelope this call created, not merely some row.
    assert named[0] == answers["success"]["admission_id"]


def test_success_answer_makes_no_recorded_saved_added_or_published_claim(
    answers: dict[str, Any],
) -> None:
    answer = _success_answer(answers)
    assert _forbidden_claims(answer) == []
    # The same predicate, on a sentence that lies, finds the lie. Without
    # this, an empty result would equally mean the check looks for nothing.
    dishonest = "Your finding was recorded and saved; it has been added and published."
    assert sorted(_forbidden_claims(dishonest)) == sorted(
        FORBIDDEN_WRITE_ANSWER_CLAIMS
    )
    # The claim the answer is REQUIRED to make instead, so "says none of the
    # four" is not satisfied by an answer that says nothing.
    assert "not in the graph" in answer.lower()


# ---------------------------------------------------------------------------
# The refusal arm
# ---------------------------------------------------------------------------

def test_refusal_answer_states_the_reason(answers: dict[str, Any]) -> None:
    """The refusal is real, and its reason travels into the sentence."""
    result = answers["refusal"]
    assert result["status"] == "refused", result
    # Nothing was admitted: the count is the observable end state, so a
    # "refusal" that quietly created an envelope cannot pass.
    assert answers["envelopes_after_refusal"] == answers["envelopes_before_refusal"]
    answer = _refusal_answer(answers)
    assert REFUSED_PHRASE in answer.lower()
    code = str((result.get("error") or {}).get("code") or "")
    assert code, result
    assert code in answer, (answer, code)


def test_refusal_answer_is_textually_distinct_from_the_success_answer(
    answers: dict[str, Any],
) -> None:
    success = _success_answer(answers)
    refusal = _refusal_answer(answers)
    assert refusal != success
    # Distinct in the way that matters: the refusal never claims the pending
    # status, and the success never claims the refusal. Two texts that merely
    # differed could still both read as "your write is queued".
    assert PENDING_REVIEW_PHRASE not in refusal.lower()
    assert REFUSED_PHRASE not in success.lower()


def test_refusal_answer_invents_no_envelope_id(answers: dict[str, Any]) -> None:
    refusal = _refusal_answer(answers)
    assert _UUID_RE.findall(refusal) == []
    # The regex is not blind: it finds one in the success answer, from the
    # same run and the same renderer.
    assert _UUID_RE.findall(_success_answer(answers))
    assert answers["success"]["admission_id"] not in refusal


# ---------------------------------------------------------------------------
# The boundary the answers sit behind
# ---------------------------------------------------------------------------

def test_every_record_return_path_carries_an_answer(
    answers: dict[str, Any],
) -> None:
    """Both outcomes are answered, including the one that failed.

    The refusal path is where a missing answer does the damage: a model with
    no sentence to relay will compose its own, and the sentence it composes
    about a write it believes it made is the false one.
    """
    for arm in ("success", "refusal"):
        assert isinstance(answers[arm].get("answer"), str), arm
        assert answers[arm]["answer"].strip(), arm


def test_a_read_only_server_offers_no_record_tool_to_refuse(
    answers: dict[str, Any],
) -> None:
    """The other half of "distinguishable": on a read connection there is no tool.

    A chat client bound to the read connection is not offered ``record`` at
    all, so it cannot produce a write whose answer needs disambiguating. The
    write-capable server in this same fixture does offer it -- the pair is
    what makes the absence attributable to ``allow_writes`` rather than to a
    server that registered nothing.
    """
    names = answers["read_only_tool_names"]
    assert names, "a read-only server that registered no tools proves nothing"
    assert "record" not in names
    assert "propose" not in names
    assert "inspect" in names
