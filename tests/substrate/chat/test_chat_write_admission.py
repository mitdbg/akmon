"""A chat write is HELD: pending, on no branch, and not yet a fact.

PACT ``chat-record-affordance``, req.pending-envelope-not-graph.

Every assertion is on a DATABASE ROW or on a read-surface result, never on the
tool response body -- a submit that returned the right dict while writing
nothing has to fail here.

The two absence arms are paired, in the SAME session and against the SAME
reads, with a positive control that accepts the envelope, lets the decide step
emit the facts, publishes, and shows those identical reads returning the
content by node id. Absence proven by a read that never worked is not absence.

Note on the transport. The tool functions exercised here are the ones the write
CONNECTION advertises (``test_write_connection_projection.py`` proves the
connection carries exactly this pair). What this file proves is what those
calls do to the database; that the HTTP transport in front of them is
authenticated is ``mcp-http-auth``'s, and the connection is not projected
without it.

The ``quarantine:review`` branch plays no part in this path and is asserted
nowhere: an MCP write is never placed on it, so "the quarantine branch shows
nothing pending" would be a true statement about an unrelated mechanism.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.deployment.testing import mock_dbos_context
from okg.substrate.mcp.server import build_server
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "chat-record-fixture"
FORK_BRANCH = "fork/chat-record-lane"

#: One token per RUN, woven into every summary below.
#:
#: A record node id is derived from its content, and the positive control
#: PUBLISHES the node -- so a second run against the same database would find
#: the first run's node already in the graph and read its own "absent before
#: acceptance" arms as satisfied by a node that was never absent. That is the
#: exact false witness this program keeps shipping: an assertion passing for a
#: reason unrelated to the thing under test. A fresh token makes every node id
#: in this module new, so the absence arms are absences of THIS run's content.
RUN_TOKEN = uuid.uuid4().hex

SUMMARY = f"The chat user observed that the closure table was produced [{RUN_TOKEN}]."
FORK_SUMMARY = f"The fork lane observed a second closure table [{RUN_TOKEN}]."
CALLER_SUMMARY = (
    f"The caller tried to route and fast-track its own write [{RUN_TOKEN}]."
)
#: The control content: recorded AND accepted before the pre-acceptance
#: publish, so the generation the absence arms interrogate provably carries a
#: fact that arrived by this exact route.
CONTROL_SUMMARY = f"The control finding was accepted before the read [{RUN_TOKEN}]."

AUTHORITY: dict[str, Any] = {
    "producer_id": "agent:chat-record-fixture",
    "producer_kind": "agent",
}


def _seed_agent_memory_catalog(dsn: str, tmp_path: Path) -> int:
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "chat-record-agent-memory-dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "chat-record-agent-memory-dep",
                "description": "chat record affordance agent-memory fixture",
                "modules": ["agent_memory"],
            }
        ),
        encoding="utf-8",
    )
    (dep / "schemas").mkdir()
    composed = compose_catalog(dep)
    with psycopg.connect(dsn) as conn:
        return int(apply_composed_catalog(conn, composed))


def _published_graph_query(
    dsn: str, generation_id: Any, sql: str, params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation_id)
        return graph.query(sql, params or {})


def _envelope_row(dsn: str, admission_id: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT admission_mode, status, target_branch_name, authority "
            "FROM okg.admission_envelopes WHERE admission_id = %s",
            (admission_id,),
        )
        row = cur.fetchone()
    assert row is not None, f"no admission_envelopes row for {admission_id}"
    return {
        "admission_mode": row[0],
        "status": row[1],
        "target_branch_name": row[2],
        "authority": row[3],
    }


def _payload_ref_rows(dsn: str, admission_id: str) -> list[dict[str, Any]]:
    """The HELD payload rows for one envelope, content included.

    ``payload`` is the jsonb column the review payload actually lands in
    (``admission.attach_payload_ref``); the ``ref_key`` is the literal string
    ``'payload'``, not the record id. Reading the column rather than the key is
    what makes "the payload is held" checkable -- a row whose jsonb is ``{}``
    would satisfy any assertion phrased over keys alone.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ref_key, ref_kind, payload FROM okg.admission_record_refs "
            "WHERE admission_id = %s ORDER BY ref_key",
            (admission_id,),
        )
        rows = cur.fetchall()
    return [{"ref_key": r[0], "ref_kind": r[1], "payload": r[2]} for r in rows]


@pytest.fixture(scope="module")
def chat_write(okg_migrated: str, tmp_path_factory: Any) -> Iterator[dict[str, Any]]:
    """One session: submit, observe absence, accept, publish, observe presence.

    Module-scoped on purpose. The requirement makes the positive control
    admissible only when it runs "in the same run against the same reads", so
    the absence reads and the after-acceptance reads are the SAME calls in the
    SAME process against the SAME server, captured here and asserted
    individually below.

    Every database read is taken HERE, at the moment it is about it, and
    handed to the tests as a snapshot. A test that queried
    ``okg.admission_envelopes`` in its own body would run after this whole
    sequence -- including the acceptance -- and would read ``accepted`` where
    the requirement is about ``pending``. Both snapshots are kept
    (``envelopes_pending`` and ``envelope_after_decide``) so "pending" is a
    state the same query is shown LEAVING, not a constant.
    """
    dsn = okg_migrated
    tmp_path = tmp_path_factory.mktemp("chat-record")
    from okg.substrate.runtime import admission_workflows

    _seed_agent_memory_catalog(dsn, tmp_path)
    deployments_root = tmp_path / "deployments"
    dep = deployments_root / DEPLOYMENT
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        f"name: {DEPLOYMENT}\n"
        "description: chat record affordance fixture\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )

    observed: dict[str, Any] = {"dsn": dsn}
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("OKG_DEPLOYMENTS_DIR", str(deployments_root))
        with mock_dbos_context():
            mp.setattr(
                admission_workflows, "_resolve_dsn", lambda _deployment: dsn,
            )
            admission_workflows.register_admission_workflows(deployment=DEPLOYMENT)

            # `auto_advance="always"` for one reason, and it is a correctness
            # reason rather than a convenience: under the default "never" the
            # latest pin is served from a 500ms cache
            # (`server.LATEST_PIN_CACHE_TTL_S`), so a read taken shortly after
            # a publish answers at the PREVIOUS generation. Both arms below
            # would then be artefacts of that window -- the absence would be
            # "we looked at an older graph", and the positive control would
            # fail for a reason having nothing to do with admission. "always"
            # resolves the latest generation per call, which makes the absence
            # the strictly stronger claim (absent at the newest published
            # generation, not merely at some pin we happened to hold).
            mcp = build_server(
                dsn=dsn,
                deployment_name=DEPLOYMENT,
                allow_writes=True,
                auto_advance="always",
            )
            fork_mcp = build_server(
                dsn=dsn,
                deployment_name=DEPLOYMENT,
                allow_writes=True,
                branch_name=FORK_BRANCH,
                auto_advance="always",
            )
            tools = mcp._tool_manager._tools
            fork_tools = fork_mcp._tool_manager._tools
            try:
                # A generation must exist before the read surface can answer
                # at all; otherwise "the content is absent" would be
                # indistinguishable from "there is nothing to read from".
                observed["publish_bootstrap"] = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
                observed["submitted"] = tools["record"].fn(
                    kind="observation",
                    payload={
                        "summary": SUMMARY,
                        "confidence_kind": "deterministic",
                        "occurred_at": "2026-08-11T10:00:00Z",
                    },
                    authority=dict(AUTHORITY),
                )
                # The caller ASSERTS a branch and a fast-track admission mode
                # in the same block the server builds its authority from.
                observed["caller_asserted"] = tools["record"].fn(
                    kind="observation",
                    payload={
                        "summary": CALLER_SUMMARY,
                        "confidence_kind": "deterministic",
                        "occurred_at": "2026-08-11T10:01:00Z",
                    },
                    authority={
                        **AUTHORITY,
                        "target_branch_name": "fork/attacker-chosen",
                        "admission_mode": "fast_track",
                        "producer_policy": {"admission_mode": "fast_track"},
                        "admission_policy": {"admission_mode": "fast_track"},
                    },
                )
                # The control: a NON-default server, whose own branch IS
                # pinned. Without it, "target_branch_name is NULL" is equally
                # explained by a column nothing ever writes.
                observed["fork_submitted"] = fork_tools["record"].fn(
                    kind="observation",
                    payload={
                        "summary": FORK_SUMMARY,
                        "confidence_kind": "deterministic",
                        "occurred_at": "2026-08-11T10:02:00Z",
                    },
                    authority=dict(AUTHORITY),
                )

                node_id = observed["submitted"]["record_id"]
                observed["node_id"] = node_id

                # --- the ROW snapshots, BEFORE any acceptance ---
                # Taken here rather than in the test bodies: the acceptance
                # below moves `submitted`'s envelope to `accepted`, so a row
                # read at test time would answer about the wrong moment.
                observed["envelopes_pending"] = {
                    name: _envelope_row(dsn, observed[name]["admission_id"])
                    for name in ("submitted", "caller_asserted", "fork_submitted")
                }
                observed["refs_pending"] = _payload_ref_rows(
                    dsn, observed["submitted"]["admission_id"],
                )

                # --- the CONTROL content: same route, taken all the way ---
                # Recorded and ACCEPTED before the pre-acceptance publish, so
                # the generation the absence arms interrogate carries a fact
                # that got there by record -> accept -> publish. Without it,
                # "no row for the pending node" would be equally explained by
                # a generation that carries nothing at all, which is the
                # ordinary state of a deployment with no sources.
                control = tools["record"].fn(
                    kind="observation",
                    payload={
                        "summary": CONTROL_SUMMARY,
                        "confidence_kind": "deterministic",
                        "occurred_at": "2026-08-11T09:59:00Z",
                    },
                    authority=dict(AUTHORITY),
                )
                observed["control_node_id"] = control["record_id"]
                observed["control_decided"] = tools["propose"].fn(
                    action="decide",
                    payload={
                        "admission_id": control["admission_id"],
                        "decision": "accept",
                        "reason": "chat-record-affordance absence control",
                    },
                    authority={"actor": "reviewer", "allow_write": True},
                )

                # --- the reads, BEFORE the pending envelope is accepted ---
                # Publish FIRST, then read. A publish cycle between the submit
                # and the read is what makes "absent at the current
                # generation" a claim about a generation that had every
                # opportunity to carry the fact, rather than about a graph
                # that had not been advanced since the submit.
                before = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
                observed["publish_before"] = before
                assert before.get("generation_id") is not None, before
                observed["read_before"] = _read_node(tools, node_id)
                observed["read_before_control"] = _read_node(
                    tools, observed["control_node_id"],
                )
                observed["graph_before"] = _published_graph_query(
                    dsn,
                    before["generation_id"],
                    "SELECT node_id FROM okg.graph_nodes WHERE node_id = :nid",
                    {"nid": node_id},
                )
                # The same SQL, the same generation, the control's id.
                observed["graph_before_control"] = _published_graph_query(
                    dsn,
                    before["generation_id"],
                    "SELECT node_id FROM okg.graph_nodes WHERE node_id = :nid",
                    {"nid": observed["control_node_id"]},
                )

                # --- accept, emit, publish ---
                observed["decided"] = tools["propose"].fn(
                    action="decide",
                    payload={
                        "admission_id": observed["submitted"]["admission_id"],
                        "decision": "accept",
                        "reason": "chat-record-affordance fixture",
                    },
                    authority={"actor": "reviewer", "allow_write": True},
                )
                after = Publisher(dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT)).cycle()
                observed["publish_after"] = after
                observed["envelope_after_decide"] = _envelope_row(
                    dsn, observed["submitted"]["admission_id"],
                )

                # --- THE SAME reads, after ---
                observed["read_after"] = _read_node(tools, node_id)
                observed["graph_after"] = _published_graph_query(
                    dsn,
                    after["generation_id"],
                    "SELECT node_id, attrs->>'summary' AS summary "
                    "FROM okg.graph_nodes WHERE node_id = :nid",
                    {"nid": node_id},
                )
                yield observed
            finally:
                for server in (mcp, fork_mcp):
                    server._okg_pool.close()
                    if server._okg_write_pool:
                        server._okg_write_pool.close()


def _read_node(tools: Any, node_id: str) -> dict[str, Any]:
    """One ``inspect(kind='node')`` read, captured either way.

    The read surface answers an absent id with a STRUCTURED ``node_not_found``
    carrying ``exists: False`` (``mcp/node_lookup``), which is what makes the
    absence arms assertable: "absent" is a specific answer the read gave, not
    "something went wrong". Any other exception is re-raised, so a broken read
    can never be captured as an absence.
    """
    from okg.substrate.errors import NodeNotFound

    try:
        payload = tools["inspect"].fn(
            target={"kind": "node", "node_id": node_id},
        )
    except NodeNotFound as exc:
        return {
            "present": False,
            "code": exc.code,
            "exists": exc.data.get("exists"),
            "pinned_generation_id": exc.data.get("pinned_generation_id"),
        }
    return {"present": True, "payload": payload}


def test_chat_record_creates_pending_envelope_with_review_mode(
    chat_write: dict[str, Any],
) -> None:
    row = chat_write["envelopes_pending"]["submitted"]
    assert row["admission_mode"] == "review"
    assert row["status"] == "pending"
    # `pending` is a state, not a constant the query always reports: the SAME
    # read of the SAME row after the acceptance says `accepted`. Without this
    # pairing, a status column that never changed would satisfy the arm above.
    assert chat_write["envelope_after_decide"]["status"] == "accepted"


def test_chat_record_payload_is_held_in_admission_record_refs(
    chat_write: dict[str, Any],
) -> None:
    refs = chat_write["refs_pending"]
    assert refs, "the payload must be HELD on the envelope, not dropped"
    held = [r for r in refs if r["ref_kind"] == "payload"]
    assert held, [r["ref_kind"] for r in refs]
    # The CONTENT is what is held, checked by looking inside the jsonb rather
    # than at the row's existence: an empty `{}` payload row is a held nothing.
    blob = str(held[0]["payload"])
    assert SUMMARY in blob
    assert chat_write["submitted"]["record_id"] in blob


def test_chat_record_leaves_target_branch_name_null_on_a_default_server(
    chat_write: dict[str, Any],
) -> None:
    row = chat_write["envelopes_pending"]["submitted"]
    assert row["target_branch_name"] is None


def test_caller_supplied_branch_is_dropped_over_the_chat_transport(
    chat_write: dict[str, Any],
) -> None:
    row = chat_write["envelopes_pending"]["caller_asserted"]
    assert row["target_branch_name"] is None
    assert "fork/attacker-chosen" not in str(row["authority"])
    # ... and the response does not report the caller's value back as accepted.
    assert (
        chat_write["caller_asserted"].get("target_branch_name")
        != "fork/attacker-chosen"
    )


def test_caller_supplied_fast_track_is_downgraded_to_review(
    chat_write: dict[str, Any],
) -> None:
    row = chat_write["envelopes_pending"]["caller_asserted"]
    assert row["admission_mode"] == "review"
    assert row["status"] == "pending"
    assert "fast_track" not in str(row["authority"])
    assert chat_write["caller_asserted"]["admission_mode"] == "review"


def test_non_default_server_still_pins_its_own_branch(
    chat_write: dict[str, Any],
) -> None:
    """The control for the NULL above: the column IS written when it applies."""
    row = chat_write["envelopes_pending"]["fork_submitted"]
    assert row["target_branch_name"] == FORK_BRANCH


def test_pending_content_is_absent_from_the_mcp_read_surface(
    chat_write: dict[str, Any],
) -> None:
    read = chat_write["read_before"]
    assert read["present"] is False
    # The specific answer, not merely "an error happened": the read surface
    # says the id does not exist at the pin.
    assert read["code"] == "node_not_found"
    assert read["exists"] is False
    assert read["pinned_generation_id"] is not None
    # The same read, the same pin, an accepted node: present. A read surface
    # that answered "not found" for everything would otherwise satisfy the
    # arm above.
    control = chat_write["read_before_control"]
    assert control["present"] is True, control
    assert control["payload"]["node_id"] == chat_write["control_node_id"]


def test_pending_content_is_absent_from_the_graph_at_the_current_generation(
    chat_write: dict[str, Any],
) -> None:
    assert chat_write["graph_before"] == []
    # An empty result must not be free. The SAME query against the SAME
    # generation returns the control node -- recorded through the same tool
    # and accepted before this publish -- so "no row for the pending node" is
    # a fact about that node's pending status, not about a generation that
    # carries nothing or a query that selects nothing.
    control_rows = chat_write["graph_before_control"]
    assert len(control_rows) == 1, control_rows
    assert control_rows[0]["node_id"] == chat_write["control_node_id"]


def test_accepted_envelope_makes_the_same_reads_return_the_content_by_node_id(
    chat_write: dict[str, Any],
) -> None:
    """The positive control that makes the two absence arms non-vacuous."""
    assert chat_write["decided"]["status"] == "accepted"
    assert chat_write["publish_after"]["status"] == "published"
    # The graph MOVED between the two observations; otherwise both arms would
    # be reading the same picture and only one of them could be right.
    assert (
        chat_write["publish_after"]["generation_id"]
        != chat_write["publish_before"]["generation_id"]
    )
    rows = chat_write["graph_after"]
    assert len(rows) == 1, rows
    assert rows[0]["node_id"] == chat_write["node_id"]
    assert rows[0]["summary"] == SUMMARY
    read = chat_write["read_after"]
    assert read["present"] is True
    assert read["payload"]["node_id"] == chat_write["node_id"]
