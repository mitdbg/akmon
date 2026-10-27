"""`inspect` node_history and subtype_diff over a real MCP client session.

PACT okg-attribute-history-read. One deployment, five real publishes:

* X (compute_node): g2 changes a real field (cpus), g3 only a volatile field
  (sampled_at), g4 a real field (state) and the volatile field together, g5
  only the lineage revision;
* Y (rack) changes in g2, so another subtype changes in the same publishes;
* W moves from compute_node to rack in g3; V is added in g4; U is retracted
  in g5; Z never changes;
* M (classification restricted) changes only its owner_email in g2, a value
  the Nomos policy masks;
* S is from a source the reader may not read; H is from that source until g5
  and from the readable source in g5;
* G (host) is reclassified by its source in g5 to a classification the
  policy denies: visible at g1 to g4, hidden at the pin.

Rows from `inventory` are untrusted (it has no producer profile); S and H
carry no source column and read as trusted.

Every read runs through `build_server`, whose read pool is the okg_mcp role,
so no grant beyond the role's existing ones is used.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

import anyio
import psycopg
import pytest
from mcp.shared.memory import create_connected_server_and_client_session

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.nomos.read_masking import MASK_TOKEN
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR, normalize_source_lineage
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp._result_payload import JSON_META, payload

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:mcp-client"
_PII = {
    "classification": "non_pii", "cpus": "non_pii", "state": "non_pii",
    "sampled_at": "non_pii", "name": "non_pii", "slots": "non_pii",
    "owner_email": "direct_identifier",
}
SUBTYPES = [
    SubtypeSpec("compute_node", metadata={"volatile_fields": ["sampled_at"], "pii_classes": _PII}),
    SubtypeSpec("rack", metadata={"pii_classes": _PII}),
    SubtypeSpec("host", metadata={"pii_classes": _PII}),
]
FENCE_OPEN = "⟦UNTRUSTED_DATA source=inventory⟧\n"
FENCE_CLOSE = "\n⟦/UNTRUSTED_DATA⟧"


def _fenced(value: str) -> str:
    return f"{FENCE_OPEN}{value}{FENCE_CLOSE}"


def _names(keys: list[str]) -> list[str]:
    """Key names from an `inventory` row: each one fenced, unwrapped here."""
    assert all(k.startswith(FENCE_OPEN) and k.endswith(FENCE_CLOSE) for k in keys), keys
    return [k[len(FENCE_OPEN): -len(FENCE_CLOSE)] for k in keys]


def _node(node_id: str, subtype: str, attrs: dict, *, revision: int = 1, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id, subtype, {"classification": "internal", **attrs},
        {"id": node_id}, {"v": revision}, op=op,
    )


def _apply(dsn: str, facts: list[NodeFact], *, source: str | None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source=source, strict_endpoints=True,
        )


def _publish(dsn: str) -> str:
    out = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert out["status"] == "published", out
    return str(out["generation_id"])


def _lineage_node(node_id: str, source_id: str, *, text: str) -> NodeFact:
    return _node(node_id, "compute_node", {
        "name": text, LINEAGE_ATTR: normalize_source_lineage(source_id=source_id),
    })


@pytest.fixture
def graph(clean_runtime: str) -> dict[str, Any]:
    sfx = uuid.uuid4().hex[:8]
    ids = {k: f"{k.lower()}:{sfx}" for k in "XYZWVUMSHG"}
    x = {"cpus": 8, "state": "idle", "sampled_at": "t1"}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=SUBTYPES)
    _apply(clean_runtime, [
        _node(ids["X"], "compute_node", x),
        _node(ids["Y"], "rack", {"slots": 4}),
        _node(ids["Z"], "compute_node", {"cpus": 4}),
        _node(ids["W"], "compute_node", {"name": "w"}),
        _node(ids["U"], "compute_node", {"name": "u"}),
        _node(ids["M"], "compute_node", {
            "classification": "restricted", "cpus": 2, "owner_email": "a@example.org",
        }),
        _node(ids["G"], "host", {"name": "g"}),
    ], source="inventory")
    _apply(clean_runtime, [_lineage_node(ids["S"], "secret", text="s")], source=None)
    _apply(clean_runtime, [_lineage_node(ids["H"], "secret", text="h")], source=None)
    g1 = _publish(clean_runtime)

    x["cpus"] = 16
    _apply(clean_runtime, [
        _node(ids["X"], "compute_node", x),
        _node(ids["Y"], "rack", {"slots": 8}),
        _node(ids["M"], "compute_node", {
            "classification": "restricted", "cpus": 2, "owner_email": "b@example.org",
        }),
    ], source="inventory")
    g2 = _publish(clean_runtime)

    x["sampled_at"] = "t3"
    _apply(clean_runtime, [
        _node(ids["X"], "compute_node", x),
        _node(ids["W"], "rack", {"name": "w"}),
    ], source="inventory")
    g3 = _publish(clean_runtime)

    x.update(state="busy", sampled_at="t4")
    _apply(clean_runtime, [
        _node(ids["X"], "compute_node", x),
        _node(ids["V"], "compute_node", {"name": "v"}),
    ], source="inventory")
    g4 = _publish(clean_runtime)

    _apply(clean_runtime, [
        _node(ids["X"], "compute_node", x, revision=2),
        _node(ids["U"], "compute_node", {"name": "u"}, op="R"),
        _node(ids["G"], "host", {"classification": "confidential", "name": "g"}),
    ], source="inventory")
    _apply(clean_runtime, [_lineage_node(ids["H"], "inventory", text="h")], source=None)
    g5 = _publish(clean_runtime)
    return {"ids": ids, "g": [g1, g2, g3, g4, g5]}


def _activate_policy(dsn: str, *, version: str = "v1") -> None:
    """The test policy. Any other `version` adds one rule no row matches, so
    the active policy (and its fingerprint) changes and nothing else."""
    boundary = {"deployment": DEPLOYMENT, "scope": f"deployment:{DEPLOYMENT}"}
    extra = [] if version == "v1" else [
        {"rule_id": "deny_unused", "effect": "deny", "actions": ["graph.read.mcp"],
         "subjects": [SUBJECT], "priority": 300,
         "match": {"classification": "unused-class", "source_id": "inventory"}},
    ]
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:history-reads",
        "policy_version": version,
        "default_effect": "deny",
        "rules": [
            {"rule_id": "allow_tool", "effect": "allow", "actions": ["mcp.tool.call"],
             "subjects": [SUBJECT], "priority": 100,
             "match": {**boundary, "resource_kind": "mcp_tool"}},
            {"rule_id": "allow_boundary", "effect": "allow", "actions": ["graph.read.mcp"],
             "subjects": [SUBJECT], "priority": 100,
             "match": {**boundary, "resource_kind": "mcp_graph_read_boundary"}},
            {"rule_id": "allow_operator", "effect": "allow", "actions": ["graph.read.mcp"],
             "subjects": [SUBJECT], "priority": 100,
             "match": {**boundary, "operator_name": "mcp.read"}},
            {"rule_id": "allow_inventory", "effect": "allow", "actions": ["graph.read.mcp"],
             "subjects": [SUBJECT], "priority": 100,
             "match": {**boundary, "source_id": "inventory"}},
            {"rule_id": "redact_restricted", "effect": "redact", "actions": ["graph.read.mcp"],
             "subjects": [SUBJECT], "priority": 200,
             "obligations": [{"type": "redaction"}],
             "match": {"classification": "restricted", "source_id": "inventory"}},
            {"rule_id": "deny_confidential", "effect": "deny", "actions": ["graph.read.mcp"],
             "subjects": [SUBJECT], "priority": 300,
             "match": {"classification": "confidential", "source_id": "inventory"}},
            *extra,
        ],
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn, deployment_name=DEPLOYMENT, bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}", activated_by="test:operator",
        )
        conn.commit()


def _drive(dsn: str, body) -> None:
    server = build_server(dsn=dsn, deployment_name=DEPLOYMENT)
    with server._okg_pool.connection() as conn:  # type: ignore[attr-defined]
        role = conn.execute("SELECT current_user").fetchone()[0]
    assert role == "okg_mcp", f"the MCP read pool connected as {role!r}, not okg_mcp"

    async def run() -> None:
        async with create_connected_server_and_client_session(server) as client:
            await body(client)

    try:
        anyio.run(run)
    finally:
        server._okg_pool.close()  # type: ignore[attr-defined]
        if getattr(server, "_okg_write_pool", None):
            server._okg_write_pool.close()  # type: ignore[attr-defined]


async def _inspect(client, target: dict, **arguments) -> dict:
    result = await client.call_tool(
        "inspect", {"target": target, **arguments}, meta=JSON_META,
    )
    return payload(result)


def _spans(reply: dict) -> list[tuple[str, str]]:
    return [(v["first"]["generation_id"], v["last"]["generation_id"]) for v in reply["versions"]]


def test_node_history_merges_volatile_and_revision_changes(clean_runtime, graph) -> None:
    g1, g2, g3, g4, g5 = graph["g"]
    x = graph["ids"]["X"]
    keys = ["cpus", "state", "sampled_at"]
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["history"] = await _inspect(
            client, {"kind": "node_history", "node_id": x, "attr_keys": keys},
        )
        for generation in graph["g"]:
            seen[generation] = await _inspect(
                client, {"kind": "node", "node_id": x}, generation_id=generation,
            )

    _drive(clean_runtime, body)
    history = seen["history"]
    assert history["status"] == "ok", history
    assert history["attrs_layer"] == "graph"
    assert history["from_generation_id"] == g1 and history["to_generation_id"] == g5
    assert _spans(history) == [(g4, g5), (g2, g3), (g1, g1)]
    newest, middle, oldest = history["versions"]
    assert newest["change"] == "changed" and _names(newest["changed_keys"]) == ["state"]
    assert newest["added_keys"] == [] and newest["removed_keys"] == []
    assert _names(middle["changed_keys"]) == ["cpus"]
    assert oldest["change"] == "oldest_shown"
    assert history["total_count"] == 3 and history["truncated"] is False

    # The generation ids are the ones the deployment lists.
    with psycopg.connect(clean_runtime) as conn:
        listed = {
            str(g.checkpoint_id)
            for g in ChronosGraphStore.from_connection(conn, ensure_schema=False)
            .list_generations(deployment_name=DEPLOYMENT, branch="default")
        }
    assert set(graph["g"]) <= listed

    # Field for field against five pinned node reads: the real fields agree
    # at every generation a version spans, and every value at its last.
    index = {generation: i for i, generation in enumerate(graph["g"])}
    for version in history["versions"]:
        first = index[version["first"]["generation_id"]]
        last = index[version["last"]["generation_id"]]
        for generation in graph["g"][first: last + 1]:
            pinned = seen[generation]["attrs"]
            for key in ("cpus", "state"):
                assert pinned[key] == version["values"][key], (generation, key)
        pinned_last = seen[graph["g"][last]]["attrs"]
        assert {k: pinned_last[k] for k in keys} == version["values"]


def test_a_subtype_change_reads_as_retracted_and_added(clean_runtime, graph) -> None:
    g1, g2, g3, _, g5 = graph["g"]
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["w"] = await _inspect(client, {"kind": "node_history", "node_id": graph["ids"]["W"]})

    _drive(clean_runtime, body)
    assert _spans(seen["w"]) == [(g3, g5), (g1, g2)]
    newest = seen["w"]["versions"][0]
    assert newest["change"] == "subtype_changed"
    assert (newest["subtype_retracted"], newest["subtype_added"]) == ("compute_node", "rack")


def _sql_counts(dsn: str, subtype: str, old: str, new: str) -> dict[str, int]:
    """The diff counted with SQL over the two pinned checkouts."""
    with psycopg.connect(dsn) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)

        def rows(generation: str) -> dict[str, dict]:
            return {
                str(r["node_id"]): dict(r["attrs"])
                for r in store.checkout_generation(generation).query(
                    "SELECT node_id, attrs - 'sampled_at' AS attrs "
                    "FROM okg.graph_nodes WHERE subtype = :s", {"s": subtype},
                )
            }

        before, after = rows(old), rows(new)
    return {
        "added": len(after.keys() - before.keys()),
        "retracted": len(before.keys() - after.keys()),
        "changed": sum(1 for k in before.keys() & after.keys() if before[k] != after[k]),
    }


def test_subtype_diff_lists_the_changes_of_one_subtype(clean_runtime, graph) -> None:
    g1, _, _, _, g5 = graph["g"]
    ids = graph["ids"]
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["nodes"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node",
            "from_generation": g1, "to_generation": g5, "attr_keys": ["cpus"],
        })
        seen["racks"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "rack", "from_generation": g1,
        })

    _drive(clean_runtime, body)
    nodes = seen["nodes"]
    by_id = {e["node_id"]: e for e in nodes["entries"]}
    assert {k for k, e in by_id.items() if e["change"] == "added"} == {ids["V"]}
    assert {k for k, e in by_id.items() if e["change"] == "retracted"} == {ids["U"], ids["W"]}
    assert {k for k, e in by_id.items() if e["change"] == "changed"} == {ids["X"], ids["M"], ids["H"]}
    assert ids["Y"] not in by_id and ids["Z"] not in by_id
    assert sorted(_names(by_id[ids["X"]]["changed_keys"])) == ["cpus", "state"]
    assert by_id[ids["X"]]["from_values"] == {"cpus": 8}
    assert by_id[ids["X"]]["to_values"] == {"cpus": 16}
    expected = _sql_counts(clean_runtime, "compute_node", g1, g5)
    assert (nodes["added_count"], nodes["retracted_count"], nodes["changed_count"]) == (
        expected["added"], expected["retracted"], expected["changed"],
    ) == (1, 2, 3)
    assert [e["node_id"] for e in nodes["entries"]] == sorted(by_id)

    racks = {e["node_id"]: e["change"] for e in seen["racks"]["entries"]}
    assert racks == {ids["W"]: "added", ids["Y"]: "changed"}
    assert seen["racks"]["to_generation_id"] == g5


def test_pages_join_to_the_whole_history_across_a_publish(clean_runtime, graph) -> None:
    x = graph["ids"]["X"]
    target = {"kind": "node_history", "node_id": x, "limit": 1}
    seen: dict[str, Any] = {"pages": []}

    async def first(client) -> None:
        seen["whole"] = await _inspect(client, {"kind": "node_history", "node_id": x})
        seen["pages"].append(await _inspect(client, target))

    _drive(clean_runtime, first)
    # A publish lands between pages; the next pages still read g1..g5.
    _apply(clean_runtime, [_node(x, "compute_node", {
        "cpus": 64, "state": "busy", "sampled_at": "t6",
    })], source="inventory")
    g6 = _publish(clean_runtime)

    async def rest(client) -> None:
        page = seen["pages"][-1]
        while page.get("cursor"):
            page = await _inspect(client, page["next_target"])
            seen["pages"].append(page)
        seen["fresh"] = await _inspect(client, {"kind": "node_history", "node_id": x})
        seen["wrong"] = await _inspect(client, {
            **seen["pages"][0]["next_target"], "node_id": graph["ids"]["Z"],
        })

    _drive(clean_runtime, rest)
    joined = [v for page in seen["pages"] for v in page["versions"]]
    assert len(seen["pages"]) == 3
    assert joined == seen["whole"]["versions"]
    assert all(page["to_generation_id"] == graph["g"][-1] for page in seen["pages"])
    assert seen["fresh"]["to_generation_id"] == g6
    assert seen["fresh"]["total_count"] == 4
    # Failure case: the cursor sent with another node_id is refused.
    assert seen["wrong"]["error"]["code"] == "continuation_token_invalid"
    assert "target" in seen["wrong"]["error"]["data"]["mismatched"]


def _tombstone(dsn: str, checkpoint_id: str) -> None:
    from okg.substrate.history_compaction import ensure_history_compaction_schema

    plan_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_history_compaction_schema(conn)
        with conn.transaction():
            conn.execute(
                "INSERT INTO okg.history_compaction_plans "
                "(plan_id, deployment_name, chronos_branch_id, status, plan) "
                "VALUES (%s, %s, 'default', 'applying', '{}'::jsonb)",
                (plan_id, DEPLOYMENT),
            )
            conn.execute(
                "INSERT INTO okg.compacted_generations "
                "(checkpoint_id, deployment_name, branch_id, plan_id) "
                "VALUES (%s, %s, 'default', %s)",
                (checkpoint_id, DEPLOYMENT, plan_id),
            )


def test_unknown_compacted_reversed_and_over_budget_reads_are_refused(
    clean_runtime, graph, monkeypatch,
) -> None:
    """Failure cases, each with its typed code; none falls back to latest."""
    import okg.substrate.mcp.history_reads as history_reads

    g1, _, _, _, g5 = graph["g"]
    x = graph["ids"]["X"]
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    retired = f"okg:retired-{uuid.uuid4().hex[:8]}"
    _tombstone(clean_runtime, retired)
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["unknown"] = await _inspect(client, {"kind": "node_history", "node_id": x, "to": "okg:nope"})
        seen["compacted"] = await _inspect(client, {"kind": "node_history", "node_id": x, "from": retired})
        seen["diff_compacted"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node", "from_generation": retired,
        })
        seen["reversed"] = await _inspect(client, {"kind": "node_history", "node_id": x, "from": g5, "to": g1})
        monkeypatch.setattr(history_reads, "NODE_HISTORY_ROW_BUDGET", 1)
        seen["rows"] = await _inspect(client, {"kind": "node_history", "node_id": x})
        # Nomos off: three changed rows (X, M, H) over a compare budget of 1.
        monkeypatch.setattr(history_reads, "SUBTYPE_DIFF_COMPARE_ROW_BUDGET", 1)
        seen["compare"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node", "from_generation": g1,
        })
        monkeypatch.setattr(history_reads, "SUBTYPE_DIFF_SCAN_ROW_BUDGET", 1)
        seen["scan"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node", "from_generation": g1,
        })

    _drive(clean_runtime, body)
    assert seen["unknown"]["error"]["code"] == "generation_not_found"
    assert seen["compacted"]["error"]["code"] == "generation_compacted"
    assert seen["diff_compacted"]["error"]["code"] == "generation_compacted"
    assert seen["reversed"]["error"]["code"] == "history_range_invalid"
    for name, budget in (
        ("rows", "node_history_row_budget"),
        ("compare", "subtype_diff_compare_row_budget"),
        ("scan", "subtype_diff_scan_row_budget"),
    ):
        error = seen[name]["error"]
        assert error["code"] == "history_row_budget_exceeded"
        assert error["data"]["budget"] == 1 and error["data"]["budget_name"] == budget
        assert "versions" not in seen[name] and "entries" not in seen[name]


def test_under_enforce_hidden_values_and_rows_are_not_revealed(
    clean_runtime, graph, monkeypatch,
) -> None:
    import okg.substrate.mcp.session as session_module

    g1, _, _, _, g5 = graph["g"]
    ids = graph["ids"]
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["s_node"] = await _inspect(client, {"kind": "node", "node_id": ids["S"]})
        seen["s_history"] = await _inspect(client, {"kind": "node_history", "node_id": ids["S"]})
        seen["m"] = await _inspect(client, {
            "kind": "node_history", "node_id": ids["M"], "attr_keys": ["owner_email", "cpus"],
        })
        seen["h"] = await _inspect(client, {"kind": "node_history", "node_id": ids["H"]})
        seen["diff"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node",
            "from_generation": g1, "to_generation": g5, "attr_keys": ["owner_email"],
        })
        monkeypatch.setattr(session_module, "_PREVIEW_ADVANCE_EXACT_ROW_BUDGET", 1)
        seen["over"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node", "from_generation": g1,
        })

    _drive(clean_runtime, body)
    # A node the reader may not read is refused as inspect(node) refuses it.
    assert seen["s_node"]["error"]["code"] == "node_not_found"
    assert seen["s_history"]["error"]["code"] == seen["s_node"]["error"]["code"]
    # The masked owner changed in g2: one version, no changed key, value masked.
    m = seen["m"]
    assert _spans(m) == [(g1, g5)]
    # Served masked (and fenced, as every untrusted producer string is).
    assert MASK_TOKEN in m["versions"][0]["values"]["owner_email"]
    assert "example.org" not in json.dumps(m)
    # H is shown only from g5, where its source became readable.
    assert _spans(seen["h"]) == [(g5, g5)]
    diff = seen["diff"]
    by_id = {e["node_id"]: e["change"] for e in diff["entries"]}
    assert by_id[ids["H"]] == "added"
    assert ids["M"] not in by_id and ids["S"] not in by_id
    assert (diff["added_count"], diff["retracted_count"], diff["changed_count"]) == (2, 2, 1)
    # Above the exact budget the counts and entries are withheld.
    over = seen["over"]
    assert over["truncated"] is True and over["entries"] is None
    assert over["added_count"] is None and over["retracted_count"] is None
    assert over["changed_count"] is None and over["note"]


def test_under_audit_nothing_is_hidden(clean_runtime, graph, monkeypatch) -> None:
    g1, _, _, g4, g5 = graph["g"]
    ids = graph["ids"]
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["s"] = await _inspect(client, {"kind": "node_history", "node_id": ids["S"]})
        seen["h"] = await _inspect(client, {"kind": "node_history", "node_id": ids["H"]})
        seen["diff"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node",
            "from_generation": g1, "to_generation": g5,
        })

    _drive(clean_runtime, body)
    assert seen["s"]["status"] == "ok" and _spans(seen["s"]) == [(g1, g5)]
    assert _spans(seen["h"]) == [(g5, g5), (g1, g4)]
    by_id = {e["node_id"]: e["change"] for e in seen["diff"]["entries"]}
    assert by_id[ids["H"]] == "changed"
    assert ids["S"] not in by_id
    assert seen["diff"]["added_count"] == 1 and seen["diff"]["retracted_count"] == 2


def test_every_item_carries_a_trust_label_and_untrusted_text_is_fenced(
    clean_runtime, graph, monkeypatch,
) -> None:
    """Without attr_keys every version and entry is still labelled; key names
    and string values from the untrusted `inventory` rows are fenced, and the
    trusted rows (no source column) are not."""
    g1, _, _, _, g5 = graph["g"]
    ids = graph["ids"]
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["x"] = await _inspect(client, {"kind": "node_history", "node_id": ids["X"]})
        seen["w"] = await _inspect(client, {
            "kind": "node_history", "node_id": ids["W"], "attr_keys": ["name"],
        })
        seen["h"] = await _inspect(client, {
            "kind": "node_history", "node_id": ids["H"], "attr_keys": ["name"],
        })
        seen["diff"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node",
            "from_generation": g1, "to_generation": g5,
        })
        seen["diff_values"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "compute_node",
            "from_generation": g1, "to_generation": g5, "attr_keys": ["name"],
        })

    _drive(clean_runtime, body)
    untrusted = {"source": "inventory", "trust": "untrusted"}
    trusted = {"source": None, "trust": "trusted"}
    x = seen["x"]["versions"]
    assert [v["provenance"] for v in x] == [untrusted] * 3
    assert "values" not in x[0]
    assert x[0]["changed_keys"] == [_fenced("state")]
    # Values: an untrusted producer's string is fenced, a trusted one is not.
    assert [v["values"]["name"] for v in seen["w"]["versions"]] == [_fenced("w")] * 2
    assert seen["w"]["versions"][0]["values_provenance"] == untrusted
    h = seen["h"]["versions"]
    assert [v["provenance"] for v in h] == [trusted] * 2
    assert [v["values"]["name"] for v in h] == ["h", "h"]
    assert all(not k.startswith("⟦") for k in h[0]["changed_keys"]), h[0]
    by_id = {e["node_id"]: e for e in seen["diff"]["entries"]}
    assert all(e["provenance"]["trust"] in {"trusted", "untrusted"} for e in by_id.values())
    assert by_id[ids["V"]]["provenance"] == untrusted
    assert by_id[ids["U"]]["provenance"] == untrusted
    assert by_id[ids["H"]]["provenance"] == trusted
    assert by_id[ids["X"]]["provenance"] == untrusted
    assert sorted(by_id[ids["X"]]["changed_keys"]) == [_fenced("cpus"), _fenced("state")]
    valued = {e["node_id"]: e for e in seen["diff_values"]["entries"]}
    assert valued[ids["V"]]["to_values"] == {"name": _fenced("v")}
    assert valued[ids["U"]]["from_values"] == {"name": _fenced("u")}


def test_under_enforce_a_hidden_node_over_budget_reads_as_absent(
    clean_runtime, graph, monkeypatch,
) -> None:
    """Failure case for a leak: over the row budget, a node hidden at the pin
    (S always, G since g5) is refused `node_not_found` like an absent one,
    never with the budget refusal that would say it has stored versions; a
    visible node gets the budget refusal."""
    import okg.substrate.mcp.history_reads as history_reads

    ids = graph["ids"]
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    monkeypatch.setattr(history_reads, "NODE_HISTORY_ROW_BUDGET", 0)
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        for name in ("S", "G", "X"):
            seen[name] = await _inspect(client, {"kind": "node_history", "node_id": ids[name]})
        seen["absent"] = await _inspect(
            client, {"kind": "node_history", "node_id": f"nope:{uuid.uuid4().hex[:8]}"},
        )

    _drive(clean_runtime, body)
    assert seen["S"]["error"]["code"] == "node_not_found"
    assert seen["G"]["error"]["code"] == "node_not_found"
    assert seen["absent"]["error"]["code"] == "node_not_found"
    assert seen["X"]["error"]["code"] == "history_row_budget_exceeded"
    for name in ("S", "G"):
        assert "budget" not in json.dumps(seen[name]["error"]), seen[name]


def test_under_enforce_a_record_hidden_now_still_shows_the_versions_it_was_visible_in(
    clean_runtime, graph, monkeypatch,
) -> None:
    """Documented behavior, not a leak of a value the reader could not read
    then: G's source reclassified it in g5 to a classification the policy
    denies. Each version is decided on its own row, so g1 to g4 are served,
    unmasked, as `inspect(kind node, generation_id=g4)` serves them."""
    g1, _, _, g4, g5 = graph["g"]
    gid = graph["ids"]["G"]
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    seen: dict[str, Any] = {}

    async def body(client) -> None:
        seen["now"] = await _inspect(client, {"kind": "node", "node_id": gid})
        seen["then"] = await _inspect(client, {"kind": "node", "node_id": gid}, generation_id=g4)
        seen["history"] = await _inspect(client, {
            "kind": "node_history", "node_id": gid, "attr_keys": ["classification", "name"],
        })
        seen["diff"] = await _inspect(client, {
            "kind": "subtype_diff", "subtype": "host", "from_generation": g1, "to_generation": g5,
        })

    _drive(clean_runtime, body)
    assert seen["now"]["error"]["code"] == "node_not_found"
    assert "error" not in seen["then"], seen["then"]
    history = seen["history"]
    assert history["status"] == "ok", history
    assert _spans(history) == [(g1, g4)]
    values = history["versions"][0]["values"]
    assert values == {"classification": _fenced("internal"), "name": _fenced("g")}
    assert seen["then"]["attrs"]["name"] == values["name"]
    assert "confidential" not in json.dumps(history)
    # Visible to hidden between g1 and g5 is a retraction.
    assert [(e["node_id"], e["change"]) for e in seen["diff"]["entries"]] == [(gid, "retracted")]


def test_under_enforce_pages_continue_and_a_cursor_dies_with_the_policy(
    clean_runtime, graph, monkeypatch,
) -> None:
    """Second pages under enforce join to the whole read; a cursor read after
    the active policy changed is refused (failure case), never served under a
    policy other than the one it was issued under."""
    g1, _, _, _, g5 = graph["g"]
    x = graph["ids"]["X"]
    _activate_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)
    diff_target = {
        "kind": "subtype_diff", "subtype": "compute_node",
        "from_generation": g1, "to_generation": g5,
    }
    seen: dict[str, Any] = {"history": [], "diff": []}

    async def first(client) -> None:
        seen["whole_history"] = await _inspect(client, {"kind": "node_history", "node_id": x})
        seen["whole_diff"] = await _inspect(client, diff_target)
        for key, target in (
            ("history", {"kind": "node_history", "node_id": x, "limit": 1}),
            ("diff", {**diff_target, "limit": 1}),
        ):
            page = await _inspect(client, target)
            seen[key].append(page)
            while page.get("cursor"):
                page = await _inspect(client, page["next_target"])
                seen[key].append(page)

    _drive(clean_runtime, first)
    # The operator replaces the policy. v1 grants no `policy.activate`, so the
    # replacement is activated with enforcement off, as an operator would.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    _activate_policy(clean_runtime, version="v2")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    async def after(client) -> None:
        seen["stale_history"] = await _inspect(client, seen["history"][0]["next_target"])
        seen["stale_diff"] = await _inspect(client, seen["diff"][0]["next_target"])

    _drive(clean_runtime, after)
    assert len(seen["history"]) == 3
    assert [v for p in seen["history"] for v in p["versions"]] == seen["whole_history"]["versions"]
    assert len(seen["diff"]) == len(seen["whole_diff"]["entries"]) > 1
    assert [e for p in seen["diff"] for e in p["entries"]] == seen["whole_diff"]["entries"]
    for name in ("stale_history", "stale_diff"):
        error = seen[name]["error"]
        assert error["code"] == "continuation_token_invalid", seen[name]
        assert "authority" in error["data"]["mismatched"], error
