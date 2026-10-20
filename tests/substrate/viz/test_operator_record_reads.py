"""Operator UI record reads against a real published graph (PACT operator-ui-search-view).

Every read names a published version, is authorized at the operator UI's Nomos
boundary, and is filtered and audited as graph.read.local_operator.
"""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any, Iterator

import psycopg
import pytest

pytest.importorskip("fastapi")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph  # noqa: E402
from okg.substrate.nomos.read_masking import MASK_TOKEN  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.viz.record_endpoints import register_operator_record_routes  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate._db_helpers import seed_catalog_version  # noqa: E402

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"
SUBJECT = "subject:operator"


class _Pool:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def connection(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn)


class _Ctx:
    """The slice of VizContext the record routes read."""

    def __init__(self, dsn: str, *, branch_name: str = "default") -> None:
        self.dsn = dsn
        self.deployment_name = DEPLOYMENT
        self.session = SimpleNamespace(branch_name=branch_name)
        self.pool = _Pool(dsn)

    @contextmanager
    def conn(self) -> Iterator[psycopg.Connection]:
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            yield conn


def _client(dsn: str, *, branch_name: str = "default") -> TestClient:
    app = FastAPI()
    from fastapi import APIRouter

    router = APIRouter(prefix="/viz/api")
    ctx = _Ctx(dsn, branch_name=branch_name)
    register_operator_record_routes(router, ctx_provider=lambda: ctx)
    app.include_router(router)
    return TestClient(app)


def _publish(dsn: str, nodes: list[NodeFact], edges: list[EdgeFact] | None = None) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([*nodes, *(edges or [])], source="fixture")
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"


def _node(node_id: str, subtype: str, **attrs: Any) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs=attrs,
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


@pytest.fixture
def two_versions(clean_runtime: str) -> dict[str, str]:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _publish(
        clean_runtime,
        [
            _node(
                "page:alpha", "page",
                title="Alpha guide", label="Alpha guide", body="Alpha body",
                text="alpha release notes", url=MASK_TOKEN, nomos_team="platform",
                evidence=None,
            ),
            _node("release:r1", "release", name="R1", text=MASK_TOKEN),
            _node("release:r2", "release", name="R2", text=MASK_TOKEN),
            _node("release:r3", "release", name="R3", text=MASK_TOKEN),
        ],
        [EdgeFact(src="page:alpha", dst="release:r1", edge_type="mentions")],
    )
    _publish(
        clean_runtime,
        [_node("page:beta", "page", title="Beta guide", text="beta release notes")],
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at",
            (DEPLOYMENT,),
        )
        first, second = [str(row[0]) for row in cur.fetchall()]
    return {"first": first, "second": second}


def _activate(dsn: str, rules: list[dict[str, Any]]) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:operator-ui-record-reads",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": rules,
    })
    with psycopg.connect(dsn) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=f"deployment:{DEPLOYMENT}",
            activated_by="test:operator",
        )
        conn.commit()


def _rule(rule_id: str, *, match: dict[str, Any]) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "effect": "allow",
        "actions": ["graph.read.local_operator"],
        "subjects": [SUBJECT],
        "match": match,
        "priority": 100,
        "reason_code": f"nomos.test.{rule_id}",
    }


BOUNDARY_RULE = _rule("allow_ui_boundary", match={"resource_kind": "local_operator_boundary"})
ROW_RULE = _rule("allow_ui_rows", match={})
DENY_RESTRICTED_RULE = {
    **_rule("deny_restricted", match={"classification": "restricted"}),
    "effect": "deny",
    "priority": 200,
}
RESTRICTED = {"classification": "restricted"}
# Neighbours the policy hides that sort before a visible one, more of them than
# one related-records batch holds.
HIDDEN_NEIGHBOURS = [f"secret:{index:04d}" for index in range(45)]


@pytest.fixture
def policy_mix(clean_runtime: str) -> str:
    """One version mixing records, relationships and values the policy hides."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _publish(
        clean_runtime,
        [
            _node(
                "doc:hub", "doc",
                title="Hub guide", body=MASK_TOKEN, summary=MASK_TOKEN,
                owner=MASK_TOKEN, text="hub notes",
            ),
            _node("release:a", "release", name="A"),
            _node("release:b", "release", name="B", **RESTRICTED),
            _node("release:c", "release", name="C", **RESTRICTED),
            _node("release:d", "release", name="D"),
            _node("release:e", "release", name="E"),
            _node("zeta:visible", "zeta", name="Zeta"),
            *[_node(node_id, "secret", name=node_id, **RESTRICTED) for node_id in HIDDEN_NEIGHBOURS],
            _node(
                "doc:masked-label", "doc",
                title=MASK_TOKEN, label=MASK_TOKEN, body=MASK_TOKEN, text="readable notes",
            ),
            _node("tail:1", "tail", name="T1"),
            _node("tail:2", "tail", name="T2"),
            _node("tail:3", "tail", name="T3"),
            _node("tail:4", "tail", name="T4", **RESTRICTED),
            _node("doc:fan", "doc", title="Fan"),
            _node("doc:echo", "doc", title="Echo", text="doc echo"),
            *[
                _node(f"fan:{index:02d}", "fan", name=f"F{index:02d}", **(RESTRICTED if index == 5 else {}))
                for index in range(41)
            ],
        ],
        [
            EdgeFact(src="doc:hub", dst="release:d", edge_type="cites", attrs=dict(RESTRICTED)),
            EdgeFact(src="doc:hub", dst="release:a", edge_type="mentions"),
            EdgeFact(src="doc:hub", dst="release:b", edge_type="mentions"),
            *[
                EdgeFact(src="doc:hub", dst=node_id, edge_type="mentions")
                for node_id in HIDDEN_NEIGHBOURS
            ],
            EdgeFact(src="doc:hub", dst="zeta:visible", edge_type="mentions"),
            *[
                EdgeFact(src="doc:fan", dst=f"fan:{index:02d}", edge_type="links")
                for index in range(41)
            ],
        ],
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        return str(cur.fetchone()[0])


def _enforce(monkeypatch: pytest.MonkeyPatch) -> None:
    """Turn on runtime enforcement AFTER the policy is active: activating a
    policy is itself a gated surface, and with no active bundle it is denied."""
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT)


def _decisions(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = %s AND action LIKE 'graph.read%%'
             ORDER BY created_at, decision_row_id
            """,
            (DEPLOYMENT,),
        )
        return [(str(a), str(e), str(k)) for a, e, k in cur.fetchall()]


def test_versions_lists_published_versions_newest_first(two_versions: dict[str, str], clean_runtime: str) -> None:
    body = _client(clean_runtime).get("/viz/api/console/versions").json()["payload"]

    assert [row["generation_id"] for row in body["versions"]] == [
        two_versions["second"], two_versions["first"],
    ]
    assert body["latest"] == two_versions["second"]


def test_reads_refuse_missing_malformed_and_unpublished_versions(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    missing = client.get("/viz/api/console/records/search", params={"q": "alpha"})
    malformed = client.get(
        "/viz/api/console/records/search", params={"q": "alpha", "version": "12345"},
    )
    unknown_version = two_versions["second"].rsplit(":", 1)[0] + ":000000000000"
    unpublished = client.get(
        "/viz/api/console/records/search", params={"q": "alpha", "version": unknown_version},
    )

    assert missing.status_code == 422
    assert malformed.status_code == 422
    assert malformed.json()["detail"]["reason"] == "version_not_a_published_checkpoint"
    assert unpublished.status_code == 404
    assert unpublished.json()["detail"]["reason"] == "version_not_published"
    assert unpublished.json()["detail"]["version"] == unknown_version


def test_a_read_that_cannot_open_its_branch_is_not_called_an_unpublished_version(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    response = _client(clean_runtime, branch_name="missing").get(
        "/viz/api/console/records/search",
        params={"q": "alpha", "version": two_versions["second"]},
    )

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "version_unavailable"
    assert "missing" in response.json()["detail"]["error"]


def test_reads_are_pinned_to_the_requested_version(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    first = client.get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["first"], "id": "page:beta"},
    ).json()
    second = client.get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["second"], "id": "page:beta"},
    ).json()
    first_search = client.get(
        "/viz/api/console/records/search",
        params={"version": two_versions["first"], "q": "release"},
    ).json()

    assert first["generation_id"] == two_versions["first"]
    assert first["payload"] == {"found": False, "node_id": "page:beta"}
    assert second["payload"]["found"] is True
    assert second["payload"]["label"] == "Beta guide"
    assert "page:beta" not in {row["node_id"] for row in first_search["payload"]["results"]}


def test_reads_are_authorized_and_audited_as_the_operator_ui(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    mcp_rows = {**_rule("allow_mcp_rows", match={}), "actions": ["graph.read.mcp"]}
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, mcp_rows])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    response = client.get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["second"], "id": "page:alpha"},
    )

    assert response.status_code == 200
    decisions = _decisions(clean_runtime)
    assert ("graph.read.local_operator", "allow", "local_operator") in decisions
    assert any(kind.startswith("local_operator_node_") for _, _, kind in decisions)
    assert any(kind.startswith("local_operator_edge_") for _, _, kind in decisions)
    assert {action for action, _, _ in decisions} == {"graph.read.local_operator"}

    # A read through the shared search by a session that names no identity,
    # as MCP opens it, still decides and records as MCP.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=two_versions["second"],
            nomos_subject=SUBJECT,
        )
        shared = search(conn, session, "release notes", method="lexical", limit=20)
    later = _decisions(clean_runtime)[len(decisions):]

    assert shared["results"]
    assert later
    assert {action for action, _, _ in later} == {"graph.read.mcp"}
    assert all(kind.startswith("mcp_") for _, _, kind in later)


def test_policy_without_a_boundary_rule_refuses_and_names_the_fix(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": two_versions["second"], "q": "alpha"},
    )

    assert response.status_code == 403
    detail = response.json()["detail"]
    assert detail["reason"] == "policy_denied"
    assert "okg deployment apply default" in detail["next_check"]
    assert detail["data"]["boundary_rule_present"] is False
    assert ("graph.read.local_operator", "deny", "local_operator") in _decisions(clean_runtime)


def test_rows_the_policy_hides_are_not_returned(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)
    version = two_versions["second"]

    detail = client.get(
        "/viz/api/console/records/detail", params={"version": version, "id": "page:alpha"},
    ).json()["payload"]
    listing = client.get(
        "/viz/api/console/records/list", params={"version": version, "type": "release"},
    ).json()["payload"]
    search = client.get(
        "/viz/api/console/records/search", params={"version": version, "q": "page:alpha"},
    ).json()["payload"]

    assert detail == {"found": False, "node_id": "page:alpha"}
    assert listing["records"] == []
    assert search["results"] == []


def test_listing_pages_through_a_type_in_record_id_order(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)
    version = two_versions["second"]

    first = client.get(
        "/viz/api/console/records/list",
        params={"version": version, "type": "release", "limit": 2},
    ).json()["payload"]
    second = client.get(
        "/viz/api/console/records/list",
        params={"version": version, "type": "release", "limit": 2, "offset": first["next_offset"]},
    ).json()["payload"]

    assert [row["node_id"] for row in first["records"]] == ["release:r1", "release:r2"]
    assert first["next_offset"] == 2
    assert first["short_page"] is False
    assert [row["node_id"] for row in second["records"]] == ["release:r3"]
    assert second["next_offset"] is None


def _all_pages(client: TestClient, version: str, record_type: str, limit: int) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    offset: int | None = 0
    while offset is not None:
        response = client.get(
            "/viz/api/console/records/list",
            params={"version": version, "type": record_type, "limit": limit, "offset": offset},
        )
        assert response.status_code == 200
        pages.append(response.json()["payload"])
        offset = pages[-1]["next_offset"]
        assert len(pages) < 50, "listing did not end"
    return pages


def test_listing_never_names_a_record_the_policy_hides(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    releases = _all_pages(client, policy_mix, "release", limit=2)
    secrets = _all_pages(client, policy_mix, "secret", limit=1)

    assert [row["node_id"] for page in releases for row in page["records"]] == [
        "release:a", "release:d", "release:e",
    ]
    assert releases[0]["short_page"] is False
    # Every secret record is hidden: the scan stops after its refill rounds
    # with an empty, short page whose cursor is a position, not a record ID.
    assert [row for page in secrets for row in page["records"]] == []
    assert secrets[0]["short_page"] is True
    assert isinstance(secrets[0]["next_offset"], int)
    listed = repr([*releases, *secrets])
    assert "release:b" not in listed and "release:c" not in listed
    assert "secret:" not in listed


def test_listing_does_not_offer_a_next_page_with_nothing_visible_on_it(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    # tail:4, the last record of its type, is hidden.
    pages = _all_pages(_client(clean_runtime), policy_mix, "tail", limit=1)

    assert [[row["node_id"] for row in page["records"]] for page in pages] == [
        ["tail:1"], ["tail:2"], ["tail:3"],
    ]
    assert pages[-1]["next_offset"] is None


def test_search_puts_an_exact_record_id_first_and_uses_the_shared_search(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)
    version = two_versions["second"]

    by_id = client.get(
        "/viz/api/console/records/search", params={"version": version, "q": "release:r2"},
    ).json()["payload"]
    by_text = client.get(
        "/viz/api/console/records/search", params={"version": version, "q": "release notes"},
    ).json()["payload"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=version,
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
        )
        shared = search(conn, session, "release notes", method="lexical", limit=20)

    assert by_id["results"][0]["node_id"] == "release:r2"
    assert by_id["results"][0]["match"] == "record_id"
    assert [row["node_id"] for row in by_text["results"]] == [
        row["node_id"] for row in shared["results"]
    ]
    assert {row["node_id"] for row in by_text["results"]} == {"page:alpha", "page:beta"}


def test_record_detail_reports_hidden_fields_source_identity_and_related_records(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _client(clean_runtime).get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["second"], "id": "page:alpha"},
    ).json()["payload"]

    fields = {row["key"]: row for row in payload["fields"]}
    assert fields["url"] == {"key": "url", "value": None, "hidden": True, "echoes_title": False}
    assert fields["title"]["hidden"] is False
    assert "label" not in fields, "a label that repeats the title is not a separate field"
    assert fields["nomos_team"] == {"key": "nomos_team", "value": "platform", "hidden": False, "echoes_title": False}, (
        "a source attribute that happens to start with nomos_ is still a field"
    )
    assert "nomos_source_lineage" not in fields and "nomos_redacted" not in fields
    assert payload["sections"] == [
        {"key": "body", "title": "Content", "value": "Alpha body", "hidden": False},
    ], "the composed search text repeats the body and is not shown beside it"
    assert payload["policy_redacted"] is True
    assert payload["source"] == "fixture"
    assert payload["source_record_id"] == {"id": "page:alpha"}
    assert payload["source_revision"] == {"v": 1}
    # Each related row carries the neighbour's own source identity -- the
    # same value that record's own detail read serves -- so a row can be
    # named by where it came from, not only by its label.
    assert payload["related"]["outgoing"] == [
        {
            "relationship": "mentions",
            "node_id": "release:r1",
            "type": "release",
            "label": "R1",
            "source_record_id": {"id": "release:r1"},
        },
    ]
    neighbour = _client(clean_runtime).get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["second"], "id": "release:r1"},
    ).json()["payload"]
    assert neighbour["source_record_id"] == payload["related"]["outgoing"][0]["source_record_id"]


def test_hidden_body_values_are_shown_as_hidden_not_dropped(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _client(clean_runtime).get(
        "/viz/api/console/records/detail", params={"version": policy_mix, "id": "doc:hub"},
    ).json()["payload"]

    fields = {row["key"]: row for row in payload["fields"]}
    assert fields["owner"] == {"key": "owner", "value": None, "hidden": True, "echoes_title": False}
    assert payload["sections"] == [
        {"key": "summary", "title": "Summary", "value": None, "hidden": True},
        {"key": "body", "title": "Content", "value": None, "hidden": True},
        {"key": "text", "title": "Content", "value": "hub notes", "hidden": False},
    ]
    assert payload["policy_redacted"] is True
    assert MASK_TOKEN not in repr(payload)


def test_hidden_labels_stay_hidden_and_readable_search_text_is_kept(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _client(clean_runtime).get(
        "/viz/api/console/records/detail", params={"version": policy_mix, "id": "doc:masked-label"},
    ).json()["payload"]

    fields = {row["key"]: row for row in payload["fields"]}
    assert fields["title"]["hidden"] is True
    assert fields["label"] == {"key": "label", "value": None, "hidden": True, "echoes_title": False}, (
        "a hidden label is not a repeat of a hidden title"
    )
    assert payload["sections"] == [
        {"key": "body", "title": "Content", "value": None, "hidden": True},
        {"key": "text", "title": "Content", "value": "readable notes", "hidden": False},
    ], "beside a hidden body, the search text is the only readable content"


def test_values_a_read_time_redaction_removes_are_shown_as_hidden(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    redact_rows = {
        **_rule("redact_ui_rows", match={"resource_kind": "graph_object", "kind": "node"}),
        "effect": "redact",
        "priority": 150,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, redact_rows])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get(
        "/viz/api/console/records/detail",
        params={"version": two_versions["second"], "id": "page:alpha"},
    )
    payload = response.json()["payload"]

    assert "Alpha body" not in response.text
    assert "alpha release notes" not in response.text
    assert payload["sections"] == [
        {"key": "body", "title": "Content", "value": None, "hidden": True},
    ]
    fields = {row["key"]: row["hidden"] for row in payload["fields"]}
    assert fields["title"] is False
    assert "evidence" not in fields, "an empty value is not reported as hidden"
    assert payload["policy_redacted"] is True


def test_related_records_apply_the_policy_to_each_relationship_and_neighbour(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    related = _client(clean_runtime).get(
        "/viz/api/console/records/detail", params={"version": policy_mix, "id": "doc:hub"},
    ).json()["payload"]["related"]

    # release:d is visible, but the relationship to it is restricted; release:b
    # and the secret records are restricted neighbours. zeta:visible sorts after
    # more hidden neighbours than one batch holds and is still found.
    assert related["outgoing"] == [
        {
            "relationship": "mentions",
            "node_id": "release:a",
            "type": "release",
            "label": "A",
            "source_record_id": {"id": "release:a"},
        },
        {
            "relationship": "mentions",
            "node_id": "zeta:visible",
            "type": "zeta",
            "label": "Zeta",
            "source_record_id": {"id": "zeta:visible"},
        },
    ]
    assert related["outgoing_capped"] is False
    assert "release:d" not in repr(related) and "secret:" not in repr(related)


def test_related_records_say_more_may_exist_only_when_more_follow(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    # 41 relationships, one of them to a hidden record: exactly 40 visible.
    related = _client(clean_runtime).get(
        "/viz/api/console/records/detail", params={"version": policy_mix, "id": "doc:fan"},
    ).json()["payload"]["related"]

    assert len(related["outgoing"]) == 40
    assert "fan:05" not in {row["node_id"] for row in related["outgoing"]}
    assert related["outgoing_capped"] is False


def test_accepted_corrections_show_whether_the_policy_is_on_or_off(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )

    version = two_versions["second"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="page:beta",
            generation_id=version,
            attr_key="title",
            value="Beta guide, corrected",
            producer="reviewer",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=version)

    def labels() -> tuple[str, str, str]:
        client = _client(clean_runtime)
        detail = client.get(
            "/viz/api/console/records/detail", params={"version": version, "id": "page:beta"},
        ).json()["payload"]
        listing = client.get(
            "/viz/api/console/records/list", params={"version": version, "type": "page"},
        ).json()["payload"]
        listed = {row["node_id"]: row["label"] for row in listing["records"]}
        title = {row["key"]: row["value"] for row in detail["fields"]}["title"]
        return detail["label"], title, listed["page:beta"]

    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    policy_off = labels()
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    policy_on = labels()

    assert policy_off == ("Beta guide, corrected",) * 3
    assert policy_on == policy_off


def test_an_advanced_session_keeps_the_operator_ui_identity(
    two_versions: dict[str, str], clean_runtime: str,
) -> None:
    from okg.substrate.mcp.session import advance_session, open_session

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=two_versions["first"],
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
            nomos_read_surface="local_operator",
        )
        advanced = advance_session(conn, session, to="latest")

    assert advanced.generation_id == two_versions["second"]
    assert (advanced.nomos_read_action, advanced.nomos_read_surface) == (
        "graph.read.local_operator", "local_operator",
    )


def test_search_lists_an_exact_match_that_is_also_a_text_match_once(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    # doc:echo's searchable text holds the words of its own record ID.
    payload = _client(clean_runtime).get(
        "/viz/api/console/records/search", params={"version": policy_mix, "q": "doc:echo"},
    ).json()["payload"]

    ids = [row["node_id"] for row in payload["results"]]
    assert ids[0] == "doc:echo"
    assert payload["results"][0]["match"] == "record_id"
    assert ids.count("doc:echo") == 1
    assert payload["exact_match_in_text"] is True


def test_the_version_listing_is_authorized_at_the_same_boundary(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The version listing names no version, but it is still a policy-checked read.

    It is the read that tells a caller which versions exist, so it cannot pin
    one. A deployment that refuses the operator UI must refuse it exactly as
    it refuses the reads that do name a version -- otherwise the one read that
    runs before the version picker would be the one read nobody authorized.
    """
    _activate(clean_runtime, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    denied = client.get("/viz/api/console/versions")
    search_denied = client.get(
        "/viz/api/console/records/search",
        params={"version": two_versions["second"], "q": "alpha"},
    )

    assert denied.status_code == search_denied.status_code == 403
    detail = denied.json()["detail"]
    assert detail["reason"] == search_denied.json()["detail"]["reason"] == "policy_denied"
    assert "okg deployment apply default" in detail["next_check"]
    assert detail["data"]["boundary_rule_present"] is False
    # The refusal happened instead of the read, not after it.
    assert "payload" not in denied.json()
    versions_decisions = [
        (action, effect) for action, effect, kind in _decisions(clean_runtime)
        if kind == "local_operator"
    ]
    assert ("graph.read.local_operator", "deny") in versions_decisions


def test_the_version_listing_is_allowed_by_the_same_boundary_rule(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get("/viz/api/console/versions")

    assert response.status_code == 200
    payload = response.json()["payload"]
    assert [row["generation_id"] for row in payload["versions"]] == [
        two_versions["second"], two_versions["first"],
    ]
    assert ("graph.read.local_operator", "allow", "local_operator") in _decisions(clean_runtime)


def _types(client: TestClient, version: str) -> dict[str, Any]:
    response = client.get("/viz/api/console/records/types", params={"version": version})
    assert response.status_code == 200
    return response.json()["payload"]


@pytest.mark.parametrize("rollup", [True, False], ids=["rollup", "flag_off_scan"])
def test_record_type_counts_are_published_totals_before_access_rules(
    policy_mix: str,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    rollup: bool,
) -> None:
    """The type list declares totals the reader may not be able to reach.

    This is a disclosure the operator accepted: a reader denied a whole type
    still learns the type exists and how many records it holds in the version.
    What the count must NOT do is leak a record: no record ID, label or
    attribute of a hidden row appears, and listing that type returns nothing.
    Both the rollup read and the flag-off scan compute the same totals, so
    turning the rollup off changes performance, not what is disclosed.
    """
    if not rollup:
        monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "0")
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    payload = _types(client, policy_mix)
    by_type = {row["type"]: row["count"] for row in payload["types"]}
    listing = client.get(
        "/viz/api/console/records/list", params={"version": policy_mix, "type": "secret"},
    ).json()["payload"]

    # Declared, and declared as what it is.
    assert payload["count_basis"] == "published_total_before_access_rules"
    # Every record of `secret` is restricted, so the reader can reach none of
    # them -- and still sees the type and its full total.
    assert by_type["secret"] == len(HIDDEN_NEIGHBOURS)
    assert listing["records"] == []
    # A partly hidden type reports its total, not the visible remainder.
    assert by_type["release"] == 5
    # The disclosure stops at the count: no identifier, label or attribute.
    body = payload["types"]
    assert set(body[0]) == {"type", "count"}
    serialized = repr(payload)
    assert not any(node_id in serialized for node_id in HIDDEN_NEIGHBOURS)
    assert "release:b" not in serialized and "restricted" not in serialized


def test_record_type_counts_agree_between_the_rollup_and_the_scan(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The flag-off scan is a real second path; it must not report different totals."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    with_rollup = _types(client, policy_mix)
    monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "0")
    without_rollup = _types(client, policy_mix)

    assert with_rollup == without_rollup


def test_record_types_are_denied_when_the_boundary_is(
    policy_mix: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    _enforce(monkeypatch)

    response = _client(clean_runtime).get(
        "/viz/api/console/records/types", params={"version": policy_mix},
    )

    assert response.status_code == 403
    assert response.json()["detail"]["reason"] == "policy_denied"


def _version_read_branches(dsn: str) -> list[tuple[str, str]]:
    """(effect, branch named in the audit target) for each version-listing decision."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, target->>'branch_name'
              FROM okg.nomos_decisions
             WHERE deployment_name = %s
               AND action = 'graph.read.local_operator'
               AND target->>'read' = 'versions'
             ORDER BY created_at, decision_row_id
            """,
            (DEPLOYMENT,),
        )
        return [(str(effect), str(branch)) for effect, branch in cur.fetchall()]


BRANCH = "release-2026"


def test_an_allowed_version_listing_audits_the_branch_it_reads(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An access record has to name the branch the access touched.

    The version listing pins no generation, so it passes no opened session to
    the boundary. Passing none at all let the audit target fall back to the
    literal "default", which on any deployment reading another branch records
    the access against a branch the request never touched -- a quietly wrong
    audit trail, and one nobody reading it could tell from a real one.
    """
    _activate(clean_runtime, [BOUNDARY_RULE])
    _enforce(monkeypatch)

    response = _client(clean_runtime, branch_name=BRANCH).get("/viz/api/console/versions")

    assert response.status_code == 200
    assert _version_read_branches(clean_runtime) == [("allow", BRANCH)]


def test_a_refused_version_listing_audits_the_branch_it_would_have_read(
    two_versions: dict[str, str], clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refusal is an access record too, and names the same branch."""
    _activate(clean_runtime, [_rule("rows_only", match={"resource_kind": "graph_object"})])
    _enforce(monkeypatch)

    response = _client(clean_runtime, branch_name=BRANCH).get("/viz/api/console/versions")

    assert response.status_code == 403
    recorded = _version_read_branches(clean_runtime)
    assert recorded == [("deny", BRANCH)]
    assert "default" not in {branch for _effect, branch in recorded}


def test_a_version_listing_with_no_branch_refuses_instead_of_guessing(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No branch to name means no read, not a read audited against a guess."""
    _activate(clean_runtime, [BOUNDARY_RULE])
    _enforce(monkeypatch)
    ctx = _Ctx(clean_runtime)
    ctx.session = SimpleNamespace(branch_name=None)

    app = FastAPI()
    from fastapi import APIRouter

    router = APIRouter(prefix="/viz/api")
    register_operator_record_routes(router, ctx_provider=lambda: ctx)
    app.include_router(router)

    response = TestClient(app).get("/viz/api/console/versions")

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "branch_unavailable"
    assert _version_read_branches(clean_runtime) == []


# --- the source filter (PACT operator-console-search-filters) ----------------
#
# The console's Explore view sends a producer id; the route used to read only
# `q` and `type` and drop it in silence. These cases hold the filter to what
# the rail claims: it narrows the result set and its count, it composes with
# `type`, it survives paging, it binds the exact-ID tier too, and it never
# widens what the reader may see.

GIT = "source-07-git"
PAPERS = "source-11-papers"
#: A producer whose every record this reader is denied. Its records carry a
#: word the other producers' do not, so the cases that count `widget` hits are
#: unaffected by its presence.
CLASSIFIED = "source-99-classified"


def _publish_from(dsn: str, batches: dict[str, list[NodeFact]]) -> str:
    """Publish one version whose records come from several producers.

    `_publish` above attributes every fact to one fixture producer, which is
    all its cases need. A source filter has nothing to bite on until the
    version holds records from more than one, so this applies each producer's
    facts under its own attribution and publishes once.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for producer, nodes in batches.items():
            store.apply_facts(list(nodes), source=producer)
    result = Publisher(
        dsn, DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
    ).cycle()
    assert result["status"] == "published"
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        return str(cur.fetchone()[0])


@pytest.fixture
def two_producers(clean_runtime: str) -> str:
    """One version, two producers, one word every record shares."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    return _publish_from(clean_runtime, {
        GIT: [
            _node("gitrec:1", "commit", name="G1", text="widget ingest alpha"),
            _node("gitrec:2", "commit", name="G2", text="widget ingest beta"),
            _node("gitrec:3", "note", name="G3", text="widget ingest gamma"),
            _node(
                "gitrec:hidden", "commit", name="G4",
                text="widget ingest delta", **RESTRICTED,
            ),
        ],
        PAPERS: [
            _node("paperrec:1", "paper", name="P1", text="widget ingest delta"),
            _node("paperrec:2", "paper", name="P2", text="widget ingest epsilon"),
        ],
        CLASSIFIED: [
            _node("cls:1", "brief", name="C1", text="zeta classified brief", **RESTRICTED),
        ],
    })


def _search(client: TestClient, version: str, **params: Any) -> dict[str, Any]:
    response = client.get(
        "/viz/api/console/records/search",
        params={"version": version, **params},
    )
    assert response.status_code == 200, response.text
    return response.json()["payload"]


def _ids(payload: dict[str, Any]) -> list[str]:
    return sorted(row["node_id"] for row in payload["results"])


def test_the_source_filter_narrows_the_result_set_and_its_count(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each producer's filter returns that producer's records, and counts them."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    unfiltered = _search(client, two_producers, q="widget")
    git_only = _search(client, two_producers, q="widget", source=GIT)
    papers_only = _search(client, two_producers, q="widget", source=PAPERS)

    assert _ids(unfiltered) == [
        "gitrec:1", "gitrec:2", "gitrec:3", "gitrec:hidden",
        "paperrec:1", "paperrec:2",
    ]
    assert _ids(git_only) == ["gitrec:1", "gitrec:2", "gitrec:3", "gitrec:hidden"]
    assert _ids(papers_only) == ["paperrec:1", "paperrec:2"]
    # The default lexical read is a bounded candidate selection, so it states
    # no count at all (PACT lexical-index-scan-cap-3162, policy option A) --
    # never the whole version's "6" beside two rows.
    assert unfiltered["total_count"] is None
    assert git_only["total_count"] is None
    assert papers_only["total_count"] is None


def test_the_source_filter_composes_with_the_type_filter(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both filters apply, not the last one to be read."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    both = _search(
        client, two_producers, q="widget", source=GIT, **{"type": "commit"},
    )

    assert _ids(both) == ["gitrec:1", "gitrec:2", "gitrec:hidden"]
    assert both["total_count"] is None
    # A type this producer never wrote is empty, not the other producer's rows.
    empty = _search(
        client, two_producers, q="widget", source=GIT, **{"type": "paper"},
    )
    assert empty["results"] == [] and empty["total_count"] is None


def test_paging_under_a_source_filter_walks_only_that_producer(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every page of a filtered search stays inside the filter."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    seen: list[str] = []
    for offset in range(0, 4):
        page = _search(
            client, two_producers, q="widget", source=GIT, limit=1, offset=offset,
        )
        seen.extend(row["node_id"] for row in page["results"])
        assert page["total_count"] is None

    assert sorted(seen) == ["gitrec:1", "gitrec:2", "gitrec:3", "gitrec:hidden"]
    assert not any(node_id.startswith("paperrec:") for node_id in seen)


def test_the_response_states_the_filters_it_applied(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The echo lets the client show the filters and detect a mismatch."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    filtered = _search(
        client, two_producers, q="widget", source=GIT, **{"type": "commit"},
    )
    plain = _search(client, two_producers, q="widget")

    assert filtered["filters"] == {"type": "commit", "source": GIT}
    assert plain["filters"] == {"type": None, "source": None}
    # An empty query answers with the same echo rather than dropping it.
    assert _search(client, two_producers, q=" ", source=GIT)["filters"] == {
        "type": None, "source": GIT,
    }


def test_an_exact_record_id_does_not_escape_the_source_filter(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Typing a filtered-out record's id must not bring it back.

    The exact-ID tier runs before the text search; if it ignored the filter,
    the one record the reader had excluded would be the first row on the page.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    in_filter = _search(client, two_producers, q="gitrec:1", source=GIT)
    out_of_filter = _search(client, two_producers, q="gitrec:1", source=PAPERS)

    assert in_filter["exact_match"] is True
    assert in_filter["results"][0]["node_id"] == "gitrec:1"
    assert out_of_filter["exact_match"] is False
    # The echo repeats the id the reader typed; the RESULTS must not.
    assert out_of_filter["results"] == []
    assert out_of_filter["total_count"] is None


def test_the_source_filter_never_widens_what_the_reader_may_see(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A record the policy hides stays hidden under its own producer's filter."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    git_only = _search(client, two_producers, q="widget", source=GIT)

    assert _ids(git_only) == ["gitrec:1", "gitrec:2", "gitrec:3"]
    assert "gitrec:hidden" not in repr(git_only)
    # No count escapes for the hidden row: the bounded default states none.
    assert git_only["total_count"] is None


def test_an_unseeable_producer_reads_the_same_as_one_that_does_not_exist(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing tells the reader which of the two it was.

    Every record this producer wrote is hidden from this reader, so the page is
    empty. A producer that was never configured is also empty. If the two
    answered differently, the filter would be an oracle for which producers
    exist.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    unseeable = _search(client, two_producers, q="zeta", source=CLASSIFIED)
    absent = _search(client, two_producers, q="zeta", source="source-does-not-exist")

    assert unseeable["results"] == [] and absent["results"] == []
    assert unseeable["total_count"] is None and absent["total_count"] is None
    assert unseeable["zero_hit_reason"] == absent["zero_hit_reason"]
    # The two responses differ only where they must: in the filter they echo.
    assert {**unseeable, "filters": None} == {**absent, "filters": None}


def test_the_producer_predicate_rides_in_the_ranked_query(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bounded cost: the filter is SQL in the ranked read, not a page sieve.

    The rank cap is a SQL LIMIT. A producer applied after ranking would page
    over a ranking the excluded rows had already filled, so the page would be
    short for a reason nothing discloses. This asserts placement directly: the
    statement carrying the BM25 order also carries the producer predicate.
    """
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    statements: list[str] = []

    class _Recording(psycopg.Cursor):
        """psycopg forbids rebinding `execute` on an instance, so record here."""

        def execute(self, query: Any, params: Any = None, **rest: Any) -> Any:
            statements.append(str(query))
            return super().execute(query, params, **rest)

    with psycopg.connect(
        clean_runtime, autocommit=True, cursor_factory=_Recording,
    ) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=two_producers,
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
        )
        search(
            conn, session, "widget", method="lexical",
            filter={"source": GIT}, limit=5,
        )

    ranked = [sql for sql in statements if "<@>" in sql and "LIMIT" in sql]
    assert ranked, "no ranked BM25 statement was executed"
    assert all("filter_source" in sql for sql in ranked), (
        "the producer predicate is missing from a ranked statement: "
        "it would be applied to the page after the rank cap"
    )


def test_the_shared_search_filters_a_producer_page_by_policy(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The key is agent-reachable through MCP, and carries no authority.

    The MCP search tool declares `filter` as a free-form dict and merges it
    straight into this call, so an agent can pass `source`. That is acceptable
    only because it is selection: the page it produces passes the same policy
    filter as an unfiltered one.
    """
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE, DENY_RESTRICTED_RULE])
    _enforce(monkeypatch)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=two_producers,
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
        )
        filtered = search(
            conn, session, "widget", method="lexical",
            filter={"source": GIT}, limit=20,
        )
        unfiltered = search(conn, session, "widget", method="lexical", limit=20)

    returned = {str(row["node_id"]) for row in filtered["results"]}
    assert returned == {"gitrec:1", "gitrec:2", "gitrec:3"}
    # The hidden record is absent from the unfiltered page too: the filter
    # removed nothing the policy was not already removing.
    assert "gitrec:hidden" not in {
        str(row["node_id"]) for row in unfiltered["results"]
    }


def test_an_unsupported_filter_key_is_still_refused(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adding `source` did not open the filter dict to anything else."""
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=two_producers,
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
        )
        with pytest.raises(ValueError, match="unknown filter keys"):
            search(
                conn, session, "widget", method="lexical",
                filter={"since": "2026-01-01"},
            )
        # And a method that cannot carry it into the ranked query refuses it
        # rather than sieving the page afterwards.
        with pytest.raises(ValueError, match="not supported on method"):
            search(
                conn, session, "widget", method="alias", filter={"source": GIT},
            )


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_source_is_no_filter_in_both_tiers_and_in_the_echo(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
    blank: str,
) -> None:
    """`?source=` means no filter, and means it everywhere.

    The exact-ID tier and the text tier read the same value, so a blank cannot
    filter one and be dropped by the other; the echo reports the value that was
    actually applied, which is none.
    """
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    blank_text = _search(client, two_producers, q="widget", source=blank)
    no_param = _search(client, two_producers, q="widget")
    blank_exact = _search(client, two_producers, q="gitrec:1", source=blank)

    # The text tier: a blank searches every producer, as no parameter does.
    assert _ids(blank_text) == _ids(no_param)
    assert blank_text["total_count"] is None and no_param["total_count"] is None
    # The exact-ID tier agrees rather than filtering on the empty string.
    assert blank_exact["exact_match"] is True
    assert blank_exact["results"][0]["node_id"] == "gitrec:1"
    # The echo reports no filter, not an empty one.
    assert blank_text["filters"] == {"type": None, "source": None}


def test_a_blank_type_is_no_filter_in_both_tiers_and_in_the_echo(
    two_producers: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same normalisation, for the filter that had the same split."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    blank = _search(client, two_producers, q="gitrec:1", **{"type": "  "})

    assert blank["exact_match"] is True
    assert blank["results"][0]["node_id"] == "gitrec:1"
    assert blank["filters"] == {"type": None, "source": None}


# --- PACT console-archiv3-read-layer-fixes: search tiers and type spread ----
#
# The shapes Archiv3 (`cern-r3final`) holds, reduced: many text chunks that
# repeat a word, the release records whose NAME is that word, a person titled
# only by `display_name`, and a page whose title holds a word its chunks
# repeat. Live, `CMSSW_13_0_0` ranked the release seventh with
# `exact_match: false`, `CMSSW` returned twenty chunks and none of 2,415
# releases, and `Deleted` ranked the real page eighth.

ARCHIV3_LIKE = "cern-r3final.twiki-eos"


@pytest.fixture
def archiv3_like(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    chunks = [
        _node(
            f"chunk:{index:02d}", "document_chunk",
            text=f"CMSSW CMSSW CMSSW Deleted Deleted Deleted {index}",
        )
        for index in range(8)
    ]
    return _publish_from(clean_runtime, {
        ARCHIV3_LIKE: [
            *chunks,
            _node(
                "cmssw_release:CMSSW_13_0_0", "cmssw_release",
                name="CMSSW_13_0_0", text="CMSSW_13_0_0 release",
            ),
            _node(
                "cmssw_release:CMSSW_13_0_0_pre1", "cmssw_release",
                name="CMSSW_13_0_0_pre1", text="CMSSW_13_0_0_pre1 release",
            ),
            _node(
                "person:234c3d3b65e30f95", "person",
                display_name="Piljun Gwak", person_id="234c3d3b65e30f95",
                text="Piljun Gwak",
            ),
            _node(
                "twiki:CMS:Deleted_3_1_X", "documentation_page",
                title="Deleted_3_1_X",
                text="Deleted_3_1_X release notes page for an older series",
            ),
        ],
    })


def test_a_release_name_is_an_exact_match_through_its_type(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _search(_client(clean_runtime), archiv3_like, q="CMSSW_13_0_0")

    assert payload["exact_match"] is True
    first = payload["results"][0]
    assert first["node_id"] == "cmssw_release:CMSSW_13_0_0"
    assert first["match"] == "typed_record_id"
    ids = [row["node_id"] for row in payload["results"]]
    assert ids.count("cmssw_release:CMSSW_13_0_0") == 1


def test_a_record_s_exact_label_is_an_exact_match(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _search(_client(clean_runtime), archiv3_like, q="piljun gwak")

    assert payload["exact_match"] is True
    first = payload["results"][0]
    assert first["node_id"] == "person:234c3d3b65e30f95"
    assert first["match"] == "label"
    assert first["label"] == "Piljun Gwak"


def _shared_top(dsn: str, version: str, query: str, limit: int) -> list[str]:
    """The shared search's own first rows: the order the page used to serve."""
    from okg.substrate.mcp.search import search
    from okg.substrate.mcp.session import open_session

    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name=DEPLOYMENT,
            requested_generation=version,
            nomos_subject=SUBJECT,
            nomos_read_action="graph.read.local_operator",
        )
        payload = search(conn, session, query, method="lexical", limit=limit)
    return [str(row["node_id"]) for row in payload["results"]]


def test_a_word_one_type_repeats_does_not_fill_the_page(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three rows asked for, eight chunks outrank every other match."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    # The fixture reproduces the defect: ranked alone, chunks fill the page.
    assert all(
        node.startswith("chunk:")
        for node in _shared_top(clean_runtime, archiv3_like, "CMSSW", 3)
    )

    payload = _search(_client(clean_runtime), archiv3_like, q="CMSSW", limit=3)

    types = {row["type"] for row in payload["results"]}
    assert "cmssw_release" in types, payload["results"]
    assert "document_chunk" in types
    leaders = [row for row in payload["results"] if row["promoted"] == "type_leader"]
    assert {row["type"] for row in leaders} == types
    assert payload["type_counts"]["document_chunk"] == 8
    assert payload["type_counts"]["cmssw_release"] == 2
    assert payload["type_counts_basis"] == "ranked_window"


def test_the_real_page_is_on_the_first_rows_for_its_word(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    # Ranked alone, the page is below the first three rows.
    assert "twiki:CMS:Deleted_3_1_X" not in _shared_top(
        clean_runtime, archiv3_like, "Deleted", 3,
    )

    payload = _search(_client(clean_runtime), archiv3_like, q="Deleted", limit=3)

    ids = [row["node_id"] for row in payload["results"]]
    assert "twiki:CMS:Deleted_3_1_X" in ids


def test_a_single_type_answer_promotes_nothing(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure case: a type filter leaves one type, so no row is moved."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _search(
        _client(clean_runtime), archiv3_like, q="CMSSW", type="document_chunk",
    )

    assert {row["type"] for row in payload["results"]} == {"document_chunk"}
    assert all(row["promoted"] is None for row in payload["results"])
    assert payload["type_counts"] == {"document_chunk": 8}


def test_paging_a_spread_page_neither_repeats_nor_drops_a_record(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    seen: list[str] = []
    offset: int | None = 0
    while offset is not None:
        payload = _search(client, archiv3_like, q="CMSSW", limit=2, offset=offset)
        seen.extend(row["node_id"] for row in payload["results"])
        offset = payload["next_offset"]
    everything = _search(client, archiv3_like, q="CMSSW", limit=50)

    assert len(seen) == len(set(seen))
    assert set(seen) == {row["node_id"] for row in everything["results"]}


def test_paging_past_an_exact_match_neither_repeats_nor_drops_a_record(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact match is also a ranked text match. It leads page one and must
    not come back, or shift the later pages, when the reader pages on."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    client = _client(clean_runtime)

    seen: list[str] = []
    offset: int | None = 0
    while offset is not None:
        payload = _search(client, archiv3_like, q="CMSSW_13_0_0", limit=1, offset=offset)
        seen.extend(row["node_id"] for row in payload["results"])
        offset = payload["next_offset"]
    everything = _search(client, archiv3_like, q="CMSSW_13_0_0", limit=50)

    assert seen.count("cmssw_release:CMSSW_13_0_0") == 1
    assert len(seen) == len(set(seen))
    assert set(seen) == {row["node_id"] for row in everything["results"]}
    # The pages walk the text rows alone: the exact row leads page one and
    # is not counted among the rows later pages hold (issue #2789).
    assert everything["next_offset"] is None
    assert everything["paged_count"] == sum(
        1 for row in everything["results"] if row["match"] == "text"
    )
    assert everything["paged_count"] == len(seen) - 1


def test_every_record_read_names_the_source_by_its_declared_name(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Search rows said `cern-r3final.twiki-eos` where Sources said
    `twiki_eos`; each read now carries both, each under its own name."""
    from okg.substrate.console_views import records

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    monkeypatch.setattr(
        records, "source_display_names",
        lambda deployment: ({ARCHIV3_LIKE: "twiki_eos"}, None),
    )
    client = _client(clean_runtime)

    hit = _search(client, archiv3_like, q="CMSSW_13_0_0")["results"][0]
    detail = client.get(
        "/viz/api/console/records/detail",
        params={"version": archiv3_like, "id": "person:234c3d3b65e30f95"},
    ).json()["payload"]
    raw = client.get(
        "/viz/api/console/records/raw",
        params={"version": archiv3_like, "id": "person:234c3d3b65e30f95"},
    ).json()["payload"]

    assert hit["source"] == ARCHIV3_LIKE
    assert hit["source_name"] == "twiki_eos"
    assert detail["source"] == ARCHIV3_LIKE
    assert detail["source_name"] == "twiki_eos"
    assert detail["label"] == "Piljun Gwak"
    assert raw["row"]["source"] == ARCHIV3_LIKE
    assert raw["row"]["source_name"] == "twiki_eos"


def test_a_deployment_with_no_registry_says_why_no_source_is_named(
    archiv3_like: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)

    payload = _search(_client(clean_runtime), archiv3_like, q="CMSSW_13_0_0")

    assert payload["results"][0]["source_name"] is None
    assert payload["source_names_unavailable_reason"]


# ---------- console-search-profile-subtypes (issue #2545) ----------
#
# okg-workspace's `analyst` profile names 25 types its published catalog does
# not define, and every console search answered a generic 500. These run the
# route against a real published version with a deployment `search:` block and
# a module-composed catalog substituted, so the only thing that differs between
# the cases is whether a missing type is one the deployment's modules define.


@pytest.fixture
def profiled_version(clean_runtime: str) -> str:
    from tests.pytest.catalog_fixture_builders import seed_catalog

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["page", "pact_task"])
    _publish(
        clean_runtime,
        [
            _node("page:deploy", "page", title="Deploy guide", text="deploy notes"),
            _node("task:deploy", "pact_task", name="T", text="deploy task tracking"),
        ],
    )
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM okg.v_generations WHERE deployment_name = %s "
            "AND status = 'published' ORDER BY published_at DESC LIMIT 1",
            (DEPLOYMENT,),
        )
        return str(cur.fetchone()[0])


def _profile_deployment(
    monkeypatch: pytest.MonkeyPatch, *, profile: list[str], module_types: set[str],
) -> list[int]:
    """Give the route a deployment whose `analyst` profile is `profile` and
    whose modules compose `module_types`. Returns a list counting compositions."""
    import okg.substrate.catalog.modules_compose as modules_compose
    import okg.substrate.deployments as deployments

    manifest = SimpleNamespace(
        name=DEPLOYMENT,
        deployment_dir=None,
        search={
            "default_profile": "analyst",
            "profiles": {
                "analyst": {"subtypes": profile},
                "ops": {"subtypes": ["pact_task"]},
            },
        },
    )
    composed: list[int] = []

    def compose_catalog(target: Any) -> Any:
        assert target is manifest
        composed.append(1)
        return SimpleNamespace(subtypes=set(module_types))

    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: manifest)
    monkeypatch.setattr(modules_compose, "compose_catalog", compose_catalog)
    return composed


def test_search_leaves_out_profile_types_the_version_lacks_and_names_them(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    composed = _profile_deployment(
        monkeypatch,
        profile=["page", "paper", "venue"],
        module_types={"page", "pact_task", "paper", "venue"},
    )

    payload = _search(_client(clean_runtime), profiled_version, q="deploy")

    # It answers, and is still scoped: the ops-profile task is not on the page.
    assert _ids(payload) == ["page:deploy"]
    assert payload["search_profile"] == "analyst"
    assert payload["searched_types"] == ["page"]
    assert payload["skipped_types"] == ["paper", "venue"]
    assert payload["skipped_types_reason"] == "not_in_this_version_catalog"
    assert composed == [1]


def test_search_with_every_profile_type_present_skips_nothing(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    composed = _profile_deployment(
        monkeypatch, profile=["page"], module_types={"page", "pact_task"},
    )

    payload = _search(_client(clean_runtime), profiled_version, q="deploy")

    assert _ids(payload) == ["page:deploy"]
    assert payload["search_profile"] == "analyst"
    assert payload["skipped_types"] == []
    assert payload["skipped_types_reason"] is None
    # A catalog that matches its manifest never pays for a composition.
    assert composed == []


def test_search_refuses_a_profile_type_no_module_defines_with_a_typed_reason(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typo in the profile is a broken profile: refused and named, not a 500."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _profile_deployment(
        monkeypatch,
        profile=["page", "paper", "pgae"],
        module_types={"page", "pact_task", "paper"},
    )

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": profiled_version, "q": "deploy"},
    )

    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "search_profile_subtype_absent"
    assert detail["error_type"] == "ProfileSubtypeAbsent"
    assert detail["data"]["absent"] == ["pgae"]
    assert detail["version"] == profiled_version
    assert detail["next_check"]


PRIVATE_MANIFEST_DIR = "/home/operator/private-deployments/demo"


def _search_block_deployment(
    monkeypatch: pytest.MonkeyPatch, search: dict[str, Any],
) -> None:
    """Give the route a deployment whose `search:` block is `search`, loaded
    from a manifest directory whose path must never reach the page."""
    import okg.substrate.deployments as deployments

    manifest = SimpleNamespace(
        name=DEPLOYMENT, deployment_dir=PRIVATE_MANIFEST_DIR, search=search,
    )
    monkeypatch.setattr(deployments, "resolve_deployment", lambda name: manifest)


def _assert_search_block_refused_without_path(
    response: Any, *, error_type: str, version: str,
) -> None:
    assert response.status_code == 503, response.text
    detail = response.json()["detail"]
    assert detail["error_type"] == error_type
    assert detail["reason"].startswith("search_")
    assert detail["version"] == version
    assert detail["error"] == (
        "The deployment's `search:` block did not load; the cause is in the "
        "server log."
    )
    assert detail["data"] is None
    assert PRIVATE_MANIFEST_DIR not in response.text
    assert "Manifest:" not in response.text


def test_an_undeclared_default_profile_keeps_the_manifest_path_off_the_page(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`default_profile: analyts` raises UnknownSearchProfile at load, whose
    message ends in "(Manifest: <path>)"; the page gets the fixed sentence."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _search_block_deployment(monkeypatch, {
        "default_profile": "analyts",
        "profiles": {"analyst": {"subtypes": ["page"]}},
    })

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": profiled_version, "q": "deploy"},
    )

    _assert_search_block_refused_without_path(
        response, error_type="UnknownSearchProfile", version=profiled_version,
    )


def test_a_profile_naming_an_undeclared_family_keeps_the_manifest_path_off_the_page(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A profile naming a source family the block does not declare raises
    UnknownSourceFamily at load; the page gets the fixed sentence."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _search_block_deployment(monkeypatch, {
        "default_profile": "analyst",
        "profiles": {
            "analyst": {"subtypes": ["page"], "source_families": ["no_such_family"]},
        },
    })

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": profiled_version, "q": "deploy"},
    )

    _assert_search_block_refused_without_path(
        response, error_type="UnknownSourceFamily", version=profiled_version,
    )


def test_a_malformed_concept_groups_block_keeps_the_manifest_path_off_the_page(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`concept_groups: 5` raises ConceptGroupConfigInvalid while the block
    loads, whose message ends in "(Manifest: <path>)"; it was an opaque 500
    and is now the fixed 503 sentence with no path."""
    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _search_block_deployment(monkeypatch, {
        "default_profile": "analyst",
        "profiles": {"analyst": {"subtypes": ["page"]}},
        "concept_groups": 5,
    })

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": profiled_version, "q": "deploy"},
    )

    _assert_search_block_refused_without_path(
        response, error_type="ConceptGroupConfigInvalid", version=profiled_version,
    )


def test_a_failing_catalog_composition_fails_the_search_rather_than_unscoping_it(
    profiled_version: str, clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a profile type is absent the console composes the deployment's
    catalog to decide whether to leave it out. If that composition fails, the
    search must fail: running it unscoped would put the ops-profile task on
    the page while the envelope claimed the analyst profile."""
    import importlib

    import okg.substrate.catalog.modules_compose as modules_compose

    # The `okg.substrate.mcp` package re-exports the `search` function under the
    # submodule's name, so take the module itself from the import system.
    search_module = importlib.import_module("okg.substrate.mcp.search")

    _activate(clean_runtime, [BOUNDARY_RULE, ROW_RULE])
    _enforce(monkeypatch)
    _profile_deployment(
        monkeypatch, profile=["page", "paper"], module_types={"page", "paper"},
    )

    def broken_compose(target: Any) -> Any:
        raise RuntimeError("module file /home/operator/modules/x.yaml unreadable")

    searched: list[int] = []
    real_search = search_module.search

    def counting_search(*args: Any, **kwargs: Any) -> Any:
        searched.append(1)
        return real_search(*args, **kwargs)

    monkeypatch.setattr(modules_compose, "compose_catalog", broken_compose)
    monkeypatch.setattr(search_module, "search", counting_search)

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"version": profiled_version, "q": "deploy"},
    )

    assert response.status_code == 500, response.text
    detail = response.json()["detail"]
    assert detail["reason"] == "record_search_failed"
    assert detail["error_type"] == "RuntimeError"
    assert "/home/operator" not in response.text
    assert searched == []


def test_a_text_search_hit_masks_the_path_in_an_untrusted_record_identity(
    clean_runtime: str,
) -> None:
    """Issue #2689 fences an untrusted row's `source_record_id` on the shared
    search. The console masks absolute paths in that identity, which only works
    on the producer's own value: fed the fenced string, it showed the whole
    home-directory path with the fence markers as text."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(
                node_id="page:pathed",
                subtype="page",
                attrs={"title": "Pathed guide", "text": "quokka release notes"},
                source_record_id={"path": "/Users/operator/secret/guide.md"},
                source_revision={"rev": "r1"},
            ),
        ], source="fixture")
    published = Publisher(
        clean_runtime, DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(clean_runtime, DEPLOYMENT),
    ).cycle()
    assert published["status"] == "published"

    response = _client(clean_runtime).get(
        "/viz/api/console/records/search",
        params={"q": "quokka", "version": str(published["generation_id"])},
    )

    assert response.status_code == 200, response.text
    (hit,) = [
        row for row in response.json()["payload"]["results"]
        if row["node_id"] == "page:pathed"
    ]
    # The fixture producer is unprofiled, so the row is untrusted.
    assert hit["trust"] == "untrusted"
    assert hit["source_record_id"] == {"path": "guide.md"}
    assert hit["source_revision"] == {"rev": "r1"}
    assert "/Users/operator" not in response.text
    assert "UNTRUSTED_DATA" not in str(hit["source_record_id"])
