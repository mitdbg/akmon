"""expose-mcp-node-lookup (GitHub #641): typed attrs/path node lookup for
least-privilege MCP readers, and an explicit inspect(node) not-found signal.

Two pinned proofs (one per graph-backed requirement):

- ``test_path_lookup_least_privilege`` — under Nomos ENFORCE with the
  default ``subject:mcp_client`` (zero ``graph.read.sql_admin`` grants), a
  reader resolves a file node by exact path and a symbol by name through
  ``search(method="attrs", attr_filters=...)`` (the issue's reported
  parameter shape, query-less, routes there automatically), and a path
  SUFFIX through ``filter``'s ``suffix`` predicate — while the raw-SQL
  ``query`` path stays denied. The lookup is served by the indexed
  effective-attrs read path (PR #383): the generated WHERE carries the
  GIN-indexable ``attrs @> ...`` containment, and EXPLAIN over that exact
  shape uses an index on ``okg.attribute_effective_view``, not a Seq Scan
  (req.lookup.typed-attrs-path).
- ``test_inspect_missing_id_not_found`` — ``inspect`` on a node id absent
  at the pin raises the structured ``node_not_found`` error naming the id
  and the pinned generation with ``exists: false``; a found node with
  sparse attrs still returns its normal payload, so absence and
  sparseness are distinguishable (req.lookup.inspect-not-found).
"""
from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("mcp")

from okg.substrate.attributes import (  # noqa: E402
    materialize_effective_attributes_for_generation,
)
from okg.substrate.errors import NodeNotFound, SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

FIXTURE_SOURCE = "fixture"
REPO = "marius"
FILE_NODE = f"file:{REPO}:CMakeLists.txt"
DEEP_FILE_NODE = f"file:{REPO}:examples/docker/Dockerfile"
SYMBOL_NODE = f"symbol:{REPO}:src/util/helpers.py:load_config:12"
SPARSE_NODE = f"file:{REPO}:empty/.gitkeep"
MISSING_NODE = f"file:{REPO}:examples/docker/Dockerfile.missing"


@pytest.fixture(autouse=True)
def _no_subject_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with no explicit Nomos subject exported, so the MCP
    server presents the baseline-granted default ``subject:mcp_client``."""
    for var in ("OKG_NOMOS_SUBJECT", "OKG_MCP_NOMOS_SUBJECT", "OKG_PRINCIPAL"):
        monkeypatch.delenv(var, raising=False)


def _baseline_manifest() -> dict:
    """The recognized deployment rollout baseline: mcp_client is a read
    principal; graph.read.sql_admin stays human_operator-only (least
    privilege — exactly the marius posture from issue #641)."""
    return {
        "nomos": {
            "runtime_enforcement": "enforce",
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": [
                    "human_operator", "mcp_client", "okg_service",
                ],
                "write_principals": [
                    "human_operator", "okg_service", "source_adapter",
                ],
                "mcp_write_default": "review",
                "export_policy": "no_export",
                "secret_handling": "secret_ref_required",
                "live_call_default": "deny",
            },
        },
    }


def _seed_baseline(dsn: str) -> None:
    from okg.substrate.nomos import seed_deployment_baseline_nomos_state

    with psycopg.connect(dsn) as conn:
        payload = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name="default",
            manifest=_baseline_manifest(),
            source_names=[FIXTURE_SOURCE],
            source_policies={FIXTURE_SOURCE: {"live_call_allowed": False}},
            runtime_source_names=[FIXTURE_SOURCE],
        )
        conn.commit()
    assert payload["ok"] is True, payload


def _publish_lookup_fixture(dsn: str) -> str:
    """Publish a small workspace-shaped graph (files + a symbol) and
    materialize the effective-attrs snapshot for the generation. Runs
    BEFORE the enforcement env flips (the publisher is not the subject
    under test)."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file", "code_symbol"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=FILE_NODE,
                    subtype="source_file",
                    # File paths/names are NOT in the searchable text —
                    # the content-indexed miss that motivated the typed
                    # lookup (issue #641 repro step 1).
                    attrs={
                        "path": "CMakeLists.txt",
                        "repo": REPO,
                        "text": "cmake minimum required project neighbor sampling",
                    },
                    source_record_id={"id": FILE_NODE},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id=DEEP_FILE_NODE,
                    subtype="source_file",
                    attrs={"path": "examples/docker/Dockerfile", "repo": REPO},
                    source_record_id={"id": DEEP_FILE_NODE},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id=SYMBOL_NODE,
                    subtype="code_symbol",
                    attrs={
                        "name": "load_config",
                        "path": "src/util/helpers.py",
                        "kind": "function",
                    },
                    source_record_id={"id": SYMBOL_NODE},
                    source_revision={"v": 1},
                ),
                NodeFact(
                    node_id=SPARSE_NODE,
                    subtype="source_file",
                    attrs={},  # found-but-sparse control for not-found
                    source_record_id={"id": SPARSE_NODE},
                    source_revision={"v": 1},
                ),
            ],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def _mcp_read_decisions(dsn: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, subject
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND action IN ('mcp.tool.call', 'graph.read.mcp')
             ORDER BY created_at, decision_row_id
            """,
        )
        return [(str(a), str(e), str(s)) for a, e, s in cur.fetchall()]


def test_path_lookup_least_privilege(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.lookup.typed-attrs-path: exact path, name, and path suffix all
    resolve through typed operators for the enforce-mode default
    ``subject:mcp_client`` — no sql_admin — via the indexed attribute
    read path."""
    gen = _publish_lookup_fixture(clean_runtime)
    _seed_baseline(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    # Under access rules the server withholds and refuses `query` (issue
    # #3666); step 4 proves run_sql's own run-time check below that, which
    # still guards run_sql's other callers, so the server is told to serve it.
    monkeypatch.setattr(
        "okg.substrate.mcp.effective_surface.query_withheld_by_access_rules",
        lambda _name: None,
    )

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    tools = mcp._tool_manager._tools
    try:
        # Repro contrast (issue #641 step 1): content search cannot find the
        # file by name — the path is not in the indexed text.
        lexical = tools["search"].fn(
            query="CMakeLists.txt", method="lexical", generation_id=gen,
        )
        assert lexical["results"] == [], lexical

        # 1. Exact path, the issue's reported parameter shape: query-less
        #    attr_filters + subtype_filter routes to the typed attrs lookup.
        by_path = tools["search"].fn(
            attr_filters={"path": "CMakeLists.txt"},
            subtype_filter="source_file",
            generation_id=gen,
        )
        assert by_path["method"] == "attrs"
        assert [r["node_id"] for r in by_path["results"]] == [FILE_NODE]
        assert by_path["total_count"] == 1
        assert by_path["truncated"] is False
        assert by_path["pinned_generation_id"] == gen
        # Effective attrs come back (the lookup is decision-shaped). The
        # unprofiled fixture source is untrusted, so string attrs arrive
        # FENCED (PACT fence-untrusted-node-text) with a provenance block —
        # the same trust boundary as every other node read.
        fenced_path = by_path["results"][0]["attrs"]["path"]
        assert "CMakeLists.txt" in fenced_path
        assert by_path["results"][0]["provenance"]["trust"] == "untrusted"

        # 2. Explicit method="attrs": resolve a symbol by name.
        by_name = tools["search"].fn(
            method="attrs",
            attr_filters={"name": "load_config"},
            subtype_filter="code_symbol",
            generation_id=gen,
        )
        assert [r["node_id"] for r in by_name["results"]] == [SYMBOL_NODE]

        # 3. Path SUFFIX through filter's typed predicate tree.
        by_suffix = tools["filter"].fn(
            input={"kind": "nodes", "subtype": "source_file"},
            predicate={
                "op": "suffix",
                "field": "attrs.path",
                "value": "docker/Dockerfile",
            },
            select=["node_id"],
            generation_id=gen,
        )
        assert [item["node_id"] for item in by_suffix["items"]] == [
            DEEP_FILE_NODE
        ]

        # 4. Least privilege is real: the same reader is DENIED the raw-SQL
        #    recipe the old docs pointed at (graph.read.sql_admin is
        #    human_operator-scoped in the baseline).
        with pytest.raises(SubstrateError) as exc_info:
            tools["query"].fn(
                sql="SELECT node_id FROM okg.v_nodes LIMIT 1",
                generation_id=gen,
            )
        assert exc_info.value.code == "nomos_runtime_denied"
        assert exc_info.value.data["surface"] == "graph.read.sql_admin"
        assert exc_info.value.data["presented_subject"] == "subject:mcp_client"

        # ... and every typed read above was allowed AS subject:mcp_client.
        boundary = _mcp_read_decisions(clean_runtime)
        assert boundary, "expected nomos decisions for the typed reads"
        assert all(effect == "allow" for _a, effect, _s in boundary), boundary
        assert all(
            subject == "subject:mcp_client" for _a, _e, subject in boundary
        ), boundary

        # 5. Indexed path (PR #383), not a seq scan over attrs:
        #    (a) the operator's generated WHERE carries the GIN-indexable
        #        containment over the effective-attrs snapshot;
        where, params = mcp._okg_effective_view_where(
            "nodes",
            {
                "op": "and",
                "clauses": [
                    {"op": "eq", "field": "subtype", "value": "source_file"},
                    {
                        "op": "contains",
                        "field": "attrs",
                        "value": {"path": "CMakeLists.txt"},
                    },
                ],
            },
        )
        assert "attrs @>" in where, where
        assert "subtype =" in where, where

        #    (b) EXPLAIN over that exact WHERE shape uses an index on
        #        okg.attribute_effective_view — enable_seqscan=off makes any
        #        seq-scan-only plan loudly visible on this small fixture.
        #        schema-interval-narrow-split: the payload (and so the attrs
        #        containment) resolves through the version-store join, exactly
        #        as the production reader's from-clause does.
        sql_params: dict[str, object] = {"tk": "node", "gen": gen, "lim": 10}
        sql_params.update(params)
        with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
            cur.execute("SET enable_seqscan = off")
            cur.execute(
                "EXPLAIN SELECT target_id, subtype, attrs "
                "FROM okg.attribute_effective_view "
                "JOIN okg.attribute_versions "
                "USING (target_kind, target_id, attrs_hash) "
                "WHERE target_kind = %(tk)s AND generation_id = %(gen)s "
                "AND namespace = 'default'" + where + " LIMIT %(lim)s",
                sql_params,
            )
            plan = "\n".join(row[0] for row in cur.fetchall())
        assert (
            "Index Scan" in plan
            or "Index Only Scan" in plan
            or "Bitmap Index Scan" in plan
        ), plan
        assert "Seq Scan on attribute_effective_view" not in plan, plan
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_inspect_missing_id_not_found(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.lookup.inspect-not-found: a missing id is an explicit, structured
    not-found naming the id + pinned generation (exists: false) — never an
    echo of the requested id with null attrs — while found-but-sparse keeps
    returning its normal payload."""
    gen = _publish_lookup_fixture(clean_runtime)
    _seed_baseline(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    tools = mcp._tool_manager._tools
    try:
        # Absent at the pin -> loud structured not-found.
        with pytest.raises(NodeNotFound) as exc_info:
            tools["inspect"].fn(
                target={"kind": "node", "node_id": MISSING_NODE},
                generation_id=gen,
            )
        err = exc_info.value
        assert err.code == "node_not_found"
        assert err.data["exists"] is False
        assert err.data["node_id"] == MISSING_NODE
        assert err.data["generation_id"] == gen
        assert err.data["pinned_generation_id"] == gen
        # The message names both the id and the pinned generation.
        assert MISSING_NODE in err.message
        assert gen in err.message
        # The fix hint routes to the sanctioned lookup, not id-guessing.
        assert "attrs" in (err.fix_hint or "")

        # Found-but-sparse stays a normal payload — distinguishable from
        # absent (no exception, no exists:false, the id is genuinely real).
        sparse = tools["inspect"].fn(
            target={"kind": "node", "node_id": SPARSE_NODE},
            generation_id=gen,
        )
        assert sparse["node_id"] == SPARSE_NODE
        assert sparse["subtype"] == "source_file"
        assert isinstance(sparse.get("attrs"), dict)
        assert sparse.get("exists") is not False
        assert sparse["pinned_generation_id"] == gen
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_a_hand_built_id_names_the_close_visible_ids(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3605: 197 of 225 not-found ids in lane X's runs were built by hand
    from the id patterns with a wrong repo or path. The refusal names the
    visible ids with the same prefix and path tail; nothing is accepted in
    the missing id's place, and a node the reader cannot see is never
    named."""
    gen = _publish_lookup_fixture(clean_runtime)
    _seed_baseline(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")

    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn

    def refusal(kind: str, node_id: str) -> NodeNotFound:
        with pytest.raises(NodeNotFound) as caught:
            inspect(target={"kind": kind, "node_id": node_id}, generation_id=gen)
        return caught.value

    try:
        # Wrong repo, and a leading directory too many: same prefix and tail.
        for kind in ("node", "text"):
            for guess in (
                "file:other:examples/docker/Dockerfile",
                f"file:{REPO}:repo/examples/docker/Dockerfile",
            ):
                err = refusal(kind, guess)
                assert err.code == "node_not_found"
                assert err.data["close_id_check"] == "checked"
                assert err.data["close_ids"] == [DEEP_FILE_NODE], (kind, guess)
                assert DEEP_FILE_NODE in err.message
                assert err.fix_hint.startswith(f"retry with one of ['{DEEP_FILE_NODE}']")
        # A symbol id with a guessed line: the named symbol comes first.
        err = refusal("node", f"symbol:{REPO}:src/util/helpers.py:load_config:??")
        assert err.data["close_ids"][0] == SYMBOL_NODE
        # Another prefix with that path: none named, and the hint says so.
        err = refusal("node", f"doc:{REPO}:examples/docker/Dockerfile")
        assert "close_ids" not in err.data
        assert err.data["close_id_check"] == "checked"
        assert err.fix_hint.startswith("no visible doc: id has that path; ")
        # An id with no path is not looked up.
        err = refusal("node", "jira:NOPE-1")
        assert err.data["close_id_check"] == "no_path"
        assert "attrs" in err.fix_hint

        # The reader's own filter decides: a hidden node is never named, and
        # a path the reader sees masked is never matched (filter, then path).
        import okg.substrate.mcp.tools as tools_mod
        from okg.substrate.nomos.read_masking import MASK_TOKEN

        real_filter = tools_mod.filter_node_rows_for_session
        for stub in (
            lambda conn, session, rows, **kw: [],
            lambda conn, session, rows, **kw: [
                {**row, "attrs": {**row["attrs"], "path": MASK_TOKEN}}
                for row in real_filter(conn, session, rows, **kw)
            ],
        ):
            monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", stub)
            err = refusal("node", "file:other:examples/docker/Dockerfile")
            assert "close_ids" not in err.data
            assert DEEP_FILE_NODE not in err.message + (err.fix_hint or "")
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)

        # A failed close-id read says so and leaves the caller's connection
        # and its statement timeout as they were.
        from okg.substrate.mcp import open_session

        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default")
            conn.execute("SET statement_timeout = 7777")
            # (This session has no reader identity, so enforce shows it no
            # rows; only the check and the timeout are asserted here.)
            assert tools_mod.close_node_ids(
                conn, session, "file:other:examples/docker/Dockerfile",
            )[1] == "checked"
            assert conn.execute("SHOW statement_timeout").fetchone()[0] == "7777ms"
            monkeypatch.setattr(tools_mod, "_chronos_checkout",
                                lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
            assert tools_mod.close_node_ids(
                conn, session, "file:other:examples/docker/Dockerfile",
            ) == ([], "unavailable")
            assert conn.execute("SHOW statement_timeout").fetchone()[0] == "7777ms"
        err = refusal("node", "file:other:examples/docker/Dockerfile")
        assert err.data["close_id_check"] == "unavailable"
        assert "close_ids" not in err.data
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


RULE_DOC = "doc:rucio:lib/rucio/cli/rule.py"
RULE_SYMBOL = "symbol:rucio:lib/rucio/cli/rule.py:main"
SHARED_DOCS = ["doc:cmsrucio:lib/shared.py", "doc:rucio:lib/shared.py"]
REPAIRER_DOC = "doc:rucio:lib/rucio/daemons/judge/repairer.py"


def _publish_path_fixture(dsn: str) -> str:
    """Files named by path (#3666 step 3b): one file with a symbol of its
    own (the symbol has the path too, and is not the file), one path in two
    repositories, and one file a guessed path names by its last two parts."""
    def doc(node_id: str, path: str, repo: str) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype="source_file",
            attrs={"path": path, "repo": repo, "text": f"contents of {path}"},
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file", "code_symbol"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                doc(RULE_DOC, "lib/rucio/cli/rule.py", "rucio"),
                NodeFact(
                    node_id=RULE_SYMBOL, subtype="code_symbol",
                    attrs={"path": "lib/rucio/cli/rule.py", "name": "main"},
                    source_record_id={"id": RULE_SYMBOL}, source_revision={"v": 1},
                ),
                doc(SHARED_DOCS[0], "lib/shared.py", "cmsrucio"),
                doc(SHARED_DOCS[1], "lib/shared.py", "rucio"),
                doc(REPAIRER_DOC, "lib/rucio/daemons/judge/repairer.py", "rucio"),
            ],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def test_inspect_names_a_file_by_path_only_when_one_record_has_it(
    clean_runtime: str,
) -> None:
    """Jason 2026-10-12: `inspect` takes `path` (and optional `source`) on
    `node` and `text`. Exactly one record: served, the reply naming the id.
    Several: refused with the candidates. None: refused, with a call to
    retry when exactly one near candidate exists. Nothing is guessed."""
    from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
    from okg.substrate.mcp.result_text import render

    gen = _publish_path_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    try:
        # One record (the symbol has the path too, but is not the file).
        node = inspect(target={"kind": "node", "path": "lib/rucio/cli/rule.py"},
                       generation_id=gen)
        assert node["node_id"] == RULE_DOC
        assert node["resolved_path"] == {"path": "lib/rucio/cli/rule.py", "node_id": RULE_DOC}
        lines = render(node, tool="inspect", kind="node").split("\n")
        assert RULE_DOC in lines[0]
        assert f'! path "lib/rucio/cli/rule.py" resolved to "{RULE_DOC}"' in lines
        text = inspect(target={"kind": "text", "path": "./lib/rucio/cli/rule.py",
                               "source": "rucio"}, generation_id=gen)
        assert text["node_id"] == RULE_DOC
        assert text["resolved_path"]["source"] == "rucio"
        assert RULE_DOC in render(text, tool="inspect", kind="text").split("\n", 1)[0]

        # Two repositories hold the path: refused with both; `source` picks one.
        with pytest.raises(InvalidOperatorArguments) as several:
            inspect(target={"kind": "node", "path": "lib/shared.py"}, generation_id=gen)
        assert several.value.data["candidate_ids"] == SHARED_DOCS
        assert "add source" in several.value.fix_hint
        picked = inspect(target={"kind": "node", "path": "lib/shared.py",
                                 "source": "cmsrucio"}, generation_id=gen)
        assert picked["node_id"] == SHARED_DOCS[0]

        # A source that does not hold the path: the one file that does is a
        # call to retry, and that call is served.
        with pytest.raises(NodeNotFound) as wrong_source:
            inspect(target={"kind": "text", "path": "lib/rucio/cli/rule.py",
                            "source": "cms"}, generation_id=gen)
        assert wrong_source.value.data["close_ids"] == [RULE_DOC]
        retry = wrong_source.value.retry_call
        assert retry["arguments"]["target"] == {"kind": "text", "node_id": RULE_DOC}
        assert inspect(**retry["arguments"], generation_id=gen)["node_id"] == RULE_DOC

        # A moved file: the same last two parts under another directory.
        with pytest.raises(NodeNotFound) as moved:
            inspect(target={"kind": "node", "path": "lib/rucio/judge/repairer.py"},
                    generation_id=gen)
        assert moved.value.data["close_ids"] == [REPAIRER_DOC]
        assert moved.value.retry_call["arguments"]["target"]["node_id"] == REPAIRER_DOC

        # Nothing near: refused, no call offered.
        with pytest.raises(NodeNotFound) as missing:
            inspect(target={"kind": "node", "path": "lib/none.py"}, generation_id=gen)
        assert missing.value.data["exists"] is False
        assert "close_ids" not in missing.value.data
        assert getattr(missing.value, "retry_call", None) is None

        # A path and an id that could disagree: refused.
        with pytest.raises(InvalidOperatorArguments):
            inspect(target={"kind": "node", "node_id": RULE_DOC,
                            "path": "lib/shared.py"}, generation_id=gen)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


#: More repositories with a README.md than one path read covers.
README_REPOS = [f"r{index:02d}" for index in range(55)]


def _publish_readme_fixture(dsn: str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id=f"doc:{repo}:README.md", subtype="source_file",
                    attrs={"path": "README.md", "repo": repo, "text": f"readme of {repo}"},
                    source_record_id={"id": f"doc:{repo}:README.md"},
                    source_revision={"v": 1},
                )
                for repo in [*README_REPOS, "zrepo"]
            ],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def test_a_path_in_more_repositories_than_one_read_covers_is_never_called_missing(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of 3b: README.md in 56 repositories. `source` narrows in SQL,
    so the repository past the first 50 is found; without it the lookup
    says it read only the first 50 ("at least"), never "names 50" or "not
    found"; and rows the reader cannot see crowding the read never turn
    into "exists: false"."""
    import okg.substrate.mcp.tools as tools_mod
    from okg.substrate.mcp.response_bounds import InvalidOperatorArguments

    gen = _publish_readme_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    real_filter = tools_mod.filter_node_rows_for_session
    try:
        served = inspect(target={"kind": "node", "path": "README.md", "source": "zrepo"},
                         generation_id=gen)
        assert served["node_id"] == "doc:zrepo:README.md"

        with pytest.raises(InvalidOperatorArguments) as many:
            inspect(target={"kind": "node", "path": "README.md"}, generation_id=gen)
        assert "at least 50 records" in many.value.message
        assert many.value.data["path_check"] == "incomplete"
        assert "add source" in many.value.fix_hint

        # Every row but zrepo's hidden (as access rules under enforce
        # would): the first 50 stored rows hold nothing readable.
        monkeypatch.setattr(
            tools_mod, "filter_node_rows_for_session",
            lambda conn, session, rows, **kw: [
                row for row in real_filter(conn, session, rows, **kw)
                if row["node_id"] == "doc:zrepo:README.md"
            ],
        )
        with pytest.raises(NodeNotFound) as crowded:
            inspect(target={"kind": "node", "path": "README.md"}, generation_id=gen)
        assert crowded.value.data["exists"] is None
        assert crowded.value.data["path_check"] == "incomplete"
        assert "not a finding that none exists" in crowded.value.message
        # With source, the hidden rows take no room: zrepo's record is served.
        assert inspect(
            target={"kind": "node", "path": "README.md", "source": "zrepo"},
            generation_id=gen,
        )["node_id"] == "doc:zrepo:README.md"
    finally:
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_a_record_the_reader_may_not_see_is_never_resolved_or_suggested(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reader's own filter runs before the path is compared: a denied
    record is neither served by its path nor named as a candidate, and a
    path the reader sees masked matches nothing."""
    import okg.substrate.mcp.tools as tools_mod
    from okg.substrate.nomos.read_masking import MASK_TOKEN

    gen = _publish_path_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    real_filter = tools_mod.filter_node_rows_for_session
    try:
        for stub in (
            lambda conn, session, rows, **kw: [],
            lambda conn, session, rows, **kw: [
                {**row, "attrs": {**row["attrs"], "path": MASK_TOKEN}}
                for row in real_filter(conn, session, rows, **kw)
            ],
        ):
            monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", stub)
            for target in (
                {"kind": "node", "path": "lib/rucio/cli/rule.py"},
                {"kind": "text", "path": "lib/rucio/cli/rule.py", "source": "cms"},
                {"kind": "node", "path": "lib/rucio/judge/repairer.py"},
            ):
                with pytest.raises(NodeNotFound) as denied:
                    inspect(target=target, generation_id=gen)
                said = denied.value.message + (denied.value.fix_hint or "")
                assert "close_ids" not in denied.value.data, target
                assert RULE_DOC not in said and REPAIRER_DOC not in said, target
                assert getattr(denied.value, "retry_call", None) is None, target
    finally:
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def test_a_path_target_is_accepted_by_the_served_schema(clean_runtime: str) -> None:
    """Through a real client: the published `node` target takes `path` in
    place of `node_id`, and the reply's first line names the id."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    _publish_path_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")

    async def run() -> None:
        async with create_connected_server_and_client_session(mcp) as client:
            result = await client.call_tool(
                "inspect", {"target": {"kind": "node", "path": "lib/rucio/cli/rule.py"}},
            )
            text = result.content[0].text
            assert not result.isError, text
            head = text.split("\n", 1)[0]
            assert head.startswith("inspect ok") and RULE_DOC in head, head
            refused = await client.call_tool(
                "inspect", {"target": {"kind": "node", "path": "lib/shared.py"}},
            )
            assert refused.isError
            assert SHARED_DOCS[0] in refused.content[0].text

    try:
        anyio.run(run)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


ENGINE_DOC = "doc:app:src/core/Engine/loader.py"
SPACED_DOC = "doc:docs:guide/Getting started.md"
INSTALL_DOC = "doc:docs:guide/Install.md"
MERGE_DOC = "doc:app:tools/merge.py"
STEP_DOCS = ["doc:app:src/stage/runner/step.py", "doc:app:other/step.py"]
REPOSITORY = "repository:team/project"


def _publish_spelling_fixture(dsn: str) -> str:
    """Records a hand-built id or path misspells (#3666): a path in another
    case, a path with a space, a file a `/`-for-`:` id names, a file only
    its name finds, one name in two directories, and a record with no path."""
    def doc(node_id: str, path: str | None) -> NodeFact:
        attrs = {"text": f"contents of {node_id}"}
        if path is not None:
            attrs["path"] = path
        return NodeFact(
            node_id=node_id, subtype="source_file", attrs=attrs,
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                doc(ENGINE_DOC, "src/core/Engine/loader.py"),
                doc(SPACED_DOC, "guide/Getting started.md"),
                doc(INSTALL_DOC, "guide/Install.md"),
                doc(MERGE_DOC, "tools/merge.py"),
                doc(STEP_DOCS[0], "src/stage/runner/step.py"),
                doc(STEP_DOCS[1], "other/step.py"),
                doc(REPOSITORY, None),
            ],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def test_a_misspelled_id_or_path_names_the_record_it_meant(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3666: ids built by hand in another case, with `%20` for a space, a
    `/` for the `:` after the source, a space after the `:`, or under
    another directory got no near id. Each now names the visible record,
    closest path first; nothing is served in its place, a hidden record is
    never named, and a spent read budget says "unavailable"."""
    import okg.substrate.mcp.tools as tools_mod

    gen = _publish_spelling_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    real_filter = tools_mod.filter_node_rows_for_session

    def refusal(target: dict) -> NodeNotFound:
        with pytest.raises(NodeNotFound) as caught:
            inspect(target=target, generation_id=gen)
        return caught.value

    try:
        for guess, first in (
            ("doc:app:src/core/Engine/Loader.py", ENGINE_DOC),  # case
            ("doc:docs:guide/Getting%20started.md", SPACED_DOC),  # %20
            ("doc:docs/guide/Install.md", INSTALL_DOC),  # `/` for `:`
            ("repository: team/project", REPOSITORY),  # space after `:`
            ("doc:app:lib/sync/merge.py", MERGE_DOC),  # file name only
            ("doc:app:runner/Step.py", STEP_DOCS[0]),  # tail, another case
        ):
            err = refusal({"kind": "node", "node_id": guess})
            assert err.data["exists"] is False, guess
            assert err.data["close_id_check"] == "checked", guess
            assert err.data["close_ids"][0] == first, (guess, err.data)
            assert err.fix_hint.startswith(f"retry with one of ['{first}'"), guess
        # One name in two directories: both, closest path first.
        err = refusal({"kind": "node", "node_id": "doc:app:src/stage/step.py"})
        assert err.data["close_ids"] == STEP_DOCS
        # Another prefix with that file name: none named.
        err = refusal({"kind": "node", "node_id": "file:app:lib/sync/merge.py"})
        assert "close_ids" not in err.data
        assert err.data["close_id_check"] == "checked"

        # The path lookup: another case, `%20`, and the file name alone each
        # name one near record as a call to retry, and that call is served.
        for path, meant in (
            ("src/core/engine/Loader.py", ENGINE_DOC),
            ("guide/Getting%20started.md", SPACED_DOC),
            ("lib/sync/merge.py", MERGE_DOC),
        ):
            err = refusal({"kind": "node", "path": path})
            assert err.data["close_ids"] == [meant], (path, err.data)
            retry = err.retry_call
            assert retry["mode"] == "guess"
            assert inspect(**retry["arguments"], generation_id=gen)["node_id"] == meant

        # Hidden from this reader: never named, by id spelling or by path.
        monkeypatch.setattr(
            tools_mod, "filter_node_rows_for_session",
            lambda conn, session, rows, **kw: [],
        )
        for target in (
            {"kind": "node", "node_id": "repository: team/project"},
            {"kind": "node", "node_id": "doc:app:lib/sync/merge.py"},
            {"kind": "node", "path": "lib/sync/merge.py"},
        ):
            err = refusal(target)
            said = err.message + (err.fix_hint or "")
            assert "close_ids" not in err.data, target
            assert REPOSITORY not in said and MERGE_DOC not in said, target
            assert getattr(err, "retry_call", None) is None, target
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)

        # A spent budget: "unavailable", never "checked" with nothing named,
        # and the caller's statement timeout is put back.
        from okg.substrate.mcp import open_session

        monkeypatch.setattr(tools_mod, "_CLOSE_IDS_TIMEOUT_MS", 0)
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = open_session(conn, deployment_name="default")
            conn.execute("SET statement_timeout = 7777")
            assert tools_mod.close_node_ids(
                conn, session, "doc:app:lib/sync/merge.py",
            ) == ([], "unavailable")
            assert conn.execute("SHOW statement_timeout").fetchone()[0] == "7777ms"
    finally:
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


#: More files whose name only ENDS like `loader.py` than one read covers,
#: all before the one `loader.py` in id order; and more `runner.py` files
#: than one close-id read covers.
CROWDING_DOCS = [f"doc:app:a{index:02d}/xloader.py" for index in range(30)]
LOADER_DOC = "doc:app:z/loader.py"
RUNNER_DOCS = [f"doc:app:b{index:02d}/runner.py" for index in range(30)]


def _publish_crowded_fixture(dsn: str) -> str:
    def doc(node_id: str) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype="source_file",
            attrs={"path": node_id.split(":", 2)[2], "text": f"contents of {node_id}"},
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [doc(node_id) for node_id in [*CROWDING_DOCS, LOADER_DOC, *RUNNER_DOCS]],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def test_a_common_file_name_is_matched_on_a_part_boundary_and_a_cut_read_says_so(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review of #3666 fix E: the bounded read is cut before the reader's
    filter and the path check, so `xloader.py` rows must not fill the read
    meant for `loader.py`, and a read cut at its bound must say the near ids
    were not all checked instead of "checked" with nothing named."""
    import okg.substrate.mcp.tools as tools_mod

    gen = _publish_crowded_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    real_filter = tools_mod.filter_node_rows_for_session

    def refusal(target: dict) -> NodeNotFound:
        with pytest.raises(NodeNotFound) as caught:
            inspect(target=target, generation_id=gen)
        return caught.value

    try:
        err = refusal({"kind": "node", "node_id": "doc:app:lib/loader.py"})
        assert err.data["close_ids"] == [LOADER_DOC], err.data
        assert err.data["close_id_check"] == "checked"
        err = refusal({"kind": "node", "path": "lib/loader.py"})
        assert err.data["close_ids"] == [LOADER_DOC], err.data
        assert err.retry_call["arguments"]["target"]["node_id"] == LOADER_DOC

        # 30 `runner.py` files, more than one close-id read: named, and said
        # to be a lower bound.
        err = refusal({"kind": "node", "node_id": "doc:app:lib/runner.py"})
        assert err.data["close_id_check"] == "incomplete"
        assert len(err.data["close_ids"]) == tools_mod.CLOSE_IDS_SHOWN
        assert "not all checked" in err.message
        # All hidden: nothing named, and never "no visible doc: ... has that path".
        monkeypatch.setattr(
            tools_mod, "filter_node_rows_for_session",
            lambda conn, session, rows, **kw: [],
        )
        err = refusal({"kind": "node", "node_id": "doc:app:lib/runner.py"})
        assert err.data["close_id_check"] == "incomplete"
        assert "close_ids" not in err.data
        assert not err.fix_hint.startswith("no visible doc: id has that path")
        assert not any(doc in err.message for doc in RUNNER_DOCS)
    finally:
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


#: Round 2 (#3666), laid out as a crawled site stores its pages: a page id
#: of its own (`<kind>:<hash>`), the page URL in `attrs.url`, an empty
#: `attrs.path`; a wiki page; and a chunk known by its hash.
SITE_SOURCE = "handbook.site"
SITE_URL = "https://docs.example.org/handbook/guide"
SETUP_PAGE = "site_page:3c1d0e5f7a9b2c4d"
INSTALL_PAGE = "site_page:8e2f4a6b0c1d3e5f"
WIKI_PAGE = "wiki:Main:StartHere"
HASHED_CHUNK = "chunk:9f3c2a7b1d0e44"
#: A commit as the git source stores it: `commit:<repo>:<sha>`.
REPO_COMMIT = "commit:app:3f9a2b1c7d5e4f60718293a4b5c6d7e8f9012345"


def _publish_round_two_fixture(dsn: str) -> str:
    def node(node_id: str, subtype: str, attrs: dict) -> NodeFact:
        return NodeFact(
            node_id=node_id, subtype=subtype,
            attrs={"text": f"contents of {node_id}", **attrs},
            source_record_id={"id": node_id}, source_revision={"v": 1},
        )

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["source_file", "site_page"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                node(SETUP_PAGE, "site_page", {
                    "url": f"{SITE_URL}/setup-a-project", "path": "",
                    "title": "Set up a project | Handbook",
                    "site_name": "docs.example.org",
                }),
                node(INSTALL_PAGE, "site_page", {
                    "url": f"{SITE_URL}/install.html", "path": "",
                    "title": "Install | Handbook", "site_name": "docs.example.org",
                }),
                node(HASHED_CHUNK, "source_file", {}),
                node(REPO_COMMIT, "source_file", {}),
                node(WIKI_PAGE, "source_file", {"title": "Start here"}),
            ],
            source=FIXTURE_SOURCE,
        )
    published = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert published["status"] == "published", published
    gen = str(published["generation_id"])
    with psycopg.connect(dsn, autocommit=True) as conn:
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
    return gen


def test_a_page_url_a_wiki_name_or_a_hash_start_names_the_record_it_meant(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#3666 round 2: of 27 not-found replies in the Format v7 runs, 11 named
    no near id. The Format v10 replay of E2 showed three faults, each
    reproduced here for `inspect` node AND text: a source with a "."
    (`doc:<site>.<kind>:`) was read as a file path, so an empty or
    placeholder id got "no visible doc: id has that path"; a page URL was
    looked up by scanning every record's URL, which ran out of time
    ("unavailable"), and that scan also ran for every missing path id; a
    wiki name or hash start must still be named. A page nothing has is
    still "none"; a hidden record is never named; nothing is served in the
    id's place."""
    import okg.substrate.mcp.tools as tools_mod

    gen = _publish_round_two_fixture(clean_runtime)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    inspect = mcp._tool_manager._tools["inspect"].fn
    real_filter = tools_mod.filter_node_rows_for_session

    def refusal(target: dict) -> NodeNotFound:
        with pytest.raises(NodeNotFound) as caught:
            inspect(target=target, generation_id=gen)
        return caught.value

    try:
        for guess, meant in (
            (f"doc:{SITE_SOURCE}:{SITE_URL}/setup-a-project.md", SETUP_PAGE),
            (f"doc:{SITE_SOURCE}:{SITE_URL}/setup-a-project", SETUP_PAGE),
            (f"doc:{SITE_SOURCE}:{SITE_URL}/install.md", INSTALL_PAGE),
            ("doc:notes:StartHere", WIKI_PAGE),
            ("doc:notes:starthere", WIKI_PAGE),
            ("chunk:9f3c2a", HASHED_CHUNK),
            # Review F2: a commit sha without its repo is looked up under
            # every repo (`commit:<repo>:<sha>`).
            ("commit:3f9a2b1c7d", REPO_COMMIT),
        ):
            for kind in ("node", "text"):
                err = refusal({"kind": kind, "node_id": guess})
                assert err.data["exists"] is False, guess
                assert err.data["close_id_check"] == "checked", (kind, guess, err.data)
                assert err.data["close_ids"] == [meant], (kind, guess, err.data)
                assert err.fix_hint.startswith(f"retry with one of ['{meant}']"), guess

        # Missing its part: a specific fix, nothing named, for node and text.
        for guess, needs in (
            (f"doc:{SITE_SOURCE}:", f"the id needs the part after 'doc:{SITE_SOURCE}:'"),
            (f"doc:{SITE_SOURCE}:?", f"the id needs the part after 'doc:{SITE_SOURCE}:'"),
            (f"doc:{SITE_SOURCE}", f"the id needs the part after 'doc:{SITE_SOURCE}:'"),
            ("doc:handbook", "the id needs the part after 'doc:handbook:'"),
            ("chunk:10fe", "'10fe' is too short a hash to look up"),
            # Failure case: a sha no commit has names the repo slot, never
            # a part after the sha.
            ("commit:abcdef0123", "the id needs the repo before the sha"),
        ):
            for kind in ("node", "text"):
                err = refusal({"kind": kind, "node_id": guess})
                assert err.data["close_id_check"] == "id_incomplete", (kind, guess, err.data)
                assert "close_ids" not in err.data
                assert err.fix_hint.startswith(needs), (kind, guess, err.fix_hint)

        # A page or file nothing has: still none, and "checked" (never
        # "unavailable" from a slow read).
        for guess in (
            f"doc:{SITE_SOURCE}:{SITE_URL}/missing.md",
            f"doc:{SITE_SOURCE}:guide/missing.md",
            "doc:app:deploy/dev-jobs.yaml",
            "doc:notes:NoSuchPage",
        ):
            for kind in ("node", "text"):
                err = refusal({"kind": kind, "node_id": guess})
                assert err.data["close_id_check"] == "checked", (kind, guess, err.data)
                assert "close_ids" not in err.data

        # Hidden from this reader: never named.
        monkeypatch.setattr(
            tools_mod, "filter_node_rows_for_session",
            lambda conn, session, rows, **kw: [],
        )
        for guess in (
            f"doc:{SITE_SOURCE}:{SITE_URL}/setup-a-project.md",
            "doc:notes:StartHere",
            "chunk:9f3c2a",
            "commit:3f9a2b1c7d",
        ):
            err = refusal({"kind": "node", "node_id": guess})
            said = err.message + (err.fix_hint or "")
            assert "close_ids" not in err.data, guess
            assert not any(
                found in said for found in (SETUP_PAGE, WIKI_PAGE, HASHED_CHUNK, REPO_COMMIT)
            ), guess
    finally:
        monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", real_filter)
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
