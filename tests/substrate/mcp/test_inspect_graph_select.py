"""bound-inspect-graph-select.

`inspect(target={"kind": "graph"})` accepted a `select` list and ignored it on
the graph branch, so a caller asking for two count blocks still paid for four
whole-graph scans. Measured on live okg-workspace at the published pin, the four
blocks cost 1992 / 2413 / 2435 / 104 ms; reading the two count blocks from the
maintained `okg.console_count_rollup` took 72 ms.

Two behaviours are proved here:

  (a) `select` narrows the WORK -- an unselected block does not run its query.
      Asserted on the statements actually issued, captured at the `graph.query`
      seam BEFORE the Chronos rewrite. After the rewrite the literal table names
      are gone (replaced by `_chronos_b_interval_*` subqueries), so a
      post-rewrite substring check for them could never fail and would pass
      vacuously. A payload-shape assertion would be just as weak: it would pass
      if the query ran and its result were discarded, which is the bug.

  (b) the two count blocks come from the maintained rollup, and when they cannot
      the payload SAYS SO via `counts_source` / `counts_source_reason` rather
      than degrading silently.
"""

from __future__ import annotations

import dataclasses
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
psycopg_conninfo = pytest.importorskip("psycopg.conninfo")

from okg.substrate.mcp.envelope import SelectUnknownField  # noqa: E402
from okg.substrate.mcp.session import open_session  # noqa: E402
# Imported as a MODULE, not `from ...tools import inspect_graph`: `_inspect_capturing`
# monkeypatches `_chronos_checkout` on it, and importing the same module both ways
# trips CodeQL's py/import-and-import-from.
from okg.substrate.mcp import tools as tools_mod  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


_ALL_KEYS = {
    "subtype_counts",
    "edge_type_counts",
    "embed_lag",
    "unresolved_mentions",
    "schema",
}


def _seed(clean_runtime: str) -> str:
    """Publish a graph with two node subtypes, one edge type, and mentions.

    A REAL publish cycle, not a bare `checkpoint_generation`: the count rollup is
    maintained by the publisher, so a checkpoint-only fixture would leave it
    unmaintained and every rollup assertion below would skip -- proving nothing
    about the path this change exists to add.
    """
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import seed_catalog

    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["entity_mention", "open_spec_change"],
            narrowings=[("entity_mention", "mentions", "open_spec_change")],
        )
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(
                f"em:{i}:{suffix}", "entity_mention",
                {"entity_type": "change_slug"},
                {"id": f"em:{i}:{suffix}"}, {"v": 1},
            )
            for i in range(3)
        ] + [
            NodeFact(
                f"os:{i}:{suffix}", "open_spec_change", {"slug": f"s{i}"},
                {"id": f"os:{i}:{suffix}"}, {"v": 1},
            )
            for i in range(2)
        ] + [
            EdgeFact(
                src=f"em:0:{suffix}",
                dst=f"os:0:{suffix}",
                edge_type="mentions",
                source_record_id={"id": f"e:0:{suffix}"},
                source_revision={"v": 1},
            ),
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


class _CapturingGraph:
    """Wrap a chronos checkout and record the SQL it is asked to run.

    This is the pre-rewrite seam: `graph.query(sql, params)` is what
    `_graph_summary_sql` calls, and the Chronos rewrite happens inside it.
    """

    def __init__(self, inner) -> None:
        self._inner = inner
        self.statements: list[str] = []

    def query(self, sql, params=None, *a, **kw):
        self.statements.append(str(sql))
        return self._inner.query(sql, params, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _inspect_capturing(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, select=None
):
    """Run inspect_graph, returning (payload, statements issued at the seam)."""
    captured: dict[str, _CapturingGraph] = {}
    real_checkout = tools_mod._chronos_checkout

    def wrapper(conn, session):
        graph = _CapturingGraph(real_checkout(conn, session))
        captured["graph"] = graph
        return graph

    monkeypatch.setattr(tools_mod, "_chronos_checkout", wrapper)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        payload = tools_mod.inspect_graph(conn, session, select)
    return payload, captured["graph"].statements


# --------------------------------------------------------------------------- #
# (a) select narrows the work
# --------------------------------------------------------------------------- #


def test_two_field_select_issues_neither_unrequested_scan(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The reported failing request must not pay for the two blocks it omits.

    `embedding IS NULL` and the `entity_mention` group-by are the two blocks a
    counts-only caller never asked for; together they were ~2.5s of the ~6.9s
    measured on the live deployment.
    """
    _seed(clean_runtime)
    payload, statements = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    joined = "\n".join(statements)
    assert "embedding IS NULL" not in joined, joined
    assert "entity_mention" not in joined, joined
    assert "subtype_counts" in payload
    assert "edge_type_counts" in payload


def test_omitting_select_returns_the_full_payload(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The compatibility case: every existing caller passes no `select`.

    They must see exactly what they saw before, including the two blocks a
    narrowed call now skips.
    """
    _seed(clean_runtime)
    payload, _ = _inspect_capturing(clean_runtime, monkeypatch, select=None)
    for key in _ALL_KEYS:
        assert key in payload, (key, sorted(payload))
    assert "node_count" in payload and "edge_count" in payload
    assert "subtype_count" in payload and "edge_type_count" in payload


def test_unknown_select_entry_is_refused_not_ignored(
    clean_runtime: str,
) -> None:
    """A misspelled block must not silently widen back to the full slow answer."""
    _seed(clean_runtime)
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        with pytest.raises(SelectUnknownField) as excinfo:
            tools_mod.inspect_graph(conn, session, ["subtypes", "subtpyes"])
    assert "subtpyes" in excinfo.value.data["unknown_fields"]
    assert set(excinfo.value.data["valid_fields"]) == {
        "subtypes", "edge_types", "embed_lag", "unresolved_mentions",
    }


def test_unselected_blocks_are_absent_rather_than_null(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A null would be indistinguishable from a block that ran and found nothing.

    The identity, pin and counts-provenance keys stay present regardless.
    """
    _seed(clean_runtime)
    payload, _ = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes"]
    )
    for absent in (
        "embed_lag", "unresolved_mentions", "edge_count", "edge_type_counts",
        "edge_type_count", "schema",
    ):
        assert absent not in payload, (absent, sorted(payload))
    for present in (
        "subtype_counts", "node_count", "generation_id",
        "pinned_generation_id", "counts_source",
    ):
        assert present in payload, (present, sorted(payload))


def test_schema_is_omitted_rather_than_built_from_partial_counts(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The digest is built from BOTH count maps and coerces a missing one to {}.

    Built from one, it would report `edge_type_count: 0` and `edge_types: []` --
    telling a caller that selected only `subtypes` that the graph has no edge
    types at all. Omit it instead of publishing a zero for a block that did not
    run.
    """
    _seed(clean_runtime)
    for select in (["subtypes"], ["edge_types"]):
        payload, _ = _inspect_capturing(clean_runtime, monkeypatch, select=select)
        assert "schema" not in payload, (select, sorted(payload))
    both, _ = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    assert both["schema"]["subtype_count"] > 0
    assert both["schema"]["edge_type_count"] > 0


# --------------------------------------------------------------------------- #
# (b) counts come from the rollup, and say where they came from
# --------------------------------------------------------------------------- #


def test_counts_come_from_the_rollup_without_a_graph_group_by(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the rollup serves the pin, no whole-graph GROUP BY is issued."""
    _seed(clean_runtime)
    payload, statements = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    # NOT a skip. A skip here would make the headline requirement unfalsifiable:
    # if the rollup path ever stopped engaging -- a renamed dimension kind, a
    # mis-firing guard -- this test would quietly skip and CI would stay green
    # while the change did nothing.
    assert payload["counts_source"] == "console_count_rollup", (
        payload.get("counts_source_reason"),
        payload["counts_source"],
    )
    joined = "\n".join(statements)
    assert "GROUP BY subtype" not in joined, joined
    assert "GROUP BY edge_type" not in joined, joined


def test_rollup_counts_equal_the_aggregate(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ROLLUP's counts equal the whole-graph aggregate at the same pin.

    Equality as MAPS, including the absence of a key whose count is zero.

    The `counts_source` assertion is load-bearing: without it this test compares
    the fallback scan against the scan -- one query equal to itself -- and would
    pass with the rollup path entirely dead.
    """
    _seed(clean_runtime)
    payload, _ = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    assert payload["counts_source"] == "console_count_rollup", (
        payload.get("counts_source_reason"),
    )
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        graph = tools_mod._chronos_checkout(conn, session)
        expected = tools_mod._graph_summary_sql(
            graph, frozenset({"subtypes", "edge_types"})
        )
    assert payload["subtype_counts"] == expected["subtype_counts"]
    assert payload["edge_type_counts"] == expected["edge_type_counts"]


def test_missing_rollup_reports_graph_scan_with_a_reason(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    """An unavailable rollup degrades LOUDLY: declared in the payload and logged.

    A degraded answer that does not announce itself is the defect class this
    repo treats as a P0, so the reason travels with the response.
    """
    _seed(clean_runtime)
    from okg.substrate.console_views import console_count_rollup_read as rollup_mod

    def boom(*a, **kw):
        raise rollup_mod.ConsoleCountRollupUnavailable(
            "gen", "no_sentinel", "forced by test"
        )

    monkeypatch.setattr(rollup_mod, "read_console_count_rollup", boom)
    with caplog.at_level("WARNING"):
        payload, _ = _inspect_capturing(
            clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
        )
    assert payload["counts_source"] == "graph_scan"
    assert payload["counts_source_reason"] == "no_sentinel"
    # And the counts are still correct -- degraded means slower, never wrong.
    assert payload["subtype_counts"]
    assert any("fell back to a whole-graph scan" in r.message for r in caplog.records)


def test_disabled_flag_selects_the_scan_not_leftover_rows(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The documented emergency rollback must actually stop these reads.

    `read_console_count_rollup` does not check the flag itself, so without an
    explicit `require_console_count_rollup_enabled` call this caller would keep
    serving rows left over from earlier generations after an operator set the
    flag to 0.
    """
    _seed(clean_runtime)
    monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "0")
    payload, _ = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    assert payload["counts_source"] == "graph_scan"
    assert payload["counts_source_reason"] == "count_rollup_disabled"
    assert payload["subtype_counts"]


def test_an_unrelated_rollup_error_is_not_swallowed(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only ConsoleCountRollupUnavailable selects the scan.

    Catching broadly would turn a real database fault into a silent multi-second
    scan -- a fail-loud read quietly becoming fail-open.
    """
    _seed(clean_runtime)
    from okg.substrate.console_views import console_count_rollup_read as rollup_mod

    def boom(*a, **kw):
        raise RuntimeError("connection reset")

    monkeypatch.setattr(rollup_mod, "read_console_count_rollup", boom)
    with pytest.raises(RuntimeError, match="connection reset"):
        _inspect_capturing(
            clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
        )


# --------------------------------------------------------------------------- #
# (c) the three guards the first round of this change shipped untested
# --------------------------------------------------------------------------- #


def test_a_non_default_branch_refuses_the_rollup(clean_runtime: str) -> None:
    """Off the default branch the rollup must not answer at all.

    `read_console_count_rollup` checks out with NO branch argument, i.e. always
    the default branch, while the scan it replaces is branch-correct through
    `_chronos_checkout(conn, session)`. Serving the rollup on a fork branch would
    report ANOTHER branch's numbers under an authoritative label -- wrong, not
    merely stale, and invisible to the caller.

    Asserted against `_graph_counts_from_rollup` directly rather than through
    `inspect_graph`, because reaching the guard through the tool would first need
    a real published generation on a real fork branch; the guard is a property of
    this function and is checked where it lives. The default-branch call is
    asserted too, so a guard that refused EVERYTHING would fail this test rather
    than pass it.
    """
    _seed(clean_runtime)
    from okg.substrate.graph_store import DEFAULT_BRANCH

    blocks = frozenset({"subtypes", "edge_types"})
    with psycopg.connect(clean_runtime) as conn:
        session = open_session(conn, deployment_name="default")
        assert session.branch_name == DEFAULT_BRANCH, session.branch_name
        _, default_reason = tools_mod._graph_counts_from_rollup(
            conn, session, blocks
        )
        assert default_reason != "non_default_branch", default_reason

        forked = dataclasses.replace(session, branch_name="fork/experiment")
        counts, reason = tools_mod._graph_counts_from_rollup(
            conn, forked, blocks
        )
    assert reason == "non_default_branch", reason
    assert counts == {}, counts


def test_the_mcp_role_can_read_the_rollup(
    clean_runtime: str, okg_mcp_can_log_in: str
) -> None:
    """The read must work as `okg_mcp`, not only as the substrate owner.

    Every other rollup test in this repo runs as the owner. The MCP server reads
    on the constrained `okg_mcp` role, and `okg.console_count_rollup` is not in
    that role's allow-list in db/views.sql -- it is reached through the Chronos
    rewrite, which redirects the logical name to interval storage the role IS
    granted. If that ever stops holding, the symptom is a PERMANENT silent
    degradation to the slow scan reported as `table_unregistered` -- a reason
    that sends the next reader looking at registration, not permissions.
    """
    gen = _seed(clean_runtime)
    from okg.substrate.console_views.console_count_rollup_read import (
        read_console_count_rollup,
    )
    from okg.substrate.local_database_roles import LOCAL_DATABASE_ROLE_PASSWORDS

    role_dsn = psycopg_conninfo.make_conninfo(
        clean_runtime,
        user="okg_mcp",
        password=LOCAL_DATABASE_ROLE_PASSWORDS["okg_mcp"],
    )
    with psycopg.connect(role_dsn) as conn:
        counts = read_console_count_rollup(conn, generation_id=gen)
    kinds = {kind for (kind, _value) in counts}
    assert "subtype" in kinds, sorted(kinds)
    assert "edge_type" in kinds, sorted(kinds)


def test_enforce_mode_keeps_the_per_row_path_and_reports_graph_scan(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Under Nomos `enforce` the rollup must not be consulted at all.

    The rollup carries no source lineage (the per-row rule deciding which rows a
    caller may see), so it cannot answer a visibility-filtered question. The
    per-row path stays authoritative and the payload says the counts came from a
    scan, with `nomos_enforce` as the reason -- so a caller cannot read
    `console_count_rollup` on an enforce deployment and conclude the numbers were
    lineage-filtered. The rollup reader is replaced with a raiser, so "was not
    consulted" is enforced rather than inferred from the reason string.
    """
    _seed(clean_runtime)

    def always_enforce(_deployment: str) -> str:
        return "enforce"

    monkeypatch.setattr(
        tools_mod, "deployment_nomos_enforcement_mode", always_enforce
    )

    def explode(*a, **kw):
        raise AssertionError("enforce mode must not consult the rollup")

    monkeypatch.setattr(tools_mod, "_graph_counts_from_rollup", explode)

    payload, _ = _inspect_capturing(
        clean_runtime, monkeypatch, select=["subtypes", "edge_types"]
    )
    assert payload["counts_source"] == "graph_scan"
    assert payload["counts_source_reason"] == "nomos_enforce"
    assert payload["visibility"] == "enforced"
    # The counts are still real, not omitted -- this fixture is under the
    # exact-count budget, so the per-row path runs.
    assert payload["subtype_counts"]
