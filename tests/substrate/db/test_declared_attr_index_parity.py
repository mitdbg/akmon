"""okg#3112 step 2, slice 1: generation parity and drift (PACT
lane-h-3112-step2, req.lane-h-3112-step2.parity). Native PostgreSQL.

* Cut-over gate 1 (design.md section 7): the reconciler builds every declared
  index on an empty deployment BEFORE the first publish; the E11 data then
  runs as three real publishes (``apply_facts`` + ``Publisher.cycle()``).
  At every ordinal the rows each index holds equal the graph rows that
  admit the key (G = X, read through the index with the index's own
  predicate), and at the retained ordinals the view equals the graph
  (G = V, ``verify --against-graph``). Failure case: an index built without
  ``deleted = false``.
* Cut-over gate 4: ``okg db attr-index verify --against-graph`` exits 0 on
  the clean run and 1 naming the attribute after one view version row is
  altered; a drifted target outside every publish's change set (what a
  wrongly skipped sync leaves) is missed by the change-set check and found
  by the rotating slice within N publishes, for a node and for an edge (the
  edge drift is reported by the checks; no edge index exists to flag, step 3
  decision 287d). Edge
  ids use all 63 bits (most have 19 digits), and every rotating slice of a
  clean graph reports no difference for edges that carry declared keys.
* An overlay hides a target from the drift check only in the checked
  ordinal's generation and namespace.
* ``verify --against-graph`` does not report clean when it compared
  nothing: it exits 3 with status ``nothing_compared``.
* okg#3112 step 4b (decision 304, D7): plain ``verify`` compared the indexes
  with ``okg.attr_index``, which is gone; it is refused by name with
  ``removed_check`` and a fix hint naming ``--against-graph``, before any
  connection.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.cli.main import main  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402

pytestmark = pytest.mark.postgres


def _three_publishes(dsn: str, identity: Any) -> list[dict[str, Any]]:
    out = []
    for generation in (s.generation_1, s.generation_2, s.generation_3):
        nodes, rels = generation()
        s.apply(dsn, nodes)
        if rels:
            s.apply(dsn, rels)
        out.append(s.publish(dsn, identity))
    return out


_g_equals_x = s.g_equals_x


def _slices_at_every_retained_ordinal(dsn: str, *, n_slices: int) -> list[dict[str, Any]]:
    """For each retained synced ordinal, the full G = V check and the same
    check split into the drift check's id-range slices, per kind."""
    tables = s.physical(dsn)
    out = []
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        declared = dai._declarations(conn)
        retained, _unsynced = dai._retained_ordinals(cur, "default")
        assert retained
        for o in retained:
            common = dict(tables=tables, declared=declared, branch="default",
                          ordinal=int(o["ordinal"]), bp=int(o["bp"]),
                          generation_ids=dai._generation_ids(o["checkpoint_id"]))
            full = dai._check_scope_both_kinds(
                cur, scopes={k: {"kind": "all"} for k in ("node", "edge")}, **common)
            boundaries = {
                k: dai._slice_boundaries(cur, kind=k, physical_table=tables[k],
                                         bp=int(o["bp"]), slices=n_slices)
                for k in ("node", "edge")
            }
            item = {"ordinal": int(o["ordinal"]), "differences": 0, "samples": [],
                    "full_values": {k: full["by_kind"][k]["values_checked"] for k in ("node", "edge")},
                    "slice_values": {"node": 0, "edge": 0}}
            for k in range(n_slices):
                part = dai._check_scope_both_kinds(
                    cur, scopes={kind: dai._slice_scope(boundaries[kind], k)
                                 for kind in ("node", "edge")}, **common)
                item["differences"] += part["differences"]
                item["samples"].extend(part["sample"][:3])
                for kind in ("node", "edge"):
                    item["slice_values"][kind] += part["by_kind"][kind]["values_checked"]
            out.append(item)
    return out


def test_gate1_indexes_built_before_the_first_publish_are_maintained_by_writes(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn)
    built = s.reconcile(dsn)
    assert built["status"] == "converged", built
    # Nodes only: no edge index is declared (okg#3112 step 3, decision 287d).
    assert len(built["built"]) == len(s.DECL)
    assert s.ordinals(dsn) == [], "the indexes must exist before the first publish"

    _three_publishes(dsn, identity)
    ordinals = s.ordinals(dsn)
    assert len(ordinals) == 3

    result = _g_equals_x(dsn)
    assert result["mismatches"] == []
    assert result["plans_without_index"] == []
    # Every (ordinal, kind, attr) pair compared, and the corpus is not trivial.
    assert len(result["pairs"]) == 3 * len(s.DECL)
    assert {k for _o, k, _a, _n in result["pairs"]} == {"node"}
    assert sum(n for *_, n in result["pairs"]) > 1000
    assert all(n > 0 for o, k, a, n in result["pairs"] if a == "t_text")

    # G = V at the ordinals the view retains.
    graph = dai.verify_against_graph(dsn)
    assert graph["ok"], graph
    assert len(graph["ordinals"]) >= 2
    assert all(o["values_checked"] > 0 for o in graph["ordinals"])

    # G = V again, slice by slice, as the rotating drift check reads it: the
    # slices together cover what the full check covers, edges included (19-
    # digit edge ids), and no slice reports a difference on this clean graph.
    sliced = _slices_at_every_retained_ordinal(dsn, n_slices=4)
    for item in sliced:
        assert item["differences"] == 0, item
        for kind in ("node", "edge"):
            assert item["slice_values"][kind] == item["full_values"][kind], item
        assert item["full_values"]["edge"] > 0, "edges must carry declared keys here"

    assert not hasattr(dai, "verify_against_attr_index")


def test_gate1_failure_case_an_index_without_deleted_false_holds_tombstones(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = clean_runtime
    # Since okg#3284 a new delete marker keeps only its key and keep set, so
    # its attrs are NULL and cannot reach this index. The markers that still
    # carry a declared key are the full-copy ones written before that change,
    # which deployments hold until scripts/storage/clear_tombstone_payloads.sql
    # runs. Write these retractions in that legacy shape, explicitly: the
    # upstream Chronos full copy, with OKG's keep sets withheld. The witness
    # below no longer depends on what a new marker carries.
    from chronos_core.branching import _interval_backend as interval_backend
    from okg.substrate.graph_store_internal import schema as store_schema

    # monkeypatch restores OKG's keep sets after the test. Every store
    # construction reapplies them through the schema mixin, so stub that
    # call where the mixin looks it up.
    monkeypatch.setattr(interval_backend, "_TOMBSTONE_KEEP", {})
    monkeypatch.setattr(interval_backend, "_TOMBSTONE_KEEP_DEFAULT", None)
    monkeypatch.setattr(
        store_schema, "configure_tombstone_payload", lambda: None
    )
    identity = s.seed(dsn, {"t_text": "text"})
    tables = s.physical(dsn)
    # The wrong index, built before the first publish like the right one.
    bad = "gate1_no_deleted_predicate"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f"CREATE INDEX {bad} ON public.\"{tables['node']}\" "
            f"({dai.key_expression('t_text', 'text')}, live_lo, live_hi) "
            "WHERE jsonb_typeof(attrs -> 't_text') = 'string'"
        )
    # Tombstones that keep their declared key: a retraction carrying attrs.
    s.apply(dsn, [s.node(i, 1) for i in range(40)])
    s.publish(dsn, identity)
    s.apply(dsn, [NodeFact(s.nid(i), "thing", {"t_text": f"gone{i}"}, {"row": i}, {"rev": 2}, op="R")
                  for i in range(0, 40, 2)])
    s.publish(dsn, identity)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT count(*) AS n FROM public.\"{tables['node']}\" "
            "WHERE deleted AND jsonb_typeof(attrs -> 't_text') = 'string'"
        )
        tombstones_with_key = int(cur.fetchone()["n"])
        cur.execute(
            f"SELECT count(*) AS n FROM public.\"{tables['node']}\" WHERE deleted"
        )
        tombstones = int(cur.fetchone()["n"])
        assert tombstones >= 20
        mismatched = []
        for o in s.ordinals(dsn):
            g = s.graph_set(cur, table=tables["node"], kind="node", attr="t_text",
                            value_type="text", bp=int(o["bp"]))
            x, plan = s.index_set(cur, index_name=bad, table=tables["node"], kind="node",
                                  attr="t_text", value_type="text", bp=int(o["bp"]))
            assert bad in plan
            if g != x:
                mismatched.append(int(o["ordinal"]))
    # The legacy full-copy markers carry the declared key; if they ever stop,
    # this failure case must be rebuilt on another witness.
    assert tombstones_with_key > 0, "no tombstone carries the key; the failure arm is vacuous"
    assert mismatched, "an index without deleted = false must disagree with the graph"


def _cli(*argv: str) -> tuple[int, dict[str, Any]]:
    import contextlib
    import io

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = main(["--json", *argv])
    text = buf.getvalue().strip()
    return code, (json.loads(text) if text else {})


def _cli_error(*argv: str) -> tuple[int, dict[str, Any]]:
    """Run the CLI and return its exit code and the JSON error envelope it
    printed on stderr."""
    import contextlib
    import io

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(["--json", *argv])
    assert out.getvalue().strip() == "", out.getvalue()
    return code, json.loads(err.getvalue().strip())


def _assert_plain_verify_refused(code: int, envelope: dict[str, Any]) -> None:
    assert code == 1, envelope
    error = envelope["error"]
    assert error["code"] == "removed_check", error
    assert error["data"] == {"reason": "removed_check", "check": "verify"}
    assert "--against-graph" in error["fix_hint"]
    assert "okg.attr_index" in error["message"]


def test_plain_verify_is_refused_by_name_before_any_connection() -> None:
    """D7: no DSN and no database at all; the refusal comes first, so it
    cannot be mistaken for a connection error, and it never runs a check."""
    code, envelope = _cli_error(
        "db", "attr-index", "verify", "--dsn", "postgresql://nobody@127.0.0.1:1/none"
    )
    _assert_plain_verify_refused(code, envelope)


def _alter_one_view_row(dsn: str, target_id: str, attr: str, *, kind: str = "node") -> int:
    """Rewrite the declared key in the version row the view uses for
    ``target_id`` at the head ordinal, as a broken derivation would."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT max(ordinal) FROM okg.generation_ordinals WHERE branch_id = 'default'")
        head = int(cur.fetchone()[0])
        cur.execute(
            """
            UPDATE okg.attribute_versions av
               SET attrs = jsonb_set(av.attrs, ARRAY[%s], '"DRIFTED"')
              FROM okg.attribute_effective_view_intervals iv
             WHERE av.target_kind = iv.target_kind AND av.target_id = iv.target_id
               AND av.attrs_hash = iv.attrs_hash
               AND iv.target_kind = %s AND iv.target_id = %s
               AND iv.from_ord <= %s AND %s < iv.to_ord AND av.attrs ? %s
            """,
            (attr, kind, target_id, head, head, attr),
        )
        return cur.rowcount


def test_gate4_verify_against_graph_exits_zero_clean_and_names_the_drifted_attribute(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn)
    s.reconcile(dsn)
    _three_publishes(dsn, identity)

    code, clean = _cli("db", "attr-index", "verify", "--against-graph", "--dsn", dsn)
    assert code == 0, clean
    assert clean["ok"] is True and clean["differences"] == 0 and clean["status"] == "clean"
    _assert_plain_verify_refused(*_cli_error("db", "attr-index", "verify", "--dsn", dsn))

    assert _alter_one_view_row(dsn, s.nid(1), "t_text") >= 1
    code, drift = _cli("db", "attr-index", "verify", "--against-graph", "--dsn", dsn)
    assert code == 1
    assert drift["ok"] is False
    assert drift["attrs_with_differences"] == ["node:t_text"]
    # Recorded: the full check is a metric row and sets the drift flag, and
    # doctor reports an error naming the attribute.
    flagged = {r["attr_id"] for r in s.manifest(dsn).values() if r["drift"]}
    assert flagged == {"t_text"}
    report = dai.doctor_report(dsn)
    assert report["status"] == "ERROR"
    assert any(f["code"] == "view_graph_drift" and f["attr_id"] == "t_text"
               for f in report["findings"])
    code, _ = _cli("doctor", "--check", "declared-attr-indexes", "--dsn", dsn)
    assert code == 1


def test_gate4_a_target_outside_every_change_set_is_found_by_the_rotating_slice(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wrongly skipped sync copies a target's old view row forward, so the
    target is in no publish's change set. The change-set check misses it;
    the rotating slice finds it within N publishes (N = 4 here)."""
    n_slices = 4
    monkeypatch.setenv(dai.DRIFT_SLICES_ENV, str(n_slices))
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.reconcile(dsn)
    s.apply(dsn, [s.node(i, 1) for i in range(200)])
    s.publish(dsn, identity)
    first = dai.run_drift_checks(dsn)
    assert first["status"] == "checked" and first["differences"] == 0, first

    # Drift on a target no later publish touches (node 150), with a graph
    # value present. Later publishes change only nodes 0-9.
    victim = s.nid(150)
    assert _alter_one_view_row(dsn, victim, "t_text") >= 1
    found_after = None
    dirty_saw_it = False
    for publish_no in range(1, n_slices + 1):
        s.apply(dsn, [s.node(i, 10 + publish_no) for i in range(10)])
        s.publish(dsn, identity)
        report = dai.run_drift_checks(dsn)
        assert report["status"] == "checked", report
        assert report["dirty"]["targets"]["node"] >= 10
        if report["dirty"]["differences"]:
            dirty_saw_it = True
        if any(c["differences"] for c in report["checks"]):
            found_after = publish_no
            sample = [c for c in report["checks"] if c["differences"]][0]["sample"]
            assert any(item["target_id"] == victim for item in sample)
            break
    assert not dirty_saw_it, "the victim is in no change set; the dirty check must not see it"
    assert found_after is not None and found_after <= n_slices
    assert {r["attr_id"] for r in s.manifest(dsn).values() if r["drift"]} == {"t_text"}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT check_kind, differences FROM okg.declared_attr_index_drift_checks "
            "WHERE differences > 0"
        )
        kinds = {r["check_kind"] for r in cur.fetchall()}
    assert kinds == {"slice"}


def test_drift_slices_stop_at_a_preemption_request_and_resume_next_run(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A run that owes several slices checks for a migrate's preemption
    request before each one. It stops after the slices already done, keeps
    the cursor, does not mark the ordinal checked, and the next run checks
    the slices still owed."""
    n_slices = 4
    monkeypatch.setenv(dai.DRIFT_SLICES_ENV, str(n_slices))
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.reconcile(dsn)
    s.apply(dsn, [s.node(i, 1) for i in range(200)])
    s.publish(dsn, identity)
    assert dai.run_drift_checks(dsn)["status"] == "checked"
    for publish_no in range(1, 4):
        s.apply(dsn, [s.node(i, 10 + publish_no) for i in range(10)])
        s.publish(dsn, identity)

    calls = {"n": 0}
    real = dai.preempt_requested

    def requested_after_one_slice(cur):
        calls["n"] += 1
        # Call 1 is the run's entry check, call 2 the first slice's.
        return calls["n"] >= 3 or real(cur)

    monkeypatch.setattr(dai, "preempt_requested", requested_after_one_slice)
    stopped = dai.run_drift_checks(dsn)
    assert stopped["status"] == "stopped" and stopped["reason"] == "preempt_requested", stopped
    assert len(stopped["checks"]) == 1, stopped
    first_slice = stopped["checks"][0]["slice"]

    monkeypatch.setattr(dai, "preempt_requested", real)
    resumed = dai.run_drift_checks(dsn)
    assert resumed["status"] == "checked", resumed
    assert resumed["checks"] and resumed["checks"][0]["slice"] == (first_slice + 1) % n_slices
    assert dai.run_drift_checks(dsn)["status"] == "up_to_date"


def test_gate4_failure_case_without_the_slice_the_skipped_target_stays_hidden(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure arm: with the rotating slice turned into a no-op (one slice
    that covers nothing new because its range is empty), only the change-set
    check runs, and the drifted target is never reported. Shows the slice is
    what finds it."""
    monkeypatch.setenv(dai.DRIFT_SLICES_ENV, "4")
    monkeypatch.setattr(dai, "_slice_scope", lambda boundaries, k: {"kind": "ids", "ids": []})
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.reconcile(dsn)
    s.apply(dsn, [s.node(i, 1) for i in range(200)])
    s.publish(dsn, identity)
    dai.run_drift_checks(dsn)
    assert _alter_one_view_row(dsn, s.nid(150), "t_text") >= 1
    for publish_no in range(1, 5):
        s.apply(dsn, [s.node(i, 10 + publish_no) for i in range(10)])
        s.publish(dsn, identity)
        report = dai.run_drift_checks(dsn)
        assert report["differences"] == 0, report
    # The full check still sees it.
    assert dai.verify_against_graph(dsn)["attrs_with_differences"] == ["node:t_text"]


def test_gate4_edges_with_declared_keys_are_clean_in_every_slice_and_a_drifted_edge_is_found(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Edge ids use all 63 bits, so most have 19 digits. On a clean graph
    every rotating slice reports no difference for edges carrying a declared
    key; a drifted edge no publish touches is found by the slice within N
    publishes, like a node."""
    n_slices = 4
    monkeypatch.setenv(dai.DRIFT_SLICES_ENV, str(n_slices))
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.reconcile(dsn)
    s.apply(dsn, [s.node(i, 1) for i in range(201)])
    s.apply(dsn, s.edges(range(200), 1))
    s.publish(dsn, identity)
    tables = s.physical(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*), count(*) FILTER (WHERE length(edge_id::text) = 19) "
            f"FROM public.\"{tables['edge']}\" WHERE deleted = false AND attrs ? 't_text'"
        )
        with_key, nineteen_digits = cur.fetchone()
    assert with_key >= 100 and nineteen_digits > with_key // 2, (with_key, nineteen_digits)
    first = dai.run_drift_checks(dsn)
    assert first["status"] == "checked" and first["differences"] == 0, first

    # A full rotation on the clean graph: no slice reports a difference.
    for publish_no in range(1, n_slices + 1):
        s.apply(dsn, [s.node(i, 10 + publish_no) for i in range(5)])
        s.publish(dsn, identity)
        report = dai.run_drift_checks(dsn)
        assert report["status"] == "checked", report
        assert report["differences"] == 0, report

    # Drift on an edge no later publish touches (between nodes 151 and 152;
    # s.edges gives it a t_text value).
    from okg.substrate.graph_store_internal.common import edge_id

    victim = str(edge_id(s.nid(151), "rel", s.nid(152)))
    assert _alter_one_view_row(dsn, victim, "t_text", kind="edge") >= 1
    found_after = None
    for publish_no in range(1, n_slices + 1):
        s.apply(dsn, [s.node(i, 20 + publish_no) for i in range(5)])
        s.publish(dsn, identity)
        report = dai.run_drift_checks(dsn)
        assert report["dirty"]["differences"] == 0, report["dirty"]
        if any(c["differences"] for c in report["checks"]):
            found_after = publish_no
            hits = [c for c in report["checks"] if c["differences"]][0]
            assert hits["by_attr"] == {"edge:t_text": 2}, hits
            assert {item["target_id"] for item in hits["sample"]} == {victim}
            break
    assert found_after is not None and found_after <= n_slices
    # okg#3112 step 3, decision 287d: edges get no declared index, so the
    # edge drift has no manifest row to flag; it is reported by the checks
    # (above) and by the full verify, and no node index is flagged for it.
    manifest = s.manifest(dsn)
    assert {r["target_kind"] for r in manifest.values()} == {"node"}
    assert not any(r["drift"] for r in manifest.values())
    full = dai.verify_against_graph(dsn, record=False)
    assert full["status"] == "drift" and full["attrs_with_differences"] == ["edge:t_text"], full


def _overlay(dsn: str, *, target_id: str, generation_id: str, namespace: str = "default") -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DELETE FROM okg.attribute_effective_attrs")
        conn.execute(
            """
            INSERT INTO okg.attribute_effective_attrs
              (target_kind, target_id, generation_id, namespace, attr_key, observation_id,
               value, value_hash, producer)
            VALUES ('node', %s, %s, %s, 't_text', 'obs-test', '"overlaid"', 'h', 'test')
            """,
            (target_id, generation_id, namespace),
        )


def test_an_overlay_hides_drift_only_in_its_own_generation_and_namespace(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.apply(dsn, [s.node(i, 1) for i in range(40)])
    s.publish(dsn, identity)
    head = s.ordinals(dsn)[-1]
    victim = s.nid(7)
    assert _alter_one_view_row(dsn, victim, "t_text") >= 1
    assert dai.verify_against_graph(dsn, record=False)["attrs_with_differences"] == ["node:t_text"]

    # An overlay of another generation, or of another namespace, explains
    # nothing at this ordinal: the drift stays visible.
    _overlay(dsn, target_id=victim, generation_id="generation:0")
    assert dai.verify_against_graph(dsn, record=False)["attrs_with_differences"] == ["node:t_text"]
    _overlay(dsn, target_id=victim, generation_id=str(head["checkpoint_id"]), namespace="elsewhere")
    assert dai.verify_against_graph(dsn, record=False)["attrs_with_differences"] == ["node:t_text"]
    # An overlay in this ordinal's generation and namespace is why the view
    # differs from the graph: left out.
    _overlay(dsn, target_id=victim, generation_id=str(head["checkpoint_id"]))
    clean = dai.verify_against_graph(dsn, record=False)
    assert clean["status"] == "clean", clean


def test_verify_against_graph_does_not_report_clean_when_it_compared_nothing(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn, {"t_text": "text"})
    s.apply(dsn, [s.node(i, 1) for i in range(20)])
    # Indexes built, nothing published: no ordinal to compare at.
    assert s.reconcile(dsn)["status"] == "converged"
    code, graph = _cli("db", "attr-index", "verify", "--against-graph", "--dsn", dsn)
    assert code == 3, graph
    assert graph["status"] == "nothing_compared" and graph["ok"] is False
    assert graph["reason"] == "no_synced_retained_ordinal"
    # Plain verify is refused, not reported as nothing compared.
    _assert_plain_verify_refused(*_cli_error("db", "attr-index", "verify", "--dsn", dsn))
    # Once there is something to compare, it is clean and exits 0.
    s.publish(dsn, identity)
    assert _cli("db", "attr-index", "verify", "--against-graph", "--dsn", dsn)[0] == 0
