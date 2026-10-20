"""How the attempt-detail read composes, and what it refuses to serve.

PACT operator-console-live-updates. These run WITHOUT a database, so the fast
lane selects them: the shapes here are decided by the composer, not by
Postgres, and each one has a way to be wrong that the Postgres file could not
show cheaply -- an absent key rendered as a zero, a parent version that could
not be read rendered as a parent count of zero, a gate blob passed through, a
psycopg message forwarded as a reason.

The live measurements these assertions are anchored on were taken from the
published ``lane-d-home-render`` deployment on 2026-09-21: one consistency
gate per attempt, whose ``details`` and ``preflight`` are 146,414 and 113,924
bytes, and whose ``check_id`` is 82 characters -- two over the allowlist's
80-character code cap, so it is WITHHELD and not served as an identifier.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from chronos_core.branching import BranchNotFoundError

from okg.substrate.console_views import attempt_detail
from okg.substrate.console_views.attempt_detail import (
    ROLLUP_ROWS_PER_SECTION,
    AttemptNotFound,
    MalformedAttemptId,
    compose_attempt_detail,
)
from okg.substrate.console_views import console_count_rollup_read
from okg.substrate.console_views.console_count_rollup_read import (
    ConsoleCountRollupUnavailable,
)
from okg.substrate.mcp.session import Session

DEPLOYMENT = "lane-d"
BRANCH = "default"


def _session(generation_id: str) -> Session:
    return Session(
        deployment_name=DEPLOYMENT,
        generation_id=generation_id,
        catalog_version_id=4,
        db_fingerprint="fingerprint",
        branch_name=BRANCH,
    )


def _checkpoint(checkpoint_id: str, **metadata: Any) -> SimpleNamespace:
    base = {
        # The scope keys every real checkpoint carries and both of the
        # composer's reads filter on -- the window read in the store, the
        # by-id fallback in the composer. Without them here the fallback
        # correctly refuses a checkpoint that does not say whose it is.
        "kind": "okg_generation",
        "deployment_name": DEPLOYMENT,
        "status": "published",
        "target_generation_id": abs(hash(checkpoint_id)) % 10**12,
    }
    base.update(metadata)
    return SimpleNamespace(
        checkpoint_id=checkpoint_id,
        created_at="2026-09-21T02:48:10+00:00",
        metadata=base,
    )


def _install(
    monkeypatch: pytest.MonkeyPatch,
    *,
    ascending: list[SimpleNamespace],
    rollups: dict[str, Any] | None = None,
    counts: dict[str, tuple[int, int]] | None = None,
) -> list[str]:
    """Stand the composer's four collaborators up in memory.

    Returns the list every rollup read appends its generation id to, so a test
    can assert the read happens ONCE per version and never once per row.
    """
    rollups = rollups or {}
    counts = counts or {}
    asked: list[str] = []

    class _Store:
        """The bounded reader's slice, behaving as the real one does.

        Newest first, projected to the keys it was asked for, and honouring
        the scan window: a composer that reads an unprojected key sees None
        here too, and one that mistakes a short window for a whole history is
        caught here rather than on a deployment.
        """

        @staticmethod
        def from_connection(conn: Any, ensure_schema: bool = True) -> "_Store":
            return _Store()

        def recent_generations(self, **kwargs: Any):
            from okg.substrate.graph_store_internal.generations import (
                GenerationRow,
                RecentGenerations,
                _generation_projection,
            )

            assert kwargs["deployment_name"] == DEPLOYMENT
            assert kwargs["branch"] == BRANCH
            projected = _generation_projection(kwargs.get("keys"))
            window = kwargs.get("scan_window") or 200
            scanned = list(reversed(ascending))[:window]
            rows = [
                GenerationRow(
                    row.checkpoint_id, BRANCH, 0, row.created_at,
                    {key: (row.metadata or {}).get(key) for key in projected},
                )
                for row in scanned
            ]
            return RecentGenerations(
                rows,
                scanned=len(scanned),
                scan_window=window,
                window_exhausted=len(scanned) >= window,
            )

        def count_generation_checkpoints(self, **kwargs: Any) -> int:
            assert kwargs["deployment_name"] == DEPLOYMENT
            assert kwargs["branch"] == BRANCH
            return len(ascending)

        def get_generation(self, generation_id: Any) -> SimpleNamespace | None:
            """One checkpoint by id, as the store's own exact read answers.

            The fallback the composer takes for an attempt older than its
            window. It answers from the SAME history, so a test that puts an
            attempt out of the window still gets the real row back.
            """
            for row in ascending:
                if row.checkpoint_id == str(generation_id):
                    return SimpleNamespace(
                        checkpoint_id=row.checkpoint_id,
                        branch_id=BRANCH,
                        created_at=row.created_at,
                        metadata=dict(row.metadata or {}),
                    )
            raise BranchNotFoundError(f"checkpoint:{generation_id}")

    def _rollup(conn: Any, *, generation_id: Any, kinds: Any = None) -> Any:
        asked.append(str(generation_id))
        answer = rollups.get(str(generation_id))
        if isinstance(answer, Exception):
            raise answer
        if answer is None:
            raise ConsoleCountRollupUnavailable(
                generation_id, "checkpoint_absent", "no such checkpoint",
            )
        return answer

    monkeypatch.setattr(attempt_detail, "ChronosGraphStore", _Store)
    monkeypatch.setattr(attempt_detail, "read_console_count_rollup", _rollup)
    monkeypatch.setattr(
        attempt_detail, "_view_runs_by_target", lambda conn, **kwargs: {},
    )
    # The per-source runs window is its own statement; its own tests are in
    # test_console_wording_reads.py. Here it answers the empty window.
    monkeypatch.setattr(
        attempt_detail, "_source_writes",
        lambda conn, **kwargs: {"rows": [], "window": {}, "basis": "test",
                                "unavailable": None},
    )
    monkeypatch.setattr(
        attempt_detail,
        "generation_counts",
        lambda conn, session: counts.get(session.generation_id, (0, 0)),
    )
    return asked


# --------------------------------------------------------------------------- #
# which ids this read will look up at all                                      #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "bad",
    [
        "",
        "postgres://okg:pw@db.example:5432/okg",
        "/Users/example/okg",
        "okg:lane-d:branch:default:gen:x' OR 1=1--",
        "okg:lane-d:branch:default:gen:" + "x" * 600,
    ],
)
def test_an_id_no_checkpoint_could_have_is_refused_as_malformed(
    bad: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DSN, a path, a quote or an over-long string never reaches a query."""
    _install(monkeypatch, ascending=[_checkpoint("okg:lane-d:gen:one")])
    with pytest.raises(MalformedAttemptId):
        compose_attempt_detail(
            object(), deployment_name=DEPLOYMENT,
            session=_session("okg:lane-d:gen:one"), attempt_id=bad,
        )


def test_an_id_this_branch_does_not_hold_is_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Well-shaped, absent from the scoped history: not found, not malformed.

    The scope is the composer's own bounded history read, which is already
    filtered to this deployment and this branch, so an id from ANOTHER
    deployment travels the identical path to the identical exception.
    """
    _install(monkeypatch, ascending=[_checkpoint("okg:lane-d:gen:one")])
    for absent in ("okg:lane-d:gen:missing", "okg:other-deployment:gen:one"):
        with pytest.raises(AttemptNotFound):
            compose_attempt_detail(
                object(), deployment_name=DEPLOYMENT,
                session=_session("okg:lane-d:gen:one"), attempt_id=absent,
            )


# --------------------------------------------------------------------------- #
# what the version holds                                                       #
# --------------------------------------------------------------------------- #
def test_holds_serves_updates_the_list_delta_cannot_see(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A version that rewrote rows without changing size is not "no change".

    The list's delta is two consecutive TOTALS subtracted, so an attempt that
    updated 40 nodes and added none reads as zero there. ``projection_deltas``
    records the update and this section serves it.
    """
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one",
        node_count=1723, edge_count=3006,
        projection_deltas={
            "nodes_added": 0, "nodes_updated": 40, "nodes_retracted": 0,
            "edges_added": 0, "edges_updated": 7, "edges_retracted": 12,
        },
    )])
    holds = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["holds"]
    assert holds["node_count"] == 1723
    assert holds["edge_count"] == 3006
    assert holds["nodes"] == {"added": 0, "updated": 40, "retracted": 0}
    assert holds["edges"] == {"added": 0, "updated": 7, "retracted": 12}
    assert holds["unavailable"] == []


def test_an_unrecorded_hold_is_a_named_marker_and_never_a_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Absent counts say so. Zero is a measured fact and must not be invented."""
    _install(monkeypatch, ascending=[_checkpoint("okg:lane-d:gen:one")])
    holds = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["holds"]
    assert holds["node_count"] is None
    assert holds["edge_count"] is None
    assert holds["nodes"] is None and holds["edges"] is None
    reasons = {marker["reason"] for marker in holds["unavailable"]}
    assert reasons == {
        "node_count_not_recorded",
        "edge_count_not_recorded",
        "projection_deltas_not_recorded",
    }


def test_a_partially_recorded_delta_names_the_missing_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A checkpoint predating `*_updated` must not read as zero updates."""
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one",
        projection_deltas={"nodes_added": 5, "edges_added": 9},
    )])
    holds = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["holds"]
    assert holds["nodes"] == {"added": 5, "updated": None, "retracted": None}
    missing = [m for m in holds["unavailable"]
               if m["reason"] == "projection_delta_field_not_recorded"]
    assert {m["scope"] for m in missing} == {"nodes", "edges"}
    assert missing[0]["fields"] == ["retracted", "updated"]


# --------------------------------------------------------------------------- #
# the counts, against the parent version                                       #
# --------------------------------------------------------------------------- #
def _two_attempt_history() -> list[SimpleNamespace]:
    return [
        _checkpoint("okg:lane-d:gen:one"),
        _checkpoint("okg:lane-d:gen:two",
                    predecessor_generation_id="okg:lane-d:gen:one"),
    ]


def test_each_count_row_carries_its_parent_side(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A type present here and absent from a READABLE parent is a real zero."""
    asked = _install(
        monkeypatch,
        ascending=_two_attempt_history(),
        rollups={
            "okg:lane-d:gen:two": {
                ("subtype", "page"): 30, ("subtype", "commit"): 90,
                ("edge_type", "mentions"): 12,
                ("source", "papers"): 30,
            },
            "okg:lane-d:gen:one": {
                ("subtype", "page"): 25,
                ("source", "papers"): 25,
            },
        },
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:two"),
        attempt_id="okg:lane-d:gen:two",
    )
    # ONE read per version, never one per row or per dimension.
    assert asked == ["okg:lane-d:gen:two", "okg:lane-d:gen:one"]

    rows = {row["name"]: row for row in payload["by_record_type"]["rows"]}
    # Ordered by size, so the page does not have to sort to be useful.
    assert [row["name"] for row in payload["by_record_type"]["rows"]] == [
        "commit", "page",
    ]
    assert rows["page"]["count"] == 30
    assert rows["page"]["parent_count"] == 25
    assert rows["page"]["parent_available"] is True
    # Absent from a parent that WAS read: a real zero, not an unknown.
    assert rows["commit"]["parent_count"] == 0
    assert payload["by_relationship_type"]["rows"][0]["name"] == "mentions"
    assert payload["by_source"]["rows"][0]["name"] == "papers"
    assert payload["by_record_type"]["unavailable"] is None
    assert "parent_unavailable" not in payload["by_record_type"]


def test_a_name_the_parent_held_and_this_version_dropped_keeps_its_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The defect that made a retraction page not add up.

    Measured on the published lane-d-home-render deployment, 2026-09-21:
    attempt e71bec30ed62 retracted 988 records, and every one of them came
    from `deployment_configs`. Its count at that version is therefore 0 and
    its count at the parent is 988 -- and the section was built from THIS
    version's names alone, so the row vanished and the -988 with it. The page
    listed one source holding 735 with no change, beside a header that said
    -988 records.

    A name is a row when it is recorded at either end. A name that is zero at
    both ends is cut, because a row of two zeros states nothing.
    """
    _install(
        monkeypatch,
        ascending=_two_attempt_history(),
        rollups={
            "okg:lane-d:gen:two": {
                ("subtype", "page"): 30,
                ("source", "ontologies"): 30,
            },
            "okg:lane-d:gen:one": {
                ("subtype", "page"): 25,
                ("subtype", "commit"): 90,
                ("source", "ontologies"): 30,
                ("source", "deployment_configs"): 988,
                ("source", "never_held"): 0,
            },
        },
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:two"),
        attempt_id="okg:lane-d:gen:two",
    )
    sources = {row["name"]: row for row in payload["by_source"]["rows"]}
    assert sources["deployment_configs"]["count"] == 0
    assert sources["deployment_configs"]["parent_count"] == 988
    assert sources["deployment_configs"]["parent_available"] is True
    # Zero at both ends states nothing, so it is not a row.
    assert "never_held" not in sources
    # The section's own total counts what it actually lists.
    assert payload["by_source"]["total"] == len(payload["by_source"]["rows"])

    # The same rule on the record types, so one table cannot add up while
    # another silently drops a name.
    records = {row["name"]: row for row in payload["by_record_type"]["rows"]}
    assert records["commit"]["count"] == 0
    assert records["commit"]["parent_count"] == 90

    # What the fix is for: each table's deltas sum to the change the attempt
    # recorded. Records: 30 + 0 here against 25 + 90 at the parent, so -85.
    assert sum(
        row["count"] - row["parent_count"]
        for row in payload["by_record_type"]["rows"]
    ) == -85
    assert sum(
        row["count"] - row["parent_count"]
        for row in payload["by_source"]["rows"]
    ) == -988


def test_an_unreadable_parent_is_null_on_every_row_not_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The one that would silently lie: an unread parent rendered as zero.

    "25 -> 30" and "unknown -> 30" are different answers, and a parent side of
    0 would make every row of a compacted predecessor read as brand new.
    """
    _install(
        monkeypatch,
        ascending=_two_attempt_history(),
        rollups={
            "okg:lane-d:gen:two": {("subtype", "page"): 30},
            "okg:lane-d:gen:one": ConsoleCountRollupUnavailable(
                "okg:lane-d:gen:one", "no_sentinel",
                "host=db.example user=okg password leaked in str(exc)",
            ),
        },
    )
    section = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:two"),
        attempt_id="okg:lane-d:gen:two",
    )["by_record_type"]
    assert section["rows"][0]["parent_count"] is None
    assert section["rows"][0]["parent_available"] is False
    assert section["parent_unavailable"]["reason"] == (
        "rollup_unavailable_for_parent_version"
    )
    # The CODE, not the message. psycopg's own text names the host and the
    # role, and this reader never forwards it.
    assert section["parent_unavailable"]["rollup_reason"] == "no_sentinel"
    assert "db.example" not in json.dumps(section)


def test_a_missing_rollup_marks_its_sections_and_does_not_refuse_the_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blocked attempt has no maintained rollup. The rest still reads."""
    _install(
        monkeypatch,
        ascending=[_checkpoint("okg:lane-d:gen:one", node_count=7,
                               status="blocked")],
        rollups={},
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )
    for name in ("by_record_type", "by_relationship_type", "by_source"):
        section = payload[name]
        assert section["rows"] == []
        assert section["unavailable"]["reason"] == "rollup_unavailable_for_version"
        assert section["unavailable"]["rollup_reason"] == "checkpoint_absent"
    # And the sections that do NOT depend on the rollup still answered.
    assert payload["holds"]["node_count"] == 7
    assert payload["attempt"]["status"] == "blocked"


def test_the_first_attempt_has_no_parent_version_and_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asked = _install(
        monkeypatch,
        ascending=[_checkpoint("okg:lane-d:gen:one")],
        rollups={"okg:lane-d:gen:one": {("subtype", "page"): 3}},
    )
    section = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["by_record_type"]
    assert asked == ["okg:lane-d:gen:one"]
    assert section["parent_unavailable"]["reason"] == "no_predecessor_version"
    assert section["rows"][0]["parent_available"] is False


def test_a_count_section_is_capped_with_its_ceiling_stated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversize = {
        ("subtype", f"type{index:04d}"): index
        for index in range(ROLLUP_ROWS_PER_SECTION + 25)
    }
    _install(
        monkeypatch,
        ascending=[_checkpoint("okg:lane-d:gen:one")],
        rollups={"okg:lane-d:gen:one": oversize},
    )
    section = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["by_record_type"]
    assert section["shown"] == ROLLUP_ROWS_PER_SECTION
    assert section["total"] == ROLLUP_ROWS_PER_SECTION + 25
    assert section["capped"] is True
    assert section["limit"] == ROLLUP_ROWS_PER_SECTION


# --------------------------------------------------------------------------- #
# the checks, through the ONE allowlist                                        #
# --------------------------------------------------------------------------- #
#: The gate shape measured live on 2026-09-21, with the two blobs shrunk to a
#: size a test can hold. Everything else is the recorded value.
_LIVE_GATE = {
    "check_id": "transform.consistency.no_dangling+narrowing+cardinality+"
                "derived_node_liveness.v4",
    "gate_name": "transform_consistency_gate",
    "result": "pass",
    "reason": "no_violations",
    "elapsed_ms": 112,
    "budget_ms": 5000,
    "blocks_publish": True,
    "is_probe": False,
    "coverage_complete": True,
    "coverage_basis": "chronos_dirty_edges+incident_dirty_nodes+"
                      "skipped_families:cardinality",
    "details": {"dangling_edge_ids": ["node:secret:1", "node:secret:2"]},
    "preflight": {"error": "connection to host db.example user okg failed"},
}


def test_checks_serve_passed_gates_through_the_blocker_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same allowlist as the blocker detail, so a gate is safe in both.

    A published attempt's checks section is its only answer to "what did this
    verify", so passed gates are served -- but the two blobs that carry graph
    row ids and ``str(exc)`` are withheld with their size stated, and the
    82-character ``check_id`` is two over the code cap and is withheld too.
    """
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one",
        invariant_summary={"consistency_gates": {
            "total": 1, "passed": 1, "blocked": 0, "results": [_LIVE_GATE],
        }},
    )])
    checks = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["checks"]
    assert (checks["total"], checks["passed"], checks["blocked"]) == (1, 1, 0)
    gate = checks["gates"][0]
    assert gate["gate_name"] == "transform_consistency_gate"
    assert gate["result"] == "pass"
    assert gate["elapsed_ms"] == 112
    assert gate["blocks_publish"] is True
    assert gate["coverage_complete"] is True

    withheld = {row["key"]: row for row in gate["withheld"]}
    assert withheld["details"]["withheld_reason"] == "exception_text"
    assert withheld["details"]["kind"] == "object"
    assert withheld["preflight"]["withheld_reason"] == "exception_text"
    # The live check_id is EXACTLY 80 characters -- the allowlist's code cap,
    # to the character. It is served, and this assertion is the tripwire: one
    # more family name in that gate's id and the console stops showing which
    # check ran. That is the right failure (an unbounded string is not an
    # identifier) but it must not happen silently.
    assert len(_LIVE_GATE["check_id"]) == 80
    assert gate["check_id"] == _LIVE_GATE["check_id"]
    assert "check_id" not in withheld
    assert gate["coverage_basis"] == _LIVE_GATE["coverage_basis"]

    blob = json.dumps(checks)
    assert "node:secret:1" not in blob
    assert "db.example" not in blob


def test_an_attempt_with_no_recorded_checks_says_so(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, ascending=[_checkpoint("okg:lane-d:gen:one")])
    checks = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["checks"]
    assert checks["gates"] == []
    assert checks["total"] is None
    assert checks["unavailable"]["reason"] == "invariant_summary_not_recorded"


# --------------------------------------------------------------------------- #
# neighbours and configuration                                                 #
# --------------------------------------------------------------------------- #
def test_the_parent_is_the_one_the_checkpoint_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A blocked attempt sits in the list without being anyone's parent.

    List order would name it as the parent of the publish above it, which is
    a version that publish never built on.
    """
    _install(monkeypatch, ascending=[
        _checkpoint("okg:lane-d:gen:one"),
        _checkpoint("okg:lane-d:gen:blocked", status="blocked"),
        _checkpoint("okg:lane-d:gen:three",
                    predecessor_generation_id="okg:lane-d:gen:one"),
    ])
    neighbours = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:three"),
        attempt_id="okg:lane-d:gen:three",
    )["neighbours"]
    assert neighbours["parent_generation_id"] == "okg:lane-d:gen:one"
    assert neighbours["parent_source"] == "recorded_predecessor"
    # Position and the two walk links follow the LIST's order, newest first.
    assert neighbours["position"] == 1
    assert neighbours["total"] == 3
    assert neighbours["newer_attempt_id"] is None
    assert neighbours["older_attempt_id"] == "okg:lane-d:gen:blocked"


def test_an_unrecorded_predecessor_falls_back_to_list_order_and_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fallback the payload DECLARES, so nothing reads as recorded fact."""
    _install(monkeypatch, ascending=[
        _checkpoint("okg:lane-d:gen:one"),
        _checkpoint("okg:lane-d:gen:two"),
    ])
    neighbours = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:two"),
        attempt_id="okg:lane-d:gen:two",
    )["neighbours"]
    assert neighbours["parent_generation_id"] == "okg:lane-d:gen:one"
    assert neighbours["parent_source"] == "list_order"


def test_configured_separates_recorded_empty_from_never_recorded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """"Nothing was configured" and "this predates the field" are different.

    Measured live: ``deployment_attestations`` is present and null, and
    ``nomos_policy_bundle_ids`` is a present, empty list. Rendering both the
    same as an absent key would tell the operator the checkpoint is older
    than it is.
    """
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one",
        nomos_policy_bundle_ids=[],
        excluded_sources=["cmssw_releases"],
        enricher_governance={"record_count": 12},
        catalog_version_id=4,
    )])
    configured = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["configured"]
    assert configured["nomos_policy_bundle_ids"] == {
        "recorded": True, "names": [], "withheld_count": 0,
    }
    # Absent from the checkpoint entirely.
    assert configured["deployment_attestations"]["recorded"] is False
    assert configured["excluded_sources"] == {
        "recorded": True, "names": ["cmssw_releases"],
    }
    assert configured["enricher_governance_record_count"] == 12
    assert configured["catalog_version_id"] == 4


def test_a_configured_entry_that_is_not_code_shaped_is_counted_not_echoed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An attestor id is an identifier. A path arriving there is withheld."""
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one",
        deployment_attestations=["okg.attestor.signed", "/Users/example/key.pem"],
    )])
    entry = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["configured"]["deployment_attestations"]
    assert entry == {
        "recorded": True,
        "names": ["okg.attestor.signed"],
        "withheld_count": 1,
    }


# --------------------------------------------------------------------------- #
# the headline is the row the operator clicked                                 #
# --------------------------------------------------------------------------- #
def test_the_attempt_section_is_the_list_row_itself(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Not a second copy of it. A detail headline that disagreed with the row
    the operator clicked would be a defect nothing could catch."""
    from okg.substrate.console_views.generations import compose_attempt_row

    history = _two_attempt_history()
    _install(
        monkeypatch, ascending=history,
        counts={"okg:lane-d:gen:one": (10, 20), "okg:lane-d:gen:two": (14, 20)},
    )
    session = _session("okg:lane-d:gen:two")
    attempt = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT, session=session,
        attempt_id="okg:lane-d:gen:two",
    )["attempt"]
    expected = compose_attempt_row(
        history[1], session=session, view_runs={},
        previous_generation=history[0],
        counts_or_reason=lambda gen: (
            {"okg:lane-d:gen:one": (10, 20),
             "okg:lane-d:gen:two": (14, 20)}[gen.checkpoint_id], None,
        ),
    )
    assert attempt == expected
    assert attempt["nodes_added"] == 4
    assert attempt["edges_added"] == 0
    assert attempt["duration_ms"] is None
    assert attempt["stopped_stage"] is None


# --------------------------------------------------------------------------- #
# the unpublished marker: served under its own key, in every section           #
# --------------------------------------------------------------------------- #
def test_the_unpublished_marker_is_its_own_key_not_a_totals_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refused publish's counts are real facts about that checkpoint, so
    they are served -- with the marker that says which version they describe.

    That marker is NOT an entry in ``holds["unavailable"]``: a page reading
    that list prints it under "Part of this version's totals was not
    recorded", which is false. The totals WERE recorded. It gets its own key,
    on ``holds`` and on every count section, so each can print its own
    sentence.
    """
    _install(
        monkeypatch,
        ascending=[_checkpoint(
            "okg:lane-d:gen:one",
            status="blocked",
            node_count=3,
            edge_count=2,
            projection_deltas={
                "nodes_added": 1, "nodes_updated": 0, "nodes_retracted": 0,
                "edges_added": 1, "edges_updated": 0, "edges_retracted": 0,
            },
        )],
        rollups={"okg:lane-d:gen:one": {("subtype", "page"): 3}},
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )
    assert payload["version_published"] is False
    marker = {
        "reason": "counts_describe_an_unpublished_version",
        "attempt_status": "blocked",
    }
    assert payload["holds"]["unpublished_version"] == marker
    # The totals were recorded. Nothing about them is unavailable.
    assert payload["holds"]["unavailable"] == []
    for section in ("by_record_type", "by_relationship_type", "by_source"):
        assert payload[section]["unpublished_version"] == marker
    # The section's own `unavailable` stays what the ROLLUP said, so a
    # refused rollup and an unpublished version are never merged.
    assert payload["by_record_type"]["unavailable"] is None


def test_a_published_attempt_carries_the_key_set_to_null(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The key is always present, so the page reads one field rather than
    inferring the fact from `version_published` and a row count."""
    _install(
        monkeypatch,
        ascending=[_checkpoint("okg:lane-d:gen:one", node_count=3)],
        rollups={"okg:lane-d:gen:one": {("subtype", "page"): 3}},
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )
    assert payload["version_published"] is True
    assert payload["holds"]["unpublished_version"] is None
    for section in ("by_record_type", "by_relationship_type", "by_source"):
        assert payload[section]["unpublished_version"] is None


def test_a_blocked_attempt_with_no_rollup_rows_still_carries_the_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The rollup came back EMPTY, so every count table is cut and the page
    has only the holds numbers to print. The sentence must still appear: the
    numbers it qualifies are on the page."""
    _install(
        monkeypatch,
        ascending=[_checkpoint(
            "okg:lane-d:gen:one", status="blocked", node_count=3, edge_count=2,
        )],
        rollups={"okg:lane-d:gen:one": {}},
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )
    assert payload["by_record_type"]["rows"] == []
    assert payload["holds"]["node_count"] == 3
    assert payload["holds"]["unpublished_version"]["reason"] == (
        "counts_describe_an_unpublished_version"
    )


# --------------------------------------------------------------------------- #
# a malformed gate results list is not "the checks all passed"                 #
# --------------------------------------------------------------------------- #
def test_a_non_list_results_blob_is_named_and_serves_no_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`total` and `passed` come from the same blob the results list does.

    Keeping them while silently emptying the list prints "3 checks ran, 3
    passed" over no rows -- a summary of gates this read could not see. The
    section is named unavailable instead and states no number.
    """
    for results in ({"a": 1}, "three", 3, None):
        _install(monkeypatch, ascending=[_checkpoint(
            "okg:lane-d:gen:one",
            invariant_summary={"consistency_gates": {
                "total": 3, "passed": 3, "blocked": 0, "results": results,
            }},
        )])
        checks = compose_attempt_detail(
            object(), deployment_name=DEPLOYMENT,
            session=_session("okg:lane-d:gen:one"),
            attempt_id="okg:lane-d:gen:one",
        )["checks"]
        assert checks["gates"] == []
        assert checks["total"] is None, results
        assert checks["passed"] is None
        assert checks["blocked"] is None
        assert checks["unavailable"]["reason"] == "consistency_gates_malformed"


# --------------------------------------------------------------------------- #
# the runs section carries "could not look" as its own fact                    #
# --------------------------------------------------------------------------- #
def test_the_runs_section_names_an_attempt_it_could_not_join(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one", target_generation_id=None,
    )])
    runs = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["runs"]
    assert runs["runs"] == []
    assert runs["total"] == 0
    assert runs["unavailable"] == {"reason": "target_generation_id_not_recorded"}


def test_the_runs_section_is_available_when_the_join_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, ascending=[_checkpoint(
        "okg:lane-d:gen:one", target_generation_id=41,
    )])
    runs = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )["runs"]
    assert runs["total"] == 0
    assert runs["unavailable"] is None


# --------------------------------------------------------------------------- #
# the rollup flag refuses its OWN sections, not the whole read                 #
# --------------------------------------------------------------------------- #
def test_the_count_rollup_flag_being_off_marks_its_sections_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The module docstring says a missing rollup does not 500 the whole
    read. The enablement check stood at the top of the composer and did
    exactly that, so the flag being off cost the operator the holds, the
    checks, the runs and the neighbours as well."""
    _install(
        monkeypatch,
        ascending=[_checkpoint("okg:lane-d:gen:one", node_count=3, edge_count=2)],
        rollups={"okg:lane-d:gen:one": {("subtype", "page"): 3}},
    )
    monkeypatch.setattr(
        console_count_rollup_read, "console_count_rollup_enabled", lambda: False,
    )
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:one"),
        attempt_id="okg:lane-d:gen:one",
    )
    assert payload["holds"]["node_count"] == 3
    section = payload["by_record_type"]
    assert section["rows"] == []
    assert section["unavailable"]["reason"] == "rollup_unavailable_for_version"
    assert section["unavailable"]["rollup_reason"] == "count_rollup_disabled"


# --------------------------------------------------------------------------- #
# the history read is bounded, and says what the bound cost                    #
# --------------------------------------------------------------------------- #
def _long_history(count: int) -> list[SimpleNamespace]:
    """`count` attempts, oldest first, as the store returns them ascending."""
    return [_checkpoint(f"okg:lane-d:gen:{index:04d}") for index in range(count)]


def test_the_history_read_is_bounded_and_projects_only_declared_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One window, this page's declared keys, and no unbounded list.

    The read this replaces was `store.list_generations`, which has no LIMIT
    and no projection: measured at 1.75-1.88 s on okg-workspace's
    88-checkpoint branch, moving every checkpoint's whole metadata document to
    read twelve keys off one of them.
    """
    asked: list[dict[str, Any]] = []
    _install(monkeypatch, ascending=_long_history(10))
    store_class = attempt_detail.ChronosGraphStore
    inner = store_class.recent_generations

    def _recording(self: Any, **kwargs: Any):
        asked.append(kwargs)
        return inner(self, **kwargs)

    monkeypatch.setattr(store_class, "recent_generations", _recording)
    assert not hasattr(store_class, "list_generations"), (
        "the fake store still offers the unbounded read; a composer that "
        "called it would pass this file without anything noticing"
    )
    compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:0009"),
        attempt_id="okg:lane-d:gen:0009",
    )
    assert len(asked) == 1, "the page reads the history once, not once a section"
    assert asked[0]["scan_window"] == attempt_detail.ATTEMPT_DETAIL_SCAN_WINDOW
    assert asked[0]["keys"] == attempt_detail.ATTEMPT_DETAIL_METADATA_KEYS


def test_an_attempt_older_than_the_window_is_read_by_id_not_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The named fallback: window, then the exact id, and 404 only after both.

    A bounded window turns "not in the newest N" into "not found" unless the
    read has a second way to ask. It has one, and it is one row rather than a
    scan, so the page still opens for an attempt at the far end of a long
    history.
    """
    monkeypatch.setattr(attempt_detail, "ATTEMPT_DETAIL_SCAN_WINDOW", 4)
    _install(monkeypatch, ascending=_long_history(40))
    payload = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:0000"),
        attempt_id="okg:lane-d:gen:0000",
    )
    assert payload["attempt_id"] == "okg:lane-d:gen:0000"
    # And it says what the window could not tell it, rather than guessing.
    neighbours = payload["neighbours"]
    assert neighbours["unavailable_reason"] == (
        "attempt_is_older_than_the_window_this_read_scanned"
    )
    assert neighbours["position"] is None
    assert neighbours["newer_attempt_id"] is None
    assert neighbours["older_attempt_id"] is None
    assert neighbours["window_exhausted"] is True
    # The total is still the whole history, counted separately.
    assert neighbours["total"] == 40
    assert neighbours["total_basis"] == "checkpoint_id_prefix"


def test_an_id_outside_the_window_and_outside_this_branch_is_still_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fallback must not become a way around the scope.

    A bounded window plus an unscoped exact read would answer for another
    deployment's checkpoint, which is the one thing `AttemptNotFound` exists
    to prevent.
    """
    monkeypatch.setattr(attempt_detail, "ATTEMPT_DETAIL_SCAN_WINDOW", 4)
    _install(monkeypatch, ascending=_long_history(40))
    with pytest.raises(AttemptNotFound):
        compose_attempt_detail(
            object(), deployment_name=DEPLOYMENT,
            session=_session("okg:lane-d:gen:0000"),
            attempt_id="okg:lane-d:gen:absent",
        )


def test_a_compacted_attempt_outside_the_window_is_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compaction is looked up without a deployment scope, so it is a 404.

    Served as its own answer, it would tell a reader that some deployment
    once held the id -- the thing the constant 404 body exists to hide.
    """
    from okg.substrate.errors import GenerationCompacted

    monkeypatch.setattr(attempt_detail, "ATTEMPT_DETAIL_SCAN_WINDOW", 4)
    _install(monkeypatch, ascending=_long_history(40))

    def _compacted(self: Any, generation_id: Any) -> Any:
        raise GenerationCompacted(f"generation {generation_id!r} was retired")

    monkeypatch.setattr(attempt_detail.ChronosGraphStore, "get_generation", _compacted)
    with pytest.raises(AttemptNotFound):
        compose_attempt_detail(
            object(), deployment_name=DEPLOYMENT,
            session=_session("okg:lane-d:gen:0000"),
            attempt_id="okg:lane-d:gen:0000",
        )


def test_a_failed_by_id_read_propagates_rather_than_reading_as_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broken read is a failed read, not an attempt that does not exist.

    The by-id fallback turns exactly two named answers into None. Anything
    else -- here a lost connection -- must reach the endpoint, which reports
    `attempt_detail_composer_failed` instead of a 404 that lies.
    """
    monkeypatch.setattr(attempt_detail, "ATTEMPT_DETAIL_SCAN_WINDOW", 4)
    _install(monkeypatch, ascending=_long_history(40))

    class _ConnectionLost(RuntimeError):
        pass

    def _broken(self: Any, generation_id: Any) -> Any:
        raise _ConnectionLost("server closed the connection unexpectedly")

    monkeypatch.setattr(attempt_detail.ChronosGraphStore, "get_generation", _broken)
    with pytest.raises(_ConnectionLost):
        compose_attempt_detail(
            object(), deployment_name=DEPLOYMENT,
            session=_session("okg:lane-d:gen:0000"),
            attempt_id="okg:lane-d:gen:0000",
        )


def test_the_oldest_row_of_an_exhausted_window_says_why_it_has_no_older_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Null "older attempt" means two things and the page must not merge them.

    At the end of a COMPLETE history there is no older attempt. At the end of
    a window there is one this read did not scan, and a page that drew "the
    oldest attempt" over it would be describing the window, not the
    deployment.
    """
    monkeypatch.setattr(attempt_detail, "ATTEMPT_DETAIL_SCAN_WINDOW", 4)
    _install(monkeypatch, ascending=_long_history(40))
    bounded = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:0036"),
        attempt_id="okg:lane-d:gen:0036",
    )["neighbours"]
    assert bounded["older_attempt_id"] is None
    assert bounded["older_attempt_unavailable_reason"] == (
        "attempt_is_older_than_the_window_this_read_scanned"
    )

    _install(monkeypatch, ascending=_long_history(3))
    complete = compose_attempt_detail(
        object(), deployment_name=DEPLOYMENT,
        session=_session("okg:lane-d:gen:0000"),
        attempt_id="okg:lane-d:gen:0000",
    )["neighbours"]
    assert complete["older_attempt_id"] is None
    assert complete["older_attempt_unavailable_reason"] is None
    assert complete["window_exhausted"] is False
