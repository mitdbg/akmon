"""The attempt-detail read against real publishes.

PACT operator-console-live-updates. The composition file beside this one
proves the shapes without a database. What only Postgres can answer is whether
those shapes are filled by a REAL publish: whether the checkpoint a publisher
writes carries the totals and projection deltas the ``holds`` section reads,
whether the maintained count rollup exists at a published version AND at its
predecessor so the by-dimension sections have a parent side, and -- the
question the console could not answer before -- what the read says about a
BLOCKED attempt, which is published here through the publisher's own
attestation refusal rather than hand-written into a table.

The cost proof lives here too, because it is only meaningful against the real
query path: the number of SQL statements one detail read issues must not grow
with the number of attempts in the deployment's history.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.console_views.attempt_detail import (
    AttemptNotFound,
    MalformedAttemptId,
    compose_attempt_detail,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.session import open_session
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.checkpoint_attestations import (
    CheckpointAttestationRefused,
    LoadedCheckpointAttestor,
)
from okg.viz.console_perf_budgets import statement_budget
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"


# --------------------------------------------------------------------------- #
# publishing, through the publisher                                            #
# --------------------------------------------------------------------------- #
def _node(node_id: str, subtype: str, *, op: str = "I") -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"title": node_id, "name": node_id, "text": node_id},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
        op=op,
    )


class _RefusingAttestor:
    """An attestor that refuses, so the publisher blocks its own cycle.

    The product's blocked path, not a simulation of it: the publisher catches
    the refusal, sets the checkpoint's status to "blocked" and writes the
    blocked checkpoint itself. Nothing here inserts a checkpoint row.
    """

    attestor_id = "tests.console.detail.refusing"
    config_schema = {
        "type": "object", "properties": {}, "required": [],
        "additionalProperties": False,
    }

    def attest(self, context: Any) -> Any:
        raise CheckpointAttestationRefused(
            "attestation_invalid", "the fixture attestor refuses this cycle",
        )


def _publish(
    dsn: str,
    facts: list[Any],
    *,
    refuse: bool = False,
) -> dict:
    if facts:
        with psycopg.connect(dsn, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts(facts, source="fixture")
    attestors = None
    if refuse:
        instance = _RefusingAttestor()
        attestors = [
            LoadedCheckpointAttestor(
                manifest_id=instance.attestor_id,
                class_name=(
                    f"{type(instance).__module__}.{type(instance).__qualname__}"
                ),
                attestor=instance,
            ),
        ]
    return Publisher(
        dsn, DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
        checkpoint_attestors=attestors,
    ).cycle()


@contextmanager
def _session(dsn: str) -> Iterator[tuple[psycopg.Connection, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name=DEPLOYMENT)


def _detail(dsn: str, attempt_id: str) -> dict:
    with _session(dsn) as (conn, session):
        return compose_attempt_detail(
            conn, deployment_name=DEPLOYMENT, session=session,
            attempt_id=attempt_id,
        )


def _attempt_ids(dsn: str) -> list[str]:
    """Every attempt this branch holds, newest first."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return [
            generation.checkpoint_id
            for generation in reversed(store.list_generations(
                deployment_name=DEPLOYMENT, branch="default", status=None,
            ))
        ]


@pytest.fixture
def two_published(clean_runtime: str) -> str:
    """Two real publishes: four records and two relationships, then one more.

    The second attempt's parent is the first, so the by-dimension sections
    have a real parent side rather than one this test invented.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    assert _publish(clean_runtime, [
        _node("page:alpha", "page"),
        _node("page:beta", "page"),
        _node("release:r1", "release"),
        _node("release:r2", "release"),
        EdgeFact(src="page:alpha", dst="release:r1", edge_type="mentions"),
        EdgeFact(src="page:beta", dst="release:r2", edge_type="mentions"),
    ])["status"] == "published"
    assert _publish(
        clean_runtime, [_node("release:r3", "release")],
    )["status"] == "published"
    return clean_runtime


# --------------------------------------------------------------------------- #
# what a real published attempt records                                        #
# --------------------------------------------------------------------------- #
def test_a_real_publish_fills_holds_from_its_own_checkpoint(
    two_published: str,
) -> None:
    """The totals and the projection deltas are both recorded, and differ.

    `holds` is the section the detail page had nothing in. This asserts it is
    filled from the checkpoint the publisher wrote, against the numbers this
    test put in: the SECOND attempt added exactly one record and no
    relationship.
    """
    newest = _attempt_ids(two_published)[0]
    holds = _detail(two_published, newest)["holds"]

    assert holds["unavailable"] == []
    # Five records and two relationships after the second publish.
    assert holds["node_count"] == 5
    assert holds["edge_count"] == 2
    assert holds["nodes"] == {"added": 1, "updated": 0, "retracted": 0}
    assert holds["edges"] == {"added": 0, "updated": 0, "retracted": 0}


def test_the_counts_are_read_against_the_real_parent_version(
    two_published: str,
) -> None:
    """The parent side comes from the PREDECESSOR's own maintained rollup.

    This is what turns "1 record added" into "releases went from 2 to 3", and
    it can only be proved with two real published versions: the rollup has to
    exist at both.
    """
    newest = _attempt_ids(two_published)[0]
    payload = _detail(two_published, newest)

    by_type = {row["name"]: row for row in payload["by_record_type"]["rows"]}
    assert payload["by_record_type"]["unavailable"] is None
    assert "parent_unavailable" not in payload["by_record_type"]
    assert by_type["release"]["count"] == 3
    assert by_type["release"]["parent_count"] == 2
    assert by_type["release"]["parent_available"] is True
    # Untouched by the second publish: recorded as unchanged, not as absent.
    assert by_type["page"]["count"] == 2
    assert by_type["page"]["parent_count"] == 2

    rel = {row["name"]: row for row in payload["by_relationship_type"]["rows"]}
    assert rel["mentions"]["count"] == 2
    assert rel["mentions"]["parent_count"] == 2

    source = {row["name"]: row for row in payload["by_source"]["rows"]}
    assert source["fixture"]["count"] == 5
    assert source["fixture"]["parent_count"] == 4


def test_a_retraction_is_served_as_a_removal_on_both_sides(
    two_published: str,
) -> None:
    """A retraction-only publish: the one the list's delta reads as negative.

    `holds` records it directly and the by-dimension rows show the parent
    side ABOVE this version's -- a shape no addition can produce.
    """
    assert _publish(
        two_published, [_node("release:r3", "release", op="R")],
    )["status"] == "published"
    newest = _attempt_ids(two_published)[0]
    payload = _detail(two_published, newest)

    assert payload["holds"]["nodes"]["retracted"] == 1
    assert payload["holds"]["nodes"]["added"] == 0
    assert payload["holds"]["node_count"] == 4
    by_type = {row["name"]: row for row in payload["by_record_type"]["rows"]}
    assert by_type["release"]["count"] == 2
    assert by_type["release"]["parent_count"] == 3
    # And the list row the detail reuses states the retraction too.
    assert payload["attempt"]["nodes_retracted"] == 1
    assert payload["attempt"]["nodes_added"] == 0


def test_the_checks_section_reads_whatever_the_publisher_recorded(
    two_published: str,
) -> None:
    """Measured, not assumed: a bare fixture deployment records NO gates.

    The live ``lane-d-home-render`` deployment records one consistency gate
    per attempt, with a 146,414-byte ``details`` blob. This fixture
    deployment configures no consistency check, so its checkpoints carry no
    ``consistency_gates`` key at all -- which is exactly the case that must
    NOT render as "0 checks ran, all passed". It renders as the named marker
    ``consistency_gates_not_recorded``.

    The served-gate path is proved against the live gate verbatim in
    test_attempt_detail_composition.py; this test owns the other branch, and
    asserts whichever one this deployment actually produces so it cannot pass
    by finding nothing.
    """
    newest = _attempt_ids(two_published)[0]
    checks = _detail(two_published, newest)["checks"]

    if checks["unavailable"] is not None:
        assert checks["unavailable"]["reason"] in (
            "invariant_summary_not_recorded", "consistency_gates_not_recorded",
        )
        # The counts stay NULL. A zero here would say the publish verified
        # nothing and passed, which is a claim no record supports.
        assert checks["total"] is None
        assert checks["passed"] is None
        assert checks["blocked"] is None
        assert checks["gates"] == []
    else:
        assert checks["total"] == len(checks["gates"])
        gate = checks["gates"][0]
        assert isinstance(gate["gate_name"], str)
        withheld = {row["key"]: row for row in gate.get("withheld", [])}
        for blob in ("details", "preflight"):
            if blob in withheld:
                assert withheld[blob]["withheld_reason"] == "exception_text"
                assert isinstance(withheld[blob]["size"], int)

    # Either way, no graph row id reaches the page out of this section.
    serialized = json.dumps(checks)
    assert "page:alpha" not in serialized
    assert "release:r1" not in serialized


def test_neighbours_walk_the_real_history(two_published: str) -> None:
    newest, oldest = _attempt_ids(two_published)
    payload = _detail(two_published, newest)["neighbours"]
    assert payload["position"] == 1
    assert payload["total"] == 2
    assert payload["newer_attempt_id"] is None
    assert payload["older_attempt_id"] == oldest
    assert payload["parent_generation_id"] == oldest
    assert payload["parent_source"] == "recorded_predecessor"

    first = _detail(two_published, oldest)["neighbours"]
    assert first["newer_attempt_id"] == newest
    assert first["older_attempt_id"] is None
    assert first["parent_generation_id"] is None
    assert _detail(two_published, oldest)["by_record_type"][
        "parent_unavailable"
    ]["reason"] == "no_predecessor_version"


def test_configured_reports_what_the_publisher_recorded(
    two_published: str,
) -> None:
    configured = _detail(
        two_published, _attempt_ids(two_published)[0],
    )["configured"]
    # Recorded and empty is not the same as never recorded, and this
    # deployment excludes nothing.
    assert configured["excluded_sources"]["recorded"] is True
    assert configured["excluded_sources"]["names"] == []
    assert isinstance(configured["catalog_version_id"], int)
    assert configured["nomos_policy_bundle_ids"]["recorded"] is True


# --------------------------------------------------------------------------- #
# the blocked attempt                                                          #
# --------------------------------------------------------------------------- #
@pytest.fixture
def published_then_blocked(two_published: str) -> str:
    result = _publish(
        two_published, [_node("page:gamma", "page")], refuse=True,
    )
    assert result["status"] != "published", result
    return two_published


def test_a_blocked_attempt_reads_without_pretending_it_published(
    published_then_blocked: str,
) -> None:
    """What the console can honestly show for a refused publish.

    The finding this test pins, measured 2026-09-21 and the opposite of what
    was expected: a blocked checkpoint DOES have a maintained count rollup,
    and that rollup counts the records the refused cycle staged. The attempt
    below adds one ``page`` and is refused; the rollup at its checkpoint
    answers ``page: 3`` against a parent of 2.

    Those are true facts about that checkpoint, so they are served rather than
    withheld. But a detail page printing "3 pages, up from 2" for a publish
    that was REFUSED would tell the operator the graph grew when it did not.
    So every section reading from a non-published checkpoint carries the named
    marker ``counts_describe_an_unpublished_version``, and ``version_published``
    is false beside them. The blocker itself still reaches the page: the read
    is not refused because part of it needs a caveat.
    """
    blocked_id = next(
        attempt for attempt in _attempt_ids(published_then_blocked)
        if _detail(published_then_blocked, attempt)["attempt"]["status"]
        == "blocked"
    )
    payload = _detail(published_then_blocked, blocked_id)

    # The blocker itself still reaches the page. This is the whole point of
    # not refusing the read when the rollup is missing.
    assert payload["attempt"]["status"] == "blocked"
    assert "checkpoint_attestation_failed" in payload["attempt"]["blocked_reason"]
    assert payload["attempt"]["blocked_detail"]["attestor_id"] == (
        "tests.console.detail.refusing"
    )

    assert payload["version_published"] is False
    for name in ("by_record_type", "by_relationship_type", "by_source"):
        section = payload[name]
        assert section["unpublished_version"]["reason"] == (
            "counts_describe_an_unpublished_version"
        ), name
        assert section["unpublished_version"]["attempt_status"] == "blocked"
    # The staged-but-unpublished count is really there, which is why the
    # marker above is load-bearing rather than defensive.
    staged = {row["name"]: row for row in payload["by_record_type"]["rows"]}
    assert staged["page"]["count"] == 3
    assert staged["page"]["parent_count"] == 2
    # Re-aimed, not deleted. The marker used to be appended to
    # `holds["unavailable"]`, where the page printed it under "Part of this
    # version's totals was not recorded" -- which is false: the totals WERE
    # recorded. It has its own key now, and the absence list stays empty.
    assert payload["holds"]["unpublished_version"]["reason"] == (
        "counts_describe_an_unpublished_version"
    )
    assert payload["holds"]["unpublished_version"]["attempt_status"] == "blocked"
    assert payload["holds"]["unavailable"] == []
    assert "password" not in json.dumps(payload)

    # And the fields nothing records stay null rather than being guessed from
    # the blocker's code.
    assert payload["attempt"]["stopped_stage"] is None
    assert payload["attempt"]["duration_ms"] is None


def test_a_published_attempt_after_a_blocked_one_still_reads_its_parent(
    published_then_blocked: str,
) -> None:
    """The blocked attempt sits in the list; it is nobody's parent version.

    Taking the parent from list order would point the newest publish at a
    version that has no rollup at all, and every count would read as unknown.
    """
    assert _publish(
        published_then_blocked, [_node("page:delta", "page")],
    )["status"] == "published"
    newest = _attempt_ids(published_then_blocked)[0]
    payload = _detail(published_then_blocked, newest)

    assert payload["neighbours"]["parent_source"] == "recorded_predecessor"
    blocked_ids = {
        attempt for attempt in _attempt_ids(published_then_blocked)
        if _detail(published_then_blocked, attempt)["attempt"]["status"]
        == "blocked"
    }
    assert payload["neighbours"]["parent_generation_id"] not in blocked_ids
    assert payload["neighbours"]["older_attempt_id"] in blocked_ids
    assert payload["by_record_type"]["rows"][0]["parent_available"] is True


# --------------------------------------------------------------------------- #
# scope, and refusals                                                          #
# --------------------------------------------------------------------------- #
def test_an_id_outside_this_deployment_is_the_same_not_found(
    two_published: str,
) -> None:
    """Indistinguishable from an id that never existed, on purpose."""
    real = _attempt_ids(two_published)[0]
    foreign = real.replace(f"okg:{DEPLOYMENT}:", "okg:some-other-deployment:")
    assert foreign != real
    for absent in (foreign, real.replace(":branch:default:", ":branch:other:"),
                   "okg:default:branch:default:gen:nope"):
        with pytest.raises(AttemptNotFound):
            _detail(two_published, absent)


def test_a_dsn_shaped_id_never_reaches_a_query(two_published: str) -> None:
    with pytest.raises(MalformedAttemptId):
        _detail(two_published, "postgres://okg:pw@db.example:5432/okg")


# --------------------------------------------------------------------------- #
# cost                                                                         #
# --------------------------------------------------------------------------- #
class _CountingConnection:
    """A connection that counts the statements executed through it.

    Wraps rather than replaces, so what is counted is the real query path.
    """

    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self.statements = 0

    def cursor(self, *args: Any, **kwargs: Any) -> Any:
        outer = self

        class _Cursor:
            def __init__(self, inner: Any) -> None:
                self._inner = inner

            def execute(self, *a: Any, **k: Any) -> Any:
                outer.statements += 1
                return self._inner.execute(*a, **k)

            def __enter__(self) -> "_Cursor":
                self._inner.__enter__()
                return self

            def __exit__(self, *exc: Any) -> Any:
                return self._inner.__exit__(*exc)

            def __getattr__(self, name: str) -> Any:
                return getattr(self._inner, name)

        return _Cursor(self._conn.cursor(*args, **kwargs))

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """psycopg's OTHER query path, which opens a cursor of its own.

        Left to `__getattr__` it went straight to the real connection and was
        never counted, so a read that moved onto it could issue one statement
        per attempt while the N+1 guard below still measured a constant.
        """
        self.statements += 1
        return self._conn.execute(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


def _statements_for_newest(dsn: str) -> int:
    attempt = _attempt_ids(dsn)[0]
    with psycopg.connect(dsn, autocommit=True) as raw:
        session = open_session(raw, deployment_name=DEPLOYMENT)
        counting = _CountingConnection(raw)
        compose_attempt_detail(
            counting, deployment_name=DEPLOYMENT, session=session,
            attempt_id=attempt,
        )
        return counting.statements


def test_the_read_does_not_issue_one_query_per_attempt(
    two_published: str,
) -> None:
    """Bounded cost, stated as a number and then proved to be a CONSTANT.

    The absolute count is recorded so a regression that doubles it is visible
    in the diff. The property that matters is the second half: three more
    published attempts must not cost three more statements. An N+1 over the
    history, the gates or the sources would fail here, not in production.
    """
    # The ceiling is DECLARED, in okg.viz.console_perf_budgets beside every
    # other console route's budget (issue #2119), and read from there rather
    # than written twice -- a number this file owned alone could drift from the
    # one the console's own budget registry publishes. It is stated above the
    # measurement (60 against 50, measured 2026-09-21 on a fresh two-attempt
    # deployment) so a one-statement change elsewhere does not red this file,
    # but a doubling does.
    budget = statement_budget("attempt-detail")
    before = _statements_for_newest(two_published)
    assert before <= budget.ceiling, (before, budget)

    for index in range(3):
        assert _publish(
            two_published, [_node(f"page:extra{index}", "page")],
        )["status"] == "published"

    after = _statements_for_newest(two_published)
    assert after == before, (before, after)
    # The declaration describes this read, not some other one: what it records
    # as measured is what this deployment shape costs.
    assert budget.measured == before, (budget.measured, before)


def test_the_statement_counter_sees_both_query_paths(two_published: str) -> None:
    """The guard above is only worth what it counts.

    psycopg offers two ways to run a statement -- `conn.cursor().execute` and
    `conn.execute`, which opens a cursor of its own. The wrapper counted the
    first and forwarded the second straight through `__getattr__`, so a read
    moved onto `conn.execute` could have issued one statement per attempt and
    the N+1 guard would have measured a constant.
    """
    with psycopg.connect(two_published, autocommit=True) as raw:
        counting = _CountingConnection(raw)
        with counting.cursor() as cur:
            cur.execute("SELECT 1")
        counting.execute("SELECT 1")
        assert counting.statements == 2
