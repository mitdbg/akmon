"""What the console's attempts read can honestly say about one attempt.

PACT operator-console-live-updates. The Updates view asks three questions of
every update attempt: what happened, what it changed, and -- when it did not
publish -- what refused it. Measured against a published deployment on
2026-09-21 the read answered the third with nothing at all: ``stopped_stage``,
``stopped_producer``, ``error_text`` and ``duration_ms`` are hard-nulled by the
composer because no column records them.

Three facts ARE recorded and this file is where that claim is proved against
real Postgres rather than asserted in a comment:

* the checkpoint's ``blocked_reason`` jsonb, written by the publisher when a
  checkpoint attestor refuses the cycle,
* the gate's ``excluded_sources`` list, recorded on every attempt whatever its
  outcome,
* one row per VIEW producer in ``okg.derived_artifact_runs``, joined to the
  attempt by ``target_generation_id`` -- the same value the publisher writes
  into the checkpoint's metadata.

The blocked attempt here is published THROUGH THE PRODUCT'S OWN PUBLISH PATH:
a checkpoint attestor refuses, the publisher blocks the cycle and writes the
blocked checkpoint itself. No row is hand-inserted into a checkpoint table, so
what these tests read is what a real refusal leaves behind.

The cursor is proved here too, because the property that matters -- a page
that does not shift when an attempt is published between two reads -- needs a
real history to move.
"""
from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.console_views import generations as generations_module
from okg.substrate.console_views.generations import (
    UnknownGenerationCursor,
    compose_generations,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp.session import open_session
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.checkpoint_attestations import (
    CheckpointAttestationRefused,
    LoadedCheckpointAttestor,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.substrate._db_helpers import seed_catalog_version

pytestmark = pytest.mark.postgres

DEPLOYMENT = "default"


# --------------------------------------------------------------------------- #
# publishing, through the publisher                                            #
# --------------------------------------------------------------------------- #
def _node(node_id: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="page",
        attrs={"title": node_id, "text": node_id},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


class _RefusingAttestor:
    """An attestor that refuses, so the publisher blocks its own cycle.

    This is the product's blocked path, not a simulation of it: the publisher
    catches the refusal, sets the checkpoint's status to "blocked" and writes
    ``blocked_reason`` into the checkpoint metadata itself.
    """

    attestor_id = "tests.console.refusing"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def attest(self, context: Any) -> Any:
        raise CheckpointAttestationRefused(
            "attestation_invalid",
            "the fixture attestor refuses this cycle",
        )


def _publish(dsn: str, node_ids: list[str], *, refuse: bool = False) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts([_node(node_id) for node_id in node_ids], source="fixture")
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
        dsn,
        DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(dsn, DEPLOYMENT),
        checkpoint_attestors=attestors,
    ).cycle()


@contextmanager
def _session(dsn: str) -> Iterator[tuple[psycopg.Connection, Any]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        yield conn, open_session(conn, deployment_name=DEPLOYMENT)


def _compose(dsn: str, **kwargs: Any) -> dict:
    with _session(dsn) as (conn, session):
        return compose_generations(
            conn, deployment_name=DEPLOYMENT, session=session, **kwargs,
        )


@pytest.fixture
def one_published(clean_runtime: str) -> str:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    assert _publish(clean_runtime, ["page:alpha"])["status"] == "published"
    return clean_runtime


@pytest.fixture
def published_then_blocked(one_published: str) -> str:
    """One published attempt, then one the publisher's own gate refused."""
    result = _publish(one_published, ["page:beta"], refuse=True)
    assert result["status"] != "published", result
    return one_published


# --------------------------------------------------------------------------- #
# what a blocked attempt records                                               #
# --------------------------------------------------------------------------- #
def test_a_blocked_attempt_carries_the_blocker_the_run_wrote(
    published_then_blocked: str,
) -> None:
    payload = _compose(published_then_blocked)
    rows = payload["generations"]
    blocked = [row for row in rows if row["status"] == "blocked"]
    assert blocked, [row["status"] for row in rows]
    row = blocked[0]

    # The one-line summary a list can hold keeps its old meaning and its old
    # type: a string, or None.
    assert isinstance(row["blocked_reason"], str)
    assert "checkpoint_attestation_failed" in row["blocked_reason"]

    # The record itself, unflattened, for the detail page's verbatim block.
    # It is the publisher's own dict, not a re-rendering of the summary.
    detail = row["blocked_detail"]
    assert isinstance(detail, dict)
    assert detail["reason"] == "checkpoint_attestation_failed"
    assert detail["attestor_id"] == "tests.console.refusing"
    # The attestor's own refusal code, carried through the publisher: an
    # explicit refusal, not the "attestor_exception" the runner records when
    # an attestor raises something it does not recognise.
    assert detail["code"] == "attestation_invalid"
    # The attestor's free-text detail is an English sentence, not a code, so
    # the allowlist withholds it and says so. A sentence is exactly where a
    # path or a row id would arrive, and this attestor's happens to be
    # harmless -- which is not something the reader can know.
    assert detail.get("detail") is None
    withheld = {row["key"]: row for row in detail["withheld"]}
    assert withheld["detail"]["withheld_reason"] == "not_code_shaped"
    assert withheld["detail"]["kind"] == "string"
    assert "refuses this cycle" not in json.dumps(detail)

    # And the fields nothing records stay null rather than being guessed from
    # the blocker's code. This is the guard on the cut: the moment a stage name
    # appears here, the console is printing a position no run measured.
    assert row["stopped_stage"] is None
    assert row["stopped_producer"] is None
    assert row["error_text"] is None
    assert row["duration_ms"] is None


def test_a_published_attempt_records_no_blocker(one_published: str) -> None:
    row = _compose(one_published)["generations"][0]
    assert row["status"] == "published"
    assert row["blocked_reason"] is None
    assert row["blocked_detail"] is None


def test_excluded_sources_is_the_list_the_checkpoint_recorded(
    one_published: str,
) -> None:
    """A list, on every attempt, whatever its outcome.

    The count was already served; the names were not, so the view could say
    "1 source excluded" and never which. The count keeps its old meaning, and
    the two agree by construction.
    """
    payload = _compose(one_published)
    for row in payload["generations"]:
        assert isinstance(row["excluded_sources"], list)
        assert all(isinstance(name, str) for name in row["excluded_sources"])
        assert len(row["excluded_sources"]) == row["excluded_source_count"]

    # This deployment's gate excluded nothing, so the rows above prove the
    # field's type and its agreement with the count but not that a NAME
    # survives. The reader is driven directly with the two shapes the
    # publisher writes -- the gate's sorted list, and the mapping older
    # checkpoints carry -- so an implementation that answered [] for both
    # would fail here rather than pass on an empty fixture.
    read = generations_module._excluded_source_names
    assert read(["cmssw_releases", "papers"]) == ["cmssw_releases", "papers"]
    assert read({"cmssw_releases": {"reason": "invariant_failed"}}) == [
        "cmssw_releases",
    ]
    assert read(None) == []
    assert read([]) == []


# --------------------------------------------------------------------------- #
# per-producer results, for the attempt they belong to                         #
# --------------------------------------------------------------------------- #
def _insert_view_run(
    dsn: str,
    *,
    definition_id: str,
    target_generation_id: int,
    status: str,
    error: dict | None,
) -> None:
    """A recorded view-producer run, written the way the view runner writes it.

    The rows this test reads back are inserted here rather than produced by a
    real view build: standing up a view definition and materialising it is a
    different subsystem's fixture, and what is under test is the JOIN -- that
    the composer attributes a run to the attempt whose target generation it
    names, and to no other.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.derived_artifact_definitions (
              definition_id, artifact_kind, deployment_name, logical_key,
              compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash,
              renderer_ref
            ) VALUES (
              %(definition_id)s, 'view', %(deployment)s, %(definition_id)s,
              'h', 'h', 'h', 'h', 'tests.renderer'
            ) ON CONFLICT (definition_id) DO NOTHING
            """,
            {"definition_id": definition_id, "deployment": DEPLOYMENT},
        )
        cur.execute(
            """
            INSERT INTO okg.derived_artifact_runs (
              run_id, definition_id, target_generation_id, status, trigger_kind,
              compute_hash, output_schema_hash, renderer_hash, dirty_rule_hash,
              error, completed_at
            ) VALUES (
              %(run_id)s, %(definition_id)s, %(target)s, %(status)s, 'full',
              'h', 'h', 'h', 'h', %(error)s, now()
            )
            """,
            {
                "run_id": str(uuid.uuid4()),
                "definition_id": definition_id,
                "target": target_generation_id,
                "status": status,
                "error": psycopg.types.json.Jsonb(error) if error else None,
            },
        )


def test_view_runs_belong_to_the_attempt_that_produced_them(
    one_published: str,
) -> None:
    target = int(_compose(one_published)["generations"][0]["generation_id"])
    _insert_view_run(
        one_published,
        definition_id="view:mine",
        target_generation_id=target,
        status="failed",
        error={"message": "renderer raised", "lines": ["one", "two"]},
    )
    # A run recorded against a DIFFERENT generation must not appear on this
    # attempt. Joining on the producer alone -- which is what "latest run per
    # definition" does -- would print another run's error on this page.
    _insert_view_run(
        one_published,
        definition_id="view:elsewhere",
        target_generation_id=target + 10_000,
        status="completed",
        error=None,
    )

    row = _compose(one_published)["generations"][0]
    assert [run["definition_id"] for run in row["view_runs"]] == ["view:mine"]
    run = row["view_runs"][0]
    assert run["status"] == "failed"
    # The error is no longer served whole: its writers put str(exc) and
    # tracebacks in that column. What survives is the KIND of failure; the
    # message is withheld with a marker and its size.
    assert run["error"]["error_text_withheld"] is True
    # Re-aimed, not relaxed. `message` is the error writers' own fixed
    # vocabulary and stays named; `lines` is not a key this reader
    # recognises, and echoing an unrecognised key name leaks the vocabulary
    # of whatever wrote it -- which can itself name a source or a record
    # type. The COUNT survives, so the page still says two fields are hidden.
    assert run["error"]["withheld_keys"] == ["message", "unknown_key"]
    assert "renderer raised" not in json.dumps(run["error"])
    assert run["completed_at"] is not None


def test_an_attempt_with_no_recorded_run_carries_an_empty_list(
    one_published: str,
) -> None:
    """Empty, not a zero. The view prints "no per-producer result is recorded"
    for this; a count of 0 would claim the deployment measured something."""
    row = _compose(one_published)["generations"][0]
    assert row["view_runs"] == []


def test_the_view_runs_read_is_one_query_for_the_whole_page(
    one_published: str,
) -> None:
    """The budget claim, checked rather than asserted in a comment.

    A per-attempt query would grow with the page and quietly turn a 300 ms
    budget into fifty round trips.
    """
    _publish(one_published, ["page:gamma"])
    _publish(one_published, ["page:delta"])

    calls: list[list[int]] = []
    real = generations_module._view_runs_by_target

    def _counting(conn: Any, **kwargs: Any) -> Any:
        calls.append(list(kwargs["targets"]))
        return real(conn, **kwargs)

    monkey = pytest.MonkeyPatch()
    monkey.setattr(generations_module, "_view_runs_by_target", _counting)
    try:
        payload = _compose(one_published, limit=50)
    finally:
        monkey.undo()

    assert len(payload["generations"]) >= 3
    # Once, for the whole page -- not once per attempt.
    assert len(calls) == 1, calls
    # And that one call asked about every attempt on the page, so "one query"
    # is not bought by asking about fewer attempts than the page shows.
    assert len(calls[0]) == len(payload["generations"])


# The pure withholding rules moved to test_console_blocker_allowlist.py: they
# need no database, and inside this postgres-marked module the fast lane
# skipped them -- the safety rules nothing else enforces were the rules
# nothing ran.

# --------------------------------------------------------------------------- #
# paging                                                                       #
# --------------------------------------------------------------------------- #
@pytest.fixture
def five_attempts(one_published: str) -> str:
    for name in ("beta", "gamma", "delta", "epsilon"):
        assert _publish(one_published, [f"page:{name}"])["status"] == "published"
    return one_published


def test_the_pages_cover_the_history_once_each(five_attempts: str) -> None:
    first = _compose(five_attempts, limit=2)
    assert first["total"] == 5
    assert first["shown"] == 2
    assert first["next_cursor"] is not None

    second = _compose(five_attempts, limit=2, cursor=first["next_cursor"])
    third = _compose(five_attempts, limit=2, cursor=second["next_cursor"])
    assert third["shown"] == 1
    # The last page reached the beginning of the history, so there is no
    # "load more" that could only return nothing.
    assert third["next_cursor"] is None

    seen = [
        row["full_generation_id"]
        for page in (first, second, third)
        for row in page["generations"]
    ]
    assert len(seen) == 5
    assert len(set(seen)) == 5


def test_a_new_attempt_between_two_pages_shifts_no_row(five_attempts: str) -> None:
    """The reason the cursor names a checkpoint and not an offset.

    With an offset, publishing between two reads pushes every row down one and
    the second page repeats the last row of the first. The reader would see an
    attempt twice and never notice.
    """
    first = _compose(five_attempts, limit=2)
    assert _publish(five_attempts, ["page:zeta"])["status"] == "published"

    second = _compose(five_attempts, limit=2, cursor=first["next_cursor"])
    first_ids = {row["full_generation_id"] for row in first["generations"]}
    second_ids = {row["full_generation_id"] for row in second["generations"]}
    assert first_ids.isdisjoint(second_ids)
    # The new attempt is newer than page one, so no page of this walk holds it.
    # It is not lost: a fresh page one shows it.
    assert len(_compose(five_attempts, limit=2)["generations"]) == 2
    assert _compose(five_attempts, limit=50)["total"] == 6


def test_an_unknown_cursor_is_refused_rather_than_answered(
    five_attempts: str,
) -> None:
    with pytest.raises(UnknownGenerationCursor) as refusal:
        _compose(five_attempts, limit=2, cursor="okg:nope:not:a:checkpoint")
    assert "okg:nope:not:a:checkpoint" in str(refusal.value)


def test_a_single_page_history_offers_no_cursor(one_published: str) -> None:
    payload = _compose(one_published, limit=50)
    assert payload["shown"] == payload["total"] == 1
    assert payload["next_cursor"] is None


# --------------------------------------------------------------------------- #
# the blocked payload the frontend contract test reads                         #
# --------------------------------------------------------------------------- #
#: The payload the console's own contract test parses, produced HERE by the
#: publisher's refusal path so it can never drift into a fixture written to
#: fit the parser.
COMMITTED_BLOCKED_PAYLOAD = (
    Path(__file__).resolve().parents[3]
    / "pact/changes/operator-console-live-updates/evidence/blocked-generations.json"
)


def _shape(value: Any, path: str = "") -> dict[str, str]:
    """Every leaf's path and type, so two payloads can be compared by SHAPE.

    Not by value: ids and timestamps differ on every run. What must not drift
    is which keys exist and what type each holds, because that is exactly what
    the console's parsers read.
    """
    out: dict[str, str] = {}
    if isinstance(value, dict):
        for key, item in sorted(value.items()):
            out.update(_shape(item, f"{path}.{key}" if path else str(key)))
    elif isinstance(value, list):
        # One entry per list, from its first element: the rows of a list are
        # homogeneous here and comparing every index would make the shape
        # depend on how many attempts a run happened to publish.
        out[f"{path}[]"] = "list"
        if value:
            out.update(_shape(value[0], f"{path}[0]"))
    else:
        out[path] = type(value).__name__
    return out


def test_the_committed_blocked_payload_still_matches_what_the_composer_emits(
    one_published: str,
) -> None:
    """The committed payload is COMPARED on every run, not only written.

    The console's contract test parses this file. Written but never checked,
    it would keep passing while the composer moved underneath it, and the
    frontend would be proved against a payload no deployment returns any more.

    Set OKG_CAPTURE_BLOCKED_PAYLOAD=<path> to rewrite it instead.
    """
    target = int(_compose(one_published)["generations"][0]["generation_id"])
    _insert_view_run(
        one_published,
        definition_id="view:contributor_graph",
        target_generation_id=target,
        status="failed",
        error={
            "type": "ValueError",
            "message": "renderer raised while building contributor_graph",
            "traceback": [
                "Traceback (most recent call last):",
                '  File "views/contributor_graph.py", line 88, in render',
                "    raise ValueError(missing)",
            ],
        },
    )
    assert _publish(one_published, ["page:beta"], refuse=True)["status"] != "published"

    payload = _compose(one_published, limit=50)
    statuses = sorted({row["status"] for row in payload["generations"]})
    assert statuses == ["blocked", "published"], statuses

    destination = os.environ.get("OKG_CAPTURE_BLOCKED_PAYLOAD")
    if destination:
        out = Path(destination)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps({"payload": payload}, indent=2, sort_keys=True, default=str) + "\n",
        )
        return

    committed = json.loads(COMMITTED_BLOCKED_PAYLOAD.read_text())["payload"]
    fresh_shape = _shape(payload)
    committed_shape = _shape(committed)
    missing = sorted(set(committed_shape) - set(fresh_shape))
    added = sorted(set(fresh_shape) - set(committed_shape))
    retyped = sorted(
        key for key in set(fresh_shape) & set(committed_shape)
        if fresh_shape[key] != committed_shape[key]
    )
    assert not missing, (
        "the committed payload has keys the composer no longer emits: "
        f"{missing}. Re-capture with OKG_CAPTURE_BLOCKED_PAYLOAD."
    )
    assert not added, (
        "the composer emits keys the committed payload does not have: "
        f"{added}. Re-capture with OKG_CAPTURE_BLOCKED_PAYLOAD."
    )
    assert not retyped, (
        "these keys changed type since the payload was captured: "
        + ", ".join(
            f"{key} {committed_shape[key]} -> {fresh_shape[key]}" for key in retyped
        )
    )

    # And the committed file still carries no message, traceback or path.
    raw = COMMITTED_BLOCKED_PAYLOAD.read_text()
    for leaked in ("Traceback", "renderer raised", "/Users/"):
        assert leaked not in raw, leaked
