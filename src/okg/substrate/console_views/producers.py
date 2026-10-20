"""One read for every producer of a deployment, in one row shape.

The console's Sources view is one table of everything that puts records into
the graph: the configured sources, the enrichers that derive from them, and
the views that build rows. Until now that table was three reads folded in the
browser, and the fold had to guess which runtime key belonged to which
configured name. The identity is the registry's (`producer_identity`), and the
shape is this module's.

This composer adds no query of its own. It projects the three diagnostics
composers, which between them issue one grouped statement per underlying
table: the maintained count rollup, `okg.source_progress`, and
`okg.v_source_run_diagnostics` read with `registry_source_name = ANY(...)`.
Nothing here runs a statement per producer.

It sits beside `console/source-diagnostics` rather than replacing it. The old
Home and Health views still ship and still read the old routes; the slice that
deletes those views deletes whatever route then has no reader.

Every field below names where it comes from. A field the reads do not supply
is `null` beside the reason they did not, never a zero or a blank standing in
for an unknown.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, TYPE_CHECKING

from okg.substrate.mcp.session import Session

if TYPE_CHECKING:  # pragma: no cover - typing only
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupCache,
    )


#: How many record types one producer's detail lists at most. A deployment
#: with more types than this says how many it omitted rather than trimming
#: silently.
RECORD_TYPE_LIMIT = 40

#: How many runs one detail answer carries at most.
RUN_LIMIT_MAX = 50
#: How many producers one list answer carries at most.
#:
#: 200, not 500, because 200 is the ceiling the read can honour:
#: `compose_source_diagnostics` clamps its own `source_limit` to 200, so
#: every value from 201 up was accepted here and silently served as 200. A
#: read must not declare a ceiling it then quietly lowers. Raising this
#: number means raising that composer's cap first.
PRODUCER_LIMIT_MAX = 200

SCHEMA = "okg.console.producers.v1"

KIND_SOURCE = "source"
KIND_ENRICHER = "enricher"
KIND_VIEW = "view"


class ProducersParameterError(ValueError):
    """A request parameter this read refuses, named rather than clamped."""

    def __init__(self, parameter: str, detail: str) -> None:
        super().__init__(f"{parameter}: {detail}")
        self.parameter = parameter
        self.detail = detail


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _int_param(
    name: str,
    value: Any,
    *,
    default: int,
    maximum: int,
    minimum: int = 0,
) -> int:
    """A strict integer parameter: refused by name, never silently clamped.

    `minimum` is 1 for a page size. Zero looks harmless and is not: a
    `run_limit` of 0 made the truncation test `len(history) >= 0` true, so a
    producer that ran three times came back as a truncated history with its
    run count withheld, and a `producer_limit` of 0 reached `max(1, ...)`
    downstream and was answered as 1. Neither is what the caller asked for.
    """
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ProducersParameterError(
            name, f"expected an integer, got {value!r}"
        ) from None
    if parsed < minimum:
        raise ProducersParameterError(
            name, f"must be at least {minimum}, got {parsed}"
        )
    if parsed > maximum:
        raise ProducersParameterError(
            name, f"must not exceed {maximum}, got {parsed}"
        )
    return parsed


# --- run adaptation --------------------------------------------------------

def _count_or_none(value: Any) -> int | None:
    """A recorded count, or None where the row carried none."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _adapt_run(run: Mapping[str, Any]) -> dict[str, Any]:
    """One `okg.v_source_run_diagnostics` row in the view's vocabulary.

    The view's columns are `status`, `observed_at`, `input_count`,
    `output_count` and `total_ms`; the page speaks of an outcome, a time, items
    out, source events in, and a duration. Only the names change here, except
    that `items_in` is null: `input_count` is not a count of items read (see
    below). `phase` is the
    slowest phase the run recorded, which is the only phase the view has, and
    it is null when the run recorded no timings.
    """
    status = run.get("status")
    return {
        "iso": run.get("observed_at") or run.get("started_at"),
        # v_source_run_diagnostics.status is upper case ("OK").
        "outcome": str(status).lower() if status is not None else None,
        # No read records how many items a run READ. The view's `input_count`
        # is what `ingest/service.py` writes: the number of pushed source
        # events handed to the run, which is 0 for every pulled run -- every
        # Archiv3 run served "0 in", including one that produced 9,857. It
        # is served under its own name and the items-in count is null with
        # the reason.
        "items_in": None,
        "items_in_unavailable_reason": "no_read_records_items_read_by_a_run",
        "source_events_in": run.get("input_count"),
        "items_out": run.get("output_count"),
        "duration_ms": run.get("total_ms"),
        "phase": run.get("slowest_phase"),
        # `phase` is the SLOWEST phase, and it is served under that name too so
        # a page cannot label it as the phase the run stopped in. The last
        # phase is not recoverable: `timings_ms` is a JSON object keyed by
        # phase, and a jsonb object keeps no insertion order, so no read
        # knows which phase came last. Null with the reason, never a guess.
        "slowest_phase": run.get("slowest_phase"),
        "last_phase": None,
        "last_phase_unavailable_reason": "run_timings_record_no_phase_order",
        # What the run wrote, split the way the page states it. The view's
        # `output_count` is the two added together (jira: 1,263 = 571 + 692).
        # Null for a run row that carries neither column (an enricher's or a
        # view's), never a zero.
        "records_written": _count_or_none(run.get("nodes_appended")),
        "relationships_written": _count_or_none(run.get("edges_appended")),
        "run_id": run.get("run_id"),
        # There is deliberately no error text. `okg.v_source_run_diagnostics`
        # records no message column, so a run that failed is named by its
        # outcome and the phase it stopped in. An `error_text` key here read
        # as "this run recorded no message" when the truth is that no read
        # records one at all, which is a different fact and the reason the
        # view must not print an error line for a source.
        "error_text_unavailable_reason": "no_message_column_in_run_diagnostics",
    }


def _adapt_latest_run(latest: Mapping[str, Any]) -> dict[str, Any] | None:
    """The latest-run summary, or null with the reason the read gave.

    The summary's `status` is its own availability marker ("available"), not
    the run's outcome; the run's recorded status is `run_status` beside it.
    Passing the summary straight to `_adapt_run` served `outcome:
    "available"` on the list for a run the detail served as `"ok"` -- one
    read reporting one run two ways. The outcome here is the run's status or
    it is null with the reason, never the marker.
    """
    if str(latest.get("status") or "") != "available":
        return None
    adapted = _adapt_run(latest)
    run_status = latest.get("run_status")
    adapted["outcome"] = (
        str(run_status).lower() if run_status is not None else None
    )
    adapted["outcome_unavailable_reason"] = (
        None if run_status is not None
        else "no_run_status_in_latest_run_summary"
    )
    return adapted


# --- one row per producer --------------------------------------------------

def _source_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A `console/source-diagnostics` source, in the one row shape.

    name            <- source_registry.yaml document key
    ownership_id    <- source_registry.yaml `ownership_id` (the runtime key)
    configured      <- whether the registry names it
    health          <- source-diagnostics `health`
    workflow        <- runtime readiness `convergence`
    contribution    <- the maintained console count rollup
    freshness       <- runtime readiness `source_freshness`, verbatim
    latest_run      <- okg.v_source_run_diagnostics, newest row
    """
    contribution = _mapping(row.get("contribution"))
    workflow = _mapping(row.get("workflow"))
    freshness = _mapping(row.get("freshness"))
    health = _mapping(row.get("health"))
    latest = _mapping(row.get("latest_run"))
    return {
        "name": row.get("name"),
        "ownership_id": row.get("source_key"),
        "kind": KIND_SOURCE,
        "row_key": f"{KIND_SOURCE}:{row.get('source_key')}",
        "configured": bool(row.get("in_registry")),
        "health": {
            "status": health.get("status"),
            "incident_count": health.get("incident_count"),
        },
        "workflow": {
            "state": workflow.get("state"),
            "status": workflow.get("status"),
            "active": workflow.get("active"),
            # No recorded issue for a source, and the reason why rather than a
            # key that reads as "the publisher recorded none". `_workflow_summary`
            # composes a source's workflow from runtime readiness `convergence`,
            # which records a state, an active-work map and stages -- no policy
            # reason of any kind. The two keys this row used to carry
            # (`withheld_reason`, `excluded_reason`) were therefore null for
            # every source of every deployment, while the field map said the
            # publisher wrote them.
            "latest_issue": None,
            "latest_issue_unavailable_reason": (
                "no_read_records_a_policy_reason_for_a_source"
            ),
            # Not a policy reason -- there is none for a source -- but the
            # code the convergence read recorded beside the state. The two are
            # decided together by `_source_convergence_row`, so a page given
            # the state alone can say a source is blocked and not say by what.
            "reason": workflow.get("reason"),
            "reason_unavailable_reason": (
                None if workflow.get("reason") is not None
                else "convergence_recorded_no_reason_for_this_source"
            ),
            "blocking_stage": workflow.get("blocking_stage"),
            # What the convergence read names as the way back: a command, or
            # why there is none and which constraint (a credential, a manual
            # run) stands in the way. The console starts nothing; the
            # producer page prints this so the reader knows who can.
            "recovery": workflow.get("recovery") or {
                "command": None,
                "command_unavailable_reason": "no_recovery_action_recorded",
                "needs_credential": None,
                "needs_manual_run": None,
            },
        },
        "contribution": {
            "records_in_gen": contribution.get("records_in_gen", 0),
            "edges_in_gen": contribution.get("edges_in_gen", 0),
            "horizon": "pinned_generation",
        },
        # Carried verbatim. On a deployment whose runtime supplies no
        # watermark this is the literal state `not_evaluated`, which the view
        # prints by name; it is never blanked or turned into an age.
        "freshness": freshness or {"status": "not_evaluated"},
        "latest_run": _adapt_latest_run(latest),
        "latest_run_unavailable_reason": (
            latest.get("unavailable_reason")
            if str(latest.get("status") or "") != "available"
            else None
        ),
        "run_count": None,
        "run_count_unavailable_reason": "run_count_is_a_detail_read",
    }


def _enricher_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A `console/enricher-diagnostics` enricher, in the one row shape."""
    contribution = _mapping(row.get("contribution"))
    run_summary = _mapping(row.get("run_summary"))
    latest = _mapping(row.get("latest_run"))
    return {
        "name": row.get("name"),
        "ownership_id": row.get("producer_key"),
        "kind": KIND_ENRICHER,
        "row_key": f"{KIND_ENRICHER}:{row.get('producer_key')}",
        # An enricher exists because the deployment's registry wires it.
        "configured": True,
        "health": {
            "status": row.get("status"),
            "incident_count": None,
        },
        "workflow": {
            "state": row.get("status"),
            "status": row.get("status"),
            "active": None,
            # Carried whatever the state, because the enricher read records
            # one in three of them and each is a recorded class or a fixed
            # sentence, never a message: `failed:<ExceptionClassName>` for
            # `failing`, the withheld prefix plus the source names the
            # publisher held on for `withheld`, and the literal "0 edges
            # emitted since wiring" for `wired-but-silent`. Taking it for
            # `withheld` alone left the page with nothing to say about the
            # other two.
            "latest_issue": row.get("latest_issue"),
            "latest_issue_unavailable_reason": (
                None if row.get("latest_issue") is not None
                else "enricher_records_no_issue_in_this_state"
            ),
            "reason": None,
            "reason_unavailable_reason": "an_enricher_has_no_convergence_row",
            "blocking_stage": None,
        },
        "contribution": {
            "records_in_gen": contribution.get("records_in_gen", 0),
            "edges_in_gen": contribution.get("edges_in_gen", 0),
            "horizon": contribution.get("horizon") or "pinned_generation",
        },
        # The enricher read records an age, not a watermark state.
        "freshness": {
            "status": "last_run_age_only",
            "age": row.get("last_run_age"),
        },
        "latest_run": _adapt_run(latest) if latest else None,
        "latest_run_unavailable_reason": (
            None if latest else "no_enricher_run_rows"
        ),
        "run_count": run_summary.get("run_count"),
        "run_count_unavailable_reason": (
            None if run_summary.get("run_count") is not None
            else "enricher_run_summary_absent"
        ),
    }


def _view_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """A `console/view-diagnostics` view, in the one row shape.

    A view builds rows; it emits no relationships, so its `edges_in_gen` is
    null with a reason rather than a zero that reads as "none this time".
    """
    contribution = _mapping(row.get("contribution"))
    latest = _mapping(row.get("latest_run"))
    return {
        "name": row.get("name"),
        "ownership_id": row.get("producer_key") or row.get("name"),
        "kind": KIND_VIEW,
        "row_key": (
            f"{KIND_VIEW}:{row.get('producer_key') or row.get('name')}"
        ),
        "configured": True,
        "health": {
            "status": row.get("status"),
            "incident_count": None,
        },
        "workflow": {
            "state": row.get("status"),
            "status": row.get("status"),
            "active": None,
            # What the view read recorded about this view, verbatim.
            #
            # This was hard-nulled with the reason
            # `view_diagnostics_records_no_issue`, which is not true: `_issue`
            # in `console_views/view_diagnostics.py` composes one, and every
            # one of okg-workspace's eight views carries "No materialization
            # run is recorded." Nothing downstream could see it, so a console
            # reading this route printed "The run recorded no error text" over
            # a read that had recorded exactly why. The reason now fires only
            # where the read really carried none.
            "latest_issue": row.get("latest_issue"),
            "latest_issue_unavailable_reason": (
                None if row.get("latest_issue")
                else "view_diagnostics_recorded_no_issue_for_this_view"
            ),
            "reason": None,
            "reason_unavailable_reason": "a_view_has_no_convergence_row",
            "blocking_stage": None,
        },
        "contribution": {
            "records_in_gen": contribution.get("records_in_gen", 0),
            "edges_in_gen": None,
            "edges_unavailable_reason": "a_view_emits_no_relationships",
            "horizon": "pinned_generation",
        },
        "freshness": {
            "status": "last_run_age_only",
            "age": row.get("last_run_age"),
        },
        "latest_run": _adapt_run(latest) if latest else None,
        "latest_run_unavailable_reason": None if latest else "no_view_run_rows",
        "run_count": None,
        "run_count_unavailable_reason": "view_run_summary_absent",
    }


#: Keys a read_authority sub-tree must never carry into the payload, whatever
#: depth they sit at. `error` is `str(exc)`: on a database or a connection
#: failure that is a table name, a fragment of SQL, the host the driver dialled
#: and, on the usual DSN spelling, the password with it. `view` is the database
#: object behind an authority, which is the server's business -- a reader acts
#: on the authority's name and its reason code.
WITHHELD_AUTHORITY_KEYS = frozenset({"error", "view"})


def _withhold(node: Any) -> Any:
    """One authority sub-tree with every withheld key dropped, at any depth.

    `compose_producers` carries another composer's `read_authority` through
    verbatim, so what that composer chooses to record is what this read
    serves. This walks whatever shape arrives rather than naming the keys
    today's version happens to have: a sub-read added later cannot smuggle a
    message through by nesting it one level deeper than this read knew about.

    What survives is what the enricher and view branches below already serve:
    the status, the reason code and the exception's class.
    """
    if isinstance(node, Mapping):
        return {
            str(key): _withhold(value)
            for key, value in node.items()
            if str(key) not in WITHHELD_AUTHORITY_KEYS
        }
    if isinstance(node, list):
        return [_withhold(value) for value in node]
    return node


def _records_by_type(
    conn: Any,
    *,
    generation_id: Any,
    ownership_id: str,
    count_rollup: ConsoleCountRollupCache | None = None,
) -> dict[str, Any]:
    """This producer's live records, grouped by record type.

    Recorded, and keyed the same way the producer's total is: the rollup's
    `source_subtype` dimension is `<producer>\x1f<subtype>`, and its producer
    domain is by construction exactly the `source` dimension's domain -- the
    same expression writes both. So joining on the registry's declared
    `ownership_id` here is the join that already produces the row's total, not
    a second guess at identity.

    ONE grouped read of one table, whatever the deployment's producer count.

    There is deliberately no relationships-by-type beside it. The rollup has
    seven dimensions and none of them pairs an edge with its producer:
    `edge_source` counts a producer's edges with no type, and `edge_type`
    counts a type's edges across every producer. Multiplying the two would
    invent a distribution no read recorded, so the view says the figure is not
    recorded instead.
    """
    try:
        from okg.substrate.console_views.console_count_rollup_read import (
            COUNT_KIND_SOURCE_SUBTYPE,
            rollup_reader,
        )
        from okg.substrate.derived_artifacts.maintained_derivation_constants import (
            parse_source_subtype,
        )

        rolled = rollup_reader(
            count_rollup, conn, generation_id=generation_id
        ).read({COUNT_KIND_SOURCE_SUBTYPE})
    except Exception as exc:  # noqa: BLE001 - name the authority that failed.
        return {
            "status": "unavailable",
            "reason": "console_count_rollup_unavailable_for_this_generation",
            "error_type": type(exc).__name__,
            "types": [],
            "total": None,
            "omitted": 0,
        }

    rows: list[dict[str, Any]] = []
    for (_kind, value), count in rolled.items():
        # A malformed dimension_value raises rather than being skipped; a
        # silent skip here would under-report a producer's records.
        producer, subtype = parse_source_subtype(str(value))
        if producer != ownership_id:
            continue
        rows.append({"type": subtype, "records": int(count)})

    rows.sort(key=lambda row: (-row["records"], row["type"]))
    shown = rows[:RECORD_TYPE_LIMIT]
    return {
        "status": "available",
        "types": shown,
        # The sum over EVERY type, not just the ones shown, so a reader can
        # see when the list is a part of the whole.
        "total": sum(row["records"] for row in rows),
        "omitted": len(rows) - len(shown),
    }


def _fold_enricher_source_row(
    enricher: dict[str, Any],
    rows: list[dict[str, Any]],
    disagreements: list[dict[str, Any]],
) -> dict[str, Any]:
    """One enricher, once -- not again as a source under its own prefix.

    The rollup's `source` dimension counts a node by the value in its `source`
    column, and an enricher's own output carries `_enricher:<name>` there. So
    `compose_sources`, which lists every recorded key the registry does not
    claim rather than dropping one silently, listed each enricher as a source
    named `_enricher:code_reference`; the enricher read then listed the same
    producer again as `code_reference`. Two rows, ONE `ownership_id`: a
    duplicate React key in the browser and a producer the operator counts
    twice. The registry's declared identity is what joins them, exactly as it
    joins everything else in this module -- `_enricher:code_reference` IS the
    enricher's ownership id, not a second producer that happens to look like
    one.

    The enricher row wins, and the source row is removed. It is removed rather
    than kept because it is not a source: no registry declares it and nothing
    ingests through it.

    The row keeps ITS OWN counts. The `enricher_node` dimension is the wider
    of the two -- the same expression that writes `_enricher:%` into `source`
    also attributes rows through `source_record_id->>'linker'`, which the
    `source` dimension never sees -- so overwriting the enricher's figure with
    the folded row's would UNDER-report it. What the folded row measured is
    carried beside it under its own name, so the fold is visible in the
    payload and not just in this comment.

    If the narrower dimension ever measures MORE than the wider one, the two
    recorded figures contradict each other and this read has no basis for
    choosing between them. It says so, in the row and in `count_disagreements`,
    rather than publishing a number no read supports. NOT in `omitted_data`:
    that list means "a read came back short", the page renders every entry of
    it as that sentence, and a disagreement is the opposite -- both figures
    were read in full and they contradict each other.

    An `_enricher:` source row that NO enricher answers to is left exactly
    where it was. A producer that ran and was later unwired is a real row.

    So is a CONFIGURED one. The fold rests on the removed row not being a
    source -- "no registry declares it and nothing ingests through it" -- and
    that was asserted rather than checked. Nothing stops a
    `source_registry.yaml` from declaring `ownership_id: _enricher:<name>`,
    and such a source was deleted from the one table an operator reads as
    complete. The fold now requires the row to be unconfigured; a declared
    one stays listed and the collision is named on the enricher instead.
    """
    key = enricher.get("ownership_id")
    if not key:
        return enricher
    index = next(
        (
            position for position, row in enumerate(rows)
            if row.get("kind") == KIND_SOURCE and row.get("ownership_id") == key
        ),
        None,
    )
    if index is None:
        return enricher

    if rows[index].get("configured"):
        # A declared source is never folded away. The two rows stay, and the
        # collision is named here rather than settled by deleting one of them.
        enricher["contribution"]["source_dimension_not_folded"] = {
            "dimension": "source",
            "ownership_id": key,
            "reason": (
                "a_configured_source_of_the_registry_answers_to_this_key"
            ),
            "note": (
                "the registry declares a source under this ownership id, so "
                "the row counted in the rollup's `source` dimension is that "
                "source and not this enricher's own output; both are listed "
                "and neither figure is folded into the other"
            ),
        }
        return enricher

    folded = rows.pop(index)
    contribution = _mapping(folded.get("contribution"))
    source_records = contribution.get("records_in_gen")
    source_edges = contribution.get("edges_in_gen")
    own_records = _mapping(enricher.get("contribution")).get("records_in_gen")
    agrees = not (
        isinstance(source_records, int)
        and isinstance(own_records, int)
        and source_records > own_records
    )
    enricher["contribution"]["folded_source_dimension"] = {
        "dimension": "source",
        "ownership_id": key,
        "records_in_gen": source_records,
        "edges_in_gen": source_edges,
        "agrees": agrees,
        "disagreement": (
            None if agrees else "source_dimension_exceeds_enricher_dimension"
        ),
        "note": (
            "counted under the enricher's own key in the rollup's `source` "
            "dimension; the row's figure is the wider `enricher_node` "
            "dimension, which also catches linker-attributed rows"
        ),
    }
    if not agrees:
        disagreements.append({
            "authority": "console_count_rollup",
            "reason": "source_dimension_exceeds_enricher_dimension",
            "ownership_id": key,
            "kind": KIND_ENRICHER,
            "name": enricher.get("name"),
            "affects": "enricher_contribution",
            # BOTH figures, because the page states both: neither read is
            # authority over the other and the sentence has to say so.
            "source_dimension_records": source_records,
            "enricher_dimension_records": own_records,
        })
    return enricher


def compose_producers(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path,
    dsn: str,
    session: Session,
    name: str | None = None,
    producer_limit: Any = None,
    run_limit: Any = None,
    run_offset: Any = None,
) -> dict[str, Any]:
    """Every producer of the deployment, once, in one row shape.

    With `name`, the same row plus that producer's run history, capped by
    `run_limit` and offset by `run_offset`. A name no producer answers to is a
    refusal by name, never an empty page that reads as a producer which never
    ran.
    """
    limit = _int_param(
        "producer_limit",
        producer_limit,
        default=PRODUCER_LIMIT_MAX,
        maximum=PRODUCER_LIMIT_MAX,
        minimum=1,
    )
    runs_wanted = _int_param(
        "run_limit", run_limit, default=20, maximum=RUN_LIMIT_MAX, minimum=1
    )
    offset = _int_param("run_offset", run_offset, default=0, maximum=100_000)

    from okg.substrate.console_views.enricher_diagnostics import (
        compose_enricher_diagnostics,
    )
    from okg.substrate.console_views.source_diagnostics import (
        compose_source_diagnostics,
    )
    from okg.substrate.console_views.view_diagnostics import (
        compose_view_diagnostics,
    )

    from okg.substrate.console_views.console_count_rollup_read import (
        COUNT_KIND_EDGE_SOURCE,
        COUNT_KIND_ENRICHER_EDGE,
        COUNT_KIND_ENRICHER_NODE,
        COUNT_KIND_SOURCE,
        ConsoleCountRollupCache,
    )

    # ONE rollup read for the whole request. The three composers below each
    # want different dimensions, and each read used to perform its own Chronos
    # `checkout_generation` -- which takes a generation read LEASE, and a
    # lease is a write: it deletes expired pins and inserts one. A read-only
    # console page therefore wrote to `okg.history_generation_pins` three
    # times per request (issue #2202). Announcing every dimension up front
    # means the first composer to read fetches the union and the other two are
    # served from it.
    count_rollup = ConsoleCountRollupCache(
        conn, generation_id=session.generation_id
    )
    count_rollup.prime({
        COUNT_KIND_SOURCE,
        COUNT_KIND_EDGE_SOURCE,
        COUNT_KIND_ENRICHER_NODE,
        COUNT_KIND_ENRICHER_EDGE,
    })

    read_authority: dict[str, Any] = {"generation_id": session.generation_id}
    rows: list[dict[str, Any]] = []
    omitted: list[dict[str, Any]] = []
    # Two recorded figures for one producer that contradict each other. Kept
    # apart from `omitted_data`, which means a read came back short and which
    # the page renders as exactly that sentence.
    count_disagreements: list[dict[str, Any]] = []
    # How many published versions the enricher read looked back over. Carried
    # because an enricher's latest run and run count are drawn from a WINDOW
    # of checkpoints, not from its whole history: a bounded history that does
    # not say it is bounded reads as a complete one, and an enricher whose
    # last run predates the window then reads as one that never ran.
    enricher_runs_window: int | None = None

    source_payload = compose_source_diagnostics(
        conn,
        deployment_name=deployment_name,
        deployment_dir=deployment_dir,
        dsn=dsn,
        session=session,
        selected_source=name,
        source_limit=limit,
        run_limit=runs_wanted + offset if name else 1,
        run_scope="selected" if name else "all",
        count_rollup=count_rollup,
    )
    read_authority["sources"] = _withhold(source_payload.get("read_authority"))
    for row in source_payload.get("sources") or []:
        rows.append(_source_row(row))

    # A sub-read of the sources composer that failed is carried here too.
    # Without it a failed runtime read is invisible: every row still appears,
    # each with `not_evaluated` freshness and a null state, which is exactly
    # what a healthy deployment whose runtime supplies no watermark looks
    # like. The page can only tell the two apart if the failure reaches it.
    #
    # Entries naming ONE source (`source_name`) are that source's own note and
    # are answered by the selection below; what is carried here is a
    # deployment-wide read that came back short.
    for entry in source_payload.get("omitted_data") or []:
        if not isinstance(entry, Mapping) or entry.get("source_name"):
            continue
        carried = {
            str(key): value for key, value in entry.items()
            if str(key) not in WITHHELD_AUTHORITY_KEYS
        }
        # Deliberately no `kind`. That key means "no producer of this kind is
        # listed", which is the enricher and view case below and is not this
        # one: every source is still in the table.
        carried.pop("kind", None)
        carried.setdefault("affects", "source_fields")
        omitted.append(carried)

    # An enricher or view read that fails names itself; it does not silently
    # shorten the one table the operator reads as complete.
    try:
        enricher_payload = compose_enricher_diagnostics(
            conn,
            deployment_name=deployment_name,
            session=session,
            count_rollup=count_rollup,
        )
        for row in enricher_payload.get("enrichers") or []:
            rows.append(_fold_enricher_source_row(
                _enricher_row(row), rows, count_disagreements,
            ))
        window = enricher_payload.get("enricher_runs_window")
        enricher_runs_window = int(window) if isinstance(window, int) else None
        read_authority["enrichers"] = {"status": "available"}
    except Exception as exc:  # noqa: BLE001 - name the authority that failed.
        read_authority["enrichers"] = {
            "status": "unavailable",
            "reason": "enricher_diagnostics_composer_failed",
            # The class, never the message. `str(exc)` on a database error
            # carries a table name, a column, a fragment of SQL and on some
            # drivers the DSN, into a payload a browser renders. The reason
            # code says which authority failed and the class says how; the
            # message belongs in the server log, which is where the route's
            # 503 body already tells an operator to look.
            "error_type": type(exc).__name__,
        }
        omitted.append({"authority": "enricher_diagnostics", "kind": KIND_ENRICHER})

    try:
        view_payload = compose_view_diagnostics(
            conn,
            deployment_name=deployment_name,
            deployment_dir=deployment_dir,
            session=session,
        )
        for row in view_payload.get("views") or []:
            rows.append(_view_row(row))
        read_authority["views"] = {"status": "available"}
    except Exception as exc:  # noqa: BLE001 - name the authority that failed.
        read_authority["views"] = {
            "status": "unavailable",
            "reason": "view_diagnostics_composer_failed",
            # The class, never the message. `str(exc)` on a database error
            # carries a table name, a column, a fragment of SQL and on some
            # drivers the DSN, into a payload a browser renders. The reason
            # code says which authority failed and the class says how; the
            # message belongs in the server log, which is where the route's
            # 503 body already tells an operator to look.
            "error_type": type(exc).__name__,
        }
        omitted.append({"authority": "view_diagnostics", "kind": KIND_VIEW})

    payload: dict[str, Any] = {
        "schema": SCHEMA,
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "producer_count": len(rows),
        "producers": rows,
        # Null, with the reason, when the enricher read did not answer -- never
        # a number standing in for a window nothing measured.
        "enricher_runs_window": enricher_runs_window,
        "enricher_runs_window_unavailable_reason": (
            None if enricher_runs_window is not None
            else "enricher_diagnostics_did_not_report_a_window"
        ),
        "read_authority": read_authority,
        "omitted_data": omitted,
        # Per producer, with both figures. Empty is the ordinary case, and the
        # key is always present so a page need not tell "no disagreement" from
        # "this read is too old to have looked".
        "count_disagreements": count_disagreements,
        "selected_producer": None,
        "selected": None,
    }

    if name is None:
        return payload

    payload["selected_producer"] = name
    match = next((row for row in rows if row["name"] == name), None)
    if match is None:
        payload["selected"] = {
            "found": False,
            "reason": "no_producer_of_this_deployment_answers_to_that_name",
        }
        return payload

    history: list[dict[str, Any]] = []
    truncated = False
    if match["kind"] == KIND_SOURCE:
        selected = _mapping(source_payload.get("selected"))
        history = [
            _adapt_run(run) for run in (selected.get("run_history") or [])
        ]
        # The run query is capped at what this page needs. When it came back
        # full, there may be older runs it did not fetch, so the number of
        # rows in hand is a floor and not the producer's run count. Reporting
        # it as the count would say "ran twice" of a producer that ran three
        # times, which is what asking for two runs did before this.
        truncated = len(history) >= runs_wanted + offset
        summary = _mapping(selected.get("run_summary"))
        if truncated:
            match["run_count"] = None
            match["run_count_unavailable_reason"] = (
                f"run_history_truncated_at_{runs_wanted + offset}; "
                "ask for more runs to count them"
            )
        else:
            match["run_count"] = summary.get("run_count")
            match["run_count_unavailable_reason"] = (
                None if summary.get("run_count") is not None
                else "source_run_summary_absent"
            )
    page = history[offset:offset + runs_wanted]
    payload["selected"] = {
        "found": True,
        "row": match,
        # DELIBERATELY not through the request's cache, and not in the
        # priming set above. `_records_by_type` is the one rollup read that
        # names its own unavailability INLINE -- a generation that cannot
        # serve the subtype dimension leaves the producer page standing with
        # one section marked unavailable, rather than failing the whole read.
        # Folding it into the shared read would move that failure into
        # `compose_sources`, which does not catch it, so a missing subtype
        # dimension would take the page down instead of one section. The
        # second lease is the price of that separation, and it is charged
        # only on the detail.
        "records_by_type": _records_by_type(
            conn,
            generation_id=session.generation_id,
            ownership_id=str(match["ownership_id"]),
        ),
        # Not a figure this read can give: no rollup dimension pairs an edge
        # with the producer that emitted it. Named so the page can say so
        # rather than leaving a reader to wonder where the section went.
        "relationships_by_type": {
            "status": "not_recorded",
            "reason": "no_rollup_dimension_pairs_an_edge_with_its_producer",
        },
        "run_history": page,
        "run_offset": offset,
        "run_limit": runs_wanted,
        # Rows this read fetched, which is not the producer's run total when
        # the query was capped; `row.run_count` says which it is.
        "run_history_fetched": len(history),
        "run_history_truncated": truncated,
        "more": truncated or offset + len(page) < len(history),
    }
    return payload
