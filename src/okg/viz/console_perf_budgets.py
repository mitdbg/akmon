"""Per-view performance budgets for the operator console (issue #445, Pillar 1).

This module is the single COMMITTED source of truth for every gating GET route
under ``/viz/api/console/``. A route is covered by one of two budget kinds --
:data:`CONSOLE_VIEW_BUDGETS` (wall clock) or :data:`CONSOLE_STATEMENT_BUDGETS`
(the number of SQL statements one read issues) -- or by a stated exclusion, and
:func:`uncovered_console_routes` fails a route that has none of the three. The
console performance test
(``tests/okg-workspace/test_console_view_performance.py``) drives this registry:
it FAILS when a view's measured ``server-timing`` (``console;dur``) exceeds its
``ci_ceiling_ms`` against a representative seeded fixture and, in opt-in live
mode, its ``prod_budget_ms`` against a real deployment. A view that is knowingly
slow until a backend fix lands may carry an explicit :class:`Waiver` referencing
that fix, so the gate stays honest instead of silently green. No view carries
one today: the last, on ``health``, was removed by issue #2547.

Two budgets, deliberately separate -- a single budget would be dishonest:

* ``ci_ceiling_ms`` -- a fixture-scale REGRESSION ceiling. The production cliff
  (``count(*)`` / ``GROUP BY`` over the ~900K-node generation-pinned graph) does
  NOT reproduce on a small CI seed, so a single production budget over the
  fixture is a vacuous PASS (every view renders in ~10ms against a 1.5s budget,
  gating nothing). The ceiling is calibrated FROM a measured fixture baseline
  (``ceiling = measured x CI_SAFETY_FACTOR``), so it catches an algorithmic
  regression (an N+1 reintroduced, a double scan) without flaking on CI noise.
  ``console;dur`` is the in-process request wall clock (perf_counter around the
  handler, no network hop under the TestClient), measured server-side -- it
  INCLUDES Postgres I/O, so it carries real jitter; the generous x8 factor and
  the floor absorb that rather than pretending the signal is noise-free.
* ``prod_budget_ms`` -- the user-facing target measured on okg-workspace (the
  numbers in issue #445). Asserted ONLY in live mode against a real deployment.
  The count-heavy views were once :class:`Waiver`-d here against issue #444;
  each waiver was removed as its view came inside budget.

Scope note -- "stats" and "overview" are OUT of this gate, honestly: the views
the issue calls "stats" (~93s) and "overview" (~11-17s) are NOT v2 console
routes. They are the legacy graph-explorer routes ``/viz/api/stats`` and
``/viz/api/overview`` (registered under the bare ``/viz/api`` prefix, not
``/viz/api/console/``), and the ``console_server_timing`` middleware that emits
``console;dur`` is scoped to ``/viz/api/console/`` -- so these two routes do not
even carry the header this gate measures. They are recorded in
:data:`KNOWN_UNGATED_LEGACY_ROUTES` so the omission is visible rather than
silent; gating them needs either a separate wall-clock harness or the v2 console
superseding them with PR #240. The three count-heavy views that ARE v2 console
routes today -- generations, source-diagnostics and health -- are covered below.
(The ``graph?source=ontology`` entry left with the Graph view in issue #1924.)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from fastapi import FastAPI

CONSOLE_PREFIX = "/viz/api/console/"

# A measured fixture latency is multiplied by this to set a regression ceiling.
# Generous on purpose: the CI runners are slower CPUs than a dev box and the
# shared CI Postgres is noisy, so a tight ceiling would flake -- and a flaky perf
# gate gets masked, defeating the whole point (issue #445). Factor 8 still trips
# on a cliff-class regression (a reintroduced N+1 / count(*) / double scan is
# 10x+ even at fixture scale) while absorbing CI variance.
CI_SAFETY_FACTOR = 8.0

# Below this floor, ``measured x CI_SAFETY_FACTOR`` would be so small that normal
# jitter trips it; sub-floor views get this fixed ceiling instead. Keeps the gate
# from flaking on a 6ms view whose 8x is 48ms.
CI_CEILING_FLOOR_MS = 300.0


@dataclass(frozen=True)
class Waiver:
    """An explicit, owner-approved suppression of a view's PRODUCTION budget.

    A waiver suppresses ONLY the live-mode ``prod_budget_ms`` assertion -- the
    view still runs and records its number (never a silent skip). It carries the
    upstream issue that will remove the need for it plus the measured live
    latency that justifies it, so the waiver is auditable and cannot rot: when
    the upstream fix lands and the view meets budget, the live test surfaces the
    now-stale waiver via an explicit fail-if-now-within-budget guard (NOT
    strict-xfail config) instead of staying green.
    """

    issue: str  # e.g. "#444"
    reason: str
    live_latency: str  # the measured okg-workspace latency that justifies it


@dataclass(frozen=True)
class ViewBudget:
    """The performance contract for one gating console view."""

    name: str  # logical view name (registry key)
    path: str  # full request path, e.g. "/viz/api/console/health"
    ci_ceiling_ms: float  # fixture-scale regression ceiling (CI-gating)
    prod_budget_ms: float  # user-facing target measured on okg-workspace (live-gating)
    query: dict[str, str] = field(default_factory=dict)
    waiver: Waiver | None = None
    # ``True`` for views that cannot be exercised against the seeded fixture
    # (e.g. they need a deployment-specific view artifact); checked in live mode
    # only. None today -- kept for the dispatcher views that arrive with #240.
    live_only: bool = False
    # ``node`` / ``subtype`` / ``source-detail`` need an id that exists in the
    # seed. The test fills this placeholder from the representative seed so the
    # request is a real 200, not a 404. ``None`` means "no seed id needed".
    needs_seed_id: str | None = None

    @property
    def label(self) -> str:
        return self.name


# Generic targets from issue #445: a warm console view should render < 1.5s, a
# cold/count one < 3s.
_WARM_BUDGET_MS = 1500.0
_COLD_BUDGET_MS = 3000.0

# ci_ceiling_ms is CALIBRATED from the measured fixture baseline that the perf
# test records (test_console_view_performance.py::test_fixture_baseline_is_recorded
# writes .scratch/console_perf_baseline.json). The recorded medians were 4-30ms
# for every view except source-diagnostics (~167ms); applying
# max(300, measured x 8) gives a 300ms floor for the fast group and 1500ms for
# source-diagnostics. (The runtime-readiness route, the other slow view, was
# deleted by issue #2258: no console view read it.) Re-record + re-derive if a
# view's real cost moves; do not hand-tune a single number to make a red gate
# green.
CONSOLE_VIEW_BUDGETS: tuple[ViewBudget, ...] = (
    ViewBudget("status", CONSOLE_PREFIX + "status", 300.0, _WARM_BUDGET_MS),
    ViewBudget("sources", CONSOLE_PREFIX + "sources", 300.0, _WARM_BUDGET_MS),
    ViewBudget("tasks", CONSOLE_PREFIX + "tasks", 300.0, _WARM_BUDGET_MS),
    ViewBudget("agents", CONSOLE_PREFIX + "agents", 300.0, _WARM_BUDGET_MS),
    ViewBudget("decisions", CONSOLE_PREFIX + "decisions", 300.0, _WARM_BUDGET_MS),
    ViewBudget("worktrees", CONSOLE_PREFIX + "worktrees", 300.0, _WARM_BUDGET_MS),
    ViewBudget(
        "subtype",
        CONSOLE_PREFIX + "subtype",
        300.0,
        _WARM_BUDGET_MS,
        needs_seed_id="name",
    ),
    ViewBudget(
        "source-detail",
        CONSOLE_PREFIX + "source-detail",
        300.0,
        _WARM_BUDGET_MS,
        needs_seed_id="name",
    ),
    # Enricher diagnostics (directly-decorated route, not in register_console_routes):
    # backs Health producer detail rows; mode=full scans enrichers + run history.
    # Not waived (the #445 table does not measure it); re-baseline if it proves
    # count-heavy at okg-workspace scale.
    ViewBudget(
        "enricher-diagnostics",
        CONSOLE_PREFIX + "enricher-diagnostics",
        300.0,
        _COLD_BUDGET_MS,
    ),
    # --- count-heavy views: once waived against #444, none waived now ---------
    # Also covers the per-attempt view-producer results (PACT
    # operator-console-live-updates): they are ONE grouped query over the
    # page's target generation ids, inside this same read, not a sibling route
    # and not a query per attempt. A separate route would have needed its own
    # budget, its own authorization and its own round trip for the same rows.
    # Measured on okg-workspace 2026-09-22 (88 checkpoints, 131.8 MB of
    # checkpoint metadata, medians of 3 warm runs on port 8800): 1.82 s before
    # the bounded generation read, 0.23 s after. The waiver is GONE -- the read
    # no longer fetches every checkpoint's full metadata, so it meets the cold
    # budget with room, and a regression that reintroduces the full fetch
    # should red the live gate rather than sit behind a suppression.
    ViewBudget(
        "generations",
        CONSOLE_PREFIX + "generations",
        300.0,
        _COLD_BUDGET_MS,
    ),
    # No waiver since issue #2547. The waiver's last cost, the runtime queue
    # and dead-letter snapshot, now takes 0.02-0.05 s, and the generation reads
    # were bounded by issue #2113. What was left was the console readiness the
    # route composes through the source-diagnostics read: its MCP graph-read
    # probe ran a 100-row Nomos-filtered sample (300 policy evaluations, about
    # 1.4-2 s) on every read. It now reuses a passing verdict while the
    # generation, catalog, active policy bundle and enforcement mode hold
    # (`McpSampleVerdicts` in `cli/deployment_readiness.py`).
    #
    # Measured on okg-workspace 2026-09-28 the way the live gate measures (the
    # real app in process, read cache emptied before each sample, median of 3
    # warm `console;dur`), baseline dev a3660efd85 and this change alternated
    # three rounds at load average 21-27: 1.41 / 1.46 / 1.59 s before, 0.85 /
    # 0.87 / 0.95 s after. The first read in a fresh process still costs
    # 2.0-3.0 s (imports and the first probe). Over HTTP with `Cache-Control:
    # no-cache` add about 1.0 s: that header also re-resolves the latest
    # generation (`refresh_to_latest`) before the read.
    #
    # Fixture baseline (test_fixture_baseline_is_recorded, one local
    # contributor Postgres, 2026-09-23): health 44.4 ms at the branch's base
    # 50a96bd45, 143.2 ms with the source-diagnostics read composed in
    # (source-diagnostics alone 88-137 ms). max(300, 143.2 x 8) = 1146 ms,
    # rounded up to 1150. The 300 ms ceiling tripped at 310.5 ms on CI run
    # 35817734765 -- the added read, not a cliff.
    ViewBudget(
        "health",
        CONSOLE_PREFIX + "health",
        1150.0,
        _WARM_BUDGET_MS,
    ),
    # okg-workspace 2026-09-22 (medians of 3 warm runs on port 8800): 2.18 s,
    # from 25 s at the start of this lane. Inside the cold budget, so the #444
    # waiver is removed -- the live suite fails a waiver on a view that now
    # meets its budget, and leaving one that says "31-36s" about a 2.2 s read
    # would be a claim nobody measured. The remaining cost is
    # `compose_console_readiness`, not a #444 count; see the note on
    # `producers` for where it sits, line by line. Re-measured on port 8822
    # for issue #2202 on a busier machine: 3.20 s with the leased rollup read,
    # 2.95 s without it; the payload differs only in live timestamps and one
    # producer's enqueue count, which moved between the two runs.
    #
    # Issue #2259 re-measured on okg-workspace 2026-09-29, dev f5f7c81ca37
    # (122,366 nodes, 741,364 edges), curl to the running console, load
    # average 13-86. A read that reuses the kept MCP sample verdict: median
    # 0.34 s over 12 reads (0.31-0.41 s). A read that recomputes it, which
    # the first read after each 300 s verdict expiry pays: median 0.94 s over
    # 5 (0.83-1.18 s). The budget is 1.5x the recomputing median, rounded up
    # to 1.4 s, so it holds for every read a user can get, not only the warm
    # one; before #2864 and #2881 that read cost 2-3 s and the budget was the
    # generic 3 s. The number of policy evaluations behind the recompute is
    # capped separately (`test_console_readiness_reads_cap_policy_evaluations`).
    ViewBudget(
        "source-diagnostics",
        CONSOLE_PREFIX + "source-diagnostics",
        1500.0,
        1400.0,
    ),
    # --- the console Home reads (PACT operator-console-live-home) ------------
    # Attempts reads the same checkpoint history the generations panel reads,
    # once, and buckets it in one pass. It is now bounded by the SPAN it is
    # asked for: the read carries a `created_at` lower bound, with two days of
    # margin for a stamp written in another UTC offset, so a 1d chart no longer
    # costs what a 90d one costs and neither costs what the whole history
    # costs. Measured on okg-workspace 2026-09-22 at `range=14d`: 1.62 s
    # before, 0.027 s after. Waiver removed with the full fetch it covered.
    #
    # `undated_count` is counted branch-wide by a separate statement that
    # parses no metadata, because a checkpoint whose stamp cannot be read
    # cannot be trusted to sort into any window -- a span-bounded scan must not
    # be the thing that counts it.
    ViewBudget(
        "attempts",
        CONSOLE_PREFIX + "attempts",
        300.0,
        _COLD_BUDGET_MS,
    ),
    # The ontology rollup is an O(1) stab of the maintained count rollup at the
    # pinned generation -- no graph scan -- so it takes the warm budget with no
    # waiver. A regression here means the rollup read stopped being O(1).
    # okg-workspace 2026-09-22, port 8822: 0.26 s with a generation read lease
    # per request, 0.11 s with none (issue #2202), byte-identical payload.
    ViewBudget(
        "ontology-rollup",
        CONSOLE_PREFIX + "ontology-rollup",
        300.0,
        _WARM_BUDGET_MS,
    ),
    # --- the console Sources read (PACT operator-console-live-sources) -------
    # producers composes the three diagnostics reads and adds no query of its
    # own, so it costs what the most expensive of them costs plus the other
    # two: source-diagnostics' budget is the floor.
    #
    # What it costs, measured rather than asserted. Against the live
    # `lane-d-home-render` deployment on 2026-09-21, with a counting proxy over
    # a real connection, the list form issued 42 statements over three sources
    # and the detail form 59. Most of those were the Chronos checkout each
    # maintained-rollup read performed -- per READ, not per producer -- and
    # that checkout took a generation read lease, a write. Since issue #2202
    # the rollup read is one statement with no lease
    # (`ChronosGraphStore.query_generation_once`): on okg-workspace
    # 2026-09-22 lease acquisitions per request fell from 3 to 2, and the two
    # left are readiness checks, not rollup reads
    # (`cli/deployment_readiness.py:1166` and `:3339`).
    #
    # `test_the_statement_count_does_not_grow_with_producers` bounds the
    # growth, not that total: on its recording double the read issues 7
    # statements on the shared connection plus 2 rollup reads and 1
    # runtime-readiness compose, identically at 5 producers and at 200. The
    # rollup read and the readiness composer cannot be counted on that
    # connection -- the first needs a real `conn.info` and a registered table,
    # the second opens its own connection from the DSN -- so that test counts
    # their INVOCATIONS instead and says so. A regression in either figure
    # means something began querying per producer, which is the defect this
    # read was built to end.
    #
    # One cost this read carries and does not need: the enricher diagnostics
    # composer scans the two fact logs on every Sources read
    # (`console_views/enricher_diagnostics.py:49` and `:56`, `SELECT source,
    # count(*) ... WHERE source LIKE '_enricher:%' GROUP BY source` over
    # `okg.node_facts` and `okg.edge_facts`). Those grow with the fact log,
    # not with the producer count, so they do not break the bound above; they
    # are recorded here rather than fixed in this change.
    #
    # Measured on okg-workspace 2026-09-22 (medians of 3 warm runs on port
    # 8800): 6.31 s, down from 60 s at the start of this lane and 2.50 s after
    # the enricher-run projection stopped re-parsing every checkpoint's
    # metadata. It is inside the cold budget now, so the waiver is removed.
    # Re-measured the same evening on port 8822 for issue #2202: 3.59 s with
    # the leased rollup read, 3.11 s without it, byte-identical payload.
    #
    # The largest remaining cost was `compose_console_readiness`, mostly the
    # MCP graph-read probe's 300 Nomos evaluations. Since issue #2547 the
    # console reuses a passing probe verdict while its generation, catalog,
    # policy and enforcement mode hold, and reads doctor verdicts through one
    # shared DBOS client. Measured on okg-workspace 2026-09-28 (real app in
    # process, read cache emptied per sample, median of 3, load 21-27):
    # 1.32 / 1.42 / 1.39 s before, 0.75 / 0.97 / 0.91 s after.
    #
    # Issue #2259 re-measured it with `source-diagnostics` above (same
    # deployment, commit, method and load): reusing the kept verdict, median
    # 0.48 s over 12 reads (0.45-0.59 s); recomputing it, median 1.01 s over
    # 5 (0.91-1.37 s). 1.5x the recomputing median, rounded up: 1.6 s, down
    # from the generic 3 s.
    ViewBudget(
        "producers",
        CONSOLE_PREFIX + "producers",
        1500.0,
        1600.0,
    ),
    # --- the console Explore reads (PACT operator-console-live-explore) ------
    # The production budgets below are MEASURED, not inherited from the generic
    # _WARM/_COLD targets: okg-workspace on 2026-09-22 through a read API this
    # worktree started on port 8802, one warm-up request discarded and the
    # median of three warm runs taken as wall clock at the socket. Each budget
    # is that median x 1.5, rounded up, with a 300 ms floor for the same reason
    # CI_CEILING_FLOOR_MS exists -- a 12 ms read whose budget is 18 ms gates
    # nothing but jitter. A measured budget is tighter than the generic target
    # for all of these, so a read that doubles reds the live gate instead of
    # sitting inside a 1.5 s number nobody measured.
    #
    # The version list the console bar reads on every page. Median 569 ms.
    # That is the bounded-generation read this change landed, and it is close
    # to its own floor: the picker serves a 50-version window, 42 of this
    # branch's 88 checkpoints are published, so the window reaches the end of
    # the history and every checkpoint's status has to be learned. The
    # remaining cost is named in the change record rather than waived.
    ViewBudget(
        "versions",
        CONSOLE_PREFIX + "versions",
        300.0,
        900.0,
    ),
    # The native-view inventory and its bounded run history. Median 11.5 ms on
    # okg-workspace: the registry read is small and the run history is capped
    # by `run_limit` (<= 50) in the route's own signature. 1.5x is 18 ms, below
    # the floor, so the floor is the budget and it still catches a read that
    # started scanning run history per view.
    ViewBudget(
        "view-diagnostics",
        CONSOLE_PREFIX + "view-diagnostics",
        300.0,
        300.0,
    ),
    # The producer metric timelines behind the Sources charts. Median 456 ms
    # with no `producers` filter, which is the widest form the console asks
    # for; `max_buckets` is capped at 720 in the route signature, so the answer
    # size is bounded whatever span is asked for.
    ViewBudget(
        "producer-metrics",
        CONSOLE_PREFIX + "producer-metrics",
        300.0,
        700.0,
    ),
    # --- the three corpus-wide record reads ---------------------------------
    # These are pinned operator reads: each needs the `version` of a published
    # checkpoint, which the fixture fills from the generation its own publish
    # produced and the live suite fills from the deployment's pinned session.
    # Their cost is set by the corpus, not by any one record, which is what
    # makes them budgetable at all -- the per-record reads beside them are
    # excluded below, with the reason.
    #
    # The type counts behind Explore's landing. Median 168 ms.
    ViewBudget(
        "records-types",
        CONSOLE_PREFIX + "records/types",
        300.0,
        300.0,
        needs_seed_id="version",
    ),
    # One type's records, the list Explore pages through. Median 404 ms for a
    # full 50-record page over `verification_step`, a type this suite's own
    # fixture seeds AND okg-workspace holds 21,324 of, so the same request is a
    # real page on both. What the page costs is the page, not the type behind
    # it: `code_reference` (491,521 records) measured 457 ms and a type the
    # deployment does not hold at all -- an empty page -- 163 ms. `limit` is
    # capped at records.LIST_LIMIT_MAX in the route, so the page cannot grow.
    ViewBudget(
        "records-list",
        CONSOLE_PREFIX + "records/list",
        300.0,
        650.0,
        query={"type": "verification_step"},
        needs_seed_id="version",
    ),
    # Operator search. Measured like the three reads above it, not waived: on
    # okg-workspace 2026-09-22 through a read API this worktree started on port
    # 8803, one warm-up discarded, median of five warm runs at the socket,
    # 1.068 s for `q=deployment` -- the widest form, a saturated 200-candidate
    # page. 1.5x is 1.60 s, rounded to 1.7 s. This is the one Explore budget
    # LOOSER than the generic 1.5 s warm target, and the reason is stated below
    # rather than hidden in the number.
    #
    # What this replaced: a #1924 waiver recording 10.3-11.8 s, a fixed cost a
    # no-match query paid in full. That cost was ONE statement. The ranked BM25
    # read put its own match test (`<@> ... < 0`) in the WHERE of the ranked
    # query, and on a 695,052-record deployment the planner stopped choosing
    # the BM25 index for it: `EXPLAIN (ANALYZE, BUFFERS)` showed an Index Scan
    # on the `live_hi` btree computing the BM25 score as a per-row Filter,
    # 697,178 rows removed, 10.2 s -- the full-table scan the BM25 path was
    # built to replace, reintroduced by a predicate. Moving the match test to
    # an outer query over the capped ranked page restores the index scan:
    # 9-62 ms for the same statement, same rows, same order
    # (`okg/substrate/mcp/search.py`, `_search_lexical`).
    #
    # The two costs that remain, named rather than waived, because neither is
    # the search and each is a change to a different subsystem:
    #
    # * ~0.77 s on a page with NO results:
    #   `search_profiles.scope_emptiness_disclosure` ->`scope_node_counts`
    #   (`search_profiles.py:1424`) counts live nodes for all 124 scoped
    #   subtypes so an empty page can say whether the scope itself is empty.
    #   That is a real disclosure, and it is a `count(*)`/`GROUP BY` over the
    #   generation-pinned graph -- the #444 shape -- paid only when the page is
    #   empty. Measured: `q=HLT` 1.07 s and `q=zzzznotaword` 1.06 s, against
    #   `q=CMSSW` (4 results, no disclosure) 0.26 s.
    # * ~1.0 s on a page WITH results: the nomos read filter,
    #   `nomos/read_filter.py:169`, one runtime-boundary evaluation per row
    #   over the 200-row candidate page (600 evaluations for `q=deployment`).
    #   This is the same per-row cost the `producers` note above records for
    #   `_mcp_status`, met here on a different read.
    #
    # Both are visible in this budget: a regression that reintroduces a scan
    # reds the live gate at 1.7 s instead of sitting behind a suppression, and
    # bounding either cost should tighten this number rather than widen it.
    ViewBudget(
        "records-search",
        CONSOLE_PREFIX + "records/search",
        300.0,
        1700.0,
        query={"q": "deployment"},
        needs_seed_id="version",
    ),
    # The Graph view's type map (PACT console-graph-views): the maintained
    # count and ontology rollups at the pinned version, no graph scan, so it
    # takes the warm budget. okg-workspace 2026-09-28, curl to the server,
    # 3 cold and 6 warm reads: 0.72-1.14 s at a load average of 13.6-29.8.
    ViewBudget(
        "graph-types",
        CONSOLE_PREFIX + "graph/types",
        300.0,
        _WARM_BUDGET_MS,
        # No `version`: the route reads the version the server pins.
    ),
)

@dataclass(frozen=True)
class StatementBudget:
    """The cost contract for a console read gated on STATEMENT COUNT.

    A ``ViewBudget`` answers "is this view slow against the CI fixture". For a
    per-attempt read that is not the question that decides whether it survives
    a real deployment: what matters is whether its cost GROWS with the history
    it is read against. That is measured as the number of SQL statements one
    read issues, and it is only meaningful on the real query path -- so this
    budget is enforced by the Postgres test named in ``enforced_by``, against
    real publishes, rather than by the TestClient perf suite that drives the
    ``ViewBudget`` entries above.

    A route covered here counts as covered by :func:`registry_paths`, so it
    satisfies the completeness gate: it has a declared, tested budget. It is
    NOT a wall-clock exemption -- see the note on the entry itself.
    """

    name: str  # logical view name (registry key)
    path: str  # full request path
    ceiling: int  # statements one read may issue
    measured: int  # what one read actually issued when last measured
    measured_on: str  # the date of that measurement
    constant_in: str  # the quantity the count must NOT grow with
    enforced_by: str  # the test that fails when either half breaks


# The attempt-detail read (PACT operator-console-live-updates) is gated on
# statement count rather than fixture wall clock. Measured 2026-09-21 on a
# fresh two-attempt deployment: 50 statements for one read; 51 since
# 2026-09-22, when the read moved off the unbounded `list_generations` onto a
# bounded window and counts the history with one separate statement so its
# "attempt N of M" still names the whole history; 43 since issue #2202, when
# the read's maintained-rollup reads stopped taking a generation read lease
# (8 statements: the pin write, its probes and its commit); 44 since PACT
# console-wording-and-labels-round, whose one grouped statement over the run
# diagnostics names the sources whose runs wrote into the version. The ceiling is 60
# -- stated above the measurement rather than pinned to it, so a one-statement
# change elsewhere does not red the file while a doubling does. The half that
# actually protects a real deployment is the CONSTANT: three further published
# attempts cost the same 50 statements, so an N+1 over the history, the gates
# or the sources fails the test rather than production.
#
# It carries no ``ViewBudget`` today, and that is a gap rather than a claim:
# the wall-clock suite requests each registry entry against the seeded fixture,
# and this route needs an ``id`` naming a checkpoint that ``_request_for`` has
# no seed value for, so an entry added now would measure a 4xx. Threading the
# seed's own attempt id through that fixture is the work that would let it join
# the latency gate.
CONSOLE_STATEMENT_BUDGETS: tuple[StatementBudget, ...] = (
    StatementBudget(
        name="attempt-detail",
        path=CONSOLE_PREFIX + "attempts/detail",
        ceiling=60,
        measured=44,
        measured_on="2026-09-23",
        constant_in="the number of published attempts in the deployment history",
        enforced_by=(
            "tests/substrate/console_views/test_attempt_detail_live_reads.py"
            "::test_the_read_does_not_issue_one_query_per_attempt"
        ),
    ),
)


def statement_budget(name: str) -> StatementBudget:
    """The declared statement budget for ``name``.

    Raises ``KeyError`` for an undeclared name rather than returning a default:
    a test that silently measured against a made-up ceiling would gate nothing.
    """
    for budget in CONSOLE_STATEMENT_BUDGETS:
        if budget.name == name:
            return budget
    raise KeyError(f"no console statement budget is declared for {name!r}")


# Console GET routes intentionally NOT budget-gated, each with a stated reason.
# The completeness check requires every live console GET route to be either in
# CONSOLE_VIEW_BUDGETS or here -- so a newly added view cannot dodge the budget.
def _per_record_exclusion(what: str) -> str:
    """The stated reason a per-record console read carries no budget.

    Shared by the five ``records/*`` reads that name one record: each needs an
    ``id``, and what it costs is set by THAT record rather than by the corpus,
    so a ceiling measured against a seeded 40-byte record would gate nothing
    and a live run would measure whichever record the harness happened to
    name. The three corpus-wide record reads beside them -- ``types``,
    ``list`` and ``search`` -- carry real budgets. Making these gateable means
    seeding a WORST-CASE record (the largest payload, the largest artifact)
    and threading its id through the perf suite's ``_request_for``; that is
    follow-up work, and this reason is a gap recorded in the open, not a claim
    that these reads are cheap.
    """
    return (
        f"{what} Per-record operator read: its cost is set by the one record "
        "it names, not by the corpus, so neither a fixture ceiling nor a live "
        "budget would measure anything repeatable. See _per_record_exclusion."
    )


EXCLUDED_CONSOLE_ROUTES: dict[str, str] = {
    CONSOLE_PREFIX + "view": (
        "GET dispatcher keyed by a deployment-specific view_def_id; not seedable "
        "in the fixture. The slow screen-level views the issue calls 'stats' and "
        "'overview' are okg-workspace view artifacts whose concrete wiring lands "
        "with the operator UI app; they are waived until that app lands, and get "
        "entries then."
    ),
    CONSOLE_PREFIX + "records/detail": _per_record_exclusion(
        "The record page's own read."
    ),
    CONSOLE_PREFIX + "records/raw": _per_record_exclusion(
        "The record's stored source bytes, streamed."
    ),
    CONSOLE_PREFIX + "records/payload": _per_record_exclusion(
        "The record's structured payload."
    ),
    CONSOLE_PREFIX + "records/artifact": _per_record_exclusion(
        "The record's artifact metadata."
    ),
    CONSOLE_PREFIX + "records/artifact/bytes": _per_record_exclusion(
        "The artifact file itself, whose cost is its size."
    ),
    CONSOLE_PREFIX + "graph/record": _per_record_exclusion(
        "The Graph view's neighbourhood of one record, bounded by a 400-link "
        "access-check budget (graph_record.GRAPH_CHECK_BUDGET; 3.4-8.6 s on "
        "okg-workspace 2026-09-28 at a load average of 37-50)."
    ),
    CONSOLE_PREFIX + "graph/ontology": (
        "The Graph view's declared-versus-populated read composes the "
        "deployment's LinkML schema files, which the perf fixture's deployment "
        "does not have, so a fixture ceiling would time a refusal. Live "
        "okg-workspace 2026-09-28: 1.4-2.4 s. Budgeting it means seeding a "
        "schema directory in the perf fixture; recorded here as that gap."
    ),
    CONSOLE_PREFIX + "graph/source-links": (
        "The Graph view's source-by-source grid is computed on the first "
        "request for a version and then served from a per-version cache, so a "
        "median over repeated reads times the cache. Live okg-workspace "
        "2026-09-28: 10.4-14.2 s on first request, 0.15-0.36 s after. A "
        "publish-time count would make it budgetable; that is an open decision."
    ),
}


# pact-review is a POST mutation, not a read view; it is excluded by the
# GET-only filter, not by name. Recorded here for the reader.

# Slow operator-facing reads that this gate does NOT cover, recorded so the gap
# is visible instead of silent. These are the legacy graph-explorer routes the
# issue calls "stats"/"overview": they live under the bare /viz/api prefix (not
# /viz/api/console/), so console_get_routes() never sees them AND the
# console_server_timing middleware does not emit console;dur for them -- this
# gate's measurement does not apply. Gating them is follow-up work (a wall-clock
# harness, or the v2 console superseding them with PR #240); do NOT pretend they
# are waived here.
KNOWN_UNGATED_LEGACY_ROUTES: dict[str, str] = {
    "/viz/api/stats": "legacy graph-explorer 'stats' (measured at ~93s); no console;dur header",
    "/viz/api/overview": "legacy graph-explorer 'overview' (measured at ~11-17s); no console;dur header",
}


def console_get_routes(app: FastAPI) -> set[str]:
    """Every GET route the app serves under ``/viz/api/console/``.

    This is read from the LIVE app, so a route added in ``router_groups`` shows
    up here and the completeness check forces it into the registry or the
    excluded set.
    """
    routes: set[str] = set()
    for route in app.routes:
        path = getattr(route, "path", None)
        methods = getattr(route, "methods", None) or set()
        if path and path.startswith(CONSOLE_PREFIX) and "GET" in methods:
            routes.add(path)
    return routes


def registry_paths() -> set[str]:
    """Console route paths covered by an explicit budget, of either kind.

    A wall-clock ``ViewBudget`` and a ``StatementBudget`` both count: the
    completeness check asks whether a route ships with a declared, tested
    budget, not which cost it is measured in.
    """
    return (
        {b.path for b in CONSOLE_VIEW_BUDGETS}
        | {b.path for b in CONSOLE_STATEMENT_BUDGETS}
    )


def uncovered_console_routes(app: FastAPI) -> set[str]:
    """GET console routes that have neither a budget nor an exclusion reason."""
    covered = registry_paths() | set(EXCLUDED_CONSOLE_ROUTES)
    return console_get_routes(app) - covered


def waived_view_names() -> set[str]:
    """Names of views whose production budget is waived (none since issue #2547)."""
    return {b.name for b in CONSOLE_VIEW_BUDGETS if b.waiver is not None}


def ci_breach_message(name: str, measured_ms: float, ceiling_ms: float) -> str:
    """The canonical perf-breach failure message.

    Defined here, in the source of truth, so the perf test and the junit_gate
    classification test cannot drift on the exact text. Deliberately free of
    junit_gate's INFRA markers (no "connection timeout expired" / Postgres-crash
    signatures), so ``test-results-gate`` classifies a breach as REAL (blocking),
    not transient-tolerated. ``tests/ci/test_junit_gate.py`` asserts that
    property against this exact string.
    """
    return (
        f"console view '{name}' exceeded its fixture ceiling: "
        f"{measured_ms:.1f}ms > {ceiling_ms:.1f}ms (server-side console;dur "
        f"median). This is a performance REGRESSION gate; "
        f"investigate the view's query before raising the ceiling."
    )


__all__ = [
    "CONSOLE_PREFIX",
    "CI_SAFETY_FACTOR",
    "CI_CEILING_FLOOR_MS",
    "Waiver",
    "ViewBudget",
    "CONSOLE_VIEW_BUDGETS",
    "StatementBudget",
    "CONSOLE_STATEMENT_BUDGETS",
    "statement_budget",
    "EXCLUDED_CONSOLE_ROUTES",
    "KNOWN_UNGATED_LEGACY_ROUTES",
    "console_get_routes",
    "registry_paths",
    "uncovered_console_routes",
    "waived_view_names",
    "ci_breach_message",
]
