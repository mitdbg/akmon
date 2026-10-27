from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path


GRAPH_RELATIONS = (
    "okg.graph_nodes",
    "okg.graph_edges",
    "okg.v_nodes",
    "okg.v_edges",
)

# Reads routed through the transform-program lowering never mention a raw
# relation string, so the literal scan above cannot see them — this is how
# inspect(subtype) shipped with no Nomos filter at all (issue #627). Any call
# whose name ends with one of these is a graph read and must either flow its
# rows through the per-row filter in the same function or be approved below
# with a justification.
PROGRAM_LOWERED_CALLS = (
    "lower_program_over_chronos",
    "execute_list_neighbors_chronos",
)


@dataclass(frozen=True)
class RawGraphRead:
    path: str
    function: str
    lineno: int
    snippet: str


REPO_ROOT = Path(__file__).resolve().parents[3]


AUDITED_FILES = (
    "src/okg/substrate/mcp/tools.py",
    "src/okg/substrate/mcp/search.py",
    "src/okg/substrate/mcp/run_sql.py",
    "src/okg/substrate/mcp/graph_sql_splice.py",
    "src/okg/substrate/mcp/session.py",
    "src/okg/substrate/mcp/set_operators.py",
    "src/okg/substrate/mcp/server.py",
    "src/okg/substrate/mcp/provenance.py",
    "src/okg/substrate/mcp/source_coverage.py",
    "src/okg/substrate/mcp/fan_out.py",
    "src/okg/substrate/mcp/schema_digest.py",
    "src/okg/substrate/mcp/schema_digest_resource.py",
    "src/okg/substrate/mcp/dataset_card.py",
    "src/okg/substrate/mcp/connector_ingest.py",
    "src/okg/substrate/cli/_read_side.py",
    "src/okg/substrate/cli/deployment.py",
    "src/okg/substrate/cli/deployment_readiness.py",
    "src/okg/substrate/exchange/outbox.py",
)


_FILTER_NODE = "filter_node_rows_for_session("
_FILTER_EDGE = "filter_edge_rows_for_session("


APPROVED_LOCATIONS: dict[str, dict[str, tuple[str, ...]]] = {
    "src/okg/substrate/mcp/tools.py": {
        "_visible_node_rows": (_FILTER_NODE,),
        "_visible_edge_rows": (_FILTER_EDGE,),
        "inspect_view": (),
        # inspect(graph) aggregate path. These read raw relations without the
        # per-row filter, but only ever run when no row can be hidden
        # (`off`/`audit` mode), or as the explicitly-labeled unfiltered
        # fallback for a large `enforce` graph. `enforce` counts that must
        # reflect per-row visibility go through _graph_summary_per_row ->
        # _visible_node_rows/_visible_edge_rows (filter-guarded above).
        "_graph_summary_sql": (),
        "_unresolved_mentions_sql": (),
        "_graph_total_rows": (),
        # inspect(subtype) mode-aware paths (issue #627): the SQL aggregate
        # and the overlay correction only run under off/audit (no row can
        # hide); the budget probe returns counts only to pick the enforce
        # branch; samples always flow through the filtered node read.
        "_subtype_summary_sql": (),
        "_subtype_total_rows": (),
        "_fold_overlay_corrections": (),
        "_subtype_sample_nodes": ("_visible_node_rows(",),
        # aggregate(edge) mode-aware paths: SQL GROUP BY under off/audit;
        # under enforce the count probe (in aggregate_edge) only sizes the
        # budget and the rows flow through _visible_edge_rows.
        "_aggregate_edge_routes_sql": (),
        "aggregate_edge": ("_visible_edge_rows(",),
        # expand's true-total probe (issue #904), same mode-aware shape as
        # aggregate_edge above: the unfiltered COUNT runs only under
        # off/audit, where no row can hide. Under enforce it sizes the
        # budget, and the count that is actually reported is taken from rows
        # that passed the per-row filter — above the budget the count is
        # omitted rather than leaked.
        "_neighbors_match_count": (
            "deployment_nomos_enforcement_mode(",
            _FILTER_EDGE,
        ),
        # inspect(node)'s per-document chunk count (issue #2669), same
        # mode-aware shape as _neighbors_match_count: the unfiltered COUNT is
        # reported only under off/audit, where no row can hide. Under enforce
        # it only sizes the exact-count budget, and the reported count is
        # taken from rows that passed _visible_edge_rows and
        # _visible_node_rows; above the budget the count is omitted.
        "_document_chunk_count": (
            "_visible_edge_rows(",
            "_visible_node_rows(",
        ),
        # expand's record rows under audit/enforce (issue #3666). The two
        # wrappers are the only graph reads of that path. Each returns only
        # rows that passed the per-row filter, called with
        # runtime_required=True and no other path, so no stored row is ever
        # returned unchecked; links are checked as counts. Only a batch's
        # size and last link id leave `_expand_visible_links` unchecked, to
        # page the stored links.
        "_expand_visible_records": (_FILTER_NODE,),
        "_expand_visible_links": (_FILTER_EDGE,),
        # expand's record rows with access rules off: links and records are
        # grouped and counted in SQL with no per-row filter. Each function
        # checks the mode itself before it reads and refuses under
        # audit/enforce, so it never runs where a row can be hidden.
        "expand_far_page": ("deployment_requires_nomos_runtime(",),
        "expand_page_links": ("deployment_requires_nomos_runtime(",),
    },
    "src/okg/substrate/mcp/search.py": {
        "_search_alias": (_FILTER_NODE,),
    },
    "src/okg/substrate/mcp/session.py": {
        # preview_advance delta scan (issue #627): (id, content-hash)
        # projection only — no row content leaves SQL; visibility-affecting
        # counts derive from _visible_delta_counts, which filters every
        # delta row per pin.
        "_id_hash_map": (),
        "_visible_delta_counts": (_FILTER_NODE, _FILTER_EDGE),
    },
    "src/okg/substrate/mcp/run_sql.py": {
        "<module>": (),
        "_expand_composite_views": (),
        "_legacy_pin_for_operational_sql": (),
        "_references_physical_graph_view": (),
        "_rewrite_graph_views": (),
        "run_sql": (
            "deployment_nomos_enforcement_mode(",
            "RunSqlInvalid(",
        ),
    },
    "src/okg/substrate/mcp/graph_sql_splice.py": {
        # Builds and runs the text of run_sql's graph-backed path (issue
        # #2899). The module-level relation map only names the base views.
        # The one caller is run_sql. Two catalog-only reads from this module
        # run BEFORE run_sql's Nomos guard: plan_graph_sql reads composite
        # view definitions (pg_get_viewdef over pg_class/pg_namespace), and
        # assert_standard_conforming_strings runs SHOW
        # standard_conforming_strings. Neither reads graph or Nomos rows.
        # The guard (refuse under enforce, record under audit) runs before
        # the checkout is read and before any caller SQL executes.
        "<module>": (),
    },
    # #3201: set_operators no longer reads the graph. Ordered filter, set
    # aggregate and filtered one-hop expand read the effective-attribute
    # relation in server.py, where each page passes the per-row read check
    # (_effective_view_sql_page, _expand_one_hop_effective) and a set
    # aggregate refuses whenever Nomos is evaluated.
    "src/okg/substrate/mcp/set_operators.py": {
        "<module>": (),
    },
    "src/okg/substrate/mcp/server.py": {
        # Admission-path existence probe: bounded to the explicit dst ids
        # the writer itself supplied; reveals only whether those ids exist.
        "_resolve_record_reference_edges": (),
        # Read-model slice 7a: names the graph tables only to resolve their
        # Chronos segment, which the reader view's SQL joins by id for each
        # effective row's read class and stored classification. It serves no
        # row: the page step that uses it (`_effective_view_sql_page`) passes
        # every row it returns through the per-row filter.
        "_reader_view_sql": ("_reader_view.open(",),
    },
    "src/okg/substrate/mcp/provenance.py": {
        # Bounded id-anchored source-column fetch for rows the caller has
        # already visibility-filtered; used to stamp read provenance.
        "serialize_edge_read_rows_at_pin": (),
        "serialize_node_read_rows_at_pin": (),
        "hydrate_node_source_rows_at_pin": (),
        # Same shape and same precondition as the source hydration above: an
        # id-anchored fetch of two extra columns for rows the caller has
        # ALREADY visibility-filtered. Its only call site is the typed attrs
        # lookup, which pages through `_effective_view_sql_page` (Nomos is
        # applied to that page) and then re-checks the residual filter in
        # Python before this runs. It widens no row set — it cannot surface a
        # node the caller could not already read, only name the source record
        # behind one it can.
        "hydrate_node_record_identity_at_pin": (),
    },
    "src/okg/substrate/mcp/fan_out.py": {
        # Single-id anchored read of the caller-supplied brief node on the
        # fan-out dispatch path.
        "_fetch_brief_attrs": (),
    },
    "src/okg/substrate/mcp/schema_digest.py": {
        # Whole-graph SQL aggregates: safe only when no row can hide. The
        # okg://schema-digest resource gates on enforcement mode before
        # calling these (schema_digest_resource.build_digest_payload omits
        # row-derived sections under `enforce`).
        "generate_schema_digest": (),
        "fetch_minimal_schema_counts": (),
    },
    "src/okg/substrate/mcp/schema_digest_resource.py": {
        "build_digest_payload": ("deployment_nomos_enforcement_mode(",),
    },
    # PACT dataset-card: the start-up card is one subject-less text for every
    # session, so its counts, sampled ids, dates and the path check run only
    # when the Nomos mode is `off`. Under any other mode (`audit` filters rows
    # per identity too) `gather_dataset_facts` returns before any row read
    # and the card says the parts are withheld. The module-level SQL is the
    # count fallback and the id sample; `_probe_path_words` is reached only
    # from past that gate.
    "src/okg/substrate/mcp/dataset_card.py": {
        "<module>": (),
        "gather_dataset_facts": ('nomos_mode != "off"',),
        "_probe_path_words": (),
    },
    "src/okg/substrate/mcp/connector_ingest.py": {},
    "src/okg/substrate/cli/_read_side.py": {
        "<module>": (),
    },
    "src/okg/substrate/cli/deployment.py": {
        "<module>": (),
        "_ready_generation_status": (),
        "_ready_mcp_status": (_FILTER_NODE,),
    },
    "src/okg/substrate/cli/deployment_readiness.py": {
        # The principal-readiness wrapper delegates to this renamed, otherwise
        # unchanged graph probe. Its bounded sample still requires the same
        # per-row filter (the precedent from issue #627).
        "_ready_mcp_graph_status": (_FILTER_NODE,),
        # Count-integrity compares whole-graph cardinality with the checkpoint
        # stamp. It returns only two aggregates, never graph rows, and must count
        # the physical pinned graph rather than subject-visible rows so a hidden
        # dangling/pruned row cannot escape the deployment health gate.
        "_ready_generation_status": (),
    },
    "src/okg/substrate/exchange/outbox.py": {
        "_visible_nodes": (_FILTER_NODE,),
        "_visible_internal_edges": (_FILTER_EDGE,),
    },
    # No literal graph relation appears in this module today: it reads the
    # RESOLVED chronos segment through an f-string, which the literal scan
    # above cannot see. That gap is covered by
    # `test_segment_interpolated_reads_stay_in_authorized_wrappers` below;
    # the file is listed here so a future literal read is caught too.
    "src/okg/substrate/mcp/source_coverage.py": {},
}


# Segment-interpolated read sites. `resolve_node_segment()` returns a physical
# table name that is then interpolated into SQL, so the relation never appears
# as a literal and `_raw_graph_reads` is blind to it — the same blind spot that
# `PROGRAM_LOWERED_CALLS` exists to cover for routed reads. Every `FROM
# {..._segment}` read must either carry the per-row filter in its own function
# or be approved here with a justification.
SEGMENT_AUDITED_FILES = ("src/okg/substrate/mcp/source_coverage.py",)

_SEGMENT_FROM_RE = re.compile(r"\bFROM\s+\{(\w*segment)\}")

APPROVED_SEGMENT_READS: dict[str, dict[str, tuple[str, ...]]] = {
    "src/okg/substrate/mcp/source_coverage.py": {
        # The `enforce` count path: rows are materialized and counted only
        # after the per-row filter, so the reported per-source counts are
        # this session's visible cardinality, not the graph's.
        "_measured_rows_enforced": (_FILTER_NODE,),
        # The off/audit aggregate. It applies no per-row filter BY DESIGN and
        # is unreachable under `enforce` — `inspect_coverage` selects it only
        # when the mode cannot hide a row, exactly as `inspect(graph)` gates
        # `_graph_summary_sql`. The guard is therefore the CALLER's mode
        # branch, which is asserted separately below.
        "_measured_rows": (),
    },
}


# Program-lowered read sites (see PROGRAM_LOWERED_CALLS). Every call site
# must either carry the per-row filter in the same function or be listed
# here with a justification.
APPROVED_PROGRAM_LOWERED: dict[str, dict[str, tuple[str, ...]]] = {
    "src/okg/substrate/mcp/tools.py": {
        "inspect_node": (_FILTER_NODE,),
        "expand_neighbors": (_FILTER_EDGE,),
        "expand_path": (_FILTER_NODE,),
        # inspect(subtype) SQL-aggregate routes: DISTINCT (edge_type,
        # subtype) aggregates, only reachable under off/audit (no row can
        # hide); the enforce path recomputes routes from filtered raw edge
        # rows in _subtype_summary_per_row.
        "_routes": (),
    },
    "src/okg/substrate/mcp/search.py": {
        # The ranked rows pass through _semantic_visible_rows, which runs
        # the per-row filter; asserted by
        # test_semantic_visible_rows_carries_the_per_row_filter below.
        "_search_semantic": ("_semantic_visible_rows(",),
    },
    "src/okg/substrate/mcp/server.py": {
        "_effective_view_sql_page": (_FILTER_NODE, _FILTER_EDGE),
        "filter": ("_effective_view_sql_page(",),
        "map": ("_effective_view_sql_page(",),
    },
}


def test_agent_visible_raw_graph_reads_stay_in_authorized_wrappers() -> None:
    failures: list[str] = []
    for rel_path in AUDITED_FILES:
        path = REPO_ROOT / rel_path
        source = path.read_text(encoding="utf-8")
        locations = APPROVED_LOCATIONS[rel_path]
        function_sources = _function_sources(source)
        for read in _raw_graph_reads(rel_path, source):
            required_tokens = locations.get(read.function)
            if required_tokens is None:
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"unapproved raw graph relation: {read.snippet}"
                )
                continue
            function_source = function_sources.get(read.function, source)
            missing = [
                token for token in required_tokens
                if token not in function_source
            ]
            if missing:
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"missing required guard(s) {missing}: {read.snippet}"
                )
    assert not failures, "\n".join(failures)


def test_program_lowered_reads_stay_in_authorized_wrappers() -> None:
    failures: list[str] = []
    for rel_path in AUDITED_FILES:
        path = REPO_ROOT / rel_path
        source = path.read_text(encoding="utf-8")
        locations = APPROVED_PROGRAM_LOWERED.get(rel_path, {})
        function_sources = _function_sources(source)
        for read in _program_lowered_reads(rel_path, source):
            required_tokens = locations.get(read.function)
            if required_tokens is None:
                # Not explicitly approved: the enclosing function must
                # apply the per-row filter itself.
                function_source = function_sources.get(read.function, "")
                if (
                    _FILTER_NODE in function_source
                    or _FILTER_EDGE in function_source
                ):
                    continue
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"program-lowered graph read without a Nomos filter "
                    f"pass or an approval entry: {read.snippet}"
                )
                continue
            function_source = function_sources.get(read.function, source)
            missing = [
                token for token in required_tokens
                if token not in function_source
            ]
            if missing:
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"missing required guard(s) {missing}: {read.snippet}"
                )
    assert not failures, "\n".join(failures)


def test_semantic_visible_rows_carries_the_per_row_filter() -> None:
    """`_search_semantic` is approved because its ranked rows go through
    `_semantic_visible_rows`; that helper must run the per-row filter."""
    source = (REPO_ROOT / "src/okg/substrate/mcp/search.py").read_text(encoding="utf-8")
    helper = _function_sources(source).get("_semantic_visible_rows")
    assert helper is not None, "search.py no longer defines _semantic_visible_rows"
    assert _FILTER_NODE in helper


# ---------------------------------------------------------------------------
# Volatile keys in decision identity (issue #635).
#
# A Nomos decision's identity hashes its `resource` mapping, so a volatile
# publish coordinate in resource re-mints the decision on every publish and
# defeats the (deployment_name, decision_id) dedup. The dynamic sweep
# (test_nomos_decision_identity.py) proves the exercised paths are clean;
# this STATIC sweep catches builders on paths no test happens to drive.
#
# Scope: every dict literal that is (a) passed directly as a `resource=`
# keyword, or (b) assigned to a variable whose name contains "resource"
# (the two shapes every decision path in this codebase uses). Keys matching
# the volatile pattern must appear in the allowlist below with a
# justification, or the test fails.

# `generation_ref` is listed alongside `generation_id` because this pattern is
# NAME-based: schema-identity-convergence renamed the admission generation
# coordinate from `target_generation_id` to `target_generation_ref`, and a
# pattern that knew only the old spelling would have let the key fall silently
# out of scope. The volatility is a property of the value (a per-publish
# generation coordinate), not of how it is spelled.
_VOLATILE_RESOURCE_KEY = re.compile(
    r"(?:^|_)(generation_id|generation_ref|branch_name|catalog_version_id)$"
)

_DECISION_SOURCE_ROOT = "src/okg/substrate"

# (file, enclosing function, key) -> why the key legitimately belongs to
# decision identity there. Everything else must keep publish coordinates on
# the audit-only `target`.
APPROVED_VOLATILE_RESOURCE_KEYS: dict[tuple[str, str, str], str] = {
    # Poros exchange: the generation IS the object of the decision — "may
    # this authority sign/export/deliver THIS generation's content to THIS
    # peer". Each is a discrete operator-initiated act, bounded by actual
    # exchange activity, and each deserves its own audit row.
    (
        "src/okg/substrate/exchange/outbox.py",
        "authorize_poros_signing_key_use",
        "sender_generation_id",
    ): "the generation is the signed object (content-of-access)",
    (
        "src/okg/substrate/exchange/outbox.py",
        "export_published_subgraph_to_outbox",
        "sender_generation_id",
    ): "the generation is the exported object (content-of-access)",
    (
        "src/okg/substrate/exchange/sender_delivery.py",
        "_authorize_poros_delivery_with_active_policy",
        "sender_generation_id",
    ): "the generation is the delivered object (content-of-access)",
    # Publishing: one decision per publish of one generation is the correct
    # cardinality — the generation (with its branch and catalog) is what is
    # being published.
    (
        "src/okg/substrate/publisher/publisher.py",
        "_nomos_generation_publish_blocker",
        "target_generation_id",
    ): "the generation is the published object (one decision per publish)",
    (
        "src/okg/substrate/publisher/publisher.py",
        "_nomos_generation_publish_blocker",
        "branch_name",
    ): "coordinate of the published object (one decision per publish)",
    (
        "src/okg/substrate/publisher/publisher.py",
        "_nomos_generation_publish_blocker",
        "catalog_version_id",
    ): "coordinate of the published object (one decision per publish)",
    # Admission admin decisions: identity is already per admission record
    # (admission_id is unique), and the target generation describes the
    # object being admitted — no per-publish re-minting is possible.
    (
        "src/okg/substrate/runtime/admission_workflows.py",
        "_authorize_nomos_admission_decision",
        "target_generation_ref",
    ): "the admitted object's coordinate; identity already per admission_id",
}


@dataclass(frozen=True)
class VolatileResourceKey:
    path: str
    function: str
    lineno: int
    key: str


def _volatile_resource_keys(path: str, source: str) -> list[VolatileResourceKey]:
    tree = ast.parse(source)
    parents = _parent_map(tree)

    def _dict_hits(node: ast.Dict) -> list[tuple[str, int]]:
        hits = []
        for key in node.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                if _VOLATILE_RESOURCE_KEY.search(key.value):
                    hits.append((key.value, key.lineno))
        return hits

    out: list[VolatileResourceKey] = []
    for node in ast.walk(tree):
        dicts: list[ast.Dict] = []
        if isinstance(node, ast.keyword) and node.arg == "resource":
            if isinstance(node.value, ast.Dict):
                dicts.append(node.value)
        elif isinstance(node, ast.Assign):
            if isinstance(node.value, ast.Dict) and any(
                isinstance(t, ast.Name) and "resource" in t.id
                for t in node.targets
            ):
                dicts.append(node.value)
        for dict_node in dicts:
            for key, lineno in _dict_hits(dict_node):
                out.append(
                    VolatileResourceKey(
                        path=path,
                        function=_enclosing_function(dict_node, parents),
                        lineno=lineno,
                        key=key,
                    )
                )
    return out


def test_no_decision_resource_builder_carries_volatile_keys() -> None:
    """Static repo-wide sweep: publish coordinates in decision-identity
    resources are either allowlisted with a justification or a failure."""
    root = REPO_ROOT / _DECISION_SOURCE_ROOT
    failures: list[str] = []
    seen_approved: set[tuple[str, str, str]] = set()
    for path in sorted(root.rglob("*.py")):
        rel = str(path.relative_to(REPO_ROOT))
        hits = _volatile_resource_keys(rel, path.read_text(encoding="utf-8"))
        for hit in hits:
            approval_key = (hit.path, hit.function, hit.key)
            if approval_key in APPROVED_VOLATILE_RESOURCE_KEYS:
                seen_approved.add(approval_key)
                continue
            failures.append(
                f"{hit.path}:{hit.lineno} {hit.function}: volatile key "
                f"{hit.key!r} in a decision-identity resource — move it to "
                f"the audit-only target (issue #635) or allowlist with a "
                f"content-of-access justification"
            )
    stale = set(APPROVED_VOLATILE_RESOURCE_KEYS) - seen_approved
    for entry in sorted(stale):
        failures.append(
            f"stale allowlist entry (no matching resource builder): {entry}"
        )
    assert not failures, "\n".join(failures)


def test_volatile_resource_scanner_rejects_leaky_builder() -> None:
    source = """
def leaky_boundary(session):
    base_resource = {
        "deployment": "d",
        "generation_id": session.generation_id,
    }
    evaluate(resource={"branch_name": session.branch_name})
    return base_resource
"""
    hits = _volatile_resource_keys("synthetic.py", source)
    assert {(h.function, h.key) for h in hits} == {
        ("leaky_boundary", "generation_id"),
        ("leaky_boundary", "branch_name"),
    }


def test_bypass_audit_scanner_rejects_unapproved_raw_graph_read() -> None:
    source = """
def unsafe_agent_read(conn):
    return conn.execute("SELECT node_id FROM okg.graph_nodes")
"""
    reads = _raw_graph_reads("synthetic.py", source)
    assert reads == [
        RawGraphRead(
            path="synthetic.py",
            function="unsafe_agent_read",
            lineno=3,
            snippet="SELECT node_id FROM okg.graph_nodes",
        )
    ]


def test_bypass_audit_scanner_rejects_unfiltered_program_read() -> None:
    source = """
def unsafe_program_read(conn, session):
    rows = lower_program_over_chronos(
        some_program(), conn,
        generation_id=session.generation_id,
        branch=session.branch_name,
    )
    return rows
"""
    reads = _program_lowered_reads("synthetic.py", source)
    assert reads == [
        RawGraphRead(
            path="synthetic.py",
            function="unsafe_program_read",
            lineno=3,
            snippet="lower_program_over_chronos(...)",
        )
    ]


def _raw_graph_reads(path: str, source: str) -> list[RawGraphRead]:
    tree = ast.parse(source)
    parents = _parent_map(tree)
    reads: list[RawGraphRead] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        if not any(relation in node.value for relation in GRAPH_RELATIONS):
            continue
        if _is_docstring(node, parents):
            continue
        reads.append(
            RawGraphRead(
                path=path,
                function=_enclosing_function(node, parents),
                lineno=int(getattr(node, "lineno", 0) or 0),
                snippet=" ".join(node.value.strip().split()),
            )
        )
    return reads


def _render_joined_str(node: ast.JoinedStr) -> str:
    """The f-string with `{name}` left in place of each interpolation."""
    parts: list[str] = []
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parts.append(value.value)
        elif isinstance(value, ast.FormattedValue):
            inner = value.value
            parts.append(
                "{" + inner.id + "}" if isinstance(inner, ast.Name) else "{...}"
            )
    return " ".join("".join(parts).split())


def _segment_interpolated_reads(path: str, source: str) -> list[RawGraphRead]:
    tree = ast.parse(source)
    parents = _parent_map(tree)
    reads: list[RawGraphRead] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        rendered = _render_joined_str(node)
        if not _SEGMENT_FROM_RE.search(rendered):
            continue
        reads.append(
            RawGraphRead(
                path=path,
                function=_enclosing_function(node, parents),
                lineno=int(getattr(node, "lineno", 0) or 0),
                snippet=rendered,
            )
        )
    return reads


def test_segment_interpolated_reads_stay_in_authorized_wrappers() -> None:
    failures: list[str] = []
    for rel_path in SEGMENT_AUDITED_FILES:
        source = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
        locations = APPROVED_SEGMENT_READS[rel_path]
        function_sources = _function_sources(source)
        for read in _segment_interpolated_reads(rel_path, source):
            required_tokens = locations.get(read.function)
            if required_tokens is None:
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"unapproved segment-interpolated graph read: "
                    f"{read.snippet}"
                )
                continue
            function_source = function_sources.get(read.function, source)
            missing = [
                token for token in required_tokens
                if token not in function_source
            ]
            if missing:
                failures.append(
                    f"{read.path}:{read.lineno} {read.function}: "
                    f"missing required guard(s) {missing}: {read.snippet}"
                )
    assert not failures, "\n".join(failures)


def test_segment_read_scanner_is_not_vacuous() -> None:
    """The scanner must actually SEE the reads it claims to audit.

    Without this, `source_coverage.py` could be listed, approve nothing, and
    pass forever — which is precisely how an audit entry becomes decoration.
    """
    source = (
        REPO_ROOT / "src/okg/substrate/mcp/source_coverage.py"
    ).read_text(encoding="utf-8")
    found = {
        read.function
        for read in _segment_interpolated_reads(
            "src/okg/substrate/mcp/source_coverage.py", source
        )
    }
    assert {"_measured_rows", "_measured_rows_enforced"} <= found, sorted(found)


def test_unfiltered_coverage_aggregate_is_unreachable_under_enforcement() -> None:
    """`_measured_rows` is approved with no guard, so prove the caller gates it.

    The approval above rests entirely on the claim that the unfiltered
    aggregate never runs where a row could be hidden. That claim lives in
    `inspect_coverage`, so it is checked here rather than assumed.
    """
    source = (
        REPO_ROOT / "src/okg/substrate/mcp/source_coverage.py"
    ).read_text(encoding="utf-8")
    body = _function_sources(source)["inspect_coverage"]
    assert "deployment_nomos_enforcement_mode(" in body
    assert 'mode == "enforce"' in body
    assert "_measured_rows_enforced(" in body
    # The unfiltered aggregate must sit on the `else` of that branch.
    enforced_at = body.index("_measured_rows_enforced(")
    unfiltered_at = body.index("measured_rows = _measured_rows(")
    assert enforced_at < unfiltered_at, (
        "the unfiltered aggregate must be the else-branch of the enforce check"
    )


def _program_lowered_reads(path: str, source: str) -> list[RawGraphRead]:
    tree = ast.parse(source)
    parents = _parent_map(tree)
    reads: list[RawGraphRead] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            name = node.func.attr
        else:
            continue
        # endswith: server.py imports the lowering as
        # `_lower_program_over_chronos`; an aliased import must not escape
        # the audit.
        if not any(name.endswith(call) for call in PROGRAM_LOWERED_CALLS):
            continue
        reads.append(
            RawGraphRead(
                path=path,
                function=_enclosing_function(node, parents),
                lineno=int(getattr(node, "lineno", 0) or 0),
                snippet=f"{name}(...)",
            )
        )
    return reads


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent
    return parents


def _is_docstring(
    node: ast.Constant,
    parents: dict[ast.AST, ast.AST],
) -> bool:
    parent = parents.get(node)
    grandparent = parents.get(parent) if parent is not None else None
    if not isinstance(parent, ast.Expr):
        return False
    if not isinstance(
        grandparent,
        (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef),
    ):
        return False
    return bool(grandparent.body and grandparent.body[0] is parent)


def _enclosing_function(
    node: ast.AST,
    parents: dict[ast.AST, ast.AST],
) -> str:
    current = node
    while current in parents:
        current = parents[current]
        if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return current.name
    return "<module>"


def _function_sources(source: str) -> dict[str, str]:
    tree = ast.parse(source)
    lines = source.splitlines()
    out: dict[str, str] = {"<module>": source}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.end_lineno is None:
            continue
        out[node.name] = "\n".join(lines[node.lineno - 1: node.end_lineno])
    return out
