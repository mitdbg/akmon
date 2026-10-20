"""Tests for the ``context_packet`` derived artifact (Layer 3B).

Coverage:
  * Definition loads + parses the YAML + sibling SQL queries.
  * Definition's hashes are deterministic content-hashes
    (no clock / env dependencies).
  * Section queries use the closure-table-join pattern
    (`ancestor_name = '<abstract>'`) over Chronos-pinned views.
  * The Engram-spec section structure is present in the
    rendered template.
  * Deterministic 4-chars-per-token estimator + trim algorithm.
  * Trim drops non-Evidence-Index sections first, then the
    Evidence Index last.
  * Evidence ids in the rendered packet are emitted as real
    node_id strings.
  * MCP build_server does NOT register the tool when reasoning
    is absent; does register it when reasoning is present.
  * problem_class_summary / experiment_advice helpers return
    the expected lookup / fallback content.

These tests do NOT touch Postgres (the closure-join query
behavior is exercised via psycopg-backed tests in a separate
file when a `postgres` mark applies; here we test the pure-
Python render + trim path + definition shape).
"""
from __future__ import annotations

import re

import pytest

from okg.substrate.library.ontologies.reasoning.derived_artifacts.context_packet import (
    DEFAULT_EVIDENCE_BUDGET,
    DEFAULT_TOKEN_BUDGET,
    EVIDENCE_SECTION,
    MARKDOWN_ARTIFACT_TYPE,
    MARKDOWN_RENDERER_REF,
    SECTION_NAMES,
    TOKEN_CHARS_PER_TOKEN,
    ContextPacketResult,
    estimate_tokens,
    experiment_advice,
    load_context_packet_definition,
    problem_class_summary,
    render_context_packet,
    trim_to_token_budget,
)


# ---------- definition load + shape ----------


def test_definition_loads_from_default_path() -> None:
    """The in-tree YAML loads + every referenced file exists."""
    d = load_context_packet_definition()
    assert d.name == "context_packet"
    assert d.module_name == "reasoning"
    assert d.template_path.is_file()
    assert d.template_source.startswith("{#")
    for query in d.queries:
        assert query.sql_path.is_file()
        assert query.sql.strip()


def test_definition_inputs_declare_six_required_fields() -> None:
    d = load_context_packet_definition()
    expected_inputs = {
        "problem_name", "domain", "topic_slugs",
        "generation_id", "evidence_budget", "token_budget",
    }
    assert set(d.inputs.keys()) == expected_inputs
    assert d.inputs["evidence_budget"].get("default") == (
        DEFAULT_EVIDENCE_BUDGET
    )
    assert d.inputs["token_budget"].get("default") == (
        DEFAULT_TOKEN_BUDGET
    )
    assert d.inputs["domain"]["type"] == "enum"
    assert "networking" in d.inputs["domain"]["values"]
    assert "ml_systems" in d.inputs["domain"]["values"]
    assert "databases" in d.inputs["domain"]["values"]
    assert "distributed_systems" in d.inputs["domain"]["values"]
    assert "other" in d.inputs["domain"]["values"]


def test_definition_renderer_and_artifact_type() -> None:
    d = load_context_packet_definition()
    assert d.renderer_ref == MARKDOWN_RENDERER_REF
    assert d.artifact_type == MARKDOWN_ARTIFACT_TYPE
    assert d.output_format == "markdown"


def test_definition_hashes_are_deterministic() -> None:
    """Loading twice yields identical compute/renderer hashes —
    confirms the hash payloads don't carry clock / env data."""
    d1 = load_context_packet_definition()
    d2 = load_context_packet_definition()
    assert d1.compute_hash == d2.compute_hash
    assert d1.output_schema_hash == d2.output_schema_hash
    assert d1.renderer_hash == d2.renderer_hash
    assert d1.dirty_rule_hash == d2.dirty_rule_hash


def test_definition_id_uses_module_scope() -> None:
    d = load_context_packet_definition()
    assert d.definition_id == (
        "derived_artifact:reasoning:context_packet"
    )


# ---------- section queries: closure-join pattern ----------


def test_each_query_uses_subtype_ancestors_closure_join() -> None:
    """Every section query SHALL include the closure-table join
    against `okg.subtype_ancestors` filtering on its abstract
    ancestor name. This is the load-bearing requirement that
    refinements from any composed module (engram_runs, external
    design-knowledge modules, future domain modules) surface automatically.
    """
    d = load_context_packet_definition()
    expected_ancestors = {
        "design_principles": "design_principle",
        "mechanisms": "mechanism",
        "trade_offs": "trade_off",
        "anti_patterns": "anti_pattern",
        "evidence": "evidence",
    }
    by_name = d.section_queries()
    assert set(by_name.keys()) == set(expected_ancestors.keys())
    for name, ancestor in expected_ancestors.items():
        q = by_name[name]
        assert q.ancestor == ancestor
        assert "subtype_ancestors" in q.sql.lower(), (
            f"section query {name} does not join "
            f"okg.subtype_ancestors"
        )
        assert f"'{ancestor}'" in q.sql, (
            f"section query {name} does not filter "
            f"ancestor_name = '{ancestor}'"
        )


def test_each_query_uses_chronos_view_surfaces() -> None:
    d = load_context_packet_definition()
    for q in d.queries:
        assert "FROM okg.v_nodes" in q.sql, (
            f"{q.name} must read the pinned okg.v_nodes view"
        )
        forbidden = ("okg.nodes_live", "okg.edges_live", "valid_to_gen")
        for token in forbidden:
            assert token not in q.sql, (
                f"{q.name} must not reference legacy live-table token {token}"
            )


def test_each_query_orders_by_strength_then_observed_at() -> None:
    d = load_context_packet_definition()
    for q in d.queries:
        assert "ORDER BY" in q.sql, (
            f"{q.name} must declare an ORDER BY clause"
        )
        # strength + observed_at appear in the order specifier.
        order_clause = q.sql.split("ORDER BY", 1)[1].lower()
        assert "strength" in order_clause, (
            f"{q.name} must order by strength"
        )
        assert "observed_at" in order_clause, (
            f"{q.name} must order by observed_at"
        )


def test_each_query_caps_with_evidence_budget() -> None:
    d = load_context_packet_definition()
    for q in d.queries:
        assert "%(evidence_budget)s" in q.sql, (
            f"{q.name} must LIMIT %(evidence_budget)s"
        )


_DESIGN_KNOWLEDGE_MODULE_YAML = """\
name: design_knowledge
version: 0.1.0
description: |-
  Test-only design-knowledge module. Four subtypes refine Claim, the
  shape the removed systems_research module had and external modules
  (for example engram-memex-sr) still ship.
provides:
  subtypes_file: schema.yaml
"""

_DESIGN_KNOWLEDGE_SCHEMA_YAML = """\
id: https://palimpzest.mit.edu/okg/modules/design_knowledge
name: design_knowledge
prefixes:
  linkml: https://w3id.org/linkml/
  dk: https://palimpzest.mit.edu/okg/modules/design_knowledge/
default_range: string
imports:
  - linkml:types
classes:
""" + "".join(
    f"""\
  {cls}:
    is_a: Claim
    description: Test-only {cls} refinement of Claim.
    class_uri: dk:{cls}
    attributes:
      statement:
        required: true
    annotations:
      archetype: claim
      transient: 'false'
"""
    for cls in ("DesignPrinciple", "Mechanism", "TradeOff", "AntiPattern")
)


def _write_design_knowledge_module(dep) -> None:
    """A deployment-owned module refining Claim four ways.

    The in-repo ``systems_research`` module that used to supply these
    refinements was removed (PACT
    remove-git-code-systems-research-profiles); the context packet still
    serves any external module of the same shape, so the compose test
    supplies one through ``module_providers``.
    """
    module_dir = dep / "modules" / "design_knowledge"
    module_dir.mkdir(parents=True)
    (module_dir / "module.yaml").write_text(_DESIGN_KNOWLEDGE_MODULE_YAML)
    (module_dir / "schema.yaml").write_text(_DESIGN_KNOWLEDGE_SCHEMA_YAML)


def test_closure_join_compose_resolves_design_knowledge_subtypes(
    tmp_path,
) -> None:
    """When ``reasoning`` is composed alongside a design-knowledge
    module (deployment-owned here, external in practice) and
    ``engram_runs`` (Layer 3A), the composed catalog's subtype set
    includes the abstract ancestor names the context-packet queries
    filter on:

      - design_principle  (refined by design_knowledge.DesignPrinciple)
      - mechanism         (refined by design_knowledge.Mechanism)
      - trade_off         (refined by design_knowledge.TradeOff)
      - anti_pattern      (refined by design_knowledge.AntiPattern)
      - evidence          (refined by engram_runs.ExperimentResult
                           + reasoning.GenericEvidence)

    Per Layer 1A: the closure populates depth-0 reflexive
    `subtype_ancestors` rows so a query filtering
    `ancestor_name = 'design_principle'` matches both the
    `design_principle` subtype's reflexive row AND any
    refinement chain rooted at it.

    This test exercises the compose-time shape (subtype set +
    parent_subtype edges); the apply-time closure SQL is tested
    in `tests/substrate/catalog/test_subtype_refines.py`.
    """
    pytest.importorskip("yaml")
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "test-context-packet-closure"
    dep.mkdir()
    (dep / "schemas").mkdir()
    _write_design_knowledge_module(dep)
    import yaml as _yaml
    (dep / "deployment.yaml").write_text(_yaml.safe_dump({
        "name": "test-context-packet-closure",
        "description": "context packet closure smoke",
        "module_providers": {"fixture": "modules"},
        # scholarly + bookkeeping + reasoning + design_knowledge are
        # the load-bearing modules; person/dataset/openspec/
        # agent_memory/git_graph satisfy the transitive narrowing
        # references; engram_runs adds Layer 3A refinements (per
        # `tests/substrate/library/sources/test_engram_runs.py`).
        "modules": [
            "scholarly",
            "bookkeeping",
            "reasoning",
            "fixture:design_knowledge",
            "extraction",
            "person",
            "dataset",
            "openspec",
            "agent_memory",
            "git_graph",
            "engram_runs",
        ],
    }))

    composed = compose_catalog(dep)

    # The five ancestor subtypes my queries filter on are present.
    expected_subtypes = {
        "design_principle",
        "mechanism",
        "trade_off",
        "anti_pattern",
        "evidence",
    }
    missing = expected_subtypes - composed.subtypes
    assert not missing, (
        f"design-knowledge + 3A composition missing expected ancestor "
        f"subtypes: {missing}; got {sorted(composed.subtypes)}"
    )

    # The refinement parents are wired correctly: each
    # design_knowledge subtype refines Claim; each engram_runs
    # refinement points at its reasoning/extraction parent.
    parents = composed.subtype_parent
    assert parents.get("design_principle") == "claim"
    assert parents.get("mechanism") == "claim"
    assert parents.get("trade_off") == "claim"
    assert parents.get("anti_pattern") == "claim"
    assert parents.get("experiment_result") == "evidence"
    assert parents.get("empirical_claim") == "claim"
    assert parents.get("generic_evidence") == "evidence"


def test_closure_join_compose_without_design_knowledge_module(
    tmp_path,
) -> None:
    """When no design-knowledge module is composed (e.g. the
    deployment loads only reasoning + engram_runs, as below; note the
    in-repository ``methodology`` module does define ``AntiPattern``, so
    a list that composes it is not this case), the design_principle /
    mechanism / trade_off / anti_pattern ancestor subtypes are ABSENT from
    the catalog — so the context-packet's section queries
    return zero rows for those sections (no spurious closure
    hits).

    The evidence query, however, still surfaces engram_runs's
    ExperimentResult through the closure (its parent_subtype
    `evidence` IS in the catalog), and the reasoning module's
    default `generic_evidence` refinement also surfaces.
    """
    pytest.importorskip("yaml")
    from okg.substrate.catalog.modules_compose import compose_catalog

    dep = tmp_path / "test-without-design-knowledge"
    dep.mkdir()
    (dep / "schemas").mkdir()
    import yaml as _yaml
    (dep / "deployment.yaml").write_text(_yaml.safe_dump({
        "name": "test-without-design-knowledge",
        "description": "context packet closure without design knowledge",
        "modules": [
            "scholarly",
            "bookkeeping",
            "reasoning",
            "extraction",
            "person",
            "dataset",
            "openspec",
            "agent_memory",
            "git_graph",
            "engram_runs",
        ],
    }))
    composed = compose_catalog(dep)

    # Design-knowledge subtypes ABSENT.
    for subtype in (
        "design_principle", "mechanism", "trade_off", "anti_pattern",
    ):
        assert subtype not in composed.subtypes, (
            f"unexpected presence of {subtype} without "
            f"a design-knowledge module"
        )

    # Evidence + claim refinements still in place via Layer 3A
    # (engram_runs) + Layer 1C (reasoning.GenericEvidence /
    # extraction.GenericClaim).
    assert "evidence" in composed.subtypes
    assert "claim" in composed.subtypes
    assert composed.subtype_parent.get("experiment_result") == "evidence"
    assert composed.subtype_parent.get("empirical_claim") == "claim"
    assert composed.subtype_parent.get("generic_evidence") == "evidence"


def test_query_ancestor_names_match_subtype_snake_case() -> None:
    """When a design-knowledge module is composed alongside
    reasoning, the LinkML PascalCase class names
    (`DesignPrinciple`, `Mechanism`, `TradeOff`, `AntiPattern`)
    convert to snake_case (`design_principle`, `mechanism`,
    `trade_off`, `anti_pattern`) at apply time. Layer 1A's
    closure populates a depth-0 reflexive row keyed on the
    snake_case subtype name, so a closure-join filter
    `ancestor_name = 'design_principle'` matches the depth-0
    row directly (when such a module is composed) and any
    refinement chain rooted at that subtype.

    This test pins the ancestor-name strings used by the
    section queries against the snake_case forms of the class
    names design-knowledge modules declare (see
    ``_DESIGN_KNOWLEDGE_SCHEMA_YAML``) — so a renamed section
    ancestor fails loudly with a clear diagnostic.
    """
    d = load_context_packet_definition()
    expected = {
        "design_principles": "design_principle",
        "mechanisms": "mechanism",
        "trade_offs": "trade_off",
        "anti_patterns": "anti_pattern",
        "evidence": "evidence",
    }
    by_name = d.section_queries()
    for section_name, ancestor in expected.items():
        assert by_name[section_name].ancestor == ancestor, (
            f"section {section_name} ancestor={by_name[section_name].ancestor!r} "
            f"differs from expected snake_case {ancestor!r}; check "
            f"design-knowledge / reasoning schemas for class rename"
        )


# ---------- template structure ----------


def _required_headers_in_order() -> tuple[str, ...]:
    # Per the Engram spec's section structure.
    return (
        "Problem Class",
        "Design Principles",
        "Mechanisms To Try",
        "Trade-Off Map",
        "Experiment Advice",
        "Anti-Patterns",
        "Evidence Index",
    )


def test_template_has_engram_section_headers_in_order() -> None:
    d = load_context_packet_definition()
    src = d.template_source
    last_pos = -1
    for header in _required_headers_in_order():
        m = re.search(r"^##\s+" + re.escape(header) + r"\s*$",
                      src, flags=re.MULTILINE)
        assert m is not None, (
            f"template missing section header {header!r}"
        )
        assert m.start() > last_pos, (
            f"section headers out of order at {header}"
        )
        last_pos = m.start()


# ---------- render with seeded section rows ----------


def _seeded_sections() -> dict[str, list[dict[str, object]]]:
    """A small fixture across all five sections."""
    return {
        "design_principles": [
            {
                "node_id": "principle:n_log_n_routing",
                "subtype": "design_principle",
                "statement": "Prefer asymptotically cheap routing.",
                "why_it_matters": (
                    "Linear-per-flow routing overheads dominate "
                    "in high-fanout topologies."
                ),
                "strength": "high",
                "observed_at": "2026-04-01T00:00:00Z",
                "evidence_ids": ["evidence:routing_microbench"],
                "topic_slugs": ["routing"],
            },
            {
                "node_id": "principle:locality_first",
                "subtype": "design_principle",
                "statement": "Place data near consumers when possible.",
                "why_it_matters": (
                    "Cross-region transfers dominate cost in "
                    "geo-distributed workloads."
                ),
                "strength": "moderate",
                "observed_at": "2026-03-20T00:00:00Z",
                "evidence_ids": ["evidence:locality_paper"],
                "topic_slugs": ["routing", "placement"],
            },
        ],
        "mechanisms": [
            {
                "node_id": "mechanism:ecmp",
                "subtype": "mechanism",
                "name": "ECMP hashing",
                "applicability": (
                    "Stateless multipath load distribution"
                ),
                "known_limits": "Re-hash on topology change",
                "strength": "high",
                "observed_at": "2026-02-15T00:00:00Z",
                "evidence_ids": ["evidence:ecmp_eval"],
            },
        ],
        "trade_offs": [
            {
                "node_id": "tradeoff:latency_vs_cost",
                "subtype": "trade_off",
                "objective_a": "Latency",
                "objective_b": "Cost",
                "tension": (
                    "Lower latency typically requires higher "
                    "redundancy / shorter paths."
                ),
                "failure_modes": "Over-provisioning",
                "strength": "high",
                "observed_at": "2026-04-10T00:00:00Z",
                "evidence_ids": ["evidence:cost_study"],
            },
        ],
        "anti_patterns": [
            {
                "node_id": "anti:single_path_routing",
                "subtype": "anti_pattern",
                "approach": "Use a single shortest path.",
                "why_it_tends_to_fail": (
                    "Single-path concentrates load and creates "
                    "tail-latency hotspots."
                ),
                "strength": "moderate",
                "observed_at": "2026-03-01T00:00:00Z",
                "evidence_ids": ["evidence:single_path_failure"],
            },
        ],
        "evidence": [
            {
                "node_id": "evidence:routing_microbench",
                "subtype": "experiment_result",
                "kind": "experiment",
                "strength": "high",
                "statement": (
                    "Routing microbenchmark: ECMP halves p99 "
                    "latency vs single-path on the cloudcast "
                    "fixture."
                ),
                "observed_at": "2026-04-01T00:00:00Z",
            },
            {
                "node_id": "evidence:locality_paper",
                "subtype": "paper_citation_evidence",
                "kind": "citation",
                "strength": "moderate",
                "statement": (
                    "Locality-aware placement reduces cross-"
                    "region cost by 30% in the cited study."
                ),
                "observed_at": "2026-03-20T00:00:00Z",
            },
            {
                "node_id": "evidence:ecmp_eval",
                "subtype": "experiment_result",
                "kind": "experiment",
                "strength": "moderate",
                "statement": (
                    "ECMP eval: 4x throughput vs single-path on "
                    "synthetic burst workload."
                ),
                "observed_at": "2026-02-15T00:00:00Z",
            },
            {
                "node_id": "evidence:cost_study",
                "subtype": "generic_evidence",
                "kind": "study",
                "strength": "low",
                "statement": (
                    "Cost study: latency-cost trade-off curve "
                    "documented for the cloudcast topology."
                ),
                "observed_at": "2026-04-10T00:00:00Z",
            },
            {
                "node_id": "evidence:single_path_failure",
                "subtype": "experiment_result",
                "kind": "experiment",
                "strength": "moderate",
                "statement": (
                    "Single-path failure mode: under bursty "
                    "load, tail latency grows 10x vs ECMP."
                ),
                "observed_at": "2026-03-01T00:00:00Z",
            },
        ],
    }


def test_render_with_seeded_sections_includes_all_headers() -> None:
    d = load_context_packet_definition()
    sections = _seeded_sections()
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows=sections,
    )
    for header in _required_headers_in_order():
        assert f"## {header}" in result.markdown


def test_render_emits_evidence_ids_as_node_id_strings() -> None:
    d = load_context_packet_definition()
    sections = _seeded_sections()
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows=sections,
    )
    # Body Evidence: line carries the evidence_ids.
    assert "evidence:routing_microbench" in result.markdown
    assert "evidence:locality_paper" in result.markdown
    # Evidence Index lists every seeded evidence node_id.
    for entry in sections["evidence"]:
        assert entry["node_id"] in result.markdown


def test_render_problem_class_summary_emitted() -> None:
    d = load_context_packet_definition()
    sections = _seeded_sections()
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows=sections,
    )
    # Lookup-table hit for cloudcast: should contain the
    # cloudcast-specific phrase.
    assert "cloudcast" in result.markdown.lower()
    # Experiment-advice hit for cloudcast.
    assert "Metrics to inspect" in result.markdown


def test_render_with_empty_sections_keeps_headers() -> None:
    """A deployment without DesignPrinciple-refined subtypes
    composed (no design-knowledge module) renders the section
    headers with the "none indexed" sentinel — no Jinja errors,
    no missing headers."""
    d = load_context_packet_definition()
    sections = {name: [] for name in SECTION_NAMES}
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows=sections,
    )
    for header in _required_headers_in_order():
        assert f"## {header}" in result.markdown
    assert "none indexed" in result.markdown


# ---------- token estimator + trim ----------


def test_estimate_tokens_uses_4_chars_per_token_floor() -> None:
    assert estimate_tokens("") == 0
    assert estimate_tokens("a") == 0
    assert estimate_tokens("abcd") == 1
    assert estimate_tokens("abcde") == 1
    assert estimate_tokens("abcdefgh") == 2
    # Confirm constant matches the contract.
    assert TOKEN_CHARS_PER_TOKEN == 4


def test_trim_no_op_when_under_budget() -> None:
    sections = _seeded_sections()

    def render(s):
        return "X" * 10  # 2 tokens

    trimmed, output = trim_to_token_budget(
        sections,
        token_budget=100,
        render_callback=render,
    )
    # Original section sizes preserved.
    for name in SECTION_NAMES:
        assert len(trimmed[name]) == len(sections[name])


def test_trim_drops_non_evidence_sections_before_evidence() -> None:
    sections = _seeded_sections()
    original_evidence_count = len(sections[EVIDENCE_SECTION])

    # Closure: render length scales with total non-evidence
    # section entries; once principles drop, output shrinks
    # below budget. Simulate by counting body entries except
    # evidence.
    def render(s):
        non_ev = sum(
            len(s[k]) for k in s if k != EVIDENCE_SECTION
        )
        ev = len(s[EVIDENCE_SECTION])
        # 200 chars per non-evidence entry, 50 per evidence entry.
        return ("X" * 200) * non_ev + ("E" * 50) * ev

    trimmed, output = trim_to_token_budget(
        sections,
        token_budget=200,  # 800 chars
        render_callback=render,
    )
    # Evidence Index preserved (non-evidence sections trimmed first).
    assert len(trimmed[EVIDENCE_SECTION]) == original_evidence_count, (
        f"Evidence Index should trim last; got "
        f"{len(trimmed[EVIDENCE_SECTION])} evidence entries vs "
        f"original {original_evidence_count}"
    )
    # At least one non-evidence section was reduced.
    non_ev_total_after = sum(
        len(trimmed[k]) for k in SECTION_NAMES if k != EVIDENCE_SECTION
    )
    non_ev_total_before = sum(
        len(sections[k]) for k in SECTION_NAMES if k != EVIDENCE_SECTION
    )
    assert non_ev_total_after < non_ev_total_before


def test_trim_evidence_last_when_all_else_exhausted() -> None:
    """When all non-Evidence-Index sections are empty and the
    output is still over budget, trim evidence."""
    sections = {
        "design_principles": [],
        "mechanisms": [],
        "trade_offs": [],
        "anti_patterns": [],
        EVIDENCE_SECTION: [
            {"node_id": f"e:{i}", "strength": "high",
             "observed_at": f"2026-01-{i:02d}T00:00:00Z"}
            for i in range(1, 11)
        ],
    }
    # Each evidence entry costs 50 chars; budget 100 chars (25 tokens).
    def render(s):
        return ("E" * 50) * len(s[EVIDENCE_SECTION])

    trimmed, output = trim_to_token_budget(
        sections,
        token_budget=25,  # 100 chars
        render_callback=render,
    )
    assert len(trimmed[EVIDENCE_SECTION]) <= 2, (
        f"evidence not trimmed to fit 100-char budget; got "
        f"{len(trimmed[EVIDENCE_SECTION])} entries"
    )
    assert estimate_tokens(output) <= 25


def test_trim_preserves_strongest_entries() -> None:
    """Trim drops weakest entries first — high-strength entries
    survive."""
    sections = {
        "design_principles": [
            {"node_id": "p:weak", "strength": "low",
             "observed_at": "2026-01-01T00:00:00Z"},
            {"node_id": "p:strong", "strength": "high",
             "observed_at": "2026-04-01T00:00:00Z"},
            {"node_id": "p:moderate", "strength": "moderate",
             "observed_at": "2026-03-01T00:00:00Z"},
        ],
        "mechanisms": [],
        "trade_offs": [],
        "anti_patterns": [],
        EVIDENCE_SECTION: [],
    }

    def render(s):
        return "X" * (100 * sum(
            len(s.get(k, [])) for k in SECTION_NAMES
        ))

    trimmed, output = trim_to_token_budget(
        sections,
        token_budget=25,  # 100 chars — leaves room for 1 entry
        render_callback=render,
    )
    # Strongest entry survives.
    surviving_ids = [
        e["node_id"] for e in trimmed["design_principles"]
    ]
    if surviving_ids:
        assert "p:strong" in surviving_ids, (
            f"strongest entry was trimmed; survivors: "
            f"{surviving_ids}"
        )


def test_full_render_with_small_token_budget_trims() -> None:
    """End-to-end: render with a tight token_budget; output is
    trimmed and falls within `budget + chrome_floor` where the
    chrome floor is the static header / Problem Class /
    Experiment Advice copy that cannot be trimmed away.

    Per design.md: the trim algorithm drops section entries
    proportionally; it does NOT delete the section chrome
    (headers, helper-rendered paragraphs). When the chrome
    alone exceeds the budget, the trimmed packet preserves
    the chrome and reports `trimmed=True` to signal that
    further reduction was attempted but bounded by the
    irreducible chrome floor.
    """
    d = load_context_packet_definition()
    sections = _seeded_sections()
    # Render with chrome only (no entries) to learn the floor.
    chrome_only = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows={name: [] for name in SECTION_NAMES},
    )
    chrome_floor = chrome_only.estimated_tokens
    # Budget = chrome floor + a small content allowance.
    budget = chrome_floor + 50
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=budget,
        section_rows=sections,
    )
    assert result.estimated_tokens <= budget, (
        f"trim left {result.estimated_tokens} tokens above "
        f"budget {budget}"
    )
    assert result.trimmed is True


def test_render_below_chrome_floor_does_not_explode() -> None:
    """Budget below the irreducible chrome floor: trim drains
    every section but doesn't crash. Trimmed flag is True.
    """
    d = load_context_packet_definition()
    sections = _seeded_sections()
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=10,  # well below the chrome floor
        section_rows=sections,
    )
    # All sections drained.
    for name in SECTION_NAMES:
        assert len(result.sections.get(name, [])) == 0, (
            f"section {name} not fully drained at sub-chrome budget"
        )
    assert result.trimmed is True


# ---------- helper functions ----------


def test_problem_class_summary_lookup_hits_for_known_pairs() -> None:
    s = problem_class_summary("cloudcast", "networking")
    assert "cloudcast" in s.lower()
    s = problem_class_summary("vidur", "ml_systems")
    assert "vidur" in s.lower()
    s = problem_class_summary("llm_sql", "databases")
    assert "llm_sql" in s.lower() or "text-to-sql" in s.lower()


def test_problem_class_summary_falls_back_to_domain() -> None:
    s = problem_class_summary("unknown_problem", "networking")
    assert "networking" in s.lower() or "throughput" in s.lower()
    s = problem_class_summary("unknown_problem", "distributed_systems")
    assert "distributed" in s.lower() or "consensus" in s.lower()


def test_problem_class_summary_fallback_for_unknown_domain() -> None:
    s = problem_class_summary("x", "x")
    # "other" fallback fires.
    assert s.strip()


def test_experiment_advice_lookup_hits_for_known_pairs() -> None:
    a = experiment_advice("cloudcast", "networking")
    assert "Metrics to inspect" in a
    assert "Stress cases" in a
    assert "Ablations" in a
    a = experiment_advice("vidur", "ml_systems")
    assert "tokens/s" in a or "TTFT" in a


def test_experiment_advice_falls_back_to_domain() -> None:
    a = experiment_advice("unknown_problem", "networking")
    assert "Metrics to inspect" in a


# ---------- ContextPacketResult shape ----------


def test_context_packet_result_carries_inputs_and_counts() -> None:
    d = load_context_packet_definition()
    sections = _seeded_sections()
    result = render_context_packet(
        d,
        problem_name="cloudcast",
        domain="networking",
        topic_slugs=["routing"],
        generation_id=42,
        evidence_budget=20,
        token_budget=DEFAULT_TOKEN_BUDGET,
        section_rows=sections,
    )
    assert isinstance(result, ContextPacketResult)
    assert result.inputs["problem_name"] == "cloudcast"
    assert result.inputs["domain"] == "networking"
    assert result.inputs["topic_slugs"] == ["routing"]
    assert result.inputs["generation_id"] == 42
    assert result.token_budget == DEFAULT_TOKEN_BUDGET
    assert result.evidence_budget == 20
    assert result.definition_id == d.definition_id
