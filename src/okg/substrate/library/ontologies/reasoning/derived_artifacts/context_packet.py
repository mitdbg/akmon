"""``context_packet`` derived-artifact runner.

Layer 3B of ``add-portable-memex-deployments``. Provides:

* :class:`ContextPacketDefinition` — parsed view of
  ``context_packet.yaml`` (the definition + its sibling SQL
  queries + the Jinja markdown template).
* :func:`load_context_packet_definition` — load the YAML +
  resolve all sibling files at definition-load time.
* :func:`register_context_packet_definition` — upsert the
  definition row into ``okg.derived_artifact_definitions``
  using the standard hash discipline.
* :func:`run_context_packet` — execute the five section
  queries against a pinned ``generation_id`` and render the
  markdown.
* :func:`render_context_packet` — pure-Python render path
  (used directly by tests; called by ``run_context_packet``
  after queries execute).
* :func:`trim_to_token_budget` — deterministic 4-chars-per-
  token trim that honors the design.md trim order: drop from
  the bottom of each non-Evidence-Index section first, then
  drop from the Evidence Index last.
* :func:`estimate_tokens` — 4-chars-per-token estimator.
* :func:`problem_class_summary` /
  :func:`experiment_advice` — small static lookup helpers
  exposed to the Jinja template.

The render path is decoupled from Postgres so unit tests can
drive it without spinning up a database.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate import fastyaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from okg.substrate.derived_artifacts.relations import stable_hash
from okg.substrate.errors import SubstrateError

try:  # Imported lazily for non-postgres tests.
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover - psycopg is a real dep
    psycopg = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment]


__all__ = [
    "ContextPacketDefinition",
    "ContextPacketResult",
    "ContextPacketDefinitionError",
    "DEFAULT_EVIDENCE_BUDGET",
    "DEFAULT_TOKEN_BUDGET",
    "MARKDOWN_RENDERER_REF",
    "MARKDOWN_ARTIFACT_TYPE",
    "MARKDOWN_MEDIA_TYPE",
    "SECTION_NAMES",
    "EVIDENCE_SECTION",
    "TOKEN_CHARS_PER_TOKEN",
    "estimate_tokens",
    "experiment_advice",
    "load_context_packet_definition",
    "problem_class_summary",
    "register_context_packet_definition",
    "render_context_packet",
    "run_context_packet",
    "trim_to_token_budget",
]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_EVIDENCE_BUDGET = 20
DEFAULT_TOKEN_BUDGET = 4000
TOKEN_CHARS_PER_TOKEN = 4
"""Deterministic 4-chars-per-token heuristic.

Per design.md: an LLM-tokenizer would add a runtime dep + cost
for no real gain — the budget is approximate anyway. Consumers
that need precision can re-tokenize on receipt.
"""

MARKDOWN_RENDERER_REF = "okg.renderer.context_packet.v1"
MARKDOWN_ARTIFACT_TYPE = "okg/context-packet-v1"
MARKDOWN_MEDIA_TYPE = "text/markdown"

SECTION_NAMES: tuple[str, ...] = (
    "design_principles",
    "mechanisms",
    "trade_offs",
    "anti_patterns",
    "evidence",
)
"""Section render order (matches the Engram spec)."""

EVIDENCE_SECTION = "evidence"
"""The Evidence Index — trims last per design.md."""


class ContextPacketDefinitionError(SubstrateError):
    """Raised when ``context_packet.yaml`` is malformed.

    Definition files ship inside the substrate library, so a
    malformed file is a code-time bug not an operator error;
    we still raise a structured envelope so tests can pin on
    a stable ``code`` value.
    """
    code = "context_packet_definition_invalid"


# ---------------------------------------------------------------------------
# Definition load
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _SectionQuery:
    """One section query loaded from the definition's queries
    list. ``ancestor`` is the abstract subtype the closure-join
    filters on — kept here so the runner / tests can audit which
    ancestor each section ranges over without re-parsing SQL.
    """

    name: str
    sql: str
    ancestor: str
    sql_path: Path


@dataclass(frozen=True)
class ContextPacketDefinition:
    """Parsed ``context_packet.yaml`` + sibling resources.

    Frozen so callers can use the instance as a cache key; the
    derived ``compute_hash`` / ``output_schema_hash`` etc. are
    computed at load time once and re-used per render.
    """

    name: str
    description: str
    module_name: str
    inputs: Mapping[str, Mapping[str, Any]]
    queries: tuple[_SectionQuery, ...]
    template_path: Path
    template_source: str
    output_format: str
    media_type: str
    artifact_type: str
    renderer_ref: str
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    yaml_path: Path

    @property
    def definition_id(self) -> str:
        """Stable definition_id used in
        ``okg.derived_artifact_definitions``.

        Shape: ``derived_artifact:reasoning:context_packet``. The
        deployment_name is stamped per-deployment at register
        time; this identity is module-scoped.
        """
        return f"derived_artifact:{self.module_name}:{self.name}"

    def section_queries(self) -> Mapping[str, _SectionQuery]:
        return {q.name: q for q in self.queries}


def _default_definition_path() -> Path:
    """Path to ``context_packet.yaml`` sitting beside this file."""
    return Path(__file__).resolve().parent / "context_packet.yaml"


def load_context_packet_definition(
    yaml_path: Path | None = None,
) -> ContextPacketDefinition:
    """Load and parse the context_packet definition.

    ``yaml_path`` defaults to the in-tree
    ``library/ontologies/reasoning/derived_artifacts/context_packet.yaml``.
    Tests can pass a different path to exercise alternative
    fixtures.

    Hashes (``compute_hash`` / ``output_schema_hash`` /
    ``renderer_hash`` / ``dirty_rule_hash``) are computed
    deterministically from definition content via
    :func:`okg.substrate.derived_artifacts.relations.stable_hash`,
    matching the discipline used by view / consistency-check
    definitions.
    """
    yaml_path = (yaml_path or _default_definition_path()).resolve()
    if not yaml_path.is_file():
        raise ContextPacketDefinitionError(
            f"context_packet.yaml not found: {yaml_path}",
            data={"path": str(yaml_path)},
            fix_hint=(
                "the substrate library ships this file; check "
                "that the install includes the reasoning module's "
                "derived_artifacts/ directory"
            ),
        )

    with yaml_path.open() as f:
        raw = fastyaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ContextPacketDefinitionError(
            f"{yaml_path}: top-level must be a mapping",
            data={"path": str(yaml_path)},
        )

    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `name` is required and must be a "
            f"non-empty string",
            data={"path": str(yaml_path), "key": "name"},
        )

    description = raw.get("description") or ""
    if not isinstance(description, str):
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `description` must be a string",
            data={"path": str(yaml_path), "key": "description"},
        )

    module_name = yaml_path.parent.parent.name
    inputs = _parse_inputs(raw.get("inputs"), yaml_path=yaml_path)
    queries = _parse_queries(
        raw.get("queries"), yaml_path=yaml_path,
    )
    output = raw.get("output")
    if not isinstance(output, dict):
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `output` block is required and must be "
            f"a mapping",
            data={"path": str(yaml_path), "key": "output"},
        )
    output_format = str(output.get("format") or "markdown")
    if output_format != "markdown":
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `output.format` must be 'markdown'; "
            f"got {output_format!r}",
            data={"path": str(yaml_path), "key": "output.format"},
        )
    template_rel = output.get("template")
    if not isinstance(template_rel, str) or not template_rel.strip():
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `output.template` is required and must "
            f"be a non-empty string",
            data={"path": str(yaml_path), "key": "output.template"},
        )
    template_path = (yaml_path.parent / template_rel.strip()).resolve()
    if not template_path.is_file():
        raise ContextPacketDefinitionError(
            f"{yaml_path}: template not found at {template_path}",
            data={"path": str(yaml_path), "template": str(template_path)},
        )
    template_source = template_path.read_text(encoding="utf-8")

    media_type = str(output.get("media_type") or MARKDOWN_MEDIA_TYPE)
    artifact_type = str(output.get("artifact_type") or MARKDOWN_ARTIFACT_TYPE)
    renderer_ref = str(output.get("renderer_ref") or MARKDOWN_RENDERER_REF)

    compute_payload = {
        "name": name.strip(),
        "module": module_name,
        "queries": [
            {"name": q.name, "ancestor": q.ancestor, "sql": q.sql}
            for q in queries
        ],
        "inputs": {
            k: dict(v) for k, v in inputs.items()
        },
    }
    output_schema_payload = {
        "format": output_format,
        "media_type": media_type,
        "artifact_type": artifact_type,
        "renderer_ref": renderer_ref,
    }
    renderer_payload = {
        "template_source": template_source,
        "renderer_ref": renderer_ref,
    }
    dirty_rule_payload = {
        "kind": "input_keyed",
        "trigger": "on_demand",
    }

    return ContextPacketDefinition(
        name=name.strip(),
        description=description.strip(),
        module_name=module_name,
        inputs=inputs,
        queries=queries,
        template_path=template_path,
        template_source=template_source,
        output_format=output_format,
        media_type=media_type,
        artifact_type=artifact_type,
        renderer_ref=renderer_ref,
        compute_hash=stable_hash(compute_payload),
        output_schema_hash=stable_hash(output_schema_payload),
        renderer_hash=stable_hash(renderer_payload),
        dirty_rule_hash=stable_hash(dirty_rule_payload),
        yaml_path=yaml_path,
    )


def _parse_inputs(
    raw: Any, *, yaml_path: Path,
) -> Mapping[str, Mapping[str, Any]]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `inputs` must be a mapping",
            data={"path": str(yaml_path), "key": "inputs"},
        )
    expected = {
        "problem_name", "domain", "topic_slugs",
        "generation_id", "evidence_budget", "token_budget",
    }
    missing = sorted(expected - set(raw.keys()))
    if missing:
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `inputs` missing required entries: "
            f"{missing}",
            data={"path": str(yaml_path), "key": "inputs"},
        )
    out: dict[str, Mapping[str, Any]] = {}
    for key, spec in raw.items():
        if not isinstance(spec, dict):
            raise ContextPacketDefinitionError(
                f"{yaml_path}: `inputs.{key}` must be a mapping",
                data={
                    "path": str(yaml_path),
                    "key": f"inputs.{key}",
                },
            )
        out[key] = dict(spec)
    return out


def _parse_queries(
    raw: Any, *, yaml_path: Path,
) -> tuple[_SectionQuery, ...]:
    if not isinstance(raw, list) or not raw:
        raise ContextPacketDefinitionError(
            f"{yaml_path}: `queries` must be a non-empty list",
            data={"path": str(yaml_path), "key": "queries"},
        )
    seen_names: set[str] = set()
    out: list[_SectionQuery] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ContextPacketDefinitionError(
                f"{yaml_path}: `queries[{i}]` must be a mapping",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}]",
                },
            )
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ContextPacketDefinitionError(
                f"{yaml_path}: `queries[{i}].name` is required",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}].name",
                },
            )
        name = name.strip()
        if name in seen_names:
            raise ContextPacketDefinitionError(
                f"{yaml_path}: duplicate query name {name!r}",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}].name",
                },
            )
        seen_names.add(name)
        ancestor = entry.get("ancestor")
        if not isinstance(ancestor, str) or not ancestor.strip():
            raise ContextPacketDefinitionError(
                f"{yaml_path}: `queries[{i}].ancestor` is required",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}].ancestor",
                },
            )
        sql_rel = entry.get("sql")
        if not isinstance(sql_rel, str) or not sql_rel.strip():
            raise ContextPacketDefinitionError(
                f"{yaml_path}: `queries[{i}].sql` is required",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}].sql",
                },
            )
        sql_path = (yaml_path.parent / sql_rel.strip()).resolve()
        if not sql_path.is_file():
            raise ContextPacketDefinitionError(
                f"{yaml_path}: SQL file not found at {sql_path}",
                data={
                    "path": str(yaml_path),
                    "key": f"queries[{i}].sql",
                    "sql_path": str(sql_path),
                },
            )
        sql_source = sql_path.read_text(encoding="utf-8")
        out.append(_SectionQuery(
            name=name,
            sql=sql_source,
            ancestor=ancestor.strip(),
            sql_path=sql_path,
        ))
    return tuple(out)


# ---------------------------------------------------------------------------
# Definition registration in okg.derived_artifact_definitions
# ---------------------------------------------------------------------------


def register_context_packet_definition(
    conn: "psycopg.Connection[Any]",
    *,
    deployment_name: str,
    definition: ContextPacketDefinition | None = None,
) -> str:
    """Upsert the context_packet derived-artifact definition row.

    Returns the ``definition_id`` written. Idempotent — repeat
    calls with identical content are a no-op (the hash columns
    match). When the YAML changes, the recomputed
    ``compute_hash`` / ``renderer_hash`` differ and the row's
    ``updated_at`` advances.

    Matches the shape used by the view / consistency-check
    registration paths so the scheduler / runner code-paths
    treat this definition the same as any other derived
    artifact.
    """
    if psycopg is None or Jsonb is None:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required to register the context_packet "
            "definition; install psycopg / psycopg-binary"
        )
    definition = definition or load_context_packet_definition()
    output_contract = {
        "name": definition.name,
        "format": definition.output_format,
        "media_type": definition.media_type,
        "artifact_type": definition.artifact_type,
        "renderer_ref": definition.renderer_ref,
        "inputs": {
            k: dict(v) for k, v in definition.inputs.items()
        },
        "queries": [
            {"name": q.name, "ancestor": q.ancestor}
            for q in definition.queries
        ],
        "module": definition.module_name,
    }
    dirty_rule = {
        "kind": "input_keyed",
        "trigger": "on_demand",
        "cache_key": [
            "problem_name",
            "domain",
            "topic_slugs",
            "generation_id",
            "evidence_budget",
            "token_budget",
        ],
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.derived_artifact_definitions (
              definition_id, artifact_kind, deployment_name,
              logical_key, compute_hash, output_schema_hash,
              renderer_hash, dirty_rule_hash, renderer_ref,
              output_contract, dirty_rule
            ) VALUES (
              %(definition_id)s, 'derived_artifact',
              %(deployment_name)s, %(logical_key)s,
              %(compute_hash)s, %(output_schema_hash)s,
              %(renderer_hash)s, %(dirty_rule_hash)s,
              %(renderer_ref)s,
              %(output_contract)s, %(dirty_rule)s
            )
            ON CONFLICT (definition_id) DO UPDATE SET
              artifact_kind = 'derived_artifact',
              deployment_name = EXCLUDED.deployment_name,
              logical_key = EXCLUDED.logical_key,
              compute_hash = EXCLUDED.compute_hash,
              output_schema_hash = EXCLUDED.output_schema_hash,
              renderer_hash = EXCLUDED.renderer_hash,
              dirty_rule_hash = EXCLUDED.dirty_rule_hash,
              renderer_ref = EXCLUDED.renderer_ref,
              output_contract = EXCLUDED.output_contract,
              dirty_rule = EXCLUDED.dirty_rule,
              updated_at = now()
            """,
            {
                "definition_id": definition.definition_id,
                "deployment_name": deployment_name,
                "logical_key": definition.name,
                "compute_hash": definition.compute_hash,
                "output_schema_hash": definition.output_schema_hash,
                "renderer_hash": definition.renderer_hash,
                "dirty_rule_hash": definition.dirty_rule_hash,
                "renderer_ref": definition.renderer_ref,
                "output_contract": Jsonb(output_contract),
                "dirty_rule": Jsonb(dirty_rule),
            },
        )
    return definition.definition_id


# ---------------------------------------------------------------------------
# Token-budget estimator and trim
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    """Deterministic 4-chars-per-token estimator.

    Per design.md "Token-budget heuristic is deterministic": no
    LLM-tokenizer dependency. Tested at the section-trim level
    so the algorithm is reproducible across Python versions.

    ``len(text) // TOKEN_CHARS_PER_TOKEN`` (integer division)
    rounds down — keeps the estimator monotonically non-
    increasing under text deletion so trim loops are safe to
    terminate when the estimate falls below budget.
    """
    if not text:
        return 0
    return len(text) // TOKEN_CHARS_PER_TOKEN


def _section_strength_key(entry: Mapping[str, Any]) -> tuple[int, str]:
    """Sort key for one section entry.

    Returns ``(strength_rank, -observed_at_or_node_id)`` so a
    descending sort on this key surfaces the strongest /
    most-recent entries first. Strength rank: high=0,
    moderate=1, low=2, unknown=3. Ties broken by observed_at
    descending, then node_id ascending.
    """
    strength = (entry.get("strength") or "").lower()
    rank = {"high": 0, "moderate": 1, "low": 2}.get(strength, 3)
    return rank, str(entry.get("observed_at") or "")


def _sort_section(entries: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Sort entries by strength desc then observed_at desc.

    The observed_at comparison is reverse-string-order: ISO 8601
    timestamps sort lexicographically the same as chronologically,
    so reversing in the sort handles it without parsing.
    """
    keyed = []
    for entry in entries:
        rank, observed_at = _section_strength_key(entry)
        keyed.append((rank, observed_at, entry))
    # Sort: strength rank asc (high first), then observed_at
    # desc (most-recent first), then by node_id asc.
    keyed.sort(key=lambda t: (
        t[0],
        # Negate observed_at by using reverse sort within the key;
        # since strings sort the same as chronologically, we
        # invert via a transform: pad-and-reverse the string.
        _reverse_string_for_sort(t[1]),
        str(t[2].get("node_id") or ""),
    ))
    return [entry for (_, _, entry) in keyed]


def _reverse_string_for_sort(value: str) -> str:
    """Return a string whose lexicographic ordering is the
    reverse of ``value``'s. Used to get a "descending
    observed_at" effect inside a stable ascending sort key
    without per-call ``functools.cmp_to_key`` overhead.

    We use ord-inversion: replace each char c with chr(255 - ord(c)).
    The inversion is bounded for Unicode in the 0-255 range
    (which covers all ISO-8601 digits + separators); any char
    above 255 maps to a sentinel below all ASCII. Empty strings
    map to "" — sort to the end via the strength-rank tiebreaker.
    """
    if not value:
        return ""
    return "".join(chr(255 - min(ord(c), 255)) for c in value)


@dataclass
class _TrimMutableSections:
    """In-place container for section lists during the trim
    loop. Lets us mutate per-section state without rebuilding
    the dict each iteration.
    """

    sections: dict[str, list[Mapping[str, Any]]]


def trim_to_token_budget(
    sections: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    token_budget: int,
    render_callback,
) -> tuple[dict[str, list[Mapping[str, Any]]], str]:
    """Drop entries until the rendered output fits ``token_budget``.

    Algorithm (per design.md):

      1. Sort each section by strength desc, observed_at desc.
      2. Render with all sections at full size; if under budget,
         return.
      3. Otherwise, drop one entry from the *bottom* of each
         non-Evidence-Index section that still has entries.
      4. Re-render and re-check.
      5. If still over budget after all non-Evidence-Index
         sections are exhausted, drop entries from the Evidence
         Index last.
      6. If even an empty packet exceeds the budget, return
         the empty rendering — the budget is too small to
         render any content.

    ``render_callback(sections_dict)`` is the renderer
    function (a closure capturing the template + helper
    bindings) that returns the rendered markdown for a given
    section state. Decoupled so this function is testable
    without a live Jinja environment.
    """
    if token_budget < 1:
        token_budget = 1

    # Sort each section descending-by-strength.
    state = _TrimMutableSections(
        sections={
            name: _sort_section(sections.get(name, []) or [])
            for name in SECTION_NAMES
        },
    )

    output = render_callback(state.sections)
    if estimate_tokens(output) <= token_budget:
        return state.sections, output

    # Round-robin drop one entry per non-Evidence-Index section
    # per iteration; bounded by sum of section sizes.
    non_evidence_sections = [
        s for s in SECTION_NAMES if s != EVIDENCE_SECTION
    ]
    max_non_evidence_pass = sum(
        len(state.sections[s]) for s in non_evidence_sections
    )
    for _ in range(max_non_evidence_pass + 1):
        if estimate_tokens(output) <= token_budget:
            return state.sections, output
        dropped_any = False
        for section_name in non_evidence_sections:
            entries = state.sections[section_name]
            if entries:
                entries.pop()  # drop weakest entry
                dropped_any = True
        if not dropped_any:
            break
        output = render_callback(state.sections)

    if estimate_tokens(output) <= token_budget:
        return state.sections, output

    # Evidence Index trims last.
    evidence_entries = state.sections[EVIDENCE_SECTION]
    while evidence_entries and estimate_tokens(output) > token_budget:
        evidence_entries.pop()
        output = render_callback(state.sections)

    return state.sections, output


# ---------------------------------------------------------------------------
# Template-helper functions
# ---------------------------------------------------------------------------


_PROBLEM_CLASS_LOOKUP: dict[tuple[str, str], str] = {
    ("cloudcast", "networking"): (
        "Wide-area network capacity planning and load balancing — "
        "cloudcast targets the cost-tuned routing / multicast "
        "selection family (objective: minimize $/byte under "
        "throughput + jitter SLOs). Adjacent problem families: "
        "BGP route reflector design, anycast prefix steering, "
        "MPLS TE path computation."
    ),
    ("vidur", "ml_systems"): (
        "LLM-serving simulator: schedule prefill + decode batches "
        "across GPUs to minimize latency / maximize throughput. "
        "Vidur targets the request-routing + batching family. "
        "Adjacent: KV-cache management, speculative decoding, "
        "MoE expert routing."
    ),
    ("llm_sql", "databases"): (
        "Natural-language query translation to SQL with execution "
        "feedback. The problem family is text-to-SQL / "
        "schema-conditioned generation under correctness + "
        "latency objectives. Adjacent: query optimization with "
        "learned plans, cache-aware re-rewrite."
    ),
}


_DOMAIN_DEFAULT_PROBLEM_CLASS: dict[str, str] = {
    "networking": (
        "Networking systems problem. Common axes: throughput, "
        "latency tail, fairness, convergence, stability. Adjacent "
        "research families include congestion control, routing, "
        "load balancing, multicast / broadcast trees."
    ),
    "ml_systems": (
        "ML-systems problem. Common axes: latency p99, throughput "
        "tokens/s, memory headroom, accuracy / cost trade-off. "
        "Adjacent families include batching, KV-cache management, "
        "speculative decoding, model parallelism."
    ),
    "databases": (
        "Database / data-management problem. Common axes: query "
        "latency, write throughput, consistency, recovery. Adjacent "
        "families include query optimization, indexing, "
        "transaction concurrency, storage layout."
    ),
    "distributed_systems": (
        "Distributed-systems problem. Common axes: consistency, "
        "availability, partition tolerance, ordering. Adjacent "
        "families include consensus protocols, replication, "
        "membership / failure detection, partitioning."
    ),
    "other": (
        "General systems-research problem. Map the task to its "
        "objective + constraints, then look for adjacent "
        "problem families with measured trade-offs in the "
        "evidence below."
    ),
}


def problem_class_summary(problem_name: str, domain: str) -> str:
    """One-paragraph problem-class summary.

    Initial impl: lookup table keyed on
    ``(problem_name, domain)`` with a domain-level fallback.
    Returns a generic stub when neither matches so the rendered
    packet always has a non-empty Problem Class section. Future
    iterations can pull from typed-node queries (e.g., a
    ``ProblemClass`` subtype) when such a subtype exists.
    """
    key = (str(problem_name or "").strip().lower(),
           str(domain or "").strip().lower())
    if key in _PROBLEM_CLASS_LOOKUP:
        return _PROBLEM_CLASS_LOOKUP[key]
    fallback = _DOMAIN_DEFAULT_PROBLEM_CLASS.get(
        key[1], _DOMAIN_DEFAULT_PROBLEM_CLASS["other"],
    )
    return (
        f"{fallback} (No problem-specific summary indexed for "
        f"`{problem_name}` / `{domain}`; consider authoring one "
        f"in the substrate's reasoning module.)"
    )


_EXPERIMENT_ADVICE_LOOKUP: dict[tuple[str, str], str] = {
    ("cloudcast", "networking"): (
        "- Metrics to inspect: per-flow $/byte, p99 latency, "
        "jitter, link utilization variance.\n"
        "- Stress cases to add: bursty traffic (Pareto-distributed "
        "flow sizes); link failures mid-run; asymmetric capacity.\n"
        "- Ablations to run: hold the topology fixed and vary the "
        "objective weighting (latency vs cost); hold the objective "
        "fixed and vary the multicast tree algorithm."
    ),
    ("vidur", "ml_systems"): (
        "- Metrics to inspect: tokens/s, TTFT p50/p99, batch fill "
        "rate, GPU utilization, KV-cache eviction rate.\n"
        "- Stress cases to add: highly variable prompt lengths; "
        "bursts that exceed steady-state capacity; long-tail "
        "completion-length distributions.\n"
        "- Ablations to run: with vs without continuous batching; "
        "with vs without prefix caching; baseline scheduler vs "
        "candidate."
    ),
    ("llm_sql", "databases"): (
        "- Metrics to inspect: exact-match accuracy, execution "
        "accuracy, average tokens per query, plan cost.\n"
        "- Stress cases to add: schemas with synonymous columns; "
        "queries requiring joins across 4+ tables; queries with "
        "ambiguous aggregate semantics.\n"
        "- Ablations to run: with vs without retrieval; with vs "
        "without execution feedback; baseline planner vs "
        "candidate."
    ),
}


_DOMAIN_DEFAULT_EXPERIMENT_ADVICE: dict[str, str] = {
    "networking": (
        "- Metrics to inspect: throughput, latency tail (p99 "
        "+), fairness index, convergence time.\n"
        "- Stress cases to add: link failures, asymmetric "
        "capacity, bursty traffic.\n"
        "- Ablations to run: hold topology fixed and vary "
        "objective weights; hold objective fixed and vary "
        "algorithm."
    ),
    "ml_systems": (
        "- Metrics to inspect: tokens/s, latency p50/p99, GPU "
        "utilization, accuracy regression.\n"
        "- Stress cases to add: long-tail input lengths, "
        "burst load, memory-pressure scenarios.\n"
        "- Ablations to run: batching on/off; cache on/off; "
        "baseline scheduler vs candidate."
    ),
    "databases": (
        "- Metrics to inspect: query latency p50/p99, "
        "throughput, plan cost, cache hit rate.\n"
        "- Stress cases to add: skewed data, contended writes, "
        "long-running queries.\n"
        "- Ablations to run: index on/off, cache size sweep, "
        "baseline planner vs candidate."
    ),
    "distributed_systems": (
        "- Metrics to inspect: convergence latency, message "
        "complexity, throughput under partition.\n"
        "- Stress cases to add: network partitions, node "
        "failures, byzantine inputs (if applicable).\n"
        "- Ablations to run: replica count sweep, quorum-size "
        "sweep, baseline protocol vs candidate."
    ),
    "other": (
        "- Metrics to inspect: latency, throughput, the primary "
        "objective stated in the problem.\n"
        "- Stress cases to add: edge inputs, contention, "
        "failure injection appropriate to the domain.\n"
        "- Ablations to run: turn each candidate mechanism off "
        "independently; compare against the strongest baseline."
    ),
}


def experiment_advice(problem_name: str, domain: str) -> str:
    """Multi-line experiment-advice paragraph.

    Initial impl: lookup table keyed on
    ``(problem_name, domain)`` with a domain-level fallback.
    Matches the Engram packet's "Metrics / Stress cases /
    Ablations" three-bullet structure.
    """
    key = (str(problem_name or "").strip().lower(),
           str(domain or "").strip().lower())
    if key in _EXPERIMENT_ADVICE_LOOKUP:
        return _EXPERIMENT_ADVICE_LOOKUP[key]
    return _DOMAIN_DEFAULT_EXPERIMENT_ADVICE.get(
        key[1], _DOMAIN_DEFAULT_EXPERIMENT_ADVICE["other"],
    )


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContextPacketResult:
    """Output of :func:`run_context_packet` /
    :func:`render_context_packet`.
    """

    markdown: str
    sections: Mapping[str, Sequence[Mapping[str, Any]]]
    estimated_tokens: int
    inputs: Mapping[str, Any]
    token_budget: int
    evidence_budget: int
    definition_id: str
    trimmed: bool


def _build_template_environment(template_path: Path) -> Environment:
    """Return a Jinja Environment loading templates from
    ``template_path.parent``.

    Uses ``StrictUndefined`` so any unbound variable in the
    template surfaces as an error at render time rather than
    rendering silently as ``""``. Per the project's
    "no silent fallbacks" stance.
    """
    return Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        undefined=StrictUndefined,
        autoescape=False,
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )


def render_context_packet(
    definition: ContextPacketDefinition,
    *,
    problem_name: str,
    domain: str,
    topic_slugs: Sequence[str],
    generation_id: int,
    evidence_budget: int,
    token_budget: int,
    section_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> ContextPacketResult:
    """Render the markdown packet from in-memory section rows.

    Decoupled from Postgres so callers can drive the renderer
    with seeded fixtures. ``section_rows`` keys MUST match
    :data:`SECTION_NAMES`; missing keys are treated as empty
    sections.

    Applies the token-budget trim per design.md before
    returning. The result's ``markdown`` reflects the
    trimmed sections.
    """
    env = _build_template_environment(definition.template_path)
    template = env.get_template(definition.template_path.name)

    inputs_for_template = {
        "problem_name": problem_name,
        "domain": domain,
        "topic_slugs": list(topic_slugs),
        "generation_id": generation_id,
        "evidence_budget": evidence_budget,
        "token_budget": token_budget,
        "problem_class_summary": problem_class_summary,
        "experiment_advice": experiment_advice,
    }

    def _render_with(sections: Mapping[str, Sequence[Mapping[str, Any]]]) -> str:
        ctx = dict(inputs_for_template)
        for name in SECTION_NAMES:
            ctx[name] = list(sections.get(name, []) or [])
        return template.render(**ctx)

    initial_output = _render_with(section_rows)
    initial_tokens = estimate_tokens(initial_output)
    if initial_tokens <= token_budget:
        return ContextPacketResult(
            markdown=initial_output,
            sections=section_rows,
            estimated_tokens=initial_tokens,
            inputs={
                "problem_name": problem_name,
                "domain": domain,
                "topic_slugs": list(topic_slugs),
                "generation_id": generation_id,
                "evidence_budget": evidence_budget,
                "token_budget": token_budget,
            },
            token_budget=token_budget,
            evidence_budget=evidence_budget,
            definition_id=definition.definition_id,
            trimmed=False,
        )

    trimmed_sections, trimmed_output = trim_to_token_budget(
        section_rows,
        token_budget=token_budget,
        render_callback=_render_with,
    )
    return ContextPacketResult(
        markdown=trimmed_output,
        sections=trimmed_sections,
        estimated_tokens=estimate_tokens(trimmed_output),
        inputs={
            "problem_name": problem_name,
            "domain": domain,
            "topic_slugs": list(topic_slugs),
            "generation_id": generation_id,
            "evidence_budget": evidence_budget,
            "token_budget": token_budget,
        },
        token_budget=token_budget,
        evidence_budget=evidence_budget,
        definition_id=definition.definition_id,
        trimmed=True,
    )


def _run_section_query(
    conn: "psycopg.Connection[Any]",
    *,
    sql: str,
    generation_id: int,
    topic_slugs: Sequence[str],
    evidence_budget: int,
) -> list[dict[str, Any]]:
    """Execute one section query and return list of dicts.

    Keeps the column→dict mapping centralized so the renderer
    receives a uniform shape regardless of which section query
    ran. JSONB ``topic_slugs`` / ``evidence_ids`` come back as
    Python lists via psycopg's default JSONB decoder.
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "generation_id": generation_id,
                "topic_slugs": list(topic_slugs),
                "evidence_budget": evidence_budget,
            },
        )
        columns = [d.name for d in (cur.description or [])]
        rows = cur.fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        entry = dict(zip(columns, row))
        # Normalize: topic_slugs / evidence_ids may decode as
        # None (NULL) — replace with empty list so the template
        # filter `(p.evidence_ids or []) | join(', ')` stays
        # consistent.
        if entry.get("topic_slugs") is None:
            entry["topic_slugs"] = []
        if entry.get("evidence_ids") is None:
            entry["evidence_ids"] = []
        out.append(entry)
    return out


def run_context_packet(
    conn: "psycopg.Connection[Any]",
    *,
    problem_name: str,
    domain: str,
    topic_slugs: Sequence[str],
    generation_id: int,
    evidence_budget: int = DEFAULT_EVIDENCE_BUDGET,
    token_budget: int = DEFAULT_TOKEN_BUDGET,
    definition: ContextPacketDefinition | None = None,
) -> ContextPacketResult:
    """Run all five section queries and render the packet.

    ``definition`` defaults to the in-tree definition;
    tests can pass their own to exercise alternative fixtures.
    The returned :class:`ContextPacketResult` carries the
    rendered markdown plus the trimmed section payloads, so
    callers can introspect the trim if needed.
    """
    if psycopg is None:  # pragma: no cover
        raise RuntimeError(
            "psycopg is required to run the context packet "
            "queries; install psycopg / psycopg-binary"
        )
    definition = definition or load_context_packet_definition()

    section_rows: dict[str, list[dict[str, Any]]] = {}
    for query in definition.queries:
        section_rows[query.name] = _run_section_query(
            conn,
            sql=query.sql,
            generation_id=generation_id,
            topic_slugs=topic_slugs,
            evidence_budget=evidence_budget,
        )

    return render_context_packet(
        definition,
        problem_name=problem_name,
        domain=domain,
        topic_slugs=topic_slugs,
        generation_id=generation_id,
        evidence_budget=evidence_budget,
        token_budget=token_budget,
        section_rows=section_rows,
    )
