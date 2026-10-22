"""Every top-level ``deployment.yaml`` key, declared once.

``DEPLOYMENT_KEYS`` is the one place that says which top-level keys a
deployment manifest may carry, which JSON types each value may take, and what
reads it. The loader's accepted key set and its top-level type check are
derived from this table, and ``schemas/deployment.schema.json`` is generated
from it (``python scripts/gen_deployment_schema.py``; ``--check`` refuses a
stale file). Issue #2571 replaced three hand-kept copies of this knowledge --
the loader's key list, the printed schema, and the comments beside the list --
which had drifted apart.

This module imports nothing from okg at module level: the deployments package
imports it while it is still initializing. The schema builder imports the
block parsers' constants lazily, so a closed block's key names are read from
the parser that enforces them rather than copied here.
"""

from __future__ import annotations

import copy
import datetime as _dt
import importlib
import json
from dataclasses import dataclass, field
from typing import Any, Mapping

# JSON Schema's type names, in the order the schema prints them.
JSON_TYPES = ("null", "boolean", "integer", "number", "string", "array", "object")

# Who acts on a key's value.
READ_BY_OKG = "okg"                # a named okg module changes behavior on it
READ_BY_DEPLOYMENT = "deployment"  # only the deployment's own code reads it
READ_BY_NOTHING = "nothing"        # accepted, and nothing acts on its content

_SCHEMA_ID = "https://palimpzest.mit.edu/okg/schemas/deployment.schema.json"

# A nested fragment may carry ``x-closed-by: "<module>:<constant>"``. The
# builder replaces it with ``additionalProperties: false`` and one property per
# name in that constant, so the schema closes a block exactly where the parser
# that owns the constant refuses unknown keys.
_CLOSED_BY = "x-closed-by"


@dataclass(frozen=True)
class DeploymentKey:
    """One top-level ``deployment.yaml`` key."""

    name: str
    summary: str
    # JSON types the value may take. ``null`` is listed only where writing
    # ``key: null`` loads exactly like leaving the key out.
    types: tuple[str, ...]
    read_by: str = READ_BY_OKG
    # The okg module that acts on the value (``read_by == "okg"``).
    reader: str = ""
    # Why nothing in okg reads it, or which deployment code does.
    reason: str = ""
    # The loader function that already refuses a wrong value for this block.
    # The table's generic type check skips the row so that function's error
    # stays the one an operator sees.
    parser: str = ""
    # Extra JSON Schema for the value. Nested value types belong here only
    # where a test compares them with the parser.
    value_schema: Mapping[str, Any] = field(default_factory=dict)
    # Nested keys the loader accepts and nothing acts on; lint warns on each.
    unread_keys: Mapping[str, str] = field(default_factory=dict)


_S = ("string",)
_S_NULL = ("null", "string")
_O = ("object",)
_O_NULL = ("null", "object")
_A_NULL = ("null", "array")

_DURATION = {"type": "string", "pattern": "^[0-9]+[smh]$"}
_UNREAD_DURATION = (
    "validated at load, but nothing reads it: the publish cadence is "
    "runtime.schedules.okg_publisher_cycle"
)

_PUBLISHER_SCHEMA: Mapping[str, Any] = {
    _CLOSED_BY: "okg.substrate.deployments:_PUBLISHER_KEYS",
    "properties": {
        "kickoff_interval": {
            **_DURATION,
            "default": "30s",
            "description": "Accepted but unread; see runtime.schedules.",
        },
        "cross_source_gate_budget": {
            **_DURATION,
            "default": "30s",
            "description": "Accepted but unread.",
        },
        "per_invariant_timeout": {
            **_DURATION,
            "default": "5s",
            "description": "Accepted but unread.",
        },
        "hard_statement_timeout_seconds": {
            "type": "number",
            "exclusiveMinimum": 0,
            "description": (
                "Hard timeout in seconds for each statement executed while "
                "the publisher lock is held."
            ),
        },
        "lock_hold_headroom_factor": {
            "type": "number",
            "minimum": 1,
            "description": (
                "Required ratio between the hard timeout and measured "
                "publisher lock hold."
            ),
        },
        "allow_raised_statement_timeout": {
            "type": "boolean",
            "description": (
                "Whether release proof may judge headroom against a "
                "deployment timeout above the 900-second compatibility bound."
            ),
        },
        "first_publish_warning_budget": {
            "type": "object",
            _CLOSED_BY: (
                "okg.substrate.deployments:_PUBLISHER_WARNING_BUDGET_KEYS"
            ),
            "required": ["edge_count", "node_count"],
            "properties": {
                "node_count": {"type": "integer", "minimum": 1},
                "edge_count": {"type": "integer", "minimum": 1},
            },
            "description": (
                "Inclusive effective-row thresholds that warn after the first "
                "authoritative count and before cold publication stages."
            ),
        },
    },
}

_POSTGRES_SCHEMA: Mapping[str, Any] = {
    _CLOSED_BY: "okg.substrate.deployments:_POSTGRES_KEYS",
    "properties": {
        "dsn": {
            "description": (
                "Postgres DSN literal or ${ENV_VAR} reference; the value every "
                "database command connects with."
            ),
        },
        "database": {
            "description": (
                "Expected database name, checked against the DSN's target."
            ),
        },
        "self_hosted_authoritative": {
            "description": (
                "Marks a self-hosted local profile as the database authority."
            ),
        },
        "idle_in_transaction_session_timeout_ms": {
            "type": "integer",
            "minimum": 0,
            "description": (
                "How long one of this deployment's Postgres sessions may sit "
                "idle inside a transaction before the server ends it, in "
                "milliseconds. Omit for okg's default (300000); 0 disables "
                "the bound. OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS overrides it "
                "per process."
            ),
        },
    },
}

_CODE_DRIFT_SCHEMA: Mapping[str, Any] = {
    _CLOSED_BY: "okg.substrate.runtime.deployment_drift:_CODE_DRIFT_KEYS",
    "properties": {
        "mode": {
            "enum": ["track_base", "pinned"],
            "default": "track_base",
            "description": (
                "track_base follows base_ref; pinned requires the sync root "
                "at the installed okg build and, for a submodule, at the "
                "superproject's committed pin."
            ),
        },
        "base_ref": {
            "type": "string",
            "pattern": "^\\S+$",
            "description": "track_base only: the ref to compare with.",
        },
        "max_commits_behind": {
            "type": "integer",
            "minimum": 0,
            "description": "track_base only: classify staleness past this.",
        },
    },
}

_CHRONOS_SCHEMA: Mapping[str, Any] = {
    _CLOSED_BY: "okg.substrate.deployments:_CHRONOS_KEYS",
    "properties": {
        "interval_continuation_percent": {
            "description": (
                "Percent (1-99) of an interval's rows that must continue for "
                "the store to extend it in place."
            ),
        },
        "history_retention": {
            "type": "object",
            _CLOSED_BY: "okg.substrate.history_compaction:_POLICY_KEYS",
            "required": ["audit_tail_generations", "default"],
            "properties": {
                "default": {"$ref": "#/$defs/historyRetentionRule"},
                "audit_tail_generations": {"type": "integer", "minimum": 1},
                "nodes": {
                    "type": "object",
                    "additionalProperties": {
                        "$ref": "#/$defs/historyRetentionRule",
                    },
                },
                "edges": {
                    "type": "object",
                    "additionalProperties": {
                        "$ref": "#/$defs/historyRetentionRule",
                    },
                },
            },
            "description": (
                "How long superseded graph history is kept; parsed strictly "
                "by okg.substrate.history_compaction.parse_history_retention."
            ),
        },
    },
}

_HISTORY_RETENTION_RULE: Mapping[str, Any] = {
    "oneOf": [
        {"enum": ["full", "latest-only"]},
        {
            "type": "object",
            _CLOSED_BY: "okg.substrate.history_compaction:_RULE_KEYS",
            "required": ["horizon"],
            "properties": {
                "horizon": {"type": "string", "pattern": "^[1-9][0-9]*[smhdw]$"},
            },
        },
    ],
}

_NOTHING_PATH = (
    "the loader checks the path exists and sealing packages it, but nothing "
    "in okg reads the file"
)


def _key(name: str, summary: str, types: tuple[str, ...], **kwargs: Any) -> DeploymentKey:
    return DeploymentKey(name=name, summary=summary, types=types, **kwargs)


DEPLOYMENT_KEYS: tuple[DeploymentKey, ...] = (
    _key(
        "agent_surface",
        "The declared agent surface: served operators, response budget, "
        "per-client projections and live bindings.",
        _O_NULL,
        reader="okg.substrate.mcp.server",
        parser="okg.substrate.deployments._validated_agent_surface_block",
        value_schema={_CLOSED_BY: "okg.substrate.mcp.effective_surface:_BLOCK_KEYS"},
    ),
    _key(
        "agent_tools",
        "Deployment-declared MCP tools, by tool name.",
        _O_NULL,
        reader="okg.substrate.mcp.deployment_tools",
    ),
    _key(
        "alias_resolver",
        "Alias resolver backends for the publish stack; absent or empty uses "
        "the default resolver.",
        _O_NULL,
        reader="okg.substrate.alias.loader",
    ),
    _key(
        "aliases",
        "Path to the deployment's alias file (aliases.json), composed into "
        "the catalog.",
        _S_NULL,
        reader="okg.substrate.catalog.modules_compose",
    ),
    _key(
        "audit_questions",
        "Path to a text file served verbatim in the MCP read-side metadata.",
        _S_NULL,
        reader="okg.substrate.mcp.server",
    ),
    _key(
        "chat",
        "The governed chat site for this deployment.",
        _O,
        reader="okg.substrate.chat.sync",
        parser="okg.substrate.deployments._validated_chat_block",
        value_schema={_CLOSED_BY: "okg.substrate.deployments.chat_block:_CHAT_KEYS"},
    ),
    _key(
        "checkpoint_attestor_root",
        "Directory holding the checkpoint attestor implementations.",
        # Not null: the publisher reads the raw key and refuses a null root.
        _S,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "checkpoint_attestors",
        "Checkpoint attestors the publisher runs, each {id, class, config}.",
        # Not null: the publisher reads the raw key and refuses a null list.
        ("array",),
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "chronos",
        "Graph history storage tuning and history retention.",
        _O_NULL,
        reader="okg.substrate.publisher.deployment_stack",
        value_schema=_CHRONOS_SCHEMA,
    ),
    _key(
        "citation_attr",
        "The record attribute a reader cites (for example a benchmark "
        "document id); search rows and inspect node and text reads name it.",
        _S_NULL,
        reader="okg.substrate.mcp.citation_id",
        value_schema={"pattern": "^[A-Za-z_][A-Za-z0-9_.-]*$"},
    ),
    _key(
        "code_drift",
        "Code-drift check policy: mode (track_base or pinned), and for "
        "track_base an optional base_ref and max_commits_behind.",
        # The drift probe validates the block when it runs and reports a bad
        # value, including an unknown inner key, as `misconfigured`; the
        # loader only checks the top-level type, so a typo there degrades the
        # drift check instead of breaking every command that loads the
        # manifest. The schema closes the block over the probe's own key set.
        _O_NULL,
        reader="okg.substrate.runtime.deployment_drift",
        value_schema=_CODE_DRIFT_SCHEMA,
    ),
    _key(
        "consistency",
        "Publish consistency policy (dangling_edges, resolution_aware_reuse).",
        _O_NULL,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "consistency_gates",
        "Frozen SQL consistency gates run at publish (deprecated path).",
        _A_NULL,
        reader="okg.substrate.consistency.sql_gate",
    ),
    _key(
        "contract",
        "Deployment lifecycle contract: version, lifecycle, owner, usage.",
        _O_NULL,
        reader="okg.substrate.deployments.admission",
    ),
    _key(
        "coverage",
        "Declared known coverage gaps, echoed by inspect(coverage).",
        _O_NULL,
        reader="okg.substrate.mcp.source_coverage",
        parser="okg.substrate.deployments._validated_coverage_block",
        value_schema={_CLOSED_BY: "okg.substrate.mcp.source_coverage:_COVERAGE_BLOCK_KEYS"},
    ),
    _key(
        "dashboards",
        "Path to supervisor-console dashboards; nothing reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "no code in okg or in the okg-deployments repository reads it; "
            "the supervisor console it served is gone"
        ),
    ),
    _key(
        "deployment_up",
        "`okg deployment up` policy: backup, restore drill and budgets.",
        _O,
        reader="okg.substrate.deployment_upgrade",
    ),
    _key(
        "derived_producers",
        "Authority policies for derived extractor and enricher producers.",
        _O_NULL,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "description",
        "One-line description shown by `okg deployment show`.",
        _S,
        reader="okg.substrate.cli.deployment",
    ),
    _key(
        "docker",
        "Compose stack written by `okg install`: compose_path, project_name "
        "and host_port, read by `okg start`, `okg stop` and `okg restart`.",
        # The lifecycle verbs read the block and refuse a missing
        # compose_path when they run; the loader only checks the top-level
        # type, as for code_drift.
        _O_NULL,
        reader="okg.substrate.cli.lifecycle",
    ),
    _key(
        "doctor_checks",
        "Deployment doctor checks, each {name, handler, help}.",
        _A_NULL,
        reader="okg.substrate.doctor",
    ),
    _key(
        "embedding",
        "Deprecated spelling of `embeddings`; loads with a warning.",
        _O,
        reader="okg.substrate.embeddings.config",
        parser="okg.substrate.deployments._embedding_config_from_manifest",
        value_schema={_CLOSED_BY: "okg.substrate.embeddings.config:_BLOCK_KEYS"},
    ),
    _key(
        "edge_precedence",
        "Producer ranking for edge types several producers write: "
        "{edge_type: [highest-ranked producer, ..., lowest]}. The graph "
        "stores the highest-ranked declarer's version of such an edge.",
        _O_NULL,
        reader="okg.substrate.graph_store_internal.edge_declarers",
        value_schema={
            "additionalProperties": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": {"type": "string", "minLength": 1},
            },
        },
    ),
    _key(
        "embeddings",
        "Whether and with which backend the deployment embeds nodes.",
        _O,
        reader="okg.substrate.embeddings.config",
        parser="okg.substrate.deployments._embedding_config_from_manifest",
        value_schema={_CLOSED_BY: "okg.substrate.embeddings.config:_BLOCK_KEYS"},
    ),
    _key(
        "enricher_registry",
        "Path to an enricher registry file; nothing reads its content.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "the loader checks the path exists and its bytes are hashed into "
            "the publish identity, but nothing reads its content"
        ),
    ),
    _key(
        "enrichers",
        "Enrichers the publisher runs, each {class, config, authority}.",
        _A_NULL,
        reader="okg.substrate.publisher.enricher_loader",
    ),
    _key(
        "extractor",
        "Projection extractor: dotted `class`, `config`, `source_subtypes`, "
        "`authority`.",
        _O_NULL,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "host_storage",
        "Host disk headroom thresholds (warn_available_pct, "
        "blocker_available_pct).",
        _O_NULL,
        reader="okg.substrate.runtime.host_headroom",
    ),
    _key(
        "ir_consistency_checks",
        "Post-publish IR consistency checks.",
        _A_NULL,
        reader="okg.substrate.consistency.ir_check",
    ),
    _key(
        "lenses",
        "Lens definitions with overlay put-back.",
        _A_NULL,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "link_config",
        "Linker configuration; nothing reads it.",
        ("null", "string", "object"),
        read_by=READ_BY_NOTHING,
        reason=(
            "the loader stores it and the publish identity hashes a string "
            "value as a file, but nothing reads its content"
        ),
    ),
    _key(
        "live_capabilities",
        "Path to a live-capabilities file; nothing reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason="no code in okg or in the okg-deployments repository reads it",
    ),
    _key(
        "maintained_derivations",
        "Maintained derivation registry: `module:attr`, or "
        "{registry|loader: `module:attr`}.",
        ("null", "string", "object"),
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "mcp",
        "MCP tool profiles that bound the served tool surface.",
        _O_NULL,
        reader="okg.substrate.mcp.tool_profiles",
        parser="okg.substrate.deployments._validated_mcp_block",
        value_schema={
            _CLOSED_BY: "okg.substrate.mcp.tool_profiles:_MCP_BLOCK_KEYS",
            "properties": {
                "dataset_card": {
                    "type": "boolean",
                    "description": (
                        "false: the server neither gathers nor serves the "
                        "dataset card. Absent means true."
                    ),
                },
            },
        },
    ),
    _key(
        "mcp_instructions",
        "Path to the MCP server instructions file.",
        _S_NULL,
        reader="okg.substrate.mcp.server",
    ),
    _key(
        "mcp_pre_join_tools",
        "Path to pre-join tool declarations; nothing in production reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "only okg.substrate.mcp.framework.generator reads it, and nothing "
            "outside tests calls that generator"
        ),
    ),
    _key(
        "mcp_trace_path",
        "Path for MCP traces; nothing reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason="MCP tracing writes to okg.mcp_trace_events and reads no path",
    ),
    _key(
        "module_providers",
        "Owner-to-directory map adding ontology module providers.",
        _O,
        reader="okg.substrate.catalog.modules_compose",
        parser="okg.substrate.deployments.resolve_declared_module_providers",
    ),
    _key(
        "modules",
        "Ontology modules the catalog composes, as module references.",
        _A_NULL,
        reader="okg.substrate.catalog.modules_compose",
    ),
    _key(
        "modules_config",
        "Per-module settings read by the deployment's own code.",
        _O_NULL,
        read_by=READ_BY_DEPLOYMENT,
        reason=(
            "okg-deployments code reads it: cern-twiki "
            "cern_twiki/deployment_config.py, intersystems "
            "intersystems_consistency/run_consistency.py, intersystems-ops "
            "derivations/_config.py"
        ),
    ),
    _key(
        "name",
        "Deployment name; defaults to the directory name.",
        _S,
        reader="okg.substrate.cli.main",
    ),
    _key(
        "native_views",
        "Native view refresh policy for the publisher.",
        _O_NULL,
        reader="okg.substrate.publisher.deployment_stack",
    ),
    _key(
        "nomos",
        "Nomos posture, enforcement and authentication settings.",
        _O_NULL,
        reader="okg.substrate.nomos.runtime_enforcement",
    ),
    _key(
        "not_ingested",
        "Short phrases naming what the deployment did not ingest; the MCP "
        "dataset card states them.",
        _A_NULL,
        reader="okg.substrate.mcp.server",
        parser="okg.substrate.deployments._validated_not_ingested",
        value_schema={
            "maxItems": 20,
            "items": {
                "type": "string", "minLength": 1, "maxLength": 120,
                "pattern": "^[^\\n\\r]+$",
            },
        },
    ),
    _key(
        "post_publish_consistency_checks",
        "Post-publish SQL consistency checks (frozen SQL path).",
        _A_NULL,
        reader="okg.substrate.consistency.sql_gate",
    ),
    _key(
        "postgres",
        "Database connection: `dsn`, expected `database`, session bounds.",
        _O_NULL,
        reader="okg.substrate.deployment_authority",
        value_schema=_POSTGRES_SCHEMA,
    ),
    _key(
        "postgres_dsn",
        "Legacy flat DSN; prefer postgres.dsn.",
        _S_NULL,
        reader="okg.substrate.deployment_env_doctor",
    ),
    _key(
        "profile",
        "The install profile that wrote this deployment (name, version, "
        "answers).",
        _O_NULL,
        reader="okg.substrate.cli.deployment_readiness",
    ),
    _key(
        "profiling",
        "Workflow profiling policy.",
        _O_NULL,
        reader="okg.substrate.runtime.profiling_bridge",
    ),
    _key(
        "publisher",
        "Publisher statement timeout, lock headroom and first-publish budget.",
        _O,
        reader="okg.substrate.publisher.deployment_stack",
        parser="okg.substrate.deployments._publisher_config_from_manifest",
        value_schema=_PUBLISHER_SCHEMA,
        unread_keys={
            "kickoff_interval": _UNREAD_DURATION,
            "cross_source_gate_budget": _UNREAD_DURATION,
            "per_invariant_timeout": _UNREAD_DURATION,
        },
    ),
    _key(
        "readiness",
        "Readiness source-group overrides and source-optimization policy.",
        _O_NULL,
        reader="okg.substrate.source_sync_convergence",
    ),
    _key(
        "record_prefixes",
        "Record prefixes the MCP record tool offers.",
        _A_NULL,
        reader="okg.substrate.mcp.server",
    ),
    _key(
        "refreshers",
        "Refresher declarations; nothing reads them.",
        _A_NULL,
        read_by=READ_BY_NOTHING,
        reason="the loader stores the list and nothing reads it",
    ),
    _key(
        "release",
        "Release contract: channel, kind and pin.",
        _O_NULL,
        reader="okg.substrate.deployment_release",
    ),
    _key(
        "resolvers",
        "Pre-join resolver declarations; nothing in production reads them.",
        _O_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "only okg.substrate.mcp.framework.generator reads them, and "
            "nothing outside tests calls that generator"
        ),
    ),
    _key(
        "runtime",
        "Worker runtime: pools, schedules, workflows and source sync.",
        _O_NULL,
        reader="okg.substrate.runtime.services",
        parser="okg.substrate.deployments._runtime_config_from_manifest",
        value_schema={_CLOSED_BY: "okg.substrate.runtime.config:RUNTIME_BLOCK_DEFAULTS"},
    ),
    _key(
        "schema_dir",
        "Directory of the deployment's own schema files. Required.",
        _S,
        reader="okg.substrate.catalog.modules_compose",
    ),
    _key(
        "search",
        "Analyst search profiles and scoping.",
        _O_NULL,
        reader="okg.substrate.mcp.server",
        parser="okg.substrate.deployments._validated_search_block",
        value_schema={_CLOSED_BY: "okg.substrate.mcp.search_profiles:_SEARCH_BLOCK_KEYS"},
    ),
    _key(
        "session_start_context",
        "Slices the MCP session-start payload renders.",
        _O_NULL,
        reader="okg.substrate.mcp.server",
    ),
    _key(
        "skill_triggers",
        "Path to per-subtype skill hints for the MCP server.",
        _S_NULL,
        reader="okg.substrate.mcp.server",
    ),
    _key(
        "skills_dir",
        "Directory of skill files hashed into the release pin.",
        _S_NULL,
        reader="okg.substrate.deployment_release",
    ),
    _key(
        "snapshot_path",
        "Path to a snapshot; nothing reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "`okg snapshot-load` takes a required --snapshot flag; only "
            "sealing checks this path"
        ),
    ),
    _key(
        "source_registry",
        "Path to the source registry. Required.",
        _S,
        reader="okg.substrate.runtime.services",
    ),
    _key(
        "sync_root",
        "Repository root for file-derived sources: a path, path anchor or "
        "${VAR}.",
        _S_NULL,
        reader="okg.substrate.runtime.source_sync_workflow",
    ),
    _key(
        "taxonomy",
        "Path to a taxonomy file; nothing reads its content.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=(
            "the loader checks the path exists and its bytes are hashed into "
            "the publish identity, but nothing reads its content"
        ),
    ),
    _key(
        "tool_description_overrides",
        "Path to per-tool description overrides for the chat projection.",
        _S_NULL,
        reader="okg.substrate.chat.sync",
    ),
    _key(
        "viz",
        "Operator console settings and theme.",
        _O_NULL,
        reader="okg.viz.boot",
    ),
    _key(
        "viz_path",
        "Path to a separate console settings file; nothing reads it.",
        _S_NULL,
        read_by=READ_BY_NOTHING,
        reason=_NOTHING_PATH + " (okg.viz.boot looks for `viz_config` instead)",
    ),
)

DEPLOYMENT_KEYS_BY_NAME: Mapping[str, DeploymentKey] = {
    key.name: key for key in DEPLOYMENT_KEYS
}
KNOWN_TOP_LEVEL_KEYS: frozenset[str] = frozenset(DEPLOYMENT_KEYS_BY_NAME)
# The strict loader raises when one of these is missing.
REQUIRED_KEYS: tuple[str, ...] = ("schema_dir", "source_registry")


def json_type(value: Any) -> str:
    """Name the JSON type of a parsed YAML value.

    YAML 1.1 reads ``yes``/``on`` as booleans and unquoted dates as dates, so
    a boolean is checked before an integer and a date or timestamp is named
    for what it is instead of being coerced.
    """

    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, _dt.datetime):
        return "timestamp"
    if isinstance(value, _dt.date):
        return "date"
    return type(value).__name__


def type_allowed(row: DeploymentKey, value: Any) -> bool:
    """True when ``value``'s JSON type is one ``row`` allows."""

    found = json_type(value)
    if found in row.types:
        return True
    # JSON Schema counts every integer as a number.
    return found == "integer" and "number" in row.types


def _load_names(spec: str) -> list[str]:
    module_name, _, attr = spec.partition(":")
    names = getattr(importlib.import_module(module_name), attr)
    return sorted(names)


def _close_blocks(node: Any) -> Any:
    """Expand every ``x-closed-by`` marker from the constant it names."""

    if isinstance(node, list):
        return [_close_blocks(item) for item in node]
    if not isinstance(node, dict):
        return node
    out = {key: _close_blocks(value) for key, value in node.items() if key != _CLOSED_BY}
    spec = node.get(_CLOSED_BY)
    if spec is None:
        return out
    names = _load_names(spec)
    declared = out.get("properties", {})
    stray = sorted(set(declared) - set(names))
    if stray:
        raise ValueError(
            f"schema fragment closed by {spec} describes keys {stray} that the "
            "parser does not accept"
        )
    out["properties"] = {name: declared.get(name, {}) for name in names}
    out["additionalProperties"] = False
    return out


def _property(row: DeploymentKey) -> dict[str, Any]:
    fragment: dict[str, Any] = {"description": row.summary}
    ordered = [t for t in JSON_TYPES if t in row.types]
    fragment["type"] = ordered[0] if len(ordered) == 1 else ordered
    fragment.update(copy.deepcopy(dict(row.value_schema)))
    return fragment


def deployment_json_schema() -> dict[str, Any]:
    """Build the JSON Schema for ``deployment.yaml`` from the table."""

    schema: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": _SCHEMA_ID,
        "title": "OKG deployment.yaml",
        "description": (
            "Top-level manifest for an OKG deployment. Generated by "
            "scripts/gen_deployment_schema.py from "
            "okg.substrate.deployments.keys.DEPLOYMENT_KEYS; do not edit by "
            "hand. A block is closed only where the code that parses it "
            "refuses unknown keys."
        ),
        "$defs": {"historyRetentionRule": copy.deepcopy(dict(_HISTORY_RETENTION_RULE))},
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_KEYS),
        "properties": {
            row.name: _property(row)
            for row in sorted(DEPLOYMENT_KEYS, key=lambda r: r.name)
        },
    }
    return _close_blocks(schema)


def render_deployment_schema() -> str:
    """The exact bytes of ``schemas/deployment.schema.json``."""

    return json.dumps(deployment_json_schema(), indent=2, ensure_ascii=False) + "\n"
