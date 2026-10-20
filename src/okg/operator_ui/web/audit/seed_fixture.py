"""Seed a deterministic FIXTURE deployment for the operator-ui audit gate.

The visual, accessibility, and SLO audit gates must run against a
deterministic, fast substrate -- NOT live okg-workspace (per-generation node
sample, ~100s reads, a real DSN no CI job should hold). This script populates a
fresh DSN with everything the audited views in ``states.mjs`` need:

  * a representative published generation -- nodes across several subtypes +
    edges shaped like okg-workspace's narrowing triples, published through the
    real Publisher with the ontology-graph rollup registered -- so Home / Health
    / the ontology + the instances graph render. (NB: this fixture seeds no
    ``okg.narrowings``, so the publish narrowing gate is SKIPPED, not enforced --
    the edges are accepted unvalidated; the fixture is for UI rendering, not
    narrowing-regression coverage.)
  * trace events in ``okg.mcp_trace_events`` for >=1 session so the Trace list
    and the Trace call-detail (which clicks the first session) render.
  * deterministic telemetry rows for the Performance view. The real publish is
    allowed to emit telemetry while building the graph, then those live-duration
    rows are replaced with a fixed-shape trace so visual baselines do not vary by
    runner speed.

Deterministic: fixed ids, fixed counts, no randomness -- a fresh seed always
produces the same graph, so the committed visual baselines stay stable. The seed
deliberately surfaces NO source-progress rows, so the Health view renders no
source producer time cells. The graph-published age still changes by wall clock,
so states.mjs masks the specific Home/Health age cards.

Usage (the audit start-servers wires this when OKG_AUDIT_FIXTURE_DSN is set):

    uv run python src/okg/operator_ui/web/audit/seed_fixture.py \\
        --dsn postgres://postgres:okg@127.0.0.1:5433/okg_audit_fixture \\
        --deployment default
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import pathlib
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.ownership import ensure_catalog_write_allowed
from okg.substrate.db import dsn as dsn_mod
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.profiling.artifacts import profile_root, write_json_atomic
from okg.substrate.publisher.deployment_stack import build_runtime_catalog_identity
from okg.substrate.publisher.factory import build_deployment_publisher
from okg.substrate.telemetry import (
    TelemetryMetric,
    TelemetrySpan,
    record_metric,
    record_span,
)
from okg.substrate.views.base import ViewMaterializer

_SEED_SOURCE = "audit.fixture_seed"
_RECEIPT_SCHEMA = "okg.operator_ui.graph_canvas_algorithm_parity.receipt.v1"
_RESET_GUARD_SCHEMA = "okg.operator_ui.audit_reset_guard.v1"
_NO_RESET_GUARD_SCHEMA = "okg.operator_ui.audit_no_reset_guard.v1"
_FIXTURE_HOST = "127.0.0.1"
_FIXTURE_PORT = 5544
_FIXTURE_DATABASE = "okg_audit_fixture"
_FIXTURE_USER = "postgres"
_FIXTURE_PASSWORD = "okg"
_FIXTURE_RESET_AUTHORITY = "allow-reset:127.0.0.1:5544/okg_audit_fixture"
_FIXTURE_RESET_GUARD = f"{_FIXTURE_HOST}:{_FIXTURE_PORT}/{_FIXTURE_DATABASE}"
_CURATED_GRAPH_PRESET_ANCHOR = (
    "pact-verification:single-operator-ui-app:test:test.operator-ui.program-map"
)
# The destructive-authority contract monkeypatches this historical name as an
# effect sentinel. Production seeding never constructs it; publication is
# exclusively through ``build_deployment_publisher`` below.
Publisher: Any = None

# Per-subtype node volume -- small + fixed so the ontology graph is non-trivial
# (several node types + producer counts) but the reads stay sub-second.
_SEED_COUNTS = {
    "pact_change": 24,
    "verification_step": 18,
    "document": 18,
    "agent_session": 12,
    "agent_message": 140,
    "tool_use_event": 30,
}


def _connect(dsn: str, **kwargs: Any) -> psycopg.Connection:
    """psycopg.connect with retry + a generous timeout.

    The shared CI Postgres times out client connects under concurrent-lane load
    even when docker reports healthy (a known okg CI flakiness). A single short
    connect is fragile for a hard-gating lane, so retry with backoff.
    """
    kwargs.setdefault("connect_timeout", 30)
    last: Exception | None = None
    for attempt in range(10):
        try:
            return psycopg.connect(dsn, **kwargs)
        except psycopg.OperationalError as exc:  # includes ConnectionTimeout
            last = exc
            time.sleep(min(2 + attempt * 2, 12))
    assert last is not None
    raise last


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _fixture_dsn_parts(dsn: str) -> dict[str, Any]:
    """Parse and validate the complete disposable-fixture identity.

    The reset boundary is intentionally stricter than the old substring check:
    a typo in a host, port, user, password, or database must fail before any
    connection or filesystem receipt write is attempted.
    """
    try:
        parsed = dsn_mod.parse(dsn)
        port = int(parsed.port) if parsed.port is not None else None
    except (dsn_mod.DsnError, ValueError) as exc:
        raise ValueError(
            f"refusing destructive fixture authority: malformed DSN ({exc})"
        ) from exc
    extra = {
        key: value
        for key, value in parsed.params.items()
        if key not in {"host", "port", "dbname", "user", "password"}
    }
    actual = {
        "form": parsed.form,
        "host": parsed.host,
        "port": port,
        "database": parsed.dbname,
        "user": parsed.user,
        "password": parsed.password,
        "extra_parameters": extra,
    }
    expected = {
        "form": "url",
        "host": _FIXTURE_HOST,
        "port": _FIXTURE_PORT,
        "database": _FIXTURE_DATABASE,
        "user": _FIXTURE_USER,
        "password": _FIXTURE_PASSWORD,
        "extra_parameters": {},
    }
    if any(actual[key] != value for key, value in expected.items()):
        # Mask the password and every other secret-bearing parameter
        # (sslpassword, passfile, ...) before the refusal repeats them.
        shown = {
            key: (
                dsn_mod.MASK
                if key == "password" and value
                else {
                    name: (dsn_mod.MASK if name in dsn_mod.SECRET_KEYS else text)
                    for name, text in value.items()
                }
                if key == "extra_parameters"
                else value
            )
            for key, value in actual.items()
        }
        raise ValueError(
            "refusing destructive fixture authority: "
            f"{shown!r}; expected host={_FIXTURE_HOST!r}, port={_FIXTURE_PORT}, "
            f"database={_FIXTURE_DATABASE!r}, user={_FIXTURE_USER!r}, "
            "workflow password and exact reset token"
        )
    return actual


def validate_destructive_fixture_authority(
    *,
    dsn: str,
    reset_authority: str,
) -> dict[str, Any]:
    """Validate the only DSN/reset token allowed to destroy audit data."""
    parts = _fixture_dsn_parts(dsn)
    if reset_authority != _FIXTURE_RESET_AUTHORITY:
        raise ValueError(
            "refusing destructive fixture authority: reset token must be "
            f"{_FIXTURE_RESET_AUTHORITY}"
        )
    return {
        "host": parts["host"],
        "port": parts["port"],
        "database": parts["database"],
        "user": parts["user"],
        "reset_guard": _FIXTURE_RESET_GUARD,
    }


def _dsn_identity(dsn: str) -> str:
    """Return the redacted identity bound into a publish receipt."""
    _fixture_dsn_parts(dsn)
    import hashlib

    return f"{_FIXTURE_DATABASE}#sha256:{hashlib.sha256(dsn.encode()).hexdigest()[:16]}"


def _reset_db(dsn: str) -> None:
    """Drop + recreate the fixture DB so each seed is deterministic (fresh).

    Guarded: refuses a DB name that does not look like a throwaway fixture, so a
    real deployment DB can never be dropped by this script.
    """
    parsed = dsn_mod.parse(dsn)
    dbname = parsed.dbname or ""
    if not any(token in dbname for token in ("fixture", "audit", "test")):
        raise SystemExit(f"refusing to reset non-fixture database {dbname!r}")
    admin = parsed.with_dbname("postgres").render()
    with _connect(admin, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
            cur.execute(f'CREATE DATABASE "{dbname}"')


def _migrate(dsn: str) -> None:
    """Migrate the fixture DB. pg_cron is popped: the compose/CI Postgres has no
    pg_cron and the fixture does not schedule anything (mirrors the test
    conftest)."""
    from okg.substrate.db import migrate as migrate_mod

    migrate_mod.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    # Provision the required extensions up front (CREATE EXTENSION IF NOT EXISTS)
    # so a fresh fixture DB does not fail migrate one extension at a time.
    migrate_mod.ensure_required_extensions(dsn)
    migrate_mod.migrate(dsn)


def _catalog_version_id(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT catalog_version_id FROM okg.catalog_versions "
            "WHERE status = 'applied' ORDER BY catalog_version_id DESC LIMIT 1"
        )
        row = cur.fetchone()
        if row is not None:
            return int(row[0])
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        return int(cur.fetchone()[0])


def _fixture_deployment_dir(deployment: str) -> pathlib.Path:
    root = (pathlib.Path(__file__).resolve().parent / "fixture-deployment").resolve()
    deployment_dir = (root / deployment).resolve()
    if deployment_dir.parent != root or not (
        deployment_dir / "deployment.yaml"
    ).is_file():
        raise ValueError(
            f"audit fixture deployment is not a checked-in deployment: {deployment!r}"
        )
    return deployment_dir


def _build_fixture_publisher(dsn: str, deployment: str) -> Any:
    """Apply and publish with one explicit server-owned fixture authority."""
    deployment_dir = _fixture_deployment_dir(deployment)
    command = "operator-ui audit fixture publish"
    identity = build_runtime_catalog_identity(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=deployment_dir,
        command=command,
    )
    composed = compose_catalog(deployment_dir)
    with _connect(dsn, autocommit=True) as conn:
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(conn, composed, identity=identity)
    publisher = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        deployment_dir=deployment_dir,
        command=command,
        publisher_overrides={
            # Preserve this fixture's original direct-Publisher contract: it
            # deliberately has no catalog narrowings or publish gates. The
            # canonical factory otherwise installs the deployment default.
            "consistency_gates": [],
            "materializers": [_ontology_rollup_materializer()],
        },
    )
    if publisher.catalog_identity.writer_fingerprint != identity.writer_fingerprint:
        raise RuntimeError("audit fixture factory changed catalog writer authority")
    return publisher


def _seed_runtime_queue_authority(conn: psycopg.Connection) -> None:
    """Install an empty DBOS queue authority for the healthy UI fixture.

    The audit fixture represents an idle deployment, not a deployment whose
    runtime evidence is missing.  Keeping the table empty exercises the real
    queue-health projection while producing the truthful ``idle`` state.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbos")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS dbos.workflow_status (
              workflow_uuid text PRIMARY KEY,
              queue_name text,
              status text NOT NULL,
              name text,
              created_at timestamptz,
              updated_at timestamptz,
              started_at_epoch_ms bigint,
              priority int,
              application_version text,
              inputs text,
              serialization text,
              error text,
              output text
            )
            """
        )


#: The rollup view the seed materializes. It is the okg-workspace deployment's
#: own view module, so a checkout without that deployment (the public copy)
#: cannot seed the audit fixture and refuses by name.
ROLLUP_VIEW_RELPATH = pathlib.PurePosixPath(
    "deployments/okg-workspace/views/ontology_graph_rollup.py"
)


def rollup_view_path(repo_root: pathlib.Path | None = None) -> pathlib.Path:
    """The rollup view module, or a refusal that names it when it is absent."""
    if repo_root is None:
        repo_root = pathlib.Path(__file__).resolve().parents[5]
    mod_path = repo_root / ROLLUP_VIEW_RELPATH
    if not mod_path.is_file():
        raise RuntimeError(
            "the operator-ui audit seed materializes the okg-workspace "
            f"deployment's ontology rollup view ({ROLLUP_VIEW_RELPATH}), which "
            f"this checkout does not have: {mod_path}"
        )
    return mod_path


def _ontology_rollup_materializer(
    repo_root: pathlib.Path | None = None,
) -> ViewMaterializer:
    """The okg-workspace ontology-graph rollup, loaded by path -- the repointed
    ontology read consumes it and fails loud when missing."""
    mod_path = rollup_view_path(repo_root)
    spec = importlib.util.spec_from_file_location(
        "audit_ontology_graph_rollup", mod_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return ViewMaterializer(
        view_def_factory=module.ontology_graph_rollup_view_def,
        build_corpus=module.build_corpus,
        name=module.VIEW_ID,
        reads_edge_types=module._READS_EDGE_TYPES,
    )


def _facts() -> list[NodeFact | EdgeFact]:
    facts: list[NodeFact | EdgeFact] = []
    ids: dict[str, list[str]] = {st: [] for st in _SEED_COUNTS}
    for subtype, count in _SEED_COUNTS.items():
        for i in range(count):
            nid = (
                _CURATED_GRAPH_PRESET_ANCHOR
                if subtype == "pact_change" and i == 0
                else f"{subtype}:{i}"
            )
            ids[subtype].append(nid)
            if subtype == "pact_change":
                attrs: dict[str, Any] = {
                    "change_id": f"c{i}",
                    "title": f"Audit change {i}",
                }
            elif subtype == "verification_step":
                attrs = {"title": f"Verify step {i}"}
            elif subtype == "document":
                attrs = {"title": f"Document {i}"}
            elif subtype == "agent_session":
                attrs = {"agent_nickname": f"keen-otter-{i}"}
            elif subtype == "agent_message":
                attrs = {"text": f"message {i}"}
            else:
                attrs = {"name": "bash"}
            facts.append(
                NodeFact(
                    node_id=nid,
                    subtype=subtype,
                    attrs=attrs,
                    source_record_id={"id": nid},
                    source_revision={"v": 1},
                )
            )

    def _edge(src: str, dst: str, edge_type: str) -> EdgeFact:
        return EdgeFact(
            src=src,
            dst=dst,
            edge_type=edge_type,
            attrs={},
            source_record_id={"id": f"{src}->{dst}"},
            source_revision={"v": 1},
        )

    pc, vs, doc = ids["pact_change"], ids["verification_step"], ids["document"]
    sess, msg, tue = ids["agent_session"], ids["agent_message"], ids["tool_use_event"]
    for i, change in enumerate(pc):
        facts.append(_edge(change, vs[i % len(vs)], "has_step"))
        facts.append(_edge(change, doc[i % len(doc)], "cites"))
        facts.append(_edge(change, tue[i % len(tue)], "touched"))
    for i, session in enumerate(sess):
        facts.append(_edge(session, msg[i % len(msg)], "contains"))
        facts.append(_edge(session, tue[i % len(tue)], "contains"))
    # One deliberately high-degree, deterministic session keeps the complete
    # neighborhood browser and its cursor paging in the static UI gate. Page
    # size remains a transport bound; the fixture has more than one page.
    for message in msg[1:]:
        facts.append(_edge(sess[0], message, "contains"))
    for tool_use in tue[1:]:
        facts.append(_edge(sess[0], tool_use, "contains"))
    for i, t in enumerate(tue):
        facts.append(_edge(t, pc[i % len(pc)], "references"))
    return facts


def _seed_traces(conn: psycopg.Connection, deployment: str) -> None:
    """A couple of trace sessions so Home's recent-activity list renders."""
    # /viz/api/traces derives each row's label from the tool name and the recorded
    # request kwargs (okg.viz.endpoints._query_trace_summary), so keep both
    # realistic; the node_ids reference seeded nodes.
    sessions = [
        (
            "audit-session-keen-otter",
            "Investigate the failing publish",
            [
                (
                    "get_node",
                    {"node_id": _CURATED_GRAPH_PRESET_ANCHOR},
                    {
                        "node_id": _CURATED_GRAPH_PRESET_ANCHOR,
                        "subtype": "pact_change",
                        "attrs": {"title": "Audit change 0"},
                    },
                ),
                (
                    "search",
                    {"query": "verification"},
                    {
                        "results": [
                            {
                                "node_id": "verification_step:0",
                                "score": 0.91,
                                "attrs": {"title": "Verify step 0"},
                            },
                            {
                                "node_id": "verification_step:1",
                                "score": 0.83,
                                "attrs": {"title": "Verify step 1"},
                            },
                        ]
                    },
                ),
                (
                    "list_neighbors",
                    {"node_id": _CURATED_GRAPH_PRESET_ANCHOR},
                    {
                        "neighbors": [
                            {
                                "src": _CURATED_GRAPH_PRESET_ANCHOR,
                                "dst": "verification_step:0",
                                "edge_id": f"{_CURATED_GRAPH_PRESET_ANCHOR}->verification_step:0",
                                "edge_type": "has_step",
                                "direction": "out",
                            },
                            {
                                "src": _CURATED_GRAPH_PRESET_ANCHOR,
                                "dst": "document:0",
                                "edge_id": f"{_CURATED_GRAPH_PRESET_ANCHOR}->document:0",
                                "edge_type": "cites",
                                "direction": "out",
                            },
                        ]
                    },
                ),
            ],
        ),
        (
            "audit-session-brave-lynx",
            "Summarise recent changes",
            [
                (
                    "get_node",
                    {"node_id": "agent_session:0"},
                    {
                        "node_id": "agent_session:0",
                        "subtype": "agent_session",
                        "attrs": {"agent_nickname": "keen-otter-0"},
                    },
                ),
                (
                    "list_neighbors",
                    {"node_id": "agent_session:0"},
                    {
                        "neighbors": [
                            {
                                "src": "agent_session:0",
                                "dst": "agent_message:0",
                                "edge_id": "agent_session:0->agent_message:0",
                                "edge_type": "contains",
                                "direction": "out",
                            },
                        ]
                    },
                ),
            ],
        ),
    ]
    with conn.cursor() as cur:
        for session_id, question, calls in sessions:
            for offset, (tool, req, resp) in enumerate(calls):
                cur.execute(
                    """
                    INSERT INTO okg.mcp_trace_events (
                      session_id, deployment_name, tool_name,
                      request_json, response_json, error_text,
                      duration_ms, question_label, event_time
                    ) VALUES (%s, %s, %s, %s, %s, NULL, %s, %s,
                              now() + (%s || ' seconds')::interval)
                    """,
                    (
                        session_id,
                        deployment,
                        tool,
                        json.dumps(req),
                        json.dumps(resp),
                        12 + offset,
                        question,
                        offset,
                    ),
                )


def _seed_speedscope_artifact(
    conn: psycopg.Connection,
    *,
    deployment: str,
    trace_id: str,
    spans: list[tuple[str, str | None, str, int, int, str | None, dict[str, Any]]],
) -> None:
    """Write a deterministic selected-execution Speedscope artifact for browser proof."""
    artifact_root = profile_root() / deployment / "audit-telemetry-profile"
    frames = [{"name": span[2]} for span in spans]
    events: list[dict[str, Any]] = []
    end_value = 0
    for index, (
        _span_id,
        _parent_span_id,
        _name,
        offset_ms,
        duration_ms,
        _source,
        _attrs,
    ) in enumerate(spans):
        end_value = max(end_value, offset_ms + duration_ms)
        events.append({"type": "O", "frame": index, "at": offset_ms})
        events.append({"type": "C", "frame": index, "at": offset_ms + duration_ms})
    speedscope = {
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {"frames": frames},
        "profiles": [
            {
                "type": "evented",
                "name": "audit fixture selected execution",
                "unit": "milliseconds",
                "startValue": 0,
                "endValue": end_value,
                "events": sorted(events, key=lambda item: (item["at"], item["type"])),
            }
        ],
    }
    bundle = {
        "schema": "okg.telemetry.profile_bundle.v1",
        "deployment": deployment,
        "trace_id": trace_id,
        "artifact_dir": str(artifact_root),
        "artifacts": [
            {
                "artifact_kind": "speedscope",
                "path": str(artifact_root / "speedscope.json"),
                "format": "speedscope-json",
                "collector": "audit-fixture-speedscope",
                "collector_status": "available",
            }
        ],
    }
    write_json_atomic(artifact_root / "profile.json", bundle)
    write_json_atomic(artifact_root / "speedscope.json", speedscope)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.telemetry_artifacts (
              artifact_id, deployment_name, trace_id, artifact_kind, path,
              format, collector, collector_status, summary
            ) VALUES
              (%s, %s, %s, 'profile-bundle', %s, 'json',
               'audit-fixture-profile-bundle', 'available', %s),
              (%s, %s, %s, 'speedscope', %s, 'speedscope-json',
               'audit-fixture-speedscope', 'available', %s)
            """,
            (
                f"telemetry:{trace_id}:profile-bundle:audit",
                deployment,
                trace_id,
                str(artifact_root / "profile.json"),
                Jsonb({"profile_type": "evented", "span_count": len(spans)}),
                f"telemetry:{trace_id}:speedscope:audit",
                deployment,
                trace_id,
                str(artifact_root / "speedscope.json"),
                Jsonb({"profile_type": "evented", "frame_count": len(frames)}),
            ),
        )


def _seed_telemetry(conn: psycopg.Connection, deployment: str) -> None:
    """Replace live publish telemetry with fixed traces for visual stability.

    Keep a short series rather than one trace so the Performance trend mode
    proves wall-clock spacing, selected-execution interaction, and stage-mix changes.
    """
    generation_id = (
        f"okg:{deployment}:branch:default:gen:20260630T000000000000Z:audittelemetry"
    )
    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=60)
    runs = [
        {
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
            "offset_ms": 0,
            "duration_ms": 820,
            "source_ms": 130,
            "publisher_ms": 560,
            "enrichers_ms": 90,
            "views_ms": 115,
            "chronos_ms": 95,
            "gates_ms": 30,
        },
        {
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa2",
            "offset_ms": 2_400,
            "duration_ms": 1_080,
            "source_ms": 170,
            "publisher_ms": 740,
            "enrichers_ms": 150,
            "views_ms": 180,
            "chronos_ms": 140,
            "gates_ms": 44,
        },
        {
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa3",
            "offset_ms": 5_200,
            "duration_ms": 930,
            "source_ms": 120,
            "publisher_ms": 650,
            "enrichers_ms": 120,
            "views_ms": 165,
            "chronos_ms": 105,
            "gates_ms": 36,
        },
        {
            "trace_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa4",
            "offset_ms": 9_000,
            "duration_ms": 1_360,
            "source_ms": 210,
            "publisher_ms": 940,
            "enrichers_ms": 210,
            "views_ms": 260,
            "chronos_ms": 190,
            "gates_ms": 60,
        },
    ]
    with conn.cursor() as cur:
        for table in (
            "okg.telemetry_artifacts",
            "okg.telemetry_metrics",
            "okg.telemetry_events",
            "okg.telemetry_spans",
            "okg.telemetry_traces",
        ):
            cur.execute(
                f"DELETE FROM {table} WHERE deployment_name = %s", (deployment,)
            )
    for index, run in enumerate(runs, start=1):
        trace_id = str(run["trace_id"])
        run_start = base + timedelta(milliseconds=int(run["offset_ms"]))
        root_span_id = f"{index:04d}000000000001"
        publisher_span_id = f"{index:04d}000000000003"
        spans = [
            (
                root_span_id,
                None,
                "ingest.run",
                0,
                int(run["duration_ms"]),
                None,
                {"okg.component": "workflow"},
            ),
            (
                f"{index:04d}000000000002",
                root_span_id,
                "source.audit-fixture",
                0,
                int(run["source_ms"]),
                "audit-fixture",
                {"okg.component": "source"},
            ),
            (
                publisher_span_id,
                root_span_id,
                "publisher.cycle",
                int(run["source_ms"]) + 30,
                int(run["publisher_ms"]),
                None,
                {"okg.component": "publisher"},
            ),
            (
                f"{index:04d}000000000004",
                publisher_span_id,
                "publisher.cycle.enrichers",
                int(run["source_ms"]) + 80,
                int(run["enrichers_ms"]),
                None,
                {"okg.component": "enrichers"},
            ),
            (
                f"{index:04d}000000000005",
                publisher_span_id,
                "publisher.cycle.native_views",
                int(run["source_ms"]) + 100 + int(run["enrichers_ms"]),
                int(run["views_ms"]),
                None,
                {"okg.component": "views"},
            ),
            (
                f"{index:04d}000000000006",
                publisher_span_id,
                "chronos.sql.query",
                int(run["source_ms"])
                + 135
                + int(run["enrichers_ms"])
                + int(run["views_ms"]),
                int(run["chronos_ms"]),
                None,
                {"okg.component": "chronos", "db.system.name": "postgresql"},
            ),
            (
                f"{index:04d}000000000007",
                publisher_span_id,
                "publisher.cycle.consistency_gates",
                int(run["source_ms"])
                + int(run["publisher_ms"])
                - int(run["gates_ms"])
                - 20,
                int(run["gates_ms"]),
                None,
                {"okg.component": "gates"},
            ),
        ]
        for (
            span_id,
            parent_span_id,
            name,
            offset_ms,
            duration_ms,
            source_name,
            attributes,
        ) in spans:
            record_span(
                conn,
                TelemetrySpan(
                    trace_id=trace_id,
                    span_id=span_id,
                    parent_span_id=parent_span_id,
                    span_name=name,
                    span_kind="internal",
                    status_code="ok",
                    deployment_name=deployment,
                    generation_id=generation_id,
                    source_name=source_name,
                    workflow_id=f"audit-workflow-{index}",
                    run_id=f"audit-run-{index}",
                    started_at=run_start + timedelta(milliseconds=offset_ms),
                    ended_at=run_start
                    + timedelta(milliseconds=offset_ms + duration_ms),
                    attributes=attributes,
                    root_name="ingest.run" if parent_span_id is None else None,
                    root_kind="workflow" if parent_span_id is None else None,
                ),
            )
        if index == len(runs):
            _seed_speedscope_artifact(
                conn,
                deployment=deployment,
                trace_id=trace_id,
                spans=spans,
            )
    record_metric(
        conn,
        TelemetryMetric(
            metric_name="telemetry.overhead.write_ms",
            metric_kind="gauge",
            deployment_name=deployment,
            metric_time=base + timedelta(seconds=1),
            value_double=2.0,
            unit="ms",
            trace_id=str(runs[-1]["trace_id"]),
            span_id="0004000000000001",
            labels={"scenario": "audit-fixture"},
        ),
    )


def seed(
    dsn: str,
    deployment: str,
    *,
    reset_authority: str,
) -> dict[str, Any]:
    validate_destructive_fixture_authority(
        dsn=dsn,
        reset_authority=reset_authority,
    )
    # Refuse before the reset when the rollup view this seed publishes with
    # is not in the checkout, rather than failing after the database is wiped.
    rollup_view_path()
    _reset_db(dsn)
    _migrate(dsn)
    with _connect(dsn, autocommit=True) as conn:
        _seed_runtime_queue_authority(conn)
        _catalog_version_id(conn)
    publisher = _build_fixture_publisher(dsn, deployment)
    with _connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        facts = _facts()
        store.apply_facts(facts, source=_SEED_SOURCE, strict_endpoints=True)
    publish = publisher.cycle()
    with _connect(dsn, autocommit=True) as conn:
        _seed_telemetry(conn, deployment)
        _seed_traces(conn, deployment)
    n_nodes = sum(_SEED_COUNTS.values())
    return {
        "nodes": n_nodes,
        "subtypes": len(_SEED_COUNTS),
        "edges": len([fact for fact in facts if isinstance(fact, EdgeFact)]),
        "generation_id": str(publish.get("generation_id", "")),
        "status": str(publish.get("status", "")),
        "node_count": int(publish.get("node_count", n_nodes)),
        "edge_count": int(publish.get("edge_count", 0)),
    }


def _receipt_health(dsn: str) -> tuple[int, int, str]:
    """Read the real post-publish health surfaces used by the audit UI."""
    from okg.substrate.publish_proof import dlq_count

    dead_letter, queue = dlq_count(dsn)
    queue_depth = sum(int(value) for value in queue.values())
    with _connect(dsn, autocommit=True) as conn:
        from okg.viz.reads_system import fetch_doctor

        doctor = fetch_doctor(conn)
    if not doctor.get("ok"):
        raise RuntimeError(f"fixture doctor is not clean: {doctor!r}")
    if queue_depth != 0 or dead_letter != 0:
        raise RuntimeError(
            f"fixture runtime queue is not empty: queue_depth={queue_depth} "
            f"dlq_count={dead_letter}"
        )
    return queue_depth, int(dead_letter), "clean"


def publish_fixture_receipt(
    *,
    dsn: str,
    deployment: str,
    receipt_path: str | pathlib.Path,
    exact_head: str,
    reset: bool = True,
    reset_authority: str,
) -> dict[str, Any]:
    """Run exactly one disposable publish and bind its receipt to the browser.

    Authority validation is deliberately the first operation.  The resulting
    receipt is self-bound: both receipt path fields identify the same absolute
    file consumed by the no-reset server and browser stages.
    """
    if not reset:
        raise ValueError(
            "refusing fixture publish without the explicit reset authority"
        )
    validate_destructive_fixture_authority(
        dsn=dsn,
        reset_authority=reset_authority,
    )
    if not isinstance(exact_head, str) or not re.fullmatch(r"[0-9a-f]{40}", exact_head):
        raise ValueError(
            "refusing fixture publish: exact_head must be a 40-character checkout SHA"
        )
    if not deployment.strip():
        raise ValueError("refusing fixture publish: deployment is required")
    target = pathlib.Path(receipt_path).expanduser().resolve()
    summary = seed(
        dsn,
        deployment,
        reset_authority=reset_authority,
    )
    generation_id = summary["generation_id"]
    if summary["status"] != "published" or not generation_id:
        raise RuntimeError(
            f"fixture publish did not produce a published generation: {summary!r}"
        )
    queue_depth, dlq, doctor = _receipt_health(dsn)
    nodes_live = int(summary["node_count"])
    edges_live = int(summary["edge_count"])
    if nodes_live <= 0 or edges_live <= 0:
        raise RuntimeError(
            f"fixture publish produced empty live graph: nodes={nodes_live} edges={edges_live}"
        )
    receipt: dict[str, Any] = {
        "schema": _RECEIPT_SCHEMA,
        "exact_head": exact_head,
        "deployment": deployment,
        "dsn_identity": _dsn_identity(dsn),
        "reset_guard": _FIXTURE_RESET_GUARD,
        "generation_id": generation_id,
        "status": "published",
        "nodes_live": nodes_live,
        "edges_live": edges_live,
        "queue_depth": queue_depth,
        "dlq_count": dlq,
        "doctor": doctor,
        "receipt_path": str(target),
        "browser_receipt_path": str(target),
    }
    write_json_atomic(target, receipt)
    return receipt


def _read_receipt(path: pathlib.Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"refusing no-reset browser proof: invalid receipt {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ValueError(
            "refusing no-reset browser proof: receipt must be a JSON object"
        )
    return value


def _validate_receipt_health(receipt: dict[str, Any]) -> None:
    required = {
        "schema",
        "exact_head",
        "deployment",
        "dsn_identity",
        "reset_guard",
        "generation_id",
        "status",
        "nodes_live",
        "edges_live",
        "queue_depth",
        "dlq_count",
        "doctor",
        "receipt_path",
        "browser_receipt_path",
    }
    missing = sorted(required.difference(receipt))
    if missing:
        raise ValueError(f"refusing no-reset browser proof: receipt missing {missing}")
    if (
        receipt["schema"] != _RECEIPT_SCHEMA
        or receipt["status"] != "published"
        or receipt["doctor"] != "clean"
        or receipt["reset_guard"] != _FIXTURE_RESET_GUARD
        or not isinstance(receipt["generation_id"], str)
        or not receipt["generation_id"].strip()
        or not isinstance(receipt["nodes_live"], int)
        or receipt["nodes_live"] <= 0
        or not isinstance(receipt["edges_live"], int)
        or receipt["edges_live"] <= 0
        or receipt["queue_depth"] != 0
        or receipt["dlq_count"] != 0
    ):
        raise ValueError("refusing no-reset browser proof: receipt health is not clean")


def validate_no_reset_receipt(
    *,
    dsn: str,
    test_dsn: str,
    receipt_path: str | pathlib.Path,
    expected_head: str,
    expected_generation: str | None,
    reset_authority: str,
    expected_deployment: str = "audit-fixture",
    guard_receipt_path: str | pathlib.Path | None = None,
    verify_live_generation: bool = True,
) -> dict[str, Any]:
    """Validate a receipt and, outside check-only mode, its live checkpoint."""
    validate_destructive_fixture_authority(dsn=dsn, reset_authority=reset_authority)
    if test_dsn != dsn:
        raise ValueError(
            "refusing no-reset browser proof: OKG_TEST_DSN differs from fixture DSN"
        )
    if not isinstance(expected_head, str) or not re.fullmatch(
        r"[0-9a-f]{40}", expected_head
    ):
        raise ValueError(
            "refusing no-reset browser proof: exact head is not a checkout SHA"
        )
    if expected_generation is not None and not expected_generation.strip():
        raise ValueError(
            "refusing no-reset browser proof: expected generation is required"
        )
    target = pathlib.Path(receipt_path).expanduser().resolve()
    receipt = _read_receipt(target)
    _validate_receipt_health(receipt)
    if (
        receipt["exact_head"] != expected_head
        or receipt["deployment"] != expected_deployment
        or (
            expected_generation is not None
            and receipt["generation_id"] != expected_generation
        )
        or receipt["dsn_identity"] != _dsn_identity(dsn)
        or pathlib.Path(str(receipt["receipt_path"])).expanduser().resolve() != target
        or pathlib.Path(str(receipt["browser_receipt_path"])).expanduser().resolve()
        != target
    ):
        raise ValueError(
            "refusing no-reset browser proof: receipt is not bound to this run"
        )
    if verify_live_generation:
        live_generation = _live_published_generation(dsn, expected_deployment)
        if live_generation != receipt["generation_id"]:
            raise ValueError(
                "refusing no-reset browser proof: live published generation "
                f"{live_generation!r} differs from receipt generation "
                f"{receipt['generation_id']!r}"
            )
    if guard_receipt_path is not None:
        guard_target = pathlib.Path(guard_receipt_path).expanduser().resolve()
        write_json_atomic(
            guard_target,
            {
                "schema": _NO_RESET_GUARD_SCHEMA,
                "check_only": True,
                "exact_head": expected_head,
                "dsn_identity": receipt["dsn_identity"],
                "generation_id": receipt["generation_id"],
                "server_started": False,
            },
        )
    return receipt


def _live_published_generation(dsn: str, deployment: str) -> str:
    """Resolve the current Chronos published checkpoint with a read-only query."""
    from okg.substrate.generation_resolver import resolve_okg_generation

    with _connect(dsn, autocommit=True) as conn:
        resolved = resolve_okg_generation(
            conn,
            deployment_name=deployment,
            requested=None,
            branch="default",
            status="published",
        )
    if resolved is None:
        raise ValueError(
            "refusing no-reset browser proof: disposable deployment has no "
            "published generation"
        )
    return resolved.generation_id


def _validate_authority_only() -> dict[str, Any]:
    dsn = _required_env("OKG_AUDIT_FIXTURE_DSN")
    authority = _required_env("OKG_AUDIT_RESET_AUTHORITY")
    result = validate_destructive_fixture_authority(dsn=dsn, reset_authority=authority)
    receipt_path = (
        pathlib.Path(_required_env("OKG_AUDIT_AUTHORITY_CHECK_RECEIPT"))
        .expanduser()
        .resolve()
    )
    payload = {
        "schema": _RESET_GUARD_SCHEMA,
        "check_only": True,
        "reset_guard": result["reset_guard"],
        "user": result["user"],
    }
    write_json_atomic(receipt_path, payload)
    return payload


def _validate_no_reset_only() -> dict[str, Any]:
    dsn = _required_env("OKG_AUDIT_FIXTURE_DSN")
    test_dsn = _required_env("OKG_TEST_DSN")
    receipt = _required_env("OKG_AUDIT_GENERATION_RECEIPT")
    head = _required_env("OKG_PROOF_HEAD")
    generation = os.environ.get("OKG_AUDIT_EXPECTED_GENERATION", "").strip() or None
    authority = _required_env("OKG_AUDIT_RESET_AUTHORITY")
    deployment = (
        os.environ.get(
            "OKG_AUDIT_EXPECTED_DEPLOYMENT",
            os.environ.get("OKG_AUDIT_DEPLOYMENT", "audit-fixture"),
        ).strip()
        or "audit-fixture"
    )
    guard_path = os.environ.get("OKG_AUDIT_NO_RESET_CHECK_RECEIPT")
    return validate_no_reset_receipt(
        dsn=dsn,
        test_dsn=test_dsn,
        receipt_path=receipt,
        expected_head=head,
        expected_generation=generation,
        expected_deployment=deployment,
        reset_authority=authority,
        guard_receipt_path=guard_path,
        verify_live_generation=(
            os.environ.get("OKG_AUDIT_NO_RESET_CHECK_ONLY", "") != "1"
        ),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed the operator-ui audit fixture")
    ap.add_argument("--dsn", help="fixture Postgres DSN")
    ap.add_argument("--deployment", default="default", help="deployment label")
    ap.add_argument("--receipt-path", help="write a one-publish proof receipt")
    ap.add_argument(
        "--exact-head", help="authoritative checkout SHA for a proof receipt"
    )
    ap.add_argument("--reset-authority", help="exact disposable reset token")
    ap.add_argument("--validate-authority-only", action="store_true")
    ap.add_argument("--validate-no-reset-receipt-only", action="store_true")
    args = ap.parse_args()
    if args.validate_authority_only:
        _validate_authority_only()
        return 0
    if args.validate_no_reset_receipt_only:
        _validate_no_reset_only()
        return 0
    dsn = args.dsn or os.environ.get("OKG_AUDIT_FIXTURE_DSN", "").strip()
    if not dsn:
        ap.error("--dsn is required outside validation-only modes")
    if args.receipt_path:
        exact_head = args.exact_head or os.environ.get("OKG_PROOF_HEAD", "").strip()
        authority = (
            args.reset_authority
            or os.environ.get("OKG_AUDIT_RESET_AUTHORITY", "").strip()
        )
        summary = publish_fixture_receipt(
            dsn=dsn,
            deployment=args.deployment,
            receipt_path=args.receipt_path,
            exact_head=exact_head,
            reset_authority=authority,
        )
        print(json.dumps(summary, sort_keys=True))
        return 0
    authority = (
        args.reset_authority or os.environ.get("OKG_AUDIT_RESET_AUTHORITY", "").strip()
    )
    summary = seed(
        dsn,
        args.deployment,
        reset_authority=authority,
    )
    print(
        f"seeded audit fixture: deployment={args.deployment} "
        f"nodes={summary['nodes']} subtypes={summary['subtypes']} + 2 trace sessions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
