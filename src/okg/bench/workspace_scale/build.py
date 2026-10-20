"""Build the synthetic production-scale okg-workspace graph in one database.

What a build does, in order, and what makes it fail:

1. Refuses, before it writes anything, a database whose name does not start
   with ``okg_workspace_`` (the isolated-clone rule deployment-authority
   resolution applies), a database that is not empty (any okg-workspace
   generation in any status, graph checkpoint, admission envelope -- pending
   runs included -- or source progress row), and a process whose Nomos
   enforcement for okg-workspace is not ``enforce``.
2. Migrates (unless told the database is already migrated), applies the
   okg-workspace catalog, and seeds and activates the deployment's own Nomos
   baseline policy bundle -- the same two calls ``okg provision`` makes.
   Migration installs ``pg_cron``, which PostgreSQL accepts only in the
   database named by the server setting ``cron.database_name``: point it at
   the build database (``ALTER SYSTEM SET cron.database_name =
   'okg_workspace_...'`` and a restart), or migrate elsewhere and pass
   ``--no-migrate``.
3. Writes every producer's records and relationships as one source run under
   that source's real owner id: producer profile from the registry's
   admission policy, an admission envelope, the Nomos ``source.ingest`` and
   ``source.emit`` boundaries, ``ChronosGraphStore.apply_facts``, then the
   ingest service's own post-run admission (``_finalize_source_admission``):
   fact refs, the producer-signature check, the Nomos source-run
   authorization, and ``accepted`` -- or retract and ``rejected``, which the
   build turns into a refusal. A run that changes nothing is refused too.
4. Publishes through the deployment's own publisher stack
   (``build_deployment_publisher``): enrichers, native views, consistency
   gates and the Nomos ``generation.publish`` check all run. Anything but
   ``published`` raises, and every envelope the build created must then read
   ``published``.
5. Re-writes slices of records and relationships and publishes again until
   the profile's publish count, which is what builds interval history.
   ``deep`` (the default) is the nightly profile: the production budgets
   were set on okg-workspace's deep history, and the views that read history
   cost several times more on it than on a single publish.
6. Writes the per-source stage-timing progress rows, reads the result back
   from the last published generation, and verifies it (``verify_report``):
   exact generator counts, the publisher's own records, each enricher within
   its stated tolerance, the search-term share, every publish published, and
   the planned interval depth. Any miss refuses, with the report attached.

There is no fallback anywhere: a refusal is an exception naming its cause.
"""
from __future__ import annotations

import json
import resource
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import chain, islice
from pathlib import Path
from typing import Any

import psycopg

from okg.bench.workspace_scale.facts import WorkspaceFacts
from okg.bench.workspace_scale.targets import ScaledPlan, load_targets, scaled_plan
from okg.substrate.db import dsn as dsn_module
from okg.substrate.library.sources.base import EdgeFact, NodeFact

# The deployment the targets were recorded from (`deployment:` in
# targets.yaml, the one place the generator's numbers live).
DEPLOYMENT = load_targets().deployment
DATABASE_PREFIX = "okg_workspace_"
_RUN_NAMESPACE = uuid.UUID("6b0b6d43-7c8e-4b8e-9d2a-6a1f3f0c5a11")


class BuildRefused(RuntimeError):
    """The build stopped: a precondition, admission, policy, or publish failed.

    ``detail`` is structured evidence (JSON-ready); ``report`` is the build
    report so far, when the refusal came from the final verification.
    """

    def __init__(
        self,
        message: str,
        detail: Any | None = None,
        *,
        report: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail
        self.report = report

    def __str__(self) -> str:
        if self.detail is None:
            return self.message
        return (
            f"{self.message}\n"
            f"{json.dumps(self.detail, indent=2, sort_keys=True, default=str)}"
        )


@dataclass
class BuildOptions:
    dsn: str
    scale: float
    # `deep` is the nightly profile: the production budgets in
    # okg.viz.console_perf_budgets were set on okg-workspace's deep history
    # (42 published generations), and the history-reading views cost several
    # times more on it than on one publish (see targets.yaml `history`).
    profile: str = "deep"
    deployment_dir: Path | None = None
    migrate: bool = True
    chunk_size: int = 20000
    log: Callable[[str], None] = field(default=lambda msg: print(msg, file=sys.stderr, flush=True))


@dataclass
class _Run:
    source: str
    run_id: str
    admission_id: str
    nodes: int
    edges: int
    seconds: float


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def default_deployment_dir() -> Path:
    return repo_root() / "deployments" / DEPLOYMENT


def _dbname(dsn: str) -> str:
    # Parsed by libpq's own rules through the shared DSN module; an
    # unparseable DSN raises its sanitized DsnParseError.
    return dsn_module.parse(dsn).dbname or ""


def _chunks(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    it = iter(items)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


# --------------------------------------------------------------------- setup
def _check_preconditions(options: BuildOptions) -> None:
    name = _dbname(options.dsn)
    if not name.startswith(DATABASE_PREFIX):
        raise BuildRefused(
            f"database {name!r} does not start with {DATABASE_PREFIX!r}; the "
            "generator only writes an isolated okg-workspace clone"
        )
    from okg.substrate.nomos.runtime_enforcement import deployment_nomos_enforcement_mode

    mode = deployment_nomos_enforcement_mode(DEPLOYMENT)
    if mode != "enforce":
        raise BuildRefused(
            f"Nomos enforcement for {DEPLOYMENT} resolves to {mode!r} in this "
            "process; the scale graph is built with the access policy ON "
            "(unset OKG_NOMOS_RUNTIME_ENFORCEMENT)"
        )


# What makes a database "not empty" for the generator: any okg-workspace
# generation in any status, any graph checkpoint, any source run or admission.
# Each is read only when its table exists, so an unmigrated database (no
# `okg` schema yet) reads as empty and a migrated-but-unused one does too.
_NOT_EMPTY_PROBES: tuple[tuple[str, str, str], ...] = (
    ("okg.v_generations",
     "SELECT count(*) FROM okg.v_generations WHERE deployment_name = %(dep)s",
     "generation(s) in any status"),
    ("okg.graph_generations",
     "SELECT count(*) FROM okg.graph_generations WHERE deployment_name = %(dep)s",
     "generation ledger row(s)"),
    ("public._chronos_branch_interval_checkpoints",
     "SELECT count(*) FROM public._chronos_branch_interval_checkpoints",
     "graph checkpoint(s)"),
    ("okg.admission_envelopes",
     "SELECT count(*) FROM okg.admission_envelopes",
     "admission envelope(s), pending runs included"),
    ("okg.source_progress",
     "SELECT count(*) FROM okg.source_progress WHERE deployment = %(dep)s",
     "source progress row(s)"),
)


def _refuse_non_empty_database(dsn: str) -> None:
    """Refuse, before anything is written, a database that is not empty."""
    found: dict[str, int] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table, sql, label in _NOT_EMPTY_PROBES:
            exists = conn.execute("SELECT to_regclass(%s)", (table,)).fetchone()[0]
            if exists is None:
                continue
            n = int(conn.execute(sql, {"dep": DEPLOYMENT}).fetchone()[0])
            if n:
                found[label] = n
    if found:
        raise BuildRefused(
            f"database {_dbname(dsn)!r} already holds {DEPLOYMENT} state; "
            "build into a fresh database",
            found,
        )


def _prepare_database(options: BuildOptions, deployment_dir: Path) -> dict[str, Any]:
    from okg.substrate.cli.provision import _apply_catalog, _seed_nomos_baseline
    from okg.substrate.db.migrate import ensure_required_extensions, migrate

    # Refuse first: the generator never migrates or otherwise alters a
    # database it will not build into.
    _refuse_non_empty_database(options.dsn)
    if options.migrate:
        ensure_required_extensions(options.dsn)
        migrate(options.dsn, deployment_name=DEPLOYMENT)
    # The same two steps `okg provision` runs between migrate and ingest.
    catalog = _apply_catalog(deployment_dir, dsn=options.dsn)
    baseline = _seed_nomos_baseline(DEPLOYMENT, deployment_dir, dsn=options.dsn)
    if baseline is None or not baseline.get("ok"):
        raise BuildRefused(
            "the okg-workspace Nomos baseline did not yield an active policy",
            {"baseline": baseline},
        )
    policy = baseline.get("active_policy") or {}
    return {
        "catalog_version_id": catalog.get("catalog_version_id"),
        "subtype_count": catalog.get("subtype_count"),
        "policy_bundle_id": policy.get("policy_bundle_id"),
        "policy_fingerprint": policy.get("policy_fingerprint"),
    }


# ----------------------------------------------------------------- admission
class _Admission:
    """One synthetic source run through the real admission calls."""

    def __init__(self, dsn: str, registry: dict[str, Any], deployment_dir: Path) -> None:
        self.dsn = dsn
        self.registry = registry
        self.deployment_dir = deployment_dir
        self.runs: list[_Run] = []

    def run(
        self,
        source: str,
        facts: Iterable[NodeFact | EdgeFact],
        *,
        label: str,
        chunk_size: int,
    ) -> _Run:
        from okg.substrate.admission import (
            ensure_envelope,
            graph_branch_for_admission_mode,
            stable_idempotency_key,
            workflow_ref_from_dbos,
        )
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.ingest.service import (
            IngestOptions,
            _finalize_source_admission,
        )
        from okg.substrate.nomos import (
            NomosRuntimeRequest,
            evaluate_runtime_boundary_with_active_policy,
        )
        from okg.substrate.producer_authority import materialize_producer_profile
        from okg.substrate.runtime.source_sync_constants import SOURCE_SYNC_WORKFLOW_NAME

        entry = self.registry.get(source)
        if entry is None or entry.admission_policy is None:
            raise BuildRefused(f"{source} has no admission policy in the registry")
        policy = entry.admission_policy
        started = time.perf_counter()
        idempotency_key = stable_idempotency_key(
            "workspace_scale", DEPLOYMENT, entry.name, policy.producer_id, label
        )
        run_id = str(uuid.uuid5(_RUN_NAMESPACE, f"{source}:{idempotency_key}"))
        scope = f"deployment:{DEPLOYMENT}"
        subject = f"source:{source}"
        workflow_ref = workflow_ref_from_dbos(SOURCE_SYNC_WORKFLOW_NAME)
        graph_branch = graph_branch_for_admission_mode(policy.admission_mode)
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            catalog_version_id = conn.execute(
                "SELECT max(catalog_version_id) FROM okg.catalog_versions "
                "WHERE status = 'applied'"
            ).fetchone()[0]
            with conn.transaction():
                profile = materialize_producer_profile(
                    conn,
                    deployment_name=DEPLOYMENT,
                    producer_id=policy.producer_id,
                    producer_kind=policy.producer_kind,
                    trust_label=policy.trust_label,
                    admission_mode=policy.admission_mode,
                    authority_scope=policy.authority_scope,
                    allowed_record_kinds=policy.allowed_record_kinds,
                    output_signature=policy.output_signature,
                    policy_clauses=policy.policy_clauses,
                    output_scope_summary=policy.as_dict()["output_scope_summary"],
                    source_ref={
                        "source_registry_name": entry.name,
                        "ownership_id": entry.ownership_id,
                    },
                    catalog_version_id=catalog_version_id,
                )
                envelope = ensure_envelope(
                    conn,
                    deployment_name=DEPLOYMENT,
                    producer_id=policy.producer_id,
                    producer_kind=policy.producer_kind,
                    intent="source_sync",
                    admission_mode=policy.admission_mode,
                    status="pending",
                    idempotency_key=idempotency_key,
                    catalog_version_id=catalog_version_id,
                    source=source,
                    run_id=run_id,
                    authority={
                        "source_registry_name": entry.name,
                        "producer_id": policy.producer_id,
                        "ownership_id": entry.ownership_id,
                        "trust": policy.trust_label,
                        "policy_declared": True,
                        "producer_policy": profile.evidence(),
                        "generator": "okg.bench.workspace_scale",
                    },
                    check_summary={
                        "run_mode": "scope_complete",
                        "producer_policy": profile.evidence(),
                    },
                    workflow_ref=workflow_ref,
                )
            admission_id = str(envelope.admission_id)
            for surface, target_kind in (
                ("source.ingest", "source_ingest"),
                ("source.emit", "source_emit"),
            ):
                outcome = evaluate_runtime_boundary_with_active_policy(
                    conn,
                    deployment_name=DEPLOYMENT,
                    scope=scope,
                    request=NomosRuntimeRequest(
                        surface=surface,
                        subject=subject,
                        classification="internal",
                        resource={
                            "deployment": DEPLOYMENT,
                            "scope": scope,
                            "source": source,
                            "run_id": run_id,
                            "mode": "scope_complete",
                            "graph_branch": graph_branch,
                        },
                    ),
                    target={"kind": target_kind, "source": source, "run_id": run_id},
                    performed_obligations=("audit",),
                )
                if outcome is None or not outcome.allowed:
                    reason = None if outcome is None else outcome.decision.reason_code
                    raise BuildRefused(
                        f"Nomos refused {surface} for {source}: "
                        f"{reason or 'no active policy decision'}"
                    )

            store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
            nodes = edges = changed = 0
            for chunk in _chunks(facts, chunk_size):
                result = store.apply_facts(
                    chunk, source=source, run_id=run_id, strict_endpoints=True
                )
                nodes += sum(1 for f in chunk if isinstance(f, NodeFact))
                edges += sum(1 for f in chunk if isinstance(f, EdgeFact))
                changed += sum(int(v) for v in (result.delta_counts or {}).values())
            if not changed:
                # Every fact repeated what the graph already holds. No publish
                # can carry this admission, and history counted from it would
                # be history that does not exist. The envelope stays pending.
                raise BuildRefused(
                    f"{source} run {run_id} ({label}) wrote nothing: every fact "
                    "repeated the graph's current state"
                )
        # The post-run admission is the ingest service's own function, so the
        # generator admits exactly as a real source run does: fact-range refs,
        # the producer-signature check, the Nomos source-run authorization
        # under `resolve_source_authorization_identity`, the graph branch for
        # the admission mode, and on any violation the run is retracted and
        # its envelope rejected (ingest/service.py `_finalize_source_admission`).
        finalized = _finalize_source_admission(
            options=IngestOptions(
                dsn=self.dsn,
                deployment=DEPLOYMENT,
                deployment_dir=self.deployment_dir,
            ),
            admission_id=admission_id,
            source=source,
            run_id=run_id,
            workflow_ref=workflow_ref,
            existing_status="pending",
            admission_mode=policy.admission_mode,
            observed_output_count=changed,
            run_mode="scope_complete",
        )
        if finalized.status != "accepted" or not finalized.refs:
            raise BuildRefused(
                f"{source} run {run_id} ({label}) was not admitted: "
                f"status={finalized.status} reason={finalized.reason}",
                {"verdict": finalized.verdict, "fact_refs": len(finalized.refs)},
            )
        run = _Run(
            source=source,
            run_id=run_id,
            admission_id=admission_id,
            nodes=nodes,
            edges=edges,
            seconds=round(time.perf_counter() - started, 3),
        )
        self.runs.append(run)
        return run


# ------------------------------------------------------------------- publish
def _publish(dsn: str, deployment_dir: Path, *, label: str) -> dict[str, Any]:
    from okg.substrate.publisher.factory import build_deployment_publisher

    publisher = build_deployment_publisher(
        dsn=dsn,
        deployment_name=DEPLOYMENT,
        command=f"okg.bench.workspace_scale {label}",
        deployment_dir=deployment_dir,
    )
    started = time.perf_counter()
    result = publisher.cycle()
    seconds = round(time.perf_counter() - started, 3)
    if result.get("status") != "published":
        raise BuildRefused(
            f"publish {label} did not publish: status={result.get('status')!r} "
            f"reason={result.get('blocked_reason') or result.get('reason')!r}",
            {k: result.get(k) for k in ("status", "blocked_reason", "reason", "generation_id")},
        )
    # The ingest service marks the admissions a publish contains right after
    # it (`_apply_publish_admission_feedback`); the generator does the same.
    from okg.substrate.admission import apply_generation_feedback, workflow_ref_from_dbos

    with psycopg.connect(dsn, autocommit=True) as conn, conn.transaction():
        feedback = apply_generation_feedback(
            conn,
            deployment_name=DEPLOYMENT,
            generation_id=str(result["generation_id"]),
            workflow_ref=workflow_ref_from_dbos("okg_source_sync"),
        )
    return {
        "label": label,
        "generation_id": result.get("generation_id"),
        "nodes_added": result.get("nodes_added"),
        "edges_added": result.get("edges_added"),
        "seconds": seconds,
        "feedback_updated": feedback.get("updated"),
    }


def _require_envelopes_published(dsn: str, runs: list[_Run]) -> None:
    ids = [r.admission_id for r in runs]
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT admission_id::text, status FROM okg.admission_envelopes "
            "WHERE admission_id = ANY(%s::uuid[])",
            (ids,),
        ).fetchall()
    status = dict(rows)
    stuck = {i: status.get(i, "missing") for i in ids if status.get(i) != "published"}
    if stuck:
        raise BuildRefused(
            f"{len(stuck)} admission envelope(s) were not published by the "
            f"publish that followed them: {dict(list(stuck.items())[:5])}"
        )


# ------------------------------------------------------------------- history
def _churn_slices(total: int, churn: int, publishes: int) -> list[tuple[int, int]]:
    """(absolute start, length) windows, one per follow-up publish.

    The start is NOT reduced modulo ``total``: ``_circular`` derives each
    record's revision from how many laps the absolute position has made, so
    a window that starts on the second lap writes revision 2, never a
    revision the record already holds (which would be a no-op run).
    """
    if publishes <= 1 or churn <= 0 or total <= 0:
        return []
    follow_ups = publishes - 1
    per = churn // follow_ups
    extra = churn % follow_ups
    out: list[tuple[int, int]] = []
    cursor = 0
    for p in range(follow_ups):
        length = per + (1 if p < extra else 0)
        if length > total:
            raise BuildRefused(
                f"a follow-up publish would re-write {length} of {total} items, "
                "the same item twice in one run; raise the profile's publishes"
            )
        out.append((cursor, length))
        cursor += length
    return out


def _circular(seq_len: int, start: int, length: int) -> Iterator[tuple[int, int]]:
    """(index, revision bump) over a circular window; each lap is a revision."""
    for k in range(length):
        absolute = start + k
        yield absolute % seq_len, 1 + absolute // seq_len


# -------------------------------------------------------------------- report
def _interval_rows(conn: psycopg.Connection) -> dict[str, int]:
    out: dict[str, int] = {}
    for kind in ("nodes", "edges"):
        row = conn.execute(
            "SELECT relname FROM pg_class WHERE relname LIKE %s AND relkind = 'r'",
            (f"_chronos_b_interval_okg_graph_{kind}_%",),
        ).fetchone()
        if row is None:
            raise BuildRefused(f"no Chronos interval table for graph {kind}")
        out[kind] = int(
            conn.execute(f'SELECT count(*) FROM public."{row[0]}"').fetchone()[0]
        )
    return out


def census(dsn: str, *, search_term: str | None = None) -> dict[str, Any]:
    """Read the shape of the last published okg-workspace generation."""
    from okg.substrate.graph_store import ChronosGraphStore

    if search_term is None:
        search_term = load_targets().search_term

    with psycopg.connect(dsn, autocommit=True) as conn:
        gen = conn.execute(
            "SELECT id FROM okg.v_generations "
            "WHERE deployment_name = %s AND status = 'published' "
            "ORDER BY published_at DESC, id DESC LIMIT 1",
            (DEPLOYMENT,),
        ).fetchone()
        if gen is None:
            raise BuildRefused("no published okg-workspace generation to read")
        generation_id = gen[0]
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        nodes = store.query_generation_once(
            generation_id,
            "SELECT source, subtype, count(*) AS n FROM okg.graph_nodes GROUP BY 1, 2",
        )
        edges = store.query_generation_once(
            generation_id,
            "SELECT source, edge_type, count(*) AS n FROM okg.graph_edges GROUP BY 1, 2",
        )
        searchable = store.query_generation_once(
            generation_id,
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE attrs::text ILIKE %(term)s",
            {"term": f"%{search_term}%"},
        )

        def count(sql: str, *params: Any) -> int:
            return int(conn.execute(sql, params).fetchone()[0])

        # Per producer kind: the publisher's own native-view envelopes from a
        # follow-up publish stay `accepted`, which is its behavior, not the
        # generator's; source runs are what the generator answers for.
        envelope_status: dict[str, dict[str, int]] = {}
        for kind, status, n in conn.execute(
            "SELECT producer_kind, status, count(*) FROM okg.admission_envelopes "
            "GROUP BY 1, 2"
        ).fetchall():
            envelope_status.setdefault(str(kind), {})[str(status)] = int(n)
        runtime = {
            "blocked_generations": count(
                "SELECT count(*) FROM okg.v_generations "
                "WHERE deployment_name = %s AND status = 'blocked'", DEPLOYMENT),
            "admission_envelopes_by_kind_status": envelope_status,
            "published_generations": count(
                "SELECT count(*) FROM okg.v_generations "
                "WHERE deployment_name = %s AND status = 'published'", DEPLOYMENT),
            "generations": count(
                "SELECT count(*) FROM okg.v_generations WHERE deployment_name = %s",
                DEPLOYMENT),
            "branch_checkpoints": count(
                "SELECT count(*) FROM public._chronos_branch_interval_checkpoints"),
            "source_progress": count(
                "SELECT count(*) FROM okg.source_progress WHERE deployment = %s", DEPLOYMENT),
            "source_progress_sources": count(
                "SELECT count(DISTINCT source_name) FROM okg.source_progress "
                "WHERE deployment = %s", DEPLOYMENT),
            "admission_envelopes": count("SELECT count(*) FROM okg.admission_envelopes"),
            "admission_events": count("SELECT count(*) FROM okg.admission_events"),
            "nomos_decisions": count("SELECT count(*) FROM okg.nomos_decisions"),
            "derived_artifact_runs": count("SELECT count(*) FROM okg.derived_artifact_runs"),
            "derived_artifact_definitions": count(
                "SELECT count(*) FROM okg.derived_artifact_definitions"),
            "enricher_progress": count("SELECT count(*) FROM okg.enricher_progress"),
            "node_facts": count("SELECT count(*) FROM okg.node_facts"),
            "edge_facts": count("SELECT count(*) FROM okg.edge_facts"),
            "node_series": count("SELECT count(*) FROM okg.node_series"),
            "active_policy_bundles": count(
                "SELECT count(*) FROM okg.nomos_policy_activations "
                "WHERE deployment_name = %s AND status = 'active'", DEPLOYMENT),
            "database_bytes": count("SELECT pg_database_size(current_database())"),
        }
        runtime["interval_rows"] = _interval_rows(conn)
    by_subtype: Counter[str] = Counter()
    by_source_subtype: dict[str, dict[str, int]] = {}
    for row in nodes:
        by_subtype[row["subtype"]] += int(row["n"])
        by_source_subtype.setdefault(str(row["source"]), {})[row["subtype"]] = int(row["n"])
    by_type: Counter[str] = Counter()
    by_edge_source: Counter[str] = Counter()
    for row in edges:
        by_type[row["edge_type"]] += int(row["n"])
        by_edge_source[str(row["source"])] += int(row["n"])
    return {
        "generation_id": generation_id,
        "records": sum(by_subtype.values()),
        "relationships": sum(by_type.values()),
        "records_by_subtype": dict(sorted(by_subtype.items())),
        "records_by_source": {k: dict(sorted(v.items())) for k, v in sorted(by_source_subtype.items())},
        "relationships_by_type": dict(sorted(by_type.items())),
        "relationships_by_source": dict(sorted(by_edge_source.items())),
        "search_term": search_term,
        "records_with_search_term": int(searchable[0]["n"]),
        "runtime": runtime,
    }


def _progress_rows(
    plan: ScaledPlan, runs: list[_Run], dsn: str
) -> int:
    from okg.substrate.ingest.runner import upsert_source_progress

    by_source: dict[str, list[_Run]] = {}
    for run in runs:
        by_source.setdefault(run.source, []).append(run)
    written = 0
    for source, rows in plan.source_progress_rows.items():
        history = by_source.get(source, [])
        # The recorded row count is per source; a source this build ran fewer
        # times than that (or never: the census has progress rows for sources
        # with no live records) gets earlier, zero-output runs under synthetic
        # run ids so its row count matches.
        earlier = [
            str(uuid.uuid5(_RUN_NAMESPACE, f"{source}:earlier:{n}"))
            for n in range(max(0, rows - 1 - len(history)))
        ]
        run_ids = earlier + [r.run_id for r in history]
        latest = history[-1] if history else None

        def payload(run_id: str) -> str:
            return json.dumps(
                {
                    "schema": "okg.source_sync.stage_timings.last_run.v1",
                    "deployment": DEPLOYMENT,
                    "source_name": source,
                    "registry_source_name": source.split(".", 1)[-1],
                    "run_id": run_id,
                    "status": "completed",
                    "timing_status": "available",
                    "run_mode": "scope_complete",
                    "timings_ms": {"total": (latest.seconds * 1000) if latest else 0.0},
                    "nodes_appended": latest.nodes if latest else 0,
                    "edges_appended": latest.edges if latest else 0,
                    "nodes_retracted": 0,
                    "edges_retracted": 0,
                    "input_count": (latest.nodes + latest.edges) if latest else 0,
                    "output_count": (latest.nodes + latest.edges) if latest else 0,
                    "sync_scope": {},
                    "source_stats": {"generator": "okg.bench.workspace_scale"},
                },
                sort_keys=True,
            )

        if rows < 1:
            continue
        if not run_ids:
            run_ids = [str(uuid.uuid5(_RUN_NAMESPACE, f"{source}:earlier:last"))]
        progress = {"sync.stage_timings.last_run": payload(run_ids[-1])}
        for run_id in run_ids[-(rows - 1):] if rows > 1 else []:
            progress[f"sync.stage_timings.run.{run_id}"] = payload(run_id)
        upsert_source_progress(
            dsn=dsn, deployment=DEPLOYMENT, source_name=source, progress=progress
        )
        written += len(progress)
    return written


# ---------------------------------------------------------------------- build
def build(options: BuildOptions) -> dict[str, Any]:
    """Build the graph; return a JSON-ready report. Raises ``BuildRefused``."""
    from okg.substrate.sources.registry import load_source_registry

    t0 = time.perf_counter()
    deployment_dir = (options.deployment_dir or default_deployment_dir()).resolve()
    targets = load_targets()
    plan = scaled_plan(targets, scale=options.scale, profile=options.profile)
    _check_preconditions(options)
    setup = _prepare_database(options, deployment_dir)
    options.log(f"setup: {setup}")
    facts = WorkspaceFacts(
        plan,
        deployment_dir=deployment_dir,
        search_term=targets.search_term,
        text_chars=targets.text_chars,
        enricher_plants=plan.enricher_edges,
    )
    registry = {
        entry.ownership_id: entry
        for entry in load_source_registry(
            deployment_dir / "source_registry.yaml", repo_root_override=repo_root()
        ).values()
    }
    admission = _Admission(options.dsn, registry, deployment_dir)
    publishes: list[dict[str, Any]] = []

    # Bulk load: one run per producer, in endpoint order.
    t_load = time.perf_counter()
    order = facts.producer_order()
    for source in order:
        run = admission.run(
            source,
            chain(facts.iter_node_facts(source), facts.iter_edge_facts(source)),
            label="bulk",
            chunk_size=options.chunk_size,
        )
        options.log(f"run {source}: {run.nodes} records, {run.edges} relationships, {run.seconds}s")
    load_seconds = round(time.perf_counter() - t_load, 3)
    publishes.append(_publish(options.dsn, deployment_dir, label="bulk"))
    options.log(f"publish bulk: {publishes[-1]}")
    _require_envelopes_published(options.dsn, admission.runs)

    # History: follow-up publishes, each re-writing a slice.
    node_index = [
        (source, subtype, i)
        for source, subtype in sorted(facts.node_specs)
        for i in range(facts.node_specs[(source, subtype)].count)
    ]
    edge_index = [
        (index, k)
        for index, spec in enumerate(facts.edge_specs)
        for k in range(spec.target.count)
    ]
    node_slices = _churn_slices(len(node_index), plan.record_churn, plan.profile.publishes)
    edge_slices = _churn_slices(len(edge_index), plan.relationship_churn, plan.profile.publishes)
    t_hist = time.perf_counter()
    for p in range(max(0, plan.profile.publishes - 1)):
        by_source: dict[str, list[NodeFact | EdgeFact]] = {}
        if p < len(node_slices):
            start, length = node_slices[p]
            for idx, rev in _circular(len(node_index), start, length):
                source, subtype, i = node_index[idx]
                by_source.setdefault(source, []).append(
                    facts.node_fact(source, subtype, i, revision=rev)
                )
        if p < len(edge_slices):
            start, length = edge_slices[p]
            for idx, rev in _circular(len(edge_index), start, length):
                spec_index, k = edge_index[idx]
                spec = facts.edge_specs[spec_index]
                src, dst = facts.edge_pairs(spec_index)[k]
                by_source.setdefault(spec.target.source, []).append(
                    facts.edge_fact(spec.target.edge_type, src, dst, revision=rev)
                )
        if not by_source:
            # A profile with publishes but no churn still needs a change per
            # publish, or the publish is a no-op. Touch one record.
            source, subtype, i = node_index[p % len(node_index)]
            by_source[source] = [facts.node_fact(source, subtype, i, revision=p + 1)]
        first = len(admission.runs)
        for source in sorted(by_source):
            admission.run(
                source, by_source[source], label=f"history-{p + 1}",
                chunk_size=options.chunk_size,
            )
        publishes.append(_publish(options.dsn, deployment_dir, label=f"history-{p + 1}"))
        _require_envelopes_published(options.dsn, admission.runs[first:])
        options.log(f"publish history-{p + 1}: {publishes[-1]}")
    history_seconds = round(time.perf_counter() - t_hist, 3)

    progress_rows = _progress_rows(plan, admission.runs, options.dsn)
    result = census(options.dsn, search_term=targets.search_term)
    expected_nodes = plan.expected_nodes_by_subtype()
    report = {
        "schema": "okg.bench.workspace_scale.report.v1",
        "deployment": DEPLOYMENT,
        "database": _dbname(options.dsn),
        "scale": options.scale,
        "profile": options.profile,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "setup": setup,
        "expected": {
            "records": sum(expected_nodes.values()),
            "records_by_subtype": dict(sorted(expected_nodes.items())),
            "generator_relationships_by_type": facts.edges_by_type(),
            "generator_relationships_by_source": facts.edges_by_source(),
            "enricher_relationships": plan.enricher_edges,
            "search_records": plan.search_records,
            "source_progress_rows": sum(plan.source_progress_rows.values()),
            "publishes": plan.profile.publishes,
            "record_churn": plan.record_churn,
            "relationship_churn": plan.relationship_churn,
        },
        "clamped_relationships": facts.clamped,
        "runs": len(admission.runs),
        "publishes": publishes,
        "progress_rows_written": progress_rows,
        "observed": result,
        "timings_s": {
            "load": load_seconds,
            "history": history_seconds,
            "total": round(time.perf_counter() - t0, 3),
        },
        "peak_rss_mb": _peak_rss_mb(),
    }
    report["verification"] = verify_report(report, facts, targets)
    failed = [c for c in report["verification"] if not c["ok"]]
    if failed:
        raise BuildRefused(
            f"the built graph missed {len(failed)} target check(s)",
            failed,
            report=report,
        )
    return report


def _peak_rss_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB, macOS bytes.
    return round(peak / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)


def verify_report(
    report: dict[str, Any], facts: WorkspaceFacts, targets: Any
) -> list[dict[str, Any]]:
    """Every check the build answers for, each with its bound and result.

    The build refuses when any is not ``ok``; the list goes into the report
    either way, so a nightly reader sees what was checked and by how much.
    """
    from okg.bench.workspace_scale.targets import scale_count

    observed, expected = report["observed"], report["expected"]
    runtime = observed["runtime"]
    plan = facts.plan
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, **detail: Any) -> None:
        checks.append({"check": name, "ok": bool(ok), **detail})

    # 1. What the generator wrote lands exactly.
    misses = []
    for source, by_subtype in plan.nodes.items():
        got = observed["records_by_source"].get(source, {})
        for subtype, want in by_subtype.items():
            if got.get(subtype, 0) != want:
                misses.append({"source": source, "type": subtype,
                               "wrote": want, "published": got.get(subtype, 0)})
    check("generator_records_exact", not misses, misses=misses[:20])
    misses = []
    for source, want in facts.edges_by_source().items():
        got = observed["relationships_by_source"].get(source, 0)
        if got != want:
            misses.append({"source": source, "wrote": want, "published": got})
    check("generator_relationships_exact", not misses, misses=misses[:20])

    # 2. The publisher's own records (native views) are the recorded count.
    for source, by_subtype in plan.publisher_nodes.items():
        got = observed["records_by_source"].get(source, {})
        check("publisher_records", got == by_subtype, source=source,
              expected=by_subtype, observed=got)

    # 3. Each enricher derived its relationships from the planted attributes,
    #    within the stated tolerance of the planned count.
    tol = targets.raw["enricher_edges"].get("tolerance") or {}
    absolute = int(tol.get("absolute", 0))
    relative = float(tol.get("relative", 0.0))
    for producer, planned in plan.enricher_edges.items():
        got = observed["relationships_by_source"].get(producer, 0)
        own = targets.raw["enricher_edges"].get(producer, {}).get("tolerance_absolute")
        bound = max(absolute if own is None else int(own), int(round(relative * planned)))
        check(
            "enricher_relationships",
            got >= 1 and abs(got - planned) <= bound,
            producer=producer, planned=planned, observed=got, bound=bound,
        )

    # 4. The search term reaches the planted share and the scaled minimum.
    minimum = scale_count(targets.search_minimum_candidates, plan.scale)
    got = observed["records_with_search_term"]
    check("search_term_share", got >= plan.search_records and got >= minimum,
          term=targets.search_term, planned=plan.search_records,
          minimum=minimum, observed=got)

    # 5. History: every publish published, and the interval tables reached
    #    the depth the profile plans (live rows plus the re-writes).
    check("publishes", runtime["published_generations"] == plan.profile.publishes
          and runtime["blocked_generations"] == 0,
          planned=plan.profile.publishes,
          published=runtime["published_generations"],
          blocked=runtime["blocked_generations"])
    depth_tol = float(targets.raw["history"].get("interval_depth_tolerance", 0.0))
    for kind, live, churn in (
        ("nodes", observed["records"], expected["record_churn"]),
        ("edges", observed["relationships"], expected["relationship_churn"]),
    ):
        planned_rows = live + churn
        got = runtime["interval_rows"][kind]
        check("interval_depth", got >= planned_rows * (1.0 - depth_tol),
              kind=kind, planned=planned_rows, observed=got, tolerance=depth_tol)
    return checks


__all__ = [
    "BuildOptions",
    "BuildRefused",
    "DATABASE_PREFIX",
    "DEPLOYMENT",
    "build",
    "census",
    "default_deployment_dir",
]
