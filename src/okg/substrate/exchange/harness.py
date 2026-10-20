"""Two-OKG Poros harness setup utilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb
from okg.substrate.db.connection import connect as db_connect


class PorosHarnessError(ValueError):
    """Raised when the two-OKG harness cannot prove separation."""


@dataclass(frozen=True)
class PorosDatabaseIdentity:
    dsn: str
    database: str
    server_addr: str
    server_port: str
    user: str

    def stable_key(self) -> tuple[str, str, str]:
        return (self.server_addr, self.server_port, self.database)

    def to_dict(self) -> dict[str, str]:
        return {
            "database": self.database,
            "server_addr": self.server_addr,
            "server_port": self.server_port,
            "user": self.user,
        }


@dataclass(frozen=True)
class PorosHarnessSide:
    role: str
    deployment: str
    identity: PorosDatabaseIdentity
    migrate: dict[str, Any] | None
    seed: dict[str, Any]
    publish: dict[str, Any]
    generation: dict[str, Any]
    doctor: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "deployment": self.deployment,
            "database": self.identity.to_dict(),
            "migrate": self.migrate,
            "seed": self.seed,
            "publish": self.publish,
            "generation": self.generation,
            "doctor": self.doctor,
        }


@dataclass(frozen=True)
class PorosTwoOKGHarnessResult:
    sender: PorosHarnessSide
    receiver: PorosHarnessSide

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "sender": self.sender.to_dict(),
            "receiver": self.receiver.to_dict(),
            "separate_databases": (
                self.sender.identity.stable_key() != self.receiver.identity.stable_key()
            ),
            "verification_commands": {
                "sender_generation": (
                    "okg generation describe "
                    f"{self.sender.generation['generation_id']} "
                    f"--deployment {self.sender.deployment} --dsn <sender-dsn>"
                ),
                "receiver_generation": (
                    "okg generation describe "
                    f"{self.receiver.generation['generation_id']} "
                    f"--deployment {self.receiver.deployment} --dsn <receiver-dsn>"
                ),
                "sender_doctor": (
                    f"okg doctor --deployment {self.sender.deployment} "
                    "--dsn <sender-dsn>"
                ),
                "receiver_doctor": (
                    f"okg doctor --deployment {self.receiver.deployment} "
                    "--dsn <receiver-dsn>"
                ),
            },
        }


def initialize_two_okg_harness(
    *,
    sender_dsn: str,
    receiver_dsn: str,
    sender_deployment: str = "poros-sender",
    receiver_deployment: str = "poros-receiver",
    migrate_schema: bool = True,
) -> PorosTwoOKGHarnessResult:
    """Migrate and publish two independent OKG deployments.

    The harness intentionally does not create databases. Operators pass
    two DSNs that already point at databases they are willing to use for
    the local Poros demonstration. The function fails if both DSNs
    resolve to the same `(server_addr, server_port, database)` identity.
    """
    sender_identity = database_identity(sender_dsn)
    receiver_identity = database_identity(receiver_dsn)
    assert_distinct_database_identities(sender_identity, receiver_identity)

    sender = _initialize_side(
        role="sender",
        dsn=sender_dsn,
        deployment=sender_deployment,
        identity=sender_identity,
        migrate_schema=migrate_schema,
    )
    receiver = _initialize_side(
        role="receiver",
        dsn=receiver_dsn,
        deployment=receiver_deployment,
        identity=receiver_identity,
        migrate_schema=migrate_schema,
    )
    return PorosTwoOKGHarnessResult(sender=sender, receiver=receiver)


def database_identity(dsn: str) -> PorosDatabaseIdentity:
    with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT current_database(),
                   COALESCE(inet_server_addr()::text, ''),
                   current_setting('port'),
                   current_user
            """
        )
        database, server_addr, server_port, user = cur.fetchone()
    info = psycopg.conninfo.conninfo_to_dict(dsn)
    fallback_host = str(info.get("host") or "localhost")
    return PorosDatabaseIdentity(
        dsn=dsn,
        database=str(database),
        server_addr=str(server_addr or fallback_host),
        server_port=str(server_port),
        user=str(user),
    )


def assert_distinct_database_identities(
    sender: PorosDatabaseIdentity,
    receiver: PorosDatabaseIdentity,
) -> None:
    if sender.stable_key() == receiver.stable_key():
        raise PorosHarnessError(
            "Poros two-OKG harness requires separate sender and receiver "
            "databases; both DSNs resolve to "
            f"{sender.server_addr}:{sender.server_port}/{sender.database}"
        )


def _initialize_side(
    *,
    role: str,
    dsn: str,
    deployment: str,
    identity: PorosDatabaseIdentity,
    migrate_schema: bool,
) -> PorosHarnessSide:
    migrate_payload: dict[str, Any] | None = None
    if migrate_schema:
        from okg.substrate.db.migrate import migrate

        migrated = migrate(dsn, deployment_name=deployment)
        migrate_payload = {
            "statements_executed": migrated.statements_executed,
            "statements_skipped": migrated.statements_skipped,
            "idempotent_second_run": migrated.idempotent_second_run,
            "extension_versions": dict(migrated.extension_versions),
        }

    seed_payload = _seed_initial_harness_fact(
        dsn=dsn,
        role=role,
        deployment=deployment,
    )
    publish_payload = _publish_initial_generation(dsn, deployment)
    generation_id = publish_payload.get("generation_id")
    if generation_id is None:
        raise PorosHarnessError(
            f"{role} publish did not report a generation_id: {publish_payload}"
        )
    generation_payload = describe_generation(dsn, generation_id)
    if generation_payload.get("status") != "published":
        raise PorosHarnessError(
            f"{role} generation {generation_id} is not published: {generation_payload}"
        )
    doctor_payload = database_doctor(dsn)
    if not doctor_payload.get("ok"):
        raise PorosHarnessError(f"{role} doctor check failed: {doctor_payload}")
    return PorosHarnessSide(
        role=role,
        deployment=deployment,
        identity=identity,
        migrate=migrate_payload,
        seed=seed_payload,
        publish=publish_payload,
        generation=generation_payload,
        doctor=doctor_payload,
    )


def _seed_initial_harness_fact(
    *,
    dsn: str,
    role: str,
    deployment: str,
) -> dict[str, Any]:
    """Insert one real substrate fact so initial publish cannot no-op."""
    run_id = str(uuid4())
    node_id = f"poros:harness:{deployment}:{run_id}"
    source = "poros_harness"
    source_record_id = {
        "kind": "poros_harness_seed",
        "role": role,
        "deployment": deployment,
        "run_id": run_id,
    }
    source_revision = {"version": 1, "run_id": run_id}
    attrs = {
        "text": f"Poros {role} harness seed for {deployment}",
        "role": role,
        "deployment": deployment,
        "poros_harness": True,
    }
    dedupe_key = f"{source}:{deployment}:{role}:{run_id}"
    with db_connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
                node_id, subtype, attrs, source, source_record_id,
                source_revision, observed_at, dedupe_key, run_id, op
            )
            VALUES (
                %s, 'concept', %s, %s, %s, %s, now(), %s, %s::uuid, 'I'
            )
            RETURNING fact_id, observed_at
            """,
            (
                node_id,
                Jsonb(attrs),
                source,
                Jsonb(source_record_id),
                Jsonb(source_revision),
                dedupe_key,
                run_id,
            ),
        )
        fact_id, observed_at = cur.fetchone()
    return {
        "node_fact_id": int(fact_id),
        "node_id": node_id,
        "subtype": "concept",
        "source": source,
        "run_id": run_id,
        "observed_at": (
            observed_at.isoformat()
            if isinstance(observed_at, datetime)
            else str(observed_at)
        ),
    }


def _publish_initial_generation(dsn: str, deployment: str) -> dict[str, Any]:
    from okg.substrate.publisher.factory import build_deployment_publisher

    payload = build_deployment_publisher(
        dsn=dsn,
        deployment_name=deployment,
        command="okg exchange harness publish",
    ).cycle()
    if payload.get("status") != "published":
        raise PorosHarnessError(
            f"initial publish for {deployment!r} did not publish: {payload}"
        )
    return dict(payload)


def describe_generation(dsn: str, generation_id: int | str) -> dict[str, Any]:
    # Chronos cutover: a published generation is a Chronos CHECKPOINT with a
    # STRING id and NO okg.graph_generations row, so resolve its metadata from
    # the checkpoint store (a bigint graph_generations lookup crashes on the
    # chronos string id). Graph-content counts resolve through a checkout pinned
    # to the generation.
    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        try:
            cp = store.get_generation(generation_id)
        except Exception:  # noqa: BLE001 — missing checkpoint -> "not found".
            cp = None
        if cp is None:
            raise PorosHarnessError(f"generation {generation_id} not found")
        meta = dict(cp.metadata or {})
        graph = store.checkout_generation(generation_id)
        nodes_live = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"] or 0
        )
        edges_live = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"] or 0
        )
    payload = {
        "generation_id": cp.checkpoint_id,
        "deployment_name": meta.get("deployment_name"),
        "status": meta.get("status"),
        "blocked_reason": meta.get("blocked_reason"),
        "catalog_version_id": meta.get("catalog_version_id"),
        "published_at": meta.get("created_at"),
        "invariant_summary": meta.get("invariant_summary"),
        "excluded_sources": meta.get("excluded_sources"),
        "correlation_id": meta.get("correlation_id"),
        "nodes_live": nodes_live,
        "edges_live": edges_live,
    }
    if "deployment_attestations" in meta:
        payload["deployment_attestations"] = meta["deployment_attestations"]
    return payload


def database_doctor(dsn: str) -> dict[str, Any]:
    from okg.substrate.db.migrate import OPTIONAL_EXTENSIONS, REQUIRED_EXTENSIONS

    report: dict[str, Any] = {
        "ok": True,
        "extensions": {},
        "roles": {},
        "errors": [],
    }
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        all_extensions = list(REQUIRED_EXTENSIONS) + list(OPTIONAL_EXTENSIONS)
        cur.execute(
            """
            SELECT extname, extversion
              FROM pg_extension
             WHERE extname = ANY(%s)
            """,
            (all_extensions,),
        )
        installed = dict(cur.fetchall())
        for name in all_extensions:
            report["extensions"][name] = installed.get(name)
            if name in REQUIRED_EXTENSIONS and name not in installed:
                report["ok"] = False
                report["errors"].append(
                    {
                        "code": "extension_missing",
                        "extension": name,
                    }
                )
        for role in ("app_rw", "app_ro", "catalog_admin"):
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            present = cur.fetchone() is not None
            report["roles"][role] = present
            if not present:
                report["ok"] = False
                report["errors"].append(
                    {
                        "code": "role_missing",
                        "role": role,
                    }
                )
    return report
