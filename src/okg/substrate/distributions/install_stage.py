"""Exact installer-owned staging observations, checked under the branch fence."""
from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json

from okg.substrate.errors import SubstrateError


@dataclass(frozen=True)
class InstallStage:
    deployment: str
    branch: str
    latest_checkpoint_id: str | None
    branch_mutation_token: str
    admission_ids: tuple[str, ...]
    admission_digest: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict) -> InstallStage:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise SubstrateError("invalid installer stage receipt")
        ids = value["admission_ids"]
        if (type(ids) not in {list, tuple} or not ids
                or not all(type(item) is str and item for item in ids)
                or tuple(sorted(set(ids))) != tuple(ids)
                or type(value["deployment"]) is not str or not value["deployment"]
                or value["branch"] != "default" or type(value["branch"]) is not str
                or (value["latest_checkpoint_id"] is not None and type(value["latest_checkpoint_id"]) is not str)
                or type(value["admission_digest"]) is not str or len(value["admission_digest"]) != 64
                or any(c not in "0123456789abcdef" for c in value["admission_digest"])
                or type(value["branch_mutation_token"]) is not str or not value["branch_mutation_token"]):
            raise SubstrateError("invalid installer stage receipt")
        result = cls(**{**value, "admission_ids": tuple(ids)})
        return result


def observe_stage(conn, *, deployment: str, admission_ids: tuple[str, ...]) -> InstallStage:
    """Caller holds the branch fence; every source writer participates in it."""
    from okg.substrate.publisher.publisher import Publisher
    rows = conn.execute(
        "SELECT admission_id::text,status,admission_mode,source,run_id::text,"
        "catalog_version_id,verdict,authority,check_summary,workflow_ref,target_generation_ref "
        "FROM okg.admission_envelopes WHERE deployment_name=%s AND producer_kind='source' "
        "AND COALESCE(target_branch_name,'default')='default' AND status!='published' "
        "ORDER BY admission_id", (deployment,),
    ).fetchall()
    if tuple(row[0] for row in rows) != admission_ids or not rows:
        raise SubstrateError("installer stage admission set changed")
    for row in rows:
        if row[10] is not None:
            raise SubstrateError("installer stage is already bound to a generation")
        if row[1] != "accepted" and not (row[1] == "pending" and row[2] in {"direct", "fast_track"}):
            raise SubstrateError("installer stage is not admitted for publication")
    refs = conn.execute(
        "SELECT admission_id::text,ref_key,ref_kind,source,run_id::text,fact_id_min,"
        "fact_id_max,record_count,payload FROM okg.admission_record_refs "
        "WHERE admission_id=ANY(%s::uuid[]) ORDER BY admission_id,ref_key",
        (list(admission_ids),),
    ).fetchall()
    if not refs:
        raise SubstrateError("installer stage has no source evidence")
    digest = hashlib.sha256(json.dumps([rows, refs], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return InstallStage(deployment, "default", latest_default_checkpoint_id(conn, deployment),
        Publisher.chronos_graph_branch_mutation_token(conn, branch_name="default"), admission_ids, digest)


def latest_default_checkpoint_id(conn, deployment: str) -> str | None:
    """The deployment's newest checkpoint on default, of any status.

    Filters on the deployment, not the generation kind, so it leaves out
    the private run-base checkpoint every source run cuts (issue #2993).
    """
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    checkpoint = conn.execute(
        "SELECT checkpoint_id FROM public._chronos_branch_interval_checkpoints "
        "WHERE metadata::jsonb->>'deployment_name'=%s "
        "AND COALESCE(metadata::jsonb->>'branch','default')='default' "
        f"AND {EXCLUDE_RUN_BASE_SQL} "
        "ORDER BY created_at DESC,checkpoint_id DESC LIMIT 1", (deployment,),
    ).fetchone()
    return str(checkpoint[0]) if checkpoint else None


def require_stage(conn, expected: InstallStage) -> None:
    if type(expected) is not InstallStage or observe_stage(conn, deployment=expected.deployment,
            admission_ids=expected.admission_ids) != expected:
        raise SubstrateError("installer stage changed; publication refused")


def capture_stage(dsn: str, *, deployment: str, admission_ids: tuple[str, ...], expected: InstallStage | None = None) -> InstallStage:
    from okg.substrate.db.connection import connect as db_connect
    with db_connect(dsn, autocommit=True, application_name="okg-install-stage") as conn, conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext('okg.chronos_graph'),hashtext('default'))")
        result = observe_stage(conn, deployment=deployment, admission_ids=admission_ids)
        if expected is not None and result != expected:
            raise SubstrateError("installer stage changed; retry refused")
        return result
