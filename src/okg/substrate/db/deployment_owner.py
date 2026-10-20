"""Per-database owner stamp — the cross-deployment write guard (P0 #419).

A physical OKG database is stamped with exactly one owner deployment identity
in ``okg.database_owner``. Before any generation-producing write, the publish
path (:meth:`Publisher.reserve`) asserts that the publishing deployment matches
the stamp and FAILS CLOSED on mismatch, so a process configured for deployment
X cannot publish into deployment Y's database — the cross-deployment
contamination class (issue #419; the 2026-06-10 clone-DB incident).

Identity is the deployment NAME, read from inside the database itself —
independent of the ambient ``OKG_DSN`` that selected the connection. That is the
one authority the two disagreeing ambient sources (the deployment manifest name
and the env DSN) never reconcile against, which is exactly why every prior guard
(resolution-side ``resolved_target``, the per-deployment catalog write-lease,
the read-side ``mixed_deployment_state`` warning) missed the contamination.
"""
from __future__ import annotations

from okg.substrate.errors import DeploymentOwnerMismatch

# The bootstrap generation (generation_id 0, deployment_name 'bootstrap') is
# planted in every DB by migration; it is not a real resident deployment and
# must never count as foreign ownership.
_BOOTSTRAP = "bootstrap"


def _owner_table_present(cur) -> bool:
    """True once ``okg db migrate`` has installed ``okg.database_owner``.

    Pre-migration databases return False so the guard is a no-op there — NOT a
    silent fallback to a permissive default, but the pre-provisioning state. It
    is surfaced loudly by ``okg doctor``'s database-owner check; the guard
    activates the moment the table exists.
    """
    cur.execute("SELECT to_regclass('okg.database_owner')")
    return cur.fetchone()[0] is not None


def read_db_owner(cur) -> str | None:
    """The stamped owner deployment name, or None if unstamped/pre-migration."""
    if not _owner_table_present(cur):
        return None
    cur.execute("SELECT owner_deployment_name FROM okg.database_owner")
    row = cur.fetchone()
    return row[0] if row else None


def foreign_published_deployments(cur, claimed: str) -> list[str]:
    """Distinct non-bootstrap deployment names already in the generation ledger
    that are NOT the claiming deployment — evidence the DB already holds another
    deployment's data (the pre-existing-contamination shape, e.g. #419). Used by
    provisioning/seed-stamp to refuse stamping a contaminated DB; NOT consulted
    on the hot publish path."""
    cur.execute(
        """
        SELECT DISTINCT deployment_name
          FROM okg.graph_generations
         WHERE deployment_name NOT IN (%s, %s)
        """,
        (claimed, _BOOTSTRAP),
    )
    return sorted(r[0] for r in cur.fetchall())


def stamp_db_owner(cur, deployment_name: str, *, command: str | None = None) -> None:
    """Stamp the DB owner if unstamped. Idempotent: the ``singleton`` primary key
    makes a second active owner a database-layer violation, so a concurrent first
    stamp by a different deployment cannot also land.

    Stamping is a PROVISIONING action (``okg deployment up`` / the owner-seed
    migration), not a side effect of publishing — so the shared test DB and any
    not-yet-provisioned DB stay unstamped (and therefore permissive) until a
    deployment is deliberately provisioned into them.
    """
    cur.execute(
        """
        INSERT INTO okg.database_owner
          (singleton, owner_deployment_name, stamped_by_command)
        VALUES (TRUE, %s, %s)
        ON CONFLICT (singleton) DO NOTHING
        """,
        (deployment_name, command),
    )


def claim_db_owner(cur, deployment_name: str, *, command: str) -> dict:
    """Idempotently claim an unstamped DB after resident-state validation."""
    owner = read_db_owner(cur)
    if owner not in {None, deployment_name}:
        raise DeploymentOwnerMismatch(
            f"database is owned by deployment {owner!r}, not {deployment_name!r}",
            data={
                "owner_deployment_name": owner,
                "claimed_deployment_name": deployment_name,
            },
            fix_hint="use the database owned by this deployment",
        )
    foreign = foreign_published_deployments(cur, deployment_name)
    if owner is None and foreign:
        raise DeploymentOwnerMismatch(
            "unstamped database contains foreign published deployments",
            data={
                "claimed_deployment_name": deployment_name,
                "resident_deployments": foreign,
            },
            fix_hint="do not claim a database containing another deployment's graph",
        )
    if owner is None:
        stamp_db_owner(cur, deployment_name, command=command)
    claimed = read_db_owner(cur)
    if claimed != deployment_name:
        raise DeploymentOwnerMismatch(
            "database ownership claim did not persist",
            data={
                "owner_deployment_name": claimed,
                "claimed_deployment_name": deployment_name,
            },
        )
    return {
        "owner_before": owner,
        "owner_after": claimed,
        "foreign_published_deployments": foreign,
        "command": command,
    }


def assert_db_owner(cur, claimed_deployment_name: str) -> None:
    """Fail closed unless the connected DB is owned by ``claimed_deployment_name``.

    Runs inside the publish reservation transaction, BEFORE any write:

    * stamped owner == claimed   -> OK.
    * stamped owner != claimed    -> raise (the #419 cross-deployment case).
    * UNSTAMPED                   -> OK (permissive). The guard is ARMED by
      stamping (provisioning / the owner-seed migration), not by publishing, so
      an unprovisioned or legacy DB is not blocked. This keeps the assertion a
      no-op until a deployment is deliberately stamped, and is surfaced — not
      hidden — by ``okg doctor``'s database-owner check.
    * owner table absent (pre-migration) -> no-op; the guard activates once
      ``okg db migrate`` installs ``okg.database_owner``.

    This is NOT a silent fallback to a permissive default in the contamination
    sense: a stamped DB (every provisioned production deployment) fails closed on
    a foreign write. The unstamped case is the pre-provisioning state, made loud
    by doctor — never a degraded-but-accepted publish into someone else's DB.

    The caller's transaction provides atomicity: a raise rolls the whole
    reservation back, so nothing is committed on refusal.
    """
    if not _owner_table_present(cur):
        return
    owner = read_db_owner(cur)
    if owner is None or owner == claimed_deployment_name:
        return
    raise DeploymentOwnerMismatch(
        f"database is owned by deployment {owner!r} but a publish claiming "
        f"deployment {claimed_deployment_name!r} attempted to write into it; "
        f"refusing (cross-deployment contamination guard)",
        data={
            "owner_deployment_name": owner,
            "claimed_deployment_name": claimed_deployment_name,
        },
        fix_hint=(
            "publish into this deployment's OWN database (check OKG_DSN / the "
            "deployment manifest's postgres.dsn); never point a process at "
            "another deployment's DB"
        ),
    )
