"""Isolated-database lifecycle (standardize-deployment-authority,
req.deploy.isolation-lifecycle).

Worktree-isolated databases are named `<declared_database>_<sha256(
worktree_root)[:10]>` (catalog.ownership.suggested_worktree_dsn). That
makes provenance DERIVABLE: a suffix that matches a live worktree is
owned; one that matches nothing is an orphan. Listing never mutates;
gc defaults to dry-run and only ever drops orphans of the declared
database - never the declared database itself.
"""

from __future__ import annotations

import hashlib
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.db import dsn as dsn_mod
from okg.substrate.deployment_authority import _declared_database


_MAX_INVENTORY_DIAGNOSTIC = 160
_MAX_INVENTORY_DIAGNOSTIC_INPUT = _MAX_INVENTORY_DIAGNOSTIC * 4
_REDACTED_SECRET = "<redacted>"
_GIT_OBJECT_ID_RE = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class _SafeInventoryCause:
    """Bounded exception metadata safe to expose to callers and logs."""

    type: str
    message: str


def _inventory_cause_type(cause: BaseException | None) -> str:
    """Return a stable, non-user-controlled category for a probe failure."""
    if cause is None:
        return "WorktreeInventoryUnavailable"
    if isinstance(cause, subprocess.TimeoutExpired):
        return "TimeoutExpired"
    if isinstance(cause, subprocess.CalledProcessError):
        return "CalledProcessError"
    if isinstance(cause, UnicodeDecodeError):
        return "UnicodeDecodeError"
    if isinstance(cause, UnicodeError):
        return "UnicodeError"
    if isinstance(cause, OSError):
        return "OSError"
    if isinstance(cause, TypeError):
        return "TypeError"
    if isinstance(cause, ValueError):
        return "ValueError"
    if isinstance(cause, RuntimeError):
        return "RuntimeError"
    return "Exception"


def _bounded_inventory_diagnostic(value: object) -> str:
    """Return a bounded, single-line diagnostic safe for CLI payloads."""
    try:
        if isinstance(value, str):
            raw = value[:_MAX_INVENTORY_DIAGNOSTIC_INPUT]
        elif isinstance(value, BaseException):
            message = next(
                (argument for argument in value.args if isinstance(argument, str)),
                type(value).__name__,
            )
            raw = message[:_MAX_INVENTORY_DIAGNOSTIC_INPUT]
        else:
            raw = type(value).__name__
        text = " ".join(raw.split())
    except Exception:  # noqa: BLE001 - diagnostic formatting must not mask the cause.
        text = type(value).__name__
    text = _over_masked(text)
    return (text or "unknown worktree inventory failure")[:_MAX_INVENTORY_DIAGNOSTIC]


def _over_masked(text: str) -> str:
    """``text`` with every URL token dropped and everything after a secret cut.

    A git inventory failure has no use for a connection string, so a token
    holding ``://`` is replaced whole, not only its password. The shared
    masker (:func:`okg.substrate.db.dsn.redact_text`) then locates the first
    secret (``password=``, ``PGPASSWORD=``, ``sslpassword=``, ...); the text is
    cut there, so an unquoted password with a blank, or a quoted one that the
    input bound truncated, cannot show a second word.
    """
    text = " ".join(
        dsn_mod.REDACTED_DSN if "://" in token else token for token in text.split(" ")
    )
    masked = dsn_mod.redact_text(text)
    if masked == text:
        return text
    cut = len(os.path.commonprefix([text, masked]))
    # A secret that starts with the mask character would otherwise show it.
    while cut and masked[cut - 1] == dsn_mod.MASK[0]:
        cut -= 1
    return text[:cut] + _REDACTED_SECRET


class WorktreeInventoryUnavailable(RuntimeError):
    """Git could not provide a trustworthy worktree inventory.

    The live-worktree map is the authority used to classify clone databases.
    This typed failure is deliberately distinct from a successful inventory map
    so callers cannot turn an inventory failure into destructive orphan cleanup.
    """

    code = "worktree_inventory_unavailable"

    def __init__(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
    ) -> None:
        self.cause_type = _inventory_cause_type(cause)
        self.detail = _bounded_inventory_diagnostic(message)
        # Keep only immutable, bounded metadata. Retaining the original
        # exception would expose its args/stdout/stderr through ``cause`` or
        # an exception repr, even though ``detail`` is redacted.
        self.cause = _SafeInventoryCause(self.cause_type, self.detail)
        super().__init__(f"worktree inventory unavailable: {self.detail}")


def live_worktree_suffixes(start: Path | None = None) -> dict[str, str]:
    """suffix -> worktree path for every live worktree of this repo."""
    failure: WorktreeInventoryUnavailable | None = None
    suffixes: dict[str, str] | None = None
    try:
        result = subprocess.run(
            ["git", "worktree", "list", "--porcelain", "-z"],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
            cwd=str(start) if start else None,
        )
        out = result.stdout
        if not isinstance(out, str):
            raise TypeError(
                f"git worktree inventory stdout must be str, got {type(out).__name__}"
            )
        if out == "":
            raise ValueError("git worktree inventory output was empty")
        if "\0" not in out or not out.endswith("\0\0"):
            raise ValueError(
                "git worktree inventory output is not complete NUL-delimited porcelain"
            )
        suffixes = {}
        records = out[:-2].split("\0\0")
        for record in records:
            fields = record.split("\0")
            if not fields or not fields[0].startswith("worktree "):
                raise ValueError("git worktree inventory record has no worktree path")
            path = fields[0][len("worktree ") :]
            if not path.strip() or not Path(path).is_absolute():
                raise ValueError(
                    "git worktree inventory contained a blank or relative path"
                )

            metadata = fields[1:]
            heads = [field for field in metadata if field.startswith("HEAD ")]
            branches = [field for field in metadata if field.startswith("branch ")]
            detached = metadata.count("detached")
            bare = metadata.count("bare")
            head_value = heads[0][len("HEAD ") :] if len(heads) == 1 else ""
            branch_value = branches[0][len("branch ") :] if len(branches) == 1 else ""
            known = all(
                field in {"detached", "bare", "locked", "prunable"}
                or field.startswith(("HEAD ", "branch ", "locked ", "prunable "))
                for field in metadata
            )
            if not known:
                raise ValueError("git worktree inventory record has an unknown field")
            if bare:
                if bare != 1 or heads or branches or detached:
                    raise ValueError(
                        "git bare-worktree inventory record is inconsistent"
                    )
            elif (
                len(heads) != 1
                or _GIT_OBJECT_ID_RE.fullmatch(head_value) is None
                or (len(branches) + detached) != 1
                or len(branches) > 1
                or (branches and not branch_value.strip())
            ):
                raise ValueError("git worktree inventory record is incomplete")

            digest = hashlib.sha256(
                str(Path(path).resolve()).encode("utf-8")
            ).hexdigest()
            suffixes[digest[:10]] = path
    except WorktreeInventoryUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - convert every probe failure to typed refusal.
        failure = WorktreeInventoryUnavailable(
            _bounded_inventory_diagnostic(exc),
            cause=exc,
        )
    if failure is not None:
        # Raise after leaving the ``except`` block so Python does not attach
        # the raw exception as ``__context__``/``__cause__``. The typed error
        # retains only the frozen, redacted metadata above.
        raise failure
    if suffixes is None:
        raise WorktreeInventoryUnavailable(
            "git worktree inventory did not produce a result"
        )
    return suffixes


def _admin_dsn(dsn: str) -> str:
    """``dsn`` pointed at the ``postgres`` maintenance database.

    Same form as ``dsn``; the query string (``sslmode``, ``options``,
    ``application_name``) and a percent-encoded password are kept.
    """
    return dsn_mod.parse(dsn).with_dbname("postgres").render()


def _server_key(dsn: str) -> str:
    parsed = dsn_mod.parse(dsn)
    return f"{parsed.host}:{parsed.port}"


def _admin_dsns_by_server(deployment: str, dsn: str) -> dict[str, str]:
    """server -> admin DSN for the deployment DSN and the clone server.

    Clones are minted by suggested_worktree_dsn, whose server may differ
    from where the canonical database lives. Raises
    :class:`okg.substrate.db.dsn.DsnError` for a DSN that does not parse.
    """
    from okg.substrate.catalog.ownership import suggested_worktree_dsn

    servers: dict[str, str] = {}
    for candidate in (dsn, suggested_worktree_dsn(deployment_name=deployment)):
        servers[_server_key(candidate)] = _admin_dsn(candidate)
    return servers


def _dsn_refusal(deployment: str, exc: dsn_mod.DsnError) -> dict[str, Any]:
    """A JSON refusal for a DSN that does not parse. The DSN is never shown."""
    reason = (
        "cannot inspect or remove isolated databases: the database "
        f"connection string does not parse ({exc.message})"
    )
    return {
        "ok": False,
        "status": "refused",
        "code": exc.code,
        "reason": reason,
        "fix_hint": exc.fix_hint,
        "deployment": deployment,
    }


def list_isolated(
    deployment: str, dsn: str, *, start: Path | None = None
) -> dict[str, Any]:
    try:
        live = live_worktree_suffixes(start)
    except WorktreeInventoryUnavailable as exc:
        return {
            "ok": False,
            "status": "unavailable",
            "code": exc.code,
            "reason": (
                "cannot determine live worktrees because worktree inventory is "
                "unavailable; refusing to inspect or remove isolated databases"
            ),
            "error": {
                "type": exc.cause_type,
                "message": exc.detail,
            },
            "deployment": deployment,
        }
    # Resolve the deployment only after worktree authority succeeds. Manifest
    # loading imports deployment/catalog modules that import psycopg
    # transitively; doing it first would violate the zero-import refusal path.
    declared = _declared_database(deployment)
    if not declared:
        return {
            "ok": False,
            "reason": (
                f"deployment {deployment} declares no postgres.database; "
                "the isolation suffix contract needs one"
            ),
        }
    # Both imports are intentionally after the authority probe. The ownership
    # module imports psycopg transitively, so moving only the direct import
    # would still violate the zero-effect refusal boundary.
    from okg.substrate.db.connection import connect as db_connect

    rows: list[dict[str, Any]] = []
    # Clones are minted by suggested_worktree_dsn, whose server may differ
    # from where the canonical database lives (today: clones on :5433,
    # canonical on :55434). Scan both, deduped.
    try:
        servers = _admin_dsns_by_server(deployment, dsn)
    except dsn_mod.DsnError as exc:
        return _dsn_refusal(deployment, exc)
    scan_errors: list[dict[str, str]] = []
    for server, admin in servers.items():
        try:
            conn = db_connect(admin, connect_timeout=5)
        except Exception as exc:  # noqa: BLE001 - report, keep scanning.
            scan_errors.append({"server": server, "error": str(exc)[:120]})
            continue
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT datname, pg_database_size(datname) "
                "FROM pg_database WHERE datname LIKE %s "
                "AND datname <> %s ORDER BY datname",
                (declared + r"\_%", declared),
            )
            for name, size in cur.fetchall():
                suffix = name[len(declared) + 1 :]
                worktree = live.get(suffix)
                rows.append(
                    {
                        "database": name,
                        "server": server,
                        "size_bytes": size,
                        "status": "live" if worktree else "orphan",
                        "worktree": worktree,
                    }
                )
    orphans = [r for r in rows if r["status"] == "orphan"]
    return {
        "ok": True,
        "deployment": deployment,
        "declared_database": declared,
        "servers_scanned": sorted(servers),
        "scan_errors": scan_errors,
        "isolated": rows,
        "count": len(rows),
        "orphans": len(orphans),
        "orphan_bytes": sum(r["size_bytes"] for r in orphans),
        "live_worktrees_checked": len(live),
    }


def gc_isolated(
    deployment: str,
    dsn: str,
    *,
    yes: bool = False,
    start: Path | None = None,
) -> dict[str, Any]:
    report = list_isolated(deployment, dsn, start=start)
    if not report.get("ok"):
        return report
    orphans = [r for r in report["isolated"] if r["status"] == "orphan"]
    report["mode"] = "drop" if yes else "dry_run"
    report["would_drop" if not yes else "dropped"] = [r["database"] for r in orphans]
    if not yes:
        report["note"] = (
            f"{len(orphans)} orphan database(s), "
            f"{report['orphan_bytes'] / 1e9:.1f} GB reclaimable; "
            "rerun with --yes to drop"
        )
        return report
    from okg.substrate.db.connection import connect as db_connect

    try:
        admin_by_server = _admin_dsns_by_server(deployment, dsn)
    except dsn_mod.DsnError as exc:
        return _dsn_refusal(deployment, exc)
    dropped: list[str] = []
    errors: list[dict[str, str]] = []
    conns: dict[str, Any] = {}
    try:
        for row in orphans:
            name = row["database"]
            server = row.get("server")
            admin = admin_by_server.get(server)
            if admin is None:
                errors.append({"database": name, "error": f"no admin dsn for {server}"})
                continue
            try:
                if server not in conns:
                    conns[server] = db_connect(
                        admin,
                        connect_timeout=5,
                        autocommit=True,
                    )
                with conns[server].cursor() as cur:
                    cur.execute(
                        "SELECT pg_terminate_backend(pid) "
                        "FROM pg_stat_activity WHERE datname = %s",
                        (name,),
                    )
                    cur.execute(f'DROP DATABASE "{name}"')
                dropped.append(f"{name}@{server}")
            except Exception as exc:  # noqa: BLE001 - per-db report
                errors.append({"database": name, "error": str(exc)[:160]})
    finally:
        for conn in conns.values():
            conn.close()
    report["dropped"] = dropped
    report["errors"] = errors
    report["ok"] = not errors
    return report
