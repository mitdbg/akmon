"""Coordinator contract rules for the PACT coordination path.

Three rules from the coordinator post-mortem are mechanical, so they live in
code rather than only in the runbook:

1. A coordinator never holds a mutation lease.  Chief and sprint assignments
   dispatch, order, and unblock; they do not edit, commit, or push.  A guarded
   ``claim`` or ``renew`` from a coordinator assignment is refused.
2. Exactly one mutation owner per pull request.  A second owner claim on the
   same pull request is refused instead of silently sharing the branch.
3. ``idle`` and ``blocked`` are reported events carrying a reason, not states
   the operator has to discover.  Building one without a reason is refused.

The module holds no database connection.  The register is a small JSON file in
the shared Git directory, so every worktree of one clone sees the same owners.
That is the exact failure the register exists to stop: two sessions in two
worktrees pushing to one branch.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Iterator, Mapping

from okg.substrate.errors import SubstrateError


# Assignment kinds that coordinate other agents.  They are never mutation
# owners.  ``legacy_task`` stays with the worker kinds because the legacy
# transition path predates the coordinator split.
COORDINATOR_ASSIGNMENT_KINDS: frozenset[str] = frozenset({"chief", "sprint"})
MUTATION_ASSIGNMENT_KINDS: frozenset[str] = frozenset(
    {"pact", "delegation", "legacy_task"}
)

# Guarded events that take or extend a mutation lease.  ``heartbeat`` and
# ``release`` are liveness and cleanup, so a coordinator may still send them.
MUTATION_LEASE_EVENT_TYPES: frozenset[str] = frozenset({"claim", "renew"})

# The reported states.  They are events with a reason, never bare badges.
REPORTED_AGENT_STATES: tuple[str, ...] = ("blocked", "idle")

# A live, unblocked lease whose heartbeat has been silent for longer than this
# is reported idle: the session still holds the work but has stopped moving it.
# Fifteen minutes is well inside the one-hour lease TTLs in pact/project.yaml,
# so idleness surfaces long before the lease itself expires.
IDLE_HEARTBEAT_SECONDS = 900.0
IDLE_REPORTED_CODE = "idle_no_heartbeat"

COORDINATOR_LEASE_REFUSED = "coordinator_lease_refused"
MUTATION_OWNER_CONFLICT = "mutation_owner_conflict"
MUTATION_OWNER_MISMATCH = "mutation_owner_mismatch"
SHARED_REGISTER_UNAVAILABLE = "shared_register_unavailable"
REPORTED_STATE_INVALID = "reported_state_invalid"
REPORTED_STATE_REASON_REQUIRED = "reported_state_reason_required"

REGISTER_ENV_VAR = "OKG_PACT_MUTATION_OWNERS_FILE"
_REGISTER_RELATIVE = Path("okg") / "pact" / "mutation-owners.json"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _kind(value: Any) -> str:
    return _text(value).lower()


def _utc_stamp(now: datetime | None) -> str:
    """Render an instant in UTC, refusing a naive datetime rather than guessing."""
    if now is None:
        return datetime.now(timezone.utc).isoformat()
    if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
        raise SubstrateError(
            "coordination timestamps must carry a timezone",
            data={"cause": "naive_timestamp"},
        )
    return now.astimezone(timezone.utc).isoformat()


def is_coordinator_kind(assignment_kind: Any) -> bool:
    """Whether this assignment kind coordinates instead of mutating."""
    return _kind(assignment_kind) in COORDINATOR_ASSIGNMENT_KINDS


def coordinator_lease_refusal(
    assignment_kind: Any, event_type: Any
) -> tuple[str, str] | None:
    """Return a refusal for a mutation lease event from a coordinator.

    Returns ``None`` when the event is allowed.  The tuple shape matches the
    native guarded-write validator so the refusal is audited like any other.
    """
    event = _text(event_type).lower()
    if event not in MUTATION_LEASE_EVENT_TYPES:
        return None
    if not is_coordinator_kind(assignment_kind):
        return None
    return (
        COORDINATOR_LEASE_REFUSED,
        f"{_kind(assignment_kind)} assignments coordinate and never hold a "
        "mutation lease; dispatch a worker assignment instead",
    )


# ---------------------------------------------------------------------------
# One mutation owner per pull request
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MutationOwner:
    """The single agent allowed to push to one pull request."""

    pull_request: str
    owner_principal: str
    owner_session_id: str = ""
    assignment_id: str = ""
    assignment_kind: str = ""
    claimed_at: str = ""

    def to_json(self) -> dict[str, str]:
        return {
            "pull_request": self.pull_request,
            "owner_principal": self.owner_principal,
            "owner_session_id": self.owner_session_id,
            "assignment_id": self.assignment_id,
            "assignment_kind": self.assignment_kind,
            "claimed_at": self.claimed_at,
        }

    @classmethod
    def from_json(cls, raw: Mapping[str, Any]) -> "MutationOwner":
        return cls(
            pull_request=_text(raw.get("pull_request")),
            owner_principal=_text(raw.get("owner_principal")),
            owner_session_id=_text(raw.get("owner_session_id")),
            assignment_id=_text(raw.get("assignment_id")),
            assignment_kind=_kind(raw.get("assignment_kind")),
            claimed_at=_text(raw.get("claimed_at")),
        )


def _git_common_dir(repo_root: Path) -> Path | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    raw = result.stdout.strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        path = repo_root / path
    return path


def mutation_owner_register_path(
    repo_root: Any, *, require_shared: bool = False
) -> Path:
    """Where the per-pull-request owner register lives.

    The register lives in the shared Git directory so sibling worktrees of one
    clone resolve to one file.  That sharing is the whole point: duplicate
    owners arise between worktrees, not inside one.  When the shared directory
    cannot be resolved there is no honest single-owner guarantee, so a write
    refuses rather than degrading quietly to a worktree-local file; a read may
    still fall back, because a worktree-local read cannot invent an owner.
    """
    override = os.environ.get(REGISTER_ENV_VAR)
    if override:
        return Path(override)
    root = Path(repo_root)
    common = _git_common_dir(root)
    if common is not None:
        return common / _REGISTER_RELATIVE
    if require_shared:
        raise SubstrateError(
            "the shared Git directory could not be resolved, so a single "
            "mutation owner cannot be guaranteed across worktrees",
            data={"cause": SHARED_REGISTER_UNAVAILABLE, "repo_root": str(root)},
        )
    return root / ".pact" / _REGISTER_RELATIVE.name


def _read_register(path: Path) -> dict[str, MutationOwner]:
    if not path.is_file():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SubstrateError(
            "mutation owner register is not valid JSON",
            data={"cause": "mutation_owner_register_invalid", "path": str(path)},
        ) from exc
    if not isinstance(raw, Mapping):
        raise SubstrateError(
            "mutation owner register must be a JSON object",
            data={"cause": "mutation_owner_register_invalid", "path": str(path)},
        )
    if "owners" not in raw:
        return {}
    owners = raw["owners"]
    if not isinstance(owners, Mapping):
        raise SubstrateError(
            "mutation owner register owners must be a JSON object",
            data={"cause": "mutation_owner_register_invalid", "path": str(path)},
        )
    result: dict[str, MutationOwner] = {}
    for key, value in owners.items():
        # A malformed entry must never read as "this pull request is free".
        if not isinstance(value, Mapping):
            raise SubstrateError(
                "mutation owner register contains a malformed owner entry",
                data={
                    "cause": "mutation_owner_register_invalid",
                    "path": str(path),
                    "pull_request": _text(key),
                },
            )
        result[_text(key)] = MutationOwner.from_json(value)
    return result


def _write_register(path: Path, owners: Mapping[str, MutationOwner]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "owners": {key: owner.to_json() for key, owner in sorted(owners.items())},
    }
    handle = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(path.parent),
        prefix=path.name,
        suffix=".tmp",
        delete=False,
    )
    try:
        with handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(handle.name, path)
    except BaseException:
        try:
            os.unlink(handle.name)
        except OSError:
            pass
        raise


@contextmanager
def _locked_register(path: Path) -> Iterator[Path]:
    """Yield the register path while holding an exclusive advisory lock.

    The lock is what makes two concurrent sessions resolve to one owner rather
    than to a last-writer-wins overwrite.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(path.name + ".lock")
    try:
        import fcntl
    except ImportError:  # pragma: no cover - POSIX-only repository
        yield path
        return
    with open(lock_path, "a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            yield path
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _pull_request_key(pull_request: Any) -> str:
    key = _text(pull_request)
    if not key:
        raise SubstrateError(
            "a mutation owner claim requires a pull request reference",
            data={"cause": "pull_request_required"},
        )
    return key


def load_mutation_owners(repo_root: Any) -> dict[str, MutationOwner]:
    """Return every recorded pull-request owner, keyed by pull request."""
    return _read_register(mutation_owner_register_path(repo_root))


def mutation_owner(repo_root: Any, pull_request: Any) -> MutationOwner | None:
    """Return the recorded owner of one pull request, if any."""
    return load_mutation_owners(repo_root).get(_pull_request_key(pull_request))


def claim_mutation_owner(
    repo_root: Any,
    *,
    pull_request: Any,
    owner_principal: Any,
    owner_session_id: Any = "",
    assignment_id: Any = "",
    assignment_kind: Any = "pact",
    now: datetime | None = None,
) -> MutationOwner:
    """Record the single mutation owner of one pull request.

    Refuses a coordinator assignment kind, and refuses a second, different
    owner.  Re-claiming as the recorded owner is idempotent and only refreshes
    the claim timestamp.
    """
    key = _pull_request_key(pull_request)
    principal = _text(owner_principal)
    if not principal:
        raise SubstrateError(
            "a mutation owner claim requires an owner principal",
            data={"cause": "owner_principal_required"},
        )
    kind = _kind(assignment_kind)
    if is_coordinator_kind(kind):
        raise SubstrateError(
            f"{kind} assignments coordinate and never own a pull request "
            "mutation; dispatch a worker assignment instead",
            data={"cause": COORDINATOR_LEASE_REFUSED, "pull_request": key},
        )
    if kind not in MUTATION_ASSIGNMENT_KINDS:
        raise SubstrateError(
            f"unknown coordination assignment kind: {assignment_kind!r}",
            data={"cause": "unknown_assignment_kind"},
        )
    stamp = _utc_stamp(now)
    path = mutation_owner_register_path(repo_root, require_shared=True)
    with _locked_register(path):
        owners = _read_register(path)
        existing = owners.get(key)
        if existing is not None and existing.owner_principal != principal:
            raise SubstrateError(
                f"pull request {key} already has mutation owner "
                f"{existing.owner_principal}; a second owner is refused",
                data={
                    "cause": MUTATION_OWNER_CONFLICT,
                    "pull_request": key,
                    "recorded_owner": existing.owner_principal,
                    "claiming_owner": principal,
                },
            )
        claimed = MutationOwner(
            pull_request=key,
            owner_principal=principal,
            owner_session_id=_text(owner_session_id)
            or (existing.owner_session_id if existing else ""),
            assignment_id=_text(assignment_id)
            or (existing.assignment_id if existing else ""),
            assignment_kind=kind,
            claimed_at=stamp,
        )
        owners[key] = claimed
        _write_register(path, owners)
    return claimed


def release_mutation_owner(
    repo_root: Any,
    *,
    pull_request: Any,
    owner_principal: Any,
) -> bool:
    """Release one pull request so it can be re-dispatched.

    Only the recorded owner may release.  Returns ``False`` when nothing was
    recorded, so a repeated release is not an error.
    """
    key = _pull_request_key(pull_request)
    principal = _text(owner_principal)
    path = mutation_owner_register_path(repo_root, require_shared=True)
    with _locked_register(path):
        owners = _read_register(path)
        existing = owners.get(key)
        if existing is None:
            return False
        if existing.owner_principal != principal:
            raise SubstrateError(
                f"pull request {key} is owned by {existing.owner_principal}; "
                f"{principal or 'an unnamed principal'} cannot release it",
                data={
                    "cause": MUTATION_OWNER_MISMATCH,
                    "pull_request": key,
                    "recorded_owner": existing.owner_principal,
                },
            )
        del owners[key]
        _write_register(path, owners)
    return True


# ---------------------------------------------------------------------------
# Reported idle and blocked events
# ---------------------------------------------------------------------------


def build_reported_state_event(
    state: Any,
    *,
    reason: Any,
    assignment_id: Any = "",
    next_action: Any = "",
    now: datetime | None = None,
) -> dict[str, str]:
    """Build one reported ``idle`` or ``blocked`` event.

    A reason is mandatory.  An agent that cannot say why it is idle or blocked
    has not reported anything the operator can act on.
    """
    value = _text(state).lower()
    if value not in REPORTED_AGENT_STATES:
        raise SubstrateError(
            f"unsupported reported agent state: {state!r}",
            data={
                "cause": REPORTED_STATE_INVALID,
                "supported": list(REPORTED_AGENT_STATES),
            },
        )
    detail = _text(reason)
    if not detail:
        raise SubstrateError(
            f"a reported {value} state requires a reason",
            data={"cause": REPORTED_STATE_REASON_REQUIRED, "state": value},
        )
    event = {
        "state": value,
        "reason": detail,
        "reported_at": _utc_stamp(now),
    }
    if _text(assignment_id):
        event["assignment_id"] = _text(assignment_id)
    if _text(next_action):
        event["next_action"] = _text(next_action)
    return event


def is_idle_assignment(
    *,
    lease_expires_at: Any,
    last_heartbeat_at: Any,
    now: Any,
    blocked: Any = False,
    idle_after_seconds: float = IDLE_HEARTBEAT_SECONDS,
) -> bool:
    """Whether a live, unblocked assignment has stopped reporting progress.

    Idle needs a heartbeat baseline to measure silence against.  An assignment
    that has never beaten has not gone quiet -- it has not started, which the
    resume path already handles -- so it is not idle.  An expired lease is a
    lease problem and a blocked assignment already reports its blocker; each
    has its own signal.  Idle is the remaining case: the lease is current, the
    agent has beaten before, and it has since gone silent.
    """
    if bool(blocked):
        return False
    if lease_expires_at is None or now is None or last_heartbeat_at is None:
        return False
    threshold = float(idle_after_seconds)
    if threshold <= 0:
        return False
    try:
        if lease_expires_at <= now:
            return False
        age = (now - last_heartbeat_at).total_seconds()
    except TypeError as exc:
        # Never read a comparison failure as "fine".  Idleness is a safety
        # signal, so an unusable timestamp is a refusal, not a quiet False.
        raise SubstrateError(
            "coordination idle check received incomparable timestamps",
            data={"cause": "idle_timestamps_incomparable"},
        ) from exc
    return age > threshold
