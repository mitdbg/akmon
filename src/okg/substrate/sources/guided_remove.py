"""Guided remove-a-source flow behind `okg deployment remove-source` (issue #2477).

The counterpart of `guided_add`. `plan_remove_source` reads everything and
changes nothing: it finds the source in the deployment's registry, opens the
deployment's database, and asks the reviewed source-retirement machinery
(`okg.substrate.source_retirement`) what removing it would retract.
`apply_remove_source` is the one confirmation. In order, it:

1. disables the entry, which stops scheduled and default imports of the
   source, and claims the deployment identity for the current registry. An
   explicit `okg ingest --include <name>` still runs. Records it writes
   before the retraction is published are retracted by a re-run, which
   continues the same, still unpublished retirement; records that arrive
   after the publish are refused (a published retirement is never
   reopened; issue #3265). A source another enabled source names as its
   `predecessor_source` is refused up front, since importing that one
   would pull this one back in;
2. applies the retirement against a plan made in the same run, refusing if
   the records differ from the ones the plan showed;
3. publishes a graph version without them;
4. removes the registry entry, only once that publish is on record;
5. re-claims the deployment identity and updates the access policy.

A source that never reached the graph -- a failed `add-source` whose import
wrote nothing -- has nothing to retract: steps 1 to 3 are skipped and only
its entry is removed, behind a branch-fenced check that it has no rows.

Every step can be resumed. The retirement receipt the database keeps makes
retract and publish idempotent; a small record under
`<deployment>/.okg/source-remove/` marks an entry that was removed before
the identity claim and policy update finished, so a re-run completes them.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence

import yaml

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import AddSourceError, RemoveSourceError, SubstrateError
from okg.substrate.sources.redaction import redact_mapping, redact_text

#: Recorded as the actor on the retirement receipt and as the commit author.
ACTOR = "okg deployment remove-source"
COMMIT_EMAIL = "okg-remove-source@localhost"
SAMPLE_LIMIT = 5
PENDING_DIRNAME = ".okg/source-remove"

# What the plan found, and therefore what `--apply` does.
RETIRE = "retire"  # disable, retract, publish, remove the entry
PUBLISH_PENDING = "publish_pending"  # a retirement was applied; publish it
REGISTRY_ONLY = "registry_only"  # nothing in the graph: remove the entry
FINISH = "finish"  # entry already gone: re-claim and update the policy

# Whether the flow commits its registry edit (decided at plan time).
COMMIT = "commit"
SKIP_COMMIT = "skip"
GIT_ERROR = "git_error"  # refused at plan time, before any write

Runner = Callable[..., "subprocess.CompletedProcess[str]"]
Connect = Callable[[str], Any]


@dataclass
class RemoveStep:
    name: str
    status: str  # ok | failed | skipped
    detail: dict[str, Any] = field(default_factory=dict)
    cause: Optional[str] = None
    fix_hint: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"step": self.name, "status": self.status}
        if self.detail:
            out["detail"] = self.detail
        if self.cause:
            out["cause"] = self.cause
        if self.fix_hint:
            out["fix_hint"] = self.fix_hint
        return out


@dataclass
class RemoveSourcePlan:
    deployment_name: str
    deployment_dir: Path
    registry_path: Path
    name: str
    ownership_id: str
    entry: Optional[dict[str, Any]]
    branch: str
    action: str
    dsn: str = field(repr=False, default="")
    records: int = 0
    relationships: int = 0
    sample: list[dict[str, Any]] = field(default_factory=list)
    plan_digest: Optional[str] = None
    footprint_digest: Optional[str] = None
    retirement_id: Optional[str] = None
    retirement_status: Optional[str] = None
    published_before: bool = False
    note: Optional[str] = None
    commit_mode: str = SKIP_COMMIT
    commit_note: str = ""
    resume_pending: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.deployment.remove_source.plan/v1",
            "deployment": self.deployment_name,
            "deployment_dir": str(self.deployment_dir),
            "name": self.name,
            "ownership_id": self.ownership_id,
            "action": self.action,
            "retract": {
                "records": self.records,
                "relationships": self.relationships,
                "sample": list(self.sample),
            },
            "registry": {
                "path": str(self.registry_path),
                "entry_to_remove": self.name if self.entry is not None else None,
                # The reader the entry runs: registry entries name it by
                # `type` (packaged connectors) or by `module` + `class`.
                "entry_type": (
                    (self.entry or {}).get("type")
                    or (self.entry or {}).get("class")
                ),
            },
            "published_before": self.published_before,
            "commit": (
                {"mode": "commit"} if self.commit_mode == COMMIT
                else {"mode": "skip", "why": self.commit_note}
            ),
            "plan_digest": self.plan_digest,
            "retirement": (
                {"retirement_id": self.retirement_id, "status": self.retirement_status}
                if self.retirement_id else None
            ),
            "note": self.note,
        }


@dataclass
class RemoveSourceOutcome:
    plan: RemoveSourcePlan
    status: str  # removed | failed
    steps: list[RemoveStep]
    generation_id: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        failed = next((s for s in self.steps if s.status == "failed"), None)
        return {
            "schema": "okg.deployment.remove_source.outcome/v1",
            "deployment": self.plan.deployment_name,
            "name": self.plan.name,
            "action": self.plan.action,
            "status": self.status,
            "generation_id": self.generation_id,
            "counts": {
                "records_removed": self.plan.records,
                "relationships_removed": self.plan.relationships,
            },
            "steps": [step.as_dict() for step in self.steps],
            "failed_step": failed.as_dict() if failed else None,
            "retry": (
                None if failed is None else
                "Correct the cause and run the same command again; it resumes "
                "from the failed step without retracting or publishing twice."
            ),
            "note": self.plan.note,
        }


def _refuse(
    message: str,
    *,
    deployment: str,
    source: str,
    fix_hint: str,
    **data: Any,
) -> RemoveSourceError:
    return RemoveSourceError(
        message,
        data={"deployment": deployment, "source": source, "cause": message, **data},
        fix_hint=fix_hint,
    )


# --------------------------------------------------------------------------
# Planning (read-only)
# --------------------------------------------------------------------------

def plan_remove_source(
    *,
    deployment: str,
    name: str,
    deployments_root: Optional[str] = None,
    dsn: Optional[str] = None,
    branch: str = "default",
    environ: Optional[Mapping[str, str]] = None,
    connect: Optional[Connect] = None,
) -> RemoveSourcePlan:
    """Work out what removing `name` would do; change nothing."""
    from okg.substrate.mcp.source_admin import _load_registry_yaml
    from okg.substrate.sources.guided_add import (
        _load_registry,
        _load_yaml,
        _resolve_deployment,
        _subprocess_dsn,
        read_ledger,
    )

    try:
        deployment_dir, deployment_name, registry_path = _resolve_deployment(
            deployment, deployments_root=deployments_root,
        )
    except AddSourceError as exc:
        raise _refuse(
            exc.message, deployment=deployment, source=name,
            fix_hint=exc.fix_hint or "pass the deployment name or directory",
        ) from exc

    registry = _load_registry(registry_path)
    sources = registry.get("sources") or {}
    key, entry = _find_entry(sources, name, deployment=deployment_name)

    # The retirement machinery reads the registry by deployment NAME; refuse
    # rather than retire against a different file than the one shown here.
    try:
        resolved_path, _raw = _load_registry_yaml(deployment_name)
    except FileNotFoundError:
        resolved_path = None
    if entry is not None and (
        resolved_path is None or resolved_path.resolve() != registry_path.resolve()
    ):
        raise _refuse(
            f"deployment name {deployment_name!r} does not resolve to "
            f"{deployment_dir}",
            deployment=deployment_name, source=name,
            fix_hint=(
                "set OKG_DEPLOYMENTS_DIR to the directory that holds this "
                "deployment, or run from its repository, then re-run"
            ),
            resolved_registry=str(resolved_path) if resolved_path else None,
        )
    commit_mode, commit_note = _commit_mode(deployment_dir, registry_path)
    if commit_mode == GIT_ERROR:
        raise _refuse(
            commit_note,
            deployment=deployment_name, source=key or name,
            fix_hint=(
                f"run `git -C {deployment_dir} status` and fix what it "
                "reports, then run the same command again"
            ),
        )
    if entry is not None:
        dependents = _dependent_sources(sources, key or name, entry)
        if dependents:
            raise _refuse(
                f"source {key!r} is the predecessor_source of enabled "
                f"source(s) {', '.join(dependents)}: importing them pulls "
                f"{key!r} back in, which would rewrite its records while "
                "they are removed",
                deployment=deployment_name, source=key or name,
                fix_hint=(
                    f"remove {key!r} from the predecessor_source of "
                    f"{', '.join(dependents)} (or remove or disable those "
                    "sources first), commit, then run the same command again"
                ),
                dependents=dependents,
            )
    if entry is not None and commit_mode == COMMIT:
        try:
            dirty = _registry_uncommitted(deployment_dir, registry_path)
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            raise _refuse(
                f"could not read the git state of {registry_path.name}: "
                f"{redact_text(str(exc))}",
                deployment=deployment_name, source=key or name,
                fix_hint=(
                    f"run `git -C {deployment_dir} status` and fix what it "
                    "reports, then run the same command again"
                ),
            ) from exc
        if dirty:
            raise _refuse(
                f"{registry_path.name} has uncommitted changes; remove-source "
                "commits only its own edit and will not sweep yours into it",
                deployment=deployment_name, source=name,
                fix_hint=(
                    f"commit or discard them (`git -C {deployment_dir} diff -- "
                    f"{registry_path.name}`), then run the same command again"
                ),
                uncommitted=dirty,
            )

    env = dict(os.environ if environ is None else environ)
    if dsn:
        env["OKG_DSN"] = dsn
    manifest = _load_yaml(deployment_dir / "deployment.yaml")
    db_dsn = _subprocess_dsn(manifest, env)
    if not db_dsn:
        raise _refuse(
            "deployment.yaml names no database this command can resolve "
            "(`postgres.dsn` is missing or uses an unset variable)",
            deployment=deployment_name, source=name,
            fix_hint=(
                "set `postgres.dsn` in deployment.yaml, or pass --dsn when it "
                "reads ${OKG_DSN}, then re-run"
            ),
        )

    ledger = read_ledger(deployment_dir, key or name) or {}
    plan = RemoveSourcePlan(
        deployment_name=deployment_name,
        deployment_dir=deployment_dir,
        registry_path=registry_path,
        name=key or name,
        ownership_id=str((entry or {}).get("ownership_id") or ""),
        entry=dict(entry) if entry is not None else None,
        branch=branch,
        action=RETIRE,
        dsn=db_dsn,
        published_before=ledger.get("status") == "published",
        commit_mode=commit_mode,
        commit_note=commit_note,
    )

    if entry is None:
        pending = _read_pending(deployment_dir, name)
        if pending is not None:
            plan.name = str(pending["name"])
            plan.ownership_id = str(pending.get("ownership_id") or "")
            plan.retirement_id = pending.get("retirement_id")
            plan.resume_pending = True
            plan.action = FINISH
            plan.note = (
                "a previous remove-source removed the registry entry but did "
                "not finish; --apply re-claims the deployment identity and "
                "updates the access policy"
            )
            return plan

    opener = connect or _default_connect(deployment_name)
    with opener(db_dsn) as conn:
        if entry is None:
            receipt = _receipt_for(conn, deployment_name, branch, name)
            if receipt is None or receipt["status"] != "published":
                raise _refuse(
                    f"source {name!r} is not in deployment "
                    f"{deployment_name!r}",
                    deployment=deployment_name, source=name,
                    fix_hint=(
                        "check the name; registered sources: "
                        + (", ".join(sorted(str(k) for k in sources)) or "none")
                    ),
                    registered_sources=sorted(str(k) for k in sources),
                )
            plan.name = receipt["registry_source_id"]
            plan.ownership_id = receipt["ownership_id"]
            plan.action = FINISH
            plan.retirement_id = receipt["retirement_id"]
            plan.retirement_status = receipt["status"]
            plan.note = (
                "the source's records were removed in graph version "
                f"{receipt['repair_generation_id']} and its entry is already "
                "gone; nothing is left to remove"
            )
            return plan
        if not plan.ownership_id:
            raise _refuse(
                f"source {plan.name!r} has no ownership_id in "
                f"{registry_path.name}, so its records cannot be identified",
                deployment=deployment_name, source=plan.name,
                fix_hint=(
                    "add `ownership_id: <deployment>.<name>` to the entry "
                    "(the id its records carry), then re-run"
                ),
            )
        _plan_against_graph(conn, plan)
    return plan


def _name_variants(name: str) -> set[str]:
    return {name, name.replace("_", "-"), name.replace("-", "_")}


def _dependent_sources(
    sources: Mapping[str, Any], name: str, entry: Mapping[str, Any],
) -> list[str]:
    """Enabled sources whose `predecessor_source` names this one.

    `okg ingest --include <dependent>` expands through predecessor closure
    and would import this source again, disabled or not.
    """
    targets = _name_variants(name)
    ownership = str(entry.get("ownership_id") or "")
    if ownership:
        targets |= _name_variants(ownership)
    found: list[str] = []
    for other, body in sources.items():
        if str(other) == name or not isinstance(body, Mapping):
            continue
        if body.get("enabled_by_default") is False:
            continue
        raw = body.get("predecessor_source")
        params = body.get("params")
        if raw is None and isinstance(params, Mapping):
            raw = params.get("predecessor_source")
        values = [raw] if isinstance(raw, str) else list(raw or ())
        if any(str(value) in targets for value in values):
            found.append(str(other))
    return sorted(found)


def _find_entry(
    sources: Mapping[str, Any], requested: str, *, deployment: str,
) -> tuple[Optional[str], Optional[dict[str, Any]]]:
    """The registry key and entry `requested` names: key or ownership id."""
    wanted = _name_variants(requested)
    matches: list[tuple[str, dict[str, Any]]] = []
    for key, value in sources.items():
        if not isinstance(value, Mapping):
            continue
        names = {str(key), str(value.get("ownership_id") or "")}
        names |= {n.replace("_", "-") for n in names} | {
            n.replace("-", "_") for n in names
        }
        if names & wanted:
            matches.append((str(key), dict(value)))
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        exact = [m for m in matches if m[0] == requested]
        if len(exact) == 1:
            return exact[0]
        raise _refuse(
            f"{requested!r} names more than one source: "
            + ", ".join(sorted(m[0] for m in matches)),
            deployment=deployment, source=requested,
            fix_hint="pass the exact registry name",
            matches=sorted(m[0] for m in matches),
        )
    return None, None


def _plan_against_graph(conn: Any, plan: RemoveSourcePlan) -> None:
    from okg.substrate.source_retirement import remediate_source_retirement

    receipt = _receipt_for(conn, plan.deployment_name, plan.branch, plan.name)
    if receipt is not None and receipt["status"] == "published":
        footprint = _footprint(conn, plan)
        if any(footprint.values()):
            # The receipt table holds one retirement per source name and a
            # published receipt is audit evidence: it is never reopened.
            # Records that return after a published removal need an
            # append-only receipt (issue #3265).
            raise _refuse(
                f"source {plan.name!r} has records again after its removal was "
                f"published (retirement {receipt['retirement_id']}, graph "
                f"version {receipt['repair_generation_id']}); a published "
                "retirement is not reopened, so this version cannot retract "
                "them",
                deployment=plan.deployment_name, source=plan.name,
                fix_hint=(
                    "disable the source (set `enabled_by_default: false` in "
                    "the registry) so it stops importing; this version cannot "
                    "remove records that return after a published removal"
                ),
                retirement_id=receipt["retirement_id"],
                **footprint,
            )
        plan.action = REGISTRY_ONLY
        plan.retirement_id = receipt["retirement_id"]
        plan.retirement_status = "published"
        plan.note = (
            "its records were already removed in graph version "
            f"{receipt['repair_generation_id']}; --apply removes the entry"
        )
        return

    try:
        result = remediate_source_retirement(
            conn,
            source_name=plan.name,
            deployment_name=plan.deployment_name,
            branch_name=plan.branch,
        )
    except SubstrateError as exc:
        if "requires a published checkpoint" not in exc.message:
            raise _refuse(
                f"cannot plan the removal: {exc.message}",
                deployment=plan.deployment_name, source=plan.name,
                fix_hint=exc.fix_hint or (
                    f"run `okg doctor --deployment {plan.deployment_name} "
                    "--json` and fix what it names, then re-run"
                ),
                retirement_error=exc.data,
            ) from exc
        _plan_without_published_version(conn, plan)
        return

    details = result.details
    status = details.get("status")
    if status == "awaiting_publish" and not details.get("continuation_required"):
        plan.action = PUBLISH_PENDING
        plan.retirement_id = str(details.get("retirement_id"))
        plan.retirement_status = str(status)
        plan.plan_digest = str(details.get("plan_digest") or "") or None
        plan.note = (
            "a previous run retracted the records but did not publish; "
            "--apply publishes, then removes the entry"
        )
        return
    retirement = details.get("plan") or {}
    plan.plan_digest = retirement.get("plan_digest")
    plan.footprint_digest = retirement.get("footprint_digest")
    plan.records = len(retirement.get("nodes") or ())
    plan.relationships = len(retirement.get("edges") or ())
    plan.sample = [_describe(row) for row in (retirement.get("nodes") or ())[:SAMPLE_LIMIT]]
    if status in ("planned", "awaiting_publish"):
        plan.retirement_id = str(details.get("retirement_id"))
        plan.retirement_status = str(status)
    if details.get("continuation_required"):
        plan.note = (
            "records were written again after this source's records were "
            "retracted and before that retraction was published (for example "
            "by an explicit import); --apply retracts them under the same, "
            "still unpublished retirement, publishes, and removes the entry"
        )
    codes = list(retirement.get("refusal_codes") or ())
    if codes:
        raise _refuse(
            "the retirement plan is blocked: " + ", ".join(codes),
            deployment=plan.deployment_name, source=plan.name,
            fix_hint=(
                "run `okg remediate retire-source --source "
                f"{plan.name} --deployment-name {plan.deployment_name} --json` "
                "to see the records that share ownership, resolve them, then "
                "re-run"
            ),
            refusal_codes=codes,
        )
    if plan.records or plan.relationships or status is not None:
        return
    # Nothing staged. If the published version still holds records (they were
    # retracted by an import that never published), the retirement still has
    # to publish; otherwise the source never reached the graph.
    footprint = _footprint(conn, plan)
    if footprint["published_records"] or footprint["published_relationships"]:
        plan.note = (
            "no records are staged, but the published graph version still "
            "holds some; --apply publishes a version without them"
        )
        return
    plan.action = REGISTRY_ONLY
    plan.note = _never_published_note(plan)


def _plan_without_published_version(conn: Any, plan: RemoveSourcePlan) -> None:
    footprint = _footprint(conn, plan)
    if footprint["records"] or footprint["relationships"]:
        raise _refuse(
            f"source {plan.name!r} has {footprint['records']} imported records "
            "but this deployment has never published a graph version, so "
            "there is nothing to retract them against",
            deployment=plan.deployment_name, source=plan.name,
            fix_hint=(
                f"publish once with `okg run --once --deployment "
                f"{plan.deployment_dir} --inline --apply --json`, then re-run"
            ),
            records=footprint["records"],
        )
    plan.action = REGISTRY_ONLY
    plan.note = _never_published_note(plan)


def _never_published_note(plan: RemoveSourcePlan) -> str:
    return (
        f"source {plan.name!r} never reached the graph (no records staged or "
        "published), so there is nothing to retract; --apply removes its "
        "registry entry only"
    )


def _describe(row: Mapping[str, Any]) -> dict[str, Any]:
    attrs = row.get("attrs") if isinstance(row.get("attrs"), Mapping) else {}
    label = None
    for key in ("title", "name", "heading", "path", "label", "url"):
        value = (attrs or {}).get(key)
        if isinstance(value, str) and value:
            label = value
            break
    return {
        "node_id": str(row.get("node_id")),
        "subtype": str(row.get("subtype")),
        "label": redact_text(label or str(row.get("node_id"))),
    }


def _receipt_for(
    conn: Any, deployment_name: str, branch: str, name: str,
) -> Optional[dict[str, Any]]:
    """The retirement receipt for a registry name or ownership id, if any."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT retirement_id, status, registry_source_id, ownership_id, "
            "repair_generation_id FROM okg.source_retirement_receipts "
            "WHERE deployment_name = %s AND branch_name = %s "
            "AND (registry_source_id = %s OR ownership_id = %s) "
            "ORDER BY retirement_id LIMIT 2",
            (deployment_name, branch, name, name),
        )
        rows = cur.fetchall()
    if len(rows) != 1:
        return None
    row = rows[0]
    return {
        "retirement_id": str(row[0]),
        "status": str(row[1]),
        "registry_source_id": str(row[2]),
        "ownership_id": str(row[3]),
        "repair_generation_id": row[4],
    }


def _subjects(plan: RemoveSourcePlan) -> tuple[str, ...]:
    subjects = {plan.name, plan.ownership_id}
    admission = (plan.entry or {}).get("admission_policy")
    if isinstance(admission, Mapping) and admission.get("producer_id"):
        subjects.add(str(admission["producer_id"]))
    return tuple(sorted(s for s in subjects if s))


def _footprint(conn: Any, plan: RemoveSourcePlan) -> dict[str, int]:
    """Count the source's rows on the live branch and in the published version."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.source_retirement import _retirement_edges, _source_nodes

    subjects = _subjects(plan)
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)

    def count(graph: Any) -> tuple[int, int]:
        nodes = _source_nodes(graph, subjects)
        edges = _retirement_edges(
            graph, subjects, [str(row["node_id"]) for row in nodes],
        )
        return len(nodes), len(edges)

    try:
        live = count(store.checkout(plan.branch))
    except Exception as exc:  # noqa: BLE001 - only a missing branch is zero
        if type(exc).__name__ != "BranchNotFoundError":
            raise
        live = (0, 0)
    latest = store.latest_generation(
        deployment_name=plan.deployment_name, branch=plan.branch,
        status="published",
    )
    published = (
        count(store.checkout_generation(str(latest.checkpoint_id), branch=plan.branch))
        if latest is not None else (0, 0)
    )
    return {
        "records": live[0],
        "relationships": live[1],
        "published_records": published[0],
        "published_relationships": published[1],
    }


def _default_connect(deployment_name: str) -> Connect:
    def _open(dsn: str) -> Any:
        from okg.substrate.db.connection import connect

        return connect(dsn, autocommit=True, deployment=deployment_name)

    return _open


# --------------------------------------------------------------------------
# The deployment checkout: only this flow's own edit is ever committed
# --------------------------------------------------------------------------

def _git(dep: Path, *args: str, check: bool = False) -> "subprocess.CompletedProcess[str]":
    return subprocess.run(
        ["git", "-C", str(dep), *args],
        capture_output=True, text=True, check=check, timeout=30,
    )


def _is_git_checkout(dep: Path) -> bool:
    try:
        return _git(dep, "rev-parse", "--show-toplevel").returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _commit_mode(dep: Path, registry_path: Path) -> tuple[str, str]:
    """Whether this flow commits the registry file: ("commit", "") or ("skip", why).

    Decided at plan time, before anything is written, so the flow never
    rewrites the file and then fails to stage it.
    """
    name = registry_path.name
    if not _is_git_checkout(dep):
        return SKIP_COMMIT, "the deployment is not a git checkout"
    path = str(registry_path)
    ignored = _git(dep, "check-ignore", "-q", "--", path)
    if ignored.returncode == 0:
        return SKIP_COMMIT, f"{name} is ignored by git in this checkout"
    if ignored.returncode != 1:
        # Exit 128 is a fatal git error; only its "outside repository"
        # message means the file lives outside this checkout.
        if "outside repository" in (ignored.stderr or ""):
            return SKIP_COMMIT, f"{name} is outside the deployment's git checkout"
        return GIT_ERROR, (
            f"`git check-ignore` failed on {name} (exit {ignored.returncode}): "
            + (_last_stderr_line(ignored) or "no message")
        )
    tracked = _git(dep, "ls-files", "--error-unmatch", "--", path)
    if tracked.returncode != 0:
        return SKIP_COMMIT, f"{name} is not tracked by git in this checkout"
    return COMMIT, ""


def _registry_uncommitted(dep: Path, registry_path: Path) -> list[str]:
    """`git status` lines for a tracked registry file; raises when git fails."""
    proc = _git(
        dep, "--no-optional-locks", "status", "--porcelain", "--", str(registry_path),
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "git status failed: " + (_last_stderr_line(proc) or f"exit {proc.returncode}")
        )
    return [line for line in proc.stdout.splitlines() if line.strip()]


def _last_stderr_line(proc: "subprocess.CompletedProcess[str]") -> str:
    lines = [line for line in (proc.stderr or "").splitlines() if line.strip()]
    return lines[-1][-300:] if lines else ""


def _commit_registry(plan: RemoveSourcePlan, message: str) -> dict[str, Any]:
    """Commit the registry file alone, as remove-source.

    Returns ``{"commit": <sha> | None}`` and, when the plan decided not to
    commit, ``"not_committed": <why>``. Raises when git refuses.
    """
    if plan.commit_mode != COMMIT:
        return {"commit": None, "not_committed": plan.commit_note}
    dep = plan.deployment_dir
    path = str(plan.registry_path)
    _git(dep, "add", "--", path, check=True)
    if _git(dep, "diff", "--cached", "--quiet", "--", path).returncode == 0:
        return {"commit": None}
    _git(
        dep, "-c", f"user.name={ACTOR}", "-c", f"user.email={COMMIT_EMAIL}",
        "commit", "-q", "-m", message, "--only", "--", path, check=True,
    )
    return {"commit": _git(dep, "rev-parse", "HEAD", check=True).stdout.strip()}


# --------------------------------------------------------------------------
# The pending-finish record (resume after the entry is gone)
# --------------------------------------------------------------------------

def _pending_path(dep: Path, name: str) -> Path:
    return dep / PENDING_DIRNAME / f"{name}.json"


def _write_pending(plan: RemoveSourcePlan) -> None:
    directory = plan.deployment_dir / PENDING_DIRNAME
    directory.mkdir(parents=True, exist_ok=True)
    ignore = directory / ".gitignore"
    if not ignore.is_file():
        ignore.write_text("*\n", encoding="utf-8")
    _pending_path(plan.deployment_dir, plan.name).write_text(json.dumps({
        "schema": "okg.deployment.remove_source.pending/v1",
        "name": plan.name,
        "ownership_id": plan.ownership_id,
        "retirement_id": plan.retirement_id,
        "status": "entry_removed",
    }, indent=2), encoding="utf-8")


def _read_pending(dep: Path, requested: str) -> Optional[dict[str, Any]]:
    """The pending record naming `requested` (name or ownership id, `-`/`_` alike)."""
    directory = dep / PENDING_DIRNAME
    if not directory.is_dir():
        return None
    wanted = _name_variants(requested)
    for path in sorted(directory.glob("*.json")):
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(body, dict):
            continue
        names: set[str] = set()
        for value in (body.get("name"), body.get("ownership_id")):
            if value:
                names |= _name_variants(str(value))
        if names & wanted:
            return body
    return None


# --------------------------------------------------------------------------
# Registry edits (removal is shared with the MCP `remove_source` tool)
# --------------------------------------------------------------------------

def remove_registry_entry_after_retirement(
    conn: Any, *, deployment: str, source_id: str,
) -> dict[str, Any]:
    """Remove one registry entry once its published retirement is proven.

    Returns ``{"status": "removed" | "not_present", "retirement": ...}``;
    raises `SubstrateError` when no published retirement covers the entry or
    the registry changed since that retirement was applied.
    """
    from okg.substrate import source_retirement

    def authority() -> Any:
        return source_retirement.source_registry_removal_authority(
            conn, source=source_id, deployment_name=deployment,
        )

    return _remove_entry(
        deployment=deployment, source_id=source_id, authority=authority,
        expected_entry=None,
    )


def _remove_entry(
    *,
    deployment: str,
    source_id: str,
    authority: Callable[[], Any],
    expected_entry: Optional[Mapping[str, Any]],
) -> dict[str, Any]:
    from okg.substrate.mcp.source_admin import _load_registry_yaml
    from okg.substrate.mcp.source_lifecycle import (
        _atomic_write_registry,
        _registry_mutation_lock,
    )

    path, _raw = _load_registry_yaml(deployment)
    # File lock precedes the graph fence, matching add_source's lock order
    # when its scoped ingest acquires graph authority.
    with _registry_mutation_lock(path):
        with authority() as granted:
            path, current_raw = _load_registry_yaml(deployment)
            expected_digest = (granted or {}).get("registry_digest")
            if expected_digest is not None:
                current_digest = hashlib.sha256(path.read_bytes()).hexdigest()
                if current_digest != expected_digest:
                    raise SubstrateError(
                        "source registry changed during removal authorization",
                        data={"source": source_id},
                        fix_hint="retry removal against the current registry",
                    )
            sources = current_raw.get("sources") or {}
            if source_id not in sources:
                return {"status": "not_present", "retirement": granted}
            if expected_entry is not None and dict(sources[source_id]) != dict(
                expected_entry
            ):
                raise SubstrateError(
                    "the source's registry entry changed since it was planned",
                    data={"source": source_id},
                    fix_hint="run the same command again to plan against it",
                )
            sources.pop(source_id, None)
            current_raw["sources"] = sources
            _atomic_write_registry(path, current_raw)
    return {"status": "removed", "retirement": granted}


def _disable_entry(plan: RemoveSourcePlan) -> bool:
    """Set the entry's `enabled_by_default: false`; True when that changed it."""
    from okg.substrate.mcp.source_lifecycle import (
        _atomic_write_registry,
        _registry_mutation_lock,
    )
    from okg.substrate.sources.guided_add import _load_registry

    with _registry_mutation_lock(plan.registry_path):
        raw = _load_registry(plan.registry_path)
        entry = (raw.get("sources") or {}).get(plan.name)
        if not isinstance(entry, dict) or entry.get("enabled_by_default") is False:
            return False
        entry["enabled_by_default"] = False
        _atomic_write_registry(plan.registry_path, raw)
    return True


@contextmanager
def _unretired_removal_authority(
    conn: Any, plan: RemoveSourcePlan,
) -> Iterator[dict[str, Any]]:
    """Fence the branch and prove the source has no rows, live or published."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    if not store._lock_branch_write_session(plan.branch):
        raise SubstrateError(
            "source removal requires PostgreSQL branch fencing",
            data={"branch": plan.branch},
        )
    try:
        footprint = _footprint(conn, plan)
        if any(footprint.values()):
            raise SubstrateError(
                "the source has records in the graph now",
                data={"source": plan.name, **footprint},
                fix_hint=(
                    "run the same command again: it plans a retirement of "
                    "those records"
                ),
            )
        yield {"footprint": footprint}
    finally:
        store._unlock_branch_write_session(plan.branch)


def _policy_step(
    plan: RemoveSourcePlan,
    env: Mapping[str, str],
    reconcile_policy: Optional[Callable[..., dict[str, Any]]],
) -> RemoveStep:
    """Rebuild the access policy from the registry that no longer lists the source.

    The same idempotent reconcile `okg deployment apply` runs for its
    `nomos_baseline` stage, against the database the `okg` children of this
    flow use (the manifest's `postgres.dsn`, expanded from their
    environment).
    """
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )
    from okg.substrate.sources.guided_add import _load_yaml, _subprocess_dsn

    hint = (
        "fix the named cause, then run the same command again; the entry is "
        "already removed, so it only re-claims and updates the policy"
    )
    try:
        manifest = _load_yaml(plan.deployment_dir / "deployment.yaml")
        mode = manifest_nomos_enforcement_mode(manifest)
    except (OSError, ValueError, yaml.YAMLError, SubstrateError) as exc:
        return RemoveStep(
            "policy", "failed",
            cause=(
                "could not read the access-policy setting in deployment.yaml: "
                + redact_text(str(exc))
            ),
            fix_hint=hint,
        )
    if mode == "off":
        return RemoveStep("policy", "skipped", detail={
            "reason": "this deployment does not enforce an access policy",
        })
    dsn = _subprocess_dsn(manifest, env)
    if not dsn:
        return RemoveStep(
            "policy", "failed",
            cause=(
                "deployment.yaml names no database this step can resolve "
                "(`postgres.dsn` is missing or uses an unset variable)"
            ),
            fix_hint=hint,
        )
    if reconcile_policy is None:
        from okg.substrate.nomos.baseline import reconcile_deployment_baseline

        reconcile_policy = reconcile_deployment_baseline
    try:
        result = reconcile_policy(
            deployment=plan.deployment_name,
            deployment_dir=plan.deployment_dir,
            dsn=dsn,
            apply=True,
        )
    except Exception as exc:  # noqa: BLE001 - every cause is reported, none hidden
        data = getattr(exc, "data", None)
        return RemoveStep(
            "policy", "failed",
            detail=(
                {"error": redact_mapping(dict(data))}
                if isinstance(data, Mapping) and data else {}
            ),
            cause=(
                "the access policy could not be updated after removing the "
                "source: " + redact_text(f"{type(exc).__name__}: {exc}")
            ),
            fix_hint=hint,
        )
    if not result.get("converged"):
        return RemoveStep(
            "policy", "failed",
            detail={
                "desired": result.get("desired"),
                "active_after": result.get("active_after"),
            },
            cause=(
                "the access policy was rebuilt without the removed source, but "
                "the active policy still does not match it"
            ),
            fix_hint=(
                "if this deployment runs a policy activated by hand, remove the "
                "source from that policy; otherwise inspect `okg doctor "
                f"--deployment {plan.deployment_name} --json` (the "
                "nomos_baseline stage), then run the same command again"
            ),
        )
    return RemoveStep("policy", "ok", detail={
        "mode": mode,
        "changed": bool(result.get("changed")),
    })


# --------------------------------------------------------------------------
# Apply
# --------------------------------------------------------------------------

def apply_remove_source(
    plan: RemoveSourcePlan,
    *,
    runner: Optional[Runner] = None,
    environ: Optional[Mapping[str, str]] = None,
    okg_argv_prefix: Optional[Sequence[str]] = None,
    connect: Optional[Connect] = None,
    reconcile_policy: Optional[Callable[..., dict[str, Any]]] = None,
) -> RemoveSourceOutcome:
    """Disable, retract, publish, remove the entry, re-claim, update the policy."""
    from okg.substrate.sources.guided_add import (
        LEDGER_DIRNAME,
        _claim_identity,
        _default_runner,
        _error_message,
        _json_payload,
        _json_tail,
        _last_line,
    )

    if not plan.dsn:
        raise _refuse(
            "no database resolved for this removal",
            deployment=plan.deployment_name, source=plan.name,
            fix_hint=(
                "set `postgres.dsn` in deployment.yaml, or pass --dsn when it "
                "reads ${OKG_DSN}, then run the same command again"
            ),
        )
    run = runner or _default_runner
    env = dict(os.environ if environ is None else environ)
    # Every child talks to the database this plan resolved (`--dsn` or the
    # manifest's), never to a different OKG_DSN left in the shell.
    env["OKG_DSN"] = plan.dsn
    prefix = list(okg_argv_prefix or running_okg_argv())
    opener = connect or _default_connect(plan.deployment_name)
    dep = str(plan.deployment_dir)
    steps: list[RemoveStep] = []
    generation_id: Optional[str] = None

    def fail(step: RemoveStep) -> RemoveSourceOutcome:
        steps.append(step)
        return RemoveSourceOutcome(plan=plan, status="failed", steps=steps)

    def claim(step_name: str) -> Optional[RemoveStep]:
        proc = _claim_identity(run, prefix, env, dep)
        if proc.returncode == 0:
            return None
        return RemoveStep(
            step_name, "failed",
            detail=_json_tail(proc),
            cause=(
                "this checkout could not claim the deployment identity for the "
                "current registry: " + redact_text(
                    _error_message(_json_payload(proc))
                    or _last_line(proc.stderr)
                    or "the ownership claim was refused"
                )
            ),
            fix_hint=(
                f"run `okg catalog ownership diagnose --deployment {dep} "
                "--json`, fix what it names, then run the same command again"
            ),
        )

    if plan.action != FINISH and plan.commit_mode == COMMIT:
        try:
            dirty = _registry_uncommitted(plan.deployment_dir, plan.registry_path)
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            return fail(RemoveStep(
                "disable", "failed",
                cause=f"could not read the git state of the registry: {redact_text(str(exc))}",
                fix_hint=f"run `git -C {dep} status`, fix it, then run the same command again",
            ))
        if dirty:
            return fail(RemoveStep(
                "disable", "failed",
                detail={"uncommitted": dirty},
                cause=(
                    f"{plan.registry_path.name} has uncommitted changes; "
                    "remove-source commits only its own edit"
                ),
                fix_hint=(
                    "commit or discard them, then run the same command again"
                ),
            ))

    # 1. Stop scheduled and default imports of this source before its records
    #    are retracted. (An explicit `okg ingest --include <name>` still runs;
    #    records it writes before the publish are retracted by a re-run, which
    #    continues the same unpublished retirement; after the publish they
    #    are refused, issue #3265.)
    if plan.action == RETIRE:
        original = plan.registry_path.read_bytes()
        changed = False
        try:
            changed = _disable_entry(plan)
            committed = (
                _commit_registry(
                    plan, f"okg deployment remove-source: disable {plan.name}",
                ) if changed else {"commit": None}
            )
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            unstaged: Optional[bool] = None
            if changed:
                # Never leave an edit of ours behind, in the file or in the
                # index: a re-run would refuse it as the user's own change.
                plan.registry_path.write_bytes(original)
                if plan.commit_mode == COMMIT:
                    unstaged = _git(
                        plan.deployment_dir, "restore", "--staged", "--",
                        str(plan.registry_path),
                    ).returncode == 0
            return fail(RemoveStep(
                "disable", "failed",
                detail={"entry_restored": changed, "unstaged": unstaged},
                cause=(
                    "could not disable the source before retracting its "
                    f"records: {redact_text(str(exc))}"
                ),
                fix_hint=(
                    "check that the registry file is writable (and, in a git "
                    "checkout, that `git commit` works), then run the same "
                    "command again"
                ),
            ))
        detail: dict[str, Any] = {"entry": plan.name, **committed}
        if not changed:
            detail["reason"] = "the entry was already disabled"
        disable_step = RemoveStep("disable", "ok" if changed else "skipped", detail=detail)
    else:
        disable_step = RemoveStep("disable", "skipped", detail={
            "reason": {
                PUBLISH_PENDING: (
                    "the records were already retracted; the entry is left as "
                    "the retirement recorded it"
                ),
                REGISTRY_ONLY: "the source has no records to retract",
                FINISH: "the entry was already removed",
            }[plan.action],
        })
    # The registry is part of the identity a publish checks (#2213). Claim it
    # before every retract or publish, whether or not this run changed the
    # entry: an earlier run (or a hand edit) may have changed it unclaimed.
    if plan.action in (RETIRE, PUBLISH_PENDING):
        refused = claim("disable")
        if refused is not None:
            return fail(refused)
        disable_step.detail["identity_claimed"] = True
    steps.append(disable_step)

    # 2. Retract, against a plan made now, after the disable, that must
    #    retract exactly the records the plan this run showed.
    if plan.action == RETIRE:
        from okg.substrate.source_retirement import remediate_source_retirement

        rerun_hint = (
            "run the same command again: it plans against the graph as it is "
            "now, shows that plan, and applies it"
        )
        try:
            with opener(plan.dsn) as conn:
                current = remediate_source_retirement(
                    conn,
                    source_name=plan.name,
                    deployment_name=plan.deployment_name,
                    branch_name=plan.branch,
                ).details.get("plan") or {}
                if (
                    plan.footprint_digest is not None
                    and current.get("footprint_digest") != plan.footprint_digest
                ):
                    return fail(RemoveStep(
                        "retract", "failed",
                        detail={"mutation_started": False},
                        cause=(
                            "the source's records changed between the plan "
                            "and the retraction; nothing was retracted"
                        ),
                        fix_hint=rerun_hint,
                    ))
                result = remediate_source_retirement(
                    conn,
                    source_name=plan.name,
                    deployment_name=plan.deployment_name,
                    branch_name=plan.branch,
                    expected_plan_digest=current.get("plan_digest"),
                    confirm_source=plan.name,
                    actor=ACTOR,
                    apply=True,
                )
        except (SubstrateError, ValueError) as exc:
            message = exc.message if isinstance(exc, SubstrateError) else str(exc)
            return fail(RemoveStep(
                "retract", "failed",
                cause=f"the records could not be retracted: {redact_text(message)}",
                fix_hint=getattr(exc, "fix_hint", None) or rerun_hint,
            ))
        receipt = result.details.get("receipt") or {}
        plan.retirement_id = receipt.get("retirement_id") or plan.retirement_id
        steps.append(RemoveStep("retract", "ok", detail={
            "records": plan.records,
            "relationships": plan.relationships,
            "retirement_id": plan.retirement_id,
        }))
    else:
        steps.append(RemoveStep("retract", "skipped", detail={
            "reason": {
                PUBLISH_PENDING: "a previous run already retracted the records",
                REGISTRY_ONLY: "the source has no records to retract",
                FINISH: "the records were already removed",
            }[plan.action],
        }))

    # 3. Publish a graph version without them; the publisher binds the
    #    retirement receipt to that version.
    if plan.action in (RETIRE, PUBLISH_PENDING):
        from okg.substrate.cli.init import _run_once_outcome

        proc = run(
            [*prefix, "run", "--once", "--deployment", dep, "--inline",
             "--apply", "--json"],
            capture_output=True, text=True, check=False, env=dict(env),
        )
        outcome = _run_once_outcome(proc)
        with opener(plan.dsn) as conn:
            receipt = _receipt_for(conn, plan.deployment_name, plan.branch, plan.name)
        bound = receipt if receipt and receipt["status"] == "published" else None
        if bound is None:
            gen = outcome.get("generation_id")
            if outcome.get("outcome") == "blocked" and gen:
                hint = (
                    f"run `okg generation describe {gen} --deployment "
                    f"{plan.deployment_name} --json` to see the failed checks, "
                    "fix them, then run the same command again; it publishes "
                    "without retracting again"
                )
            else:
                hint = (
                    "fix the cause, then run the same command again; it "
                    "publishes without retracting again"
                )
            reason = (
                redact_text(_error_message(_json_payload(proc)) or _last_line(proc.stderr))
                or f"publish outcome {outcome.get('outcome')}"
            )
            return fail(RemoveStep(
                "publish", "failed",
                detail={"run": _json_tail(proc), "outcome": outcome},
                cause=(
                    "the records were retracted but no published graph version "
                    f"records the removal yet: {reason}"
                ),
                fix_hint=hint,
            ))
        generation_id = str(bound["repair_generation_id"])
        steps.append(RemoveStep("publish", "ok", detail={
            "generation_id": generation_id,
        }))
    else:
        steps.append(RemoveStep("publish", "skipped", detail={
            "reason": "nothing to publish",
        }))

    # 4. Remove the registry entry, gated on the published retirement (or on
    #    a proven-empty footprint for a source that never reached the graph).
    #    The pending record is written first, so a re-run after a failure
    #    below finds it and finishes the claim and the policy update.
    if plan.action != FINISH:
        try:
            _write_pending(plan)
            with opener(plan.dsn) as conn:
                if plan.action == REGISTRY_ONLY and plan.retirement_id is None:
                    removed = _remove_entry(
                        deployment=plan.deployment_name,
                        source_id=plan.name,
                        authority=lambda: _unretired_removal_authority(conn, plan),
                        expected_entry=plan.entry,
                    )
                else:
                    removed = remove_registry_entry_after_retirement(
                        conn, deployment=plan.deployment_name, source_id=plan.name,
                    )
        except (SubstrateError, OSError, ValueError) as exc:
            _pending_path(plan.deployment_dir, plan.name).unlink(missing_ok=True)
            message = exc.message if isinstance(exc, SubstrateError) else str(exc)
            return fail(RemoveStep(
                "registry", "failed",
                cause=f"the registry entry was not removed: {redact_text(message)}",
                fix_hint=(
                    getattr(exc, "fix_hint", None)
                    or "check the registry file, then run the same command again"
                ),
            ))
        ledger = plan.deployment_dir / LEDGER_DIRNAME / f"{plan.name}.json"
        ledger_removed = ledger.is_file()
        ledger.unlink(missing_ok=True)
        try:
            committed = _commit_registry(
                plan, f"okg deployment remove-source: remove {plan.name}",
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return fail(RemoveStep(
                "registry", "failed",
                detail={"entry_removed": True},
                cause=(
                    "the entry was removed but the change could not be "
                    f"committed: {redact_text(str(exc))}"
                ),
                fix_hint=(
                    f"commit {plan.registry_path.name} in {dep}, then run the "
                    "same command again"
                ),
            ))
        steps.append(RemoveStep("registry", "ok", detail={
            "entry": plan.name,
            "status": removed["status"],
            **committed,
            "add_source_record_removed": ledger_removed,
        }))
    elif plan.resume_pending:
        # A previous run removed the entry; commit that removal if its commit
        # failed then. Nothing else is staged: only the registry file.
        try:
            committed = _commit_registry(
                plan, f"okg deployment remove-source: remove {plan.name}",
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return fail(RemoveStep(
                "registry", "failed",
                detail={"entry_removed": True, "committed": False},
                cause=(
                    "the entry was removed by a previous run but the change "
                    f"is still not committed: {redact_text(str(exc))}"
                ),
                fix_hint=(
                    f"commit {plan.registry_path.name} in {dep}, then run the "
                    "same command again"
                ),
            ))
        steps.append(RemoveStep(
            "registry", "ok" if committed.get("commit") else "skipped",
            detail={"reason": "the entry was already removed", **committed},
        ))
    else:
        steps.append(RemoveStep("registry", "skipped", detail={
            "reason": "the entry was already removed",
        }))

    # 5. The registry is part of the deployment identity a publish checks;
    #    record the new identity, as add-source does after it saves.
    refused = claim("claim")
    if refused is not None:
        return fail(refused)
    steps.append(RemoveStep("claim", "ok", detail={"deployment": dep}))

    # 6. The access policy lists sources by name; drop this one.
    policy = _policy_step(plan, env, reconcile_policy)
    steps.append(policy)
    if policy.status == "failed":
        return RemoveSourceOutcome(plan=plan, status="failed", steps=steps)
    _pending_path(plan.deployment_dir, plan.name).unlink(missing_ok=True)
    return RemoveSourceOutcome(
        plan=plan, status="removed", steps=steps, generation_id=generation_id,
    )


__all__ = [
    "FINISH",
    "PUBLISH_PENDING",
    "REGISTRY_ONLY",
    "RETIRE",
    "RemoveSourceOutcome",
    "RemoveSourcePlan",
    "RemoveStep",
    "apply_remove_source",
    "plan_remove_source",
    "remove_registry_entry_after_retirement",
]
