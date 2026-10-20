"""PACTSource — substrate adapter for PACT manifests.

The source projects file-backed PACT changes into the shared
proposal/task/requirement graph shape while keeping PACT changes
distinct from OpenSpec changes at the root subtype.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field, replace
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterator, Mapping, Optional

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
)
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.store import (
    intent_digest,
    load_manifest_bytes,
    load_manifest_file as load_manifest_file,
    state_digest,
)
from okg.substrate.pact.validation import derive_alignment
from okg.substrate.support import redact_text


# Synthetic manifest-hash key carrying the decision-ledger watermark; PACT
# change ids cannot start with '_' so this can never shadow a real change.
_LEDGER_HASH_KEY = "__decision_ledger__"
_EVALUATION_HASH_KEY = "__pact_evaluation__"


def _decision_rows_identity(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        rows,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return (
        f"{max((int(row.get('decision_id') or 0) for row in rows), default=0)}"
        f":{len(rows)}:{hashlib.sha256(payload).hexdigest()}"
    )


@dataclass(frozen=True)
class _PactManifestInventory:
    manifest: PactManifest
    referenced_paths: tuple[tuple[str, str], ...]
    environment_names: tuple[str, ...]
    affected_code: tuple[str, ...]


@lru_cache(maxsize=2048)
def _cached_manifest_inventory(
    content: bytes,
    source_path_text: str,
) -> _PactManifestInventory:
    """Parse immutable manifest bytes once per worker process.

    Scheduled polls construct a fresh adapter on every invocation. Keying this
    cache by the exact bytes and path preserves the same validated inventory
    while preventing every no-change poll from reparsing the entire PACT
    corpus. Invalid manifests are never cached because the exception escapes.
    """
    from okg.substrate.pact.validation import _checker_script_refs

    source_path = Path(source_path_text)
    manifest = load_manifest_bytes(content, source_path=source_path)
    references: set[tuple[str, str]] = set()
    for binding in manifest.formal:
        references.add(("formal", str(binding.module)))
        if binding.config:
            references.add(("formal", str(binding.config)))
        references.update(
            ("formal", ref)
            for ref in _checker_script_refs(binding.checker_command or "")
        )
        references.update(
            ("formal", str(ref).split("::", 1)[0])
            for ref in binding.refinement_tests
            if str(ref).split("::", 1)[0]
        )
    for evidence in manifest.evidence:
        artifact = (
            (evidence.data or {}).get("artifact_path")
            or evidence.artifact
        )
        if artifact:
            references.add(("evidence", str(artifact)))
    environment_names = {
        str(check.env or check.name or check.id)
        for task in manifest.tasks
        for check in task.environment
        if check.required and check.kind == "env_var"
    }
    return _PactManifestInventory(
        manifest=manifest,
        referenced_paths=tuple(sorted(references)),
        environment_names=tuple(sorted(environment_names)),
        affected_code=tuple(
            sorted(str(path) for path in manifest.change.affected_code)
        ),
    )


@dataclass(frozen=True)
class _CapturedPactManifest:
    change_id: str
    path: Path
    relative_path: str
    archived: bool
    content: bytes
    sha256: str
    manifest: PactManifest


@dataclass(frozen=True)
class PactEvaluationSnapshot:
    """One immutable set of every authority consumed by ``PACTSource``."""

    manifests: tuple[_CapturedPactManifest, ...]
    manifest_ids: frozenset[str]
    decision_rows_by_change: Mapping[str, tuple[dict[str, Any], ...]]
    ledger_status: Mapping[str, Any]
    evaluation_context: Any
    token: dict[str, Any]
    identity: str


class _PactEvaluationProbe:
    """Content probe whose full capture is also the source's read snapshot."""

    def __init__(
        self,
        source: "PACTSource",
        *,
        config: Mapping[str, Any],
        emit_targets: Any,
    ) -> None:
        from okg.substrate.library.sources.change_probe import emit_version_for

        self._source = source
        self._config_hash = hashlib.sha256(
            json.dumps(
                dict(config),
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        targets = (
            tuple(emit_targets)
            if isinstance(emit_targets, (list, tuple))
            else (emit_targets,)
        )
        self._emit_version = emit_version_for(*targets)
        self._inventory_cache: dict[str, _PactManifestInventory] = {}

    def token_for_identity(self, identity: str) -> dict[str, Any]:
        from okg.substrate.library.sources.change_probe import compose_token

        return compose_token(
            content={"kind": "content_hash", "value": identity},
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def build_token(self) -> dict[str, Any]:
        snapshot = self._source._capture_evaluation_snapshot(
            self,
            token_only=True,
        )
        return dict(snapshot.token)

    def capture_snapshot(self) -> PactEvaluationSnapshot:
        from okg.substrate.library.sources.change_probe import (
            ChangeProbeError,
            tokens_equal,
        )

        snapshot = self._source._capture_evaluation_snapshot(
            self,
            token_only=False,
        )
        post_token = self.build_token()
        if not tokens_equal(snapshot.token, post_token):
            raise ChangeProbeError(
                "PACT evaluation inputs changed during snapshot capture"
            )
        return snapshot

    def probe(self, *, cursor, sync_scope):
        from okg.substrate.library.sources.change_probe import (
            CURSOR_TOKEN_KEY,
            ProbeVerdict,
            tokens_equal,
        )

        del sync_scope
        prior = (
            cursor.get(CURSOR_TOKEN_KEY)
            if isinstance(cursor, Mapping)
            else None
        )
        token = self.build_token()
        changed = prior is None or not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason=(
                "pact_evaluation_changed"
                if changed
                else "pact_evaluation_unchanged"
            ),
        )

# Prose supersession declarations (operator-surface-list-work
# req.listwork.honest-pact-projection). Conservative: a change-id-shaped
# token right after the phrase. Ledger supersede verdicts are the v4
# truth; prose covers the pre-v4 history the drain dossier text-mined.
_SUPERSEDED_BY_RE = re.compile(r"[Ss]uperseded by\s+[`'\"]?([a-z0-9][a-z0-9-]{3,})")
_SUPERSEDES_RE = re.compile(
    r"[Tt]his (?:PACT |change )?supersedes\s+(?:the\s+)?[`'\"]?"
    r"([a-z0-9][a-z0-9-]{3,})"
)

# Finite graph-emission contract shared with deployment rollback admission.
# Any PACTSource change that emits a new subtype/triple must update this
# declaration and the deployment-owned producer policy in the same release.
PACT_EMISSION_CONTRACT = {
    "nodes": (
        ("pact_change", "pact-change:", "change"),
        ("spec_capability", "cap:pact", "capability"),
        ("spec_delta", "pact-delta:", "delta"),
        ("task_group", "pact-taskgroup:", "task_group"),
        ("pact_task", "pact-task:", "task"),
        ("verification_step", "pact-verification:", "verification"),
        ("acceptance_criterion", "pact-req:", "requirement"),
        ("acceptance_scenario", "pact-scenario:", "scenario"),
        ("pact_decision", "pact-decision:", "decision"),
    ),
    "edges": (
        ("pact_decision", "targets", "pact_change", "decision_targets_change"),
        (
            "pact_change",
            "supersedes",
            "pact_change",
            "change_supersedes_change",
        ),
        ("pact_change", "contains", "spec_delta", "change_contains_delta"),
        (
            "pact_change",
            "contains",
            "task_group",
            "change_contains_task_group",
        ),
        (
            "spec_delta",
            "targets",
            "spec_capability",
            "delta_targets_capability",
        ),
        (
            "spec_delta",
            "contains",
            "acceptance_criterion",
            "delta_contains_requirement",
        ),
        (
            "acceptance_criterion",
            "contains",
            "acceptance_scenario",
            "requirement_contains_scenario",
        ),
        (
            "acceptance_criterion",
            "contains",
            "verification_step",
            "requirement_contains_verification",
        ),
        ("task_group", "contains", "pact_task", "task_group_contains_task"),
        (
            "pact_task",
            "contains",
            "verification_step",
            "task_contains_verification",
        ),
        ("pact_task", "blocks", "pact_task", "task_blocks_task"),
        ("pact_task", "requires", "pact_task", "task_requires_task"),
        ("pact_task", "enables", "pact_task", "task_enables_task"),
        ("pact_task", "supersedes", "pact_task", "task_supersedes_task"),
        (
            "pact_task",
            "conflicts_with",
            "pact_task",
            "task_conflicts_with_task",
        ),
    ),
}


class _LedgerWatermarkProbe:
    """Change-probe component for one PACT ledger table (DB state, #323).

    Decision and coordination-event identities cover the complete projected
    rows. Disabled-by-env yields a stable token (the ledger is intentionally
    not consulted). An unavailable ledger RAISES, so the UnionProbe ->
    run_probe boundary forces a full read — matching PACTSource.run's
    loud-but-projects behavior on ledger unavailability (a no-change skip must
    never silently miss a ledger change that could not be checked for).
    """

    def __init__(
        self,
        repo: str,
        *,
        emit_targets: Any = None,
        watermark_kind: str = "decisions",
    ) -> None:
        self._repo = repo
        if watermark_kind not in {"decisions", "coordination_events"}:
            raise ValueError(f"unsupported PACT ledger watermark: {watermark_kind}")
        self._watermark_kind = watermark_kind
        # The emission logic the token must invalidate on. PACTSource composes
        # this probe inside a UnionProbe whose git half already fingerprints the
        # module, so the default (the probe class) suffices there. A source with
        # NO other probe half (PACTCoordinationEventSource) passes its own class
        # so an emission-logic change busts the token.
        self._emit_targets: tuple[Any, ...] = (
            tuple(emit_targets) if emit_targets else (_LedgerWatermarkProbe,)
        )

    def build_token(self) -> dict[str, Any]:
        import os

        if os.environ.get("OKG_PACT_DECISION_PROJECTION", "on") == "off":
            value = "disabled"
        else:
            from okg.substrate.pact.decisions import (
                load_coordination_event_snapshot,
                load_decision_rows,
                require_ledger_connection,
            )

            conn = require_ledger_connection()
            try:
                if self._watermark_kind == "coordination_events":
                    _rows, snapshot = load_coordination_event_snapshot(conn)
                    value = str(snapshot["identity"])
                else:
                    value = _decision_rows_identity(load_decision_rows(conn))
            finally:
                conn.close()
        return self.token_for_value(value)

    def token_for_value(self, value: str) -> dict[str, Any]:
        from okg.substrate.library.sources.change_probe import (
            compose_token,
            emit_version_for,
        )

        config_key = (
            f"pact-ledger:{self._repo}"
            if self._watermark_kind == "decisions"
            else f"pact-ledger:{self._watermark_kind}:{self._repo}"
        )
        return compose_token(
            content={"kind": "version_token", "value": value},
            config=hashlib.sha256(
                config_key.encode("utf-8")
            ).hexdigest(),
            emit_version=emit_version_for(*self._emit_targets),
        )

    def probe(self, *, cursor, sync_scope):
        from okg.substrate.library.sources.change_probe import (
            CURSOR_TOKEN_KEY,
            ProbeVerdict,
            tokens_equal,
        )

        prior = cursor.get(CURSOR_TOKEN_KEY) if isinstance(cursor, dict) else None
        token = self.build_token()
        changed = prior is None or not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="ledger_changed" if changed else "ledger_unchanged",
        )



@dataclass
class PACTSource:
    repo: str
    pact_root: Path | str

    name: str = "pact"
    profile: str = "discovery_crawl"
    # The probe captures manifests, project policy, referenced proof inputs,
    # environment truthiness, the complete decision ledger, and pinned Git
    # history. It is authoritative only at the Git work-tree root.
    change_probe_kind = "content_hash"
    emission_contract = PACT_EMISSION_CONTRACT
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _decision_rows_by_change: dict[str, list[dict[str, Any]]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _skipped_supersession_targets: list[dict[str, str]] = field(
        default_factory=list,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if isinstance(self.pact_root, str):
            self.pact_root = Path(self.pact_root).expanduser()
        self.pact_root = Path(self.pact_root).resolve()
        self.change_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        try:
            repo_root = self.pact_root.parent.resolve()
            return in_proc_workdir(repo_root) == repo_root
        except Exception:  # noqa: BLE001 - unavailable Git authority fails closed
            return False

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        config = {
            "repo": self.repo,
            "pact_root": str(self.pact_root),
            "manifest_branches": ["changes", "archive"],
        }
        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )
        return _PactEvaluationProbe(
            self,
            config=config,
            emit_targets=emit_targets,
        )

    @staticmethod
    def _manifest_inventory(
        probe: _PactEvaluationProbe,
        *,
        content: bytes,
        source_path: Path,
    ) -> _PactManifestInventory:
        digest = hashlib.sha256(content).hexdigest()
        cached = probe._inventory_cache.get(digest)
        if cached is not None:
            return cached

        inventory = _cached_manifest_inventory(
            content,
            str(source_path),
        )
        probe._inventory_cache[digest] = inventory
        return inventory

    @staticmethod
    def _manifest_paths(
        branch_root: Path,
    ) -> tuple[Path, ...]:
        import stat

        from okg.substrate.library.sources.change_probe import ChangeProbeError

        try:
            with os.scandir(branch_root) as entries:
                candidates = sorted(entries, key=lambda entry: entry.name)
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise ChangeProbeError(
                f"PACT snapshot could not enumerate manifest directory "
                f"{branch_root}"
            ) from exc

        paths: list[Path] = []
        for entry in candidates:
            try:
                if not entry.is_dir(follow_symlinks=True):
                    continue
                path = Path(entry.path) / "pact.yaml"
                mode = path.stat().st_mode
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ChangeProbeError(
                    f"PACT snapshot could not inspect manifest under "
                    f"{entry.path}"
                ) from exc
            if stat.S_ISREG(mode):
                paths.append(path)
        return tuple(paths)

    def _capture_manifest_snapshot(
        self,
        probe: _PactEvaluationProbe,
    ) -> tuple[
        tuple[_CapturedPactManifest, ...],
        list[dict[str, str]],
        set[tuple[str, str]],
        set[str],
        set[tuple[str, ...]],
    ]:
        from okg.substrate.library.sources.change_probe import ChangeProbeError

        all_documents: list[tuple[str, str, Path, bytes, str]] = []
        selected: dict[
            str,
            tuple[str, Path, bytes, str, _PactManifestInventory],
        ] = {}
        for branch in ("changes", "archive"):
            branch_root = self.pact_root / branch
            for path in self._manifest_paths(branch_root):
                try:
                    content = path.read_bytes()
                except OSError as exc:
                    raise ChangeProbeError(
                        f"PACT snapshot could not read manifest {path}"
                    ) from exc
                change_id = path.parent.name
                digest = hashlib.sha256(content).hexdigest()
                all_documents.append(
                    (branch, change_id, path, content, digest)
                )
                # Active wins over archived, matching the established source
                # projection when both placements temporarily exist.
                if change_id in selected and branch == "archive":
                    continue
                inventory = self._manifest_inventory(
                    probe,
                    content=content,
                    source_path=path,
                )
                selected[change_id] = (
                    branch,
                    path,
                    content,
                    digest,
                    inventory,
                )

        manifests: list[_CapturedPactManifest] = []
        reference_paths: set[tuple[str, str]] = set()
        environment_names: set[str] = set()
        affected_scopes: set[tuple[str, ...]] = set()
        for change_id, (
            branch,
            path,
            content,
            digest,
            inventory,
        ) in sorted(selected.items()):
            manifests.append(_CapturedPactManifest(
                change_id=change_id,
                path=path,
                relative_path=path.relative_to(
                    self.pact_root.parent
                ).as_posix(),
                archived=branch == "archive",
                content=content,
                sha256=digest,
                manifest=inventory.manifest,
            ))
            reference_paths.update(inventory.referenced_paths)
            environment_names.update(inventory.environment_names)
            if inventory.affected_code:
                affected_scopes.add(inventory.affected_code)
        manifest_identity = [
            {
                "placement": branch,
                "change_id": change_id,
                "path": path.relative_to(
                    self.pact_root.parent
                ).as_posix(),
                "sha256": digest,
            }
            for branch, change_id, path, _content, digest in all_documents
        ]
        return (
            tuple(manifests),
            manifest_identity,
            reference_paths,
            environment_names,
            affected_scopes,
        )

    @staticmethod
    def _captured_file(
        path: Path,
    ) -> bytes | None:
        import stat

        from okg.substrate.library.sources.change_probe import ChangeProbeError

        try:
            mode = path.stat().st_mode
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ChangeProbeError(
                f"PACT snapshot could not inspect referenced file {path}"
            ) from exc
        if not stat.S_ISREG(mode):
            return None
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ChangeProbeError(
                f"PACT snapshot could not read referenced file {path}"
            ) from exc

    @staticmethod
    def _captured_evidence_registry(
        project_content: bytes | None,
    ):
        from okg.substrate import fastyaml
        from okg.substrate.pact.evidence_kinds import (
            DEFAULT_REGISTRY,
            EvidenceKindRegistry,
            _registry_from_settings,
        )

        if project_content is None:
            return DEFAULT_REGISTRY
        try:
            loaded = fastyaml.safe_load(project_content.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - surfaced in graph attrs
            return EvidenceKindRegistry(
                canonical=DEFAULT_REGISTRY.canonical,
                aliases=dict(DEFAULT_REGISTRY.aliases),
                config_errors=(
                    f"pact/project.yaml unreadable ({exc}); "
                    "evidence-kind registry running on code defaults",
                ),
            )
        if not isinstance(loaded, dict):
            return EvidenceKindRegistry(
                canonical=DEFAULT_REGISTRY.canonical,
                aliases=dict(DEFAULT_REGISTRY.aliases),
                config_errors=(
                    "pact/project.yaml unreadable (YAML document must be a "
                    f"mapping; got {type(loaded).__name__}); evidence-kind "
                    "registry running on code defaults",
                ),
            )
        return _registry_from_settings(loaded)

    @staticmethod
    def _affected_code_timestamps(
        repo_root: Path,
        *,
        head_sha: str,
        affected_scopes: set[tuple[str, ...]],
    ) -> dict[tuple[str, ...], str | None]:
        from okg.substrate.library.sources._git_helpers import run_git

        timestamps: dict[tuple[str, ...], str | None] = {}
        for scope in sorted(affected_scopes):
            out = run_git(
                repo_root,
                [
                    "log",
                    "-1",
                    "--format=%cI",
                    head_sha,
                    "--",
                    *scope,
                ],
            )
            timestamps[tuple(sorted(scope))] = (
                out.strip().splitlines()[0]
                if out.strip()
                else None
            )
        return timestamps

    @classmethod
    def _git_history_authority(
        cls,
        repo_root: Path,
    ) -> dict[str, Any]:
        """Capture non-HEAD inputs that can change path-history queries."""
        from okg.substrate.library.sources._git_helpers import run_git

        def _git_path_identity(name: str) -> dict[str, Any]:
            raw = run_git(repo_root, ["rev-parse", "--git-path", name]).strip()
            path = Path(raw)
            if not path.is_absolute():
                path = repo_root / path
            path = Path(os.path.abspath(os.path.normpath(str(path))))
            content = cls._captured_file(path)
            return {
                "path": str(path),
                "present": content is not None,
                "sha256": (
                    hashlib.sha256(content).hexdigest()
                    if content is not None
                    else None
                ),
            }

        def _environment_path_identity(
            name: str,
        ) -> dict[str, Any] | None:
            raw_value = os.environ.get(name)
            if not raw_value:
                return None
            override_path = Path(raw_value).expanduser()
            if not override_path.is_absolute():
                override_path = repo_root / override_path
            override_path = Path(os.path.abspath(os.path.normpath(
                str(override_path)
            )))
            override_content = cls._captured_file(override_path)
            return {
                "path": str(override_path),
                "present": override_content is not None,
                "sha256": (
                    hashlib.sha256(override_content).hexdigest()
                    if override_content is not None
                    else None
                ),
            }

        replace_ref_base = (
            os.environ.get("GIT_REPLACE_REF_BASE") or "refs/replace"
        )
        git_environment = {
            name: os.environ.get(name)
            for name in (
                "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                "GIT_COMMON_DIR",
                "GIT_DIR",
                "GIT_GRAFT_FILE",
                "GIT_NO_REPLACE_OBJECTS",
                "GIT_OBJECT_DIRECTORY",
                "GIT_REPLACE_REF_BASE",
                "GIT_SHALLOW_FILE",
                "GIT_WORK_TREE",
            )
        }
        return {
            "shallow": _git_path_identity("shallow"),
            "shallow_override": _environment_path_identity(
                "GIT_SHALLOW_FILE"
            ),
            "grafts": _git_path_identity("info/grafts"),
            "grafts_override": _environment_path_identity("GIT_GRAFT_FILE"),
            "replace_refs": run_git(
                repo_root,
                [
                    "for-each-ref",
                    "--format=%(refname)%00%(objectname)",
                    replace_ref_base,
                ],
            ),
            "environment": git_environment,
        }

    def _capture_evaluation_snapshot(
        self,
        probe: _PactEvaluationProbe,
        *,
        token_only: bool,
    ) -> PactEvaluationSnapshot:
        from okg.substrate.library.sources._git_helpers import (
            in_proc_head_snapshot,
            in_proc_workdir,
        )
        from okg.substrate.library.sources.change_probe import ChangeProbeError
        from okg.substrate.pact.evaluation_context import (
            PactEvaluationContext,
        )
        from okg.substrate.pact.fsm import DecisionEvent

        repo_root = self.pact_root.parent.resolve()
        (
            manifests,
            manifest_identity,
            reference_paths,
            environment_names,
            affected_scopes,
        ) = self._capture_manifest_snapshot(probe)

        project_path = repo_root / "pact" / "project.yaml"
        project_content = self._captured_file(project_path)
        captured_files: dict[str, bytes | None] = {
            str(project_path): project_content,
        }
        reference_identity: list[dict[str, Any]] = []
        resolved_repo_root = repo_root.resolve()
        for reference_kind, raw_ref in sorted(reference_paths):
            raw_candidate = Path(raw_ref)
            context_path = Path(os.path.abspath(os.path.normpath(str(
                raw_candidate
                if raw_candidate.is_absolute()
                else repo_root / raw_candidate
            ))))
            if reference_kind == "formal":
                if raw_candidate.is_absolute():
                    reference_identity.append({
                        "role": reference_kind,
                        "reference": raw_ref,
                        "admitted": False,
                        "reason": "absolute_formal_path",
                    })
                    continue
                resolved = context_path.resolve()
                if (
                    resolved != resolved_repo_root
                    and resolved_repo_root not in resolved.parents
                ):
                    captured_files[str(context_path)] = None
                    reference_identity.append({
                        "role": reference_kind,
                        "reference": raw_ref,
                        "admitted": False,
                        "reason": "formal_path_escapes_repo",
                    })
                    continue
            else:
                expanded = raw_candidate.expanduser()
                resolved = (
                    expanded
                    if expanded.is_absolute()
                    else repo_root / expanded
                ).resolve()
                if (
                    raw_candidate.is_absolute()
                    and resolved != resolved_repo_root
                    and resolved_repo_root not in resolved.parents
                ):
                    reference_identity.append({
                        "role": reference_kind,
                        "reference": raw_ref,
                        "admitted": False,
                        "reason": "absolute_evidence_path_outside_repo",
                    })
                    continue
            content = self._captured_file(resolved)
            # Captured evaluators resolve paths lexically. Preserve that lookup
            # key as well as the real path used for the exact read; this is
            # especially important for evidence paths beginning with ``~``.
            captured_files[str(context_path)] = content
            captured_files[str(resolved)] = content
            reference_identity.append({
                "role": reference_kind,
                "reference": raw_ref,
                "admitted": True,
                "path": str(resolved),
                "sha256": (
                    hashlib.sha256(content).hexdigest()
                    if content is not None
                    else None
                ),
                "present": content is not None,
            })

        if manifests:
            decision_rows, ledger_status = self._load_ledger_decision_rows()
        else:
            # Decision rows are projected only through a matching manifest.
            # With no manifests there is no ledger-derived output, so requiring
            # an unrelated ledger would make first-run repository bootstrap
            # fail without adding any snapshot authority.
            decision_rows, ledger_status = {}, {
                "status": "not_applicable_no_manifests",
                "watermark": "not-applicable",
            }
        if ledger_status.get("status") not in {
            "ok",
            "disabled_by_env",
            "not_applicable_no_manifests",
        }:
            raise ChangeProbeError(
                "PACT decision ledger unavailable during exact snapshot: "
                f"{ledger_status.get('reason') or ledger_status.get('status')}"
            )
        flattened_rows = sorted(
            (
                dict(row)
                for rows in decision_rows.values()
                for row in rows
            ),
            key=lambda row: int(row.get("decision_id") or 0),
        )
        ledger_identity = _decision_rows_identity(flattened_rows)
        normalized_ledger_status = {
            **dict(ledger_status),
            "identity": (
                "disabled"
                if ledger_status.get("status") == "disabled_by_env"
                else ledger_identity
            ),
        }
        frozen_rows = MappingProxyType({
            str(change_id): tuple(dict(row) for row in rows)
            for change_id, rows in decision_rows.items()
        })

        git_root = in_proc_workdir(repo_root)
        git_authoritative = git_root == repo_root
        head_sha: str | None = None
        tree_sha: str | None = None
        branch: str | None = None
        affected_timestamps: dict[tuple[str, ...], str | None] = {}
        history_authority: dict[str, Any] | None = None
        if git_authoritative:
            head = in_proc_head_snapshot(repo_root)
            head_sha = head.head_sha
            tree_sha = head.tree_sha
            branch = head.branch
            history_authority = self._git_history_authority(repo_root)
            if not token_only:
                affected_timestamps = self._affected_code_timestamps(
                    repo_root,
                    head_sha=head_sha,
                    affected_scopes=affected_scopes,
                )

        env_truthiness = {
            name: bool(os.environ.get(name))
            for name in sorted(environment_names)
        }
        registry = self._captured_evidence_registry(project_content)
        events_by_change = {
            change_id: tuple(
                DecisionEvent(
                    change_id=row["change_id"],
                    verdict=row["verdict"],
                    principal=row["principal"],
                    authority=row.get("authority") or "operator",
                    note=row.get("note"),
                    target=row.get("target"),
                    from_state=row.get("from_state"),
                    to_state=row.get("to_state"),
                    intent_digest=row.get("intent_digest"),
                    decided_at=row.get("decided_at"),
                )
                for row in rows
            )
            for change_id, rows in frozen_rows.items()
        }
        redaction_home = str(Path.home())
        context = PactEvaluationContext(
            env_truthiness=env_truthiness,
            files=captured_files,
            evidence_registry=registry,
            ledger_authoritative=True,
            ledger_available=True,
            decision_events_by_change=events_by_change,
            git_authoritative=git_authoritative,
            git_head_sha=head_sha,
            git_available=git_authoritative,
            affected_code_timestamps=affected_timestamps,
            redaction_home=redaction_home,
        )

        identity_payload = {
            "manifests": manifest_identity,
            "project": {
                "present": project_content is not None,
                "sha256": (
                    hashlib.sha256(project_content).hexdigest()
                    if project_content is not None
                    else None
                ),
            },
            "references": reference_identity,
            "environment_truthiness": env_truthiness,
            "ledger": {
                "mode": normalized_ledger_status["status"],
                "identity": normalized_ledger_status["identity"],
            },
            "git": {
                "authoritative": git_authoritative,
                "head_sha": head_sha,
                "tree_sha": tree_sha,
                "branch": branch,
                "history_authority": history_authority,
                "affected_scopes": sorted(
                    [list(scope) for scope in affected_scopes]
                ),
            },
            "redaction_home": redaction_home,
        }
        identity = hashlib.sha256(
            json.dumps(
                identity_payload,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        ).hexdigest()
        token = probe.token_for_identity(identity)
        return PactEvaluationSnapshot(
            manifests=manifests,
            manifest_ids=frozenset(item.change_id for item in manifests),
            decision_rows_by_change=frozen_rows,
            ledger_status=MappingProxyType(normalized_ledger_status),
            evaluation_context=context,
            token=token,
            identity=identity,
        )

    def _redact_text(
        self,
        value: str | None,
        *,
        home_dir: str | None = None,
    ) -> str | None:
        if value is None:
            return None
        repo_root = self.pact_root.parent if self.pact_root.name == "pact" else None
        redacted, _markers = redact_text(
            str(value),
            repo_root=repo_root,
            home_dir=Path(home_dir) if home_dir is not None else None,
        )
        return redacted

    def _redact_value(
        self,
        value: Any,
        *,
        home_dir: str | None = None,
    ) -> Any:
        if isinstance(value, str):
            return self._redact_text(value, home_dir=home_dir)
        if isinstance(value, list):
            return [
                self._redact_value(item, home_dir=home_dir)
                for item in value
            ]
        if isinstance(value, tuple):
            return [
                self._redact_value(item, home_dir=home_dir)
                for item in value
            ]
        if isinstance(value, dict):
            return {
                str(key): self._redact_value(item, home_dir=home_dir)
                for key, item in value.items()
            }
        return value

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        scope_keys = self._scope_keys(sync_scope)
        scope_changes = self._scope_changes_from_keys(scope_keys)
        evaluation_snapshot = (
            self.change_probe.capture_snapshot()
            if self.probe_short_circuit_safe
            else None
        )
        if evaluation_snapshot is not None:
            decision_rows = {
                change_id: [dict(row) for row in rows]
                for change_id, rows
                in evaluation_snapshot.decision_rows_by_change.items()
            }
            ledger_status = dict(evaluation_snapshot.ledger_status)
            selected_snapshot_manifests = (
                tuple(
                    item
                    for item in evaluation_snapshot.manifests
                    if item.change_id in scope_changes
                )
                if scope_changes
                else evaluation_snapshot.manifests
            )
            captured_manifests = {
                item.change_id: {
                    "path": item.path,
                    "bytes": item.content,
                    "sha256": item.sha256,
                    "manifest": item.manifest,
                    "archived": item.archived,
                }
                for item in selected_snapshot_manifests
            }
            applied_token = (
                evaluation_snapshot.token if not scope_keys else None
            )
            applied_scope = "global" if not scope_keys else "unavailable"
            applied_authority = "exact" if not scope_keys else "unavailable"
            evaluation_context = evaluation_snapshot.evaluation_context
            manifest_ids = evaluation_snapshot.manifest_ids
        else:
            decision_rows, ledger_status = self._load_ledger_decision_rows()
            captured_manifests = self._capture_manifests(
                scope_changes=scope_changes
            )
            applied_token = None
            applied_scope = "unavailable"
            applied_authority = "unavailable"
            evaluation_context = None
            manifest_ids = frozenset(self._all_change_ids())
        self._decision_rows_by_change = decision_rows
        captured_item_hashes = {
            change_id: payload["sha256"]
            for change_id, payload in captured_manifests.items()
        }
        manifest_hashes = dict(captured_item_hashes)
        # New ledger decisions must bust both unchanged-noop paths even though
        # they live outside the manifest files: fold the ledger watermark into
        # the manifest-hash dict under a key no change id can collide with
        # (change ids cannot start with '_' per the PACT id grammar).
        ledger_identity = str(
            ledger_status.get("identity")
            or ledger_status["watermark"]
        )
        manifest_hashes[_LEDGER_HASH_KEY] = ledger_identity
        if evaluation_snapshot is not None:
            manifest_hashes[_EVALUATION_HASH_KEY] = (
                evaluation_snapshot.identity
            )
        manifest_count = len(captured_item_hashes)
        next_manifest_hashes = self._next_manifest_hashes(
            cursor,
            current=manifest_hashes,
            scope_changes=scope_changes,
        )
        completed_scope = mode in {"reconcile", "scope_complete"}
        if (
            scope_changes
            and self.probe_short_circuit_safe
            and isinstance(cursor, dict)
            and self._scoped_manifest_hashes_unchanged(
                cursor,
                current=manifest_hashes,
                scope_changes=scope_changes,
            )
        ):
            self.last_run_stats = {
                "scope_status": "scoped_unchanged_noop",
                "decision_ledger": ledger_status,
                "scope_keys": len(scope_keys),
                "recognized_changes": len(scope_changes),
                "manifest_hashes_checked": manifest_count,
                "manifests_loaded": 0,
                "facts_evaluated": 0,
                "facts_emitted": 0,
                "facts_filtered_unchanged": 0,
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=(
                    completed_scope and cursor_has_record_set_authority(cursor)
                ),
                next_cursor=preserve_cursor_record_set_authority(
                    {
                        "manifest_hashes": next_manifest_hashes,
                        "fact_hashes": dict(cursor.get("fact_hashes") or {}),
                    },
                    cursor,
                ),
                applied_raw_token=applied_token,
                applied_token_scope=applied_scope,
                applied_token_authority=applied_authority,
                run_mode=mode,
                record_set=None,
            )
        if (
            not scope_changes
            and self.probe_short_circuit_safe
            and isinstance(cursor, dict)
            and cursor.get("manifest_hashes") == manifest_hashes
        ):
            has_record_set_authority = cursor_has_record_set_authority(cursor)
            if not completed_scope or has_record_set_authority:
                record_set = (
                    RecordSet.from_cursor(cursor["record_set"])
                    if completed_scope and cursor.get("record_set") is not None
                    else None
                )
                self.last_run_stats = {
                    "scope_status": "unchanged_noop",
                    "decision_ledger": ledger_status,
                    "scope_keys": len(scope_keys),
                    "recognized_changes": 0,
                    "manifest_hashes_checked": manifest_count,
                    "manifests_loaded": 0,
                    "facts_emitted": 0,
                    "record_set_authority": (
                        "preserved" if has_record_set_authority else "not_required"
                    ),
                }
                return SourceRun(
                    facts=iter(()),
                    completed_scope=completed_scope and has_record_set_authority,
                    next_cursor=preserve_cursor_record_set_authority(
                        {
                            "manifest_hashes": manifest_hashes,
                            "fact_hashes": dict(cursor.get("fact_hashes") or {}),
                        },
                        cursor,
                    ),
                    applied_raw_token=applied_token,
                    applied_token_scope=applied_scope,
                    applied_token_authority=applied_authority,
                    run_mode=mode,
                    record_set=record_set,
                    # `record_set` is None here only when the prior authority
                    # is external-by-reference. The cursor above carries that
                    # reference forward, so the runner must not infer a
                    # footprint from the empty stream (issue #2122).
                    preserves_prior_record_set=(
                        completed_scope
                        and has_record_set_authority
                        and record_set is None
                    ),
                )

        manifest_entries = [
            (
                payload.get("manifest")
                or load_manifest_bytes(
                    payload["bytes"],
                    source_path=payload["path"],
                ),
                payload,
            )
            for _change_id, payload in sorted(captured_manifests.items())
        ]
        manifests = [manifest for manifest, _payload in manifest_entries]
        del run_id
        base_revision = {
            "source_manifest_sha256": hashlib.sha256(
                json.dumps(
                    manifest_hashes,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        }
        facts: list[NodeFact | EdgeFact] = []
        self._skipped_supersession_targets = []

        if not scope_changes:
            facts.append(
                NodeFact(
                    node_id="cap:pact",
                    subtype="spec_capability",
                    attrs={
                        "capability_name": "pact",
                        "purpose": "PACT specification language and changes",
                        "requirement_count": sum(
                            len(m.requirements) for m in manifests
                        ),
                        "source_provider": "pact",
                    },
                    source_record_id={"repo": self.repo, "node_id": "cap:pact"},
                    source_revision=base_revision,
                )
            )
        for manifest, payload in manifest_entries:
            facts.extend(
                self._facts_for_manifest(
                    manifest,
                    revision=base_revision,
                    evaluation_context=evaluation_context,
                    manifest_ids=manifest_ids,
                    archived=payload.get("archived"),
                )
            )

        # Writer deduplication keys on source_revision. Include each fact's
        # complete semantic output so snapshot-bound project policy, proof
        # artifacts, and Git history changes are admitted deterministically.
        facts = [
            replace(
                fact,
                source_revision={
                    **base_revision,
                    "source_fact_sha256": _fact_hash(fact),
                },
            )
            for fact in facts
        ]

        fact_hashes = self._fact_hashes_for_facts(facts)
        next_fact_hashes = self._next_fact_hashes(
            cursor,
            current=fact_hashes,
            scope_changes=scope_changes,
        )
        emitted_facts = self._changed_facts_for_scope(
            facts,
            cursor=cursor,
            current_hashes=fact_hashes,
            scope_changes=scope_changes,
        )

        record_set = self._record_set_for_facts(facts) if completed_scope else None
        if completed_scope and scope_changes:
            record_set = self._scoped_record_set(
                cursor,
                current=record_set or RecordSet(),
                replacement_keys=scope_changes,
            )
            completed_scope = record_set is not None

        self.last_run_stats = {
            "scope_status": (
                "scoped"
                if scope_changes
                else (
                    "record_set_authority_repair"
                    if completed_scope
                    and isinstance(cursor, dict)
                    and cursor.get("manifest_hashes") == manifest_hashes
                    and not cursor_has_record_set_authority(cursor)
                    else "full"
                )
            ),
            "scope_keys": len(scope_keys),
            "recognized_changes": len(scope_changes),
            "manifest_hashes_checked": manifest_count,
            "manifests_loaded": len(manifests),
            "facts_evaluated": len(facts),
            "facts_emitted": len(emitted_facts),
            "facts_filtered_unchanged": len(facts) - len(emitted_facts),
            "decision_ledger": ledger_status,
            "supersession_targets_skipped": list(self._skipped_supersession_targets),
            "record_set_authority": (
                "repair_emitted"
                if completed_scope
                and isinstance(cursor, dict)
                and cursor.get("manifest_hashes") == manifest_hashes
                and not cursor_has_record_set_authority(cursor)
                else "rebuilt"
                if completed_scope
                else "not_required"
            ),
        }
        return SourceRun(
            facts=iter(emitted_facts),
            completed_scope=completed_scope,
            next_cursor={
                "manifest_hashes": next_manifest_hashes,
                "fact_hashes": next_fact_hashes,
            },
            applied_raw_token=applied_token,
            applied_token_scope=applied_scope,
            applied_token_authority=applied_authority,
            run_mode=mode,
            record_set=record_set,
        )

    def _manifest_path_for(self, change_id: str) -> Path | None:
        """Active changes win over archived ones if both exist."""
        for branch in ("changes", "archive"):
            path = Path(self.pact_root) / branch / change_id / "pact.yaml"
            if path.is_file():
                return path
        return None

    def _all_change_ids(self) -> list[str]:
        """Active + archived change ids; retired changes keep graph nodes."""
        root = Path(self.pact_root)
        ids = {
            path.parent.name
            for branch in ("changes", "archive")
            for path in (root / branch).glob("*/pact.yaml")
        }
        return sorted(ids)

    def _manifest_content_items(self) -> list[tuple[str, Path]]:
        return [
            (change_id, path)
            for change_id in self._all_change_ids()
            if (path := self._manifest_path_for(change_id)) is not None
        ]

    def _capture_manifests(
        self,
        *,
        scope_changes: set[str],
    ) -> dict[str, dict[str, Any]]:
        root = Path(self.pact_root)
        if not root.is_dir():
            return {}
        captured: dict[str, dict[str, Any]] = {}
        if scope_changes:
            change_ids = sorted(scope_changes)
        else:
            change_ids = self._all_change_ids()
        for change_id in change_ids:
            path = self._manifest_path_for(change_id)
            if path is None:
                continue
            payload = path.read_bytes()
            captured[change_id] = {
                "path": path,
                "bytes": payload,
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        return captured

    def _load_ledger_decision_rows(
        self,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
        """Read the decision ledger for projection.

        Unavailability is LOUD (surfaced in last_run_stats and the cursor
        watermark), never silent: manifests still project so a missing
        ledger does not take down PACT coverage, but the stats say so.

        ``OKG_PACT_DECISION_PROJECTION=off`` disables the ledger read
        entirely (test harnesses; deployments without a ledger). The
        disabled state is reported in stats, not silently absorbed.
        """
        import os

        if os.environ.get("OKG_PACT_DECISION_PROJECTION", "on") == "off":
            return {}, {
                "status": "disabled_by_env",
                "watermark": "disabled",
            }
        try:
            from okg.substrate.pact.decisions import (
                load_decision_rows,
                require_ledger_connection,
            )

            conn = require_ledger_connection()
        except Exception as exc:
            return {}, {
                "status": "unavailable",
                "reason": str(exc),
                "watermark": "ledger-unavailable",
            }
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL "
                        "REPEATABLE READ READ ONLY"
                    )
                rows = load_decision_rows(conn)
        finally:
            conn.close()
        by_change: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            by_change.setdefault(row["change_id"], []).append(row)
        max_id = rows[-1]["decision_id"] if rows else 0
        return by_change, {
            "status": "ok",
            "rows": len(rows),
            "watermark": f"{max_id}:{len(rows)}",
            "identity": _decision_rows_identity(rows),
        }

    @staticmethod
    def _next_manifest_hashes(
        cursor: Optional[dict[str, Any]],
        *,
        current: dict[str, str],
        scope_changes: set[str],
    ) -> dict[str, str]:
        if not scope_changes:
            return dict(current)
        prior = (
            dict(cursor.get("manifest_hashes") or {})
            if isinstance(cursor, dict)
            else {}
        )
        for change_id in scope_changes:
            if change_id in current:
                prior[change_id] = current[change_id]
            else:
                prior.pop(change_id, None)
        # Ledger watermarks always advance with the current run, even under
        # a scoped update, so noop detection stays ledger-aware.
        if _LEDGER_HASH_KEY in current:
            prior[_LEDGER_HASH_KEY] = current[_LEDGER_HASH_KEY]
        if _EVALUATION_HASH_KEY in current:
            prior[_EVALUATION_HASH_KEY] = current[_EVALUATION_HASH_KEY]
        return prior

    @staticmethod
    def _scoped_manifest_hashes_unchanged(
        cursor: dict[str, Any],
        *,
        current: dict[str, str],
        scope_changes: set[str],
    ) -> bool:
        prior = cursor.get("manifest_hashes")
        if not isinstance(prior, dict):
            return False
        if current.get(_LEDGER_HASH_KEY) != prior.get(_LEDGER_HASH_KEY):
            return False
        if current.get(_EVALUATION_HASH_KEY) != prior.get(
            _EVALUATION_HASH_KEY
        ):
            return False
        for change_id in scope_changes:
            if change_id not in current:
                return False
            if current.get(change_id) != prior.get(change_id):
                return False
        return True

    @staticmethod
    def _scope_keys(sync_scope: Optional[dict[str, Any]]) -> list[str]:
        return [
            str(raw_key).replace("\\", "/").strip("/")
            for raw_key in (sync_scope or {}).get("scope_keys", ())
            if str(raw_key).strip()
        ]

    @staticmethod
    def _scope_changes_from_keys(scope_keys: list[str]) -> set[str]:
        changes: set[str] = set()
        for key in scope_keys:
            parts = key.split("/")
            if (
                len(parts) >= 3
                and parts[0] == "pact"
                and parts[1] in {"changes", "archive"}
            ):
                changes.add(parts[2])
            elif len(parts) >= 2 and parts[0] in {"changes", "archive"}:
                changes.add(parts[1])
        return changes

    @staticmethod
    def _record_set_for_facts(
        facts: list[NodeFact | EdgeFact],
    ) -> RecordSet:
        parts: dict[str, dict[str, Any]] = {}
        for fact in facts:
            record_id = _record_key_for_fact(fact)
            if not record_id:
                continue
            entry = parts.setdefault(
                record_id,
                {"primary": None, "descendants": [], "edges": []},
            )
            if isinstance(fact, NodeFact):
                node = RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                )
                if fact.node_id == _change_node(record_id):
                    entry["primary"] = node
                elif fact.node_id == "cap:pact":
                    entry["primary"] = node
                else:
                    entry["descendants"].append(node)
            else:
                entry["edges"].append(
                    EdgeKey(
                        src=fact.src,
                        dst=fact.dst,
                        edge_type=fact.edge_type,
                        attrs=dict(fact.attrs),
                        provenance=fact.provenance,
                        confidence=fact.confidence,
                        source_record_id=dict(fact.source_record_id),
                        source_revision=dict(fact.source_revision),
                    )
                )
        records = {
            key: RecordEmission(
                primary=value["primary"],
                descendants=value["descendants"],
                edges=value["edges"],
            )
            for key, value in parts.items()
        }
        return RecordSet(records=records)

    @staticmethod
    def _scoped_record_set(
        cursor: Optional[dict[str, Any]],
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> Optional[RecordSet]:
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        prior = RecordSet.from_cursor(prior_raw)
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    @staticmethod
    def _fact_hashes_for_facts(
        facts: list[NodeFact | EdgeFact],
    ) -> dict[str, str]:
        out: dict[str, str] = {}
        for fact in facts:
            key = _fact_key(fact)
            if not key:
                continue
            out[key] = _fact_hash(fact)
        return out

    @staticmethod
    def _next_fact_hashes(
        cursor: Optional[dict[str, Any]],
        *,
        current: dict[str, str],
        scope_changes: set[str],
    ) -> dict[str, str]:
        if not scope_changes:
            return dict(current)
        prior = (
            dict(cursor.get("fact_hashes") or {}) if isinstance(cursor, dict) else {}
        )
        prefixes = tuple(f"{change_id}:" for change_id in scope_changes)
        merged = {
            key: value for key, value in prior.items() if not key.startswith(prefixes)
        }
        merged.update(current)
        return merged

    @staticmethod
    def _changed_facts_for_scope(
        facts: list[NodeFact | EdgeFact],
        *,
        cursor: Optional[dict[str, Any]],
        current_hashes: dict[str, str],
        scope_changes: set[str],
    ) -> list[NodeFact | EdgeFact]:
        if not scope_changes or not isinstance(cursor, dict):
            return list(facts)
        prior_hashes = cursor.get("fact_hashes")
        if not isinstance(prior_hashes, dict):
            return list(facts)
        changed: list[NodeFact | EdgeFact] = []
        for fact in facts:
            key = _fact_key(fact)
            if not key:
                changed.append(fact)
                continue
            current_hash = current_hashes.get(key)
            if current_hash != prior_hashes.get(key):
                changed.append(fact)
        return changed

    def _derived_state_attrs(
        self,
        manifest: PactManifest,
        *,
        evaluation_context: Any = None,
    ) -> dict[str, Any]:
        """v4 ledger-derived lifecycle state + decision rollup.

        operator-surface-list-work req.listwork.honest-pact-projection:
        authored task status said 'todo' on ACTIVE v4 changes with shipped
        ledger evidence, and said 'done' on checkbox-closed changes with
        zero evidence - graph triage was structurally wrong. Derivation
        failures are LOUD attrs, never silent omissions.
        """
        rows = self._decision_rows_by_change.get(manifest.change.id, ())
        rollup: dict[str, Any] = {"decision_count": len(rows)}
        if rows:
            last = rows[-1]
            rollup.update(
                {
                    "last_verdict": last.get("verdict"),
                    "last_principal": last.get("principal"),
                    "last_decided_at": last.get("decided_at"),
                }
            )
        try:
            from okg.substrate.pact.fsm import DecisionEvent
            from okg.substrate.pact.review import derive_change_state

            events = [
                DecisionEvent(
                    change_id=r["change_id"],
                    verdict=r["verdict"],
                    principal=r["principal"],
                    authority=r.get("authority") or "operator",
                    note=r.get("note"),
                    target=r.get("target"),
                    from_state=r.get("from_state"),
                    to_state=r.get("to_state"),
                    intent_digest=r.get("intent_digest"),
                    decided_at=r.get("decided_at"),
                )
                for r in rows
            ]
            derived = derive_change_state(
                manifest,
                events,
                repo_root=Path(self.pact_root).parent,
                context=evaluation_context,
            )
            rollup["derived_state"] = derived.state
        except Exception as exc:  # noqa: BLE001 - loud, not silent.
            rollup["derived_state"] = "underivable"
            rollup["derived_state_error"] = str(exc)[:200]
        return rollup

    def _supersession_targets(
        self,
        manifest: PactManifest,
        *,
        manifest_ids: frozenset[str] | None = None,
    ) -> list[tuple[str, str]]:
        """(target_change_id, basis) pairs for change-level supersession."""
        out: list[tuple[str, str]] = []
        seen: set[str] = set()

        def admit(target: str, basis: str) -> None:
            if target in seen or target == manifest.change.id:
                return
            seen.add(target)
            # A target with no manifest (active or archived) would project
            # a dangling edge; the producer signature check rejects those
            # at admission and fails the whole run. Skip it here and say
            # so in last_run_stats — prose text-mining can match tokens
            # that are not change ids ("the X half of Y").
            target_exists = (
                target in manifest_ids
                if manifest_ids is not None
                else self._manifest_path_for(target) is not None
            )
            if not target_exists:
                self._skipped_supersession_targets.append(
                    {
                        "change": manifest.change.id,
                        "target": target,
                        "basis": basis,
                    }
                )
                return
            out.append((target, basis))

        for row in self._decision_rows_by_change.get(manifest.change.id, ()):
            if row.get("verdict") == "supersede" and row.get("target"):
                admit(str(row["target"]), "ledger_verdict")
        prose = " ".join(
            text
            for text in (
                manifest.change.rationale,
                manifest.change.root_cause,
            )
            if text
        )
        for regex, basis in (
            (_SUPERSEDED_BY_RE, "prose_superseded_by"),
            (_SUPERSEDES_RE, "prose_supersedes"),
        ):
            for match in regex.finditer(prose):
                admit(match.group(1).rstrip("-"), basis)
        return out

    def _facts_for_manifest(
        self,
        manifest: PactManifest,
        *,
        revision: dict[str, Any],
        evaluation_context: Any = None,
        manifest_ids: frozenset[str] | None = None,
        archived: bool | None = None,
    ) -> Iterator[Any]:
        repo_root = self.pact_root.parent if self.pact_root.name == "pact" else None
        redaction_home = (
            evaluation_context.redaction_home
            if evaluation_context is not None
            else None
        )

        def redact_captured_text(value: str | None) -> str | None:
            return self._redact_text(value, home_dir=redaction_home)

        def redact_captured_value(value: Any) -> Any:
            return self._redact_value(value, home_dir=redaction_home)

        alignment = derive_alignment(
            manifest,
            repo_root=repo_root,
            context=evaluation_context,
        )
        change_node = _change_node(manifest.change.id)
        delta_node = f"pact-delta:{manifest.change.id}:requirements"
        task_group_node = f"pact-taskgroup:{manifest.change.id}:tasks"
        record = {"repo": self.repo, "change_id": manifest.change.id}
        lifecycle = alignment.get("lifecycle", {})
        evidence_gaps = [
            {"id": req_id, "missing": state.get("satisfaction_missing", [])}
            for req_id, state in alignment.get("requirements", {}).items()
            if state.get("satisfaction_status") == "unsatisfied"
        ]
        environment_blockers = [
            {"task": task_id, **blocker}
            for task_id, state in alignment.get("tasks", {}).items()
            for blocker in state.get("environment_blockers", [])
        ]
        next_task_ids = [
            task.id
            for task in manifest.tasks
            if task.status in {"todo", "blocked"}
            and not alignment["tasks"].get(task.id, {}).get("blockers")
        ]

        yield NodeFact(
            node_id=change_node,
            subtype="pact_change",
            attrs={
                "change_id": manifest.change.id,
                "title": redact_captured_text(manifest.change.title),
                "status": manifest.change.status,
                "why_text": redact_captured_text(
                    manifest.change.root_cause or ""
                ),
                "what_changes_text": redact_captured_text(
                    manifest.change.rationale or ""
                ),
                "impact_text": redact_captured_text(
                    manifest.change.migration_notes or ""
                ),
                "text": redact_captured_text(
                    manifest.change.rationale or manifest.change.title
                ),
                "source_provider": "pact",
                "artifact_format_version": manifest.pact_version,
                "target_pact_version": alignment["versions"]["target_language_version"],
                "hierarchy_role": (
                    manifest.hierarchy.role if manifest.hierarchy else "standalone"
                ),
                "parent_change": (
                    manifest.hierarchy.parent if manifest.hierarchy else None
                ),
                "child_change_ids": (
                    manifest.hierarchy.children if manifest.hierarchy else []
                ),
                "intent_digest": intent_digest(manifest),
                "state_digest": state_digest(manifest),
                "proposal_gate": lifecycle.get("proposal", False),
                "implementation_gate": lifecycle.get("implementation", False),
                "verification_gate": lifecycle.get("verification", False),
                "evidence_count": len(manifest.evidence),
                "evidence_gaps": evidence_gaps,
                "environment_blockers": environment_blockers,
                "labels": manifest.change.labels,
                "method_pack": manifest.change.method_pack,
                "method_metadata": manifest.change.method_metadata,
                "blocked_task_count": sum(
                    1
                    for state in alignment.get("tasks", {}).values()
                    if state.get("blockers")
                ),
                "next_task_ids": next_task_ids,
                "archived": (
                    archived
                    if archived is not None
                    else (
                        (
                            path := self._manifest_path_for(
                                manifest.change.id
                            )
                        )
                        is not None
                        and path.parent.parent.name == "archive"
                    )
                ),
                **self._derived_state_attrs(
                    manifest,
                    evaluation_context=evaluation_context,
                ),
            },
            source_record_id={**record, "node_id": change_node},
            source_revision=revision,
        )
        for target_id, basis in self._supersession_targets(
            manifest,
            manifest_ids=manifest_ids,
        ):
            # Direction: successor --supersedes--> predecessor. For a
            # ledger supersede verdict and a prose "superseded by T",
            # T is the successor; for prose "this supersedes Y" the
            # declaring change is. _supersession_targets only returns
            # targets with a manifest on disk — a dangling target would
            # fail producer-signature admission for the whole run.
            target_node = f"pact-change:{target_id}"
            if basis == "prose_supersedes":
                src_node, dst_node = change_node, target_node
            else:
                src_node, dst_node = target_node, change_node
            yield _edge(
                src_node,
                dst_node,
                "supersedes",
                record,
                revision,
                attrs={"basis": basis},
            )
        for row in self._decision_rows_by_change.get(manifest.change.id, ()):
            decision_node = f"pact-decision:{manifest.change.id}:{row['decision_id']}"
            note = row.get("note") or ""
            redacted_note = redact_captured_text(note)
            yield NodeFact(
                node_id=decision_node,
                subtype="pact_decision",
                attrs={
                    "decision_id": row["decision_id"],
                    "change_id": manifest.change.id,
                    "verdict": row["verdict"],
                    "principal": row["principal"],
                    "authority": row.get("authority") or "operator",
                    "note": redacted_note,
                    "supersede_target": row.get("target"),
                    "from_state": row.get("from_state"),
                    "to_state": row.get("to_state"),
                    "intent_digest": row.get("intent_digest"),
                    "decided_at": row.get("decided_at"),
                    "text": (
                        f"{row['verdict']} {manifest.change.id} "
                        f"by {row['principal']}: {redacted_note}".strip().rstrip(":")
                    ),
                    "source_provider": "pact",
                },
                source_record_id={**record, "node_id": decision_node},
                source_revision=revision,
            )
            yield _edge(decision_node, change_node, "targets", record, revision)
        yield NodeFact(
            node_id=delta_node,
            subtype="spec_delta",
            attrs={
                "delta_id": delta_node,
                "change_id": manifest.change.id,
                "capability_name": "pact",
                "delta_kind": "modified",
                "source_provider": "pact",
            },
            source_record_id={**record, "node_id": delta_node},
            source_revision=revision,
        )
        yield NodeFact(
            node_id=task_group_node,
            subtype="task_group",
            attrs={
                "group_id": task_group_node,
                "change_id": manifest.change.id,
                "group_index": 1,
                "title": "PACT Tasks",
                "source_provider": "pact",
            },
            source_record_id={**record, "node_id": task_group_node},
            source_revision=revision,
        )
        yield _edge(change_node, delta_node, "contains", record, revision)
        yield _edge(delta_node, "cap:pact", "targets", record, revision)
        yield _edge(change_node, task_group_node, "contains", record, revision)

        requirement_ids = {req.id for req in manifest.requirements}
        acceptance_to_requirement = {
            acceptance.id: req.id
            for req in manifest.requirements
            for acceptance in req.acceptance
        }

        for req in manifest.requirements:
            req_node = _req_node(manifest.change.id, req.id)
            req_state = alignment["requirements"].get(req.id, {})
            yield NodeFact(
                node_id=req_node,
                subtype="acceptance_criterion",
                attrs={
                    "criterion_id": req.id,
                    "capability_name": "pact",
                    "title": redact_captured_text(req.title or req.id),
                    "text": redact_captured_text(req.statement),
                    "scenario_count": len(req.scenarios),
                    "source_provider": "pact",
                    "traceability_status": req_state.get("traceability_status"),
                    "satisfaction_status": req_state.get("satisfaction_status"),
                },
                source_record_id={**record, "node_id": req_node},
                source_revision=revision,
            )
            yield _edge(delta_node, req_node, "contains", record, revision)
            for index, scenario in enumerate(req.scenarios):
                scenario_node = f"pact-scenario:{manifest.change.id}:{scenario.id}"
                yield NodeFact(
                    node_id=scenario_node,
                    subtype="acceptance_scenario",
                    attrs={
                        "scenario_id": scenario.id,
                        "criterion_id": req.id,
                        "index": index,
                        "title": redact_captured_text(scenario.title),
                        "text": redact_captured_text(
                            " ".join([*scenario.given, *scenario.when, *scenario.then])
                        ),
                        "source_provider": "pact",
                    },
                    source_record_id={**record, "node_id": scenario_node},
                    source_revision=revision,
                )
                yield _edge(req_node, scenario_node, "contains", record, revision)

        for index, task in enumerate(manifest.tasks, start=1):
            task_node = _task_node(manifest.change.id, task.id)
            task_state = alignment["tasks"].get(task.id, {})
            task_method_metadata = {
                **manifest.change.method_metadata,
                **task.method_metadata,
            }
            yield NodeFact(
                node_id=task_node,
                subtype="pact_task",
                attrs={
                    "task_id": task.id,
                    "change_id": manifest.change.id,
                    "group_index": 1,
                    "task_number": str(index),
                    "description": redact_captured_text(task.title),
                    "status": task.status,
                    "source_provider": "pact",
                    "labels": task.labels,
                    "method_pack": task.method_pack or manifest.change.method_pack,
                    "method_metadata": task_method_metadata,
                    "alignment_status": task_state.get("status"),
                    "traceability_status": task_state.get("traceability_status"),
                    "satisfaction_status": task_state.get("satisfaction_status"),
                    "blockers": task_state.get("blockers", []),
                    "dependency_blockers": task_state.get("dependency_blockers", []),
                    "environment_blockers": task_state.get("environment_blockers", []),
                    "evidence_issues": task_state.get("evidence_issues", []),
                },
                source_record_id={**record, "node_id": task_node},
                source_revision=revision,
            )
            verification_node = f"pact-verification:{manifest.change.id}:{task.id}"
            yield NodeFact(
                node_id=verification_node,
                subtype="verification_step",
                attrs={
                    "step_id": verification_node,
                    "task_id": task.id,
                    "text": redact_captured_text(task.verification),
                    "source_provider": "pact",
                },
                source_record_id={**record, "node_id": verification_node},
                source_revision=revision,
            )
            yield _edge(task_group_node, task_node, "contains", record, revision)
            yield _edge(task_node, verification_node, "contains", record, revision)
            for check in task.environment:
                blocker_node = (
                    f"pact-verification:{manifest.change.id}:env:{task.id}:{check.id}"
                )
                yield NodeFact(
                    node_id=blocker_node,
                    subtype="verification_step",
                    attrs={
                        "step_id": blocker_node,
                        "task_id": task.id,
                        "text": redact_captured_text(
                            check.description
                            or f"requires {check.env or check.name or check.id}"
                        ),
                        "source_provider": "pact",
                        "step_kind": "environment_check",
                        "check_id": check.id,
                        "check_kind": check.kind,
                        "env": check.env,
                        "required": check.required,
                        "blocked": any(
                            blocker.get("id") == check.id
                            for blocker in task_state.get("environment_blockers", [])
                        ),
                    },
                    source_record_id={**record, "node_id": blocker_node},
                    source_revision=revision,
                )
                yield _edge(task_node, blocker_node, "contains", record, revision)

        task_ids = {task.id for task in manifest.tasks}
        for dep in manifest.dependencies:
            if dep.from_task not in task_ids or dep.to_task not in task_ids:
                continue
            yield _edge(
                _task_node(manifest.change.id, dep.from_task),
                _task_node(manifest.change.id, dep.to_task),
                dep.kind,
                record,
                revision,
                attrs={
                    "dependency_kind": dep.kind,
                    "from_task": dep.from_task,
                    "to_task": dep.to_task,
                    "rationale": dep.rationale,
                    "waiver": dep.waiver,
                },
            )

        task_ids_by_requirement: dict[str, list[str]] = {}
        for task in manifest.tasks:
            for requirement in task.requirements:
                task_ids_by_requirement.setdefault(requirement, []).append(task.id)

        test_requirement_by_ref: dict[str, str] = {}
        for test in manifest.test_plan:
            test_node = f"pact-verification:{manifest.change.id}:test:{test.id}"
            test_state = alignment["test_plan"].get(test.id, {})
            requirement_id = acceptance_to_requirement.get(
                test.requirement,
                test.requirement,
            )
            if requirement_id in requirement_ids:
                test_requirement_by_ref[test.id] = requirement_id
                test_requirement_by_ref[test.test_id] = requirement_id
            yield NodeFact(
                node_id=test_node,
                subtype="verification_step",
                attrs={
                    "step_id": test_node,
                    "task_id": "",
                    "text": redact_captured_text(test.command or test.test_id),
                    "source_provider": "pact",
                    "step_kind": "planned_test",
                    "test_plan_id": test.id,
                    "requirement": test.requirement,
                    "test_id": test.test_id,
                    "traceability_status": test_state.get("traceability_status"),
                    "satisfaction_status": test_state.get("satisfaction_status"),
                },
                source_record_id={**record, "node_id": test_node},
                source_revision=revision,
            )
            if requirement_id in requirement_ids:
                yield _edge(
                    _req_node(manifest.change.id, requirement_id),
                    test_node,
                    "contains",
                    record,
                    revision,
                )
            task_ids_for_test = _dedupe(
                [
                    *task_ids_by_requirement.get(test.requirement, []),
                    *task_ids_by_requirement.get(requirement_id, []),
                ]
            )
            for task_id in task_ids_for_test:
                yield _edge(
                    _task_node(manifest.change.id, task_id),
                    test_node,
                    "contains",
                    record,
                    revision,
                )

        formal_requirement_by_id: dict[str, str] = {}
        for formal in manifest.formal:
            formal_node = f"pact-verification:{manifest.change.id}:formal:{formal.id}"
            formal_requirement_by_id[formal.id] = formal.requirement
            yield NodeFact(
                node_id=formal_node,
                subtype="verification_step",
                attrs={
                    "step_id": formal_node,
                    "task_id": "",
                    "text": redact_captured_text(
                        formal.checker_command or formal.module
                    ),
                    "source_provider": "pact",
                    "step_kind": "formal_binding",
                    "formal_id": formal.id,
                    "requirement": formal.requirement,
                    "property_id": formal.property_id,
                    "module": formal.module,
                    "config": formal.config,
                    "checker_command": redact_captured_text(
                        formal.checker_command
                    ),
                    "refinement_tests": [
                        redact_captured_text(test)
                        for test in formal.refinement_tests
                    ],
                },
                source_record_id={**record, "node_id": formal_node},
                source_revision=revision,
            )
            if formal.requirement in requirement_ids:
                yield _edge(
                    _req_node(manifest.change.id, formal.requirement),
                    formal_node,
                    "contains",
                    record,
                    revision,
                )

        for evidence in manifest.evidence:
            evidence_node = (
                f"pact-verification:{manifest.change.id}:evidence:{evidence.id}"
            )
            yield NodeFact(
                node_id=evidence_node,
                subtype="verification_step",
                attrs={
                    "step_id": evidence_node,
                    "task_id": "",
                    "text": redact_captured_text(
                        evidence.summary or evidence.command or evidence.id
                    ),
                    "source_provider": "pact",
                    "step_kind": "evidence",
                    "evidence_id": evidence.id,
                    "evidence_kind": evidence.kind,
                    "evidence_status": evidence.status,
                    "refs": redact_captured_value(evidence.refs),
                    "artifact": redact_captured_text(evidence.artifact),
                    "command": redact_captured_text(evidence.command),
                    "generation_id": evidence.generation_id,
                },
                source_record_id={**record, "node_id": evidence_node},
                source_revision=revision,
            )
            evidence_requirement_refs = _evidence_requirement_refs(
                evidence.refs,
                requirement_ids=requirement_ids,
                acceptance_to_requirement=acceptance_to_requirement,
                test_requirement_by_ref=test_requirement_by_ref,
                formal_requirement_by_id=formal_requirement_by_id,
            )
            for req_id in evidence_requirement_refs:
                yield _edge(
                    _req_node(manifest.change.id, req_id),
                    evidence_node,
                    "contains",
                    record,
                    revision,
                )
            for task_id in set(evidence.refs) & {task.id for task in manifest.tasks}:
                yield _edge(
                    _task_node(manifest.change.id, task_id),
                    evidence_node,
                    "contains",
                    record,
                    revision,
                )

        waiver_edges: set[tuple[str, str]] = set()
        for waiver in manifest.waivers:
            waiver_node = f"pact-verification:{manifest.change.id}:waiver:{waiver.id}"
            yield NodeFact(
                node_id=waiver_node,
                subtype="verification_step",
                attrs={
                    "step_id": waiver_node,
                    "task_id": "",
                    "text": redact_captured_text(waiver.reason),
                    "source_provider": "pact",
                    "step_kind": "waiver",
                    "waiver_id": waiver.id,
                    "scope": waiver.scope,
                    "reason": redact_captured_text(waiver.reason),
                    "requires_owner_approval": waiver.requires_owner_approval,
                    "owner": waiver.owner,
                    "expires": waiver.expires,
                    "accepted_risk": waiver.accepted_risk,
                },
                source_record_id={**record, "node_id": waiver_node},
                source_revision=revision,
            )
            scoped_requirement = acceptance_to_requirement.get(
                waiver.scope,
                waiver.scope,
            )
            if scoped_requirement in requirement_ids:
                waiver_edges.add(
                    (
                        _req_node(manifest.change.id, scoped_requirement),
                        waiver_node,
                    )
                )
            elif waiver.scope in task_ids:
                waiver_edges.add(
                    (
                        _task_node(manifest.change.id, waiver.scope),
                        waiver_node,
                    )
                )
            for dep in manifest.dependencies:
                if dep.waiver != waiver.id:
                    continue
                for task_id in (dep.from_task, dep.to_task):
                    if task_id in task_ids:
                        waiver_edges.add(
                            (
                                _task_node(manifest.change.id, task_id),
                                waiver_node,
                            )
                        )
        for src, dst in sorted(waiver_edges):
            yield _edge(src, dst, "contains", record, revision)


@dataclass
class PACTCoordinationEventSource:
    """Append-feed projection for PACT coordination ledger events."""

    repo: str

    name: str = "pact-coordination-events"
    profile: str = "append_feed"
    probe_short_circuit_safe = True
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    # Incremental-source contract (issue #323): this source reads ONLY the PACT
    # coordination-event ledger (a mutable DB), so its probe hashes every
    # projected column in one repeatable-read snapshot. Inserts, updates, and
    # deletes bust the token; an unavailable/erroring ledger raises ->
    # run_probe forces a full read (never a silent no-change skip past an event
    # the ledger could not be checked for).
    change_probe_kind = "mutable_api"

    def __post_init__(self) -> None:
        import sys

        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        self.change_probe = _LedgerWatermarkProbe(
            self.repo,
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
            ),
            watermark_kind="coordination_events",
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "append",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        rows, ledger_status = self._load_ledger_coordination_event_rows()
        full_replay = (
            authoritative_replay
            and not self._scope_keys(sync_scope)
            and ledger_status["status"] == "ok"
        )
        last_seen_event_id = (
            0 if full_replay else _cursor_max_event_id(cursor)
        )
        candidate_rows = [
            row for row in rows if _row_event_id(row) > last_seen_event_id
        ]
        facts = [
            self._fact_for_row(row)
            for row in candidate_rows
        ]
        max_event_id = max(
            [last_seen_event_id, *(_row_event_id(row) for row in rows)],
            default=last_seen_event_id,
        )
        next_cursor = {
            "max_event_id": max_event_id,
            "row_count": len(rows),
            "ledger_watermark": ledger_status["watermark"],
        }
        self.last_run_stats = {
            "ledger": ledger_status,
            "run_mode": mode,
            "scope_keys_ignored": len(self._scope_keys(sync_scope)),
            "last_seen_event_id": last_seen_event_id,
            "max_event_id": max_event_id,
            "rows_seen": len(rows),
            "rows_emitted": len(candidate_rows),
            "facts_emitted": len(facts),
            "authoritative_replay": full_replay,
        }
        record_set = (
            RecordSet(
                records={
                    str(fact.source_record_id["event_id"]): RecordEmission(
                        primary=RecordNode(
                            node_id=fact.node_id,
                            subtype=fact.subtype,
                            source_record_id=dict(fact.source_record_id),
                            source_revision=dict(fact.source_revision),
                        )
                    )
                    for fact in facts
                }
            )
            if full_replay else None
        )
        return SourceRun(
            facts=iter(facts),
            completed_scope=full_replay,
            next_cursor=next_cursor,
            applied_raw_token=(
                self.change_probe.token_for_value(ledger_status["identity"])
                if ledger_status["status"] in {"ok", "disabled_by_env"}
                else None
            ),
            applied_token_scope=(
                "global"
                if ledger_status["status"] in {"ok", "disabled_by_env"}
                else "unavailable"
            ),
            applied_token_authority=(
                "exact"
                if ledger_status["status"] in {"ok", "disabled_by_env"}
                else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
        )

    def _load_ledger_coordination_event_rows(
        self,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Read PACT-emitted coordination events for graph projection."""
        import os

        if os.environ.get("OKG_PACT_DECISION_PROJECTION", "on") == "off":
            return [], {
                "status": "disabled_by_env",
                "watermark": "disabled",
                "identity": "disabled",
            }
        try:
            from okg.substrate.pact.decisions import (
                load_coordination_event_snapshot,
                require_ledger_connection,
            )

            conn = require_ledger_connection()
        except Exception as exc:
            return [], {
                "status": "unavailable",
                "reason": str(exc),
                "watermark": "ledger-unavailable",
            }
        try:
            rows, snapshot = load_coordination_event_snapshot(conn)
        finally:
            conn.close()
        identity = str(snapshot["identity"])
        return list(rows), {
            "status": "ok",
            "rows": int(snapshot["count"]),
            "watermark": identity,
            "identity": identity,
        }

    def _fact_for_row(
        self,
        row: dict[str, Any],
    ) -> NodeFact:
        change_id = str(row["change_id"])
        event_node = _coordination_event_node_id(change_id, row)
        return NodeFact(
            node_id=event_node,
            subtype="coordination_event",
            attrs=_coordination_event_attrs(
                change_id=change_id,
                row=row,
                event_node=event_node,
                manifest_present=False,
            ),
            source_record_id={
                "repo": self.repo,
                "event_id": str(row["event_id"]),
                "change_id": change_id,
                "node_id": event_node,
            },
            source_revision={
                "sequence_number": _row_event_id(row),
                "event_sha256": hashlib.sha256(
                    json.dumps(
                        row,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                ).hexdigest(),
            },
        )

    @staticmethod
    def _scope_keys(sync_scope: Optional[dict[str, Any]]) -> list[str]:
        return [
            str(raw_key)
            for raw_key in (sync_scope or {}).get("scope_keys", ())
            if str(raw_key).strip()
        ]


def _coordination_event_node_id(change_id: str, row: dict[str, Any]) -> str:
    return f"pact-coordination-event:{change_id}:{row['event_id']}"


def _coordination_event_attrs(
    *,
    change_id: str,
    row: dict[str, Any],
    event_node: str,
    manifest_present: bool | None = None,
) -> dict[str, Any]:
    task_id = row.get("task_id") or ""
    principal = row.get("principal") or "unknown"
    session_id = row.get("session_id") or "unknown"
    attrs = {
        "event_id": event_node,
        "event_type": row.get("event_type"),
        "event_family": "pact",
        "change_id": change_id,
        "task_id": task_id,
        "event_target": f"{change_id}:{task_id}" if task_id else change_id,
        "actor_id": principal,
        "actor_session_id": session_id,
        "workspace_id": row.get("worktree_suffix"),
        "idempotency_key": f"pact_coordination_events:{row['event_id']}",
        "status": "recorded",
        "observed_at": row.get("occurred_at"),
        "payload": row.get("payload") or {},
        "branch": row.get("branch"),
        "head_sha": row.get("head_sha"),
        "intent_digest": row.get("intent_digest"),
        "text": (
            f"{row.get('event_type')} recorded for "
            f"{change_id}" + (f":{task_id}" if task_id else "")
        ),
        "source_provider": "pact",
    }
    if manifest_present is not None:
        attrs["manifest_present"] = manifest_present
    return attrs


def _cursor_max_event_id(cursor: Optional[dict[str, Any]]) -> int:
    if not isinstance(cursor, dict):
        return 0
    try:
        return int(cursor.get("max_event_id") or 0)
    except (TypeError, ValueError):
        return 0


def _row_event_id(row: dict[str, Any]) -> int:
    return int(row["event_id"])


def _change_node(change_id: str) -> str:
    return f"pact-change:{change_id}"


def _req_node(change_id: str, req_id: str) -> str:
    return f"pact-req:{change_id}:{req_id}"


def _task_node(change_id: str, task_id: str) -> str:
    return f"pact-task:{change_id}:{task_id}"


def _record_key_for_fact(fact: NodeFact | EdgeFact) -> str:
    change_id = fact.source_record_id.get("change_id")
    if isinstance(change_id, str) and change_id:
        return change_id
    if isinstance(fact, NodeFact) and fact.node_id == "cap:pact":
        return "__cap__:pact"
    return ""


def _fact_key(fact: NodeFact | EdgeFact) -> str:
    change_id = fact.source_record_id.get("change_id")
    if isinstance(fact, NodeFact) and fact.node_id == "cap:pact":
        return "__cap__:pact:node:cap:pact"
    if not isinstance(change_id, str) or not change_id:
        return ""
    if isinstance(fact, NodeFact):
        return f"{change_id}:node:{fact.node_id}"
    return f"{change_id}:edge:{fact.src}|{fact.edge_type}|{fact.dst}"


def _fact_hash(fact: NodeFact | EdgeFact) -> str:
    if isinstance(fact, NodeFact):
        payload = {
            "kind": "node",
            "node_id": fact.node_id,
            "subtype": fact.subtype,
            "attrs": fact.attrs,
            "op": fact.op,
        }
    else:
        payload = {
            "kind": "edge",
            "src": fact.src,
            "dst": fact.dst,
            "edge_type": fact.edge_type,
            "attrs": fact.attrs,
            "provenance": fact.provenance,
            "confidence": fact.confidence,
            "op": fact.op,
        }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _evidence_requirement_refs(
    refs: list[str],
    *,
    requirement_ids: set[str],
    acceptance_to_requirement: dict[str, str],
    test_requirement_by_ref: dict[str, str],
    formal_requirement_by_id: dict[str, str],
) -> list[str]:
    requirement_refs: list[str] = []
    for ref in refs:
        if ref in requirement_ids:
            requirement_refs.append(ref)
        if ref in acceptance_to_requirement:
            requirement_refs.append(acceptance_to_requirement[ref])
        if ref in test_requirement_by_ref:
            requirement_refs.append(test_requirement_by_ref[ref])
        if ref in formal_requirement_by_id:
            requirement_refs.append(formal_requirement_by_id[ref])
    return _dedupe(requirement_refs)


def _edge(
    src: str,
    dst: str,
    edge_type: str,
    record: dict[str, str],
    revision: dict[str, Any],
    *,
    attrs: dict[str, Any] | None = None,
) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        attrs=attrs or {},
        source_record_id={
            **record,
            "src": src,
            "dst": dst,
            "edge_type": edge_type,
        },
        source_revision=revision,
    )
