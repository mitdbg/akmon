"""ExternalRepoFactory — multi-repo source factory.

Reads an operator-curated YAML config listing N external git
repositories, emits one `ExternalRepo` `NodeFact` per declared
repo, and spawns per-repo instances of the substrate's
`GitFilesSource`, `GitHistorySource`, and `CodeStructureSource`
scoped to each repo's `local_path`. Adds
`(SourceFile, member_of, ExternalRepo)` edge facts linking
every emitted source file to its parent repo.

The factory is a single substrate-library source class: a
deployment registers one `source_registry.yaml` entry pointing
at `ExternalRepoFactory`; the factory enumerates the operator
config and runs sub-sources internally per publish cycle.

Per `add-substrate-external-repos-module`:
- **D1**: single registered source, per-publish enumeration of
  declared repos.
- **D2**: per-repo `exclude_globs` / `include_exts` *replace*
  factory defaults (not union).
- **D3**: retraction via the substrate's standard bitemporal
  succession — removing a repo from the YAML drops its
  `ExternalRepo` + descendant facts on the next publish.
- **D4**: cross-repo references admitted by the existing
  `code_graph` `(CodeReference, references, CodeSymbol)`
  narrowing; no new narrowing needed.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Iterable, Iterator, Mapping, Optional, Sequence

import yaml
from okg.substrate import fastyaml

from okg.substrate.errors import SchemaValidationFailed
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    SOURCE_RUN_PARTIAL_STATUS,
    RecordSet,
    SourceHealth,
    SourceRun,
    acquire_outcome_health,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    tokens_equal,
)

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
_DEFAULT_TRACKED_BRANCH = "main"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepoEntry:
    """One parsed entry from the operator's external_repos YAML."""

    name: str
    url: str
    local_path: Path
    tracked_branch: str
    description: Optional[str] = None
    exclude_globs: Optional[tuple[str, ...]] = None
    include_exts: Optional[tuple[str, ...]] = None
    # code-repos-auto-acquire: opt-in per entry. When true, the missing-checkout
    # load-time hard-fail is skipped and the repo is materialised at ingest by
    # ExternalRepoFactory.ensure_checkout() (which the runner calls before the
    # change-probe). Defaults false -> existing require-a-clone behaviour.
    manage_checkout: bool = False

    def node_id(self) -> str:
        return f"external_repo:{self.name}"

    def to_attrs(self) -> dict[str, Any]:
        attrs: dict[str, Any] = {
            "label": self.name,
            "name": self.name,
            "url": self.url,
            "local_path": str(self.local_path),
            "tracked_branch": self.tracked_branch,
        }
        if self.description is not None:
            attrs["description"] = self.description
            attrs["text"] = f"{self.name}: {self.description}"
        else:
            attrs["text"] = self.name
        return attrs


@dataclass
class ExternalRepoConfig:
    """Parsed + validated operator external_repos config."""

    repos: list[RepoEntry] = field(default_factory=list)

    @classmethod
    def from_yaml(
        cls,
        config_path: Path | str,
        *,
        default_exclude_globs: Optional[Sequence[str]] = None,
        default_include_exts: Optional[Sequence[str]] = None,
        require_local_path: bool = True,
    ) -> "ExternalRepoConfig":
        """Load + validate an operator config YAML.

        Raises `SchemaValidationFailed` (substrate-typed error)
        on any malformed entry. No silent fallback.
        """
        cfg_path = Path(config_path).expanduser()
        if not cfg_path.is_file():
            raise SchemaValidationFailed(
                f"external_repos config not found: {cfg_path}",
                data={"config_path": str(cfg_path)},
                fix_hint=(
                    "create the YAML at this path with a top-level "
                    "`repos:` list, or update the factory's "
                    "`repos_config_path` param"
                ),
            )

        try:
            raw = fastyaml.safe_load(cfg_path.read_text()) or {}
        except yaml.YAMLError as exc:
            raise SchemaValidationFailed(
                f"external_repos config is not valid YAML: {cfg_path}: {exc}",
                data={"config_path": str(cfg_path), "yaml_error": str(exc)},
                fix_hint="fix the YAML syntax",
            ) from exc

        if not isinstance(raw, dict):
            raise SchemaValidationFailed(
                f"external_repos config root must be a mapping: {cfg_path}",
                data={"config_path": str(cfg_path), "got_type": type(raw).__name__},
                fix_hint="top-level YAML must be a mapping with key `repos:`",
            )

        repos_raw = raw.get("repos")
        if repos_raw is None:
            raise SchemaValidationFailed(
                f"external_repos config missing `repos:` key: {cfg_path}",
                data={"config_path": str(cfg_path)},
                fix_hint="add a `repos:` list at the top of the file",
            )
        if not isinstance(repos_raw, list):
            raise SchemaValidationFailed(
                f"external_repos `repos:` must be a list: {cfg_path}",
                data={
                    "config_path": str(cfg_path),
                    "got_type": type(repos_raw).__name__,
                },
                fix_hint="`repos:` must be a YAML list of mappings",
            )

        seen_names: dict[str, int] = {}
        entries: list[RepoEntry] = []
        for idx, item in enumerate(repos_raw):
            if not isinstance(item, dict):
                raise SchemaValidationFailed(
                    f"external_repos entry {idx} is not a mapping: {cfg_path}",
                    data={
                        "config_path": str(cfg_path),
                        "entry_index": idx,
                        "got_type": type(item).__name__,
                    },
                    fix_hint="each entry under `repos:` must be a mapping",
                )

            entry = cls._validate_entry(
                item,
                idx=idx,
                config_path=cfg_path,
                default_exclude_globs=default_exclude_globs,
                default_include_exts=default_include_exts,
                require_local_path=require_local_path,
            )
            if entry.name in seen_names:
                raise SchemaValidationFailed(
                    f"external_repos has duplicate `name`: {entry.name!r} "
                    f"(entries {seen_names[entry.name]} and {idx})",
                    data={
                        "config_path": str(cfg_path),
                        "duplicate_name": entry.name,
                        "first_index": seen_names[entry.name],
                        "second_index": idx,
                    },
                    fix_hint="each `name` must be unique within the file",
                )
            seen_names[entry.name] = idx
            entries.append(entry)

        return cls(repos=entries)

    @staticmethod
    def _validate_entry(
        item: dict[str, Any],
        *,
        idx: int,
        config_path: Path,
        default_exclude_globs: Optional[Sequence[str]],
        default_include_exts: Optional[Sequence[str]],
        require_local_path: bool,
    ) -> RepoEntry:
        name = item.get("name")
        if not isinstance(name, str) or not name:
            raise SchemaValidationFailed(
                f"external_repos entry {idx} missing `name`: {config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                },
                fix_hint="every entry under `repos:` must have a `name`",
            )
        if not _SLUG_RE.match(name):
            raise SchemaValidationFailed(
                f"external_repos entry {idx} has non-slug `name` "
                f"{name!r}: {config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                },
                fix_hint=(
                    "`name` must match [a-z0-9][a-z0-9-]* (kebab-case slug)"
                ),
            )

        url = item.get("url")
        if not isinstance(url, str) or not url:
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) missing `url`: "
                f"{config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                },
                fix_hint="every repo entry must declare a git `url`",
            )

        local_path_raw = item.get("local_path")
        if not isinstance(local_path_raw, str) or not local_path_raw:
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) missing `local_path`: "
                f"{config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                },
                fix_hint=(
                    "every repo entry must declare a `local_path` "
                    "pointing at the local checkout"
                ),
            )
        local_path = Path(local_path_raw).expanduser()
        # code-repos-auto-acquire: a managed entry defers materialisation to the
        # git-files source's ensure_checkout() at run time, so its missing/non-git
        # checkout must NOT hard-fail at load. Unmanaged entries keep the loud
        # require-a-clone validation.
        manage_checkout = bool(item.get("manage_checkout", False))
        require_checkout = require_local_path and not manage_checkout
        if require_checkout and not local_path.exists():
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) `local_path` does "
                f"not exist: {local_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                    "local_path": str(local_path),
                },
                fix_hint=(
                    f"clone {url!r} to {local_path}, update the entry, or set "
                    f"`manage_checkout: true` to auto-clone at ingest"
                ),
            )
        if require_checkout and not (local_path / ".git").exists():
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) `local_path` is "
                f"not a git checkout: {local_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                    "local_path": str(local_path),
                },
                fix_hint=(
                    f"`local_path` must contain a `.git` directory; "
                    f"clone {url!r} or fix the path"
                ),
            )

        tracked_branch_raw = item.get("tracked_branch", _DEFAULT_TRACKED_BRANCH)
        if not isinstance(tracked_branch_raw, str) or not tracked_branch_raw:
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) `tracked_branch` "
                f"must be a non-empty string when present: {config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                },
                fix_hint="omit `tracked_branch` to use default `main`",
            )

        description = item.get("description")
        if description is not None and not isinstance(description, str):
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) `description` "
                f"must be a string: {config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                },
            )

        exclude_globs = ExternalRepoConfig._resolve_list_override(
            item.get("exclude_globs"),
            default=default_exclude_globs,
            field_name="exclude_globs",
            config_path=config_path,
            idx=idx,
            name=name,
        )
        include_exts = ExternalRepoConfig._resolve_list_override(
            item.get("include_exts"),
            default=default_include_exts,
            field_name="include_exts",
            config_path=config_path,
            idx=idx,
            name=name,
        )

        return RepoEntry(
            name=name,
            url=url,
            local_path=local_path,
            tracked_branch=tracked_branch_raw,
            description=description,
            exclude_globs=exclude_globs,
            include_exts=include_exts,
            manage_checkout=manage_checkout,
        )

    @staticmethod
    def _resolve_list_override(
        raw: Any,
        *,
        default: Optional[Sequence[str]],
        field_name: str,
        config_path: Path,
        idx: int,
        name: str,
    ) -> Optional[tuple[str, ...]]:
        """Per-D2: per-repo lists REPLACE defaults (not union)."""
        if raw is None:
            return tuple(default) if default is not None else None
        if not isinstance(raw, list) or not all(
            isinstance(item, str) and item for item in raw
        ):
            raise SchemaValidationFailed(
                f"external_repos entry {idx} ({name}) `{field_name}` "
                f"must be a list of non-empty strings: {config_path}",
                data={
                    "config_path": str(config_path),
                    "entry_index": idx,
                    "name": name,
                    "field": field_name,
                },
            )
        return tuple(raw)


@dataclass
class ExternalRepoFactory:
    """Substrate-library source class spawning per-repo sub-sources.

    Per the spec's Requirement *Multi-Repo Factory Source
    Class*. A deployment registers this once in
    `source_registry.yaml` with `repos_config_path` pointing
    at the operator's external_repos.yaml.
    """

    repos_config_path: str
    default_exclude_globs: Optional[Sequence[str]] = None
    default_include_exts: Optional[Sequence[str]] = None
    require_local_path: bool = True

    name: str = "external_repos"
    profile: str = "discovery_crawl"
    # Issue #2818 backstop: each record is one repository. A failed sub-run
    # is reported as partial health with no record set (see run()); this
    # flag makes the runner also refuse a run that still emits a repository
    # record whose files all vanished without a reported failure, unless
    # `okg ingest --allow-full-retraction`.
    record_keys_are_sub_scopes: ClassVar[bool] = True

    # Incremental-source contract (issue #323). The factory's emission per cycle
    # is a function of THREE scopes, any of which can change independently:
    #   1. the operator config YAML itself — which repos exist plus each repo's
    #      url / local_path / tracked_branch / description / globs / exts. A repo
    #      REMOVED from the YAML drops its `external_repo` node + all descendants
    #      (D3 retraction), so the probe must be delete-sensitive over the config;
    #   2. each declared repo's working tree at HEAD — git_files + code_structure
    #      read files there (mirrored by GitScopeProbe);
    #   3. each declared repo's commit-graph reachable from all ref tips —
    #      git_history walks it (mirrored by GitRefsProbe; a non-HEAD branch
    #      advance would be a false-unchanged for a HEAD-tree-only probe).
    # No single probe covers all three, so this is a UnionProbe: changed iff ANY
    # sub-scope changed. The config YAML is digested by a delete-sensitive
    # content hash (so removing/adding/editing a repo entry busts the token);
    # the factory module is the config-probe's emit_target so a change to the
    # factory's own emission logic busts the token with no manual bump. Each
    # repo contributes a GitScopeProbe (files) + GitRefsProbe (history) whose
    # sub-tokens already embed their sub-source emit-version. A config-load
    # failure or git error in build_token propagates -> run_probe forces a full
    # read (never a silent false-unchanged).
    change_probe_kind = "union"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> "_ExternalReposUnionProbe":
        import sys

        return _ExternalReposUnionProbe(
            load_config=self.load_config,
            repos_config_path=str(Path(self.repos_config_path).expanduser()),
            factory_config={
                "default_exclude_globs": (
                    sorted(self.default_exclude_globs)
                    if self.default_exclude_globs is not None
                    else None
                ),
                "default_include_exts": (
                    sorted(self.default_include_exts)
                    if self.default_include_exts is not None
                    else None
                ),
                "require_local_path": self.require_local_path,
            },
            factory_module=sys.modules[__name__],
        )

    def load_config(self) -> ExternalRepoConfig:
        """Read + validate the operator config. Raises
        `SchemaValidationFailed` on any structural problem."""
        return ExternalRepoConfig.from_yaml(
            self.repos_config_path,
            default_exclude_globs=self.default_exclude_globs,
            default_include_exts=self.default_include_exts,
            require_local_path=self.require_local_path,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        """Per-publish enumeration of declared repos (D1)."""
        config = self.load_config()

        per_repo_cursor: dict[str, Any] = (
            dict(cursor.get("repos", {}))
            if isinstance(cursor, dict)
            else {}
        )

        facts: list[Any] = []
        next_repo_cursor: dict[str, Any] = {}
        per_repo_records: dict[str, RecordEmission] = {}
        per_repo_errors: dict[str, str] = {}
        completed_scope = mode in {"reconcile", "scope_complete"}

        for repo in config.repos:
            repo_id = repo.node_id()
            revision = {
                "run_id": run_id,
                "config_path": str(Path(self.repos_config_path).expanduser()),
                "repo_name": repo.name,
            }

            facts.append(
                NodeFact(
                    node_id=repo_id,
                    subtype="external_repo",
                    attrs=repo.to_attrs(),
                    source_record_id={"repo_name": repo.name},
                    source_revision=revision,
                    op="I",
                )
            )

            repo_record = RecordEmission(
                primary=RecordNode(
                    node_id=repo_id,
                    subtype="external_repo",
                    source_record_id={"repo_name": repo.name},
                    source_revision=revision,
                ),
            )

            try:
                sub_state = per_repo_cursor.get(repo.name, {}) or {}
                next_sub: dict[str, Any] = {}
                descendants, sub_facts, sub_state_out = self._run_sub_sources(
                    repo,
                    run_id=run_id,
                    mode=mode,
                    sync_scope=sync_scope,
                    sub_cursor=sub_state,
                )
                facts.extend(sub_facts)
                member_of_edges: list[EdgeFact] = []
                # Emit member_of edges for every emitted SourceFile.
                for descendant in descendants:
                    if descendant.subtype != "source_file":
                        continue
                    member_of_edges.append(
                        EdgeFact(
                            src=descendant.node_id,
                            dst=repo_id,
                            edge_type="member_of",
                            attrs={"repo_name": repo.name},
                            source_record_id={
                                "repo_name": repo.name,
                                "source_file_id": descendant.node_id,
                            },
                            source_revision=revision,
                            op="I",
                        )
                    )
                facts.extend(member_of_edges)
                # The repo record owns every row the repo contributes: its
                # descendants' nodes AND every edge -- the factory's own
                # member_of edges plus each sub-source's edges. The runner's
                # completed-scope coverage proof refuses any source-owned
                # graph edge the record set does not declare (issue #2939),
                # and retraction diffs these edges per repo record.
                repo_record = RecordEmission(
                    primary=repo_record.primary,
                    descendants=list(descendants),
                    edges=self._edge_keys_from_facts(
                        [*sub_facts, *member_of_edges],
                    ),
                )
                next_sub.update(sub_state_out)
                next_repo_cursor[repo.name] = next_sub
            except Exception as exc:  # noqa: BLE001 — per-D1 isolation
                # Per-D1: per-repo failures don't abort the run.
                # Record the error; continue with remaining repos.
                per_repo_errors[repo.name] = str(exc)
                log.warning(
                    "external_repos: sub-run failed for repo=%s: %s: %s; "
                    "the run reports partial health and grants no deletion",
                    repo.name,
                    type(exc).__name__,
                    exc,
                )
                # Keep prior cursor for this repo so retry is sensible.
                if repo.name in per_repo_cursor:
                    next_repo_cursor[repo.name] = per_repo_cursor[repo.name]

            per_repo_records[repo_id] = repo_record

        # A repository whose sub-run failed was not observed, so its record
        # above holds only the repo node. Declaring that as its complete
        # footprint on a full run would retract every row the repo published
        # before, turning one transient git failure into a retraction of the
        # whole repository. The run did not see its whole scope: it reports
        # partial health naming each failed repository, and claims neither a
        # completed scope nor a record set. The runner then withholds
        # deletion authority for the whole run, keeps the prior footprint,
        # and a later clean run resumes retraction.
        health: Optional[SourceHealth] = None
        if per_repo_errors:
            completed_scope = False
            health = SourceHealth(
                status=SOURCE_RUN_PARTIAL_STATUS,
                reason=(
                    "external repo sub-run failed for "
                    + "; ".join(
                        f"{name}: {error}"
                        for name, error in sorted(per_repo_errors.items())
                    )
                )[:500],
            )

        record_set: Optional[RecordSet] = None
        if completed_scope:
            record_set = RecordSet(records=dict(per_repo_records))

        next_cursor = {
            "repos": next_repo_cursor,
            "errors": per_repo_errors,
        }

        return SourceRun(
            facts=facts,
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=record_set,
            health=health,
        )

    def ensure_checkout(self) -> Optional[SourceHealth]:
        """Materialise every managed external repo BEFORE the runner's
        change-probe reads git state.

        The runner calls this seam ahead of the probe (and its short-circuit
        decision), so — unlike doing it inside run() — a repeatedly-failing
        managed clone keeps retrying every ingest and its ``acquire_failed``
        health keeps surfacing on the run report, even when the union probe would
        short-circuit an unchanged config. Never raises: clone_or_pull returns a
        structured outcome mapped to health.
        """
        from okg.substrate.library.sources._git_helpers import clone_or_pull

        config = self.load_config()
        healths: dict[str, SourceHealth] = {}
        for repo in config.repos:
            if not repo.manage_checkout:
                continue
            outcome = clone_or_pull(
                Path(repo.local_path),
                repo.url,
                tracked_branch=repo.tracked_branch,
            )
            health = acquire_outcome_health(outcome)
            if health is not None:
                healths[repo.name] = health
        if not healths:
            return None
        # Aggregate across repos: worst status wins (failed > stale); the reason
        # lists each affected repo so the run report names what degraded.
        worst = (
            "acquire_failed"
            if any(h.status == "acquire_failed" for h in healths.values())
            else "acquire_stale"
        )
        reason = "; ".join(
            f"{name}: {h.reason or h.status}" for name, h in sorted(healths.items())
        )[:500]
        return SourceHealth(status=worst, reason=reason)

    # ---------- sub-source orchestration ----------

    def _run_sub_sources(
        self,
        repo: RepoEntry,
        *,
        run_id: str,
        mode: str,
        sync_scope: Optional[dict[str, Any]],
        sub_cursor: dict[str, Any],
    ) -> tuple[list[RecordNode], list[Any], dict[str, Any]]:
        """Instantiate + run git_files / git_history / code_structure
        scoped to one repo. Returns descendants, facts, and the
        merged next-cursor slice for the repo."""
        from okg.substrate.library.sources.code_graph import (
            CodeStructureSource,
        )
        from okg.substrate.library.sources.git_graph_files import (
            GitFilesSource,
        )
        from okg.substrate.library.sources.git_graph_history import (
            GitHistorySource,
        )

        from okg.substrate.library.sources._git_graph_files_emit import (
            HAD_COMMITS_REASON,
            cursor_records_commits,
            head_is_verified_unborn,
        )

        descendants: list[RecordNode] = []
        facts: list[Any] = []
        next_sub: dict[str, Any] = {}

        if Path(repo.local_path).exists() and head_is_verified_unborn(
            Path(repo.local_path),
        ):
            # A repository that verifiably never had commits (unborn HEAD, no
            # refs, no pack and no commit object) has nothing to read. That is
            # a verified empty result: the repo record stands with no
            # descendants and no edges. Running the sub-sources would not add
            # anything, and code-structure's change probe raises on the
            # unreadable HEAD, which would turn a freshly created repository
            # into a failure that blocks every full run.
            prior_git_files = (
                sub_cursor.get("git_files")
                if isinstance(sub_cursor, dict) else None
            )
            if cursor_records_commits(prior_git_files):
                # An earlier run read commits here. Reading the repository as
                # empty now would retract every row it published; fail the
                # repository instead, so the run is partial.
                raise RuntimeError(HAD_COMMITS_REASON)
            log.info(
                "external_repos: repo=%s has no commits yet; nothing to read",
                repo.name,
            )
            return descendants, facts, next_sub

        git_files_kwargs: dict[str, Any] = {
            "repo": repo.name,
            "repo_root": repo.local_path,
        }
        if repo.exclude_globs is not None:
            git_files_kwargs["exclude_globs"] = repo.exclude_globs
        if repo.manage_checkout:
            # git-files owns the checkout; git-history/code-structure below share
            # repo.local_path and inherit it.
            git_files_kwargs["url"] = repo.url
            git_files_kwargs["tracked_branch"] = repo.tracked_branch
            git_files_kwargs["manage_checkout"] = True
        # Acquisition is NOT done here: the factory's own ensure_checkout()
        # (called by the runner BEFORE the change-probe) has already materialised
        # every managed repo, so a still-absent checkout means the clone failed —
        # gf's own manage_checkout guard then degrades gf.run() to zero facts
        # rather than crashing the domain crawl.
        gf = GitFilesSource(**git_files_kwargs)
        gf_cursor = sub_cursor.get("git_files") if isinstance(sub_cursor, dict) else None
        gf_run = gf.run(
            run_id,
            mode=mode,
            cursor=gf_cursor if isinstance(gf_cursor, dict) else None,
            sync_scope=sync_scope,
        )
        self._raise_on_degraded_sub_run("git_files", gf_run)
        gf_facts = list(gf_run.facts)
        facts.extend(gf_facts)
        descendants.extend(self._descendants_from_facts(gf_facts))
        if gf_run.next_cursor is not None:
            next_sub["git_files"] = gf_run.next_cursor
        if gf_run.record_set is not None:
            for rec in gf_run.record_set.records.values():
                if rec.primary is not None:
                    descendants.append(rec.primary)
                descendants.extend(rec.descendants)

        gh = GitHistorySource(
            repo=repo.name,
            repo_root=repo.local_path,
            source_file_exclude_globs=(
                list(repo.exclude_globs)
                if repo.exclude_globs is not None
                else None
            ),
        )
        gh_cursor = sub_cursor.get("git_history") if isinstance(sub_cursor, dict) else None
        gh_run = gh.run(
            run_id,
            mode=mode,
            cursor=gh_cursor if isinstance(gh_cursor, dict) else None,
        )
        self._raise_on_degraded_sub_run("git_history", gh_run)
        gh_facts = list(gh_run.facts)
        facts.extend(gh_facts)
        descendants.extend(self._descendants_from_facts(gh_facts))
        if gh_run.next_cursor is not None:
            next_sub["git_history"] = gh_run.next_cursor

        cs_kwargs: dict[str, Any] = {
            "repo": repo.name,
            "repo_root": repo.local_path,
        }
        if repo.include_exts is not None:
            cs_kwargs["include_exts"] = repo.include_exts
        if repo.exclude_globs is not None:
            cs_kwargs["exclude_globs"] = repo.exclude_globs
        cs = CodeStructureSource(**cs_kwargs)
        cs_cursor = sub_cursor.get("code_structure") if isinstance(sub_cursor, dict) else None
        cs_run = cs.run(
            run_id,
            mode=mode,
            cursor=cs_cursor if isinstance(cs_cursor, dict) else None,
            sync_scope=sync_scope,
        )
        self._raise_on_degraded_sub_run("code_structure", cs_run)
        cs_facts = list(cs_run.facts)
        facts.extend(cs_facts)
        descendants.extend(self._descendants_from_facts(cs_facts))
        if cs_run.next_cursor is not None:
            next_sub["code_structure"] = cs_run.next_cursor

        # The factory is one substrate source even though it composes several
        # adapters. Resolve overlapping node candidates by their declared
        # ownership contract before they reach SourceRunner: an authoritative
        # fact makes a substitute for the same identity unnecessary. This is
        # deliberately mode-based; source names and attrs do not participate.
        facts = self._compose_sub_source_node_facts(facts)

        # Deduplicate record-set descendants with the same explicit precedence
        # so the record set and emitted facts declare an identical mode on the
        # second and every later verified run.
        seen: dict[str, RecordNode] = {}
        for d in descendants:
            prior = seen.get(d.node_id)
            if prior is None or (
                prior.ownership_mode == "substitute"
                and d.ownership_mode == "authoritative"
            ):
                seen[d.node_id] = d
        return list(seen.values()), facts, next_sub

    @staticmethod
    def _raise_on_degraded_sub_run(sub_source: str, sub_run: SourceRun) -> None:
        """Turn a sub-source's degraded health into this repo's failure.

        A sub-source that could not read its scope (``partial``) or its
        checkout (``acquire_failed``) returns normally with few or no facts.
        Composed as if it had succeeded, the repository's record would lose
        every row that sub-source owns and a full run would retract them.
        Raising here routes it through the per-repository failure path, so
        the run reports partial health and deletes nothing. ``acquire_stale``
        still indexed the whole existing tree, so it is not a failure.
        """
        health = sub_run.health
        if health is None or health.status in {
            "ok",
            "not_applicable",
            "skipped_optional",
            "acquire_stale",
        }:
            return
        raise RuntimeError(
            f"{sub_source} sub-source reported {health.status}"
            f"{': ' + health.reason if health.reason else ''}"
        )

    @staticmethod
    def _compose_sub_source_node_facts(facts: Iterable[Any]) -> list[Any]:
        """Compose overlapping node inserts without ownership inference."""
        materialized = list(facts)
        authoritative_ids = {
            fact.node_id
            for fact in materialized
            if isinstance(fact, NodeFact)
            and fact.op == "I"
            and fact.ownership_mode == "authoritative"
        }
        return [
            fact
            for fact in materialized
            if not (
                isinstance(fact, NodeFact)
                and fact.op == "I"
                and fact.ownership_mode == "substitute"
                and fact.node_id in authoritative_ids
            )
        ]

    @staticmethod
    def _edge_keys_from_facts(facts: Iterable[Any]) -> list[EdgeKey]:
        """Declare every edge insert, once per (src, edge_type, dst).

        Each key carries the edge's identity only. The runner's coverage
        proof and its retraction diff both compare edges by
        ``(src, edge_type, dst)``, so the identity is all the declaration
        needs; a retraction built from an identity-only key names the record
        key instead of the edge's own record id, the same shape the runner
        uses for the edges it adopts from the live graph. Copying the attrs
        and the per-run revision (which embeds the run id) into every key
        quadrupled the stored record set: 239 MB to 999 MB for a repository
        of 14.5k files and 1.36M edges, close to PostgreSQL's 1 GB limit on
        one field.
        """
        keys: dict[tuple[str, str, str], EdgeKey] = {}
        for fact in facts:
            if not isinstance(fact, EdgeFact) or fact.op != "I":
                continue
            key = EdgeKey(
                src=fact.src,
                dst=fact.dst,
                edge_type=fact.edge_type,
            )
            keys.setdefault(key.identity(), key)
        return list(keys.values())

    @staticmethod
    def _descendants_from_facts(facts: Iterable[Any]) -> Iterator[RecordNode]:
        for fact in facts:
            if not isinstance(fact, NodeFact):
                continue
            if fact.op != "I":
                continue
            yield RecordNode(
                node_id=fact.node_id,
                subtype=fact.subtype,
                source_record_id=dict(fact.source_record_id),
                source_revision=dict(fact.source_revision),
                ownership_mode=fact.ownership_mode,
            )


class _ExternalReposUnionProbe:
    """Change-probe for the multi-repo factory (issue #323, ``union`` kind).

    The set of declared repos is dynamic (it lives in the operator config YAML
    and changes between cycles), so this probe re-enumerates the config on every
    ``build_token`` and composes a fresh ``UnionProbe`` over the live sub-probe
    set. The verdict is ``changed`` iff ANY sub-scope changed:

      - a delete-sensitive ``ContentHashProbe`` over the config YAML — its bytes
        encode every repo entry (name / url / local_path / tracked_branch /
        description / per-repo globs+exts), so adding, editing, OR removing a
        repo busts the token (D3 retraction is delete-sensitive here);
      - per declared repo: a ``GitScopeProbe`` over the working tree at HEAD
        (mirrors git_files + code_structure) and a ``GitRefsProbe`` over the ref
        tips (mirrors git_history). A repo whose ``local_path`` is absent or not
        a git checkout still contributes nothing beyond the config digest; once
        it appears, its git sub-probes join the union (the config-content change
        that declared it already busted the token, so the transition is sound).

    A config-load error or any git error inside ``build_token`` propagates; the
    runner boundary (``run_probe``) catches it and forces ``changed=True`` — a
    full read, never a silent false-unchanged. Because the token never reads
    filesystem mtime, a worktree checkout that resets mtimes without changing
    content leaves it unchanged (the churn-immunity the contract requires).
    """

    def __init__(
        self,
        *,
        load_config: Any,
        repos_config_path: str,
        factory_config: Mapping[str, Any],
        factory_module: Any,
    ) -> None:
        self._load_config = load_config
        self._repos_config_path = repos_config_path
        self._factory_config = dict(factory_config)
        self._factory_module = factory_module

    def _sub_probes(self) -> list[Any]:
        from okg.substrate.library.sources import (
            _git_graph_files_emit,
            _git_graph_history_emit,
            code_graph_structure,
            git_graph_files,
            git_graph_history,
        )
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_refs_probe import GitRefsProbe
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

        cfg_path = Path(self._repos_config_path)

        def _config_items() -> list[tuple[str, Path]]:
            # The config YAML is the membership manifest. Re-enumerate live so a
            # vanished file reads as absent (delete-sensitive). A single stable
            # key means edit/replace busts the per-item sha; an absent file is
            # the "no repos declared" identity.
            return [(str(cfg_path), cfg_path)] if cfg_path.is_file() else []

        probes: list[Any] = [
            ContentHashProbe(
                content_items=_config_items,
                config={
                    "repos_config_path": self._repos_config_path,
                    "factory_config": self._factory_config,
                },
                # The factory module owns the per-repo enumeration + edge
                # emission; fold its source so an emission-logic change busts the
                # token with no manual bump.
                emit_targets=[self._factory_module],
            )
        ]

        config = self._load_config()
        for repo in config.repos:
            local_path = Path(repo.local_path)
            if not (local_path / ".git").exists():
                # No checkout yet — the config-content digest already reflects
                # this repo's declaration; its git scope contributes nothing
                # until the checkout exists. (require_local_path=True validates
                # this at run time; the probe must not crash when it is False.)
                continue
            probes.append(
                GitScopeProbe(
                    repo_root=local_path,
                    config={
                        "external_repo": repo.name,
                        "exclude_globs": sorted(repo.exclude_globs or ()),
                        "include_exts": sorted(repo.include_exts or ()),
                        "tracked_branch": repo.tracked_branch,
                    },
                    # The emitters and helpers they delegate to, so a change
                    # in either re-reads an unchanged repo (issue #3468).
                    emit_targets=[
                        *emission_identity_targets(
                            "git_graph_files", git_graph_files, _git_graph_files_emit
                        ),
                        *emission_identity_targets(
                            "code_structure", code_graph_structure
                        ),
                    ],
                )
            )
            probes.append(
                GitRefsProbe(
                    repo_root=local_path,
                    config={
                        "external_repo": repo.name,
                        "tracked_branch": repo.tracked_branch,
                    },
                    emit_targets=list(
                        emission_identity_targets(
                            "git_graph_history",
                            git_graph_history,
                            _git_graph_history_emit,
                        )
                    ),
                    include_remotes=False,
                )
            )
        return probes

    def build_token(self) -> dict[str, Any]:
        from okg.substrate.library.sources.union_probe import UnionProbe

        return UnionProbe(self._sub_probes()).build_token()

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        prior = (
            cursor.get(CURSOR_TOKEN_KEY)
            if isinstance(cursor, Mapping)
            else None
        )
        token = self.build_token()
        if prior is None:
            return ProbeVerdict(
                changed=True, token=token, reason="no_prior_token"
            )
        changed = not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="external_repos_changed"
            if changed
            else "external_repos_unchanged",
        )
