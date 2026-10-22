"""CLI handlers for `okg init`, `okg new-module`, and the retired `okg new-source`.

Extracted from ``cli/main.py`` by ``extract-cli-init`` (2026-05). The
init/scaffolding subcommand family is a self-contained group:
``_cmd_init`` dispatches between ``_cmd_init_from_template``
(legacy template copy) and ``_cmd_init_from_profile``
(profile-driven wizard); ``_cmd_new_module`` scaffolds a deployment
module, and ``_cmd_new_source`` refuses (issue #2479).

Handlers import ``cli.main`` helpers (``_resolve_dsn``,
``_emit_error``, ``_emit_ok``, etc.) via deferred function-body
imports to avoid the top-level circular import.
"""
from __future__ import annotations
from okg.substrate import fastyaml
from okg.substrate.cli._output import command_output_tail
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.install_summary import (
    actionable_warnings,
    agent_failure_line,
    deployments_env_prefix,
    render_install_summary,
    warning_fix,
    warning_line,
)
from okg.substrate.support.user_hints import doc_reference

import argparse
import contextlib
import json
import logging
import os
import shlex
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Mapping

def deployments_env_root_error(detail: str, *, command: str):
    """The ONE refusal for `OKG_DEPLOYMENTS_DIR` naming a missing directory.

    Issue #2127. `okg init` created a missing env root while the academic-researcher
    installer refused it, so the same misspelled variable produced a silent
    second deployments root from one command and a refusal from the other.
    The resolver contract in `okg.substrate.deployments` is the tiebreak: an
    override or env var set but not pointing at an existing directory raises
    rather than silently falling through. Both commands now raise, through
    this builder, so the code, the data shape and the fix hint cannot drift
    apart.

    `--deployments-root` keeps the opposite rule in both commands: the
    operator named that root for this scaffold, so creating it IS the
    scaffold.
    """
    from okg.substrate.errors import SubstrateError

    err = SubstrateError(
        f"{command} could not resolve a deployments root: {detail}",
        data={
            "deployment": None,
            "resolution_error": detail,
            "env_var": "OKG_DEPLOYMENTS_DIR",
        },
        fix_hint=(
            "create that directory, point OKG_DEPLOYMENTS_DIR at an existing "
            "deployments root, or unset it and pass `--deployments-root "
            "<dir>` -- which the scaffold creates for you"
        ),
    )
    err.code = "deployment_not_found"
    return err


def _cmd_init(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg init` — scaffold a deployment dir.

    Two modes:
    - `--profile <name>` (Layer 2A): reads `profiles/<name>/`,
      gathers init answers from CLI flags / interactive prompts,
      writes a working deployment to `deployments/<deployment_name>/`,
      runs schema migration + first publish.
    - `--template <name>` (legacy): copies `okg.templates.<name>` to
      `--target` verbatim. Used by older devops-style templates that
      pre-date the profile schema.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    if getattr(args, "profile_name", None):
        return _cmd_init_from_profile(args, as_json=as_json)
    if not getattr(args, "template", None):
        return _emit_error(
            SubstrateError(
                "okg init requires either --profile <name> or "
                "--template <name>",
                fix_hint=(
                    "pass --profile <name> to scaffold from a "
                    "profile (preferred for new deployments) or "
                    "--template <name> for legacy templates"
                ),
            ),
            as_json=as_json,
        )
    return _cmd_init_from_template(args, as_json=as_json)


def _load_template_meta(template_src: Path) -> dict[str, Any]:
    """Load a template's `template_meta.yaml` if present.

    Empty dict when the file is absent (a template without a
    manifest is treated as a simple verbatim-copy template — the
    pre-`retire-pi-prefix-commands` default behavior).
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    import yaml  # noqa: PLC0415 (local; main.py imports lazily)
    meta_path = template_src / "template_meta.yaml"
    if not meta_path.is_file():
        return {}
    try:
        raw = fastyaml.safe_load(meta_path.read_text()) or {}
    except yaml.YAMLError:
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def _emit_template_dry_run_plan(
    *,
    template: str,
    src: Path,
    target: Path,
    blockers: list[str],
    as_json: bool,
) -> int:
    """Print what `--template` would copy, having copied nothing.

    The legacy template route renders a directory tree verbatim
    (`_render_template_dir`), so the plan is that tree rebased onto the
    target — enumerated from the same `rglob` walk the renderer uses.
    """
    from okg.substrate.cli.main import _emit_ok  # noqa: F401

    files: list[str] = []
    directories: list[str] = [str(target)]
    for src_file in sorted(src.rglob("*")):
        dst = target / src_file.relative_to(src)
        if src_file.is_dir():
            directories.append(str(dst))
        else:
            files.append(str(dst))

    payload: dict[str, Any] = {
        "status": "dry_run_blocked" if blockers else "dry_run_ok",
        "dry_run": True,
        "plan_complete": True,
        "template": template,
        "target": str(target),
        "target_exists": target.exists(),
        "target_action": "refuse" if blockers else "create",
        "files_that_would_be_written": files,
        "directories_that_would_be_created": sorted(set(directories)),
        "files_that_would_be_deleted": [],
        "blockers": blockers,
        "next_step": (
            "re-run the same command without --dry-run"
            if not blockers
            else "resolve the blockers above, then re-run with --dry-run"
        ),
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not blockers else 1


def _cmd_init_from_template(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Legacy `--template <name>` mode (pre-Layer-2A).

    Per `retire-pi-prefix-commands` Phase 4: the prior
    hard-coded repo-context template branch was replaced with a
    data-driven check against the template's
    `template_meta.yaml`. A template declaring
    `requires_repo_context: true` opts in to the repo-context
    flag set (--repo-root / --openspec-root / etc.) — the
    substrate's dispatcher names no specific template.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    from pathlib import Path
    from okg.substrate.library import LibraryRefMissing, resolve

    template = args.template
    try:
        src = resolve(f"okg.templates.{template}")
    except LibraryRefMissing as e:
        return _emit_error(e, as_json=as_json)
    template_meta = _load_template_meta(Path(src))
    requires_repo = bool(template_meta.get("requires_repo_context", False))
    deployment_name_source = (
        template_meta.get("deployment_name_source") or "template_name"
    )
    if requires_repo and not getattr(args, "repo_root", None):
        err = SubstrateError(
            f"okg init --template {template} requires --repo-root "
            f"(template manifest sets `requires_repo_context: true`)",
            fix_hint=(
                "pass --repo-root /path/to/the/repository "
                "you want this deployment to index"
            ),
        )
        if getattr(args, "dry_run", False):
            # Same contract as the profile route: a dry run that cannot
            # get far enough to describe the install says so as a
            # blocker, not as an error envelope.
            return _emit_incomplete_dry_run_plan(error=err, as_json=as_json)
        return _emit_error(err, as_json=as_json)
    repo_root = (
        Path(args.repo_root).expanduser().resolve()
        if getattr(args, "repo_root", None)
        else Path.cwd().resolve()
    )
    repo_name = repo_root.name or "repo"
    if requires_repo and deployment_name_source == "repo_basename":
        deployment_name = (
            args.deployment_name
            if getattr(args, "deployment_name", None)
            else repo_name
        )
    else:
        deployment_name = (
            args.deployment_name
            if getattr(args, "deployment_name", None)
            else template
        )
    openspec_root = (
        Path(args.openspec_root).expanduser().resolve()
        if requires_repo and getattr(args, "openspec_root", None)
        else repo_root / "openspec"
    )
    claude_projects_root = (
        Path(args.claude_projects_root).expanduser().resolve()
        if requires_repo and getattr(args, "claude_projects_root", None)
        else Path("~/.claude/projects").expanduser().resolve()
    )
    notes_root = (
        Path(args.notes_root).expanduser().resolve()
        if requires_repo and getattr(args, "notes_root", None)
        else None
    )
    since = (
        args.since
        if requires_repo and getattr(args, "since", None)
        else "90d"
    )
    target = Path(args.target or ".") / deployment_name

    if target.exists() and any(target.iterdir()):
        err = SubstrateError(
            f"target {target} is not empty; init refuses to clobber",
            data={"target": str(target)},
            fix_hint="pass --target to a non-existent directory",
        )
        if getattr(args, "dry_run", False):
            return _emit_template_dry_run_plan(
                template=template, src=Path(src), target=target,
                blockers=[str(err)], as_json=as_json,
            )
        return _emit_error(err, as_json=as_json)

    if getattr(args, "dry_run", False):
        # `--dry-run` reaches this legacy route too — it is a shared flag
        # on the `install`/`init` parsers, and a flag that is accepted and
        # ignored here would be the same defect on a different path.
        return _emit_template_dry_run_plan(
            template=template, src=Path(src), target=target,
            blockers=[], as_json=as_json,
        )

    target.mkdir(parents=True, exist_ok=True)
    openspec_enabled = openspec_root.is_dir()
    template_variables = {
        "repo_root": str(repo_root),
        "repo_name": repo_name,
        "openspec_root": str(openspec_root),
        "openspec_enabled": openspec_enabled,
        "claude_projects_root": str(claude_projects_root),
        "deployment_name": deployment_name,
        "deployment_dir": str(target),
        "deployments_dir": str(target.parent),
        "since": since,
        "notes_root": str(notes_root) if notes_root else None,
    }
    _render_template_dir(src, target, template_variables)
    # Per-template `next_commands` come from `template_meta.yaml`
    # when present; otherwise a generic single-line fallback.
    next_commands = template_meta.get("next_commands") or [
        # Two literal commands in order, not a parenthetical (issue #2128).
        f"okg migrate --deployment {deployment_name} --apply",
        f"okg run --once --deployment {deployment_name} --apply",
    ]
    # Substitute template variables into next_commands strings.
    next_commands = [
        cmd.format(
            deployment_name=deployment_name,
            target=target,
        ) if isinstance(cmd, str) else cmd
        for cmd in next_commands
    ]
    _emit_ok(
        {"template": template, "target": str(target),
         "repo_root": str(repo_root),
         "deployment_name": deployment_name,
         "openspec_root": str(openspec_root),
         "openspec_enabled": openspec_enabled,
         "claude_projects_root": str(claude_projects_root),
         "notes_root": str(notes_root) if notes_root else None,
         "since": since,
         "next": "\n".join(next_commands),
         "next_commands": next_commands},
        as_json=as_json,
    )
    return 0


def _gather_profile_answers(
    args: argparse.Namespace,
    *,
    profile,
    as_json: bool,
) -> dict[str, Any]:
    """Collect init answers from CLI flags + (optionally) prompts.

    Each `init_question.id` is mapped to a `--<id-with-dashes>`
    flag (parsed by the init subparser). Missing required
    answers prompt interactively when stdin is a TTY and we
    aren't in agent mode; otherwise they raise.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    cli_overrides: dict[str, Any] = {}
    flag_map = getattr(args, "_profile_flag_map", None)
    if flag_map is None:
        # Fallback when `_maybe_inject_profile_flags` didn't run
        # (e.g. test harness constructs args directly): derive the
        # same mapping. Each init question id maps to either its
        # injected `answer_<id>` dest OR (for question ids that
        # overlap with existing flags) to that flag's dest.
        flag_map = {}
        existing_dests = set(vars(args).keys())
        for q in profile.init_questions:
            inj = f"answer_{q.id}"
            if inj in existing_dests:
                flag_map[q.id] = inj
            elif q.id in existing_dests:
                flag_map[q.id] = q.id
        # Always map deployment_name to the canonical --deployment-name
        # dest if a question of that id exists.
        if profile.question("deployment_name"):
            flag_map.setdefault("deployment_name", "deployment_name")
    for qid, attr in flag_map.items():
        if attr in vars(args):
            val = getattr(args, attr)
            if val is not None:
                cli_overrides[qid] = val

    answers: dict[str, Any] = {}
    interactive = (
        sys.stdin.isatty()
        and not _is_agent_mode()
        and not getattr(args, "non_interactive", False)
        # `--dry-run` answers "what would this do?"; marching the
        # operator through a setup wizard to answer it is the wrong
        # shape. Missing answers become plan blockers instead.
        and not getattr(args, "dry_run", False)
    )
    for q in profile.init_questions:
        if q.id in cli_overrides:
            answers[q.id] = cli_overrides[q.id]
            continue
        if q.default is not None:
            answers[q.id] = q.default
            continue
        if q.required and interactive:
            prompt_text = (
                f"{q.prompt} ({q.type})"
                + (f" [{q.default}]" if q.default else "")
                + ": "
            )
            try:
                answers[q.id] = input(prompt_text).strip()
            except EOFError:
                answers[q.id] = None
        else:
            # leave unanswered — validate_answer enforces required
            answers[q.id] = None
    return answers


def _validate_workspace_graph_repo_root(
    *,
    profile_name: str,
    answers: dict[str, Any],
) -> None:
    """Fail early when the Workspace Graph target is not a git root."""
    if profile_name != "workspace-graph":
        return
    raw_repo_root = answers.get("repo_root")
    if raw_repo_root in (None, ""):
        return

    from okg.substrate.errors import SubstrateError

    repo_root = Path(str(raw_repo_root)).expanduser()
    if not repo_root.exists():
        raise SubstrateError(
            f"workspace-graph repo_root does not exist: {repo_root}",
            data={"repo_root": str(repo_root)},
            fix_hint="pass --repo-root /path/to/a/git/repository",
        )
    if not repo_root.is_dir():
        raise SubstrateError(
            f"workspace-graph repo_root is not a directory: {repo_root}",
            data={"repo_root": str(repo_root)},
            fix_hint="pass --repo-root /path/to/a/git/repository",
        )

    try:
        import subprocess

        git_root_text = subprocess.check_output(
            [
                "git",
                "-C",
                str(repo_root),
                "rev-parse",
                "--show-toplevel",
            ],
            stderr=subprocess.STDOUT,
            text=True,
        ).strip()
    except FileNotFoundError as e:
        raise SubstrateError(
            "workspace-graph setup requires git on PATH",
            fix_hint="install git, then re-run okg init",
        ) from e
    except subprocess.CalledProcessError as e:
        raise SubstrateError(
            f"workspace-graph repo_root is not a git repository: {repo_root}",
            data={
                "repo_root": str(repo_root),
                "git_output": (e.output or "").strip(),
            },
            fix_hint="pass --repo-root /path/to/a/git/repository",
        ) from e

    if not git_root_text:
        raise SubstrateError(
            f"git did not report a repository root for {repo_root}",
            data={"repo_root": str(repo_root)},
            fix_hint="pass --repo-root /path/to/a/git/repository",
        )
    requested = repo_root.resolve()
    git_root = Path(git_root_text).resolve()
    if requested != git_root:
        raise SubstrateError(
            "workspace-graph repo_root must be the git worktree root",
            data={
                "repo_root": str(requested),
                "git_root": str(git_root),
            },
            fix_hint=f"pass --repo-root {git_root}",
        )

    # Persist the resolved absolute root into the generated deployment.
    answers["repo_root"] = str(requested)


def _entries_rmtree_would_destroy(target: Path) -> tuple[list[str], bool]:
    """Everything under `target` that a `--force` overwrite destroys.

    Returns `(paths, scan_incomplete)`. `scaffold_deployment` calls
    `shutil.rmtree(target)`, which removes the whole tree — not just the
    regular files. Listing only `is_file()` entries under-reports symlinks
    (including broken ones and links to directories), empty directories,
    and the deployment directory itself, so the operator reads a short
    list and believes it complete.

    `Path.rglob` swallows `OSError`, which would silently drop an
    unreadable subtree from that same list. `os.walk` with an `onerror`
    hook surfaces it instead: the second element is True when any part of
    the tree could not be read, so a short list is never mistaken for a
    complete one.
    """
    entries: set[str] = {str(target)}
    incomplete = False

    def _note_error(_err: OSError) -> None:
        nonlocal incomplete
        incomplete = True

    for dirpath, dirnames, filenames in os.walk(
        target, onerror=_note_error, followlinks=False,
    ):
        for name in list(dirnames) + list(filenames):
            entries.add(str(Path(dirpath) / name))
    return sorted(entries), incomplete


def _emit_scaffold_dry_run_plan(
    *,
    plan,
    profile,
    deployment_name: str,
    deployments_root: Path,
    force: bool,
    no_publish: bool,
    as_json: bool,
    no_agent_config: bool = False,
    explicit_root: bool = False,
) -> int:
    """Print what a scaffold install would do, having done none of it.

    This is `--dry-run` for the generic scaffold path — the path every
    profile without its own installer takes. It reports the exact paths
    that would be written (asked of the same code that writes them, so
    the list cannot drift), and what would happen to the deployment
    directory if it already exists: create it, refuse, or overwrite it.

    Returns 1 when a blocker means a real run would fail, so a wrapper
    script cannot read a blocked plan as a clear one.
    """
    from okg.substrate.cli.main import _emit_error, _emit_ok  # noqa: F401
    from okg.substrate.errors import SubstrateError
    from okg.substrate.deployment_bootstrap.profile_init import (
        planned_writes,
        redacted_init_answers,
    )

    target = plan.deployment_dir
    occupied = target.is_dir() and any(target.iterdir())
    blockers: list[str] = []
    # What a real run would destroy. Under `--force` the scaffold
    # `shutil.rmtree`s the directory first, so answering "would this
    # clobber my deployment?" with the list of files it would WRITE is
    # answering a different question.
    deletions: list[str] = []
    deletion_scan_incomplete = False
    if not occupied:
        action = "create"
    elif force:
        # --force --yes was already validated by the caller.
        action = "overwrite"
        deletions, deletion_scan_incomplete = _entries_rmtree_would_destroy(
            target,
        )
    else:
        action = "refuse"
        blockers.append(
            f"deployment directory {target} already exists and is "
            f"non-empty; a real run would refuse to overwrite it "
            f"(pass --force --yes to replace it, or pick a different "
            f"--deployment-name)"
        )

    try:
        writes = planned_writes(plan)
    except SubstrateError as e:
        # The render itself refuses (a source id declared by two
        # source-default files, an inadmissible strict registry). A real
        # run raises the same error from `scaffold_deployment` and emits
        # it as a typed refusal, so the dry run emits the identical
        # envelope and exit code rather than a Python traceback.
        return _emit_error(e, as_json=as_json)
    # The directories implied by the planned writes, plus the deployment
    # dir itself. Derived rather than reported by the scaffolder, so a
    # future `mkdir` with no corresponding write would not appear here.
    directories = sorted({str(w.path.parent) for w in writes} | {str(target)})

    payload: dict[str, Any] = {
        "status": "dry_run_blocked" if blockers else "dry_run_ok",
        "dry_run": True,
        "plan_complete": True,
        "profile": {"name": profile.name, "version": profile.version},
        "deployment": {
            "name": deployment_name,
            "dir": str(target),
            "modules": list(plan.modules),
        },
        "deployments_root": str(deployments_root),
        "deployments_root_exists": deployments_root.is_dir(),
        "deployment_dir_exists": target.is_dir(),
        "deployment_dir_non_empty": occupied,
        "deployment_dir_action": action,
        "files_that_would_be_written": [str(w.path) for w in writes],
        "directories_that_would_be_created": directories,
        "files_that_would_be_deleted": deletions,
        # Loud rather than silently short: a list the payload presents as
        # complete must say when it isn't.
        "deletion_scan_incomplete": deletion_scan_incomplete,
        "would_migrate_and_publish": not no_publish,
        "agents": _dry_run_agent_plan(
            deployment_name, no_publish=no_publish,
            no_agent_config=no_agent_config,
            deployments_root=deployments_root if explicit_root else None,
        ),
        "answers": redacted_init_answers(profile, plan.answers),
        "blockers": blockers,
        "next_step": (
            "re-run the same command without --dry-run"
            if not blockers
            else "resolve the blockers above, then re-run with --dry-run"
        ),
    }
    _emit_ok(payload, as_json=as_json)
    return 0 if not blockers else 1


def _dry_run_agent_plan(
    deployment_name: str,
    *,
    no_publish: bool,
    no_agent_config: bool,
    deployments_root: Path | None,
) -> dict[str, Any]:
    """The agent-connect step a real install would run (reads only)."""
    from okg.substrate.library.agent_runtimes.connect import (
        install_connect_plan,
    )

    return install_connect_plan(
        deployment_name,
        skip=no_publish or no_agent_config,
        deployments_root=deployments_root,
    )


def _would_prompt_without_dry_run(args: argparse.Namespace) -> bool:
    """Would a real run have asked the operator for the missing answers?

    `--dry-run` suppresses the setup wizard, so a question the operator
    would have been prompted for shows up as unanswered. Whether that is a
    blocker or merely a question depends on this: the same conditions
    `_gather_profile_answers` uses, minus the dry-run term.
    """
    from okg.substrate.cli.main import _is_agent_mode  # noqa: F401

    return (
        sys.stdin.isatty()
        and not _is_agent_mode()
        and not getattr(args, "non_interactive", False)
    )


def _emit_incomplete_dry_run_plan(
    *,
    error: Exception,
    profile=None,
    resolvable_interactively: bool = False,
    as_json: bool,
) -> int:
    """Report a dry run that could not get far enough to build a plan.

    An operator running `--dry-run` before they have, say, a Postgres DSN
    is asking exactly the question `--dry-run` exists to answer. Handing
    them an error envelope instead of a plan makes the flag useless in the
    situation it is most needed, so the failure is reported as a plan
    blocker with `plan_complete: false` — never as a plan with an empty
    file list, which would read as "this would write nothing".

    `resolvable_interactively` separates the two reasons an answer can be
    missing. Without it, a dry run on a terminal reports "a real run would
    fail" for a command that would in fact succeed by prompting — an
    answer to a different question than the one asked.
    """
    from okg.substrate.cli.main import _emit_ok  # noqa: F401
    from okg.substrate.errors import SubstrateError  # noqa: F401

    fix_hint = getattr(error, "fix_hint", None)
    payload: dict[str, Any] = {
        "status": "dry_run_blocked",
        "dry_run": True,
        "plan_complete": False,
        "blockers": [] if resolvable_interactively else [str(error)],
        "blockers_resolvable_interactively": (
            [str(error)] if resolvable_interactively else []
        ),
        "next_step": (
            "re-run without --dry-run and answer the prompt, or supply the "
            "answer as a flag and re-run with --dry-run for a full plan"
            if resolvable_interactively
            else "supply the missing answers above, then re-run with --dry-run"
        ),
    }
    if profile is not None:
        payload["profile"] = {
            "name": profile.name, "version": profile.version,
        }
    if isinstance(error, SubstrateError):
        payload["error_code"] = error.code
    if fix_hint:
        payload["fix_hint"] = fix_hint
    _emit_ok(payload, as_json=as_json)
    return 1


def _cmd_init_from_profile(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg init --profile <name>` (Layer 2A).

    Pipeline:
      1. Discover profile (env / cwd / in-tree library).
      2. Gather init answers (CLI flags > prompts > defaults).
      3. Build a ScaffoldPlan; refuse to clobber existing
         deployment unless --force --yes.
      4. Write deployment.yaml, source_registry.yaml,
         invariants.yaml, dashboards/.
      5. Migrate schema for the deployment's DSN.
      6. Compose catalog; report skipped-extends as YELLOW.
      7. Apply catalog + run first publish.
      8. Emit summary.

    Steps 5–7 are best-effort under --no-publish (default off);
    operators on a system without Postgres can pass --no-publish
    to just scaffold the files.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    from okg.substrate.deployment_bootstrap.profile_init import (
        build_plan,
        discover_profile,
        redacted_init_answers,
        scaffold_deployment,
    )

    # Read once, up front: every branch below has to know whether it is
    # allowed to touch the disk.
    dry_run = bool(getattr(args, "dry_run", False))

    try:
        profile = discover_profile(args.profile_name)
    except SubstrateError as e:
        # Preserve profile discovery and archived-admission error codes.
        return _emit_error(e, as_json=as_json)

    try:
        raw_answers = _gather_profile_answers(
            args, profile=profile, as_json=as_json,
        )
        _validate_workspace_graph_repo_root(
            profile_name=profile.name,
            answers=raw_answers,
        )
    except SubstrateError as e:
        if dry_run:
            return _emit_incomplete_dry_run_plan(
                error=e, profile=profile, as_json=as_json,
            )
        return _emit_error(e, as_json=as_json)

    deployment_name = (
        getattr(args, "deployment_name", None)
        or raw_answers.get("deployment_name")
    )
    if not deployment_name:
        missing_name = SubstrateError(
            "okg init --profile requires a deployment name "
            "(supply --deployment-name <name> or answer the "
            "deployment_name init question)",
            fix_hint=(
                "pass --deployment-name=<slug> or include a "
                "deployment_name init_question in the profile"
            ),
        )
        if dry_run:
            return _emit_incomplete_dry_run_plan(
                error=missing_name, profile=profile,
                resolvable_interactively=_would_prompt_without_dry_run(args),
                as_json=as_json,
            )
        return _emit_error(missing_name, as_json=as_json)

    # Per `separate-framework-and-deployments` (2026-05-22):
    # default to the user-state XDG dir
    # (`${XDG_DATA_HOME:-$HOME/.local/share}/okg/deployments/`)
    # rather than cwd-relative `deployments/`. Explicit
    # `--deployments-root` and `OKG_DEPLOYMENTS_DIR` env var
    # both override.
    #
    # Issue #2127. ONE existence rule, shared with the academic-researcher installer
    # (`_install_deployments_root`):
    #   --deployments-root   the operator named this root for this scaffold,
    #                        so creating it is part of scaffolding.
    #   OKG_DEPLOYMENTS_DIR  an ambient pointer at an EXISTING portfolio.
    #                        A path that does not exist is refused, per the
    #                        resolver contract in `deployments/__init__.py`
    #                        ("set but not pointing at an existing directory
    #                        raises rather than silently falling through").
    #                        Creating a second root because the variable was
    #                        misspelled is the silent fallback that rule
    #                        exists to prevent.
    # Both paths `expanduser()`, so a quoted `~/roots` is the operator home
    # and never a literal directory named `~`.
    from okg.substrate.deployments import user_state_deployments_root
    explicit_root = getattr(args, "deployments_root", None)
    if explicit_root:
        deployments_root = Path(explicit_root).expanduser().resolve()
    else:
        env_root = os.environ.get("OKG_DEPLOYMENTS_DIR")
        if env_root:
            deployments_root = Path(env_root).expanduser()
            if not deployments_root.is_dir():
                return _emit_error(
                    deployments_env_root_error(
                        f"OKG_DEPLOYMENTS_DIR={str(deployments_root)!r} "
                        f"is not an existing directory",
                        command="okg init",
                    ),
                    as_json=as_json,
                )
            deployments_root = deployments_root.resolve()
        else:
            deployments_root = user_state_deployments_root().resolve()
    # `--dry-run` promises to touch nothing, and creating the
    # deployments root is a touch. Everything from here to the
    # scaffold write is read-only, so the plan can still be built.
    if not dry_run:
        deployments_root.mkdir(parents=True, exist_ok=True)

    try:
        plan = build_plan(
            profile=profile,
            deployment_name=deployment_name,
            deployments_root=deployments_root,
            raw_answers=raw_answers,
        )
    except SubstrateError as e:
        # Typically a required init answer that was never supplied — and
        # on a terminal, one a real run would simply have prompted for.
        if dry_run:
            return _emit_incomplete_dry_run_plan(
                error=e, profile=profile,
                resolvable_interactively=_would_prompt_without_dry_run(args),
                as_json=as_json,
            )
        return _emit_error(e, as_json=as_json)

    force = bool(getattr(args, "force", False))
    confirm = bool(getattr(args, "yes", False))
    if force and not confirm:
        return _emit_error(
            SubstrateError(
                "--force requires --yes to overwrite an existing "
                "deployment directory",
                fix_hint="re-run with --force --yes",
            ),
            as_json=as_json,
        )

    if dry_run:
        # Everything above this line is a read: profile discovery,
        # answer gathering, plan construction. Return here and no
        # file, directory, schema migration, or publish happens.
        return _emit_scaffold_dry_run_plan(
            plan=plan,
            profile=profile,
            deployment_name=deployment_name,
            deployments_root=deployments_root,
            force=force,
            no_publish=bool(getattr(args, "no_publish", False)),
            as_json=as_json,
            no_agent_config=bool(getattr(args, "no_claude_config", False)),
            explicit_root=bool(explicit_root),
        )

    try:
        result = scaffold_deployment(plan, force=force)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    payload: dict[str, Any] = {
        "profile": {"name": profile.name, "version": profile.version},
        "deployment": {
            "name": deployment_name,
            "dir": str(result.deployment_dir),
            "modules": list(plan.modules),
        },
        "files_written": [str(p) for p in result.files_written],
        "answers": redacted_init_answers(profile, plan.answers),
    }

    # Compose catalog to surface skipped-extends as a YELLOW
    # diagnostic (init still succeeds). We do this even when
    # --no-publish is supplied; the diagnostic is cheap.
    verbose = bool(getattr(args, "verbose", False))
    warnings: list[dict[str, str]] = []
    # The warning for the stage that stopped the install. The fatal summary
    # below states the same cause in order, so the short text leaves this
    # one out: its message carries the failed command's stderr tail, which
    # is a screen of log lines (#3928). `--verbose` and `--json` keep it.
    stage_failure_warning: dict[str, str] | None = None
    skipped_extends: list[dict[str, str]] = []
    try:
        from okg.substrate.catalog.composition import resolve as _resolve

        resolved = _resolve(plan.modules)
        skipped_extends = list(resolved.diagnostics.skipped_extends)
        for s in skipped_extends:
            warnings.append({
                "code": "skipped_extends",
                "source_module": s.get("source", ""),
                "target_module": s.get("target", ""),
                "reason": s.get("reason", ""),
            })
        payload["composition"] = {
            "fired_extends": [list(t) for t in resolved.diagnostics.fired_extends],
            "skipped_extends": skipped_extends,
            "rule_files_loaded": list(
                resolved.diagnostics.rule_files_loaded,
            ),
        }
    except Exception as e:  # noqa: BLE001
        warnings.append({
            "code": "composition_probe_failed",
            "message": str(e),
        })

    # --no-publish skips schema migration + first publish.
    do_publish = not bool(getattr(args, "no_publish", False))
    fatal_failure = False
    if do_publish:
        publish_outcome = _run_init_migrate_and_publish(
            deployment_name=deployment_name,
            deployment_dir=result.deployment_dir,
            answers=plan.answers,
            # Issue #3414: a profile with no source defaults (the minimal
            # starter) renders an empty registry by design.
            empty_registry_expected=not plan.source_defaults,
            quiet_notices=not verbose and not as_json,
        )
        payload["publish"] = publish_outcome
        # Decision 23a: a first publish whose lock hold passed the capacity
        # bar still succeeds; keep the warning visible. Exit code unchanged.
        from okg.substrate.publish_proof import surface_capacity_warning

        capacity_warning = surface_capacity_warning(
            (publish_outcome.get("details") or {}).get("publish_proof"),
            surface="okg init first publish",
        )
        if capacity_warning is not None:
            payload["capacity_warning"] = capacity_warning
        if not publish_outcome.get("ok", False):
            # Per `fix-init-scaffold-provability`
            # (req.init.fail-loud-first-publish): unless --no-publish was
            # requested, EVERY bring-up failure is fatal — init must not
            # exit 0 reporting success over a failed catalog load, a
            # failed/empty first publish, or a missing schema. The
            # warning code names the failing stage so operators and
            # wrappers (okg install) can dispatch on it.
            stage = publish_outcome.get("stage") or "publish"
            stage_codes = {
                "dsn_unresolved": "postgres_dsn_unresolved",
                "database_missing": "database_missing",
                "migrate": "schema_migration_failed",
                "nomos_baseline": "nomos_baseline_failed",
                "catalog": "catalog_load_failed",
                "ingest": "ingest_failed",
                "publish": "first_publish_failed",
                "publish_proof": "first_publish_empty",
            }
            stage_failure_warning = {
                "code": stage_codes.get(stage, "first_publish_failed"),
                "message": publish_outcome.get("message", ""),
            }
            warnings.append(stage_failure_warning)
            payload["failed_stage"] = stage
            # A stage that classified its own fix (e.g. the literal
            # createdb command for a missing database) wins over the
            # generic build-sequence recovery.
            payload["fix_hint"] = publish_outcome.get("fix_hint") or (
                "fix the failing stage, then re-run the build sequence "
                f"manually: okg migrate --deployment {deployment_name} "
                f"--apply; okg catalog load --deployment <deployment-dir> "
                f"--apply; okg ingest --deployment {deployment_name} "
                f"--progress; okg run --once --deployment {deployment_name} "
                "--apply (see "
                + doc_reference("docs/runbooks/troubleshooting/first-run.md")
                + ")"
            )
            fatal_failure = True

    if warnings:
        payload["warnings"] = warnings
        if not as_json:
            # The short text lists only warnings the user must act on;
            # `--verbose` lists them all. `--json` carries all of them.
            shown = warnings if verbose else actionable_warnings(warnings)
            for w in shown:
                if w is stage_failure_warning and not verbose:
                    continue
                print(warning_line(w), file=sys.stderr)
                fix = warning_fix(w, deployment_name)
                if fix is not None:
                    print(fix, file=sys.stderr)

    next_commands: list[str] = []
    if explicit_root:
        next_commands.append(
            f"export OKG_DEPLOYMENTS_DIR={shlex.quote(str(deployments_root))}",
        )
    # `okg migrate --apply` comes FIRST and unconditionally (issue #2128,
    # finding F7). The scaffold only writes files; it migrates the database
    # only when the operator pinned a DSN at init time, and the previous
    # list sent every other operator straight into `run --once` against an
    # unmigrated database. Migration is idempotent, so naming it on the
    # already-migrated path costs a no-op rather than a wrong instruction.
    next_commands.extend([
        f"okg migrate --deployment {deployment_name} --apply",
        f"okg doctor --deployment {deployment_name}",
        f"okg run --once --deployment {deployment_name} --apply",
        f"okg status --deployment {deployment_name}",
    ])
    if (payload.get("publish") or {}).get("no_runnable_sources"):
        # Issue #2331: the install succeeded with an empty graph. Say so, and
        # put the command that adds a first source ahead of the rest.
        payload["setup_status"] = SETUP_STATUS_AWAITING_APPROVAL
        next_commands.insert(
            1 if explicit_root else 0, add_source_next_step(deployment_name),
        )
        if not as_json and verbose:
            # The short summary says this itself, with the same command.
            print(
                "No source is runnable yet, so the first ingest was skipped "
                "and an empty graph was published. Add a source with: "
                f"{add_source_next_step(deployment_name)}",
                file=sys.stderr,
            )
    stale_claim = (
        ((payload.get("publish") or {}).get("details") or {})
        .get("ingest_failure") or {}
    ).get("code") in _OWNERSHIP_DRIFT_CODES
    if stale_claim:
        # The publish was refused for a stale claim (#3966): the run command
        # below fails the same way until the operator claims the new files.
        run_at = next(
            i for i, command in enumerate(next_commands)
            if command.startswith("okg run --once")
        )
        next_commands.insert(run_at, _claim_command(result.deployment_dir))
    payload["next"] = next_commands
    if not fatal_failure:
        # PACT agent-connect-3412: connect the agents present, read-only;
        # print the command for the rest. Never fails the install.
        from okg.substrate.library.agent_runtimes.connect import (
            connect_after_install,
        )

        skip = None
        if getattr(args, "no_claude_config", False):
            skip = "--no-claude-config was given"
        elif not do_publish:
            skip = (
                "--no-publish was given; the server starts only after a "
                "first publish"
            )
        agents_step = connect_after_install(
            deployment_name,
            deployment_dir=result.deployment_dir,
            skip_reason=skip,
            deployments_root=deployments_root if explicit_root else None,
        )
        payload["agents"] = agents_step
        for command in agents_step.get("next_commands") or ():
            # The role-password step and the connect after it (#3500).
            if command not in payload["next"]:
                payload["next"].append(command)
        if not as_json:
            if verbose:
                print(agents_step["message"], file=sys.stderr)
            else:
                # The summary names the connect command; a refusal needs
                # its own line because it carries the fix.
                failure = agent_failure_line(agents_step)
                if failure is not None:
                    print(failure, file=sys.stderr)
    if fatal_failure:
        # Emit the payload so the operator sees the diagnostic, then
        # propagate a non-zero exit so wrappers (notably `okg install`)
        # short-circuit instead of cascading into a broken ingest.
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            _print_fatal_install_summary(payload)
        return 1
    if not as_json and not verbose:
        print(render_install_summary(
            payload,
            agents_step=payload.get("agents"),
            env_prefix=deployments_env_prefix(
                deployments_root, explicit=bool(explicit_root),
            ),
        ))
        return 0
    if not as_json and "agents" in payload:
        # The agent step's message already went to stderr above; rendering
        # the same block again in the summary printed every "not connected"
        # line twice. JSON output keeps the full block.
        payload = {key: value for key, value in payload.items() if key != "agents"}
    _emit_ok(payload, as_json=as_json)
    return 0


def _resolve_install_answer_dsn(raw_dsn: str) -> str:
    """Expand ``${VAR}`` references in the operator's Postgres DSN answer.

    `okg install` / `okg init` document `${OKG_DSN}` as the value to give
    for the database question, and `cli/main.py`'s deployment manifest
    example prints exactly that. Every command run AFTER the install
    resolves it: the manifest resolvers run `_interpolate_env` and the
    deployments loader runs `_resolve_postgres_dsn`. The installer's own
    migrate stage did not, so it handed libpq the literal text and died
    with `missing "=" after "${OKG_DSN}" in connection info string`
    (issue #1948).

    This expands the answer for IN-PROCESS use only. The scaffold keeps
    writing the operator's literal answer into `deployment.yaml`'s
    `postgres.dsn` (see `deployment_bootstrap/profile_init.py`), so a
    placeholder answer leaves the secret out of the manifest on disk and
    the deployment keeps resolving it from the environment at every later
    run.

    Fails closed, per the `${OKG_DSN}`-is-not-a-silent-fallback rule at
    `cli/main.py` lines 487-488 and the 2026-05-11 silent-worker-failure
    note on `_resolve_postgres_dsn`: an unset variable raises a
    `SubstrateError` naming the variable BEFORE any database call, rather
    than passing the literal downstream. The error names only variable
    names, never the expanded DSN, which carries the password.
    """
    import re

    from okg.substrate.cli.main import _interpolate_env
    from okg.substrate.errors import SubstrateError

    expanded = _interpolate_env(str(raw_dsn)).strip()
    unresolved = sorted(set(
        re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", expanded),
    ))
    if unresolved:
        joined = ", ".join(unresolved)
        raise SubstrateError(
            f"the Postgres DSN answer references unset environment "
            f"variable(s): {joined}. Refusing to send the unexpanded "
            f"placeholder to Postgres.",
            data={"unresolved": unresolved},
            fix_hint=(
                f"export {unresolved[0]}=postgresql://... and re-run, or "
                f"answer the database question with a literal "
                f"postgresql://... DSN"
            ),
        )
    if not expanded:
        raise SubstrateError(
            "the Postgres DSN answer expanded to an empty string",
            fix_hint=(
                "answer the database question with a literal "
                "postgresql://... DSN, or set the environment variable it "
                "references to a non-empty value"
            ),
        )
    return expanded


def _run_init_migrate_and_publish(
    *,
    deployment_name: str,
    deployment_dir: Path,
    answers: dict[str, Any],
    empty_registry_expected: bool = False,
    quiet_notices: bool = False,
) -> dict[str, Any]:
    """Best-effort: migrate, ingest, and publish the deployment.

    ``quiet_notices`` drops the migrate step's one informational log line
    about a multi-phase migration that this same migrate call then runs
    (#3891). The line asks the reader for nothing; if the migration did not
    finish, migrate fails here and `okg doctor` reports it as
    ``multiphase_migration_pending``.

    ``empty_registry_expected`` is True only for a profile that declares no
    source defaults (issue #3414, the minimal starter): its empty registry
    is the design, so the first publish is an empty generation. Otherwise a
    registry with no source is a broken scaffold and fails the ingest stage.

    Returns `{"ok": bool, "message": str, "details": {...}}`. The
    init CLI surfaces this in the summary payload; failures are
    informational (the operator can re-run the steps manually).

    Skips both steps if `postgres_dsn` is not present in the
    init answers — the operator hasn't pinned a DSN yet; they
    can run `okg migrate`, `okg ingest`, and `okg run --once`
    manually.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    raw_dsn = answers.get("postgres_dsn")
    if not raw_dsn:
        return {
            "ok": True,
            "message": "no postgres_dsn answer; skipped migrate + publish",
            "details": {"skipped": True},
        }
    # Issue #1948: the documented answer is the placeholder `${OKG_DSN}`,
    # and the scaffold persists that literal text in deployment.yaml so the
    # secret never lands on disk. Everything below this line opens real
    # connections or hands the value to a child process, so it needs the
    # EXPANDED value. An unset variable refuses here, before the first
    # database call, instead of reaching libpq as a literal.
    try:
        dsn = _resolve_install_answer_dsn(raw_dsn)
    except SubstrateError as e:
        return {
            "ok": False,
            "fatal": True,
            "stage": "dsn_unresolved",
            "message": str(e),
            "fix_hint": e.fix_hint,
            "details": {},
        }
    details: dict[str, Any] = {}
    deployments_root = deployment_dir.parent.resolve()
    old_deployments_root = os.environ.get("OKG_DEPLOYMENTS_DIR")
    try:
        from okg.substrate.db.migrate import (
            ensure_required_extensions,
            migrate,
        )

        # Provision parity (H1, fix-init-scaffold-provability review): the
        # bundled Postgres stack installs the required extensions only into
        # its initial database, so a fresh `createdb <db-name>` + init died
        # with ExtensionMissing at migrate. Ensure them exactly as
        # `okg provision` does before migrating.
        ext = ensure_required_extensions(dsn)
        details["extensions"] = ext
        # `migrate()` takes a DSN string and opens its own
        # connection; passing a psycopg.Connection here crashed
        # with `'Connection' object has no attribute 'encode'`
        # (dogfood F-4a). The deployment_name kwarg lets the
        # migrate routine pick up any deployment-side views.sql.
        os.environ["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
        with _migrate_notices(quiet=quiet_notices):
            migrate(dsn, deployment_name=deployment_name)
        details["migrate"] = "ok"
        details["deployments_root"] = str(deployments_root)
    except Exception as e:  # noqa: BLE001
        # A DSN whose database does not exist is the most likely
        # cold-start failure: classify it into the one createdb command
        # that fixes it (req.provision.cold-start-hint covers init too).
        from okg.substrate.cli._db_bootstrap_errors import (
            classify_missing_database_error,
        )

        missing = classify_missing_database_error(
            e, dsn=dsn, surface="okg init",
        )
        if missing is not None:
            return {
                "ok": False,
                "fatal": True,
                "stage": "database_missing",
                "message": str(missing),
                "fix_hint": missing.fix_hint,
                "details": details,
            }
        return {
            "ok": False,
            # Migrate failure is FATAL — nothing downstream works
            # without the schema. The init caller SHALL propagate
            # this as a non-zero exit so `okg install` short-
            # circuits instead of cascading into a bogus filtered
            # ingest (dogfood F-4b).
            "fatal": True,
            "stage": "migrate",
            "message": f"schema migration failed: {e}",
            "details": details,
        }
    finally:
        if old_deployments_root is None:
            os.environ.pop("OKG_DEPLOYMENTS_DIR", None)
        else:
            os.environ["OKG_DEPLOYMENTS_DIR"] = old_deployments_root
    try:
        baseline = _seed_init_nomos_baseline_if_required(
            deployment_name=deployment_name,
            deployment_dir=deployment_dir,
            dsn=dsn,
        )
        if baseline is not None:
            details["nomos_baseline"] = baseline
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "stage": "nomos_baseline",
            "message": f"Nomos baseline setup failed: {e}",
            "details": details,
        }
    # Catalog load + ingest + first publish are heavy and require
    # the deployment to be discoverable by the canonical loader.
    # We defer to the existing CLI codepaths via subprocess to avoid
    # re-implementing them here. Per `fix-init-scaffold-provability`
    # (req.init.fail-loud-first-publish) every failure below names its
    # stage and the init CLI exits nonzero — init must not report
    # success over an empty or unpublished graph.
    # Issue #2313: every child starts through `running_okg_argv()` (`-P`),
    # so the working directory this process was started in -- which can be
    # any operator folder -- never shadows an installed module.
    try:
        import subprocess

        env = os.environ.copy()
        env["OKG_DSN"] = dsn
        env["OKG_DEPLOYMENTS_DIR"] = str(deployments_root)
        cmd_load = [
            *running_okg_argv(),
            "catalog", "load",
            "--deployment", str(deployment_dir),
            "--apply",
        ]
        load_proc = subprocess.run(
            cmd_load, capture_output=True, text=True, env=env,
        )
        details["catalog_load_returncode"] = load_proc.returncode
        if load_proc.returncode != 0:
            return {
                "ok": False,
                "stage": "catalog",
                "message": (
                    f"catalog load failed: {command_output_tail(load_proc)}"
                ),
                "details": details,
            }
        # Issue #2331. A profile whose every source waits for setup
        # approval (academic-researcher) gives a bare `okg ingest` nothing to run,
        # and that command refuses an empty plan. That is the expected state
        # of a fresh install, not an ingest failure: skip the step by name
        # and publish the empty graph. A plan that selects any source still
        # runs the child, so a runnable source that fails still fails init.
        try:
            selection = bare_ingest_selection(deployment_dir)
        except Exception as e:  # noqa: BLE001
            return {
                "ok": False,
                "stage": "ingest",
                "message": f"could not plan the first ingest: {e}",
                "details": details,
            }
        details["ingest_selection"] = selection
        if (
            not selection["runnable"]
            and not selection["awaiting_setup"]
            and not empty_registry_expected
        ):
            # A registry that declares no source at all is a broken scaffold,
            # not a deployment waiting for setup approval -- unless the
            # profile declares no source defaults (the minimal starter).
            return {
                "ok": False,
                "stage": "ingest",
                "message": (
                    "the source registry declares no sources, so the first "
                    "ingest has nothing to run or to wait for"
                ),
                "details": details,
            }
        published_before = None
        if not selection["runnable"]:
            # Recorded before the publish so the proof below can require a
            # generation this run created, scoped to this deployment.
            try:
                published_before = _deployment_published_generation(
                    dsn, deployment_name=deployment_name,
                )
            except Exception as e:  # noqa: BLE001
                return {
                    "ok": False,
                    "stage": "publish_proof",
                    "message": (
                        "could not read this deployment's published "
                        f"generation before the first publish: {e}"
                    ),
                    "details": details,
                }
        if selection["runnable"]:
            cmd_ingest = [
                *running_okg_argv(),
                "ingest",
                "--deployment", deployment_name,
                "--progress",
                # The report comes back as JSON so a failure can name each
                # failed source's own error. A text report, cut to its last
                # few thousand characters, dropped exactly that line.
                "--json",
            ]
            ingest_proc = subprocess.run(
                cmd_ingest, capture_output=True, text=True, env=env,
            )
            details["ingest_returncode"] = ingest_proc.returncode
            if ingest_proc.returncode != 0:
                failed_sources = ingest_failed_sources(ingest_proc.stdout)
                if failed_sources:
                    details["failed_sources"] = failed_sources
                    stderr_tail = command_output_tail(
                        subprocess.CompletedProcess(
                            args=cmd_ingest,
                            returncode=ingest_proc.returncode,
                            stdout="",
                            stderr=ingest_proc.stderr,
                        ),
                    )
                    message = (
                        "ingest failed: "
                        + "; ".join(
                            _failed_source_line(item) for item in failed_sources
                        )
                        + f"\n{stderr_tail}"
                    )
                    others = ingest_other_sources_message(ingest_proc.stdout)
                    if others:
                        details["other_sources"] = others
                    ingest_fix = _ingest_failure_fix(
                        deployment_name,
                        [item["source"] for item in failed_sources],
                    )
                else:
                    # No failed source. Every source can have run and the
                    # command still exit 1: its final publish failed or was
                    # blocked, or it refused before running (an error
                    # envelope). Name that cause before falling back to the
                    # raw streams.
                    ingest_fix = None
                    cause = ingest_publish_failure(
                        ingest_proc.stdout, ingest_proc.stderr,
                    )
                    if cause is not None:
                        details["ingest_failure"] = cause
                        code = f" [{cause['code']}]" if cause.get("code") else ""
                        message = f"ingest failed: {cause['message']}{code}"
                        ingest_fix = cause.get("fix_hint")
                        if cause.get("code") in _OWNERSHIP_DRIFT_CODES:
                            ingest_fix = _ownership_drift_fix(
                                deployment_name, deployment_dir,
                            )
                    else:
                        # Neither a report nor an envelope: both raw
                        # streams are all there is.
                        message = (
                            "ingest failed: "
                            f"{command_output_tail(ingest_proc)}"
                        )
                outcome: dict[str, Any] = {
                    "ok": False,
                    "stage": "ingest",
                    "message": message,
                    "details": details,
                }
                if ingest_fix:
                    outcome["fix_hint"] = ingest_fix
                return outcome
        else:
            details["ingest"] = {
                "skipped": True,
                "reason": "no_runnable_sources",
            }
        cmd_run = [
            *running_okg_argv(),
            "run", "--once",
            "--deployment", deployment_name,
            "--apply",
            "--json",
        ]
        run_proc = subprocess.run(
            cmd_run, capture_output=True, text=True, env=env,
        )
        details["run_returncode"] = run_proc.returncode
        # Issue #2422: the proof below checks exactly the version this run
        # reports, never whichever version is latest in the database.
        run_outcome = _run_once_outcome(run_proc)
        details["run_outcome"] = run_outcome
        if run_proc.returncode != 0:
            if run_outcome["outcome"] == "blocked" and run_outcome["generation_id"]:
                gen = run_outcome["generation_id"]
                message = (
                    f"first publish was blocked: graph version {gen} failed "
                    "its checks and was not published; run `okg generation "
                    f"describe {gen} --deployment {deployment_name} --json` "
                    "to see them"
                )
            else:
                message = (
                    f"first publish failed: "
                    f"{command_output_tail(run_proc)}"
                )
            return {
                "ok": False,
                "stage": "publish",
                "message": message,
                "details": details,
            }
        if (
            run_outcome["outcome"] not in ("published", "no_change")
            or not run_outcome["generation_id"]
        ):
            return {
                "ok": False,
                "stage": "publish_proof",
                "message": (
                    "first publish proved nothing: `okg run --once` reported "
                    f"outcome {run_outcome['outcome']!r} and graph version "
                    f"{run_outcome['generation_id']!r}, not a published "
                    "version"
                ),
                "details": details,
            }
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "stage": "publish",
            "message": f"first publish raised: {e}",
            "details": details,
        }
    if not selection["runnable"]:
        # Issue #2331. Nothing ran, so the graph is legitimately empty. The
        # proof is a published generation that belongs to THIS deployment,
        # is newer than the one recorded before the publish, and whose live
        # rows can be counted. No unscoped fallback: every migrated database
        # already holds generation 0 of the `bootstrap` deployment, which
        # must never pass for this deployment's publish.
        from okg.substrate.cli.provision import _generation_row_counts

        # A new version is required: this run published it (#2422).
        published_after = (
            run_outcome["generation_id"]
            if run_outcome["outcome"] == "published"
            else None
        )
        generation_id, node_count, edge_count = (None, None, None)
        if published_after is not None:
            generation_id, node_count, edge_count = _generation_row_counts(
                dsn,
                generation_id=published_after,
                deployment_name=deployment_name,
            )
        details["publish_proof"] = {
            "generation_before": published_before,
            "generation_id": generation_id,
            "node_count": node_count,
            "edge_count": edge_count,
            "rule": "no_runnable_sources_empty_generation",
        }
        if (
            published_after is None
            or published_after == published_before
            or node_count is None
            or edge_count is None
        ):
            return {
                "ok": False,
                "stage": "publish_proof",
                "message": (
                    "first publish proved nothing: no source was runnable "
                    "and no new, readable published generation exists for "
                    f"deployment {deployment_name!r}"
                ),
                "details": details,
            }
        return {
            "ok": True,
            "no_runnable_sources": True,
            "message": (
                "first ingest skipped: no source is runnable until one is "
                f"added or approved; published generation {generation_id} "
                f"with {node_count} records and {edge_count} relationships"
            ),
            "details": details,
        }
    # The same real-publish gate provision applies: a "successful" first
    # publish whose observed database state has no non-empty published
    # generation is a failure, not a success with a warning
    # (req.init.fail-loud-first-publish; provision's proof helper reads
    # observed state when handed no in-run publish payload).
    from okg.substrate.cli.provision import _provision_publish_proof

    proof = _provision_publish_proof(
        dsn, {}, deployment_name=deployment_name,
        expected_generation_id=run_outcome["generation_id"],
    )
    details["publish_proof"] = proof
    if not proof.get("ok"):
        return {
            "ok": False,
            "stage": "publish_proof",
            "message": (
                "first publish proved nothing: "
                f"{proof.get('reason') or 'no non-empty published generation'}"
            ),
            "details": details,
        }
    return {"ok": True, "message": "first publish complete", "details": details}


@contextlib.contextmanager
def _migrate_notices(*, quiet: bool) -> Iterator[None]:
    """Hold back migrate's multi-phase notice while ``quiet`` is true.

    Only that one message is dropped; every other log record from the
    migrate module, warnings and errors included, still prints.
    """
    if not quiet:
        yield
        return

    class _MultiphaseNotice(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return not (
                record.levelno == logging.WARNING
                and record.getMessage().startswith("migrate: ")
                and "declared multi-phase migration(s) deferred" in record.getMessage()
            )

    notice_filter = _MultiphaseNotice()
    migrate_log = logging.getLogger("okg.substrate.db.migrate")
    migrate_log.addFilter(notice_filter)
    try:
        yield
    finally:
        migrate_log.removeFilter(notice_filter)


def _run_once_outcome(proc: Any) -> dict[str, Any]:
    """The outcome and version `okg run --once --json` reported.

    Both run paths print one JSON object: the in-process path nests its
    verdict under ``run_summary``; the runtime-queue path reports
    ``outcome`` and ``generation_id`` at the top level. Anything else reads
    as ``unknown`` with no version.
    """
    payload: Any = None
    text = (proc.stdout or "").strip()
    for candidate in (text, text[text.find("{"):] if "{" in text else ""):
        if not candidate:
            continue
        try:
            payload = json.loads(candidate)
            break
        except ValueError:
            continue
    if not isinstance(payload, dict):
        return {"outcome": "unknown", "generation_id": None}
    verdict = payload.get("run_summary")
    if not isinstance(verdict, dict):
        verdict = payload
    generation_id = verdict.get("generation_id")
    return {
        "outcome": str(verdict.get("outcome") or "unknown"),
        "generation_id": str(generation_id) if generation_id else None,
    }


def _deployment_published_generation(
    dsn: str, *, deployment_name: str,
) -> str | None:
    """This deployment's latest published Chronos generation, or None.

    Issue #2331. Scoped to the deployment with no fallback. Provision's
    `_latest_published_generation` falls back to the highest published id
    of ANY deployment, which on a fresh database is the migration's
    generation 0 of `bootstrap`. A read failure raises; callers fail closed.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        latest = store.latest_generation(
            deployment_name=deployment_name, status="published",
        )
    finally:
        store.close()
    return None if latest is None else latest.checkpoint_id


def _ingest_reports(stdout: str | None) -> list[dict[str, Any]]:
    """Each ingest report in the ``okg ingest --json`` output on ``stdout``.

    Two report shapes exist. The inline path prints the ingest report
    itself, with ``results`` at the top. With the runtime on, the queued
    path prints ``{"report": <ingest report>, ...}`` and, for more than one
    batch, ``workflows: [{"report": ...}, ...]`` (one entry per batch, the
    last one repeating ``report``).
    """
    text = (stdout or "").strip()
    payload: Any = None
    for candidate in (text, text.splitlines()[-1] if text else ""):
        try:
            payload = json.loads(candidate)
            break
        except ValueError:
            continue
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("results"), list):
        reports: list[Any] = [payload]
    elif isinstance(payload.get("workflows"), list):
        reports = [
            item.get("report")
            for item in payload["workflows"]
            if isinstance(item, dict)
        ]
    else:
        reports = [payload.get("report")]
    return [
        report for report in reports
        if isinstance(report, dict) and isinstance(report.get("results"), list)
    ]


def ingest_failed_sources(stdout: str | None) -> list[dict[str, str]]:
    """Each failed source in an ``okg ingest --json`` report on ``stdout``.

    A result whose status is not ``OK`` is a failure, the rule the ingest
    service itself uses. Returns an empty list when ``stdout`` holds no
    report with results (for example, the command refused before it ran a
    source and wrote its error to stderr).
    """
    failed: list[dict[str, str]] = []
    for report in _ingest_reports(stdout):
        for item in report["results"]:
            if not isinstance(item, dict) or item.get("status") == "OK":
                continue
            failed.append({
                "source": str(
                    item.get("name") or item.get("source") or "unnamed",
                ),
                "stage": str(item.get("stage") or ""),
                "error": str(item.get("error") or "no error was recorded"),
            })
    return failed


def ingest_other_sources_message(stdout: str | None) -> str | None:
    """What happened to the other sources, from an ``okg ingest --json`` report.

    The ingest report carries it under ``other_sources`` when a source failed
    (#3927). Returns None when no report has it.
    """
    from okg.substrate.ingest.failure_followup import REPORT_KEY

    messages = [
        str(report[REPORT_KEY]["message"])
        for report in _ingest_reports(stdout)
        if isinstance(report.get(REPORT_KEY), dict)
        and report[REPORT_KEY].get("message")
    ]
    return messages[-1] if messages else None


def ingest_publish_failure(
    stdout: str | None, stderr: str | None,
) -> dict[str, str] | None:
    """Why `okg ingest --json` exited 1 when no source failed, or None.

    First the report's own publishes: a failed or blocked publish carries the
    error, its code and a fix. Then the error envelope the command prints on
    stderr when it refuses before running a source. None when neither is
    there (the caller then shows the raw streams).
    """
    from okg.substrate.cli.run_summary import _blocked_detail
    from okg.substrate.publish_outcome import BLOCKED, FAILED, classify_publish

    for report in reversed(_ingest_reports(stdout)):
        for entry in reversed(report.get("publishes") or []):
            if not isinstance(entry, dict):
                continue
            outcome = classify_publish(entry)
            if outcome.kind == FAILED:
                found = {
                    "message": "the final publish failed: "
                    + str(entry.get("error") or outcome.reason
                          or "no error was recorded"),
                }
            elif outcome.kind == BLOCKED:
                found = {
                    "message": "the final publish was blocked: "
                    + _blocked_detail(
                        entry.get("blocked_reason") or entry.get("reason"),
                    ),
                }
            else:
                continue
            if entry.get("code"):
                found["code"] = str(entry["code"])
            if outcome.fix_hint:
                found["fix_hint"] = outcome.fix_hint
            return found
    for text in (stderr, stdout):
        for line in reversed((text or "").strip().splitlines()[-200:]):
            try:
                payload = json.loads(line)
            except ValueError:
                continue
            error = payload.get("error") if isinstance(payload, dict) else None
            if isinstance(error, dict) and error.get("message"):
                found = {"message": str(error["message"])}
                if error.get("code"):
                    found["code"] = str(error["code"])
                if error.get("fix_hint"):
                    found["fix_hint"] = str(error["fix_hint"])
                return found
    return None


# Publish refusals that mean the database still holds the identity claimed
# before `--force` rewrote the deployment files (#3966).
_OWNERSHIP_DRIFT_CODES = frozenset({
    "catalog_manifest_drifted",
    "catalog_applied_version_absent",
})


def _claim_command(deployment_dir: Path) -> str:
    return (
        "okg catalog ownership claim --deployment "
        f"{shlex.quote(str(deployment_dir))}"
    )


def _run_once_command(deployment_name: str) -> str:
    return (
        f"okg run --once --deployment {shlex.quote(deployment_name)} --apply"
    )


def _ownership_drift_fix(deployment_name: str, deployment_dir: Path) -> str:
    """The recovery text for a publish refused because the claim is stale.

    The install never claims ownership itself: a claim changes who may
    publish to this database, so the operator runs it. The wording holds
    whether or not an earlier publish in the same run succeeded.
    """
    return (
        "the final publish was refused (the deployment files drifted from "
        "the identity this database claimed; `--force` rewrites them); "
        "`okg status` shows what was published. If the new files are the "
        f"ones this database should publish, run `{_claim_command(deployment_dir)}`"
        f", then `{_run_once_command(deployment_name)}`"
    )


def _ingest_failure_fix(deployment_name: str, sources: list[str]) -> str:
    """The line that tells the reader how to go on after a failed ingest."""
    ingest = f"okg ingest --deployment {deployment_name}"
    return (
        "the install stopped at ingest, so its remaining steps (the first "
        "publish check and the agent connection) did not run. Fix the "
        f"failed source above, then run `{ingest}` and "
        f"`okg run --once --deployment {deployment_name} --apply`; or leave "
        f"it out for one run with `{ingest} --exclude {','.join(sources)}`. "
        "To redo the whole install afterwards, run `okg install` again "
        "with `--force --yes`"
    )


def _failed_source_line(item: Mapping[str, str]) -> str:
    stage = f" at {item['stage']}" if item.get("stage") else ""
    return f"source {item['source']} failed{stage}: {item['error']}"


def _print_fatal_install_summary(payload: Mapping[str, Any]) -> None:
    """Say why the install failed and what to do, on stderr, in text mode.

    The JSON envelope carries the same fields; a text-mode operator saw only
    a WARN line whose cause could be cut off, and no recovery steps.
    """
    publish = payload.get("publish") or {}
    details = publish.get("details") or {}
    lines = [f"install failed at stage {payload.get('failed_stage')}"]
    failed_sources = details.get("failed_sources") or []
    if failed_sources:
        lines.extend(f"  {_failed_source_line(item)}" for item in failed_sources)
        if details.get("other_sources"):
            lines.append(f"  {details['other_sources']}")
    elif publish.get("message"):
        lines.append(f"  {publish['message']}")
    if payload.get("fix_hint"):
        lines.append(f"fix: {payload['fix_hint']}")
    deployment_dir = (payload.get("deployment") or {}).get("dir")
    if deployment_dir:
        lines.append(f"dir: {deployment_dir}")
    for command in payload.get("next") or []:
        lines.append(f"next: {command}")
    print("\n".join(lines), file=sys.stderr)


def bare_ingest_selection(deployment_dir: Path) -> dict[str, list[str]]:
    """Which sources a bare `okg ingest --deployment <name>` would run.

    Issue #2331. Uses the planner `okg ingest` uses, with the same defaults
    for an absent `--include` / `--exclude`, on the registry the manifest
    names. `runnable` is the plan; `awaiting_setup` lists the sources left
    out because they are off by default until the operator approves them.
    A registry that cannot be read raises: the caller must not guess.
    """
    from okg.substrate.deployments import resolve_source_registry_path
    from okg.substrate.ingest.planning import plan_ingest_sources
    from okg.substrate.sources.registry import load_source_registry

    registry = load_source_registry(
        resolve_source_registry_path(deployment_dir),
    )
    plan = plan_ingest_sources(registry)
    runnable = list(plan.ordered)
    awaiting = sorted(
        name for name, entry in registry.items()
        if name not in plan.runnable
        and not entry.legacy
        and not entry.is_registry_only
    )
    return {"runnable": runnable, "awaiting_setup": awaiting}


# Issue #2331: the one name `okg init` and `okg install` both report for a
# deployment whose sources all wait for the operator's approval.
SETUP_STATUS_AWAITING_APPROVAL = "awaiting_operator_approval"


def add_source_next_step(deployment_name: str) -> str:
    """The exact command that gives a deployment its first runnable source."""
    return (
        f"okg deployment add-source {shlex.quote(deployment_name)} notes "
        "--type local-folder --setting root=/absolute/path/to/folder "
        "--apply --yes"
    )


def _seed_init_nomos_baseline_if_required(
    *,
    deployment_name: str,
    deployment_dir: Path,
    dsn: str,
) -> dict[str, Any] | None:
    """Install the Nomos baseline whenever the deployment requires one.

    The rule is ``deployment_baseline_spec``'s ``required`` flag: a baseline
    is required whenever the Nomos mode is not ``off`` (``audit`` as well as
    ``enforce``). ``deployment apply`` and ``okg doctor`` read the same flag,
    so init must not apply a narrower gate of its own. Issue #2281: init used
    to seed only in ``enforce`` mode, so a fresh ``audit``-mode install (the
    academic-researcher profile) left doctor reporting ``nomos_baseline``
    unconverged.
    """
    from collections.abc import Mapping

    from okg.substrate.nomos.baseline import deployment_baseline_spec

    baseline_spec = deployment_baseline_spec(
        deployment=deployment_name,
        deployment_dir=deployment_dir,
    )
    if not baseline_spec.get("required"):
        return None

    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos import seed_deployment_baseline_nomos_state

    with db_connect(dsn) as conn:
        payload = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment_name,
            manifest=baseline_spec["manifest"],
            source_names=baseline_spec["source_names"],
            source_policies=baseline_spec["source_policies"],
            runtime_source_names=baseline_spec["runtime_source_names"],
            policy_graph=baseline_spec["policy_graph"],
            activated_by="subject:nomos-rollout",
        )
        conn.commit()
    if isinstance(payload, Mapping):
        return dict(payload)
    return {"ok": True, "payload": payload}


def _cmd_new_module(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg new-module <name> [--in-tree | --deployment <dep>]`.

    Scaffolds a module skeleton. With `--in-tree`, lands under
    `src/okg/substrate/library/ontologies/<name>/` so the module
    composes against the substrate's in-tree set. With
    `--deployment <dep>`, lands under `<deployment folder>/modules/<name>/`,
    the folder found as `okg deployment add-source` finds it — deployment-private
    modules don't reach the substrate-library namespace but
    can still be loaded by that deployment's `deployment.yaml`.

    Refuses to overwrite an existing module dir unless `--force`.
    """
    from okg.substrate.cli.main import (  # noqa: F401  # deferred to break circular import
        _emit_error,
        _emit_ok,
        _is_agent_mode,
        _json_safe,
        _print_db_status_brief,
        _render_template_dir,
        _resolve_deployment_dsn,
        _resolve_deployment_dsn_from_dir,
        _resolve_dsn,
    )
    from okg.substrate.errors import SubstrateError  # noqa: F401
    from okg.substrate.catalog.module_scaffold import scaffold_module

    name = args.name
    if not name or not name.replace("_", "").replace("-", "").isalnum():
        return _emit_error(
            SubstrateError(
                f"invalid module name {name!r}; use letters / "
                f"digits / underscores / hyphens (start with a "
                f"letter)",
                data={"name": name},
            ),
            as_json=as_json,
        )

    deployment = getattr(args, "deployment", None)
    in_tree = bool(getattr(args, "in_tree", False))
    if in_tree and deployment:
        return _emit_error(
            SubstrateError(
                "--in-tree and --deployment are mutually exclusive",
                fix_hint="pick one location for the new module",
            ),
            as_json=as_json,
        )
    if not in_tree and not deployment:
        # Default to in-tree (matches design.md example).
        in_tree = True

    if in_tree:
        # Walk up to find the substrate library root.
        from okg.substrate.library import resolve as _resolve_library
        try:
            ontologies_root = Path(
                _resolve_library("okg.ontology"),
            ).parent
        except Exception:  # noqa: BLE001 — library resolve uses entry-points + fs probes; any failure falls back to filesystem layout.
            # Fallback: derive from this module's path.
            here = Path(__file__).resolve()
            ontologies_root = (
                here.parent.parent / "library" / "ontologies"
            )
        module_dir = ontologies_root / name
        package_path = (
            f"okg.substrate.library.ontologies.{name}"
        )
    else:
        # Find the deployment the way `okg deployment add-source` does (a
        # name, folder or deployment.yaml path; installed deployments too),
        # never `./deployments/` under the current directory (#3417).
        from okg.substrate.deployment_lint import resolve_deployment_manifest_path

        manifest_path = resolve_deployment_manifest_path(
            deployment,
            deployments_root=getattr(args, "deployments_root", None),
        )
        if manifest_path is None or manifest_path.name != "deployment.yaml":
            return _emit_error(
                SubstrateError(
                    f"deployment {deployment!r} not found",
                    data={"deployment": deployment},
                    fix_hint=(
                        "pass the deployment name, folder or deployment.yaml path; "
                        "create a deployment first with `okg deployment new`"
                    ),
                ),
                as_json=as_json,
            )
        module_dir = manifest_path.parent / "modules" / name
        package_path = f"{manifest_path.parent.name}.modules.{name}"

    try:
        result = scaffold_module(
            name,
            module_dir=module_dir,
            package_path=package_path,
            force=bool(getattr(args, "force", False)),
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    payload = {
        "module": {
            "name": result.name,
            "dir": str(result.module_dir),
            "package_path": package_path,
        },
        "files_written": [str(p) for p in result.files_written],
        "next": [
            f"edit {module_dir / 'schema.yaml'} to declare subtypes",
            (
                f"pytest {module_dir / 'tests'}/  "
                "# verify the manifest still loads"
            ),
        ],
    }
    _emit_ok(payload, as_json=as_json)
    return 0


#: Where an author goes instead of the retired `okg new-source` (issue #2479).
NEW_SOURCE_EXAMPLE = "examples/json-record-connector"
NEW_SOURCE_REPLACEMENT = (
    f"copy the working connector in {NEW_SOURCE_EXAMPLE}/ (see its "
    "README.md), then add it with `okg deployment add-source <deployment> "
    "<name> --registration <dir>/connector.json`"
)


def _cmd_new_source(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg new-source` is retired: refuse, write nothing, name the replacement.

    Issue #2479. The command wrote a reader skeleton that was not a working
    reader -- a plain class whose `run()` returned nothing, with no
    connector adapter, no `REGISTRATION` and no change probe. Until a
    declarative way to author a source exists, authors start from the
    example connector instead. The verb stays registered (hidden from
    `okg --help`) so an old script or runbook gets this refusal rather than
    an argparse "invalid choice" error.
    """
    from okg.substrate.cli.main import _emit_error  # deferred to break circular import
    from okg.substrate.errors import CommandRetired

    return _emit_error(
        CommandRetired(
            "`okg new-source` is retired: the reader skeleton it wrote was "
            "not a working reader, so it no longer writes one. Nothing was "
            "written.",
            fix_hint=NEW_SOURCE_REPLACEMENT,
            data={
                "command": "new-source",
                "replacement": NEW_SOURCE_REPLACEMENT,
                "example": NEW_SOURCE_EXAMPLE,
            },
        ),
        as_json=as_json,
    )
