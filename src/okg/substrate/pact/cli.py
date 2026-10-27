"""CLI entrypoint for PACT v0."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import functools
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import yaml

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import SubstrateError
from okg.substrate.unknown import Unknown, read_ok
from okg.substrate.pact import envelope
from okg.substrate.pact import external_authority
from okg.substrate.pact import fsm as fsm_module
from okg.substrate.pact import workflow_authority as wa
from okg.substrate.pact.coordination_contract import (
    REPORTED_AGENT_STATES,
    build_reported_state_event,
    claim_mutation_owner,
    mutation_owner,
    release_mutation_owner,
)
from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.manifest_inventory import (
    ManifestInventory,
    ManifestInventoryEntry,
    capture_manifest_inventory,
    load_manifest_for_read,
    read_manifest_payload,
)
from okg.substrate.pact.markdown_view import (
    check_markdown_view,
    operator_brief,
    write_markdown_view,
)
from okg.substrate.pact.operations import (
    advance_change,
    approve_intent,
    attach_evidence,
    available_change_templates,
    bind_tla,
    create_change,
    create_fast_path_change,
    evidence_status,
    ingest_formal_report,
    ingest_pytest_report,
    ingest_graph_report,
    install_gate_hooks,
    migrate_openspec,
    migrate_version,
    post_edit_check,
    request_waiver,
    task_completion_gate,
    update_task_status,
)
from okg.substrate.pact.project_settings import (
    pact_version_defaults,
    pr_base_policy,
    worktree_local_settings_report,
)
from okg.substrate.pact.store import (
    load_manifest,
    load_manifest_bytes,
    state_digest,
)
from okg.substrate.pact.validation import (
    derive_alignment,
    validate_manifest,
    version_info,
)
from okg.substrate.pact.workflow_authority import (
    WorkflowAuthorityInput,
    evaluate_workflow_authority,
    git_evidence_for_manifest,
    manifest_file_authority,
    workflow_authority_payload,
)
from okg.substrate.library.agent_runtimes import atomic_write_text


# Per `simplify-pact-surface` task 2 (2026-05): these verbs are the supported
# user-facing surface. Everything else is deprecated; retained compatibility
# entries still parse, but are hidden from normal `--help` unless the user passes
# `--help-all`, and emit a one-line deprecation notice on stderr at dispatch
# time.
SUPPORTED_VERBS: set[str] = {
    # PACT v5 verbs (self-contained `pact` package; rerouted before argparse)
    "check",
    "digest",
    "migrate",
    "usage",
    "census",
    "coord",
    "new",
    "view",
    "gate",
    "evidence",
    "decide",
    "hooks",
    "migrate-version",
    "install",
}


def _repo_pact_prefix(root: Path) -> str:
    """Use the repository-authoritative command prefix in successors."""
    from okg.substrate.pact import repair

    return repair.repo_command(root)


@functools.lru_cache(maxsize=4096)
def _manifest_version(path_text: str, mtime_ns: int) -> int | None:
    """The `pact_version` of one manifest, cached per (path, mtime).

    The review queue calls the successor helper once per manifest, so an
    uncached read here re-parsed the whole corpus on every queue build.
    """
    del mtime_ns  # cache key only: an edit invalidates the entry
    import yaml

    try:
        raw = yaml.safe_load(Path(path_text).read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 - an unreadable manifest has no version
        return None
    return raw.get("pact_version") if isinstance(raw, dict) else None


def _is_v5_change(root: Path, change_id: str) -> bool:
    """True when the named manifest is v5.

    The projections this module renders (bootstrap, next, packet) read a v5
    manifest through `okg.substrate.pact.v5_adapter`, so the successor command
    has to be chosen from the manifest's OWN version, not from this reader's.
    """
    for scope in ("changes", "archive"):
        path = root / "pact" / scope / change_id / "pact.yaml"
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            continue
        return _manifest_version(str(path), mtime_ns) == 5
    return False


def _lifecycle_successor(root: Path, change_id: str, state: str) -> tuple[str, str]:
    """Return one legal next command and its owner for a derived state."""
    prefix = _repo_pact_prefix(root)
    quoted = shlex.quote(change_id)
    if _is_v5_change(root, change_id):
        # v5 removed `decide`, `gate` and the status view. The route is the
        # one check, then the digest the reviewer pastes into their review;
        # suggesting a removed verb here would send every agent that reads a
        # bootstrap brief straight into `verb_removed`.
        if state in {"proposed", "awaiting_review"}:
            return f"{prefix} pact digest {quoted}", "operator"
        return f"{prefix} pact check {quoted} --strict --json", "agent"
    if state in {"proposed", "awaiting_review"}:
        return (
            f"{prefix} pact decide {quoted} --verdict approve --json",
            "operator",
        )
    if state == "draft":
        return f"{prefix} pact gate post-edit {quoted} --json", "agent"
    if state == "retired":
        return f"{prefix} pact gate pre-merge {quoted} --json", "agent"
    if state in {"rejected", "superseded"}:
        return f"{prefix} pact view {quoted} --format=status --json", "agent"
    return f"{prefix} pact view {quoted} --format=bootstrap --json", "agent"

# Per `polish-pact-agent-ux` req.polish.verb-guidance: transcript mining found
# agents flattening `view --format=<x>` / `evidence <x>` into flat verbs that
# were deleted by `complete-pact-surface-cut`. Map each observed guess to the
# canonical shape so the error teaches the six-verb surface instead of dumping
# argparse usage.
VERB_SUGGESTIONS: dict[str, str] = {
    "validate": "okg pact gate post-edit <id>",
    "check": "okg pact gate post-edit <id>",
    "normalize": "okg pact gate post-edit <id>",
    "check-view": "okg pact gate post-edit <id>",
    "recheck-alignment": "okg pact gate post-edit <id>",
    "render": "okg pact view <id> --format=markdown",
    "markdown": "okg pact view <id> --format=markdown",
    "status": "okg pact view <id> --format=status",
    "next": "okg pact view <id> --format=next",
    "brief": "okg pact view <id> --format=brief",
    "show": "okg pact view <id> --format=manifest",
    "explain": "okg pact view <id> --format=brief",
    "list": "okg pact view --format=list",
    "review": "okg pact view <id> --format=chat-review",
    "review-queue": "okg pact view --format=review-queue",
    "bootstrap": "okg pact view <id> --format=bootstrap --json",
    "packet": "okg pact view <id> --format=packet",
    "task": "okg pact gate task-done <id> <task>",
    "task-done": "okg pact gate task-done <id> <task>",
    "advance": "okg pact gate task-done <id> <task>",
    "attach": "okg pact evidence attach <id> --kind executed_test --requirement <req> ...",
    "attach-evidence": "okg pact evidence attach <id> --kind executed_test --requirement <req> ...",
    "ingest": "okg pact evidence ingest <id> --pytest-report <junit.xml>",
    "waive": "okg pact evidence waive <id> --scope <req> --reason ...",
    "waiver": "okg pact evidence waive <id> --scope <req> --reason ...",
    "bind-tla": "okg pact evidence bind-tla <id> ...",
    "migrate": "okg pact migrate-version <id> --target <version>",
    "approve": "okg pact decide <id> --verdict approve",
    # The rest of the `complete-pact-surface-cut` deletion list. `edit`,
    # `dep`, and `require` were authoring verbs — pact.yaml is canonical, so
    # the correction is the direct edit, not a command.
    "loop": "okg pact view <id> --format=next",
    "change": "okg pact new <id>",
    "fast-path": "okg pact new <id> --shape=fast",
    "edit": "edit pact/changes/<id>/pact.yaml directly (it is canonical), then okg pact gate post-edit <id>",
    "dep": "edit dependencies in pact/changes/<id>/pact.yaml directly, then okg pact gate post-edit <id>",
    "require": "edit requirements in pact/changes/<id>/pact.yaml directly, then okg pact gate post-edit <id>",
    "version": "okg pact view <id> --format=status",
}


def _unknown_verb_hint(name: str) -> str | None:
    """Resolve a guessed/unknown pact subcommand to canonical guidance.

    Curated map first (exact guess classes seen in transcripts), then an
    edit-distance pass over curated keys + supported verbs so typos like
    `nex` still land on `view --format=next`.
    """
    if name in VERB_SUGGESTIONS:
        return VERB_SUGGESTIONS[name]
    import difflib

    candidates = sorted(set(VERB_SUGGESTIONS) | SUPPORTED_VERBS)
    close = difflib.get_close_matches(name, candidates, n=1, cutoff=0.6)
    if not close:
        return None
    match = close[0]
    return VERB_SUGGESTIONS.get(match, f"okg pact {match}")


def _install_did_you_mean(parser: argparse.ArgumentParser) -> None:
    """Replace the bare `invalid choice` dump with a one-line correction.

    argparse rejects an unknown subcommand in `parser._check_value` (before
    the subparsers action ever runs), so the interception has to happen
    there. Instance-level patch on the pact subparser only; when no hint
    resolves, the original argparse behavior is preserved. The correction
    prints without a usage dump — the usage text is what agents were
    observed to retry against verbatim.
    """
    original_check_value = parser._check_value

    def _did_you_mean_check_value(action, value):
        if (
            getattr(action, "dest", None) == "pact_subcmd"
            and action.choices is not None
            and value not in action.choices
        ):
            hint = _unknown_verb_hint(str(value))
            if hint is not None:
                supported = ", ".join(sorted(SUPPORTED_VERBS))
                print(
                    f"error: unknown `okg pact {value}`; did you mean `{hint}`?\n"
                    f"supported verbs: {supported}",
                    file=sys.stderr,
                )
                raise SystemExit(2)
        original_check_value(action, value)

    parser._check_value = _did_you_mean_check_value

# Map deprecated verb → replacement guidance shown in the warning.
# The 22 deprecated verbs were deleted outright per `complete-pact-surface-cut`
# task.delete-verbs — invoking one now yields an argparse "invalid choice"
# error rather than a deprecation warning + forward. Only two soft entries
# survive: `new-change` (the v0 alias of the canonical `new`, still parses) and
# `migrate-openspec` (hidden history-import compatibility retained after
# OpenSpec retirement). Both warn + hide from `--help` so `--help` lists exactly
# the canonical verbs.
DEPRECATED_VERBS: dict[str, str] = {
    "new-change": "okg pact new <id>",
    "migrate-openspec": (
        "hidden history-import compatibility only; do not use for new work"
    ),
}

_BOOTSTRAP_DOGFOOD_STATUS_TIMEOUT_SECONDS = 8
_BOOTSTRAP_DEPLOYMENT_READY_TIMEOUT_SECONDS = 4


def _help_all_enabled(argv: list[str] | None = None) -> bool:
    """Detect the top-level --help-all flag before argparse parses it.

    --help-all is a sentinel: if present, the parser builds with full
    help text for deprecated verbs; otherwise deprecated verbs are
    hidden via argparse.SUPPRESS.
    """
    argv = list(sys.argv if argv is None else argv)
    return "--help-all" in argv


def _deprecated_help(
    default_help: str | None,
    name: str,
    *,
    argv: list[str] | None = None,
) -> Any:
    """Resolve the help text for a (possibly deprecated) subparser.

    Hidden by argparse.SUPPRESS unless --help-all is present, in which
    case the original help text is annotated with "(deprecated; use X)".
    """
    if name not in DEPRECATED_VERBS:
        return default_help
    if not _help_all_enabled(argv):
        return argparse.SUPPRESS
    replacement = DEPRECATED_VERBS[name]
    base = default_help or ""
    return f"{base} (deprecated; {replacement})".strip()


def _deprecated_warning(subcmd: str) -> str:
    guidance = DEPRECATED_VERBS[subcmd]
    if guidance.startswith("okg "):
        return (
            f"warning: `okg pact {subcmd}` is deprecated; use `{guidance}` instead. "
            "Set OKG_PACT_QUIET_DEPRECATION=1 to suppress."
        )
    return (
        f"warning: `okg pact {subcmd}` is deprecated; {guidance}. "
        "Set OKG_PACT_QUIET_DEPRECATION=1 to suppress."
    )


def _help_repo_root(argv: list[str] | None) -> Path:
    """Pre-scan the pact-level --root so dynamic template help is accurate."""
    values = list(argv or [])
    try:
        pact_index = values.index("pact")
    except ValueError:
        pact_index = -1
    for index, value in enumerate(values[pact_index + 1 :], start=pact_index + 1):
        if value == "--root" and index + 1 < len(values):
            return Path(values[index + 1]).expanduser().resolve()
        if value.startswith("--root="):
            return Path(value.split("=", 1)[1]).expanduser().resolve()
    return Path.cwd().resolve()


# One contextual example per verb, keyed by the parser path under `pact`.
# Every one is a real, guard-fitting invocation of THAT verb.
PACT_VERB_EXAMPLES: dict[str, str] = {
    "": "okg pact view <id> --format=next --json",
    "new": 'okg pact new <id> --title "<what must hold>" --json',
    "census": "okg pact census --output reports/pact-hygiene-census.json --json",
    "coord": "okg pact coord status --json",
    "coord status": "okg pact coord status --json",
    "coord setup": "okg pact coord setup plan --json",
    "coord setup plan": "okg pact coord setup plan --json",
    "coord setup apply": "okg pact coord setup apply --json",
    "coord start": "okg pact coord start --json",
    "coord agent-event": (
        "okg pact coord agent-event claim --idempotency-key <key> --json"
    ),
    "coord owner": (
        "okg pact coord owner --pull-request <n> --owner <principal> --json"
    ),
    "coord report": 'okg pact coord report blocked --reason "<why>" --json',
    "view": "okg pact view <id> --format=next --json",
    "decide": "okg pact decide <id> --verdict approve --json",
    "evidence": "okg pact evidence status <id> --json",
    "evidence attach": (
        "okg pact evidence attach <id> <req-or-task-or-acceptance-id> "
        "--kind executed_test --ref tests/path/test_x.py --status pass --json"
    ),
    "evidence status": "okg pact evidence status <id> --json",
    "evidence sync": "okg pact evidence sync <id> --json",
    "evidence waive": (
        'okg pact evidence waive <id> --scope <req-id> --reason "<why>" --json'
    ),
    "evidence ingest": "okg pact evidence ingest <id> --pytest-report report.xml --json",
    "evidence template": "okg pact evidence template --kind graph_report --json",
    "evidence approve": "okg pact evidence approve <id> --reviewer <name> --json",
    "evidence bind-tla": (
        "okg pact evidence bind-tla <id> --requirement <req-id> "
        "--model formal/Model.tla --property-id Inv --json"
    ),
    "gate": "okg pact gate post-edit <id> --json",
    "gate post-edit": "okg pact gate post-edit <id> --json",
    "gate task-done": "okg pact gate task-done <id> <task-id> --json",
    "gate pre-merge": "okg pact gate pre-merge <id> --json",
    "gate pre-commit": "okg pact gate pre-commit <id> --json",
    "gate pre-implementation": "okg pact gate pre-implementation <id> --json",
    "gate reviewer-brief": "okg pact gate reviewer-brief <id> --json",
    "gate graph-freshness": "okg pact gate graph-freshness <id> --json",
    "hooks": "okg pact hooks install <id> --target all --json",
    "hooks install": "okg pact hooks install <id> --target all --json",
    "migrate-version": "okg pact migrate-version <id> --target 4 --json",
    "migrate-openspec": "okg pact migrate-openspec <id> --json",
    "install": "okg pact install --agent claude-code --json",
}


def _install_per_verb_examples(pact_parser: argparse.ArgumentParser) -> None:
    """Give every `okg pact` leaf ITS OWN actionable example.

    `install_actionable_errors` is idempotent — first install wins — so this
    runs BEFORE the global tree pass and claims each leaf with the example
    that belongs to it.
    """
    from okg.substrate.cli._options import install_actionable_errors

    def _walk(node: argparse.ArgumentParser, path: str) -> None:
        install_actionable_errors(
            node,
            PACT_VERB_EXAMPLES.get(path, PACT_VERB_EXAMPLES[""]),
        )
        for action in node._actions:  # noqa: SLF001 - argparse has no public reader
            if not isinstance(action, argparse._SubParsersAction):
                continue
            seen: set[int] = set()
            for name, child in action.choices.items():
                if id(child) in seen:  # aliases share the child parser
                    continue
                seen.add(id(child))
                _walk(child, f"{path} {name}".strip())

    _walk(pact_parser, "")


def register_subparser(
    subparsers: argparse._SubParsersAction,
    add_json_flag,
    *,
    argv: list[str] | None = None,
) -> None:
    parser = subparsers.add_parser(
        "pact",
        help="Manage PACT v0 specification artifacts",
    )
    add_json_flag(parser)
    parser.add_argument(
        "--root",
        default=".",
        help="Repository root containing pact/ (default: cwd)",
    )
    help_all_enabled = _help_all_enabled(argv)
    # `--help-all` sentinel: see `_help_all_enabled`. The flag is detected
    # pre-parse so deprecated subparsers can be hidden from `--help` by
    # default. When the user passes `--help-all` the action prints the full
    # help screen and exits (same shape as `--help` itself).
    class _HelpAllAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            parser.print_help()
            parser.exit()

    parser.add_argument(
        "--help-all",
        action=_HelpAllAction,
        nargs=0,
        help="Show all subcommands including deprecated forwarders",
    )
    # Per `polish-pact-agent-ux` req.polish.verb-guidance: route unknown
    # subcommands through a did-you-mean correction instead of argparse's
    # invalid-choice usage dump. The family example feeds the generic
    # actionable-error wrap (reduce-agent-friction) for the OTHER failure
    # modes (missing args, bad flag values) on pact and its subverbs.
    _install_did_you_mean(parser)
    from okg.substrate.cli._options import install_actionable_errors

    install_actionable_errors(parser, "okg pact view <id> --format=next --json")
    raw_sub = parser.add_subparsers(dest="pact_subcmd", required=True)
    # PACT v5 verbs live in the self-contained `pact` package (no okg import).
    # `okg pact <v5 verb> ...` forwards verbatim, and a v4 verb aimed at a v5
    # manifest forwards too (see cmd_pact), so both spellings keep working
    # during the corpus cutover.
    for _v5_verb, _v5_help in (
        ("check", "PACT v5: validate, bind receipts, verify the approval line, compute the tier"),
        ("digest", "PACT v5: print the intent digest and the approval line"),
        ("migrate", "PACT v5: rewrite v4 manifests into the v5 shape"),
        ("usage", "PACT v5: recompute usage measures from .okg/pact-usage.jsonl"),
    ):
        _p_v5 = raw_sub.add_parser(_v5_verb, help=_v5_help, add_help=False)
        _p_v5.add_argument("v5_rest", nargs=argparse.REMAINDER)
        _p_v5.set_defaults(v5_forward=True)
        _p_v5._okg_actionable_example = (
            f"okg pact {_v5_verb} --json"
            if _v5_verb == "usage"
            else f"okg pact {_v5_verb} <id> --json"
        )

    # Wrap add_parser so verbs in DEPRECATED_VERBS are hidden from `--help`
    # by default (visible under `--help-all`). Six supported verbs and all
    # subcommands keep their normal help text. We also track which entries
    # need to be stripped from the subparser's `_choices_actions` list
    # because argparse renders `==SUPPRESS==` literally instead of hiding
    # subparser-level help — we filter at the end.
    _suppressed: list[str] = []

    class _PactSub:
        def add_parser(self, name: str, help: str | None = None, **kwargs):
            resolved = _deprecated_help(help, name, argv=argv)
            if resolved is argparse.SUPPRESS:
                _suppressed.append(name)
                kwargs["help"] = help  # keep original for parser metadata
            else:
                kwargs["help"] = resolved
            return raw_sub.add_parser(name, **kwargs)
    pact_sub = _PactSub()

    # `validate` deleted per `complete-pact-surface-cut` task.delete-verbs —
    # folded into `gate post-edit` (which normalizes + validates + regenerates
    # the view). The `validate_manifest` operation remains for internal callers.

    # `new` is the canonical verb; `new-change` is the v0 name kept as a
    # forwarding alias. (The alias previously failed to dispatch — it was
    # registered but the dispatch keyed only on "new-change"; both names are
    # now handled.) `--shape=fast` folds `fast-path new` (a compact
    # one-requirement / one-task PACT). The fast-path-specific args are
    # optional at the argparse level and validated in dispatch only when
    # --shape=fast, so plain `new` is unaffected.
    p_new = raw_sub.add_parser(
        "new", aliases=["new-change"], help="Create a PACT change",
        description=(
            "Without --template, --shape or the fast-shape fields, `okg pact "
            "new <id> [--title T] [--tier N] [--why W]` writes a PACT v5 "
            "manifest. Those flags write a v4 manifest."
        ),
    )
    add_json_flag(p_new)
    p_new.add_argument("change")
    template_names = available_change_templates(_help_repo_root(argv))
    p_new.add_argument(
        "--template",
        metavar="NAME",
        default=None,
        help=(
            "Full-shape template. Built-ins and repository-local templates: "
            + ", ".join(template_names)
        ),
    )
    p_new.add_argument("--title")
    # Per `right-size-pact-authoring` req.rightsize.tiers: the fast shape
    # (tier 1) is the default; tier 2+ full PACTs take an explicit
    # `--shape=full` (or a `--template`, which implies full).
    p_new.add_argument(
        "--shape", choices=["full", "fast"], default=None,
        help=(
            "`fast` (default) authors a compact one-requirement/one-task "
            "PACT; `full` is the explicit tier-2+ shape (also implied by "
            "--template)."
        ),
    )
    p_new.add_argument("--intent", help="(--shape=fast) the requirement statement")
    p_new.add_argument("--acceptance", help="(--shape=fast) the acceptance description")
    p_new.add_argument("--task-title", help="(--shape=fast) the task title")
    p_new.add_argument("--test-id", help="(--shape=fast) the planned test id")
    p_new.add_argument("--test-command", help="(--shape=fast) the test command")

    p_census = pact_sub.add_parser(
        "census",
        help="Build a read-only machine-readable hygiene census",
    )
    add_json_flag(p_census)
    p_census.add_argument(
        "--output",
        default="pact/changes/issue-978-census-repair/reports/pact-hygiene-census.json",
        help="Ledger output path relative to --root",
    )

    # The guarded transport verbs (`status`, `setup`, `start`, `agent-event`)
    # are deliberately a narrow adapter over the selected restricted SSH/stdio
    # route.  They have no identity, assignment, workspace, DSN, or lease
    # override flags; the server-owned writer derives those values from sshd
    # evidence and the native assignment row.  The coordinator-contract verbs
    # (`owner`, `report`) are a separate, local surface: they never touch the
    # transport or the native ledger, so the labels they accept are
    # self-asserted bookkeeping, not authenticated authority.
    p_coord = pact_sub.add_parser(
        "coord",
        help="Inspect the guarded coordination route or validate an agent event",
    )
    add_json_flag(p_coord)
    coord_sub = p_coord.add_subparsers(dest="coord_subcmd", required=True)
    p_coord_status = coord_sub.add_parser(
        "status",
        help="Show the tracked coordination policy and selected transport",
    )
    add_json_flag(p_coord_status)
    p_coord_setup = coord_sub.add_parser(
        "setup",
        help="Plan or apply the checked first-assignment setup",
    )
    setup_sub = p_coord_setup.add_subparsers(dest="setup_subcmd", required=True)
    p_coord_setup_plan = setup_sub.add_parser(
        "plan",
        help="Read prerequisites and print the exact zero-write setup plan",
    )
    add_json_flag(p_coord_setup_plan)
    p_coord_setup_plan.add_argument(
        "--deployment",
        default=None,
        help=(
            "Provisioned deployment to inspect (no DSN or host values); "
            "default: OKG_PACT_GRAPH_DEPLOYMENT, else graph_projection.deployment "
            "in .okg/coordination.yaml; refuses when none is set"
        ),
    )
    p_coord_setup_plan.add_argument(
        "--output",
        default=None,
        help="Optional plan file below --root (written privately and atomically)",
    )
    p_coord_setup_apply = setup_sub.add_parser(
        "apply",
        help="Apply an exact plan through authenticated operator control",
    )
    add_json_flag(p_coord_setup_apply)
    p_coord_setup_apply.add_argument(
        "--plan-file",
        required=True,
        help="Saved setup plan below --root",
    )
    p_coord_setup_apply.add_argument(
        "--plan-digest",
        required=True,
        help="Exact digest of the saved setup plan",
    )
    p_coord_start = coord_sub.add_parser(
        "start",
        help="Start an injected authorized assignment after exact readback",
    )
    add_json_flag(p_coord_start)
    p_coord_start.add_argument(
        "--deployment",
        default=None,
        help="Deployment context (assignment identity remains server-derived)",
    )
    # Coordinator contract (issue #1477).  Both subcommands are local, take no
    # transport evidence, and hold no lease; they record the two facts the
    # post-mortem showed agents were leaving implicit.
    p_coord_owner = coord_sub.add_parser(
        "owner",
        help="Claim, show, or release the single mutation owner of a pull request",
    )
    add_json_flag(p_coord_owner)
    p_coord_owner.add_argument(
        "--pull-request",
        required=True,
        help="Pull request reference this mutation owner is bound to",
    )
    p_coord_owner.add_argument(
        "--owner",
        default=None,
        help=(
            "Self-asserted owner label claiming or releasing the pull request; "
            "omit to read the recorded owner"
        ),
    )
    p_coord_owner.add_argument(
        "--owner-session",
        default="",
        help="Optional session label recorded with the claim",
    )
    p_coord_owner.add_argument(
        "--assignment-ref",
        default="",
        help="Optional coordination assignment reference recorded with the claim",
    )
    p_coord_owner.add_argument(
        "--kind",
        default="pact",
        help="Assignment kind; coordinator kinds are refused",
    )
    p_coord_owner.add_argument(
        "--release",
        action="store_true",
        help="Release the pull request instead of claiming it",
    )
    p_coord_report = coord_sub.add_parser(
        "report",
        help=(
            "Format an idle or blocked report; the mandatory reason is the "
            "point, and nothing is persisted"
        ),
    )
    add_json_flag(p_coord_report)
    p_coord_report.add_argument(
        "state",
        choices=list(REPORTED_AGENT_STATES),
        help="Reported agent state",
    )
    p_coord_report.add_argument(
        "--reason",
        required=True,
        help="Why the agent is idle or blocked; a bare state is refused",
    )
    p_coord_report.add_argument(
        "--assignment-ref",
        default="",
        help="Optional coordination assignment reference",
    )
    p_coord_report.add_argument(
        "--next-action",
        default="",
        help="Optional bounded next action for the operator",
    )
    p_coord_event = coord_sub.add_parser(
        "agent-event",
        help="Validate one guarded agent event from sshd transport evidence",
    )
    add_json_flag(p_coord_event)

    # argparse normally echoes the complete unknown-argument tail.  That is
    # unsafe for a rejected ``--dsn postgresql://...`` or similar attempt, so
    # redact coordination identity/credential options before argparse formats
    # its error.  The option remains unsupported; only its value is withheld.
    _coord_event_error = p_coord_event.error

    def _redacted_coord_event_error(message: str) -> None:
        lowered = message.lower()
        sensitive = (
            "--principal",
            "--session",
            "--assignment",
            "--generation",
            "--host",
            "--workspace",
            "--scope",
            "--timestamp",
            "--observed-at",
            "--dsn",
            "--database-dsn",
            "--database-url",
            "--postgres-dsn",
            "--okg-dsn",
            "--database-credential",
            "--lease-ttl",
            "--lease-ttl-seconds",
            "--lease-seconds",
        )
        if any(option in lowered for option in sensitive):
            print(
                "error: coordination agent-event rejects caller identity, "
                "scope, credential, timestamp, and lease options",
                file=sys.stderr,
            )
            raise SystemExit(2)
        _coord_event_error(message)

    p_coord_event.error = _redacted_coord_event_error
    # Unknown options normally bubble to the top-level parser (which would
    # print their values).  Register the forbidden agent options as suppressed
    # sentinels solely so dispatch can reject them without echoing credentials
    # or identity claims.  They are not read as authority and never reach the
    # event builder.
    for forbidden_option in (
        "--principal",
        "--session",
        "--assignment",
        "--generation",
        "--host",
        "--workspace",
        "--scope",
        "--caller-time",
        "--caller-timestamp",
        "--timestamp",
        "--time",
        "--observed-at",
        "--received-at",
        "--dsn",
        "--database-dsn",
        "--database-url",
        "--postgres-dsn",
        "--okg-dsn",
        "--credential",
        "--database-credential",
        "--database-password",
        "--lease-ttl",
        "--lease-ttl-seconds",
        "--lease-seconds",
        "--ttl",
        "--event-kind",
        "--transport",
    ):
        p_coord_event.add_argument(
            forbidden_option,
            dest="_forbidden_" + forbidden_option[2:].replace("-", "_"),
            default=argparse.SUPPRESS,
            help=argparse.SUPPRESS,
        )
    p_coord_event.add_argument(
        "event_type",
        nargs="?",
        choices=("claim", "heartbeat", "renew", "release"),
        help="Guarded event verb (server-owned assignment scope remains implicit)",
    )
    p_coord_event.add_argument(
        "--event-type",
        dest="event_type_option",
        choices=("claim", "heartbeat", "renew", "release"),
        help="Guarded event verb (alternative to the positional form)",
    )
    p_coord_event.add_argument(
        "--idempotency-key",
        required=True,
        help="Stable caller request key; it is not an assignment or identity claim",
    )
    p_coord_event.add_argument(
        "--payload-json",
        help="Optional JSON object containing event data (identity fields are refused)",
    )

    # `fast-path` deleted per `complete-pact-surface-cut` task.delete-verbs —
    # `fast-path new` folded into `new --shape=fast`; `fast-path upgrade` is
    # done by editing pact.yaml directly (the create_fast_path_change /
    # upgrade_fast_path_change operations remain for internal + test callers).

    # `view` is the canonical read verb: one dispatcher over every PACT
    # projection. It folds status / brief / list / show / next / render /
    # `review packet` (see `simplify-pact-surface` req.simplify.minimal-cli-surface
    # and `complete-pact-surface-cut` req.view-folds-reads). Each --format
    # routes to the same handler the deprecated verb called, so the output
    # is identical by construction.
    p_view = pact_sub.add_parser("view", help="Read projections of a PACT")
    add_json_flag(p_view)
    p_view.add_argument("change", nargs="?")
    p_view.add_argument(
        "--format",
        choices=[
            "manifest",
            "status",
            "brief",
            "next",
            "markdown",
            "packet",
            "chat-review",
            "chat-review-queue",
            "list",
            "bootstrap",
            "review-queue",
        ],
        default="manifest",
        help=(
            "Projection to render. `list` and `review-queue` take no change "
            "id; all others require one."
        ),
    )
    p_view.add_argument("--reviewer-kind", choices=["ai", "human"], default="human")
    p_view.add_argument(
        "--full", action="store_true",
        help="Unbudgeted output for formats with a default summary "
             "(bootstrap; per budget-the-pact-loop)",
    )
    p_view.add_argument("--for-agent", action="store_true")
    p_view.add_argument(
        "--query",
        help=(
            "For --format=chat-review-queue: operator's natural-language "
            "review question to answer from the inbox"
        ),
    )
    p_view.add_argument(
        "--graph",
        action="store_true",
        help="For --format=list: include best-effort graph projection freshness",
    )
    p_view.add_argument(
        "--status",
        choices=["open", "retired", "all"],
        default="open",
        help="For --format=list: filter by derived lifecycle (default: open)",
    )

    # status / brief / list / show / next deleted per
    # `complete-pact-surface-cut` task.delete-verbs — folded into
    # `view --format=status|brief|list|<default>|next` (default == show).
    # Their projection helpers (_status, _brief, _list_changes, _show,
    # _next) remain; `view` dispatches to them. `workbench` / `explain`
    # were the earlier `simplify-pact-surface` batch.

    # change / require / require-test / task / dep / waiver / attach-evidence
    # deleted per `complete-pact-surface-cut` task.delete-verbs. Authoring is
    # done by editing pact.yaml directly — the operations layer
    # (update_change_metadata, add_requirement, upsert_requirement_scenario /
    # _acceptance, require_test, add_task, update_task_status, add_dependency,
    # request_waiver) remains for internal + test callers. Manual evidence is
    # `evidence attach`; `loop record` was the earlier `simplify-pact-surface`
    # batch (folded into "write a PACT, attach evidence, commit").

    p_evidence = pact_sub.add_parser("evidence", help="Evidence helpers")
    add_json_flag(p_evidence)
    evidence_sub = p_evidence.add_subparsers(dest="evidence_subcmd", required=True)
    p_evidence_ingest = evidence_sub.add_parser("ingest")
    add_json_flag(p_evidence_ingest)
    p_evidence_ingest.add_argument("change")
    p_evidence_ingest.add_argument("--pytest-report")
    p_evidence_ingest.add_argument("--formal-report")
    p_evidence_ingest.add_argument("--graph-report")
    p_evidence_status = evidence_sub.add_parser("status")
    add_json_flag(p_evidence_status)
    p_evidence_status.add_argument("change")
    # `template` emits a commented, ingest-valid artifact skeleton per
    # evidence kind (polish-pact-first-run-dx req.pactdx.evidence-template)
    # so artifact shapes are discoverable without cribbing sibling repos.
    p_evidence_template = evidence_sub.add_parser(
        "template",
        help="Emit a commented evidence-artifact skeleton for --kind",
    )
    add_json_flag(p_evidence_template)
    p_evidence_template.add_argument(
        "--kind",
        required=True,
        help="Evidence kind (canonical or alias), e.g. graph_report, formal_check",
    )
    # `sync` replays closure intents queued while the decision ledger was
    # unreachable (polish-pact-agent-ux req.polish.deferred-closure).
    p_evidence_sync = evidence_sub.add_parser(
        "sync",
        help="Replay closure intents queued while the ledger was unreachable",
    )
    add_json_flag(p_evidence_sync)
    p_evidence_sync.add_argument("change", nargs="?")
    # attach / waive / bind-tla / approve fold the top-level attach-evidence,
    # `waiver request`, bind-tla, and `review approve` verbs under the single
    # `evidence` namespace (complete-pact-surface-cut req.evidence-namespace).
    # Each mirrors its predecessor's args and forwards to the same operation,
    # so the manifest mutation is identical.
    p_evidence_attach = evidence_sub.add_parser(
        "attach",
        epilog=(
            "This is the legacy PACT v4 form. A pact_version 5 change (the "
            "default) uses: okg pact evidence attach <id> --kind "
            "{executed_test,formal_check,published_generation,design_review,"
            "manual} --requirement <req> [--artifact PATH] [--command ...] "
            "[--note ...]."
        ),
    )
    add_json_flag(p_evidence_attach)
    p_evidence_attach.add_argument("change")
    p_evidence_attach.add_argument("target")
    p_evidence_attach.add_argument("--kind", required=True)
    p_evidence_attach.add_argument("--ref", required=True)
    p_evidence_attach.add_argument("--status", default="pass")
    p_evidence_attach.add_argument("--command", dest="evidence_command")
    p_evidence_attach.add_argument("--summary")
    p_evidence_attach.add_argument("--generation-id", type=int)
    p_evidence_attach.add_argument("--data-json")
    p_evidence_waive = evidence_sub.add_parser("waive")
    add_json_flag(p_evidence_waive)
    p_evidence_waive.add_argument("change")
    p_evidence_waive.add_argument("--id")
    p_evidence_waive.add_argument("--scope", required=True)
    p_evidence_waive.add_argument("--reason", required=True)
    p_evidence_waive.add_argument("--owner")
    p_evidence_waive.add_argument("--expires")
    p_evidence_tla = evidence_sub.add_parser("bind-tla")
    add_json_flag(p_evidence_tla)
    p_evidence_tla.add_argument("change")
    p_evidence_tla.add_argument("--requirement", required=True)
    p_evidence_tla.add_argument("--model", required=True)
    p_evidence_tla.add_argument("--property-id", required=True)
    p_evidence_tla.add_argument("--config")
    p_evidence_tla.add_argument("--checker-command")
    p_evidence_tla.add_argument("--refinement-test", action="append", default=[])
    p_evidence_approve = evidence_sub.add_parser("approve")
    add_json_flag(p_evidence_approve)
    p_evidence_approve.add_argument("change")
    p_evidence_approve.add_argument("--reviewer", required=True)
    p_evidence_approve.add_argument("--decision", default="approved")
    p_evidence_approve.add_argument("--notes")

    p_gate = pact_sub.add_parser("gate", help="Run hookable PACT gates")
    add_json_flag(p_gate)
    gate_sub = p_gate.add_subparsers(dest="gate_subcmd", required=True)
    p_gate_post_edit = gate_sub.add_parser("post-edit")
    p_gate_post_edit.add_argument(
        "--full", action="store_true",
        help="Emit the full unbudgeted report (default: 2 KB summary per "
             "budget-the-pact-loop req.budget.machine-surfaces)",
    )
    add_json_flag(p_gate_post_edit)
    p_gate_post_edit.add_argument("change")
    p_gate_task_done = gate_sub.add_parser("task-done")
    add_json_flag(p_gate_task_done)
    p_gate_task_done.add_argument("change")
    p_gate_task_done.add_argument("task")
    # `pre-implementation` folds `advance`: it checks the intent-review
    # lifecycle gate and advances change.status to implementation on pass.
    for gate_name in ("pre-commit", "pre-merge", "pre-implementation", "reviewer-brief"):
        p_named_gate = gate_sub.add_parser(gate_name)
        add_json_flag(p_named_gate)
        p_named_gate.add_argument("change")
    # `graph-freshness --refresh` folds `sync`: with --refresh it performs the
    # projection refresh; without it, it only checks freshness.
    p_gate_graph = gate_sub.add_parser("graph-freshness")
    add_json_flag(p_gate_graph)
    p_gate_graph.add_argument("change")
    p_gate_graph.add_argument(
        "--refresh",
        action="store_true",
        help="Perform the projection refresh (folds `sync`), not just check freshness",
    )
    p_gate_graph.add_argument("--deployment", default=None, help="(--refresh) deployment override")
    p_gate_graph.add_argument("--dry-run", action="store_true", help="(--refresh) plan only, do not ingest")

    p_decide = pact_sub.add_parser(
        "decide",
        help="Record a verdict in the decision ledger and apply the transition",
    )
    add_json_flag(p_decide)
    p_decide.add_argument("change", nargs="?")
    p_decide.add_argument(
        "--verdict",
        required=True,
        choices=list(fsm_module.ALL_VERDICTS),
    )
    p_decide.add_argument("--note")
    p_decide.add_argument("--target", help="For supersede: the replacing change id")
    p_decide.add_argument("--to", dest="escalate_to", help="For escalate: recipient principal")
    p_decide.add_argument("--principal", help="Acting principal (default: OKG_PRINCIPAL or OS user)")
    p_decide.add_argument("--dsn", help="Explicit ledger DSN override")
    p_decide.add_argument(
        "--all-stale",
        action="store_true",
        help="With --verdict defer, remove all expired intent claims from the active queue",
    )
    p_decide.add_argument(
        "--dry-run",
        action="store_true",
        help="For --all-stale, report eligible stale claims without removing them",
    )

    p_hooks = pact_sub.add_parser("hooks", help="Install PACT local and CI hooks")
    add_json_flag(p_hooks)
    hooks_sub = p_hooks.add_subparsers(dest="hooks_subcmd", required=True)
    p_hooks_install = hooks_sub.add_parser("install")
    add_json_flag(p_hooks_install)
    p_hooks_install.add_argument("change")
    p_hooks_install.add_argument(
        "--target",
        choices=["local", "ci", "all"],
        default="all",
    )
    p_hooks_install.add_argument("--dry-run", action="store_true")
    p_hooks_install.add_argument("--force", action="store_true")

    # sync / bind-tla / normalize / render / check-view / review / advance /
    # edit deleted per `complete-pact-surface-cut` task.delete-verbs:
    #   sync        -> gate graph-freshness --refresh
    #   bind-tla    -> evidence bind-tla
    #   normalize   -> gate post-edit (normalizes)
    #   render      -> view --format=markdown
    #   check-view  -> gate post-edit (regenerates + checks the view)
    #   review packet  -> view --format=packet; review approve -> evidence approve
    #   advance     -> gate pre-implementation (intent-review-gated transition)
    #   edit        -> edit pact.yaml directly (save_edit_plan / apply_edit_plan
    #                  operations remain for internal + test callers)
    # `recheck-alignment` / `version` were the earlier `simplify-pact-surface`
    # batch (alignment + version negotiation are derived on read / in
    # `gate post-edit` output).

    p_migrate = pact_sub.add_parser(
        "migrate-openspec",
        help="Import one legacy OpenSpec change into PACT",
    )
    add_json_flag(p_migrate)
    p_migrate.add_argument("change")

    p_migrate_version = pact_sub.add_parser("migrate-version", help="Set target PACT version")
    add_json_flag(p_migrate_version)
    p_migrate_version.add_argument("change")
    p_migrate_version.add_argument("--target", type=int, required=True)
    p_migrate_version.add_argument("--artifact-format", type=int)

    p_install = pact_sub.add_parser(
        "install",
        help="Install PACT/OKG agent instructions + skills for a coding agent",
    )
    add_json_flag(p_install)
    p_install.add_argument(
        "--agent",
        choices=["claude-code", "codex", "gemini"],
        required=True,
        help="Coding agent to set up",
    )

    p_install.add_argument(
        "--dest",
        default=None,
        help="Target repo directory (default: --root)",
    )
    p_install.add_argument(
        "--no-gh-alias",
        action="store_true",
        help="Skip setting the `gh prd` = `pr create --base dev` alias",
    )
    # Per ship-harness-integration-pack: optionally register the
    # deployment's OKG MCP server via a thin per-harness manifest.
    p_install.add_argument(
        "--mcp-server-name",
        default=None,
        help="Register an OKG MCP server under this name",
    )
    p_install.add_argument(
        "--mcp-command",
        default=None,
        help="MCP server launch command (e.g. python)",
    )
    p_install.add_argument(
        "--mcp-arg",
        action="append",
        default=None,
        help="MCP server launch arg (repeatable)",
    )
    p_install.add_argument(
        "--mcp-env",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="MCP server env var (repeatable)",
    )

    # Per `simplify-pact-surface` task 2: strip deprecated subparsers from
    # the help-rendering choices list. argparse renders subparser
    # `help=argparse.SUPPRESS` as the literal string `==SUPPRESS==` instead
    # of hiding the entry — so we filter `_choices_actions` ourselves.
    # The parsers themselves remain in `_name_parser_map` so dispatch still
    # works; only the visible help text changes.
    if not help_all_enabled:
        raw_sub._choices_actions = [
            action for action in raw_sub._choices_actions
            if action.dest not in _suppressed
        ]
        raw_sub.metavar = "{" + ",".join(
            action.dest for action in raw_sub._choices_actions
        ) + "}"

    # Per-verb examples, LAST — every subparser must exist before the walk, or
    # a verb registered later silently keeps no example of its own. (This ran
    # halfway through `install`'s own flag list, which worked only because the
    # remaining calls added flags rather than parsers: a placement that is
    # correct by accident is a defect waiting for the next verb.)
    # The family-level wrap installs ONE example on every leaf, so
    # `okg pact evidence attach` with a missing `--kind` answered with
    # `okg pact view <id> --format=next --json` — a command from a different
    # verb, which is worse than no example because it looks authoritative
    # (pact-self-repairing-refusals req.refusal.complete).
    _install_per_verb_examples(parser)


def cmd_pact(args: argparse.Namespace, *, as_json: bool) -> int:
    # PACT v5 verbs and v5 manifests never reach this parser: `okg` reroutes
    # their raw argv to the `pact` package before argparse (cli/main.py).
    # Per `simplify-pact-surface` task 2: one-line deprecation notice
    # before forwarding. The warning goes to stderr so it doesn't pollute
    # the JSON payload on stdout for `--json` consumers.
    subcmd = getattr(args, "pact_subcmd", None)
    if subcmd in DEPRECATED_VERBS and not os.environ.get("OKG_PACT_QUIET_DEPRECATION"):
        print(_deprecated_warning(subcmd), file=sys.stderr)
    # First-run nudge (per `pact-agent-install`): point the operator at
    # `okg pact install` until the agent bundle is installed. Human mode only
    # (suppressed under --json / agent mode so it never pollutes machine
    # output), opt-out via OKG_PACT_QUIET_NUDGE, and never for `install`
    # itself. The nudge writes nothing — it only prints to stderr.
    if (
        subcmd != "install"
        and not as_json
        and not os.environ.get("OKG_PACT_QUIET_NUDGE")
    ):
        from okg.substrate.pact.agent_install import nudge_message
        _tip = nudge_message(Path(getattr(args, "root", ".")).resolve())
        if _tip:
            print(_tip, file=sys.stderr)
    try:
        payload = _dispatch(args)
    except SubstrateError as err:
        # A typed error is a REFUSAL with the same envelope as any other:
        # it used to print `{"error": {...}}` with no `ok` key at all, so
        # `payload.get("ok")` yielded None and agents read past it.
        refusal = envelope.from_error(err)
        if as_json:
            print(json.dumps(refusal))
        else:
            print(f"error [{err.code}]: {err.message}")
            if err.fix_hint:
                print(f"  fix_hint: {err.fix_hint}")
            if err.data:
                print(f"  data: {err.data}")
        return envelope.exit_code(refusal)
    # `view --format=brief` (and the deprecated `brief`) print the human
    # operator-brief text rather than the JSON payload when --json is absent.
    brief_text = (
        args.pact_subcmd == "brief"
        or args.pact_subcmd == "decide"
        or (args.pact_subcmd == "view" and getattr(args, "format", None) == "brief")
        or (args.pact_subcmd == "view" and getattr(args, "format", None) == "chat-review")
        or (args.pact_subcmd == "view" and getattr(args, "format", None) == "chat-review-queue")
        # `evidence template` prints the raw YAML skeleton in human mode so
        # `... > report.yaml` yields an editable artifact directly.
        or (
            args.pact_subcmd == "evidence"
            and getattr(args, "evidence_subcmd", None) == "template"
        )
    )
    if brief_text and not as_json and "text" in payload:
        print(payload["text"])
    else:
        _emit(payload, as_json=as_json)
    # recorded 0, deferred 3, refused 1. Deferred is nonzero so a
    # `set -e` pipeline stops treating queued-not-recorded as success,
    # and distinct from 1 so a blocking hook can allow it deliberately
    # (see operations._hook_script).
    return envelope.exit_code(payload)


def _dispatch(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.root).resolve()
    subcmd = args.pact_subcmd
    if subcmd == "install":
        from okg.substrate.pact.agent_install import (
            McpServerSpec,
            ensure_gh_pr_alias,
            install_agent_setup,
            is_okg_repository,
        )
        dest = Path(args.dest).resolve() if getattr(args, "dest", None) else root
        mcp = None
        if getattr(args, "mcp_server_name", None):
            if not getattr(args, "mcp_command", None):
                raise SubstrateError(
                    "--mcp-server-name requires --mcp-command",
                )
            mcp = McpServerSpec(
                name=args.mcp_server_name,
                command=args.mcp_command,
                args=list(args.mcp_arg or []),
                env=dict(
                    pair.split("=", 1)
                    for pair in (args.mcp_env or [])
                    if "=" in pair
                ),
            )
        # A repository other than okg gets the neutral block and the `pact`
        # skill only: the okg block names paths that exist only in okg.
        repository = "okg" if is_okg_repository(dest) else "generic"
        payload = install_agent_setup(
            args.agent, dest, mcp=mcp, repository=repository
        ).to_json()
        # The alias targets okg's `dev` branch; never set it for another repo.
        if repository == "okg" and not getattr(args, "no_gh_alias", False):
            payload["gh_alias"] = ensure_gh_pr_alias()
        return payload
    if subcmd == "census":
        from okg.substrate.pact.census import write_census

        output = Path(args.output)
        if not output.is_absolute():
            output = root / output
        try:
            ledger = write_census(root, output)
        except ValueError as exc:
            raise SubstrateError(str(exc)) from exc
        return envelope.recorded(
            text=f"wrote read-only PACT hygiene census to {output}",
            path=str(output),
            schema=ledger["schema"],
            source_commit=ledger["source_commit"],
            comparison=ledger["comparison"],
            disposition_counts=ledger["disposition_counts"],
            unresolved_count=len(ledger["unresolved"]),
        )
    if subcmd == "coord":
        if args.coord_subcmd == "setup":
            from okg.substrate.pact.coordination_operations import (
                SetupPlan,
                VizCoordinationClient,
            )

            def safe_plan_path(raw: str) -> Path:
                candidate = Path(raw)
                if candidate.is_absolute() or ".." in candidate.parts:
                    raise SubstrateError(
                        "coordination plan path must stay below --root",
                        data={"cause": "plan_path_unsafe"},
                    )
                target = (root / candidate).resolve()
                try:
                    target.relative_to(root)
                except ValueError as exc:
                    raise SubstrateError(
                        "coordination plan path must stay below --root",
                        data={"cause": "plan_path_unsafe"},
                    ) from exc
                return target

            if args.setup_subcmd == "plan":
                # Coordination belongs to the repository's PACT graph
                # deployment, like every other PACT surface: --deployment,
                # else OKG_PACT_GRAPH_DEPLOYMENT, else graph_projection.
                # OKG_DEPLOYMENT (set by agent launchers to the agent's
                # target) is deliberately not consulted.
                from okg.substrate.deployment_default import DeploymentNotSpecified
                from okg.substrate.pact.project_settings import (
                    PACT_GRAPH_DEPLOYMENT_FIX,
                )

                deployment = args.deployment or _graph_projection_deployment(root)
                if not deployment:
                    raise DeploymentNotSpecified(
                        "okg pact coord setup plan needs a deployment and none "
                        "was given or configured",
                        data={"command": "okg pact coord setup plan"},
                        fix_hint=(
                            f"pass --deployment <name>, or {PACT_GRAPH_DEPLOYMENT_FIX}"
                        ),
                    )
                client = VizCoordinationClient.from_environment()
                try:
                    wire_plan = client.setup_plan(deployment)
                    plan = SetupPlan.from_json(wire_plan, repo_root=root)
                except ValueError as exc:
                    raise SubstrateError(
                        "coordination service returned a malformed setup plan",
                        data={"cause": "viz_plan_invalid"},
                    ) from exc
                if args.output:
                    output = safe_plan_path(args.output)
                    if output.exists() and output.is_dir():
                        raise SubstrateError(
                            "coordination plan output is a directory",
                            data={"cause": "plan_output_invalid"},
                        )
                    atomic_write_text(
                        output,
                        json.dumps(plan.to_json(), indent=2, sort_keys=True) + "\n",
                    )
                    output.chmod(0o600)
                detail = {"setup": plan.to_json()}
                if plan.ready:
                    return envelope.recorded(
                        text="coordination setup plan is ready for authenticated apply",
                        **detail,
                    )
                return envelope.refused(
                    text="coordination setup plan is not ready",
                    message="setup prerequisites are incomplete",
                    blocked_by=plan.blocked_by,
                    next_commands=[plan.next_action],
                    **detail,
                )
            plan_path = safe_plan_path(args.plan_file)
            try:
                raw_plan = json.loads(plan_path.read_text(encoding="utf-8"))
                plan = SetupPlan.from_json(raw_plan, repo_root=root)
            except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise SubstrateError(
                    "saved coordination setup plan is malformed",
                    data={"cause": "saved_plan_invalid"},
                ) from exc
            if args.plan_digest != plan.plan_digest:
                raise SubstrateError(
                    "saved coordination setup plan digest does not match",
                    data={"cause": "stale_setup_plan"},
                )
            client = VizCoordinationClient.from_environment()
            result = client.apply_setup(plan)
            return envelope.recorded(
                text="coordination setup plan was submitted to operator control",
                setup=result,
                plan_digest=plan.plan_digest,
            )
        if args.coord_subcmd == "owner":
            if args.owner is not None and not args.owner.strip():
                raise SubstrateError(
                    "coordination owner requires a non-empty owner label",
                    data={"cause": "owner_principal_required"},
                )
            if args.release:
                released = release_mutation_owner(
                    root,
                    pull_request=args.pull_request,
                    owner_principal=args.owner,
                )
                return envelope.recorded(
                    text=(
                        "mutation owner released"
                        if released
                        else "pull request had no recorded mutation owner"
                    ),
                    coordination={
                        "pull_request": args.pull_request,
                        "released": released,
                        "register_scope": "shared_git_directory",
                    },
                )
            if not args.owner:
                current = mutation_owner(root, args.pull_request)
                return envelope.recorded(
                    text=(
                        "recorded mutation owner"
                        if current is not None
                        else "pull request has no recorded mutation owner"
                    ),
                    coordination={
                        "pull_request": args.pull_request,
                        "owner": current.to_json() if current is not None else None,
                    },
                )
            claimed = claim_mutation_owner(
                root,
                pull_request=args.pull_request,
                owner_principal=args.owner,
                owner_session_id=args.owner_session,
                assignment_id=args.assignment_ref,
                assignment_kind=args.kind,
            )
            return envelope.recorded(
                text="mutation owner recorded for this pull request",
                coordination={
                    "pull_request": claimed.pull_request,
                    "owner": claimed.to_json(),
                    "register_scope": "shared_git_directory",
                },
            )
        if args.coord_subcmd == "report":
            event = build_reported_state_event(
                args.state,
                reason=args.reason,
                assignment_id=args.assignment_ref,
                next_action=args.next_action,
            )
            return envelope.recorded(
                text=f"reported {event['state']} state with a reason",
                coordination={"reported": event, "persisted": False},
            )
        if args.coord_subcmd == "start":
            raise SubstrateError(
                "assignment start is unavailable from the unauthenticated CLI",
                data={"cause": "host_local_executor_required"},
                fix_hint="use the authenticated host-local start seam",
            )
        from okg.substrate.pact.coordination import (
            build_guarded_event,
            load_coordination_binding,
            submit_guarded_event,
            transport_status,
        )

        binding = load_coordination_binding(root)
        if args.coord_subcmd == "status":
            return envelope.recorded(
                text="guarded coordination policy is loaded",
                coordination={
                    "transport": transport_status(binding),
                    "policy": binding.to_json(),
                    "policy_digest": binding.policy_digest,
                    "project_digest": binding.project_digest,
                    "lease_ttl_seconds": dict(binding.lease_ttl_seconds),
                    "write_authority": "native guarded writer",
                },
            )
        if args.coord_subcmd == "agent-event":
            if any(
                name.startswith("_forbidden_")
                for name in vars(args)
            ):
                raise SubstrateError(
                    "coordination agent-event rejects caller identity, scope, "
                    "credential, timestamp, transport, and lease options",
                    data={"cause": "caller_fields_rejected"},
                )
            event_type = args.event_type or args.event_type_option
            if args.event_type and args.event_type_option:
                raise SubstrateError(
                    "coordination agent-event accepts one event verb",
                    data={"cause": "event_type_ambiguous"},
                )
            if not event_type:
                raise SubstrateError(
                    "coordination agent-event requires an event verb",
                    fix_hint=(
                        "okg pact coord agent-event <claim|heartbeat|renew|release> "
                        "--idempotency-key <key> --json"
                    ),
                )
            # Build from the already-loaded policy so the response can retain
            # the exact digest binding if the server entrypoint is absent.
            event = build_guarded_event(
                root,
                event_type=event_type,
                idempotency_key=args.idempotency_key,
                payload=args.payload_json,
                environ=os.environ,
                expected_policy_digest=binding.policy_digest,
                expected_project_digest=binding.project_digest,
            )
            try:
                result = submit_guarded_event(event)
            except SubstrateError as err:
                cause = (err.data or {}).get("cause", "forced_command_server_missing")
                return envelope.refused(
                    text="guarded coordination event was validated but not submitted",
                    message=err.message,
                    blocked_by=[cause],
                    coordination={
                        "event_type": event.event_type,
                        "validated": True,
                        "written": False,
                        "transport": transport_status(event.policy),
                        "policy": event.policy.to_json(),
                        "policy_digest": event.policy.policy_digest,
                        "project_digest": event.policy.project_digest,
                    },
                )
            if hasattr(result, "as_dict"):
                result = result.as_dict()
            return envelope.recorded(
                text="guarded coordination event submitted",
                coordination={
                    "event_type": event.event_type,
                    "validated": True,
                    "written": True,
                    "result": result,
                    "policy": event.policy.to_json(),
                    "policy_digest": event.policy.policy_digest,
                    "project_digest": event.policy.project_digest,
                },
            )
        raise SubstrateError(
            "unknown coordination command",
            data={"cause": "unknown_coordination_command"},
        )
    if subcmd in ("new", "new-change"):
        # Tier resolution (right-size-pact-authoring req.rightsize.tiers):
        # explicit --shape wins; --template implies the full tier-2+ shape;
        # otherwise the fast shape is the default.
        shape = args.shape or ("full" if args.template else "fast")
        if shape == "fast":
            slug = re.sub(r"[^A-Za-z0-9]+", "-", args.change.lower()).strip("-")
            title = args.title or args.change.replace("-", " ")
            manifest = create_fast_path_change(
                root,
                args.change,
                title=title,
                intent=args.intent or (
                    f"TODO: state the requirement — what must hold once "
                    f"{args.change} lands."
                ),
                acceptance=args.acceptance or (
                    "TODO: the observable check that proves the requirement."
                ),
                task_title=args.task_title,
                test_id=args.test_id or (
                    f"tests/TODO/test_{slug.replace('-', '_')}.py::"
                    f"test_{slug.replace('-', '_')}"
                ),
                test_command=args.test_command,
            )
            return envelope.recorded(
                text=f"created {manifest.change.id} (fast shape)",
                change=manifest.change.id,
                path=str(root / "pact" / "changes" / manifest.change.id / "pact.yaml"),
                pact_shape=(
                    manifest.change.method_metadata.get("pact_shape")
                    or manifest.prose.get("pact_shape")
                ),
                requirements=len(manifest.requirements),
                tasks=len(manifest.tasks),
                test_plan=len(manifest.test_plan),
                state="draft",
                next_commands=[
                    f"{_repo_pact_prefix(root)} pact gate post-edit "
                    f"{shlex.quote(manifest.change.id)} --json"
                ],
                next_action={
                    "command": (
                        f"{_repo_pact_prefix(root)} pact gate post-edit "
                        f"{shlex.quote(manifest.change.id)} --json"
                    ),
                    "owner": "agent",
                    "why": "admit the new PACT before implementation",
                },
            )
        manifest = create_change(
            root, args.change,
            template=args.template or "feature",
            title=args.title,
        )
        return envelope.recorded(
            text=f"created {manifest.change.id} from template",
            change=manifest.change.id,
            path=str(root / "pact" / "changes" / manifest.change.id / "pact.yaml"),
            state="draft",
            next_commands=[
                f"{_repo_pact_prefix(root)} pact gate post-edit "
                f"{shlex.quote(manifest.change.id)} --json"
            ],
            next_action={
                "command": (
                    f"{_repo_pact_prefix(root)} pact gate post-edit "
                    f"{shlex.quote(manifest.change.id)} --json"
                ),
                "owner": "agent",
                "why": "admit the new PACT before implementation",
            },
        )
    if subcmd == "view":
        fmt = args.format
        if fmt == "list":
            return _list_changes(
                root, include_graph=args.graph, status_filter=args.status,
            )
        if fmt == "review-queue":
            from okg.substrate.pact.review import build_queue
            return build_queue(root)
        if fmt == "chat-review-queue":
            return _chat_review_queue(root, query=args.query)
        if not args.change:
            raise SubstrateError(
                "`okg pact view` requires a change id (except `--format=list`)",
                fix_hint="okg pact view <change-id> [--format=status|brief|next|markdown|packet|chat-review|bootstrap]",
            )
        if fmt == "manifest":
            return _show(root, args.change, for_agent=args.for_agent)
        if fmt == "status":
            return _status(root, args.change)
        if fmt == "brief":
            return _brief(root, args.change)
        if fmt == "chat-review":
            return _chat_review(root, args.change)
        if fmt == "next":
            return _next(root, args.change)
        if fmt == "bootstrap":
            payload = _agent_bootstrap(root, args.change)
            if getattr(args, "full", False):
                return payload
            return _bootstrap_summary(payload)
        if fmt == "markdown":
            path, digest = write_markdown_view(root, args.change)
            return envelope.recorded(
                text=f"rendered {args.change} review view to {path}",
                change=args.change,
                format="markdown",
                path=str(path),
                source_digest=digest,
            )
        if fmt == "packet":
            return _review_packet(root, args.change, reviewer_kind=args.reviewer_kind)
        raise SubstrateError("unknown view format", data={"format": fmt})
    # status / brief / list / show / next / change / require /
    # require-test / task / dep / waiver / attach-evidence dispatch arms
    # deleted per `complete-pact-surface-cut` task.delete-verbs (folded into
    # view / gate / evidence / direct-YAML-edit; the operations remain).
    if subcmd == "decide":
        if args.all_stale:
            if args.change:
                raise SubstrateError(
                    "`okg pact decide --all-stale` does not take a change id",
                    fix_hint="uv run okg pact decide --all-stale --verdict defer --json",
                )
            if args.verdict != "defer":
                raise SubstrateError(
                    "`okg pact decide --all-stale` only supports --verdict defer",
                    data={"verdict": args.verdict},
                    fix_hint="uv run okg pact decide --all-stale --verdict defer --json",
                )
            from okg.substrate.pact import decisions as ledger
            conn = ledger.require_ledger_connection(
                explicit_dsn=args.dsn,
                repo_root=root,
            )
            try:
                return ledger.cleanup_stale_intents(
                    conn,
                    principal=args.principal,
                    dry_run=args.dry_run,
                )
            finally:
                conn.close()
        if args.dry_run:
            raise SubstrateError(
                "`okg pact decide --dry-run` is only valid with --all-stale",
                fix_hint="uv run okg pact decide --all-stale --verdict defer --dry-run --json",
            )
        if not args.change:
            raise SubstrateError(
                "`okg pact decide` requires a change id unless --all-stale is used",
                fix_hint="uv run okg pact decide <pact-id> --verdict approve --json",
            )
        from okg.substrate.pact.review import decide_change
        decided = decide_change(
            root,
            args.change,
            verdict=args.verdict,
            note=args.note,
            target=args.target,
            escalate_to=args.escalate_to,
            principal=args.principal,
            explicit_dsn=args.dsn,
        )
        if args.verdict == "approve":
            decided = _record_external_receipt(root, args.change, decided)
        return decided
    if subcmd == "evidence" and args.evidence_subcmd == "ingest":
        if args.pytest_report:
            manifest = ingest_pytest_report(root, args.change, Path(args.pytest_report))
        elif args.formal_report:
            manifest = ingest_formal_report(root, args.change, Path(args.formal_report))
        elif args.graph_report:
            manifest = ingest_graph_report(root, args.change, Path(args.graph_report))
        else:
            raise SubstrateError(
                "evidence ingest requires --pytest-report, --formal-report, or --graph-report",
                fix_hint=(
                    "okg pact evidence ingest <change> "
                    "--pytest-report junit.xml"
                ),
            )
        result = _with_closure_intents(
            root,
            args.change,
            envelope.recorded(
                text=f"ingested evidence into {manifest.change.id}",
                change=manifest.change.id,
                evidence=len(manifest.evidence),
            ),
        )
        derived = _derived_state_payload(root, args.change)
        state = str(derived.get("state") or "in_progress")
        if (
            envelope.verdict_of(result) == envelope.RECORDED
            and not derived.get("authoritative", False)
        ):
            deferred = envelope.restate(
                result,
                envelope.DEFERRED,
                text=f"ingested evidence into {manifest.change.id}; lifecycle ledger unavailable",
                message="the local evidence write landed but lifecycle state is not authoritative",
                blocked_by=["ledger_not_fresh"],
                next_commands=[],
            )
            deferred.update(state=None, derived_state=derived)
            return deferred
        if envelope.verdict_of(result) == envelope.RECORDED:
            command, owner = _lifecycle_successor(root, args.change, state)
            result.update(
                state=state,
                next_commands=[command],
                next_action={
                    "command": command,
                    "owner": owner,
                    "why": "continue the derived lifecycle",
                },
            )
        return result
    if subcmd == "evidence" and args.evidence_subcmd == "status":
        return evidence_status(root, args.change)
    if subcmd == "evidence" and args.evidence_subcmd == "template":
        from okg.substrate.pact.evidence_templates import evidence_template

        return evidence_template(args.kind, repo_root=root)
    if subcmd == "evidence" and args.evidence_subcmd == "sync":
        from okg.substrate.pact.operations import sync_closure_intents

        return sync_closure_intents(root, args.change)
    if subcmd == "evidence" and args.evidence_subcmd == "attach":
        data = json.loads(args.data_json) if args.data_json else None
        manifest = attach_evidence(
            root,
            args.change,
            args.target,
            kind=args.kind,
            ref=args.ref,
            status=args.status,
            command=args.evidence_command,
            summary=args.summary,
            generation_id=args.generation_id,
            data=data,
        )
        return _with_closure_intents(
            root,
            args.change,
            envelope.recorded(
                text=f"attached evidence to {manifest.change.id}",
                change=manifest.change.id,
                evidence=len(manifest.evidence),
            ),
        )
    if subcmd == "evidence" and args.evidence_subcmd == "waive":
        manifest = request_waiver(
            root,
            args.change,
            waiver_id=args.id,
            scope=args.scope,
            reason=args.reason,
            owner=args.owner,
            expires=args.expires,
        )
        return envelope.recorded(
            text=f"waiver recorded on {manifest.change.id}",
            change=manifest.change.id,
            waivers=len(manifest.waivers),
        )
    if subcmd == "evidence" and args.evidence_subcmd == "bind-tla":
        manifest = bind_tla(
            root,
            args.change,
            requirement=args.requirement,
            model=args.model,
            property_id=args.property_id,
            config=args.config,
            checker_command=args.checker_command,
            refinement_tests=args.refinement_test,
        )
        return envelope.recorded(
            text=f"bound TLA model on {manifest.change.id}",
            change=manifest.change.id,
            formal=len(manifest.formal),
        )
    if subcmd == "evidence" and args.evidence_subcmd == "approve":
        manifest = approve_intent(
            root,
            args.change,
            reviewer=args.reviewer,
            decision=args.decision,
            notes=args.notes,
        )
        return envelope.recorded(
            text=f"intent approval recorded on {manifest.change.id}",
            change=manifest.change.id,
            evidence=len(manifest.evidence),
        )
    if subcmd == "gate" and args.gate_subcmd == "post-edit":
        payload = post_edit_check(root, args.change)
        if getattr(args, "full", False):
            return payload
        return _post_edit_summary(payload)
    if subcmd == "gate" and args.gate_subcmd == "task-done":
        # `gate task-done` marks the task done on a passing check (folds the
        # deprecated `task done`) and refuses on missing proof.
        result = task_completion_gate(root, args.change, args.task)
        if result["ok"]:
            if result["task_status"] != "done":
                update_task_status(root, args.change, args.task, status="done")
                result["task_status"] = "done"
            result["marked"] = True
            derived = _derived_state_payload(root, args.change)
            state = str(derived.get("state") or "in_progress")
            if state == "awaiting_review":
                next_command = (
                    f"{_repo_pact_prefix(root)} pact decide {shlex.quote(args.change)} "
                    "--verdict approve --json"
                )
            else:
                next_command = (
                    f"{_repo_pact_prefix(root)} pact view {shlex.quote(args.change)} "
                    "--format=bootstrap --json"
                )
            result.update(
                {
                    "state": state,
                    "task": {"id": args.task, "status": result.get("task_status")},
                    "owner": result.get("owner") or "agent",
                    "next_commands": [next_command],
                    "next_action": {
                        "command": next_command,
                        "owner": "operator" if state == "awaiting_review" else "agent",
                        "why": "continue the derived lifecycle",
                    },
                }
            )
        else:
            result["marked"] = False
            # Ledger-only refusal with complete proof: queue a closure
            # intent so the proof is not discarded (req.polish.deferred-closure).
            from okg.substrate.pact.operations import maybe_defer_task_done

            deferred = maybe_defer_task_done(root, args.change, args.task, result)
            if deferred is not None:
                result["closure_intent"] = deferred
                commands = _deferred_closure_commands(result, deferred)
                result["recommended_commands"] = commands
                # The proof is complete and the subject holds; only the
                # ledger write is queued. That is a DEFERRAL, not the
                # refusal it used to report — and never `ok: true` with
                # the deferral buried in `closure_intent`.
                result = envelope.restate(
                    result,
                    envelope.DEFERRED,
                    text=(
                        f"task-done {args.change} {args.task}: deferred "
                        "(closure intent queued; ledger unreachable)"
                    ),
                    message=(
                        str(deferred.get("reason") or "decision ledger unreachable")
                    ),
                    # UNION, never overwrite: replacing `blocked_by` with the
                    # authority blockers erased whatever else the refusal
                    # named (a dangling evidence ref vanished behind
                    # `ledger_dsn_not_configured`). `blocked_by` is the single
                    # cause location, so a re-stamp may only ADD to it.
                    blocked_by=envelope.tokens(
                        result.get("blocked_by"),
                        (result.get("workflow_authority") or {}).get("blockers")
                        or ["ledger_not_fresh"],
                    ),
                    next_commands=commands,
                    why=_deferred_closure_why(result, deferred),
                )
        return result
    if subcmd == "gate" and args.gate_subcmd in {"pre-commit", "reviewer-brief"}:
        result = _simple_gate(root, args.change, args.gate_subcmd)
        if args.gate_subcmd == "pre-commit":
            tier = _tier_size_warning(root, change_id=args.change)
            if tier is not None:
                result["tier_warning"] = tier
        return result
    if subcmd == "gate" and args.gate_subcmd == "pre-merge":
        result = _simple_gate(root, args.change, args.gate_subcmd, lifecycle_gate="verification")
        gate_manifest = _load_manifest_for_gate(root, args.change)
        target_version = gate_manifest.target_pact_version or 0
        decision_gate: dict[str, Any] | None = None
        external_block: dict[str, Any] | None = None
        ledger_state: dict[str, Any] = {}
        if target_version >= 4:
            decision_gate = _ledger_decision_gate(
                root, args.change, capture=ledger_state
            )
            result["decision_gate"] = decision_gate
            # Lifted to the top level so the surface is visible without
            # reading into the decision gate, on accepted results as much as
            # refused ones (issue #1424).
            external_block = (
                (decision_gate.get("workflow_authority") or {}).get(
                    "external_authority"
                )
            )
            if external_block:
                result["external_authority"] = external_block
            if not decision_gate["ok"]:
                # NEVER a bare `--verdict approve`: this line recommended it
                # unconditionally and it fired nine times in states where the
                # very next call answered "verdict approve is not legal in
                # state in_progress". The legal set is computed from the FSM
                # FIRST; when approve is not in it, the refusal carries the
                # enabling path instead of a verdict that will be rejected.
                decide_plan = _decide_recommendation(
                    root,
                    args.change,
                    decisions=ledger_state.get("decisions"),
                    ledger_consulted=bool(ledger_state.get("ledger_consulted")),
                )
                result["decide_plan"] = decide_plan
                # The plan is always REPORTED, but it is only RECOMMENDED when
                # a decision is actually the repair (issue #1443). On
                # `approval_not_terminal_archive` an approval already binds
                # this digest and the FSM-legal verdict is `defer` — running it
                # records a defer and leaves the gate refusing identically,
                # which is the inert-command loop `repair` exists to prevent.
                # The decide-repairable blockers name the cases where a NEW
                # decision genuinely clears the refusal.
                from okg.substrate.pact.repair import (
                    COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS,
                )

                decision_blockers = set(
                    (decision_gate.get("workflow_authority") or {}).get("blockers")
                    or []
                )
                if decision_blockers & COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS:
                    result.setdefault("recommended_commands", []).extend(
                        decide_plan["commands"]
                    )
        # Per `right-size-pact-authoring` req.rightsize.consistency: the
        # merge boundary reports changed paths the declared scope does not
        # cover — the drift that previously surfaced only as reconcile PRs.
        from okg.substrate.pact.project_settings import pr_base_policy
        from okg.substrate.pact.scope_drift import drift_report

        drift = drift_report(
            root,
            gate_manifest,
            base_branch=pr_base_policy(root),
        )
        result["scope_drift"] = drift
        if drift.get("warning"):
            result.setdefault("recommended_commands", []).append(
                f"declare change.affected_code for {args.change} or split the "
                "uncovered paths into their own change"
            )
        # A tracked code-proof receipt enables the narrow metadata-only
        # closure exception.  Validate it against this exact Git head; the
        # head/tree are external CI inputs and are never written into the
        # receipt they would otherwise recursively invalidate.
        if gate_manifest.proof_receipts.code:
            from okg.substrate.pact.proof_identity import (
                ProofIdentityError,
                validate_code_proof_receipts,
            )

            try:
                proof_identity = validate_code_proof_receipts(
                    root,
                    gate_manifest,
                    "HEAD",
                ).to_dict()
            except ProofIdentityError as exc:
                proof_identity = envelope.component(
                    ok=False,
                    applies=True,
                    **exc.to_dict(),
                )
                result = envelope.restate(
                    result,
                    envelope.REFUSED,
                    text=f"gate pre-merge {args.change}: refused (code proof identity)",
                    message=exc.message,
                    blocked_by=envelope.tokens(
                        result.get("blocked_by"),
                        [exc.code],
                    ),
                    next_commands=result.get("recommended_commands") or [],
                )
            else:
                proof_identity = envelope.component(ok=True, **proof_identity)
            result["code_proof_identity"] = proof_identity
        # Deferred closure must not silently become permanent: pending
        # intents are a first-class pre-merge warning.
        from okg.substrate.pact import closure_intents

        pending = closure_intents.pending_intents(root, args.change)
        if pending:
            result["pending_closure_intents"] = {
                "count": len(pending),
                "intents": [rec.get("intent_id") for rec in pending],
                "warning": (
                    f"{len(pending)} closure intent(s) queued while the "
                    "decision ledger was unreachable have not been synced"
                ),
            }
            result.setdefault("recommended_commands", []).append(
                f"okg pact evidence sync {args.change} --json"
            )
        projection_deferral = (
            decision_gate is not None
            and not decision_gate["ok"]
            # Never upgrade a refusal: if any other part of the gate already
            # refused, that refusal is the answer.
            and envelope.verdict_of(result) == envelope.RECORDED
            and decision_gate.get("status") == "approved"
            and wa.projection_only(
                (decision_gate.get("workflow_authority") or {}).get("blockers")
            )
        )
        if projection_deferral:
            # The ledger approved THIS intent digest and every check that does
            # not read the graph holds; the only thing missing is the
            # projection. That is exit 3 with an EMPTY `blocked_by`, not a
            # refusal (issue #1476): a green head must not wait on a projector
            # that is not running. Deferred is still nonzero, so a `set -e`
            # pipeline stops and a caller has to allow it deliberately.
            deferrals = (decision_gate.get("workflow_authority") or {})["blockers"]
            result["projection_deferrals"] = list(deferrals)
            result = envelope.restate(
                result,
                envelope.DEFERRED,
                text=(
                    f"gate pre-merge {args.change}: deferred "
                    f"({', '.join(deferrals)})"
                ),
                message=(
                    "approved, and every check that does not need the graph "
                    "projection holds; the projection itself could not be "
                    f"read ({', '.join(deferrals)}), so the gate reports what "
                    "it could not check instead of refusing"
                ),
                blocked_by=[],
                next_commands=result.get("recommended_commands") or [],
            )
        elif decision_gate is not None and not decision_gate["ok"]:
            # A failing BLOCKING gate may not be hand-patched onto `ok`:
            # `result["ok"] = result["ok"] and decision_gate["ok"]` left
            # `verdict: recorded` beside `ok: false` with an EMPTY
            # `blocked_by`, and the CLI exits on the verdict — so a
            # pre-merge whose decision gate failed exited 0. Re-stamp
            # through the constructor and union the cause in.
            result = envelope.restate(
                result,
                envelope.REFUSED,
                text=(
                    f"gate pre-merge {args.change}: refused "
                    f"(decision gate: {decision_gate.get('status')})"
                ),
                message=str(
                    decision_gate.get("reason")
                    or "the decision ledger has no approval for this intent digest"
                ),
                blocked_by=envelope.tokens(
                    result.get("blocked_by"),
                    ["decision_gate_unsatisfied"],
                    (
                        [f"decision_gate:{decision_gate['status']}"]
                        if decision_gate.get("status")
                        else []
                    ),
                    (decision_gate.get("workflow_authority") or {}).get("blockers")
                    or [],
                ),
                next_commands=result.get("recommended_commands") or [],
            )
        if external_block:
            # The verdict is not changed here — the decision gate above owns
            # that. What is added is the NAME of the authority the result was
            # computed under, and the receipt-level causes, so no reader has
            # to infer either from a payload that otherwise looks like an
            # ordinary graph-backed merge check.
            result = envelope.restate(
                result,
                envelope.verdict_of(result),
                text=(
                    f"{result.get('text')} "
                    f"[authority: {external_authority.AUTHORITY_SCOPE}]"
                ),
                blocked_by=envelope.tokens(
                    result.get("blocked_by"),
                    external_block.get("blockers") or [],
                ),
            )
        return result
    if subcmd == "gate" and args.gate_subcmd == "pre-implementation":
        return _pre_implementation_gate(root, args.change)
    if subcmd == "gate" and args.gate_subcmd == "graph-freshness":
        if args.refresh:
            return _sync_pact_projection(
                root, args.change, deployment=args.deployment, dry_run=args.dry_run,
            )
        return _graph_freshness_gate(root, args.change)
    if subcmd == "hooks" and args.hooks_subcmd == "install":
        return install_gate_hooks(
            root,
            args.change,
            target=args.target,
            dry_run=args.dry_run,
            force=args.force,
        )
    # sync / bind-tla / advance / normalize / review / render / check-view /
    # edit dispatch arms deleted per `complete-pact-surface-cut`
    # task.delete-verbs (folded into gate / evidence / view / direct-YAML;
    # the operations remain). `recheck-alignment` / `version` were the
    # earlier `simplify-pact-surface` batch.
    if subcmd == "migrate-openspec":
        manifest = migrate_openspec(root, args.change)
        return envelope.recorded(
            text=f"migrated {manifest.change.id} from OpenSpec",
            change=manifest.change.id,
            path=str(root / "pact" / "changes" / manifest.change.id / "pact.yaml"),
        )
    if subcmd == "migrate-version":
        manifest = migrate_version(
            root,
            args.change,
            target_version=args.target,
            artifact_version=args.artifact_format,
        )
        return envelope.recorded(
            text=(
                f"migrated {manifest.change.id} to pact_version "
                f"{manifest.target_pact_version}"
            ),
            change=manifest.change.id,
            **{
                "artifact_format_version": manifest.pact_version,
                "target_pact_version": manifest.target_pact_version,
                **version_info(manifest),
            },
        )
    raise SubstrateError("unknown pact subcommand", data={"subcmd": subcmd})


# _metadata_list_arg / _cmd_validate / _manifest_for_validate_arg /
# validate_schema_only deleted per `complete-pact-surface-cut`
# task.delete-verbs — they were the `validate` / `change update` handlers,
# now dead with those verbs. Validation is reached via `gate post-edit` (the
# `validate_manifest` operation in validation.py is unchanged).


def _requirement_waived(manifest, req_id: str) -> bool:
    """True iff an owner-approved waiver excuses this requirement.

    Mirrors validation's `_waiver_for`: a waiver counts only when it is scoped
    to the requirement AND attributed to an owner (the accountability anchor).
    """
    return any(
        waiver.scope == req_id and waiver.owner for waiver in manifest.waivers
    )


def _task_terminal(manifest, task) -> bool:
    """True iff a task is terminal — no longer active backlog work.

    A task is terminal when it is `done`, OR when every requirement it carries
    is excused by an owner-approved waiver (consciously declined or deferred,
    with a recorded reason). We deliberately do NOT force such a task to
    `done`: the waiver — not a false completion — is the honest record, and a
    task-less or no-requirement task can only be terminal by being `done`.
    """
    if task.status == "done":
        return True
    return bool(task.requirements) and all(
        _requirement_waived(manifest, req_id) for req_id in task.requirements
    )


def _review_view_status(root: Path, manifest) -> dict[str, Any]:
    target_version = manifest.target_pact_version or manifest.pact_version
    if target_version >= 4:
        return envelope.component(ok=True, status="on-demand")
    return check_markdown_view(root, manifest.change.id)


def _local_operator_status(status: dict[str, Any], *, surface: str) -> dict[str, Any]:
    guidance = status.get("agent_guidance") or {}
    hierarchy = status.get("hierarchy") or {}
    view = status.get("review_view") or {}
    retired = bool(status.get("retired", False))
    blocked_count = len(status.get("blocked_tasks") or [])
    environment_blocker_count = len(status.get("environment_blockers") or [])
    evidence_gap_count = len(status.get("evidence_gaps") or [])
    next_count = len(status.get("next") or [])
    task_state = guidance.get("state")
    review_view_status = view.get("status")

    if not status.get("ok", False):
        queue_state = "invalid"
        authoritative = "validation"
    elif retired:
        queue_state = "retired"
        authoritative = "retired"
    elif blocked_count or environment_blocker_count:
        queue_state = "blocked"
        authoritative = "task_state"
    elif evidence_gap_count:
        queue_state = "needs_evidence"
        authoritative = "evidence"
    elif review_view_status in {"missing", "stale"}:
        queue_state = "refresh_review"
        authoritative = "review_view"
    elif next_count or task_state == "ready":
        queue_state = "ready"
        authoritative = "task_state"
    else:
        queue_state = task_state or "unknown"
        authoritative = "task_state" if task_state else "local_status"

    followup_backlog = "none"
    if retired and (blocked_count or hierarchy.get("children")):
        followup_backlog = "external_issue_or_child_pact_backlog"

    return {
        "surface": surface,
        "authoritative": authoritative,
        "queue_state": queue_state,
        "local_status": status.get("status"),
        "task_state": task_state,
        "retired": retired,
        "review_view_status": review_view_status,
        "hierarchy_role": hierarchy.get("role"),
        "followup_backlog": followup_backlog,
        "blocker_count": blocked_count,
        "environment_blocker_count": environment_blocker_count,
        "evidence_gap_count": evidence_gap_count,
        "next_count": next_count,
    }


def _invalid_list_operator_status(
    *, change_id: str, message: str | None = None,
) -> dict[str, Any]:
    return {
        "surface": "list",
        "authoritative": "validation",
        "queue_state": "invalid",
        "local_status": None,
        "task_state": "unreadable",
        "retired": False,
        "review_view_status": None,
        "hierarchy_role": None,
        "followup_backlog": "none",
        "blocker_count": 0,
        "environment_blocker_count": 0,
        "evidence_gap_count": 0,
        "next_count": 0,
        "change": change_id,
        "message": message,
    }


def _status_evidence_gaps(
    root: Path,
    manifest: Any,
    alignment: Mapping[str, Any],
    *,
    evaluation_context: PactEvaluationContext | None = None,
) -> list[dict[str, Any]]:
    """Merge static policy gaps with revocations from scope-drift evaluation."""
    from okg.substrate.pact.scope_drift import staleness_report

    gaps = [
        {"id": req_id, "missing": list(state["missing"])}
        for req_id, state in alignment["requirements"].items()
        if state.get("satisfaction_status") == "unsatisfied"
        or state.get("status") == "missing_evidence"
    ]
    gaps_by_requirement = {gap["id"]: gap for gap in gaps}

    for requirement in manifest.requirements:
        staleness = staleness_report(
            root,
            manifest,
            requirement.id,
            context=evaluation_context,
        )
        if not staleness.get("revoked"):
            continue

        stale_ids = [
            str(item["evidence_id"])
            for item in staleness.get("stale", [])
            if item.get("evidence_id")
        ]
        satisfying_rows = [
            evidence
            for evidence in manifest.evidence
            if requirement.id in (evidence.refs or [])
            and evidence.status in {"pass", "waived"}
        ]
        revoked_ids = list(dict.fromkeys([
            *stale_ids,
            *(str(evidence.id) for evidence in satisfying_rows),
        ]))
        missing_obligations = [
            (
                "executed_test"
                if kind == "planned_test"
                and (manifest.target_pact_version or manifest.pact_version) >= 1
                else kind
            )
            for kind in requirement.evidence_policy.required
        ]
        if requirement.evidence_policy.required_any:
            missing_obligations.append(
                "any:" + "|".join(requirement.evidence_policy.required_any)
            )

        gap = gaps_by_requirement.get(requirement.id)
        if gap is None:
            gap = {"id": requirement.id, "missing": []}
            gaps.append(gap)
            gaps_by_requirement[requirement.id] = gap
        gap["missing"] = sorted(set([*gap["missing"], *missing_obligations]))

        proof_identity = staleness.get("proof_identity") or {}
        if staleness.get("freshness_authority") == "invalid_code_proof":
            reason = "invalid_code_proof"
            boundary = {
                "status": "invalid_proof_identity",
                **(
                    {"error": proof_identity["error"]}
                    if proof_identity.get("error")
                    else {}
                ),
            }
            action = (
                "Repair the code-proof receipt, then rerun and attach the "
                "required evidence."
            )
        elif staleness.get("authority_missing"):
            reason = "git_authority_missing"
            boundary = {
                "status": "unavailable",
                "reason": staleness.get("reason"),
                **(
                    {"head_sha": staleness["head_sha"]}
                    if staleness.get("head_sha")
                    else {}
                ),
            }
            action = (
                "Restore Git authority for the affected scope, then rerun and "
                "attach evidence after the resolved code-change boundary."
            )
        elif staleness.get("code_changed_at"):
            reason = "code_changed"
            code_changed_at = str(staleness["code_changed_at"])
            boundary = {
                "status": "known",
                "code_changed_at": code_changed_at,
                **(
                    {"head_sha": staleness["head_sha"]}
                    if staleness.get("head_sha")
                    else {}
                ),
            }
            action = (
                f"Rerun and attach fresh evidence after {code_changed_at}."
            )
        else:
            reason = "boundary_unavailable"
            boundary = {
                "status": "unavailable",
                "reason": staleness.get("reason") or "scope-drift boundary unavailable",
            }
            action = (
                "Restore the scope-drift authority, then rerun and attach the "
                "required evidence."
            )

        gap.update({
            "gap_type": "scope_drift",
            "reason": reason,
            "revoked_evidence_ids": revoked_ids,
            "affected_code": list(manifest.change.affected_code or []),
            "code_change_boundary": boundary,
            "action": action,
        })
        if staleness.get("code_changed_at"):
            gap["code_changed_at"] = str(staleness["code_changed_at"])

    return gaps


def _status(
    root: Path,
    change_id: str,
    *,
    include_graph: bool = True,
    decision_events: list[Any] | None = None,
    ledger_consulted: bool | None = None,
    manifest_entry: ManifestInventoryEntry | None = None,
    validation_report: Any | None = None,
    manifest_inventory: ManifestInventory | None = None,
    inventory_validation_reports: dict[str, Any] | None = None,
    captured_hierarchy_manifests: Mapping[str, Any] | None = None,
    captured_hierarchy_failures: Mapping[str, Exception] | None = None,
    evaluation_context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    manifest = (
        manifest_entry.manifest
        if manifest_entry is not None
        else _load_manifest_for_gate(root, change_id)
    )
    if manifest is None:
        if manifest_entry is not None and manifest_entry.error is not None:
            raise manifest_entry.error
        raise SubstrateError(f"PACT manifest is unreadable: {change_id}")
    report = validation_report or validate_manifest(
        manifest,
        repo_root=root,
        context=evaluation_context,
    )
    alignment = report.alignment
    view = _review_view_status(root, manifest)
    target_version = manifest.target_pact_version or manifest.pact_version
    if target_version >= 4:
        evidence_gaps = _status_evidence_gaps(
            root,
            manifest,
            alignment,
            evaluation_context=evaluation_context,
        )
    else:
        evidence_gaps = [
            {"id": req_id, "missing": state["missing"]}
            for req_id, state in alignment["requirements"].items()
            if state.get("satisfaction_status") == "unsatisfied"
            or state.get("status") == "missing_evidence"
        ]
    task_evidence_issues = _task_evidence_issues(alignment)
    environment_blockers = _environment_blockers(alignment)
    blocked_tasks = _blocked_tasks(manifest, alignment)
    normative_v4 = target_version >= 4
    if normative_v4:
        # Lifecycle is derived once through the normative v4 state machine.
        # Terminal tasks remain awaiting review until a digest-bound ledger
        # decision archives the change.
        if ledger_consulted is None:
            # Preserve the single-change helper seam used by callers/tests.
            # Repository list mode supplies one captured ledger snapshot and
            # the matching validation report through the optimized arm below.
            derived_state = _derived_state_payload(root, change_id)
        else:
            derived_state = _derived_state_payload(
                root,
                change_id,
                decision_events=decision_events,
                ledger_consulted=ledger_consulted,
                manifest=manifest,
                validation_report=report,
            )
        lifecycle_state = str(derived_state.get("state") or "draft")
        next_tasks = (
            _next_tasks(manifest, alignment=alignment)
            if lifecycle_state == "in_progress"
            else []
        )
        retired = lifecycle_state == "retired"
    else:
        # v0-v3 keep their historical authored-status/task retirement
        # contract. Applying v4's decision boundary here would silently hide
        # almost the entire legacy corpus from status/list/bootstrap.
        lifecycle_state = manifest.change.status
        derived_state = {
            "state": lifecycle_state,
            "authoritative": False,
            "source": "legacy_authored_status",
        }
        next_tasks = _next_tasks(manifest, alignment=alignment)
        retired = (
            bool(manifest.tasks)
            and all(_task_terminal(manifest, task) for task in manifest.tasks)
            and report.ok
        )
    payload = envelope.from_ok(
        report.ok,
        text=(
            f"status {manifest.change.id}: {lifecycle_state}, "
            f"errors={sum(1 for i in report.issues if i.severity == 'error')}"
        ),
        blocked_by=envelope.tokens(
            issue.code
            for issue in report.issues
            if issue.severity == "error"
        ),
        **{
        "change": manifest.change.id,
        "file_authority": manifest_file_authority(
            root,
            manifest.change.id,
            manifest,
            captured_path=(manifest_entry.path if manifest_entry else None),
            captured_state_digest=(
                manifest_entry.state_digest if manifest_entry else None
            ),
        ),
        "status": manifest.change.status,
        "state": lifecycle_state,
        "normative_v4": normative_v4,
        "retired": retired,
        "derived_state": derived_state,
        "requirements": len(manifest.requirements),
        "tasks": len(manifest.tasks),
        "versions": alignment.get("versions", {}),
        "lifecycle": alignment.get("lifecycle", {}),
        # The brief must speak with the same voice as `derived_state` in this
        # same payload; publishing a second, file-only answer beside it is how
        # a reader was told `revise` on a change the ledger says is elsewhere.
        "operator_brief": operator_brief(
            manifest,
            alignment,
            state=lifecycle_state if normative_v4 else None,
        ),
        "evidence_gaps": evidence_gaps,
        "missing_evidence": evidence_gaps,
        "task_evidence_issues": task_evidence_issues,
        "hierarchy": _hierarchy_rollup(
            root,
            manifest,
            manifest_inventory=manifest_inventory,
            validation_reports=inventory_validation_reports,
            captured_manifests=captured_hierarchy_manifests,
            captured_failures=captured_hierarchy_failures,
            context=evaluation_context,
        ),
        "environment_blockers": environment_blockers,
        "blocked_tasks": blocked_tasks,
        "next": next_tasks,
        "agent_guidance": _agent_guidance(
            manifest=manifest,
            report_ok=report.ok,
            view=view,
            next_tasks=next_tasks,
            blocked_tasks=blocked_tasks,
            evidence_gaps=evidence_gaps,
            environment_blockers=environment_blockers,
            lifecycle=alignment.get("lifecycle", {}),
            derived_state=derived_state,
            normative_v4=normative_v4,
        ),
        "review_view": view,
        "human_review": str(_manifest_path_for_gate(root, change_id).with_name("pact.md")),
        "errors": [i.to_dict() for i in report.issues],
        },
    )
    payload["operator_status"] = _local_operator_status(payload, surface="status")
    if include_graph:
        payload["graph_projection"] = _graph_projection_status(manifest, root)
    return payload


TIER_WARN_DIFF_LINES_DEFAULT = 200


def _tier_size_warning(
    root: Path,
    change_id: str | None = None,
) -> dict[str, Any] | None:
    """Per `right-size-pact-authoring` req.rightsize.tiers: a staged diff
    past the size threshold gets a visible tier warning — the tier-0 escape
    (commit-only, no PACT) stays legal, but never invisible. The commit
    message does not exist yet at pre-commit, so the trailer itself cannot
    be checked; the warning names the exact trailer to carry."""
    import subprocess as _sp

    threshold = TIER_WARN_DIFF_LINES_DEFAULT
    try:
        from okg.substrate.pact.project_settings import load_project_settings

        tier_policy = load_project_settings(root).get("tier_policy") or {}
        threshold = int(
            tier_policy.get("pre_commit_warn_diff_lines", threshold)
        )
    except Exception:  # noqa: BLE001
        # Unreadable project settings must not break the pre-commit hook;
        # the module default threshold is the contract in that case.
        threshold = TIER_WARN_DIFF_LINES_DEFAULT
    try:
        proc = _sp.run(
            ["git", "-C", str(root), "diff", "--cached", "--numstat"],
            capture_output=True, text=True, timeout=10, check=False,
        )
    except (OSError, _sp.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    total = 0
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2:
            for cell in parts[:2]:
                if cell.isdigit():
                    total += int(cell)
    if total <= threshold:
        return None
    trailer = f"Pact-Id: {change_id}" if change_id else "Pact-Id: <change>"
    return {
        "staged_lines": total,
        "threshold": threshold,
        "warning": (
            f"tier policy: {total} staged lines exceed the {threshold}-line "
            f"threshold — a diff this size needs a PACT (tier 1+) and a "
            f"`{trailer}` commit trailer (not verifiable at pre-commit; "
            "committing without one — tier 0 — stays legal but visible)"
        ),
    }


def _decide_recommendation(
    root: Path,
    change_id: str,
    *,
    decisions: list[Any] | None = None,
    ledger_consulted: bool = False,
) -> dict[str, Any]:
    """What `decide` a gate may legally recommend right now, and why.

    Reads the derived state through the same path `decide` will, so the
    recommendation and the enforcement cannot disagree. When the state cannot
    be derived (no ledger, unreadable manifest) it recommends NOTHING rather
    than guessing a verdict — an unverifiable recommendation is the defect
    this replaces.

    ``decisions`` is the change's LEDGER HISTORY, which is half of what derives
    the state (fsm.derive_state: the other half is manifest condition). Passing
    an empty list instead re-created the nine-occurrence bug from the other
    side: a change whose recorded history says `in_progress` derived as
    `proposed`, `approve` looked legal, and the very next call answered
    "verdict approve is not legal in state in_progress". An unreachable ledger
    is reported as such — never silently derived from `[]`.
    """
    from okg.substrate.pact import fsm as fsm_mod
    from okg.substrate.pact import repair as repair_mod
    from okg.substrate.pact import review as review_mod

    prefix = repair_mod.repo_command(root)
    if not ledger_consulted:
        return {
            "state": None,
            "legal_verdicts": [],
            "commands": [],
            "ledger_consulted": False,
            "why": (
                "the decision ledger is unreachable, so the lifecycle state "
                "cannot be derived; no verdict is recommended (a verdict "
                "derived from an empty history is a guess the next call "
                "rejects)"
            ),
        }
    try:
        manifest = _load_manifest_for_gate(root, change_id)
        derived = review_mod.derive_change_state(
            manifest, list(decisions or []), repo_root=root
        )
        state = derived.state
    except Exception as exc:  # noqa: BLE001 - a gate must still report
        return {
            "state": None,
            "legal_verdicts": [],
            "commands": [],
            "ledger_consulted": True,
            "why": f"lifecycle state could not be derived ({exc})",
        }
    legal = fsm_mod.legal_verdicts(state, is_operator=False)
    commands = repair_mod.legal_decide_commands(change_id, legal, prefix=prefix)
    if "approve" in legal:
        return {
            "state": state,
            "legal_verdicts": legal,
            "commands": commands,
            "ledger_consulted": True,
            "decision_count": len(list(decisions or [])),
            "why": f"approve is legal in state {state}",
        }
    path = fsm_mod.enabling_path(state, "approve", is_operator=False)
    enabling = repair_mod.enabling_commands(
        path,
        change_id,
        prefix=prefix,
        open_tasks=review_mod.open_task_ids(manifest),
    )
    return {
        "state": state,
        "legal_verdicts": legal,
        "enabling_path": path.to_dict(),
        "commands": enabling or commands,
        "ledger_consulted": True,
        "decision_count": len(list(decisions or [])),
        "why": (
            f"approve is not legal in state {state}; "
            + (
                f"reach {path.target_state} first ({' then '.join(path.events)})"
                if path.reachable
                else f"no path reaches an approvable state ({path.unreachable_reason})"
            )
        ),
    }


def _ledger_remedy_for(
    payload: dict,
    reason: Any = None,
    *,
    authority: dict | None = None,
) -> dict[str, Any]:
    """The remedy for THIS payload's ledger blocker, from its real cause.

    `evidence attach` / `ingest` carry no `workflow_authority` block, so this
    used to hand `ledger_remedy` a bare reason string and get the DEFAULTED
    remedy — a different fix from the one `gate task-done` named in the same
    shell for the same dead ledger. The classified authority now travels on the
    closure intent and is passed in explicitly (req.refusal.complete).
    """
    from okg.substrate.pact import repair as repair_mod

    resolved = dict(
        authority
        or (payload.get("workflow_authority") or {}).get("ledger_authority")
        or payload.get("ledger_authority")
        or {}
    )
    if reason and not resolved.get("reason"):
        resolved["reason"] = reason
    return repair_mod.ledger_remedy(resolved)


def _deferred_closure_commands(payload: dict, deferred: dict) -> list[str]:
    """`next_commands` for a queued closure — deliberately NOT `evidence sync`.

    The audited dead-ledger transcript looped: the deferral recommended
    `okg pact evidence sync <id> --json  # once the ledger is reachable`, the
    agent ran it, sync refused for the same reason, and the deferral
    recommended it again. A command whose precondition is the very blocker it
    is offered to clear is not a repair, and the trailing comment made it
    unparseable besides. The sync command stays on the payload as
    `closure_intent.sync_command` — the thing to run AFTER an operator
    restores the ledger — and `next_commands` carries only what advances state
    now.
    """
    from okg.substrate.pact import repair as repair_mod

    del deferred
    return repair_mod.executable_commands(payload.get("recommended_commands") or [])


def _deferred_closure_why(payload: dict, deferred: dict) -> str:
    remedy = _ledger_remedy_for(
        payload,
        deferred.get("reason"),
        authority=deferred.get("ledger_authority"),
    )
    return (
        "the proof is complete and the closure is queued locally; only the "
        f"decision-ledger write is outstanding. {remedy['remedy']} "
        f"(owner: {remedy['owner']}), then run "
        f"{deferred.get('sync_command')}. No okg pact command clears this "
        "blocker, so none is recommended."
    )


def _with_closure_intents(root: Path, change_id: str, payload: dict) -> dict:
    """Surface deferred coordination on evidence verbs — an unreachable
    ledger queues intents, and the command output must say so loudly
    (req.polish.deferred-closure: never a silent pass)."""
    from okg.substrate.pact import closure_intents

    pending = closure_intents.pending_intents(root, change_id)
    if not pending:
        return payload
    sync_command = f"okg pact evidence sync {change_id} --json"
    payload["coordination"] = "deferred"
    payload["pending_closure_intents"] = len(pending)
    payload["ledger_reason"] = pending[-1].get("ledger_reason")
    payload["sync_command"] = sync_command
    remedy = _ledger_remedy_for(
        payload,
        pending[-1].get("ledger_reason"),
        authority=pending[-1].get("ledger_authority"),
    )
    payload["owner"] = remedy["owner"]
    payload["remedy"] = remedy["remedy"]
    payload["operator_action"] = remedy
    # The mutation persisted locally, but the ledger write is queued — the
    # verdict says so instead of leaving `ok: true` beside a `coordination:
    # deferred` an agent has to know to look for.
    return envelope.restate(
        payload,
        envelope.DEFERRED,
        text=(
            f"{payload.get('text') or change_id} "
            f"[deferred: {len(pending)} closure intent(s) queued]"
        ),
        message=str(
            pending[-1].get("ledger_reason") or "decision ledger unreachable"
        ),
        blocked_by=["ledger_not_fresh"],
        # `evidence sync` cannot clear an unreachable ledger — recommending it
        # here is the audited loop. It is on the payload as `sync_command`,
        # to run once the operator has restored the connection.
        next_commands=[],
        why=(
            f"the local write landed; the ledger write is queued. "
            f"{remedy['remedy']} (owner: {remedy['owner']}), then run "
            f"{sync_command}."
        ),
    )


POST_EDIT_SUMMARY_BUDGET_BYTES = 2048


def _post_edit_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """Budget the default gate output (req.budget.machine-surfaces).

    Today's full report measures ~23 KB; agents carry it in context on
    every one of thousands of gate calls. The summary keeps what drives
    the next action - verdict, violated guards, view freshness, next
    command - inside a hard 2 KB budget. Errors are capped at 10 with a
    count, never silently dropped; the full report stays one --full away.
    """
    # v4 post_edit_check nests the validation report under `validation:`;
    # older payloads carried top-level errors/warnings. Read both so the
    # summary never reports 0 for a manifest that has issues.
    validation = payload.get("validation") or {}
    errors = payload.get("errors") or validation.get("errors") or []
    warnings = payload.get("warnings") or validation.get("warnings") or []
    view = payload.get("review_view") or {}
    graph = payload.get("graph_projection") or {}
    guidance = payload.get("agent_guidance") or {}
    next_cmd = None
    commands = (
        guidance.get("recommended_commands")
        or payload.get("next_commands")
        or payload.get("recommended_commands")
        or []
    )
    if commands:
        next_cmd = commands[0]
    intent = payload.get("intent_registration") or {}
    verdict = envelope.verdict_of(payload)
    review_view = view.get("status") if isinstance(view, dict) else view
    summary: dict[str, Any] = envelope.envelope(
        verdict,
        text=(
            f"post-edit {payload.get('change')}: {verdict} "
            f"errors={len(errors)} view={review_view}"
            + (f" intent={intent.get('status')}" if intent else "")
            + (f" next: {next_cmd}" if next_cmd else "")
        ),
        blocked_by=payload.get("blocked_by") or [],
        next_commands=commands,
        change=payload.get("change"),
        state=payload.get("state"),
        next_action=payload.get("next_action"),
        error_count=len(errors),
        # `message` on EVERY row. The compact form carried code + path only,
        # and pointed at `--full` for the text; "rerun with --full" was
        # emitted 406 times and honored 9, so agents recovered the message
        # with ~450 extra `view` calls instead. A refusal that needs a second
        # invocation to be understood is not complete
        # (pact-self-repairing-refusals req.refusal.complete).
        errors=[
            {
                "code": e.get("code"),
                "path": e.get("path"),
                "message": e.get("message"),
            }
            for e in errors[:10]
        ],
        warning_count=len(warnings),
        warnings=[
            {
                "code": w.get("code"),
                "path": w.get("path"),
                "message": w.get("message"),
            }
            for w in warnings[:5]
        ],
        repair=_compact_repair(payload.get("repair")),
        review_view=review_view,
        graph_projection=(
            {"status": graph.get("status"), "stale": graph.get("stale")}
            if graph else None
        ),
        next_command=next_cmd,
        intent_registration=payload.get("intent_registration"),
        # The flag still exists for VERBOSE ARTIFACTS (the full validation
        # report, the semantic diff). It is named as a command, not as the
        # instruction "rerun with --full" that used to stand in for the
        # message this payload now carries.
        full_report=(
            f"okg pact gate post-edit {payload.get('change')} --full --json"
        ),
    )
    # Size never lies. Overflow used to set ok=False, turning a byte problem
    # into a fake gate failure; now it trims the verbose lists and leaves the
    # verdict, blocked_by, message, next_commands, and text intact.
    return envelope.fit_budget(
        summary,
        POST_EDIT_SUMMARY_BUDGET_BYTES,
        trim=(("errors", 3), ("warnings", 2)),
    )


def _read_verdict(payload: Any, *, source: str) -> tuple[bool, list[str]]:
    """A sub-report's ``ok``, with an unknown verdict failing closed.

    #2573 slice 10 (decision D1): a payload with no ``ok``, or a non-boolean
    one, is not a pass. The verdict is False and the causes are
    ``<source>_unknown`` plus the ``read_ok`` reason, so ``blocked_by`` tells
    an unknown sub-check from a failed one. A real boolean comes back as is,
    with no causes, so well-formed payloads behave exactly as before.
    """
    verdict = read_ok(payload, source=source)
    if isinstance(verdict, Unknown):
        return False, [f"{source}_unknown", verdict.reason]
    return verdict, []


def _compact_repair(repair: Any) -> dict[str, Any] | None:
    """The repair block, inside the compact gate's byte budget.

    Kept rather than dropped: the whole point of the compact form is that it
    is ACTIONABLE without a second call, and the edit pointer plus the legal
    set is what makes an edit applicable blind.
    """
    if not isinstance(repair, dict):
        return None
    edits = []
    for edit in (repair.get("manifest_edits") or [])[:5]:
        row = {
            key: value
            for key, value in edit.items()
            if key in ("code", "pointer", "action", "message")
        }
        legal = edit.get("legal_values") or []
        if legal:
            row["legal_values"] = legal[:12]
            if len(legal) > 12:
                row["legal_values_total"] = len(legal)
        edits.append(row)
    compact: dict[str, Any] = {"manifest_edits": edits}
    if repair.get("operator_action"):
        compact["operator_action"] = repair["operator_action"]
    return compact


def _brief(root: Path, change_id: str) -> dict[str, Any]:
    status = _status(root, change_id)
    status["derived_state"] = _derived_state_payload(root, status["change"])
    status_ok, status_unknown = _read_verdict(status, source="pact_status")
    payload = envelope.from_ok(
        status_ok,
        # The brief's `text` is the rendered human brief, printed raw in
        # non-JSON mode — not the envelope's one-line summary.
        text=_brief_text(status),
        blocked_by=[*status_unknown, *(status.get("blocked_by") or [])],
        next_commands=(status.get("agent_guidance") or {}).get(
            "recommended_commands", []
        ),
        **{
        "change": status["change"],
        "operator_brief": status["operator_brief"],
        "agent_guidance": status["agent_guidance"],
        "review_view": status["review_view"],
        "human_review": status["human_review"],
        "graph_projection": status.get("graph_projection"),
        "derived_state": status["derived_state"],
        },
    )
    # The decision brief carries the same scope-drift warning the pre-merge
    # gate reports (right-size-pact-authoring req.rightsize.consistency).
    try:
        from okg.substrate.pact.project_settings import pr_base_policy
        from okg.substrate.pact.scope_drift import drift_report

        drift = drift_report(
            root,
            load_manifest(root, change_id),
            base_branch=pr_base_policy(root),
        )
        if drift.get("warning"):
            payload["scope_drift"] = drift
            payload["operator_brief"]["scope_drift_warning"] = drift["warning"]
    except SubstrateError:
        # An unloadable manifest cannot be drift-checked; the brief's other
        # sections still render (validation reports the manifest problem).
        payload.setdefault(
            "scope_drift",
            envelope.component(ok=None, status="unavailable"),
        )
    return payload


def _chat_review(root: Path, change_id: str) -> dict[str, Any]:
    status = _status(root, change_id)
    status["derived_state"] = _derived_state_payload(root, status["change"])
    manifest = _load_manifest_for_gate(root, change_id)
    packet = _chat_review_packet(manifest, status)
    status_ok, status_unknown = _read_verdict(status, source="pact_status")
    return envelope.from_ok(
        status_ok,
        text=_chat_review_text(packet),
        blocked_by=[*status_unknown, *(status.get("blocked_by") or [])],
        change=status["change"],
        format="chat-review",
        chat_review=packet,
    )


CHAT_REVIEW_QUEUE_MAX_ROWS = 20
CHAT_REVIEW_QUEUE_MAX_STALE_ROWS = 3


def _chat_review_queue(root: Path, *, query: str | None = None) -> dict[str, Any]:
    from okg.substrate.pact.review import build_queue

    raw = build_queue(root)
    all_rows = [
        _chat_queue_row(row, number=i + 1)
        for i, row in enumerate(raw.get("queue") or [])
    ]
    rows = all_rows[:CHAT_REVIEW_QUEUE_MAX_ROWS]
    stale_claims = _dedupe_stale_claims(raw.get("in_flight") or [])
    stale_rows = [
        _chat_stale_queue_row(root, row)
        for row in stale_claims[:CHAT_REVIEW_QUEUE_MAX_STALE_ROWS]
    ]
    stale_total = len(stale_claims)
    action_total = raw.get("action_count", raw.get("count", len(all_rows)))
    decision_total = raw.get(
        "decision_count",
        sum(row.get("action") == "decision" for row in all_rows),
    )
    rework_total = raw.get(
        "rework_count",
        sum(row.get("action") == "rework" for row in all_rows),
    )
    archive_total = raw.get(
        "archive_count",
        sum(row.get("action") == "archive" for row in all_rows),
    )
    packet = _chat_review_inbox_packet(
        rows=rows,
        all_rows=all_rows,
        stale_rows=stale_rows,
        stale_total=stale_total,
        warnings=raw.get("warnings", []),
        total=action_total,
        query=query,
    )
    packet["shown"] = len(rows)
    packet["total"] = action_total
    packet["decision_total"] = decision_total
    packet["rework_total"] = rework_total
    packet["archive_total"] = archive_total
    packet["rows"] = rows
    packet["stale_rows"] = stale_rows
    packet["stale_total"] = stale_total
    packet["warnings"] = [_chat_queue_warning(w) for w in raw.get("warnings", [])]
    packet["rank_note"] = _chat_queue_rank_note(rows)
    queue_ok, queue_unknown = _read_verdict(raw, source="pact_review_queue")
    return envelope.from_ok(
        queue_ok,
        text=_chat_review_queue_text(packet),
        blocked_by=[*queue_unknown, *(raw.get("blocked_by") or [])],
        format="chat-review-queue",
        chat_review_queue=packet,
    )


def _chat_review_inbox_packet(
    *,
    rows: list[dict[str, Any]],
    all_rows: list[dict[str, Any]],
    stale_rows: list[dict[str, Any]],
    stale_total: int,
    warnings: list[str],
    total: int,
    query: str | None,
) -> dict[str, Any]:
    mode = _chat_review_query_mode(query)
    buckets = _bucket_review_rows(all_rows, stale_total=stale_total)
    warning_lines = [_chat_queue_warning(w) for w in warnings]
    if mode == "batch_low_risk":
        return _batch_low_risk_packet(
            buckets=buckets,
            stale_rows=stale_rows,
            query=query,
            warnings=warning_lines,
        )
    if mode == "blockers":
        return _blockers_packet(
            buckets=buckets,
            query=query,
            warnings=warning_lines,
        )
    if mode == "judgment":
        return _judgment_packet(
            buckets=buckets,
            query=query,
            warnings=warning_lines,
        )
    if mode == "risk_plan":
        return _risk_plan_packet(
            buckets=buckets,
            query=query,
            warnings=warning_lines,
        )
    if mode == "changed":
        return _changed_packet(
            buckets=buckets,
            query=query,
            warnings=warning_lines,
        )
    if mode == "ranked":
        return _ranked_packet(
            rows=rows,
            total=total,
            query=query,
            warnings=warning_lines,
        )
    if mode == "two_minutes":
        return _two_minute_packet(
            buckets=buckets,
            query=query,
            warnings=warning_lines,
        )
    if mode == "stale":
        return _stale_packet(
            stale_rows=stale_rows,
            stale_total=stale_total,
            query=query,
            warnings=warning_lines,
        )
    return _default_inbox_packet(
        rows=rows,
        buckets=buckets,
        total=total,
        query=query,
        warnings=warning_lines,
    )


def _chat_review_query_mode(query: str | None) -> str:
    q = _clean_prose(query).lower()
    if not q:
        return "summary"
    if "batch" in q and ("low" in q or "risk" in q):
        return "batch_low_risk"
    if "block" in q or "continue" in q or "agent" in q:
        return "blockers"
    if "judgment" in q or "judgement" in q or "product" in q:
        return "judgment"
    if "changed" in q or "since" in q or "last looked" in q:
        return "changed"
    if (
        "rank" in q
        or "order" in q
        or "why is 1" in q
        or "why first" in q
        or "what is 1" in q
        or "show queue" in q
    ):
        return "ranked"
    if "risk" in q or "hour" in q or "plan" in q:
        return "risk_plan"
    if "two minutes" in q or "2 minutes" in q or "attention" in q:
        return "two_minutes"
    if "stale" in q or "cleanup" in q:
        return "stale"
    return "summary"


def _bucket_review_rows(
    rows: list[dict[str, Any]], *, stale_total: int,
) -> dict[str, Any]:
    buckets: dict[str, Any] = {
        "urgent": [],
        "judgment": [],
        "low_risk": [],
        "cleanup": [],
        "other": [],
        "stale_total": stale_total,
    }
    for row in rows:
        bucket = row.get("bucket") or "other"
        buckets.setdefault(bucket, []).append(row)
    buckets["counts"] = {
        "urgent": len(buckets["urgent"]),
        "judgment": len(buckets["judgment"]),
        "low_risk": len(buckets["low_risk"]),
        "cleanup": len(buckets["cleanup"]),
        "stale": stale_total,
        "other": len(buckets["other"]),
        "decision": sum(row.get("action") == "decision" for row in rows),
        "rework": sum(row.get("action") == "rework" for row in rows),
        "archive": sum(row.get("action") == "archive" for row in rows),
    }
    return buckets


def _default_inbox_packet(
    *,
    rows: list[dict[str, Any]],
    buckets: dict[str, Any],
    total: int,
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    counts = buckets["counts"]
    urgent = buckets["urgent"]
    suggested = _suggested_inbox_mode(buckets)
    answer = (
        f"{len(urgent)} urgent {_plural('blocker', len(urgent))} "
        f"{_verb('need', len(urgent))} attention."
        if urgent
        else "Nothing urgent is blocking active work."
    )
    packet = {
        "heading": "Review Inbox",
        "mode": "summary",
        "query": query,
        "interpreted_as": "current review inbox",
        "answer": answer,
        "summary_lines": [
            f"Urgent blockers: {counts['urgent']}",
            f"Needs human judgment: {counts['judgment']}",
            f"Low-risk completed review: {counts['low_risk']}",
            (
                f"Stale cleanup: {counts['stale']}; queue actions: "
                f"{counts['decision']} decision, {counts['rework']} rework, "
                f"{counts['archive']} archive"
            ),
        ],
        "sections": [],
        "show_ranked_rows": False,
        "notes": [f"Best next move: {suggested}."],
        "reply_options": [
            "review urgent",
            "batch low-risk",
            "where does my judgment matter?",
            "what blocks agents?",
            "show stale",
            "stop",
        ],
        "warnings": warnings,
    }
    if urgent:
        packet["sections"].append({"title": "Urgent", "rows": urgent[:3]})
    elif total == 0 and not rows:
        packet["notes"] = ["No decisions need attention."]
    return packet


def _ranked_packet(
    *,
    rows: list[dict[str, Any]],
    total: int,
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "heading": "Ranked Review Queue",
        "mode": "ranked",
        "query": query,
        "interpreted_as": "explain the displayed queue order",
        "answer": (
            "Numbers are handles, not automatic recommendations."
            if rows else "No decisions need attention."
        ),
        "summary_lines": [],
        "sections": [],
        "show_ranked_rows": True,
        "notes": [
            "Use this when you need to inspect ordering; the default inbox stays summarized.",
        ],
        "reply_options": [
            "review <number>",
            "approve <number>",
            "revise <number>: <what should change>",
            "not now <number>",
            "show summary",
        ],
        "warnings": warnings,
    }


def _batch_low_risk_packet(
    *,
    buckets: dict[str, Any],
    stale_rows: list[dict[str, Any]],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    included = buckets["low_risk"]
    excluded = (
        buckets["urgent"]
        + buckets["judgment"]
        + buckets["cleanup"]
        + buckets["other"]
        + stale_rows
    )
    return {
        "heading": "Batch Low-Risk Preview",
        "mode": "batch_low_risk",
        "query": query,
        "interpreted_as": "batch the low-risk completed reviews",
        "answer": (
            f"I can prepare a batch decision for {len(included)} "
            f"{_plural('change', len(included))}."
            if included else "No low-risk completed reviews are ready to batch."
        ),
        "summary_lines": [],
        "sections": [
            {"title": "Included", "rows": included},
            {"title": "Excluded examples", "rows": excluded[:5]},
        ],
        "notes": [
            "Low-risk means docs, guidance, or cleanup-style review with no caveat or risk flags.",
            "I will not record anything until you confirm.",
            "Each approval is recorded separately after confirmation.",
            "Excluded examples are not affected by approve batch.",
        ],
        "reply_options": [
            "approve batch",
            "review <number>",
            "cancel",
        ],
        "warnings": warnings,
    }


def _blockers_packet(
    *,
    buckets: dict[str, Any],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    urgent = buckets["urgent"]
    non_blocking = buckets["judgment"] + buckets["low_risk"]
    return {
        "heading": "Agent Blockers",
        "mode": "blockers",
        "query": query,
        "interpreted_as": "what stops agents from continuing safely",
        "answer": (
            f"{len(urgent)} urgent {_plural('item', len(urgent))} can block agents."
            if urgent else "No review item is currently blocking agents."
        ),
        "summary_lines": [],
        "sections": [
            {"title": "Blocking work", "rows": urgent[:5]},
            {"title": "Not blockers", "rows": non_blocking[:5]},
        ],
        "notes": [
            "Completed reviews are optional capacity unless an agent is explicitly waiting.",
        ],
        "reply_options": [
            "fix blocker",
            "show failure",
            "review judgment",
            "stop",
        ],
        "warnings": warnings,
    }


def _judgment_packet(
    *,
    buckets: dict[str, Any],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    judgment = buckets["judgment"]
    low_risk = buckets["low_risk"]
    hidden = buckets["urgent"] + buckets["cleanup"] + buckets["other"]
    return {
        "heading": "Human Judgment",
        "mode": "judgment",
        "query": query,
        "interpreted_as": "show work where your judgment matters",
        "answer": (
            f"{len(judgment)} {_plural('item', len(judgment))} need judgment "
            "before mechanical cleanup."
            if judgment else "No review item needs product judgment right now."
        ),
        "summary_lines": [],
        "sections": [
            {"title": "Judgment needed", "rows": judgment[:5]},
            {"title": "Low-risk if you want a quick batch", "rows": low_risk[:5]},
            {"title": "Hidden mechanical work", "rows": hidden[:5]},
        ],
        "notes": [
            "I hid stale cleanup from the main answer unless you ask for it.",
        ],
        "reply_options": [
            f"review {_first_visible_number(judgment)}",
            "batch low-risk",
            "show hidden",
            "stop",
        ],
        "warnings": warnings,
    }


def _first_visible_number(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        number = row.get("number")
        if number:
            return str(number)
    return "<number>"


def _risk_plan_packet(
    *,
    buckets: dict[str, Any],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    counts = buckets["counts"]
    actions: list[str] = []
    if counts["urgent"]:
        actions.append("Fix urgent blockers first.")
    if counts["judgment"]:
        actions.append("Review the highest-judgment item next.")
    if counts["low_risk"]:
        actions.append("Batch low-risk completed reviews if time remains.")
    if not actions:
        actions.append("Stop; no review work needs attention.")
    return {
        "heading": "Risk-Based Plan",
        "mode": "risk_plan",
        "query": query,
        "interpreted_as": "risk-based plan for the available time",
        "answer": actions[0],
        "summary_lines": [f"{i + 1}. {action}" for i, action in enumerate(actions)],
        "sections": [
            {"title": "Highest risk", "rows": buckets["urgent"][:3]},
            {"title": "Judgment after blockers", "rows": buckets["judgment"][:3]},
        ],
        "notes": [
            "Skip stale cleanup and raw proof spelunking unless everything above is clear.",
        ],
        "reply_options": [
            "start with blocker",
            "review judgment",
            "batch low-risk",
            "stop",
        ],
        "warnings": warnings,
    }


def _changed_packet(
    *,
    buckets: dict[str, Any],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    counts = buckets["counts"]
    return {
        "heading": "Changed Since Last Look",
        "mode": "changed",
        "query": query,
        "interpreted_as": "current inbox state because no last-look marker exists",
        "answer": (
            "I do not have a saved last-look timestamp, so this is the current "
            "review inbox rather than a true delta."
        ),
        "summary_lines": [
            f"Current decisions: {counts['decision']}",
            f"Urgent blockers: {counts['urgent']}",
            f"Needs human judgment: {counts['judgment']}",
            f"Low-risk completed review: {counts['low_risk']}",
        ],
        "sections": [
            {"title": "Most relevant now", "rows": (
                buckets["urgent"] + buckets["judgment"] + buckets["low_risk"]
            )[:5]},
        ],
        "notes": [
            "A future version can use a per-operator last-reviewed marker.",
        ],
        "reply_options": [
            "review urgent",
            "where does my judgment matter?",
            "batch low-risk",
            "stop",
        ],
        "warnings": warnings,
    }


def _two_minute_packet(
    *,
    buckets: dict[str, Any],
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    if buckets["urgent"]:
        rows = buckets["urgent"][:1]
        answer = "Use the time on the urgent blocker."
    elif buckets["judgment"]:
        rows = buckets["judgment"][:1]
        answer = "Use the time on one judgment decision."
    elif buckets["low_risk"]:
        rows = buckets["low_risk"][:3]
        answer = "Use the time to batch low-risk completed reviews."
    else:
        rows = []
        answer = "Nothing needs your attention in the next two minutes."
    return {
        "heading": "Two-Minute Review",
        "mode": "two_minutes",
        "query": query,
        "interpreted_as": "smallest useful review action",
        "answer": answer,
        "summary_lines": [],
        "sections": [{"title": "Use the time on", "rows": rows}],
        "notes": [
            "Everything else can wait unless you want to clean the backlog.",
        ],
        "reply_options": [
            "review this",
            "approve",
            "revise: <what should change>",
            "stop",
        ],
        "warnings": warnings,
    }


def _stale_packet(
    *,
    stale_rows: list[dict[str, Any]],
    stale_total: int,
    query: str | None,
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "heading": "Stale Cleanup",
        "mode": "stale",
        "query": query,
        "interpreted_as": "show stale or cleanup work",
        "answer": (
            f"{stale_total} stale {_plural('claim', stale_total)} can be cleaned up."
            if stale_total else "No stale claims are visible from this checkout."
        ),
        "summary_lines": [],
        "sections": [{"title": "Stale work", "rows": stale_rows}],
        "notes": [
            "This is operational cleanup, not urgent review work.",
            "Run `uv run okg pact decide --all-stale --verdict defer --json` to remove the expired intent claims.",
        ],
        "reply_options": [
            "uv run okg pact decide --all-stale --verdict defer --json",
            "inspect stale work",
            "stop",
        ],
        "warnings": warnings,
    }


def _dedupe_stale_claims(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stale: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if (row.get("operator_status") or {}).get("queue_state") != "stale_claim":
            continue
        change_id = str(row.get("change") or "")
        key = change_id or json.dumps(row, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        stale.append(row)
    return stale


def _chat_stale_queue_row(root: Path, row: dict[str, Any]) -> dict[str, Any]:
    change_id = row.get("change") or "unknown"
    title = _title_for_change_id(root, change_id)
    divergence = row.get("divergence")
    if divergence == "absent-here":
        why = "Another worktree has an expired claim that is absent here."
    elif divergence == "differs":
        why = "Another worktree has an expired claim that differs from this checkout."
    else:
        why = "Another worktree has an expired claim."
    return {
        "change": change_id,
        "title": title,
        "recommendation": "Check stale work",
        "why": why,
        "bucket": "stale",
        "caveat": "",
    }


def _title_for_change_id(root: Path, change_id: str) -> str:
    try:
        manifest = _load_manifest_for_gate(root, change_id)
        return manifest.change.title
    except SubstrateError:
        return change_id.replace("-", " ").replace("_", " ").strip().capitalize()


def _chat_queue_warning(warning: str) -> str:
    if "ledger unavailable" in warning:
        return "Decision history is unavailable; recorded decisions may be missing."
    return _clean_prose(warning)


def _chat_queue_rank_note(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    priorities = {row.get("priority") for row in rows}
    if len(rows) > 1 and len(priorities) == 1:
        return (
            "Visible rows share the same priority; numbers are handles, not "
            "recommendations."
        )
    return (
        "Rows are ordered by explicit priority; each row says why it is near "
        "the top."
    )


def _chat_queue_row(row: dict[str, Any], *, number: int) -> dict[str, Any]:
    decision = row.get("decision") or "review"
    action = row.get("action") or {
        "repair": "rework",
        "review": "decision",
        "go-ahead": "decision",
        "archive": "archive",
    }.get(decision, "decision")
    labels = {
        "repair": ("Repair first", "This item cannot be reviewed until it loads cleanly."),
        "review": ("Review now", "Completed work is waiting for a human decision."),
        "go-ahead": ("Decide whether to start", "The idea needs approval before work begins."),
        "archive": ("Clean up archive state", "A finished item still appears in the active list."),
    }
    recommendation, why = labels.get(
        decision,
        ("Review", "This item needs human attention."),
    )
    flags = row.get("flags") or []
    caveat = ", ".join(flag.replace("_", " ").replace("-", " ") for flag in flags)
    title = _clean_prose(row.get("title")) or "Untitled work"
    bucket = _chat_queue_bucket(decision, flags, title=title)
    return {
        "number": number,
        "change": row.get("change"),
        "title": title,
        "decision_type": decision,
        "action": action,
        "bucket": bucket,
        "recommendation": recommendation,
        "why": why,
        "priority": row.get("priority"),
        "priority_reason": (
            _clean_prose(row.get("priority_reason"))
            or "Ordered by the review queue priority model."
        ),
        "caveat": caveat,
        "age_days": row.get("age_days"),
    }


def _chat_queue_bucket(decision: str, flags: list[str], *, title: str) -> str:
    if decision == "repair":
        return "urgent"
    if decision == "go-ahead":
        return "judgment"
    if decision == "review":
        if flags:
            return "judgment"
        return "low_risk" if _looks_low_risk_review(title) else "judgment"
    if decision == "archive":
        return "cleanup"
    return "other"


def _looks_low_risk_review(title: str) -> bool:
    normalized = (
        title.lower()
        .replace("/", " ")
        .replace("-", " ")
        .replace("_", " ")
        .replace(".", " ")
    )
    tokens = set(normalized.split())
    low_risk_terms = (
        "docs",
        "documentation",
        "readme",
        "readmes",
        "runbook",
        "runbooks",
        "wording",
        "copy",
        "guidance",
        "instruction",
        "markdown",
        "typo",
        "cleanup",
        "rename",
    )
    high_risk_terms = {
        "auth",
        "ci",
        "credential",
        "credentials",
        "database",
        "ingest",
        "migration",
        "migrate",
        "publish",
        "runtime",
        "schema",
        "secret",
        "security",
        "source",
    }
    return bool(tokens & set(low_risk_terms)) and not bool(tokens & high_risk_terms)


def _chat_review_queue_text(packet: dict[str, Any]) -> str:
    lines = [
        packet["heading"],
        "",
    ]
    if packet.get("query"):
        lines.extend([
            f"I understood your question as: {packet['interpreted_as']}.",
            "",
        ])
    if packet["warnings"]:
        lines.extend(["Note:", *[f"- {w}" for w in packet["warnings"]], ""])
    lines.append(packet["answer"])
    if packet.get("summary_lines"):
        lines.extend(["", "Summary:", *[f"- {line}" for line in packet["summary_lines"]]])
    if packet.get("show_ranked_rows") and packet.get("rows"):
        lines.extend(["", f"Showing {packet['shown']} of {packet['total']}."])
        if packet.get("rank_note"):
            lines.append(packet["rank_note"])
        for row in packet["rows"]:
            lines.append(_format_inbox_row(row, include_priority=True))
    for section in packet.get("sections") or []:
        rows = section.get("rows") or []
        if not rows:
            continue
        lines.extend(["", f"{section['title']}:"])
        for row in rows:
            lines.append(_format_inbox_row(row))
    if packet.get("notes"):
        lines.extend(["", *packet["notes"]])
    lines.extend(["", "Reply with one:", " | ".join(packet["reply_options"])])
    return "\n".join(lines)


def _format_inbox_row(
    row: dict[str, Any], *, include_priority: bool = False,
) -> str:
    prefix = f"{row['number']}." if row.get("number") else "-"
    line = f"{prefix} {row['recommendation']}: {row['title']} - {row['why']}"
    if include_priority and row.get("priority_reason"):
        line += f" Priority: {row['priority_reason']}"
    if row.get("caveat"):
        line += f" Caveat: {row['caveat']}."
    return line


def _suggested_inbox_mode(buckets: dict[str, Any]) -> str:
    counts = buckets["counts"]
    if counts["urgent"]:
        return "review urgent"
    if counts["judgment"]:
        return "review judgment"
    if counts["low_risk"]:
        return "batch low-risk"
    if counts["stale"]:
        return "show stale"
    return "stop"


def _plural(word: str, count: int) -> str:
    return word if count == 1 else f"{word}s"


def _verb(base_plural: str, count: int) -> str:
    if count != 1:
        return base_plural
    if base_plural == "can":
        return "can"
    if base_plural.endswith("y"):
        return f"{base_plural[:-1]}ies"
    return f"{base_plural}s"


def _clean_prose(text: str | None) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def _strip_root_cause_prompt(text: str | None) -> str:
    cleaned = _clean_prose(text)
    prefix = "Why is this broken?"
    if cleaned.startswith(prefix):
        return cleaned[len(prefix):].strip()
    return cleaned


def _first_sentence(text: str | None, *, fallback: str) -> str:
    cleaned = _clean_prose(text)
    if not cleaned:
        return fallback
    for sep in (". ", "? ", "! "):
        if sep in cleaned:
            return cleaned.split(sep, 1)[0].strip() + sep.strip()
    return cleaned


def _human_evidence_kind(kind: str) -> str:
    names = {
        "executed_test": "executed test",
        "operator_report": "operator report",
        "published_generation": "published generation",
        "graph_report": "graph report",
        "manual_visual": "human visual approval",
        "ui_visual": "UI screenshot proof",
        "ui_interaction": "UI interaction proof",
        "ui_quality_gate": "UI quality gate",
    }
    return names.get(kind, kind.replace("_", " "))


def _proof_gap_lines(manifest, status: dict[str, Any]) -> list[str]:
    titles = {
        req.id: (req.title or req.statement.splitlines()[0])
        for req in manifest.requirements
    }
    gaps = status.get("evidence_gaps") or []
    if not gaps:
        return ["No missing proof reported."]
    lines: list[str] = []
    for gap in gaps[:4]:
        title = titles.get(gap.get("id"), "Untitled requirement")
        if gap.get("gap_type") == "scope_drift" and gap.get("action"):
            action = str(gap["action"]).rstrip(".")
            lines.append(f"{title}: {action}.")
            continue
        missing = ", ".join(_human_evidence_kind(k) for k in gap.get("missing", []))
        lines.append(f"{title} needs {missing}.")
    if len(gaps) > 4:
        lines.append(f"{len(gaps) - 4} more proof gap(s) hidden.")
    return lines


def _plain_next_action(status: dict[str, Any]) -> str:
    next_tasks = status.get("next") or []
    if not next_tasks:
        if status.get("evidence_gaps") or status.get("task_evidence_issues"):
            return "The missing proof above must be resolved before this can be accepted."
        derived = status.get("derived_state") or {}
        state = derived.get("state")
        if state in {"awaiting_review", "retired", "rejected", "superseded"} or status.get("retired"):
            brief = status.get("operator_brief") or {}
            proof = brief.get("proof_state")
            if proof:
                return proof
            return "All required proof is satisfied; no missing proof was reported."
        return "No next implementation step is currently unblocked."
    first = next_tasks[0]
    title = first.get("title") or "Untitled task"
    return f"{title}. The linked verification must pass before completion."


def _chat_recommendation(status: dict[str, Any]) -> tuple[str, str, str, str]:
    verdict, reason, against = _brief_recommendation(status)
    derived = status.get("derived_state") or {}
    state = derived.get("state")
    if state in {"retired", "rejected", "superseded"}:
        labels = {
            "retired": "already approved and archived",
            "rejected": "already rejected",
            "superseded": "already superseded",
        }
        return (
            labels.get(state, "already archived"),
            reason,
            against,
            "No decision is needed unless you want to reopen this work.",
        )
    if verdict == "none":
        return (
            "continue implementation",
            "Work is still being implemented.",
            against,
            "Should the agent keep implementing this shape, or should the plan change?",
        )
    if verdict == "approve" and state == "awaiting_review":
        return (
            "approve completed work",
            reason,
            against,
            "Do you accept this completed work?",
        )
    if verdict == "approve":
        return (
            "approve the idea",
            reason,
            against,
            "Do you agree this work should happen?",
        )
    if verdict == "revise":
        return (
            "ask for revision",
            reason,
            against,
            "What needs to change before this is worth continuing?",
        )
    return (
        verdict,
        reason,
        against,
        "What decision should be recorded?",
    )


def _chat_review_packet(manifest, status: dict[str, Any]) -> dict[str, Any]:
    recommendation, reason, against, judgment = _chat_recommendation(status)
    derived = status.get("derived_state") or {}
    no_decision_needed = derived.get("state") in {"retired", "rejected", "superseded"}
    requirement_titles = [
        req.title or _first_sentence(req.statement, fallback="Untitled requirement")
        for req in manifest.requirements[:4]
    ]
    if requirement_titles:
        after = "; ".join(requirement_titles)
        if len(manifest.requirements) > 4:
            after += f"; {len(manifest.requirements) - 4} more change(s)"
    else:
        after = "No concrete change has been described yet."
    return {
        "heading": "No Decision Needed" if no_decision_needed else "Decision Needed",
        "outcome": recommendation,
        "current_state": reason,
        "work": manifest.change.title,
        "recommendation": recommendation,
        "recommendation_reason": reason,
        "why": _first_sentence(
            _strip_root_cause_prompt(manifest.change.root_cause),
            fallback=_first_sentence(
                manifest.change.rationale,
                fallback="No reason has been recorded yet.",
            ),
        ),
        "what_would_change": after,
        "judgment": judgment,
        "concern": against,
        "missing_proof": _proof_gap_lines(manifest, status),
        "how_we_know": _plain_next_action(status),
        "reply_options": [
            "approve",
            "revise: <what should change>",
            "not now",
            "reject",
            "show proof",
            "show risks",
            "show details",
            "show raw YAML",
        ],
    }


def _chat_review_text(packet: dict[str, Any]) -> str:
    lines = [
        packet["heading"],
        "",
        f"Outcome: {packet['outcome']}",
        f"Current state: {packet['current_state']}",
        "",
        f"Work: {packet['work']}",
        "",
        f"My recommendation: {packet['recommendation']}",
        "",
        "Why:",
        packet["why"],
        "",
        "What would change:",
        packet["what_would_change"],
        "",
        "The part I want you to judge:",
        packet["judgment"],
        "",
        "Concern:",
        packet["concern"],
        "",
        "Missing proof:",
    ]
    lines.extend(f"- {line}" for line in packet["missing_proof"])
    lines.extend([
        "",
        "How we will know it worked:",
        packet["how_we_know"],
        "",
        "Reply with one:",
    ])
    lines.extend(packet["reply_options"])
    return "\n".join(lines)


def _derived_state_payload(
    root: Path,
    change_id: str,
    *,
    decision_events: list[Any] | None = None,
    ledger_consulted: bool | None = None,
    manifest: Any | None = None,
    validation_report: Any | None = None,
) -> dict[str, Any]:
    """Best-effort v4 derived state for the brief: ledger decisions are
    folded in when reachable; their absence is reported, never hidden."""
    from okg.substrate.pact import decisions as ledger_mod
    from okg.substrate.pact.review import derive_change_state, infer_tier

    events = list(decision_events or [])
    ledger_ok = bool(ledger_consulted)
    if ledger_consulted is None:
        ledger_ok = False
        try:
            conn = ledger_mod.require_ledger_connection(repo_root=root)
            try:
                # Closure-intent tests and compatibility adapters may expose only
                # the event-write half of the ledger protocol. Such a connection
                # cannot make lifecycle reads authoritative.
                if hasattr(conn, "cursor"):
                    events = ledger_mod.load_decisions(conn, change_id)
                    ledger_ok = True
            finally:
                conn.close()
        except SubstrateError:
            # The review view can still report manifest-derived state when the
            # optional ledger is unavailable.
            pass
    try:
        if manifest is None:
            manifest = _load_manifest_for_gate(root, change_id)
        derived = derive_change_state(
            manifest,
            events,
            repo_root=root,
            validation_report=validation_report,
        )
        tier = infer_tier(manifest, repo_root=root)
        return {
            "state": derived.state,
            "pending_decision": derived.pending_decision,
            "reason": "; ".join(derived.reasons),
            "ledger_consulted": ledger_ok,
            "authoritative": ledger_ok or tier < 2,
            "decision_count": len(events),
        }
    except SubstrateError as err:
        return {
            "state": "draft",
            "pending_decision": "repair",
            "reason": err.message[:120],
            "ledger_consulted": ledger_ok,
            "authoritative": False,
            "decision_count": len(events),
        }


def _list_decision_events(
    root: Path,
) -> tuple[dict[str, list[Any]], bool]:
    """Read the decision ledger once for a repository-wide list operation."""
    from okg.substrate.pact import decisions as ledger_mod

    try:
        conn = ledger_mod.require_ledger_connection(repo_root=root)
        try:
            if not hasattr(conn, "cursor"):
                return {}, False
            return {
                change_id: list(events)
                for change_id, events in ledger_mod.load_all_decisions(conn).items()
            }, True
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 - list must survive adapter/query outages
        return {}, False


# PACT v4 brief budget (req.review.brief-budget): hard caps, decision-first,
# recommendation + strongest-reason-against at the end, loud unavailability.
BRIEF_MAX_LINES = 60
BRIEF_MAX_BYTES = 4096

# DISPLAY filter, not the execution funnel. `repair.is_executable_command` is
# the stricter gate on what an agent may RUN (`okg`/`git` only); this decides
# what the brief and the workbench text render AS a command rather than as
# prose, so it also admits the diagnostics an operator is told to run by hand.
#
# Until 2026-09-03 this tuple existed twice: a second `_looks_like_command`
# further down the module shadowed this one, so the brief silently dropped every
# `git`/`psql` line and lost the `.strip()`. The union of both sets is what each
# call site was written to want; `tests/architecture/test_no_shadowed_definitions.py`
# refuses the shadowing that hid the difference.
_COMMAND_PREFIXES = (
    "okg ",
    "uv ",
    "python ",
    "pytest",
    "git ",
    "psql ",
    "export ",
)


def _looks_like_command(text: str) -> bool:
    # Finding (9) of standardize-pact-review-interface: task verification
    # PROSE was rendered as a runnable command. Only echo real commands.
    return text.strip().startswith(_COMMAND_PREFIXES)


def _brief_recommendation(status: dict[str, Any]) -> tuple[str, str, str]:
    """(verdict, reason, strongest_reason_against) for the brief ending."""
    derived = status.get("derived_state") or {}
    state = derived.get("state")
    graph = status.get("graph_projection") or {}
    ledger_warning = (
        "ledger/graph unreachable from this worktree; a verdict can only "
        "be recorded where the deployment DSN resolves"
        if (graph.get("status") or "unavailable") == "unavailable"
        else None
    )
    brief = status.get("operator_brief", {})
    if state == "awaiting_review":
        against = ledger_warning or (
            "approve archives the change; reversing it needs an operator reopen"
        )
        return ("approve", derived.get("reason") or
                "all tasks terminal, evidence satisfied", against)
    if state == "proposed":
        return ("approve", "valid tier-2+ proposal awaiting go-ahead",
                ledger_warning or "scope is cheapest to narrow now, before work starts")
    if state == "draft":
        return ("revise", "manifest does not validate",
                ledger_warning or "rejecting outright discards authored intent")
    proof = brief.get("proof_state") or ""
    proof_concern = (
        proof[:100]
        if proof and not proof.startswith("No unsatisfied") and "gap" in proof
        else "none identified"
    )
    return ("none", f"no decision pending ({state or 'in progress'})",
            ledger_warning or proof_concern)


def _brief_text(status: dict[str, Any]) -> str:
    brief = status["operator_brief"]
    guidance = status.get("agent_guidance", {})
    graph = status.get("graph_projection") or {}
    graph_state = graph.get("status") or "unavailable"
    graph_detail = graph.get("reason")
    if not graph_detail and graph.get("projected_generation_id") is not None:
        graph_detail = f"generation {graph['projected_generation_id']}"
    derived = status.get("derived_state") or {}
    lines = [
        f"Decision needed: {brief['decision_needed']}.",
        (
            f"Derived state: {derived.get('state', 'unknown')}"
            + (f" ({derived.get('reason')})" if derived.get("reason") else "")
            + f"; blocker count {brief['blocker_count']}."
        ),
        f"Next action: {brief['next_action']}",
        f"Proof state: {brief['proof_state']}",
    ]
    # Loud unavailability is a feature, not a footnote.
    if graph_state == "unavailable":
        lines.append(
            "WARNING: graph/ledger unavailable"
            + (f" ({graph_detail})" if graph_detail else "")
            + " - derived state ignores recorded decisions; decide will refuse here."
        )
    else:
        lines.append(f"Graph projection: {graph_state}"
                     + (f" ({graph_detail})." if graph_detail else "."))
    commands = [
        c for c in (guidance.get("recommended_commands") or [])
        if _looks_like_command(c)
    ]
    if commands:
        lines.extend(["Commands:", *[f"- {c}" for c in commands]])
    verdict, reason, against = _brief_recommendation(status)
    lines.extend([
        f"Recommendation: {verdict} - {reason}.",
        f"Against: {against}.",
    ])
    # Hard budget: drop middle detail before the recommendation, never the
    # first two lines or the last two (decision + recommendation survive).
    dropped = 0
    while len(lines) > BRIEF_MAX_LINES or len("\n".join(lines).encode()) > BRIEF_MAX_BYTES:
        if len(lines) <= 4:
            break
        lines.pop(len(lines) - 3)
        dropped += 1
    if dropped:
        lines.insert(len(lines) - 2, f"... {dropped} detail line(s) trimmed (budget).")
    return "\n".join(lines)


def _final_verdict_event_guard(
    manifest,
    decision_row: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bind terminal approval to exact manifest state and code receipts."""
    from okg.substrate.pact.proof_identity import code_proof_receipt_digests
    from okg.substrate.pact.store import intent_digest as compute_intent_digest

    expected_keys = {
        "command",
        "verdict",
        "from_state",
        "to_state",
        "decision_id",
        "note",
        "target",
        "state_digest",
        "intent_digest",
        "code_proof_receipt_digests",
    }
    matching = [
        event
        for event in events
        if event.get("event_type") == "verdict"
        and (event.get("payload") or {}).get("decision_id")
        == decision_row.get("decision_id")
    ]
    if len(matching) != 1:
        return envelope.component(
            ok=False,
            status="final_verdict_event_missing_or_duplicate",
            decision_id=decision_row.get("decision_id"),
            event_count=len(matching),
        )
    event = matching[0]
    payload = event.get("payload") or {}
    expected_payload = {
        "command": "decide",
        "verdict": decision_row.get("verdict"),
        "from_state": decision_row.get("from_state"),
        "to_state": decision_row.get("to_state"),
        "decision_id": decision_row.get("decision_id"),
        "note": decision_row.get("note"),
        "target": manifest.change.id,
        "state_digest": state_digest(manifest),
        "intent_digest": compute_intent_digest(manifest),
        "code_proof_receipt_digests": code_proof_receipt_digests(manifest),
    }
    mismatches: list[str] = []
    if set(payload) != expected_keys:
        mismatches.append("payload_keys")
    for key, expected in expected_payload.items():
        if payload.get(key) != expected:
            mismatches.append(f"payload.{key}")
    if event.get("change_id") != manifest.change.id:
        mismatches.append("event.change_id")
    if event.get("principal") != decision_row.get("principal"):
        mismatches.append("event.principal")
    if event.get("intent_digest") != decision_row.get("intent_digest"):
        mismatches.append("event.intent_digest")
    return envelope.component(
        ok=not mismatches,
        status="bound" if not mismatches else "final_verdict_event_mismatch",
        decision_id=decision_row.get("decision_id"),
        event_id=event.get("event_id"),
        mismatches=mismatches,
        state_digest=expected_payload["state_digest"],
        intent_digest=expected_payload["intent_digest"],
        code_proof_receipt_digests=expected_payload["code_proof_receipt_digests"],
    )


# What each post-read refusal branch of `_ledger_decision_gate` reports, keyed
# by that branch's own status: (pact_coordination_agree, ledger_approval). Every
# one of these branches READ the ledger, so all of them report
# `ledger_fresh=True`; what differs is which fault the read revealed, and each
# has a different repair (issue #1443).
#
# Data, not lookalike closures, so the mapping is testable directly and a new
# branch status has to be added HERE rather than silently inheriting whichever
# helper happened to be nearest in the file.
_POST_READ_LEDGER_ANSWER: dict[str, tuple[bool, str]] = {
    # An approval binds this digest; only the archive step is missing. Calling
    # this "no approval" would be false, and the FSM-legal verdict here is
    # `defer` — a command that records a defer and leaves the gate refusing
    # identically. The live check names the archive step instead.
    "approval_not_terminal_archive": (True, wa.LEDGER_APPROVAL_NOT_ARCHIVED),
    # The decision rows and the coordination events disagree — a verdict event
    # missing, duplicated, or whose payload does not match the decision row it
    # claims. That IS what `pact_coordination_disagreement` is reserved for. An
    # approval row EXISTS, so the approval answer stays `approved`.
    "final_verdict_receipt_missing_decision": (False, wa.LEDGER_APPROVAL_APPROVED),
    "final_verdict_event_missing_or_duplicate": (False, wa.LEDGER_APPROVAL_APPROVED),
    "final_verdict_event_mismatch": (False, wa.LEDGER_APPROVAL_APPROVED),
    # No approval binds the current digest: superseded by a later decision,
    # stranded by a manifest edit, or never recorded. A NEW decision is the
    # repair in all three, so `okg pact decide` is a genuine command.
    "approval_superseded_by_later_decision": (True, wa.LEDGER_APPROVAL_ABSENT),
    "approval_stale_intent_changed": (True, wa.LEDGER_APPROVAL_ABSENT),
    "no_approval_recorded": (True, wa.LEDGER_APPROVAL_ABSENT),
}

# An unmapped status refuses on BOTH axes rather than raising: a gate that
# crashes helps nobody, and a gate that reports clean authority for a branch
# nobody classified is the failure this table exists to prevent.
_POST_READ_LEDGER_ANSWER_FALLBACK = (False, wa.LEDGER_APPROVAL_ABSENT)


def _ledger_decision_gate(
    root: Path,
    change_id: str,
    *,
    capture: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The v4 merge boundary (design.md section 5): a change reaches the main
    branch only with a recorded ledger decision approving its CURRENT intent
    digest. An unreachable ledger refuses the merge - degraded mode permits
    authoring and implementation, never MERGE.

    ``capture`` collects the ledger history this gate already loaded, so the
    caller's decide-recommendation derives state from the SAME decisions this
    gate judged instead of re-deriving from an empty list."""
    from okg.substrate.pact.decisions import (
        load_coordination_events,
        load_decision_rows,
        load_intents,
        load_decisions,
        require_ledger_connection,
    )
    from okg.substrate.pact.store import intent_digest as compute_intent_digest

    def authority_payload(
        manifest_for_gate,
        *,
        ledger_fresh: bool,
        pact_coordination_agree: bool,
        # REQUIRED, deliberately without a default: every branch of this gate
        # must state what the ledger answered about approval. A permissive
        # default here would let a future branch report clean approval
        # authority by forgetting one keyword.
        ledger_approval: str,
        ledger_no_dsn: bool = False,
        graph_status: dict[str, Any] | None = None,
    ) -> dict[str, object]:
        graph_status = graph_status or _merge_authority_status(
            root, manifest_for_gate, digest
        )
        intent_status = graph_status.get("event_plane_intent") or {}
        event_plane_authority = graph_status.get("event_plane_authority") or {}
        lifecycle_gate = (
            None if _gate_manifest_is_archived(root, change_id) else "verification"
        )
        verification_report = validate_manifest(
            manifest_for_gate,
            lifecycle_gate=lifecycle_gate,  # type: ignore[arg-type]
            repo_root=root,
        )
        file_authority = manifest_file_authority(
            root,
            manifest_for_gate.change.id,
            manifest_for_gate,
        )
        code_claims = tuple(manifest_for_gate.change.affected_code or ())
        git_evidence = git_evidence_for_manifest(root, manifest_for_gate)
        scope = graph_status.get("authority_scope")
        projection_authority = (
            scope
            if scope in {"event_plane_intent", external_authority.AUTHORITY_SCOPE}
            else "default_graph"
        )
        external_arm = projection_authority == external_authority.AUTHORITY_SCOPE
        external_blockers = (
            set(graph_status.get("blockers") or ()) if external_arm else set()
        )
        decision = evaluate_workflow_authority(
            WorkflowAuthorityInput(
                file_fresh=file_authority.get("fresh") is True,
                graph_required=True,
                graph_ready=graph_status.get("ok") is True,
                graph_fresh=graph_status.get("status") == "fresh",
                ledger_fresh=ledger_fresh,
                ledger_no_dsn=ledger_no_dsn,
                ledger_approval=ledger_approval,
                generation_pin_matches=graph_status.get("status") == "fresh",
                evidence_artifacts_fresh=verification_report.ok,
                pact_coordination_agree=pact_coordination_agree,
                projection_authority=projection_authority,
                event_plane_authority_allowed=(
                    event_plane_authority.get("event_plane_authority_allowed") is True
                    if projection_authority == "event_plane_intent"
                    else False
                ),
                intent_event_fresh=(
                    intent_status.get("status") == "fresh"
                    if projection_authority == "event_plane_intent"
                    else True
                ),
                # The external arm answers the same three identity questions
                # from the RECEIPT rather than from the intent event, so each
                # one is read off the receipt verdict's own blocker instead of
                # being passed as an unexamined True.
                external_authority_allowed=(
                    (graph_status.get("provenance") or {}).get("declaration")
                    is not None
                    if external_arm
                    else False
                ),
                external_receipt_bound=(
                    graph_status.get("ok") is True if external_arm else True
                ),
                intent_digest_matches=(
                    intent_status.get("intent_digest") == digest
                    if projection_authority == "event_plane_intent"
                    else (
                        "external_receipt_intent_digest_mismatch"
                        not in external_blockers
                        if external_arm
                        else True
                    )
                ),
                worktree_head_matches=(
                    intent_status.get("head_sha")
                    == intent_status.get("current_head_sha")
                    if projection_authority == "event_plane_intent"
                    else (
                        "external_receipt_head_mismatch" not in external_blockers
                        if external_arm
                        else True
                    )
                ),
                worktree_clean=(
                    git_evidence.live_checked and git_evidence.diff_ref is None
                    if projection_authority == "event_plane_intent"
                    else (
                        "external_receipt_worktree_not_clean" not in external_blockers
                        if external_arm
                        else True
                    )
                ),
                code_claims=code_claims,
                git_evidence=git_evidence,
            )
        )
        return {
            **workflow_authority_payload(decision),
            "file_authority": file_authority,
            "graph_projection": {
                "status": graph_status.get("status"),
                "stale": graph_status.get("stale"),
                "projected_generation_id": graph_status.get("projected_generation_id"),
                "authority_scope": graph_status.get("authority_scope"),
                "file_projection": graph_status.get("file_projection"),
                "event_plane_intent": intent_status,
                "event_plane_authority": event_plane_authority,
            },
            # Reported on EVERY external-arm result, accepted ones included:
            # what the surface attests, where it came from, and what it does
            # not verify. A surface that only explained itself when refusing
            # would be accepted silently, which is the thing issue #1424 asks
            # this path never to be.
            **(
                {
                    "external_authority": envelope.component(
                        ok=graph_status.get("ok") is True,
                        authority_scope=external_authority.AUTHORITY_SCOPE,
                        blockers=sorted(external_blockers),
                        provenance=graph_status.get("provenance"),
                        attested=True,
                        verified_against_graph=False,
                        unverified=list(external_authority.UNVERIFIED_BY_RECEIPT),
                    )
                }
                if external_arm
                else {}
            ),
            "verification_ok": verification_report.ok,
            "code_claims": list(code_claims),
        }

    def post_read_authority(
        manifest_for_gate,
        status: str,
        graph_status_for_gate: dict[str, Any],
    ) -> dict[str, object]:
        """Authority for a branch that READ the ledger and is refusing.

        `ledger_fresh=True` on every one of them: the ledger answered. What
        differs is WHICH fault the answer revealed, and each has a different
        repair — so the answer is looked up per branch status in
        `_POST_READ_LEDGER_ANSWER` rather than assumed. Collapsing all of them
        onto `ledger_fresh=False` plus `pact_coordination_agree=False` named
        two faults that did not exist and published no command (issue #1443).

        None of these accepts: the caller sets `ok=False`, and only the
        `approved` branch may accept.
        """
        agree, approval = _POST_READ_LEDGER_ANSWER.get(
            status, _POST_READ_LEDGER_ANSWER_FALLBACK
        )
        return authority_payload(
            manifest_for_gate,
            ledger_fresh=True,
            pact_coordination_agree=agree,
            ledger_approval=approval,
            graph_status=graph_status_for_gate,
        )

    manifest = _load_manifest_for_gate(root, change_id)
    digest = compute_intent_digest(manifest)
    # Presence of the external declaration is the authority selector, and
    # there is no fallback from it: a manifest that declares the surface is
    # judged by the surface even when the declaration is malformed. Falling
    # back to the sync-root projection on a typo would turn a declared,
    # visible authority into a silent one.
    external_declared = external_authority.change_declares_external_authority(manifest)
    file_graph_status = _merge_authority_status(root, manifest, digest)
    graph_status = file_graph_status
    if capture is not None:
        capture["ledger_consulted"] = False
        capture["decisions"] = []
    try:
        conn = require_ledger_connection(repo_root=root)
    except SubstrateError as exc:
        # No DSN in this shell is a configuration gap, not a coordination
        # disagreement (issue #643, req.pactdx.ledger-hints): the blocker
        # names the fix and `pact_coordination_disagreement` stays
        # reserved for genuine disagreement.
        no_dsn = (exc.data or {}).get("cause") == "no_dsn_configured"
        return envelope.component(
            ok=False,
            status="ledger_unreachable",
            reason=str(exc),
            **({"fix_hint": exc.fix_hint} if exc.fix_hint else {}),
            intent_digest=digest,
            workflow_authority=authority_payload(
                manifest,
                ledger_fresh=False,
                pact_coordination_agree=no_dsn,
                ledger_no_dsn=no_dsn,
                # The ledger was never read, so it testified to nothing about
                # approval. The approval blockers are gated on `ledger_fresh`
                # and so stay off; this value is stated, not defaulted.
                ledger_approval=wa.LEDGER_APPROVAL_NOT_JUDGED,
                graph_status=graph_status,
            ),
        )
    try:
        decisions = load_decisions(conn, change_id)
        decision_rows: list[dict[str, Any]] = []
        coordination_events: list[dict[str, Any]] = []
        if manifest.proof_receipts.code:
            decision_rows = [
                row for row in load_decision_rows(conn)
                if row.get("change_id") == change_id
            ]
            coordination_events = load_coordination_events(conn, change_id)
        intent_status = _event_plane_intent_status(
            change_id,
            digest,
            load_intents(conn),
        )
    finally:
        conn.close()
    if capture is not None:
        capture["ledger_consulted"] = True
        capture["decisions"] = list(decisions)
    event_plane_authority = _event_plane_authority_provenance(root)
    graph_status = (
        file_graph_status
        if external_declared
        else _workflow_graph_authority_status(
            file_graph_status,
            intent_status,
            event_plane_authority,
        )
    )
    review_approvals = [
        d
        for d in decisions
        if d.verdict == "approve" and d.to_state in (None, "retired")
    ]
    matching = [d for d in review_approvals if d.intent_digest == digest]
    latest_decision = decisions[-1] if decisions else None
    if (
        matching
        and latest_decision is not None
        and latest_decision.verdict == "approve"
        and latest_decision.intent_digest == digest
        and latest_decision.to_state in (None, "retired")
    ):
        latest = matching[-1]
        if not _gate_manifest_is_archived(root, change_id):
            return envelope.component(
                ok=False,
                status="approval_not_terminal_archive",
                intent_digest=digest,
                principal=latest.principal,
                decided_at=latest.decided_at,
                workflow_authority=post_read_authority(
                    manifest,
                    "approval_not_terminal_archive",
                    graph_status,
                ),
            )
        if manifest.proof_receipts.code:
            matching_rows = [
                row
                for row in decision_rows
                if row.get("verdict") == "approve"
                and row.get("intent_digest") == digest
                and row.get("to_state") in (None, "retired")
            ]
            if not matching_rows:
                return envelope.component(
                    ok=False,
                    status="final_verdict_receipt_missing_decision",
                    intent_digest=digest,
                    workflow_authority=post_read_authority(
                        manifest,
                        "final_verdict_receipt_missing_decision",
                        graph_status,
                    ),
                )
            final_event_guard = _final_verdict_event_guard(
                manifest,
                matching_rows[-1],
                coordination_events,
            )
            if not final_event_guard["ok"]:
                return envelope.component(
                    ok=False,
                    status=final_event_guard["status"],
                    intent_digest=digest,
                    final_verdict_event=final_event_guard,
                    workflow_authority=post_read_authority(
                        manifest,
                        final_event_guard["status"],
                        graph_status,
                    ),
                )
        authority = authority_payload(
            manifest,
            ledger_fresh=True,
            pact_coordination_agree=True,
            ledger_approval=wa.LEDGER_APPROVAL_APPROVED,
            graph_status=graph_status,
        )
        return envelope.component(
            ok=authority["accepted"] is True,
            status="approved",
            intent_digest=digest,
            principal=latest.principal,
            decided_at=latest.decided_at,
            **(
                {"final_verdict_event": final_event_guard}
                if manifest.proof_receipts.code
                else {}
            ),
            workflow_authority=authority,
        )
    if matching and latest_decision is not None:
        return envelope.component(
            ok=False,
            status="approval_superseded_by_later_decision",
            intent_digest=digest,
            latest_decision=latest_decision.verdict,
            approved_digests=sorted(
                {d.intent_digest for d in review_approvals if d.intent_digest}
            ),
            workflow_authority=post_read_authority(
                manifest,
                "approval_superseded_by_later_decision",
                graph_status,
            ),
        )
    if review_approvals:
        # An approval exists but for a DIFFERENT intent digest: the manifest
        # changed after the decision. This is drift, not approval.
        return envelope.component(
            ok=False,
            status="approval_stale_intent_changed",
            intent_digest=digest,
            approved_digests=sorted(
                {d.intent_digest for d in review_approvals if d.intent_digest}
            ),
            workflow_authority=post_read_authority(
                manifest,
                "approval_stale_intent_changed",
                graph_status,
            ),
        )
    return envelope.component(
        ok=False,
        status="no_approval_recorded",
        intent_digest=digest,
        decisions_seen=[d.verdict for d in decisions],
        workflow_authority=post_read_authority(
            manifest,
            "no_approval_recorded",
            graph_status,
        ),
    )


def _event_plane_intent_status(
    change_id: str,
    digest: str,
    intents: list[dict[str, Any]],
) -> dict[str, Any]:
    from okg.substrate.pact import decisions as ledger

    worktree = ledger.current_worktree_identity()
    if worktree is None:
        return envelope.component(
            ok=False,
            status="unavailable",
            reason="current worktree identity unavailable",
        )
    rows = [
        row for row in intents
        if row.get("change_id") == change_id
    ]
    current_rows = [
        row for row in rows
        if row.get("worktree_suffix") == worktree.get("suffix")
    ]
    if not current_rows:
        return envelope.component(
            ok=False,
            status="missing",
            reason="current worktree has no matching intent event",
            worktree_suffix=worktree.get("suffix"),
            matching_other_worktrees=len(
                [row for row in rows if row.get("intent_digest") == digest]
            ),
        )
    row = current_rows[0]
    digest_matches = row.get("intent_digest") == digest
    head_matches = (
        not worktree.get("head_sha")
        or row.get("head_sha") == worktree.get("head_sha")
    )
    fresh = not row.get("claim_expired") and digest_matches and head_matches
    reason = None
    if row.get("claim_expired"):
        reason = "intent event expired"
    elif not digest_matches:
        reason = "intent event digest does not match current pact.yaml"
    elif not head_matches:
        reason = "intent event head does not match current checkout"
    return envelope.component(
        ok=fresh,
        status="fresh" if fresh else "stale",
        reason=reason,
        change_id=change_id,
        worktree_suffix=row.get("worktree_suffix"),
        branch=row.get("branch"),
        head_sha=row.get("head_sha"),
        current_head_sha=worktree.get("head_sha"),
        intent_digest=row.get("intent_digest"),
        current_intent_digest=digest,
        derived_state=row.get("derived_state"),
        claim_freshness=row.get("claim_freshness"),
        last_event_age_seconds=row.get("last_event_age_seconds"),
        registered_at=row.get("registered_at"),
        last_event_at=row.get("last_event_at"),
    )


def _workflow_graph_authority_status(
    file_graph_status: dict[str, Any],
    intent_status: dict[str, Any],
    event_plane_authority: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event_plane_authority = event_plane_authority or {
        "event_plane_authority_allowed": False,
        "mode": "unknown",
    }
    if file_graph_status.get("status") == "fresh":
        return {
            **file_graph_status,
            "authority_scope": "file_projection",
            "file_projection": {
                "status": file_graph_status.get("status"),
                "stale": file_graph_status.get("stale"),
                "projected_generation_id": file_graph_status.get(
                    "projected_generation_id"
                ),
            },
            "event_plane_intent": intent_status,
            "event_plane_authority": event_plane_authority,
        }
    if intent_status.get("status") in {"fresh", "stale"}:
        event_plane_ok = (
            intent_status.get("ok") is True
            and event_plane_authority.get("event_plane_authority_allowed") is True
        )
        detail = {
            **file_graph_status,
            "status": "fresh" if event_plane_ok else "stale",
            "stale": not event_plane_ok,
            "authority_scope": "event_plane_intent",
            "projected_generation_id": None,
            "file_projection": {
                "status": file_graph_status.get("status"),
                "stale": file_graph_status.get("stale"),
                "projected_generation_id": file_graph_status.get(
                    "projected_generation_id"
                ),
                "reason": file_graph_status.get("reason"),
            },
            "event_plane_intent": intent_status,
            "event_plane_authority": event_plane_authority,
            "recommended_commands": [],
        }
        detail.pop("ok", None)
        return envelope.component(ok=event_plane_ok, **detail)
    return {
        **file_graph_status,
        "authority_scope": "file_projection",
        "file_projection": {
            "status": file_graph_status.get("status"),
            "stale": file_graph_status.get("stale"),
            "projected_generation_id": file_graph_status.get(
                "projected_generation_id"
            ),
            "reason": file_graph_status.get("reason"),
        },
        "event_plane_intent": intent_status,
        "event_plane_authority": event_plane_authority,
    }


def _event_plane_authority_provenance(root: Path) -> dict[str, Any]:
    deployment, unconfigured = _graph_projection_deployment_or_reason(root)
    if deployment is None:
        return {
            "deployment": None,
            "event_plane_authority_allowed": False,
            "mode": "unconfigured",
            "reason": unconfigured,
        }
    try:
        from okg.substrate.deployment_authority import check_publish_provenance
    except Exception as exc:  # noqa: BLE001 - authority must fail closed.
        return {
            "deployment": deployment,
            "event_plane_authority_allowed": False,
            "mode": "unavailable",
            "reason": str(exc),
        }
    dsn_resolution = _graph_projection_dsn_resolution(
        deployment,
        explicit_dsn=os.environ.get("OKG_DSN"),
    )
    try:
        provenance = check_publish_provenance(
            deployment,
            dsn_resolution.get("dsn"),
            repo_start=root,
            file_derived=True,
        )
    except Exception as exc:  # noqa: BLE001 - authority must fail closed.
        return {
            "deployment": deployment,
            "event_plane_authority_allowed": False,
            "mode": "unavailable",
            "reason": str(exc),
        }
    mode = provenance.get("mode")
    return {
        **provenance,
        "deployment": deployment,
        "event_plane_authority_allowed": mode == "foreign_checkout",
        "reason": provenance.get("reason")
        or (
            "event-plane merge authority is reserved for foreign checkouts"
            if mode != "foreign_checkout"
            else None
        ),
    }


def _load_manifest_for_gate(root: Path, change_id: str):
    # An approved change has already moved to pact/archive/; read-only gates
    # must verify that terminal manifest as well as active pact/changes rows.
    return load_manifest_for_read(root, change_id).manifest


def _manifest_path_for_gate(root: Path, change_id: str) -> Path:
    active_manifest = root / "pact" / "changes" / change_id / "pact.yaml"
    if active_manifest.is_file():
        return active_manifest
    archive_manifest = root / "pact" / "archive" / change_id / "pact.yaml"
    if archive_manifest.is_file():
        return archive_manifest
    return active_manifest


def _gate_manifest_is_archived(root: Path, change_id: str) -> bool:
    active_manifest = root / "pact" / "changes" / change_id / "pact.yaml"
    archive_manifest = root / "pact" / "archive" / change_id / "pact.yaml"
    return not active_manifest.is_file() and archive_manifest.is_file()


def _simple_gate(
    root: Path,
    change_id: str,
    gate_name: str,
    *,
    lifecycle_gate: str | None = None,
) -> dict[str, Any]:
    manifest = _load_manifest_for_gate(root, change_id)
    effective_lifecycle_gate = (
        None
        if _gate_manifest_is_archived(root, change_id)
        and lifecycle_gate == "verification"
        else lifecycle_gate
    )
    report = validate_manifest(
        manifest,
        lifecycle_gate=effective_lifecycle_gate,  # type: ignore[arg-type]
        repo_root=root,
    )
    # Fold in the PACT-hygiene lint (task.audit-2026-07.enforce-pact-hygiene,
    # #699) so pre-commit/pre-merge surface the same status/evidence rot as
    # `gate post-edit`, not just post-edit. v1 rules are severity="warning"
    # (visible, non-blocking); only error-severity hygiene would flip report.ok.
    from okg.substrate.pact.operations import _authored_raw_document
    from okg.substrate.pact.validation import pact_hygiene_issues

    hygiene = pact_hygiene_issues(
        manifest, raw_document=_authored_raw_document(root, change_id)
    )
    if hygiene:
        report.issues.extend(hygiene)
        report.ok = report.ok and not any(
            issue.severity == "error" for issue in hygiene
        )
    view = _review_view_status(root, manifest)
    generated_view_guard = _generated_review_view_index_guard(root)
    # The PR-base guard is a merge-boundary concern only: refuse a pre-merge
    # gate when the change's open PR targets the wrong branch (per project.yaml
    # pr_policy.base). Best-effort — never blocks when there is no PR or gh.
    pr_base_guard = (
        _pr_base_guard(root)
        if gate_name == "pre-merge"
        else envelope.component(ok=True, status="not_checked")
    )
    pr_base_ok, pr_base_unknown = _read_verdict(
        pr_base_guard, source="pr_base_guard"
    )
    ok = (
        report.ok
        and view.get("ok", False)
        and generated_view_guard.get("ok", False)
        and pr_base_ok
    )
    recommended_commands: list[str] = []
    if not view.get("ok", False):
        recommended_commands.extend([
            f"okg pact view {change_id} --format=markdown --json",
            f"okg pact view {change_id} --format=status --json",
        ])
    if not generated_view_guard.get("ok", False):
        recommended_commands.extend(
            generated_view_guard.get("recommended_commands", []),
        )
    if not pr_base_ok:
        recommended_commands.extend(
            pr_base_guard.get("recommended_commands", []),
        )
    causes = envelope.tokens(
        (
            issue.code
            for issue in report.issues
            if issue.severity == "error"
        ),
        [] if view.get("ok", False) else [
            f"review_view_{view.get('status') or 'stale'}"
        ],
        [] if generated_view_guard.get("ok", False) else [
            str(generated_view_guard.get("status"))
        ],
        pr_base_unknown or (
            [] if pr_base_ok else [str(pr_base_guard.get("status"))]
        ),
    )
    return envelope.from_ok(
        ok,
        text=f"gate {gate_name} {change_id}: {'passed' if ok else 'refused'}",
        blocked_by=causes,
        next_commands=[] if ok else recommended_commands,
        change=change_id,
        gate=gate_name,
        validation=report.to_dict(),
        review_view=view,
        generated_review_view_guard=generated_view_guard,
        pr_base_guard=pr_base_guard,
        recommended_commands=[] if ok else recommended_commands,
    )


GENERATED_PACT_ARTIFACT_PATHS: tuple[str, ...] = (
    "pact/changes/*/pact.md",
    "pact/archive/*/pact.md",
    "pact/changes/*/pact.yaml.lock",
    "pact/archive/*/pact.yaml.lock",
)


def _generated_review_view_index_guard(root: Path) -> dict[str, Any]:
    """Refuse tracked generated PACT review views and lock files.

    `pact.md` is an ignored, render-on-demand view of canonical `pact.yaml`;
    `pact.yaml.lock` is a local writer lock. `.gitignore` blocks normal adds
    but cannot block `git add -f`, so the gate checks the index explicitly.
    """

    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "ls-files",
                "--",
                *GENERATED_PACT_ARTIFACT_PATHS,
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except Exception:  # noqa: BLE001 - non-git test roots have no index.
        return envelope.component(ok=True, status="not_checked")
    if result.returncode != 0:
        return envelope.component(ok=True, status="not_git_repo")
    paths = [line for line in result.stdout.splitlines() if line.strip()]
    if not paths:
        return envelope.component(
            ok=True, status="clean", tracked_paths=[]
        )
    commands = [f"git rm --cached {shlex.quote(path)}" for path in paths]
    return envelope.component(
        ok=False,
        status="tracked_generated_pact_artifact",
        tracked_paths=paths,
        message=(
            "Generated PACT review views and lock files are local artifacts "
            "and must not be tracked"
        ),
        recommended_commands=commands,
    )


def evaluate_pr_base(base_ref: str | None, expected: str) -> dict[str, Any] | None:
    """Pure decision: a blocker dict when an open PR's base is wrong, else None.

    Returns None when there is no base to judge (base_ref is None) or it already
    matches — the guard is fail-open on "nothing to judge", fail-closed only on a
    real wrong-base PR.
    """
    if base_ref is None or base_ref == expected:
        return None
    code = "base_not_dev" if expected == "dev" else "pr_base_mismatch"
    return {"code": code, "base": base_ref, "expected": expected}


def _pr_base_guard(root: Path) -> dict[str, Any]:
    """Refuse a pre-merge gate when the change's open PR targets the wrong base.

    Best-effort and non-blocking when it cannot judge: no `gh`, not a git repo,
    no branch, or no open PR for the branch all return ok=True. It blocks only on
    a real open PR whose base != project.yaml `pr_policy.base`.
    """
    expected = pr_base_policy(root)
    try:
        branch = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
    except Exception:  # noqa: BLE001 - non-git test roots.
        return envelope.component(
            ok=True, status="not_git_repo", expected_base=expected
        )
    branch_name = branch.stdout.strip()
    if branch.returncode != 0 or not branch_name or branch_name == "HEAD":
        return envelope.component(
            ok=True, status="no_branch", expected_base=expected
        )
    try:
        pr = subprocess.run(
            ["gh", "pr", "list", "--head", branch_name, "--state", "open",
             "--json", "number,baseRefName", "--limit", "1"],
            cwd=str(root), capture_output=True, text=True, timeout=20, check=False,
        )
    except FileNotFoundError:
        return envelope.component(
            ok=True, status="gh_unavailable", expected_base=expected
        )
    except Exception:  # noqa: BLE001 - gh present but unrunnable; do not mask other gate findings.
        return envelope.component(
            ok=True, status="gh_error", expected_base=expected
        )
    if pr.returncode != 0:
        return envelope.component(
            ok=True, status="gh_error", expected_base=expected
        )
    try:
        rows = json.loads(pr.stdout or "[]")
    except json.JSONDecodeError:
        return envelope.component(
            ok=True, status="parse_error", expected_base=expected
        )
    if not rows:
        return envelope.component(
            ok=True, status="no_pr", branch=branch_name,
            expected_base=expected,
        )
    base = rows[0].get("baseRefName")
    number = rows[0].get("number")
    blocker = evaluate_pr_base(base, expected)
    if blocker is None:
        return envelope.component(
            ok=True, status="base_ok", base=base,
            expected_base=expected, pr=number,
        )
    return envelope.component(
        ok=False,
        status=blocker["code"],
        base=base,
        expected_base=expected,
        pr=number,
        message=(
            f"PR #{number} targets `{base}`, not `{expected}` — repoint it "
            f"before merge (`gh pr edit {number} --base {expected}`)"
        ),
        recommended_commands=[f"gh pr edit {number} --base {expected}"],
    )


def _graph_freshness_gate(root: Path, change_id: str) -> dict[str, Any]:
    status = _status(root, change_id, include_graph=True)
    graph = status.get("graph_projection") or {}
    fresh = graph.get("status") == "fresh"
    return envelope.from_ok(
        fresh,
        text=f"gate graph-freshness {change_id}: {graph.get('status')}",
        blocked_by=[] if fresh else [
            f"graph_projection_{graph.get('status') or 'unavailable'}"
        ],
        next_commands=graph.get("recommended_commands", []),
        change=change_id,
        gate="graph-freshness",
        graph_projection=graph,
        recommended_commands=graph.get("recommended_commands", []),
    )


def _pre_implementation_gate(root: Path, change_id: str) -> dict[str, Any]:
    """Fold `advance` (proposal -> implementation).

    Checks the intent-review lifecycle gate; on pass, advances
    `change.status` to implementation; on fail, returns a structured refusal
    (no raise) naming the missing approval. This is the meaningful gated
    lifecycle transition — the intent-review checkpoint that `advance`
    enforced — now reachable under `gate`.
    """
    manifest = load_manifest(root, change_id)
    target_version = manifest.target_pact_version or manifest.pact_version
    if target_version >= 4:
        derived = _derived_state_payload(root, change_id)
        state = str(derived.get("state") or "draft")
        if state == "proposed":
            command = (
                f"{_repo_pact_prefix(root)} pact decide {shlex.quote(change_id)} "
                "--verdict approve --json"
            )
            return envelope.refused(
                text=f"gate pre-implementation {change_id}: not advanced",
                message="the v4 proposal decision is still pending",
                blocked_by=["proposal_decision_required"],
                next_commands=[command],
                change=change_id,
                gate="pre-implementation",
                status=manifest.change.status,
                state=state,
                advanced=False,
                authored_status_unchanged=True,
                derived_state=derived,
                next_action={"command": command, "owner": "operator", "why": "approve the proposal"},
            )
        return envelope.recorded(
            text=f"gate pre-implementation {change_id}: already {state}",
            change=change_id,
            gate="pre-implementation",
            status=manifest.change.status,
            state=state,
            advanced=False,
            authored_status_unchanged=True,
            derived_state=derived,
            next_commands=[
                f"{_repo_pact_prefix(root)} pact view {shlex.quote(change_id)} "
                "--format=bootstrap --json"
            ],
            next_action={
                "command": (
                    f"{_repo_pact_prefix(root)} pact view {shlex.quote(change_id)} "
                    "--format=bootstrap --json"
                ),
                "owner": "agent",
                "why": "continue from the derived lifecycle state",
            },
        )
    report = validate_manifest(
        manifest,
        lifecycle_gate="implementation",
        repo_root=root,
    )
    if not report.ok:
        commands = [
            f"okg pact evidence approve {change_id} "
            "--reviewer <id> --decision approved",
        ]
        return envelope.refused(
            text=f"gate pre-implementation {change_id}: not advanced",
            message="the intent-review lifecycle gate does not pass",
            blocked_by=envelope.tokens(
                issue.code
                for issue in report.issues
                if issue.severity == "error"
            ),
            next_commands=commands,
            change=change_id,
            gate="pre-implementation",
            status=manifest.change.status,
            advanced=False,
            validation=report.to_dict(),
            recommended_commands=commands,
        )
    advanced = advance_change(root, change_id, status="implementation")
    return envelope.recorded(
        text=f"gate pre-implementation {change_id}: advanced to implementation",
        change=change_id,
        gate="pre-implementation",
        status=advanced.change.status,
        advanced=True,
    )


def _sync_pact_projection(
    root: Path,
    change_id: str,
    *,
    deployment: str | None,
    dry_run: bool,
) -> dict[str, Any]:
    manifest = _load_manifest_for_gate(root, change_id)
    if not deployment:
        deployment, unconfigured = _graph_projection_deployment_or_reason(root)
        if deployment is None:
            from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

            raise SubstrateError(
                str(unconfigured),
                data={"cause": "deployment_not_configured", "deployment": None},
                fix_hint=f"pass --deployment <name>, or {PACT_GRAPH_DEPLOYMENT_FIX}",
            )
    manifest_path = _manifest_path_for_gate(root, change_id)
    rel_manifest_path = manifest_path.relative_to(root).as_posix()
    rel_review_path = manifest_path.with_name("pact.md").relative_to(root).as_posix()
    scope_keys = [
        rel_manifest_path,
        rel_review_path,
    ]
    dsn_resolution = _graph_projection_dsn_resolution(
        deployment,
        explicit_dsn=os.environ.get("OKG_DSN"),
    )
    from okg.substrate.deployment_authority import check_publish_provenance

    provenance = check_publish_provenance(
        deployment,
        dsn_resolution.get("dsn"),
        repo_start=root,
        file_derived=True,
    )
    ingest_cmd = _graph_projection_ingest_command(deployment)
    dry_run_checkout_unknown = (
        dry_run and provenance.get("mode") == "checkout_unknown"
    )
    allowed = provenance.get("allowed") is True or dry_run_checkout_unknown
    commands = (
        [
            f"uv run okg deployment env {deployment} --json",
            " ".join(ingest_cmd),
            f"uv run okg pact view {change_id} --format=status --json",
        ]
        if allowed
        else _graph_projection_foreign_checkout_commands(change_id, provenance)
    )
    plan = {
        "change": manifest.change.id,
        "deployment": deployment,
        "dry_run": dry_run,
        "source": "pact",
        "provenance": provenance,
        "provenance_warning": (
            "checkout provenance unknown; dry-run did not execute ingest"
            if dry_run_checkout_unknown
            else None
        ),
        "sync_scope_keys": scope_keys,
        "watch_roots": ["pact"],
        "triggers": ["fs_event", "poll", "manual", "reconcile"],
        "commands": commands,
        "recommended_commands": commands,
    }
    if not allowed:
        reason = (
            provenance.get("reason")
            or "file-derived graph projection is not allowed from this checkout"
        )
        return envelope.refused(
            text=f"graph-freshness --refresh {change_id}: refused",
            message=reason,
            blocked_by=[f"publish_provenance_{provenance.get('mode')}"],
            next_commands=commands,
            **plan,
            status="refused",
            reason=reason,
            ingest={
                "skipped": True,
                "reason": provenance.get("mode"),
                "command": " ".join(ingest_cmd),
            },
        )
    if dry_run:
        return envelope.recorded(
            text=f"graph-freshness --refresh {change_id}: dry run plan",
            next_commands=commands,
            **plan,
        )
    from okg.substrate.deployment_authority import (
        deployment_authority_subprocess_env,
    )

    env = deployment_authority_subprocess_env(deployment)
    completed = subprocess.run(  # noqa: S603 - repo-local operator command
        ingest_cmd,
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )
    status = _status(root, change_id, include_graph=True)
    projection = status.get("graph_projection") or {}
    synced = (
        completed.returncode == 0
        and projection.get("status") == "fresh"
    )
    return envelope.from_ok(
        synced,
        text=f"graph-freshness --refresh {change_id}: {projection.get('status')}",
        blocked_by=[] if synced else [
            f"graph_projection_{projection.get('status') or 'unavailable'}"
        ],
        next_commands=commands,
        **plan,
        ingest={
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "command": " ".join(ingest_cmd),
        },
        graph_projection=projection,
    )


def _review_packet(
    root: Path,
    change_id: str,
    *,
    reviewer_kind: str,
) -> dict[str, Any]:
    status = _status(root, change_id, include_graph=True)
    manifest = _load_manifest_for_gate(root, change_id)
    report = validate_manifest(manifest, repo_root=root)
    reviewer_quality = _reviewer_quality_fields(manifest, status, report)
    if reviewer_kind == "human":
        return envelope.recorded(
            text=f"review packet {change_id} (human)",
            **{
            "change": change_id,
            "reviewer_kind": "human",
            "operator_brief": status["operator_brief"],
            "brief_text": _brief_text(status),
            "decision_checklist": reviewer_quality["decision_checklist"],
            "priority_risks": reviewer_quality["priority_risks"],
            "semantic_diff": reviewer_quality["semantic_diff"],
            "contradictions": reviewer_quality["contradictions"],
            "risks": manifest.prose.get("risks"),
            "review_questions": manifest.prose.get("review_questions")
            or manifest.prose.get("open_questions"),
            "human_review": status["human_review"],
            "review_view": status["review_view"],
            },
        )
    return envelope.recorded(
        text=f"review packet {change_id} (ai)",
        **{
        "change": change_id,
        "reviewer_kind": "ai",
        "structured": True,
        "versions": status.get("versions", {}),
        "lifecycle": status.get("lifecycle", {}),
        "requirements": [
            req.model_dump(mode="json", by_alias=True, exclude_none=True)
            for req in manifest.requirements
        ],
        "tasks": [
            task.model_dump(mode="json", by_alias=True, exclude_none=True)
            for task in manifest.tasks
        ],
        "evidence": [
            evidence.model_dump(mode="json", by_alias=True, exclude_none=True)
            for evidence in manifest.evidence
        ],
        "proof_gaps": status.get("evidence_gaps", []),
        "task_evidence_issues": status.get("task_evidence_issues", []),
        "acceptance": report.alignment.get("acceptance", {}),
        "semantic_diff": reviewer_quality["semantic_diff"],
        "priority_risks": reviewer_quality["priority_risks"],
        "decision_checklist": reviewer_quality["decision_checklist"],
        "contradictions": reviewer_quality["contradictions"],
        "warnings": reviewer_quality["warnings"],
        "hierarchy": _hierarchy_rollup(root, manifest),
        "graph_projection": status.get("graph_projection"),
        "review_view": status.get("review_view"),
        "human_review": status.get("human_review"),
        "touched_files": manifest.change.affected_code,
        "next": status.get("next", []),
        },
    )


def _reviewer_quality_fields(
    manifest,
    status: dict[str, Any],
    report,
) -> dict[str, Any]:
    latest = manifest.edits[-1] if manifest.edits else None
    warnings = [
        issue.to_dict()
        for issue in report.issues
        if issue.severity == "warning"
    ]
    errors = [
        issue.to_dict()
        for issue in report.issues
        if issue.severity == "error"
    ]
    graph = status.get("graph_projection") or {}
    contradictions = []
    if errors:
        contradictions.append({
            "kind": "validation_errors",
            "count": len(errors),
            "first": errors[0],
        })
    view = status.get("review_view") or {}
    if not view.get("ok"):
        contradictions.append({
            "kind": "stale_review_view",
            "status": view.get("status"),
            "issues": view.get("issues", []),
        })
    if graph.get("status") in {"stale", "missing", "unavailable"}:
        contradictions.append({
            "kind": "graph_projection_not_fresh",
            "status": graph.get("status"),
            "next_action": graph.get("next_action"),
        })
    proof_gaps = status.get("evidence_gaps", [])
    if proof_gaps:
        contradictions.append({
            "kind": "proof_gaps",
            "count": len(proof_gaps),
            "items": proof_gaps[:5],
        })
    priority_risks = []
    if manifest.prose.get("risks"):
        priority_risks.append({
            "kind": "declared_risks",
            "summary": manifest.prose["risks"],
        })
    priority_risks.extend(
        {
            "kind": warning["code"],
            "summary": warning["message"],
            "path": warning["path"],
        }
        for warning in warnings[:5]
    )
    decision_checklist = [
        envelope.component(
            ok=bool(status.get("lifecycle", {}).get("implementation")),
            item="intent_review",
        ),
        envelope.component(ok=not proof_gaps, item="executed_evidence"),
        envelope.component(
            ok=not status.get("blocked_tasks") and not status.get("next"),
            item="tasks_complete",
        ),
        envelope.component(
            ok=bool(view.get("ok")), item="review_view_fresh"
        ),
        envelope.component(
            ok=graph.get("status") == "fresh",
            item="graph_projection_fresh",
        ),
    ]
    return {
        "semantic_diff": {
            "changed": bool(latest and latest.before_digest != latest.after_digest),
            "command": latest.command if latest else None,
            "before_digest": latest.before_digest if latest else None,
            "after_digest": latest.after_digest if latest else None,
            "affected_ids": latest.affected_ids if latest else [],
        },
        "priority_risks": priority_risks,
        "decision_checklist": decision_checklist,
        "contradictions": contradictions,
        "warnings": warnings,
    }


def _hierarchy_rollup(
    root: Path,
    manifest,
    *,
    manifest_inventory: ManifestInventory | None = None,
    validation_reports: dict[str, Any] | None = None,
    captured_manifests: Mapping[str, Any] | None = None,
    captured_failures: Mapping[str, Exception] | None = None,
    context: PactEvaluationContext | None = None,
) -> dict[str, Any]:
    hierarchy = manifest.hierarchy
    if hierarchy is None:
        return {
            "role": "standalone",
            "parent": None,
            "children": [],
            "child_rollup": [],
        }
    child_rollup: list[dict[str, Any]] = []
    inventory_by_change = (
        manifest_inventory.by_change if manifest_inventory is not None else {}
    )
    for child_id in hierarchy.children:
        try:
            entry = inventory_by_change.get(child_id)
            if entry is not None:
                if entry.manifest is None:
                    if entry.error is not None:
                        raise entry.error
                    raise SubstrateError(f"PACT manifest is unreadable: {child_id}")
                child = entry.manifest
                report = (validation_reports or {}).get(child_id)
                child_alignment = (
                    report.alignment
                    if report is not None
                    else derive_alignment(
                        child,
                        repo_root=root,
                        context=context,
                    )
                )
            elif manifest_inventory is not None:
                failure = (captured_failures or {}).get(child_id)
                if failure is not None:
                    raise failure
                child = (captured_manifests or {}).get(child_id)
                if child is None:
                    raise SubstrateError(
                        f"PACT manifest is unreadable: {child_id}"
                    )
                report = (validation_reports or {}).get(child_id)
                child_alignment = (
                    report.alignment
                    if report is not None
                    else derive_alignment(
                        child,
                        repo_root=root,
                        context=context,
                    )
                )
            else:
                # Preserve the direct status/archive behavior for children that
                # are outside the active list snapshot.
                child = load_manifest_for_read(root, child_id).manifest
                child_alignment = derive_alignment(child, repo_root=root)
            child_rollup.append({
                "change": child_id,
                "status": child.change.status,
                "requirements": len(child.requirements),
                "tasks": len(child.tasks),
                "verification_gate": child_alignment.get("lifecycle", {}).get(
                    "verification",
                ),
                "proof_gaps": [
                    {"id": req_id, "missing": state.get("satisfaction_missing", [])}
                    for req_id, state in child_alignment.get(
                        "requirements",
                        {},
                    ).items()
                    if state.get("satisfaction_status") == "unsatisfied"
                ],
                "blocked_tasks": [
                    task_id
                    for task_id, state in child_alignment.get("tasks", {}).items()
                    if state.get("blockers")
                ],
                "proof_owner": "child",
            })
        except Exception as exc:  # noqa: BLE001 - rollup should identify missing child
            child_rollup.append({
                "change": child_id,
                "status": "missing",
                "error": str(exc),
                "proof_owner": "child",
            })
    return {
        "role": hierarchy.role,
        "parent": hierarchy.parent,
        "children": list(hierarchy.children),
        "integration_acceptance": list(hierarchy.integration_acceptance),
        "child_rollup": child_rollup,
        "proof_ownership": (
            "child requirements, tasks, and evidence remain authoritative; "
            "parent carries coordination and integration acceptance only"
        ),
    }


def _inventory_evaluation_context(
    root: Path,
    inventory: ManifestInventory,
) -> PactEvaluationContext:
    """Capture only manifests needed by active-list semantic validation.

    This authority is deliberately separate from ``PactEvaluationContext.files``:
    evidence, test, registry, and configuration lookups retain their live behavior.
    Active paths come from the fresh inventory. Referenced archive candidates are
    captured once so child lookup never mixes snapshots mid-validation.
    """
    manifest_files: dict[str, bytes | None] = {
        entry.relative_path: entry.payload for entry in inventory
    }
    referenced_ids: set[str] = set()
    for entry in inventory.valid_entries:
        assert entry.manifest is not None
        hierarchy = entry.manifest.hierarchy
        if hierarchy is None:
            continue
        referenced_ids.update(hierarchy.children)
        if hierarchy.parent:
            referenced_ids.add(hierarchy.parent)

    for change_id in sorted(referenced_ids):
        active_ref = f"pact/changes/{change_id}/pact.yaml"
        manifest_files.setdefault(active_ref, None)
        if manifest_files[active_ref] is not None:
            continue
        archive_ref = f"pact/archive/{change_id}/pact.yaml"
        archive_path = root / archive_ref
        try:
            manifest_files[archive_ref] = read_manifest_payload(
                root,
                change_id,
                archive_path,
                collection="archive",
            )
        except OSError:
            manifest_files[archive_ref] = None
    return PactEvaluationContext(manifest_files=manifest_files)


def _captured_hierarchy_manifests(
    root: Path,
    inventory: ManifestInventory,
    context: PactEvaluationContext,
) -> tuple[dict[str, Any], dict[str, Exception]]:
    """Parse referenced archived children once without reopening live paths."""
    manifests: dict[str, Any] = {
        entry.change_id: entry.manifest
        for entry in inventory.valid_entries
        if entry.manifest is not None
    }
    failures: dict[str, Exception] = {}
    active_ids = set(inventory.by_change)
    referenced_ids = {
        child_id
        for entry in inventory.valid_entries
        if entry.manifest is not None and entry.manifest.hierarchy is not None
        for child_id in entry.manifest.hierarchy.children
    }
    for child_id in sorted(referenced_ids - active_ids):
        for relative in (
            f"pact/changes/{child_id}/pact.yaml",
            f"pact/archive/{child_id}/pact.yaml",
        ):
            lookup = context.manifest_file_lookup(root, relative)
            if lookup.content is None:
                continue
            try:
                manifests[child_id] = load_manifest_bytes(
                    lookup.content,
                    source_path=root / relative,
                )
            except Exception as exc:  # noqa: BLE001 - preserve row isolation
                failures[child_id] = exc
            break
        else:
            failures[child_id] = SubstrateError(
                f"PACT manifest is unreadable: {child_id}"
            )
    return manifests, failures


def _list_changes(
    root: Path,
    *,
    include_graph: bool,
    status_filter: str = "open",
    manifest_inventory: ManifestInventory | None = None,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    decisions_by_change, ledger_consulted = _list_decision_events(root)
    inventory = (
        manifest_inventory
        if manifest_inventory is not None
        else capture_manifest_inventory(root)
    )
    context = _inventory_evaluation_context(root, inventory)
    captured_manifests, captured_failures = _captured_hierarchy_manifests(
        root,
        inventory,
        context,
    )
    validation_reports: dict[str, Any] = {}
    validation_failures: dict[str, Exception] = {}
    for entry in inventory.valid_entries:
        assert entry.manifest is not None
        try:
            validation_reports[entry.change_id] = validate_manifest(
                entry.manifest,
                repo_root=root,
                context=context,
            )
        except Exception as exc:  # noqa: BLE001 - one row remains isolated
            validation_failures[entry.change_id] = exc
    for child_id, child_manifest in captured_manifests.items():
        if child_id in validation_reports or child_id in inventory.by_change:
            continue
        try:
            validation_reports[child_id] = validate_manifest(
                child_manifest,
                repo_root=root,
                context=context,
            )
        except Exception as exc:  # noqa: BLE001 - rollup remains isolated
            captured_failures[child_id] = exc

    for entry in inventory:
        change_id = entry.change_id
        try:
            if entry.error is not None:
                raise entry.error
            if change_id in validation_failures:
                raise validation_failures[change_id]
            status = _status(
                root,
                change_id,
                include_graph=include_graph,
                decision_events=decisions_by_change.get(change_id, []),
                ledger_consulted=ledger_consulted,
                manifest_entry=entry,
                validation_report=validation_reports.get(change_id),
                manifest_inventory=inventory,
                inventory_validation_reports=validation_reports,
                captured_hierarchy_manifests=captured_manifests,
                captured_hierarchy_failures=captured_failures,
                evaluation_context=context,
            )
        except Exception as exc:  # noqa: BLE001 - list must not abort on one bad PACT
            rows.append(envelope.component(
                ok=False,
                **{
                "change": change_id,
                "state": "unreadable",
                "status": None,
                "next_action": "fix_manifest",
                "review_view_status": None,
                "error_codes": [type(exc).__name__],
                "warning_codes": [],
                "error_count": 1,
                "warning_count": 0,
                "blocker_count": 0,
                "environment_blocker_count": 0,
                "evidence_gap_count": 0,
                "next_count": 0,
                "retired": False,
                "operator_status": _invalid_list_operator_status(
                    change_id=change_id,
                    message=str(exc),
                ),
                "message": str(exc),
                },
            ))
            continue
        issues = status.get("errors", [])
        errors = [issue for issue in issues if issue.get("severity") == "error"]
        warnings = [
            issue for issue in issues if issue.get("severity") == "warning"
        ]
        guidance = status.get("agent_guidance", {})
        row = envelope.component(
            ok=status.get("ok", False),
            **{
            "change": status["change"],
            "status": status.get("status"),
            "state": (status.get("derived_state") or {}).get("state"),
            "workflow_state": guidance.get("state"),
            "next_action": guidance.get("next_action"),
            "review_view_status": status.get("review_view", {}).get("status"),
            "error_codes": [issue.get("code") for issue in errors],
            "warning_codes": [issue.get("code") for issue in warnings],
            "error_count": len(errors),
            "warning_count": len(warnings),
            "blocker_count": len(status.get("blocked_tasks", [])),
            "environment_blocker_count": len(status.get("environment_blockers", [])),
            "evidence_gap_count": len(status.get("evidence_gaps", [])),
            "next_count": len(status.get("next", [])),
            "requirements": status.get("requirements"),
            "tasks": status.get("tasks"),
            "retired": status.get("retired", False),
            "operator_status": _local_operator_status(status, surface="list"),
            },
        )
        if include_graph:
            row["graph_projection"] = status.get("graph_projection")
        rows.append(row)
    # `--status` filters the returned rows; the summary always reports the full
    # picture (open / retired / total) so a filtered view still shows the
    # backlog size. Per `retire-completed-pacts`.
    if status_filter == "open":
        shown = [row for row in rows if not row.get("retired")]
    elif status_filter == "retired":
        shown = [row for row in rows if row.get("retired")]
    else:  # "all"
        shown = rows
    return envelope.recorded(
        text=f"pact list ({status_filter}): {len(shown)} shown",
        status_filter=status_filter,
        changes=shown,
        summary={
            "total": len(rows),
            "open": sum(1 for row in rows if not row.get("retired")),
            "retired": sum(1 for row in rows if row.get("retired")),
            "shown": len(shown),
            "invalid": sum(1 for row in rows if not row.get("ok")),
            "stale_views": sum(
                1 for row in rows if row.get("review_view_status") == "stale"
            ),
            "blocked": sum(1 for row in rows if row.get("blocker_count", 0) > 0),
            "ready": sum(1 for row in rows if row.get("state") == "ready"),
            "done": sum(1 for row in rows if row.get("state") == "done"),
        },
    )


def _workbench(
    root: Path,
    change_id: str | None,
    *,
    related: list[str],
    include_graph: bool,
) -> dict[str, Any]:
    listing = _list_changes(root, include_graph=include_graph)
    listing_ok, listing_unknown = _read_verdict(listing, source="pact_list")
    rows = listing["changes"]
    primary_status = (
        _status(root, change_id, include_graph=True)
        if change_id
        else _select_workbench_primary(root, rows)
    )
    primary_id = primary_status["change"] if primary_status else None
    selected_rows = _workbench_rows(rows, primary_id=primary_id, related=related)
    classifications = _classify_workbench_rows(selected_rows, primary_id=primary_id)
    classification_counts = {
        name: len(values) for name, values in classifications.items()
    }
    stale_views = [
        {
            "change": row["change"],
            "review_view_status": row.get("review_view_status"),
            "next_action": row.get("next_action"),
        }
        for row in selected_rows
        if row.get("review_view_status") in {"missing", "stale"}
    ]
    if primary_status:
        graph = primary_status.get("graph_projection") or {}
        primary_ok, primary_unknown = _read_verdict(
            primary_status, source="pact_status"
        )
        payload = envelope.from_ok(
            listing_ok and primary_ok,
            text=f"workbench: {primary_status['change']}",
            blocked_by=envelope.tokens(
                listing_unknown,
                listing.get("blocked_by") or [],
                primary_unknown,
                primary_status.get("blocked_by") or [],
            ),
            **{
            "program": {
                **listing["summary"],
                "selected": len(selected_rows),
                "classification_counts": classification_counts,
            },
            "primary": _workbench_primary_summary(primary_status),
            "next_action": _workbench_next_action(primary_status),
            "blockers": {
                "tasks": primary_status.get("blocked_tasks", []),
                "environment": primary_status.get("environment_blockers", []),
            },
            "proof_gaps": primary_status.get("evidence_gaps", []),
            "stale_views": stale_views,
            "graph_freshness": graph,
            "classifications": classifications,
            "related_changes": selected_rows,
            "recommended_commands": _workbench_recommended_commands(
                primary_status,
                stale_views=stale_views,
            ),
            },
        )
    else:
        payload = envelope.from_ok(
            listing_ok,
            text="workbench: no primary PACT selected",
            blocked_by=[*listing_unknown, *(listing.get("blocked_by") or [])],
            **{
            "program": {
                **listing["summary"],
                "selected": len(selected_rows),
                "classification_counts": classification_counts,
            },
            "primary": None,
            "next_action": None,
            "blockers": {"tasks": [], "environment": []},
            "proof_gaps": [],
            "stale_views": stale_views,
            "graph_freshness": None,
            "classifications": classifications,
            "related_changes": selected_rows,
            "recommended_commands": ["okg pact view --format=list --json"],
            },
        )
    payload["text"] = _workbench_text(payload)
    return payload


def _select_workbench_primary(
    root: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    preferred_states = (
        "ready",
        "needs_evidence",
        "blocked",
        "refresh_review",
        "advance_lifecycle",
        "invalid",
    )
    for state in preferred_states:
        for row in rows:
            if row.get("state") == state:
                return _status(root, row["change"], include_graph=True)
    for row in rows:
        if row.get("ok"):
            return _status(root, row["change"], include_graph=True)
    return None


def _workbench_rows(
    rows: list[dict[str, Any]],
    *,
    primary_id: str | None,
    related: list[str],
) -> list[dict[str, Any]]:
    if not related:
        return rows
    wanted = set(related)
    if primary_id:
        wanted.add(primary_id)
    return [row for row in rows if row["change"] in wanted]


def _classify_workbench_rows(
    rows: list[dict[str, Any]],
    *,
    primary_id: str | None,
) -> dict[str, list[str]]:
    classifications: dict[str, list[str]] = {
        "active": [],
        "ready": [],
        "blocked": [],
        "proof_gap": [],
        "stale_view_only": [],
        "done": [],
        "invalid": [],
        "deferred": [],
    }
    for row in rows:
        bucket = _classify_workbench_row(row, primary_id=primary_id)
        classifications[bucket].append(row["change"])
    return {key: value for key, value in classifications.items() if value}


def _classify_workbench_row(row: dict[str, Any], *, primary_id: str | None) -> str:
    if primary_id and row.get("change") == primary_id:
        return "active"
    if not row.get("ok", False):
        return "invalid"
    if row.get("state") == "done":
        return "done"
    if row.get("blocker_count", 0) or row.get("environment_blocker_count", 0):
        return "blocked"
    if row.get("evidence_gap_count", 0):
        return "proof_gap"
    if row.get("review_view_status") in {"missing", "stale"}:
        return "stale_view_only"
    if row.get("next_count", 0) or row.get("state") == "ready":
        return "ready"
    return "deferred"


def _workbench_primary_summary(status: dict[str, Any]) -> dict[str, Any]:
    guidance = status.get("agent_guidance", {})
    return {
        "change": status["change"],
        "status": status.get("status"),
        "state": guidance.get("state"),
        "decision_needed": status.get("operator_brief", {}).get("decision_needed"),
        "next_action": status.get("operator_brief", {}).get("next_action"),
        "human_review": status.get("human_review"),
        "review_view": status.get("review_view"),
        "hierarchy": status.get("hierarchy"),
        "requirements": status.get("requirements"),
        "tasks": status.get("tasks"),
    }


def _workbench_next_action(status: dict[str, Any]) -> dict[str, Any]:
    guidance = status.get("agent_guidance", {})
    next_tasks = status.get("next") or []
    return {
        "state": guidance.get("state"),
        "action": guidance.get("next_action"),
        "why": guidance.get("why"),
        "task": next_tasks[0] if next_tasks else None,
        "decision_needed": status.get("operator_brief", {}).get("decision_needed"),
        "human_choices": status.get("operator_brief", {}).get("human_choices", []),
    }


def _workbench_recommended_commands(
    status: dict[str, Any],
    *,
    stale_views: list[dict[str, Any]],
) -> list[str]:
    commands = [
        command
        for command in status.get("agent_guidance", {}).get(
            "recommended_commands",
        )
        or []
        if _looks_like_command(command)
    ]
    change_id = status["change"]
    commands.extend([
        f"okg pact view {change_id} --for-agent --json",
        f"okg pact view {change_id} --format=status --json",
        "okg pact view --format=list --json",
    ])
    if stale_views:
        commands.append(
            f"okg pact view {stale_views[0]['change']} --format=markdown --json"
        )
    return _dedupe(commands)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _workbench_text(payload: dict[str, Any]) -> str:
    program = payload.get("program") or {}
    primary = payload.get("primary") or {}
    next_action = payload.get("next_action") or {}
    graph = payload.get("graph_freshness") or {}
    graph_state = graph.get("status") or "unavailable"
    graph_detail = graph.get("reason")
    if not graph_detail and graph.get("projected_generation_id") is not None:
        graph_detail = f"generation {graph['projected_generation_id']}"
    lines = [
        "PACT workbench",
        (
            f"Program: {program.get('total', 0)} changes; "
            f"{program.get('ready', 0)} ready; "
            f"{program.get('blocked', 0)} blocked; "
            f"{program.get('stale_views', 0)} stale views; "
            f"{program.get('done', 0)} done."
        ),
    ]
    if primary:
        lines.extend([
            (
                f"Primary: {primary.get('change')} "
                f"({primary.get('status')} / {primary.get('state')})."
            ),
            f"Decision needed: {primary.get('decision_needed')}.",
            f"Next action: {primary.get('next_action')}",
            (
                f"Agent action: {next_action.get('action')} - "
                f"{next_action.get('why')}"
            ),
        ])
    else:
        lines.append("Primary: none.")
    lines.append(
        "Proof gaps: "
        f"{len(payload.get('proof_gaps') or [])}; blockers: "
        f"{len((payload.get('blockers') or {}).get('tasks') or [])}; "
        f"stale selected views: {len(payload.get('stale_views') or [])}."
    )
    lines.append(
        "Graph freshness: "
        + graph_state
        + (f" ({graph_detail})." if graph_detail else ".")
    )
    classifications = payload.get("classifications") or {}
    if classifications:
        lines.append("Classifications:")
        for label, changes in classifications.items():
            lines.append(f"- {label}: {', '.join(changes)}")
    commands = payload.get("recommended_commands") or []
    if commands:
        lines.append("Recommended commands:")
        lines.extend(f"- {command}" for command in commands)
    return "\n".join(lines)


def _record_external_receipt(
    root: Path,
    change_id: str,
    decided: dict[str, Any],
) -> dict[str, Any]:
    """Write the external approval receipt beside a recorded approval.

    The receipt is produced by the SAME call that records the ledger decision,
    on purpose: a standalone "regenerate the receipt" verb would let a
    checkout mint its own merge authority with no decision behind it. When the
    manifest does not declare the surface this is a no-op; when it declares it
    and the receipt cannot be written, the reason rides on the decide payload
    rather than leaving an approval the merge gate will refuse without saying
    why.
    """
    from okg.substrate.pact.store import intent_digest as compute_intent_digest

    if envelope.verdict_of(decided) != envelope.RECORDED:
        return decided
    try:
        manifest = _load_manifest_for_gate(root, change_id)
    except Exception:  # noqa: BLE001 - the decision already landed
        return decided
    if not external_authority.change_declares_external_authority(manifest):
        return decided
    from datetime import datetime, timezone

    principal = str((decided.get("recorded") or {}).get("principal") or "")
    if not principal:
        return _receipt_write_failed(
            decided,
            {
                "written": False,
                "reason": "the decision payload named no principal",
                "blockers": ["external_receipt_provenance_incomplete"],
            },
        )
    receipt_deployment, unconfigured = _graph_projection_deployment_or_reason(root)
    if receipt_deployment is None:
        return _receipt_write_failed(
            decided,
            {
                "written": False,
                "reason": unconfigured,
                "blockers": ["external_receipt_deployment_unconfigured"],
            },
        )
    written = external_authority.write_receipt(
        root,
        manifest,
        # The digest is recomputed from the manifest AS IT NOW STANDS, not
        # copied from the decision payload: the receipt has to bind what the
        # merge gate will read, and a digest carried forward from before the
        # archive move would bind a manifest nobody checks.
        intent_digest=compute_intent_digest(manifest),
        generation_id=os.environ.get("OKG_PACT_EXTERNAL_GENERATION_ID"),
        deployment=receipt_deployment,
        recorded_by=principal,
        recorded_at=datetime.now(timezone.utc).isoformat(),
    )
    if not written.get("written"):
        return _receipt_write_failed(decided, written)
    decided["external_authority_receipt"] = written
    return decided


def _receipt_write_failed(
    decided: dict[str, Any],
    written: dict[str, Any],
) -> dict[str, Any]:
    """Report an approval whose merge authority artifact was not produced.

    The ledger decision landed, so this is not a refusal; but an approval
    with no receipt cannot pass `gate pre-merge`, and leaving `recorded` and
    exit 0 buried the failure in a nested field — the next thing the operator
    saw was the merge gate refusing for a cause nothing had announced.
    Deferred is nonzero, so a `set -e` pipeline stops here instead.
    """
    decided["external_authority_receipt"] = written
    blockers = list(written.get("blockers") or ["external_receipt_missing"])
    commands = list(decided.get("next_commands") or [])
    if "external_receipt_generation_unpinned" in blockers:
        commands = [
            "export OKG_PACT_EXTERNAL_GENERATION_ID=<generation the approval "
            "was taken against>",
            *commands,
        ]
    return envelope.restate(
        decided,
        envelope.DEFERRED,
        text=f"{decided.get('text')}; external approval receipt not written",
        message=str(
            written.get("reason") or "the external approval receipt was not written"
        ),
        blocked_by=envelope.tokens(decided.get("blocked_by"), blockers),
        next_commands=commands,
    )


def _merge_authority_status(root: Path, manifest, digest: str) -> dict[str, Any]:
    """The projection-shaped authority the merge boundary reads.

    One seam, two surfaces: a manifest that declares the external-repository
    receipt (issue #1424) is judged by the receipt and NEVER by the sync-root
    projection, and everything else keeps the projection unchanged. The
    selection lives here rather than at each call site so a caller cannot
    consult one surface and report the other.
    """
    if external_authority.change_declares_external_authority(manifest):
        return external_authority.verify_external_authority(
            root,
            manifest,
            intent_digest=digest,
        )
    return _graph_projection_status(manifest, root)


def _graph_projection_status(
    manifest, root: Path | None = None, *, dsn: str | None = None
) -> dict[str, Any]:
    current_digest = state_digest(manifest)
    deployment, unconfigured = _graph_projection_deployment_or_reason(root)
    if deployment is None:
        return envelope.component(
            ok=False,
            current_state_digest=current_digest,
            node_id=f"pact-change:{manifest.change.id}",
            deployment=None,
            authority={"mode": "unconfigured", "deployment": None},
            target=None,
            status="unavailable",
            stale=None,
            reason=unconfigured,
            next_action="configure_pact_graph_deployment",
            recommended_commands=[],
        )
    dsn_resolution = _graph_projection_dsn_resolution(
        deployment,
        explicit_dsn=dsn,
    )
    dsn = dsn_resolution.get("dsn")
    base = {
        "current_state_digest": current_digest,
        "node_id": f"pact-change:{manifest.change.id}",
        "deployment": deployment,
        "authority": dsn_resolution.get("authority", {}),
        "target": dsn_resolution.get("target"),
    }
    if not dsn:
        return envelope.component(
            ok=False,
            **base,
            status="unavailable",
            stale=None,
            reason=dsn_resolution.get("reason") or "OKG_DSN is not set",
            next_action=dsn_resolution.get("next_action") or "configure_okg_dsn",
            recommended_commands=[
                f"uv run okg deployment env {deployment} --json",
            ],
        )
    try:
        row = _load_graph_projection_row(dsn, manifest.change.id)
    except Exception as exc:  # noqa: BLE001 - status must degrade, not fail
        return envelope.component(
            ok=False,
            **base,
            status="unavailable",
            stale=None,
            reason=str(exc),
            next_action="fix_graph_projection_access",
            recommended_commands=[
                f"uv run okg deployment env {deployment} --json",
            ],
        )
    if not row:
        return envelope.component(
            ok=False,
            **base,
            status="missing",
            stale=True,
            reason="PACT change node not found in graph projection",
            next_action="publish_graph_projection",
            recommended_commands=_graph_projection_refresh_commands(
                manifest.change.id,
                deployment=deployment,
            ),
        )
    projected_digest = row.get("projected_state_digest")
    stale = projected_digest != current_digest
    return envelope.component(
        ok=True,
        **base,
        status="stale" if stale else "fresh",
        stale=stale,
        next_action="refresh_graph_projection" if stale else "none",
        recommended_commands=(
            _graph_projection_refresh_commands(
                manifest.change.id,
                deployment=deployment,
            )
            if stale
            else []
        ),
        **row,
    )


def _graph_projection_deployment(root: Path | None = None) -> str | None:
    """The PACT graph deployment for ``root`` (or the enclosing checkout).

    ``None`` when nothing is configured; there is no built-in deployment name.
    """
    from okg.substrate.pact.project_settings import resolve_pact_graph_deployment

    if root is None:
        from okg.substrate.deployment_default import _configured_root

        root = _configured_root(Path.cwd().resolve())
    return resolve_pact_graph_deployment(root)


def _graph_projection_deployment_or_reason(
    root: Path | None = None,
) -> tuple[str | None, str | None]:
    """(deployment, None) or (None, why) -- for surfaces that must degrade."""
    from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

    try:
        deployment = _graph_projection_deployment(root)
    except SubstrateError as exc:
        return None, str(exc)
    if deployment is None:
        return None, (
            "no PACT graph deployment is configured; " + PACT_GRAPH_DEPLOYMENT_FIX
        )
    return deployment, None


def _graph_projection_dsn_resolution(
    deployment: str,
    *,
    explicit_dsn: str | None,
) -> dict[str, Any]:
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    resolution = resolve_deployment_dsn_authority(
        deployment,
        explicit_dsn=explicit_dsn,
        local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
    )
    if resolution.get("dsn"):
        return resolution
    return {
        "dsn": None,
        "reason": resolution.get("reason") or "OKG_DSN is not set",
        "next_action": resolution.get("next_action") or "configure_okg_dsn",
        "authority": resolution.get("authority") or {
            "mode": "unresolved", "deployment": deployment,
        },
        "target": resolution.get("target"),
    }


def _graph_projection_refresh_commands(
    change_id: str,
    *,
    deployment: str | None = None,
) -> list[str]:
    deployment = deployment or _graph_projection_deployment_or_reason()[0]
    env_command = (
        [f"uv run okg deployment env {deployment} --json"] if deployment else []
    )
    return [
        *env_command,
        f"uv run okg pact gate graph-freshness {change_id} --refresh --json",
        f"uv run okg pact view {change_id} --format=status --json",
    ]


def _graph_projection_ingest_command(deployment: str) -> list[str]:
    return [
        *running_okg_argv(),
        "ingest",
        "--deployment",
        deployment,
        "--include",
        "pact",
        "--mode",
        "scope_complete",
        "--inline",
        "--json",
    ]


def _graph_projection_foreign_checkout_commands(
    change_id: str,
    provenance: dict[str, Any],
) -> list[str]:
    commands = [f"uv run okg pact gate pre-merge {change_id} --json"]
    commands.extend(
        move for move in provenance.get("legal_moves", [])
        if isinstance(move, str)
    )
    return commands


def _load_graph_projection_row(
    dsn: str,
    change_id: str,
) -> dict[str, Any] | None:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    # Chronos cutover: the PACT change node is read from the Chronos
    # graph (`okg.graph_nodes`). The legacy per-generation visibility
    # window (`valid_from_gen` -> `projected_generation_id`) has no
    # Chronos equivalent and is dropped; the checkout injects
    # visibility, so the bitemporal pin predicate is gone too.
    with db_connect(dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        rows = graph.query(
            """
            SELECT
                attrs->>'state_digest' AS projected_state_digest,
                attrs->>'status' AS projected_status,
                attrs->>'verification_gate' AS projected_verification_gate,
                attrs->'next_task_ids' AS projected_next_task_ids,
                attrs->'evidence_gaps' AS projected_evidence_gaps
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": f"pact-change:{change_id}"},
        )
    return dict(rows[0]) if rows else None


def _show(root: Path, change_id: str, *, for_agent: bool) -> dict[str, Any]:
    manifest = _load_manifest_for_gate(root, change_id)
    if not for_agent:
        # Even the raw manifest dump carries the envelope: `no output shape
        # may omit ok`, and this was the last shape that did. The dump stays
        # SPREAD AT THE TOP LEVEL, exactly where it was before the envelope:
        # `--format=manifest` is the DEFAULT of `okg pact view <id> --json`,
        # so nesting it under a `manifest` key silently changed the type of
        # `payload["change"]` from the change object to the id string. The
        # envelope keys are additive; no pre-envelope key moved.
        return envelope.recorded(
            text=f"manifest {manifest.change.id}",
            format="manifest",
            **manifest.model_dump(mode="json", by_alias=True, exclude_none=True),
        )
    alignment = derive_alignment(manifest, repo_root=root)
    return envelope.recorded(
        text=f"manifest {manifest.change.id} (agent view)",
        **{
        "change": manifest.change.model_dump(mode="json"),
        "versions": alignment.get("versions", {}),
        "lifecycle": alignment.get("lifecycle", {}),
        "hierarchy": _hierarchy_rollup(root, manifest),
        "evidence_gaps": [
            {"id": req_id, "missing": state["missing"]}
            for req_id, state in alignment["requirements"].items()
            if state.get("satisfaction_status") == "unsatisfied"
            or state.get("status") == "missing_evidence"
        ],
        "task_evidence_issues": _task_evidence_issues(alignment),
        "environment_blockers": _environment_blockers(alignment),
        "requirements": [
            {
                "id": req.id,
                "title": req.title,
                "required_evidence": req.evidence_policy.required,
                "alignment": alignment["requirements"].get(req.id),
            }
            for req in manifest.requirements
        ],
        "tasks": [
            {
                "id": task.id,
                "title": task.title,
                "status": task.status,
                "requirements": task.requirements,
                "verification": task.verification,
                "alignment": alignment["tasks"].get(task.id),
            }
            for task in manifest.tasks
        ],
        "next": _next_tasks(manifest, alignment=alignment),
        "blocked_tasks": _blocked_tasks(manifest, alignment),
        "review_view": _review_view_status(root, manifest),
        },
    )


def _next(root: Path, change_id: str) -> dict[str, Any]:
    manifest = _load_manifest_for_gate(root, change_id)
    alignment = derive_alignment(manifest, repo_root=root)
    next_tasks = _next_tasks(manifest, alignment=alignment)
    blocked = _blocked_tasks(manifest, alignment)
    environment = _environment_blockers(alignment)
    # A read is actionable when it can name the next executable task.
    # With nothing runnable and something blocking, the answer is a
    # refusal that names the blocker, not an empty list under ok: true.
    return envelope.from_ok(
        bool(next_tasks) or not (blocked or environment),
        text=f"next {change_id}: {len(next_tasks)} runnable, {len(blocked)} blocked",
        blocked_by=envelope.tokens(
            (f"blocked_task:{row.get('id')}" for row in blocked)
            if not next_tasks
            else [],
            (str(row.get("code") or row) for row in environment)
            if not next_tasks
            else [],
        ),
        change=change_id,
        next=next_tasks,
        blocked_tasks=blocked,
        environment_blockers=environment,
    )


_REPORT_REQUIRED_FIELDS = [
    "intent",
    "pact_id",
    "task_id",
    "report_kind",
    "current_mode",
    "final",
    "work_completed",
    "evidence",
    "commands",
    "graph_state",
    "blocker",
    "next_action",
    "confidence",
    "effort_budget",
    "unresolved_risks",
]
_REPORT_GRAPH_FIELDS = [
    "okg_ready",
    "generation_id",
    "generation_status",
    "source_freshness",
    "queue_depth",
    "dlq_count",
    "doctor_status",
    "mcp_graph_read",
]
_REPORT_KINDS = ["checkpoint", "blocker", "handoff", "subagent-rollup", "closeout"]
_REPORT_CLOSEOUT_REQUIRED_FIELDS = [
    *_REPORT_REQUIRED_FIELDS,
    "changed_files",
    "residual_risk",
]
_CONTINUATION_REQUIRED_FIELDS = [
    "intent",
    "pact_id",
    "task_id",
    "repo",
    "worktree",
    "branch",
    "commit",
    "dirty_state",
    "evidence",
    "commands_run",
    "blocker",
    "next_command",
    "confidence",
    "unresolved_risks",
]


BOOTSTRAP_SUMMARY_BUDGET_BYTES = 4096


def _bootstrap_actionable(cutover_gate: dict[str, Any]) -> bool:
    """Whether the ENVIRONMENT is ready — graph, ledger, skills, coordination.

    Distinct from the envelope verdict, which answers a different question:
    can this CHANGE be worked at all? A packet for a change whose selected task
    carries manifest blockers is `refused` however healthy the environment is,
    and a change with nothing blocked is `recorded` even where the cutover gate
    reports an unconfigured graph. The two used to be conflated onto a single
    `ok: true`, which is why the blocked and healthy packets read identically.

    Kept as a named function so the derivation is directly testable: a literal
    here is the defect this exists to prevent, and a test that reconstructed the
    rule itself would not catch the literal coming back.
    """

    return bool(cutover_gate.get("ready"))


def _bootstrap_task_blockers(task_bundle: dict[str, Any]) -> list[str]:
    """The blockers that stop work on the CHANGE, from the manifest itself.

    Not `actionable`: that is derived from the cutover gate, so it reports the
    same environmental value for a fully blocked change and a healthy one.

    TOKENS, not prose: a task marked `blocked` carries a free-text
    `blocked_reason`, and putting that sentence in `blocked_by` made the single
    cause location carry an unmatched string per author instead of a glossary
    token. The sentence is still published — as the operator REMEDY, which is
    what it actually is (see :func:`_bootstrap_operator_action`).
    """

    tokens: list[str] = []
    task_id = str(task_bundle.get("id") or "task")
    for token in task_bundle.get("blockers") or []:
        text = str(token)
        tokens.append(
            f"task_blocked:{task_id}" if " " in text.strip() else text
        )
    return list(dict.fromkeys(tokens))


def _bootstrap_operator_action(
    task_bundle: dict[str, Any],
    manifest,
) -> dict[str, Any] | None:
    """Owner + remedy for a task whose blocker is out of band.

    A task the author marked `blocked` with a prose reason is, by construction,
    waiting on something no `okg pact` command performs. The refusal names the
    owner and quotes the author's own reason as the remedy — a remedy DERIVED
    FROM THE STATED CAUSE, never a defaulted instruction.
    """
    from okg.substrate.pact import repair as repair_mod

    task_id = task_bundle.get("id")
    task = next((row for row in manifest.tasks if row.id == task_id), None)
    if task is None or task.status != "blocked":
        return None
    reason = (task.blocked_reason or "").strip()
    if not reason:
        return None
    return {
        "owner": repair_mod.OPERATOR_OWNER_DEFAULT,
        "cause": f"task_blocked:{task_id}",
        "remedy": reason,
        "remedy_source": "cause_fix_hint",
        "in_band_repair": False,
    }


def _bootstrap_blocker_detail(check: dict[str, Any] | None) -> dict[str, Any] | None:
    """What is unmet, who can clear it, and the command that does.

    A bare action token such as `configure_environment` does not say that
    OKG_DSN is unresolved, that `.okg/local.env` is absent, or which command
    writes one -- so a blocked agent re-derives the diagnosis, or works around
    it. The cutover check already carries all of that; this surfaces it instead
    of discarding it.
    """

    if not check:
        return None
    return {
        "category": check.get("category"),
        "owner": check.get("owner"),
        "next_action": check.get("next_action"),
        "command": check.get("command"),
        "detail": _bootstrap_blocker_reason(check.get("detail")),
    }


def _bootstrap_blocker_reason(detail: Any, *, budget: int = 240) -> Any:
    """A detail small enough for the summary's 4 KB budget.

    Runtime-authority details carry the whole dogfood status payload, which on
    its own can exceed the budget for the entire packet. The summary needs the
    reason, not the report; `--full` still carries the untrimmed check.
    """

    if detail is None or isinstance(detail, (bool, int, float)):
        return detail
    if isinstance(detail, str):
        return detail if len(detail) <= budget else detail[: budget - 3] + "..."
    if isinstance(detail, dict):
        # Keep scalar leaves and flat lists of scalars: those carry the reason
        # (`{"unresolved": ["OKG_DSN"]}` is the whole point). Deeper structures
        # are the report and belong behind --full.
        def _keep(value: Any) -> bool:
            if isinstance(value, (str, bool, int, float)) or value is None:
                return True
            return isinstance(value, list) and all(
                isinstance(item, (str, bool, int, float)) for item in value
            )

        kept = {key: value for key, value in detail.items() if _keep(value)}
        trimmed = {
            key: _bootstrap_blocker_reason(value, budget=budget)
            for key, value in list(kept.items())[:6]
        }
        return trimmed or {"summary": "see cutover_gate in --full"}
    if isinstance(detail, list):
        return [_bootstrap_blocker_reason(item, budget=budget) for item in detail[:5]]
    return str(detail)[:budget]


def _bootstrap_summary(payload: dict[str, Any]) -> dict[str, Any]:
    """The one-call pre-task packet (req.budget.one-call-bootstrap).

    Everything CLAUDE.md used to source from four runbooks, inside 4 KB:
    active task, graph state, coordination move, files, next command.
    Full report behind --full; pointers replace embedded contracts."""
    pact = payload.get("pact") or {}
    bundle = payload.get("task_bundle") or {}
    readiness = payload.get("graph_readiness") or {}
    _blocking_check = _bootstrap_blocking_check(payload.get("cutover_gate") or {})
    coordination = payload.get("coordination") or {}
    skill_freshness = payload.get("skill_freshness") or {}
    graph = pact.get("graph_projection") or {}
    commands = payload.get("commands") or {}
    next_action = payload.get("next_action") or {}
    lifecycle_command = (
        next_action.get("command")
        if isinstance(next_action, dict) and pact.get("state") != "in_progress"
        else None
    )
    default_next_command = (
        lifecycle_command
        or (
            str(_blocking_check.get("command"))
            if _blocking_check and _blocking_check.get("command")
            else commands.get("coordination_next") or commands.get("continue")
        )
    )
    project_settings = payload.get("project_settings") or {}
    version_defaults = project_settings.get("version_defaults") or {}
    local_settings = project_settings.get("worktree_local_settings") or {}
    verification = bundle.get("verification")
    if isinstance(verification, str) and len(verification) > 200:
        verification = verification[:197] + "..."
    verdict = envelope.verdict_of(payload)
    summary: dict[str, Any] = envelope.envelope(
        verdict,
        text=(
            f"bootstrap {payload.get('change')}: {verdict} "
            f"task={bundle.get('task_id')}"
        ),
        blocked_by=payload.get("blocked_by") or [],
        next_commands=[
            command
            for command in [default_next_command]
            if command
        ],
        **{
        "actionable": bool(payload.get("actionable")),
        "change": payload.get("change"),
        "format": "bootstrap",
        "title": pact.get("title"),
        "task": {
            "id": bundle.get("task_id"),
            "title": bundle.get("title"),
            "verification": verification,
            "evidence_gaps": (bundle.get("evidence_gaps") or [])[:5],
            "blockers": (bundle.get("blockers") or [])[:5],
        },
        "graph": {
            "projection": graph.get("status"),
            "stale": graph.get("stale"),
            "generation": readiness.get("generation_id")
            or readiness.get("generation"),
            "trust": readiness.get("trust_level") or readiness.get("status"),
        },
        "coordination": {
            "state": coordination.get("state") or coordination.get("status"),
            "next": coordination.get("next_command_key"),
        },
        "skills": {
            "status": skill_freshness.get("status"),
            "missing": skill_freshness.get("missing_count"),
            "stale": skill_freshness.get("stale_count"),
            "orphaned": skill_freshness.get("orphaned_count"),
        },
        "project_settings": {
            "path": project_settings.get("path"),
            "target_pact_version": version_defaults.get("target_pact_version"),
            "artifact_format_version": version_defaults.get("artifact_format_version"),
            "local_overlay_count": len(local_settings.get("overlays") or []),
            "warnings": (local_settings.get("warnings") or [])[:3],
        },
        "relevant_files": (bundle.get("relevant_files") or [])[:10],
        "expected_tests": (bundle.get("expected_tests") or [])[:5],
        "next_action": next_action,
        # Paired with next_action off the same blocking row. When nothing
        # blocks, the ordinary coordination command stands.
        "next_command": default_next_command,
        "blocker": _bootstrap_blocker_detail(_blocking_check),
        # Carried into the compact packet: the owner and the out-of-band
        # remedy are the whole repair for a blocked task, and dropping them in
        # the summary left the budgeted surface naming a cause with no fix.
        "operator_action": payload.get("operator_action"),
        "contracts": {
            "reporting": "docs/runbooks/agents/reporting.md",
            "continuation": "docs/runbooks/agents/reporting.md",
            "worktree_tiers": "docs/runbooks/agents/worktrees.md",
        },
        # A command, not the instruction "rerun with --full": the compact
        # packet already carries every message a reader needs, and `--full`
        # is for verbose artifacts only.
        "full_report": (
            f"okg pact view {payload.get('change')} --format=bootstrap --full --json"
        ),
        },
    )
    return envelope.fit_budget(
        summary,
        BOOTSTRAP_SUMMARY_BUDGET_BYTES,
        trim=(("relevant_files", 3), ("expected_tests", 2)),
    )


def _agent_bootstrap(root: Path, change_id: str) -> dict[str, Any]:
    manifest = _load_manifest_for_gate(root, change_id)
    alignment = derive_alignment(manifest, repo_root=root)
    status = _status(root, change_id, include_graph=True)
    # Bootstrap is the agent start/resume surface. A stale generated human
    # review view should be reported, but it must not hide the executable task.
    derived_state = status.get("derived_state") or {}
    normative_v4 = bool(status.get("normative_v4"))
    display_state = str(status.get("state") or "draft")
    lifecycle_state = (
        str(derived_state.get("state") or "draft")
        if normative_v4
        else "in_progress"
    )
    next_tasks = list(status.get("next") or [])
    status["next"] = next_tasks
    active_task = (
        next_tasks[0]
        if next_tasks
        else (
            _bootstrap_task_frontier(manifest, alignment)
            if lifecycle_state == "in_progress"
            else None
        )
    )
    task = (
        next((row for row in manifest.tasks if row.id == active_task["id"]), None)
        if active_task
        else None
    )
    git_state = _git_worktree_state(root)
    project_settings = {
        "path": "pact/project.yaml",
        "version_defaults": pact_version_defaults(root).to_json(),
        "worktree_local_settings": worktree_local_settings_report(root),
    }
    # The repository's own PACT graph deployment, never a built-in name: a
    # repository that configures none gets a clear "not configured" blocker.
    bootstrap_deployment, unconfigured = _graph_projection_deployment_or_reason(root)
    if bootstrap_deployment is None:
        readiness = _bootstrap_unconfigured_readiness(str(unconfigured))
        catalog_ownership = envelope.component(
            ok=None,
            recognized=False,
            status="not_configured",
            deployment_path=None,
            command=None,
            returncode=None,
            error=unconfigured,
        )
    else:
        readiness = _bootstrap_readiness(root, deployment=bootstrap_deployment)
        in_tree = root / "deployments" / bootstrap_deployment
        catalog_ownership = _bootstrap_catalog_ownership(
            root,
            deployment_path=(
                f"deployments/{bootstrap_deployment}"
                if in_tree.is_dir()
                else bootstrap_deployment
            ),
        )
    from okg.substrate.pact.agent_install import agent_skill_freshness
    skill_freshness = agent_skill_freshness(root)
    coordination = _bootstrap_coordination(
        root,
        manifest.change.id,
        task.id if task else None,
        task.title if task else None,
        git_state,
    )
    if active_task is None and lifecycle_state == "in_progress":
        active_task = _bootstrap_coordination_task(
            manifest,
            alignment,
            coordination,
        )
        task = (
            next((row for row in manifest.tasks if row.id == active_task["id"]), None)
            if active_task
            else None
        )
        if task is not None:
            coordination = _bootstrap_coordination(
                root,
                manifest.change.id,
                task.id,
                task.title,
                git_state,
            )
    expected_tests = _bootstrap_expected_tests(manifest, task)
    relevant_files = _bootstrap_relevant_files(manifest, expected_tests)
    commands = _bootstrap_commands(
        manifest.change.id,
        task.id if task else None,
        task.title if task else None,
        git_state,
        deployment=bootstrap_deployment,
    )
    lifecycle_command: str | None = None
    lifecycle_owner = "agent"
    if normative_v4 and lifecycle_state != "in_progress":
        lifecycle_command, lifecycle_owner = _lifecycle_successor(
            root,
            manifest.change.id,
            lifecycle_state,
        )
        commands["lifecycle_next"] = lifecycle_command
    next_coord_key = coordination.get("next_command_key")
    if isinstance(next_coord_key, str) and next_coord_key in commands:
        commands["coordination_next"] = commands[next_coord_key]
    worktree_contract = {
        "source": "docs/runbooks/agents/worktrees.md",
        "tiers": {
            "R": "read canonical pinned; check source_freshness in responses",
            "W": (
                "file-derived publish to canonical only from sync_root; "
                "worktrees merge first or use an isolated clone"
            ),
            "E": (
                "decisions/sessions/intents always shared; gate post-edit "
                "registers your in-flight PACT for other agents"
            ),
        },
    }
    report_contract = {
        "source": "docs/runbooks/agents/reporting.md",
        "default_report_kind": "checkpoint",
        "closeout_report_kind": "closeout",
        "effort_mode": "standard-pact-task",
        "required_fields": _REPORT_REQUIRED_FIELDS,
        "graph_state_fields": _REPORT_GRAPH_FIELDS,
        "report_kinds": {
            "checkpoint": {
                "final": False,
                "required_fields": _REPORT_REQUIRED_FIELDS,
            },
            "blocker": {
                "final": False,
                "required_fields": _REPORT_REQUIRED_FIELDS,
            },
            "handoff": {
                "final": True,
                "required_fields": _REPORT_REQUIRED_FIELDS,
            },
            "subagent-rollup": {
                "final": False,
                "required_fields": [*_REPORT_REQUIRED_FIELDS, "subagents"],
            },
            "closeout": {
                "final": True,
                "required_fields": _REPORT_CLOSEOUT_REQUIRED_FIELDS,
            },
        },
    }
    continuation = {
        "source": "docs/runbooks/agents/reporting.md",
        "operator_move": "continue",
        "report_kind": "handoff",
        "required_fields": _CONTINUATION_REQUIRED_FIELDS,
        "next_command": lifecycle_command or commands["continue"],
    }
    cutover_gate = _bootstrap_cutover_gate(
        readiness=readiness,
        catalog_ownership=catalog_ownership,
        skill_freshness=skill_freshness,
        coordination=coordination,
        status=status,
        active_task=active_task,
        report_contract=report_contract,
    )
    task_blockers = _bootstrap_task_blockers(active_task or {})
    operator_action = _bootstrap_operator_action(active_task or {}, manifest)
    # Derived, never a literal: a consumer that branches on `ok` alone must
    # stop when a precondition blocks. Reporting ok=true beside a blocking
    # `next_action` is how a knowable-at-step-zero blocker became a
    # discovered-at-the-last-step one -- the task gets fully implemented and
    # proven before anything refuses.
    return envelope.from_ok(
        not task_blockers,
        text=(
            f"bootstrap {manifest.change.id}: "
            f"task={active_task.get('id') if active_task else None}"
        ),
        blocked_by=task_blockers,
        next_commands=[lifecycle_command or commands["continue"]],
        **{
        "actionable": _bootstrap_actionable(cutover_gate),
        "change": manifest.change.id,
        # Present only when the blocker is genuinely out of band; `None`
        # otherwise, so a reader cannot mistake "nobody named an owner" for
        # "there is no owner to name".
        "operator_action": operator_action,
        "format": "bootstrap",
        "operator_moves": ["start", "continue"],
        "pact": {
            "id": manifest.change.id,
            "title": manifest.change.title,
            "status": manifest.change.status,
            "state": display_state,
            "review_view": status.get("review_view"),
            "graph_projection": status.get("graph_projection"),
            "next_task": active_task,
            "blocked_tasks": status.get("blocked_tasks", []),
            "environment_blockers": status.get("environment_blockers", []),
        },
        "task_bundle": {
            "change_id": manifest.change.id,
            "task_id": task.id if task else None,
            "title": task.title if task else None,
            "verification": task.verification if task else None,
            "requirements": list(task.requirements) if task else [],
            "evidence_gaps": active_task.get("evidence_gaps", []) if active_task else [],
            "blockers": active_task.get("blockers", []) if active_task else [],
            "dependency_blockers": (
                active_task.get("dependency_blockers", []) if active_task else []
            ),
            "environment_blockers": (
                active_task.get("environment_blockers", []) if active_task else []
            ),
            "expected_tests": expected_tests,
            "relevant_files": relevant_files,
        },
        "graph_readiness": readiness,
        "project_settings": project_settings,
        "skill_freshness": skill_freshness,
        "catalog_ownership": catalog_ownership,
        "coordination": coordination,
        "git": git_state,
        "commands": commands,
        "report_contract": report_contract,
        "worktree_contract": worktree_contract,
        "continuation": continuation,
        "cutover_gate": cutover_gate,
        "next_action": (
            {
                "command": lifecycle_command,
                "owner": lifecycle_owner,
                "why": "continue the derived lifecycle before selecting implementation work",
            }
            if lifecycle_command
            else {
                "action": _bootstrap_next_action(
                    cutover_gate,
                    active_task,
                    coordination,
                ),
                "command": commands.get("coordination_next") or commands["continue"],
                "owner": "agent",
                "why": "claim or continue the active implementation task",
            }
        ),
        },
    )


def _bootstrap_task_frontier(
    manifest,
    alignment: dict[str, Any],
) -> dict[str, Any] | None:
    tasks_by_id = {task.id: task for task in manifest.tasks}
    for blocked_task in _blocked_tasks(manifest, alignment):
        if blocked_task.get("dependency_blockers"):
            continue
        task = tasks_by_id.get(blocked_task["id"])
        if task is None:
            continue
        blockers = list(blocked_task.get("blockers", []))
        blocker_text = ", ".join(str(blocker) for blocker in blockers)
        why = f"dependencies satisfied; blocked by {blocker_text}"
        return {
            "id": task.id,
            "title": task.title,
            "status": blocked_task.get("status", task.status),
            "why": why,
            "verification": task.verification,
            "evidence_gaps": (
                alignment["tasks"].get(task.id, {}).get("satisfaction_missing", [])
            ),
            "blockers": blockers,
            "dependency_blockers": blocked_task.get("dependency_blockers", []),
            "environment_blockers": blocked_task.get("environment_blockers", []),
            "suggested": [
                task.verification,
                (
                    f"okg pact evidence ingest {manifest.change.id} "
                    "--pytest-report <junit.xml>"
                ),
                f"okg pact gate task-done {manifest.change.id} {task.id} --json",
                f"okg pact view {manifest.change.id} --json",
            ],
        }
    return None


def _bootstrap_coordination_task(
    manifest,
    alignment: dict[str, Any],
    coordination: dict[str, Any],
) -> dict[str, Any] | None:
    known = {task.id for task in manifest.tasks}
    candidates: list[str] = []
    for raw in [
        coordination.get("current_order"),
        *_list_or_empty(coordination.get("matching_orders")),
        *_list_or_empty(coordination.get("human_decisions")),
    ]:
        if not isinstance(raw, dict):
            continue
        task_id = raw.get("task_id")
        if isinstance(task_id, str):
            candidates.append(task_id)
        for target in _list_or_empty(raw.get("target_tasks")):
            if isinstance(target, str):
                candidates.append(target)
    for task_id in candidates:
        if task_id in known:
            task = next(row for row in manifest.tasks if row.id == task_id)
            state = alignment["tasks"].get(task.id, {})
            return {
                "id": task.id,
                "title": task.title,
                "status": state.get("status", task.status),
                "why": "selected from active coordination order",
                "verification": task.verification,
                "evidence_gaps": state.get("satisfaction_missing", []),
                "blockers": state.get("blockers", []),
                "dependency_blockers": state.get("dependency_blockers", []),
                "environment_blockers": state.get("environment_blockers", []),
                "suggested": [
                    task.verification,
                    (
                        f"okg pact evidence ingest {manifest.change.id} "
                        "--pytest-report <junit.xml>"
                    ),
                    f"okg pact gate task-done {manifest.change.id} {task.id} --json",
                    f"okg pact view {manifest.change.id} --json",
                ],
            }
    return None


def _bootstrap_blocking_check(cutover_gate: dict[str, Any]) -> dict[str, Any] | None:
    """The check that blocks this packet, or None when nothing does.

    Single source for the blocker: `next_action`, `next_command`, and the
    `blocker` detail are all read off THIS row. Deriving them independently is
    what let the packet report `configure_environment` beside a `next_command`
    pointing at the review queue -- naming a blocker while offering an unrelated
    command manufactures confidence rather than withholding it.
    """

    if cutover_gate.get("ready"):
        return None
    rows = [row for row in cutover_gate.get("checks", []) if row.get("status") != "pass"]
    # A real blocker outranks a skipped check, so a skipped doctor cannot supply
    # the next command ahead of an unmet precondition. But a skipped check is
    # still the reason the gate is not ready when nothing else is outstanding --
    # `run_doctor_for_verification` is pending work, not noise -- so fall back to
    # it rather than reporting no blocker at all and stranding the caller with a
    # generic `clear_cutover_blocker`.
    return next(
        (row for row in rows if row.get("status") != "skipped"),
        next(iter(rows), None),
    )


def _bootstrap_next_action(
    cutover_gate: dict[str, Any],
    active_task: dict[str, Any] | None,
    coordination: dict[str, Any] | None = None,
) -> str:
    if not cutover_gate.get("ready"):
        blocker = _bootstrap_blocking_check(cutover_gate)
        return blocker.get("next_action", "clear_cutover_blocker") if blocker else "clear_cutover_blocker"
    if not active_task:
        return "no_unblocked_task"
    claim_state = _dict_or_empty(
        _dict_or_empty(coordination).get("claim_state")
    ).get("state")
    return {
        "no_order_recorded": "record_coordination_intent",
        "claim_required": "claim_active_task",
        "continue_existing_claim": "continue_active_claim",
        "review_required": "record_coordination_review",
        "merge_ready": "finalize_coordination_order",
        "coordination_unavailable": "fix_coordination_status",
        "no_active_task": "no_unblocked_task",
    }.get(str(claim_state), "claim_or_continue_active_task")


def _bootstrap_readiness(root: Path, *, deployment: str) -> dict[str, Any]:
    # Issue #2301: these probes run with cwd set to the PACT root, which can
    # be another repository (external-repository PACTs such as
    # okg-deployments). `uv run okg` there would run THAT project's okg, so
    # every probe starts the running okg through the helper instead.
    dogfood_command = [
        *running_okg_argv(),
        "dogfood",
        "status",
        "--deployment",
        deployment,
        "--skip-doctor",
        "--json",
    ]
    dogfood_result = _bootstrap_run_json_command(
        dogfood_command,
        cwd=root,
        timeout_seconds=_bootstrap_timeout_seconds(
            "OKG_PACT_BOOTSTRAP_DOGFOOD_STATUS_TIMEOUT_SECONDS",
            _BOOTSTRAP_DOGFOOD_STATUS_TIMEOUT_SECONDS,
        ),
    )
    dogfood_payload = _dict_or_empty(dogfood_result.get("json"))
    if dogfood_payload.get("schema") == "okg.dogfood.runtime_status.v1":
        return _bootstrap_readiness_from_dogfood_status(
            deployment=deployment,
            result=dogfood_result,
            payload=dogfood_payload,
        )
    if dogfood_result.get("error"):
        return _bootstrap_partial_readiness(
            deployment=deployment,
            result=dogfood_result,
            source="dogfood_status",
        )

    command = [
        *running_okg_argv(),
        "deployment",
        "ready",
        deployment,
        "--checks",
        "dogfood",
        "--skip-doctor",
        "--json",
    ]
    result = _bootstrap_run_json_command(
        command,
        cwd=root,
        timeout_seconds=_bootstrap_timeout_seconds(
            "OKG_PACT_BOOTSTRAP_DEPLOYMENT_READY_TIMEOUT_SECONDS",
            _BOOTSTRAP_DEPLOYMENT_READY_TIMEOUT_SECONDS,
        ),
    )
    payload = _dict_or_empty(result.get("json"))
    if not payload:
        return _bootstrap_partial_readiness(
            deployment=deployment,
            result=result,
            source="deployment_ready",
        )
    generation = _dict_or_empty(payload.get("generation"))
    queues = _dict_or_empty(payload.get("queues"))
    doctor = _dict_or_empty(payload.get("doctor"))
    mcp = _dict_or_empty(payload.get("mcp"))
    source_freshness = _dict_or_empty(payload.get("source_freshness"))
    source_sync_status = _dict_or_empty(payload.get("source_sync_status"))
    source_sync_liveness = _dict_or_empty(payload.get("source_sync_liveness"))
    return {
        "deployment": deployment,
        "profile": _dict_or_empty(payload.get("profile")),
        "okg_ready": bool(payload.get("ok")) if payload else False,
        "status": "pass" if payload.get("ok") else "blocker",
        "generation_id": generation.get("generation_id") or generation.get("id"),
        "generation_status": generation.get("status"),
        "source_freshness": source_freshness,
        "source_sync_status": source_sync_status,
        "source_sync_liveness": source_sync_liveness,
        "queue_depth": queues.get("active_queue_depth"),
        "dlq_count": queues.get("dlq_count"),
        "doctor_status": doctor.get("status"),
        "mcp_graph_read": bool(mcp.get("ok")) if mcp else False,
        "blockers": _list_or_empty(payload.get("blockers")),
        "warnings": _list_or_empty(payload.get("warnings")),
        "command": result["command"],
        "returncode": result.get("returncode"),
        "error": result.get("error"),
}


def _bootstrap_catalog_ownership(
    root: Path,
    *,
    deployment_path: str,
) -> dict[str, Any]:
    command = [
        *running_okg_argv(),
        "catalog",
        "ownership",
        "diagnose",
        "--deployment",
        deployment_path,
        "--json",
    ]
    result = _bootstrap_run_json_command(
        command,
        cwd=root,
        timeout_seconds=_bootstrap_timeout_seconds(
            "OKG_PACT_BOOTSTRAP_CATALOG_OWNERSHIP_TIMEOUT_SECONDS",
            4,
        ),
    )
    payload = _dict_or_empty(result.get("json"))
    recognized = (
        "status" in payload
        and "ok" in payload
        and "recommended_commands" in payload
    )
    if not recognized:
        return envelope.component(
            ok=None,
            recognized=False,
            status="unknown",
            deployment_path=deployment_path,
            command=result.get("command"),
            returncode=result.get("returncode"),
            error=result.get("error"),
        )
    owner = _dict_or_empty(payload.get("owner"))
    expected = _dict_or_empty(payload.get("expected"))
    owner_metadata = _dict_or_empty(owner.get("metadata"))
    expected_provenance = _dict_or_empty(expected.get("provenance"))
    return envelope.component(
        ok=bool(payload.get("ok")),
        **{
        "recognized": True,
        "status": payload.get("status"),
        "issue": payload.get("issue"),
        "deployment_name": payload.get("deployment_name"),
        "deployment_path": deployment_path,
        "owner": {
            "status": owner.get("status"),
            "claimed_at": owner.get("claimed_at"),
            "git_commit": owner_metadata.get("git_commit"),
            "git_branch": owner_metadata.get("git_branch"),
            "last_seen_catalog_version_id": owner.get(
                "last_seen_catalog_version_id"
            ),
        },
        "expected": {
            "git_commit": expected_provenance.get("git_commit"),
            "git_branch": expected_provenance.get("git_branch"),
            "git_dirty": expected_provenance.get("git_dirty"),
        },
        "latest_compatible": payload.get("latest_compatible"),
        "legacy_rows_present": payload.get("legacy_rows_present"),
        "mixed_recent_shapes": payload.get("mixed_recent_shapes"),
        "recommended_commands": _list_or_empty(
            payload.get("recommended_commands")
        ),
        "command": result.get("command"),
        "returncode": result.get("returncode"),
        "error": result.get("error"),
        },
    )


def _doctor_recovery_command(deployment: str) -> str:
    """The doctor command readiness runs for ``deployment``.

    Same rule as ``_ready_doctor_status``: a deployment that declares a doctor
    check under its own name gets ``--check <deployment>``, so the suggested
    recovery reproduces the check that blocked readiness.
    """
    quoted = shlex.quote(deployment)
    try:
        from okg.substrate.doctor import load_deployment_doctor_checks

        own_check = any(
            check.name == deployment
            for check in load_deployment_doctor_checks(deployment)
        )
    except Exception:  # noqa: BLE001 - a hint must not fail the packet
        own_check = False
    check = f"--check {quoted} " if own_check else ""
    return f"uv run okg doctor {check}--deployment {quoted} --json"


def _bootstrap_unconfigured_readiness(reason: str) -> dict[str, Any]:
    """Readiness when the repository names no PACT graph deployment.

    Nothing is probed: there is no deployment to ask. The blocker says so and
    names the configuration that fixes it.
    """
    from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

    return {
        "deployment": None,
        "profile": {"name": "dogfood"},
        "okg_ready": False,
        "status": "blocker",
        "partial": True,
        "partial_reason": reason,
        "source": "pact_graph_deployment",
        "generation_id": None,
        "generation_status": "unknown",
        "source_freshness": {"status": "unknown"},
        "source_sync_status": {"status": "unknown"},
        "source_sync_liveness": {"status": "unknown"},
        "queue_depth": None,
        "dlq_count": None,
        "doctor_status": "not_checked",
        "mcp_graph_read": False,
        "blockers": [
            {
                "id": "pact_graph_deployment.not_configured",
                "status": "blocker",
                "message": reason,
            }
        ],
        "warnings": [],
        "command": None,
        "returncode": None,
        "error": reason,
        "next_action": {
            "code": "configure_pact_graph_deployment",
            "owner": "operator",
            "command": PACT_GRAPH_DEPLOYMENT_FIX,
        },
    }


def _bootstrap_partial_readiness(
    *,
    deployment: str,
    result: dict[str, Any],
    source: str,
) -> dict[str, Any]:
    return {
        "deployment": deployment,
        "profile": {"name": "dogfood"},
        "okg_ready": False,
        "status": "partial",
        "partial": True,
        "partial_reason": result.get("error") or "no JSON status returned",
        "source": source,
        "generation_id": None,
        "generation_status": "unknown",
        "source_freshness": {"status": "unknown"},
        "source_sync_status": {"status": "unknown"},
        "source_sync_liveness": {"status": "unknown"},
        "queue_depth": None,
        "dlq_count": None,
        "doctor_status": "not_checked",
        "mcp_graph_read": False,
        "blockers": [
            {
                "id": f"{source}.partial",
                "status": "partial",
                "message": result.get("error") or "status command did not return JSON",
            }
        ],
        "warnings": [],
        "command": result.get("command"),
        "returncode": result.get("returncode"),
        "error": result.get("error"),
        "next_action": {
            "code": "run_dogfood_status_details",
            "owner": "agent",
            "command": (
                "uv run okg dogfood status --deployment "
                f"{shlex.quote(deployment)} --details --json"
            ),
        },
    }


def _bootstrap_readiness_from_dogfood_status(
    *,
    deployment: str,
    result: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    generation = _dict_or_empty(payload.get("generation"))
    queues = _dict_or_empty(payload.get("queues"))
    substrate_queues = _dict_or_empty(queues.get("substrate")) or queues
    doctor = _dict_or_empty(payload.get("doctor"))
    mcp = _dict_or_empty(payload.get("mcp"))
    source_freshness = _dict_or_empty(payload.get("source_freshness"))
    source_sync_status = _dict_or_empty(payload.get("source_sync_status"))
    read_authority = _dict_or_empty(payload.get("read_authority"))
    return {
        "deployment": deployment,
        "profile": {"name": "dogfood"},
        "okg_ready": bool(payload.get("ok")),
        "status": "pass" if payload.get("ok") else "blocker",
        "trust_level": payload.get("trust_level"),
        "generation_id": (
            generation.get("generation_id")
            or generation.get("id")
            or read_authority.get("generation_id")
        ),
        "generation_status": (
            generation.get("status")
            or read_authority.get("generation_status")
        ),
        "source_freshness": source_freshness,
        "source_sync_status": source_sync_status,
        "source_sync_liveness": _dict_or_empty(
            source_sync_status.get("runtime_liveness")
        ),
        "queue_depth": substrate_queues.get("active_queue_depth"),
        "dlq_count": substrate_queues.get("dlq_count"),
        "doctor_status": doctor.get("status") or (
            "pass" if doctor.get("ok") is True else None
        ),
        "mcp_graph_read": (
            bool(read_authority.get("mcp_graph_read"))
            if "mcp_graph_read" in read_authority
            else bool(mcp.get("ok")) if mcp else False
        ),
        "runtime_worker": _dict_or_empty(payload.get("runtime_worker")),
        "workflow_recovery": _dict_or_empty(payload.get("workflow_recovery")),
        "next_action": _dict_or_empty(payload.get("next_action")),
        "runtime_status": envelope.component(
            ok=payload.get("ok"),
            schema=payload.get("schema"),
            trust_level=payload.get("trust_level"),
            read_authority=read_authority,
            recovery_commands=_dict_or_empty(
                payload.get("recovery_commands")
            ),
        ),
        "blockers": _list_or_empty(payload.get("blockers")),
        "warnings": _list_or_empty(payload.get("warnings")),
        "command": result["command"],
        "returncode": result.get("returncode"),
        "error": result.get("error"),
    }


def _bootstrap_coordination(
    root: Path,
    change_id: str,
    task_id: str | None,
    task_title: str | None,
    git_state: dict[str, Any],
) -> dict[str, Any]:
    commands = _coordination_commands(
        change_id,
        task_id,
        task_title,
        git_state,
    )
    try:
        ledger = _bootstrap_pact_coordination_read(root, change_id)
    except Exception as exc:
        return {
            "status": "unavailable",
            "source": "pact_coordination_events ledger",
            "generation": None,
            "health": {},
            "caveats": [],
            "read_model_summary": {},
            "active_work_count": 0,
            "section_counts": {},
            "current_order": None,
            "matching_orders": [],
            "human_decisions": [],
            "claim_state": _coordination_claim_state(
                current_order=None,
                task_id=task_id,
                available=False,
            ),
            "next_command_key": "coordination_status",
            "commands": commands,
            "command": commands["status"],
            "returncode": None,
            "error": str(exc),
        }
    matching_orders = [
        _pact_intent_coordination_order(row, task_id=task_id)
        for row in ledger["intents"]
        if row.get("change_id") == change_id
    ]
    current_order = matching_orders[0] if matching_orders else None
    claim_state = _coordination_claim_state(
        current_order=current_order,
        task_id=task_id,
        available=True,
    )
    return {
        "status": "available",
        "source": "pact_coordination_events ledger",
        "generation": None,
        "health": {"coordination_status": "OK"},
        "caveats": [],
        "read_model_summary": {
            "active_work_count": len(ledger["intents"]),
            "matching_change_count": len(matching_orders),
            "source": "pact_coordination_events",
            "ledger_authority": ledger.get("authority"),
            "target": ledger.get("target"),
        },
        "active_work_count": len(ledger["intents"]),
        "section_counts": {"active_orders": len(matching_orders)},
        "current_order": current_order,
        "matching_orders": matching_orders,
        "human_decisions": [],
        "claim_state": claim_state,
        "next_command_key": claim_state["next_command_key"],
        "commands": commands,
        "command": commands["status"],
        "returncode": 0,
        "error": None,
    }


def _bootstrap_pact_coordination_read(
    root: Path,
    change_id: str,
) -> dict[str, Any]:
    del change_id
    from okg.substrate.pact import decisions as ledger

    resolution = ledger.resolve_ledger_dsn(repo_root=root)
    dsn = resolution.get("dsn")
    if not dsn:
        raise RuntimeError(
            "PACT coordination ledger is unavailable: "
            + str(resolution.get("reason") or "no DSN resolved")
        )
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn) as conn:
        intents = ledger.load_intents(conn)
    return {
        "intents": intents,
        "authority": resolution.get("authority"),
        "target": resolution.get("target"),
    }


def _pact_intent_coordination_order(
    intent: dict[str, Any],
    *,
    task_id: str | None,
) -> dict[str, Any]:
    change_id = str(intent.get("change_id") or "")
    order: dict[str, Any] = {
        "order_id": _coord_order_id(change_id, task_id),
        "change_id": change_id,
        "order_state": intent.get("derived_state") or "in_progress",
        "assignment_state": (
            "stale" if intent.get("claim_expired") else "claimed"
        ),
        "next_action": (
            "refresh_pact_intent"
            if intent.get("claim_expired") else "continue_active_claim"
        ),
        "in_progress": True,
        "stale": bool(intent.get("claim_expired")),
        "branch": intent.get("branch"),
        "head_sha": intent.get("head_sha"),
        "worktree_path": intent.get("worktree_path"),
        "worktree_suffix": intent.get("worktree_suffix"),
        "session_id": intent.get("session_id"),
        "claim_freshness": intent.get("claim_freshness"),
        "last_event_age_seconds": intent.get("last_event_age_seconds"),
        "transcript_watermark_status": intent.get("transcript_watermark_status"),
        "transcript_watermark_age_seconds": intent.get(
            "transcript_watermark_age_seconds"
        ),
        "transcript_watermark_last_reconcile_at": intent.get(
            "transcript_watermark_last_reconcile_at"
        ),
        "transcript_watermarks": intent.get("transcript_watermarks", []),
    }
    if task_id:
        order["task_id"] = task_id
        order["target_tasks"] = [task_id]
    return order


def _bootstrap_timeout_seconds(env_name: str, default: int) -> int:
    raw = os.environ.get(env_name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(value, 300))


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_or_empty(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _coordination_section_counts(sections: dict[str, Any]) -> dict[str, int]:
    return {
        str(name): len(value) if isinstance(value, list) else 0
        for name, value in sections.items()
    }


def _coordination_health_ok(coordination: dict[str, Any]) -> bool:
    if coordination.get("status") != "available":
        return False
    health = _dict_or_empty(coordination.get("health"))
    if not health:
        return True
    status = str(health.get("coordination_status") or "").upper()
    if status:
        return status in {"HEALTHY", "OK", "READY"}
    failure_count = health.get("coordination_failure_count")
    if failure_count is not None:
        try:
            return int(failure_count) == 0
        except (TypeError, ValueError):
            return False
    return True


def _coordination_matching_orders(
    sections: dict[str, Any],
    change_id: str,
    task_id: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for section in ("active_orders", "review_queue", "merge_queue", "blocked"):
        for raw in _list_or_empty(sections.get(section)):
            if not isinstance(raw, dict):
                continue
            if not _coordination_order_matches(raw, change_id, task_id):
                continue
            order = _compact_coordination_order(raw)
            key = str(order.get("order_id") or order)
            if key in seen:
                continue
            seen.add(key)
            rows.append(order)
    return rows


def _coordination_current_order(
    sections: dict[str, Any],
    change_id: str,
    task_id: str | None,
) -> dict[str, Any] | None:
    rows = _coordination_matching_orders(sections, change_id, task_id)
    return rows[0] if rows else None


def _coordination_order_matches(
    order: dict[str, Any],
    change_id: str,
    task_id: str | None,
) -> bool:
    if order.get("change_id") != change_id:
        return False
    if not task_id:
        return True
    if order.get("task_id") == task_id:
        return True
    target_tasks = order.get("target_tasks")
    return isinstance(target_tasks, list) and task_id in target_tasks


def _coordination_human_decisions(
    sections: dict[str, Any],
    change_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_decision(raw: dict[str, Any], decision_type: str) -> None:
        if not _coordination_order_matches(raw, change_id, None):
            return
        order = _compact_coordination_order(raw)
        key = str(order.get("order_id") or order)
        if key in seen:
            return
        seen.add(key)
        order["decision_type"] = decision_type
        rows.append(order)

    for raw in _list_or_empty(sections.get("review_queue")):
        if isinstance(raw, dict):
            add_decision(raw, "review")
    for raw in _list_or_empty(sections.get("merge_queue")):
        if isinstance(raw, dict):
            add_decision(raw, "finalize")
    for raw in _list_or_empty(sections.get("active_orders")):
        if not isinstance(raw, dict):
            continue
        if raw.get("awaiting_review") or raw.get("review_state") in {
            "requested",
            "pending",
            "needed",
        }:
            add_decision(raw, "review")
        elif raw.get("merge_ready") or raw.get("order_state") == "MERGE_READY":
            add_decision(raw, "finalize")
    return rows


def _compact_coordination_order(order: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "order_id",
        "change_id",
        "task_id",
        "target_tasks",
        "order_state",
        "assignment_state",
        "review_state",
        "report_status",
        "next_action",
        "in_progress",
        "awaiting_review",
        "merge_ready",
        "blocked",
        "stale",
        "workspace_observation_fresh",
        "dirty_worktree",
        "tracked_dirty_paths",
        "untracked_paths",
        "proof_ids",
        "gate_result_ids",
        "why_not_merge_ready",
    ]
    return {field: order.get(field) for field in fields if field in order}


def _coordination_claim_state(
    *,
    current_order: dict[str, Any] | None,
    task_id: str | None,
    available: bool,
) -> dict[str, Any]:
    if not available:
        return {
            "state": "coordination_unavailable",
            "next_command_key": "coordination_status",
            "why": "PACT coordination ledger did not return a usable read model",
        }
    if not task_id:
        return {
            "state": "no_active_task",
            "next_command_key": "continue",
            "why": "PACT did not identify an active task",
        }
    if current_order is None:
        return {
            "state": "no_order_recorded",
            "next_command_key": "intent",
            "why": "no PACT coordination intent matches the active task",
        }
    if current_order.get("awaiting_review") or current_order.get("review_state") in {
        "requested",
        "pending",
        "needed",
    }:
        return {
            "state": "review_required",
            "next_command_key": "review",
            "why": "matching coordination order is waiting for review",
        }
    if current_order.get("merge_ready") or current_order.get("order_state") == "MERGE_READY":
        return {
            "state": "merge_ready",
            "next_command_key": "finalize",
            "why": "matching coordination order is merge-ready",
        }
    if current_order.get("assignment_state") in {None, "none", "unclaimed"}:
        return {
            "state": "claim_required",
            "next_command_key": "claim",
            "why": "matching coordination order exists but is unclaimed",
        }
    return {
        "state": "continue_existing_claim",
        "next_command_key": "continue",
        "why": "matching coordination order is already assigned or in progress",
    }


def _coordination_commands(
    change_id: str,
    task_id: str | None,
    task_title: str | None,
    git_state: dict[str, Any],
) -> dict[str, str]:
    del task_title, git_state
    task = task_id or "<task-id>"
    base = "uv run okg pact"
    return {
        "intent": f"{base} gate post-edit {change_id} --json",
        "claim": f"{base} gate post-edit {change_id} --json",
        "review": f"{base} decide {change_id} --verdict approve --json",
        "finalize": f"{base} decide {change_id} --verdict approve --json",
        "status": f"{base} view --format=review-queue --json",
        "task_done": f"{base} gate task-done {change_id} {task} --json",
    }


def _coord_order_id(change_id: str, task_id: str | None) -> str:
    if not task_id:
        return f"{change_id}:<task-id>"
    return f"{change_id}:{task_id}"


def _bootstrap_workspace_lease_id(workspace: Any) -> str | None:
    workspace_text = str(workspace or "")
    if not workspace_text or workspace_text.startswith("<"):
        return None
    resolved = Path(workspace_text).expanduser().resolve()
    digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:16]
    slug = "".join(
        char.lower() if char.isalnum() else "-"
        for char in (resolved.name or "workspace")
    )
    slug = "-".join(part for part in slug.split("-") if part)[:48] or "workspace"
    return f"workspace-lease-{slug}-{digest}"


def _bootstrap_commands(
    change_id: str,
    task_id: str | None,
    task_title: str | None,
    git_state: dict[str, Any],
    *,
    deployment: str | None,
) -> dict[str, str]:
    coord = _coordination_commands(change_id, task_id, task_title, git_state)
    if deployment:
        quoted = shlex.quote(deployment)
        runtime_status = f"uv run okg dogfood status --deployment {quoted} --json"
        deployment_ready = (
            f"uv run okg deployment ready {quoted} --checks dogfood --json"
        )
    else:
        from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

        runtime_status = deployment_ready = PACT_GRAPH_DEPLOYMENT_FIX
    return {
        "start": f"uv run okg pact view {change_id} --format=bootstrap --json",
        "continue": f"uv run okg pact view {change_id} --format=bootstrap --json",
        "runtime_status": runtime_status,
        "graph_ready": runtime_status,
        "deployment_ready": deployment_ready,
        "coordination_status": coord["status"],
        "intent": coord["intent"],
        "claim": coord["claim"],
        "review": coord["review"],
        "finalize": coord["finalize"],
        "task_done": (
            f"uv run okg pact gate task-done {change_id} {task_id or '<task-id>'} --json"
        ),
    }


def _bootstrap_expected_tests(manifest, task) -> list[dict[str, Any]]:
    if not task:
        return []
    task_requirements = set(task.requirements)
    rows = [
        entry.model_dump(mode="json", by_alias=True, exclude_none=True)
        for entry in manifest.test_plan
        if entry.requirement in task_requirements
    ]
    return rows


def _bootstrap_relevant_files(
    manifest,
    expected_tests: list[dict[str, Any]],
) -> list[str]:
    files = [f"pact/changes/{manifest.change.id}/pact.yaml"]
    files.extend(manifest.change.affected_code)
    for test in expected_tests:
        test_id = str(test.get("test_id") or "")
        if "::" in test_id:
            test_id = test_id.split("::", 1)[0]
        if test_id.endswith((".py", ".yaml", ".yml", ".md", ".json")):
            files.append(test_id)
    return _dedupe([path for path in files if path])


def _git_worktree_state(root: Path) -> dict[str, Any]:
    def run_git(args: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(  # noqa: S603 - git is the local source of truth
            ["git", *args],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )

    top = run_git(["rev-parse", "--show-toplevel"])
    if top.returncode != 0:
        return {
            "available": False,
            "worktree_path": str(root),
            "dirty": None,
            "tracked_dirty_paths": [],
            "untracked_paths": [],
            "error": top.stderr.strip() or top.stdout.strip(),
        }
    branch = run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    head = run_git(["rev-parse", "HEAD"])
    status = run_git(["--no-optional-locks", "status", "--porcelain=v1"])
    tracked: list[str] = []
    untracked: list[str] = []
    for line in status.stdout.splitlines():
        if not line:
            continue
        path = line[3:] if len(line) > 3 else line
        if line.startswith("?? "):
            untracked.append(path)
        else:
            tracked.append(path)
    return {
        "available": True,
        "worktree_path": top.stdout.strip(),
        "branch": branch.stdout.strip() if branch.returncode == 0 else None,
        "head": head.stdout.strip() if head.returncode == 0 else None,
        "dirty": bool(tracked or untracked),
        "tracked_dirty_paths": tracked,
        "untracked_paths": untracked,
    }


def _bootstrap_cutover_gate(
    *,
    readiness: dict[str, Any],
    catalog_ownership: dict[str, Any] | None = None,
    skill_freshness: dict[str, Any] | None = None,
    coordination: dict[str, Any],
    status: dict[str, Any],
    active_task: dict[str, Any] | None,
    report_contract: dict[str, Any],
) -> dict[str, Any]:
    source_freshness = readiness.get("source_freshness") or {}
    source_sync_status = readiness.get("source_sync_status") or {}
    source_sync_runtime = (
        source_sync_status.get("runtime_liveness")
        if isinstance(source_sync_status, dict) else {}
    )
    source_sync_runtime = (
        source_sync_runtime if isinstance(source_sync_runtime, dict) else {}
    )
    source_sync_liveness_ok = (
        True
        if not source_sync_status
        else source_sync_runtime.get("ok") is True
    )
    change_id = str(status.get("change") or "<change-id>")
    command = f"uv run okg pact view {change_id} --format=bootstrap --json"
    readiness_deployment = readiness.get("deployment")
    if readiness.get("command"):
        dogfood_status_command = str(readiness.get("command"))
    elif readiness_deployment:
        dogfood_status_command = (
            "uv run okg dogfood status --deployment "
            f"{shlex.quote(str(readiness_deployment))} --json"
        )
    else:
        from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

        dogfood_status_command = PACT_GRAPH_DEPLOYMENT_FIX
    dogfood_ready_command = dogfood_status_command
    runtime_next_action = _dict_or_empty(readiness.get("next_action"))
    runtime_next_code = str(
        runtime_next_action.get("code") or "check_dogfood_runtime_status"
    )
    runtime_next_command = str(
        runtime_next_action.get("command") or dogfood_status_command
    )
    report_completeness = _report_contract_completeness(report_contract)
    human_approval = _human_approval_cutover_check(coordination)
    graph_ready = (
        readiness.get("trust_level") == "ready"
        and readiness.get("generation_status") == "published"
        and readiness.get("mcp_graph_read") is True
    )
    graph_fresh = (
        source_freshness.get("ok") is True
        and source_sync_liveness_ok
        and readiness.get("queue_depth") == 0
        and readiness.get("dlq_count") == 0
    )
    coordination_ok = _coordination_health_ok(coordination)
    file_authority = _dict_or_empty(status.get("file_authority"))
    authority_decision = evaluate_workflow_authority(
        WorkflowAuthorityInput(
            file_fresh=file_authority.get("fresh") is True,
            graph_required=True,
            graph_ready=graph_ready,
            graph_fresh=graph_fresh,
            ledger_fresh=coordination_ok,
            # Stated, not defaulted (issue #1443): the cutover report asks
            # whether the runtime is ready, and reads no change's decision
            # rows. A merge approval is no part of that question.
            ledger_approval=wa.LEDGER_APPROVAL_NOT_JUDGED,
            generation_pin_matches=bool(readiness.get("generation_id")),
            evidence_artifacts_fresh=report_completeness["passed"],
            pact_coordination_agree=coordination_ok,
        )
    )
    checks = []
    if readiness.get("trust_level") is not None:
        checks.append(
            _cutover_check(
                "runtime_authority",
                readiness.get("trust_level") == "ready",
                str(runtime_next_action.get("owner") or "runtime-operator"),
                runtime_next_code,
                runtime_next_command,
                {
                    "trust_level": readiness.get("trust_level"),
                    "runtime_worker": readiness.get("runtime_worker"),
                    "workflow_recovery": readiness.get("workflow_recovery"),
                    "runtime_status": readiness.get("runtime_status"),
                },
            )
        )
    catalog_ownership = _dict_or_empty(catalog_ownership)
    if catalog_ownership.get("recognized") is True:
        recommended = _list_or_empty(catalog_ownership.get("recommended_commands"))
        checks.append(
            _cutover_check(
                "catalog_ownership",
                catalog_ownership.get("ok") is True,
                "graph-operator",
                "diagnose_or_claim_catalog_ownership",
                str(
                    recommended[0]
                    if recommended
                    else catalog_ownership.get("command")
                    or (
                        "uv run okg catalog ownership diagnose --deployment "
                        f"{shlex.quote(str(catalog_ownership.get('deployment_path')))} --json"
                        if catalog_ownership.get("deployment_path")
                        else dogfood_status_command
                    )
                ),
                catalog_ownership,
            )
        )
    skill_freshness = _dict_or_empty(skill_freshness)
    if skill_freshness:
        checks.append(
            _cutover_check(
                "agent_skills",
                skill_freshness.get("ok") is True,
                "implementer",
                "sync_agent_skills",
                str(
                    skill_freshness.get("fix_hint")
                    or "uv run okg pact install --agent codex && uv run okg pact install --agent claude-code"
                ),
                {
                    "status": skill_freshness.get("status"),
                    "missing_count": skill_freshness.get("missing_count"),
                    "stale_count": skill_freshness.get("stale_count"),
                    "orphaned_count": skill_freshness.get("orphaned_count"),
                    "targets": skill_freshness.get("targets"),
                },
            )
        )
    checks.extend([
        _cutover_check(
            "graph",
            readiness.get("generation_status") == "published",
            "graph-operator",
            "publish_or_fix_deployment_generation",
            dogfood_ready_command,
            readiness.get("generation_id"),
        ),
        _cutover_check(
            "source_freshness",
            source_freshness.get("ok") is True,
            "graph-operator",
            "refresh_stale_sources",
            dogfood_ready_command,
            source_freshness,
        ),
        _cutover_check(
            "source_sync_liveness",
            source_sync_liveness_ok,
            "runtime-operator",
            "start_worker_or_refresh_source_sync_producers",
            dogfood_ready_command,
            source_sync_status or readiness.get("source_sync_liveness"),
        ),
        _cutover_check(
            "queue_dlq",
            readiness.get("queue_depth") == 0
            and readiness.get("dlq_count") == 0,
            "runtime-operator",
            "drain_queue_or_clear_dlq",
            dogfood_ready_command,
            {
                "queue_depth": readiness.get("queue_depth"),
                "dlq_count": readiness.get("dlq_count"),
            },
        ),
        _doctor_cutover_check(
            readiness.get("doctor_status"), readiness.get("deployment")
        ),
        _cutover_check(
            "mcp",
            readiness.get("mcp_graph_read") is True,
            "graph-operator",
            "fix_mcp_graph_read",
            dogfood_ready_command,
            readiness.get("mcp_graph_read"),
        ),
        _cutover_check(
            "coordination",
            coordination_ok,
            "coordinator",
            "fix_coordination_status",
            coordination.get("command") or coordination["commands"]["status"],
            {
                "health": coordination.get("health"),
                "caveats": coordination.get("caveats"),
                "read_model_summary": coordination.get("read_model_summary"),
            },
        ),
        _cutover_check(
            "pact_lifecycle",
            bool(active_task) or status.get("agent_guidance", {}).get("state") == "done",
            "implementer",
            "select_or_unblock_pact_task",
            f"uv run okg pact view {change_id} --format=next --json",
            status.get("agent_guidance"),
        ),
        _cutover_check(
            "report_contract",
            bool(report_contract.get("required_fields"))
            and bool(report_contract.get("graph_state_fields")),
            "implementer",
            "fix_report_contract",
            "open docs/runbooks/agents/reporting.md",
            report_contract.get("source"),
        ),
        _cutover_check(
            "report_completeness",
            report_completeness["passed"],
            "implementer",
            "complete_report_contract",
            "open docs/runbooks/agents/reporting.md",
            report_completeness["detail"],
        ),
        _cutover_check(
            "human_approval",
            human_approval["passed"],
            human_approval["owner"],
            human_approval["next_action"],
            human_approval["command"],
            human_approval["detail"],
        ),
    ])
    checks.append(
        _cutover_check(
            "workflow_authority",
            authority_decision.accepted,
            "coordinator",
            "refresh_authority_surfaces",
            command,
            {
                **workflow_authority_payload(authority_decision),
                "file_authority": file_authority,
            },
        )
    )
    blockers = [row for row in checks if row["status"] == "blocker"]
    pending = [row for row in checks if row["status"] not in {"pass", "blocker"}]
    return {
        "ready": all(row["status"] == "pass" for row in checks),
        "command": command,
        "summary": {
            "pass_count": sum(1 for row in checks if row["status"] == "pass"),
            "blocker_count": len(blockers),
            "pending_count": len(pending),
        },
        "checks": checks,
        "blockers": blockers,
    }


def _cutover_check(
    category: str,
    passed: bool,
    owner: str,
    next_action: str,
    command: str,
    detail: Any,
) -> dict[str, Any]:
    return {
        "category": category,
        "status": "pass" if passed else "blocker",
        "owner": owner,
        "next_action": "none" if passed else next_action,
        "command": command,
        "detail": detail,
    }


def _doctor_cutover_check(status: Any, deployment: Any = None) -> dict[str, Any]:
    if deployment:
        command = _doctor_recovery_command(str(deployment))
    else:
        from okg.substrate.pact.project_settings import PACT_GRAPH_DEPLOYMENT_FIX

        command = PACT_GRAPH_DEPLOYMENT_FIX
    normalized = str(status).lower() if status is not None else ""
    if normalized in {"", "none", "not_checked", "skipped"}:
        return {
            "category": "doctor",
            "status": "skipped",
            "owner": "maintainer",
            "next_action": "run_doctor_for_cutover_verification",
            "command": command,
            "detail": status,
        }
    return _cutover_check(
        "doctor",
        normalized in {"pass", "ok"},
        "maintainer",
        "fix_doctor_warnings_or_failures",
        command,
        status,
    )


def _report_contract_completeness(report_contract: dict[str, Any]) -> dict[str, Any]:
    required_fields = set(_list_or_empty(report_contract.get("required_fields")))
    graph_fields = set(_list_or_empty(report_contract.get("graph_state_fields")))
    report_kinds = _dict_or_empty(report_contract.get("report_kinds"))
    closeout_contract = _dict_or_empty(report_kinds.get("closeout"))
    closeout_required = set(
        _list_or_empty(closeout_contract.get("required_fields"))
    )
    subagent_contract = _dict_or_empty(report_kinds.get("subagent-rollup"))
    subagent_required = set(
        _list_or_empty(subagent_contract.get("required_fields"))
    )
    detail = {
        "missing_required_fields": [
            field for field in _REPORT_REQUIRED_FIELDS if field not in required_fields
        ],
        "missing_graph_state_fields": [
            field for field in _REPORT_GRAPH_FIELDS if field not in graph_fields
        ],
        "missing_report_kinds": [
            kind for kind in _REPORT_KINDS if kind not in report_kinds
        ],
        "missing_closeout_fields": [
            field
            for field in _REPORT_CLOSEOUT_REQUIRED_FIELDS
            if field not in closeout_required
        ],
        "missing_subagent_rollup_fields": [
            "subagents"
        ] if "subagents" not in subagent_required else [],
    }
    return {
        "passed": not any(detail.values()),
        "detail": detail,
    }


def _human_approval_cutover_check(coordination: dict[str, Any]) -> dict[str, Any]:
    claim_state = str(
        _dict_or_empty(coordination.get("claim_state")).get("state") or ""
    )
    commands = _dict_or_empty(coordination.get("commands"))
    current_order = coordination.get("current_order")
    human_decisions = _list_or_empty(coordination.get("human_decisions"))
    detail = {
        "claim_state": claim_state or None,
        "current_order": current_order,
        "human_decisions": human_decisions,
    }
    review_decisions = [
        row
        for row in human_decisions
        if isinstance(row, dict) and row.get("decision_type") == "review"
    ]
    finalize_decisions = [
        row
        for row in human_decisions
        if isinstance(row, dict) and row.get("decision_type") == "finalize"
    ]
    if review_decisions:
        return {
            "passed": False,
            "owner": "human-operator",
            "next_action": "record_coordination_review",
            "command": commands.get("review")
            or "uv run okg pact decide <pact-id> --verdict approve --json",
            "detail": {
                **detail,
                "human_decision_required": "review",
            },
        }
    if finalize_decisions:
        return {
            "passed": False,
            "owner": "human-operator",
            "next_action": "finalize_coordination_order",
            "command": commands.get("finalize")
            or "uv run okg pact decide <pact-id> --verdict approve --json",
            "detail": {
                **detail,
                "human_decision_required": "finalize",
            },
        }
    if claim_state == "review_required":
        return {
            "passed": False,
            "owner": "human-operator",
            "next_action": "record_coordination_review",
            "command": commands.get("review")
            or "uv run okg pact decide <pact-id> --verdict approve --json",
            "detail": {
                **detail,
                "human_decision_required": "review",
            },
        }
    if claim_state == "merge_ready":
        return {
            "passed": False,
            "owner": "human-operator",
            "next_action": "finalize_coordination_order",
            "command": commands.get("finalize")
            or "uv run okg pact decide <pact-id> --verdict approve --json",
            "detail": {
                **detail,
                "human_decision_required": "finalize",
            },
        }
    return {
        "passed": True,
        "owner": "human-operator",
        "next_action": "none",
        "command": commands.get("status")
        or "uv run okg pact view --format=review-queue --json",
        "detail": {
            **detail,
            "human_decision_required": False,
        },
    }


def _bootstrap_run_json_command(
    command: list[str],
    *,
    cwd: Path,
    timeout_seconds: int,
) -> dict[str, Any]:
    command_text = " ".join(shlex.quote(part) for part in command)
    try:
        completed = subprocess.run(  # noqa: S603 - bounded repo-local command
            command,
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        return envelope.component(
            ok=False,
            command=command_text,
            returncode=None,
            json=None,
            stdout=exc.stdout,
            stderr=exc.stderr,
            error=f"timed out after {timeout_seconds}s",
        )
    except OSError as exc:
        return envelope.component(
            ok=False,
            command=command_text,
            returncode=None,
            json=None,
            stdout="",
            stderr="",
            error=str(exc),
        )
    parsed = None
    error = None
    try:
        parsed = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        error = f"could not parse JSON output: {exc}"
    return envelope.component(
        ok=completed.returncode == 0 and parsed is not None,
        command=command_text,
        returncode=completed.returncode,
        json=parsed,
        stdout=completed.stdout,
        stderr=completed.stderr,
        error=error,
    )


def _next_tasks(manifest, alignment: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    alignment = alignment or derive_alignment(manifest)
    out = []
    for task in manifest.tasks:
        state = alignment["tasks"].get(task.id, {})
        if task.status == "todo" and not state.get("blockers"):
            out.append({
                "id": task.id,
                "title": task.title,
                "status": task.status,
                "why": "no incomplete blocking dependencies",
                "verification": task.verification,
                "evidence_gaps": state.get("satisfaction_missing", []),
                "suggested": [
                    task.verification,
                    (
                        f"okg pact evidence ingest {manifest.change.id} "
                        "--pytest-report <junit.xml>"
                    ),
                    f"okg pact gate task-done {manifest.change.id} {task.id} --json",
                    f"okg pact view {manifest.change.id} --json",
                ],
            })
    return out


def _agent_guidance(
    *,
    manifest,
    report_ok: bool,
    view: dict[str, Any],
    next_tasks: list[dict[str, Any]],
    blocked_tasks: list[dict[str, Any]],
    evidence_gaps: list[dict[str, Any]],
    environment_blockers: list[dict[str, Any]],
    lifecycle: dict[str, Any],
    derived_state: dict[str, Any],
    normative_v4: bool,
) -> dict[str, Any]:
    if not report_ok:
        return {
            "state": "invalid",
            "next_action": "fix_manifest",
            "next_empty_reason": None if next_tasks else "invalid",
            "why": "PACT validation has errors.",
            "recommended_commands": [
                f"okg pact gate post-edit {manifest.change.id} --json",
            ],
        }
    if not view.get("ok"):
        return {
            "state": "refresh_review",
            "next_action": "render_view",
            "next_empty_reason": None if next_tasks else "stale_review_view",
            "why": "The generated human review view is stale or invalid.",
            "recommended_commands": [
                f"okg pact gate post-edit {manifest.change.id} --json",
            ],
        }
    state = str(derived_state.get("state") or "draft")
    if normative_v4 and state == "draft":
        return {
            "state": "awaiting_admission",
            "next_action": "run_post_edit",
            "next_empty_reason": "lifecycle_draft",
            "why": "The authored PACT has not passed its current post-edit admission.",
            "recommended_commands": [
                f"uv run okg pact gate post-edit {manifest.change.id} --json",
            ],
        }
    if normative_v4 and state in {"proposed", "awaiting_review"}:
        boundary = "proposal" if state == "proposed" else "final review"
        return {
            "state": "awaiting_decision",
            "next_action": "record_decision",
            "next_empty_reason": f"lifecycle_{state}",
            "why": f"The {boundary} boundary requires one digest-bound decision.",
            "recommended_commands": [
                f"okg pact decide {manifest.change.id} --verdict approve --json",
            ],
        }
    if normative_v4 and state in {"retired", "rejected", "superseded"}:
        command = (
            f"okg pact gate pre-merge {manifest.change.id} --json"
            if state == "retired"
            else f"okg pact view {manifest.change.id} --format=status --json"
        )
        return {
            "state": "terminal",
            "next_action": (
                "verify_merge_boundary" if state == "retired" else "inspect_terminal"
            ),
            "next_empty_reason": f"lifecycle_{state}",
            "why": f"The decision ledger derives the change as {state}.",
            "recommended_commands": [command],
        }
    if next_tasks:
        first = next_tasks[0]
        return {
            "state": "ready",
            "next_action": "run_task",
            "next_empty_reason": None,
            "why": f"`{first['id']}` is unblocked.",
            "recommended_commands": first.get("suggested", []),
        }
    if blocked_tasks:
        blocker_ids = sorted({
            str(blocker)
            for task in blocked_tasks
            for blocker in task.get("blockers", [])
        })
        commands = [f"okg pact view {manifest.change.id} --json"]
        for blocker in environment_blockers:
            name = blocker.get("name") or blocker.get("id")
            if name:
                commands.append(f"export {name}=<value>")
        return {
            "state": "blocked",
            "next_action": "unblock",
            "next_empty_reason": "blocked",
            "why": "No task is currently unblocked; blockers are "
            + ", ".join(blocker_ids),
            "recommended_commands": commands,
        }
    if evidence_gaps:
        return {
            "state": "needs_evidence",
            "next_action": "run_verification",
            "next_empty_reason": "missing_evidence",
            "why": "No task is unblocked, but requirements still have evidence gaps.",
            "recommended_commands": [
                f"okg pact view {manifest.change.id} --for-agent --json",
                f"okg pact evidence ingest {manifest.change.id} --pytest-report <junit.xml>",
            ],
        }
    if lifecycle.get("verification") and manifest.change.status != "verification":
        return {
            "state": "advance_lifecycle",
            "next_action": "verify",
            "next_empty_reason": "lifecycle_status_lag",
            "why": (
                "Verification evidence is satisfied, but the change lifecycle "
                f"status is still `{manifest.change.status}`. Run the verification "
                "gate; set change.status by editing pact.yaml directly."
            ),
            "recommended_commands": [
                f"okg pact gate pre-merge {manifest.change.id} --json",
            ],
        }
    return {
        "state": "done",
        "next_action": "none",
        "next_empty_reason": "complete",
        "why": "No unblocked tasks, blockers, stale view, or evidence gaps remain.",
        "recommended_commands": [
            f"okg pact gate pre-merge {manifest.change.id} --json",
        ],
    }


def _blocked_tasks(manifest, alignment: dict[str, Any]) -> list[dict[str, Any]]:
    blocked = []
    for task in manifest.tasks:
        state = alignment["tasks"].get(task.id, {})
        blockers = list(state.get("blockers", []))
        if task.status == "blocked" and not blockers:
            blockers.append(task.blocked_reason or "task.status=blocked")
        if not blockers:
            continue
        blocked.append({
            "id": task.id,
            "title": task.title,
            "status": state.get("status", task.status),
            "blockers": blockers,
            "dependency_blockers": state.get("dependency_blockers", []),
            "environment_blockers": state.get("environment_blockers", []),
            "blocked_reason": task.blocked_reason,
        })
    return blocked


def _environment_blockers(alignment: dict[str, Any]) -> list[dict[str, Any]]:
    blockers = []
    for task_id, state in alignment.get("tasks", {}).items():
        for blocker in state.get("environment_blockers", []):
            blockers.append({"task": task_id, **blocker})
    return blockers


def _task_evidence_issues(alignment: dict[str, Any]) -> list[dict[str, Any]]:
    issues = []
    for task_id, state in alignment.get("tasks", {}).items():
        for issue in state.get("evidence_issues", []):
            issues.append({"task": task_id, "issue": issue})
    return issues


# _load_operations deleted per `complete-pact-surface-cut` task.delete-verbs —
# it parsed `edit plan --op-json/--op-file`; the `edit` verb is gone (edit
# pact.yaml directly). The save_edit_plan / apply_edit_plan operations remain.


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return
    print(yaml.safe_dump(payload, sort_keys=False).rstrip())
