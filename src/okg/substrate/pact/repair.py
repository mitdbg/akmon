"""Executable, state-validated repairs for PACT refusals.

Every refusal's ``next_commands`` is built here, and nothing reaches that list
without passing three checks (`pact-self-repairing-refusals`
req.refusal.executable):

1. it PARSES as an ``okg pact`` (or ``git``) command — prose belongs in
   ``text`` / ``why`` / ``manifest_edit``, never in ``next_commands``;
2. it FITS the repo command guards (at most 500 characters, at most 2
   statement separators). The audit measured ~19% of inline attach attempts
   blocked by those guards and 171 wrapper scripts written to evade them: a
   recommendation that trips the guard is not a repair;
3. it is LEGAL in the change's current state — a recommended ``decide``
   verdict must be in the FSM's legal set here, and when the caller's intended
   transition is illegal the refusal names the ENABLING PATH computed from the
   transition table instead of a verdict the next call will reject.

Not every blocker has an in-band repair. Three shapes are distinguished:

``command``
    an ``okg pact`` invocation clears it;
``manifest_edit``
    the canonical file is the repair surface (``pact.yaml`` is authored, not
    generated) — no verb writes it, so the refusal names the file, the YAML
    pointer, the offending value and the legal vocabulary instead of emitting
    a command that would not shrink anything;
``operator_action``
    nothing in this process can clear it (the ledger DSN is the canonical
    case) — the refusal names an OWNER and a documented out-of-band REMEDY,
    and emits no command. Re-emitting ``evidence sync`` there is the measured
    infinite loop, not a repair.
"""

from __future__ import annotations

import difflib
import shlex
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# --- command guards --------------------------------------------------------
# Mirrored by the benchmark (`bench/commands.py`) and by the repo's own
# PreToolUse hook. Stated once here so a generator cannot drift from the guard
# its output has to survive.
MAX_COMMAND_CHARS = 500
MAX_STATEMENT_SEPARATORS = 2
STATEMENT_SEPARATORS = ("&&", "||", ";")

# `okg <verb>` (not only `okg pact`) is executable: a bootstrap packet may
# legitimately point at `okg dogfood status`. `git` is executable too. Nothing
# else is — an English sentence is guidance, and guidance belongs in `why`.
COMMAND_PREFIXES = (
    ["uv", "run", "okg"],
    ["okg"],
    ["uv", "run", "git"],
    ["git"],
)
# Public name above; the private alias remains for in-module readers.
_COMMAND_PREFIXES = COMMAND_PREFIXES

DEFAULT_REPO_COMMAND = "okg"


def count_statement_separators(command: str) -> int:
    return sum(command.count(token) for token in STATEMENT_SEPARATORS)


def fits_command_guards(command: str) -> bool:
    return (
        len(command) <= MAX_COMMAND_CHARS
        and count_statement_separators(command) <= MAX_STATEMENT_SEPARATORS
    )


def is_executable_command(command: str) -> bool:
    """Does this string parse as a command an agent can paste and run?

    A trailing ``# once the ledger is reachable`` comment is NOT executable as
    emitted — that exact string was the first `next_command` of every deferred
    closure, and it is why the dead-ledger episode looped.
    """
    if not isinstance(command, str):
        return False
    text = command.strip()
    if not text or "#" in text:
        return False
    try:
        argv = shlex.split(text)
    except ValueError:
        return False
    for prefix in _COMMAND_PREFIXES:
        if argv[: len(prefix)] == prefix:
            return len(argv) > len(prefix)
    return False


def executable_commands(candidates: Iterable[Any]) -> list[str]:
    """Order-preserving filter: only guard-fitting, parseable commands survive.

    This is the one funnel every emitter goes through, so a prose string added
    to a recommendation list in some future edit is DROPPED rather than shipped
    as a repair.
    """
    kept: dict[str, None] = {}
    for candidate in candidates or ():
        if not isinstance(candidate, str):
            continue
        command = candidate.strip()
        if not command or not is_executable_command(command):
            continue
        if not fits_command_guards(command):
            continue
        kept.setdefault(command, None)
    return list(kept)


def repo_command(repo_root: Path | None) -> str:
    """The repo-authoritative command path (`pact/project.yaml`).

    This repo declares ``uv run okg``; a bare global ``okg`` may be stale, so
    an emitted command that ignores the declaration is not copy-pasteable.
    """
    if repo_root is None:
        return DEFAULT_REPO_COMMAND
    try:
        from okg.substrate.pact.project_settings import load_project_settings

        settings = load_project_settings(Path(repo_root))
    except Exception:  # noqa: BLE001 - unreadable settings must not break a refusal
        return DEFAULT_REPO_COMMAND
    authority = settings.get("command_authority")
    if isinstance(authority, dict):
        declared = authority.get("repo_command")
        if isinstance(declared, str) and declared.strip():
            return declared.strip()
    return DEFAULT_REPO_COMMAND


# --- near-miss candidates --------------------------------------------------


NEAR_MISS_CUTOFF = 0.55

# How the candidate list was produced. A CORRECTED RE-INVOCATION may only be
# built on `near_miss`: the `listing` arm is a courtesy print of the legal set
# for a totally unrelated id, and treating its first element as "what you meant"
# is a guess dressed as a repair — the same defect as `legal_values[0]`.
CANDIDATE_NEAR_MISS = "near_miss"
CANDIDATE_LISTING = "listing"
CANDIDATE_NONE = "none"


def near_miss_candidates(
    value: str,
    candidates: Iterable[str],
    *,
    limit: int = 3,
) -> tuple[list[str], str]:
    """Close matches for an unknown id, AND how they were found.

    An unknown-id refusal that does not list candidates costs a `view` call to
    learn the legal ids; the audit measured that round trip on task ids,
    requirement ids, evidence refs, and change ids alike. The SOURCE travels
    with the list because the two arms mean different things: a difflib hit is
    a correction, a truncated listing is a menu.
    """
    pool = [str(candidate) for candidate in candidates if candidate]
    if not pool:
        return [], CANDIDATE_NONE
    close = difflib.get_close_matches(
        str(value), pool, n=limit, cutoff=NEAR_MISS_CUTOFF
    )
    if close:
        return close, CANDIDATE_NEAR_MISS
    # A totally unrelated id still deserves the legal set when it is small
    # enough to print; "no suggestions" is the shape that sent agents grepping.
    return sorted(pool)[:limit], CANDIDATE_LISTING


def near_misses(value: str, candidates: Iterable[str], *, limit: int = 3) -> list[str]:
    """The candidate list alone. Prefer :func:`near_miss_candidates`."""
    return near_miss_candidates(value, candidates, limit=limit)[0]


# --- proof slots -----------------------------------------------------------


@dataclass(frozen=True)
class ProofSlot:
    """One entry of ``missing_proof``, parsed into its addressable parts.

    The composite form ``req.X:acceptance:acc.Y`` names a link no documented
    command created before this change; it is now the attach TARGET.
    """

    token: str
    requirement: str | None = None
    kind: str | None = None
    acceptance: str | None = None
    note: str | None = None

    @property
    def target(self) -> str | None:
        """What `evidence attach` must be pointed at to fill this slot."""
        return self.acceptance or self.requirement


_NON_KIND_NOTES = frozenset({"stale_evidence"})
# Notes that NO `evidence attach` can clear, so they emit no command at all.
# A refutation is not an empty proof slot: the requirement carries a row whose
# recorded outcome contradicts it, and attaching another row leaves that row
# exactly where it is. The repair is to fix what the row measured and re-run
# it, or to correct the row's status — neither of which is an attach, and
# emitting one would recommend a command that runs cleanly and moves nothing
# (`pact-evidence-policy-blindness`).
_UNFILLABLE_NOTES = frozenset({"refuted_by"})


def parse_proof_slot(token: str) -> ProofSlot:
    text = str(token)
    requirement, separator, rest = text.partition(":")
    if not separator:
        return ProofSlot(token=text, requirement=None, note=text)
    if any(rest.startswith(f"{note}:") for note in _UNFILLABLE_NOTES):
        return ProofSlot(
            token=text,
            requirement=requirement,
            note=rest.partition(":")[0],
        )
    if rest.startswith("acceptance:"):
        return ProofSlot(
            token=text,
            requirement=requirement,
            acceptance=rest.split(":", 1)[1] or None,
        )
    if rest.startswith("any:"):
        # `required_any` is an OR: the first listed kind is a sufficient repair.
        options = [part for part in rest[4:].split("|") if part]
        return ProofSlot(
            token=text,
            requirement=requirement,
            kind=options[0] if options else None,
        )
    if rest in _NON_KIND_NOTES:
        return ProofSlot(token=text, requirement=requirement, note=rest)
    return ProofSlot(token=text, requirement=requirement, kind=rest)


def _requirement_by_id(manifest: Any, requirement_id: str | None) -> Any | None:
    if requirement_id is None:
        return None
    for requirement in manifest.requirements:
        if requirement.id == requirement_id:
            return requirement
    return None


def _policy_kind(manifest: Any, requirement_id: str | None) -> str:
    """The kind an acceptance slot should be proven with.

    Drawn from the requirement's OWN evidence policy, never guessed: a slot
    filled with a kind the policy does not demand proves nothing.
    """
    requirement = _requirement_by_id(manifest, requirement_id)
    if requirement is not None:
        for kind in (
            *requirement.evidence_policy.required,
            *requirement.evidence_policy.required_any,
        ):
            if kind and kind != "planned_test":
                return str(kind)
    return "executed_test"


PLACEHOLDER_ARTIFACT = "TODO-evidence-artifact-path"


def artifact_for(manifest: Any, slot: ProofSlot) -> str:
    """The artifact THIS slot's requirement is proven with, or a placeholder.

    Read off the manifest's own test plan, matched on the slot's requirement —
    an acceptance id is resolved back to the requirement that owns it, because
    a plan row is keyed by requirement (or by criterion) and a bare acceptance
    id matched neither.

    There is deliberately NO fallback to "the first test in the plan". That
    borrowed another requirement's artifact and emitted it as this slot's
    proof: an attach command that runs cleanly and records a row whose artifact
    never covered the thing it now claims to prove. Fabricated proof through
    the repair path is the defect this change exists to close
    (req.refusal.executable), so an unresolvable artifact is named as a
    PLACEHOLDER the author must fill.
    """
    wanted = {
        value
        for value in (
            slot.requirement,
            slot.acceptance,
            _requirement_of_acceptance(manifest, slot.acceptance)
            if slot.acceptance
            else None,
        )
        if value is not None
    }
    for test in manifest.test_plan:
        if test.requirement in wanted and test.test_id:
            return str(test.test_id)
    return PLACEHOLDER_ARTIFACT


def attach_command(
    prefix: str,
    change_id: str,
    target: str,
    *,
    kind: str,
    ref: str,
) -> str:
    return (
        f"{prefix} pact evidence attach {shlex.quote(change_id)} "
        f"{shlex.quote(target)} --kind {shlex.quote(kind)} "
        f"--ref {shlex.quote(ref)} --status pass --json"
    )


def unlinked_acceptance_ids(manifest: Any, requirement_id: str | None = None) -> list[str]:
    """Acceptance criteria carrying no proof link.

    These are the subject of `missing_acceptance_evidence`, and — since
    `evidence attach` against a criterion id now writes the link — they are
    command-repairable, not manifest-edit-only.
    """
    ids: list[str] = []
    for requirement in manifest.requirements:
        if requirement_id is not None and requirement.id != requirement_id:
            continue
        for criterion in requirement.acceptance:
            if not criterion.evidence:
                ids.append(criterion.id)
    return ids


def proof_slot_commands(
    manifest: Any,
    change_id: str,
    slots: Iterable[str],
    *,
    subject_requirements: Iterable[str],
    prefix: str = DEFAULT_REPO_COMMAND,
) -> list[str]:
    """The exact attach invocation for every fillable proof slot.

    Includes the acceptance-id form: ``req.X:acceptance:acc.Y`` attaches
    against ``acc.Y``, which links the row into that criterion's proof list.

    When a requirement's slot could be filled through one of its own UNLINKED
    acceptance criteria, the criterion wins as the attach target: the row then
    satisfies the requirement policy AND writes the missing proof link, so one
    command clears both blockers instead of leaving the second for a round
    trip the audit measured at 28 calls.

    ``subject_requirements`` is the REFUSAL'S NAMED SCOPE — the refused task's
    linked requirements, or the refused row's own requirement. Nothing outside
    it may be attached to. Before this, the trailing sweep ran over every
    unlinked criterion in the WHOLE manifest, so refusing `task.one` (linked
    only to `req.one`) emitted an attach against `acc.two`, a criterion of a
    requirement that task does not prove. A repair that writes proof for
    someone else's requirement is fabricated closure, not a repair.

    It is REQUIRED, with no default: an omitted scope would silently mean "the
    whole manifest", which is precisely the defect — and a default that
    restores a defect is worse than no default at all.
    """
    scope = {str(value) for value in subject_requirements}

    def _in_scope(requirement_id: str | None) -> bool:
        return requirement_id is not None and requirement_id in scope

    commands: list[str] = []
    for token in slots or ():
        slot = parse_proof_slot(token)
        if slot.note in _UNFILLABLE_NOTES:
            continue
        # An acceptance slot's scope is the requirement that OWNS the
        # criterion, resolved through the manifest — never the bare id.
        owner = slot.requirement or _requirement_of_acceptance(manifest, slot.acceptance)
        if not _in_scope(owner):
            continue
        target = slot.target
        if slot.acceptance is None and slot.requirement is not None:
            unlinked = unlinked_acceptance_ids(manifest, slot.requirement)
            if unlinked:
                target = unlinked[0]
        if target is None:
            continue
        kind = slot.kind or _policy_kind(manifest, owner)
        if slot.note in _NON_KIND_NOTES:
            # Stale proof: the repair is a FRESH row of the same kind, so the
            # policy kind is the right one to re-attach.
            kind = _policy_kind(manifest, owner)
        commands.append(
            attach_command(
                prefix,
                change_id,
                target,
                kind=kind,
                ref=artifact_for(manifest, slot),
            )
        )
    # Criteria no slot covered still need their proof link, and the command
    # that writes it is the same one — but only within the refusal's subject.
    covered = {parse_proof_slot(token).acceptance for token in slots or ()}
    for requirement in manifest.requirements:
        if not _in_scope(requirement.id):
            continue
        for acceptance_id in unlinked_acceptance_ids(manifest, requirement.id):
            if acceptance_id in covered:
                continue
            slot = ProofSlot(
                token=acceptance_id,
                requirement=requirement.id,
                acceptance=acceptance_id,
            )
            commands.append(
                attach_command(
                    prefix,
                    change_id,
                    acceptance_id,
                    kind=_policy_kind(manifest, requirement.id),
                    ref=artifact_for(manifest, slot),
                )
            )
    return executable_commands(commands)


def _requirement_of_acceptance(manifest: Any, acceptance_id: str) -> str | None:
    for requirement in manifest.requirements:
        for criterion in requirement.acceptance:
            if criterion.id == acceptance_id:
                return requirement.id
    return None


# --- FSM enabling paths ----------------------------------------------------


def enabling_commands(
    path: Any,
    change_id: str,
    *,
    prefix: str = DEFAULT_REPO_COMMAND,
    open_tasks: Iterable[str] = (),
    target: str | None = None,
) -> list[str]:
    """The commands that walk an :class:`fsm.EnablingPath`, in order.

    ``tasks_terminal`` expands to one ``gate task-done`` per task that is not
    yet terminal — the exact commands the audited "approve in in_progress"
    refusal should have named instead of re-recommending ``approve``.
    """
    if path is None or not getattr(path, "reachable", False):
        return []
    commands: list[str] = []
    for event in getattr(path, "events", ()):  # noqa: B007 - explicit event walk
        if event == "gate_post_edit_pass":
            commands.append(f"{prefix} pact gate post-edit {shlex.quote(change_id)} --json")
        elif event == "tasks_terminal":
            for task_id in open_tasks:
                commands.append(
                    f"{prefix} pact gate task-done {shlex.quote(change_id)} "
                    f"{shlex.quote(str(task_id))} --json"
                )
        elif event == "evidence_stale":
            # Not an action a caller takes: the machine moves itself when
            # proof goes stale. Emitting a command here would be fiction.
            continue
        else:
            command = (
                f"{prefix} pact decide {shlex.quote(change_id)} "
                f"--verdict {shlex.quote(event)}"
            )
            if event == "supersede" and target:
                command += f" --target {shlex.quote(target)}"
            commands.append(command + " --json")
    return executable_commands(commands)


def legal_decide_commands(
    change_id: str,
    verdicts: Iterable[str],
    *,
    prefix: str = DEFAULT_REPO_COMMAND,
    target: str | None = None,
) -> list[str]:
    """`decide` invocations the FSM will accept in the current state.

    `gate pre-merge` used to recommend ``--verdict approve`` unconditionally;
    it fired nine times in states where the next call refused it. A gate may
    only recommend from this set.
    """
    commands: list[str] = []
    for verdict in verdicts or ():
        command = (
            f"{prefix} pact decide {shlex.quote(change_id)} "
            f"--verdict {shlex.quote(str(verdict))}"
        )
        if verdict == "supersede":
            if not target:
                # `supersede` without --target trips a different guard; a
                # recommendation the next call rejects is not a repair.
                continue
            command += f" --target {shlex.quote(target)}"
        if verdict == "escalate":
            # `escalate` needs a recipient the gate cannot invent.
            continue
        commands.append(command + " --json")
    return executable_commands(commands)


# --- manifest-edit repairs -------------------------------------------------
# Validation codes whose repair surface is the canonical file. `pact.yaml` is
# authored, not generated, and no `okg pact` verb rewrites these fields — so
# the refusal names the edit precisely instead of emitting a command that
# would run cleanly and change nothing.

_EDIT_ACTIONS: dict[str, str] = {
    "missing_evidence_policy": (
        "declare the kinds this requirement must be proven with"
    ),
    "unknown_evidence_ref": "point the evidence row at an id this manifest declares",
    "unknown_requirement_ref": "link the task to a requirement this manifest declares",
    "task_requirements_link": "link the task to at least one requirement",
    # The LINK arm of `task_done_missing_evidence` (path `tasks.<id>.requirements`):
    # a done task that resolves to no obligation at all. It is the same edit as
    # `task_requirements_link` and had no descriptor, so the one code whose
    # subject-deletion arm is mechanically repairable was published as
    # unrepairable. The ALIGNMENT arm (path `tasks.<id>`) is NOT here: its
    # repair is an `evidence attach`, which is a command, not an edit.
    "task_done_missing_evidence": "link the task to at least one requirement",
    "unknown_evidence_kind": "use a kind the evidence-kind registry knows",
    "missing_task_verification": "state the check that proves this task",
    "stale_test_reference": "point the test at a requirement this manifest declares",
    "unknown_acceptance_evidence": "name a proof token the registry knows",
    "missing_scenario": "add a scenario, or declare an evidence policy",
    "missing_acceptance": "add an acceptance criterion, or declare an evidence policy",
}

# Codes whose repair surface is the canonical file, published so a classifier
# can ask "is this an author action?" without restating the list.
EDIT_REPAIRABLE_CODES = frozenset(_EDIT_ACTIONS)


_NO_VALUE = object()


@dataclass
class ManifestEdit:
    """One mechanically-locatable edit, complete enough to apply blind."""

    code: str
    path: str
    pointer: str
    action: str
    message: str
    offending_value: Any = _NO_VALUE
    legal_values: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "path": self.path,
            "pointer": self.pointer,
            "action": self.action,
            "message": self.message,
        }
        # `is not _NO_VALUE`, never truthiness: for the subject-deletion codes
        # the offending value IS the empty list, and dropping it would leave
        # the applier unable to tell "no value named" from "the value is []".
        if self.offending_value is not _NO_VALUE:
            payload["offending_value"] = self.offending_value
        if self.legal_values:
            payload["legal_values"] = self.legal_values
        return payload


def manifest_edits(
    repo_root: Path,
    change_id: str,
    issues: Iterable[Any],
    *,
    legal_values: dict[str, list[str]] | None = None,
    resolve: Any = None,
) -> list[dict[str, Any]]:
    """Turn validation errors into edit instructions for the canonical file.

    ``resolve(row) -> dict`` is the manifest-aware narrowing hook (supplied by
    ``operations``, which may read the models this module deliberately does
    not): it may return a ``pointer`` naming the exact list or scalar to edit
    and a ``legal_values`` set narrowed to what is applicable HERE. Without a
    determinate descriptor a blind applier has to guess, and a guess is what
    ``legal_values[0]`` was.
    """
    path = str(Path(repo_root) / "pact" / "changes" / change_id / "pact.yaml")
    vocabulary = legal_values or {}
    edits: list[dict[str, Any]] = []
    # One EDIT per (pointer, action), not one per code. Two codes routinely
    # name the same repair — an unlinked task is both `task_requirements_link`
    # and the link arm of `task_done_missing_evidence` — and emitting two
    # descriptors for one edit makes the second unapplicable the moment the
    # first lands, which reads as a failed repair. The extra codes are recorded
    # on the descriptor as `also_clears` so nothing is lost.
    by_edit: dict[tuple[str, str], dict[str, Any]] = {}
    for issue in issues or ():
        row = issue if isinstance(issue, dict) else issue.to_dict()
        if row.get("severity") not in (None, "error"):
            continue
        code = str(row.get("code") or "")
        action = _EDIT_ACTIONS.get(code)
        if action is None:
            continue
        resolution = dict(resolve(row) or {}) if resolve is not None else {}
        if resolution.get("skip"):
            # The manifest-aware pass says this arm of the code is not an
            # edit (the alignment arm of `task_done_missing_evidence` is
            # repaired by an attach). Silence here beats a descriptor whose
            # application would change nothing.
            continue
        offending = (
            row["offending_value"] if "offending_value" in row else _NO_VALUE
        )
        pointer = str(resolution.get("pointer") or row.get("path") or "")
        resolved_action = str(resolution.get("action") or action)
        existing = by_edit.get((pointer, resolved_action))
        if existing is not None:
            also = existing.setdefault("also_clears", [])
            if code != existing["code"] and code not in also:
                also.append(code)
            continue
        edit = ManifestEdit(
            code=code,
            path=path,
            pointer=pointer,
            action=resolved_action,
            message=str(row.get("message") or ""),
            offending_value=offending,
            legal_values=list(
                resolution.get("legal_values")
                if resolution.get("legal_values") is not None
                else (vocabulary.get(code) or [])
            ),
        ).to_dict()
        by_edit[(pointer, resolved_action)] = edit
        edits.append(edit)
    return edits


# --- operator-action remedies ----------------------------------------------
# The ledger-DSN blocker is the design's counterexample to a flat 100%
# self-repair bar: no `okg pact` command clears it. Re-emitting `evidence
# sync` as the repair is the measured infinite loop (the audited dead-ledger
# transcript), so the remedy is carried as TEXT with a named owner, and
# `next_commands` stays empty.

LEDGER_BLOCKERS = frozenset({"ledger_dsn_not_configured", "ledger_not_fresh"})

# The merge boundary's OTHER ledger causes, and deliberately not among the
# above: the ledger ANSWERED, so nothing about the database is broken. Routing
# them through `ledger_remedy` published an owner-and-prose remedy naming a
# deployment connection that was working, with an empty `next_commands`
# (issue #1443).
#
# `no_ledger_approval` — no approval binds the current intent digest (never
# recorded, superseded, or stranded by a later manifest edit). A NEW decision
# clears it, so `okg pact decide <id> --verdict <legal verdict>` is a genuine
# repair; `legal_decide_commands` builds it from the FSM-legal set, so the gate
# never recommends a verdict the next call rejects.
NO_LEDGER_APPROVAL_BLOCKER = "no_ledger_approval"

# `approval_not_archived` — an approval DOES bind the current digest and the
# change is simply not archived yet. No decide verdict clears it: the FSM-legal
# verdict in that state is `defer`, which records a defer and leaves the gate
# refusing identically. That is the inert-command loop, so this token is
# deliberately absent from COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS and the
# refusal carries the archive step as a live check instead.
APPROVAL_NOT_ARCHIVED_BLOCKER = "approval_not_archived"

# Authority blockers whose repair is an `okg pact` command rather than an
# out-of-band operator action or a repository-state fix. Read by
# `cli._pact_command`'s pre-merge arm, which recommends the decide plan only
# when one of these is actually present — so the membership is enforced by the
# emitted `next_commands`, not merely asserted here.
COMMAND_REPAIRABLE_AUTHORITY_BLOCKERS = frozenset({NO_LEDGER_APPROVAL_BLOCKER})

OPERATOR_OWNER_DEFAULT = "repo operator (deployment owner)"


REMEDY_FROM_CAUSE = "cause_fix_hint"
REMEDY_CAUSE_UNKNOWN = "cause_unknown"


def ledger_remedy(ledger_authority: dict[str, Any] | None) -> dict[str, Any]:
    """Owner + documented out-of-band remedy for an unreachable ledger.

    The remedy is DERIVED FROM THE CAUSE the ledger connection reported, never
    invented. This used to emit ``export OKG_DSN=<deployment dsn>`` whenever no
    fix hint was carried — which is the fix for `no_dsn_configured` and the
    WRONG fix for `deployment_not_configured` (where no deployment is resolved
    at all, so there is no DSN to export). Two verbs in the same shell then
    disagreed about the remedy, because one of them passed the real authority
    and the other passed a bare reason string.

    When the cause is genuinely unknown the remedy SAYS SO. A named unknown is
    actionable — it tells the operator to look — while a defaulted instruction
    is a false lead.
    """
    authority = ledger_authority or {}
    deployment = authority.get("deployment") or "<deployment>"
    reason = authority.get("reason") or "the PACT decision ledger is unreachable"
    cause = authority.get("cause")
    hint = authority.get("fix_hint")
    if isinstance(hint, str) and hint.strip():
        remedy = hint.strip()
        source = REMEDY_FROM_CAUSE
    else:
        named = f" (cause: {cause})" if cause else ""
        remedy = (
            "the decision ledger is unreachable and this refusal carries no "
            f"documented fix for it{named}; the deployment owner must diagnose "
            f"the {deployment} ledger connection before this can be recorded"
        )
        source = REMEDY_CAUSE_UNKNOWN
    return {
        "owner": OPERATOR_OWNER_DEFAULT,
        "deployment": deployment,
        "remedy": remedy,
        "reason": str(reason),
        "cause": cause,
        # Which arm produced the remedy, so a reader can tell a documented fix
        # from a named unknown without string-matching the prose.
        "remedy_source": source,
        # Stated so no reader has to rediscover it by looping: the remedy is
        # NOT an `okg pact` command, which is exactly why it is not emitted as
        # one.
        "in_band_repair": False,
    }


def is_operator_action_only(blockers: Iterable[str]) -> bool:
    """True only when EVERY cause is out-of-band for this process.

    `no_ledger_approval` is not one of them: it is repaired in band by
    `okg pact decide`, so a refusal carrying it is never scored or presented as
    an unrepairable ledger fault (issue #1443).
    """
    tokens = {str(token) for token in blockers or ()}
    return bool(tokens) and tokens <= LEDGER_BLOCKERS
