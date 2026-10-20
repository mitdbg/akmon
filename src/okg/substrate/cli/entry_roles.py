"""The entry surface: which command a newcomer types first.

Per PACT `define-newcomer-entry-path`. A newcomer's first decision is
which command starts the system, and for a long time this repository
answered that several ways with nothing observing the growth.

**Layering.** This module is the canonical role map because the CLI needs
it at runtime, including in an installed package where `tests/` does not
ship. `tests/substrate/cli/CLI_SMOKE_INDEX.yaml` carries the same roles as
the gated mirror, and `test_cli_surface_coverage.py` asserts the two agree —
so this is one registry with a test-side mirror, not a second opinion.
Change a role here and the gate fails until the mirror follows.

**Roles.**

``newcomer_bring_up``
    The one documented way in. Exactly one verb.
``superseded_entry``
    Also begins a bring-up but is not the documented door. Transitional:
    `req.entry.migrate-the-other-doors` retires each to alias, frozen, or
    deleted. The count is a ratchet — it may fall, never rise.
``alias``
    Delegates to the door and reaches the same end state.
``frozen``
    Still executes, but emits a migration notice naming its replacement.
``lifecycle``
    Operates an EXISTING deployment. Deliberately outside the
    alias/frozen/delete trichotomy: `start` is the only day-2 capability
    the newcomer path has — added by `dead-simple-install` because "after
    reboot, the container stops" — and freezing it would delete the
    reboot story.
``not_entry``
    Cannot begin a bring-up.

**What counts as a door.** A door is a verb a newcomer plausibly types
FIRST to obtain a working graph — NOT merely any verb that can result in
one. That distinction matters, because the loose reading admits half the
CLI and makes the role meaningless. Under it:

- ``install`` and ``init`` were both doors. Both took a newcomer from
  nothing to a scaffolded, publishable deployment, and each was named as
  step one by documentation a newcomer reads. ``install`` is now the one
  door; ``init`` delegates to it (``okg init <args>`` runs
  ``okg install <args> --scaffold-only``) and is classified ``alias``.
- ``fetch-deployments`` is NOT. It clones the reference-deployment cache
  and creates no deployment; it is a prerequisite step, and now that
  ``install --profile`` routes reference deployments its error message
  names it at the moment it is needed.
- ``snapshot-load`` is NOT. It can publish a first generation from a JSON
  file into a bare database, but a newcomer has no snapshot — it is an
  import/restore capability.
- ``deployment`` is NOT. Its door is three levels down at
  ``deployment bootstrap apply``, a deliberate authoring workflow (plan →
  draft → verify → apply) for someone designing a new deployment, not a
  first command.
- ``provision`` is NOT. Against a deployment that does not exist it exits
  ``deployment dir for 'X' not found``, so it presumes a scaffold.

Classification is grounded in probes, not in a verb's name or help text.
Each of the five above was checked by running it; three of them were
misclassified on an earlier reading of their help strings alone.
"""
from __future__ import annotations

NEWCOMER_BRING_UP = "newcomer_bring_up"
SUPERSEDED_ENTRY = "superseded_entry"
ALIAS = "alias"
FROZEN = "frozen"
LIFECYCLE = "lifecycle"
NOT_ENTRY = "not_entry"

VALID_ROLES = frozenset({
    NEWCOMER_BRING_UP,
    SUPERSEDED_ENTRY,
    ALIAS,
    FROZEN,
    LIFECYCLE,
    NOT_ENTRY,
})

#: Verbs that are part of the entry surface. Everything absent is
#: ``not_entry``; absence here is not a default the gate accepts, because
#: `test_cli_surface_coverage.py` requires an explicit role in the mirror
#: for every top-level verb argparse ships.
ENTRY_ROLES: dict[str, str] = {
    "install": NEWCOMER_BRING_UP,
    # The one genuine competing door, retired by
    # `req.entry.migrate-the-other-doors`: it now delegates to `install`
    # rather than bringing a deployment up on its own, so scripts and
    # runbooks that call it keep working and reach the same end state.
    "init": ALIAS,
    "start": LIFECYCLE,
    "stop": LIFECYCLE,
    "restart": LIFECYCLE,
    "up": LIFECYCLE,
    "reset": LIFECYCLE,
}

#: For ``alias`` verbs: the door they delegate to.
ALIAS_TARGETS: dict[str, str] = {
    # `okg init <args>` runs `okg install <args> --scaffold-only`.
    "init": "install",
}

#: For ``frozen`` verbs: the verb that replaces them, and why.
FROZEN_REPLACEMENTS: dict[str, tuple[str, str]] = {}


#: Help visibility, a separate axis from the entry role. ``shown`` verbs
#: appear in the default ``okg --help`` listing; ``hidden`` verbs still
#: parse, dispatch and answer ``okg <verb> --help`` exactly as before, but
#: are left out of the default listing and named in one epilog line.
#: Per okg#3109 slice 0 (decision 1b): only the internal verbs that serve
#: this repository's own agents, CI and benchmarks are hidden; no path moves.
SHOWN = "shown"
HIDDEN = "hidden"

#: ``retired`` verbs are also left out of the listing, but not named in the
#: epilog: running one refuses with ``command_retired`` and names what
#: replaces it, and writes nothing.
RETIRED = "retired"

#: ``unlisted`` verbs leave the listing and the usage choice list and are
#: not named in the epilog either, yet they parse, run and answer
#: ``okg <verb> --help`` exactly as before. They are commands this release
#: does not support for the public (issue #3416), not internal tooling.
UNLISTED = "unlisted"

VALID_VISIBILITIES = frozenset({SHOWN, HIDDEN, RETIRED, UNLISTED})

#: Verbs hidden from the default help listing. Everything absent is
#: ``shown``. `CLI_SMOKE_INDEX.yaml` mirrors this with `visibility: hidden`
#: and `test_cli_surface_coverage.py` asserts the two agree.
HIDDEN_FROM_DEFAULT_HELP: frozenset[str] = frozenset({
    "agent-ssh-run",
    "agent-ssh-client",
    "agent-runtime-launch",
    "dogfood",
    "pact",
    "bench",
    "environment",
    "external-action",
})


#: Renamed top-level verbs: ``{old: new}``. The old verb still parses and
#: dispatches to the same handler for one release, prints the rename warning
#: on stderr, and is left out of the default help listing and the usage
#: choice list. Unlike ``HIDDEN_FROM_DEFAULT_HELP`` it is NOT named in the
#: "internal commands" epilog: it is retired, not internal. Decision 310.
#: `CLI_SMOKE_INDEX.yaml` mirrors this with `renamed_to:`.
RENAMED_VERBS: dict[str, str] = {
    "profile": "perf",
}


#: Retired verbs. `CLI_SMOKE_INDEX.yaml` mirrors this with
#: `visibility: retired`. `new-source` wrote a reader skeleton that was not a
#: working reader (issue #2479); authors copy the example connector instead.
RETIRED_VERBS: frozenset[str] = frozenset({"new-source"})


#: Unlisted verbs and why. `CLI_SMOKE_INDEX.yaml` mirrors this with
#: `visibility: unlisted`. Federation is not supported in the public release
#: (Jason, 2026-10-05); every top-level verb the federation and Poros modules
#: register must be here, and `test_cli_surface_coverage.py` enforces that.
_FEDERATION_UNSUPPORTED = "federation is not supported in the public release"
UNLISTED_VERBS: dict[str, str] = {
    "connect": _FEDERATION_UNSUPPORTED,
    "send": _FEDERATION_UNSUPPORTED,
    "pull": _FEDERATION_UNSUPPORTED,
    "inbox": _FEDERATION_UNSUPPORTED,
    "proof": _FEDERATION_UNSUPPORTED,
    "federation": _FEDERATION_UNSUPPORTED,
    "poros": _FEDERATION_UNSUPPORTED,
}


def visibility_of(verb: str) -> str:
    """Help visibility for a top-level verb; verbs in no map are ``shown``."""
    if verb in RETIRED_VERBS:
        return RETIRED
    if verb in UNLISTED_VERBS:
        return UNLISTED
    return HIDDEN if verb in HIDDEN_FROM_DEFAULT_HELP else SHOWN


def role_of(verb: str) -> str:
    """Role for a top-level verb; unlisted verbs are ``not_entry``."""
    return ENTRY_ROLES.get(verb, NOT_ENTRY)


def newcomer_door() -> str:
    """The single documented way in.

    Raises if the map does not name exactly one, so a second door is a
    loud failure at import rather than a quietly ambiguous CLI.
    """
    doors = sorted(
        v for v, r in ENTRY_ROLES.items() if r == NEWCOMER_BRING_UP
    )
    if len(doors) != 1:
        raise RuntimeError(
            f"expected exactly one {NEWCOMER_BRING_UP} verb, found {doors}; "
            "a second door must be an alias or frozen"
        )
    return doors[0]


def help_prefix(verb: str) -> str:
    """Marker prepended to a verb's help text in ``okg --help``.

    Gives a reader scanning 58 verbs a way to find the door, and makes a
    frozen verb announce itself at the point of listing rather than only
    when run.
    """
    role = role_of(verb)
    if role == NEWCOMER_BRING_UP:
        return "[START HERE] "
    if role == FROZEN:
        replacement = FROZEN_REPLACEMENTS.get(verb, ("", ""))[0]
        return f"[frozen; use `okg {replacement}`] " if replacement else "[frozen] "
    if role == ALIAS:
        target = ALIAS_TARGETS.get(verb, "")
        return f"[alias for `okg {target}`] " if target else "[alias] "
    return ""


#: Display order for ``okg --help``: the door, then the verbs that keep a
#: deployment alive, then everything else in declaration order, then the
#: retired doors last. Ordering is help-only — it never affects dispatch,
#: which stays keyed on ``action.choices``.
ROLE_DISPLAY_ORDER: tuple[str, ...] = (
    NEWCOMER_BRING_UP,
    LIFECYCLE,
    NOT_ENTRY,
    SUPERSEDED_ENTRY,
    ALIAS,
    FROZEN,
)


def display_rank(verb: str) -> int:
    """Sort key for help listing; lower sorts first."""
    try:
        return ROLE_DISPLAY_ORDER.index(role_of(verb))
    except ValueError:  # pragma: no cover - unreachable while roles are valid
        return len(ROLE_DISPLAY_ORDER)
