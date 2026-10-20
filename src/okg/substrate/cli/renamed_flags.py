"""Deprecated spellings for renamed CLI flags and verbs.

Decision 310 ("profile B") renamed the overloaded ``--profile`` flags to
names that say what each one selects (``--target``, ``--checks``,
``--capture``, ``--permissions``, ``--scope``, ``--tools``, ``--provider``)
and the ``okg profile`` verb to ``okg perf``. Each old spelling stays for
one release as a deprecated alias:

* it is a real option string argparse knows, writing the same ``dest`` as
  the new name, so parsing, defaults and choices are identical;
* it is hidden from ``--help`` and the usage line (``help=SUPPRESS``);
* using it prints one warning line to stderr per flag per invocation.
  Stdout is untouched, so ``--json`` output stays valid.

One helper serves every renamed flag, so the warning text and behaviour
cannot drift between commands. Giving both spellings of one flag in one
invocation is refused with an argparse usage error naming both.

**Version skew.** Some callers start a child against code that may predate
the rename: ``okg deployment up`` re-enters ``okg`` after it checks out
``release.pin.code_revision`` (a rollback may land on a pre-rename commit),
and the Helm chart's probes run in an image whose tag may be older than
the chart. For this one release those callers pass the OLD spelling, which
old and new code both accept, and set ``OKG_RENAMED_FLAG_WARNINGS=0`` so
new code does not warn about a spelling chosen on purpose. When the aliases
are removed they flip to the new names; each is marked with a
``renamed-flag compat`` comment.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

#: The one warning pattern for every renamed flag and verb.
RENAME_WARNING = (
    "okg: warning: `{old}` is renamed to `{new}`; "
    "the old name will be removed in the next release"
)


#: Environment switch: ``0`` silences the rename warning, for the
#: version-skew callers described in the module docstring. Unset or any
#: other value keeps the warning on.
SILENCE_ENV = "OKG_RENAMED_FLAG_WARNINGS"

#: Merge into a child's environment when the caller passes an old spelling
#: on purpose for version-skew compatibility.
COMPAT_CHILD_ENV: dict[str, str] = {SILENCE_ENV: "0"}

#: The `okg deployment ready` checks flag that both pre-rename and
#: post-rename code accept. renamed-flag compat: flip to "--checks" when the
#: aliases are removed.
COMPAT_READY_CHECKS_FLAG = "--profile"

#: `okg deployment up` / `backup verify` database target, and `deployment up`
#: checks, in the spellings both pre-rename and post-rename code accept.
#: renamed-flag compat: flip to "--target" / "--checks" with the aliases.
COMPAT_TARGET_FLAG = "--profile"
COMPAT_UP_CHECKS_FLAG = "--readiness-profile"


def rename_warning(old: str, new: str) -> str:
    """The warning line for a renamed flag or verb."""
    return RENAME_WARNING.format(old=old, new=new)


def warnings_silenced() -> bool:
    """True when ``OKG_RENAMED_FLAG_WARNINGS=0`` is set."""
    return os.environ.get(SILENCE_ENV, "").strip() == "0"


def warn_renamed(old: str, new: str) -> None:
    """Print the rename warning to stderr (never stdout), unless silenced."""
    if warnings_silenced():
        return
    print(rename_warning(old, new), file=sys.stderr)


def _deprecated_action_class(
    base: type[argparse.Action], old: str, new: str,
) -> type[argparse.Action]:
    def __call__(self, parser, namespace, values, option_string=None):
        if not getattr(self, "_okg_warned", False):
            # The parser is built once per invocation, so one flag on this
            # action instance warns once even when repeated (`append`).
            self._okg_warned = True
            warn_renamed(old, new)
        return base.__call__(self, parser, namespace, values, option_string)

    return type(
        f"_Renamed{base.__name__}",
        (base,),
        {"__call__": __call__, "okg_renamed_to": new},
    )


def add_renamed_argument(
    parser: argparse.ArgumentParser,
    new: str,
    old: str,
    *,
    dest: str,
    **kwargs: Any,
) -> argparse.Action:
    """Add ``new`` plus a hidden, warning ``old`` alias sharing ``dest``.

    ``kwargs`` are the ordinary ``add_argument`` keywords for ``new``. The
    alias gets the same action, choices, nargs and const, no default of its
    own (the new option's default fills ``dest``), and no help.

    The pair sits in one mutually exclusive group, so giving both spellings
    in one invocation is an argparse usage error naming both (exit 2), not
    a silent last-one-wins. A ``required=True`` flag makes that group
    required, so the old spelling alone still satisfies it while the error
    names only the new one.
    """
    if not (new.startswith("--") and old.startswith("--")):
        raise ValueError(f"renamed flags must be long options: {old} -> {new}")
    required = bool(kwargs.pop("required", False))
    target: Any = parser.add_mutually_exclusive_group(required=required)
    action_spec = kwargs.get("action")
    new_action = target.add_argument(new, dest=dest, **kwargs)

    if isinstance(action_spec, type):
        base = action_spec
    else:
        base = parser._registry_get("action", action_spec, action_spec)  # noqa: SLF001
    old_kwargs = {
        k: v for k, v in kwargs.items() if k not in {"help", "default", "action"}
    }
    target.add_argument(
        old,
        dest=dest,
        action=_deprecated_action_class(base, old, new),
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
        **old_kwargs,
    )
    return new_action


def renamed_option_strings(parser: argparse.ArgumentParser) -> dict[str, str]:
    """``{old: new}`` for every deprecated alias on this parser (one level)."""
    out: dict[str, str] = {}
    for action in parser._actions:  # noqa: SLF001
        new = getattr(action, "okg_renamed_to", None)
        if new:
            for opt in action.option_strings:
                out[opt] = new
    return out
