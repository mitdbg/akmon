"""Shared CLI authorization boundary for commands that always mutate."""

from __future__ import annotations

import argparse

from okg.substrate.errors import SubstrateError


def missing_apply_error(
    args: argparse.Namespace,
    *,
    command: str,
    fix_command: str | None = None,
) -> SubstrateError | None:
    """Return a stable refusal when a mutating command lacks ``--apply``."""

    if bool(getattr(args, "apply", False)):
        return None
    actionable = fix_command or f"okg {command} --apply ..."
    return SubstrateError(
        f"`okg {command}` requires --apply",
        data={
            "mutation_requested": False,
            "mutation_started": False,
            "command": command,
        },
        fix_hint=f"re-run as `{actionable}` to authorize the write",
    )
