"""Regression test for the `okg generation list` fix_hint bug.

Per `align-substrate-cli-docs-and-runbooks` Phase 1: a generation-
resolution failure in `mcp.session` SHALL NOT direct operators at
a fictional CLI command. The fix_hint either names a real verb or
points at the AKMON read surface.
"""
from __future__ import annotations

from pathlib import Path


def test_no_fictional_generation_list_in_session_fix_hints() -> None:
    """Pre-Phase-2: the fictional `okg generation list --json`
    string SHALL NOT appear anywhere in mcp/session.py. After
    Phase 2 adds `okg generation list` as a real verb, the fix_hint
    refers to a real command.
    """
    session_py = (
        Path(__file__).resolve().parents[3]
        / "src/okg/substrate/mcp/session.py"
    )
    text = session_py.read_text()
    # Verify no `okg generation list --json` (the old fictional form
    # that took `--json` as an option, before the verb existed).
    assert "okg generation list --json" not in text, (
        "session.py still references the old fictional fix_hint "
        "form `okg generation list --json`; rewrite to point at "
        "the real `okg generation list --deployment <name>` verb "
        "(added in Phase 2)"
    )
    # Verify fix_hints in the file don't suggest commands containing
    # known fictional substrings.
    fictional_substrs = [
        "okg jobs ", "okg source pause", "okg source resume",
        "okg source replay", "okg fact evict",
    ]
    for sub in fictional_substrs:
        assert sub not in text, (
            f"session.py contains fictional command reference: "
            f"{sub!r}"
        )
