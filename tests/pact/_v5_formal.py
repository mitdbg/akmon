"""Read the formal bindings the v4 -> v5 migration carried into evidence.

v5 has no ``formal[]`` block. `pact migrate` folds each v4 binding into a
``formal_check`` evidence row whose note names the property, the module and
the checker command it came from, and whose status is ``pass`` only when the
v4 row recorded its own pass and named an artifact that still resolves. The
tests that pinned "this model is bound by that change" read the note here
instead of a dropped field.

What is NOT recoverable: the v4 ``config`` and ``refinement_tests`` lists.
Those tests assert them against the ``.cfg`` and the model text directly, so
the claim survives; it is just no longer restated by the manifest.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pact.store import load

REPO_ROOT = Path(__file__).resolve().parents[2]

#: `formal binding property <id> in <module>[ via <command>][ (<disposition>)]`
_NOTE = re.compile(
    r"^formal binding property (?P<property_id>\S+) in (?P<module>\S+?)"
    r"(?: via (?P<checker_command>.+?))?"
    r"(?: \((?P<disposition>[^()]*(?:\([^()]*\)[^()]*)*)\))?$",
    re.S,
)


@dataclass(frozen=True)
class CarriedBinding:
    property_id: str
    module: str
    checker_command: str | None
    requirement: str
    status: str
    disposition: str | None


def manifest_path(change_id: str) -> Path:
    """The change's manifest, active or archived."""
    for scope in ("changes", "archive"):
        candidate = REPO_ROOT / "pact" / scope / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise AssertionError(f"no pact.yaml for {change_id}")


def carried_bindings(change_id: str) -> dict[str, CarriedBinding]:
    """Every formal binding the migration carried, keyed by property id."""
    bindings: dict[str, CarriedBinding] = {}
    for row in load(manifest_path(change_id)).evidence:
        if row.kind != "formal_check" or not row.note:
            continue
        # A migrated note is one logical line; YAML folding inserts newlines.
        match = _NOTE.match(" ".join(row.note.split()))
        if match is None:
            continue
        bindings[match["property_id"]] = CarriedBinding(
            property_id=match["property_id"],
            module=match["module"],
            checker_command=(match["checker_command"] or "").strip() or None,
            requirement=row.requirement,
            status=row.status,
            disposition=match["disposition"],
        )
    return bindings
