"""Identity-stability probe witnesses (review finding A2, design §5).

The probe compares id → (value, scope) content maps, not id sets: a
deletion that renumbers a same-(predicate, key) sibling onto a removed id
changes that id's content and must read UNSTABLE — the exact
gold-corruption path the gate exists to prevent. Clean removals shrink the
map; insertions after existing occurrences mint new ids; insertions before
them shift content and must read unstable; an unparseable mutated document
is unstable, never an abort.
"""

from __future__ import annotations

from okg.bench.injection import ring0_stability_probe

_BASE = """# Svc

## State
status: green
status: blue
"""


def test_deletion_shift_onto_removed_id_is_unstable():
    mutated = _BASE.replace("status: green\n", "")
    # The surviving line renumbers onto occurrence 0 with value blue != green.
    assert ring0_stability_probe("d.md", _BASE, mutated) is False


def test_unique_key_removal_is_stable():
    original = "# Svc\n\n## Steps\n1. Stop it.\n2. Start it.\n"
    mutated = original.replace("1. Stop it.\n", "")
    # Steps carry per-number keys, so removing step 1 shifts nothing.
    assert ring0_stability_probe("d.md", original, mutated) is True


def test_insertion_after_existing_occurrences_is_stable():
    mutated = _BASE + "status: red\n"
    assert ring0_stability_probe("d.md", _BASE, mutated) is True


def test_insertion_before_existing_occurrences_is_unstable():
    mutated = _BASE.replace("status: green\n", "status: red\nstatus: green\n")
    assert ring0_stability_probe("d.md", _BASE, mutated) is False


def test_unparseable_mutated_document_is_unstable_not_an_abort():
    assert ring0_stability_probe("d.md", _BASE, "no h1 here\n") is False