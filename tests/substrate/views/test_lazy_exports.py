"""Runtime proof that okg.substrate.views' registry exports resolve via the
module-level __getattr__ (enforce-lint-hygiene-gate).

These names are in __all__ but provided lazily (PEP 562 __getattr__), which
CodeQL reports as "explicit export is not defined" — a false positive, since it
cannot follow __getattr__. This test is the evidence that the names DO resolve at
runtime, so the documented false positive is also guarded against real breakage.
"""
from __future__ import annotations

import importlib

import pytest

import okg.substrate.views as views


@pytest.mark.parametrize("name", sorted(views._REGISTRY_EXPORTS))
def test_lazy_registry_export_resolves(name: str) -> None:
    obj = getattr(views, name)  # would AttributeError if the lazy wiring broke
    registry = importlib.import_module("okg.substrate.views.registry")
    assert obj is getattr(registry, name)


def test_every_all_entry_is_importable() -> None:
    # `from okg.substrate.views import *` must not raise on any __all__ name.
    for name in views.__all__:
        assert getattr(views, name) is not None


def test_getattr_rejects_unknown_name() -> None:
    with pytest.raises(AttributeError):
        getattr(views, "this_name_does_not_exist")
