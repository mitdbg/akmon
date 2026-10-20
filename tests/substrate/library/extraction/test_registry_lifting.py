"""Stride A.1 (2026-04-26) — `_instantiate` lifts backend
SubstrateErrors to `ExtractorChainMissing`.

When an extractor's constructor raises its own structured
error (e.g. `ModelNotInstalled` from spaCy when the model
isn't on disk, `GLiNERModelNotInstalled` from GLiNER, or
the future `LspServerNotInstalled`), the registry SHALL
wrap it as `ExtractorChainMissing` so operators see the
chain-level context alongside the original cause.

The tests monkey-patch the registry's class-resolution hook
so we don't depend on the `tests/` tree being a Python
package.
"""
from __future__ import annotations

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction import registry as reg_mod
from okg.substrate.library.extraction.registry import (
    ExtractorChainMissing,
    load_extractors,
)


class _BackendError(SubstrateError):
    code = "test_backend_unavailable"


class _BackendThatRaises:
    """Stand-in for SpacyNER/GLiNERBackend/LspSymbolExtractor;
    raises a structured SubstrateError at construction time."""

    def __init__(self, *, message: str = "model missing", **_: object):
        raise _BackendError(
            message,
            data={"requested": "missing-thing"},
            fix_hint="install the missing thing",
        )


@pytest.fixture
def patched_resolver(monkeypatch):
    """Substitute `_resolve_class` so a sentinel class path
    routes to our local stub."""

    def _fake_resolve(class_path: str):
        if class_path == "stub:raises":
            return _BackendThatRaises
        return reg_mod._resolve_class.__wrapped__(class_path) \
            if hasattr(reg_mod._resolve_class, "__wrapped__") \
            else _real(class_path)

    _real = reg_mod._resolve_class
    monkeypatch.setattr(reg_mod, "_resolve_class", _fake_resolve)


def test_chain_missing_lifts_substrate_error_with_cause(
    tmp_path, patched_resolver,
):
    extractors_dir = tmp_path / "ext"
    extractors_dir.mkdir()
    (extractors_dir / "extractors.yaml").write_text(yaml.safe_dump({
        "ner": [{
            "class": "stub:raises",
            "config": {"message": "no model on disk"},
        }],
    }))

    with pytest.raises(ExtractorChainMissing) as exc_info:
        load_extractors(extractors_dir)
    err = exc_info.value
    assert err.data["underlying_code"] == "test_backend_unavailable"
    assert err.data["class"] == "stub:raises"
    assert isinstance(err.__cause__, _BackendError)
    assert err.fix_hint == "install the missing thing"


def test_typeerror_still_lifts_with_constructor_hint(
    tmp_path, monkeypatch,
):
    """Plain TypeError on bad config keeps its existing
    handling — `underlying_code` field absent."""

    class _BackendStrictKwargs:
        def __init__(self):
            pass  # accepts no kwargs

    monkeypatch.setattr(
        reg_mod, "_resolve_class",
        lambda cp: _BackendStrictKwargs if cp == "stub:typeerror"
        else (_ for _ in ()).throw(LookupError(cp)),
    )
    extractors_dir = tmp_path / "ext"
    extractors_dir.mkdir()
    (extractors_dir / "extractors.yaml").write_text(yaml.safe_dump({
        "ner": [{
            "class": "stub:typeerror",
            "config": {"unknown_kwarg": "boom"},
        }],
    }))

    with pytest.raises(ExtractorChainMissing) as exc_info:
        load_extractors(extractors_dir)
    err = exc_info.value
    assert "underlying_code" not in err.data
    assert isinstance(err.__cause__, TypeError)
