"""Stride A — axis registries (chunker / ner / validator).

Confirms the three new dispatch tables match the shape of the
existing parser dispatch:

  - bootstrap pre-registers the in-package backends;
  - `_for(name)` returns instances/callables;
  - unknown name raises a structured `*NotFound` carrying the
    requested name + available list + fix-hint.
"""
from __future__ import annotations

import pytest


def test_chunker_dispatch_bootstrap_registers_known_names() -> None:
    from okg.substrate.library.extraction.chunk.dispatch import (
        ChunkerNotFound,
        available_chunkers,
        chunker_for,
    )
    names = available_chunkers()
    for expected in (
        "window",
        "semantic",
        "structural_md",
        "structural_html",
        "latex_structural",
        "composite",
    ):
        assert expected in names, names

    inst = chunker_for("window")
    assert inst is not None
    # Window chunker takes optional kwargs.
    sized = chunker_for("window", max_chars=400, overlap=50)
    assert sized.max_chars == 400 and sized.overlap == 50

    with pytest.raises(ChunkerNotFound) as exc_info:
        chunker_for("not_a_real_chunker")
    err = exc_info.value
    assert "not_a_real_chunker" in str(err)
    assert "window" in err.data["available"]
    assert err.fix_hint and "register_chunker" in err.fix_hint


def test_ner_dispatch_bootstrap_registers_known_backends() -> None:
    from okg.substrate.library.extraction.ner.dispatch import (
        NerBackendNotFound,
        available_ner_backends,
        ner_for,
    )
    names = available_ner_backends()
    for expected in ("spacy", "gliner", "regex"):
        assert expected in names, names

    # `regex` is the only one that boots without a model file
    # so we can instantiate it here without optional deps.
    inst = ner_for("regex", rules=[])
    assert inst is not None

    with pytest.raises(NerBackendNotFound) as exc_info:
        ner_for("not_a_real_backend")
    err = exc_info.value
    assert "not_a_real_backend" in str(err)
    assert "regex" in err.data["available"]
    assert err.fix_hint and "register_ner" in err.fix_hint


def test_validator_dispatch_bootstrap_registers_known_kinds() -> None:
    from okg.substrate.library.extraction.validators import (
        ValidatorNotFound,
        available_validators,
        validate,
        validator_for,
    )
    kinds = available_validators()
    for expected in ("doi_checksum", "isbn_checksum", "orcid_checksum"):
        assert expected in kinds, kinds

    fn = validator_for("doi_checksum")
    assert callable(fn)

    with pytest.raises(ValidatorNotFound) as exc_info:
        validator_for("not_a_real_kind")
    err = exc_info.value
    assert "not_a_real_kind" in str(err)
    assert "doi_checksum" in err.data["available"]

    # Legacy `validate(value, NULL)` shim still returns True.
    assert validate("anything", None) is True
    # Unknown kind via shim still returns True (back-compat).
    assert validate("anything", "not_a_real_kind") is True


def test_validate_dispatches_through_registry() -> None:
    """The `validate(value, kind)` shim now reads from the
    registry, so a freshly-registered kind takes effect
    without code changes elsewhere."""
    from okg.substrate.library.extraction.validators import (
        register_validator,
        validate,
    )
    register_validator(
        "always_true_test", lambda _v: True,
    )
    register_validator(
        "always_false_test", lambda _v: False,
    )
    assert validate("X", "always_true_test") is True
    assert validate("X", "always_false_test") is False
