"""Name-keyed NER backend dispatch.

`ner_for(name)` returns a registered EntityExtractor FACTORY-
returning instance — same shape as
`parse.dispatch.parser_for` and
`chunk.dispatch.chunker_for`. Bootstrap pre-registers the
three backends shipped in this package: `spacy`, `gliner`,
`regex`.

Per `add-doc-corpus-extraction-adapter` (2026-04-25).
"""

from __future__ import annotations

from typing import Any, Callable

from okg.substrate.errors import SubstrateError


__all__ = [
    "NerBackendNotFound",
    "available_ner_backends",
    "ner_for",
    "register_ner",
]


class NerBackendNotFound(SubstrateError):
    code = "ner_backend_not_found"


_REGISTRY: dict[str, Callable[..., object]] = {}


def register_ner(
    name: str, factory: Callable[..., object],
) -> None:
    """Register a factory for `name`. Idempotent."""
    _REGISTRY[name] = factory


def ner_for(name: str, **kwargs: Any):
    """Return a NER backend instance for `name`. Raises
    `NerBackendNotFound` on miss. Kwargs forwarded to factory."""
    factory = _REGISTRY.get(name)
    if factory is None:
        raise NerBackendNotFound(
            f"no NER backend registered for name {name!r}",
            data={
                "name": name,
                "available": sorted(_REGISTRY.keys()),
            },
            fix_hint=(
                "register a NER backend via "
                "`okg.substrate.library.extraction.ner.dispatch."
                "register_ner` or use one of the available names"
            ),
        )
    return factory(**kwargs)


def available_ner_backends() -> list[str]:
    return sorted(_REGISTRY.keys())


def _bootstrap() -> None:
    """Register the three NER backends shipped here."""
    from okg.substrate.library.extraction.ner.gliner_backend import (
        GLiNERBackend,
    )
    from okg.substrate.library.extraction.ner.regex_backend import (
        RegexEntityExtractor,
    )
    from okg.substrate.library.extraction.ner.spacy_backend import (
        SpacyNER,
    )

    register_ner("spacy", SpacyNER)
    register_ner("gliner", GLiNERBackend)
    register_ner("regex", RegexEntityExtractor)


_bootstrap()
