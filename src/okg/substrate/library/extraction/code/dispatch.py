"""Language-keyed dispatch for code-symbol extractors.

Same shape as `parse.dispatch.register_parser` /
`chunk.dispatch.register_chunker` /
`ner.dispatch.register_ner`. Bootstrap pre-registers
`python` (the AST backend); other languages register at
deployment time via `extractors.yaml` -> the backend's
`__init__` calls `register_code_extractor(language, factory)`.

Per `add-code-symbol-extraction` (2026-04-26).
"""
from __future__ import annotations

from typing import Any, Callable

from okg.substrate.errors import SubstrateError


__all__ = [
    "CodeExtractorNotFound",
    "available_languages",
    "code_extractor_for",
    "register_code_extractor",
]


class CodeExtractorNotFound(SubstrateError):
    code = "code_extractor_not_found"


_REGISTRY: dict[str, Callable[..., object]] = {}


def register_code_extractor(
    language: str, factory: Callable[..., object],
) -> None:
    """Register a factory for `language`. Idempotent."""
    _REGISTRY[language] = factory


def code_extractor_for(language: str, **kwargs: Any):
    """Return a CodeSymbolExtractor instance for `language`.

    Raises `CodeExtractorNotFound` on miss. Kwargs forwarded
    to the factory.
    """
    factory = _REGISTRY.get(language)
    if factory is None:
        raise CodeExtractorNotFound(
            f"no code extractor registered for language "
            f"{language!r}",
            data={
                "language": language,
                "available": sorted(_REGISTRY.keys()),
            },
            fix_hint=(
                "register a CodeSymbolExtractor via "
                "`okg.substrate.library.extraction.code.dispatch."
                "register_code_extractor` or use one of the "
                "available languages"
            ),
        )
    return factory(**kwargs)


def available_languages() -> list[str]:
    return sorted(_REGISTRY.keys())


def _bootstrap() -> None:
    # Eager-import the Python AST backend so `python` is
    # always available without deployment-side registration.
    from okg.substrate.library.extraction.code import (  # noqa: F401
        python_ast,
    )


_bootstrap()
