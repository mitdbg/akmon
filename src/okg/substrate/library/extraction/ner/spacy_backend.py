"""spaCy NER wrapper. Lazy-loads the model on first `extract()`.

Missing models surface as `ModelNotInstalled` with a fix-hint
naming the `python -m spacy download` command.
"""

from __future__ import annotations

from typing import Any

from okg.extraction import ExtractedEntity
from okg.substrate.errors import SubstrateError


class ModelNotInstalled(SubstrateError):
    code = "spacy_model_not_installed"


class SpacyNER:
    """spaCy-backed NER. Conforms to the EntityExtractor Protocol."""

    def __init__(
        self,
        model: str = "en_core_web_sm",
        *,
        confidence: float = 0.85,
    ) -> None:
        self.model = model
        self.confidence = float(confidence)
        self._nlp: Any = None  # lazy

    def _load(self) -> None:
        if self._nlp is not None:
            return
        from okg.substrate.support.env_provenance import (
            annotate_optional_dependency_error,
        )
        try:
            import spacy
        except ImportError as e:
            # Issue #639: name the interpreter that lacked the package so
            # a uv-tool-shim run is distinguishable from a missing install.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    "spacy package not installed",
                    "pip install 'okg[nlp]'",
                    extras=("nlp",),
                )
            )
            raise ModelNotInstalled(
                message,
                data={"model": self.model, **provenance},
                fix_hint=fix_hint,
            ) from e
        try:
            self._nlp = spacy.load(self.model)
        except OSError as e:
            # The download command installs into THIS interpreter's env —
            # the same #639 confusion vector, so name the interpreter.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    f"spaCy model {self.model!r} is not installed",
                    f"python -m spacy download {self.model}",
                    extras=("nlp",),
                )
            )
            raise ModelNotInstalled(
                message,
                data={"model": self.model, **provenance},
                fix_hint=fix_hint,
            ) from e

    def warmup(self) -> None:
        """Eager-load the model. Used by `okg doctor --warm` to
        surface missing-model failures at deploy, not at first
        ingest."""
        self._load()

    def extract(self, text: str) -> list[ExtractedEntity]:
        if not text:
            return []
        self._load()
        doc = self._nlp(text)
        out: list[ExtractedEntity] = []
        for ent in doc.ents:
            out.append(ExtractedEntity(
                type=ent.label_,
                value=ent.text,
                start=ent.start_char,
                end=ent.end_char,
                confidence=self.confidence,
            ))
        return out


__all__ = ["ModelNotInstalled", "SpacyNER"]
