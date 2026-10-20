"""GLiNER wrapper — zero-shot NER over a caller-specified label set.

GLiNER is the smaller-than-spaCy-transformer alternative for
deployments needing custom entity types beyond spaCy's stock
inventory (PERSON, ORG, GPE, ...).
"""

from __future__ import annotations

from typing import Any, Sequence

from okg.extraction import ExtractedEntity
from okg.substrate.errors import SubstrateError
from okg.substrate.support.env_provenance import (
    annotate_optional_dependency_error,
)


class GLiNERModelNotInstalled(SubstrateError):
    code = "gliner_model_not_installed"


class GLiNERBackend:
    """GLiNER-backed NER. Conforms to the EntityExtractor Protocol.

    `labels` is the entity-type set the model is asked to find;
    GLiNER is genuinely zero-shot, so the labels can be arbitrary
    strings (the per-deployment vocabulary).
    """

    def __init__(
        self,
        model_name: str = "urchade/gliner_small-v2.1",
        *,
        labels: Sequence[str] = ("Person", "Organization", "Location"),
        threshold: float = 0.5,
    ) -> None:
        self.model_name = model_name
        self.labels = list(labels)
        self.threshold = float(threshold)
        self._model: Any = None

    def _load(self) -> None:
        if self._model is not None:
            return
        try:
            from gliner import GLiNER
        except ImportError as e:
            # Issue #639: name the interpreter that lacked the package so
            # a uv-tool-shim run is distinguishable from a missing install.
            message, fix_hint, provenance = (
                annotate_optional_dependency_error(
                    "gliner package not installed",
                    "pip install 'okg[nlp]'",
                    extras=("nlp",),
                )
            )
            raise GLiNERModelNotInstalled(
                message,
                data={"model": self.model_name, **provenance},
                fix_hint=fix_hint,
            ) from e
        try:
            self._model = GLiNER.from_pretrained(self.model_name)
        except Exception as e:  # GLiNER raises a variety of types here
            raise GLiNERModelNotInstalled(
                f"GLiNER model {self.model_name!r} could not be loaded",
                data={"model": self.model_name, "error": str(e)},
                fix_hint=(
                    f"verify network connectivity to HuggingFace, or pre-fetch "
                    f"the model via `huggingface-cli download {self.model_name}`"
                ),
            ) from e

    def warmup(self) -> None:
        self._load()

    def extract(self, text: str) -> list[ExtractedEntity]:
        if not text:
            return []
        self._load()
        predictions = self._model.predict_entities(
            text, self.labels, threshold=self.threshold,
        )
        out: list[ExtractedEntity] = []
        for p in predictions:
            out.append(ExtractedEntity(
                type=p.get("label", ""),
                value=p.get("text", ""),
                start=int(p.get("start", 0)),
                end=int(p.get("end", 0)),
                confidence=float(p.get("score", 0.0)),
            ))
        return out


__all__ = ["GLiNERBackend", "GLiNERModelNotInstalled"]
