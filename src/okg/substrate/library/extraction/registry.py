"""Extractor-chain registry — load `extractors.yaml` from a
deployment dir, instantiate every declared backend, return a
typed bundle.

Schema:

    parser:
      - format: pdf
        class: okg.substrate.library.extraction.parse.pdf_text.PdfTextParser
      - format: pdf_tables
        class: okg.substrate.library.extraction.parse.pdf_tables.PdfTablesParser
    chunker:
      - class: okg.substrate.library.extraction.chunk.semantic.SemanticChunker
        config: {max_chars: 1500, overlap: 200}
    ner:
      - class: okg.substrate.library.extraction.ner.spacy_backend.SpacyNER
        config: {model: en_core_web_sm}
      - class: okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor
        config:
          rules:
            - [jira_ticket, "(CMS|ATLAS)-[0-9]+"]

Refuse-to-boot: if any declared backend can't be instantiated,
raise `ExtractorChainMissing` with the axis + class identifier.
No silent `Null<X>` stand-ins — an axis omitted from YAML yields
`None` in the bundle (a typed sentinel callers branch on).
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from okg.substrate import fastyaml

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction._protocols import Chunker, Parser
from okg.substrate.library.extraction.chunk.composite import CompositeChunker
from okg.substrate.library.extraction.ner.composite import CompositeNER


class ExtractorChainMissing(SubstrateError):
    code = "extractor_chain_missing"


class ExtractorAxisMissing(SubstrateError):
    code = "extractor_axis_missing"


@dataclass
class NerRoute:
    """One NER entry from `extractors.yaml`. `formats` is the set
    of parser-format strings the extractor applies to; `"*"` is
    the wildcard. Per `refine-doc-corpus-extraction` (2026-04-26).
    """

    formats: tuple[str, ...]
    extractor: Any  # EntityExtractor

    def matches(self, fmt: Optional[str]) -> bool:
        if "*" in self.formats:
            return True
        if fmt is None:
            return False
        return fmt in self.formats


@dataclass
class ExtractorBundle:
    """Loaded extractors for a deployment.

    Each axis is `None` / empty when the YAML omitted it.
    Callers branch on `None` explicitly — there are NO no-op
    stand-ins.

    Per `refine-doc-corpus-extraction` (2026-04-26):

    - `chunkers_by_format` keys chunkers by parser-format name
      (or `"default"` for the fallthrough). Legacy bare-list
      YAML (no `format:` keys) wraps as a `CompositeChunker`
      registered under `"default"`.
    - `ner_routes` is a list of `NerRoute`, each with a
      `formats` tuple. `DocCorpusSource` runs every route whose
      `formats` matches the chunk's source format OR `"*"`.
    - `chunker` and `ner` are kept for back-compat with legacy
      callers that read a single instance; both are derived
      from the new fields. New code SHOULD use
      `chunkers_by_format` / `ner_routes`.
    """

    parsers: dict[str, Parser]  # format -> Parser instance
    chunker: Optional[Chunker]  # legacy: == chunkers_by_format.get("default")
    ner: Optional[Any]          # legacy: union of ner_routes
    chunkers_by_format: dict[str, Chunker] = field(default_factory=dict)
    ner_routes: list[NerRoute] = field(default_factory=list)

    def chunker_for_format(self, fmt: Optional[str]) -> Optional[Chunker]:
        """Return the chunker registered for `fmt`, falling back
        to the `"default"` slot. Returns None when neither is
        registered."""
        if fmt is not None and fmt in self.chunkers_by_format:
            return self.chunkers_by_format[fmt]
        return self.chunkers_by_format.get("default")

    def ner_for_format(self, fmt: Optional[str]) -> list[Any]:
        """Return every NER backend whose route matches `fmt`
        (or matches `"*"`)."""
        return [
            r.extractor for r in self.ner_routes if r.matches(fmt)
        ]


def _resolve_class(class_path: str):
    """Resolve `'pkg.mod.ClassName'` to the class object via importlib."""
    try:
        module_path, class_name = class_path.rsplit(".", 1)
    except ValueError as e:
        raise ExtractorChainMissing(
            f"class path {class_path!r} is not in 'pkg.mod.Class' form",
            data={"class": class_path},
        ) from e
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        raise ExtractorChainMissing(
            f"could not import module {module_path!r}",
            data={"class": class_path, "import_error": str(e)},
            fix_hint=f"check that {module_path} is on the Python path",
        ) from e
    try:
        cls = getattr(module, class_name)
    except AttributeError as e:
        raise ExtractorChainMissing(
            f"module {module_path!r} has no attribute {class_name!r}",
            data={"class": class_path},
        ) from e
    return cls


def _instantiate(class_path: str, config: Optional[dict] = None):
    cls = _resolve_class(class_path)
    cfg = config or {}
    try:
        return cls(**cfg)
    except TypeError as e:
        raise ExtractorChainMissing(
            f"class {class_path!r} could not be instantiated with config "
            f"{list(cfg.keys())}",
            data={"class": class_path, "config_keys": list(cfg.keys()),
                  "error": str(e)},
            fix_hint="check the constructor signature",
        ) from e
    except SubstrateError as e:
        # Per `add-doc-corpus-extraction-adapter` Stride A.1
        # (2026-04-26): a backend's own structured error
        # (`ModelNotInstalled`, `GLiNERModelNotInstalled`,
        # `LspServerNotInstalled`, ...) lifts to
        # `ExtractorChainMissing` so operators see the chain-
        # level context (which class in extractors.yaml broke)
        # alongside the original cause. The original error is
        # preserved as `__cause__` for debugging.
        raise ExtractorChainMissing(
            f"class {class_path!r} refused to instantiate: "
            f"{type(e).__name__}: {e}",
            data={
                "class": class_path,
                "config_keys": list(cfg.keys()),
                "underlying_code": e.code,
                "underlying_data": dict(e.data or {}),
            },
            fix_hint=(
                e.fix_hint
                or f"see {type(e).__name__} for backend-specific guidance"
            ),
        ) from e


def _redacted_shape(value: Any) -> Any:
    """Return useful value shape without persisting authored strings."""
    if isinstance(value, Mapping):
        return {
            "type": "mapping",
            "entries": [
                {
                    "key_type": type(key).__name__,
                    "value": _redacted_shape(item),
                }
                for key, item in value.items()
            ],
        }
    if isinstance(value, list):
        return [_redacted_shape(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redacted_shape(item) for item in value)
    if isinstance(value, str):
        return "<redacted:string>"
    if value is None:
        return {"type": "null"}
    if type(value) in {bool, int, float}:
        return {"type": type(value).__name__}
    return f"<redacted:{type(value).__name__}>"


def _invalid_extractor_shape(
    *,
    path: Path,
    axis: str,
    message: str,
    offending_value: Any,
    index: int | None = None,
) -> ExtractorChainMissing:
    data: dict[str, Any] = {
        "path": str(path),
        "axis": axis,
        "offending_value": _redacted_shape(offending_value),
    }
    if index is not None:
        data["entry_index"] = index
    return ExtractorChainMissing(
        f"{path}: {axis}: {message}",
        data=data,
    )


def _extractor_axis_entries(
    document: Mapping[str, Any],
    *,
    path: Path,
    axis: str,
    aliases: tuple[str, ...],
) -> list[dict[str, Any]]:
    authored = [alias for alias in aliases if alias in document]
    if len(authored) > 1:
        raise _invalid_extractor_shape(
            path=path,
            axis=axis,
            message=f"duplicate aliases are not allowed: {authored!r}",
            offending_value={alias: document[alias] for alias in authored},
        )
    if not authored:
        return []
    value = document[authored[0]]
    if not isinstance(value, list):
        raise _invalid_extractor_shape(
            path=path,
            axis=axis,
            message="axis must be a list",
            offending_value=value,
        )
    entries: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, Mapping):
            raise _invalid_extractor_shape(
                path=path,
                axis=axis,
                index=index,
                message="entry must be a mapping",
                offending_value=item,
            )
        entry = dict(item)
        if "config" in entry and not isinstance(entry["config"], Mapping):
            raise _invalid_extractor_shape(
                path=path,
                axis=axis,
                index=index,
                message="config must be a non-null mapping when authored",
                offending_value=entry["config"],
            )
        entry["config"] = dict(entry.get("config", {}))
        entries.append(entry)
    return entries


def _required_string(
    entry: Mapping[str, Any],
    field: str,
    *,
    path: Path,
    axis: str,
    index: int,
) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _invalid_extractor_shape(
            path=path,
            axis=axis,
            index=index,
            message=f"entry requires a nonblank {field!r} string",
            offending_value=value,
        )
    return value.strip()


def load_extractors(deployment_dir: Path | str) -> ExtractorBundle:
    """Read `<deployment_dir>/extractors.yaml`; instantiate each
    declared backend; return an `ExtractorBundle`.

    Raises:
      - `FileNotFoundError` if `extractors.yaml` doesn't exist
      - `ExtractorChainMissing` if any declared backend fails to
        instantiate
    """
    deployment_dir = Path(deployment_dir)
    yaml_path = deployment_dir / "extractors.yaml"
    if not yaml_path.is_file():
        raise FileNotFoundError(f"extractors.yaml not found: {yaml_path}")

    with yaml_path.open() as f:
        doc = fastyaml.safe_load(f)
    if not isinstance(doc, Mapping):
        raise _invalid_extractor_shape(
            path=yaml_path,
            axis="document",
            message="document must be a mapping; an empty mapping is valid",
            offending_value=doc,
        )

    # ---- parsers (format -> Parser) ----
    parsers: dict[str, Parser] = {}
    parser_specs = _extractor_axis_entries(
        doc,
        path=yaml_path,
        axis="parser",
        aliases=("parser", "parsers"),
    )
    for index, spec in enumerate(parser_specs):
        fmt = _required_string(
            spec, "format", path=yaml_path, axis="parser", index=index
        )
        cls_path = _required_string(
            spec, "class", path=yaml_path, axis="parser", index=index
        )
        if fmt in parsers:
            raise _invalid_extractor_shape(
                path=yaml_path,
                axis="parser",
                index=index,
                message="duplicate parser format",
                offending_value=fmt,
            )
        cfg = spec["config"]
        parsers[fmt] = _instantiate(cls_path, cfg)

    # ---- chunker ----
    # Per `refine-doc-corpus-extraction` (2026-04-26):
    # - YAML entries with `format:` keys register per-format.
    # - YAML entries without `format:` keys (the legacy shape)
    #   wrap into a CompositeChunker registered under
    #   `"default"`.
    # - Mixing both shapes in one list is refused at load time
    #   so operators see one shape, not silent demotion.
    chunkers_by_format: dict[str, Chunker] = {}
    chunker_specs = _extractor_axis_entries(
        doc,
        path=yaml_path,
        axis="chunker",
        aliases=("chunker", "chunkers"),
    )
    if chunker_specs:
        format_keyed = [s for s in chunker_specs if "format" in s]
        bare = [s for s in chunker_specs if "format" not in s]
        if format_keyed and bare:
            error = _invalid_extractor_shape(
                path=yaml_path,
                axis="chunker",
                message="bare and per-format chunkers may not be mixed",
                offending_value=chunker_specs,
            )
            error.data.update({
                "format_keyed_count": len(format_keyed),
                "bare_count": len(bare),
            })
            raise error
        if bare:
            instances: list[Chunker] = []
            for index, spec in enumerate(bare):
                cls_path = _required_string(
                    spec,
                    "class",
                    path=yaml_path,
                    axis="chunker",
                    index=index,
                )
                cfg = spec["config"]
                instances.append(_instantiate(cls_path, cfg))
            chunkers_by_format["default"] = (
                instances[0] if len(instances) == 1
                else CompositeChunker(*instances)
            )
        else:
            for index, spec in enumerate(format_keyed):
                fmt = _required_string(
                    spec,
                    "format",
                    path=yaml_path,
                    axis="chunker",
                    index=index,
                )
                cls_path = _required_string(
                    spec,
                    "class",
                    path=yaml_path,
                    axis="chunker",
                    index=index,
                )
                if fmt in chunkers_by_format:
                    raise _invalid_extractor_shape(
                        path=yaml_path,
                        axis="chunker",
                        index=index,
                        message="duplicate chunker format",
                        offending_value=fmt,
                    )
                chunkers_by_format[fmt] = _instantiate(
                    cls_path, spec["config"]
                )
    chunker = chunkers_by_format.get("default")

    # ---- ner ----
    # Per `refine-doc-corpus-extraction` (2026-04-26): each
    # entry MAY carry a `formats:` list keyed on parser format
    # names; entries without `formats:` map to a single route
    # with `formats=("*",)` so legacy bare lists keep working.
    ner_routes: list[NerRoute] = []
    ner_specs = _extractor_axis_entries(
        doc,
        path=yaml_path,
        axis="ner",
        aliases=("ner",),
    )
    if ner_specs:
        for index, spec in enumerate(ner_specs):
            cls_path = _required_string(
                spec, "class", path=yaml_path, axis="ner", index=index
            )
            cfg = spec["config"]
            if "formats" not in spec:
                formats = ("*",)
            else:
                formats_field = spec["formats"]
                if isinstance(formats_field, str):
                    formats = (
                        _required_string(
                            {"formats": formats_field},
                            "formats",
                            path=yaml_path,
                            axis="ner",
                            index=index,
                        ),
                    )
                elif isinstance(formats_field, list) and formats_field:
                    if not all(
                        isinstance(value, str) and value.strip()
                        for value in formats_field
                    ):
                        raise _invalid_extractor_shape(
                            path=yaml_path,
                            axis="ner",
                            index=index,
                            message="formats must contain nonblank strings",
                            offending_value=formats_field,
                        )
                    formats = tuple(value.strip() for value in formats_field)
                    if len(set(formats)) != len(formats):
                        raise _invalid_extractor_shape(
                            path=yaml_path,
                            axis="ner",
                            index=index,
                            message="formats must be unique",
                            offending_value=formats_field,
                        )
                else:
                    raise _invalid_extractor_shape(
                        path=yaml_path,
                        axis="ner",
                        index=index,
                        message="formats must be a nonblank string or nonempty list",
                        offending_value=formats_field,
                    )
            extractor = _instantiate(cls_path, cfg)
            ner_routes.append(NerRoute(
                formats=formats,
                extractor=extractor,
            ))
    legacy_ner: Optional[Any] = None
    if ner_routes:
        legacy_ner = (
            ner_routes[0].extractor if len(ner_routes) == 1
            else CompositeNER(*[r.extractor for r in ner_routes])
        )

    return ExtractorBundle(
        parsers=parsers,
        chunker=chunker,
        ner=legacy_ner,
        chunkers_by_format=chunkers_by_format,
        ner_routes=ner_routes,
    )


__all__ = [
    "ExtractorAxisMissing",
    "ExtractorBundle",
    "ExtractorChainMissing",
    "NerRoute",
    "load_extractors",
]
