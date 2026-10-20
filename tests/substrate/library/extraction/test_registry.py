"""Registry tests — refuses to boot on missing config."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from okg.substrate.library.extraction.registry import (
    ExtractorChainMissing,
    load_extractors,
)


def _write_yaml(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(body))


def test_load_extractors_empty_yaml_is_not_a_mapping(tmp_path: Path) -> None:
    """YAML null is distinct from the valid empty mapping."""
    _write_yaml(tmp_path / "extractors.yaml", "")
    from okg.substrate.library.extraction.registry import (
        ExtractorChainMissing,
        load_extractors,
    )

    with pytest.raises(ExtractorChainMissing, match="document must be a mapping"):
        load_extractors(tmp_path)


def test_load_extractors_empty_mapping(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", "{}")
    from okg.substrate.library.extraction.registry import load_extractors

    bundle = load_extractors(tmp_path)
    assert bundle.parsers == {}
    assert bundle.chunker is None
    assert bundle.ner is None


def test_load_extractors_full_chain(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", """
        parser:
          - format: pdf
            class: okg.substrate.library.extraction.parse.pdf_text.PdfTextParser
        chunker:
          - class: okg.substrate.library.extraction.chunk.window.WindowChunker
            config: {max_chars: 100, overlap: 10}
        ner:
          - class: okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor
            config:
              rules:
                - [ticket, "CMS-[0-9]+"]
    """)
    from okg.substrate.library.extraction.registry import load_extractors
    bundle = load_extractors(tmp_path)
    assert "pdf" in bundle.parsers
    assert bundle.chunker is not None
    assert bundle.ner is not None


def test_load_extractors_unknown_class_refuses_boot(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", """
        ner:
          - class: okg.substrate.library.extraction.ner.NoSuchBackend
    """)
    from okg.substrate.library.extraction.registry import (
        ExtractorChainMissing, load_extractors,
    )
    with pytest.raises(ExtractorChainMissing) as exc:
        load_extractors(tmp_path)
    assert "NoSuchBackend" in str(exc.value)


def test_load_extractors_unknown_module_refuses_boot(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", """
        ner:
          - class: not.a.real.module.path.SomeClass
    """)
    from okg.substrate.library.extraction.registry import (
        ExtractorChainMissing, load_extractors,
    )
    with pytest.raises(ExtractorChainMissing):
        load_extractors(tmp_path)


def test_load_extractors_missing_required_field(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", """
        parser:
          - format: pdf
            # missing 'class'
    """)
    from okg.substrate.library.extraction.registry import (
        ExtractorChainMissing, load_extractors,
    )
    with pytest.raises(ExtractorChainMissing):
        load_extractors(tmp_path)


def test_load_extractors_no_yaml_raises(tmp_path: Path) -> None:
    """No extractors.yaml in dir → FileNotFoundError."""
    from okg.substrate.library.extraction.registry import load_extractors
    with pytest.raises(FileNotFoundError):
        load_extractors(tmp_path)


def test_load_extractors_multiple_ner_yields_composite(tmp_path: Path) -> None:
    _write_yaml(tmp_path / "extractors.yaml", """
        ner:
          - class: okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor
            config:
              rules:
                - [a, "x"]
          - class: okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor
            config:
              rules:
                - [b, "y"]
    """)
    from okg.substrate.library.extraction.ner import CompositeNER
    from okg.substrate.library.extraction.registry import load_extractors
    bundle = load_extractors(tmp_path)
    assert isinstance(bundle.ner, CompositeNER)


def _shape_cases() -> list[tuple[str, object, str]]:
    cases: list[tuple[str, object, str]] = [
        ("document-null", None, "document"),
        ("document-scalar", 7, "document"),
        ("document-list", [], "document"),
    ]
    aliases = {
        "parser": ("parser", {"format": "pdf", "class": "x.Parser"}),
        "parsers": ("parser", {"format": "pdf", "class": "x.Parser"}),
        "chunker": ("chunker", {"class": "x.Chunker"}),
        "chunkers": ("chunker", {"class": "x.Chunker"}),
        "ner": ("ner", {"class": "x.Ner"}),
    }
    for alias, (axis, _valid) in aliases.items():
        for label, value in (("null", None), ("mapping", {}), ("scalar", "x")):
            cases.append((f"{alias}-{label}", {alias: value}, axis))
    for alias, axis in (("parser", "parser"), ("chunker", "chunker"), ("ner", "ner")):
        for label, value in (("null-entry", None), ("list-entry", []), ("scalar-entry", "x")):
            cases.append((f"{alias}-{label}", {alias: [value]}, axis))
    for alias, (axis, valid) in {
        "parser": ("parser", {"format": "pdf", "class": "x.Parser"}),
        "chunker": ("chunker", {"class": "x.Chunker"}),
        "ner": ("ner", {"class": "x.Ner"}),
    }.items():
        for label, value in (("null-config", None), ("list-config", []), ("scalar-config", "secret-token")):
            cases.append((
                f"{alias}-{label}",
                {alias: [{**valid, "config": value}]},
                axis,
            ))
    cases.extend([
        ("parser-missing-class", {"parser": [{"format": "pdf"}]}, "parser"),
        ("parser-blank-class", {"parser": [{"format": "pdf", "class": " "}]}, "parser"),
        ("parser-missing-format", {"parser": [{"class": "x.Parser"}]}, "parser"),
        ("parser-blank-format", {"parser": [{"format": " ", "class": "x.Parser"}]}, "parser"),
        ("chunker-missing-class", {"chunker": [{}]}, "chunker"),
        ("chunker-blank-class", {"chunker": [{"class": " "}]}, "chunker"),
        ("chunker-null-format", {"chunker": [{"class": "x.Chunker", "format": None}]}, "chunker"),
        ("chunker-blank-format", {"chunker": [{"class": "x.Chunker", "format": " "}]}, "chunker"),
        ("ner-missing-class", {"ner": [{}]}, "ner"),
    ])
    assert len(cases) == 45
    return cases


@pytest.mark.parametrize(
    ("case_name", "document", "axis"),
    _shape_cases(),
    ids=lambda value: value if isinstance(value, str) else None,
)
def test_extractor_shape_boundary_matrix(
    tmp_path: Path,
    case_name: str,
    document: object,
    axis: str,
) -> None:
    del case_name
    path = tmp_path / "extractors.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    with pytest.raises(ExtractorChainMissing) as caught:
        load_extractors(tmp_path)

    assert caught.value.data["path"] == str(path)
    assert caught.value.data["axis"] == axis
    assert "offending_value" in caught.value.data


@pytest.mark.parametrize(
    "document",
    [
        {"parser": [], "parsers": []},
        {
            "chunker": [
                {"format": "pdf", "class": "x.Chunker"},
                {"class": "x.Other"},
            ]
        },
        {"ner": [{"class": "x.Ner", "formats": None}]},
        {"ner": [{"class": "x.Ner", "formats": []}]},
        {"ner": [{"class": "x.Ner", "formats": ["pdf", "pdf"]}]},
        {"ner": [{"class": "x.Ner", "formats": ["pdf", " "]}]},
    ],
)
def test_extractor_duplicate_and_format_boundaries(
    tmp_path: Path,
    document: object,
) -> None:
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump(document),
        encoding="utf-8",
    )
    with pytest.raises(ExtractorChainMissing):
        load_extractors(tmp_path)


@pytest.mark.parametrize(
    ("axis", "class_path"),
    [
        (
            "parser",
            "okg.substrate.library.extraction.parse.pdf_text.PdfTextParser",
        ),
        (
            "chunker",
            "okg.substrate.library.extraction.chunk.structural_markdown."
            "MarkdownStructuralChunker",
        ),
    ],
)
def test_duplicate_format_refuses_without_echoing_authored_value(
    tmp_path: Path,
    axis: str,
    class_path: str,
) -> None:
    authored_format = "private-format-value"
    document = {
        axis: [
            {"format": authored_format, "class": class_path},
            {"format": authored_format, "class": class_path},
        ]
    }
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump(document),
        encoding="utf-8",
    )

    with pytest.raises(
        ExtractorChainMissing,
        match=rf"duplicate {axis} format$",
    ) as caught:
        load_extractors(tmp_path)

    assert authored_format not in str(caught.value)
    assert authored_format not in repr(caught.value.data)


@pytest.mark.parametrize("value", [None, True, 7, 3.5])
def test_extractor_error_redacts_scalar_contents(
    tmp_path: Path,
    value: object,
) -> None:
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump(value),
        encoding="utf-8",
    )

    with pytest.raises(ExtractorChainMissing) as caught:
        load_extractors(tmp_path)

    assert caught.value.data["offending_value"] == {
        "type": "null" if value is None else type(value).__name__,
    }


def test_extractor_error_redacts_nested_authored_strings(tmp_path: Path) -> None:
    secret = "postgresql://user:password@host/private"
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump({"parser": {"dsn": secret, "token": "abc123"}}),
        encoding="utf-8",
    )

    with pytest.raises(ExtractorChainMissing) as caught:
        load_extractors(tmp_path)

    serialized = repr(caught.value.data)
    assert secret not in serialized
    assert "abc123" not in serialized


def test_extractor_error_redacts_authored_mapping_keys(tmp_path: Path) -> None:
    secret_key = "postgresql://user:password@host/private"
    token_key = "short-token-key"
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump(
            {"parser": {secret_key: "value", token_key: "second"}}
        ),
        encoding="utf-8",
    )

    with pytest.raises(ExtractorChainMissing) as caught:
        load_extractors(tmp_path)

    serialized = repr(caught.value.data)
    assert secret_key not in serialized
    assert token_key not in serialized
