"""Deployment lint refuses an unranked collision of edge producers (item 92a)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.deployment_lint import lint_deployment

ENRICHER = "_enricher:code_reference"


def _source(edge_triple: tuple[str, str, str] | None) -> dict[str, Any]:
    body: dict[str, Any] = {
        "module": "okg.substrate.library.sources.code_graph",
        "class": "CodeStructureSource",
        "params": {"repo": "demo", "repo_root": "/tmp/demo"},
    }
    if edge_triple is not None:
        src, edge_type, dst = edge_triple
        body["admission_policy"] = {
            "output_signature": {
                "edges": [{
                    "src_subtype": src,
                    "edge_type": edge_type,
                    "dst_subtype": dst,
                }],
            },
        }
    return body


def _lint(
    tmp_path: Path,
    *,
    edge_precedence: Any = None,
    with_enricher: bool = True,
    lsp_triple: tuple[str, str, str] | None = ("code_symbol", "calls", "code_symbol"),
) -> list[dict[str, Any]]:
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "schemas").mkdir()
    manifest: dict[str, Any] = {
        "name": "lint-edge-precedence",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": ["code_graph"],
    }
    if with_enricher:
        resolver = "okg.substrate.library.linkers.code_reference.CodeReferenceResolver"
        manifest["enrichers"] = [{"class": resolver}]
    if edge_precedence is not None:
        manifest["edge_precedence"] = edge_precedence
    (dep / "deployment.yaml").write_text(yaml.safe_dump(manifest))
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "code-structure": _source(("code_symbol", "calls", "code_symbol")),
            "code-lsp-resolution": _source(lsp_triple),
        },
    }))
    report = lint_deployment(dep)
    return [
        finding
        for finding in report["findings"]
        if str(finding["code"]).startswith("deployment.edge_precedence")
    ]


def test_producer_missing_from_ranking_is_refused_naming_producers(
    tmp_path: Path,
) -> None:
    findings = _lint(
        tmp_path, edge_precedence={"calls": [ENRICHER, "code-structure"]},
    )
    assert [finding["code"] for finding in findings] == [
        "deployment.edge_precedence.unranked_collision",
    ]
    finding = findings[0]
    assert finding["severity"] == "blocker"
    assert finding["observed"]["edge_type"] == "calls"
    assert finding["observed"]["producers"] == [
        ENRICHER, "code-lsp-resolution", "code-structure",
    ]
    assert finding["observed"]["unranked"] == ["code-lsp-resolution"]
    assert "code-lsp-resolution" in finding["message"]


def test_unranked_edge_type_keeps_todays_path(tmp_path: Path) -> None:
    """Out of scope: producers sharing a triple of a type nobody ranks keep
    last-writer-wins and are not reported."""
    assert _lint(tmp_path) == []


def test_full_ranking_passes(tmp_path: Path) -> None:
    assert _lint(
        tmp_path,
        edge_precedence={
            "calls": [ENRICHER, "code-lsp-resolution", "code-structure"],
        },
    ) == []


def test_unknown_ranked_producer_is_refused(tmp_path: Path) -> None:
    findings = _lint(
        tmp_path,
        edge_precedence={
            "calls": [ENRICHER, "code-lsp-resolution", "code-structur"],
        },
    )
    codes = sorted(finding["code"] for finding in findings)
    assert codes == [
        "deployment.edge_precedence.unknown_producer",
        "deployment.edge_precedence.unranked_collision",
    ]
    unknown = next(f for f in findings if f["code"].endswith("unknown_producer"))
    assert unknown["observed"] == ["code-structur"]


@pytest.mark.parametrize(
    "value",
    [["calls"], {"calls": []}, {"calls": [ENRICHER, ENRICHER]}],
)
def test_malformed_block_is_refused(tmp_path: Path, value: Any) -> None:
    findings = _lint(tmp_path, edge_precedence=value)
    assert [finding["code"] for finding in findings] == [
        "deployment.edge_precedence.invalid",
    ]


def test_producers_on_different_triples_need_no_rank(tmp_path: Path) -> None:
    assert _lint(
        tmp_path,
        with_enricher=False,
        lsp_triple=("code_reference", "references", "code_symbol"),
        edge_precedence={"calls": ["code-structure"]},
    ) == []


def test_ranking_may_name_a_source_by_its_ownership_id(tmp_path: Path) -> None:
    """Ingest writes a source's rows under its ownership id, so a ranking may
    name either identity."""
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "schemas").mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "lint-edge-precedence",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": ["code_graph"],
        "edge_precedence": {"calls": ["demo.code-lsp", "code-structure"]},
    }))
    calls = ("code_symbol", "calls", "code_symbol")
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "code-structure": {
                **_source(calls), "ownership_id": "demo.code-structure",
            },
            "code-lsp": {**_source(calls), "ownership_id": "demo.code-lsp"},
        },
    }))
    findings = [
        finding
        for finding in lint_deployment(dep)["findings"]
        if str(finding["code"]).startswith("deployment.edge_precedence")
    ]
    assert findings == []
