from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import psycopg
import pytest
import yaml
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_security_finding_linker_publish_connects_findings_to_targets(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.linkers.security_finding import (
        SecurityFindingLinker,
    )
    from okg.substrate.library.sources.base import NodeFact, SourceRun
    from okg.substrate.publisher import Publisher

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-security-finding",
        "description": "security finding linker fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
            "quality_evidence",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    security_id = "security-finding:semgrep:fixture"
    quality_id = "code-quality-finding:codeql:fixture"
    secret_id = "secret-finding:gitleaks:fixture"
    vuln_id = "dependency-vuln:pip-audit:fixture"
    file_id = "file:demo:src/app.py"
    env_file_id = "file:demo:.env"
    symbol_id = "symbol:demo:src/app.py:load:10"
    package_id = "dep:python:pydantic"
    config_id = "config:demo:env:TOKEN"
    asset_id = "asset:demo:file_path:env"

    class FixtureSource:
        name = "fixture"
        profile = "discovery_crawl"

        def run(
            self,
            run_id: str,
            *,
            mode: str,
            cursor: dict[str, Any] | None = None,
        ) -> SourceRun:
            del run_id, mode, cursor

            def facts() -> Iterable[NodeFact]:
                for node_id, subtype, attrs in (
                    (file_id, "source_file", {"repo": "demo", "path": "src/app.py"}),
                    (env_file_id, "source_file", {"repo": "demo", "path": ".env"}),
                    (
                        symbol_id,
                        "code_symbol",
                        {
                            "repo": "demo",
                            "path": "src/app.py",
                            "kind": "function",
                            "name": "load",
                            "qualified_name": "load",
                            "start_line": 10,
                            "end_line": 20,
                        },
                    ),
                    (
                        package_id,
                        "dependency_package",
                        {
                            "package_id": package_id,
                            "name": "pydantic",
                            "ecosystem": "python",
                        },
                    ),
                    (
                        config_id,
                        "config_key",
                        {
                            "config_id": config_id,
                            "repo": "demo",
                            "key": "TOKEN",
                            "source_kind": "env",
                        },
                    ),
                    (
                        asset_id,
                        "data_asset",
                        {
                            "asset_id": asset_id,
                            "repo": "demo",
                            "kind": "file_path",
                            "name": ".env",
                        },
                    ),
                    (
                        quality_id,
                        "code_quality_finding",
                        {
                            "finding_id": quality_id,
                            "repo": "demo",
                            "tool": "codeql",
                            "rule_id": "py/unused-import",
                            "category": "maintainability",
                            "severity": "warning",
                            "path": "src/app.py",
                            "start_line": 12,
                            "end_line": 12,
                        },
                    ),
                    (
                        security_id,
                        "security_finding",
                        {
                            "finding_id": security_id,
                            "repo": "demo",
                            "tool": "semgrep",
                            "rule_id": "demo.security",
                            "path": "src/app.py",
                            "start_line": 12,
                            "end_line": 12,
                            "asset_id": asset_id,
                        },
                    ),
                    (
                        secret_id,
                        "secret_finding",
                        {
                            "finding_id": secret_id,
                            "repo": "demo",
                            "tool": "gitleaks",
                            "rule_id": "generic-api-key",
                            "path": ".env",
                            "start_line": 1,
                            "end_line": 1,
                            "redacted_context": "TOKEN=[REDACTED]",
                        },
                    ),
                    (
                        vuln_id,
                        "dependency_vulnerability",
                        {
                            "vuln_id": "PYSEC-FIXTURE",
                            "repo": "demo",
                            "source": "pip-audit",
                            "package_id": package_id,
                            "package_name": "pydantic",
                            "ecosystem": "pypi",
                        },
                    ),
                ):
                    yield NodeFact(
                        node_id=node_id,
                        subtype=subtype,
                        attrs=attrs,
                        source_record_id={"node_id": node_id},
                        source_revision={"rev": 1},
                    )

            return SourceRun(facts=facts(), completed_scope=True)

    SourceRunner(clean_runtime, source_name="fixture.security-finding").run(
        FixtureSource(),
        mode="scope_complete",
    )

    out = Publisher(
        clean_runtime,
        "test-security-finding",
        enrichers=[SecurityFindingLinker()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "test-security-finding"),
    ).cycle()
    assert out["status"] == "published", out

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkout = store.checkout_generation(out["generation_id"])
        rows = [
            (row["src"], row["edge_type"], row["dst_subtype"], row["dst"])
            for row in checkout.query(
                """
                SELECT e.src, e.edge_type, d.subtype AS dst_subtype, e.dst
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE e.src = ANY(:srcs)
                 ORDER BY e.src, e.edge_type, d.subtype, e.dst
                """,
                {"srcs": [quality_id, security_id, secret_id, vuln_id]},
            )
        ]
    finally:
        store.close()
    with psycopg.connect(clean_runtime) as conn:
        diagnostics = SecurityFindingLinker().diagnostics(conn)

    assert (quality_id, "affects", "source_file", file_id) in rows
    assert (quality_id, "affects", "code_symbol", symbol_id) in rows
    assert (security_id, "affects", "source_file", file_id) in rows
    assert (security_id, "affects", "code_symbol", symbol_id) in rows
    assert (security_id, "references", "data_asset", asset_id) in rows
    assert (secret_id, "affects", "source_file", env_file_id) in rows
    assert (secret_id, "references", "config_key", config_id) in rows
    assert (secret_id, "references", "data_asset", asset_id) in rows
    assert (vuln_id, "affects", "dependency_package", package_id) in rows
    assert diagnostics.eligible_findings == 4
    assert diagnostics.matched_findings == 4
    assert diagnostics.emitted_edges == 9
    assert diagnostics.unmatched_finding_count == 0
