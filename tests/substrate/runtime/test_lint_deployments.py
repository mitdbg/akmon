"""Tests for `okg lint deployments` (Phase 2 §6.3).

Uses a temporary deployment directory with crafted Python modules so
the lint exercises:

  * Direct import flagged (`from okg.substrate.publisher import ...`)
  * Transitive import flagged (deployment module → helper module →
    substrate import)
  * Clean deployment module produces no findings
"""
from __future__ import annotations

from pathlib import Path

import pytest


CLEAN_MODULE = '''
"""A clean deployment workflow module."""
from okg.deployment import deployment_workflow, emit_node_fact


@deployment_workflow(name="clean")
def clean_workflow():
    emit_node_fact("n-1", "Subtype", {}, dedupe_key="d")
'''

DIRECT_VIOLATION_MODULE = '''
"""A workflow that breaks the boundary directly."""
from okg.deployment import deployment_workflow
from okg.substrate.publisher import Publisher  # DEPLOYMENT-001
from okg.substrate.runtime.escalation import create_escalation  # DEPLOYMENT-001


@deployment_workflow(name="bad")
def bad_workflow():
    pass
'''


HELPER_VIOLATION_MODULE = '''
"""A helper that breaks the boundary; will be flagged transitively."""
from okg.substrate.publisher import Publisher  # DEPLOYMENT-001


def helper():
    return Publisher.__name__
'''


PARENT_USING_HELPER = '''
"""Clean on its own, but imports a helper that violates the boundary."""
from okg.deployment import deployment_workflow

from . import _helper  # transitive substrate import lands here


@deployment_workflow(name="parent")
def parent_workflow():
    _helper.helper()
'''


@pytest.fixture
def deployments_tree(tmp_path: Path) -> Path:
    """Build a tmp deployments/ directory with three deployments:
    one clean, one with a direct violation, one with a transitive
    violation."""
    deployments_root = tmp_path / "deployments"

    # Deployment A: clean.
    a = deployments_root / "alpha"
    (a / "workflows").mkdir(parents=True)
    (a / "workflows" / "__init__.py").write_text("")
    (a / "workflows" / "workflow.py").write_text(CLEAN_MODULE)
    (a / "deployment.yaml").write_text(
        "name: alpha\n"
        "runtime:\n"
        "  enabled: true\n"
        "  workflows: [alpha.workflows.workflow]\n"
    )
    # Make the deployment a python package importable from
    # deployments_root by adding __init__.
    (a / "__init__.py").write_text("")

    # Deployment B: direct violation.
    b = deployments_root / "beta"
    (b / "workflows").mkdir(parents=True)
    (b / "workflows" / "__init__.py").write_text("")
    (b / "workflows" / "workflow.py").write_text(DIRECT_VIOLATION_MODULE)
    (b / "deployment.yaml").write_text(
        "name: beta\n"
        "runtime:\n"
        "  enabled: true\n"
        "  workflows: [beta.workflows.workflow]\n"
    )
    (b / "__init__.py").write_text("")

    # Deployment C: transitive violation through sibling helper.
    c = deployments_root / "gamma"
    (c / "workflows").mkdir(parents=True)
    (c / "workflows" / "__init__.py").write_text("")
    (c / "workflows" / "_helper.py").write_text(HELPER_VIOLATION_MODULE)
    (c / "workflows" / "workflow.py").write_text(PARENT_USING_HELPER)
    (c / "deployment.yaml").write_text(
        "name: gamma\n"
        "runtime:\n"
        "  enabled: true\n"
        "  workflows: [gamma.workflows.workflow]\n"
    )
    (c / "__init__.py").write_text("")

    return tmp_path


def test_clean_deployment_yields_no_findings(deployments_tree: Path):
    from okg.substrate.runtime.lint import lint_deployments

    # Filter the test to just the clean deployment by walking
    # alpha in isolation.
    findings = lint_deployments(
        deployments_root=deployments_tree / "deployments",
        repo_root=deployments_tree,
    )
    alpha_findings = [
        f for f in findings if "alpha" in str(f.path)
    ]
    assert alpha_findings == [], (
        f"clean deployment should produce no findings; got {alpha_findings}"
    )


def test_direct_substrate_import_is_flagged(deployments_tree: Path):
    from okg.substrate.runtime.lint import (
        DEPLOYMENT_001, lint_deployments,
    )

    findings = lint_deployments(
        deployments_root=deployments_tree / "deployments",
        repo_root=deployments_tree,
    )
    beta_findings = [
        f for f in findings if "beta" in str(f.path)
    ]
    assert len(beta_findings) >= 2, (
        "expected both substrate imports to be flagged; "
        f"got {len(beta_findings)}: {beta_findings}"
    )
    assert all(f.code == DEPLOYMENT_001 for f in beta_findings)
    symbols = sorted(f.importing_symbol for f in beta_findings)
    assert "okg.substrate.publisher" in symbols
    assert "okg.substrate.runtime.escalation" in symbols


def test_transitive_import_is_flagged(deployments_tree: Path):
    """The lint must follow same-package imports and catch substrate
    imports in a sibling helper module."""
    from okg.substrate.runtime.lint import lint_deployments

    findings = lint_deployments(
        deployments_root=deployments_tree / "deployments",
        repo_root=deployments_tree,
    )
    gamma_findings = [
        f for f in findings if "gamma" in str(f.path)
    ]
    assert len(gamma_findings) >= 1, (
        "expected the transitive substrate import to be flagged; "
        f"got {gamma_findings}"
    )
    # The finding's path should point to the helper, not the parent
    # workflow.
    assert any("_helper" in str(f.path) for f in gamma_findings), (
        f"expected _helper.py to be in the flagged paths: "
        f"{[str(f.path) for f in gamma_findings]}"
    )


def test_findings_carry_suggestions(deployments_tree: Path):
    """Each finding's message names the SDK helper to use instead."""
    from okg.substrate.runtime.lint import lint_deployments

    findings = lint_deployments(
        deployments_root=deployments_tree / "deployments",
        repo_root=deployments_tree,
    )
    # The substrate.publisher hint points to emit_node_fact /
    # emit_edge_fact.
    pub_findings = [
        f for f in findings
        if f.importing_symbol == "okg.substrate.publisher"
    ]
    assert pub_findings
    assert any(
        "emit_node_fact" in f.message for f in pub_findings
    ), f"expected emit_node_fact hint; got messages: {[f.message for f in pub_findings]}"


# ---------------------------------------------------------------------------
# Connector + enricher entries (issue #1181 slice 6)
# ---------------------------------------------------------------------------

EXTERNAL_CONNECTOR_VIOLATION = '''
"""An external connector that breaks the boundary."""
raise RuntimeError("the boundary lint must never import this module")

from okg.substrate.library.sources.base import NodeFact  # DEPLOYMENT-001


class ExternalSource:
    pass
'''

EXTERNAL_CONNECTOR_CLEAN = '''
"""An external connector that stays inside the SDK."""
raise RuntimeError("the boundary lint must never import this module")

from okg.deployment import NodeFact, EdgeFact


class ExternalSource:
    pass
'''

EXTERNAL_ENRICHER_VIOLATION = '''
"""An external enricher that breaks the boundary."""
from okg.substrate.publisher import Publisher  # DEPLOYMENT-001


class ExternalEnricher:
    def enrich(self, conn):
        return Publisher
'''


# The five modules an existing database-backed enricher may still import
# (LEGACY_ENRICHER_SURFACE, issue #1793): exactly what the Archi wheel's
# enrichers use, `_chronos` included.
EXTERNAL_ENRICHER_LEGACY_SURFACE = '''
"""An external enricher on the legacy read surface only."""
from okg.substrate.alias.protocol import AliasMatch
from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import insert_deterministic_edges
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers.declarative import DeclarativeLinker


class LegacyEnricher:
    def enrich(self, conn, *, generation_id, incremental=None):
        _chronos.query(conn.cursor(), "SELECT 1")
        return EnrichResult("legacy", n_edges_emitted=0)
'''

EXTERNAL_ENRICHER_MIXED = '''
"""Legacy surface plus one import that has an SDK equivalent."""
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.library.linkers import _chronos
from okg.substrate.publisher import Publisher  # DEPLOYMENT-001


class MixedEnricher:
    def enrich(self, conn, *, generation_id, incremental=None):
        return EnrichResult("mixed", n_edges_emitted=0)
'''

EXTERNAL_CONNECTOR_CHRONOS = '''
"""A connector reaching for the enricher read helper."""
from okg.deployment import NodeFact
from okg.substrate.library.linkers import _chronos  # DEPLOYMENT-001


class ExternalSource:
    def run(self, ctx):
        return []
'''

SHARED_READER = '''
"""One module declared both as a connector and as an enricher."""
from okg.substrate.library.linkers import _chronos  # DEPLOYMENT-001 (connector)


class ExternalSource:
    pass


class Shared:
    def enrich(self, conn, *, generation_id, incremental=None):
        return None
'''


def _write_registry_deployment(
    root: Path,
    *,
    name: str,
    connector_source: str | None,
    connector_module: str = "ext_connector",
    enricher_class: str | None = None,
    enricher_source: str | None = None,
    registry_body: str | None = None,
) -> Path:
    """A tmp deployment whose registry names one connector entry."""
    d = root / name
    d.mkdir(parents=True)
    if connector_source is not None:
        (d / f"{connector_module}.py").write_text(connector_source)
    if enricher_source is not None:
        module = (enricher_class or "").rsplit(".", 1)[0]
        (d / f"{module}.py").write_text(enricher_source)
    if registry_body is None:
        registry_body = (
            "sources:\n"
            "  external-feed:\n"
            f"    module: {connector_module}\n"
            "    class: ExternalSource\n"
            f"    ownership_id: {name}.external-feed\n"
            "    source_class: discovery_crawl\n"
        )
    (d / "source_registry.yaml").write_text(registry_body)
    manifest = (
        f"name: {name}\n"
        "source_registry: source_registry.yaml\n"
    )
    if enricher_class:
        manifest += f"enrichers:\n  - class: {enricher_class}\n"
    (d / "deployment.yaml").write_text(manifest)
    return d


def test_external_connector_substrate_import_is_refused(tmp_path: Path):
    """A registry-named connector module is scanned, and the finding names
    the registry key that brought it in."""
    from okg.substrate.runtime.lint import DEPLOYMENT_001, lint_deployments

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=EXTERNAL_CONNECTOR_VIOLATION,
    )

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert len(findings) == 1, findings
    finding = findings[0]
    assert finding.code == DEPLOYMENT_001
    assert finding.importing_symbol == "okg.substrate.library.sources.base"
    # The readback names the ENTRY, not merely the file.
    assert "external-feed" in finding.entry, finding.entry
    assert "connector" in finding.entry
    assert "external-feed" in finding.message
    # The suggestion table applies to connector entries too.
    assert "NodeFact" in finding.message


def test_external_connector_using_only_the_sdk_is_clean(tmp_path: Path):
    from okg.substrate.runtime.lint import lint_deployments

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=EXTERNAL_CONNECTOR_CLEAN,
    )

    assert lint_deployments(
        deployments_root=root, repo_root=tmp_path,
    ) == []


def test_external_enricher_substrate_import_is_refused(tmp_path: Path):
    """A declared enricher CLASS is resolved to its module and scanned."""
    from okg.substrate.runtime.lint import DEPLOYMENT_001, lint_deployments

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=EXTERNAL_CONNECTOR_CLEAN,
        enricher_class="ext_enricher.ExternalEnricher",
        enricher_source=EXTERNAL_ENRICHER_VIOLATION,
    )

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert len(findings) == 1, findings
    finding = findings[0]
    assert finding.code == DEPLOYMENT_001
    assert finding.importing_symbol == "okg.substrate.publisher"
    assert "ext_enricher.ExternalEnricher" in finding.entry, finding.entry
    assert "enricher" in finding.entry


def test_external_enricher_on_legacy_surface_is_stated_not_reported(
    tmp_path: Path,
):
    """Issue #1793: an enricher importing only the legacy read surface
    (``_chronos`` included) is not a finding; the coverage names the surface
    and the modules, so nothing is inferred from the empty list."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_LEGACY_ENRICHER,
        LEGACY_ENRICHER_SURFACE,
        lint_deployments_report,
    )

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=EXTERNAL_CONNECTOR_CLEAN,
        enricher_class="legacy_enricher.LegacyEnricher",
        enricher_source=EXTERNAL_ENRICHER_LEGACY_SURFACE,
    )

    report = lint_deployments_report(deployments_root=root, repo_root=tmp_path)

    assert report.findings == []
    by_kind = {c.kind: c for c in report.entry_coverage}
    enricher = by_kind["enricher"]
    assert enricher.state == BOUNDARY_LEGACY_ENRICHER
    assert set(enricher.detail.split(": ", 1)[1].split(", ")) == set(
        LEGACY_ENRICHER_SURFACE
    )
    assert "_chronos" in enricher.detail
    # The connector entry is unaffected by the disposition.
    assert by_kind["connector"].state == "scanned"


def test_external_enricher_beyond_legacy_surface_is_still_refused(
    tmp_path: Path,
):
    """The surface is an allow-list, not a kind-wide exemption: the one
    import outside it is the one finding, and the entry still reads as on
    the legacy surface for the imports that were."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_LEGACY_ENRICHER,
        DEPLOYMENT_001,
        lint_deployments_report,
    )

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=EXTERNAL_CONNECTOR_CLEAN,
        enricher_class="mixed_enricher.MixedEnricher",
        enricher_source=EXTERNAL_ENRICHER_MIXED,
    )

    report = lint_deployments_report(deployments_root=root, repo_root=tmp_path)

    assert [f.importing_symbol for f in report.findings] == [
        "okg.substrate.publisher"
    ]
    assert report.findings[0].code == DEPLOYMENT_001
    enricher = next(c for c in report.entry_coverage if c.kind == "enricher")
    assert enricher.state == BOUNDARY_LEGACY_ENRICHER
    assert "okg.substrate.publisher" not in enricher.detail


def test_connector_importing_the_enricher_surface_is_refused(tmp_path: Path):
    """The disposition is keyed on the enricher entry kind: a connector
    importing ``_chronos`` is DEPLOYMENT-001 exactly as before."""
    from okg.substrate.runtime.lint import DEPLOYMENT_001, lint_deployments

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=EXTERNAL_CONNECTOR_CHRONOS,
    )

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert len(findings) == 1, findings
    assert findings[0].code == DEPLOYMENT_001
    assert findings[0].importing_symbol == "okg.substrate.library.linkers"
    assert findings[0].imported_names == ("_chronos",)
    assert "connector" in findings[0].entry


def test_module_shared_with_a_connector_entry_stays_deny_all(tmp_path: Path):
    """A module both a connector entry and an enricher entry pull in gets
    no legacy allowance: the finding stands and names both entries."""
    from okg.substrate.runtime.lint import DEPLOYMENT_001, lint_deployments_report

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=SHARED_READER,
        connector_module="shared_reader",
        enricher_class="shared_reader.Shared",
        enricher_source=SHARED_READER,
    )

    report = lint_deployments_report(deployments_root=root, repo_root=tmp_path)

    assert len(report.findings) == 1, report.findings
    finding = report.findings[0]
    assert finding.code == DEPLOYMENT_001
    assert finding.importing_symbol == "okg.substrate.library.linkers"
    assert finding.imported_names == ("_chronos",)
    assert "connector" in finding.entry and "enricher" in finding.entry
    assert all(c.state == "scanned" for c in report.entry_coverage)


def test_cli_json_states_legacy_surface_and_exits_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys,
):
    """``okg --json lint deployments`` keeps the contract okg-deployments'
    compatibility check reads -- ``count == len(findings)`` and exit 0 iff no
    findings and no chat blockers -- while the entry summary counts the
    legacy-surface enricher."""
    import json

    from okg.substrate.cli.main import main

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=EXTERNAL_CONNECTOR_CLEAN,
        enricher_class="legacy_enricher.LegacyEnricher",
        enricher_source=EXTERNAL_ENRICHER_LEGACY_SURFACE,
    )
    monkeypatch.chdir(tmp_path)

    rc = main(["--json", "lint", "deployments", "--deployments-root", str(root)])
    out = capsys.readouterr().out
    payload = json.loads(out[out.index("{"):])

    assert rc == 0
    assert payload["count"] == 0 and payload["findings"] == []
    assert payload["entry_summary"]["legacy-enricher-surface"] == 1
    assert payload["entry_summary"]["scanned"] == 1
    assert payload["entry_summary"]["total"] == 2
    legacy = [
        c for c in payload["entry_coverage"]
        if c["state"] == "legacy-enricher-surface"
    ]
    assert len(legacy) == 1 and legacy[0]["kind"] == "enricher"
    assert "okg.substrate.library.linkers._chronos" in legacy[0]["detail"]


def test_in_tree_library_connector_is_exempt_by_location(tmp_path: Path):
    """The in-tree library sources ARE the substrate, so the deny-all rule
    does not apply to them -- and the exemption is STATED, not silent."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_IN_TREE,
        boundary_entry_coverage,
        lint_deployments,
    )

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=None,
        connector_module="okg.substrate.library.sources.github",
    )

    assert lint_deployments(
        deployments_root=root, repo_root=tmp_path,
    ) == []

    coverage = boundary_entry_coverage(
        deployments_root=root, repo_root=tmp_path,
    )
    assert len(coverage) == 1, coverage
    assert coverage[0].state == BOUNDARY_IN_TREE
    assert coverage[0].kind == "connector"
    assert coverage[0].key == "external-feed"
    # Exempted by where the file lives, not by the module's name.
    assert coverage[0].source_path is not None
    assert "okg/substrate/library/sources" in str(coverage[0].source_path)


def test_unresolvable_entry_is_a_coded_refusal(tmp_path: Path):
    """An entry whose module cannot be located statically is refused with a
    stable code -- never dropped into a clean report."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_UNRESOLVED,
        DEPLOYMENT_002,
        boundary_entry_coverage,
        lint_deployments,
    )

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root,
        name="delta",
        connector_source=None,
        connector_module="no_such_connector_module_ekq",
    )

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert len(findings) == 1, findings
    assert findings[0].code == DEPLOYMENT_002
    assert "external-feed" in findings[0].entry
    assert findings[0].importing_symbol == "no_such_connector_module_ekq"

    coverage = boundary_entry_coverage(
        deployments_root=root, repo_root=tmp_path,
    )
    assert [c.state for c in coverage] == [BOUNDARY_UNRESOLVED]


def test_unreadable_source_registry_is_a_coded_refusal(tmp_path: Path):
    """A declared registry lint cannot read blocks; its connector entries
    are not silently absent."""
    from okg.substrate.runtime.lint import DEPLOYMENT_003, lint_deployments

    root = tmp_path / "deployments"
    d = root / "delta"
    d.mkdir(parents=True)
    (d / "deployment.yaml").write_text(
        "name: delta\nsource_registry: source_registry.yaml\n"
    )
    # Declared but never written.

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert [f.code for f in findings] == [DEPLOYMENT_003], findings
    assert "source_registry.yaml" in findings[0].message


def test_entry_scan_never_imports_the_module(tmp_path: Path):
    """The scan is static: a module whose import-time code raises is still
    read. Both connector fixtures raise at module scope."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_SCANNED,
        boundary_entry_coverage,
    )

    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=EXTERNAL_CONNECTOR_VIOLATION,
    )

    coverage = boundary_entry_coverage(
        deployments_root=root, repo_root=tmp_path,
    )
    assert [c.state for c in coverage] == [BOUNDARY_SCANNED]



def _wheel_site(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, ext_modules: dict[str, str],
) -> Path:
    """A site-packages directory holding a fake okg and an external
    distribution, as two wheel installs leave it (issue #2819). okg's own
    modules import okg internals, which are never the deployment's."""
    from okg.substrate.runtime import lint

    site = tmp_path / "site-packages"
    okg_pkg = site / "okg"
    enrichers = okg_pkg / "substrate" / "enrichers"
    facade = okg_pkg / "deployment"
    for pkg in (okg_pkg, okg_pkg / "substrate", enrichers, facade):
        pkg.mkdir(parents=True, exist_ok=True)
        (pkg / "__init__.py").write_text("")
    (enrichers / "base.py").write_text(
        "from okg.substrate.db import connect\n"
        "from okg.substrate.nomos.core import Policy\n"
    )
    (facade / "helpers.py").write_text(
        "from okg.substrate.publisher import Publisher\n"
    )
    ext = site / "extdist"
    ext.mkdir()
    (ext / "__init__.py").write_text("")
    for name, source in ext_modules.items():
        (ext / f"{name}.py").write_text(source)
    monkeypatch.setattr(lint, "_OKG_PACKAGE_ROOT", okg_pkg.resolve())
    return site


def test_wheel_installed_distribution_walk_stops_at_the_okg_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """The distribution's own substrate imports are reported; the walk never
    follows them into okg's modules. On dev the walk also reported the two
    imports inside okg's enrichers/base.py."""
    from okg.substrate.runtime.lint import DEPLOYMENT_001, lint_deployments

    _wheel_site(tmp_path, monkeypatch, {"source": (
        "from okg.substrate.enrichers.base import EnrichResult\n"
        "from okg.substrate.publisher import Publisher\n"
    )})
    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=None,
        connector_module="extdist.source",
    )

    findings = lint_deployments(deployments_root=root, repo_root=tmp_path)

    assert sorted((f.path.name, f.importing_symbol) for f in findings) == [
        ("source.py", "okg.substrate.enrichers.base"),
        ("source.py", "okg.substrate.publisher"),
    ], findings
    assert all(f.code == DEPLOYMENT_001 for f in findings)


def test_wheel_installed_enricher_is_on_the_legacy_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """The comp-ops case: a wheel-installed enricher-only module importing
    okg.substrate.enrichers.base is recorded on the legacy enricher surface,
    and okg's base.py internals are not reported against it."""
    from okg.substrate.runtime.lint import (
        BOUNDARY_LEGACY_ENRICHER,
        lint_deployments_report,
    )

    _wheel_site(tmp_path, monkeypatch, {"rollup": (
        "from okg.substrate.enrichers.base import EnrichResult\n"
        "class Rollup:\n"
        "    pass\n"
    )})
    d = tmp_path / "deployments" / "delta"
    d.mkdir(parents=True)
    (d / "deployment.yaml").write_text(
        "name: delta\nenrichers:\n  - class: extdist.rollup.Rollup\n"
    )

    report = lint_deployments_report(
        deployments_root=tmp_path / "deployments", repo_root=tmp_path,
    )

    assert report.findings == []
    assert [c.state for c in report.entry_coverage] == [
        BOUNDARY_LEGACY_ENRICHER,
    ]
    assert "okg.substrate.enrichers.base" in report.entry_coverage[0].detail


def test_wheel_installed_facade_import_is_not_walked_into_okg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A distribution that imports only okg's public okg.deployment surface
    is clean, even though that surface's own modules import the substrate."""
    from okg.substrate.runtime.lint import lint_deployments

    _wheel_site(tmp_path, monkeypatch, {
        "source": "from okg.deployment.helpers import emit\n",
    })
    root = tmp_path / "deployments"
    _write_registry_deployment(
        root, name="delta", connector_source=None,
        connector_module="extdist.source",
    )

    assert lint_deployments(deployments_root=root, repo_root=tmp_path) == []
