"""Deployment-lint change-probe contract (#323, task registry-fail-closed).

The lint statically reads each runnable source's class-level `change_probe_kind`
(via AST, no import) and:
  - a missing probe is a BLOCKER by default;
  - a declaration without runtime `self.change_probe` wiring is a BLOCKER;
  - a declared stat-digest probe (mtime/size) is a BLOCKER always;
  - registry_only and live_overlay are exempt (closed allowlist);
  - a sound probe (git_scope / content_hash / ...) produces no finding.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate import fastyaml
from okg.substrate.deployment_lint import lint_deployment

_DISCOVERY_TUPLE = (
    "    source_class: discovery_crawl\n"
    "    record_identity_kind: scoped_locator\n"
    "    record_identity_fields: [path]\n"
    "    source_revision_kind: content_hash\n"
    "    deletion_semantics: missing_from_completed_scope\n"
    "    publication_mode: published_generation\n"
)


def _base(deployment_dir: Path, registry_body: str, name: str = "demo") -> None:
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas" / "schema.yaml").write_text(
        "classes: {}\n", encoding="utf-8",
    )
    (deployment_dir / "fixture_probe_base.py").write_text(
        "class ImportedRuntimeBase:\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n",
        encoding="utf-8",
    )
    (deployment_dir / "fixture_probe_sources.py").write_text(
        "import fixture_probe_base as imported_base_module\n"
        "from fixture_probe_base import ImportedRuntimeBase\n\n"
        "class ProbelessSource:\n"
        "    pass\n\n"
        "class SoundGitSource:\n"
        "    change_probe_kind = 'git_scope'\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n\n"
        "class TupleRuntimeSource:\n"
        "    change_probe_kind = 'git_scope'\n"
        "    def __post_init__(self):\n"
        "        self.change_probe, self.snapshot_probe = object(), object()\n\n"
        "class DeclarationOnlySource:\n"
        "    change_probe_kind = 'content_hash'\n\n"
        "class HelperOnlySource:\n"
        "    change_probe_kind = 'content_hash'\n"
        "    def build_probe(self):\n"
        "        self.change_probe = object()\n\n"
        "class NestedConstructorHelperSource:\n"
        "    change_probe_kind = 'content_hash'\n"
        "    def __init__(self):\n"
        "        def build_probe():\n"
        "            self.change_probe = object()\n"
        "        self.name = 'nested'\n\n"
        "class RuntimeBase:\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n\n"
        "class InheritedRuntimeSource(RuntimeBase):\n"
        "    change_probe_kind = 'content_hash'\n\n"
        "class ShadowedInheritedRuntimeSource(RuntimeBase):\n"
        "    change_probe_kind = 'content_hash'\n"
        "    def __init__(self):\n"
        "        self.name = 'shadowed'\n\n"
        "class ImportedRuntimeSource(ImportedRuntimeBase):\n"
        "    change_probe_kind = 'content_hash'\n\n"
        "class ModuleImportedRuntimeSource(imported_base_module.ImportedRuntimeBase):\n"
        "    change_probe_kind = 'content_hash'\n\n"
        "class UnsoundSource:\n"
        "    change_probe_kind = 'mtime'\n\n"
        "class OverlaySource:\n"
        "    pass\n",
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n" + registry_body, encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        f"name: {name}\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        f"  database: okg_{name}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
        encoding="utf-8",
    )


def _probe_findings(report, code):
    return [f for f in report["findings"] if f["code"] == code]


_MIXED_REGISTRY = (
    "  probeless:\n"
    "    module: fixture_probe_sources\n"
    "    class: ProbelessSource\n"
    "    ownership_id: demo.probeless\n" + _DISCOVERY_TUPLE +
    "  sound-git:\n"
    "    module: fixture_probe_sources\n"
    "    class: SoundGitSource\n"
    "    ownership_id: demo.sound-git\n" + _DISCOVERY_TUPLE +
    "  overlay:\n"
    "    module: fixture_probe_sources\n"
    "    class: OverlaySource\n"
    "    ownership_id: demo.overlay\n"
    "    source_class: live_overlay\n"
    "    deletion_semantics: external_overlay\n"
    "    publication_mode: live_overlay\n"
    "  docs-only:\n"
    "    ownership_id: demo.docs-only\n"
    "    source_class: registry_only\n"
)


def test_missing_probe_is_blocker_by_default(tmp_path: Path):
    d = tmp_path / "demo"
    _base(d, _MIXED_REGISTRY)
    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})

    missing = _probe_findings(report, "deployment.source_registry.probe_missing")
    assert len(missing) == 1, "only the probeless source should be flagged"
    assert missing[0]["severity"] == "blocker"
    assert "probeless" in missing[0]["message"]
    assert report["ok"] is False


def test_explicitly_dormant_missing_probe_is_warning_until_activation(
    tmp_path: Path,
):
    d = tmp_path / "demo"
    registry = (
        "  probeless:\n"
        "    module: fixture_probe_sources\n"
        "    class: ProbelessSource\n"
        "    ownership_id: demo.probeless\n"
        "    enabled_by_default: false\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"},
    )

    missing = _probe_findings(
        report, "deployment.source_registry.probe_missing",
    )
    assert len(missing) == 1
    assert missing[0]["severity"] == "warning"
    assert report["ok"] is True


def test_dormant_unsound_probe_remains_blocking(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  bad-probe:\n"
        "    module: fixture_probe_sources\n"
        "    class: UnsoundSource\n"
        "    ownership_id: demo.bad-probe\n"
        "    enabled_by_default: false\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"},
    )

    unsound = _probe_findings(
        report, "deployment.source_registry.probe_unsound",
    )
    assert len(unsound) == 1
    assert unsound[0]["severity"] == "blocker"
    assert report["ok"] is False


@pytest.mark.parametrize(
    "probe",
    ["stat_tree", "content_hash", "ttl", "remote_etag"],
)
def test_dormant_explicit_unsound_poll_selector_remains_blocking(
    tmp_path: Path,
    probe: str,
) -> None:
    d = tmp_path / "demo"
    registry = _poll_registry(probe).replace(
        "    ownership_id: demo.poller\n",
        "    ownership_id: demo.poller\n"
        "    enabled_by_default: false\n",
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"},
    )

    poll = _probe_findings(
        report, "deployment.source_registry.poll_probe_unsound",
    )
    assert len(poll) == 1
    assert poll[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_activation_blocks_unresolvable_adapter_module(tmp_path: Path):
    from okg.substrate.deployment_lint import (
        source_activation_probe_findings,
    )

    d = tmp_path / "demo"
    _base(d, _MIXED_REGISTRY)
    findings = source_activation_probe_findings(
        "missing-module",
        {
            "module": "fixture_module_that_does_not_exist",
            "class": "MissingSource",
            "ownership_id": "demo.missing-module",
            "enabled_by_default": True,
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
        },
        deployment_dir=d,
        registry_path=d / "source_registry.yaml",
    )

    assert [finding["code"] for finding in findings] == [
        "deployment.source_registry.module_not_found",
    ]


@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        ("module", None, "deployment.source_registry.module_missing"),
        ("module", "   ", "deployment.source_registry.module_missing"),
        ("class", None, "deployment.source_registry.class_missing"),
        ("class", "   ", "deployment.source_registry.class_missing"),
    ],
)
def test_activation_blocks_missing_or_blank_adapter_identity(
    tmp_path: Path,
    field: str,
    value: str | None,
    expected_code: str,
):
    from okg.substrate.deployment_lint import (
        source_activation_probe_findings,
    )

    d = tmp_path / "demo"
    _base(d, _MIXED_REGISTRY)
    body = {
        "module": "fixture_probe_sources",
        "class": "SoundGitSource",
        "ownership_id": "demo.invalid",
        "enabled_by_default": True,
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    if value is None:
        body.pop(field)
    else:
        body[field] = value

    findings = source_activation_probe_findings(
        "invalid",
        body,
        deployment_dir=d,
        registry_path=d / "source_registry.yaml",
    )

    assert expected_code in {
        finding["code"] for finding in findings
    }


def test_sound_and_exempt_sources_produce_no_probe_finding(tmp_path: Path):
    d = tmp_path / "demo"
    _base(d, _MIXED_REGISTRY)
    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})

    probe_codes = {
        "deployment.source_registry.probe_missing",
        "deployment.source_registry.probe_runtime_missing",
        "deployment.source_registry.probe_unsound",
    }
    flagged = [
        f["message"] for f in report["findings"] if f["code"] in probe_codes
    ]
    joined = " ".join(flagged)
    assert "sound-git" not in joined  # has a sound probe
    assert "overlay" not in joined  # live_overlay exempt
    assert "docs-only" not in joined  # registry_only exempt


def test_tuple_runtime_probe_assignment_satisfies_contract(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  tuple-runtime:\n"
        "    module: fixture_probe_sources\n"
        "    class: TupleRuntimeSource\n"
        "    ownership_id: demo.tuple-runtime\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )
    assert not _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert report["ok"] is True


def test_legacy_env_flag_cannot_reopen_missing_probe_contract(tmp_path: Path):
    d = tmp_path / "demo"
    _base(d, _MIXED_REGISTRY)
    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
            "OKG_ENFORCE_SOURCE_PROBE": "0",
        },
    )

    missing = _probe_findings(report, "deployment.source_registry.probe_missing")
    assert len(missing) == 1
    assert missing[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_declaration_only_runtime_probe_is_enforced(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  declaration-only:\n"
        "    module: fixture_probe_sources\n"
        "    class: DeclarationOnlySource\n"
        "    ownership_id: demo.declaration-only\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})
    runtime = _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert len(runtime) == 1
    assert runtime[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_helper_only_runtime_probe_assignment_is_not_enough(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  helper-only:\n"
        "    module: fixture_probe_sources\n"
        "    class: HelperOnlySource\n"
        "    ownership_id: demo.helper-only\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )

    runtime = _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert len(runtime) == 1
    assert runtime[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_nested_constructor_helper_probe_assignment_is_not_enough(
    tmp_path: Path,
):
    d = tmp_path / "demo"
    registry = (
        "  nested-helper:\n"
        "    module: fixture_probe_sources\n"
        "    class: NestedConstructorHelperSource\n"
        "    ownership_id: demo.nested-helper\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )

    runtime = _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert len(runtime) == 1
    assert runtime[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_shadowed_base_constructor_runtime_probe_is_not_enough(
    tmp_path: Path,
):
    d = tmp_path / "demo"
    registry = (
        "  shadowed-runtime:\n"
        "    module: fixture_probe_sources\n"
        "    class: ShadowedInheritedRuntimeSource\n"
        "    ownership_id: demo.shadowed-runtime\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )

    runtime = _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert len(runtime) == 1
    assert runtime[0]["severity"] == "blocker"
    assert report["ok"] is False


def test_same_module_base_runtime_probe_satisfies_contract(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  inherited-runtime:\n"
        "    module: fixture_probe_sources\n"
        "    class: InheritedRuntimeSource\n"
        "    ownership_id: demo.inherited-runtime\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )
    assert not _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert report["ok"] is True


def test_imported_base_runtime_probe_satisfies_contract(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  imported-runtime:\n"
        "    module: fixture_probe_sources\n"
        "    class: ImportedRuntimeSource\n"
        "    ownership_id: demo.imported-runtime\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )
    assert not _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert report["ok"] is True


def test_module_imported_base_runtime_probe_satisfies_contract(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  module-imported-runtime:\n"
        "    module: fixture_probe_sources\n"
        "    class: ModuleImportedRuntimeSource\n"
        "    ownership_id: demo.module-imported-runtime\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)

    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )
    assert not _probe_findings(
        report, "deployment.source_registry.probe_runtime_missing",
    )
    assert report["ok"] is True


def test_unsound_stat_digest_probe_is_blocker_even_in_advisory(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  bad-probe:\n"
        "    module: fixture_probe_sources\n"
        "    class: UnsoundSource\n"
        "    ownership_id: demo.bad-probe\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)
    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})

    unsound = _probe_findings(report, "deployment.source_registry.probe_unsound")
    assert len(unsound) == 1
    assert unsound[0]["severity"] == "blocker"
    assert "mtime" in unsound[0]["message"]
    assert report["ok"] is False  # an unsound probe is rejected regardless of mode


def test_sound_only_deployment_is_clean(tmp_path: Path):
    d = tmp_path / "demo"
    registry = (
        "  sound-git:\n"
        "    module: fixture_probe_sources\n"
        "    class: SoundGitSource\n"
        "    ownership_id: demo.sound-git\n" + _DISCOVERY_TUPLE
    )
    _base(d, registry)
    report = lint_deployment(
        d,
        env={
            "OKG_DSN": "postgres://u:p@h:5432/okg_demo",
        },
    )
    probe_codes = {
        "deployment.source_registry.probe_missing",
        "deployment.source_registry.probe_runtime_missing",
        "deployment.source_registry.probe_unsound",
    }
    assert not [f for f in report["findings"] if f["code"] in probe_codes]
    assert report["ok"] is True  # sound source passes even when enforcing


def test_resolve_import_module_handles_package_init_reexport():
    # A source registered by its PACKAGE (`sources.findings`) whose __init__
    # re-exports the class from a submodule (`from .findings import FindingsSource`)
    # must resolve to the submodule `sources.findings.findings`, not back to the
    # package itself. For a package __init__ a level-1 relative import strips one
    # fewer level than for a regular module.
    import ast

    from okg.substrate.deployment_lint import _resolve_import_module

    node = ast.ImportFrom(module="findings", names=[ast.alias("FindingsSource")], level=1)
    # package __init__ of `sources.findings`
    assert _resolve_import_module(
        node, "sources.findings", is_package=True
    ) == "sources.findings.findings"
    # a regular module `sources.foo`: `from .bar import X` -> sibling `sources.bar`
    sib = ast.ImportFrom(module="bar", names=[ast.alias("X")], level=1)
    assert _resolve_import_module(sib, "sources.foo", is_package=False) == "sources.bar"


def _poll_registry(probe: str | None) -> str:
    probe_line = f"        probe: {probe}\n" if probe is not None else ""
    return (
        "  poller:\n"
        "    module: fixture_probe_sources\n"
        "    class: SoundGitSource\n"
        "    ownership_id: demo.poller\n" + _DISCOVERY_TUPLE +
        "    sync:\n"
        "      triggers: [poll]\n"
        "      poll:\n"
        + probe_line +
        "        interval_seconds: 300\n"
    )


@pytest.mark.parametrize(
    "probe",
    ["stat_tree", "content_hash", "ttl", "remote_etag", None],
)
def test_legacy_or_missing_poll_probe_is_flagged_unsound(
    tmp_path: Path,
    probe: str | None,
):
    d = tmp_path / "demo"
    _base(d, _poll_registry(probe))
    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})
    poll = _probe_findings(report, "deployment.source_registry.poll_probe_unsound")
    assert len(poll) == 1
    assert poll[0]["severity"] == "blocker"
    assert report["ok"] is False


@pytest.mark.parametrize("probe", ["change_probe", "always_changed"])
def test_closed_poll_probe_contract_is_clean(tmp_path: Path, probe: str):
    d = tmp_path / "demo"
    _base(d, _poll_registry(probe))
    report = lint_deployment(d, env={"OKG_DSN": "postgres://u:p@h:5432/okg_demo"})
    assert not _probe_findings(report, "deployment.source_registry.poll_probe_unsound")


def test_pinned_poll_registry_inventory_uses_closed_contract() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    deployments = repo_root / "deployments"
    rows: list[tuple[str, str, str | None]] = []
    for deployment in ("okg-ci", "okg-workspace", "synth-bench"):
        registry = fastyaml.safe_load(
            (deployments / deployment / "source_registry.yaml").read_text(
                encoding="utf-8",
            )
        )
        for source_name, body in registry["sources"].items():
            sync = body.get("sync") or {}
            if "poll" not in (sync.get("triggers") or []):
                continue
            rows.append((
                deployment,
                source_name,
                (sync.get("poll") or {}).get("probe"),
            ))

    # 22 -> 20 and 19 -> 17 change_probe, 2026-08-24: okg-workspace
    # quarantined compute-env-remote and pact-coordination-events (each failed
    # on every run and contributed zero data), and both were poll sources on
    # change_probe. Measured from the registries, not adjusted arithmetically.
    # Restore both counts when those entries are uncommented in
    # deployments/okg-workspace/source_registry.yaml. The always_changed set
    # below is unaffected -- it is entirely okg-ci.
    assert len(rows) == 20
    assert sum(probe == "change_probe" for _, _, probe in rows) == 17
    assert {
        (deployment, source)
        for deployment, source, probe in rows
        if probe == "always_changed"
    } == {
        ("okg-ci", "github-actions"),
        ("okg-ci", "github-issues"),
        ("okg-ci", "test-reports"),
    }
    assert {probe for _, _, probe in rows} == {
        "always_changed",
        "change_probe",
    }
    shared_manifest = fastyaml.safe_load(
        (deployments / "okg-ci-dev" / "deployment.yaml").read_text(
            encoding="utf-8",
        )
    )
    assert shared_manifest["source_registry"] == "../okg-ci/source_registry.yaml"
