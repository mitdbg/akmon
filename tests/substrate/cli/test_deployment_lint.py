from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.mirror_scope import is_public_copy
from tests.private_deployments import names

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEPLOYMENTS_ROOT = _REPO_ROOT / "deployments"
_IN_REPO_DEPLOYMENTS = [
    "okg-workspace",
    "okg-ci",
    *names("retired_prototype"),
    "beaver-okg",
    "synth-bench",
]
#: The deployments above that ship in the public copy of this repository.
#: The others are this repository's own and are left out of it.
_SHIPPED_IN_REPO_DEPLOYMENTS = ["beaver-okg", "synth-bench"]


def _write_valid_deployment(root: Path, name: str = "demo") -> Path:
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas" / "schema.yaml").write_text(
        "classes: {}\n",
        encoding="utf-8",
    )
    (deployment_dir / "fixture_sources.py").write_text(
        "raise RuntimeError('lint imported source module')\n\n"
        "class DemoSource:\n"
        # Declares both halves of the incremental-source change-probe contract
        # so the valid fixture is contract-compliant; lint reads this via AST.
        "    change_probe_kind = 'content_hash'\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n",
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  demo-source:\n"
        "    module: fixture_sources\n"
        "    class: DemoSource\n"
        "    ownership_id: demo.demo-source\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        # A valid deployment declares its readiness profile (#3958).
        "readiness:\n"
        "  profile: standard\n",
        encoding="utf-8",
    )
    return deployment_dir


def test_lint_report_is_stable_db_free_and_non_instantiating(tmp_path: Path):
    from okg.substrate.deployment_lint import (
        LINT_SCHEMA,
        format_diagnostic_summary,
        lint_deployment,
    )

    deployment_dir = _write_valid_deployment(tmp_path)

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    assert report["schema"] == LINT_SCHEMA
    assert report["mode"] == "static"
    assert report["db_required"] is False
    assert report["ok"] is True
    assert {
        finding["code"] for finding in report["findings"]
    } == {
        "deployment.live_checks.skipped",
        # A deployment with no nomos.deployment_posture is surfaced as a
        # governance gap (warning, not a blocker — ok stays True).
        "deployment.nomos.deployment_posture_missing",
        # demo-source declares no admission_policy, so it has no producer write
        # envelope — surfaced as an advisory (info) gap, never a blocker
        # (spec §9.3, PACT consistency-output-envelopes).
        "producer.envelope_missing",
        # This fixture activates no record-template subtypes, so the record
        # reference-route check examined ZERO fields. That is reported as a
        # distinct `skipped` state rather than silence: "evaluated nothing" must
        # never read as "nothing wrong" (PACT record-route-deployment-diagnostic).
        "deployment.record.reference_routes_not_applicable",
    }
    assert "lint imported source module" not in json.dumps(report)
    assert "deployment lint:" in format_diagnostic_summary(report)


def test_cli_json_refuses_misplaced_narrowing_schema(
    tmp_path: Path,
    capsys,
) -> None:
    from okg.substrate.cli.main import main

    deployment_dir = _write_valid_deployment(tmp_path)
    schema_path = deployment_dir / "schemas" / "misplaced.yaml"
    schema_path.write_text(
        "classes:\n"
        "  EdgeNarrowings:\n"
        "    annotations:\n"
        "      okg_edge_narrowings: 'true'\n"
        "    attributes: {}\n",
        encoding="utf-8",
    )

    rc = main(["deployment", "lint", str(deployment_dir), "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])

    assert rc == 1
    assert payload["ok"] is False
    finding = next(
        item
        for item in payload["findings"]
        if item["code"] == "deployment.catalog.narrowing_schema_misplaced"
    )
    expected_path = deployment_dir / "schemas" / "bridges" / "misplaced.yaml"
    assert finding["severity"] == "blocker"
    assert finding["stage"] == "catalog"
    assert finding["location"]["path"] == str(schema_path)
    assert finding["details"] == {
        "schema_path": str(schema_path),
        "expected_path": str(expected_path),
        "schema_dir": str(deployment_dir / "schemas"),
    }
    assert "instead of overwriting" in finding["next_action"]


def test_cli_json_refuses_symlinked_bridges_without_target_read(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli.main import main

    deployment_dir = _write_valid_deployment(tmp_path)
    external_dir = tmp_path / "external-bridges"
    external_dir.mkdir()
    (external_dir / "external.yaml").write_text(
        "classes:\n"
        "  EdgeNarrowings:\n"
        "    annotations:\n"
        "      okg_edge_narrowings: 'true'\n"
        "    attributes: {}\n",
        encoding="utf-8",
    )
    schema_dir = deployment_dir / "schemas"
    bridges_path = schema_dir / "bridges"
    bridges_path.symlink_to(external_dir, target_is_directory=True)
    external_path = external_dir / "external.yaml"
    external_lexical_path = bridges_path / "external.yaml"
    original_glob = Path.glob
    original_open = Path.open

    def guarded_glob(path: Path, pattern: str):
        if path == bridges_path:
            pytest.fail("symlinked bridge target was listed")
        return original_glob(path, pattern)

    def guarded_open(path: Path, *args, **kwargs):
        if path in {external_path, external_lexical_path}:
            pytest.fail("symlinked bridge target was read")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", guarded_glob)
    monkeypatch.setattr(Path, "open", guarded_open)

    rc = main(["deployment", "lint", str(deployment_dir), "--json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])

    assert rc == 1
    assert payload["ok"] is False
    finding = next(
        item
        for item in payload["findings"]
        if item["code"]
        == "deployment.catalog.narrowing_bridge_directory_symlink"
    )
    assert finding["severity"] == "blocker"
    assert finding["stage"] == "catalog"
    assert finding["location"]["path"] == str(bridges_path)
    assert finding["details"] == {
        "bridges_path": str(bridges_path),
        "schema_dir": str(schema_dir),
    }
    assert "replace" in finding["next_action"]
    assert "real directory" in finding["next_action"]


def test_lint_accepts_from_import_reexported_source_symbols(tmp_path: Path):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "actual_source.py").write_text(
        "class ReExportedSource:\n"
        "    pass\n",
        encoding="utf-8",
    )
    (deployment_dir / "fixture_reexport.py").write_text(
        "from actual_source import ReExportedSource\n",
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  demo-source:\n"
        "    module: fixture_reexport\n"
        "    class: ReExportedSource\n"
        "    ownership_id: demo.demo-source\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
        encoding="utf-8",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {finding["code"] for finding in report["findings"]}
    assert "deployment.source_registry.class_not_found" not in codes


def test_lint_module_resolution_does_not_execute_package_init(
    tmp_path: Path,
    monkeypatch,
):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    marker = tmp_path / "imported.txt"
    package = tmp_path / "pkg"
    package.mkdir()
    (package / "__init__.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('boom')\n",
        encoding="utf-8",
    )
    (package / "mod.py").write_text(
        "class PackageSource:\n"
        "    change_probe_kind = 'content_hash'\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n",
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  demo-source:\n"
        "    module: pkg.mod\n"
        "    class: PackageSource\n"
        "    ownership_id: demo.demo-source\n"
        "    source_class: discovery_crawl\n"
        "    record_identity_kind: scoped_locator\n"
        "    record_identity_fields: [path]\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    assert report["ok"] is True
    assert not marker.exists()


def test_lint_live_mode_reports_unimplemented_live_checks(tmp_path: Path):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
        live=True,
    )

    assert report["mode"] == "live"
    assert report["db_required"] is True
    assert {
        finding["code"] for finding in report["findings"]
    } == {
        "deployment.live_checks.not_implemented",
        "deployment.nomos.deployment_posture_missing",
        # demo-source declares no admission_policy, so it has no producer write
        # envelope — an advisory (info) gap (spec §9.3).
        "producer.envelope_missing",
        # No record-template subtypes are active here, so the record
        # reference-route check examined zero fields and says so explicitly
        # (PACT record-route-deployment-diagnostic).
        "deployment.record.reference_routes_not_applicable",
    }


def test_lint_collects_manifest_source_enricher_and_catalog_findings(
    tmp_path: Path,
):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = tmp_path / "bad"
    deployment_dir.mkdir()
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources:\n"
        "  broken:\n"
        "    module: missing_module\n"
        "    class: MissingSource\n"
        "    ownership_id: bad.broken\n"
        "    source_class: not_a_profile\n"
        "    record_identity_kind: scoped_locator\n"
        "    source_revision_kind: content_hash\n"
        "    deletion_semantics: missing_from_completed_scope\n"
        "    publication_mode: published_generation\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        "name: bad\n"
        "postgres:\n"
        "  dsn: ${MISSING_DSN}\n"
        "schema_dir: missing-schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: [missing_catalog_module]\n"
        "enrichers:\n"
        "  - class: not.a.RealEnricher\n",
        encoding="utf-8",
    )

    report = lint_deployment(deployment_dir, env={})
    codes = [finding["code"] for finding in report["findings"]]

    assert report["ok"] is False
    assert "deployment.manifest.schema_dir_not_found" in codes
    assert "deployment.env.required_missing" in codes
    assert "deployment.source_registry.profile_invalid" in codes
    assert "deployment.source_registry.module_not_found" in codes
    assert "deployment.enrichers.module_not_found" in codes
    assert any(code.startswith("deployment.catalog.") for code in codes)


def test_lint_treats_declared_unset_env_authority_as_info(tmp_path: Path):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "contract:\n"
        "  environment:\n"
        "    required:\n"
        "      OKG_DSN:\n"
        "        purpose: postgres.dsn\n"
        "        source: operator_secret\n"
        "        owner: okg-operators\n"
        "        review_by: '2999-01-01'\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
        encoding="utf-8",
    )

    report = lint_deployment(deployment_dir, env={})
    findings = {finding["code"]: finding for finding in report["findings"]}

    assert "deployment.env.required_missing" not in findings
    declared = findings["deployment.env.required_declared_unset"]
    assert declared["severity"] == "info"
    assert declared["observed"] == ["OKG_DSN"]
    assert declared["details"]["authorities"]["OKG_DSN"] == {
        "purpose": "postgres.dsn",
        "source": "operator_secret",
        "owner": "okg-operators",
        "review_by": "2999-01-01",
    }


def test_lint_keeps_warning_for_invalid_env_authority(tmp_path: Path):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "contract:\n"
        "  environment:\n"
        "    required:\n"
        "      OKG_DSN:\n"
        "        purpose: postgres.dsn\n"
        "        source: unsupported\n"
        "        owner: okg-operators\n"
        "        review_by: '2999-01-01T00:00:00Z'\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
        encoding="utf-8",
    )

    report = lint_deployment(deployment_dir, env={})
    codes = {finding["code"] for finding in report["findings"]}
    authority = next(
        finding for finding in report["findings"]
        if finding["code"] == "deployment.env.authority_invalid"
    )

    assert "deployment.env.authority_invalid" in codes
    assert "deployment.env.required_missing" in codes
    assert "deployment.env.required_declared_unset" not in codes
    assert "review_by must be an ISO YYYY-MM-DD date" in authority["observed"]


def test_lint_validates_declared_env_authority_even_when_env_is_set(
    tmp_path: Path,
):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "contract:\n"
        "  environment:\n"
        "    required:\n"
        "      OKG_DSN:\n"
        "        purpose: postgres.dsn\n"
        "        source: unsupported\n"
        "        owner: okg-operators\n"
        "        review_by: '2999-01-01'\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
        encoding="utf-8",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://user:pass@localhost:5432/demo"},
    )
    codes = {finding["code"] for finding in report["findings"]}

    assert "deployment.env.authority_invalid" in codes
    assert "deployment.env.required_missing" not in codes
    assert "deployment.env.required_declared_unset" not in codes


def test_lint_uses_legacy_postgres_dsn_pointer_for_env_findings(
    tmp_path: Path,
):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "contract:\n"
        "  environment:\n"
        "    required:\n"
        "      OKG_DSN:\n"
        "        purpose: postgres.dsn\n"
        "        source: worktree_main_local_env\n"
        "        owner: okg-operators\n"
        "        review_by: '2999-01-01'\n"
        "postgres_dsn: ${OKG_DSN}\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
        encoding="utf-8",
    )

    report = lint_deployment(deployment_dir, env={})
    findings = {finding["code"]: finding for finding in report["findings"]}
    declared = findings["deployment.env.required_declared_unset"]

    assert declared["location"]["pointer"] == "/postgres_dsn"
    assert declared["details"]["authorities"]["OKG_DSN"]["source"] == (
        "worktree_main_local_env"
    )


@pytest.mark.parametrize("deployment_name", _IN_REPO_DEPLOYMENTS)
def test_lint_passes_for_in_repo_deployments(deployment_name: str):
    """Every committed deployment must pass static lint with ok=True.

    Module-only deployments (beaver-okg, synth-bench) ship a schemas/
    dir whose only tracked entry is a `.gitkeep` placeholder; lint's
    schema_dir directory-existence check is satisfied and catalog
    composition contributes no local subtypes (the ontology comes
    from `modules:`).
    """
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _DEPLOYMENTS_ROOT / deployment_name
    assert deployment_dir.is_dir(), deployment_dir

    report = lint_deployment(
        deployment_dir,
        env={
            "OKG_DSN": "postgres://u:p@localhost:5432/okg",
            "BEAVER_OKG_DSN": "postgres://u:p@localhost:5432/okg_beaver",
        },
    )

    blockers = [
        finding["code"]
        for finding in report["findings"]
        if finding["severity"] == "blocker"
    ]
    assert report["ok"] is True, blockers
    assert "deployment.manifest.schema_dir_not_found" not in blockers


# req.gov.lint-and-doctor — the static posture audit runs inside lint.


def test_lint_flags_missing_deployment_posture(tmp_path: Path):
    """A deployment with no nomos.deployment_posture is surfaced as a gap."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    # _write_valid_deployment ships no nomos block at all.
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )
    codes = {f["code"] for f in report["findings"]}
    assert "deployment.nomos.deployment_posture_missing" in codes
    # A posture gap is a finding, NOT a hard block — ok stays True for an
    # otherwise-valid deployment so the in-repo contract lint stays green.
    assert report["ok"] is True


def test_lint_does_not_flag_posture_for_deferred_rollout(tmp_path: Path):
    """A deferred rollout (its own bounded-governance path) is exempt."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    (deployment_dir / "deployment.yaml").write_text(
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        "nomos:\n"
        "  rollout:\n"
        "    status: deferred\n"
        "    owner:\n"
        "      name: team\n"
        "      contact: t@x\n"
        "    review_by: '2027-01-01'\n",
        encoding="utf-8",
    )
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )
    codes = {f["code"] for f in report["findings"]}
    assert "deployment.nomos.deployment_posture_missing" not in codes


def test_lint_posture_present_deployment_has_no_posture_gap():
    """The audit-mode in-repo deployments DO have postures (no gap)."""
    from okg.substrate.deployment_lint import lint_deployment

    for name in ("beaver-okg", *names("retired_prototype")):
        report = lint_deployment(_DEPLOYMENTS_ROOT / name)
        nomos_codes = {
            f["code"] for f in report["findings"] if f["stage"] == "nomos"
        }
        assert nomos_codes == set(), (name, nomos_codes)


# req.lint.contract-blockers — lint emits a specific blocker per invariant.


def _write_deployment_yaml(deployment_dir: Path, body: str) -> None:
    (deployment_dir / "deployment.yaml").write_text(body, encoding="utf-8")


def test_lint_requires_explicit_model_enricher_quarantine_limit(tmp_path: Path):
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    base = (
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers:\n"
        "  - class: okg.substrate.library.enrichers.extraction."
        "claim_extractor.ClaimExtractor\n"
        "    config:\n"
        "      confidence_floor: 0.6\n"
        "      llm_model: fixture/model\n"
    )
    _write_deployment_yaml(deployment_dir, base)
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )
    assert "deployment.enrichers.input_quarantine_limit_missing" in {
        finding["code"] for finding in report["findings"]
    }

    _write_deployment_yaml(
        deployment_dir,
        base + "      input_quarantine_max_attempts: 3\n",
    )
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )
    assert "deployment.enrichers.input_quarantine_limit_missing" not in {
        finding["code"] for finding in report["findings"]
    }

    no_model = base.replace("      llm_model: fixture/model\n", "")
    _write_deployment_yaml(deployment_dir, no_model)
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )
    assert "deployment.enrichers.input_quarantine_limit_missing" not in {
        finding["code"] for finding in report["findings"]
    }


def test_repository_model_enricher_manifests_author_retry_limit() -> None:
    """Inventory is intentionally explicit even when no lane is enabled."""
    import yaml

    shared = {
        "okg.substrate.library.enrichers.extraction.claim_extractor.ClaimExtractor",
        "okg.substrate.library.enrichers.extraction.concept_extractor.ConceptExtractor",
        "okg.substrate.library.enrichers.extraction."
        "evidence_extractor.EvidenceExtractor",
        "okg.substrate.library.enrichers.extraction."
        "hypothesis_extractor.HypothesisExtractor",
    }
    enabled: list[tuple[Path, dict]] = []
    for manifest_path in sorted(_DEPLOYMENTS_ROOT.glob("*/deployment.yaml")):
        raw = yaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
        for spec in raw.get("enrichers") or []:
            if not isinstance(spec, dict):
                continue
            config = spec.get("config") or {}
            if (
                str(spec.get("class", "")) in shared
                and isinstance(config, dict)
                and bool(config.get("llm_model") or config.get("llm_canary"))
            ):
                enabled.append((manifest_path, spec))

    missing = [
        str(path)
        for path, spec in enabled
        if not isinstance(
            (spec.get("config") or {}).get("input_quarantine_max_attempts"),
            int,
        )
        or isinstance(
            (spec.get("config") or {}).get("input_quarantine_max_attempts"),
            bool,
        )
        or (spec.get("config") or {}).get("input_quarantine_max_attempts", 0) <= 0
    ]
    assert missing == []
    if not enabled:
        # Current repository fact: shared model-backed extractors are library
        # surfaces but no checked-in deployment enables one. Constructor,
        # lint, and loader fixtures prove the no-default contract directly.
        assert enabled == []


def test_lint_blocks_on_unknown_top_level_key(tmp_path: Path):
    """An unknown top-level key is a specific blocker, not a crash."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    _write_deployment_yaml(
        deployment_dir,
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        # `not_a_real_block` is not in the known-key census; the strict
        # manifest model RAISES on it. Lint must convert that to a finding.
        "not_a_real_block:\n"
        "  some: value\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    assert report["ok"] is False
    assert "deployment.manifest.unknown_key" in codes


def test_lint_blocks_on_missing_schema_dir(tmp_path: Path):
    """A schema_dir pointing at a missing path is a specific blocker."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    _write_deployment_yaml(
        deployment_dir,
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: does-not-exist\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    assert report["ok"] is False
    assert "deployment.manifest.schema_dir_not_found" in codes


def test_lint_blocks_on_runtime_enabled_without_database(tmp_path: Path):
    """runtime.enabled without postgres.database is a specific blocker."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    _write_deployment_yaml(
        deployment_dir,
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"  # no database: declared
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        "runtime:\n"
        "  enabled: true\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    assert report["ok"] is False
    assert "deployment.runtime.database_undeclared" in codes


def test_lint_allows_runtime_enabled_with_database(tmp_path: Path):
    """runtime.enabled WITH a declared database does not blocker-fire."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    _write_deployment_yaml(
        deployment_dir,
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        "runtime:\n"
        "  enabled: true\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    assert "deployment.runtime.database_undeclared" not in codes


def test_lint_blocks_on_declared_but_missing_optional_path(tmp_path: Path):
    """A declared-but-missing optional registry path is a specific blocker."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    _write_deployment_yaml(
        deployment_dir,
        "name: demo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        # enricher_registry is an OPTIONAL declared path; the strict model
        # RAISES because the file does not exist. Lint must surface that.
        "enricher_registry: does-not-exist.yaml\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    assert report["ok"] is False
    assert "deployment.manifest.declared_path_missing" in codes


def test_lint_blocks_on_unexpected_model_load_error(tmp_path: Path, monkeypatch):
    """An UNEXPECTED model exception (not the ValueError / DeploymentConfigError
    cases other checks own) must fail the lint gate loudly with a structured
    blocker, not be swallowed by a bare ``return`` and pass green."""
    from okg.substrate import deployments as deployments_mod
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)

    def _raise_runtime_error(_manifest_path):
        raise RuntimeError("simulated unexpected model-load failure")

    monkeypatch.setattr(
        deployments_mod.DeploymentManifest,
        "from_yaml",
        classmethod(lambda cls, manifest_path: _raise_runtime_error(manifest_path)),
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    findings_by_code = {f["code"]: f for f in report["findings"]}
    assert report["ok"] is False
    assert "deployment.manifest.model_load_error" in findings_by_code
    finding = findings_by_code["deployment.manifest.model_load_error"]
    assert finding["severity"] == "blocker"
    assert "RuntimeError" in finding["observed"]
    assert "simulated unexpected model-load failure" in finding["observed"]


def test_lint_does_not_double_report_missing_required_key(tmp_path: Path):
    """A missing required key (ValueError) stays owned by the path/env checks —
    the model check must NOT also emit a model_load_error blocker for it."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    # Drop the required `name` key so DeploymentManifest.from_yaml raises a
    # ValueError (missing required key), which is owned by other checks.
    _write_deployment_yaml(
        deployment_dir,
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        "  database: okg_demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n",
    )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    codes = {f["code"] for f in report["findings"]}
    # The model check must not double-report the ValueError as a model_load_error.
    assert "deployment.manifest.model_load_error" not in codes


def test_lint_missing_posture_stays_warning_not_blocker(tmp_path: Path):
    """A missing posture is a finding (warning) but does not block ok."""
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    # _write_valid_deployment ships no nomos block at all.
    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    posture = [
        f
        for f in report["findings"]
        if f["code"] == "deployment.nomos.deployment_posture_missing"
    ]
    assert posture, "missing-posture finding should be present"
    assert posture[0]["severity"] == "warning"
    # Reconciled per the contract: a posture gap is surfaced but does not
    # regress the in-repo deployments to ok=false.
    assert report["ok"] is True


# req.lint.always-on — lint runs as an always-on gate over deployments/*.


def _lint_all_deployments(deployments_root: Path) -> dict[str, dict]:
    """Run static lint over every deployment dir under ``deployments_root``."""
    from okg.substrate.deployment_lint import lint_deployment

    reports: dict[str, dict] = {}
    for child in sorted(deployments_root.iterdir()):
        if not (child / "deployment.yaml").is_file():
            continue
        reports[child.name] = lint_deployment(
            child,
            env={
                "OKG_DSN": "postgres://u:p@localhost:5432/okg",
                "BEAVER_OKG_DSN": "postgres://u:p@localhost:5432/okg_beaver",
            },
        )
    return reports


def test_lint_gate_is_green_over_all_in_repo_deployments():
    """The always-on gate is GREEN today over every in-repo deployment."""
    reports = _lint_all_deployments(_DEPLOYMENTS_ROOT)

    # The gate must actually see all six committed deployments, not silently
    # lint zero of them.
    expected = _SHIPPED_IN_REPO_DEPLOYMENTS if is_public_copy() else _IN_REPO_DEPLOYMENTS
    assert set(reports) >= set(expected), set(reports)

    offenders = {
        name: [
            f["code"]
            for f in report["findings"]
            if f["severity"] == "blocker"
        ]
        for name, report in reports.items()
        if not report["ok"]
    }
    assert offenders == {}, offenders


def test_lint_gate_goes_red_on_a_contract_violating_fixture(tmp_path: Path):
    """The gate goes RED when a deliberately broken deployment is present.

    Construct a deployments root that mirrors a valid deployment plus one
    contract-violating deployment, point the gate at it, and assert the gate
    fails (proving it actually catches violations rather than rubber-stamping).
    """
    deployments_root = tmp_path / "deployments"
    deployments_root.mkdir()

    # A valid deployment so the gate has something green to contrast against.
    _write_valid_deployment(deployments_root, name="good")

    # A contract-violating deployment: runtime.enabled with no database.
    broken = _write_valid_deployment(deployments_root, name="broken")
    _write_deployment_yaml(
        broken,
        "name: broken\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"  # runtime enabled but no database declared
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: []\n"
        "enrichers: []\n"
        "runtime:\n"
        "  enabled: true\n",
    )

    reports = _lint_all_deployments(deployments_root)

    assert reports["good"]["ok"] is True, reports["good"]
    assert reports["broken"]["ok"] is False
    broken_codes = {f["code"] for f in reports["broken"]["findings"]}
    assert "deployment.runtime.database_undeclared" in broken_codes

    # The gate aggregate (any deployment red -> gate red) must be red.
    gate_ok = all(report["ok"] for report in reports.values())
    assert gate_ok is False


def test_ready_explain_adapter_is_additive_and_preserves_ready_payload():
    from okg.substrate.deployment_lint import attach_ready_explain_diagnostics

    ready_payload = {
        "ok": False,
        "deployment": "demo",
        "environment": {
            "ok": False,
            "manifest_path": "/tmp/demo/deployment.yaml",
            "unresolved": ["OKG_DSN"],
            "recommended_commands": [
                "uv run okg deployment env demo --json",
            ],
        },
        "checks": [
            {
                "id": "database.live_checks",
                "status": "skipped",
                "message": "live checks require a DSN",
            }
        ],
        "blockers": [],
        "warnings": [],
    }

    explained = attach_ready_explain_diagnostics(ready_payload)

    assert explained["environment"] is ready_payload["environment"]
    assert explained["checks"] == ready_payload["checks"]
    diagnostics = explained["diagnostics"]
    assert diagnostics["schema"] == (
        "okg.deployment_ready.explain_diagnostics.v1"
    )
    codes = {finding["code"] for finding in diagnostics["findings"]}
    assert "deployment.env.required_missing" in codes
    assert "deployment.ready.database.live_checks" in codes


def test_lint_inspects_checkpoint_attestor_without_importing_it(
    tmp_path: Path,
) -> None:
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    package = deployment_dir / "attestors"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "receipt.py").write_text(
        "raise RuntimeError('lint imported attestor module')\n\n"
        "class ReceiptAttestor:\n"
        "    attestor_id = 'demo.receipt'\n"
        "    config_schema = {\n"
        "        'type': 'object',\n"
        "        'properties': {'label': {'type': 'string'}},\n"
        "        'required': ['label'],\n"
        "        'additionalProperties': False,\n"
        "    }\n"
        "    def __init__(self, *, deployment_root, label): pass\n"
        "    def attest(self, context): raise NotImplementedError\n",
        encoding="utf-8",
    )
    with (deployment_dir / "deployment.yaml").open("a", encoding="utf-8") as fh:
        fh.write(
            "checkpoint_attestors:\n"
            "  - id: demo.receipt\n"
            "    class: attestors.receipt.ReceiptAttestor\n"
            "    config:\n"
            "      label: pilot\n"
        )

    report = lint_deployment(
        deployment_dir,
        env={"OKG_DSN": "postgres://u:p@localhost:5432/okg_demo"},
    )

    assert "lint imported attestor module" not in json.dumps(report)
    assert not any(
        item["code"].startswith("deployment.checkpoint_attestors")
        for item in report["findings"]
    )


def test_lint_blocks_raw_checkpoint_attestor_placeholder(tmp_path: Path) -> None:
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    with (deployment_dir / "deployment.yaml").open("a", encoding="utf-8") as fh:
        fh.write(
            "checkpoint_attestors:\n"
            "  - id: demo.receipt\n"
            "    class: attestors.receipt.ReceiptAttestor\n"
            "    config:\n"
            "      label: ${ATTESTATION_LABEL}\n"
        )

    report = lint_deployment(
        deployment_dir,
        env={
            "OKG_DSN": "postgres://u:p@localhost:5432/okg_demo",
            "ATTESTATION_LABEL": "resolved-but-forbidden",
        },
    )

    finding = next(
        item
        for item in report["findings"]
        if item["code"] == "deployment.checkpoint_attestors.invalid"
    )
    assert report["ok"] is False
    assert finding["severity"] == "blocker"
    assert "environment substitutions" in finding["observed"]


# ---------------------------------------------------------------------------
# A declared release block that no command can load (issue #2127)
# ---------------------------------------------------------------------------


def _lint_codes(deployment_dir: Path) -> set[str]:
    from okg.substrate.deployment_lint import lint_deployment

    report = lint_deployment(
        deployment_dir.name, deployments_root=deployment_dir.parent,
    )
    return {f["code"] for f in report["findings"]}


def test_a_release_block_without_a_channel_is_a_blocker(tmp_path: Path):
    """`release: {kind: standalone}` loads nowhere and nothing said so.

    That is the exact block an older fix hint told operators to hand-write.
    It satisfies the publish gate and is then refused by `okg run` and
    `okg mcp-serve`, which load the full release contract. Lint is the
    surface that can name it before the operator meets the refusal.
    """
    from okg.substrate.deployment_lint import lint_deployment

    deployment_dir = _write_valid_deployment(tmp_path)
    manifest = deployment_dir / "deployment.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8")
        + "release:\n  kind: standalone\n",
        encoding="utf-8",
    )

    report = lint_deployment(
        deployment_dir.name, deployments_root=tmp_path,
    )
    finding = next(
        f for f in report["findings"]
        if f["code"] == "deployment.manifest.release_channel_missing"
    )
    assert finding["severity"] == "blocker"
    assert finding["expected"] == "dev or prod"
    assert "channel: dev" in finding["next_action"]


def test_a_release_block_that_is_not_a_mapping_is_a_blocker(tmp_path: Path):
    deployment_dir = _write_valid_deployment(tmp_path)
    manifest = deployment_dir / "deployment.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8") + "release: standalone\n",
        encoding="utf-8",
    )

    assert "deployment.manifest.release_not_mapping" in _lint_codes(
        deployment_dir,
    )


def test_a_complete_or_absent_release_block_is_not_reported(tmp_path: Path):
    """Absent is legal and must stay quiet.

    A deployment with no `release` block skips the contract entirely
    (`enforce_release_contract_if_declared` returns None), so reporting it
    would make the check fire on almost every fixture in the tree.
    """
    absent = _write_valid_deployment(tmp_path / "a")
    complete = _write_valid_deployment(tmp_path / "b")
    manifest = complete / "deployment.yaml"
    manifest.write_text(
        manifest.read_text(encoding="utf-8")
        + "release:\n  channel: dev\n  kind: standalone\n",
        encoding="utf-8",
    )

    for deployment_dir in (absent, complete):
        codes = _lint_codes(deployment_dir)
        assert not {c for c in codes if c.startswith(
            "deployment.manifest.release",
        )}, codes
