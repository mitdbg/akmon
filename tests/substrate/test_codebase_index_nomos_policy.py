"""A deployment made from the codebase-index template passes ingest's Nomos checks.

Issue #2694: the template shipped no ``nomos:`` block, so ``okg ingest`` on a
fresh ``okg init --template codebase-index`` deployment refused every source
before it read a file (``nomos.audit.deployment_posture_missing`` and
``nomos.audit.source_policy_missing``). Declaring a posture with
``runtime_enforcement: enforce`` only moved the refusal to
``nomos.default_deny.no_active_bundle``, because nothing on the manual
``okg init --no-publish`` + ``okg ingest`` path seeds a policy bundle.

These tests render the template through the real ``okg init`` entry point and
then run the two Nomos checks ingest runs, without a database:

* ``resolve_deployment_source_pii_posture`` for every registry source, called
  with the same arguments ``ingest/service.py`` passes before the run stage;
* the enforcement mode ``deployment_nomos_enforcement_mode`` resolves from
  the rendered ``deployment.yaml``: ``off`` since issue #3414 (no access
  rules declared, so runtime checks are opt-in), which skips the runtime
  boundary;
* the runtime ``source.ingest`` / ``source.emit`` boundary with no active
  bundle under the documented opt-in to ``audit``.

The real ``okg ingest`` of a rendered deployment against a disposable
database is ``test_a_fresh_template_deployment_ingests_and_publishes`` in
``test_codebase_index_stores_code_bodies.py``.
"""

from __future__ import annotations

import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate import fastyaml
from okg.substrate.ingest.pii_posture import resolve_deployment_source_pii_posture
from okg.substrate.nomos.runtime_enforcement import (
    NomosRuntimeRequest,
    _clear_nomos_enforcement_mode_cache,
    deployment_nomos_enforcement_mode,
    deployment_requires_nomos_runtime,
    evaluate_runtime_boundary,
)
from okg.substrate.nomos.rollout import runtime_enforce_mode_dsn_keys
from okg.substrate.sources.registry import load_source_registry

DEPLOYMENT = "w2694"


@pytest.fixture
def rendered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """Run ``okg init --template codebase-index`` into a disposable target."""
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    target = tmp_path / "deployments"
    rc = cli.main([
        "init",
        "--template", "codebase-index",
        "--target", str(target),
        "--repo-root", str(repo),
        "--deployment-name", DEPLOYMENT,
        "--no-publish",
        "--non-interactive",
        "--yes",
    ])
    assert rc == 0, "okg init --template codebase-index failed"
    deployment_dir = target / DEPLOYMENT
    assert (deployment_dir / "deployment.yaml").is_file()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(target))
    _clear_nomos_enforcement_mode_cache()
    yield deployment_dir
    _clear_nomos_enforcement_mode_cache()


def _manifest(deployment_dir: Path) -> dict:
    # Parsed exactly as ingest/service.py parses it before the run stage.
    return fastyaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    ) or {}


def test_every_template_source_passes_the_ingest_source_policy_audit(
    rendered: Path,
) -> None:
    manifest = _manifest(rendered)
    registry = load_source_registry(rendered / "source_registry.yaml")
    assert registry, "rendered template has no sources; test is vacuous"
    assert "git-files" in registry and "doc-corpus" in registry

    for name, entry in registry.items():
        # Raises SubstrateError("source privacy policy for ... failed Nomos
        # audit") on a template without a posture or a class default. The
        # arguments are the ones ingest/service.py passes, including the
        # enforce-mode DSN keys it collects from the deployment folder.
        posture = resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=name,
            source_entry=entry.raw,
            enforce_dsn_keys=runtime_enforce_mode_dsn_keys(rendered),
        )
        assert posture.store_raw is True, name


@pytest.mark.parametrize("surface", ["source.ingest", "source.emit"])
def test_runtime_source_boundary_allows_a_run_with_no_active_bundle(
    rendered: Path, surface: str,
) -> None:
    mode = deployment_nomos_enforcement_mode(DEPLOYMENT)
    assert mode == "off", (
        f"template resolves nomos.runtime_enforcement to {mode!r}; the shipped "
        "default is off because the template declares no access rules "
        "(issue #3414); audit and enforce are opt-in"
    )
    # ingest/runner.py skips the boundary entirely when this is False.
    assert deployment_requires_nomos_runtime(DEPLOYMENT) is False

    # ingest/runner.py calls evaluate_runtime_boundary_with_active_policy,
    # which loads the active bundle from PostgreSQL, calls this function with
    # it and the deployment's resolved mode, and persists the decision. With
    # no database here, the bundle it would load on this path is None; the
    # DB test named in the module docstring runs the real call.
    outcome = evaluate_runtime_boundary(
        None,  # nothing has seeded a bundle on the manual okg ingest path
        NomosRuntimeRequest(
            surface=surface,
            subject=f"source:{DEPLOYMENT}.git-files",
            classification="internal",
            resource={
                "deployment": DEPLOYMENT,
                "scope": f"deployment:{DEPLOYMENT}",
                "source": f"{DEPLOYMENT}.git-files",
            },
        ),
        performed_obligations=("audit",),
        # The documented opt-in: audit evaluates and records, never blocks.
        enforcement_mode="audit",
    )

    assert outcome.allowed is True
    # The strict verdict is kept as the audit record, not hidden.
    assert outcome.audit_log_and_allow is True
    assert outcome.decision.reason_code == "nomos.default_deny.no_active_bundle"
    assert outcome.call_effect == "audit"


def test_enforce_mode_without_a_bundle_still_refuses(rendered: Path) -> None:
    """Library behaviour this template's choice depends on, not its failure case.

    Under ``enforce`` the same boundary refuses with no bundle, because
    ``okg init --no-publish`` seeds none. This is why enforce is an opt-in
    that comes with a seeded bundle. This passes with or without the
    template change.
    """
    outcome = evaluate_runtime_boundary(
        None,
        NomosRuntimeRequest(
            surface="source.ingest",
            subject=f"source:{DEPLOYMENT}.git-files",
            classification="internal",
            resource={"deployment": DEPLOYMENT},
        ),
        performed_obligations=("audit",),
        enforcement_mode="enforce",
    )
    assert outcome.allowed is False
    assert outcome.decision.reason_code == "nomos.default_deny.no_active_bundle"
