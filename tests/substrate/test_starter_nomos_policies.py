"""Every shipped starter renders sources that ingest's Nomos audit accepts.

Issue #2757: the ``devops`` and ``docs-corpus`` starters shipped no ``nomos:``
block, so ``okg ingest`` on a fresh ``okg init --template <name>`` deployment
refused every source before it read a record ("source privacy policy for
'<source>' failed Nomos audit"). The same gap in ``codebase-index`` is issue
#2694, fixed by pull request #2727.

The starter list is read from the library at test time, so a starter added
later without a policy fails here. Each starter is rendered through the real
``okg init`` entry point and then checked the way ingest checks it, without a
database:

* ``resolve_deployment_source_pii_posture`` for every registry source, with
  the arguments ``ingest/service.py`` passes before the run stage;
* the declared runtime mode is ``off`` (issue #3414, Jason 2026-10-05
  ``nomosdefault A``: a fresh install leaves runtime checks off when the
  deployment declares no access rules; audit and enforce are opt-in);
* the documented opt-in to ``audit`` still works before a bundle exists: the
  runtime ``source.ingest`` / ``source.emit`` boundary with no active policy
  bundle lets the run proceed and records the refusal;
* readiness for the ``ci``, ``dogfood`` and ``production`` profiles, which
  must refuse the shipped ``off`` mode.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import okg.substrate.cli.main as cli
from okg.substrate import fastyaml
from okg.substrate.cli import deployment_readiness
from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.pii_posture import resolve_deployment_source_pii_posture
from okg.substrate.library.resolver import available_refs
from okg.substrate.nomos import audit_deployment_policy
from okg.substrate.nomos.core import compile_policy_graph
from okg.substrate.nomos.rollout import runtime_enforce_mode_dsn_keys
from okg.substrate.nomos.runtime_enforcement import (
    NomosRuntimeRequest,
    evaluate_runtime_boundary,
    manifest_nomos_enforcement_mode,
)
from okg.substrate.nomos.store import deployment_baseline_policy_graph
from okg.substrate.sources.registry import (
    load_source_registry,
    runtime_source_subject_ids,
)

DEPLOYMENT = "w2757"

# Starters whose policy ships in another, unmerged pull request, mapped to the
# reason. strict=True: once that pull request lands the row XPASSes, which
# fails, so the marker cannot outlive the fix. Empty since #2727 merged.
_PENDING_ELSEWHERE: dict[str, str] = {}


def _starters() -> list[object]:
    names = available_refs("templates")
    assert {"devops", "docs-corpus", "codebase-index", "empty"} <= set(names), names
    params = []
    for name in names:
        marks = []
        if name in _PENDING_ELSEWHERE:
            marks.append(pytest.mark.xfail(
                reason=_PENDING_ELSEWHERE[name], strict=True,
            ))
        params.append(pytest.param(name, marks=marks, id=name))
    return params


def _render(template: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run ``okg init --template <template> --no-publish`` into a temp target."""
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "a.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    target = tmp_path / "deployments"
    rc = cli.main([
        "init",
        "--template", template,
        "--target", str(target),
        "--repo-root", str(repo),
        "--deployment-name", DEPLOYMENT,
        "--no-publish",
        "--non-interactive",
        "--yes",
    ])
    assert rc == 0, f"okg init --template {template} failed"
    deployment_dir = target / DEPLOYMENT
    assert (deployment_dir / "deployment.yaml").is_file()
    return deployment_dir


def _manifest(deployment_dir: Path) -> dict:
    # Parsed exactly as ingest/service.py parses it before the run stage.
    return fastyaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    ) or {}


@pytest.mark.parametrize("template", _starters())
def test_starter_sources_pass_the_ingest_nomos_gate(
    template: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment_dir = _render(template, tmp_path, monkeypatch)
    manifest = _manifest(deployment_dir)
    registry = load_source_registry(deployment_dir / "source_registry.yaml")
    if not registry:
        # The empty starter has no sources, so ingest has nothing to audit.
        assert template == "empty", f"{template} renders no sources"
        return

    refused: dict[str, str] = {}
    for name, entry in registry.items():
        # Raises SubstrateError("source privacy policy for ... failed Nomos
        # audit") when the manifest has no posture or no policy for the
        # source's class.
        try:
            resolve_deployment_source_pii_posture(
                manifest=manifest,
                source_name=name,
                source_entry=entry.raw,
                enforce_dsn_keys=runtime_enforce_mode_dsn_keys(deployment_dir),
            )
        except SubstrateError as exc:
            codes = sorted({
                str(finding.get("code"))
                for finding in (exc.data or {}).get("findings", [])
                if isinstance(finding, dict)
            })
            refused[name] = f"{exc} {codes}"
    assert not refused, (
        f"{template}: {len(refused)} of {len(registry)} sources refused by the "
        f"ingest Nomos audit: {refused}"
    )

    mode = manifest_nomos_enforcement_mode(manifest)
    assert mode == "off", (
        f"{template} resolves nomos.runtime_enforcement to {mode!r}; a starter "
        "declares no access rules, so it ships off and audit/enforce are "
        "opt-in (issue #3414)"
    )
    # The documented opt-in: switching the starter to audit still lets the
    # first manual ingest run before a bundle exists, and records the refusal.
    source = next(iter(registry))
    for surface in ("source.ingest", "source.emit"):
        outcome = evaluate_runtime_boundary(
            None,  # okg init --no-publish + okg ingest seeds no bundle
            NomosRuntimeRequest(
                surface=surface,
                subject=f"source:{DEPLOYMENT}.{source}",
                classification="internal",
                resource={
                    "deployment": DEPLOYMENT,
                    "scope": f"deployment:{DEPLOYMENT}",
                    "source": f"{DEPLOYMENT}.{source}",
                },
            ),
            performed_obligations=("audit",),
            enforcement_mode="audit",
        )
        assert outcome.allowed is True, (template, surface)
        # The strict verdict is kept as the audit record, not hidden.
        assert outcome.audit_log_and_allow is True
        assert outcome.decision.reason_code == "nomos.default_deny.no_active_bundle"

    for profile in ("ci", "dogfood", "production"):
        status = deployment_readiness._ready_nomos_active_state_status(
            deployment=DEPLOYMENT,
            dsn=None,
            manifest=manifest,
            deployment_dir=deployment_dir,
            readiness_profile=profile,
        )
        assert status["status"] == "failed", (template, profile, status)
        assert [f["code"] for f in status["findings"]] == [
            "nomos.runtime.mode_not_blocking"
        ], (template, profile)


@pytest.mark.parametrize("template", _starters())
def test_starter_sources_pass_the_enforce_baseline(
    template: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The comments tell users to switch to enforce once a bundle is seeded.

    That must not refuse the starter's own sources. ``okg up``, ``okg
    provision`` and ``--apply-baseline`` seed the rollout baseline built by
    ``deployment_baseline_policy_graph``; ingest/runner.py checks
    ``source.ingest`` and ``source.emit`` for every source and also
    ``source.live_call`` for every mutable_api reader, even one that reads a
    local snapshot, with the audit and review obligations performed.
    """
    deployment_dir = _render(template, tmp_path, monkeypatch)
    manifest = _manifest(deployment_dir)
    raw_registry = fastyaml.safe_load(
        (deployment_dir / "source_registry.yaml").read_text(encoding="utf-8")
    ) or {}
    sources = raw_registry.get("sources") or {}
    if not sources:
        assert template == "empty", f"{template} renders no sources"
        return
    audit = audit_deployment_policy(
        manifest, source_registry=raw_registry, deployment=DEPLOYMENT,
    )
    assert audit["ok"], audit["findings"]
    policies = dict(audit["source_policies"])
    for name, entry in sources.items():
        policies.setdefault(entry["ownership_id"], policies[name])
    scope = f"deployment:{DEPLOYMENT}"
    bundle = compile_policy_graph(deployment_baseline_policy_graph(
        manifest,
        deployment_name=DEPLOYMENT,
        scope=scope,
        activated_by="subject:test",
        source_names=sorted(runtime_source_subject_ids(sources)),
        source_policies=policies,
        runtime_source_names=sorted(runtime_source_subject_ids(sources)),
    ))

    denied = []
    for name, entry in sources.items():
        source_id = entry["ownership_id"]
        surfaces = ["source.ingest", "source.emit"]
        if entry.get("source_class") == "mutable_api":
            surfaces.append("source.live_call")
        for surface in surfaces:
            outcome = evaluate_runtime_boundary(
                bundle,
                NomosRuntimeRequest(
                    surface=surface,
                    subject=f"source:{source_id}",
                    classification="internal",
                    resource={
                        "deployment": DEPLOYMENT,
                        "scope": scope,
                        "source": source_id,
                    },
                ),
                performed_obligations=(
                    ("audit", "review")
                    if surface == "source.live_call"
                    else ("audit",)
                ),
                enforcement_mode="enforce",
            )
            if not outcome.allowed:
                denied.append((name, surface, outcome.decision.reason_code))
    assert not denied, f"{template}: enforce baseline refuses {denied}"


def test_a_starter_without_a_policy_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gate this file relies on refuses a source with no policy."""
    deployment_dir = _render("devops", tmp_path, monkeypatch)
    manifest = _manifest(deployment_dir)
    manifest.pop("nomos", None)
    registry = load_source_registry(deployment_dir / "source_registry.yaml")
    name, entry = next(iter(registry.items()))
    with pytest.raises(SubstrateError, match="failed Nomos audit"):
        resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=name,
            source_entry=entry.raw,
            enforce_dsn_keys=runtime_enforce_mode_dsn_keys(deployment_dir),
        )
