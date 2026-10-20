"""A deployment scaffolded from a library profile can run its first ingest.

The ingest privacy posture refuses a source whose resolved policy leaves
``store_raw`` implicit. Before ``profile-store-raw`` the git-code,
academic-pi, systems-research, and cs-researcher deployment defaults gave
each source class a policy without it, so every source of a freshly
scaffolded deployment failed with "source privacy policy must explicitly
declare store_raw". This test scaffolds every profile that ships deployment
defaults and resolves the posture of every scaffolded source the way the
ingest runner does.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    scaffold_deployment,
)
from okg.substrate.ingest.pii_posture import resolve_deployment_source_pii_posture


REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILES_ROOT = REPO_ROOT / "src/okg/substrate/library/profiles"


def _profiles_with_deployment_defaults() -> list[str]:
    names = []
    for profile_dir in sorted(PROFILES_ROOT.iterdir()):
        if not (profile_dir / "profile.yaml").is_file():
            continue
        profile = load_profile(profile_dir / "profile.yaml")
        if profile.deployment_defaults_file is not None:
            names.append(profile_dir.name)
    assert names
    return names


def _required_answers(profile, tmp_path: Path) -> dict:
    """Answer only what the profile requires; optional questions keep their
    defaults, exactly as an unattended `okg init` run would."""
    answers: dict = {}
    for question in profile.init_questions:
        if not question.required:
            continue
        if question.type == "slug":
            answers[question.id] = "store_raw_probe"
        elif question.type == "path":
            answers[question.id] = str(tmp_path)
        else:
            answers[question.id] = "postgres://postgres:okg@localhost:5433/okg_probe"
    return answers


@pytest.mark.parametrize("profile_name", _profiles_with_deployment_defaults())
def test_scaffolded_sources_resolve_explicit_store_raw(
    profile_name: str, tmp_path: Path,
) -> None:
    profile = load_profile(PROFILES_ROOT / profile_name / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="store_raw_probe",
        deployments_root=tmp_path / "deployments",
        raw_answers=_required_answers(profile, tmp_path),
    )
    result = scaffold_deployment(plan)
    manifest = yaml.safe_load(
        (result.deployment_dir / "deployment.yaml").read_text(encoding="utf-8"),
    )
    sources = yaml.safe_load(
        (result.deployment_dir / "source_registry.yaml").read_text(encoding="utf-8"),
    )["sources"]
    if not plan.source_defaults:
        # The `minimal` starter ships no source by design (issue #3414): an
        # empty registry is correct and there is no store_raw to resolve.
        assert not sources, f"{profile_name} declares no sources but scaffolded {sorted(sources)}"
        return
    assert sources, f"{profile_name} scaffolded no sources"

    for source_name, entry in sources.items():
        posture = resolve_deployment_source_pii_posture(
            manifest=manifest,
            source_name=source_name,
            source_entry=entry,
            build_detector=False,
        )
        assert posture.store_raw is True, (profile_name, source_name)
