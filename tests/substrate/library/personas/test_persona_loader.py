"""Tests for the persona registry loader.

Per `add-substrate-pre-join-mcp-framework` S-A7-R6 / S-A7-R7:
- substrate ships one canonical persona seed per YAML under
  ``SEEDS_DIR``, including the historical vonNeumann and Dijkstra
- each seed YAML carries the required attrs and points at a
  filesystem prompt path
- the loader computes the prompt's sha256 at load time so the
  emitted Persona NodeFact carries `system_prompt_content_hash`
- deployments can register additional personas via a
  `personas_dir:`; canonical-slug collision fails loud at load
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from okg.substrate.library.personas import (
    SEEDS_DIR,
    PersonaLoadError,
    PersonaSlugCollision,
    compute_prompt_hash,
    iter_persona_yaml_files,
    load_deployment_personas,
    load_seed_personas,
)


# The historical personas every build ships. A checkout may ship more
# seeds; the count is read from the seed directory, not pinned here.
_SHIPPED_HISTORICAL_SLUGS = {"dijkstra", "vonneumann"}


def _seed_yaml_slugs() -> list[str]:
    # Read the seed directory directly, not through the loader's own file
    # walk, so a walk that drops a seed file fails this test. Each seed file
    # is named after its slug.
    return sorted(
        path.stem
        for path in SEEDS_DIR.glob("*.y*ml")
        if path.suffix in {".yaml", ".yml"}
        and not path.name.startswith(("_", "."))
    )


def test_canonical_personas_load() -> None:
    seeds = load_seed_personas()
    slugs = sorted(s.slug for s in seeds)
    # One seed per shipped YAML, and nothing loads that has no YAML.
    assert slugs == _seed_yaml_slugs()
    assert _SHIPPED_HISTORICAL_SLUGS <= set(slugs)
    for seed in seeds:
        assert seed.origin == "substrate"
        assert seed.persona_id == f"persona:{seed.slug}"
        # Each canonical persona's prompt file ships in-repo and the
        # content hash is computed at load time.
        assert seed.system_prompt_content_hash, (
            f"{seed.slug} missing prompt hash; prompt file should exist"
        )
        assert len(seed.system_prompt_content_hash) == 64


def test_persona_yaml_missing_required_field_fails_loud(tmp_path: Path) -> None:
    bad = tmp_path / "bad.yaml"
    bad.write_text(textwrap.dedent("""\
        slug: bad
        name: Bad Persona
        # missing description, system_prompt_ref,
        # pre_join_tool_for_input, default_request_type
    """))
    files = list(iter_persona_yaml_files(tmp_path))
    assert files == [bad]
    with pytest.raises(PersonaLoadError, match="missing required field"):
        load_deployment_personas(
            tmp_path,
            deployment_name="test",
            canonical_slugs={"dijkstra", "vonneumann"},
        )


def test_deployment_persona_slug_collision_fails_loud(tmp_path: Path) -> None:
    collision = tmp_path / "dijkstra.yaml"
    collision.write_text(textwrap.dedent("""\
        slug: dijkstra
        name: Not-the-real-Dijkstra
        description: A reckless override that should fail loud.
        system_prompt_ref: /tmp/nope.md
        pre_join_tool_for_input: x_y
        default_request_type: other
    """))
    canonical = {s.slug for s in load_seed_personas()}
    with pytest.raises(PersonaSlugCollision, match="dijkstra"):
        load_deployment_personas(
            tmp_path,
            deployment_name="testdep",
            canonical_slugs=canonical,
        )


def test_deployment_persona_loads_when_slug_is_distinct(tmp_path: Path) -> None:
    novel = tmp_path / "domain_db.yaml"
    novel.write_text(textwrap.dedent("""\
        slug: domain_db
        name: Domain DB Specialist
        description: A deployment-registered persona for testing.
        system_prompt_ref: /tmp/missing-but-thats-ok.md
        pre_join_tool_for_input: substrate_review_target_neighborhood
        default_request_type: design review
        lifecycle_stage: experimental
    """))
    canonical = {s.slug for s in load_seed_personas()}
    seeds = load_deployment_personas(
        tmp_path,
        deployment_name="memex",
        canonical_slugs=canonical,
    )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.slug == "domain_db"
    assert seed.origin == "deployment:memex"
    assert seed.lifecycle_stage == "experimental"
    # Missing prompt file -> hash is None; loader does not raise.
    assert seed.system_prompt_content_hash is None


def test_compute_prompt_hash_changes_with_content(tmp_path: Path) -> None:
    p = tmp_path / "prompt.md"
    p.write_text("you are a careful reviewer.\n")
    h1 = compute_prompt_hash(p)
    p.write_text("you are a careless reviewer.\n")
    h2 = compute_prompt_hash(p)
    assert h1 != h2
    assert len(h1) == 64
    assert len(h2) == 64


def test_each_canonical_persona_has_distinct_prompt_hash() -> None:
    seeds = load_seed_personas()
    hashes = {s.slug: s.system_prompt_content_hash for s in seeds}
    # One distinct hash per seed — no accidental file duplication.
    assert len(hashes) == len(seeds) >= len(_SHIPPED_HISTORICAL_SLUGS)
    assert len(set(hashes.values())) == len(seeds)
