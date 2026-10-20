from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.profile import load_profile, load_profile_modules
from okg.substrate.deployment_bootstrap.profile_init import discover_profile
from okg.substrate.deployment_profile_explain import (
    BUILTIN_V1_PROFILE_NAMES,
    resolve_profile_explain,
)
from tests.substrate._fake_credentials import PEM_BEGIN


EXPECTED_V1_PROFILES = {
    "academic-researcher",
    # Issue #3414: the empty starter `okg install` builds with no --profile.
    "minimal",
    "workspace-graph",
}


def test_named_v1_profiles_explain_metadata() -> None:
    assert set(BUILTIN_V1_PROFILE_NAMES) == EXPECTED_V1_PROFILES

    for name in BUILTIN_V1_PROFILE_NAMES:
        explanation = resolve_profile_explain(name)
        payload = explanation.to_dict()

        assert payload["name"] == name
        assert payload["version"]
        assert payload["classification"] in {
            "reusable",
            "demo",
            "example",
            "fixture",
        }
        assert payload["classification_flags"][payload["classification"]]
        assert payload["description"]
        assert payload["modules"], name
        if name == "minimal":
            # Issue #3414: the default starter ships no sources by design;
            # the team adds its own with `okg deployment add-source`.
            assert payload["source_packs"] == [], name
        else:
            assert payload["source_packs"], name
        assert any(pack["kind"] == "invariants" for pack in payload["asset_packs"]), (
            name
        )
        assert {point["name"] for point in payload["override_points"]} >= {
            "init_answers",
            "modules",
            "source_defaults",
            "invariants",
            "dashboards",
            "runtime_defaults",
        }
        assert name in explanation.to_text()


def test_workspace_graph_profile_exposes_reusable_source_pack_provenance() -> None:
    explanation = resolve_profile_explain("workspace-graph").to_dict()

    assert explanation["classification"] == "reusable"
    modules = {module["name"]: module for module in explanation["modules"]}
    for required in {
        "git_graph",
        "openspec",
        "agent_sessions",
        "agent_memory",
        "compute_env",
        "code_graph",
        "extraction",
        "reasoning",
        "ci",
        "quality_evidence",
        "views",
    }:
        assert modules[required]["required"] is True
        assert modules[required]["origin"].endswith(
            f"/ontologies/{required}/module.yaml"
        )
        assert modules[required]["version"]
        assert modules[required]["override_point"].startswith("deployment.yaml:modules")

    sources = {
        source_pack["name"]: source_pack for source_pack in explanation["source_packs"]
    }
    for source_name in {
        "git_files",
        "git_history",
        "code_structure",
        "package_manifests",
        "openspec",
        "compute_env_local",
        "test_reports",
    }:
        source = sources[source_name]
        assert source["version"] == "0.1.0"
        assert source["origin"].endswith(
            f"/profiles/workspace-graph/source-defaults/{source_name}.yaml"
        )
        assert source["override_point"] == (
            f"source_registry.yaml:sources.{source_name}"
        )

    assert sources["code_structure"]["required"] is True
    assert sources["code_structure"]["placeholders"] == [
        "deployment_name",
        "repo_root",
    ]
    assert sources["code_structure"]["config"]["params"]["repo_root"] == (
        "${repo_root}"
    )
    assert sources["code_structure"]["config"]["deletion_semantics"] == (
        "missing_from_completed_scope"
    )
    assert sources["code_structure"]["config"]["params"]["max_files"] == 5000
    assert sources["git_files"]["config"]["params"]["include_untracked"] is False


def test_profile_explain_is_no_write(monkeypatch) -> None:
    def fail_write(*args, **kwargs):
        raise AssertionError("profile explain attempted to write")

    monkeypatch.setattr(Path, "write_text", fail_write)
    monkeypatch.setattr(Path, "mkdir", fail_write)

    explanation = resolve_profile_explain("workspace-graph").to_dict()

    assert explanation["name"] == "workspace-graph"
    assert explanation["source_packs"]


def test_profile_explain_redacts_secret_bearing_values(
    tmp_path: Path,
) -> None:
    profile_dir = tmp_path / "secret-profile"
    source_dir = profile_dir / "source-defaults"
    source_dir.mkdir(parents=True)
    (profile_dir / "profile.yaml").write_text(
        yaml.safe_dump(
            {
                "name": "secret-profile",
                "version": "0.1.0",
                "classification": "fixture",
                "description": "secret-bearing fixture",
                "init_questions": [
                    {
                        "id": "postgres_dsn",
                        "prompt": "dsn",
                        "type": "string",
                        "required": False,
                        "default": ("postgres://demo:supersecret@localhost:5432/db"),
                    },
                    {
                        "id": "api_token",
                        "prompt": "token",
                        "type": "string",
                        "required": False,
                        "default": "literal-init-token",
                    },
                    {
                        "id": "display_label",
                        "prompt": "label",
                        "type": "string",
                        "required": False,
                        "default": None,
                    }
                ],
                "modules": "modules.yaml",
                "source_defaults": "source-defaults/",
            }
        ),
        encoding="utf-8",
    )
    (profile_dir / "modules.yaml").write_text(
        yaml.safe_dump({"modules": ["git_graph"]}),
        encoding="utf-8",
    )
    (source_dir / "secret_source.yaml").write_text(
        yaml.safe_dump(
            {
                "secret_source": {
                    "module": "okg.substrate.library.sources.example",
                    "class": "ExampleSource",
                    "params": {
                        "dsn": "postgres://demo:sourcepass@localhost/db",
                        "password": "literal-password",
                        "api_key": "ghp_abcdefghijklmnopqrstuvwxyz123456",
                        "private_key": PEM_BEGIN,
                        "Authorization": "Bearer literal-token",
                        "nested": {
                            "token": "nested-token-value",
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    profile = load_profile(profile_dir / "profile.yaml")
    payload = resolve_profile_explain(profile).to_dict()
    dumped = json.dumps(payload, sort_keys=True)
    init_defaults = {
        question["id"]: question["default"] for question in payload["init_questions"]
    }

    assert "supersecret" not in dumped
    assert "literal-init-token" not in dumped
    assert "sourcepass" not in dumped
    assert "literal-password" not in dumped
    assert "abcdefghijklmnopqrstuvwxyz123456" not in dumped
    assert "literal-token" not in dumped
    assert "nested-token-value" not in dumped
    assert init_defaults["display_label"] is None
    assert "postgres://demo:***@" in dumped
    assert "<redacted>" in dumped


def test_named_profiles_compose_from_reported_modules(
    tmp_path: Path,
) -> None:
    for name in BUILTIN_V1_PROFILE_NAMES:
        profile = discover_profile(name)
        modules = load_profile_modules(profile)
        deployment_dir = tmp_path / name
        deployment_dir.mkdir()
        (deployment_dir / "deployment.yaml").write_text(
            yaml.safe_dump({"name": name, "modules": modules}),
            encoding="utf-8",
        )

        composed = compose_catalog(deployment_dir)

        assert composed.modules == modules
        assert composed.subtypes, name
