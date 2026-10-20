from __future__ import annotations

import ast
import warnings
from pathlib import Path

import pytest

from okg.substrate.deployments import DeploymentConfigError, DeploymentManifest
from okg.substrate.embeddings import (
    EmbeddingConfig,
    EmbeddingConfigDeprecationWarning,
    EmbeddingConfigError,
    parse_embedding_config,
)
from tests.mirror_scope import shipped


ENABLED = {"enabled": True, "backend": "hash", "reason": "test"}
DISABLED = {"enabled": False, "reason": "not used"}


@pytest.mark.parametrize(
    ("singular", "plural", "expected", "warns", "fails"),
    [
        (None, None, EmbeddingConfig(), False, False),
        (ENABLED, None, EmbeddingConfig(True, "hash", "test"), True, False),
        (None, ENABLED, EmbeddingConfig(True, "hash", "test"), False, False),
        (ENABLED, ENABLED, EmbeddingConfig(True, "hash", "test"), True, False),
        (ENABLED, DISABLED, None, False, True),
        ("enabled", None, None, False, True),
        (None, ["enabled"], None, False, True),
        ({"enabled": "yes"}, None, None, False, True),
        (None, {"enabled": False, "unknown": 1}, None, False, True),
    ],
    ids=[
        "absent",
        "singular",
        "plural",
        "dual-identical",
        "dual-conflicting",
        "singular-malformed",
        "plural-malformed",
        "coercive-boolean",
        "unknown-key",
    ],
)
def test_closed_embedding_configuration_decision_table(
    singular: object,
    plural: object,
    expected: EmbeddingConfig | None,
    warns: bool,
    fails: bool,
) -> None:
    kwargs: dict[str, object] = {}
    if singular is not None:
        kwargs["singular"] = singular
    if plural is not None:
        kwargs["plural"] = plural

    if fails:
        with pytest.raises(EmbeddingConfigError):
            parse_embedding_config(**kwargs)
        return
    if warns:
        with pytest.warns(EmbeddingConfigDeprecationWarning, match="deprecated"):
            actual = parse_embedding_config(**kwargs)
    else:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            actual = parse_embedding_config(**kwargs)
        assert not caught
    assert actual == expected


@pytest.mark.parametrize(
    "block",
    [
        {"enabled": True},
        {"enabled": True, "backend": ""},
        {"enabled": True, "backend": "   "},
        {"enabled": False, "backend": 12},
        {"enabled": False, "reason": False},
        {"enabled": False, "backend": "not-a-backend"},
    ],
)
def test_invalid_nested_values_fail(block: dict[str, object]) -> None:
    with pytest.raises(EmbeddingConfigError):
        parse_embedding_config(plural=block)


def test_manifest_exposes_only_normalized_embedding_config(
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "schema").mkdir()
    (deployment / "sources.yaml").write_text("sources: []\n")
    manifest_path = deployment / "deployment.yaml"
    manifest_path.write_text(
        "name: test\n"
        "schema_dir: schema\n"
        "source_registry: sources.yaml\n"
        "embeddings:\n"
        "  enabled: true\n"
        "  backend: hash\n",
        encoding="utf-8",
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.embedding_config == EmbeddingConfig(True, "hash", None)
    assert not hasattr(manifest, "embedding")
    assert not hasattr(manifest, "embeddings")


def test_manifest_rejects_conflicting_blocks(tmp_path: Path) -> None:
    path = tmp_path / "deployment.yaml"
    path.write_text(
        "embedding:\n  enabled: false\nembeddings:\n  enabled: true\n  backend: hash\n",
        encoding="utf-8",
    )
    with pytest.raises(DeploymentConfigError, match="conflict"):
        DeploymentManifest.from_yaml_lenient(path)


def test_embedding_consumers_do_not_reparse_raw_manifest_blocks() -> None:
    """Census every deployment-level decision reader for split authority."""
    repo = Path(__file__).resolve().parents[2]
    consumers = [
        "src/okg/substrate/runtime/services.py",
        "src/okg/substrate/runtime/worker.py",
        "src/okg/substrate/runtime/embed_workflow.py",
        "src/okg/substrate/runtime/gates_reevaluate_workflow.py",
        "src/okg/substrate/publisher/deployment_stack.py",
        "src/okg/substrate/publisher/publisher.py",
        "src/okg/substrate/alias/loader.py",
        "src/okg/substrate/mcp/deployment_entry.py",
        "src/okg/substrate/mcp/semantic_readiness.py",
        "src/okg/substrate/cli/_read_side.py",
        "src/okg/substrate/cli/deployment_readiness.py",
        "src/okg/substrate/cli/main.py",
    ]
    forbidden = (
        '.get("embedding")',
        ".get('embedding')",
        '.get("embeddings")',
        ".get('embeddings')",
        "EMBED_BACKEND_DEFAULT",
        "enqueue_embeddings: bool = True",
    )
    offenders: list[str] = []
    for relative in consumers:
        text = (repo / relative).read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                offenders.append(f"{relative}: {token}")
    assert offenders == []

    # These health checks consume a normalized snapshot field named
    # ``embedding``; that internal report key is not deployment YAML. Assert
    # their manifest-facing helper reaches the typed loader and never reparses
    # YAML instead of banning the harmless snapshot key.
    # The okg-workspace module is private; the public copy checks the shipped
    # template (tests/mirror_scope.py).
    for relative in shipped((
        "deployments/okg-workspace/doctor_checks_metrics.py",
        "src/okg/substrate/library/templates/codebase-index/doctor_checks.py",
    )):
        text = (repo / relative).read_text(encoding="utf-8")
        tree = ast.parse(text)
        helper = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "_okg_workspace_embedding_config"
        )
        helper_text = ast.get_source_segment(text, helper) or ""
        assert "resolve_deployment" in helper_text
        assert ".embedding_config" in helper_text
        assert "fastyaml" not in helper_text
