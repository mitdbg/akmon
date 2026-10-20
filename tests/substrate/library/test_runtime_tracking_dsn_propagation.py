from __future__ import annotations

from pathlib import Path

import yaml
import pytest


def test_enricher_loader_injects_worker_dsn_only_for_explicit_mode(tmp_path: Path) -> None:
    from okg.substrate.publisher.enricher_loader import load_enrichers

    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "enrichers": [
                    {
                        "class": (
                            "okg.substrate.library.enrichers."
                            "visual_caption_enricher.VisualCaptionEnricher"
                        ),
                        "config": {
                            "model": "test/model",
                            "tracking_mode": "optional",
                        },
                    }
                ]
            }
        )
    )

    [enricher] = load_enrichers(
        deployment,
        active_profiles=set(),
        runtime_dsn="postgresql://user:p%40ss@db.example/one",
        deployment="fixture",
    )
    assert enricher._tracking_dsn.endswith("/one")
    assert not hasattr(enricher, "tracking_dsn")


def test_enricher_loader_rejects_manifest_runtime_authority_override(
    tmp_path: Path,
) -> None:
    from okg.substrate.publisher.enricher_loader import EnricherLoadError, load_enrichers

    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        yaml.safe_dump(
            {
                "enrichers": [
                    {
                        "class": (
                            "okg.substrate.library.enrichers."
                            "visual_caption_enricher.VisualCaptionEnricher"
                        ),
                        "config": {
                            "model": "test/model",
                            "tracking_mode": "optional",
                            "runtime_dsn": "postgresql://redirected/db",
                        },
                    }
                ]
            }
        )
    )
    with pytest.raises(EnricherLoadError, match="reserved runtime"):
        load_enrichers(
            deployment,
            active_profiles=set(),
            runtime_dsn="postgresql://worker/db",
            deployment="fixture",
        )


def test_views_registry_tracking_mode_is_carried_without_a_dsn() -> None:
    from okg.substrate.views.registry import parse_views_registry

    [entry] = parse_views_registry(
        {
            "deployment": "fixture",
            "views": [
                {
                    "view_def_id": "fixture.view",
                    "view_def_factory": "fixture.views:definition",
                    "build_corpus": "fixture.views:corpus",
                    "llm_client": "litellm:test/model",
                    "tracking_mode": "optional",
                }
            ],
        }
    )
    attrs = entry.to_node_attrs()
    assert attrs["tracking_mode"] == "optional"
    assert "dsn" not in attrs


def test_doc_corpus_captioning_config_resolves_runtime_dsn(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import _load_captioning_config

    secret = "postgresql://user:secret@db.example/one"
    (tmp_path / "extractors.yaml").write_text(
        yaml.safe_dump(
            {
                "captioning": {
                    "enabled": True,
                    "model": "test/model",
                    "tracking_mode": "optional",
                    "tracking_dsn": secret,
                }
            }
        )
    )
    cfg = _load_captioning_config(tmp_path)
    assert cfg.resolve_tracking("postgresql://user:p%40ss@db.example/one") == secret
    assert secret not in repr(cfg)
