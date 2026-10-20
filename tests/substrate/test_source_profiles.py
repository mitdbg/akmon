"""M3.1 — source profile validation."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.main import main
from okg.substrate.source_sync import (
    SourceSyncCapabilities,
    SourceSyncCompaction,
    SourceSyncDependents,
    SourceSyncPoll,
    SourceSyncScope,
    SourceSyncWatch,
    SourceSyncWebhook,
)
from okg.substrate.sources import (
    REGISTRY_ONLY,
    load_source_registry,
    validate_profile_tuple,
)


def _write_yaml(tmp_path: Path, sources: dict) -> Path:
    p = tmp_path / "source_registry.yaml"
    p.write_text(yaml.safe_dump({"sources": sources}))
    return p


def test_valid_reference_catalog(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "reference_codes": {
            "ownership_id": "example.reference_codes",
            "source_class": "reference_catalog",
            "record_identity_kind": "domain_key",
            "record_identity_fields": ["code"],
            "source_revision_kind": "release_tag",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
        }
    })
    out = load_source_registry(p)
    assert "reference_codes" in out
    assert not out["reference_codes"].legacy


def test_valid_mutable_api(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "issues": {
            "ownership_id": "example.issues",
            "source_class": "mutable_api",
            "record_identity_kind": "remote_id",
            "record_identity_fields": ["item_key"],
            "source_revision_kind": "updated_at",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
        }
    })
    load_source_registry(p)


def test_registry_preserves_credential_metadata(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "issues": {
            "ownership_id": "example.issues",
            "source_class": "mutable_api",
            "record_identity_kind": "remote_id",
            "record_identity_fields": ["item_key"],
            "source_revision_kind": "updated_at",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "credential_refs": ["EXAMPLE_API_TOKEN"],
            "credential_aliases": {
                "EXAMPLE_API_TOKEN": ["LEGACY_EXAMPLE_TOKEN"],
            },
            "required_for_baseline": True,
        }
    })
    out = load_source_registry(p)
    entry = out["issues"]
    assert entry.credential_refs == ["EXAMPLE_API_TOKEN"]
    assert entry.credential_aliases == {
        "EXAMPLE_API_TOKEN": ["LEGACY_EXAMPLE_TOKEN"],
    }
    assert entry.required_for_baseline is True


def test_registry_preserves_sync_metadata(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "files": {
            "ownership_id": "example.files",
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["repo", "path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "sync": {
                "watch": {
                    "roots": ["${source_registry_dir}/repo"],
                    "include_globs": ["**/*.py"],
                    "exclude_globs": ["**/__pycache__/**"],
                },
                "triggers": ["fs_event", "manual", "reconcile"],
                "default_event_mode": "cursor",
                "reconcile_mode": "scope_complete",
                "deletes_require_reconcile": True,
                "cheap_probe": "stat_tree",
            },
        }
    })

    entry = load_source_registry(p)["files"]

    assert entry.sync is not None
    assert entry.sync.watch is not None
    assert entry.sync.watch.roots == (f"{tmp_path}/repo",)
    assert entry.sync.watch.include_globs == ("**/*.py",)
    assert entry.sync.watch.exclude_globs == ("**/__pycache__/**",)
    assert entry.sync.triggers == ("fs_event", "manual", "reconcile")
    assert entry.sync.default_event_mode == "cursor"
    assert entry.sync.reconcile_mode == "scope_complete"
    assert entry.sync.deletes_require_reconcile is True
    assert entry.sync.cheap_probe == "stat_tree"


def test_registry_preserves_admission_policy(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "files": {
            "ownership_id": "example.files",
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["repo", "path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "admission_policy": {
                "producer_id": "example.files",
                "producer_kind": "source",
                "trust_label": "repo-local-trusted",
                "admission_mode": "fast_track",
                "authority_scope": {
                    "source_family": "repo",
                    "path_globs": ["**/*"],
                },
                "allowed_record_kinds": ["node"],
                "output_signature": {
                    "nodes": [
                        {
                            "subtype": "source_file",
                            "id_prefix": "file:example:",
                            "attrs": {"repo": "example"},
                            "clause_id": "example.files.nodes",
                        },
                    ],
                },
                "policy_clauses": [
                    {
                        "id": "example.files.nodes",
                        "effect": "allow",
                        "record_kind": "node",
                        "subtype": "source_file",
                    },
                ],
                "output_scope_summary": {
                    "nodes": ["source_file"],
                },
            },
        },
    })

    policy = load_source_registry(p)["files"].admission_policy

    assert policy is not None
    # The write envelope (spec §9.3) is DERIVED from the declared
    # output_signature (deterministic — a source adapter is not a model call)
    # and rides both the policy and its output_scope_summary so the
    # materialized profile carries it.
    derived_envelope = {
        "nodes": [
            {
                "subtype": "source_file",
                "id_prefix": "file:example:",
                "attrs": {"repo": "example"},
                "clause_id": "example.files.nodes",
            },
        ],
        "edges": [],
        "claim_keys": [],
        "value_vocabularies": {},
        "unbounded": [],
        "nondeterministic": False,
    }
    assert policy.as_dict() == {
        "producer_id": "example.files",
        "producer_kind": "source",
        "trust_label": "repo-local-trusted",
        "admission_mode": "fast_track",
        "authority_scope": {
            "source_family": "repo",
            "path_globs": ["**/*"],
        },
        "allowed_record_kinds": ["node"],
        "output_signature": {
            "nodes": [
                {
                    "subtype": "source_file",
                    "id_prefix": "file:example:",
                    "attrs": {"repo": "example"},
                    "clause_id": "example.files.nodes",
                },
            ],
        },
        "policy_clauses": [
            {
                "id": "example.files.nodes",
                "effect": "allow",
                "record_kind": "node",
                "subtype": "source_file",
            },
        ],
        "output_scope_summary": {
            "nodes": ["source_file"],
            "write_envelope": derived_envelope,
        },
        "write_envelope": derived_envelope,
    }
    assert policy.is_compat_summary_only is False


def test_registry_marks_summary_only_admission_policy_compat(
    tmp_path: Path,
) -> None:
    p = _write_yaml(tmp_path, {
        "files": {
            "ownership_id": "example.files",
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["repo", "path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "admission_policy": {
                "producer_id": "example.files",
                "producer_kind": "source",
                "trust_label": "repo-local-trusted",
                "admission_mode": "fast_track",
                "output_scope_summary": "legacy summary only",
            },
        },
    })

    policy = load_source_registry(p)["files"].admission_policy

    assert policy is not None
    assert policy.is_compat_summary_only is True
    assert policy.output_scope_summary == {"summary": "legacy summary only"}


def test_registry_rejects_malformed_admission_policy(tmp_path: Path) -> None:
    base = {
        "ownership_id": "example.files",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }

    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "admission_policy": {
                "trust_label": "repo-local-trusted",
                "admission_mode": "unknown",
            },
        },
    })

    with pytest.raises(ValueError, match="admission_policy.admission_mode"):
        load_source_registry(p)


def test_registry_rejects_unknown_admission_policy_fields(
    tmp_path: Path,
) -> None:
    base = {
        "ownership_id": "example.files",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "admission_policy": {
                "trust_label": "repo-local-trusted",
                "unexpected": True,
            },
        },
    })

    with pytest.raises(ValueError, match="unknown keys"):
        load_source_registry(p)


def test_registry_rejects_unsupported_signature_predicates(
    tmp_path: Path,
) -> None:
    base = {
        "ownership_id": "example.files",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "admission_policy": {
                "trust_label": "repo-local-trusted",
                "output_signature": {
                    "nodes": [
                        {
                            "subtype": "source_file",
                            "regex": ".*",
                        },
                    ],
                },
            },
        },
    })

    with pytest.raises(
        ValueError,
        match="producer_policy_unsupported_predicate",
    ):
        load_source_registry(p)


@pytest.mark.postgres
def test_doctor_sources_warns_on_implicit_admission_policy(
    tmp_path: Path,
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    deployments = tmp_path / "deployments"
    dep = deployments / "implicit"
    dep.mkdir(parents=True)
    # Deployment discovery keys on deployment.yaml; without it `doctor`
    # reports the deployment as not found.
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "implicit",
        "source_registry": "source_registry.yaml",
    }))
    (dep / "source_registry.yaml").write_text(yaml.safe_dump({
        "sources": {
            "files": {
                "ownership_id": "example.files",
                "source_class": "discovery_crawl",
                "record_identity_kind": "scoped_locator",
                "record_identity_fields": ["repo", "path"],
                "source_revision_kind": "content_hash",
                "deletion_semantics": "missing_from_completed_scope",
                "publication_mode": "published_generation",
            },
        },
    }))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(deployments))

    rc = main([
        "--json",
        "doctor",
        "--check",
        "sources",
        "--deployment",
        "implicit",
        "--dsn",
        clean_runtime,
    ])

    assert rc == 0
    report = json.loads(capsys.readouterr().out)
    assert report["warnings"][0]["code"] == "implicit_admission_policy"
    source = report["sources"][0]
    assert source["admission_policy"]["declared"] is False
    assert source["admission_policy"]["warnings"] == [
        "implicit_admission_policy",
    ]


def test_registry_rejects_malformed_sync_metadata(tmp_path: Path) -> None:
    base = {
        "ownership_id": "example.files",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "sync": {"triggers": ["publish"]},
        }
    })
    with pytest.raises(ValueError, match="trigger_kind"):
        load_source_registry(p)

    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "sync": {"default_event_mode": "full_refresh"},
        }
    })
    with pytest.raises(ValueError, match="default_event_mode"):
        load_source_registry(p)

    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "sync": {
                "watch": {
                    "roots": [],
                },
            },
        }
    })
    with pytest.raises(ValueError, match="sync.watch.roots"):
        load_source_registry(p)

    p = _write_yaml(tmp_path, {
        "files": {
            **base,
            "sync": {"deletes_require_reconcile": "yes"},
        }
    })
    with pytest.raises(ValueError, match="deletes_require_reconcile"):
        load_source_registry(p)


def test_registry_sync_overrides_adapter_defaults_without_mutation(
    tmp_path: Path,
) -> None:
    defaults = SourceSyncCapabilities(
        watch=SourceSyncWatch(
            roots=("/adapter-root",),
            include_globs=("**/*.md",),
            exclude_globs=("**/.git/**",),
        ),
        triggers=("poll",),
        default_event_mode="reconcile",
        reconcile_mode="scope_complete",
        deletes_require_reconcile=False,
        cheap_probe="etag",
        scope=SourceSyncScope(
            accepts=("record_id",),
            unsupported_fallback="reconcile",
        ),
        dependents=SourceSyncDependents(
            on_event="closure",
            on_reconcile="closure",
        ),
        compaction=SourceSyncCompaction(window_seconds=45),
        webhook=SourceSyncWebhook(
            path="/adapter",
            secret_ref="ADAPTER_SECRET",
        ),
        poll=SourceSyncPoll(probe="change_probe", interval_seconds=120),
    )

    class Adapter:
        def sync_capabilities(self):
            return defaults

    p = _write_yaml(tmp_path, {
        "files": {
            "ownership_id": "example.files",
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["repo", "path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "sync": {
                "watch": {
                    "roots": ["${source_registry_dir}/repo"],
                    "include_globs": ["**/*.py"],
                },
                "triggers": ["fs_event", "manual"],
                "deletes_require_reconcile": True,
                "scope": {
                    "accepts": ["path"],
                },
                "dependents": {
                    "on_event": "direct",
                },
                "compaction": {
                    "max_scope_keys": 25,
                },
                "webhook": {
                    "path": "/registry",
                },
                "poll": {
                    "interval_seconds": 60,
                },
            },
        }
    })

    merged = load_source_registry(p)["files"].sync_capabilities_for(
        Adapter(),
    )

    assert merged is not None
    assert merged.watch is not None
    assert merged.watch.roots == (f"{tmp_path}/repo",)
    assert merged.watch.include_globs == ("**/*.py",)
    assert merged.watch.exclude_globs == ("**/.git/**",)
    assert merged.triggers == ("fs_event", "manual")
    assert merged.default_event_mode == "reconcile"
    assert merged.reconcile_mode == "scope_complete"
    assert merged.deletes_require_reconcile is True
    assert merged.cheap_probe == "etag"
    assert merged.scope == SourceSyncScope(
        accepts=("path",),
        unsupported_fallback="reconcile",
    )
    assert merged.dependents == SourceSyncDependents(
        on_event="direct",
        on_reconcile="closure",
    )
    assert merged.compaction == SourceSyncCompaction(
        window_seconds=45,
        max_events=500,
        max_scope_keys=25,
    )
    assert merged.webhook == SourceSyncWebhook(
        path="/registry",
        secret_ref="ADAPTER_SECRET",
    )
    assert merged.poll == SourceSyncPoll(
        probe="change_probe",
        interval_seconds=60,
    )
    assert defaults.watch is not None
    assert defaults.watch.roots == ("/adapter-root",)
    assert defaults.triggers == ("poll",)
    assert defaults.scope == SourceSyncScope(
        accepts=("record_id",),
        unsupported_fallback="reconcile",
    )


def test_registry_rejects_malformed_credential_metadata(
    tmp_path: Path,
) -> None:
    base = {
        "ownership_id": "example.issues",
        "source_class": "mutable_api",
        "record_identity_kind": "remote_id",
        "record_identity_fields": ["item_key"],
        "source_revision_kind": "updated_at",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    p = _write_yaml(tmp_path, {
        "issues": {
            **base,
            "credential_refs": "EXAMPLE_API_TOKEN",
        }
    })
    with pytest.raises(ValueError, match="credential_refs"):
        load_source_registry(p)

    p = _write_yaml(tmp_path, {
        "issues": {
            **base,
            "credential_aliases": {
                "EXAMPLE_API_TOKEN": "LEGACY_EXAMPLE_TOKEN",
            },
        }
    })
    with pytest.raises(ValueError, match="credential_aliases"):
        load_source_registry(p)

    p = _write_yaml(tmp_path, {
        "issues": {
            **base,
            "required_for_baseline": "yes",
        }
    })
    with pytest.raises(ValueError, match="required_for_baseline"):
        load_source_registry(p)


def test_valid_append_feed(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "faers": {
            "ownership_id": "intersystems.openfda_faers",
            "source_class": "append_feed",
            "record_identity_kind": "event_id",
            "record_identity_fields": ["event_uuid"],
            "source_revision_kind": "sequence_number",
            "deletion_semantics": "append_only",
            "publication_mode": "published_generation",
        }
    })
    load_source_registry(p)


def test_valid_live_overlay(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "status_overlay": {
            "ownership_id": "example.status_overlay",
            "source_class": "live_overlay",
            "deletion_semantics": "external_overlay",
            "publication_mode": "live_overlay",
        }
    })
    load_source_registry(p)


def test_registry_only_skips_contract_keys(tmp_path: Path) -> None:
    """registry_only entries may omit identity/revision/deletion keys."""
    p = _write_yaml(tmp_path, {
        "future_source": {
            "ownership_id": "example.future",
            "source_class": REGISTRY_ONLY,
        }
    })
    out = load_source_registry(p)
    assert out["future_source"].is_registry_only


def test_source_registry_path_placeholders(tmp_path: Path) -> None:
    dep = tmp_path / "repo" / "deployments" / "okg-workspace"
    dep.mkdir(parents=True)
    p = dep / "source_registry.yaml"
    p.write_text(yaml.safe_dump({"sources": {
        "files": {
            "ownership_id": "demo.files",
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["repo", "path"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
            "params": {
                "repo_root": "${repo_root}",
                "fixture": "${deployment_dir}/fixtures/report.json",
                "legacy_anchor": "${manifest_dir}/source_registry.yaml",
                "registry_dir": "${source_registry_dir}",
            },
        }
    }}))

    out = load_source_registry(p)
    params = out["files"].raw["params"]
    assert params["repo_root"] == str(tmp_path / "repo")
    assert params["fixture"] == str(dep / "fixtures" / "report.json")
    assert params["legacy_anchor"] == str(dep / "source_registry.yaml")
    assert params["registry_dir"] == str(dep)


def test_invalid_tuple_rejected(tmp_path: Path) -> None:
    """The example tuple from tasks.md 3.1: a
    reference_catalog with mutable_api's deletion_semantics is
    rejected with a readable error.
    """
    p = _write_yaml(tmp_path, {
        "garbled": {
            "ownership_id": "example.garbled",
            "source_class": "reference_catalog",
            "record_identity_kind": "remote_id",  # wrong; should be domain_key
            "record_identity_fields": ["x"],
            "source_revision_kind": "updated_at",
            "deletion_semantics": "append_only",  # wrong
            "publication_mode": "published_generation",
        }
    })
    with pytest.raises(ValueError) as exc:
        load_source_registry(p)
    msg = str(exc.value)
    assert "reference_catalog" in msg
    assert "record_identity_kind" in msg
    assert "deletion_semantics" in msg


def test_unknown_source_class(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "x": {
            "ownership_id": "example.x",
            "source_class": "made_up_profile",
        }
    })
    with pytest.raises(ValueError, match="unknown source_class"):
        load_source_registry(p)


def test_legacy_entry_accepted(tmp_path: Path) -> None:
    """Existing entries without `source_class` validate as legacy."""
    p = _write_yaml(tmp_path, {
        "old": {
            "ownership_id": "example.old",
            "module": "foo.bar",
            "class": "BazSource",
            "params": {"tier": 1, "fixture": "data/foo.json"},
        }
    })
    out = load_source_registry(p)
    assert out["old"].legacy
    assert out["old"].source_class is None


def test_missing_ownership_id_rejected(tmp_path: Path) -> None:
    p = _write_yaml(tmp_path, {
        "x": {"source_class": "reference_catalog"}
    })
    with pytest.raises(ValueError, match="ownership_id"):
        load_source_registry(p)


def test_validate_profile_tuple_directly() -> None:
    """The pure validator works without YAML."""
    validate_profile_tuple(
        source_class="discovery_crawl",
        record_identity_kind="scoped_locator",
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
    )
    with pytest.raises(ValueError):
        validate_profile_tuple(
            source_class="discovery_crawl",
            record_identity_kind="domain_key",  # wrong
            source_revision_kind="content_hash",
            deletion_semantics="missing_from_completed_scope",
            publication_mode="published_generation",
        )
