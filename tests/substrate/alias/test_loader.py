"""load_alias_resolver — deployment.yaml's alias_resolver block."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.alias import (
    AliasBackendLoadError,
    CascadeAliasResolver,
    load_alias_resolver,
)
from okg.substrate.embeddings import (
    EmbeddingConfig,
    build_embedding_execution_plan,
)


def _write_manifest(tmp_path: Path, doc: dict) -> Path:
    out = tmp_path / "dep"
    out.mkdir()
    (out / "deployment.yaml").write_text(yaml.safe_dump(doc))
    return out


def test_no_block_no_conn_raises(tmp_path: Path) -> None:
    """Default cascade needs SqlCascadeBackend which needs conn.
    Without one, the loader fails loudly rather than silently
    skipping resolution."""
    dep = _write_manifest(tmp_path, {
        "name": "x",
        "embeddings": {"enabled": True, "backend": "hash"},
    })
    with pytest.raises(AliasBackendLoadError):
        load_alias_resolver(dep, conn=None)


def test_no_manifest_no_conn_raises(tmp_path: Path) -> None:
    dep = tmp_path / "empty"
    dep.mkdir()
    with pytest.raises(AliasBackendLoadError):
        load_alias_resolver(dep, conn=None)


def test_no_block_with_conn_and_embedder_yields_two_backends(
    tmp_path: Path,
) -> None:
    """Per `auto-default-alias-resolver` (2026-04-26): the
    embedder-aware default returns SQL + embedding."""

    dep = _write_manifest(tmp_path, {
        "name": "x",
        "embeddings": {"enabled": True, "backend": "hash"},
    })
    # The constructors store conn / embedder but don't query
    # at __init__ time, so stub objects are fine.
    fake_conn = object()
    plan = build_embedding_execution_plan(
        EmbeddingConfig(enabled=True, backend="hash")
    )
    resolver = load_alias_resolver(
        dep, conn=fake_conn, embedding_plan=plan,
    )
    classes = [type(b).__name__ for b in resolver._backends]
    assert classes == [
        "SqlCascadeBackend", "EmbeddingAliasBackend",
    ]


def test_enabled_manifest_without_verified_plan_fails_closed(
    tmp_path: Path,
) -> None:
    dep = _write_manifest(tmp_path, {
        "name": "x",
        "embeddings": {"enabled": True, "backend": "hash"},
    })
    with pytest.raises(AliasBackendLoadError, match="verified"):
        load_alias_resolver(dep, conn=object())


def test_no_block_with_conn_no_embedder_yields_sql_only(
    tmp_path: Path,
) -> None:
    """Without an embedder, default falls back to SQL-only."""
    dep = _write_manifest(tmp_path, {"name": "x"})
    resolver = load_alias_resolver(dep, conn=object())
    classes = [type(b).__name__ for b in resolver._backends]
    assert classes == ["SqlCascadeBackend"]


@pytest.mark.parametrize("key", ["embedding", "embeddings"])
def test_no_block_with_conn_and_embedder_honors_embedding_disabled(
    tmp_path: Path,
    key: str,
) -> None:
    dep = _write_manifest(tmp_path, {
        "name": "x",
        key: {"enabled": False},
    })
    if key == "embedding":
        with pytest.warns(UserWarning, match="deprecated"):
            resolver = load_alias_resolver(dep, conn=object())
    else:
        resolver = load_alias_resolver(dep, conn=object())
    classes = [type(b).__name__ for b in resolver._backends]
    assert classes == ["SqlCascadeBackend"]


def test_static_backend_loads_without_conn(tmp_path: Path) -> None:
    """A deployment that pins a non-SQL backend should not need
    a conn at load time."""
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {
            "on_empty": "drop",
            "backends": [
                {
                    "class": "okg.substrate.alias.backends.StaticAliasBackend",
                    "config": {
                        "mapping": {},
                    },
                },
            ],
        },
    })
    # YAML can't represent tuple keys, so StaticAliasBackend's
    # mapping is empty here — that's fine for the loader contract.
    resolver = load_alias_resolver(dep, conn=None)
    assert isinstance(resolver, CascadeAliasResolver)


def test_sql_backend_config_loads_exact_only(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {
            "on_empty": "drop",
            "backends": [
                {
                    "class": "okg.substrate.alias.backends.SqlCascadeBackend",
                    "config": {"exact_only": True},
                },
            ],
        },
    })

    resolver = load_alias_resolver(dep, conn=object())
    assert resolver._backends[0].exact_only is True


def test_bad_class_path_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {
            "backends": [
                {"class": "okg.does.not.exist.Backend"},
            ],
        },
    })
    with pytest.raises(AliasBackendLoadError) as exc_info:
        load_alias_resolver(dep, conn=None)
    err = exc_info.value
    assert err.data["class"] == "okg.does.not.exist.Backend"


def test_malformed_class_path_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {
            "backends": [{"class": "no_dots_here"}],
        },
    })
    with pytest.raises(AliasBackendLoadError):
        load_alias_resolver(dep, conn=None)


def test_entry_without_class_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {"backends": [{"config": {}}]},
    })
    with pytest.raises(AliasBackendLoadError):
        load_alias_resolver(dep, conn=None)


def test_backends_not_a_list_raises(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {"backends": "not-a-list"},
    })
    with pytest.raises(AliasBackendLoadError):
        load_alias_resolver(dep, conn=None)


def test_constructor_typeerror_lifts(tmp_path: Path) -> None:
    dep = _write_manifest(tmp_path, {
        "alias_resolver": {
            "backends": [
                {
                    "class": "okg.substrate.alias.backends.StaticAliasBackend",
                    "config": {"unknown_kwarg": "x"},
                },
            ],
        },
    })
    with pytest.raises(AliasBackendLoadError) as exc_info:
        load_alias_resolver(dep, conn=None)
    err = exc_info.value
    assert "unknown_kwarg" in err.data["config_keys"]
    assert isinstance(err.__cause__, TypeError)
