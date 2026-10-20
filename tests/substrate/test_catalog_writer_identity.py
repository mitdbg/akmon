"""#2229: the catalog writer is the deployment and its claim, not the schema.

No database. Adding a module (or any ontology edit) moves the catalog and
schema fingerprints but leaves the writer fingerprint alone; a different
checkout path, deployment name, or environment id is still a different
writer; and the v1 (schema-derived) key is reproduced exactly so rows
written before the change are recognized.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    legacy_writer_fingerprint_for,
    writer_key_match,
)


def _dir(root: Path, name: str, deployment: str = "writer-id") -> Path:
    path = root / name / deployment
    path.mkdir(parents=True)
    (path / "deployment.yaml").write_text(
        f"name: {deployment}\nmodules: []\n", encoding="utf-8",
    )
    return path


def _catalog(deployment: str = "writer-id", *modules: str) -> ComposedCatalog:
    return ComposedCatalog(deployment_name=deployment, modules=["core", *modules])


def _identity(path: Path, catalog: ComposedCatalog, environment_id=None):
    return catalog_identity_for(
        composed=catalog,
        deployment_dir=path,
        command="test",
        writer_scope="environment" if environment_id else "deployment_root",
        environment_id=environment_id,
    )


@pytest.mark.parametrize("environment_id", [None, "env-production"])
def test_adding_a_module_moves_the_catalog_not_the_writer(
    tmp_path: Path, environment_id,
) -> None:
    path = _dir(tmp_path, "checkout")
    before = _identity(path, _catalog(), environment_id)
    after = _identity(path, _catalog("writer-id", "extraction"), environment_id)

    assert before.catalog_fingerprint != after.catalog_fingerprint
    assert before.schema_fingerprint != after.schema_fingerprint
    assert before.writer_fingerprint == after.writer_fingerprint


def test_a_different_checkout_path_is_a_different_writer(tmp_path: Path) -> None:
    one = _identity(_dir(tmp_path, "one"), _catalog())
    two = _identity(_dir(tmp_path, "two"), _catalog())

    assert one.catalog_fingerprint == two.catalog_fingerprint
    assert one.writer_fingerprint != two.writer_fingerprint


def test_a_different_deployment_name_is_a_different_writer(tmp_path: Path) -> None:
    one = _identity(_dir(tmp_path, "one", "alpha"), _catalog("alpha"))
    two = _identity(_dir(tmp_path, "one", "beta"), _catalog("beta"))

    assert one.writer_fingerprint != two.writer_fingerprint


def test_a_different_environment_id_is_a_different_writer(tmp_path: Path) -> None:
    path = _dir(tmp_path, "sealed")
    production = _identity(path, _catalog(), "env-production")
    staging = _identity(path, _catalog(), "env-staging")

    assert production.writer_fingerprint != staging.writer_fingerprint


def _v1(payload: dict) -> str:
    # The pre-#2229 formula, written out independently of the module.
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.mark.parametrize("environment_id", [None, "env-production"])
def test_the_legacy_key_reproduces_the_v1_writer_fingerprint(
    tmp_path: Path, environment_id,
) -> None:
    identity = _identity(_dir(tmp_path, "checkout"), _catalog(), environment_id)
    if environment_id:
        expected = _v1({
            "deployment_name": "writer-id",
            "schema_fingerprint": identity.schema_fingerprint,
            "environment_id": environment_id,
        })
    else:
        expected = _v1({
            "deployment_name": "writer-id",
            "schema_fingerprint": identity.schema_fingerprint,
            "deployment_root_hash": identity.deployment_root_hash,
            "worktree_root_hash": identity.worktree_root_hash,
        })

    legacy = legacy_writer_fingerprint_for(identity, identity.schema_fingerprint)

    assert legacy == expected
    assert legacy != identity.writer_fingerprint
    assert writer_key_match(
        identity, legacy, schema_fingerprint=identity.schema_fingerprint,
    ) == "legacy"
    assert writer_key_match(
        identity, identity.writer_fingerprint, schema_fingerprint=None,
    ) == "current"


def test_a_foreign_v1_key_is_not_this_writer(tmp_path: Path) -> None:
    mine = _identity(_dir(tmp_path, "mine"), _catalog())
    theirs = _identity(_dir(tmp_path, "theirs"), _catalog())
    foreign_v1 = legacy_writer_fingerprint_for(theirs, theirs.schema_fingerprint)

    assert writer_key_match(
        mine, foreign_v1, schema_fingerprint=theirs.schema_fingerprint,
    ) is None
    # A v1 key only matches with the schema it was built from.
    own_v1 = legacy_writer_fingerprint_for(mine, mine.schema_fingerprint)
    assert writer_key_match(
        mine, own_v1, schema_fingerprint="sha256:" + "0" * 64,
    ) is None
