"""Regression tests for bind-deployment-identity (#709).

A deployment's publish identity used to cover only a path + name + the
composed ontology fingerprint, never the manifest blocks that decide what
publishes. Flipping the worktree to a branch carrying a retired manifest
(same name, same ontology, different enrichers/maintained_derivations)
published silently. These tests pin the fix: the manifest content-hash is
recorded at claim and compared at publish, with a block-on-mismatch
posture, an armed-by-stamp refusal for identity-less publishes, a
legacy-NULL warn state, and a sanctioned re-claim escape hatch.

Design rulings R1-R5:
  pact/changes/bind-deployment-identity/reports/design-review.md
"""
from __future__ import annotations

import logging
from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.manifest_identity import (
    ManifestFingerprintError,
    manifest_fingerprint_for,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import ComposedCatalog, NarrowingRow
from okg.substrate.catalog.ownership import (
    CatalogIdentity,
    catalog_identity_for,
    claim_catalog_ownership,
    ensure_catalog_ownership_schema,
    ensure_catalog_write_allowed,
    resolve_catalog_for_publish,
)
from okg.substrate.errors import CatalogOwnershipMismatch, SubstrateError

# No file-level postgres mark: the census / fingerprint tests are pure
# set-arithmetic + tmp_path and MUST gate in the fast per-PR lane — a
# file-level mark hid the census coupling test in nightly-only sweeps,
# which is how deployment_up/host_storage landed on dev unclassified.
# DB-backed tests below carry @pytest.mark.postgres individually.


# --- fixtures ---------------------------------------------------------------

_MANIFEST_BASE = """name: {name}
modules: []
enrichers:
  - class: okg.demo.EnricherA
    config:
      weight: 1
maintained_derivations:
  producers:
    - producer_one
viz:
  theme: cool
mcp_instructions: instructions.md
"""

# Same name + ontology, different behavior-bearing blocks: the #709 flip.
_MANIFEST_FLIP = """name: {name}
modules: []
enrichers:
  - class: okg.demo.EnricherA
    config:
      weight: 2
maintained_derivations:
  producers:
    - producer_one
    - producer_two
viz:
  theme: cool
mcp_instructions: instructions.md
"""

# Same behavior, different read-side surface only (viz theme + mcp file):
# must NOT change the manifest fingerprint.
_MANIFEST_READONLY_EDIT = """name: {name}
modules: []
enrichers:
  - class: okg.demo.EnricherA
    config:
      weight: 1
maintained_derivations:
  producers:
    - producer_one
viz:
  theme: warm
mcp_instructions: other.md
"""


def _write_manifest(dep_dir: Path, name: str, template: str) -> None:
    dep_dir.mkdir(parents=True, exist_ok=True)
    (dep_dir / "deployment.yaml").write_text(template.format(name=name))


def _composed(name: str) -> ComposedCatalog:
    return ComposedCatalog(
        deployment_name=name,
        modules=[],
        subtypes={"source_file", "git_commit"},
        subtype_origin={
            "source_file": f"deployment:{name}",
            "git_commit": "test",
        },
        narrowings=[
            NarrowingRow(
                parent_subtype="source_file",
                edge_archetype="references",
                child_subtype="git_commit",
                origin="test",
                narrowing_name="file_references_commit",
            )
        ],
    )


def _identity(dep_dir: Path, name: str, template: str) -> CatalogIdentity:
    _write_manifest(dep_dir, name, template)
    return catalog_identity_for(
        composed=_composed(name),
        deployment_dir=dep_dir,
        command="test",
    )


def _reset_catalog(conn: psycopg.Connection) -> None:
    ensure_catalog_ownership_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE okg.deployment_write_ownership")
        cur.execute("TRUNCATE okg.catalog_versions CASCADE")
        cur.execute(
            """
            INSERT INTO okg.catalog_versions
              (catalog_version_id, status, applied_at,
               diff_summary, reconcile_plan)
            VALUES
              (1, 'applied', now(),
               '{"reason":"test seed"}'::jsonb,
               '{"reconcile":[]}'::jsonb)
            """
        )
        cur.execute(
            """
            SELECT setval(
              pg_get_serial_sequence(
                'okg.catalog_versions', 'catalog_version_id'
              ),
              1
            )
            """
        )


def _claim_and_apply(
    conn: psycopg.Connection,
    identity: CatalogIdentity,
) -> int:
    """Claim ownership + apply the ontology so a compatible cv exists."""
    ensure_catalog_write_allowed(conn, identity=identity)
    return apply_composed_catalog(
        conn,
        _composed(identity.deployment_name),
        identity=identity,
    )


# --- census (ruling R1) -----------------------------------------------------
# The census coupling test moved to
# tests/substrate/test_manifest_fingerprint_census.py (unmarked, so the
# per-PR fast lane runs it) per PACT per-pr-manifest-census-gate — this
# file's postgres mark had demoted it to nightly-only coverage, which is how
# deployment_up/host_storage shipped unclassified.


# Ops/lifecycle blocks classified OUT: present or edited, they must not
# perturb the manifest fingerprint (a branch flip changing only these
# never refuses a publish). Pins the classification, not just the
# partition — a mistaken IN would hash the block and fail here.
_MANIFEST_OPS_BLOCKS = """deployment_up:
  backup:
    restore_peak_factor: 2.0
host_storage:
  min_free_gib: 25
"""


def test_out_classified_ops_blocks_do_not_perturb_fingerprint(
    tmp_path: Path,
) -> None:
    dep = tmp_path / "dep"
    base = _identity(dep, "ops-out-fixture", _MANIFEST_BASE)
    with_ops = _identity(
        dep, "ops-out-fixture", _MANIFEST_BASE + _MANIFEST_OPS_BLOCKS
    )
    edited_ops = _identity(
        dep,
        "ops-out-fixture",
        _MANIFEST_BASE
        + _MANIFEST_OPS_BLOCKS.replace("2.0", "3.5").replace("25", "80"),
    )
    assert base.manifest_fingerprint is not None
    assert with_ops.manifest_fingerprint == base.manifest_fingerprint, (
        "adding deployment_up/host_storage (OUT) must not change identity"
    )
    assert edited_ops.manifest_fingerprint == base.manifest_fingerprint, (
        "editing deployment_up/host_storage (OUT) must not change identity"
    )


# --- claim-hash task (req.deployment-identity.enforced) ---------------------


def test_manifest_flip_changes_recorded_identity(tmp_path: Path) -> None:
    """Two manifests differing only in enrichers/maintained_derivations
    produce different manifest fingerprints; a read-side-only edit does
    not. Fails pre-fix, where CatalogIdentity has no manifest binding."""
    dep = tmp_path / "dep"
    base = _identity(dep, "flip-fixture", _MANIFEST_BASE)
    flip = _identity(dep, "flip-fixture", _MANIFEST_FLIP)
    readonly = _identity(dep, "flip-fixture", _MANIFEST_READONLY_EDIT)

    assert base.manifest_fingerprint is not None
    assert flip.manifest_fingerprint != base.manifest_fingerprint, (
        "enricher/maintained_derivation drift must change the identity"
    )
    assert readonly.manifest_fingerprint == base.manifest_fingerprint, (
        "read-side-only edits (viz/mcp) must not break identity"
    )
    # The ontology (writer/catalog fingerprint) is unchanged across all
    # three -- this is precisely the #709 blind spot the manifest hash closes.
    assert flip.writer_fingerprint == base.writer_fingerprint
    assert flip.catalog_fingerprint == base.catalog_fingerprint


@pytest.mark.postgres
def test_claim_records_manifest_fingerprint(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    dep = tmp_path / "dep"
    identity = _identity(dep, "claim-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        report = claim_catalog_ownership(conn, identity=identity, force=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT manifest_fingerprint,
                       metadata->'manifest_block_hashes'
                  FROM okg.deployment_write_ownership
                 WHERE deployment_name = %s
                """,
                (identity.deployment_name,),
            )
            row = cur.fetchone()

    assert row[0] == identity.manifest_fingerprint
    assert "enrichers" in (row[1] or {})
    assert report["owner"]["manifest_fingerprint"] == (
        identity.manifest_fingerprint
    )


# --- publish-check task (req.deployment-identity.enforced) ------------------


@pytest.mark.postgres
def test_publish_refuses_manifest_flip(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """#709: claim manifest A, then a branch flip to retired manifest B
    (same name + ontology) refuses at publish. Pre-fix resolve returns a
    catalog version silently."""
    dep = tmp_path / "dep"
    base = _identity(dep, "publish-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)

        # Sanity: publishing the claimed manifest succeeds.
        assert resolve_catalog_for_publish(
            conn, identity=base, deployment_name=base.deployment_name
        ) > 0

        # The #709 flip: same dir, retired manifest content.
        flip = _identity(dep, "publish-fixture", _MANIFEST_FLIP)
        assert flip.writer_fingerprint == base.writer_fingerprint

        with pytest.raises(CatalogOwnershipMismatch) as exc:
            resolve_catalog_for_publish(
                conn, identity=flip, deployment_name=flip.deployment_name
            )
    assert "manifest" in str(exc.value).lower()
    assert "enrichers" in (exc.value.data or {}).get("observed", {}).get(
        "drifted_blocks", []
    )


@pytest.mark.postgres
def test_reclaim_then_publish_succeeds(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A sanctioned re-claim of the edited manifest publishes again."""
    dep = tmp_path / "dep"
    base = _identity(dep, "reclaim-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)
        flip = _identity(dep, "reclaim-fixture", _MANIFEST_FLIP)

        with pytest.raises(CatalogOwnershipMismatch):
            resolve_catalog_for_publish(
                conn, identity=flip, deployment_name=flip.deployment_name
            )

        # Break-glass: explicit re-claim of the edited manifest.
        report = claim_catalog_ownership(conn, identity=flip, force=True)

        cv = resolve_catalog_for_publish(
            conn, identity=flip, deployment_name=flip.deployment_name
        )
    assert cv > 0
    assert report["owner"]["manifest_fingerprint"] == flip.manifest_fingerprint


@pytest.mark.postgres
def test_identityless_publish_refuses_when_armed(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Armed-by-stamp (R5b): a publish carrying no CatalogIdentity refuses
    once the ownership row records a manifest fingerprint -- this is the
    DBOS-worker hole (#709) that a CLI-only guard would miss."""
    dep = tmp_path / "dep"
    base = _identity(dep, "armed-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)

        with pytest.raises(SubstrateError) as exc:
            resolve_catalog_for_publish(
                conn,
                identity=None,
                deployment_name=base.deployment_name,
            )
    assert exc.value.code == "catalog_writer_scope_required"


@pytest.mark.postgres
def test_identityless_legacy_null_refuses_without_writer_scope(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A legacy NULL stamp cannot substitute for explicit writer scope."""
    dep = tmp_path / "dep"
    identity = _identity(dep, "legacy-unwired-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, identity)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.deployment_write_ownership
                   SET manifest_fingerprint = NULL,
                       metadata = metadata - 'manifest_block_hashes'
                 WHERE deployment_name = %s
                """,
                (identity.deployment_name,),
            )

        with pytest.raises(SubstrateError) as exc:
            resolve_catalog_for_publish(
                conn,
                identity=None,
                deployment_name=identity.deployment_name,
            )
    assert exc.value.code == "catalog_writer_scope_required"


@pytest.mark.postgres
def test_legacy_null_row_warns_and_publishes(
    clean_runtime: str,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A claim that predates #709 (NULL manifest_fingerprint) publishes
    with a loud warning naming the backfill command, not a refusal (R4)."""
    dep = tmp_path / "dep"
    identity = _identity(dep, "legacy-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        cv = _claim_and_apply(conn, identity)
        # Simulate a pre-#709 claim: erase the recorded manifest hash.
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.deployment_write_ownership
                   SET manifest_fingerprint = NULL,
                       metadata = metadata - 'manifest_block_hashes'
                 WHERE deployment_name = %s
                """,
                (identity.deployment_name,),
            )

        with caplog.at_level(logging.WARNING, logger="okg.catalog.ownership"):
            resolved = resolve_catalog_for_publish(
                conn, identity=identity,
                deployment_name=identity.deployment_name,
            )
    assert resolved == cv
    assert any(
        "predates manifest-identity" in rec.message
        and "claim" in rec.message
        for rec in caplog.records
    )


@pytest.mark.postgres
def test_publish_refuses_when_live_manifest_uncomputable(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A live identity that could not compute a manifest fingerprint
    refuses against an armed row (R3: compute-failure is never a warn)."""
    dep = tmp_path / "dep"
    base = _identity(dep, "compute-fail-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)

        import dataclasses

        blind = dataclasses.replace(
            base, manifest_fingerprint=None, manifest_block_hashes=None
        )
        with pytest.raises(CatalogOwnershipMismatch) as exc:
            resolve_catalog_for_publish(
                conn, identity=blind, deployment_name=blind.deployment_name
            )
    assert "could not compute a manifest identity" in str(exc.value)


def test_unreadable_manifest_raises_not_defaults(tmp_path: Path) -> None:
    """A present-but-unreadable manifest is a hard failure, not a benign
    default (R3, no-benign-defaults)."""
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text("name: x\nenrichers: [oops\n")  # bad
    with pytest.raises(ManifestFingerprintError):
        manifest_fingerprint_for(dep)


def test_missing_manifest_is_permissive(tmp_path: Path) -> None:
    """No manifest file -> (None, {}); pathless/legacy callers stay
    permissive until a stamped row arms enforcement (R5b)."""
    fp, blocks = manifest_fingerprint_for(tmp_path / "absent")
    assert fp is None
    assert blocks == {}


# --- runtime-shaped construction (ruling R5a) -------------------------------


def test_runtime_path_builds_manifest_bound_identity(tmp_path: Path) -> None:
    """The runtime helper (used by the DBOS worker) constructs a
    manifest-bound identity, so a flip changes it -- a CLI-only guard
    cannot cover this surface (#709)."""
    from okg.substrate.publisher.deployment_stack import (
        build_runtime_catalog_identity,
    )

    # A real deployment dir needs a composable schema; use the packaged
    # default deployment so compose_catalog succeeds, and assert the
    # helper binds the manifest.
    from okg.substrate.deployments import find_deployment_dir

    dep_dir = find_deployment_dir("okg-workspace") or find_deployment_dir(
        "default"
    )
    if dep_dir is None:
        pytest.skip("no packaged deployment available to compose")
    identity = build_runtime_catalog_identity(
        dsn="postgresql://test-authority",
        deployment_name=dep_dir.name,
        deployment_dir=dep_dir,
        command="test",
    )
    assert identity is not None
    assert identity.manifest_fingerprint is not None


# --- deployment up restamp (ruling R4) --------------------------------------


@pytest.mark.postgres
def test_restamp_updates_owning_writer_only(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The ontology-unchanged `okg up` restamp refreshes the owning
    writer's manifest fingerprint, converging an enricher-only edit so a
    later publish does not refuse it (R4)."""
    from okg.substrate.catalog.ownership import restamp_manifest_fingerprint

    dep = tmp_path / "dep"
    base = _identity(dep, "restamp-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)
        flip = _identity(dep, "restamp-fixture", _MANIFEST_FLIP)

        # Pre-restamp: the flip refuses.
        with pytest.raises(CatalogOwnershipMismatch):
            resolve_catalog_for_publish(
                conn, identity=flip, deployment_name=flip.deployment_name
            )

        result = restamp_manifest_fingerprint(conn, identity=flip)
        cv = resolve_catalog_for_publish(
            conn, identity=flip, deployment_name=flip.deployment_name
        )
    assert result["restamped"] is True
    assert result["changed"] is True
    assert cv > 0


@pytest.mark.postgres
def test_restamp_noops_for_foreign_writer(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    from okg.substrate.catalog.ownership import restamp_manifest_fingerprint

    dep_a = tmp_path / "a"
    dep_b = tmp_path / "b"
    owner = _identity(dep_a, "foreign-fixture", _MANIFEST_BASE)
    # Different deployment_dir path -> different writer_fingerprint.
    intruder = _identity(dep_b, "foreign-fixture", _MANIFEST_FLIP)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, owner)
        result = restamp_manifest_fingerprint(conn, identity=intruder)
    assert result["restamped"] is False


# --- #828: refusals are self-diagnosing from str(exc) alone -----------------


@pytest.mark.postgres
def test_refusal_message_is_self_diagnosing(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """#828: DBOS workflow_status and worker logs persist only str(exc);
    the data dict is dropped. The drift refusal's message alone must name
    both fingerprints, the drifted blocks, and be self-dating, so a stale
    terminal ERROR row cannot be mistaken for a live refusal."""
    dep = tmp_path / "dep"
    base = _identity(dep, "diagnosis-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)
        flip = _identity(dep, "diagnosis-fixture", _MANIFEST_FLIP)

        with pytest.raises(CatalogOwnershipMismatch) as exc:
            resolve_catalog_for_publish(
                conn, identity=flip, deployment_name=flip.deployment_name
            )

    message = str(exc.value)
    assert base.manifest_fingerprint in message, "recorded fp not in message"
    assert flip.manifest_fingerprint in message, "live fp not in message"
    assert "enrichers" in message, "drifted block names not in message"
    assert "maintained_derivations" in message
    assert "refused_at=" in message, "refusal is not self-dating"
    assert "recorded_claim_at=" in message, "claim time not in message"
    assert str(dep) in message, "live deployment dir not in message"


@pytest.mark.postgres
def test_identityless_refusal_names_scope_and_deployment(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Missing authority refuses before deriving context from DB history."""
    dep = tmp_path / "dep"
    base = _identity(dep, "armed-dated-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _claim_and_apply(conn, base)

        with pytest.raises(SubstrateError) as exc:
            resolve_catalog_for_publish(
                conn, identity=None, deployment_name=base.deployment_name
            )
    assert exc.value.code == "catalog_writer_scope_required"
    assert exc.value.data == {
        "reason_code": "catalog_writer_scope_required",
        "deployment_name": base.deployment_name,
    }


# --- #828: mark_catalog_version_seen owner guard + backfill-only fp ---------


@pytest.mark.postgres
def test_mark_seen_foreign_writer_cannot_restamp(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A caller that does not own the row must raise, not silently restamp
    catalog/schema fingerprints (and must never poison the row into the
    'writer matches, manifest differs' refusal state)."""
    from okg.substrate.catalog.ownership import mark_catalog_version_seen

    dep_a = tmp_path / "a"
    dep_b = tmp_path / "b"
    owner = _identity(dep_a, "seen-foreign-fixture", _MANIFEST_BASE)
    intruder = _identity(dep_b, "seen-foreign-fixture", _MANIFEST_FLIP)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        cv = _claim_and_apply(conn, owner)

        with pytest.raises(CatalogOwnershipMismatch):
            mark_catalog_version_seen(
                conn, identity=intruder, catalog_version_id=cv,
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT owner_fingerprint, manifest_fingerprint,
                       catalog_fingerprint, schema_fingerprint
                  FROM okg.deployment_write_ownership
                 WHERE deployment_name = %s
                """,
                (owner.deployment_name,),
            )
            row = cur.fetchone()
    assert row[0] == owner.writer_fingerprint
    assert row[1] == owner.manifest_fingerprint
    assert row[2] == owner.catalog_fingerprint
    assert row[3] == owner.schema_fingerprint


@pytest.mark.postgres
def test_mark_seen_keeps_recorded_fp_on_owned_flip(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """#828: the recorded manifest fingerprint is backfill-only. An owned
    catalog apply from a flipped branch (same writer, drifted manifest)
    must NOT re-arm the row with the drifted hash -- otherwise
    `okg catalog load --apply` silently disarms the continuity guard and
    the next publish sails through the exact hole #709 closed."""
    from okg.substrate.catalog.ownership import mark_catalog_version_seen

    dep = tmp_path / "dep"
    base = _identity(dep, "seen-flip-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        cv = _claim_and_apply(conn, base)
        flip = _identity(dep, "seen-flip-fixture", _MANIFEST_FLIP)
        assert flip.writer_fingerprint == base.writer_fingerprint

        mark_catalog_version_seen(conn, identity=flip, catalog_version_id=cv)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT manifest_fingerprint,
                       metadata->'manifest_block_hashes'
                  FROM okg.deployment_write_ownership
                 WHERE deployment_name = %s
                """,
                (base.deployment_name,),
            )
            row = cur.fetchone()
        # Recorded fp and its block breakdown still describe the CLAIMED
        # manifest, so the flip still refuses at publish.
        assert row[0] == base.manifest_fingerprint
        assert row[1] == base.manifest_block_hashes

        with pytest.raises(CatalogOwnershipMismatch):
            resolve_catalog_for_publish(
                conn, identity=flip, deployment_name=flip.deployment_name
            )


@pytest.mark.postgres
def test_mark_seen_backfills_null_fp(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Ruling R4 backfill intent survives the hardening: a pre-#709 row
    (NULL fingerprint) is stamped by the owner's next apply."""
    from okg.substrate.catalog.ownership import mark_catalog_version_seen

    dep = tmp_path / "dep"
    base = _identity(dep, "seen-backfill-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        cv = _claim_and_apply(conn, base)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.deployment_write_ownership
                   SET manifest_fingerprint = NULL,
                       metadata = metadata - 'manifest_block_hashes'
                 WHERE deployment_name = %s
                """,
                (base.deployment_name,),
            )

        mark_catalog_version_seen(conn, identity=base, catalog_version_id=cv)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT manifest_fingerprint,
                       metadata->'manifest_block_hashes'
                  FROM okg.deployment_write_ownership
                 WHERE deployment_name = %s
                """,
                (base.deployment_name,),
            )
            row = cur.fetchone()
    assert row[0] == base.manifest_fingerprint
    assert "enrichers" in (row[1] or {})


@pytest.mark.postgres
def test_mark_seen_no_row_is_permissive(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """No ownership row (fresh/legacy DB where ensure_catalog_write_allowed
    deliberately claimed nothing) stays a permissive no-op -- the guard
    must not break first-ever catalog loads."""
    from okg.substrate.catalog.ownership import mark_catalog_version_seen

    dep = tmp_path / "dep"
    base = _identity(dep, "seen-norow-fixture", _MANIFEST_BASE)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        mark_catalog_version_seen(conn, identity=base, catalog_version_id=1)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.deployment_write_ownership"
            )
            assert cur.fetchone()[0] == 0
