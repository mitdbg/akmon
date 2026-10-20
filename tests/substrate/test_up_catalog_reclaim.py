"""Fixture-only catalog phase regression; no PostgreSQL or host services."""
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace

import pytest

from okg.substrate import deployment_upgrade as upgrade
from okg.substrate.catalog import ownership
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import CatalogOwnershipMismatch


class OwnershipStore:
    """Model the locked ownership row, keeping production identity/guards real."""

    def __init__(self, owner):
        self.owner = owner
        self.locked = False
        self.in_transaction = False
        self.schema_ready = False
        self.writes = 0
        # Whole INSERT parameter tuples. manifest_fingerprint is params[7];
        # recording only a few columns is why a re-armed manifest could pass
        # unnoticed here (review P2).
        self.claims: list[tuple] = []
        # #2229 one-time v1 -> v2 owner-key moves (guarded UPDATEs).
        self.rekeys: list[tuple] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    @contextmanager
    def transaction(self):
        saved = deepcopy(self.owner)
        self.in_transaction = True
        try:
            yield
        except Exception:
            self.owner = saved
            raise
        finally:
            self.locked = False
            self.in_transaction = False
            self.schema_ready = False

    def cursor(self):
        return self

    def execute(self, sql, params):
        if "FOR UPDATE" in sql:
            assert self.in_transaction and self.schema_ready
            self.locked = True
        elif "INSERT INTO okg.deployment_write_ownership" in sql:
            assert self.locked
            self.writes += 1
            self.claims.append(tuple(params))
            self.owner.update(
                owner_fingerprint=params[1], schema_fingerprint=params[2],
                catalog_fingerprint=params[3], manifest_fingerprint=params[7],
                metadata=params[8].obj,
            )
        elif "UPDATE okg.deployment_write_ownership" in sql:
            # _rekey_locked_owner: (new_fp, metadata patch, name, old_fp).
            assert self.locked
            new_fp, patch, _name, old_fp = params
            self.rowcount = 0
            if self.owner["owner_fingerprint"] == old_fp:
                self.rekeys.append(tuple(params))
                self.owner["owner_fingerprint"] = new_fp
                self.owner["metadata"] = {**self.owner["metadata"], **patch.obj}
                self.rowcount = 1
        else:
            raise AssertionError(sql)


@pytest.fixture
def fixture_catalog(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / ".git").mkdir(parents=True)
    deployment = root / "deployments" / "fixture"
    (deployment / "schemas").mkdir(parents=True)
    (deployment / "deployment.yaml").write_text("name: fixture\nmodules: []\n")
    schema = deployment / "schemas" / "fixture.yaml"
    schema.write_text(
        "classes:\n  Record:\n    annotations:\n      archetype: artifact\n"
    )
    monkeypatch.setattr(ownership, "_machine_id", lambda: "sha256:fixture-machine")

    def identity():
        return ownership.catalog_identity_for(
            composed=compose_catalog(deployment), deployment_dir=deployment,
            command="fixture catalog load",
        )

    old = identity()
    store = OwnershipStore({
        "deployment_name": "fixture", "owner_fingerprint": old.writer_fingerprint,
        "catalog_fingerprint": old.catalog_fingerprint,
        "schema_fingerprint": old.schema_fingerprint,
        "deployment_root_hash": old.deployment_root_hash,
        "worktree_root_hash": old.worktree_root_hash,
        "manifest_fingerprint": old.manifest_fingerprint,
        "metadata": old.provenance,
    })
    monkeypatch.setattr(ownership, "diagnose_catalog_ownership", lambda conn, **kw: {
        "owner": conn.owner, "status": "fixture", "recent_catalog_versions": [],
    })
    def ensure_schema(conn):
        assert conn.in_transaction
        conn.schema_ready = True

    monkeypatch.setattr(ownership, "ensure_catalog_ownership_schema", ensure_schema)
    monkeypatch.setattr(ownership, "owner_is_stale", lambda owner: (False, None))
    monkeypatch.setattr("psycopg.connect", lambda *a, **kw: store)
    monkeypatch.setattr(upgrade, "_resolve_upgrade_deployment", lambda ctx: SimpleNamespace(
        deployment_dir=deployment,
    ))
    monkeypatch.setattr(upgrade, "_ensure_backup_revalidated_for_mutation", lambda ctx: {"ok": True})
    monkeypatch.setattr(upgrade, "_latest_applied_catalog_fingerprint", lambda *a: old.catalog_fingerprint)
    monkeypatch.setattr(upgrade, "_restamp_manifest_fingerprint", lambda *a: {"restamped": True})
    loaded = []

    def load(ctx, command):
        current = identity()
        ownership.ensure_catalog_write_allowed(store, identity=current)
        loaded.append(current.catalog_fingerprint)
        return upgrade.CommandResult(0, '{"ok": true, "catalog_version_id": 2}', "")

    monkeypatch.setattr(upgrade, "_run_okg", load)
    ctx = SimpleNamespace(options=SimpleNamespace(plan_only=False, deployment="fixture"),
                          dsn="postgresql://fixture.invalid/fixture", data={})
    return SimpleNamespace(ctx=ctx, schema=schema, identity=identity, old=old,
                           store=store, loaded=loaded)


def edit_catalog(fixture):
    # A one-line catalog edit, composed by the real catalog implementation.
    fixture.schema.write_text(fixture.schema.read_text().replace("artifact", "entity"))


def refusal_reason(excinfo):
    # The machine-readable discriminator, not just the exception type: every
    # refusal below shares CatalogOwnershipMismatch (review P2).
    return excinfo.value.to_json()["error"]["data"]["reason"]


def make_v1_owner(fixture):
    # The pre-#2229 row: this writer's key bound the schema fingerprint the
    # row records.
    legacy = ownership.legacy_writer_fingerprint_for(
        fixture.old, fixture.old.schema_fingerprint,
    )
    assert legacy and legacy != fixture.old.writer_fingerprint
    fixture.store.owner["owner_fingerprint"] = legacy
    return legacy


def test_up_after_one_line_catalog_edit_keeps_the_writer(fixture_catalog):
    # #2229: an ontology edit moves the catalog, not the writer, so the
    # same writer loads it with no ownership write at all.
    f = fixture_catalog
    edit_catalog(f)
    current = f.identity()
    assert current.catalog_fingerprint != f.old.catalog_fingerprint
    assert current.writer_fingerprint == f.old.writer_fingerprint
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is True
    assert f.store.writes == 0
    assert f.store.rekeys == []
    assert f.store.owner["owner_fingerprint"] == current.writer_fingerprint
    assert f.loaded == [current.catalog_fingerprint]
    assert result["ownership_refresh"] == {"reclaimed": False}


def test_up_rekeys_a_v1_owner_once_after_a_catalog_edit(fixture_catalog):
    f = fixture_catalog
    legacy = make_v1_owner(f)
    edit_catalog(f)
    current = f.identity()
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is True
    # A guarded key move, not a forced claim.
    assert f.store.writes == 0
    assert f.store.claims == []
    assert len(f.store.rekeys) == 1
    assert f.store.owner["owner_fingerprint"] == current.writer_fingerprint
    audit = {
        "from": legacy,
        "to": current.writer_fingerprint,
        "command": "okg up (catalog ownership refresh)",
    }
    assert f.store.owner["metadata"]["writer_fingerprint_rekey"] == audit
    assert result["ownership_refresh"] == {
        "reclaimed": False, "rekeyed": True, **audit,
    }
    assert f.loaded == [current.catalog_fingerprint]

    # A second `up` finds the v2 key and moves nothing.
    second = upgrade._phase_catalog_apply(f.ctx)
    assert second["ok"] is True
    assert len(f.store.rekeys) == 1
    assert second["ownership_refresh"] == {"reclaimed": False}


@pytest.mark.parametrize("field", ["deployment_root_hash", "worktree_root_hash", "machine_id"])
@pytest.mark.parametrize("value", [None, "foreign"])
def test_up_refuses_foreign_or_missing_identity(fixture_catalog, field, value):
    # A foreign writer's row carries a key this writer cannot reproduce; the
    # same-root reclaim still requires every identity field to match.
    f = fixture_catalog
    edit_catalog(f)
    f.store.owner["owner_fingerprint"] = "sha256:foreign-writer"
    record = f.store.owner["metadata"] if field == "machine_id" else f.store.owner
    record[field] = value
    with pytest.raises(CatalogOwnershipMismatch) as excinfo:
        upgrade._phase_catalog_apply(f.ctx)
    assert refusal_reason(excinfo) == "catalog_ownership_foreign_writer"
    assert f.store.writes == 0
    assert f.store.rekeys == []
    assert f.loaded == []


def test_up_same_path_on_another_machine_is_the_same_writer(fixture_catalog):
    # #2229 decision A: the machine id is not part of the writer identity.
    f = fixture_catalog
    edit_catalog(f)
    f.store.owner["metadata"]["machine_id"] = "sha256:another-machine"
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is True
    assert f.store.writes == 0
    assert f.loaded == [f.identity().catalog_fingerprint]


def test_up_refuses_environment_scope(fixture_catalog):
    f = fixture_catalog
    edit_catalog(f)
    environment_writer = ownership.catalog_identity_for(
        composed=compose_catalog(f.schema.parent.parent),
        deployment_dir=f.schema.parent.parent,
        command="environment writer",
        writer_scope="environment",
        environment_id="env",
    )
    f.store.owner["owner_fingerprint"] = environment_writer.writer_fingerprint
    f.store.owner["metadata"].update(writer_scope="environment", environment_id="env")
    with pytest.raises(CatalogOwnershipMismatch) as excinfo:
        upgrade._phase_catalog_apply(f.ctx)
    assert refusal_reason(excinfo) == ownership.WRITER_SCOPE_CHANGE
    assert f.store.writes == 0
    assert f.loaded == []


@pytest.mark.parametrize("key", ["current", "legacy"])
def test_up_never_rearms_a_retired_manifest_fingerprint(
    fixture_catalog, monkeypatch, key,
):
    # #709/#828: a recorded manifest that differs from the live one (a branch
    # flip to a retired manifest) must keep refusing publish. #2229 review F1:
    # once this writer loads a changed catalog with no claim, the SECOND `up`
    # finds the catalog unchanged and takes the restamp path, which re-arms
    # the drifted manifest -- so the next publish succeeds. The reclaim must
    # refuse on the drift before the load, for this writer's v2 and v1 keys.
    f = fixture_catalog
    if key == "legacy":
        make_v1_owner(f)
    edit_catalog(f)
    f.store.owner["manifest_fingerprint"] = "sha256:retired-manifest"

    # The real convergence path: `up` skips the load when the latest applied
    # catalog equals the composed one, then restamps the manifest for the
    # owning writer (restamp_manifest_fingerprint's contract).
    monkeypatch.setattr(
        upgrade, "_latest_applied_catalog_fingerprint",
        lambda *a: f.loaded[-1] if f.loaded else f.old.catalog_fingerprint,
    )

    def restamp(ctx, deployment_dir):
        live = f.identity()
        if ownership._owner_key_match(live, f.store.owner) is None:
            return {"restamped": False, "reason": "held by a different writer"}
        prior = f.store.owner["manifest_fingerprint"]
        f.store.owner["manifest_fingerprint"] = live.manifest_fingerprint
        return {"restamped": True, "changed": prior != live.manifest_fingerprint}

    monkeypatch.setattr(upgrade, "_restamp_manifest_fingerprint", restamp)

    for _attempt in ("first up", "second up"):
        with pytest.raises(CatalogOwnershipMismatch) as excinfo:
            upgrade._phase_catalog_apply(f.ctx)
        assert refusal_reason(excinfo) == "catalog_ownership_foreign_writer"
        assert "manifest" in str(excinfo.value)
    assert f.loaded == []
    assert f.store.writes == 0
    assert f.store.claims == []
    assert f.store.rekeys == []
    assert f.store.owner["manifest_fingerprint"] == "sha256:retired-manifest"

    # And the publish-time continuity check still refuses the retired
    # manifest.
    with pytest.raises(CatalogOwnershipMismatch) as publish:
        ownership._enforce_manifest_continuity(
            identity=f.identity(), owner=f.store.owner,
        )
    assert refusal_reason(publish) == "catalog_manifest_drifted"


def test_up_loads_a_changed_catalog_when_the_recorded_manifest_is_absent(
    fixture_catalog,
):
    # A row claimed before manifest_fingerprint existed records None; that is
    # backfill, not drift, so the F1 refusal does not apply.
    f = fixture_catalog
    edit_catalog(f)
    f.store.owner["manifest_fingerprint"] = None
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is True
    assert result["ownership_refresh"] == {"reclaimed": False}
    assert f.loaded == [f.identity().catalog_fingerprint]


def test_up_refuses_to_reclaim_a_retired_manifest_for_an_unrecognized_key(
    fixture_catalog,
):
    # The pre-#2229 same-root reclaim survives for a key this writer cannot
    # reproduce; it still refuses rather than re-arm a drifted manifest.
    f = fixture_catalog
    edit_catalog(f)
    f.store.owner["owner_fingerprint"] = "sha256:unrecognized"
    f.store.owner["manifest_fingerprint"] = "sha256:retired-manifest"
    with pytest.raises(CatalogOwnershipMismatch) as excinfo:
        upgrade._phase_catalog_apply(f.ctx)
    assert refusal_reason(excinfo) == "catalog_ownership_foreign_writer"
    assert f.store.writes == 0
    assert f.store.claims == []
    assert f.store.owner["manifest_fingerprint"] == "sha256:retired-manifest"
    assert f.loaded == []


def test_up_reclaims_an_unrecognized_key_predating_the_manifest_column(
    fixture_catalog,
):
    # A row claimed before manifest_fingerprint existed records None; that is
    # backfill, not drift, so the same-root reclaim stays eligible.
    f = fixture_catalog
    edit_catalog(f)
    f.store.owner["owner_fingerprint"] = "sha256:unrecognized"
    f.store.owner["manifest_fingerprint"] = None
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is True
    assert f.store.writes == 1
    assert result["ownership_refresh"]["reclaimed"] is True
    assert f.store.owner["owner_fingerprint"] == f.identity().writer_fingerprint
    assert f.store.owner["manifest_fingerprint"] == f.identity().manifest_fingerprint


def test_failed_apply_still_reports_the_ownership_rekey(fixture_catalog, monkeypatch):
    # The key move precedes the apply and is not reverted when the apply
    # fails. The report must say ownership changed (review P2).
    f = fixture_catalog
    make_v1_owner(f)
    edit_catalog(f)

    def failed_load(ctx, command):
        ownership.ensure_catalog_write_allowed(f.store, identity=f.identity())
        return upgrade.CommandResult(1, "", "catalog apply exploded")

    monkeypatch.setattr(upgrade, "_run_okg", failed_load)
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is False
    assert result["reason"] == "catalog apply failed"
    assert result["ownership_refresh"]["rekeyed"] is True
    assert result["ownership_refresh"]["to"] == f.identity().writer_fingerprint
    assert len(f.store.rekeys) == 1


@pytest.mark.parametrize("mode", ["plan", "backup_failed", "unchanged", "no_physical_rollback"])
def test_nonmutating_paths_never_reclaim(fixture_catalog, monkeypatch, mode):
    f = fixture_catalog
    legacy = make_v1_owner(f)
    if mode != "unchanged":
        edit_catalog(f)
    if mode == "plan":
        f.ctx.options.plan_only = True
    if mode == "backup_failed":
        monkeypatch.setattr(upgrade, "_ensure_backup_revalidated_for_mutation", lambda ctx: {"ok": False})
    if mode == "no_physical_rollback":
        monkeypatch.setattr(upgrade, "_ensure_backup_revalidated_for_mutation", lambda ctx: {
            "ok": True, "verification_mode": "no_physical_rollback_required",
        })
    result = upgrade._phase_catalog_apply(f.ctx)
    assert result["ok"] is (mode != "backup_failed")
    assert f.store.writes == 0
    assert f.store.rekeys == []
    assert f.store.owner["owner_fingerprint"] == legacy
    assert f.loaded == []
