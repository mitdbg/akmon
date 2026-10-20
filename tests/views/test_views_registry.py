"""Tests for ``okg.substrate.views.registry`` — Slice B of
``viz-materialized-view-nodes``.

Three layers:

* **Parser unit tests** (no DB) — schema validation, error
  messages, idempotent parsing.
* **Postgres-backed apply tests** — Chronos-graph upsert of
  ``view_definition`` nodes (``okg.graph_nodes`` via
  ``ChronosGraphStore`` on the default branch); idempotent reload;
  retraction on YAML edit. Post-cutover ``apply_views_registry`` keys
  on ``node_id`` (one row per node, no bitemporal ``valid_*`` window)
  and does NOT write ``okg.node_facts``.
* **Discovery integration test** — end-to-end:
  ``parse_views_registry`` → ``apply_views_registry`` →
  ``materializers_from_catalog`` builds a real
  :class:`ViewMaterializer` whose factory + build_corpus
  resolve correctly via importlib.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.views import registry as view_registry
from okg.substrate.views import (
    ViewMaterializer,
    ViewRegistryEntry,
    apply_views_registry,
    load_views_registry,
    materializers_from_catalog,
    parse_views_registry,
)


def _live_view_definition(
    conn: psycopg.Connection, node_id: str
) -> dict | None:
    """Read a view_definition node from the Chronos default-branch graph.

    apply_views_registry writes ``view_definition`` nodes to
    okg.graph_nodes on the default branch; this is the cutover
    replacement for the legacy ``SELECT ... FROM okg.nodes_live``
    reads. Returns a dict with ``subtype``/``attrs`` or None.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    rows = store.checkout().query(
        """
        SELECT subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = :node_id
        """,
        {"node_id": node_id},
    )
    return rows[0] if rows else None


def _live_view_definition_ids(conn: psycopg.Connection) -> set[str]:
    """node_ids of all live ``view_definition`` nodes (default branch)."""
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    rows = store.checkout().query(
        "SELECT node_id FROM okg.graph_nodes WHERE subtype = 'view_definition'"
    )
    return {row["node_id"] for row in rows}

_AF_PUBS_DEPLOYMENT = Path(__file__).resolve().parents[2] / "deployments" / "af_pubs"
_AF_PUBS_SUPERSEDURE_VIEW = (
    _AF_PUBS_DEPLOYMENT / "views" / "supersedure_health.py"
)
_ENVIRONMENT_FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "substrate/fixtures/deployment_environments"
)


def test_active_environment_accepts_shared_definition_registry_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = _ENVIRONMENT_FIXTURES / "fixture-staging/environment.yaml"
    monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", str(descriptor))
    monkeypatch.setenv("TEST_STATE_ROOT", str(tmp_path / "state"))
    monkeypatch.setenv(
        "TEST_STAGING_DSN",
        "postgresql://postgres:test@127.0.0.1:55435/fixture_staging",
    )

    assert view_registry._accepted_registry_deployments(
        "fixture-graph-staging"
    ) == {"fixture-graph-staging", "shared-repository-graph"}


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class TestParseViewsRegistry:
    def test_minimal_valid_entry(self):
        data = {
            "deployment": "af_pubs",
            "views": [{
                "view_def_id": "af_pubs.x",
                "view_def_factory": "views.x:factory",
                "build_corpus": "views.x:build_corpus",
            }],
        }
        entries = parse_views_registry(data)
        assert len(entries) == 1
        e = entries[0]
        assert e.view_def_id == "af_pubs.x"
        assert e.deployment == "af_pubs"
        assert e.view_name == "af_pubs.x"  # falls back to id
        assert e.schedule == ""
        assert e.params == {}
        assert e.llm_client == "stub"

    def test_view_name_overrides_default(self):
        data = {
            "deployment": "af_pubs",
            "views": [{
                "view_def_id": "af_pubs.x",
                "view_name": "Pretty Name",
                "view_def_factory": "v:f",
                "build_corpus": "v:b",
            }],
        }
        entries = parse_views_registry(data)
        assert entries[0].view_name == "Pretty Name"

    def test_disabled_registry_returns_no_entries(self):
        data = {
            "deployment": "af_pubs",
            "enabled": False,
            "views": [{
                "view_def_id": "af_pubs.x",
                "view_def_factory": "v:f",
                "build_corpus": "v:b",
            }],
        }
        assert parse_views_registry(data) == []

    def test_enabled_must_be_boolean(self):
        with pytest.raises(SubstrateError, match=r"enabled.*boolean"):
            parse_views_registry({
                "deployment": "af_pubs",
                "enabled": "false",
                "views": [],
            })

    def test_missing_deployment_raises(self):
        with pytest.raises(SubstrateError, match=r"deployment"):
            parse_views_registry({"views": []})

    def test_views_must_be_list(self):
        with pytest.raises(SubstrateError, match=r"views.*list"):
            parse_views_registry({"deployment": "x", "views": "not-a-list"})

    def test_missing_required_field_raises(self):
        for missing in ("view_def_id", "view_def_factory", "build_corpus"):
            entry = {
                "view_def_id": "x",
                "view_def_factory": "v:f",
                "build_corpus": "v:b",
            }
            entry.pop(missing)
            with pytest.raises(SubstrateError, match=missing):
                parse_views_registry({
                    "deployment": "x",
                    "views": [entry],
                })

    def test_malformed_import_ref_raises(self):
        with pytest.raises(SubstrateError, match="module:attribute"):
            parse_views_registry({
                "deployment": "x",
                "views": [{
                    "view_def_id": "x",
                    "view_def_factory": "missing_colon",
                    "build_corpus": "v:b",
                }],
            })

    def test_duplicate_view_def_id_raises(self):
        entry = {
            "view_def_id": "x",
            "view_def_factory": "v:f",
            "build_corpus": "v:b",
        }
        with pytest.raises(SubstrateError, match=r"duplicate"):
            parse_views_registry({
                "deployment": "x",
                "views": [entry, entry],
            })

    def test_load_views_registry_returns_empty_when_missing(self, tmp_path):
        # No file → empty list, not error.
        assert load_views_registry(tmp_path) == []

    def test_load_views_registry_parses_real_file(self, tmp_path):
        (tmp_path / "views_registry.yaml").write_text(
            "deployment: x\n"
            "views:\n"
            "  - view_def_id: x.a\n"
            "    view_def_factory: v:f\n"
            "    build_corpus: v:b\n"
        )
        entries = load_views_registry(tmp_path)
        assert len(entries) == 1
        assert entries[0].view_def_id == "x.a"


class TestEntrySerialization:
    def test_to_node_attrs_carries_required_fields(self):
        e = ViewRegistryEntry(
            view_def_id="x",
            view_name="X",
            deployment="d",
            view_def_factory="v:f",
            build_corpus="v:b",
            schedule="*/5 * * * *",
            params={"k": "v"},
            llm_client="stub",
        )
        attrs = e.to_node_attrs()
        assert attrs["view_def_id"] == "x"
        assert attrs["deployment"] == "d"
        assert attrs["params"] == {"k": "v"}
        assert attrs["schedule"] == "*/5 * * * *"
        assert attrs["llm_client"] == "stub"
        assert "view_def_hash" in attrs
        assert len(attrs["view_def_hash"]) == 16

    def test_view_def_hash_changes_with_params(self):
        base = dict(
            view_def_id="x", view_name="X", deployment="d",
            view_def_factory="v:f", build_corpus="v:b",
        )
        a = ViewRegistryEntry(**base, params={"opr": "AF/A3"})
        b = ViewRegistryEntry(**base, params={"opr": "AF/A4"})
        assert a.to_node_attrs()["view_def_hash"] != \
            b.to_node_attrs()["view_def_hash"]


# ---------------------------------------------------------------------------
# Apply (Postgres-backed)
# ---------------------------------------------------------------------------


def _insert_generation(
    conn: psycopg.Connection, *,
    deployment_name: str = "test",
    status: str = "published",
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations
              (deployment_name, status, catalog_version_id,
               correlation_id, published_at)
            VALUES (%s, %s, 1, %s, now())
            RETURNING generation_id
            """,
            (deployment_name, status, f"test-{deployment_name}"),
        )
        return int(cur.fetchone()[0])


def _entry(view_def_id: str, **overrides) -> ViewRegistryEntry:
    base = dict(
        view_def_id=view_def_id,
        view_name=view_def_id,
        deployment="test",
        view_def_factory="views.dummy:factory",
        build_corpus="views.dummy:build_corpus",
    )
    base.update(overrides)
    return ViewRegistryEntry(**base)


@pytest.fixture
def conn(clean_runtime):
    with psycopg.connect(clean_runtime, autocommit=True) as c:
        # Ensure the Chronos schema + default branch exist before
        # apply_views_registry (and the read helpers) run.
        ChronosGraphStore.from_connection(c, ensure_schema=True)
        yield c


@pytest.mark.postgres
class TestApplyViewsRegistry:
    def test_first_apply_inserts_view_definition_node(self, conn):
        gen = _insert_generation(conn)
        entries = [_entry("test.a")]
        result = apply_views_registry(
            conn, entries,
            deployment_name="test",
            generation_id=gen,
        )
        assert result.upserted == 1
        assert result.retracted == 0

        row = _live_view_definition(conn, "view_definition:test.a")
        assert row is not None
        assert row["subtype"] == "view_definition"
        assert row["attrs"]["view_def_id"] == "test.a"
        assert row["attrs"]["deployment"] == "test"

    def test_idempotent_reapply_is_noop(self, conn):
        gen = _insert_generation(conn)
        entries = [_entry("test.a")]
        apply_views_registry(
            conn, entries,
            deployment_name="test",
            generation_id=gen,
        )

        # Run again at a different gen — but with same content.
        gen2 = _insert_generation(conn)
        result = apply_views_registry(
            conn, entries,
            deployment_name="test",
            generation_id=gen2,
        )
        # Same content → no-op (the bitemporal upsert
        # short-circuits on equal attrs).
        assert result.upserted == 0
        assert result.retracted == 0

        # One live view_definition node on the Chronos default branch.
        assert "view_definition:test.a" in _live_view_definition_ids(conn)
        assert _live_view_definition(conn, "view_definition:test.a") is not None

    def test_change_in_attrs_upserts_node_in_place(self, conn):
        gen = _insert_generation(conn)
        apply_views_registry(
            conn, [_entry("test.a", params={"opr": "AF/A3"})],
            deployment_name="test",
            generation_id=gen,
        )

        gen2 = _insert_generation(conn)
        result = apply_views_registry(
            conn, [_entry("test.a", params={"opr": "AF/A4"})],
            deployment_name="test",
            generation_id=gen2,
        )
        assert result.upserted == 1

        # Chronos cutover: apply_views_registry keys on node_id, so an
        # attr change upserts the single graph node in place (no
        # bitemporal old/new row pair). The visible attrs carry AF/A4.
        ids = _live_view_definition_ids(conn)
        assert ids == {"view_definition:test.a"}
        row = _live_view_definition(conn, "view_definition:test.a")
        assert row is not None
        assert row["attrs"]["params"]["opr"] == "AF/A4"

    def test_same_generation_attr_change_updates_in_place(self, conn):
        gen = _insert_generation(conn)
        apply_views_registry(
            conn, [_entry("test.a", params={"opr": "AF/A3"})],
            deployment_name="test",
            generation_id=gen,
        )

        result = apply_views_registry(
            conn, [_entry("test.a", params={"opr": "AF/A4"})],
            deployment_name="test",
            generation_id=gen,
        )
        assert result.upserted == 1

        # Same generation: the node is upserted in place — one live node
        # carrying the latest attrs (AF/A4). apply_views_registry writes
        # only to the Chronos graph, not okg.node_facts.
        ids = _live_view_definition_ids(conn)
        assert ids == {"view_definition:test.a"}
        row = _live_view_definition(conn, "view_definition:test.a")
        assert row is not None
        assert row["attrs"]["params"]["opr"] == "AF/A4"

    def test_removed_entry_is_retracted(self, conn):
        gen = _insert_generation(conn)
        apply_views_registry(
            conn, [_entry("test.a"), _entry("test.b")],
            deployment_name="test",
            generation_id=gen,
        )
        gen2 = _insert_generation(conn)
        # Drop af_pubs.b from the YAML.
        result = apply_views_registry(
            conn, [_entry("test.a")],
            deployment_name="test",
            generation_id=gen2,
        )
        assert result.retracted == 1

        # Chronos cutover: a retraction removes the node from the default
        # branch entirely (no bitemporal "closed at gen2" row). test.a
        # remains live; test.b is gone.
        ids = _live_view_definition_ids(conn)
        assert "view_definition:test.a" in ids
        assert "view_definition:test.b" not in ids

    def test_same_generation_removed_entry_deletes_live_row(self, conn):
        gen = _insert_generation(conn)
        apply_views_registry(
            conn, [_entry("test.a"), _entry("test.b")],
            deployment_name="test",
            generation_id=gen,
        )

        result = apply_views_registry(
            conn, [_entry("test.a")],
            deployment_name="test",
            generation_id=gen,
        )
        assert result.retracted == 1

        # Same-generation removal deletes the node from the default
        # branch: only test.a remains live.
        assert _live_view_definition_ids(conn) == {"view_definition:test.a"}

    def test_apply_isolates_by_deployment(self, conn):
        # Other deployment's view_definitions are untouched.
        gen_a = _insert_generation(conn, deployment_name="test")
        gen_o = _insert_generation(conn, deployment_name="other")
        apply_views_registry(
            conn, [_entry("test.x")],
            deployment_name="test", generation_id=gen_a,
        )
        # Manually seed a view_definition for a DIFFERENT deployment
        # into the Chronos graph — should not be retracted by the test
        # deployment's apply.
        other_store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )
        other_store.apply_facts(
            [
                NodeFact(
                    "view_definition:other.y",
                    "view_definition",
                    {"view_def_id": "other.y", "deployment": "other"},
                    {"view_def_id": "other.y"},
                    {"generation_id": gen_o},
                )
            ],
            source="catalog:views_registry",
        )

        # Re-apply the test registry with one entry — other.y
        # must remain untouched (still live on the default branch).
        gen_a2 = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn, [_entry("test.x")],
            deployment_name="test", generation_id=gen_a2,
        )
        assert (
            _live_view_definition(conn, "view_definition:other.y")
            is not None
        )  # still live

    # NOTE: the legacy node_facts audit-trail tests
    # (test_node_facts_audit_trail_for_upsert / _for_retraction) were
    # removed in the Chronos cutover: apply_views_registry now writes
    # only the Chronos graph (okg.graph_nodes) via
    # ChronosGraphStore.apply_facts, which by design does NOT write
    # okg.node_facts. There is no node_facts audit trail to assert.

    def test_no_published_generation_raises(self, conn):
        # No generation seeded → resolve_published fails loudly.
        with pytest.raises(SubstrateError, match="no published"):
            apply_views_registry(
                conn, [_entry("test.a")],
                deployment_name="test",
            )

    def test_resolves_chronos_published_generation_without_legacy_row(self, conn):
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.checkpoint_generation(
            "okg:test:branch:default:gen:chronos-only",
            deployment_name="test",
            status="published",
            metadata={"target_generation_id": 4242},
        )

        result = apply_views_registry(
            conn,
            [_entry("test.a")],
            deployment_name="test",
        )

        assert result.generation_id == 4242
        assert result.upserted == 1
        row = _live_view_definition(conn, "view_definition:test.a")
        assert row is not None
        assert row["attrs"]["view_def_id"] == "test.a"

    def test_resolves_legacy_generation_when_chronos_metadata_missing(
        self, conn, monkeypatch,
    ):
        gen = _insert_generation(conn)

        def _missing_chronos_metadata(*_args, **_kwargs):
            raise psycopg.errors.UndefinedTable("missing chronos metadata")

        monkeypatch.setattr(
            ChronosGraphStore,
            "from_connection",
            _missing_chronos_metadata,
        )

        assert (
            view_registry._resolve_latest_published_generation(conn, "test")
            == gen
        )

    def test_deployment_mismatch_raises(self, conn):
        gen = _insert_generation(conn)
        bad_entry = _entry("test.a")
        # Caller asks for deployment 'other' but entry says 'af_pubs'.
        with pytest.raises(SubstrateError, match="declares.*deployment"):
            apply_views_registry(
                conn, [bad_entry],
                deployment_name="other",
                generation_id=gen,
            )

    def test_active_environment_projects_shared_registry_as_runtime_identity(
        self,
        conn,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import okg.substrate.deployments.environment as environment_module

        monkeypatch.setenv("OKG_ENVIRONMENT_DESCRIPTOR", "/sealed/environment.yaml")
        monkeypatch.setattr(
            environment_module,
            "load_environment",
            lambda _descriptor: SimpleNamespace(
                id="fixture-staging",
                graph_deployment_name="test",
                definition_id="shared-repository-graph",
            ),
        )
        generation = _insert_generation(conn)

        result = apply_views_registry(
            conn,
            [_entry("shared.view", deployment="shared-repository-graph")],
            deployment_name="test",
            generation_id=generation,
        )

        assert result.upserted == 1
        row = _live_view_definition(conn, "view_definition:shared.view")
        assert row is not None
        assert row["attrs"]["deployment"] == "test"


# ---------------------------------------------------------------------------
# Discovery — materializers_from_catalog end-to-end
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestMaterializersFromCatalog:
    @pytest.mark.skipif(
        not _AF_PUBS_SUPERSEDURE_VIEW.is_file(),
        reason="af_pubs deployment views are optional in this workspace",
    )
    def test_resolves_factory_and_build_corpus_via_importlib(
        self, conn,
    ):
        # Insert the af_pubs views.sql + a generation.
        # Use the supersedure_health view's REAL refs so
        # importlib actually has to resolve them. The test's
        # conftest already adds deployments/af_pubs/ to sys.path.
        gen = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn,
            [ViewRegistryEntry(
                view_def_id="af_pubs.supersedure_health",
                view_name="Supersedure-chain health",
                deployment="test",
                view_def_factory=(
                    "views.supersedure_health:supersedure_health_view_def"
                ),
                build_corpus="views.supersedure_health:build_corpus",
            )],
            deployment_name="test",
            generation_id=gen,
        )

        deployment_root = (
            Path(__file__).resolve().parents[2]
            / "deployments" / "af_pubs"
        )
        materializers = materializers_from_catalog(
            conn,
            deployment_name="test",
            deployment_root=deployment_root,
        )
        assert len(materializers) == 1
        m = materializers[0]
        assert isinstance(m, ViewMaterializer)
        assert m.name == "af_pubs.supersedure_health"
        # Calling the factory returns a real ViewDefinition.
        view_def = m.view_def_factory()
        assert view_def.view_id == "af_pubs.supersedure_health"
        # build_corpus is callable; takes (conn, *, generation_id).
        assert callable(m.build_corpus)
        # llm_client defaults to DeterministicStubLLM.
        from okg.views.execution.llm import DeterministicStubLLM
        assert isinstance(m.llm_client, DeterministicStubLLM)

    def test_unknown_module_raises_at_discovery(
        self, conn,
    ):
        gen = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn,
            [_entry(
                "af_pubs.broken",
                view_def_factory="views.does_not_exist:factory",
                build_corpus="views.does_not_exist:build_corpus",
            )],
            deployment_name="test",
            generation_id=gen,
        )
        deployment_root = (
            Path(__file__).resolve().parents[2]
            / "deployments" / "af_pubs"
        )
        with pytest.raises(SubstrateError, match="not found"):
            materializers_from_catalog(
                conn,
                deployment_name="test",
                deployment_root=deployment_root,
            )

    def test_unknown_attribute_raises_at_discovery(
        self, conn,
    ):
        if not _AF_PUBS_SUPERSEDURE_VIEW.is_file():
            pytest.skip("af_pubs deployment views are optional in this workspace")
        gen = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn,
            [_entry(
                "af_pubs.broken",
                view_def_factory="views.supersedure_health:not_a_thing",
                build_corpus="views.supersedure_health:build_corpus",
            )],
            deployment_name="test",
            generation_id=gen,
        )
        deployment_root = (
            Path(__file__).resolve().parents[2]
            / "deployments" / "af_pubs"
        )
        with pytest.raises(SubstrateError, match="no attribute"):
            materializers_from_catalog(
                conn,
                deployment_name="test",
                deployment_root=deployment_root,
            )

    def test_litellm_spec_builds_litellm_client(self, conn):
        if not _AF_PUBS_SUPERSEDURE_VIEW.is_file():
            pytest.skip("af_pubs deployment views are optional in this workspace")
        gen = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn,
            [ViewRegistryEntry(
                view_def_id="af_pubs.supersedure_health",
                view_name="x",
                deployment="test",
                view_def_factory=(
                    "views.supersedure_health:supersedure_health_view_def"
                ),
                build_corpus="views.supersedure_health:build_corpus",
                llm_client="litellm:claude-sonnet-4-5-20250929",
            )],
            deployment_name="test",
            generation_id=gen,
        )
        deployment_root = (
            Path(__file__).resolve().parents[2]
            / "deployments" / "af_pubs"
        )
        materializers = materializers_from_catalog(
            conn,
            deployment_name="test",
            deployment_root=deployment_root,
        )
        from okg.views.execution.llm import LiteLLMClient
        assert isinstance(materializers[0].llm_client, LiteLLMClient)
        assert materializers[0].llm_client.model == (
            "claude-sonnet-4-5-20250929"
        )
        assert materializers[0].llm_client._timeout_s == 60.0

    def test_none_llm_client_yields_none(self, conn):
        if not _AF_PUBS_SUPERSEDURE_VIEW.is_file():
            pytest.skip("af_pubs deployment views are optional in this workspace")
        gen = _insert_generation(conn, deployment_name="test")
        apply_views_registry(
            conn,
            [ViewRegistryEntry(
                view_def_id="af_pubs.supersedure_health",
                view_name="x",
                deployment="test",
                view_def_factory=(
                    "views.supersedure_health:supersedure_health_view_def"
                ),
                build_corpus="views.supersedure_health:build_corpus",
                llm_client="none",
            )],
            deployment_name="test",
            generation_id=gen,
        )
        deployment_root = (
            Path(__file__).resolve().parents[2]
            / "deployments" / "af_pubs"
        )
        materializers = materializers_from_catalog(
            conn,
            deployment_name="test",
            deployment_root=deployment_root,
        )
        assert materializers[0].llm_client is None
