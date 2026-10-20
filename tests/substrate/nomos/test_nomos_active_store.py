from __future__ import annotations

import json
import uuid

import psycopg
import pytest

from okg.substrate.db.migrate import migrate
from okg.substrate.nomos import (
    DecisionInput,
    NomosPolicyError,
    NomosRuntimeRequest,
    activate_config_set,
    activate_policy_bundle,
    authorize_source_run_with_active_policy,
    active_nomos_summary,
    authorize_graph_export_with_active_policy,
    compile_config_graph,
    compile_policy_graph,
    deployment_baseline_policy_graph,
    evaluate_runtime_boundary_with_active_policy,
    evaluate_runtime_boundary,
    evaluate,
    evaluate_with_active_policy,
    persist_nomos_decision,
    read_nodes_with_active_policy,
    resolve_active_config_entry,
    seed_compatibility_config_set,
    seed_compatibility_policy_bundle,
    upsert_config_set,
    upsert_policy_bundle,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_compile_config_graph_is_deterministic_and_rejects_plaintext_secret() -> None:
    graph = _config_graph()

    first = compile_config_graph(graph)
    second = compile_config_graph(graph)

    assert first.config_fingerprint == second.config_fingerprint
    assert [entry.config_key for entry in first.entries] == [
        "runtime.max_batch",
        "runtime.openai_token",
    ]

    unsafe = _config_graph()
    unsafe["entries"].append({
        "config_key": "runtime.password",
        "value": "plaintext-secret",
    })
    with pytest.raises(NomosPolicyError, match="must use secret_ref"):
        compile_config_graph(unsafe)


# ---- api_key-class secret detection
# (require-secret-refs-not-literals) ----


@pytest.mark.parametrize(
    "key",
    ["api_key", "apikey", "access_key", "client_secret", "openai_api_key"],
)
def test_compile_config_graph_rejects_literal_api_key_class(key: str) -> None:
    graph = _config_graph()
    graph["entries"].append({"config_key": f"runtime.{key}", "value": "sk-literal"})
    with pytest.raises(NomosPolicyError, match="must use secret_ref"):
        compile_config_graph(graph)


@pytest.mark.parametrize("key", ["api_key", "apikey", "access_key"])
def test_compile_config_graph_accepts_api_key_secret_ref(key: str) -> None:
    graph = _config_graph()
    graph["entries"].append({
        "config_key": f"runtime.{key}",
        "secret_ref": "secret://okg/some-key",
        "secret": True,
    })
    out = compile_config_graph(graph)  # no raise
    assert any(e.config_key == f"runtime.{key}" for e in out.entries)


def test_seed_compatibility_rejects_literal_api_key() -> None:
    # compile_config_graph raises before the connection is used, so a literal
    # api_key bootstrap setting is refused with no DB.
    with pytest.raises(NomosPolicyError, match="must use secret_ref"):
        seed_compatibility_config_set(
            None,  # never reached
            deployment_name="boot",
            scope="deployment:boot",
            settings={"api_key": "sk-literal"},
            activated_by="test:operator",
        )


def test_deployment_baseline_policy_allows_reads_but_not_admin_override() -> None:
    manifest = {
        "nomos": {
            "deployment_posture": {
                "deployment_class": "dogfood_workspace",
                "read_principals": ["human_operator", "mcp_client"],
                "write_principals": ["okg_service", "source_adapter"],
                "export_policy": "internal_only",
                "live_call_default": "allow",
                "mcp_write_default": "review",
                "secret_handling": "secret_ref_required",
            },
        },
    }
    bundle = compile_policy_graph(
        deployment_baseline_policy_graph(
            manifest,
            deployment_name="okg-workspace",
            scope="deployment:okg-workspace",
            activated_by="subject:rollout",
            source_names=["git-files", "okg-workspace.git-files"],
            source_policies={
                "git-files": {"live_call_allowed": False},
                "github-issues": {"live_call_allowed": True},
                "okg-workspace.github-issues": {"live_call_allowed": True},
            },
            runtime_source_names=["git-files", "github-issues"],
        ),
    )
    source_lineage_rule = next(
        rule
        for rule in bundle.rules
        if rule.rule_id == "rollout_allow_source_lineage_filtered_reads"
    )

    allowed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="graph.read.cli_search",
            subject="subject:human_operator",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
                "source_id": "git-files",
                "source_ids": ["git-files"],
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit", "review"),
    )
    arbitrary_mcp = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="mcp.tool.call",
            subject="subject:anyone",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit", "review"),
    )
    source_allowed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.emit",
            subject="source:git-files",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    source_alias_allowed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.emit",
            subject="source:okg-workspace.git-files",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    source_other_deployment_denied = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.emit",
            subject="source:other-deployment.github-issues",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    source_live_call_denied = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.live_call",
            subject="source:git-files",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    source_live_call_allowed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.live_call",
            subject="source:github-issues",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit", "review"),
    )
    source_live_call_alias_allowed = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.live_call",
            subject="source:okg-workspace.github-issues",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit", "review"),
    )
    source_live_call_other_deployment_denied = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="source.live_call",
            subject="source:other-deployment.github-issues",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit", "review"),
    )
    admin_denied = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="admin.override",
            subject="subject:human_operator",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )
    secret_denied = evaluate_runtime_boundary(
        bundle,
        NomosRuntimeRequest(
            surface="secret.use",
            subject="subject:okg_service",
            resource={
                "deployment": "okg-workspace",
                "scope": "deployment:okg-workspace",
            },
        ),
        active_policy_fingerprint=bundle.policy_fingerprint,
        performed_obligations=("audit",),
    )

    assert allowed.allowed is True
    assert source_lineage_rule.match["source_ids"] == [
        "git-files",
        "github-issues",
        "okg-workspace.git-files",
        "okg-workspace.github-issues",
    ]
    assert allowed.decision.reason_code == "nomos.rollout.baseline_read_principal_allow"
    assert source_allowed.allowed is True
    # The deployment-qualified source subject (source:okg-workspace.git-files)
    # is now an accepted variant of the registered source: the baseline binds
    # both the bare and the deployment-qualified subject so production adapters
    # that emit under the qualified subject are not default-denied
    # (PACT repair-okg-ci-source-health). This is a deliberate widening of the
    # *registered* source set, not of admin/secret/arbitrary-mcp boundaries,
    # which still default-deny below.
    assert source_alias_allowed.allowed is True
    assert source_alias_allowed.decision.reason_code == (
        "nomos.rollout.baseline_registered_source_allow"
    )
    # Federation lineage scoping: a source subject qualified by a *different*
    # deployment is not a variant of this deployment's registered source and
    # still default-denies (PACT tighten-nomos-federation-lineage).
    assert source_other_deployment_denied.allowed is False
    assert source_other_deployment_denied.decision.reason_code == "nomos.default_deny"
    assert source_live_call_denied.allowed is False
    assert source_live_call_denied.decision.reason_code == "nomos.default_deny"
    assert source_live_call_allowed.allowed is True
    assert source_live_call_allowed.final_effect == "allow"
    assert source_live_call_allowed.decision.reason_code == (
        "nomos.rollout.baseline_source_live_call_allow"
    )
    # Qualified live-call subject is likewise an accepted variant of the
    # explicitly live-call-allowed source.
    assert source_live_call_alias_allowed.allowed is True
    assert source_live_call_alias_allowed.final_effect == "allow"
    assert source_live_call_alias_allowed.decision.reason_code == (
        "nomos.rollout.baseline_source_live_call_allow"
    )
    # A live-call subject qualified by a *different* deployment is still
    # default-denied (PACT tighten-nomos-federation-lineage).
    assert source_live_call_other_deployment_denied.allowed is False
    assert (
        source_live_call_other_deployment_denied.decision.reason_code
        == "nomos.default_deny"
    )
    assert arbitrary_mcp.allowed is False
    assert arbitrary_mcp.decision.reason_code == "nomos.default_deny"
    # Security property under test: the baseline never grants admin.override or
    # secret.use to any subject (including read principals) — both default-deny.
    assert admin_denied.allowed is False
    assert admin_denied.decision.reason_code == "nomos.default_deny"
    assert secret_denied.allowed is False
    assert secret_denied.decision.reason_code == "nomos.default_deny"


def test_deployment_baseline_policy_derives_bounded_mcp_record_grants() -> None:
    deployment = "record-baseline"
    scope = f"deployment:{deployment}"

    def _bundle(mode: str | None):
        posture = {
            "deployment_class": "dogfood_workspace",
            "read_principals": ["human_operator"],
            "write_principals": ["mcp_client", "agent"],
            "export_policy": "no_export",
            "live_call_default": "deny",
            "secret_handling": "secret_ref_required",
        }
        if mode is not None:
            posture["mcp_write_default"] = mode
        return compile_policy_graph(
            deployment_baseline_policy_graph(
                {"nomos": {"deployment_posture": posture}},
                deployment_name=deployment,
                scope=scope,
                activated_by="subject:nomos-rollout",
            )
        )

    review = _bundle("review")
    record_rules = [
        rule for rule in review.rules if "mcp.record" in rule.actions
    ]
    assert len(record_rules) == 1
    rule = record_rules[0]
    assert rule.actions == ("mcp.record",)
    assert rule.subjects == ("subject:agent", "subject:mcp_client")
    assert rule.match == {"deployment": deployment, "scope": scope}
    assert rule.obligations == ({"type": "audit"},)

    def _record_outcome(bundle, subject: str, *, request_deployment=deployment):
        request_scope = f"deployment:{request_deployment}"
        return evaluate_runtime_boundary(
            bundle,
            NomosRuntimeRequest(
                surface="mcp.record",
                subject=subject,
                resource={
                    "deployment": request_deployment,
                    "scope": request_scope,
                    "surface": "mcp",
                    "tool": "record",
                    "record_kind": "observation",
                },
            ),
            active_policy_fingerprint=bundle.policy_fingerprint,
            performed_obligations=("audit",),
        )

    assert _record_outcome(review, "subject:mcp_client").allowed is True
    assert _record_outcome(review, "subject:agent").allowed is True
    assert _record_outcome(review, "subject:undeclared").allowed is False
    assert (
        _record_outcome(
            review,
            "subject:mcp_client",
            request_deployment="other-deployment",
        ).allowed
        is False
    )

    for denied in (_bundle("deny"), _bundle(None)):
        assert all("mcp.record" not in rule.actions for rule in denied.rules)
        assert _record_outcome(denied, "subject:mcp_client").allowed is False


def test_active_policy_and_config_round_trip_through_postgres(
    clean_runtime: str,
) -> None:
    scope = "deployment:nomos-active"
    bundle = compile_policy_graph(_policy_graph())
    config_set = compile_config_graph(_config_graph())

    with psycopg.connect(clean_runtime) as conn:
        policy_activation = activate_policy_bundle(
            conn,
            deployment_name="nomos-active",
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
        )
        config_activation = activate_config_set(
            conn,
            deployment_name="nomos-active",
            config_set=config_set,
            scope=scope,
            activated_by="test:operator",
        )
        allowed = evaluate_with_active_policy(
            conn,
            deployment_name="nomos-active",
            scope=scope,
            decision_input=DecisionInput(
                subject="source:fixture",
                action="source.emit",
                classification="public",
                resource={"deployment": "nomos-active", "scope": scope},
            ),
            enforcement_point="test.active",
        )
        denied = evaluate_with_active_policy(
            conn,
            deployment_name="nomos-active",
            scope=scope,
            decision_input=DecisionInput(
                subject="source:blocked",
                action="source.emit",
                classification="public",
                resource={"deployment": "nomos-active", "scope": scope},
            ),
            enforcement_point="test.active",
        )
        entry = resolve_active_config_entry(
            conn,
            deployment_name="nomos-active",
            scope=scope,
            config_key="runtime.max_batch",
        )
        summary = active_nomos_summary(
            conn,
            deployment_name="nomos-active",
            scope=scope,
        )
        conn.commit()

    assert policy_activation.startswith("nomos:policy_activation:")
    assert config_activation.startswith("nomos:config_activation:")
    assert allowed.effect == "allow"
    assert denied.effect == "deny"
    assert entry.value == 2000
    assert summary["active_policy"]["policy_fingerprint"] == (
        bundle.policy_fingerprint
    )
    assert summary["active_config"]["config_fingerprint"] == (
        config_set.config_fingerprint
    )
    assert summary["recent_decision_count"] == 4

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.nomos_policy_activations
                   SET status = 'disabled'
                 WHERE deployment_name = 'nomos-active'
                """
            )
        fallback = evaluate_with_active_policy(
            conn,
            deployment_name="nomos-active",
            scope=scope,
            decision_input=DecisionInput(
                subject="source:fixture",
                action="source.emit",
                classification="public",
                resource={"deployment": "nomos-active", "scope": scope},
            ),
            enforcement_point="test.active",
        )
        conn.rollback()

    assert fallback.effect == "deny"
    assert fallback.reason_code == "nomos.default_deny.no_active_bundle"


def test_activation_records_explicit_decisions_when_runtime_enforcement_disabled(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = "deployment:nomos-opt-out"
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "0")
    bundle = compile_policy_graph(_policy_graph() | {
        "policy_bundle_id": "nomos:test:activation-opt-out",
        "scope": {"deployment": "nomos-opt-out", "scope": scope},
    })
    config_set = compile_config_graph(_config_graph() | {
        "config_set_id": "nomos:test:activation-opt-out-config",
        "scope": {"deployment": "nomos-opt-out", "scope": scope},
    })

    with psycopg.connect(clean_runtime) as conn:
        policy_activation = activate_policy_bundle(
            conn,
            deployment_name="nomos-opt-out",
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
        )
        config_activation = activate_config_set(
            conn,
            deployment_name="nomos-opt-out",
            config_set=config_set,
            scope=scope,
            activated_by="test:operator",
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.activation_decision_id, d.effect, d.reason_code,
                   d.target->>'kind', d.target->>'activation_path'
              FROM okg.nomos_policy_activations a
              JOIN okg.nomos_decisions d
                ON d.deployment_name = a.deployment_name
               AND d.decision_id = a.activation_decision_id
             WHERE a.activation_id = %s
            """,
            (policy_activation,),
        )
        policy_row = cur.fetchone()
        cur.execute(
            """
            SELECT a.activation_decision_id, d.effect, d.reason_code,
                   d.target->>'kind', d.target->>'activation_path'
              FROM okg.nomos_config_activations a
              JOIN okg.nomos_decisions d
                ON d.deployment_name = a.deployment_name
               AND d.decision_id = a.activation_decision_id
             WHERE a.activation_id = %s
            """,
            (config_activation,),
        )
        config_row = cur.fetchone()

    for row, kind in (
        (policy_row, "policy_activation"),
        (config_row, "config_activation"),
    ):
        assert row is not None
        decision_id, effect, reason_code, target_kind, activation_path = row
        assert decision_id
        assert effect == "allow"
        assert reason_code == "nomos.runtime.opt_out.activation"
        assert target_kind == kind
        assert activation_path == "runtime_enforcement_opt_out"


def test_migrate_backfills_nullable_activation_decisions(
    clean_runtime: str,
) -> None:
    scope = "deployment:nomos-legacy-null"
    bundle = compile_policy_graph(_policy_graph() | {
        "policy_bundle_id": "nomos:test:legacy-null-policy",
        "scope": {"deployment": "nomos-legacy-null", "scope": scope},
    })
    config_set = compile_config_graph(_config_graph() | {
        "config_set_id": "nomos:test:legacy-null-config",
        "scope": {"deployment": "nomos-legacy-null", "scope": scope},
    })

    with psycopg.connect(clean_runtime) as conn:
        upsert_policy_bundle(
            conn,
            deployment_name="nomos-legacy-null",
            bundle=bundle,
            status="active",
        )
        upsert_config_set(
            conn,
            deployment_name="nomos-legacy-null",
            config_set=config_set,
            status="active",
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                ALTER TABLE okg.nomos_policy_activations
                  ALTER COLUMN activation_decision_id DROP NOT NULL
                """
            )
            cur.execute(
                """
                ALTER TABLE okg.nomos_config_activations
                  ALTER COLUMN activation_decision_id DROP NOT NULL
                """
            )
            cur.execute(
                """
                INSERT INTO okg.nomos_policy_activations (
                  deployment_name, activation_id, scope, policy_fingerprint,
                  status, activated_by, activation_decision_id
                )
                VALUES (
                  'nomos-legacy-null',
                  'nomos:policy_activation:legacy-null',
                  %(scope)s,
                  %(policy_fingerprint)s,
                  'active',
                  'test:legacy-migrate',
                  NULL
                )
                """,
                {
                    "scope": scope,
                    "policy_fingerprint": bundle.policy_fingerprint,
                },
            )
            cur.execute(
                """
                INSERT INTO okg.nomos_config_activations (
                  deployment_name, activation_id, scope, config_fingerprint,
                  status, activated_by, activation_decision_id
                )
                VALUES (
                  'nomos-legacy-null',
                  'nomos:config_activation:legacy-null',
                  %(scope)s,
                  %(config_fingerprint)s,
                  'active',
                  'test:legacy-migrate',
                  NULL
                )
                """,
                {
                    "scope": scope,
                    "config_fingerprint": config_set.config_fingerprint,
                },
            )
        with conn.cursor() as cur:
            # make-migrate-idempotent (design ruling 1): the checksum fast path
            # skips the schema body on an unchanged upgrade. The activation
            # backfill DO-block lives in the body and runs on a real schema
            # change (checksum mismatch); this test corrupts columns out-of-band
            # (invisible to the checksum), so force a body replay to exercise it.
            cur.execute(
                """
                UPDATE okg.schema_migrations
                   SET result_metadata =
                       jsonb_set(
                         result_metadata,
                         '{executable_schema_checksum}',
                         '"force-backfill-replay"'
                       )
                 WHERE migration_id = '0000_schema_snapshot'
                """
            )
        conn.commit()

    migrate(clean_runtime)

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name, is_nullable
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name IN (
                 'nomos_policy_activations',
                 'nomos_config_activations'
               )
               AND column_name = 'activation_decision_id'
             ORDER BY table_name
            """
        )
        nullability = cur.fetchall()
        cur.execute(
            """
            SELECT 'policy' AS activation_kind, d.effect, d.reason_code
              FROM okg.nomos_policy_activations a
              JOIN okg.nomos_decisions d
                ON d.deployment_name = a.deployment_name
               AND d.decision_id = a.activation_decision_id
             WHERE a.activation_id = 'nomos:policy_activation:legacy-null'
            UNION ALL
            SELECT 'config' AS activation_kind, d.effect, d.reason_code
              FROM okg.nomos_config_activations a
              JOIN okg.nomos_decisions d
                ON d.deployment_name = a.deployment_name
               AND d.decision_id = a.activation_decision_id
             WHERE a.activation_id = 'nomos:config_activation:legacy-null'
             ORDER BY activation_kind
            """
        )
        decisions = cur.fetchall()

    assert nullability == [
        ("nomos_config_activations", "NO"),
        ("nomos_policy_activations", "NO"),
    ]
    assert decisions == [
        ("config", "allow", "nomos.migration.activation_decision_backfill"),
        ("policy", "allow", "nomos.migration.activation_decision_backfill"),
    ]


def test_postgres_read_and_export_surfaces_are_nomos_enforced(
    clean_runtime: str,
) -> None:
    scope = "deployment:nomos-read"
    bundle = compile_policy_graph(_read_policy_graph())
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="nomos-read",
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
        )
        # The chronos-native cutover dropped okg.nodes_live;
        # read_nodes_with_active_policy reads okg.graph_nodes through a
        # checkpoint-pinned Chronos checkout. Seed the nodes via the
        # canonical apply_facts path and checkpoint the branch head as the
        # generation we read at.
        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "node:public", "document",
                    {"classification": "public", "text": "hello"},
                    {"id": "node:public"}, {"v": 1},
                ),
                NodeFact(
                    "node:restricted", "document",
                    {
                        "classification": "restricted",
                        "text": "secret",
                        "private_body": "hidden",
                        "title": "visible",
                    },
                    {"id": "node:restricted"}, {"v": 1},
                ),
            ],
            source="nomos-read",
            strict_endpoints=True,
        )
        gen = store.checkpoint_generation(0, deployment_name="nomos-read")
        public_rows = read_nodes_with_active_policy(
            conn,
            deployment_name="nomos-read",
            scope=scope,
            subject="subject:public-reader",
            generation_id=gen,
        )
        audit_rows = read_nodes_with_active_policy(
            conn,
            deployment_name="nomos-read",
            scope=scope,
            subject="subject:audit-reader",
            generation_id=gen,
            audit=True,
        )
        export_without_evidence = authorize_graph_export_with_active_policy(
            conn,
            deployment_name="nomos-read",
            scope=scope,
            subject="subject:public-reader",
            graph_id="graph:restricted",
            classification="restricted",
        )
        export_with_evidence = authorize_graph_export_with_active_policy(
            conn,
            deployment_name="nomos-read",
            scope=scope,
            subject="subject:public-reader",
            graph_id="graph:restricted",
            classification="restricted",
            redaction_evidence=True,
        )
        conn.commit()

    public_by_id = {row["node_id"]: row for row in public_rows}
    audit_by_id = {row["node_id"]: row for row in audit_rows}
    assert public_by_id["node:public"]["attrs"]["text"] == "hello"
    restricted_public = public_by_id["node:restricted"]["attrs"]
    assert restricted_public["title"] == "visible"
    assert restricted_public["nomos_redacted"] is True
    assert "text" not in restricted_public
    assert "private_body" not in restricted_public
    assert audit_by_id["node:restricted"]["attrs"]["private_body"] == "hidden"
    assert export_without_evidence.effect == "deny"
    assert export_with_evidence.effect == "redact"


def test_published_generation_records_active_nomos_policy_pin(
    clean_runtime: str,
) -> None:
    scope = "deployment:default"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:generation-pin",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_fixture_source",
                "effect": "allow",
                "actions": ["source.emit"],
                "subjects": ["source:fixture"],
            }
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bundle,
            scope=scope,
            activated_by="test:generation-pin",
        )
        _insert_node_fact(conn, node_id="n:nomos-pin", subtype="page")
        conn.commit()

    published = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    assert published["status"] == "published"
    # The chronos-native publish records the active Nomos policy pin in the generation
    # CHECKPOINT metadata (the generation boundary now); generation_id is a STRING
    # checkpoint id, not a bigint okg.graph_generations row.
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        meta = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).get_generation(published["generation_id"]).metadata
    if isinstance(meta, str):
        meta = json.loads(meta)
    bundle_ids = meta.get("nomos_policy_bundle_ids") or []
    fingerprints = meta.get("nomos_policy_fingerprints") or []

    assert bundle.policy_bundle_id in bundle_ids
    assert bundle.policy_fingerprint in fingerprints


def test_source_run_authorization_uses_active_policy_and_persists_decision(
    clean_runtime: str,
) -> None:
    scope = "deployment:nomos-source"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:source-admission",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_fixture_source",
                "effect": "allow",
                "actions": ["source.emit"],
                "subjects": ["source:fixture"],
                "match": {
                    "deployment": "nomos-source",
                    "scope": scope,
                    "source": "fixture",
                },
            }
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        assert authorize_source_run_with_active_policy(
            conn,
            deployment_name="nomos-source",
            scope=scope,
            producer_id="source:fixture",
            source="fixture",
            run_id="run-1",
            fact_ref_count=1,
            admission_mode="fast_track",
        ) is None
        activate_policy_bundle(
            conn,
            deployment_name="nomos-source",
            bundle=bundle,
            scope=scope,
            activated_by="test:source",
        )
        allowed = authorize_source_run_with_active_policy(
            conn,
            deployment_name="nomos-source",
            scope=scope,
            producer_id="source:fixture",
            source="fixture",
            run_id="run-2",
            fact_ref_count=1,
            admission_mode="fast_track",
        )
        raw_allowed = authorize_source_run_with_active_policy(
            conn,
            deployment_name="nomos-source",
            scope=scope,
            producer_id="fixture",
            source="fixture",
            run_id="run-raw",
            fact_ref_count=1,
            admission_mode="fast_track",
        )
        denied = authorize_source_run_with_active_policy(
            conn,
            deployment_name="nomos-source",
            scope=scope,
            producer_id="source:blocked",
            source="blocked",
            run_id="run-3",
            fact_ref_count=1,
            admission_mode="fast_track",
        )
        conn.commit()

    assert allowed is not None
    assert allowed.effect == "allow"
    assert raw_allowed is not None
    assert raw_allowed.effect == "allow"
    assert denied is not None
    assert denied.effect == "deny"
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'nomos-source'
               AND target->>'kind' = 'source_run'
             ORDER BY created_at, decision_row_id
            """
        )
        rows = cur.fetchall()
    assert rows == [
        ("allow", "source_run"),
        ("allow", "source_run"),
        ("deny", "source_run"),
    ]


def test_runtime_boundary_active_policy_bridge_persists_decision(
    clean_runtime: str,
) -> None:
    scope = "deployment:nomos-runtime-bridge"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:runtime-bridge",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cli_search",
                "effect": "allow",
                "actions": ["graph.read.cli_search"],
                "subjects": ["subject:operator"],
            }
        ],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="nomos-runtime-bridge",
            bundle=bundle,
            scope=scope,
            activated_by="test:operator",
        )
        allowed = evaluate_runtime_boundary_with_active_policy(
            conn,
            deployment_name="nomos-runtime-bridge",
            scope=scope,
            request=NomosRuntimeRequest(
                surface="graph.read.cli_search",
                subject="subject:operator",
                resource={
                    "query": "policy",
                    "params": {"token": "raw-token"},
                },
            ),
            target={
                "kind": "cli_search",
                "query": "policy",
                "api_key": "sk-target-secret",
            },
            performed_obligations=("audit",),
        )
        conn.commit()

    assert allowed is not None
    assert allowed.allowed is True
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT action, effect, resource::text, target::text
              FROM okg.nomos_decisions
             WHERE deployment_name = 'nomos-runtime-bridge'
               AND target->>'kind' = 'cli_search'
            """
        )
        rows = cur.fetchall()

    assert len(rows) == 1
    action, effect, resource_text, target_text = rows[0]
    assert action == "graph.read.cli_search"
    assert effect == "allow"
    assert "raw-token" not in resource_text
    assert "sk-target-secret" not in target_text
    assert '"redacted"' in resource_text
    assert '"redacted"' in target_text


def test_policy_activation_nomos_denial_blocks_before_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:blocked-activation",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_unmatched",
                "effect": "allow",
                "actions": ["policy.activate"],
                "subjects": ["subject:someone-else"],
            }
        ],
    })
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(NomosPolicyError, match="Nomos denied policy.activate"):
            activate_policy_bundle(
                conn,
                deployment_name="default",
                bundle=bundle,
                scope="deployment:default",
                activated_by="subject:admin",
            )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.nomos_policy_activations
                 WHERE deployment_name = 'default'
                   AND policy_fingerprint = %s
                """,
                (bundle.policy_fingerprint,),
            )
            (activation_count,) = cur.fetchone()
            cur.execute(
                """
                SELECT action, effect, target->>'kind'
                  FROM okg.nomos_decisions
                 WHERE deployment_name = 'default'
                 ORDER BY created_at, decision_row_id
                """
            )
            decisions = cur.fetchall()

    assert activation_count == 0
    assert decisions == [("policy.activate", "deny", "policy_activation")]


def test_policy_activation_explicit_bootstrap_is_first_policy_only(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = "deployment:default"
    bootstrap_policy = _activation_admin_policy("bootstrap-first")
    second_policy = _activation_admin_policy("bootstrap-second")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")

    with psycopg.connect(clean_runtime) as conn:
        activation_id = activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=bootstrap_policy,
            scope=scope,
            activated_by="subject:bootstrap-admin",
            bootstrap=True,
        )
        with pytest.raises(NomosPolicyError, match="active policy already exists"):
            activate_policy_bundle(
                conn,
                deployment_name="default",
                bundle=second_policy,
                scope=scope,
                activated_by="subject:bootstrap-admin",
                bootstrap=True,
            )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT activation_decision_id
              FROM okg.nomos_policy_activations
             WHERE activation_id = %s
            """,
            (activation_id,),
        )
        (activation_decision_id,) = cur.fetchone()
        cur.execute(
            """
            SELECT decision_id, action, effect, reason_code,
                   target->>'kind', target->>'bootstrap'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
             ORDER BY created_at, decision_row_id
            """
        )
        decisions = cur.fetchall()

    assert decisions == [(
        activation_decision_id,
        "policy.activate",
        "allow",
        "nomos.bootstrap.first_policy_activation",
        "policy_activation_bootstrap",
        "true",
    )]


def test_policy_and_config_activation_use_active_nomos_policy(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scope = "deployment:default"
    admin_policy = _activation_admin_policy("bootstrap")
    next_policy = _activation_admin_policy("next")
    config_set = compile_config_graph({
        "config_set_id": "nomos:test:activation-config",
        "config_version": "v1",
        "scope": {"deployment": "default"},
        "entries": [
            {
                "config_key": "runtime.batch_size",
                "value": 50,
                "classification": "internal",
            }
        ],
    })

    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=admin_policy,
            scope=scope,
            activated_by="test:bootstrap",
            bootstrap=True,
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn:
        policy_activation = activate_policy_bundle(
            conn,
            deployment_name="default",
            bundle=next_policy,
            scope=scope,
            activated_by="subject:admin",
        )
        config_activation = activate_config_set(
            conn,
            deployment_name="default",
            config_set=config_set,
            scope=scope,
            activated_by="subject:admin",
        )
        conn.commit()

    assert policy_activation.startswith("nomos:policy_activation:")
    assert config_activation.startswith("nomos:config_activation:")
    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT decision_id, action, effect, target->>'kind'
              FROM okg.nomos_decisions
             WHERE deployment_name = 'default'
               AND target->>'kind' IN ('policy_activation', 'config_activation')
             ORDER BY created_at, decision_row_id
            """
        )
        decisions = cur.fetchall()
        cur.execute(
            """
            SELECT activation_decision_id
              FROM okg.nomos_policy_activations
             WHERE activation_id = %s
            """,
            (policy_activation,),
        )
        (policy_decision_id,) = cur.fetchone()
        cur.execute(
            """
            SELECT activation_decision_id
              FROM okg.nomos_config_activations
             WHERE activation_id = %s
            """,
            (config_activation,),
        )
        (config_decision_id,) = cur.fetchone()

    assert [row[1:] for row in decisions] == [
        ("policy.activate", "allow", "policy_activation"),
        ("config.activate", "allow", "config_activation"),
    ]
    assert policy_decision_id == decisions[0][0]
    assert config_decision_id == decisions[1][0]


def test_persisted_nomos_decision_target_is_sanitized(
    clean_runtime: str,
) -> None:
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:target-sanitize",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_secret_use",
                "effect": "allow",
                "actions": ["secret.use"],
                "subjects": ["subject:operator"],
            },
        ],
    })
    decision = evaluate(
        bundle,
        DecisionInput(
            subject="subject:operator",
            action="secret.use",
            resource={"deployment": "target-sanitize"},
        ),
    )

    with psycopg.connect(clean_runtime) as conn:
        persist_nomos_decision(
            conn,
            deployment_name="target-sanitize",
            decision=decision,
            target={
                "kind": "secret_use",
                "api_key": "sk-target-secret",
                "params": {"token": "raw-target-token"},
            },
        )
        conn.commit()

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT target::text
              FROM okg.nomos_decisions
             WHERE deployment_name = 'target-sanitize'
            """
        )
        target_text = cur.fetchone()[0]

    assert "sk-target-secret" not in target_text
    assert "raw-target-token" not in target_text
    assert '"redacted"' in target_text


def test_compatibility_seeds_from_producer_profiles_and_bootstrap_config(
    clean_runtime: str,
) -> None:
    scope = "deployment:compat"
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.producer_profiles (
                  deployment_name, producer_id, producer_kind,
                  trust_label, admission_mode, profile_fingerprint,
                  status
                )
                VALUES (
                  'compat', 'source:fixture', 'source',
                  'compat-trusted', 'fast_track',
                  'sha256:producer-fixture', 'compat'
                )
                """
            )
        bundle = seed_compatibility_policy_bundle(
            conn,
            deployment_name="compat",
            scope=scope,
            activated_by="test:compat",
        )
        config = seed_compatibility_config_set(
            conn,
            deployment_name="compat",
            scope=scope,
            settings={
                "runtime.max_batch": 500,
                "runtime.api_token": {
                    "secret_ref": "secret://compat/api-token",
                    "classification": "restricted",
                },
            },
            activated_by="test:compat",
        )
        decision = authorize_source_run_with_active_policy(
            conn,
            deployment_name="compat",
            scope=scope,
            producer_id="source:fixture",
            source="fixture",
            run_id="run-compat",
            fact_ref_count=1,
            admission_mode="fast_track",
        )
        token_entry = resolve_active_config_entry(
            conn,
            deployment_name="compat",
            scope=scope,
            config_key="runtime.api_token",
        )
        conn.commit()

    assert bundle.policy_bundle_id == "nomos:compat:policy:compat"
    assert config.config_set_id == "nomos:compat:config:compat"
    assert decision is not None
    assert decision.effect == "allow"
    assert token_entry.secret_ref == "secret://compat/api-token"


def _policy_graph() -> dict:
    return {
        "policy_bundle_id": "nomos:test:active",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_fixture_source",
                "effect": "allow",
                "priority": 100,
                "subjects": ["source:fixture"],
                "actions": ["source.emit"],
                "match": {
                    "deployment": "nomos-active",
                    "scope": "deployment:nomos-active",
                },
            }
        ],
    }


def _activation_admin_policy(suffix: str):
    return compile_policy_graph({
        "policy_bundle_id": f"nomos:test:activation-admin:{suffix}",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_policy_activation",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:admin"],
                "actions": ["policy.activate"],
                "obligations": [{"type": "audit"}],
            },
            {
                "rule_id": "allow_config_activation",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:admin"],
                "actions": ["config.activate"],
                "obligations": [{"type": "audit"}],
            },
        ],
    })


def _read_policy_graph() -> dict:
    return {
        "policy_bundle_id": "nomos:test:read",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_audit_read",
                "effect": "allow",
                "priority": 300,
                "subjects": ["subject:audit-reader"],
                "actions": ["graph.read.audit"],
            },
            {
                "rule_id": "redact_public_restricted_read",
                "effect": "redact",
                "priority": 200,
                "subjects": ["subject:public-reader"],
                "actions": ["graph.read"],
                "match": {"classification": "restricted"},
                "obligations": [{"type": "redaction"}],
            },
            {
                "rule_id": "allow_public_read",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:public-reader"],
                "actions": ["graph.read"],
                "match": {"classification": "public"},
            },
            {
                "rule_id": "redact_restricted_export_with_evidence",
                "effect": "redact",
                "priority": 100,
                "subjects": ["subject:public-reader"],
                "actions": ["graph.export"],
                "match": {
                    "classification": "restricted",
                    "redaction_evidence": True,
                },
                "obligations": [{"type": "redaction"}],
            },
        ],
    }


def _config_graph() -> dict:
    return {
        "config_set_id": "nomos:test:active-config",
        "config_version": "v1",
        "scope": {"deployment": "nomos-active"},
        "entries": [
            {
                "config_key": "runtime.max_batch",
                "value": 2000,
                "classification": "internal",
            },
            {
                "config_key": "runtime.openai_token",
                "secret_ref": "secret://okg/openai-token",
                "classification": "restricted",
                "secret": True,
            },
        ],
    }


def _insert_node_fact(
    conn: psycopg.Connection,
    *,
    node_id: str,
    subtype: str,
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, %s::jsonb, 'fixture', %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                node_id,
                subtype,
                json.dumps({"text": "nomos generation pin"}),
                json.dumps({"id": node_id}),
                json.dumps({"v": 1}),
                f"{run_id}:{node_id}:{uuid.uuid4().hex[:8]}",
                run_id,
            ),
        )
