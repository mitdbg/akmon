from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest

from okg.substrate.cli import main as cli
from okg.substrate.cli.main import _build_parser
from okg.substrate.nomos import (
    OIDCLoginConfig,
    OIDCLoginResult,
    PermissionGrant,
    SubjectBinding,
    activate_config_set,
    activate_policy_bundle,
    compile_config_graph,
    compile_policy_graph,
    evaluate_identity_claim_from_postgres,
    institutional_provider_fixture,
    upsert_identity_provider,
    upsert_permission_grant,
    upsert_subject_binding,
)


def test_nomos_demo_argparse() -> None:
    args = _build_parser().parse_args(
        ["nomos", "demo", "--out", "/tmp/nomos-demo", "--overwrite"]
    )

    assert args.command == "nomos"
    assert args.nomos_subcmd == "demo"
    assert args.out == "/tmp/nomos-demo"
    assert args.overwrite is True


def test_nomos_provider_fixture_and_validate_cli(
    tmp_path: Path,
    capsys,
) -> None:
    profiles = [
        ("cern-oidc", "akmon-cms", "oidc", ["mfa"]),
        ("cern-saml", "akmon-cms", "saml", ["mfa"]),
        ("mit-touchstone", "akmon-mit-lab", "touchstone_oidc", ["duo_mfa"]),
        (
            "mit-kerberos-gssapi",
            "HTTP/okg-gateway.example.org@ATHENA.MIT.EDU",
            "kerberos_gssapi",
            ["kerberos_gssapi"],
        ),
    ]
    for profile, audience, provider_type, required_assurance in profiles:
        provider_path = tmp_path / f"{profile}-provider.json"

        rc = cli.main([
            "nomos",
            "provider",
            "fixture",
            "--provider",
            profile,
            "--audience",
            audience,
            "--out",
            str(provider_path),
            "--json",
        ])
        assert rc == 0
        fixture_payload = json.loads(capsys.readouterr().out)
        assert fixture_payload["status"] == "ok"
        assert provider_path.is_file()

        provider = json.loads(provider_path.read_text(encoding="utf-8"))
        assert provider["provider_type"] == provider_type
        assert provider["key_reference"].startswith("secret://")
        assert "opaque" not in json.dumps(provider).lower()

        rc = cli.main([
            "nomos",
            "provider",
            "validate",
            "--provider",
            str(provider_path),
            "--json",
        ])
        assert rc == 0
        validate_payload = json.loads(capsys.readouterr().out)
        assert validate_payload["provider_type"] == provider_type
        assert validate_payload["required_assurance"] == required_assurance


def test_nomos_provider_oidc_login_writes_bounded_claim(
    tmp_path: Path,
    capsys,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    out = tmp_path / "cern-claim.json"

    def fake_login(
        config: OIDCLoginConfig,
        *,
        status_callback=None,
    ) -> OIDCLoginResult:
        assert config.issuer == "https://auth.cern.ch/auth/realms/cern"
        assert config.client_id == "akmon-cms"
        assert config.provider_id == "cern-sso"
        assert config.provider_type == "oidc"
        assert config.verified_by == "verifier:cern-sso-gateway"
        assert config.scopes == ("openid", "profile", "email")
        assert config.open_browser is False
        assert config.redirect_port == 8765
        if status_callback:
            status_callback("fake callback waiting")
        return OIDCLoginResult(
            claim={
                "provider_id": "cern-sso",
                "verification_status": "valid",
                "provider_type": "oidc",
                "issuer": "https://auth.cern.ch/auth/realms/cern",
                "external_subject": "cern-user-1",
                "audiences": ["akmon-cms"],
                "verified_by": "verifier:cern-sso-gateway",
                "verification_method": "oidc-jwks",
                "verified_at": "2026-05-25T12:00:00Z",
                "claim_fingerprint": "sha256:test",
                "claims": {
                    "sub": "cern-user-1",
                    "aud": "akmon-cms",
                    "cern_roles": ["cms-operator"],
                },
            },
            issuer="https://auth.cern.ch/auth/realms/cern",
            provider_id="cern-sso",
            provider_type="oidc",
            external_subject="cern-user-1",
            audiences=("akmon-cms",),
            included_claim_keys=("aud", "cern_roles", "sub"),
            redirect_uri="http://127.0.0.1:8765/nomos/oidc/callback",
        )

    monkeypatch.setattr(
        "okg.substrate.cli.nomos.run_oidc_loopback_login",
        fake_login,
    )

    rc = cli.main([
        "nomos",
        "provider",
        "oidc-login",
        "--issuer",
        "https://auth.cern.ch/auth/realms/cern",
        "--client-id",
        "akmon-cms",
        "--provider-id",
        "cern-sso",
        "--verified-by",
        "verifier:cern-sso-gateway",
        "--no-browser",
        "--out",
        str(out),
        "--json",
    ])

    assert rc == 0
    stderr = capsys.readouterr()
    payload = json.loads(stderr.out)
    assert payload["status"] == "ok"
    assert payload["claim"] == str(out.resolve())
    assert payload["external_subject"] == "cern-user-1"
    assert "fake callback waiting" in stderr.err
    claim = json.loads(out.read_text(encoding="utf-8"))
    encoded = json.dumps({"stdout": payload, "claim": claim}, sort_keys=True)
    assert claim["verified_by"] == "verifier:cern-sso-gateway"
    assert claim["claims"]["cern_roles"] == ["cms-operator"]
    assert "id_token" not in encoded
    assert "access_token" not in encoded
    assert "refresh_token" not in encoded


def test_nomos_demo_writes_artifacts(tmp_path: Path, capsys) -> None:
    rc = cli.main(["nomos", "demo", "--out", str(tmp_path), "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["decision_counts"] == {
        "allow": 2,
        "deny": 1,
        "quarantine": 1,
        "redact": 1,
        "review": 1,
    }
    artifacts = payload["artifacts"]
    for path in artifacts.values():
        assert Path(path).is_file()

    state = json.loads(Path(artifacts["refinement_state"]).read_text())
    assert state["model"] == "formal/nomos/Nomos.tla"
    assert state["unsupported"] == []


def test_nomos_compile_evaluate_inspect_decision_trace(
    tmp_path: Path,
    capsys,
) -> None:
    assert cli.main(["nomos", "demo", "--out", str(tmp_path), "--json"]) == 0
    demo_payload = json.loads(capsys.readouterr().out)
    policy = demo_payload["artifacts"]["policy_graph"]
    bundle = tmp_path / "bundle.json"

    rc = cli.main([
        "nomos",
        "compile",
        "--policy",
        policy,
        "--out",
        str(bundle),
        "--json",
    ])
    assert rc == 0
    compiled_payload = json.loads(capsys.readouterr().out)
    assert compiled_payload["status"] == "ok"
    assert bundle.is_file()

    decision_input = tmp_path / "input.json"
    decision_input.write_text(
        json.dumps(
            {
                "subject": "okg:lab-a",
                "action": "poros.graph.submit",
                "classification": "public",
                "resource": {
                    "root_pattern": "context",
                    "graph_profile": "context",
                    "node_subtypes": ["context_packet"],
                    "root_subtypes": ["context_packet"],
                    "requested_incorporation_mode": "fast_track",
                },
            }
        ),
        encoding="utf-8",
    )
    decision_path = tmp_path / "decision.json"
    rc = cli.main([
        "nomos",
        "evaluate",
        "--bundle",
        str(bundle),
        "--input",
        str(decision_input),
        "--out",
        str(decision_path),
        "--json",
    ])
    assert rc == 0
    decision_payload = json.loads(capsys.readouterr().out)
    assert decision_payload["effect"] == "allow"
    assert decision_path.is_file()

    rc = cli.main([
        "nomos",
        "inspect",
        "--bundle",
        str(bundle),
        "--json",
    ])
    assert rc == 0
    inspect_payload = json.loads(capsys.readouterr().out)
    assert inspect_payload["rule_count"] == 5

    rc = cli.main([
        "nomos",
        "decision",
        "--decisions",
        demo_payload["artifacts"]["decisions"],
        "--decision-id",
        demo_payload["visible_decisions"][0],
        "--json",
    ])
    assert rc == 0
    lookup_payload = json.loads(capsys.readouterr().out)
    assert lookup_payload["effect"] == "allow"

    trace_path = tmp_path / "trace.json"
    rc = cli.main([
        "nomos",
        "trace",
        "--decisions",
        demo_payload["artifacts"]["decisions"],
        "--bundle",
        str(bundle),
        "--generation-id",
        "demo-generation",
        "--out",
        str(trace_path),
        "--json",
    ])
    assert rc == 0
    trace_payload = json.loads(capsys.readouterr().out)
    assert trace_payload["status"] == "ok"
    assert trace_path.is_file()
    trace_state = json.loads(trace_path.read_text(encoding="utf-8"))
    assert "demo-generation" in trace_state["generationPolicyPins"]


@pytest.mark.postgres
def test_nomos_inspect_can_report_active_postgres_state(
    clean_runtime: str,
    capsys,
) -> None:
    scope = "deployment:nomos-cli"
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:cli",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_cli_inspect",
                "effect": "allow",
                "actions": ["nomos.inspect"],
                "subjects": ["subject:cli"],
            }
        ],
    })
    config = compile_config_graph({
        "config_set_id": "nomos:test:cli-config",
        "config_version": "v1",
        "entries": [{"config_key": "runtime.max_batch", "value": 100}],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name="nomos-cli",
            bundle=bundle,
            scope=scope,
            activated_by="test:cli",
        )
        activate_config_set(
            conn,
            deployment_name="nomos-cli",
            config_set=config,
            scope=scope,
            activated_by="test:cli",
        )
        conn.commit()

    rc = cli.main([
        "nomos",
        "inspect",
        "--dsn",
        clean_runtime,
        "--deployment",
        "nomos-cli",
        "--scope",
        scope,
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["active_policy"]["policy_fingerprint"] == (
        bundle.policy_fingerprint
    )
    assert payload["active_config"]["config_fingerprint"] == (
        config.config_fingerprint
    )
    assert payload["subject_binding_count"] == 0


@pytest.mark.postgres
def test_nomos_provider_inspect_and_drift_cli_reports_redacted_state(
    clean_runtime: str,
    capsys,
) -> None:
    deployment = "nomos-cli-provider"
    provider = replace(
        institutional_provider_fixture(
            "cern-oidc",
            audience="akmon-cms",
            provider_id="cern-sso",
            metadata_fingerprint="sha256:actual",
        ),
        metadata={
            "expected_issuer": "https://auth.cern.ch/auth/realms/cern",
            "expected_metadata_fingerprint": "sha256:expected",
        },
    )
    with psycopg.connect(clean_runtime) as conn:
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        conn.commit()

    rc = cli.main([
        "nomos",
        "provider",
        "inspect",
        "--dsn",
        clean_runtime,
        "--deployment",
        deployment,
        "--provider-id",
        "cern-sso",
        "--json",
    ])
    assert rc == 0
    inspect_payload = json.loads(capsys.readouterr().out)
    assert inspect_payload["provider_count"] == 1
    inspected_provider = inspect_payload["providers"][0]
    assert inspected_provider["provider_id"] == "cern-sso"
    assert inspected_provider["key_reference"].startswith("secret://")
    assert "private_key" not in json.dumps(inspect_payload).lower()

    rc = cli.main([
        "nomos",
        "provider",
        "drift",
        "--dsn",
        clean_runtime,
        "--deployment",
        deployment,
        "--json",
    ])
    assert rc == 1
    drift_payload = json.loads(capsys.readouterr().out)
    assert drift_payload["provider_error_count"] == 1
    assert drift_payload["provider_errors"][0]["field"] == "metadata_fingerprint"

    rc = cli.main([
        "nomos",
        "provider",
        "readiness",
        "--dsn",
        clean_runtime,
        "--deployment",
        deployment,
        "--json",
    ])
    assert rc == 1
    readiness_payload = json.loads(capsys.readouterr().out)
    assert readiness_payload["active_provider_count"] == 1
    assert readiness_payload["binding_count"] == 0
    assert readiness_payload["grant_count"] == 0
    assert readiness_payload["active_policy_count"] == 0
    assert readiness_payload["active_config_count"] == 0

    rc = cli.main([
        "nomos",
        "provider",
        "readiness",
        "--dsn",
        clean_runtime,
        "--deployment",
        deployment,
        "--production",
        "--json",
    ])
    assert rc == 1
    production_payload = json.loads(capsys.readouterr().out)
    production_codes = {
        error["code"] for error in production_payload["production_errors"]
    }
    assert "nomos_identity_provider_not_production" in production_codes


@pytest.mark.postgres
def test_nomos_provider_production_readiness_passes_registered_provider(
    clean_runtime: str,
    capsys,
) -> None:
    deployment = "nomos-cli-production-provider"
    scope = f"deployment:{deployment}"
    provider = replace(
        institutional_provider_fixture(
            "cern-oidc",
            audience="akmon-cms",
            provider_id="cern-sso",
            key_reference="secret://cern-sso/prod-jwks",
            metadata_fingerprint="sha256:prod-cern-metadata",
        ),
        metadata={
            "production": True,
            "trusted_verifiers": ["verifier:cern-sso-gateway"],
            "expected_issuer": "https://auth.cern.ch/auth/realms/cern",
            "expected_audiences": ["akmon-cms"],
            "expected_key_reference": "secret://cern-sso/prod-jwks",
            "expected_required_assurance": ["mfa"],
            "expected_metadata_fingerprint": "sha256:prod-cern-metadata",
            "expected_freshness_seconds": 3600,
            "expected_status": "active",
        },
    )
    bundle = compile_policy_graph({
        "policy_bundle_id": "nomos:test:production-readiness",
        "policy_version": "v1",
        "default_effect": "deny",
        "rules": [
            {
                "rule_id": "allow_production_cern_submit",
                "effect": "allow",
                "priority": 100,
                "subjects": ["subject:cern:operator"],
                "actions": ["poros.graph.submit"],
                "match": {
                    "deployment": deployment,
                    "scope": scope,
                    "identity_provider_id": provider.provider_id,
                },
            }
        ],
    })
    config = compile_config_graph({
        "config_set_id": "nomos:test:production-readiness-config",
        "config_version": "v1",
        "entries": [{"config_key": "runtime.max_batch", "value": 100}],
    })
    with psycopg.connect(clean_runtime) as conn:
        activate_policy_bundle(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            scope=scope,
            activated_by="test:production-readiness",
        )
        activate_config_set(
            conn,
            deployment_name=deployment,
            config_set=config,
            scope=scope,
            activated_by="test:production-readiness",
        )
        upsert_identity_provider(conn, deployment_name=deployment, provider=provider)
        upsert_subject_binding(
            conn,
            deployment_name=deployment,
            binding=SubjectBinding(
                binding_id="binding:cern:operator",
                provider_id=provider.provider_id,
                issuer=provider.issuer,
                claim_path="roles",
                claim_value="cms-operator",
                subject="subject:cern:operator",
                scope=scope,
                required_assurance=("mfa",),
                priority=10,
            ),
        )
        upsert_permission_grant(
            conn,
            deployment_name=deployment,
            grant=PermissionGrant(
                grant_id="grant:cern:operator-submit",
                subject="subject:cern:operator",
                actions=("poros.graph.submit",),
                scope=scope,
                classifications=("internal",),
                priority=10,
            ),
        )
        result = evaluate_identity_claim_from_postgres(
            conn,
            deployment_name=deployment,
            bundle=bundle,
            claim={
                "provider_id": provider.provider_id,
                "verification_status": "valid",
                "issuer": provider.issuer,
                "verified_by": "verifier:cern-sso-gateway",
                "verification_method": "oidc-jwks",
                "verified_at": "2026-05-25T11:59:30Z",
                "claims": {
                    "sub": "cern-user-123",
                    "aud": "akmon-cms",
                    "cern_roles": ["cms-operator"],
                    "acr": "mfa",
                    "exp": "2026-05-25T13:00:00Z",
                    "auth_time": "2026-05-25T11:59:00Z",
                },
            },
            action="poros.graph.submit",
            resource={"deployment": deployment, "scope": scope},
            scope=scope,
            now="2026-05-25T12:00:00Z",
        )
        conn.commit()

    assert result.decision.effect == "allow"
    rc = cli.main([
        "nomos",
        "provider",
        "readiness",
        "--dsn",
        clean_runtime,
        "--deployment",
        deployment,
        "--production",
        "--json",
    ])
    assert rc == 0
    readiness_payload = json.loads(capsys.readouterr().out)
    assert readiness_payload["production"] is True
    assert readiness_payload["active_provider_count"] == 1
    assert readiness_payload["binding_count"] == 1
    assert readiness_payload["grant_count"] == 1
    assert readiness_payload["active_policy_count"] == 1
    assert readiness_payload["active_config_count"] == 1
    assert readiness_payload["recent_claim_count"] == 1
    # `recent_decision_count` counts every nomos_decision for the deployment in
    # the last 24h. Beyond the identity-claim `poros.graph.submit` evaluation,
    # governance now persists an activation-audit decision per activation, so
    # this setup yields three: policy.activate + config.activate + the submit.
    assert readiness_payload["recent_decision_count"] == 3
    assert readiness_payload["production_errors"] == []


def test_nomos_decisions_prune_argparse() -> None:
    # Destructive-verb guard (#694): DELETE requires --apply; dry-run is the
    # default (no --dry-run flag).
    args = _build_parser().parse_args([
        "nomos", "decisions-prune",
        "--deployment", "default",
        "--days", "30",
        "--apply",
        "--json",
    ])

    assert args.command == "nomos"
    assert args.nomos_subcmd == "decisions-prune"
    assert args.deployment == "default"
    assert args.days == 30
    assert args.apply is True

    # Default (no flag) is a dry-run.
    default_args = _build_parser().parse_args([
        "nomos", "decisions-prune", "--deployment", "default",
    ])
    assert default_args.apply is False


@pytest.mark.postgres
def test_nomos_decisions_prune_cli_prunes_aged_unreferenced_rows(
    clean_runtime: str,
    capsys,
) -> None:
    """The CLI verb prunes aged unreferenced decisions (dry-run first) and
    leaves recent rows untouched."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            for decision_id, age_days in (
                ("nomos:decision:cli-prune-old", 400),
                ("nomos:decision:cli-prune-recent", 1),
            ):
                cur.execute(
                    """
                    INSERT INTO okg.nomos_decisions (
                      deployment_name, decision_id, input_hash, subject,
                      action, resource, classification, context, effect,
                      reason_code, matched_rule_ids, obligations,
                      enforcement_point, target, created_at
                    )
                    VALUES (
                      'default', %(decision_id)s, %(decision_id)s,
                      'subject:cli-prune', 'graph.read.mcp', '{}'::jsonb,
                      'internal', '{}'::jsonb, 'allow', 'test',
                      ARRAY[]::text[], '[]'::jsonb,
                      'mcp.source_lineage_filter', '{}'::jsonb,
                      now() - make_interval(days => %(age_days)s)
                    )
                    """,
                    {"decision_id": decision_id, "age_days": age_days},
                )

    # Default invocation dry-runs (#694): reports the count, deletes nothing.
    rc = cli.main([
        "nomos", "decisions-prune",
        "--dsn", clean_runtime,
        "--deployment", "default",
        "--json",
    ])
    assert rc == 0
    dry_payload = json.loads(capsys.readouterr().out)
    assert dry_payload["status"] == "ok"
    assert dry_payload["dry_run"] is True
    assert dry_payload["deleted_rows"] == 1

    # --apply actually performs the DELETE.
    rc = cli.main([
        "nomos", "decisions-prune",
        "--dsn", clean_runtime,
        "--deployment", "default",
        "--apply",
        "--json",
    ])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is False
    assert payload["deleted_rows"] == 1

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT decision_id FROM okg.nomos_decisions "
                "WHERE subject = 'subject:cli-prune'"
            )
            surviving = [row[0] for row in cur.fetchall()]
    assert surviving == ["nomos:decision:cli-prune-recent"]
