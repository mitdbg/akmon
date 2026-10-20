"""Real PostgreSQL and authenticated HTTP proof for run registration facts."""

from __future__ import annotations

import hashlib
import json
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import yaml
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from starlette.testclient import TestClient
import pytest

from okg.substrate.external_actions.action_contracts import ActionContract
from okg.substrate.external_actions.api import ExternalActionAPI
from tests.substrate.external_actions.test_service import VECTORS
from okg.substrate.external_actions.service import ExternalActionService
from okg.substrate.external_actions.signing import b64url_encode, canonical_json_bytes
from okg.substrate.external_actions.store import PostgresExternalActionStore
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from tests.substrate.conftest import (
    _ensure_test_db_exists,
    _pause_timescale_jobs,
    dsn_with_dbname,
    require_okg_test_dsn,
)
from tests.substrate.external_actions.test_store_postgres import (
    _TEST_VERIFIER_CAPABILITY,
    _bind_verifier_role,
    _proposal_row,
    _set_verifier_capability,
)
from tests.substrate.mcp._result_payload import payload as _result_payload
from tests.substrate.mcp._two_principal_fixture import DEPLOYMENT, seed_all


pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield
ROOT = Path(__file__).resolve().parents[3]
ISSUER = "https://run-proof.example"
SUBJECT = "subject:run-proof"


@pytest.fixture(scope="module")
def run_database():
    """Own a UUID template0 database; no pre-existing database is dropped."""
    from okg.substrate.db import migrate as migrate_module
    from okg.substrate.db.pool import close_all

    # require_okg_test_dsn skips rather than returning None: a None here
    # produced the bare conninfo "dbname=postgres", which libpq resolves
    # against PGHOST or the local socket, and this fixture then CREATEs a
    # database on whatever it found (issue #1913).
    maintenance = dsn_with_dbname(require_okg_test_dsn(), "postgres")
    database_name = "okg_test_run_identity_" + uuid4().hex
    dsn = dsn_with_dbname(maintenance, database_name)
    oid = None
    created = False
    extensions = dict(migrate_module.REQUIRED_EXTENSIONS)
    try:
        with psycopg.connect(maintenance, autocommit=True, connect_timeout=10) as admin:
            admin.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                    sql.Identifier(database_name)
                )
            )
            created = True
            oid = admin.execute(
                "SELECT oid FROM pg_database WHERE datname = %s", (database_name,)
            ).fetchone()[0]
        # pg_cron can be installed in only one database on this cluster. This
        # fixture never schedules work; all other migration behavior is real.
        migrate_module.REQUIRED_EXTENSIONS.pop("pg_cron", None)
        _ensure_test_db_exists(dsn)
        migrate_module.migrate(dsn)
        generation = seed_all(dsn)
        yield dsn, generation
    finally:
        close_all()
        migrate_module.REQUIRED_EXTENSIONS.clear()
        migrate_module.REQUIRED_EXTENSIONS.update(extensions)
        if created and oid is not None:
            with psycopg.connect(maintenance, autocommit=True) as admin:
                current = admin.execute(
                    "SELECT oid FROM pg_database WHERE datname = %s", (database_name,)
                ).fetchone()
                if current != (oid,):
                    raise AssertionError(
                        "owned database identity changed; refusing cleanup"
                    )
                admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(database_name)
                    )
                )


class RunFixture:
    def __init__(self, dsn, generation):
        self.dsn, self.generation = dsn, generation
        self.values = {
            row["id"]: row for row in yaml.safe_load(VECTORS.read_text())["vectors"]
        }
        self.contract = ActionContract.from_mapping(
            self.values["action_contract"]["value"]
        )
        self.commands = []
        self.mutate_command = None
        self.role = "okg_run_" + uuid4().hex[:16]
        self.password = uuid4().hex
        self.runtime_dsn = make_conninfo(dsn, user=self.role, password=self.password)
        self.key = Ed25519PrivateKey.generate()
        self.provider = "provider-" + uuid4().hex
        self.binding = "binding-" + uuid4().hex
        self.digest = (
            "sha256:"
            + hashlib.sha256(self.key.public_key().public_bytes_raw()).hexdigest()
        )
        self.role_oid = None
        try:
            with psycopg.connect(dsn) as admin:
                admin.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} INHERIT").format(
                        sql.Identifier(self.role), sql.Literal(self.password)
                    )
                )
                self.role_oid = admin.execute(
                    "SELECT oid FROM pg_roles WHERE rolname=%s", (self.role,)
                ).fetchone()[0]
                admin.execute(
                    sql.SQL("GRANT okg_external_action_api TO {}").format(
                        sql.Identifier(self.role)
                    )
                )
                _bind_verifier_role(admin, DEPLOYMENT, session_user=self.role)
                admin.execute(
                    "INSERT INTO okg.nomos_identity_providers (deployment_name,provider_id,provider_type,issuer,subject_claim,audiences,status,metadata) VALUES (%s,%s,'oidc',%s,'sub',ARRAY['deployment_scoped_review_api'],'active',%s::jsonb)",
                    (
                        DEPLOYMENT,
                        self.provider,
                        ISSUER,
                        json.dumps({"credential_generation_digest": self.digest}),
                    ),
                )
                admin.execute(
                    "INSERT INTO okg.nomos_subject_bindings (deployment_name,binding_id,provider_id,issuer,claim_path,claim_value,subject,scope,status,metadata) VALUES (%s,%s,%s,%s,'sub',%s,%s,'external_action','active',%s::jsonb)",
                    (
                        DEPLOYMENT,
                        self.binding,
                        self.provider,
                        ISSUER,
                        self.binding,
                        SUBJECT,
                        json.dumps({"credential_generation_digest": self.digest}),
                    ),
                )

            self.policy()
        except BaseException:
            self.close()
            raise

    def policy(self, actions=None, subjects=None):
        actions = (
            actions
            if actions is not None
            else [
                "external_automation.register",
                "external_automation.read",
                "external_automation.link",
                "external_action.read",
                "external_action.propose",
                "external_action.decide",
            ]
        )
        bundle = compile_policy_graph(
            {
                "policy_bundle_id": "run-proof-"
                + hashlib.sha256(
                    canonical_json_bytes(
                        {
                            "actions": actions,
                            "subjects": subjects or [SUBJECT, "subject:other-service"],
                        }
                    )
                ).hexdigest()[:16],
                "policy_version": "1",
                "default_effect": "deny",
                "rules": [
                    {
                        "rule_id": "owned-service",
                        "effect": "allow",
                        "actions": actions,
                        "subjects": subjects or [SUBJECT, "subject:other-service"],
                        "match": {},
                        "priority": 100,
                    }
                ],
            }
        )
        with psycopg.connect(self.dsn) as admin:
            activate_policy_bundle(
                admin,
                deployment_name=DEPLOYMENT,
                bundle=bundle,
                scope="external_action",
                activated_by="test:operator",
            )

    def headers(self, method, uri):
        public = b64url_encode(self.key.public_key().public_bytes_raw())
        jwk = {"kty": "OKP", "crv": "Ed25519", "x": public}
        jkt = b64url_encode(hashlib.sha256(canonical_json_bytes(jwk)).digest())
        now = math.ceil(time.time())

        def signed(header, body):
            value = (
                b64url_encode(canonical_json_bytes(header))
                + "."
                + b64url_encode(canonical_json_bytes(body))
            )
            return value + "." + b64url_encode(self.key.sign(value.encode("ascii")))

        token = signed(
            {"alg": "EdDSA", "kid": "run-key", "typ": "JWT"},
            {
                "iss": ISSUER,
                "aud": "deployment_scoped_review_api",
                "sub": self.binding,
                "iat": now,
                "exp": now + 120,
                "cnf": {"jkt": jkt},
            },
        )
        proof = signed(
            {"alg": "EdDSA", "typ": "dpop+jwt", "jwk": jwk},
            {
                "htm": method,
                "htu": uri,
                "iat": now,
                "jti": uuid4().hex,
                "ath": b64url_encode(hashlib.sha256(token.encode()).digest()),
            },
        )
        return {"Authorization": "DPoP " + token, "DPoP": proof}

    @contextmanager
    def application(self):
        store = PostgresExternalActionStore(
            self.runtime_dsn,
            deployment=DEPLOYMENT,
            verifier_capability=_TEST_VERIFIER_CAPABILITY,
        )
        if not store.retained_leases(deployment=DEPLOYMENT):
            from okg.substrate.external_actions.config import load_config
            from okg.substrate.external_actions.restore_fence import (
                CHAIN_SCHEMA,
                pre_bind_restore_fence,
            )
            from okg.substrate.external_actions.signing import (
                build_detached_envelope,
                generate_keypair,
            )
            from okg.substrate.external_actions.server import (
                _startup_login_binding,
                _startup_verification_config,
            )

            private, public = generate_keypair()
            config = load_config(
                {
                    "schema": "okg.external_action_review.config/v1",
                    "deployment": DEPLOYMENT,
                    "authentication": {
                        "audience": "deployment_scoped_review_api",
                        "allowed_algorithms": ["EdDSA"],
                    },
                    "signing_keys": [
                        {
                            "kid": "fixture-operator",
                            "purpose": "execution_epoch_lease",
                            "algorithm": "EdDSA",
                            "public_key_b64": public,
                            "state": "current",
                        }
                    ],
                }
            )
            now = datetime.now(timezone.utc)
            envelope = build_detached_envelope(
                {
                    "deployment": DEPLOYMENT,
                    "epoch": 1,
                    "lease_sequence": 1,
                    "previous_lease_digest": None,
                    "previous_epoch_digest": None,
                    "issued_at": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "expires_at": (now + timedelta(hours=2)).strftime(
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ),
                    "signer_separation_subject": "subject:fixture-operator",
                    "nonce": uuid4().hex,
                },
                private,
                kid="fixture-operator",
                typ="okg-execution-epoch+jws",
                domain="okg-external-action-execution-epoch-lease-envelope-v1",
            )
            assert not any(store.counts().values()), store.counts()
            assert not store.restore_fences, dict(store.restore_fences)
            startup = pre_bind_restore_fence(
                {
                    "schema": CHAIN_SCHEMA,
                    "deployment": DEPLOYMENT,
                    "leases": [envelope],
                },
                config=config,
                store=store,
                deployment=DEPLOYMENT,
                verification_config=_startup_verification_config(config),
                login_binding=_startup_login_binding(store, deployment=DEPLOYMENT),
            )
            assert startup.inserted_count == 1 and startup.service_mode == "normal"
            store.connection.close()
            store = PostgresExternalActionStore(
                self.runtime_dsn,
                deployment=DEPLOYMENT,
                verifier_capability=_TEST_VERIFIER_CAPABILITY,
            )
        config = SimpleNamespace(
            deployment=DEPLOYMENT,
            service_mode="normal",
            authentication=SimpleNamespace(
                jwks={
                    "run-key": b64url_encode(self.key.public_key().public_bytes_raw())
                },
                issuer=ISSUER,
                audience="deployment_scoped_review_api",
                allowed_algorithms=("EdDSA",),
                max_token_lifetime_seconds=300,
                clock_skew_seconds=30,
            ),
            limits=SimpleNamespace(request_body_bytes_maximum=4096),
        )
        original_command = store.run_record_command

        def observed_command(operation, command):
            self.commands.append((operation, dict(command)))
            if self.mutate_command is not None:
                command = self.mutate_command(dict(command))
            return original_command(operation, command)

        store.run_record_command = observed_command
        service = ExternalActionService(
            store, action_contracts={self.contract.action_type: self.contract}
        )
        app = ExternalActionAPI(
            service,
            config=config,
            principal_mapping=store.resolve_principal,
            nonce_store=store,
            require_auth_audit=True,
            require_identity_barrier=True,
            auth_config_digest=self.digest,
        )
        try:
            yield app
        finally:
            store.connection.close()

    @contextmanager
    def client(self):
        with self.application() as app:
            client = TestClient(
                app, base_url=getattr(self, "origin", "https://run-api.example")
            )
            try:
                yield client
            finally:
                client.close()

    @contextmanager
    def server_process(self):
        import multiprocessing
        import socket
        import httpx

        with self.client():
            pass  # Complete real signed genesis before crossing the process boundary.
        state = dict(self.__dict__)
        state["key"] = self.key.private_bytes_raw()
        state["mutate_command"] = None
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        sock.listen(64)
        self.origin = "http://127.0.0.1:" + str(sock.getsockname()[1])
        state["origin"] = self.origin
        process = multiprocessing.get_context("spawn").Process(
            target=_serve_run_identity, args=(state, sock)
        )
        try:
            process.start()
            with httpx.Client(base_url=self.origin, timeout=10) as client:
                deadline = time.monotonic() + 20
                while True:
                    if not process.is_alive():
                        raise AssertionError("owned run service exited during startup")
                    try:
                        response = client.get(self.base)
                    except httpx.TransportError:
                        response = None
                    if response is not None and response.status_code in (401, 404, 405):
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError("owned run service did not start")
                    time.sleep(0.1)
                yield client, process.pid
        finally:
            if process.pid is not None:
                process.terminate()
                process.join(10)
                if process.is_alive():
                    process.kill()
                    process.join(5)
                assert not process.is_alive()
                process.close()
            sock.close()
            self.origin = "https://run-api.example"

    def request(self, client, method, path, body=None, key=None):
        headers = self.headers(
            method, getattr(self, "origin", "https://run-api.example") + path
        )
        if key is not None:
            headers["Idempotency-Key"] = key
        return client.request(method, path, json=body, headers=headers)

    @property
    def base(self):
        return f"/v1/deployments/{DEPLOYMENT}/external-automation-runs"

    def body(self, **changes):
        return {
            "run_id": "run-" + uuid4().hex,
            "automation_id": "triage",
            "automation_version": "1",
            "correlation_id": "corr",
            "reported_distribution": {
                "name": "support",
                "version": "3",
                "package_digest": None,
            },
            "generation_id": self.generation,
            **changes,
        }

    def propose(self, client, run, **changes):
        body = {
            **{
                key: run[key]
                for key in (
                    "automation_id",
                    "automation_version",
                    "run_id",
                    "correlation_id",
                )
            },
            "action_type": self.contract.action_type,
            "target_ref": "ticket:T-42",
            "risk_tier": "high",
            "action_schema_digest": self.contract.action_schema_digest,
            "requested_review_expires_at": (
                datetime.now(timezone.utc) + timedelta(hours=1)
            ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "summary_code": "resolve_after_deploy",
            "payload": self.values["payload_original"]["value"],
            "evidence_digests": ["sha256:" + "a" * 64],
            **changes,
        }
        key = "proposal-" + uuid4().hex
        self.last_submission = (dict(body), key)
        response = self.request(
            client,
            "POST",
            self.base.replace("external-automation-runs", "external-actions"),
            body,
            key,
        )
        assert response.status_code == 201, response.json()
        return response

    def close(self):
        if self.role_oid is None:
            return
        with psycopg.connect(self.dsn) as admin:
            current = admin.execute(
                "SELECT oid FROM pg_roles WHERE rolname=%s", (self.role,)
            ).fetchone()
            if current is None:
                # CREATE ROLE may have rolled back with failed initialization.
                self.role_oid = None
                return
            if current != (self.role_oid,):
                raise AssertionError("owned role identity changed; refusing cleanup")
            admin.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(self.role)))
            admin.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(self.role)))
        self.role_oid = None


def _serve_run_identity(state, sock):
    import uvicorn

    fixture = RunFixture.__new__(RunFixture)
    fixture.__dict__.update(state)
    fixture.key = Ed25519PrivateKey.from_private_bytes(state["key"])
    with fixture.application() as app:
        uvicorn.Server(
            uvicorn.Config(app, lifespan="off", log_level="critical", access_log=False)
        ).run(sockets=[sock])


@pytest.fixture
def run_fixture(run_database):
    fixture = RunFixture(*run_database)
    try:
        yield fixture
    finally:
        fixture.close()


def test_run_registration_replay_conflicts_and_concurrency_are_durable(run_fixture):
    f = run_fixture
    body = f.body()
    key = "register-" + uuid4().hex
    with f.client() as client:
        first = f.request(client, "POST", f.base, body, key)
        assert first.status_code == 201, first.json()
        replay = f.request(client, "POST", f.base, body, key)
        assert replay.status_code == 200, replay.json()
        assert replay.json() == first.json()
        changed = f.request(
            client, "POST", f.base, {**body, "automation_version": "2"}, key
        )
        assert changed.status_code == 409, changed.json()
        other_key = f.request(client, "POST", f.base, body, "register-" + uuid4().hex)
        assert other_key.status_code == 409, other_key.json()
    with psycopg.connect(f.dsn) as admin:
        assert admin.execute(
            "SELECT count(*) FROM okg.external_automation_runs WHERE run_id=%s",
            (body["run_id"],),
        ).fetchone() == (1,)

    race_body, race_key = f.body(), "registration-" + uuid4().hex

    def register_race(_):
        with f.client() as client:
            response = f.request(client, "POST", f.base, race_body, race_key)
            return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(register_race, range(4)))
    assert sorted(status for status, _ in results) == [200, 200, 200, 201], results
    assert len({row["record_id"] for _, row in results}) == 1
    with psycopg.connect(f.dsn) as admin:
        assert admin.execute(
            "SELECT count(*) FROM okg.external_automation_runs WHERE run_id=%s",
            (race_body["run_id"],),
        ).fetchone() == (1,)


def test_run_proposal_links_are_immutable_bounded_and_match_all_identity_fields(
    run_fixture,
):
    f = run_fixture
    body = f.body()
    with f.client() as client:
        registered = f.request(
            client, "POST", f.base, body, "registration-" + uuid4().hex
        )
        assert registered.status_code == 201, registered.json()
        record_id = registered.json()["record_id"]
        proposal = f.propose(client, body)
        proposal_id = proposal.json()["proposal_id"]
        path = f.base + "/" + record_id + "/proposals"
        key = "association-" + uuid4().hex
        linked = f.request(client, "POST", path, {"proposal_id": proposal_id}, key)
        assert linked.status_code == 201, linked.json()
        replay = f.request(client, "POST", path, {"proposal_id": proposal_id}, key)
        assert replay.status_code == 200 and replay.json() == linked.json()
        assert (
            f.request(
                client,
                "POST",
                path,
                {"proposal_id": proposal_id},
                "association-" + uuid4().hex,
            ).status_code
            == 409
        )
        read = f.request(client, "GET", f.base + "/" + record_id)
        assert read.status_code == 200, read.json()
        assert read.json()["proposals"] == [
            {"proposal_id": proposal_id, "linked_at": linked.json()["linked_at"]}
        ]
        for field in (
            "automation_id",
            "automation_version",
            "run_id",
            "correlation_id",
        ):
            mismatch = f.propose(client, body, **{field: "different"}).json()[
                "proposal_id"
            ]
            refusal = f.request(
                client,
                "POST",
                path,
                {"proposal_id": mismatch},
                "association-" + uuid4().hex,
            )
            assert (
                refusal.status_code == 409
                and refusal.json()["code"] == "run_proposal_mismatch"
            )
        with psycopg.connect(f.dsn) as admin:
            admin.execute(
                "UPDATE okg.nomos_subject_bindings SET subject='subject:other-service' WHERE binding_id=%s",
                (f.binding,),
            )
        f.policy(subjects=[SUBJECT, "subject:other-service"])
        foreign = f.propose(client, body).json()["proposal_id"]
        with psycopg.connect(f.dsn) as admin:
            admin.execute(
                "UPDATE okg.nomos_subject_bindings SET subject=%s WHERE binding_id=%s",
                (SUBJECT, f.binding),
            )
        f.policy()
        refusal = f.request(
            client, "POST", path, {"proposal_id": foreign}, "association-" + uuid4().hex
        )
        assert (
            refusal.status_code == 409
            and refusal.json()["code"] == "run_proposal_mismatch"
        )
        race_id = f.propose(client, body).json()["proposal_id"]
        race_key = "association-" + uuid4().hex

        def link_race(_):
            with f.client() as concurrent:
                response = f.request(
                    concurrent, "POST", path, {"proposal_id": race_id}, race_key
                )
                return response.status_code, response.json()

        with ThreadPoolExecutor(max_workers=3) as pool:
            raced = list(pool.map(link_race, range(3)))
        assert sorted(status for status, _ in raced) == [200, 200, 201], raced
        assert all(value == raced[0][1] for _, value in raced)
        for _ in range(62):
            extra = f.propose(client, body).json()["proposal_id"]
            response = f.request(
                client,
                "POST",
                path,
                {"proposal_id": extra},
                "association-" + uuid4().hex,
            )
            assert response.status_code == 201, response.json()
        overflow = f.propose(client, body).json()["proposal_id"]
        response = f.request(
            client,
            "POST",
            path,
            {"proposal_id": overflow},
            "association-" + uuid4().hex,
        )
        assert (
            response.status_code == 409 and response.json()["code"] == "run_link_limit"
        )
        assert (
            len(f.request(client, "GET", f.base + "/" + record_id).json()["proposals"])
            == 64
        )


def test_run_generation_reference_refuses_missing_unpublished_and_foreign_rows(
    run_fixture,
):
    f = run_fixture
    body = f.body()
    key = "registration-" + uuid4().hex
    with f.client() as client:
        registered = f.request(client, "POST", f.base, body, key)
        assert registered.status_code == 201, registered.json()
        record_id = registered.json()["record_id"]
        for missing in (1, "1", "missing-checkpoint", f.generation.upper()):
            response = f.request(
                client,
                "POST",
                f.base,
                f.body(generation_id=missing),
                "registration-" + uuid4().hex,
            )
            assert response.status_code == 422, response.json()
        with psycopg.connect(f.dsn) as admin:
            original_raw = admin.execute(
                "SELECT metadata FROM public._chronos_branch_interval_checkpoints WHERE checkpoint_id=%s",
                (f.generation,),
            ).fetchone()[0]
            original = (
                json.loads(original_raw)
                if isinstance(original_raw, str)
                else original_raw
            )
        # Change only fixture checkpoint metadata to exercise the authoritative
        # view's status/deployment predicates; never manufacture a valid row.
        try:
            for changes in (
                {"status": "staged"},
                {"deployment_name": "other-deployment"},
            ):
                with psycopg.connect(f.dsn) as admin:
                    admin.execute(
                        "UPDATE public._chronos_branch_interval_checkpoints SET metadata=%s::jsonb WHERE checkpoint_id=%s",
                        (json.dumps({**original, **changes}), f.generation),
                    )
                response = f.request(
                    client, "POST", f.base, f.body(), "registration-" + uuid4().hex
                )
                assert response.status_code == 422, response.json()
                assert (
                    f.request(client, "GET", f.base + "/" + record_id).json()
                    == registered.json()
                )
                assert (
                    f.request(client, "POST", f.base, body, key).json()
                    == registered.json()
                )
        finally:
            with psycopg.connect(f.dsn) as admin:
                admin.execute(
                    "UPDATE public._chronos_branch_interval_checkpoints SET metadata=%s::jsonb WHERE checkpoint_id=%s",
                    (json.dumps(original), f.generation),
                )
        with psycopg.connect(f.dsn) as admin:
            assert admin.execute(
                "SELECT count(*) FROM okg.external_automation_runs WHERE run_id=%s",
                (body["run_id"],),
            ).fetchone() == (1,)


def test_run_reads_and_links_recheck_identity_policy_and_owner(run_fixture):
    f = run_fixture
    body = f.body()
    with f.client() as client:
        registered = f.request(
            client, "POST", f.base, body, "registration-" + uuid4().hex
        )
        assert registered.status_code == 201, registered.json()
        path = f.base + "/" + registered.json()["record_id"]
        proposal_id = f.propose(client, body).json()["proposal_id"]
        link = {"proposal_id": proposal_id}
        for permissions in (["external_automation.link"], ["external_action.read"]):
            f.policy(permissions)
            response = f.request(
                client, "POST", path + "/proposals", link, "association-" + uuid4().hex
            )
            assert response.status_code == 403, response.json()
        f.policy(["external_automation.link", "external_action.read"])
        linked = f.request(
            client, "POST", path + "/proposals", link, "association-" + uuid4().hex
        )
        assert linked.status_code == 201, linked.json()
        assert f.request(client, "GET", path).status_code == 403
        f.policy()
        assert f.request(client, "GET", path).status_code == 200
        with psycopg.connect(f.dsn) as admin:
            row = admin.execute(
                "SELECT authentication_audit_id,nomos_decision_id,proposal_read_authentication_audit_id,proposal_read_nomos_decision_id FROM okg.external_automation_run_proposals WHERE proposal_id=%s",
                (proposal_id,),
            ).fetchone()
            assert row[0] != row[2] and row[1] != row[3]
            admin.execute(
                "UPDATE okg.nomos_subject_bindings SET status='disabled' WHERE binding_id=%s",
                (f.binding,),
            )
        assert f.request(client, "GET", path).status_code == 401
        with psycopg.connect(f.dsn) as admin:
            admin.execute(
                "UPDATE okg.nomos_subject_bindings SET status='active',subject='subject:other-service' WHERE binding_id=%s",
                (f.binding,),
            )
        f.policy(
            ["external_automation.read"], subjects=[SUBJECT, "subject:other-service"]
        )
        assert f.request(client, "GET", path).status_code == 404
        with psycopg.connect(f.dsn) as admin:
            admin.execute(
                "UPDATE okg.nomos_subject_bindings SET subject=%s WHERE binding_id=%s",
                (SUBJECT, f.binding),
            )
        f.policy()
        assert f.request(
            client, "GET", path.replace(DEPLOYMENT, "different-deployment")
        ).status_code in (403, 404)


def test_run_rpc_boundary_refuses_raw_writes_forged_decisions_and_unbound_generation_access(
    run_fixture,
):
    f = run_fixture
    from okg.substrate.external_actions.store import ExternalActionStoreError

    with psycopg.connect(f.runtime_dsn, autocommit=True) as runtime:
        for statement in (
            "SELECT * FROM okg.external_automation_runs",
            "INSERT INTO okg.external_automation_runs(record_id) VALUES ('forged')",
            "UPDATE okg.external_automation_runs SET run_id='forged'",
            "DELETE FROM okg.external_automation_runs",
            "SELECT * FROM okg.external_automation_run_proposals",
            "SELECT id FROM okg.v_generations",
            "SELECT checkpoint_id FROM public._chronos_branch_interval_checkpoints",
            "SELECT okg.external_automation_authority('{}'::jsonb,'external_automation.read','primary')",
            "SELECT okg.external_automation_projection('forged')",
            "SELECT okg.external_automation_transaction_context()",
        ):
            with pytest.raises(psycopg.Error) as refusal:
                runtime.execute(statement)
            assert refusal.value.sqlstate in {"42501", "28000"}
    body = f.body()
    with f.client() as client:
        record = f.request(client, "POST", f.base, body, "registration-" + uuid4().hex)
        assert record.status_code == 201, record.json()
        proposal_id = f.propose(client, body).json()["proposal_id"]
        path = f.base + "/" + record.json()["record_id"] + "/proposals"

        def swapped(command):
            for suffix in ("authentication_audit_id", "nomos_decision_id"):
                a, b = "primary_" + suffix, "proposal_read_" + suffix
                command[a], command[b] = command[b], command[a]
            return command

        mutations = [
            lambda c: {**c, "primary_nomos_decision_id": "missing-decision"},
            lambda c: {
                **c,
                "proposal_read_authentication_audit_id": c[
                    "primary_authentication_audit_id"
                ],
                "proposal_read_nomos_decision_id": c["primary_nomos_decision_id"],
            },
            swapped,
            lambda c: {**c, "body": {"proposal_id": "01J00000000000000000000000"}},
            lambda c: {**c, "request_hash": "sha256:" + "0" * 64},
            lambda c: {**c, "owner": "subject:other-service"},
            lambda c: {**c, "deployment": "different-deployment"},
        ]
        for mutation in mutations:
            f.mutate_command = mutation
            response = f.request(
                client,
                "POST",
                path,
                {"proposal_id": proposal_id},
                "association-" + uuid4().hex,
            )
            assert (response.status_code, response.json()["code"]) in {
                (403, "run_authority_invalid"),
                (503, "review_authority_unavailable"),
            }, response.json()
        f.mutate_command = None
        linked = f.request(
            client,
            "POST",
            path,
            {"proposal_id": proposal_id},
            "association-" + uuid4().hex,
        )
        assert linked.status_code == 201, linked.json()
        _, retained_command = f.commands[-1]
    store = PostgresExternalActionStore(
        f.runtime_dsn,
        deployment=DEPLOYMENT,
        verifier_capability=_TEST_VERIFIER_CAPABILITY,
    )
    try:
        with pytest.raises(ExternalActionStoreError) as refusal:
            with store.lock:
                store._typed("external_automation_link", retained_command)
        assert refusal.value.sqlstate in {"42501", "28000"}
    finally:
        store.connection.close()
    with psycopg.connect(f.dsn) as admin:
        assert admin.execute(
            "SELECT count(*) FROM okg.external_automation_run_proposals WHERE proposal_id=%s",
            (proposal_id,),
        ).fetchone() == (1,)


def test_authenticated_run_registration_survives_restart_and_correlates_real_graph_read_and_action(
    run_fixture, tmp_path, record_property
):
    import asyncio
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client
    from tests.substrate.mcp._two_principal_fixture import (
        configure_header_identity,
        _boundary_rules,
        SCOPE,
        PUBLIC_NODE,
    )
    from tests.substrate.mcp.test_multi_user_http_e2e import (
        _start_server,
        _stop_server,
        _free_port,
        _wait_until_listening,
        _headers,
        _FILTER_ARGUMENTS,
        _node_ids,
        _decision_watermark,
        _decisions_since,
        _request_subjects,
    )

    f = run_fixture
    configure_header_identity(
        f.dsn, principals={"fixture-run-service": SUBJECT}, drive_subjects=()
    )
    rules = _boundary_rules(SUBJECT, "registered-run") + [
        {
            "rule_id": "public-source",
            "effect": "allow",
            "actions": ["graph.read.mcp"],
            "subjects": [SUBJECT],
            "match": {
                "deployment": DEPLOYMENT,
                "scope": SCOPE,
                "source_id": "public_docs",
            },
            "priority": 100,
        }
    ]
    bundle = compile_policy_graph(
        {
            "policy_bundle_id": "registered-run-read",
            "policy_version": "1",
            "default_effect": "deny",
            "rules": rules,
        }
    )
    with psycopg.connect(f.dsn) as admin:
        activate_policy_bundle(
            admin,
            deployment_name=DEPLOYMENT,
            bundle=bundle,
            scope=SCOPE,
            activated_by="test:operator",
        )
    watermark = _decision_watermark(f.dsn)
    token = uuid4().hex
    port = _free_port()
    log_path = tmp_path / "mcp.log"
    proc = _start_server(f.dsn, port, log_path, token=token)

    async def read_graph():
        async with streamablehttp_client(
            f"http://127.0.0.1:{port}/mcp",
            headers=_headers("fixture-run-service", token=token),
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                response = await session.call_tool("filter", dict(_FILTER_ARGUMENTS))
                payload = _result_payload(response)
                assert _node_ids(payload) == {PUBLIC_NODE}
                inspected = await session.call_tool(
                    "inspect", {"target": {"kind": "session"}}
                )
                state = _result_payload(inspected)
                return state["generation_id"]

    try:
        assert _wait_until_listening(proc, port, log_path)
        observed_generation = asyncio.run(read_graph())
        assert observed_generation == f.generation
    finally:
        _stop_server(proc)
    assert _request_subjects(_decisions_since(f.dsn, watermark)) == {SUBJECT}
    body = f.body(generation_id=observed_generation)
    key = "registration-" + uuid4().hex
    with f.server_process() as (client, first_pid):
        registered = f.request(client, "POST", f.base, body, key)
        assert registered.status_code == 201, registered.json()
        record_id = registered.json()["record_id"]
        proposal = f.propose(client, body)
        proposal_id = proposal.json()["proposal_id"]
        assert proposal.json()["review_state"] == "pending"
        linked = f.request(
            client,
            "POST",
            f.base + "/" + record_id + "/proposals",
            {"proposal_id": proposal_id},
            "association-" + uuid4().hex,
        )
        assert linked.status_code == 201, linked.json()
        read = f.request(client, "GET", f.base + "/" + record_id)
        assert read.status_code == 200
        persisted = read.json()
    with f.server_process() as (client, second_pid):
        assert second_pid != first_pid
        assert f.request(client, "GET", f.base + "/" + record_id).json() == persisted
        assert f.request(client, "POST", f.base, body, key).json() == persisted
        action = f.request(
            client,
            "GET",
            f.base.replace("external-automation-runs", "external-actions")
            + "/"
            + proposal_id,
        )
        assert action.status_code == 200, action.json()
        assert action.json()["proposal"]["review_state"] == "pending"
    record_property("generation_id", observed_generation)
    record_property("authenticated_separation_subject", SUBJECT)
    record_property("registered_run_id", record_id)
    record_property("proposal_id", proposal_id)
    record_property("actual_graph_visible_nodes", 1)
    record_property("actual_proposal_review_state", "pending")
    record_property(
        "read_connection_role_scope",
        "fixture superuser graph read pool; runtime review uses dedicated least-privilege login",
    )


def _wait_for_no_running_timescale_jobs(dsn: str, *, timeout: float = 120.0) -> None:
    """Wait until no TimescaleDB job is running in ``dsn``'s database.

    Unscheduling a job does not stop a run already in progress, so a job
    started by the restarted scheduler can still hold chunk locks. Fails the
    test, naming the jobs, if any is still running after ``timeout`` seconds.
    """

    deadline = time.monotonic() + timeout
    running: list[tuple] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        while True:
            running = conn.execute(
                "SELECT job_id FROM timescaledb_information.job_stats "
                "WHERE job_status = 'Running' ORDER BY job_id"
            ).fetchall()
            if not running or time.monotonic() >= deadline:
                break
            time.sleep(0.5)
    assert not running, (
        f"TimescaleDB jobs still running {timeout:.0f}s after they were "
        f"unscheduled on the restored clone: {running!r}"
    )


def test_run_restore_quarantine_and_clone_preserve_authority_boundaries(
    run_fixture, tmp_path, record_property
):
    from psycopg.conninfo import conninfo_to_dict
    from okg.substrate.environment_clone import (
        create_environment_clone_backup,
        claim_environment_database_owner,
        _libpq_command_target,
        _tool,
        _run,
    )
    from okg.substrate.postgres_restore import timescaledb_restore_context
    from okg.substrate.external_actions.store import ExternalActionStoreError

    f = run_fixture
    body = f.body()
    key = "registration-" + uuid4().hex
    with f.client() as client:
        registered = f.request(client, "POST", f.base, body, key)
        assert registered.status_code == 201, registered.json()
        record_id = registered.json()["record_id"]
        proposal_id = f.propose(client, body).json()["proposal_id"]
        link_key = "association-" + uuid4().hex
        path = f.base + "/" + record_id
        linked = f.request(
            client, "POST", path + "/proposals", {"proposal_id": proposal_id}, link_key
        )
        assert linked.status_code == 201, linked.json()
        with psycopg.connect(f.dsn) as admin:
            old = admin.execute(
                "SELECT service_mode,quarantine_state,store_record FROM okg.external_action_restore_fences WHERE deployment_name=%s",
                (DEPLOYMENT,),
            ).fetchone()
            admin.execute(
                "UPDATE okg.external_action_restore_fences SET service_mode='restore_only',quarantine_state='blocked',store_record=store_record || jsonb_build_object('service_mode','restore_only','quarantine_state','blocked') WHERE deployment_name=%s",
                (DEPLOYMENT,),
            )
        try:
            assert f.request(client, "GET", path).status_code == 200
            assert f.request(client, "POST", f.base, body, key).status_code == 200
            assert (
                f.request(
                    client,
                    "POST",
                    path + "/proposals",
                    {"proposal_id": proposal_id},
                    link_key,
                ).status_code
                == 200
            )
            refusal = f.request(
                client, "POST", f.base, f.body(), "registration-" + uuid4().hex
            )
            assert (
                refusal.status_code == 409
                and refusal.json()["code"] == "run_restore_quarantined"
            )
            # A fresh association key is a new mutation even for a known proposal.
            refusal = f.request(
                client,
                "POST",
                path + "/proposals",
                {"proposal_id": proposal_id},
                "association-" + uuid4().hex,
            )
            assert (
                refusal.status_code == 409
                and refusal.json()["code"] == "run_restore_quarantined"
            )
        finally:
            with psycopg.connect(f.dsn) as admin:
                admin.execute(
                    "UPDATE okg.external_action_restore_fences SET service_mode=%s,quarantine_state=%s,store_record=%s::jsonb WHERE deployment_name=%s",
                    (old[0], old[1], json.dumps(old[2]), DEPLOYMENT),
                )
    target_name = "okg_test_run_clone_" + uuid4().hex
    maintenance = make_conninfo(f.dsn, dbname="postgres")
    target_dsn = make_conninfo(f.dsn, dbname=target_name)
    target_oid = None
    try:
        with psycopg.connect(maintenance, autocommit=True) as admin:
            admin.execute(
                sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                    sql.Identifier(target_name)
                )
            )
            target_oid = admin.execute(
                "SELECT oid FROM pg_database WHERE datname=%s", (target_name,)
            ).fetchone()[0]
        # These are explicit local test descriptors, not a sealed supervised
        # environment or fabricated host-service fence authority.
        definition = (
            "sha256:"
            + hashlib.sha256(
                (ROOT / "deployments" / DEPLOYMENT / "deployment.yaml").read_bytes()
            ).hexdigest()
        )
        source = SimpleNamespace(
            id="run-proof-source",
            tier="production",
            definition_digest=definition,
            graph_deployment_name=DEPLOYMENT,
            database=SimpleNamespace(dsn=f.dsn, name=conninfo_to_dict(f.dsn)["dbname"]),
        )
        target = SimpleNamespace(
            id="run-proof-target",
            tier="staging",
            definition_digest=definition,
            graph_deployment_name=DEPLOYMENT + "-clone",
            database=SimpleNamespace(dsn=target_dsn, name=target_name),
        )
        with psycopg.connect(f.dsn) as admin:
            version = admin.execute(
                "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            ).fetchone()[0]
            # Explicit archive fixtures, not claims of production execution.
            for age in (45, 0):
                admin.execute(
                    "INSERT INTO okg.mcp_trace_events(session_id,deployment_name,tool_name,event_time) VALUES ('clone-transport-proof',%s,'fixture-trace',now()-%s*interval '1 day')",
                    (DEPLOYMENT, age),
                )
            admin.execute(
                "INSERT INTO okg.node_facts(node_id,subtype,attrs,source,source_record_id,source_revision,observed_at,dedupe_key,run_id,op) VALUES ('clone-preserved-fact','fixture','{}','clone-transport-proof','{}','{}',now()-interval '45 days',%s,%s,'I')",
                (uuid4().hex, uuid4()),
            )
            for table in ("okg.mcp_trace_events", "okg.node_facts"):
                chunks = admin.execute(
                    "SELECT show_chunks(%s::regclass,older_than=>interval '30 days')::text",
                    (table,),
                ).fetchall()
                assert chunks
                for (chunk,) in chunks:
                    admin.execute(
                        "SELECT compress_chunk(%s::regclass,if_not_compressed=>true)",
                        (chunk,),
                    )
            # Resolve actual physical OIDs independently of the backup helper.
            physical = admin.execute(
                "SELECT h.table_name,c.relid::regclass::text,s.compress_relid::regclass::text FROM _timescaledb_catalog.hypertable h JOIN _timescaledb_catalog.chunk c ON c.hypertable_id=h.id LEFT JOIN _timescaledb_catalog.compression_settings s ON s.relid=c.relid WHERE h.schema_name='okg' AND h.table_name IN ('mcp_trace_events','node_facts') ORDER BY 1,2"
            ).fetchall()
            purge_physical = {
                name
                for table, *names in physical
                if table == "mcp_trace_events"
                for name in names
                if name
            }
            preserve_physical = {
                name
                for table, *names in physical
                if table == "node_facts"
                for name in names
                if name
            }
            assert any(
                table == "mcp_trace_events" and compressed
                for table, _, compressed in physical
            )
            assert any(
                table == "mcp_trace_events" and compressed is None
                for table, _, compressed in physical
            )
            assert any(
                table == "node_facts" and compressed
                for table, _, compressed in physical
            )
            facts = admin.execute(
                "SELECT row_to_json(n)::text FROM okg.node_facts n ORDER BY fact_id,observed_at"
            ).fetchall()
            assert facts
            trace_count = admin.execute(
                "SELECT count(*) FROM okg.mcp_trace_events"
            ).fetchone()[0]
            assert trace_count >= 2
        owner = claim_environment_database_owner(source, apply=True)
        assert owner["ok"] and owner["owner_after"] == DEPLOYMENT
        report = create_environment_clone_backup(
            source=source,
            target=target,
            dump_path=tmp_path / "clone.dump",
            report_path=tmp_path / "clone.json",
            apply=True,
        )
        assert report["dump"]["digest"] and (tmp_path / "clone.dump").stat().st_size > 0
        for table in (
            "okg.external_automation_runs",
            "okg.external_automation_run_proposals",
        ):
            item = next(row for row in report["tables"] if row["table"] == table)
            assert item["disposition"] == "purge" and item["rows"] > 0
        assert purge_physical <= set(report["physical_purge_relations"])
        assert not preserve_physical & set(report["physical_purge_relations"])
        with psycopg.connect(maintenance) as admin:
            assert admin.execute(
                "SELECT oid FROM pg_database WHERE datname=%s", (target_name,)
            ).fetchone() == (target_oid,)
        database, env = _libpq_command_target(target_dsn)
        with timescaledb_restore_context(
            target_dsn, version=report["source"]["timescaledb_version"]
        ):
            _run(
                [
                    _tool("pg_restore"),
                    "--exit-on-error",
                    "--no-owner",
                    "--no-privileges",
                    "--dbname",
                    database,
                    str(tmp_path / "clone.dump"),
                ],
                operation="test-only clone archive transport",
                env=env,
            )
        # The restore restarts TimescaleDB's scheduler, and the dump carries
        # the source's job schedules but not their run history, so every job
        # is due at once. A compression or retention job can then take an
        # AccessExclusive lock on a chunk while the reads below hold
        # AccessShare locks across chunks: "deadlock detected" on a busy
        # runner, even in autocommit, because one statement over node_facts
        # spans chunks. Unschedule every job on the restored database, then
        # wait until none is still running.
        _pause_timescale_jobs(target_dsn)
        _wait_for_no_running_timescale_jobs(target_dsn)
        # Autocommit: each read takes and releases its own locks.
        with psycopg.connect(target_dsn, autocommit=True) as admin:
            assert admin.execute("SELECT current_database()").fetchone() == (
                target_name,
            )
            for table in (
                "external_automation_runs",
                "external_automation_run_proposals",
                "external_action_api_role_bindings",
                "mcp_trace_events",
            ):
                assert admin.execute(
                    sql.SQL("SELECT count(*) FROM okg.{}").format(sql.Identifier(table))
                ).fetchone() == (0,)
            assert (
                admin.execute(
                    "SELECT row_to_json(n)::text FROM okg.node_facts n ORDER BY fact_id,observed_at"
                ).fetchall()
                == facts
            )
        with pytest.raises(ExternalActionStoreError):
            PostgresExternalActionStore(
                make_conninfo(target_dsn, user=f.role, password=f.password),
                deployment=DEPLOYMENT,
                verifier_capability=_TEST_VERIFIER_CAPABILITY,
            )
        with psycopg.connect(f.dsn) as admin:
            assert admin.execute(
                "SELECT count(*) FROM okg.external_automation_runs WHERE record_id=%s",
                (record_id,),
            ).fetchone() == (1,)
            assert admin.execute(
                "SELECT count(*) FROM okg.external_automation_run_proposals WHERE proposal_id=%s",
                (proposal_id,),
            ).fetchone() == (1,)
        record_property(
            "clone_proof_scope",
            "actual backup/archive transport; supervised host-fenced restore unproven",
        )
        record_property("clone_archive_digest", report["dump"]["digest"])
        record_property("timescaledb_version", version)
        record_property("purged_physical_relations", json.dumps(sorted(purge_physical)))
        record_property(
            "preserved_physical_relations", json.dumps(sorted(preserve_physical))
        )
        record_property("preserved_fact_rows", len(facts))
        record_property("source_trace_rows", trace_count)
        record_property("restored_trace_rows", 0)
    finally:
        if target_oid is not None:
            with psycopg.connect(maintenance, autocommit=True) as admin:
                assert admin.execute(
                    "SELECT oid FROM pg_database WHERE datname=%s", (target_name,)
                ).fetchone() == (target_oid,)
                admin.execute(
                    sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                        sql.Identifier(target_name)
                    )
                )


def test_run_identity_migration_preserves_legacy_proposal_bytes_and_replay(
    run_fixture, record_property
):
    from dataclasses import fields
    from okg.substrate.external_actions.models import AuditEvent

    f = run_fixture
    body = f.body()
    with f.client() as client:
        legacy = f.propose(client, body)
        proposal_id = legacy.json()["proposal_id"]
        request, key = f.last_submission
    with psycopg.connect(f.dsn) as admin:

        def retained_bytes():
            return tuple(
                admin.execute(query, (proposal_id,)).fetchall()
                for query in (
                    "SELECT row_to_json(p)::text FROM okg.external_action_proposals p WHERE proposal_id=%s",
                    "SELECT row_to_json(v)::text FROM okg.external_action_payload_versions v WHERE proposal_id=%s ORDER BY payload_version",
                    "SELECT row_to_json(e)::text FROM okg.external_action_events e WHERE proposal_id=%s ORDER BY sequence",
                )
            )

        before = retained_bytes()
        event = admin.execute(
            "SELECT to_jsonb(e),store_record->'event_projection',event_hash FROM okg.external_action_events e WHERE proposal_id=%s",
            (proposal_id,),
        ).fetchone()
        assert {field.name for field in fields(AuditEvent)} <= event[0].keys()
        store_projection = PostgresExternalActionStore._coerce_record(
            event[0], AuditEvent
        )
        hydrated = AuditEvent(**store_projection).to_projection()
        assert hydrated == {**event[1], "event_hash": event[2]}

        def new_schema_shape():
            return (
                admin.execute(
                    "SELECT table_name,column_name,data_type,is_nullable,collation_name FROM information_schema.columns WHERE table_schema='okg' AND table_name IN ('external_automation_runs','external_automation_run_proposals') ORDER BY table_name,ordinal_position"
                ).fetchall(),
                admin.execute(
                    "SELECT c.relname,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid WHERE c.relnamespace='okg'::regnamespace AND c.relname IN ('external_automation_runs','external_automation_run_proposals') ORDER BY c.relname,k.conname"
                ).fetchall(),
                admin.execute(
                    "SELECT proname,prosrc,prosecdef,proconfig,pg_get_userbyid(proowner),ARRAY(SELECT grantor::regrole::text||':'||grantee::regrole::text||':'||privilege_type||':'||is_grantable::text FROM aclexplode(COALESCE(proacl,acldefault('f',proowner))) ORDER BY 1) FROM pg_proc WHERE pronamespace='okg'::regnamespace AND proname LIKE 'external_automation_%' ORDER BY proname"
                ).fetchall(),
            )

        fresh_shape = new_schema_shape()
        # Reconstruct only this new feature's absent pre-0033 objects inside
        # the owned database; retain the populated legacy action ledgers.
        admin.execute(
            "DROP TABLE okg.external_automation_run_proposals,okg.external_automation_runs CASCADE"
        )
        for name, args in (
            ("transaction_context", ""),
            ("register", "jsonb"),
            ("read", "jsonb"),
            ("link", "jsonb"),
            ("authority", "jsonb,text,text"),
            ("projection", "text"),
        ):
            admin.execute(
                sql.SQL("DROP FUNCTION IF EXISTS okg.{}(" + args + ")").format(
                    sql.Identifier("external_automation_" + name)
                )
            )
        historical = (
            ROOT / "src/okg/substrate/db/migrations/0030_external_action_review.sql"
        ).read_text()
        start = historical.index(
            "CREATE OR REPLACE FUNCTION okg.external_action_store_snapshot()\nRETURNS JSONB LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, okg AS $external_action_store_snapshot_v2$"
        )
        end = historical.index("$external_action_store_snapshot_v2$;", start) + len(
            "$external_action_store_snapshot_v2$;"
        )
        admin.execute(historical[start:end])
        assert retained_bytes() == before
        admin.execute(
            (
                ROOT
                / "src/okg/substrate/db/migrations/0033_external_automation_run_identity.sql"
            ).read_text()
        )
        forward_shape = new_schema_shape()
        differences = [
            (before_row[0], index, str(before_value)[:350], str(after_value)[:350])
            for before_row, after_row in zip(fresh_shape[2], forward_shape[2])
            for index, (before_value, after_value) in enumerate(
                zip(before_row, after_row)
            )
            if before_value != after_value
        ]
        assert forward_shape == fresh_shape, differences
        assert retained_bytes() == before
        assert admin.execute(
            "SELECT count(*) FROM okg.external_automation_runs"
        ).fetchone() == (0,)
        assert admin.execute(
            "SELECT count(*) FROM okg.external_automation_run_proposals"
        ).fetchone() == (0,)
    with f.client() as client:
        replay = f.request(
            client,
            "POST",
            f.base.replace("external-automation-runs", "external-actions"),
            request,
            key,
        )
        assert replay.status_code == 200 and replay.content == legacy.content
        registered = f.request(
            client, "POST", f.base, body, "registration-" + uuid4().hex
        )
        assert registered.status_code == 201, registered.json()
        linked = f.request(
            client,
            "POST",
            f.base + "/" + registered.json()["record_id"] + "/proposals",
            {"proposal_id": proposal_id},
            "association-" + uuid4().hex,
        )
        assert linked.status_code == 201, linked.json()
    with psycopg.connect(f.dsn) as admin:
        assert retained_bytes() == before
        times = admin.execute(
            "SELECT p.created_at,r.registered_at,l.linked_at FROM okg.external_action_proposals p JOIN okg.external_automation_run_proposals l USING (deployment_name,proposal_id) JOIN okg.external_automation_runs r USING (deployment_name,record_id) WHERE p.proposal_id=%s",
            (proposal_id,),
        ).fetchone()
        assert times[0] < times[1] <= times[2]
    record_property("legacy_proposal_id", proposal_id)
    record_property("legacy_projection_replay_bytes_unchanged", True)
    record_property("fresh_and_forward_run_schema_equal", True)


_PROPOSAL_INSERT = """
INSERT INTO okg.external_action_proposals (
  proposal_id, deployment_name, execution_epoch, effect_instance_key,
  submission_content_hash, submission_request_hash,
  submission_idempotency_key_hash, proposer_separation_subject,
  automation_id, automation_version, run_id, correlation_id,
  action_type, target_ref, risk_tier, action_schema_digest,
  action_contract_digest, executor_contract_digest,
  payload_renderer_digest, governed_tool_binding_digest,
  provider_connection_contract_digest, provider_capability_digest,
  provider_operation_id, provider_operation_descriptor_digest,
  original_payload_hash, requested_review_expires_at,
  effective_review_expires_at, summary_code, current_payload_hash,
  review_state, execution_state, execute_before,
  current_decision_id, current_decision_binding_hash
) VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
  %s, %s, %s, %s, %s
)
"""


def _approved_proposal_row(
    deployment: str,
    proposal_id: str,
    effect_key: str,
    *,
    review_expires_at: datetime,
    execute_before: datetime,
    decision_id: str,
) -> tuple[object, ...]:
    """An approved, execution-ready proposal row with explicit deadlines."""

    base = _proposal_row(deployment, proposal_id, effect_key)
    # The submission idempotency key is unique per proposer, so two rows in one
    # deployment need distinct hashes.
    return (
        *base[:6],
        "sha256:" + hashlib.sha256(proposal_id.encode()).hexdigest(),
        *base[7:25],
        review_expires_at,
        review_expires_at,
        *base[27:],
        "approved",
        "ready",
        execute_before,
        decision_id,
        "sha256:" + ("b" * 64),
    )


def test_postgres_execution_deadline_expiry_matches_the_in_memory_code(run_database):
    """The SQL twin of ``_materialize_expiry`` records ``approval_expired``.

    An approved proposal whose ``execute_before`` has passed -- while its
    review deadline has not -- must expire under the ``execute_before``
    deadline kind, exactly once, and a control proposal whose deadline is
    still ahead must not expire at all.  This is the durable counterpart of
    the in-memory service proof in ``test_service.py``.

    Scope: this exercises ``okg.external_action_materialize_expiry``, the SQL
    twin of the service's expiry transition.  The durable claim function does
    not call it; see the follow-up issue named in this change's pull request.
    """

    dsn, _generation = run_database
    deployment = "external-action-exec-deadline-" + uuid4().hex[:12]
    expired_id = "proposal-" + uuid4().hex
    control_id = "proposal-" + uuid4().hex
    expired_key = "effect-" + uuid4().hex
    control_key = "effect-" + uuid4().hex
    decision_id = "nomos-" + uuid4().hex[:12]
    now = datetime.now(timezone.utc)

    with psycopg.connect(dsn) as conn:
        _bind_verifier_role(conn, deployment)
        _set_verifier_capability(conn)
        conn.execute(
            _PROPOSAL_INSERT,
            _approved_proposal_row(
                deployment, expired_id, expired_key,
                # The review deadline is deliberately still ahead, so only the
                # execution deadline can classify this row.
                review_expires_at=now + timedelta(hours=1),
                execute_before=now - timedelta(minutes=1),
                decision_id=decision_id,
            ),
        )
        conn.execute(
            _PROPOSAL_INSERT,
            _approved_proposal_row(
                deployment, control_id, control_key,
                review_expires_at=now + timedelta(hours=1),
                execute_before=now + timedelta(hours=1),
                decision_id=decision_id,
            ),
        )
        conn.commit()

        # The verifier capability is transaction-local; the commit above ends
        # the transaction that carried it.
        _set_verifier_capability(conn)
        conn.execute("SET LOCAL ROLE okg_external_action_api")
        materialized = conn.execute(
            "SELECT okg.external_action_materialize_expiry(%s)",
            (expired_id,),
        ).fetchone()[0]
        replay = conn.execute(
            "SELECT okg.external_action_materialize_expiry(%s)",
            (expired_id,),
        ).fetchone()[0]
        control = conn.execute(
            "SELECT okg.external_action_materialize_expiry(%s)",
            (control_id,),
        ).fetchone()[0]
        conn.execute("RESET ROLE")

        assert materialized["expired"] is True
        assert materialized["replay"] is False
        assert replay["replay"] is True
        assert control["expired"] is False
        assert control["replay"] is False

        assert conn.execute(
            "SELECT review_state, execution_state FROM okg.external_action_proposals "
            "WHERE deployment_name = %s AND proposal_id = %s",
            (deployment, expired_id),
        ).fetchone() == ("expired", "unavailable")
        # The control proposal is untouched and still claimable.
        assert conn.execute(
            "SELECT review_state, execution_state FROM okg.external_action_proposals "
            "WHERE deployment_name = %s AND proposal_id = %s",
            (deployment, control_id),
        ).fetchone() == ("approved", "ready")

        # The SQL path emits the same code the in-memory service emits, once,
        # under the same deadline kind, and never the review-deadline code.
        assert conn.execute(
            "SELECT event_type, deadline_kind, before_review_state,"
            " after_review_state, before_execution_state, after_execution_state,"
            " actor_kind, decision_id, effective_at, count(*) OVER () "
            "FROM okg.external_action_events "
            "WHERE deployment_name = %s AND proposal_id = %s",
            (deployment, expired_id),
        ).fetchall() == [
            (
                "approval_expired", "execute_before", "approved", "expired",
                "ready", "unavailable", "system_clock", decision_id,
                now - timedelta(minutes=1), 1,
            )
        ]
        assert conn.execute(
            "SELECT count(*) FROM okg.external_action_events "
            "WHERE deployment_name = %s AND proposal_id = %s",
            (deployment, control_id),
        ).fetchone() == (0,)


def test_late_review_decision_commits_its_review_expired_event(run_fixture):
    """A decision after the review deadline keeps the expiry it records (#2342).

    ``ExternalActionService._transition`` materializes the expiry inside the
    store-lock transaction and then refuses with ``proposal_expired``. On the
    PostgreSQL store that refusal used to roll the transaction back, so the
    ``review_expired`` event and the expired state were lost and the proposal
    stayed pending; the in-memory store kept both. The refusal is now a
    committed refusal: exactly one event commits, the proposal is expired,
    and a repeated late decision adds nothing. The proposer decides here on
    purpose: the expiry check runs before the proposer/approver separation
    check, so the deadline is what refuses it.
    """

    f = run_fixture
    body = f.body()
    actions = f.base.replace("external-automation-runs", "external-actions")
    with f.client() as client:
        registered = f.request(
            client, "POST", f.base, body, "registration-" + uuid4().hex
        )
        assert registered.status_code == 201, registered.json()
        deadline = datetime.now(timezone.utc) + timedelta(seconds=2)
        proposal = f.propose(
            client,
            body,
            requested_review_expires_at=deadline.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        proposal_id = proposal.json()["proposal_id"]
        etag = proposal.headers["etag"]
        time.sleep(max(0.0, (deadline - datetime.now(timezone.utc)).total_seconds()) + 1.0)

        def decide():
            path = actions + "/" + proposal_id + "/decisions"
            headers = f.headers(
                "POST", getattr(f, "origin", "https://run-api.example") + path
            )
            headers["Idempotency-Key"] = "decision-" + uuid4().hex
            headers["If-Match"] = etag
            return client.request(
                "POST",
                path,
                json={
                    "verdict": "approve",
                    "reason_code": "verified_nonterminal_update",
                    "execute_before": (
                        datetime.now(timezone.utc) + timedelta(minutes=30)
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                },
                headers=headers,
            )

        def committed():
            with psycopg.connect(f.dsn) as observer:
                state = observer.execute(
                    "SELECT review_state, execution_state"
                    " FROM okg.external_action_proposals"
                    " WHERE deployment_name = %s AND proposal_id = %s",
                    (DEPLOYMENT, proposal_id),
                ).fetchone()
                events = observer.execute(
                    "SELECT event_type, deadline_kind"
                    " FROM okg.external_action_events"
                    " WHERE deployment_name = %s AND proposal_id = %s"
                    " ORDER BY sequence",
                    (DEPLOYMENT, proposal_id),
                ).fetchall()
            return tuple(state), events

        late = decide()
        assert late.status_code == 410, late.json()
        assert late.json()["code"] == "proposal_expired"
        expected = (
            ("expired", "unavailable"),
            [("submitted", None), ("review_expired", "review_expires_at")],
        )
        assert committed() == expected

        repeated = decide()
        assert repeated.status_code == 410, repeated.json()
        assert repeated.json()["code"] == "proposal_expired"
        assert committed() == expected


def test_startup_contract_checks_leave_the_store_connection_idle(run_fixture):
    """The server's start-up sequence keeps the store connection idle (#2574).

    ``run_server`` builds the store, runs ``_require_runtime_store_contract``
    and then opens the login-binding transaction. The contract check and the
    database clock read ran outside any transaction, so on the default
    non-autocommit connection they left a transaction open that nothing
    committed; every later store transaction nested in it as a savepoint.
    The shared unit of work now refuses to nest, so each read must end its
    own transaction.
    """

    from okg.substrate.external_actions.server import (
        _require_runtime_store_contract,
        _startup_login_binding,
    )

    f = run_fixture
    store = PostgresExternalActionStore(
        f.runtime_dsn,
        deployment=DEPLOYMENT,
        verifier_capability=_TEST_VERIFIER_CAPABILITY,
    )
    try:
        idle = psycopg.pq.TransactionStatus.IDLE
        assert store.connection.info.transaction_status == idle
        _require_runtime_store_contract(store)
        assert store.connection.info.transaction_status == idle
        store.now()
        assert store.connection.info.transaction_status == idle
        binding = _startup_login_binding(store, deployment=DEPLOYMENT)
        assert binding
        assert store.connection.info.transaction_status == idle
    finally:
        store.connection.close()
