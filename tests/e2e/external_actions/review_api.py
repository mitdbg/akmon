"""Standalone review-API process used by the fake-worker acceptance tests."""

from __future__ import annotations

import asyncio
import copy
import json
import multiprocessing
from datetime import datetime, timezone
from multiprocessing.connection import Connection
from types import SimpleNamespace
from typing import Any, Mapping

from okg.substrate.external_actions.api import ExternalActionAPI
from okg.substrate.external_actions.auth import AuthenticationError
from okg.substrate.external_actions.service import ExternalActionService
from okg.substrate.external_actions.signing import canonical_json_bytes
from okg.substrate.external_actions.store import InMemoryExternalActionStore

from .fake_identity import resolve
from .rpc import _STOP, call, ready_listener, serve, stop


DEPLOYMENT = "support-prod"
FIXED_NOW = datetime(2026, 8, 26, 14, 0, tzinfo=timezone.utc)
FIXED_POLICY_DIGEST = "sha256:" + "3" * 64
FIXED_LEASE_DIGEST = "sha256:" + "2" * 64
FIXED_IDS = iter(
    (
        "01J63N8FV2P3XT8VZB4Z7NJ27A",
        "01J63P2AER9Z51WAV9XVVB5P8R",
        "01J63P3MYGAK26WRHHM2GPAF2D",
        "01J63P4K1T7QX4PZB6YQ2Q8FGA",
        "01J63P5M4J2N8M9R5Q2E7D6C1B",
        "01J63P6N7K4D3F2G1H8J9K5L2M",
        "01J63P7Q8W6E5R4T3Y2U1I9O0P",
        "01J63P8A9S7D6F5G4H3J2K1L0Z",
    )
)


def _next_id() -> str:
    try:
        return next(FIXED_IDS)
    except StopIteration:  # each process has a fixed upper bound in tests
        raise RuntimeError("fake review fixture exhausted deterministic IDs")


def _response_headers(headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in headers:
        result[key.decode("ascii")] = value.decode("ascii")
    return result


def _asgi_request(app: ExternalActionAPI, command: Mapping[str, Any]) -> dict[str, Any]:
    method = str(command.get("method", "GET")).upper()
    path = command.get("path")
    if not isinstance(path, str):
        raise ValueError("review request path is required")
    raw_body = command.get("body", b"")
    if isinstance(raw_body, Mapping):
        raw_body = canonical_json_bytes(raw_body)
    if isinstance(raw_body, str):
        raw_body = raw_body.encode("utf-8")
    if not isinstance(raw_body, bytes):
        raise ValueError("review request body must be bytes or an object")
    headers = {str(key).lower(): str(value) for key, value in dict(command.get("headers", {})).items()}
    identity = command.get("identity")
    if identity is not None:
        headers["x-e2e-identity"] = str(identity)
    if raw_body and "content-type" not in headers:
        headers["content-type"] = "application/json"
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "headers": [(key.encode("ascii"), value.encode("ascii")) for key, value in headers.items()],
        "client": ("127.0.0.1", 41700),
        "server": ("review-api", 443),
        "scheme": "https",
    }
    sent: list[Mapping[str, Any]] = []
    consumed = False

    async def receive() -> Mapping[str, Any]:
        nonlocal consumed
        if consumed:
            return {"type": "http.disconnect"}
        consumed = True
        return {"type": "http.request", "body": raw_body, "more_body": False}

    async def send(message: Mapping[str, Any]) -> None:
        sent.append(message)

    asyncio.run(app(scope, receive, send))
    start = next(item for item in sent if item.get("type") == "http.response.start")
    body = next(item for item in sent if item.get("type") == "http.response.body").get("body", b"")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = {"raw_body": body.decode("utf-8", "replace")}
    return {
        "status": int(start["status"]),
        "headers": _response_headers(start.get("headers", [])),
        "json": payload,
    }


def _snapshot(store: InMemoryExternalActionStore, service: ExternalActionService) -> dict[str, Any]:
    names = (
        "proposals", "payload_versions", "decisions", "executions", "outcomes", "events",
        "transition_idempotency", "submit_idempotency", "attestations", "recovery_tombstones",
        "contracts", "leases", "restore_fences",
    )
    return {
        "store": {name: copy.deepcopy(getattr(store, name)) for name in names},
        "clock": store.now(),
        "execution_epoch": store.execution_epoch,
        "execution_epoch_lease_digest": store.execution_epoch_lease_digest,
        "service_claim_idempotency": copy.deepcopy(service._claim_idempotency),
        "service_outcome_idempotency": copy.deepcopy(service._outcome_idempotency),
    }


def _restore(store: InMemoryExternalActionStore, service: ExternalActionService, snapshot: Mapping[str, Any]) -> None:
    for name, value in snapshot["store"].items():
        setattr(store, name, copy.deepcopy(value))
    store.set_now(snapshot["clock"])
    store.execution_epoch = snapshot["execution_epoch"]
    store.execution_epoch_lease_digest = snapshot["execution_epoch_lease_digest"]
    service._claim_idempotency = copy.deepcopy(snapshot["service_claim_idempotency"])
    service._outcome_idempotency = copy.deepcopy(snapshot["service_outcome_idempotency"])


def _main(ready: Connection, contract_mapping: Mapping[str, Any]) -> None:
    from okg.substrate.external_actions.action_contracts import ActionContract

    contract = ActionContract.from_mapping(contract_mapping)
    store = InMemoryExternalActionStore(
        now=FIXED_NOW,
        execution_epoch=17,
        execution_epoch_lease_digest=FIXED_LEASE_DIGEST,
        policy_digest=FIXED_POLICY_DIGEST,
    )
    service = ExternalActionService(
        store,
        action_contracts={contract.action_type: contract},
        id_factory=_next_id,
        policy_digest=FIXED_POLICY_DIGEST,
    )

    def authenticator(*, headers, **_kwargs):
        selected = next((value for key, value in headers if key == "x-e2e-identity"), None)
        if selected is None:
            raise AuthenticationError("authentication_required")
        try:
            return resolve(selected)
        except ValueError as exc:
            raise AuthenticationError("authentication_required") from exc

    config = SimpleNamespace(
        deployment=DEPLOYMENT,
        service_mode="normal",
        limits=SimpleNamespace(request_body_bytes_maximum=262_144),
    )
    app = ExternalActionAPI(
        service,
        config=config,
        authenticator=authenticator,
        action_contracts={contract.action_type: contract},
    )
    listener = ready_listener()
    ready.send(listener.address)
    snapshots: dict[str, Any] = {}

    def handle(command: Mapping[str, Any]) -> Any:
        operation = command.get("op")
        if operation == "shutdown":
            return _STOP
        if operation == "http":
            return _asgi_request(app, command)
        if operation == "clock":
            value = command.get("value")
            if not isinstance(value, str):
                raise ValueError("clock value must be an RFC3339 string")
            store.set_now(datetime.fromisoformat(value.replace("Z", "+00:00")))
            return {"now": store.now().isoformat()}
        if operation == "advance":
            return {"now": store.advance(seconds=int(command.get("seconds", 0))).isoformat()}
        if operation == "state":
            proposals = [proposal.to_projection() for proposal in store.all_proposals(deployment=DEPLOYMENT)]
            return {"counts": store.counts(), "proposals": proposals}
        if operation == "timeline":
            proposal = store.get(command["proposal_id"])
            if proposal is None:
                return {"events": []}
            return {"events": [event.to_projection() for event in store.get_events(proposal.effect_instance_key)]}
        if operation == "save_snapshot":
            snapshot_id = str(command.get("snapshot_id", "default"))
            snapshots[snapshot_id] = _snapshot(store, service)
            return {"snapshot_id": snapshot_id}
        if operation == "restore_snapshot":
            snapshot_id = str(command.get("snapshot_id", "default"))
            _restore(store, service, snapshots[snapshot_id])
            return {"snapshot_id": snapshot_id, "counts": store.counts()}
        raise ValueError("unknown review-api command")

    serve(listener, handle)
    listener.close()


class ReviewAPIProcess:
    """Process wrapper around the real ``ExternalActionAPI`` callable."""

    def __init__(self, contract_mapping: Mapping[str, Any]) -> None:
        parent, child = multiprocessing.Pipe(False)
        context = multiprocessing.get_context("spawn")
        self.process = context.Process(target=_main, args=(child, dict(contract_mapping)), name="review-api")
        self.process.daemon = True
        self.process.start()
        child.close()
        self.address = parent.recv()
        parent.close()

    def request(self, *, method: str, path: str, identity: str, body: Mapping[str, Any] | bytes | None = None, headers: Mapping[str, str] | None = None) -> Mapping[str, Any]:
        command = {"op": "http", "method": method, "path": path, "identity": identity, "headers": dict(headers or {})}
        if body is not None:
            command["body"] = body
        return call(self.address, command)

    def command(self, command: Mapping[str, Any]) -> Any:
        return call(self.address, command)

    def state(self) -> Mapping[str, Any]:
        return self.command({"op": "state"})

    def close(self) -> None:
        if self.process.is_alive():
            try:
                stop(self.address)
            finally:
                self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()


__all__ = ["DEPLOYMENT", "FIXED_NOW", "ReviewAPIProcess"]
