"""Separate-process fake worker for the external-action review contract."""

from __future__ import annotations

import multiprocessing
from multiprocessing.connection import Connection
from typing import Any, Mapping

from okg.substrate.external_actions.action_contracts import digest_value, strict_json_loads

from .fake_worker_journal import FakeWorkerJournal
from .rpc import _STOP, call, ready_listener, serve, stop


def _journal_digest(journal: FakeWorkerJournal) -> str:
    return journal.digest


def _main(ready: Connection, review_address: Any, provider_address: Any, ledger_address: Any, contract_mapping: Mapping[str, Any], worker_identity: str) -> None:
    journal = FakeWorkerJournal()
    pending: dict[str, Any] = {}
    claim_checkpoints: dict[tuple[str, str], dict[str, Any]] = {}

    def review(command: Mapping[str, Any]) -> Mapping[str, Any]:
        return call(review_address, command)

    def http(method: str, path: str, *, body: Mapping[str, Any] | None = None, headers: Mapping[str, str] | None = None) -> Mapping[str, Any]:
        command = {
            "op": "http", "method": method, "path": path, "identity": worker_identity,
            "headers": dict(headers or {}),
        }
        if body is not None:
            command["body"] = body
        return review(command)

    def claim_request(proposal_id: str, idempotency_key: str) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        checkpoint = claim_checkpoints.get((proposal_id, idempotency_key))
        if checkpoint is not None:
            response = http(
                "POST",
                f"/v1/deployments/support-prod/external-actions/{proposal_id}/execution-claims",
                body=checkpoint["support"],
                headers={"Idempotency-Key": idempotency_key, "If-Match": checkpoint["etag"]},
            )
            return response, checkpoint
        detail = http("GET", f"/v1/deployments/support-prod/external-actions/{proposal_id}")
        if detail["status"] != 200:
            return detail, {}
        value = detail["json"]
        support = {
            "decision_id": value["decision_id"],
            "approved_lifecycle_version": value["lifecycle_version"],
            "approved_payload_version": value["payload_version"],
            "approved_payload_hash": value["current_payload_hash"],
            "supported_action_schema_digest": value["action_schema_digest"],
            "supported_action_contract_digest": value["action_contract_digest"],
            "supported_executor_contract_digest": value["executor_contract_digest"],
            "supported_payload_renderer_digest": value["payload_renderer_digest"],
            "supported_governed_tool_binding_digest": value["governed_tool_binding_digest"],
            "supported_provider_connection_contract_digest": value["provider_connection_contract_digest"],
            "supported_provider_capability_digest": value["provider_capability_digest"],
            "supported_provider_operation_id": value["provider_operation_id"],
            "supported_provider_operation_descriptor_digest": value["provider_operation_descriptor_digest"],
        }
        response = http(
            "POST",
            f"/v1/deployments/support-prod/external-actions/{proposal_id}/execution-claims",
            body=support,
            headers={"Idempotency-Key": idempotency_key, "If-Match": value["etag"]},
        )
        checkpoint = {"detail": value, "support": support, "etag": value["etag"]}
        claim_checkpoints[(proposal_id, idempotency_key)] = checkpoint
        return response, checkpoint

    last_report: dict[str, Any] | None = None

    def report(proposal_id: str, execution_id: str, idempotency_key: str, *, outcome: str, reason: str, receipt: str | None) -> Mapping[str, Any]:
        nonlocal last_report
        current = http("GET", f"/v1/deployments/support-prod/external-actions/{proposal_id}")
        if current["status"] != 200:
            return current
        body = {
            "outcome": outcome,
            "outcome_reason_code": reason,
            "provider_receipt_digest": receipt,
            "recovery_journal_digest": _journal_digest(journal),
        }
        request = {
            "method": "POST",
            "path": f"/v1/deployments/support-prod/external-actions/{proposal_id}/execution-claims/{execution_id}/outcome",
            "body": body,
            "headers": {"Idempotency-Key": idempotency_key, "If-Match": current["json"]["etag"]},
        }
        last_report = dict(request)
        return http(
            "POST",
            request["path"],
            body=body,
            headers=request["headers"],
        )

    def run_after_claim(proposal_id: str, claim: Mapping[str, Any], *, provider_mode: str, pause_before_ledger: bool = False, detail: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        execution_id = claim["execution_id"]
        canonical = claim.get("canonical_payload")
        if not isinstance(canonical, str):
            return {"status": 503, "code": "claim_payload_missing"}
        try:
            parsed = strict_json_loads(canonical.encode("utf-8"))
            computed_hash = digest_value("okg-external-action-payload-v1", parsed)
        except Exception:
            return {"status": 503, "code": "claim_payload_invalid"}
        if computed_hash != claim["approved_payload_hash"]:
            return {"status": 503, "code": "claim_payload_hash_mismatch"}
        effect_instance_key = claim.get("effect_instance_key") or (detail or {}).get("effect_instance_key")
        if not isinstance(effect_instance_key, str):
            return {"status": 503, "code": "claim_effect_binding_missing"}
        journal.append(
            "effect_starting",
            proposal_id=proposal_id,
            execution_id=execution_id,
            effect_instance_key=effect_instance_key,
            approved_payload_hash=claim["approved_payload_hash"],
            execution_binding_hash=claim["execution_binding_hash"],
        )
        if pause_before_ledger:
            pending[proposal_id] = {"claim": dict(claim), "detail": dict(detail or {}), "journal": journal.snapshot()}
            return {"status": 202, "paused": "before_ledger", "claim": dict(claim)}
        ledger_entry = {
            "effect_instance_key": effect_instance_key,
            "execution_id": execution_id,
            "execution_epoch": claim["execution_epoch"],
            "approved_payload_hash": claim["approved_payload_hash"],
            "execution_binding_hash": claim["execution_binding_hash"],
        }
        ledger_result = call(ledger_address, {"op": "begin", "entry": ledger_entry})
        if not ledger_result.get("accepted"):
            report_result = report(
                proposal_id,
                execution_id,
                "worker-outcome-pre-effect-417",
                outcome="failed",
                reason="worker_pre_effect_failure",
                receipt=None,
            )
            return {"claim": dict(claim), "ledger": ledger_result, "report": report_result, "provider_calls": 0}
        if ledger_result.get("replay"):
            return {"claim": dict(claim), "ledger": ledger_result, "provider_calls": 0}
        provider_result = call(
            provider_address,
            {
                "op": "invoke",
                "request": {
                    "operation_id": claim["provider_operation_id"],
                    "method": "PATCH",
                    "target_ref": claim["target_ref"],
                    "media_type": "application/json",
                    "body": canonical,
                    "idempotency_key": claim["provider_idempotency_key"],
                },
            },
        )
        if provider_mode == "response_loss" or provider_result.get("response_lost"):
            journal.append("provider_accepted_response_lost", execution_id=execution_id)
            outcome, reason, receipt = "indeterminate", "provider_receipt_missing", None
        elif provider_result.get("accepted") is True:
            receipt = provider_result.get("receipt_digest")
            journal.append("provider_receipt", execution_id=execution_id, receipt_digest=receipt)
            outcome, reason = "succeeded", "provider_receipt_observed"
        else:
            receipt = provider_result.get("receipt_digest")
            journal.append("provider_explicit_failure", execution_id=execution_id, receipt_digest=receipt)
            outcome, reason = "failed", "provider_explicit_failure"
        report_result = report(
            proposal_id,
            execution_id,
            "worker-outcome-417",
            outcome=outcome,
            reason=reason,
            receipt=receipt,
        )
        return {
            "claim": dict(claim),
            "ledger": ledger_result,
            "provider": provider_result,
            "report": report_result,
            "provider_calls": 1,
            "journal": journal.snapshot(),
        }

    def handle(command: Mapping[str, Any]) -> Any:
        operation = command.get("op")
        if operation == "shutdown":
            return _STOP
        if operation == "snapshot":
            return journal.snapshot()
        if operation == "duplicate_outcome":
            if last_report is None:
                return {"status": 404, "code": "worker_outcome_checkpoint_missing"}
            return review({"op": "http", "identity": worker_identity, **last_report})
        if operation == "execute":
            proposal_id = str(command["proposal_id"])
            idempotency_key = str(command.get("idempotency_key", "worker-claim-417"))
            claim_response, context = claim_request(proposal_id, idempotency_key)
            if claim_response["status"] not in {200, 201}:
                return {"claim": claim_response, "provider_calls": 0}
            claim = claim_response["json"]
            result = run_after_claim(
                proposal_id,
                claim,
                provider_mode=str(command.get("provider_mode", "success")),
                pause_before_ledger=bool(command.get("pause_before_ledger", False)),
                detail=context.get("detail"),
            )
            result["claim_response"] = claim_response
            result["claim_context"] = context
            return result
        if operation == "resume":
            proposal_id = str(command["proposal_id"])
            existing = pending.pop(proposal_id, None)
            if existing is None:
                return {"status": 404, "code": "worker_checkpoint_missing", "provider_calls": 0}
            return run_after_claim(proposal_id, existing["claim"], provider_mode=str(command.get("provider_mode", "success")), detail=existing.get("detail"))
        raise ValueError("unknown fake-worker command")

    listener = ready_listener()
    ready.send(listener.address)
    serve(listener, handle)
    listener.close()


class FakeWorkerProcess:
    """Worker process that can reach only the review API, ledger, provider."""

    def __init__(self, *, review_address: Any, provider_address: Any, ledger_address: Any, contract_mapping: Mapping[str, Any], identity: str = "worker") -> None:
        parent, child = multiprocessing.Pipe(False)
        context = multiprocessing.get_context("spawn")
        self.process = context.Process(
            target=_main,
            args=(child, review_address, provider_address, ledger_address, dict(contract_mapping), identity),
            name="fake-worker",
        )
        self.process.daemon = True
        self.process.start()
        child.close()
        self.address = parent.recv()
        parent.close()

    def command(self, command: Mapping[str, Any]) -> Any:
        return call(self.address, command)

    def execute(self, proposal_id: str, **kwargs: Any) -> Mapping[str, Any]:
        return self.command({"op": "execute", "proposal_id": proposal_id, **kwargs})

    def resume(self, proposal_id: str, **kwargs: Any) -> Mapping[str, Any]:
        return self.command({"op": "resume", "proposal_id": proposal_id, **kwargs})

    def close(self) -> None:
        if self.process.is_alive():
            try:
                stop(self.address)
            finally:
                self.process.join(timeout=5)
        if self.process.is_alive():
            self.process.terminate()


__all__ = ["FakeWorkerProcess"]
