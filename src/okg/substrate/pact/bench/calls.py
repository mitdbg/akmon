"""In-process PACT verb surface for the benchmark.

Episode replay drives the operations/CLI entry points **in-process**: a
subprocess `okg pact ...` costs ~0.6 s warm and would blow the lane budget.
Each wrapper reproduces exactly what ``cli._dispatch`` does around the
operation — including the parts that live only in ``cli.py`` (``marked`` on
``gate task-done``, the closure-intent surfacing on ``evidence attach``,
the summary budgeting) — so what the harness measures is what an agent sees.
"""
from __future__ import annotations

import json
import re
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope as pact_envelope

# Payload byte counts must not depend on where the tmp fixture landed, so the
# fixture root is replaced by a fixed token before the payload is sized.
FIXTURE_PATH_TOKEN = "/pact-bench-fixture"


@dataclass
class CallResult:
    verb: str
    payload: dict[str, Any]
    raised: bool
    size_bytes: int
    # What it would take to make this exact call again. Metric 3 needs it: a
    # repair only counts if re-running the refused call afterwards shrinks
    # blocked_by.
    origin: dict[str, Any] = field(default_factory=dict)

    @property
    def ok_present(self) -> bool:
        return "ok" in self.payload

    @property
    def ok(self) -> bool | None:
        value = self.payload.get("ok")
        return value if isinstance(value, bool) else None

    @property
    def verdict(self) -> str | None:
        value = self.payload.get("verdict")
        return value if isinstance(value, str) else None

    @property
    def refused(self) -> bool:
        """The verb REFUSED — which is narrower than `not ok`.

        `ok is False` also covers a DEFERRAL (the subject holds, only the
        ledger write is queued). A gate instrument that reads `not ok` as
        "the gate refused" would score every deferral as a refusal.
        """
        if self.verdict is not None:
            return self.verdict == pact_envelope.REFUSED
        return self.ok is False

    @property
    def accepted(self) -> bool:
        """The verb did not refuse: recorded, or deferred on the ledger."""
        if self.verdict is not None:
            return self.verdict != pact_envelope.REFUSED
        return self.ok is True


def _prefixes(root: Path) -> list[str]:
    # Both spellings: on macOS a tmp dir is `/var/folders/...` but anything
    # that calls `Path.resolve()` stores `/private/var/folders/...`, and only
    # stripping one of them leaves a machine-length-dependent byte count.
    candidates = {str(root), str(root.resolve())}
    return sorted(candidates, key=len, reverse=True)


def normalize_payload(payload: Any, root: Path) -> Any:
    """Strip machine-specific paths so sizes and shapes are reproducible."""
    if isinstance(payload, dict):
        return {key: normalize_payload(value, root) for key, value in payload.items()}
    if isinstance(payload, list):
        return [normalize_payload(value, root) for value in payload]
    if isinstance(payload, str):
        text = payload
        for prefix in _prefixes(root):
            text = text.replace(prefix, FIXTURE_PATH_TOKEN)
        return text
    return payload


def payload_bytes(payload: Any, root: Path) -> int:
    return len(
        json.dumps(
            normalize_payload(payload, root),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    )


@dataclass
class Recorder:
    """Every envelope the harness produces, for metric 6 and metric 3.

    ``root`` is the path prefix stripped before an envelope is sized — the
    harness workdir, not necessarily the fixture repo, because payloads also
    quote the isolated HOME.
    """

    root: Path
    results: list[CallResult] = field(default_factory=list)

    def record(
        self,
        verb: str,
        payload: dict[str, Any],
        *,
        raised: bool,
        origin: dict[str, Any] | None = None,
    ) -> CallResult:
        from okg.substrate.pact.bench.isolation import active_controls

        stamped = dict(origin or {})
        controls = active_controls()
        if stamped and controls is not None:
            # The AUTHORITY ARM this call was made under. A replay has to run
            # under the same arm or it answers a different question: a
            # pin-mismatch refusal recorded with a fresh projection, replayed
            # with none, comes back with missing-projection blockers instead.
            stamped.setdefault("graph_projection", controls.graph_projection)
        result = CallResult(
            verb=verb,
            payload=normalize_payload(payload, self.root),
            raised=raised,
            size_bytes=payload_bytes(payload, self.root),
            origin=stamped,
        )
        self.results.append(result)
        return result

    def call(
        self,
        verb: str,
        fn: Any,
        *args: Any,
        origin: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> CallResult:
        try:
            payload = fn(*args, **kwargs)
        except SubstrateError as err:
            # This is the shape the CLI prints (cli.py:703-716): the refusal
            # envelope, with the `error` block preserved as detail.
            return self.record(
                verb, pact_envelope.from_error(err), raised=True, origin=origin
            )
        return self.record(verb, payload, raised=False, origin=origin)


# --- verb wrappers ---------------------------------------------------------


def view_bootstrap(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import cli as pact_cli

    def _run() -> dict[str, Any]:
        return pact_cli._bootstrap_summary(pact_cli._agent_bootstrap(root, change_id))

    return recorder.call(
        "view --format=bootstrap",
        _run,
        origin={"verb": "view_bootstrap", "change": change_id},
    )


def view_status(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import cli as pact_cli

    return recorder.call(
        "view --format=status",
        pact_cli._status,
        root,
        change_id,
        origin={"verb": "view_status", "change": change_id},
    )


def view_next(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import cli as pact_cli

    return recorder.call(
        "view --format=next",
        pact_cli._next,
        root,
        change_id,
        origin={"verb": "view_next", "change": change_id},
    )


def view_brief(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import cli as pact_cli

    return recorder.call(
        "view --format=brief",
        pact_cli._brief,
        root,
        change_id,
        origin={"verb": "view_brief", "change": change_id},
    )


def view_markdown(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact.markdown_view import write_markdown_view

    def _run() -> dict[str, Any]:
        path, digest = write_markdown_view(root, change_id)
        return pact_envelope.recorded(
            text=f"rendered {change_id} review view to {path}",
            change=change_id,
            format="markdown",
            path=str(path),
            source_digest=digest,
        )

    return recorder.call(
        "view --format=markdown",
        _run,
        origin={"verb": "view_markdown", "change": change_id},
    )


def evidence_status(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import operations

    return recorder.call(
        "evidence status",
        operations.evidence_status,
        root,
        change_id,
        origin={"verb": "evidence_status", "change": change_id},
    )


def gate_post_edit(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import operations

    return recorder.call(
        "gate post-edit --full",
        operations.post_edit_check,
        root,
        change_id,
        origin={"verb": "gate_post_edit", "change": change_id},
    )


def gate_post_edit_summary(recorder: Recorder, root: Path, change_id: str) -> CallResult:
    from okg.substrate.pact import cli as pact_cli
    from okg.substrate.pact import operations

    def _run() -> dict[str, Any]:
        return pact_cli._post_edit_summary(operations.post_edit_check(root, change_id))

    return recorder.call(
        "gate post-edit",
        _run,
        origin={"verb": "gate_post_edit_summary", "change": change_id},
    )


def gate_task_done(
    recorder: Recorder,
    root: Path,
    change_id: str,
    task_id: str,
) -> CallResult:
    from okg.substrate.pact import cli as pact_cli
    from okg.substrate.pact import operations

    def _run() -> dict[str, Any]:
        # Mirrors cli.py:991-1012 — `marked` and `closure_intent` are added by
        # the CLI, not by the operation, and the audited lie class is stated
        # in terms of them.
        result = operations.task_completion_gate(root, change_id, task_id)
        if result["ok"]:
            if result["task_status"] != "done":
                operations.update_task_status(root, change_id, task_id, status="done")
                result["task_status"] = "done"
            result["marked"] = True
        else:
            result["marked"] = False
            deferred = operations.maybe_defer_task_done(
                root, change_id, task_id, result
            )
            if deferred is not None:
                result["closure_intent"] = deferred
                commands = pact_cli._deferred_closure_commands(result, deferred)
                result["recommended_commands"] = commands
                result = pact_envelope.restate(
                    result,
                    pact_envelope.DEFERRED,
                    text=(
                        f"task-done {change_id} {task_id}: deferred "
                        "(closure intent queued; ledger unreachable)"
                    ),
                    message=(
                        str(deferred.get("reason") or "decision ledger unreachable")
                    ),
                    blocked_by=pact_envelope.tokens(
                        result.get("blocked_by"),
                        (result.get("workflow_authority") or {}).get("blockers")
                        or ["ledger_not_fresh"],
                    ),
                    next_commands=commands,
                    why=pact_cli._deferred_closure_why(result, deferred),
                )
        return result

    return recorder.call(
        "gate task-done",
        _run,
        origin={"verb": "gate_task_done", "change": change_id, "task": task_id},
    )


def evidence_sync(
    recorder: Recorder,
    root: Path,
    change_id: str | None,
) -> CallResult:
    """`okg pact evidence sync` — cli.py:931-934.

    The verb the deferred-closure path recommends (cli.py:1010). Its `change`
    is optional on the real parser, so ``None`` replays every queued intent.
    """
    from okg.substrate.pact.operations import sync_closure_intents

    return recorder.call(
        "evidence sync",
        sync_closure_intents,
        root,
        change_id,
        origin={"verb": "evidence_sync", "change": change_id},
    )


def decide(
    recorder: Recorder,
    root: Path,
    change_id: str,
    *,
    verdict: str,
    note: str | None = None,
    target: str | None = None,
    escalate_to: str | None = None,
    principal: str | None = None,
    explicit_dsn: str | None = None,
) -> CallResult:
    """`okg pact decide` — cli.py:894-904 (the non-`--all-stale` arm)."""
    from okg.substrate.pact.review import decide_change

    return recorder.call(
        "decide",
        decide_change,
        root,
        change_id,
        verdict=verdict,
        note=note,
        target=target,
        escalate_to=escalate_to,
        principal=principal,
        explicit_dsn=explicit_dsn,
        origin={
            "verb": "decide",
            "change": change_id,
            "kwargs": {
                "verdict": verdict,
                "note": note,
                "target": target,
                "escalate_to": escalate_to,
                "principal": principal,
                "explicit_dsn": explicit_dsn,
            },
        },
    )


def evidence_waive(
    recorder: Recorder,
    root: Path,
    change_id: str,
    *,
    scope: str,
    reason: str,
    waiver_id: str | None = None,
    owner: str | None = None,
    expires: str | None = None,
) -> CallResult:
    """`okg pact evidence waive` — cli.py:954-964."""
    from okg.substrate.pact import operations

    def _run() -> dict[str, Any]:
        manifest = operations.request_waiver(
            root,
            change_id,
            waiver_id=waiver_id,
            scope=scope,
            reason=reason,
            owner=owner,
            expires=expires,
        )
        return pact_envelope.recorded(
            text=f"waiver recorded on {manifest.change.id}",
            change=manifest.change.id,
            waivers=len(manifest.waivers),
        )

    return recorder.call(
        "evidence waive",
        _run,
        origin={
            "verb": "evidence_waive",
            "change": change_id,
            "kwargs": {
                "scope": scope,
                "reason": reason,
                "waiver_id": waiver_id,
                "owner": owner,
                "expires": expires,
            },
        },
    )


def evidence_attach(
    recorder: Recorder,
    root: Path,
    change_id: str,
    target: str,
    *,
    kind: str,
    ref: str,
    status: str = "pass",
    command: str | None = None,
    summary: str | None = None,
    generation_id: int | None = None,
    data: dict[str, Any] | None = None,
) -> CallResult:
    from okg.substrate.pact import cli as pact_cli
    from okg.substrate.pact import operations

    def _run() -> dict[str, Any]:
        # Mirrors cli.py:936-953 exactly, including the two arguments an
        # earlier wrapper dropped (`--generation-id`, `--data-json`).
        manifest = operations.attach_evidence(
            root,
            change_id,
            target,
            kind=kind,
            ref=ref,
            status=status,
            command=command,
            summary=summary,
            generation_id=generation_id,
            data=data,
        )
        return pact_cli._with_closure_intents(
            root,
            change_id,
            pact_envelope.recorded(
                text=f"attached evidence to {manifest.change.id}",
                change=manifest.change.id,
                evidence=len(manifest.evidence),
            ),
        )

    return recorder.call(
        "evidence attach",
        _run,
        origin={
            "verb": "evidence_attach",
            "change": change_id,
            "task": target,
            # The full argument set, so a BAD-ARGUMENT refusal can be
            # re-invoked with the one token corrected. Without it the harness
            # could not tell "the repair is a corrected re-invocation" from
            # "there is no repair".
            "kwargs": {
                "target": target,
                "kind": kind,
                "ref": ref,
                "status": status,
                "command": command,
                "summary": summary,
                "generation_id": generation_id,
                "data": data,
            },
        },
    )


def new_change(
    recorder: Recorder,
    root: Path,
    change_id: str,
    *,
    title: str | None = None,
    intent: str | None = None,
    acceptance: str | None = None,
    task_title: str | None = None,
    test_id: str | None = None,
    test_command: str | None = None,
) -> CallResult:
    """`okg pact new` (the default fast shape) — cli.py:767-804.

    Every default the CLI supplies is reproduced here, and ``pact_shape`` is
    READ OFF THE MANIFEST rather than restated: a literal "fast" here made the
    harness create a manifest shape the real verb never produces.
    """
    from okg.substrate.pact import operations

    slug = re.sub(r"[^A-Za-z0-9]+", "-", change_id.lower()).strip("-")
    resolved_title = title or change_id.replace("-", " ")
    resolved_intent = intent or (
        f"TODO: state the requirement — what must hold once {change_id} lands."
    )
    resolved_acceptance = acceptance or (
        "TODO: the observable check that proves the requirement."
    )
    resolved_test_id = test_id or (
        f"tests/TODO/test_{slug.replace('-', '_')}.py::"
        f"test_{slug.replace('-', '_')}"
    )

    def _run() -> dict[str, Any]:
        from okg.substrate.pact import cli as pact_cli

        manifest = operations.create_fast_path_change(
            root,
            change_id,
            title=resolved_title,
            intent=resolved_intent,
            acceptance=resolved_acceptance,
            task_title=task_title,
            test_id=resolved_test_id,
            test_command=test_command,
        )
        return pact_envelope.recorded(
            text=f"created {manifest.change.id} (fast shape)",
            change=manifest.change.id,
            path=str(root / "pact" / "changes" / manifest.change.id / "pact.yaml"),
            pact_shape=(
                manifest.change.method_metadata.get("pact_shape")
                or manifest.prose.get("pact_shape")
            ),
            requirements=len(manifest.requirements),
            tasks=len(manifest.tasks),
            test_plan=len(manifest.test_plan),
            state="draft",
            next_commands=[
                f"{pact_cli._repo_pact_prefix(root)} pact gate post-edit "
                f"{shlex.quote(manifest.change.id)} --json"
            ],
            next_action={
                "command": (
                    f"{pact_cli._repo_pact_prefix(root)} pact gate post-edit "
                    f"{shlex.quote(manifest.change.id)} --json"
                ),
                "owner": "agent",
                "why": "admit the new PACT before implementation",
            },
        )

    return recorder.call("new", _run, origin={"verb": "new", "change": change_id})


REPLAYABLE = {
    "view_bootstrap": lambda rec, root, origin: view_bootstrap(
        rec, root, origin["change"]
    ),
    "view_status": lambda rec, root, origin: view_status(rec, root, origin["change"]),
    "view_next": lambda rec, root, origin: view_next(rec, root, origin["change"]),
    "view_brief": lambda rec, root, origin: view_brief(rec, root, origin["change"]),
    "view_markdown": lambda rec, root, origin: view_markdown(
        rec, root, origin["change"]
    ),
    "evidence_status": lambda rec, root, origin: evidence_status(
        rec, root, origin["change"]
    ),
    "gate_post_edit": lambda rec, root, origin: gate_post_edit(
        rec, root, origin["change"]
    ),
    "gate_post_edit_summary": lambda rec, root, origin: gate_post_edit_summary(
        rec, root, origin["change"]
    ),
    "gate_task_done": lambda rec, root, origin: gate_task_done(
        rec, root, origin["change"], origin["task"]
    ),
    "evidence_sync": lambda rec, root, origin: evidence_sync(
        rec, root, origin["change"]
    ),
    "decide": lambda rec, root, origin: decide(
        rec, root, origin["change"], **origin["kwargs"]
    ),
    "evidence_waive": lambda rec, root, origin: evidence_waive(
        rec, root, origin["change"], **origin["kwargs"]
    ),
    "evidence_attach": lambda rec, root, origin: evidence_attach(
        rec,
        root,
        origin["change"],
        origin["kwargs"]["target"],
        **{k: v for k, v in origin["kwargs"].items() if k != "target"},
    ),
}


def replay(recorder: Recorder, root: Path, origin: dict[str, Any]) -> CallResult | None:
    """Re-run a recorded call against another copy of the fixture."""
    runner = REPLAYABLE.get(str(origin.get("verb")))
    if runner is None:
        return None
    return runner(recorder, root, origin)
