"""Drift detector: every bench wrapper must equal what `cli._dispatch` returns.

The harness measures PACT by calling operations in-process rather than by
spawning `okg pact ...`, because a subprocess costs ~0.6 s warm. That trade is
only sound while each wrapper reproduces the CLI arm it stands for. Nothing
enforced that before: a wrapper silently dropped `--generation-id` and
`--data-json` (cli.py:936-953), and another passed a literal `"fast"` where the
real verb reads `pact_shape` off the manifest (cli.py:797-800). Both made the
harness measure a CLI that does not exist.

So: for EVERY wrapped verb, build the `argparse.Namespace` the real parser
would produce, run `cli._dispatch` against one copy of the fixture, run the
bench wrapper against an identical copy, and require the payloads to match.
Two copies, because these verbs mutate the manifest.
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any, Callable

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact.bench import calls
from okg.substrate.pact.bench.calls import Recorder, normalize_payload
from okg.substrate.pact.bench.isolation import (
    RuntimeControls,
    fixture_root_redirected,
    isolated_runtime,
)
from okg.substrate.pact.bench.runner import BenchResult

# change ids seeded by the benchmark fixture, reused here as subjects
PROVEN = "bench-honesty-proven"
UNPROVEN = "bench-honesty-unproven"
ATTACH = "bench-honesty-attach"

# Defaults for every attribute `_dispatch` may read on a pact Namespace, so a
# case only has to state what it actually varies.
_NS_DEFAULTS: dict[str, Any] = {
    "root": None,
    "pact_subcmd": None,
    "evidence_subcmd": None,
    "gate_subcmd": None,
    "change": None,
    "format": None,
    "full": False,
    "graph": False,
    "status": None,
    "query": None,
    "for_agent": False,
    "reviewer_kind": None,
    "task": None,
    "target": None,
    "kind": None,
    "ref": None,
    "evidence_command": None,
    "summary": None,
    "generation_id": None,
    "data_json": None,
    "id": None,
    "scope": None,
    "reason": None,
    "owner": None,
    "expires": None,
    "verdict": None,
    "note": None,
    "escalate_to": None,
    "principal": None,
    "dsn": None,
    "all_stale": False,
    "dry_run": False,
}


def _namespace(root: Path, **overrides: Any) -> argparse.Namespace:
    values = dict(_NS_DEFAULTS)
    values.update(overrides)
    values["root"] = str(root)
    return argparse.Namespace(**values)


# (id, namespace-builder, bench-wrapper) for every verb the harness wraps.
# `commands.EXECUTABLE_VERBS` is asserted to be covered by this table below.
PARITY_CASES: tuple[tuple[str, Callable[[Path], argparse.Namespace], Callable[[Recorder, Path], Any]], ...] = (
    (
        "view --format=status",
        lambda root: _namespace(root, pact_subcmd="view", format="status", change=PROVEN),
        lambda rec, root: calls.view_status(rec, root, PROVEN),
    ),
    (
        "view --format=next",
        lambda root: _namespace(root, pact_subcmd="view", format="next", change=PROVEN),
        lambda rec, root: calls.view_next(rec, root, PROVEN),
    ),
    (
        "view --format=brief",
        lambda root: _namespace(root, pact_subcmd="view", format="brief", change=PROVEN),
        lambda rec, root: calls.view_brief(rec, root, PROVEN),
    ),
    (
        "view --format=markdown",
        lambda root: _namespace(root, pact_subcmd="view", format="markdown", change=PROVEN),
        lambda rec, root: calls.view_markdown(rec, root, PROVEN),
    ),
    (
        # The BUDGETED arm (no --full) — the one an agent actually gets.
        "view --format=bootstrap",
        lambda root: _namespace(root, pact_subcmd="view", format="bootstrap", change=PROVEN),
        lambda rec, root: calls.view_bootstrap(rec, root, PROVEN),
    ),
    (
        "evidence status",
        lambda root: _namespace(root, pact_subcmd="evidence", evidence_subcmd="status", change=PROVEN),
        lambda rec, root: calls.evidence_status(rec, root, PROVEN),
    ),
    (
        "evidence sync",
        lambda root: _namespace(root, pact_subcmd="evidence", evidence_subcmd="sync", change=PROVEN),
        lambda rec, root: calls.evidence_sync(rec, root, PROVEN),
    ),
    (
        # `change` is nargs="?" on the real parser: a bare sync replays every
        # queued intent.
        "evidence sync (no change)",
        lambda root: _namespace(root, pact_subcmd="evidence", evidence_subcmd="sync", change=None),
        lambda rec, root: calls.evidence_sync(rec, root, None),
    ),
    (
        "gate post-edit",
        lambda root: _namespace(root, pact_subcmd="gate", gate_subcmd="post-edit", change=UNPROVEN),
        lambda rec, root: calls.gate_post_edit_summary(rec, root, UNPROVEN),
    ),
    (
        "gate post-edit --full",
        lambda root: _namespace(root, pact_subcmd="gate", gate_subcmd="post-edit", change=UNPROVEN, full=True),
        lambda rec, root: calls.gate_post_edit(rec, root, UNPROVEN),
    ),
    (
        "gate task-done",
        lambda root: _namespace(root, pact_subcmd="gate", gate_subcmd="task-done", change=PROVEN, task="task.one"),
        lambda rec, root: calls.gate_task_done(rec, root, PROVEN, "task.one"),
    ),
    (
        # The unknown-task arm: `_dispatch` raises, the wrapper records the
        # error envelope. Parity must hold on the refusal too.
        "gate task-done (unknown task)",
        lambda root: _namespace(root, pact_subcmd="gate", gate_subcmd="task-done", change=PROVEN, task="task.nope"),
        lambda rec, root: calls.gate_task_done(rec, root, PROVEN, "task.nope"),
    ),
    (
        # Carries BOTH arguments an earlier wrapper dropped.
        "evidence attach",
        lambda root: _namespace(
            root,
            pact_subcmd="evidence",
            evidence_subcmd="attach",
            change=ATTACH,
            target="req.one",
            kind="executed_test",
            ref="tests/bench/test_probe.py",
            status="pass",
            evidence_command="uv run pytest tests/bench/test_probe.py -q",
            summary="parity probe",
            generation_id=4242,
            data_json='{"rows": 1}',
        ),
        lambda rec, root: calls.evidence_attach(
            rec,
            root,
            ATTACH,
            "req.one",
            kind="executed_test",
            ref="tests/bench/test_probe.py",
            status="pass",
            command="uv run pytest tests/bench/test_probe.py -q",
            summary="parity probe",
            generation_id=4242,
            data={"rows": 1},
        ),
    ),
    (
        "evidence waive",
        lambda root: _namespace(
            root,
            pact_subcmd="evidence",
            evidence_subcmd="waive",
            change=UNPROVEN,
            id="wv.parity",
            scope="req.one",
            reason="parity probe",
            owner="bench",
            expires=None,
        ),
        lambda rec, root: calls.evidence_waive(
            rec,
            root,
            UNPROVEN,
            waiver_id="wv.parity",
            scope="req.one",
            reason="parity probe",
            owner="bench",
            expires=None,
        ),
    ),
    (
        "decide",
        lambda root: _namespace(
            root,
            pact_subcmd="decide",
            change=PROVEN,
            verdict="approve",
            note="parity probe",
        ),
        lambda rec, root: calls.decide(
            rec, root, PROVEN, verdict="approve", note="parity probe"
        ),
    ),
    (
        "new",
        lambda root: _namespace(
            root,
            pact_subcmd="new",
            change="bench-parity-new",
            shape=None,
            template=None,
            title=None,
            intent=None,
            acceptance=None,
            task_title=None,
            test_id=None,
            test_command=None,
        ),
        lambda rec, root: calls.new_change(rec, root, "bench-parity-new"),
    ),
)


def _parses_as_command(text: str) -> bool:
    """The command contract, restated — against the REAL guard constants.

    The expected value stays hand-built (calling `repair.executable_commands`
    would make the arm tautological), but the numbers and prefixes are
    IMPORTED: hand-copied `500`, `2`, and the prefix list agreed with
    `repair.py` on the day they were written and had nothing keeping them in
    agreement, so a guard change would have left this test asserting the old
    contract while the emitter enforced the new one.
    """
    import shlex

    from okg.substrate.pact import repair

    candidate = text.strip()
    if not candidate or "#" in candidate:
        return False
    if len(candidate) > repair.MAX_COMMAND_CHARS:
        return False
    separators = sum(
        candidate.count(token) for token in repair.STATEMENT_SEPARATORS
    )
    if separators > repair.MAX_STATEMENT_SEPARATORS:
        return False
    try:
        argv = shlex.split(candidate)
    except ValueError:
        return False
    for prefix in repair.COMMAND_PREFIXES:
        if argv[: len(prefix)] == prefix and len(argv) > len(prefix):
            return True
    return False


def _expected_refusal(err: SubstrateError) -> dict[str, Any]:
    """The refusal envelope `cmd_pact` prints for a typed error, spelled out.

    Deliberately NOT `envelope.from_error(err)`: calling the system under test
    to build the expected value makes the error arm tautological — a wrapper
    and a dispatch that both regressed identically would still agree. This is
    the contract restated as literals.
    """
    # The cause token: the subclass wire code, or — when the raise site used
    # the base class, whose code is the undiscriminating `substrate_error` —
    # the first `data` discriminator.
    token = err.code
    if token == "substrate_error":
        token = next(
            (
                str(err.data[key]).strip()
                for key in ("cause", "reason", "code", "status")
                if isinstance(err.data.get(key), str) and err.data[key].strip()
            ),
            "substrate_error",
        )
    # `next_commands` publishes only strings that PARSE as a command and fit
    # the repo guards (pact-self-repairing-refusals req.refusal.executable).
    # A prose fix_hint is still shown — as `why` — but never as something an
    # agent is told to run. Restated here as literals, same as the rest.
    candidates = [*(err.data.get("next_commands") or []), err.fix_hint]
    commands: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        if _parses_as_command(candidate) and candidate not in commands:
            commands.append(candidate)
    why = err.fix_hint if err.fix_hint and err.fix_hint not in commands else None
    return {
        "verdict": "refused",
        "ok": False,
        "blocked_by": [token],
        "next_commands": commands,
        "text": f"refused [{err.code}]: {err.message}",
        "message": err.message,
        **({"why": why} if why else {}),
        "error": {
            "code": err.code,
            "message": err.message,
            **({"fix_hint": err.fix_hint} if err.fix_hint else {}),
            "data": err.data,
        },
    }


def _dispatch_payload(root: Path, args: argparse.Namespace) -> dict[str, Any]:
    """Run the real CLI arm, normalizing a raise the way `cmd_pact` does."""
    try:
        payload = pact_cli._dispatch(args)
    except SubstrateError as err:
        # Mirrors cli.cmd_pact: a typed error is printed as the REFUSAL
        # envelope, not as a bare `{"error": {...}}` with no `ok` key.
        payload = _expected_refusal(err)
    return normalize_payload(payload, root)


@pytest.fixture(scope="module")
def parity_root(bench_result: BenchResult, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A pristine copy of the fixture repo, taken once for this module."""
    pristine = tmp_path_factory.mktemp("parity-pristine") / "repo"
    shutil.copytree(bench_result.fixture_root, pristine, symlinks=True)
    return pristine


@pytest.mark.parametrize(
    ("label", "build_args", "wrapper"),
    PARITY_CASES,
    ids=[case[0] for case in PARITY_CASES],
)
def test_bench_wrapper_matches_cli_dispatch(
    label: str,
    build_args: Callable[[Path], argparse.Namespace],
    wrapper: Callable[[Recorder, Path], Any],
    parity_root: Path,
    tmp_path: Path,
) -> None:
    # Both arms run at the SAME absolute path, with the tree reset in between.
    # Several of these verbs write the manifest, so they cannot share a tree;
    # but they also cannot use different paths, because a closure intent's id
    # is a digest over a payload that quotes the repo root
    # (closure_intents.py:51-64) — two paths would differ for a reason that is
    # not drift. Same path, fresh tree: any remaining difference is real.
    work = tmp_path / "repo"

    def _reset() -> None:
        if work.exists():
            shutil.rmtree(work)
        shutil.copytree(parity_root, work, symlinks=True)

    _reset()
    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        expected = _dispatch_payload(work, build_args(work))
    _reset()
    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        observed = wrapper(Recorder(root=work), work).payload

    assert observed == expected, label


def test_parity_does_not_consult_live_ledger(
    parity_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A live ledger change between arms cannot affect the parity result."""
    from okg.substrate.pact import decisions

    live_state = {"version": 1}
    live_reads: list[int] = []

    def _live_resolver(**_kwargs: Any) -> dict[str, Any]:
        live_reads.append(live_state["version"])
        raise AssertionError("parity consulted the live decision ledger")

    monkeypatch.setattr(decisions, "resolve_ledger_dsn", _live_resolver)
    work = tmp_path / "repo"
    shutil.copytree(parity_root, work, symlinks=True)
    args = _namespace(work, pact_subcmd="decide", change=PROVEN, verdict="approve")

    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        expected = _dispatch_payload(work, args)
    live_state["version"] = 2
    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        observed = calls.decide(Recorder(root=work), work, PROVEN, verdict="approve").payload

    assert observed == expected
    assert live_reads == []


def test_bootstrap_parity_ignores_live_authority(
    parity_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bootstrap's suggested action stays bound to the fixture authority."""
    from okg.substrate.pact import decisions

    live_state = {"version": 1}
    live_reads: list[int] = []

    def _live_resolver(**_kwargs: Any) -> dict[str, Any]:
        live_reads.append(live_state["version"])
        raise AssertionError("bootstrap consulted the live decision ledger")

    monkeypatch.setattr(decisions, "resolve_ledger_dsn", _live_resolver)
    work = tmp_path / "fixture"
    live = tmp_path / "live"
    shutil.copytree(parity_root, work, symlinks=True)
    shutil.copytree(parity_root, live, symlinks=True)
    live_manifest = live / "pact" / "changes" / PROVEN / "pact.yaml"
    live_manifest.write_text(
        live_manifest.read_text(encoding="utf-8") + "\n# concurrent live write\n",
        encoding="utf-8",
    )
    args = _namespace(work, pact_subcmd="view", format="bootstrap", change=PROVEN)

    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        expected = _dispatch_payload(work, args)
    live_state["version"] = 2
    monkeypatch.chdir(live)
    with isolated_runtime(RuntimeControls(fixture_root=work)), fixture_root_redirected(work):
        observed = calls.view_bootstrap(Recorder(root=work), work, PROVEN).payload

    assert observed == expected
    assert observed["next_action"] == expected["next_action"]
    assert live_reads == []


def test_every_executable_verb_is_covered_by_the_parity_table() -> None:
    """A verb the harness claims it can execute must be pinned to the CLI.

    Without this, wiring a new verb into `EXECUTABLE_VERBS` would let the
    harness score refusals through an unverified wrapper.
    """
    from okg.substrate.pact.bench import commands

    covered = {label.split(" (")[0] for label, _args, _wrapper in PARITY_CASES}
    # `view` is one parser verb but many surfaces; the table pins each format.
    view_formats = {
        label.split("=", 1)[1] for label in covered if label.startswith("view --format=")
    }
    assert view_formats == set(commands.EXECUTABLE_VIEW_FORMATS)

    non_view = {label for label in covered if not label.startswith("view")}
    # `gate post-edit --full` is the same parser verb as `gate post-edit`.
    non_view = {label.removesuffix(" --full") for label in non_view}
    # `new` is wrapped and parity-checked but deliberately not executable from
    # an emitted command (the harness never repairs by creating a change).
    assert non_view - {"new"} == set(commands.EXECUTABLE_VERBS) - {"view"}


def test_dispatch_and_wrapper_agree_on_the_pact_shape_literal(parity_root: Path, tmp_path: Path) -> None:
    """Regression pin for the `"fast"` vs `"fast_path"` drift.

    The wrapper used to hardcode `pact_shape: "fast"`. The real verb reads it
    off the created manifest, so the harness reported a shape the CLI never
    emits.
    """
    bench_root = tmp_path / "shape"
    shutil.copytree(parity_root, bench_root, symlinks=True)
    result = calls.new_change(Recorder(root=bench_root), bench_root, "bench-shape-probe")
    assert result.payload["pact_shape"] == "fast_path"
