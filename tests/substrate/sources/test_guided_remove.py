"""No-database checks for `okg deployment remove-source` (issue #2477).

Planning runs against a real scaffold deployment directory; the database
reads (`remediate_source_retirement`, the retirement receipt, the graph
footprint) and the `okg` subprocesses are replaced with fakes so every
branch of the flow -- preview, apply, never-published, refusal, resume --
is observed without PostgreSQL. The real retirement path is covered by the
PostgreSQL tests in `tests/substrate/test_source_retirement.py` and by the
recorded publish under `pact/changes/lane-y-remove-source/evidence/`.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate import source_retirement
from okg.substrate.cli import source_remove
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.errors import RemoveSourceError, SubstrateError
from okg.substrate.source_retirement import SourceRetirementResult
from okg.substrate.sources import guided_remove
from okg.substrate.sources.guided_remove import (
    FINISH,
    PUBLISH_PENDING,
    REGISTRY_ONLY,
    RETIRE,
    apply_remove_source,
    plan_remove_source,
)

DSN = "postgresql://unit@localhost:1/unit"


class _Conn:
    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def _connect(_dsn: str) -> _Conn:
    return _Conn()


@pytest.fixture
def deployment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "deployments"
    dep = create_deployment_scaffold("unit", deployments_root=root).deployment_dir
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    registry = dep / "source_registry.yaml"
    raw = yaml.safe_load(registry.read_text()) or {}
    raw.setdefault("sources", {})["notes"] = {
        "type": "local-folder",
        "ownership_id": "unit.notes",
        "enabled_by_default": True,
        "admission_policy": {"producer_id": "unit.notes"},
    }
    registry.write_text(yaml.safe_dump(raw, sort_keys=False))
    return dep


def _node(node_id: str, title: str) -> dict[str, Any]:
    return {"node_id": node_id, "subtype": "document_chunk", "attrs": {"title": title}}


def _retirement_plan(nodes: int, edges: int) -> dict[str, Any]:
    return {
        "plan_digest": f"digest-{nodes}-{edges}",
        "footprint_digest": f"fp-{nodes}-{edges}",
        "nodes": [_node(f"n{i}", f"Note {i}") for i in range(nodes)],
        "edges": [{"edge_id": i} for i in range(edges)],
        "refusal_codes": [],
    }


class _Graph:
    """The database as the flow sees it: one receipt and a footprint."""

    def __init__(self, *, nodes: int = 3, edges: int = 2) -> None:
        self.registry: Path | None = None
        self.enabled_at_apply: list[Any] = []
        self.nodes = nodes
        self.edges = edges
        self.receipt: dict[str, Any] | None = None
        self.published_records = 0
        self.no_checkpoint = False
        self.applied: list[dict[str, Any]] = []

    def remediate(self, _conn: Any, **kwargs: Any) -> SourceRetirementResult:
        if self.no_checkpoint:
            raise SubstrateError("source retirement requires a published checkpoint")
        if (
            self.receipt and self.receipt["status"] == "awaiting_publish"
            and not kwargs.get("apply") and not (self.nodes or self.edges)
        ):
            return SourceRetirementResult(
                "source_retirement", True, 0,
                {"retirement_id": "ret-1", "status": "awaiting_publish",
                 "plan_digest": "digest-3-2", "replayed": True},
            )
        plan = _retirement_plan(self.nodes, self.edges)
        if not kwargs.get("apply"):
            details: dict[str, Any] = {"plan": plan}
            if self.receipt:  # records again under an existing retirement
                details.update(
                    status=self.receipt["status"], continuation_required=True,
                    retirement_id=self.receipt["retirement_id"],
                )
            return SourceRetirementResult("source_retirement", False, 0, details)
        if kwargs["expected_plan_digest"] != plan["plan_digest"]:
            raise ValueError("source retirement plan digest changed")
        self.applied.append(kwargs)
        if self.registry is not None:
            entry = (yaml.safe_load(self.registry.read_text()) or {})["sources"]["notes"]
            self.enabled_at_apply.append(entry.get("enabled_by_default"))
        self.receipt = {
            "retirement_id": "ret-1", "status": "awaiting_publish",
            "registry_source_id": "notes", "ownership_id": "unit.notes",
            "repair_generation_id": None,
        }
        self.nodes = self.edges = 0
        return SourceRetirementResult(
            "source_retirement", True, 5,
            {"plan": plan, "receipt": {"retirement_id": "ret-1",
                                       "status": "awaiting_publish"}},
        )

    def receipt_for(self, *_args: Any) -> dict[str, Any] | None:
        return dict(self.receipt) if self.receipt else None

    def footprint(self, *_args: Any) -> dict[str, int]:
        return {
            "records": self.nodes, "relationships": self.edges,
            "published_records": self.published_records,
            "published_relationships": 0,
        }


@pytest.fixture
def graph(monkeypatch: pytest.MonkeyPatch, deployment: Path) -> _Graph:
    state = _Graph()
    state.registry = deployment / "source_registry.yaml"
    monkeypatch.setattr(source_retirement, "remediate_source_retirement", state.remediate)
    monkeypatch.setattr(guided_remove, "_receipt_for", state.receipt_for)
    monkeypatch.setattr(guided_remove, "_footprint", state.footprint)
    monkeypatch.setattr(guided_remove, "_default_connect", lambda _name: _connect)
    registry = deployment / "source_registry.yaml"

    @contextmanager
    def _authority(_conn: Any, *, source: str, deployment_name: str, **_kw: Any):
        if state.receipt is None or state.receipt["status"] != "published":
            raise SubstrateError(
                "source registry removal requires a published retirement receipt",
                data={"source": source},
            )
        yield {
            "retirement_id": "ret-1",
            "registry_digest": hashlib.sha256(registry.read_bytes()).hexdigest(),
            "repair_generation_id": state.receipt["repair_generation_id"],
        }

    monkeypatch.setattr(source_retirement, "source_registry_removal_authority", _authority)

    @contextmanager
    def _unretired(_conn: Any, plan: Any):
        if state.nodes or state.edges:
            raise SubstrateError("the source has records in the graph now")
        yield {}

    monkeypatch.setattr(guided_remove, "_unretired_removal_authority", _unretired)
    return state


class _Runner:
    """Fake `okg` children: `run --once` publishes, `catalog ... claim` claims."""

    def __init__(
        self, graph: _Graph, *, publish: str = "published", claim_failures: int = 0,
    ) -> None:
        self.graph = graph
        self.publish = publish
        self.claim_failures = claim_failures
        self.envs: list[dict[str, str]] = []
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(argv))
        self.envs.append(dict(_kwargs.get("env") or {}))
        if "run" in argv:
            if self.publish == "published" and self.graph.receipt:
                self.graph.receipt.update(status="published", repair_generation_id="42")
                body = {"run_summary": {"outcome": "published", "generation_id": "42"}}
                return subprocess.CompletedProcess(argv, 0, json.dumps(body), "")
            body = {"run_summary": {"outcome": "blocked", "generation_id": "43"}}
            return subprocess.CompletedProcess(argv, 1, json.dumps(body), "")
        if "claim" in argv:
            if self.claim_failures:
                self.claim_failures -= 1
                body = {"error": {"message": "claim refused by test"}}
                return subprocess.CompletedProcess(argv, 1, json.dumps(body), "")
            return subprocess.CompletedProcess(argv, 0, "{}", "")
        raise AssertionError(argv)


def _policy(**_kwargs: Any) -> dict[str, Any]:
    return {"converged": True, "changed": True, "active_after": {}}


def _plan(deployment: Path, name: str = "notes"):
    return plan_remove_source(
        deployment=str(deployment), name=name, dsn=DSN,
        environ={"PATH": "/usr/bin"}, connect=_connect,
    )


def _apply(plan: Any, runner: _Runner):
    return apply_remove_source(
        plan, runner=runner, environ={"PATH": "/usr/bin", "OKG_DSN": DSN},
        okg_argv_prefix=["okg"], connect=_connect, reconcile_policy=_policy,
    )


def _registry(deployment: Path) -> dict[str, Any]:
    return (yaml.safe_load((deployment / "source_registry.yaml").read_text()) or {}).get(
        "sources"
    ) or {}


# ---- preview ----------------------------------------------------------------


def test_preview_names_counts_sample_and_registry_entry_and_changes_nothing(
    deployment: Path, graph: _Graph,
) -> None:
    before = (deployment / "source_registry.yaml").read_bytes()
    plan = _plan(deployment)
    payload = plan.as_dict()

    assert plan.action == RETIRE
    assert payload["retract"]["records"] == 3
    assert payload["retract"]["relationships"] == 2
    assert [r["label"] for r in payload["retract"]["sample"]] == [
        "Note 0", "Note 1", "Note 2",
    ]
    assert payload["registry"]["entry_to_remove"] == "notes"
    assert payload["registry"]["entry_type"] == "local-folder"
    text = source_remove.render_text(payload)
    assert "Records to retract: 3, relationships to retract: 2" in text
    assert "Registry entry to remove: notes" in text
    assert (deployment / "source_registry.yaml").read_bytes() == before
    assert graph.applied == []


def test_cli_preview_without_apply_exits_zero_and_keeps_entry(
    deployment: Path, graph: _Graph, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import main as cli

    monkeypatch.setenv("OKG_DSN", DSN)
    code = cli.main(["deployment", "remove-source", str(deployment), "notes", "--json"])

    assert code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["apply"] is False
    assert out["retract"]["records"] == 3
    assert "notes" in _registry(deployment)


# ---- apply ------------------------------------------------------------------


def test_apply_retracts_publishes_removes_entry_and_reclaims(
    deployment: Path, graph: _Graph,
) -> None:
    ledger = deployment / ".okg" / "source-add" / "notes.json"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_text(json.dumps({"status": "published", "generation_id": "7"}))
    plan = _plan(deployment)
    assert plan.published_before is True
    runner = _Runner(graph)

    outcome = _apply(plan, runner)

    payload = outcome.as_dict()
    assert outcome.status == "removed", payload
    assert [s["step"] for s in payload["steps"]] == [
        "disable", "retract", "publish", "registry", "claim", "policy",
    ]
    # The entry was disabled BEFORE the retraction, so no import can write
    # the records back in the gap (one retirement per name).
    assert graph.enabled_at_apply == [False]
    assert graph.applied[0]["confirm_source"] == "notes"
    assert graph.applied[0]["expected_plan_digest"] == "digest-3-2"
    assert outcome.generation_id == "42"
    assert payload["counts"] == {"records_removed": 3, "relationships_removed": 2}
    assert "notes" not in _registry(deployment)
    # A later add-source of the same name must not reuse the old "published"
    # record and report `no_change` for records that are gone.
    assert not ledger.exists()
    run_call = next(c for c in runner.calls if "run" in c)
    assert {"--once", "--apply", "--inline", str(deployment)} <= set(run_call)
    assert any("claim" in c for c in runner.calls)


def test_source_named_by_its_ownership_id(deployment: Path, graph: _Graph) -> None:
    plan = _plan(deployment, name="unit.notes")

    assert plan.name == "notes"
    assert plan.ownership_id == "unit.notes"


def test_publish_failure_keeps_entry_and_rerun_resumes_at_publish(
    deployment: Path, graph: _Graph,
) -> None:
    plan = _plan(deployment)
    blocked = _apply(plan, _Runner(graph, publish="blocked"))

    payload = blocked.as_dict()
    assert blocked.status == "failed"
    assert payload["failed_step"]["step"] == "publish"
    assert "okg generation describe 43" in payload["failed_step"]["fix_hint"]
    assert _registry(deployment)["notes"]["enabled_by_default"] is False

    # Re-running reads the receipt the failed run left: publish only.
    assert graph.receipt["status"] == "awaiting_publish"
    resumed_plan = _plan(deployment)
    assert resumed_plan.action == PUBLISH_PENDING
    resumed = _apply(resumed_plan, _Runner(graph))
    assert resumed.status == "removed", resumed.as_dict()
    steps = {s["step"]: s["status"] for s in resumed.as_dict()["steps"]}
    assert steps["retract"] == "skipped"
    assert steps["disable"] == "skipped"
    # It stays disabled on resume; it is never re-enabled.
    assert _registry(deployment).get("notes") is None
    assert len(graph.applied) == 1  # never retracted twice
    assert "notes" not in _registry(deployment)


# ---- never published --------------------------------------------------------


def test_never_published_source_only_loses_its_registry_entry(
    deployment: Path, graph: _Graph,
) -> None:
    graph.nodes = graph.edges = 0
    plan = _plan(deployment)

    assert plan.action == REGISTRY_ONLY
    assert "never reached the graph" in (plan.note or "")
    runner = _Runner(graph)
    outcome = _apply(plan, runner)

    assert outcome.status == "removed", outcome.as_dict()
    steps = {s["step"]: s["status"] for s in outcome.as_dict()["steps"]}
    assert steps["retract"] == "skipped"
    assert steps["publish"] == "skipped"
    assert steps["registry"] == "ok"
    assert not any("run" in c for c in runner.calls)
    assert graph.applied == []
    assert "notes" not in _registry(deployment)


def test_never_published_deployment_without_any_version_is_registry_only(
    deployment: Path, graph: _Graph,
) -> None:
    graph.no_checkpoint = True
    graph.nodes = graph.edges = 0

    assert _plan(deployment).action == REGISTRY_ONLY


def test_staged_records_without_any_published_version_are_refused(
    deployment: Path, graph: _Graph,
) -> None:
    graph.no_checkpoint = True

    with pytest.raises(RemoveSourceError) as caught:
        _plan(deployment)

    assert caught.value.code == "remove_source_refused"
    assert "never published a graph version" in caught.value.message
    assert "okg run --once" in str(caught.value.fix_hint)
    assert {"deployment", "source", "cause"} <= set(caught.value.data)


def test_published_records_with_nothing_staged_still_retire(
    deployment: Path, graph: _Graph,
) -> None:
    graph.nodes = graph.edges = 0
    graph.published_records = 4

    assert _plan(deployment).action == RETIRE


# ---- refusals ---------------------------------------------------------------


def test_unknown_source_is_refused_naming_registered_sources(
    deployment: Path, graph: _Graph,
) -> None:
    with pytest.raises(RemoveSourceError) as caught:
        _plan(deployment, name="nope")

    assert caught.value.code == "remove_source_refused"
    assert "is not in deployment" in caught.value.message
    assert "notes" in str(caught.value.fix_hint)
    assert "notes" in caught.value.data["registered_sources"]


def test_unknown_deployment_is_refused(tmp_path: Path, graph: _Graph) -> None:
    with pytest.raises(RemoveSourceError) as caught:
        _plan(tmp_path / "missing")

    assert caught.value.data["source"] == "notes"


def test_records_back_after_a_published_retirement_are_refused(
    deployment: Path, graph: _Graph,
) -> None:
    """A published retirement is never reopened (issue #3265): records that
    return after it are refused, naming the cause and the limitation."""
    graph.receipt = {
        "retirement_id": "ret-1", "status": "published",
        "registry_source_id": "notes", "ownership_id": "unit.notes",
        "repair_generation_id": "9",
    }
    graph.nodes, graph.edges = 1, 0

    with pytest.raises(RemoveSourceError, match="cannot retract them") as caught:
        _plan(deployment)

    assert caught.value.code == "remove_source_refused"
    assert "not reopened" in caught.value.message
    assert "cannot remove records that return" in str(caught.value.fix_hint)
    assert caught.value.data["retirement_id"] == "ret-1"
    assert graph.applied == []
    assert "notes" in _registry(deployment)


def test_unstages_the_disable_when_its_commit_fails(
    deployment: Path, graph: _Graph, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_checkout(deployment)
    real = guided_remove._git

    def commit_fails(dep: Path, *args: str, check: bool = False):
        if "commit" in args:
            raise subprocess.CalledProcessError(1, ["git", *args])
        return real(dep, *args, check=check)

    monkeypatch.setattr(guided_remove, "_git", commit_fails)
    outcome = _apply(_plan(deployment), _Runner(graph))

    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "disable"
    assert failed["detail"] == {"entry_restored": True, "unstaged": True}
    # Neither the file nor the index keeps our edit, so the re-run is clean.
    assert _vcs(deployment, "status", "--porcelain") == ""
    monkeypatch.setattr(guided_remove, "_git", real)
    assert _apply(_plan(deployment), _Runner(graph)).status == "removed"


def test_git_failure_on_the_registry_is_refused_before_any_write(
    deployment: Path, graph: _Graph, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_checkout(deployment)
    real = guided_remove._git

    def fatal_check_ignore(dep: Path, *args: str, check: bool = False):
        if "check-ignore" in args:
            return subprocess.CompletedProcess(
                ["git", *args], 128, "", "fatal: index file corrupt\n",
            )
        return real(dep, *args, check=check)

    monkeypatch.setattr(guided_remove, "_git", fatal_check_ignore)
    before = (deployment / "source_registry.yaml").read_bytes()

    with pytest.raises(RemoveSourceError, match="index file corrupt") as caught:
        _plan(deployment)

    assert "exit 128" in caught.value.message
    assert (deployment / "source_registry.yaml").read_bytes() == before


def test_apply_without_a_database_is_a_typed_refusal(
    deployment: Path, graph: _Graph,
) -> None:
    plan = _plan(deployment)
    plan.dsn = ""

    with pytest.raises(RemoveSourceError, match="no database resolved"):
        _apply(plan, _Runner(graph))
    assert graph.applied == []


def test_cli_refusal_is_typed_json_and_nonzero(
    deployment: Path, graph: _Graph, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import main as cli

    monkeypatch.setenv("OKG_DSN", DSN)
    code = cli.main(["deployment", "remove-source", str(deployment), "nope", "--json"])

    captured = capsys.readouterr()
    assert code != 0
    body = json.loads(captured.err or captured.out)
    assert body["error"]["code"] == "remove_source_refused"


# ---- interactive confirmation ----------------------------------------------


def test_confirm_treats_end_of_input_as_no() -> None:
    def eof(_prompt: str) -> str:
        raise EOFError

    assert source_remove.confirm("Remove? ", read=eof) is False
    assert source_remove.confirm("Remove? ", read=lambda _p: "yes") is True


class _TerminalAtEndOfInput(io.StringIO):
    """A terminal whose input is already closed: `input()` raises EOFError."""

    def isatty(self) -> bool:
        return True


def test_cli_apply_at_end_of_terminal_input_changes_nothing(
    deployment: Path, graph: _Graph, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.cli import main as cli

    monkeypatch.setenv("OKG_DSN", DSN)
    monkeypatch.setattr("sys.stdin", _TerminalAtEndOfInput(""))
    code = cli.main(["deployment", "remove-source", str(deployment), "notes", "--apply"])

    assert code == 0
    assert "Nothing was changed." in capsys.readouterr().out
    assert graph.applied == []
    assert _registry(deployment)["notes"]["enabled_by_default"] is True


@pytest.mark.parametrize("extra", [[], ["--json"]])
def test_cli_apply_without_yes_and_no_terminal_is_refused(
    deployment: Path, graph: _Graph, capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch, extra: list[str],
) -> None:
    from okg.substrate.cli import main as cli

    monkeypatch.setenv("OKG_DSN", DSN)
    with open(os.devnull, encoding="utf-8") as closed:
        monkeypatch.setattr("sys.stdin", closed)
        code = cli.main([
            "deployment", "remove-source", str(deployment), "notes", "--apply",
            *extra,
        ])

    captured = capsys.readouterr()
    assert code != 0
    assert "add --yes" in captured.out + captured.err
    assert "remove_source_refused" in captured.out + captured.err
    assert graph.applied == []
    assert _registry(deployment)["notes"]["enabled_by_default"] is True


# ---- resumability and the checkout -----------------------------------------


def test_never_published_removal_resumes_after_a_failed_claim(
    deployment: Path, graph: _Graph,
) -> None:
    graph.nodes = graph.edges = 0
    first = _apply(_plan(deployment), _Runner(graph, claim_failures=1))

    assert first.status == "failed"
    assert first.as_dict()["failed_step"]["step"] == "claim"
    assert "run the same command again" in first.as_dict()["failed_step"]["fix_hint"]
    assert "notes" not in _registry(deployment)

    # The entry is gone, but the re-run finds the pending record and finishes.
    resumed_plan = _plan(deployment)
    assert resumed_plan.action == FINISH
    assert "did not finish" in (resumed_plan.note or "")
    resumed = _apply(resumed_plan, _Runner(graph))
    assert resumed.status == "removed", resumed.as_dict()
    steps = {s["step"]: s["status"] for s in resumed.as_dict()["steps"]}
    assert steps == {
        "disable": "skipped", "retract": "skipped", "publish": "skipped",
        "registry": "skipped", "claim": "ok", "policy": "skipped",
    }
    assert not (deployment / ".okg" / "source-remove" / "notes.json").exists()

    # Once finished, the name is unknown again: the typed refusal.
    with pytest.raises(RemoveSourceError, match="is not in deployment"):
        _plan(deployment)


def test_records_changed_after_the_plan_refuse_before_retracting(
    deployment: Path, graph: _Graph,
) -> None:
    plan = _plan(deployment)
    graph.nodes = 4  # an import wrote one more record after the preview

    outcome = _apply(plan, _Runner(graph))

    payload = outcome.as_dict()
    assert outcome.status == "failed"
    assert payload["failed_step"]["step"] == "retract"
    assert payload["failed_step"]["detail"] == {"mutation_started": False}
    assert "run the same command again" in payload["failed_step"]["fix_hint"]
    assert graph.applied == []
    # The recovery it names works: a re-run plans 4 records and removes them.
    again = _plan(deployment)
    assert again.records == 4
    assert _apply(again, _Runner(graph)).status == "removed"


def _vcs(dep: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(dep), *args], capture_output=True, text=True, check=True,
    ).stdout


def _init_checkout(dep: Path) -> None:
    _vcs(dep, "init", "-q")
    _vcs(dep, "add", ".")
    _vcs(dep, "-c", "user.name=t", "-c", "user.email=t@example.invalid",
         "commit", "-q", "-m", "scaffold")


def test_uncommitted_registry_edits_are_refused_not_swept(
    deployment: Path, graph: _Graph,
) -> None:
    _init_checkout(deployment)
    registry = deployment / "source_registry.yaml"
    registry.write_text(registry.read_text() + "# a hand edit\n")

    with pytest.raises(RemoveSourceError, match="uncommitted changes") as caught:
        _plan(deployment)

    assert "commit or discard" in str(caught.value.fix_hint)


def test_commits_only_the_registry_as_remove_source(
    deployment: Path, graph: _Graph,
) -> None:
    _init_checkout(deployment)
    manifest = deployment / "deployment.yaml"
    manifest.write_text(manifest.read_text() + "# the user's own edit\n")

    outcome = _apply(_plan(deployment), _Runner(graph))

    assert outcome.status == "removed", outcome.as_dict()
    log = _vcs(deployment, "log", "--format=%an|%s", "-3").splitlines()
    assert log[:2] == [
        "okg deployment remove-source|okg deployment remove-source: remove notes",
        "okg deployment remove-source|okg deployment remove-source: disable notes",
    ]
    for commit in ("HEAD", "HEAD~1"):
        assert _vcs(deployment, "show", "--name-only", "--format=", commit).split() == [
            "source_registry.yaml",
        ]
    assert " M deployment.yaml" in _vcs(deployment, "status", "--porcelain")


def test_claim_failure_after_the_disable_is_retried_by_a_rerun(
    deployment: Path, graph: _Graph,
) -> None:
    first = _apply(_plan(deployment), _Runner(graph, claim_failures=1))

    assert first.status == "failed"
    assert first.as_dict()["failed_step"]["step"] == "disable"
    assert graph.applied == []  # nothing retracted without a claim
    assert _registry(deployment)["notes"]["enabled_by_default"] is False

    # The entry is already disabled; the re-run still claims before retracting.
    runner = _Runner(graph)
    resumed = _apply(_plan(deployment), runner)
    assert resumed.status == "removed", resumed.as_dict()
    steps = resumed.as_dict()["steps"]
    assert steps[0]["status"] == "skipped"
    assert steps[0]["detail"]["identity_claimed"] is True
    claim_at = next(i for i, c in enumerate(runner.calls) if "claim" in c)
    run_at = next(i for i, c in enumerate(runner.calls) if "run" in c)
    assert claim_at < run_at
    assert len(graph.applied) == 1


def test_children_use_the_plan_database_not_the_shell(
    deployment: Path, graph: _Graph,
) -> None:
    shell = "postgresql://shell@localhost:2/other"
    plan = plan_remove_source(
        deployment=str(deployment), name="notes", dsn=DSN,
        environ={"PATH": "/usr/bin", "OKG_DSN": shell}, connect=_connect,
    )
    assert plan.dsn == DSN
    runner = _Runner(graph)
    outcome = apply_remove_source(
        plan, runner=runner, environ={"PATH": "/usr/bin", "OKG_DSN": shell},
        okg_argv_prefix=["okg"], connect=_connect, reconcile_policy=_policy,
    )

    assert outcome.status == "removed", outcome.as_dict()
    assert runner.envs and {env["OKG_DSN"] for env in runner.envs} == {DSN}


def test_predecessor_of_an_enabled_source_is_refused(
    deployment: Path, graph: _Graph,
) -> None:
    registry = deployment / "source_registry.yaml"
    raw = yaml.safe_load(registry.read_text())
    raw["sources"]["digest"] = {
        "ownership_id": "unit.digest",
        "enabled_by_default": True,
        "params": {"predecessor_source": "unit.notes"},
    }
    registry.write_text(yaml.safe_dump(raw, sort_keys=False))

    with pytest.raises(RemoveSourceError, match="predecessor_source") as caught:
        _plan(deployment)

    assert caught.value.data["dependents"] == ["digest"]
    assert "digest" in str(caught.value.fix_hint)

    # A disabled dependent does not pull it in by default: allowed.
    raw["sources"]["digest"]["enabled_by_default"] = False
    registry.write_text(yaml.safe_dump(raw, sort_keys=False))
    assert _plan(deployment).action == RETIRE


def test_ignored_registry_is_edited_but_not_committed(
    deployment: Path, graph: _Graph,
) -> None:
    (deployment / ".gitignore").write_text("source_registry.yaml\n")
    _init_checkout(deployment)
    head = _vcs(deployment, "rev-parse", "HEAD").strip()

    plan = _plan(deployment)
    assert plan.as_dict()["commit"] == {
        "mode": "skip", "why": "source_registry.yaml is ignored by git in this checkout",
    }
    outcome = _apply(plan, _Runner(graph))

    assert outcome.status == "removed", outcome.as_dict()
    registry_step = next(s for s in outcome.as_dict()["steps"] if s["step"] == "registry")
    assert registry_step["detail"]["not_committed"].endswith("ignored by git in this checkout")
    assert "notes" not in _registry(deployment)
    assert _vcs(deployment, "rev-parse", "HEAD").strip() == head


def test_failed_removal_commit_is_committed_by_the_rerun(
    deployment: Path, graph: _Graph, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _init_checkout(deployment)
    real = guided_remove._commit_registry
    failures = {"left": 1}

    def flaky(plan: Any, message: str) -> dict[str, Any]:
        if message.endswith("remove notes") and failures["left"]:
            failures["left"] -= 1
            raise subprocess.CalledProcessError(1, ["git", "commit"])
        return real(plan, message)

    monkeypatch.setattr(guided_remove, "_commit_registry", flaky)
    first = _apply(_plan(deployment), _Runner(graph))
    assert first.status == "failed"
    assert first.as_dict()["failed_step"]["step"] == "registry"
    assert "notes" not in _registry(deployment)

    resumed_plan = _plan(deployment)
    assert resumed_plan.action == FINISH
    resumed = _apply(resumed_plan, _Runner(graph))
    assert resumed.status == "removed", resumed.as_dict()
    registry_step = next(s for s in resumed.as_dict()["steps"] if s["step"] == "registry")
    assert registry_step["status"] == "ok" and registry_step["detail"]["commit"]
    assert _vcs(deployment, "log", "-1", "--format=%s").strip() == (
        "okg deployment remove-source: remove notes"
    )
    assert _vcs(deployment, "status", "--porcelain", "--", "source_registry.yaml") == ""


def test_pending_record_matches_name_variants(deployment: Path, graph: _Graph) -> None:
    pending = deployment / ".okg" / "source-remove"
    pending.mkdir(parents=True)
    (pending / "my_notes.json").write_text(json.dumps(
        {"name": "my_notes", "ownership_id": "unit.my_notes", "status": "entry_removed"},
    ))

    assert _plan(deployment, name="my-notes").action == FINISH
    assert _plan(deployment, name="unit.my-notes").action == FINISH
