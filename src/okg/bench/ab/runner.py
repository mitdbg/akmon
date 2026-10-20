"""The run loop: load -> order -> (resume-check) -> per-task drive -> score.

One scripted runner drives every arm with identical prompts and budgets
(protocol requirement). State layout under ``<runs_root>/<run_id>/``::

    manifest.json            reproducibility record (manifest.py)
    tasks/<slug>.json        per-task record: result + metrics + between-task info
    transcripts/<slug>.stream.jsonl
    workdirs/<slug>/         preserved task workdirs (scenario scoring reads them)
    scores.json              finalize_scores output
    summary.json             report.py aggregation

Resume: an existing task record is skipped (continue, don't retry); the
manifest identity must match or the run refuses (ManifestMismatch).
"""

from __future__ import annotations

import dataclasses
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION, checkouts
from .adapters import get_adapter
from .arms import RunContext, get_driver
from .arms.claude_code import carry_memory_forward, reset_shared_workdir
from .errors import AbBenchError
from .manifest import (
    RunManifest,
    check_resume,
    claude_cli_version,
    framework_git_sha,
    load_manifest,
    new_run_id,
    now_utc,
    prompt_digest,
    write_manifest,
)
from .model import TaskInstance, TaskResult, dump_json, load_json

CLAUDE_ARMS = ("fresh", "native", "okg")


@dataclass(frozen=True)
class RunConfig:
    benchmark: str
    arm: str
    model: str
    runs_root: Path
    data_root: Path
    pilot: bool = False
    limit: int | None = None
    subset: str | None = None
    resume_run_id: str | None = None
    offline_fixture: Path | None = None
    task_timeout_s: float = 1800.0
    permission_mode: str = "bypassPermissions"
    native_memory_mode: str = "shared-workdir"
    corpus_config: Path | None = None  # okg arm: corpus.json from `corpus build`
    no_workdir: bool = False  # echo-arm testing without repo clones
    score: bool = True


def execute(cfg: RunConfig) -> Path:
    # Absolute paths everywhere: task subprocesses run with the WORKDIR as
    # cwd, so a relative runs_root would make CLAUDE_CONFIG_DIR (and every
    # other handed-down path) resolve against the wrong directory.
    cfg = dataclasses.replace(
        cfg,
        runs_root=cfg.runs_root.resolve(),
        data_root=cfg.data_root.resolve(),
        offline_fixture=(
            cfg.offline_fixture.resolve() if cfg.offline_fixture else None
        ),
        corpus_config=(cfg.corpus_config.resolve() if cfg.corpus_config else None),
    )
    adapter = get_adapter(cfg.benchmark)
    driver = get_driver(cfg.arm)

    instances = adapter.load_instances(
        data_root=cfg.data_root, offline_fixture=cfg.offline_fixture
    )
    dataset_ref = _dataset_ref(adapter, cfg)
    corpus_cfg = _load_corpus_config(cfg, adapter)
    if corpus_cfg is not None:
        instances = adapter.bind_corpus(instances, corpus_cfg)
    if cfg.subset:
        instances = adapter.subset(instances, cfg.subset)
    ordered = adapter.pilot_subset(instances) if cfg.pilot else adapter.order(instances)
    if cfg.limit is not None:
        ordered = ordered[: cfg.limit]
    if not ordered:
        raise AbBenchError("no instances selected — check --subset/--limit")
    manifest = _build_manifest(cfg, ordered, dataset_ref, corpus_cfg)

    run_dir = cfg.runs_root / manifest.run_id
    if cfg.resume_run_id:
        # The recorded manifest is the truth for a resume and is never
        # rewritten; the freshly built one is only the comparison operand.
        # Nothing below this block reads `manifest` -- run_dir was already
        # derived from it -- so there is nothing to reassign, and every
        # identity field (ordering included) is guarded by check_resume.
        check_resume(load_manifest(run_dir), manifest)
    else:
        if run_dir.exists():
            raise AbBenchError(f"run dir already exists: {run_dir}")
        run_dir.mkdir(parents=True)
        write_manifest(run_dir, manifest)

    ctx = RunContext(
        run_dir=run_dir,
        data_root=cfg.data_root,
        model=cfg.model,
        task_timeout_s=cfg.task_timeout_s,
        permission_mode=cfg.permission_mode,
        native_memory_mode=(cfg.native_memory_mode if cfg.arm == "native" else None),
        corpus=corpus_cfg,
    )
    driver.setup_run(ctx)
    _validate_native_workdir_policy(cfg, ordered)

    try:
        results = _run_tasks(cfg, adapter, driver, ctx, ordered, run_dir)
    finally:
        driver.teardown_run(ctx)

    if cfg.score:
        scores = adapter.finalize_scores(
            run_dir=run_dir, data_root=cfg.data_root,
            instances=ordered, results=results,
        )
        dump_json(
            run_dir / "scores.json",
            [dataclasses.asdict(s) for s in scores],
        )
        _log(f"scored {sum(1 for s in scores if s.value is not None)}/{len(scores)} tasks")
    _log(f"run complete: {run_dir}")
    return run_dir


def _run_tasks(
    cfg: RunConfig,
    adapter: Any,
    driver: Any,
    ctx: RunContext,
    ordered: list[TaskInstance],
    run_dir: Path,
) -> list[TaskResult]:
    results: list[TaskResult] = []
    prev_workdir: Path | None = None
    for index, task in enumerate(ordered):
        record_path = run_dir / "tasks" / f"{_slug(task.instance_id)}.json"
        if record_path.exists():
            record = load_json(record_path)
            result = _result_from_record(record)
            results.append(result)
            recorded_workdir = record["result"]["detail"].get("workdir")
            prev_workdir = Path(recorded_workdir) if recorded_workdir else None
            if record.get("between") is None:
                # The paid-for task was recorded but the between-task step
                # (okg arm: session ingest + publish) never completed —
                # re-run it so task k+1 reads a graph that includes task k.
                _log(
                    f"[{index + 1}/{len(ordered)}] {task.instance_id}: "
                    "re-running incomplete between-task step"
                )
                record["between"] = driver.between_tasks(task, result, ctx)
                dump_json(record_path, record)
            else:
                _log(
                    f"[{index + 1}/{len(ordered)}] {task.instance_id}: "
                    "already done, skipping"
                )
            continue

        workdir = _provision_workdir(cfg, adapter, task, run_dir, prev_workdir)
        _log(f"[{index + 1}/{len(ordered)}] {task.instance_id}: running arm={cfg.arm}")
        result = driver.run_task(task, ctx, workdir)
        result = _attach_patch(result, workdir)
        # Persist the (paid-for) task result BEFORE the between-task step:
        # a between-task failure must not lose the session, and a resume
        # must not re-spend it.
        record = {
            "task_index": index,
            "instance": task.public_dict(),
            "result": dataclasses.asdict(result),
            "between": None,
            "recorded_at": now_utc(),
        }
        dump_json(record_path, record)
        record["between"] = driver.between_tasks(task, result, ctx)
        dump_json(record_path, record)
        results.append(result)
        prev_workdir = workdir
    return results


# -- workdir policy -----------------------------------------------------


def _provision_workdir(
    cfg: RunConfig,
    adapter: Any,
    task: TaskInstance,
    run_dir: Path,
    prev_workdir: Path | None,
) -> Path:
    workdirs = run_dir / "workdirs"
    if cfg.no_workdir or (task.repo is None and task.extra.get("add_dirs")):
        # Repo-less benchmarks (e.g. ctibench) work in an empty scratch dir
        # with their corpus mounted read-only via add_dirs.
        dest = workdirs / _slug(task.instance_id)
        dest.mkdir(parents=True, exist_ok=True)
        return dest
    if task.repo is None:
        raise AbBenchError(
            f"instance {task.instance_id} has no repo, no add_dirs mounts, "
            "and --no-workdir was not set"
        )
    base = task.base_commit or checkouts.resolve_pin(task.repo, data_root=cfg.data_root)

    if cfg.arm == "native" and cfg.native_memory_mode == "shared-workdir":
        shared = workdirs / "shared"
        if prev_workdir is None or not shared.exists():
            checkouts.task_workdir(task.repo, base, shared, data_root=cfg.data_root)
        else:
            reset_shared_workdir(shared, base)
        adapter.prepare_workdir(task, shared, data_root=cfg.data_root)
        return shared

    dest = workdirs / _slug(task.instance_id)
    checkouts.task_workdir(task.repo, base, dest, data_root=cfg.data_root)
    adapter.prepare_workdir(task, dest, data_root=cfg.data_root)
    if cfg.arm == "native" and cfg.native_memory_mode == "carry-forward" and prev_workdir:
        carry_memory_forward(prev_workdir, dest)
    return dest


def _validate_native_workdir_policy(
    cfg: RunConfig, ordered: list[TaskInstance]
) -> None:
    if cfg.arm != "native" or cfg.native_memory_mode != "shared-workdir":
        return
    repos = {t.repo for t in ordered}
    if len(repos) > 1:
        raise AbBenchError(
            "native shared-workdir mode requires a single-repo task sequence "
            f"(got {sorted(r or '' for r in repos)}); use "
            "--native-memory-mode carry-forward for multi-repo runs"
        )


# -- helpers ------------------------------------------------------------


def _dataset_ref(adapter: Any, cfg: RunConfig) -> dict[str, Any]:
    if cfg.offline_fixture is not None:
        import hashlib

        # Byte digest, not parsed-JSON digest: fixtures are not all JSON
        # (FreshQA mandates an operator-exported CSV), and byte identity is
        # the stronger reproducibility pin anyway.
        return {
            "dataset": f"offline-fixture:{cfg.offline_fixture.name}",
            "revision": hashlib.sha256(
                cfg.offline_fixture.read_bytes()
            ).hexdigest(),
            "license": "local fixture",
        }
    return adapter.dataset_ref(data_root=cfg.data_root)


def _load_corpus_config(cfg: RunConfig, adapter: Any) -> dict[str, Any] | None:
    if cfg.arm == "okg":
        # A benchmark with no okg-arm corpus definition refuses HERE, with
        # the recorded reason — not with a confusing missing-deployment
        # error three layers down in the arm driver.
        from .corpus_domain import NoCorpus, corpus_for_benchmark

        corpus = corpus_for_benchmark(adapter.name)
        if isinstance(corpus, NoCorpus):
            raise AbBenchError(
                f"benchmark {adapter.name} has no okg-arm corpus definition: "
                f"{corpus.reason}. Run the vanilla arms, or define a corpus "
                "recipe first."
            )
    needed = cfg.arm == "okg" or adapter.requires_corpus_config
    if not needed:
        if cfg.corpus_config is not None:
            raise AbBenchError(
                f"--corpus-config is not meaningful for --arm {cfg.arm} on "
                f"benchmark {adapter.name}"
            )
        return None
    if cfg.corpus_config is None:
        raise AbBenchError(
            f"{'okg arm' if cfg.arm == 'okg' else adapter.name} requires "
            "--corpus-config <corpus.json> (from `okg-bench corpus build` / "
            "`corpus build-cti`)"
        )
    return load_json(cfg.corpus_config)


def _build_manifest(
    cfg: RunConfig,
    ordered: list[TaskInstance],
    dataset_ref: dict[str, Any],
    corpus_cfg: dict[str, Any] | None,
) -> RunManifest:
    cli_version = (
        claude_cli_version("claude") if cfg.arm in CLAUDE_ARMS else None
    )
    from .adapters.swe_qa import judge_config

    return RunManifest(
        schema_version=SCHEMA_VERSION,
        run_id=cfg.resume_run_id or new_run_id(cfg.benchmark, cfg.arm),
        created_at=now_utc(),
        benchmark=cfg.benchmark,
        arm=cfg.arm,
        model=cfg.model,
        claude_cli_version=cli_version,
        framework_git_sha=framework_git_sha(),
        dataset_ref=dataset_ref,
        subset=cfg.subset,
        pilot=cfg.pilot,
        limit=cfg.limit,
        prompt_digest=prompt_digest([t.prompt for t in ordered]),
        ordering={
            "mode": "pilot" if cfg.pilot else "full",
            "instance_ids": [t.instance_id for t in ordered],
        },
        budgets={
            "task_timeout_s": cfg.task_timeout_s,
            "permission_mode": cfg.permission_mode,
        },
        native_memory_mode=(cfg.native_memory_mode if cfg.arm == "native" else None),
        judge=(judge_config() if cfg.benchmark == "swe-qa" else None),
        corpus=corpus_cfg,
        argv=list(sys.argv),
    )


def _attach_patch(result: TaskResult, workdir: Path) -> TaskResult:
    if not (workdir / ".git").exists():
        return result
    try:
        patch = checkouts.working_diff(workdir)
    except AbBenchError as exc:
        return dataclasses.replace(
            result, detail={**result.detail, "patch_error": str(exc)}
        )
    return dataclasses.replace(result, patch=patch or None)


def _result_from_record(record: dict[str, Any]) -> TaskResult:
    from .model import TaskMetrics

    r = dict(record["result"])
    r["metrics"] = TaskMetrics(**r["metrics"])
    return TaskResult(**r)


def _slug(instance_id: str) -> str:
    return instance_id.replace("/", "__")


def _log(msg: str) -> None:
    print(f"[okg-bench] {msg}", flush=True)
