"""SWE-bench Verified adapter (Track B chronological + Track C regression subset).

Dataset: HF ``SWE-bench/SWE-bench_Verified`` (500 instances). License note:
the dataset card carries no explicit license tag — local evaluation only,
instance data is never vendored into this repo (PACT okg-bench-ab-framework,
designs/decision.md).

Scoring is the OFFICIAL swebench docker harness, invoked as a subprocess —
resolve scoring is never reimplemented here. ``swebench`` (like inspect_ai)
is not a declared dependency; it is injected per-invocation via
``uv run --with swebench``.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError, ScoringError
from ..model import Score, TaskInstance, TaskResult, dump_json, load_json
from .base import BenchmarkAdapter

DATASET = "SWE-bench/SWE-bench_Verified"
SPLIT = "test"

PROMPT_TEMPLATE = """You are working in a checkout of {repo} at commit {base_commit}.

The following GitHub issue needs to be resolved:

<issue>
{problem_statement}
</issue>

Modify the repository source code so the issue is fixed. Do not modify tests.
Work only inside the current directory. When you are done, ensure your edits
are saved to disk; your working-tree diff is the deliverable.
"""

# Track C regression-subset filter: issue text indicating the bug is a
# regression. Deterministic over a pinned dataset snapshot; the matched
# pattern is recorded per instance.
REGRESSION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (label, re.compile(rx, re.IGNORECASE))
    for label, rx in [
        ("regression_word", r"\bregress(?:ion|ed|es)?\b"),
        ("worked_in", r"\bworked (?:fine |correctly |well )?(?:in|with|under|on|before)\b"),
        ("used_to_work", r"\bused to (?:work|function|behave)\b"),
        ("no_longer", r"\bno longer (?:works|functions|passes|behaves)\b"),
        ("broke_since", r"\b(?:broke|broken|breaks|stopped working)\b.{0,40}\b(?:in|since|after|between)\b"),
        ("since_version", r"\bsince (?:upgrading|updating|version|v?\d+\.\d+)\b"),
        ("worked_until", r"\bworked until\b"),
        ("bisect", r"\bgit bisect\b"),
    ]
)


class SweBenchVerifiedAdapter(BenchmarkAdapter):
    name = "swebench-verified"
    pilot_size = 20

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        rows, revision = hfdata.fetch_split(DATASET, "default", SPLIT, data_root=data_root)
        return {"dataset": DATASET, "revision": revision, "rows": len(rows)}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": "no explicit dataset-card license; local evaluation only",
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            rows = load_json(offline_fixture)
        else:
            rows = hfdata.load_cached_split(DATASET, SPLIT, data_root=data_root)
        instances = []
        for row in rows:
            missing = [
                k
                for k in ("instance_id", "repo", "base_commit", "created_at", "problem_statement")
                if not row.get(k)
            ]
            if missing:
                raise DatasetError(
                    f"SWE-bench row missing required fields {missing}: "
                    f"keys={sorted(row)}"
                )
            instances.append(
                TaskInstance(
                    instance_id=row["instance_id"],
                    benchmark=self.name,
                    prompt=PROMPT_TEMPLATE.format(
                        repo=row["repo"],
                        base_commit=row["base_commit"],
                        problem_statement=row["problem_statement"],
                    ),
                    repo=row["repo"],
                    base_commit=row["base_commit"],
                    created_at=row["created_at"],
                    cutoff=row["created_at"],
                    gold={
                        "patch": row.get("patch"),
                        "test_patch": row.get("test_patch"),
                        "FAIL_TO_PASS": row.get("FAIL_TO_PASS"),
                        "PASS_TO_PASS": row.get("PASS_TO_PASS"),
                    },
                    extra={
                        "version": row.get("version"),
                        "environment_setup_commit": row.get("environment_setup_commit"),
                        "gold_pr_number": _pr_number(row["instance_id"]),
                        "problem_statement": row["problem_statement"],
                    },
                )
            )
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Chronological within repo (Track B: the graph accumulates in
        instance-creation order), repos grouped by descending instance count
        so the many-instance repo leads."""
        counts: dict[str, int] = {}
        for inst in instances:
            counts[inst.repo or ""] = counts.get(inst.repo or "", 0) + 1
        return sorted(
            instances,
            key=lambda i: (-counts[i.repo or ""], i.repo or "", i.created_at or "", i.instance_id),
        )

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name == "regression":
            return [i for i in instances if regression_match(i) is not None]
        if name.startswith("repo:"):
            repo = name.split(":", 1)[1]
            picked = [i for i in instances if i.repo == repo]
            if not picked:
                raise DatasetError(
                    f"no instances for repo {repo!r}; known repos: "
                    f"{sorted({i.repo for i in instances})}"
                )
            return picked
        return super().subset(instances, name)

    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        raise ScoringError(
            "swebench-verified scoring is run-level (official docker harness); "
            "use finalize_scores"
        )

    def finalize_scores(
        self,
        *,
        run_dir: Path,
        data_root: Path,
        instances: list[TaskInstance],
        results: list[TaskResult],
    ) -> list[Score]:
        """Write the official predictions file and run the official harness.

        An empty patch is definitionally unresolved (value 0.0) and is not
        sent to the harness. A missing harness or docker daemon leaves tasks
        UNSCORED with the reason — never a fabricated 0/1.
        """
        preds = [
            {
                "instance_id": r.instance_id,
                "model_name_or_path": f"okg-bench:{r.arm}",
                "model_patch": r.patch or "",
            }
            for r in results
            if r.patch
        ]
        scores: list[Score] = []
        empty = [r for r in results if not r.patch]
        for r in empty:
            scores.append(
                Score(
                    instance_id=r.instance_id,
                    value=0.0,
                    kind="resolve",
                    detail={"empty_patch": True, "status": r.status},
                )
            )
        if not preds:
            return scores
        preds_path = run_dir / "predictions.jsonl"
        preds_path.write_text(
            "".join(_jsonl_line(p) for p in preds), encoding="utf-8"
        )
        report, harness_reason = _run_official_harness(
            run_dir=run_dir,
            preds_path=preds_path,
            instance_ids=[p["instance_id"] for p in preds],
        )
        if report is None:
            for p in preds:
                scores.append(
                    Score(
                        instance_id=p["instance_id"],
                        value=None,
                        kind="resolve",
                        unscored_reason=(
                            f"official swebench harness did not score: {harness_reason}; "
                            "patch preserved in predictions.jsonl"
                        ),
                    )
                )
            return scores
        resolved = set(report.get("resolved_ids", []))
        evaluated = set(report.get("submitted_ids", [])) | resolved
        for p in preds:
            iid = p["instance_id"]
            if iid in resolved:
                scores.append(Score(instance_id=iid, value=1.0, kind="resolve"))
            elif iid in evaluated:
                scores.append(Score(instance_id=iid, value=0.0, kind="resolve"))
            else:
                scores.append(
                    Score(
                        instance_id=iid,
                        value=None,
                        kind="resolve",
                        unscored_reason="harness report did not cover this instance",
                    )
                )
        return scores


def regression_match(instance: TaskInstance) -> str | None:
    """Return the first matching regression-pattern label, else None."""
    text = str(instance.extra.get("problem_statement", ""))
    for label, rx in REGRESSION_PATTERNS:
        if rx.search(text):
            return label
    return None


def _pr_number(instance_id: str) -> int:
    """SWE-bench instance ids are ``owner__repo-NNNN`` where NNNN is the gold
    PR number."""
    m = re.search(r"-(\d+)$", instance_id)
    if not m:
        raise DatasetError(f"cannot parse gold PR number from {instance_id!r}")
    return int(m.group(1))


def _jsonl_line(obj: dict[str, Any]) -> str:
    import json

    return json.dumps(obj, sort_keys=True) + "\n"


def _run_official_harness(
    *, run_dir: Path, preds_path: Path, instance_ids: list[str]
) -> tuple[dict[str, Any] | None, str]:
    """Invoke the official swebench evaluation harness. Returns
    (report, reason): report is None when no scoring happened, and the
    reason names WHICH failure occurred (docker missing vs harness crash vs
    unparseable report) — the caller must not conflate them."""
    if shutil.which("docker") is None:
        return None, "docker not on PATH (harness unavailable)"
    run_id = run_dir.name
    cmd = [
        "uv", "run", "--with", "swebench",
        "python", "-m", "swebench.harness.run_evaluation",
        "--dataset_name", DATASET,
        "--predictions_path", str(preds_path),
        "--run_id", run_id,
        "--instance_ids", *instance_ids,
        "--report_dir", str(run_dir),
    ]
    proc = subprocess.run(
        cmd, capture_output=True, text=True, cwd=str(run_dir), check=False
    )
    (run_dir / "swebench_harness.log").write_text(
        proc.stdout + "\n--- stderr ---\n" + proc.stderr, encoding="utf-8"
    )
    if proc.returncode != 0:
        return None, (
            f"harness exited {proc.returncode} (see swebench_harness.log)"
        )
    reports = sorted(run_dir.glob(f"*{run_id}*.json")) + sorted(
        run_dir.glob("results/*.json")
    )
    for path in reports:
        try:
            payload = load_json(path)
        except ValueError:
            continue
        if "resolved_ids" in payload or "resolved_instances" in payload:
            if "resolved_ids" not in payload:
                payload["resolved_ids"] = payload.get("resolved_instances", [])
            dump_json(run_dir / "swebench_report.json", payload)
            return payload, "ok"
    return None, (
        "harness exited 0 but produced no parseable report "
        "(see swebench_harness.log)"
    )
