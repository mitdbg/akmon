"""GitGoodBench adapter (agentic git scenarios).

Dataset: HF ``JetBrains/git_good_bench-lite`` (120 scenarios; CC BY-SA 4.0,
code Apache-2.0). The released data contains exactly two sample types —
``merge`` (merge-conflict resolution, scored by exact match against the
ground-truth merge commit, per the paper) and ``file_commit_chain``
(iterative committing, LLM-judge scored in the paper).

PREMISE FALSIFIED (recorded 2026-07-27): the handoff brief assigned
GitGoodBench to Track C for its "bisect (bug-inducing-commit localization)"
scenarios. The released datasets contain NO bisect scenarios; the paper
explicitly lists bisect as future work. ``subset("bisect")`` therefore fails
loudly instead of returning a fabricated subset; Track C's
bug-inducing-commit leg rests on the SWE-bench regression subset and the
RegMiner format donor. See designs/decision.md of PACT
okg-bench-ab-framework.
"""

from __future__ import annotations

import ast
import subprocess
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError, ScoringError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

DATASET = "JetBrains/git_good_bench-lite"
SPLIT = "train"

MERGE_PROMPT = """You are working in a checkout of {repo}.

A merge is in progress and has stopped on merge conflicts in the following
file(s):

{files}

Resolve every conflict the way the original developers did: produce the
resolution that best reconciles both sides' intent, then stage the resolved
files with `git add`. Do NOT commit — leave the merge staged. Do not touch
files outside the conflict set.
"""

CHAIN_PROMPT = """You are working in a checkout of {repo}.

The working tree contains uncommitted changes to {file}. Split these changes
into a sequence of well-organized commits: each commit should be a coherent,
logically separate step with a clear, conventional commit message. Create the
commits with git. When you are done, the working tree must be clean.
"""


class GitGoodBenchAdapter(BenchmarkAdapter):
    name = "gitgoodbench"
    pilot_size = 10

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        rows, revision = hfdata.fetch_split(DATASET, "default", SPLIT, data_root=data_root)
        return {"dataset": DATASET, "revision": revision, "rows": len(rows)}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": "CC BY-SA 4.0 (data), Apache-2.0 (code)",
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            rows = load_json(offline_fixture)
        else:
            rows = hfdata.load_cached_split(DATASET, SPLIT, data_root=data_root)
        instances: list[TaskInstance] = []
        for row in rows:
            for key in ("id", "name", "sample_type", "scenario"):
                if not row.get(key):
                    raise DatasetError(
                        f"git_good_bench row missing {key!r}: keys={sorted(row)}"
                    )
            scenario = _parse_scenario(row["scenario"])
            sample_type = row["sample_type"]
            if sample_type == "merge":
                prompt = MERGE_PROMPT.format(
                    repo=row["name"],
                    files="\n".join(
                        f"- {f}" for f in scenario.get("files_in_merge_conflict", [])
                    ),
                )
                base_commit = _first_parent(scenario, row)
            elif sample_type == "file_commit_chain":
                prompt = CHAIN_PROMPT.format(
                    repo=row["name"], file=scenario.get("file", "<see working tree>")
                )
                oldest = scenario.get("oldest_commit")
                if not oldest:
                    raise DatasetError(
                        f"chain scenario missing oldest_commit in {row['id']!r}"
                    )
                base_commit = f"{oldest}^"  # parent of the chain's first commit
            else:
                raise DatasetError(
                    f"unknown git_good_bench sample_type {sample_type!r} in "
                    f"{row['id']!r}; known: merge, file_commit_chain"
                )
            instances.append(
                TaskInstance(
                    instance_id=row["id"],
                    benchmark=self.name,
                    prompt=prompt,
                    repo=row["name"],
                    base_commit=base_commit,
                    gold={"scenario": scenario, "sample_type": sample_type},
                    extra={
                        "sample_type": sample_type,
                        "default_branch": row.get("default_branch"),
                        "difficulty": row.get("difficulty"),
                        "language": row.get("programming_language"),
                    },
                )
            )
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: (i.repo or "", i.instance_id))

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Pilot = first 10 merge scenarios (deterministically scoreable)."""
        merges = [i for i in self.order(instances) if i.extra["sample_type"] == "merge"]
        return merges[: self.pilot_size]

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name in ("merge", "file_commit_chain"):
            return [i for i in instances if i.extra["sample_type"] == name]
        if name == "bisect":
            raise DatasetError(
                "PREMISE FALSIFIED: the released GitGoodBench datasets contain no "
                "bisect / bug-inducing-commit scenarios (the paper lists bisect as "
                "future work). Track C uses the SWE-bench regression subset "
                "(`--subset regression`) and the RegMiner format donor instead."
            )
        return super().subset(instances, name)

    def prepare_workdir(
        self, instance: TaskInstance, workdir: Path, *, data_root: Path
    ) -> None:
        """Set up the scenario's git state inside the provisioned checkout.

        merge: fetch the second parent from the mirror and start the merge —
        it MUST conflict (a clean merge means the scenario premise failed).
        file_commit_chain: materialize the file's newest state as uncommitted
        working-tree changes, then prune the fetched objects so the original
        chain (the ground truth) is not discoverable in the workdir.
        """
        from ..checkouts import ensure_mirror

        scenario = instance.gold["scenario"]
        mirror = ensure_mirror(instance.repo or "", data_root=data_root)
        if instance.extra["sample_type"] == "merge":
            theirs = str(scenario["parents"][1])
            _git_ok(workdir, "fetch", str(mirror), theirs)
            proc = subprocess.run(
                ["git", "-C", str(workdir), "merge", "--no-commit", theirs],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode == 0:
                raise ScoringError(
                    f"merge scenario {instance.instance_id} merged cleanly; "
                    "expected conflicts — scenario premise failed"
                )
            conflicted = _git_out(workdir, "diff", "--name-only", "--diff-filter=U")
            if not conflicted.strip():
                raise ScoringError(
                    f"merge scenario {instance.instance_id} produced no conflicted "
                    f"files: {proc.stderr.strip()[:300]}"
                )
        else:  # file_commit_chain
            newest = str(scenario["newest_commit"])
            path = str(scenario["file"])
            _git_ok(workdir, "fetch", str(mirror), newest)
            content = _git_show(workdir, f"{newest}:{path}")
            if content is None:
                raise ScoringError(
                    f"chain scenario {instance.instance_id}: cannot read "
                    f"{path!r} at {newest}"
                )
            target = workdir / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            # Drop the fetched tip so the ground-truth chain is not in reach.
            (workdir / ".git" / "FETCH_HEAD").unlink(missing_ok=True)
            _git_ok(workdir, "reflog", "expire", "--all", "--expire=now")
            _git_ok(workdir, "gc", "--prune=now", "--quiet")

    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        sample_type = instance.extra["sample_type"]
        if sample_type == "merge":
            return self._score_merge(
                data_root=data_root, instance=instance, result=result
            )
        return Score(
            instance_id=instance.instance_id,
            value=None,
            kind="scenario",
            unscored_reason=(
                "file_commit_chain scoring is LLM-judge-based per the paper and "
                "is not yet wired; pilot scope is merge scenarios"
            ),
        )

    def _score_merge(
        self, *, data_root: Path, instance: TaskInstance, result: TaskResult
    ) -> Score:
        """Exact match against the ground-truth merge commit's resolution of
        each conflicted file (the paper's MCR criterion).

        The gold resolution is read from the MIRROR, never the task workdir:
        provisioned workdirs are gc'd so gold objects are not discoverable
        (or readable) there."""
        from ..checkouts import ensure_mirror

        workdir = result.detail.get("workdir")
        if not workdir or not Path(workdir).exists():
            raise ScoringError(
                f"merge scoring needs the preserved task workdir; got {workdir!r}"
            )
        gold_repo = ensure_mirror(instance.repo or "", data_root=data_root)
        scenario = instance.gold["scenario"]
        merge_sha = scenario["merge_commit_hash"]
        files = scenario.get("files_in_merge_conflict", [])
        if not files:
            raise ScoringError(f"scenario for {instance.instance_id} lists no conflict files")
        matched = 0
        per_file: dict[str, bool] = {}
        for path in files:
            gold = _git_show(gold_repo, f"{merge_sha}:{path}")
            if gold is None:
                raise ScoringError(
                    f"gold resolution {merge_sha}:{path} unreadable from mirror "
                    f"{gold_repo}"
                )
            actual_path = Path(workdir) / path
            actual = actual_path.read_bytes() if actual_path.exists() else None
            ok = actual is not None and actual == gold
            per_file[path] = ok
            matched += int(ok)
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if matched == len(files) else 0.0,
            kind="scenario",
            detail={
                "criterion": "exact_match_vs_merge_commit",
                "files": per_file,
                "matched": matched,
                "total": len(files),
            },
        )


def _parse_scenario(raw: str | dict[str, Any]) -> dict[str, Any]:
    """Scenario cells are Python-dict-literal strings in the dataset."""
    if isinstance(raw, dict):
        return raw
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError) as exc:
        raise DatasetError(f"unparseable scenario cell: {exc}: {raw[:200]!r}") from exc
    if not isinstance(parsed, dict):
        raise DatasetError(f"scenario cell is not a dict: {raw[:200]!r}")
    return parsed


def _first_parent(scenario: dict[str, Any], row: dict[str, Any]) -> str:
    parents = scenario.get("parents")
    if not parents:
        raise DatasetError(f"merge scenario missing parents in {row.get('id')!r}")
    return str(parents[0])


def _git_show(workdir: Path, spec: str) -> bytes | None:
    proc = subprocess.run(
        ["git", "-C", str(workdir), "show", spec], capture_output=True, check=False
    )
    return proc.stdout if proc.returncode == 0 else None


def _git_ok(workdir: Path, *args: str) -> None:
    proc = subprocess.run(
        ["git", "-C", str(workdir), *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise ScoringError(
            f"git {' '.join(args)} failed in {workdir}: {proc.stderr.strip()[:300]}"
        )


def _git_out(workdir: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(workdir), *args], capture_output=True, text=True, check=False
    )
    if proc.returncode != 0:
        raise ScoringError(
            f"git {' '.join(args)} failed in {workdir}: {proc.stderr.strip()[:300]}"
        )
    return proc.stdout
