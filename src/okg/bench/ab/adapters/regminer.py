"""RegMiner / RegMiner4APR adapter — format donor first.

License ruling (designs/decision.md): RegMiner itself is Apache-2.0, but the
RegMiner4APR benchmark repo (brojackvn/RegressionBug4APR-Benchmark-Java)
carries NO license — all rights reserved. Therefore this adapter never
downloads or vendors the dataset: it loads a normalized JSON file the
operator prepares from their own local copy, and its real contribution is
the DONATED FORMAT — the Track C regression-instance schema
(regression-inducing commit, regression-fixing commit, their predecessors,
witness test) that the regression track scores against.

Normalized schema ``regminer.v1``::

    {"format": "regminer.v1",
     "bugs": [{"id": str,
               "repo": str,                        # owner/name or URL
               "regression_fixing_commit": str,    # rfc
               "regression_inducing_commit": str,  # ric  (the gold answer)
               "work_commit": str,                 # ric's parent (last good)
               "test": {"module": str, "case": str}}]}

Scoring: bug-inducing-commit identification — the agent's answer must name
the ric (>= 7-hex-char prefix match). Running the witness test under a Java
toolchain is the deferred runnable-adapter half (go/no-go recorded in the
PACT).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

FORMAT = "regminer.v1"

PROMPT_TEMPLATE = """You are working in a checkout of {repo} at commit {work_head}.

The test `{test}` passes at some past commit but fails at {work_head}: this
is a regression introduced by a specific past commit.

Identify the commit that introduced the regression. Investigate however you
see fit (git history, running the test at candidate commits, reading diffs).

Answer with exactly one line at the end:
REGRESSION_COMMIT: <full or abbreviated sha>
"""


class RegMinerAdapter(BenchmarkAdapter):
    name = "regminer"
    pilot_size = 10

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        raise DatasetError(
            "regminer has no downloadable dataset: RegMiner4APR carries no "
            "license, so okg-bench never fetches it. Prepare a normalized "
            f"{FORMAT} JSON from your own local copy and pass it via "
            "--offline-fixture / the regminer_path run config."
        )

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        return {
            "dataset": "regminer4apr (operator-supplied local copy)",
            "revision": None,
            "license": "NO LICENSE upstream - format donor only, never redistributed",
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is None:
            raise DatasetError(
                f"regminer requires an operator-supplied {FORMAT} JSON "
                "(no downloadable dataset; see adapter docstring)"
            )
        payload = load_json(offline_fixture)
        if payload.get("format") != FORMAT:
            raise DatasetError(
                f"regminer file {offline_fixture} declares format "
                f"{payload.get('format')!r}, expected {FORMAT!r}"
            )
        bugs = payload.get("bugs")
        if not bugs:
            raise DatasetError(f"regminer file {offline_fixture} has no bugs")
        instances: list[TaskInstance] = []
        for bug in bugs:
            missing = [
                k
                for k in (
                    "id", "repo", "regression_fixing_commit",
                    "regression_inducing_commit", "work_commit", "test",
                )
                if not bug.get(k)
            ]
            if missing:
                raise DatasetError(
                    f"regminer bug missing fields {missing}: keys={sorted(bug)}"
                )
            test = bug["test"]
            if not test.get("module") or not test.get("case"):
                raise DatasetError(f"regminer bug {bug['id']!r} test missing module/case")
            work = bug["work_commit"]
            instances.append(
                TaskInstance(
                    instance_id=f"regminer/{bug['id']}",
                    benchmark=self.name,
                    prompt=PROMPT_TEMPLATE.format(
                        repo=bug["repo"],
                        work_head=work,
                        test=f"{test['module']}::{test['case']}",
                    ),
                    repo=bug["repo"],
                    base_commit=work,
                    gold={
                        "regression_inducing_commit": bug["regression_inducing_commit"],
                        "regression_fixing_commit": bug["regression_fixing_commit"],
                        "test": test,
                    },
                    extra={"language": "java"},
                )
            )
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.instance_id)

    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        """Bug-inducing-commit identification (>= 7-char prefix match)."""
        gold = instance.gold["regression_inducing_commit"].lower()
        answered = _extract_commit(result.answer)
        if answered is None:
            return Score(
                instance_id=instance.instance_id,
                value=0.0,
                kind="bic_match",
                detail={"reason": "no REGRESSION_COMMIT line in answer"},
            )
        hit = len(answered) >= 7 and gold.startswith(answered)
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if hit else 0.0,
            kind="bic_match",
            detail={"answered": answered, "gold": gold},
        )


def _extract_commit(answer: str) -> str | None:
    matches = re.findall(
        r"REGRESSION_COMMIT:\s*([0-9a-fA-F]{7,40})", answer or ""
    )
    return matches[-1].lower() if matches else None
