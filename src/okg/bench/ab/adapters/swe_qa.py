"""SWE-QA adapter (Track A: repo-history knowledge QA).

Dataset: HF ``swe-qa/SWE-QA-Benchmark`` (Apache-2.0), 720 questions across 15
repo splits of ``{question, answer}`` rows. Instance ids are synthesized as
``sweqa/<split>/<idx>`` — deterministic because the loader preserves dataset
row order under a pinned revision.

Scoring: reference-answer LLM judge on a cheap model (litellm). The judge
model id, prompt template, and rubric version are recorded in the run
manifest and every score's detail. No judge configured => tasks are left
UNSCORED with the reason — never heuristically scored.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError, ScoringError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

DATASET = "swe-qa/SWE-QA-Benchmark"
# Repo splits (the "default" split is excluded: it is not one of the 15 repos).
SPLITS = (
    "astropy", "conan", "django", "flask", "matplotlib", "pylint", "pytest",
    "reflex", "requests", "scikit_learn", "sphinx", "sqlfluff", "streamlink",
    "sympy", "xarray",
)
# Repo split -> GitHub slug for workdir provisioning.
SPLIT_REPOS = {
    "astropy": "astropy/astropy",
    "conan": "conan-io/conan",
    "django": "django/django",
    "flask": "pallets/flask",
    "matplotlib": "matplotlib/matplotlib",
    "pylint": "pylint-dev/pylint",
    "pytest": "pytest-dev/pytest",
    "reflex": "reflex-dev/reflex",
    "requests": "psf/requests",
    "scikit_learn": "scikit-learn/scikit-learn",
    "sphinx": "sphinx-doc/sphinx",
    "sqlfluff": "sqlfluff/sqlfluff",
    "streamlink": "streamlink/streamlink",
    "sympy": "sympy/sympy",
    "xarray": "pydata/xarray",
}

PROMPT_TEMPLATE = """You are working in a checkout of {repo}.

Answer the following question about this repository. Ground your answer in
the actual code and history available to you; cite file paths where relevant.

<question>
{question}
</question>

Give a direct, complete answer. End with a line `ANSWER COMPLETE`.
"""

JUDGE_RUBRIC_VERSION = "sweqa-judge.v1"
JUDGE_MODEL_ENV = "OKG_BENCH_AB_JUDGE_MODEL"
JUDGE_PROMPT_TEMPLATE = """You are grading an answer to a question about a software repository.

<question>
{question}
</question>

<reference_answer>
{reference}
</reference_answer>

<candidate_answer>
{candidate}
</candidate_answer>

Score the candidate against the reference on factual correctness only
(style and length do not matter; extra correct detail does not hurt).
Respond with EXACTLY one JSON object, no prose:
{{"score": <float 0.0-1.0>, "rationale": "<one sentence>"}}
Anchors: 1.0 = fully correct on every material fact; 0.5 = right thrust but
a material fact wrong or missing; 0.0 = wrong or unresponsive.
"""


class SweQaAdapter(BenchmarkAdapter):
    name = "swe-qa"
    pilot_size = 20

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        total = 0
        revision = ""
        for split in SPLITS:
            rows, revision = hfdata.fetch_split(
                DATASET, "default", split, data_root=data_root
            )
            total += len(rows)
        return {"dataset": DATASET, "revision": revision, "rows": total}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {"dataset": DATASET, "revision": meta["revision"], "license": "Apache-2.0"}

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            per_split: dict[str, list[dict[str, Any]]] = load_json(offline_fixture)
        else:
            per_split = {
                split: hfdata.load_cached_split(DATASET, split, data_root=data_root)
                for split in SPLITS
            }
        instances: list[TaskInstance] = []
        for split, rows in per_split.items():
            repo = SPLIT_REPOS.get(split)
            if repo is None:
                raise DatasetError(
                    f"unknown SWE-QA split {split!r}; known: {sorted(SPLIT_REPOS)}"
                )
            for idx, row in enumerate(rows):
                if not row.get("question") or not row.get("answer"):
                    raise DatasetError(
                        f"SWE-QA row {split}/{idx} missing question/answer: keys={sorted(row)}"
                    )
                instances.append(
                    TaskInstance(
                        instance_id=f"sweqa/{split}/{idx:03d}",
                        benchmark=self.name,
                        prompt=PROMPT_TEMPLATE.format(repo=repo, question=row["question"]),
                        repo=repo,
                        gold={"answer": row["answer"]},
                        extra={"split": split, "question": row["question"]},
                    )
                )
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Grouped by repo, dataset order within repo — cache-friendly and
        deterministic."""
        return sorted(instances, key=lambda i: i.instance_id)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """20 questions spread across repos (round-robin over the ordered
        list) so the pilot isn't a single-repo artifact."""
        ordered = self.order(instances)
        by_repo: dict[str, list[TaskInstance]] = {}
        for inst in ordered:
            by_repo.setdefault(inst.repo or "", []).append(inst)
        picked: list[TaskInstance] = []
        rank = 0
        while len(picked) < self.pilot_size and any(by_repo.values()):
            for repo in sorted(by_repo):
                bucket = by_repo[repo]
                if rank < len(bucket) and len(picked) < self.pilot_size:
                    picked.append(bucket[rank])
            rank += 1
        return picked

    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        judge_model = os.environ.get(JUDGE_MODEL_ENV, "").strip()
        if not judge_model:
            return Score(
                instance_id=instance.instance_id,
                value=None,
                kind="llm_judge",
                unscored_reason=(
                    f"no judge configured: set {JUDGE_MODEL_ENV} to a litellm "
                    f"model id (cheap tier recommended)"
                ),
            )
        if result.status != "completed" or not result.answer.strip():
            return Score(
                instance_id=instance.instance_id,
                value=0.0,
                kind="llm_judge",
                detail={"reason": f"no answer (status={result.status})"},
            )
        prompt = JUDGE_PROMPT_TEMPLATE.format(
            question=instance.extra["question"],
            reference=instance.gold["answer"],
            candidate=result.answer,
        )
        raw = _call_judge(judge_model, prompt)
        payload = _parse_judge_json(raw)
        score = float(payload["score"])
        if not 0.0 <= score <= 1.0:
            raise ScoringError(f"judge returned out-of-range score {score!r}")
        return Score(
            instance_id=instance.instance_id,
            value=score,
            kind="llm_judge",
            detail={
                "judge_model": judge_model,
                "rubric_version": JUDGE_RUBRIC_VERSION,
                "rationale": payload.get("rationale", ""),
            },
        )


def judge_config() -> dict[str, Any]:
    """Judge identity for the run manifest."""
    return {
        "judge_model_env": JUDGE_MODEL_ENV,
        "judge_model": os.environ.get(JUDGE_MODEL_ENV, "") or None,
        "rubric_version": JUDGE_RUBRIC_VERSION,
        "prompt_template": JUDGE_PROMPT_TEMPLATE,
    }


def _call_judge(model: str, prompt: str) -> str:
    """One judge completion via litellm (a core dependency of this repo)."""
    try:
        import litellm
    except ImportError as exc:  # pragma: no cover - litellm is a core dep
        raise ScoringError(f"litellm unavailable for SWE-QA judging: {exc}") from exc
    resp = litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_tokens=300,
    )
    content = resp["choices"][0]["message"]["content"]
    if not content:
        raise ScoringError(f"judge model {model!r} returned empty content")
    return str(content)


def _parse_judge_json(raw: str) -> dict[str, Any]:
    m = re.search(r"\{.*\}", raw, re.DOTALL)
    if not m:
        raise ScoringError(f"judge output is not JSON: {raw[:200]!r}")
    try:
        payload = json.loads(m.group(0))
    except json.JSONDecodeError as exc:
        raise ScoringError(f"judge JSON parse failed: {exc}: {raw[:200]!r}") from exc
    if "score" not in payload:
        raise ScoringError(f"judge JSON missing 'score': {payload!r}")
    return payload
