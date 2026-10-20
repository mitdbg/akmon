"""Benchmark-adapter interface + registry.

A fifth benchmark is a new subclass registered in ``adapters/__init__.py``,
not a rewrite: the runner only ever talks to this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from ..errors import DatasetError, ScoringError  # noqa: F401  (re-export for adapters)
from ..model import Score, TaskInstance, TaskResult


class BenchmarkAdapter(ABC):
    """Load instances, order them, produce task prompts, score results."""

    #: registry key, e.g. "swebench-verified"
    name: str = ""
    #: pilot subset size per the handoff brief (10-20 instances)
    pilot_size: int = 20
    #: open-book benchmarks need the corpus freeze manifest on EVERY arm
    #: (snapshot dir + digests), not just the okg arm
    requires_corpus_config: bool = False

    # -- data -----------------------------------------------------------
    @abstractmethod
    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        """Download + cache the dataset. Returns the dataset_ref. Network."""

    @abstractmethod
    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        """Load all instances from cache (or a pinned test fixture). No network."""

    @abstractmethod
    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        """Pinned dataset identity for the run manifest (name, revision, license)."""

    # -- ordering -------------------------------------------------------
    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Task order for a run. Default: dataset order. Deterministic."""
        return list(instances)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Deterministic pilot subset (first N of the ordered list)."""
        return self.order(instances)[: self.pilot_size]

    # -- runtime binding ------------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Bind run-time corpus facts (e.g. the shared snapshot dir) into
        instances before ordering/manifesting. Default: unchanged."""
        return instances

    @staticmethod
    def refuse_gold_in_snapshot(
        snapshot_dir: Path, forbidden_names: tuple[str, ...]
    ) -> None:
        """Fail-closed guard for open-book snapshots: a snapshot directory
        containing gold-bearing files (claims_dev.jsonl, questions.json,
        an exported answer sheet, ...) would be mounted read-only into
        every arm — e.g. an operator pointing --corpus-config at the fetch
        CACHE instead of a purpose-built snapshot (review R1)."""
        hits = sorted(
            str(p.relative_to(snapshot_dir))
            for name in forbidden_names
            for p in snapshot_dir.rglob(name)
        )
        if hits:
            raise DatasetError(
                f"snapshot dir {snapshot_dir} contains gold-bearing files "
                f"{hits}; mount a purpose-built corpus snapshot, never the "
                "dataset cache"
            )

    # -- workdir --------------------------------------------------------
    def prepare_workdir(
        self, instance: TaskInstance, workdir: Path, *, data_root: Path
    ) -> None:
        """Benchmark-specific setup inside the provisioned checkout (e.g.
        starting a conflicted merge). Default: nothing."""

    # -- subsets --------------------------------------------------------
    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        """Named subset filter (e.g. "regression"). ``instance:<id>`` selects
        one instance on any benchmark. Unknown name is an error."""
        if name.startswith("instance:"):
            wanted = name.split(":", 1)[1]
            picked = [i for i in instances if i.instance_id == wanted]
            if not picked:
                raise DatasetError(
                    f"instance {wanted!r} not found in {self.name}"
                )
            return picked
        raise DatasetError(f"benchmark {self.name!r} has no subset {name!r}")

    # -- scoring --------------------------------------------------------
    @abstractmethod
    def score_task(
        self,
        *,
        run_dir: Path,
        data_root: Path,
        instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        """Score one task. Raise ScoringError rather than defaulting.
        ``data_root`` gives scorers mirror access so gold artifacts are read
        from the mirror, never from an agent-touched workdir."""

    def finalize_scores(
        self,
        *,
        run_dir: Path,
        data_root: Path,
        instances: list[TaskInstance],
        results: list[TaskResult],
    ) -> list[Score]:
        """Run-level scoring. Default: per-task ``score_task``; a scorer
        failure is recorded as an unscored Score with the reason, never a 0."""
        by_id = {i.instance_id: i for i in instances}
        scores: list[Score] = []
        for result in results:
            inst = by_id.get(result.instance_id)
            if inst is None:
                raise ScoringError(
                    f"result for unknown instance {result.instance_id!r}"
                )
            try:
                scores.append(
                    self.score_task(
                        run_dir=run_dir, data_root=data_root,
                        instance=inst, result=result,
                    )
                )
            except ScoringError as exc:
                scores.append(
                    Score(
                        instance_id=result.instance_id,
                        value=None,
                        kind="error",
                        unscored_reason=str(exc),
                    )
                )
        return scores
