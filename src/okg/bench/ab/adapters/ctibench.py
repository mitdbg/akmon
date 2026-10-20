"""CTIBench adapter (NeurIPS 2024, arXiv 2406.07599) — open-book agentic.

Dataset: HF ``AI4Sec/cti-bench`` (CC BY-NC-SA 4.0 — research evaluation
use; instance data never vendored; the NonCommercial clause is recorded in
PACT okg-bench-ctibench designs/decision.md). Configs: cti-mcq (2500),
cti-rcm (1000), cti-rcm-2021, cti-vsp (1000), cti-ate (60) — all with
deterministic ground truth. ``cti-taa`` (50) carries NO ground-truth column
in the released data (attribution is judged in the paper, not
exact-matched) and is EXCLUDED from scoreable scope; selecting it fails
loudly with that finding.

Open-book design (operator decision, 2026-07-27): every arm gets the same
frozen, dated snapshot of the CTI corpora (NVD feeds, CWE XML, ATT&CK
STIX) mounted read-only via ``--add-dir``; the okg arm additionally sees
the graph built from that snapshot. Prompts are arm-neutral: they name the
snapshot directory via the ``{SNAPSHOT_DIR}`` token, bound at run time from
the corpus freeze manifest (``bind_corpus``); runs therefore REQUIRE
``--corpus-config`` for every arm, not just okg.

Scoring is deterministic — exact-match/regex, no LLM judge:
MCQ answer letter; last CWE id; CVSS v3.x vector (exact-match value,
per-metric agreement in detail); ATT&CK technique-id set F1 (normalized to
parent techniques, as the paper scores ATE).
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

DATASET = "AI4Sec/cti-bench"
SPLIT = "test"
SCOREABLE_CONFIGS = ("cti-mcq", "cti-rcm", "cti-rcm-2021", "cti-vsp", "cti-ate")
EXCLUDED_CONFIGS = ("cti-taa",)
SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"

OPEN_BOOK_PREAMBLE = (
    "You are working in a scratch directory. A frozen snapshot of the CTI "
    "corpora — NVD CVE JSON feeds, the MITRE CWE catalog XML, and the MITRE "
    f"ATT&CK STIX bundle — is available read-only under {SNAPSHOT_TOKEN}. "
    "Consult it however you like.\n\n"
)

# Pilot: 20 tasks spread across the four primary configs, deterministic
# under the pinned dataset revision (first-N per config in dataset order).
PILOT_SPREAD = (("cti-mcq", 8), ("cti-rcm", 5), ("cti-vsp", 5), ("cti-ate", 2))

CVSS_METRICS = ("AV", "AC", "PR", "UI", "S", "C", "I", "A")

# A handful of released rows are individually unusable (a null option, a
# GT naming an absent option). Excluding them WITH A COUNT is honest;
# excluding many silently would hide a schema change, so the loader caps
# the exclusion rate.
MAX_MALFORMED_RATE = 0.01
# ...but a rate alone would call a 1-of-3 fixture "drift". Both the count
# and the rate must exceed their bounds before a load is refused.
MAX_MALFORMED_ABS = 5


class MalformedRow(DatasetError):
    """One unusable dataset row — excluded and counted, not fatal."""


class CtiBenchAdapter(BenchmarkAdapter):
    name = "ctibench"
    pilot_size = 20
    requires_corpus_config = True

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        total = 0
        revision = ""
        for cfg in SCOREABLE_CONFIGS:
            rows, revision = hfdata.fetch_split(DATASET, cfg, SPLIT, data_root=data_root)
            total += len(rows)
        return {"dataset": DATASET, "revision": revision, "rows": total}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": "CC BY-NC-SA 4.0 (research evaluation; no redistribution)",
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            per_config: dict[str, list[dict[str, Any]]] = load_json(offline_fixture)
        else:
            per_config = {
                cfg: hfdata.load_cached_split(DATASET, cfg, data_root=data_root)
                for cfg in SCOREABLE_CONFIGS
            }
        instances: list[TaskInstance] = []
        excluded: list[str] = []
        total = 0
        for cfg, rows in per_config.items():
            if cfg in EXCLUDED_CONFIGS:
                raise DatasetError(
                    "cti-taa is excluded from scoreable scope: the released "
                    "rows carry no ground-truth column (attribution is judged "
                    "in the paper, not exact-matched)"
                )
            if cfg not in SCOREABLE_CONFIGS:
                raise DatasetError(
                    f"unknown cti-bench config {cfg!r}; known: {SCOREABLE_CONFIGS}"
                )
            for idx, row in enumerate(rows):
                try:
                    instances.append(_instance(cfg, idx, row))
                except MalformedRow as exc:
                    excluded.append(str(exc))
            total += len(rows)
        if excluded:
            rate = len(excluded) / max(total, 1)
            if len(excluded) > MAX_MALFORMED_ABS and rate > MAX_MALFORMED_RATE:
                raise DatasetError(
                    f"{len(excluded)}/{total} cti-bench rows are malformed "
                    f"({rate:.1%} > {MAX_MALFORMED_RATE:.0%}, over "
                    f"{MAX_MALFORMED_ABS} rows) — that is schema drift, not "
                    f"upstream noise; first: {excluded[:3]}"
                )
            self.load_report = {
                "total_rows": total,
                "excluded_malformed": len(excluded),
                "reasons": excluded,
            }
            for i, inst in enumerate(instances):
                instances[i] = dataclasses.replace(
                    inst, extra={**inst.extra, "load_report": self.load_report}
                )
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.instance_id)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        by_cfg: dict[str, list[TaskInstance]] = {}
        for inst in self.order(instances):
            by_cfg.setdefault(inst.extra["config"], []).append(inst)
        picked: list[TaskInstance] = []
        for cfg, count in PILOT_SPREAD:
            bucket = by_cfg.get(cfg, [])
            picked.extend(bucket[:count])
        return picked

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name in SCOREABLE_CONFIGS:
            return [i for i in instances if i.extra["config"] == name]
        if name == "cti-taa":
            raise DatasetError(
                "cti-taa has no deterministic ground truth in the released "
                "data — excluded from scoreable scope (PACT okg-bench-ctibench)"
            )
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Substitute the snapshot dir into every prompt and mount it via
        add_dirs. CTIBench runs are open-book for EVERY arm, so the corpus
        freeze manifest is required regardless of arm."""
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got {snapshot_dir!r}); "
                "build one with `okg-bench corpus build-cti`"
            )
        bound = []
        for inst in instances:
            bound.append(
                dataclasses.replace(
                    inst,
                    prompt=inst.prompt.replace(SNAPSHOT_TOKEN, str(snapshot_dir)),
                    extra={**inst.extra, "add_dirs": [str(snapshot_dir)]},
                )
            )
        return bound

    # -- scoring --------------------------------------------------------
    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        cfg = instance.extra["config"]
        gt = instance.gold["gt"]
        answer = result.answer or ""
        if cfg == "cti-mcq":
            return _score_exact(
                instance, _extract_mcq_letter(answer), gt.strip().upper(), "mcq"
            )
        if cfg in ("cti-rcm", "cti-rcm-2021"):
            return _score_exact(
                instance, _extract_last(answer, r"CWE-\d+"), gt.strip().upper(), "rcm"
            )
        if cfg == "cti-vsp":
            return _score_vsp(instance, answer, gt)
        if cfg == "cti-ate":
            return _score_ate(instance, answer, gt)
        raise DatasetError(f"unscoreable config {cfg!r}")  # unreachable by load


def _instance(cfg: str, idx: int, row: dict[str, Any]) -> TaskInstance:
    prompt_col = row.get("Prompt")
    gt = row.get("GT")
    if not prompt_col or not gt:
        raise DatasetError(
            f"cti-bench row {cfg}/{idx} missing Prompt/GT: keys={sorted(row)}"
        )
    if cfg == "cti-mcq":
        if not row.get("Question"):
            raise DatasetError(f"cti-mcq row {idx} missing Question")
        # Released rows are not uniformly 4-option: 5 of 2,500 carry a
        # JSON-null option (verified 2026-07-27), including one GAP
        # (A/B/D present, C null). Build the block from the options that
        # exist and keep their original letters — renumbering would
        # invalidate the GT letter.
        options = [
            (letter, str(row[f"Option {letter}"]).strip())
            for letter in "ABCD"
            if str(row.get(f"Option {letter}") or "").strip()
        ]
        if len(options) < 2:
            raise MalformedRow(
                f"cti-mcq row {idx} has {len(options)} usable options"
            )
        letters = {letter for letter, _ in options}
        if str(gt).strip().upper() not in letters:
            # Gold names an option the row does not contain (observed at
            # row 2235: GT 'C' with Option C null) — unscoreable.
            raise MalformedRow(
                f"cti-mcq row {idx} GT {str(gt).strip()!r} names no present "
                f"option (present: {sorted(letters)})"
            )
        body = f"{row['Question']}\n" + "\n".join(
            f"{letter}. {text}" for letter, text in options
        )
    elif cfg == "cti-ate":
        if not row.get("Description"):
            raise DatasetError(f"cti-ate row {idx} missing Description")
        body = row["Description"]
    else:  # rcm / rcm-2021 / vsp
        if not row.get("Description"):
            raise DatasetError(f"{cfg} row {idx} missing Description")
        body = row["Description"]
    return TaskInstance(
        instance_id=f"ctibench/{cfg}/{idx:04d}",
        benchmark="ctibench",
        prompt=f"{OPEN_BOOK_PREAMBLE}{prompt_col}\n\n{body}",
        gold={"gt": str(gt)},
        extra={"config": cfg, "url": row.get("URL")},
    )


def _score_exact(
    instance: TaskInstance, extracted: str | None, gt: str, kind: str
) -> Score:
    if extracted is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind=kind,
            detail={"reason": "no parseable answer", "gt": gt},
        )
    return Score(
        instance_id=instance.instance_id,
        value=1.0 if extracted == gt else 0.0,
        kind=kind,
        detail={"answered": extracted, "gt": gt},
    )


def _score_vsp(instance: TaskInstance, answer: str, gt: str) -> Score:
    answered = _extract_last(answer, r"CVSS:3\.[01]/[A-Za-z:/\.]+")
    gt_metrics = _parse_cvss(gt)
    if gt_metrics is None:
        raise DatasetError(f"unparseable GT CVSS vector {gt!r}")
    if answered is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="vsp",
            detail={"reason": "no CVSS vector in answer", "gt": gt},
        )
    ans_metrics = _parse_cvss(answered)
    if ans_metrics is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="vsp",
            detail={"reason": f"unparseable vector {answered!r}", "gt": gt},
        )
    per_metric = {m: ans_metrics.get(m) == gt_metrics.get(m) for m in CVSS_METRICS}
    exact = all(per_metric.values())
    return Score(
        instance_id=instance.instance_id,
        value=1.0 if exact else 0.0,
        kind="vsp",
        detail={
            "answered": answered, "gt": gt, "per_metric": per_metric,
            "metric_agreement": sum(per_metric.values()) / len(CVSS_METRICS),
        },
    )


def _score_ate(instance: TaskInstance, answer: str, gt: str) -> Score:
    """Technique-id set F1, normalized to parent techniques (paper's ATE
    granularity). The final line's ids are preferred; the whole answer is
    the fallback so reasoning mentions don't force a zero."""
    gt_set = {_parent(t) for t in re.findall(r"T\d{4}(?:\.\d{3})?", gt)}
    if not gt_set:
        raise DatasetError(f"GT for {instance.instance_id} has no technique ids: {gt!r}")
    lines = [ln for ln in (answer or "").splitlines() if ln.strip()]
    last_ids = (
        {_parent(t) for t in re.findall(r"T\d{4}(?:\.\d{3})?", lines[-1])}
        if lines else set()
    )
    ans_set = last_ids or {
        _parent(t) for t in re.findall(r"T\d{4}(?:\.\d{3})?", answer or "")
    }
    if not ans_set:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="ate",
            detail={"reason": "no technique ids in answer", "gt": sorted(gt_set)},
        )
    tp = len(ans_set & gt_set)
    precision = tp / len(ans_set)
    recall = tp / len(gt_set)
    f1 = 0.0 if tp == 0 else 2 * precision * recall / (precision + recall)
    return Score(
        instance_id=instance.instance_id,
        value=round(f1, 4),
        kind="ate",
        detail={
            "answered": sorted(ans_set), "gt": sorted(gt_set),
            "precision": round(precision, 4), "recall": round(recall, 4),
        },
    )


def _extract_mcq_letter(answer: str) -> str | None:
    lines = [ln.strip() for ln in (answer or "").splitlines() if ln.strip()]
    for line in reversed(lines):
        m = re.search(r"\b([ABCD])\b", line)
        if m:
            return m.group(1)
    return None


def _extract_last(answer: str, pattern: str) -> str | None:
    matches = re.findall(pattern, answer or "", re.IGNORECASE)
    return matches[-1].upper().rstrip(".").rstrip("/") if matches else None


def _parse_cvss(vector: str) -> dict[str, str] | None:
    parts = vector.upper().split("/")
    if not parts or not parts[0].startswith("CVSS:3."):
        return None
    metrics: dict[str, str] = {}
    for part in parts[1:]:
        if ":" not in part:
            return None
        key, value = part.split(":", 1)
        metrics[key] = value
    if not all(m in metrics for m in CVSS_METRICS):
        return None
    return metrics


def _parent(technique_id: str) -> str:
    return technique_id.split(".")[0].upper()
