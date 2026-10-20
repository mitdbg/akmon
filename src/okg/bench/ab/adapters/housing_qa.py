"""Housing Statute QA adapter (reglab/housing_qa) — open-book legal QA.

Dataset: HF ``reglab/housing_qa`` (CC-BY-SA-4.0; ShareAlike — mind
derived artifacts). It is a SCRIPT-based dataset: the datasets-server
viewer is disabled, so ``hfdata.fetch_split`` cannot be used. ``fetch``
instead downloads the raw ``data/questions.json.zip`` from the HF
resolve endpoint into the standard hfdata cache layout and pins the
dataset's git revision in ``meta.json``.

The statute pool (``data/statutes.tsv``, 3.36 GB) is deliberately NOT
downloaded by ``fetch``: the per-state statute slice is built at
corpus-build time, operator-driven (the operator downloads/slices the
TSV once and feeds it to ``StatuteCorpusSource`` /
the vanilla arms' mount). ``fetch`` handles only the 0.99 MB question
file.

Configs: only ``questions`` (6,853 yes/no rows WITH ``statute_idx``
labels) is scoreable. The ``questions_aux`` config (9,297 rows) carries
NO statute annotations and is unusable for retrieval scoring — a row
without statute labels is refused loudly as the aux-config trap.

Open-book design (same discipline as ctibench): every arm gets the same
frozen per-state statute slice mounted read-only via ``{SNAPSHOT_DIR}``
(bound at run time from the corpus freeze manifest); the okg arm
additionally sees the graph built from that slice. Runs REQUIRE
``--corpus-config`` for every arm.

Scoring is deterministic — no LLM judge, ever (forbidden by
req.dom.housing): answer accuracy is exact yes/no from the last
``ANSWER:`` line (a bare final yes/no is also accepted); supporting-
statute retrieval is citation overlap (normalized recall / precision /
F1 of the answer's ``CITATIONS:`` line against the gold citations).
``Score.value`` is the answer accuracy; retrieval lives in ``detail``
so both of the paper's metrics are reported.
"""

from __future__ import annotations

import dataclasses
import io
import json
import re
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

DATASET = "reglab/housing_qa"
QUESTIONS_ZIP_URL = (
    "https://huggingface.co/datasets/reglab/housing_qa/resolve/main/"
    "data/questions.json.zip"
)
QUESTIONS_FILE = "questions.json"
SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"
_TIMEOUT_S = 120

OPEN_BOOK_PREAMBLE = (
    "You are working in a scratch directory. A frozen snapshot of the "
    "relevant state's housing statutes — the per-state slice of the "
    "housing_qa statute pool, with each passage's citation, header path, "
    f"and full text — is available read-only under {SNAPSHOT_TOKEN}. "
    "Consult it however you like.\n\n"
)

ANSWER_INSTRUCTIONS = (
    "Answer the question strictly. Your final output MUST end with two "
    "lines:\n"
    "ANSWER: yes|no\n"
    "CITATIONS: <citation>; <citation>; ...\n"
    "where the CITATIONS line lists the citation strings of the statute "
    "passages that support your answer, exactly as cited in the snapshot, "
    "separated by semicolons (leave the list empty if none apply)."
)

_ANSWER_RE = re.compile(r"^\s*ANSWER\s*:\s*(yes|no)\b", re.IGNORECASE)
_CITATIONS_RE = re.compile(r"^\s*CITATIONS\s*:\s*(.*)$", re.IGNORECASE)
_BARE_RE = re.compile(r"^(yes|no)[.!]?$", re.IGNORECASE)


class HousingQaAdapter(BenchmarkAdapter):
    name = "housing-qa"
    pilot_size = 15
    requires_corpus_config = True

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        """Download data/questions.json.zip (raw file — script dataset,
        datasets-server disabled), unzip, cache. NEVER statutes.tsv."""
        revision = hfdata.dataset_revision(DATASET)
        raw = _download_bytes(QUESTIONS_ZIP_URL)
        try:
            archive = zipfile.ZipFile(io.BytesIO(raw))
        except zipfile.BadZipFile as exc:
            raise DatasetError(
                f"GET {QUESTIONS_ZIP_URL} returned a payload that is not a "
                f"zip archive: {exc}"
            ) from exc
        with archive:
            member = next(
                (n for n in archive.namelist() if n.endswith(QUESTIONS_FILE)),
                None,
            )
            if member is None:
                raise DatasetError(
                    f"{QUESTIONS_ZIP_URL} contains no {QUESTIONS_FILE!r}; "
                    f"members: {archive.namelist()}"
                )
            payload = archive.read(member)
        rows = _parse_rows(payload, origin=QUESTIONS_ZIP_URL)
        out = hfdata.cache_dir(data_root, DATASET)
        out.mkdir(parents=True, exist_ok=True)
        (out / QUESTIONS_FILE).write_bytes(payload)
        meta_path = out / "meta.json"
        meta = (
            json.loads(meta_path.read_text(encoding="utf-8"))
            if meta_path.exists() else {}
        )
        meta.setdefault("dataset", DATASET)
        meta["revision"] = revision
        meta.setdefault("files", {})[QUESTIONS_FILE] = {
            "rows": len(rows),
            "source_url": QUESTIONS_ZIP_URL,
            "fetched_at": _now(),
        }
        meta_path.write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )
        return {"dataset": DATASET, "revision": revision, "rows": len(rows)}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": "CC-BY-SA-4.0 (ShareAlike; mind derived artifacts)",
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            rows = load_json(offline_fixture)
        else:
            path = hfdata.cache_dir(data_root, DATASET) / QUESTIONS_FILE
            if not path.exists():
                raise DatasetError(
                    f"dataset {DATASET!r} questions are not cached at {path}; "
                    f"run `okg-bench fetch --benchmark {self.name}` first "
                    f"(network required once)"
                )
            rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list):
            raise DatasetError(
                f"housing_qa questions payload must be a JSON array of "
                f"question rows, got {type(rows).__name__}"
            )
        return [_instance(pos, row) for pos, row in enumerate(rows)]

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.instance_id)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """First 15 questions (id order) of the single most-question-rich
        state in the loaded set; ties break to the alphabetically first
        state. Deterministic under the pinned revision."""
        ordered = self.order(instances)
        counts: dict[str, int] = {}
        for inst in ordered:
            counts[inst.extra["state"]] = counts.get(inst.extra["state"], 0) + 1
        if not counts:
            raise DatasetError("housing-qa pilot: no instances loaded")
        richest = max(counts.values())
        state = min(s for s, n in counts.items() if n == richest)
        return [i for i in ordered if i.extra["state"] == state][: self.pilot_size]

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name.startswith("state:"):
            wanted = name.split(":", 1)[1].strip().upper()
            picked = [
                i for i in instances if i.extra["state"].upper() == wanted
            ]
            if not picked:
                states = sorted({i.extra["state"] for i in instances})
                raise DatasetError(
                    f"no housing-qa questions for state {wanted!r}; "
                    f"states in the loaded set: {states}"
                )
            return picked
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Substitute the snapshot dir into every prompt and mount it via
        add_dirs. Housing-QA runs are open-book for EVERY arm, so the
        corpus freeze manifest is required regardless of arm."""
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got {snapshot_dir!r}); "
                "build the per-state statute slice first"
            )
        self.refuse_gold_in_snapshot(
            Path(snapshot_dir), ("questions.json", "questions_aux.json")
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
        answer = result.answer or ""
        gold_answer = instance.gold["answer"]
        gold_citations = [
            _normalize_citation(c) for c in instance.gold["citations"]
        ]
        retrieval = _score_citations(answer, gold_citations)
        answered = _extract_answer(answer)
        if answered is None:
            return Score(
                instance_id=instance.instance_id,
                value=0.0,
                kind="housing_qa",
                detail={
                    "reason": "no parseable ANSWER line or bare yes/no",
                    "gt": gold_answer,
                    "retrieval": retrieval,
                },
            )
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if answered == gold_answer else 0.0,
            kind="housing_qa",
            detail={
                "answered": answered,
                "gt": gold_answer,
                "retrieval": retrieval,
            },
        )


# ---------- instance construction ----------


def _instance(pos: int, row: Any) -> TaskInstance:
    if not isinstance(row, dict):
        raise DatasetError(
            f"housing_qa row {pos} is not an object: {type(row).__name__}"
        )
    # `idx` 0 and other falsy-but-present values are valid: test absence,
    # None, and empty strings only — never truthiness.
    missing = [
        k for k in ("idx", "state", "question", "answer")
        if row.get(k) is None or (isinstance(row.get(k), str) and not row.get(k).strip())
    ]
    if missing:
        raise DatasetError(
            f"housing_qa row {pos} missing {missing}: keys={sorted(row)}"
        )
    try:
        idx = int(row["idx"])
    except (TypeError, ValueError) as exc:
        raise DatasetError(
            f"housing_qa row {pos} has non-integer idx {row['idx']!r}"
        ) from exc
    state = str(row["state"]).strip()
    gold_answer = str(row["answer"]).strip().casefold()
    if gold_answer not in ("yes", "no"):
        raise DatasetError(
            f"housing_qa row {state}/{idx} has non-yes/no answer "
            f"{row['answer']!r}"
        )
    statutes = row.get("statutes")
    if not isinstance(statutes, list) or not statutes:
        raise DatasetError(
            f"housing_qa row {state}/{idx} has no statute annotations — "
            f"this is the aux-config trap: the questions_aux config carries "
            f"no statute_idx labels and is unusable for retrieval scoring; "
            f"load the labeled 'questions' config"
        )
    statute_idxs: list[int] = []
    citations: list[str] = []
    for n, statute in enumerate(statutes):
        if (
            not isinstance(statute, dict)
            or statute.get("statute_idx") is None
            or not statute.get("citation")
        ):
            raise DatasetError(
                f"housing_qa row {state}/{idx} statute annotation {n} lacks "
                f"citation/statute_idx — the aux-config trap: only the "
                f"labeled 'questions' config is scoreable"
            )
        statute_idxs.append(int(statute["statute_idx"]))
        citation = str(statute["citation"]).strip()
        if citation not in citations:
            citations.append(citation)
    prompt = (
        f"{OPEN_BOOK_PREAMBLE}"
        f"State: {state}\n\n"
        f"Question: {row['question']}\n\n"
        f"{ANSWER_INSTRUCTIONS}"
    )
    return TaskInstance(
        instance_id=f"housing/{state}/{idx:05d}",
        benchmark="housing-qa",
        prompt=prompt,
        gold={
            "answer": gold_answer,
            "statute_idxs": statute_idxs,
            "citations": citations,
        },
        extra={"state": state, "config": "questions"},
    )


# ---------- answer extraction + citation overlap ----------


def _extract_answer(answer: str) -> str | None:
    """The last ANSWER: line's yes/no, else a bare final yes/no line."""
    lines = [ln for ln in answer.splitlines() if ln.strip()]
    for line in reversed(lines):
        m = _ANSWER_RE.match(line)
        if m:
            return m.group(1).casefold()
    if lines:
        m = _BARE_RE.match(lines[-1].strip())
        if m:
            return m.group(1).casefold()
    return None


def _extract_citations(answer: str) -> list[str] | None:
    """Normalized citations from the last CITATIONS: line; None when the
    answer carries no CITATIONS line at all (distinct from an empty one)."""
    found: str | None = None
    for line in answer.splitlines():
        m = _CITATIONS_RE.match(line)
        if m:
            found = m.group(1)
    if found is None:
        return None
    cited = []
    for part in found.split(";"):
        normalized = _normalize_citation(part)
        if normalized and normalized not in cited:
            cited.append(normalized)
    return cited


def _normalize_citation(citation: str) -> str:
    """Casefold, collapse whitespace, strip trailing punctuation."""
    collapsed = " ".join(str(citation).split()).casefold()
    return collapsed.rstrip(".,;:!? ")


def _score_citations(answer: str, gold_citations: list[str]) -> dict[str, Any]:
    gold_set = set(gold_citations)
    cited = _extract_citations(answer)
    if cited is None:
        return {
            "reason": "no CITATIONS line in answer",
            "precision": 0.0, "recall": 0.0, "f1": 0.0,
            "answered": [], "gold": sorted(gold_set),
        }
    cited_set = set(cited)
    tp = len(cited_set & gold_set)
    precision = tp / len(cited_set) if cited_set else 0.0
    recall = tp / len(gold_set) if gold_set else 0.0
    f1 = (
        0.0 if tp == 0
        else 2 * precision * recall / (precision + recall)
    )
    return {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "answered": sorted(cited_set),
        "gold": sorted(gold_set),
        "matched": sorted(cited_set & gold_set),
    }


# ---------- raw-file download (script dataset; no datasets-server) ----------


def _download_bytes(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "okg-bench/1"})
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last_err = exc
            time.sleep(2**attempt)
    raise DatasetError(f"GET {url} failed after 3 attempts: {last_err}")


def _parse_rows(payload: bytes, *, origin: str) -> list[Any]:
    try:
        rows = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DatasetError(
            f"{origin} member {QUESTIONS_FILE!r} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(rows, list) or not rows:
        raise DatasetError(
            f"{origin} member {QUESTIONS_FILE!r} must be a non-empty JSON "
            f"array of question rows"
        )
    return rows


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
