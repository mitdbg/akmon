"""SciFact adapter (EMNLP 2020, arXiv 2004.14974) — open-book claim
verification over a frozen abstract corpus.

Dataset: the upstream AI2 S3 release (``data.tar.gz``, 3.1 MB).
License per upstream LICENSE.md (authoritative): claims CC-BY-4.0,
corpus (S2ORC abstracts) ODC-By-1.0. The HF card's ``cc-by-nc-2.0``
tag contradicts upstream and is recorded as a discrepancy (PACT
okg-bench-domain-adapters designs/first-gates.md).

Scoring scope is the DEV split only (450 claims): the 300 test-split
labels are hidden and scored only via the AI2 leaderboard — any request
for test fails loudly, never guesses.

Open-book design: every arm gets the same frozen 5,183-abstract
``corpus.jsonl`` mounted read-only via the ``{SNAPSHOT_DIR}`` token
(bound at run time from the corpus freeze manifest); the okg arm
additionally sees the graph built from that snapshot by
``PaperAbstractsSource``.

Scoring is deterministic — no LLM judge: verdict exact match
(SUPPORT/CONTRADICT/NOINFO, last VERDICT line) is the score value;
evidence sentence-level F1 against the gold sentence indices is
recorded in the score detail (meaningful only when both gold and
answer are non-NOINFO).
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import json
import re
import tarfile
import time
import urllib.request
from pathlib import Path, PurePosixPath
from typing import Any

from .. import hfdata
from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult
from .base import BenchmarkAdapter

DATASET = "allenai/scifact-s3"
TARBALL_URL = (
    "https://scifact.s3-us-west-2.amazonaws.com/release/latest/data.tar.gz"
)
LICENSE_NOTE = (
    "claims CC-BY-4.0, corpus ODC-By-1.0 (upstream LICENSE.md; "
    "HF card cc-by-nc-2.0 tag contradicted)"
)
RELEASE_FILES = ("claims_dev.jsonl", "claims_train.jsonl", "corpus.jsonl")
LABELS = ("SUPPORT", "CONTRADICT", "NOINFO")
SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"
_DOWNLOAD_TIMEOUT_S = 120
_PILOT_PER_LABEL = 5

OPEN_BOOK_PREAMBLE = (
    "You are working in a scratch directory. A frozen snapshot of the "
    "SciFact corpus — 5,183 paper abstracts in corpus.jsonl, one JSON "
    "object per line with doc_id, title, and abstract (a list of "
    "sentences, 0-indexed) — is available read-only under "
    f"{SNAPSHOT_TOKEN}. Consult it however you like.\n\n"
)

INSTRUCTIONS = (
    "\n\nVerify the claim against the corpus. When your verdict is not "
    "NOINFO, cite your evidence as one line per document of the form:\n"
    "EVIDENCE: <doc_id>: <comma-separated 0-based sentence indices>\n"
    "Then end your answer with exactly one line of the form:\n"
    "VERDICT: SUPPORT|CONTRADICT|NOINFO"
)

_VERDICT_RE = re.compile(
    r"^\s*VERDICT:\s*(SUPPORT|CONTRADICT|NOINFO)\b", re.IGNORECASE
)
_EVIDENCE_RE = re.compile(r"^\s*EVIDENCE:\s*([^:\s]+)\s*:\s*(.+?)\s*$")


class SciFactAdapter(BenchmarkAdapter):
    name = "scifact"
    pilot_size = 15
    requires_corpus_config = True

    # -- data -----------------------------------------------------------
    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        req = urllib.request.Request(
            TARBALL_URL, headers={"User-Agent": "okg-bench/1"}
        )
        with urllib.request.urlopen(  # nosec B310 — pinned https URL
            req, timeout=_DOWNLOAD_TIMEOUT_S
        ) as resp:
            raw = resp.read()
        revision = hashlib.sha256(raw).hexdigest()
        found: dict[str, bytes] = {}
        try:
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
                for member in tar.getmembers():
                    base = PurePosixPath(member.name).name
                    if not member.isfile() or base not in RELEASE_FILES:
                        continue
                    if base in found:
                        raise DatasetError(
                            f"scifact tarball contains {base!r} twice"
                        )
                    extracted = tar.extractfile(member)
                    if extracted is None:
                        raise DatasetError(
                            f"scifact tarball member {member.name!r} is "
                            "not extractable"
                        )
                    found[base] = extracted.read()
        except tarfile.TarError as exc:
            raise DatasetError(
                f"scifact download from {TARBALL_URL} is not a readable "
                f".tar.gz: {exc}"
            ) from exc
        missing = [name for name in RELEASE_FILES if name not in found]
        if missing:
            raise DatasetError(
                f"scifact tarball is missing {missing}; got {sorted(found)}"
            )
        out = hfdata.cache_dir(data_root, DATASET)
        out.mkdir(parents=True, exist_ok=True)
        files: dict[str, int] = {}
        for base in RELEASE_FILES:
            data = found[base]
            (out / base).write_bytes(data)
            files[base] = sum(1 for ln in data.splitlines() if ln.strip())
        meta = {
            "dataset": DATASET,
            "revision": revision,
            "url": TARBALL_URL,
            "license": LICENSE_NOTE,
            "files": files,
            "fetched_at": time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            ),
        }
        (out / "meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )
        return {"dataset": DATASET, "revision": revision, "rows": files}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": LICENSE_NOTE,
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        """Load the DEV claims only. Test labels are hidden upstream, so
        there is nothing scoreable to load for test — see ``subset``."""
        path = (
            offline_fixture
            if offline_fixture is not None
            else hfdata.cache_dir(data_root, DATASET) / "claims_dev.jsonl"
        )
        if not path.exists():
            raise DatasetError(
                f"scifact dev claims are not cached at {path}; run "
                "`okg-bench fetch --benchmark scifact` first "
                "(network required once)"
            )
        instances: list[TaskInstance] = []
        seen: set[int] = set()
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise DatasetError(
                    f"{path} line {lineno} is not valid JSON: {exc}"
                ) from exc
            instance = _instance(row, path=path, lineno=lineno)
            claim_id = instance.extra["claim_id"]
            if claim_id in seen:
                raise DatasetError(
                    f"{path} line {lineno}: duplicate claim id {claim_id}"
                )
            seen.add(claim_id)
            instances.append(instance)
        if not instances:
            raise DatasetError(f"{path} contains zero claims")
        return instances

    # -- ordering -------------------------------------------------------
    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.extra["claim_id"])

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """15 dev claims mixing labels: the first 5 by claim id from each
        gold-label bucket. A short bucket is a loud error, never padded."""
        buckets: dict[str, list[TaskInstance]] = {label: [] for label in LABELS}
        for inst in self.order(instances):
            buckets[inst.gold["label"]].append(inst)
        picked: list[TaskInstance] = []
        for label in LABELS:
            bucket = buckets[label]
            if len(bucket) < _PILOT_PER_LABEL:
                raise DatasetError(
                    f"scifact pilot needs {_PILOT_PER_LABEL} {label} dev "
                    f"claims but only {len(bucket)} are available"
                )
            picked.extend(bucket[:_PILOT_PER_LABEL])
        return self.order(picked)

    # -- subsets --------------------------------------------------------
    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name == "test":
            raise DatasetError(
                "scifact test labels are hidden (scored only via the AI2 "
                "leaderboard) — never scored locally, never guessed; "
                "evaluate on dev"
            )
        if name == "dev":
            return list(instances)
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Substitute the snapshot dir into every prompt and mount it via
        add_dirs. SciFact runs are open-book for EVERY arm, so the corpus
        freeze manifest is required regardless of arm."""
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got {snapshot_dir!r}); "
                "build one from the frozen scifact corpus.jsonl"
            )
        self.refuse_gold_in_snapshot(
            Path(snapshot_dir),
            ("claims_dev.jsonl", "claims_train.jsonl", "claims_test.jsonl"),
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
        gold_label: str = instance.gold["label"]
        gold_evidence = {
            doc_id: set(indices)
            for doc_id, indices in instance.gold["evidence"].items()
        }
        answer = result.answer or ""
        verdict = _extract_verdict(answer)
        if verdict is None:
            return Score(
                instance_id=instance.instance_id, value=0.0, kind="verdict",
                detail={"reason": "no parseable VERDICT line", "gt": gold_label},
            )
        detail: dict[str, Any] = {"answered": verdict, "gt": gold_label}
        answered_evidence, parse_errors = _extract_evidence(answer)
        if answered_evidence:
            detail["answered_evidence"] = {
                doc_id: sorted(indices)
                for doc_id, indices in answered_evidence.items()
            }
        if parse_errors:
            detail["evidence_parse_errors"] = parse_errors
        if gold_evidence and verdict != "NOINFO":
            precision, recall, f1 = _sentence_f1(answered_evidence, gold_evidence)
            detail["evidence_precision"] = precision
            detail["evidence_recall"] = recall
            detail["evidence_f1"] = f1
            detail["gold_evidence"] = {
                doc_id: sorted(indices)
                for doc_id, indices in gold_evidence.items()
            }
        else:
            # Sentence F1 is meaningful only when both gold and answer are
            # non-NOINFO; recorded as absent, never as a defaulted number.
            detail["evidence_f1"] = None
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if verdict == gold_label else 0.0,
            kind="verdict",
            detail=detail,
        )


def _instance(row: dict[str, Any], *, path: Path, lineno: int) -> TaskInstance:
    if not isinstance(row, dict):
        raise DatasetError(f"{path} line {lineno} is not a JSON object")
    claim_id = row.get("id")
    claim = row.get("claim")
    if not isinstance(claim_id, int):
        raise DatasetError(
            f"{path} line {lineno} has no integer 'id': keys={sorted(row)}"
        )
    if not isinstance(claim, str) or not claim.strip():
        raise DatasetError(f"{path} line {lineno} (claim {claim_id}) has no claim text")
    evidence = row.get("evidence", {})
    if not isinstance(evidence, dict):
        raise DatasetError(
            f"{path} line {lineno} (claim {claim_id}): evidence must be an "
            f"object, got {type(evidence).__name__}"
        )
    gold_label, gold_evidence = _gold(evidence, claim_id=claim_id)
    cited = row.get("cited_doc_ids", [])
    if not isinstance(cited, list):
        raise DatasetError(
            f"{path} line {lineno} (claim {claim_id}): cited_doc_ids must "
            f"be a list, got {type(cited).__name__}"
        )
    return TaskInstance(
        instance_id=f"scifact/dev/{claim_id}",
        benchmark="scifact",
        prompt=f"{OPEN_BOOK_PREAMBLE}Claim: {claim}{INSTRUCTIONS}",
        gold={"label": gold_label, "evidence": gold_evidence},
        extra={"split": "dev", "claim_id": claim_id, "cited_doc_ids": cited},
    )


def _gold(
    evidence: dict[str, Any], *, claim_id: int
) -> tuple[str, dict[str, list[int]]]:
    """Gold label (majority evidence label; NOINFO when evidence is empty)
    + gold sentence indices per doc (union across evidence entries)."""
    if not evidence:
        return "NOINFO", {}
    label_counts: dict[str, int] = {}
    gold_evidence: dict[str, list[int]] = {}
    for doc_id, entries in evidence.items():
        if not isinstance(entries, list) or not entries:
            raise DatasetError(
                f"claim {claim_id}: evidence for doc {doc_id!r} must be a "
                "non-empty list"
            )
        sentences: set[int] = set()
        for entry in entries:
            label = entry.get("label") if isinstance(entry, dict) else None
            if label not in ("SUPPORT", "CONTRADICT"):
                raise DatasetError(
                    f"claim {claim_id}: evidence entry for doc {doc_id!r} "
                    f"has unknown label {label!r}"
                )
            entry_sentences = entry.get("sentences")
            if not isinstance(entry_sentences, list) or not all(
                isinstance(s, int) for s in entry_sentences
            ):
                raise DatasetError(
                    f"claim {claim_id}: evidence entry for doc {doc_id!r} "
                    "has no integer sentence indices"
                )
            label_counts[label] = label_counts.get(label, 0) + 1
            sentences.update(entry_sentences)
        gold_evidence[str(doc_id)] = sorted(sentences)
    top = max(label_counts.values())
    majority = [label for label, count in label_counts.items() if count == top]
    if len(majority) != 1:
        raise DatasetError(
            f"claim {claim_id}: evidence labels tie {label_counts}; no "
            "majority label exists"
        )
    return majority[0], gold_evidence


def _extract_verdict(answer: str) -> str | None:
    verdict: str | None = None
    for line in answer.splitlines():
        m = _VERDICT_RE.match(line)
        if m:
            verdict = m.group(1).upper()
    return verdict


def _extract_evidence(
    answer: str,
) -> tuple[dict[str, set[int]], list[str]]:
    """Parse EVIDENCE lines into {doc_id: sentence-index set}.

    A line whose indices don't all parse as integers contributes nothing
    and is recorded as a parse error in the score detail — deterministic,
    and loud in the record rather than silently coerced."""
    evidence: dict[str, set[int]] = {}
    errors: list[str] = []
    for line in answer.splitlines():
        m = _EVIDENCE_RE.match(line)
        if not m:
            continue
        doc_id, index_text = m.group(1), m.group(2)
        tokens = [t.strip() for t in index_text.split(",") if t.strip()]
        if not tokens or not all(t.isdigit() for t in tokens):
            errors.append(line.strip())
            continue
        evidence.setdefault(doc_id, set()).update(int(t) for t in tokens)
    return evidence, errors


def _sentence_f1(
    answered: dict[str, set[int]], gold: dict[str, set[int]]
) -> tuple[float, float, float]:
    """Sentence-level F1 over (doc_id, sentence-index) pairs across docs."""
    tp = sum(
        len(answered.get(doc_id, set()) & indices)
        for doc_id, indices in gold.items()
    )
    total_answered = sum(len(indices) for indices in answered.values())
    total_gold = sum(len(indices) for indices in gold.values())
    precision = tp / total_answered if total_answered else 0.0
    recall = tp / total_gold if total_gold else 0.0
    f1 = (
        0.0 if tp == 0
        else 2 * precision * recall / (precision + recall)
    )
    return round(precision, 4), round(recall, 4), round(f1, 4)
