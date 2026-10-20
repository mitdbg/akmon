"""STaRK-Prime adapter (NeurIPS 2024 D&B, arXiv 2404.13207) — open-book
retrieval over a real typed biomedical knowledge graph.

The answer unit is NOT text. It is a RANKED LIST OF ENTITY IDS: integer
indices into the SKB's own `0..129374` entity space, scored by
`stark_qa/evaluator.py` with torchmetrics MRR / Hit@k / Recall@k.

Dataset: `snap-stanford/stark` on Hugging Face, ungated.
  qa/prime/stark_qa/stark_qa.csv    11,204 rows, three columns
  qa/prime/split/{train,val,test,test-0.1}.index
  skb/prime/processed.zip           the graph, consumed by the CORPUS side

LICENCE, recorded because it is thinner than it looks. STaRK's grant over
the DATA is a Hugging Face card YAML tag (`license: cc-by-4.0`) and
nothing else: the GitHub repo's LICENSE is MIT and covers "the Software"
only, with no data clause, and the card body says nothing about
licensing. Prime's upstream is clean — PrimeKG is CC0 1.0 on Harvard
Dataverse, doi:10.7910/DVN/IXA7BM — so the chain holds, but it holds by
way of a card tag rather than a licence file. MAG is deliberately absent
from this module: ogbn-mag / Microsoft Academic Graph state no licence on
the pages fetched, so the PACT ships Prime only.

SPLITS COME FROM THE INDEX FILES, NEVER FROM SLICING THE TABLE.
`stark_qa.csv` is one flat table in no split order; the separation lives
entirely in `split/*.index`, each a line-list of ROW IDS. Measured on the
release: train 6,162 + val 2,241 + test 2,801 = 11,204 = the table's row
count exactly, pairwise disjoint, together covering `0..11203`, with
`test-0.1` a 280-row subset of test. A `df[-2801:]` would silently score a
different 2,801 questions.

GOLD IS IN THE TABLE THE CORPUS MUST NOT SEE. `stark_qa.csv` has exactly
three columns — id, query, answer_ids — and carries `answer_ids` for EVERY
row, test included; there is no answer-free copy anywhere in the release.
Any snapshot directory holding it is a gold-bearing snapshot, so it and
its siblings are in `FORBIDDEN_SNAPSHOT_NAMES` and `refuse_gold_in_snapshot`
rejects them. `StarkPrimeSource` refuses the same names independently.

THE EMISSION PROTOCOL — STARK-RANK-1
====================================
The reference evaluator does this (verbatim, `evaluator.py` lines 56-68):

    all_pred = torch.ones(max(candidate_ids) + 1) * (min(pred) - 1)
    all_pred[pred_ids] = pred

so every candidate the arm did NOT name is floored at `min(pred) - 1` —
a tie block of ~129,000 identical scores under a list of 20. What that
does to the metrics is not uniform, and it is not what the PACT assumed.
MEASURED against torch 2.9 / torchmetrics 1.9.0, the three metrics STaRK
reports do not agree with each other:

  * `retrieval_reciprocal_rank` and `retrieval_recall` both apply
    `target = torch.where(preds > 0, target, 0)` — a gold whose score is
    <= 0 is silently reclassified as NOT RELEVANT.
  * `retrieval_hit_rate` applies no such mask. It argsorts and takes the
    top k, tie block included.

Three conditions follow, and each was verified to be load-bearing by
running the reference evaluator with it violated:

  P1. EVERY EMITTED SCORE IS STRICTLY POSITIVE. Otherwise a gold the arm
      DID rank counts for hit@k while being masked out of MRR and
      recall@20. Verified: gold last of 20, scores 19..0 -> reference
      reports recall@20 = 0.0 and mrr = 0.0 for a hit.
  P2. THE SMALLEST EMITTED SCORE IS <= 1, so the evaluator's floor
      `min(pred) - 1` is <= 0 and every unnamed candidate is masked out of
      MRR and recall. Otherwise a MISS earns a nonzero MRR that tracks the
      gold entity's INDEX rather than the prediction. Verified with scores
      100..81: the same 20 predictions score mrr 0.0049751 / 0.0011099 /
      0.0006662 / 0.0005 for gold ids 200 / 900 / 1500 / 1999.
  P3. THE LIST IS EXACTLY `TOP_K` LONG, and `TOP_K` is at least the
      largest k evaluated. Otherwise hit@k — which has no positive-score
      mask — reaches into the floor tie block. Verified: emitting only
      [100, 101] and missing entirely, the reference still reports
      hit@5 = 1.0 when the gold id happens to be 1999.

STARK-RANK-1 satisfies all three: the arm emits an ORDERED list of entity
ids, best first, and the ADAPTER assigns the scores — rank r of n maps to
`1.0 - r * (1 - 1/TOP_K) / (n - 1)`, strictly decreasing across `(0, 1]`.
The arm never supplies a score, so no arm-side scale can move a metric.

Under those conditions the metrics depend only on the emitted list and the
gold set — not on the candidate set, not on argsort tie order, not on
float scale. That is what lets this module score without torch,
torchmetrics or the SKB: `_score_ranked` is the reference's semantics
written out. VERIFIED, not asserted — 400 randomised (list length, gold
size, hit/miss) trials agree with the real `Evaluator` on mrr, hit@1,
hit@3, hit@5 and recall@20 to within float32, and the fixture in
`tests/bench/ab/test_adapter_stark.py` pins the cases the sweep covered.

A list SHORTER than TOP_K is scored rather than rejected — an arm that
found only three plausible entities has said something — but P3 is then
violated, so the score detail carries `protocol_complete: false` and
`emitted`, and the hit@k values for k above the emitted count are the
protocol's (defined) truncation rather than the reference's tie-block
lottery. An EMPTY emission is a defined miss on every metric with the
reason recorded, never an unscored skip and never a crash — the reference
evaluator would fail on `min(pred)` of an empty tensor.
"""

from __future__ import annotations

import ast
import csv
import dataclasses
import hashlib
import json
import re
import time
import urllib.request
from pathlib import Path
from typing import Any

from okg.substrate.library.sources.stark_prime import (
    GOLD_ARTIFACT_NAMES,
    PRIME_ENTITY_COUNT,
)

from .. import hfdata
from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult
from .base import BenchmarkAdapter

DATASET = "snap-stanford/stark"
_HF_BASE = f"https://huggingface.co/datasets/{DATASET}/resolve/main"
_DOWNLOAD_TIMEOUT_S = 180

LICENSE_NOTE = (
    "data grant is the HF card YAML tag `license: cc-by-4.0` and nothing "
    "else (repo LICENSE is MIT over 'the Software' only, card body silent); "
    "Prime's upstream PrimeKG is CC0 1.0, Harvard Dataverse "
    "doi:10.7910/DVN/IXA7BM"
)

#: The QA artifacts this adapter fetches. The SKB (`skb/prime/processed.zip`,
#: 28,013,696 bytes, sha256 f6f265f60b761784fb7c2f052359f9cd3bf9ea20c4a307865f62ce173b294dfc)
#: is deliberately NOT fetched here: it is the corpus side's input, and
#: pulling it into the same cache is how an operator ends up mounting the
#: cache — QA table included — as the snapshot.
QA_CSV = "qa/prime/stark_qa/stark_qa.csv"
SPLIT_FILES = {
    "train": "qa/prime/split/train.index",
    "val": "qa/prime/split/val.index",
    "test": "qa/prime/split/test.index",
    "test-0.1": "qa/prime/split/test-0.1.index",
}
DEFAULT_SPLIT = "test"

#: Names that must not appear anywhere under a mounted snapshot. Same list
#: the source refuses, restated here because the two gates are independent
#: on purpose (review R1: an operator pointing --corpus-config at the fetch
#: cache).
FORBIDDEN_SNAPSHOT_NAMES = GOLD_ARTIFACT_NAMES

#: STARK-RANK-1's stated top-k. 20 because recall@20 is the largest k the
#: benchmark reports, and P3 requires the list to reach it.
TOP_K = 20

#: The metrics scored. `value` on the Score is `mrr`.
METRICS = ("mrr", "hit@1", "hit@3", "hit@5", "recall@20")

SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"

_ANSWER_RE = re.compile(r"^\s*RANKED_ENTITY_IDS:\s*(.*?)\s*$")
_INT_RE = re.compile(r"^-?\d+$")

OPEN_BOOK_PREAMBLE = (
    "You are working in a scratch directory. A frozen snapshot of the "
    "STaRK-Prime biomedical knowledge graph is available read-only under "
    f"{SNAPSHOT_TOKEN}: 129,375 entities across 10 types (disease, "
    "gene/protein, drug, pathway, anatomy, effect/phenotype, "
    "biological_process, molecular_function, cellular_component, exposure) "
    "and 8,100,498 typed relation rows across 18 relation types. Every "
    "entity has a 0-based index; that index is the identifier you answer "
    "with. Consult the snapshot however you like.\n\n"
)

INSTRUCTIONS = (
    "\n\nAnswer with the entity indices most likely to be the answer, best "
    f"first. End your answer with exactly one line of the form:\n"
    f"RANKED_ENTITY_IDS: <id>, <id>, ... ({TOP_K} ids, comma-separated, "
    "most likely first)\n"
    f"Emit exactly {TOP_K} distinct indices. Do not emit scores — ranking "
    "order is the whole answer. Fewer is accepted but scores worse; more "
    f"is truncated to the first {TOP_K}."
)


class StarkPrimeAdapter(BenchmarkAdapter):
    name = "stark-prime"
    pilot_size = 20
    requires_corpus_config = True

    # -- data -----------------------------------------------------------
    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        out = hfdata.cache_dir(data_root, DATASET)
        out.mkdir(parents=True, exist_ok=True)
        digests: dict[str, str] = {}
        sizes: dict[str, int] = {}
        for rel in (QA_CSV, *SPLIT_FILES.values()):
            raw = _download(f"{_HF_BASE}/{rel}")
            target = out / Path(rel).name
            target.write_bytes(raw)
            digests[Path(rel).name] = hashlib.sha256(raw).hexdigest()
            sizes[Path(rel).name] = len(raw)
        revision = hfdata.dataset_revision(DATASET)
        meta = {
            "dataset": DATASET,
            "revision": revision,
            "license": LICENSE_NOTE,
            "files": sizes,
            "sha256": digests,
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        (out / "meta.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True), encoding="utf-8"
        )
        return {"dataset": DATASET, "revision": revision, "files": sizes}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = hfdata.cached_meta(DATASET, data_root=data_root)
        return {
            "dataset": DATASET,
            "revision": meta["revision"],
            "license": LICENSE_NOTE,
            "sha256": meta.get("sha256", {}),
        }

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        """Load the split named by `DEFAULT_SPLIT`, in the ORDER its index
        file lists. `offline_fixture` points at a directory holding the same
        file names (a pinned test fixture)."""
        root = (
            offline_fixture
            if offline_fixture is not None
            else hfdata.cache_dir(data_root, DATASET)
        )
        rows = _load_qa_rows(root)
        return _instances_for_split(rows, root, DEFAULT_SPLIT)

    # -- subsets --------------------------------------------------------
    def subset(
        self, instances: list[TaskInstance], name: str
    ) -> list[TaskInstance]:
        """`test` is the loaded split; `test-0.1` is the release's own
        280-row cut of it.

        `test-0.1` is NOT a slice of the loaded list: membership is read
        from `split/test-0.1.index` at load time and stamped on each
        instance, and this filter reads that stamp. Measured on the
        release, the 280 ids are a subset of test's 2,801."""
        if name == DEFAULT_SPLIT:
            return list(instances)
        if name == "test-0.1":
            picked = [i for i in instances if i.extra.get("in_test_0_1")]
            if not picked:
                raise DatasetError(
                    "no loaded instance carries the test-0.1 stamp; the "
                    "split index was not read at load time"
                )
            return picked
        if name in SPLIT_FILES:
            raise DatasetError(
                f"stark-prime split {name!r} is not the loaded split "
                f"({DEFAULT_SPLIT!r}); load it from {SPLIT_FILES[name]} "
                "rather than filtering an already-loaded split"
            )
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got "
                f"{snapshot_dir!r}); build one holding "
                "skb/prime/processed/ and nothing from qa/"
            )
        self.refuse_gold_in_snapshot(
            Path(snapshot_dir), FORBIDDEN_SNAPSHOT_NAMES
        )
        return [
            dataclasses.replace(
                inst,
                prompt=inst.prompt.replace(SNAPSHOT_TOKEN, str(snapshot_dir)),
                extra={**inst.extra, "add_dirs": [str(snapshot_dir)]},
            )
            for inst in instances
        ]

    # -- scoring --------------------------------------------------------
    def score_task(
        self,
        *,
        run_dir: Path,
        data_root: Path,
        instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        del run_dir, data_root
        gold = list(instance.gold["answer_ids"])
        if not gold:
            raise DatasetError(
                f"{instance.instance_id} carries an empty gold answer set; "
                "every released Prime question has at least one answer id"
            )
        ranked, parse_detail = parse_ranked_ids(result.answer or "")
        metrics = _score_ranked(ranked, gold)
        detail: dict[str, Any] = {
            "protocol": "STARK-RANK-1",
            "top_k": TOP_K,
            "emitted": len(ranked),
            "protocol_complete": len(ranked) == TOP_K,
            "gold_size": len(gold),
            **metrics,
            **parse_detail,
        }
        if not ranked:
            detail["reason"] = (
                "no parseable RANKED_ENTITY_IDS line; scored as a miss on "
                "every metric, which is the defined outcome for an empty "
                "emission — the reference evaluator cannot score one at all "
                "(min() of an empty prediction tensor)"
            )
        return Score(
            instance_id=instance.instance_id,
            value=metrics["mrr"],
            kind="ranked_retrieval",
            detail=detail,
        )


# -- the protocol --------------------------------------------------------


def protocol_scores(ranked: list[int]) -> dict[int, float]:
    """STARK-RANK-1 scores for a ranked list: strictly decreasing over
    `(0, 1]` with the smallest at `1 / TOP_K`.

    Both bounds are load-bearing against the reference evaluator — see the
    module docstring's P1 and P2 — so this function, not the arm, is the
    sole authority on scores."""
    n = len(ranked)
    if n == 0:
        return {}
    lo = 1.0 / TOP_K
    if n == 1:
        return {int(ranked[0]): 1.0}
    step = (1.0 - lo) / (n - 1)
    return {int(node): 1.0 - i * step for i, node in enumerate(ranked)}


def parse_ranked_ids(answer: str) -> tuple[list[int], dict[str, Any]]:
    """Parse the LAST `RANKED_ENTITY_IDS:` line into distinct in-range ids.

    Every token this drops is counted and named in the returned detail;
    nothing is silently coerced, padded or reordered."""
    payload: str | None = None
    for line in answer.splitlines():
        match = _ANSWER_RE.match(line)
        if match:
            payload = match.group(1)
    detail: dict[str, Any] = {}
    if payload is None:
        return [], {"answer_line_found": False}
    detail["answer_line_found"] = True
    tokens = [t.strip() for t in payload.split(",") if t.strip()]
    ranked: list[int] = []
    seen: set[int] = set()
    unparsed: list[str] = []
    out_of_range: list[int] = []
    duplicates: list[int] = []
    for token in tokens:
        if not _INT_RE.match(token):
            unparsed.append(token[:64])
            continue
        value = int(token)
        if not 0 <= value < PRIME_ENTITY_COUNT:
            out_of_range.append(value)
            continue
        if value in seen:
            duplicates.append(value)
            continue
        seen.add(value)
        ranked.append(value)
    truncated = max(len(ranked) - TOP_K, 0)
    ranked = ranked[:TOP_K]
    if unparsed:
        detail["unparsed_tokens"] = unparsed[:16]
    if out_of_range:
        detail["out_of_range_ids"] = out_of_range[:16]
    if duplicates:
        detail["duplicate_ids"] = duplicates[:16]
    if truncated:
        detail["truncated_beyond_top_k"] = truncated
    return ranked, detail


def _score_ranked(ranked: list[int], gold: list[int]) -> dict[str, float]:
    """`stark_qa/evaluator.py`'s semantics for a STARK-RANK-1 emission.

    Equivalent to running the real `Evaluator` with `protocol_scores` —
    verified over 400 randomised trials against torch 2.9 / torchmetrics
    1.9.0 — and independent of `candidate_ids`, because P1/P2 mask every
    unnamed candidate out of MRR and recall and P3 keeps the floor tie
    block out of the top-k that hit@k reads."""
    gold_set = {int(g) for g in gold}
    hit_ranks = [i for i, node in enumerate(ranked) if node in gold_set]
    out: dict[str, float] = {
        "mrr": (1.0 / (hit_ranks[0] + 1)) if hit_ranks else 0.0,
    }
    for metric in METRICS:
        if metric == "mrr":
            continue
        kind, _, k_text = metric.partition("@")
        k = int(k_text)
        if kind == "hit":
            out[metric] = 1.0 if any(i < k for i in hit_ranks) else 0.0
        else:
            out[metric] = (
                len([i for i in hit_ranks if i < k]) / len(gold_set)
            )
    return out


# -- loading -------------------------------------------------------------


def _load_qa_rows(root: Path) -> list[dict[str, Any]]:
    path = root / Path(QA_CSV).name
    if not path.exists():
        raise DatasetError(
            f"stark-prime QA table is not cached at {path}; run "
            "`okg-bench fetch --benchmark stark-prime` first (network "
            "required once)"
        )
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    if columns != ["id", "query", "answer_ids"]:
        raise DatasetError(
            f"{path} has columns {columns}; the released Prime QA table has "
            "exactly id, query, answer_ids. A different shape means a "
            "different release and the split index files may no longer "
            "address the rows this adapter thinks they do"
        )
    if not rows:
        raise DatasetError(f"{path} holds zero questions")
    return rows


def read_split_ids(root: Path, split: str) -> list[int]:
    """Row ids for `split`, read from `split/<name>.index` in file order.

    Never derived by slicing, sorting or filtering the QA table: the table
    is in no split order, and the index files are the only statement of
    which question belongs to which split."""
    if split not in SPLIT_FILES:
        raise DatasetError(
            f"unknown stark-prime split {split!r}; the release ships "
            f"{sorted(SPLIT_FILES)}"
        )
    path = root / Path(SPLIT_FILES[split]).name
    if not path.exists():
        raise DatasetError(
            f"stark-prime split index {path} is not cached; run "
            "`okg-bench fetch --benchmark stark-prime` first"
        )
    ids: list[int] = []
    for lineno, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        text = line.strip()
        if not text:
            continue
        if not _INT_RE.match(text):
            raise DatasetError(
                f"{path} line {lineno} is {text[:32]!r}, not a row id; the "
                "split index is a line-list of integers"
            )
        ids.append(int(text))
    if not ids:
        raise DatasetError(f"{path} lists zero row ids")
    if len(set(ids)) != len(ids):
        raise DatasetError(f"{path} lists a row id twice")
    return ids


def _instances_for_split(
    rows: list[dict[str, Any]], root: Path, split: str
) -> list[TaskInstance]:
    ids = read_split_ids(root, split)
    # The release's 280-row test cut, read from its own index file so
    # `subset("test-0.1")` filters on a stamp the RELEASE wrote rather than
    # on a position in the loaded list.
    small = (
        set(read_split_ids(root, "test-0.1")) if split == DEFAULT_SPLIT
        else set()
    )
    stray = sorted(small - set(ids))
    if stray:
        raise DatasetError(
            f"test-0.1 names {len(stray)} row ids that test does not "
            f"(first: {stray[:8]}); the release states test-0.1 is a subset "
            "of test, so this pairing is from two different releases"
        )
    instances: list[TaskInstance] = []
    for row_id in ids:
        if not 0 <= row_id < len(rows):
            raise DatasetError(
                f"split {split!r} names row {row_id}, outside the QA table's "
                f"0..{len(rows) - 1} rows; the split files and the table are "
                "from different releases"
            )
        row = rows[row_id]
        instances.append(
            _instance(
                row, row_id=row_id, split=split, in_test_0_1=row_id in small
            )
        )
    return instances


def _instance(
    row: dict[str, Any], *, row_id: int, split: str, in_test_0_1: bool
) -> TaskInstance:
    query = str(row.get("query") or "").strip()
    if not query:
        raise DatasetError(f"stark-prime row {row_id} has no query text")
    answer_ids = _parse_answer_ids(row.get("answer_ids"), row_id=row_id)
    qid = str(row.get("id") or row_id)
    return TaskInstance(
        instance_id=f"stark-prime/{split}/{qid}",
        benchmark="stark-prime",
        prompt=f"{OPEN_BOOK_PREAMBLE}Question: {query}{INSTRUCTIONS}",
        gold={"answer_ids": answer_ids},
        extra={
            "split": split,
            "row_id": row_id,
            "query_id": qid,
            "in_test_0_1": in_test_0_1,
        },
    )


def _parse_answer_ids(raw: Any, *, row_id: int) -> list[int]:
    """The released `answer_ids` cell is a Python list literal."""
    if isinstance(raw, (list, tuple)):
        values = list(raw)
    else:
        text = str(raw or "").strip()
        if not text:
            raise DatasetError(
                f"stark-prime row {row_id} has an empty answer_ids cell"
            )
        try:
            values = ast.literal_eval(text)
        except (ValueError, SyntaxError) as exc:
            raise DatasetError(
                f"stark-prime row {row_id} answer_ids is not a literal list: "
                f"{text[:64]!r} ({exc})"
            ) from exc
    if not isinstance(values, (list, tuple)) or not values:
        raise DatasetError(
            f"stark-prime row {row_id} answer_ids is not a non-empty list"
        )
    out: list[int] = []
    for value in values:
        index = int(value)
        if not 0 <= index < PRIME_ENTITY_COUNT:
            raise DatasetError(
                f"stark-prime row {row_id} names answer entity {index}, "
                f"outside the release's 0..{PRIME_ENTITY_COUNT - 1} space"
            )
        out.append(index)
    return out


def _download(url: str) -> bytes:
    request = urllib.request.Request(
        url, headers={"User-Agent": "okg-bench/1"}
    )
    try:
        with urllib.request.urlopen(  # nosec B310 — pinned https URL
            request, timeout=_DOWNLOAD_TIMEOUT_S
        ) as response:
            return bytes(response.read())
    except OSError as exc:
        raise DatasetError(f"GET {url} failed: {exc}") from exc
