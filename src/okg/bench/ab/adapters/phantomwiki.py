"""PhantomWiki adapter — a GENERATED universe, scored by set equality.

PhantomWiki (ICLR 2025, `kilian-group/phantom-wiki`; MIT for BOTH code and
data, stated in the repo LICENSE and twice on the HF card) generates a
fictional world from a seed: one biographical article per person, a Prolog
knowledge base of the same world, and multi-hop questions whose answers are
entity SETS. Regenerate at a new seed and the world has never existed, so
contamination is zero by construction — the reason this benchmark is in the
program at all.

GENERATED, NOT DOWNLOADED, and that is a measurement rather than a taste.
The largest released corpus split is 2,614,872 bytes over 5,030 articles,
about 0.65M tokens: it FITS INSIDE a long context window, so an arm could
read the entire corpus and the A/B would measure nothing. `fetch` therefore
refuses to download and names the `pw-generate` command instead. The
generation triple (question depth, universe size, seed) travels with every
instance, because a universe that cannot be regenerated from its recorded
parameters is not a corpus.

NESTED SPLITS. Released size splits at a FIXED SEED are not independent:
size_50 and size_5000 at seed 1 begin with the same people and identical
family sections. Any comparison across sizes must vary the seed, and this
adapter records the whole triple per instance so a nested comparison is
detectable after the fact rather than discovered in a reviewer's question.

SCORING IS THE UPSTREAM SCORER, READ NOT ASSUMED. `phantom_eval/score.py`
with `phantom_eval/utils.normalize_pred`: split on ",", strip, lowercase,
build a SET, and compare with `==`. Set EQUALITY, not intersection —
naming one of three correct people scores zero exact-match. Precision and
recall run over the same normalisation, and the gold list is joined with
the same separator before normalising (`evaluate_utils` does
`sep.join(x["true"])`). One sharp edge is preserved deliberately: an empty
prediction normalises to the ONE-ELEMENT set holding the empty string, so
precision is 0/1 rather than a division error. It is a zero, not a skip.

PACT okg-bench-graph-benchmarks / req.pw.adapter, task.pw.adapter.
"""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path
from typing import Any

from ..errors import DatasetError, ScoringError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

DATASET = "kilian-group/phantom-wiki"
LICENSE = "MIT (code AND data; repo LICENSE + HF card, stated twice)"

#: `phantom_eval.constants.answer_sep`.
ANSWER_SEP = ","

#: Fields of a released question record that must never reach an arm.
#: `prolog` holds the SOLVING QUERY — strictly worse to leak than the answer,
#: because it hands over the derivation and not just the result.
#: `solution_traces` is this generator's name for the intermediate bindings
#: (the HF release calls the same thing `intermediate_answers`); both are
#: listed so the guard covers either shape. `template` reveals the hop
#: structure and `difficulty` the hop count.
GOLD_BEARING_QUESTION_FIELDS = (
    "prolog",
    "solution_traces",
    "intermediate_answers",
    "difficulty",
    "template",
)

#: Snapshot artifacts that would hand an arm the structured world. The KB
#: and the per-article Prolog sidecars are not gold — they hold the same
#: facts the articles state — but a snapshot carrying them gives the PROSE
#: arm the structured encoding that is the treatment under test, which
#: destroys the contrast just as surely. `questions` and its `type*.json`
#: files carry the answers outright.
FORBIDDEN_SNAPSHOT_NAMES = (
    "facts.pl",
    "*_facts.txt",
    "questions",
    "type*.json",
)

SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"

OPEN_BOOK_PREAMBLE = (
    "You are answering a question about a fictional world. Every person in "
    "it is invented; nothing you already know applies. A read-only snapshot "
    "of the world's biographical articles — one plain-text file per person, "
    f"named for that person — is available under {SNAPSHOT_TOKEN}. It is far "
    "larger than your context window, so you will have to search it rather "
    "than read it.\n\n"
    "Answer with the names only, separated by commas, and nothing else. If "
    "several people satisfy the question, name all of them.\n\n"
)

#: The generation triple plus its digest, carried on every instance.
TRIPLE_KEYS = (
    "universe_seed",
    "universe_family_trees",
    "universe_max_family_tree_size",
    "universe_question_depth",
    "universe_id",
)

_TYPE_FILE = re.compile(r"^type(\d+)\.json$")


def normalize_pred(pred: str, sep: str = ANSWER_SEP) -> set[str]:
    """`phantom_eval.utils.normalize_pred`, transcribed.

    Upstream is exactly
    ``set(map(str.lower, map(str.strip, pred.split(sep))))``. Note what it
    does NOT do: it does not drop empty elements, so `""` normalises to
    `{""}` and a trailing comma adds an empty member. Both behaviours are
    load-bearing for the scores and are reproduced rather than tidied.
    """
    return set(map(str.lower, map(str.strip, pred.split(sep))))


def exact_match(pred: str, true: str, sep: str = ANSWER_SEP) -> bool:
    return normalize_pred(pred, sep) == normalize_pred(true, sep)


def precision(pred: str, true: str, sep: str = ANSWER_SEP) -> float:
    preds = normalize_pred(pred, sep)
    trues = normalize_pred(true, sep)
    # `preds` is never empty — `str.split` always yields at least one
    # element — so this never divides by zero and an empty prediction
    # scores 0/1 rather than raising or being skipped.
    return sum(1 for word in preds if word in trues) / len(preds)


def recall(pred: str, true: str, sep: str = ANSWER_SEP) -> float:
    preds = normalize_pred(pred, sep)
    trues = normalize_pred(true, sep)
    return sum(1 for word in trues if word in preds) / len(trues)


def f1(pred: str, true: str, sep: str = ANSWER_SEP) -> float:
    pres = precision(pred, true, sep)
    rec = recall(pred, true, sep)
    if pres + rec == 0:
        return 0.0
    return 2 * pres * rec / (pres + rec)


class PhantomWikiAdapter(BenchmarkAdapter):
    name = "phantomwiki"
    pilot_size = 20
    requires_corpus_config = True

    # -- data -----------------------------------------------------------
    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        """Verify a GENERATED instance is present. Never downloads.

        The released splits are deliberately not an option here; see the
        module docstring for the byte count that rules them out.
        """
        instance = self.instance_dir(data_root)
        if not (instance / "questions").is_dir():
            raise DatasetError(
                f"no generated PhantomWiki instance under {instance}. This "
                "benchmark is GENERATED, never downloaded: the largest "
                "released split is 2,614,872 bytes over 5,030 articles "
                "(~0.65M tokens) and fits inside a long context window, "
                "which would suppress the effect the A/B measures. "
                "Generate one with SWI-Prolog installed:\n"
                "  pw-generate --num-family-trees 2000 "
                "--max-family-tree-size 50 --question-depth 10 "
                "--num-questions-per-type 10 "
                f"--seed 1 --output-dir {instance}\n"
                "then record the generation triple in "
                f"{instance / 'generation.json'}.\n"
                "Depth 10 rather than 20 is a MEASURED choice: the article "
                "corpus is identical either way (byte-for-byte at a fixed "
                "seed and size), while question generation runs a Prolog "
                "query per sampled question over the whole universe and is "
                "roughly quadratic in it. At this size depth 10 finished in "
                "362s; depth 20 had not finished in over half an hour of "
                "CPU. Depth is part of the recorded triple, so it is a "
                "stated corpus parameter — just hold it fixed across arms."
            )
        meta = self.generation_meta(data_root)
        return {
            "dataset": DATASET,
            "revision": meta["universe_id"],
            "rows": len(self.load_instances(data_root=data_root)),
            "generation": meta,
        }

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        meta = self.generation_meta(data_root)
        return {
            "dataset": DATASET,
            "revision": meta["universe_id"],
            "license": LICENSE,
            "provenance": "generated",
            "generation": meta,
        }

    @staticmethod
    def instance_dir(data_root: Path) -> Path:
        return Path(data_root) / "phantomwiki" / "instance"

    def generation_meta(self, data_root: Path) -> dict[str, Any]:
        """The generation triple, read from the instance's provenance.

        Required, not defaulted. A universe whose parameters are unrecorded
        cannot be regenerated, cannot be checked for nesting against another
        universe, and is therefore not a corpus.
        """
        path = self.instance_dir(data_root) / "generation.json"
        if not path.is_file():
            raise DatasetError(
                f"generated instance at {self.instance_dir(data_root)} has no "
                "generation.json recording the generation triple "
                "(universe_seed, universe_family_trees, "
                "universe_max_family_tree_size, universe_question_depth). A "
                "universe that cannot be regenerated from its recorded "
                "parameters is not a corpus."
            )
        return self.validate_generation_meta(load_json(path))

    @staticmethod
    def validate_generation_meta(meta: Any) -> dict[str, Any]:
        if not isinstance(meta, dict):
            raise DatasetError("generation.json must hold a JSON object")
        missing = [k for k in TRIPLE_KEYS if meta.get(k) in (None, "")]
        if missing:
            raise DatasetError(
                f"generation.json is missing {missing}; the whole generation "
                "triple must travel with the corpus because size splits at a "
                "fixed seed are NESTED, not independent — size_50 and "
                "size_5000 at seed 1 begin with the same people and "
                "identical family sections"
            )
        return {k: meta[k] for k in TRIPLE_KEYS}

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            payload = load_json(offline_fixture)
            meta = self.validate_generation_meta(payload["generation"])
            per_type = payload["questions"]
        else:
            meta = self.generation_meta(data_root)
            per_type = self._read_question_files(data_root)
        instances: list[TaskInstance] = []
        for type_key in sorted(per_type, key=_type_sort_key):
            rows = per_type[type_key]
            if not isinstance(rows, list):
                raise DatasetError(
                    f"question file {type_key} does not hold a JSON list"
                )
            for idx, row in enumerate(rows):
                instances.append(_instance(type_key, idx, row, meta))
        if not instances:
            raise DatasetError(
                "the generated instance carries zero questions; regenerate "
                "with --num-questions-per-type >= 1"
            )
        return instances

    def _read_question_files(self, data_root: Path) -> dict[str, list[Any]]:
        root = self.instance_dir(data_root) / "questions"
        files = sorted(p for p in root.glob("type*.json"))
        if not files:
            raise DatasetError(
                f"{root} holds no type*.json question files; the generator "
                "writes one per question template"
            )
        return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in files}

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.instance_id)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        """Round-robin across question types rather than the first N.

        The generator writes one file per template and the templates are
        ordered by shape, so the first N of the ordered list would be one
        template's questions and the pilot would measure a single hop
        structure.
        """
        by_type: dict[str, list[TaskInstance]] = {}
        for inst in self.order(instances):
            by_type.setdefault(inst.extra["question_type"], []).append(inst)
        picked: list[TaskInstance] = []
        depth = 0
        while len(picked) < self.pilot_size:
            added = False
            for key in sorted(by_type, key=_type_sort_key):
                bucket = by_type[key]
                if depth < len(bucket):
                    picked.append(bucket[depth])
                    added = True
                    if len(picked) == self.pilot_size:
                        return picked
            if not added:
                break
            depth += 1
        return picked

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name.startswith("type"):
            picked = [i for i in instances if i.extra["question_type"] == name]
            if not picked:
                raise DatasetError(f"no phantomwiki questions of type {name!r}")
            return picked
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got {snapshot_dir!r})"
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
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        answers = instance.gold.get("answer")
        if not isinstance(answers, list) or not answers:
            raise ScoringError(
                f"{instance.instance_id} has no gold answer list"
            )
        true = ANSWER_SEP.join(str(a) for a in answers)
        # NOT special-cased into a skip. `None`/`""` normalises to the
        # one-element set holding the empty string, so precision is 0/1 and
        # exact match is False unless the gold is itself empty. Turning
        # that into an unscored task would quietly drop every refusal and
        # every timeout out of the denominator.
        pred = result.answer or ""
        em = exact_match(pred, true)
        return Score(
            instance_id=instance.instance_id,
            value=1.0 if em else 0.0,
            kind="set_exact_match",
            detail={
                "answered": sorted(normalize_pred(pred)),
                "gold": sorted(normalize_pred(true)),
                "precision": round(precision(pred, true), 4),
                "recall": round(recall(pred, true), 4),
                "f1": round(f1(pred, true), 4),
                **{k: instance.extra[k] for k in TRIPLE_KEYS},
            },
        )


def _type_sort_key(name: str) -> tuple[int, str]:
    match = _TYPE_FILE.match(f"{name}.json")
    return (int(match.group(1)), name) if match else (10**9, name)


def _instance(
    type_key: str, idx: int, row: Any, meta: dict[str, Any]
) -> TaskInstance:
    if not isinstance(row, dict):
        raise DatasetError(
            f"phantomwiki {type_key}[{idx}] is not a JSON object"
        )
    question = row.get("question")
    answer = row.get("answer")
    if not isinstance(question, str) or not question.strip():
        raise DatasetError(f"phantomwiki {type_key}[{idx}] has no question")
    if not isinstance(answer, list) or not answer:
        raise DatasetError(
            f"phantomwiki {type_key}[{idx}] has no answer list; the answer "
            "unit is an entity SET and a row without one is unscoreable"
        )
    question_id = row.get("id") or f"{type_key}-{idx:05d}"
    # The gold-bearing fields are dropped HERE, at the one place a released
    # row becomes an instance, so nothing downstream has to remember to.
    extra = {
        "question_type": type_key,
        "question_id": question_id,
        "answer_cardinality": len(answer),
        **meta,
    }
    leaked = sorted(set(GOLD_BEARING_QUESTION_FIELDS) & set(extra))
    if leaked:  # pragma: no cover - structural guard
        raise DatasetError(
            f"phantomwiki instance carries gold-bearing fields {leaked}"
        )
    return TaskInstance(
        instance_id=f"phantomwiki/{type_key}/{idx:05d}",
        benchmark="phantomwiki",
        prompt=f"{OPEN_BOOK_PREAMBLE}{question.strip()}",
        gold={"answer": [str(a) for a in answer]},
        extra=extra,
    )
