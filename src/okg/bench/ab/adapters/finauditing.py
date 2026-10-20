"""FinAuditing adapter (arXiv 2510.08886) — open-book agentic.

Datasets: HF ``TheFinAI/FinSM`` (330), ``TheFinAI/FinRE`` (440),
``TheFinAI/FinMR`` (332) — all CC-BY-4.0, single ``test`` split
(first-gate findings, PACT okg-bench-domain-adapters
designs/first-gates.md). Three task subsets:

- ``finsm`` — identify the erroneous ``us-gaap:*`` concepts (gold is a
  JSON array); scored by exact SET match with precision/recall detail.
- ``finre`` — 3-way error classification via the released
  ``choices``/``gold`` fields; scored by choice mapping (index, letter,
  or verbatim choice text — last occurrence wins).
- ``finmr`` — extracted/calculated value pair; scored by numeric
  comparison against gold at relative tolerance 1e-4.

Each dataset row's ``query`` embeds BOTH the prompt framing and the raw
XBRL document set of a real SEC filer, delimited by markdown-ish
headers ("##Schema document", "##Presentation document", ...).
``split_query`` re-splits it: framing stays in the prompt; the document
set is exported to per-filing files (``export_docsets``) that every arm
mounts read-only as the corpus snapshot — the okg arm additionally
reads the graph the ``edgar_xbrl`` substrate source builds from the
same files. A known trap: some rows carry a literal "None" body for a
sub-document; the instance is kept and the missing kinds are recorded
in ``extra`` (never silently dropped, never fabricated).

Prompts are arm-neutral: they name the snapshot directory via the
``{SNAPSHOT_DIR}`` token, bound at run time from the corpus freeze
manifest (``bind_corpus``); runs therefore REQUIRE ``--corpus-config``
for every arm, not just okg.

Scoring is deterministic — set/label/numeric match, no LLM judge. An
unparseable answer scores 0.0 with the reason in detail (never
unscored, never a silent default).
"""

from __future__ import annotations

import ast
import dataclasses
import json
import math
import re
from pathlib import Path
from typing import Any

from .. import hfdata
from ..errors import DatasetError
from ..model import Score, TaskInstance, TaskResult, load_json
from .base import BenchmarkAdapter

#: (config name, HF dataset id) — dataset order is the subset order.
CONFIG_DATASETS = (
    ("finsm", "TheFinAI/FinSM"),
    ("finre", "TheFinAI/FinRE"),
    ("finmr", "TheFinAI/FinMR"),
)
CONFIGS = tuple(cfg for cfg, _ in CONFIG_DATASETS)
DATASET_FOR = dict(CONFIG_DATASETS)
HF_CONFIG = "default"
SPLIT = "test"
LICENSE = "CC-BY-4.0"
SNAPSHOT_TOKEN = "{SNAPSHOT_DIR}"

OPEN_BOOK_PREAMBLE = (
    "You are auditing an SEC XBRL filing. A frozen snapshot of the "
    "benchmark's filing document sets is available read-only under "
    f"{SNAPSHOT_TOKEN}. Consult it however you like.\n\n"
)

# Pilot: 15 tasks, 5 per config, deterministic under the pinned dataset
# revision (first-N per config in dataset order).
PILOT_SPREAD = (("finsm", 5), ("finre", 5), ("finmr", 5))

#: "##Schema document", "## Presentation document:", ... (tolerant of
#: spacing, case, and a trailing colon; the kind may be multi-word).
_DOC_MARKER_RE = re.compile(
    r"^[ \t]*##[ \t]*(?P<kind>[A-Za-z][A-Za-z0-9 _-]*?)[ \t]+document"
    r"[ \t]*:?[ \t]*$",
    re.MULTILINE | re.IGNORECASE,
)
#: ANY "##..." section header. Real rows carry non-document sections too —
#: "##US GAAP Taxonomy" sits after the six XML documents, followed by the
#: actual question. A document body must stop at the next header of ANY
#: kind, or the last document swallows the taxonomy block and the question
#: (observed: the exported instance.xml was unparseable, and the question
#: text never reached the prompt).
_SECTION_RE = re.compile(r"^[ \t]*##[ \t]*\S.*$", re.MULTILINE)
_GAAP_CONCEPT_RE = re.compile(r"us-gaap:[A-Za-z0-9_]+")
_EXTRACTED_KEYS = ("extracted_value", "extracted")
_CALCULATED_KEYS = ("calculated_value", "calculated")
#: relative tolerance for finmr numeric comparison (req.dom.finauditing).
REL_TOL = 1e-4


class FinAuditingAdapter(BenchmarkAdapter):
    name = "finauditing"
    pilot_size = 15
    requires_corpus_config = True

    def fetch(self, *, data_root: Path) -> dict[str, Any]:
        refs: list[dict[str, Any]] = []
        total = 0
        for _cfg, dataset in CONFIG_DATASETS:
            rows, revision = hfdata.fetch_split(
                dataset, HF_CONFIG, SPLIT, data_root=data_root
            )
            refs.append(
                {"dataset": dataset, "revision": revision, "rows": len(rows)}
            )
            total += len(rows)
        return {"datasets": refs, "rows": total}

    def dataset_ref(self, *, data_root: Path) -> dict[str, Any]:
        refs: list[dict[str, Any]] = []
        for _cfg, dataset in CONFIG_DATASETS:
            meta = hfdata.cached_meta(dataset, data_root=data_root)
            refs.append(
                {
                    "dataset": dataset,
                    "revision": meta["revision"],
                    "license": LICENSE,
                }
            )
        return {"datasets": refs}

    def load_instances(
        self, *, data_root: Path, offline_fixture: Path | None = None
    ) -> list[TaskInstance]:
        if offline_fixture is not None:
            per_config: dict[str, list[dict[str, Any]]] = load_json(offline_fixture)
        else:
            per_config = {
                cfg: hfdata.load_cached_split(
                    DATASET_FOR[cfg], SPLIT, data_root=data_root
                )
                for cfg in CONFIGS
            }
        instances: list[TaskInstance] = []
        for cfg, rows in per_config.items():
            if cfg not in CONFIGS:
                raise DatasetError(
                    f"unknown finauditing config {cfg!r}; known: {CONFIGS}"
                )
            for idx, row in enumerate(rows):
                instances.append(_instance(cfg, idx, row))
        return instances

    def order(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        return sorted(instances, key=lambda i: i.instance_id)

    def pilot_subset(self, instances: list[TaskInstance]) -> list[TaskInstance]:
        by_cfg: dict[str, list[TaskInstance]] = {}
        for inst in self.order(instances):
            by_cfg.setdefault(inst.extra["config"], []).append(inst)
        picked: list[TaskInstance] = []
        for cfg, count in PILOT_SPREAD:
            picked.extend(by_cfg.get(cfg, [])[:count])
        return picked

    def subset(self, instances: list[TaskInstance], name: str) -> list[TaskInstance]:
        if name in CONFIGS:
            return [i for i in instances if i.extra["config"] == name]
        return super().subset(instances, name)

    # -- open-book binding ---------------------------------------------
    def bind_corpus(
        self, instances: list[TaskInstance], corpus_cfg: dict[str, Any]
    ) -> list[TaskInstance]:
        """Substitute the snapshot dir into every prompt and mount it via
        add_dirs. FinAuditing runs are open-book for EVERY arm, so the
        corpus freeze manifest is required regardless of arm."""
        snapshot_dir = corpus_cfg.get("snapshot_dir")
        if not snapshot_dir or not Path(snapshot_dir).is_dir():
            raise DatasetError(
                f"corpus config has no usable snapshot_dir (got {snapshot_dir!r}); "
                "export the re-split filing document sets into one with "
                "FinAuditingAdapter.export_docsets and freeze it"
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

    # -- docset export --------------------------------------------------
    def export_docsets(
        self, instances: list[TaskInstance], dest_dir: Path
    ) -> dict[str, Any]:
        """Write every instance's re-split XBRL document set under
        ``<dest_dir>/docsets/<slug>/<doc_kind>.xml``.

        Both the okg arm's corpus build (the ``edgar_xbrl`` source's
        ``docsets_root``) and the vanilla arms' read-only mount consume
        exactly these files. Deterministic (instance-id order); literal
        "None" documents are skipped and counted; an existing docsets
        dir refuses loudly rather than silently overwriting a frozen
        snapshot."""
        docsets_dir = Path(dest_dir) / "docsets"
        if docsets_dir.exists():
            raise DatasetError(
                f"docsets dir {docsets_dir} already exists; refusing to "
                "overwrite a (possibly frozen) snapshot — remove it first"
            )
        files = 0
        skipped_missing = 0
        ordered = self.order(instances)
        for inst in ordered:
            docset = inst.extra.get("docset")
            slug = inst.extra.get("docset_slug")
            if not isinstance(docset, dict) or not slug:
                raise DatasetError(
                    f"instance {inst.instance_id!r} carries no re-split "
                    "docset; it was not loaded by the finauditing adapter"
                )
            filing_dir = docsets_dir / slug
            filing_dir.mkdir(parents=True)
            for kind, xml_text in docset.items():
                if xml_text is None:
                    skipped_missing += 1
                    continue
                (filing_dir / f"{kind}.xml").write_text(
                    xml_text, encoding="utf-8"
                )
                files += 1
        return {
            "docsets_dir": str(docsets_dir),
            "filings": len(ordered),
            "files": files,
            "skipped_missing": skipped_missing,
        }

    # -- scoring --------------------------------------------------------
    def score_task(
        self, *, run_dir: Path, data_root: Path, instance: TaskInstance,
        result: TaskResult,
    ) -> Score:
        cfg = instance.extra["config"]
        answer = result.answer or ""
        if cfg == "finsm":
            return _score_finsm(instance, answer)
        if cfg == "finre":
            return _score_finre(instance, answer)
        if cfg == "finmr":
            return _score_finmr(instance, answer)
        raise DatasetError(f"unscoreable config {cfg!r}")  # unreachable by load


# ---------- query re-split ----------


def split_query(query: str) -> tuple[str, dict[str, str | None], str]:
    """Split a dataset ``query`` into (task framing, ordered doc set).

    Everything before the first ``##<Kind> document`` marker is the
    task framing; each marker starts one embedded XBRL document whose
    body runs to the next marker (or the end). A literal "None" (or
    blank) body is the dataset's marker for a missing sub-document —
    the doc kind is kept with a ``None`` body so callers can record the
    absence instead of silently dropping it. A query with no document
    markers at all is malformed and raises.

    Real released rows (verified 2026-07-27 against TheFinAI/FinSM row 0)
    store the cell DOUBLE-ENCODED: a JSON-quoted string starting with a
    literal '"' whose newlines are backslash escapes. Decode that form
    exactly once before splitting; anything else about the shape stays a
    loud error."""
    matches = list(_DOC_MARKER_RE.finditer(query))
    if not matches and query.lstrip().startswith('"') and "\\n" in query:
        try:
            decoded = json.loads(query)
        except (ValueError, TypeError) as exc:
            raise DatasetError(
                "query looks JSON-quoted but does not decode: "
                f"{exc} (starts {query[:80]!r})"
            ) from exc
        if not isinstance(decoded, str):
            raise DatasetError(
                f"JSON-quoted query decoded to {type(decoded).__name__}, "
                "expected a string"
            )
        query = decoded
        matches = list(_DOC_MARKER_RE.finditer(query))
    if not matches:
        raise DatasetError(
            "query embeds no '##<kind> document' markers — cannot re-split "
            f"the XBRL document set (query starts {query[:80]!r})"
        )
    framing = query[: matches[0].start()].strip()
    # Every section boundary, document or not: a doc body ends at the next
    # one. Without this the final document absorbs "##US GAAP Taxonomy" and
    # the trailing question.
    boundaries = [m.start() for m in _SECTION_RE.finditer(query)]
    docs: dict[str, str | None] = {}
    for match in matches:
        kind = (
            match.group("kind").strip().lower().replace("-", " ")
        )
        kind = "_".join(kind.split())
        if kind in docs:
            raise DatasetError(
                f"query declares doc kind {kind!r} twice — malformed row"
            )
        nexts = [b for b in boundaries if b > match.start()]
        end = nexts[0] if nexts else len(query)
        body = query[match.end(): end].strip()
        docs[kind] = None if (not body or body.lower() == "none") else body
    # Trailer = the non-document sections after the last document plus
    # anything following them. This is where the released rows keep the
    # US GAAP taxonomy context AND THE QUESTION ITSELF, so it belongs in
    # the prompt — dropping it left the agent no question to answer.
    last_doc_end = max(
        ([b for b in boundaries if b > m.start()] or [len(query)])[0]
        for m in matches
    )
    trailer = query[last_doc_end:].strip()
    return framing, docs, trailer


def docset_slug(instance_id: str) -> str:
    """Flat, filesystem-safe per-filing directory name (the substrate
    source's ``<filing_slug>`` contract wants ONE directory level)."""
    return instance_id.replace("/", "__")


# ---------- instance construction ----------


def _instance(cfg: str, idx: int, row: dict[str, Any]) -> TaskInstance:
    query = row.get("query")
    answer = row.get("answer")
    if not query or not isinstance(query, str):
        raise DatasetError(
            f"finauditing row {cfg}/{idx} missing query: keys={sorted(row)}"
        )
    if cfg != "finre" and not answer:
        raise DatasetError(
            f"finauditing row {cfg}/{idx} missing answer: keys={sorted(row)}"
        )
    framing, docs, trailer = split_query(query)
    instance_id = f"finauditing/{cfg}/{idx:04d}"
    slug = docset_slug(instance_id)
    present = [k for k, v in docs.items() if v is not None]
    missing = [k for k, v in docs.items() if v is None]

    if cfg == "finsm":
        gold = {"concepts": _gold_concepts(cfg, idx, answer)}
        choices_block = ""
    elif cfg == "finre":
        choices = row.get("choices")
        gold_idx = row.get("gold")
        if not isinstance(choices, list) or not choices:
            raise DatasetError(
                f"finre row {idx} missing/empty choices: keys={sorted(row)}"
            )
        if not isinstance(gold_idx, int) or isinstance(gold_idx, bool) or not (
            0 <= gold_idx < len(choices)
        ):
            raise DatasetError(
                f"finre row {idx} gold {gold_idx!r} is not an index into "
                f"{len(choices)} choices"
            )
        gold = {"gold": gold_idx, "choices": [str(c) for c in choices]}
        choices_block = (
            "\n\nChoices:\n"
            + "\n".join(
                f"{chr(65 + i)}. {c}" for i, c in enumerate(gold["choices"])
            )
            + "\n\nEnd your response with a line 'ANSWER: <letter>'."
        )
    else:  # finmr
        gold = {"pair": _gold_pair(cfg, idx, answer)}
        choices_block = ""

    pointer = (
        f"\n\nThis filing's XBRL document set "
        f"({', '.join(present) if present else 'no documents present'}) is "
        f"available as files under {SNAPSHOT_TOKEN}/docsets/{slug}/"
        "<doc_kind>.xml."
    )
    # The trailer carries the released row's US GAAP taxonomy context AND
    # the question itself — without it the agent has instructions but
    # nothing to answer.
    if not trailer:
        raise DatasetError(
            f"finauditing row {cfg}/{idx} has no post-document trailer; the "
            "question text lives there — refusing a question-less prompt"
        )
    question_block = f"\n\n{trailer}"
    return TaskInstance(
        instance_id=instance_id,
        benchmark="finauditing",
        prompt=(
            f"{OPEN_BOOK_PREAMBLE}{framing}{pointer}{question_block}"
            f"{choices_block}"
        ),
        gold=gold,
        extra={
            "config": cfg,
            "dqc_id": row.get("dqc_id"),
            "source_id": row.get("id"),
            "docset_slug": slug,
            "doc_kinds": present,
            "missing_docs": missing,
            "docset": docs,
        },
    )


def _parse_json_loose(text: str) -> Any:
    """Parse a gold payload that is JSON — or the JSON-ish single-quoted
    repr some HF exports carry. The released rows are DOUBLE-ENCODED
    (verified 2026-07-27: FinSM answer decodes to the string
    "['us-gaap:PreferredStockValue']"), so when one parse yields a plain
    string, unwrap exactly one more level — bounded, never a loop. Raises
    ValueError when nothing parses."""

    def _once(value: str) -> Any:
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            pass
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError) as exc:
            raise ValueError(
                f"not JSON or a Python literal: {value[:120]!r}"
            ) from exc

    parsed = _once(text)
    if isinstance(parsed, str):
        parsed = _once(parsed)
    return parsed


def _gold_concepts(cfg: str, idx: int, answer: Any) -> list[str]:
    try:
        parsed = _parse_json_loose(str(answer))
    except ValueError as exc:
        raise DatasetError(
            f"{cfg} row {idx} answer is not a parseable concept array: {exc}"
        ) from exc
    if not isinstance(parsed, list) or not all(
        isinstance(c, str) and c.strip() for c in parsed
    ):
        raise DatasetError(
            f"{cfg} row {idx} answer must be a JSON array of concept "
            f"strings, got {parsed!r}"
        )
    if not parsed:
        # A FinSM instance with ZERO erroneous concepts is malformed gold:
        # an agent answering [] would vacuously score 1.0 (review D4).
        raise DatasetError(f"{cfg} row {idx} gold concept array is empty")
    return [c.strip() for c in parsed]


def _gold_pair(cfg: str, idx: int, answer: Any) -> dict[str, float]:
    try:
        parsed = _parse_json_loose(str(answer))
    except ValueError as exc:
        raise DatasetError(
            f"{cfg} row {idx} answer is not a parseable value pair: {exc}"
        ) from exc
    pair = _pair_from_obj(parsed) if isinstance(parsed, dict) else None
    if pair is None:
        raise DatasetError(
            f"{cfg} row {idx} answer must be a JSON object with "
            f"extracted_value/calculated_value, got {parsed!r}"
        )
    extracted, calculated = _to_number(pair[0]), _to_number(pair[1])
    if extracted is None or calculated is None:
        raise DatasetError(
            f"{cfg} row {idx} gold values are not numeric: {pair!r}"
        )
    return {"extracted_value": extracted, "calculated_value": calculated}


# ---------- scorers (all deterministic) ----------


def _score_finsm(instance: TaskInstance, answer: str) -> Score:
    gold = instance.gold["concepts"]
    gold_set = {c.casefold() for c in gold}
    answered = _extract_concept_list(answer)
    if answered is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="finsm",
            detail={
                "reason": "no JSON array or us-gaap concept list in answer",
                "gt": sorted(gold),
            },
        )
    ans_set = {c.casefold() for c in answered}
    tp = len(ans_set & gold_set)
    precision = tp / len(ans_set) if ans_set else (1.0 if not gold_set else 0.0)
    recall = tp / len(gold_set) if gold_set else (1.0 if not ans_set else 0.0)
    return Score(
        instance_id=instance.instance_id,
        value=1.0 if ans_set == gold_set else 0.0,
        kind="finsm",
        detail={
            "answered": sorted(set(answered)),
            "gt": sorted(gold),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
        },
    )


def _extract_concept_list(answer: str) -> list[str] | None:
    """The answer's LAST JSON array of strings; else the ``us-gaap:*``
    tokens mentioned (comma-list fallback). None when neither parses."""
    last_array: list[str] | None = None
    for candidate in re.findall(r"\[[^][]*\]", answer or ""):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list) and all(isinstance(c, str) for c in parsed):
            last_array = [c.strip() for c in parsed]
    if last_array is not None:
        return last_array
    tokens = _GAAP_CONCEPT_RE.findall(answer or "")
    return tokens or None


def _score_finre(instance: TaskInstance, answer: str) -> Score:
    choices = instance.gold["choices"]
    gold_idx = instance.gold["gold"]
    mapped = _map_choice(answer, choices)
    if mapped is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="finre",
            detail={
                "reason": "answer names no choice (index, letter, or text)",
                "gt": gold_idx,
                "choices": choices,
            },
        )
    idx, via = mapped
    return Score(
        instance_id=instance.instance_id,
        value=1.0 if idx == gold_idx else 0.0,
        kind="finre",
        detail={
            "answered": idx,
            "answered_choice": choices[idx],
            "matched_via": via,
            "gt": gold_idx,
        },
    )


def _map_choice(answer: str, choices: list[str]) -> tuple[int, str] | None:
    """Map an answer onto a choice index. Accepted forms, most to least
    specific: verbatim choice text anywhere (last occurrence wins), or an
    ANCHORED letter — an `ANSWER: X` line or a final line that is just the
    letter. Bare digits and free-floating letters are NOT accepted: prose
    like "the two values differ by 1" or a sentence-initial article "A"
    must never be read as a choice (adversarial-review D3), and digit
    answers are 0/1-base ambiguous against a lettered prompt."""
    low = (answer or "").casefold()
    text_hits: list[tuple[int, int]] = []  # (end_pos, idx)
    for i, choice in enumerate(choices):
        text = str(choice).strip().casefold()
        if not text:
            continue
        pos = low.rfind(text)
        if pos >= 0:
            text_hits.append((pos + len(text), i))
    if text_hits:
        _, idx = max(text_hits)
        return idx, "text"
    letters = "".join(chr(65 + i) for i in range(min(len(choices), 26)))
    anchored = re.findall(
        rf"^\s*ANSWER\s*:\s*([{letters}])\b",
        answer or "", re.IGNORECASE | re.MULTILINE,
    )
    if anchored:
        return ord(anchored[-1].upper()) - 65, "letter"
    lines = [ln.strip() for ln in (answer or "").splitlines() if ln.strip()]
    if lines and re.fullmatch(rf"[{letters}]\.?", lines[-1], re.IGNORECASE):
        return ord(lines[-1][0].upper()) - 65, "letter"
    return None


def _score_finmr(instance: TaskInstance, answer: str) -> Score:
    gold = instance.gold["pair"]
    pair = _last_answer_pair(answer)
    if pair is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="finmr",
            detail={
                "reason": "no JSON object with extracted/calculated values "
                          "in answer",
                "gt": gold,
            },
        )
    extracted, calculated = _to_number(pair[0]), _to_number(pair[1])
    if extracted is None or calculated is None:
        return Score(
            instance_id=instance.instance_id, value=0.0, kind="finmr",
            detail={
                "reason": f"non-numeric answer values {pair!r}",
                "gt": gold,
            },
        )
    matches = {
        "extracted_match": _close(extracted, gold["extracted_value"]),
        "calculated_match": _close(calculated, gold["calculated_value"]),
    }
    return Score(
        instance_id=instance.instance_id,
        value=1.0 if all(matches.values()) else 0.0,
        kind="finmr",
        detail={
            "answered": {
                "extracted_value": extracted, "calculated_value": calculated,
            },
            "gt": gold,
            **matches,
        },
    )


def _close(answered: float, gold: float) -> bool:
    return math.isclose(answered, gold, rel_tol=REL_TOL, abs_tol=1e-9)


def _last_answer_pair(answer: str) -> tuple[Any, Any] | None:
    """The LAST JSON object in the answer carrying both value keys
    (accepting the extracted/calculated minor key variants)."""
    last: tuple[Any, Any] | None = None
    for candidate in _iter_json_objects(answer or ""):
        try:
            parsed = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        pair = _pair_from_obj(parsed)
        if pair is not None:
            last = pair
    return last


def _pair_from_obj(obj: dict[str, Any]) -> tuple[Any, Any] | None:
    normalized = {
        str(k).strip().lower().replace(" ", "_"): v for k, v in obj.items()
    }
    extracted = next(
        (normalized[k] for k in _EXTRACTED_KEYS if k in normalized), None
    )
    calculated = next(
        (normalized[k] for k in _CALCULATED_KEYS if k in normalized), None
    )
    if extracted is None or calculated is None:
        return None
    return extracted, calculated


def _iter_json_objects(text: str):
    """Yield balanced ``{...}`` candidate substrings, string-aware."""
    depth = 0
    start = None
    in_string = False
    escaped = False
    for i, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            if depth > 0:
                in_string = True
        elif ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}" and depth > 0:
            depth -= 1
            if depth == 0 and start is not None:
                yield text[start: i + 1]
                start = None


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    text = value.strip().replace(",", "").replace("$", "").rstrip("%").strip()
    negative = text.startswith("(") and text.endswith(")")
    if negative:
        text = text[1:-1].strip()
    try:
        number = float(text)
    except ValueError:
        return None
    return -number if negative else number
