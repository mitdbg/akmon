"""Question sets: one format, a per-deployment registry, coverage, held-out view.

Every evaluation and the optimizer read questions in one format
(schema ``okg.question_set``, version 1). Each question records where it came
from (``origin``), the facts a correct answer must contain (each optionally
traced to source records), the scope tags it covers, whether it is held out
from the optimizing loop, and, for answers read from a live system, the
snapshot or as-of version the answer is checked against.

A deployment declares its sets in ``question_sets.yaml`` (id to format, path
and pinned digest). The id is the one ``objectives.yaml`` names in
``question_set:``, and :attr:`QuestionSet.digest` is the value a measurement
record carries as ``question_set.digest``. Private sets stay outside the
repository; the registry names them by path (``${ENV}`` allowed) and digest.

Nothing here defaults a missing field: an unknown key, a duplicate id, a
missing origin or held-out flag, and an empty fact list are each refused with a
named error code. Pure at import: no substrate, database, or CLI imports;
registry path expansion borrows OKG's shared ``${NAME}`` expander when called.

Loading by id: ``Registry.load`` returns the loop view (held-out questions
withheld) by default; only ``Registry.load_with_held_out`` returns them.

Entry point: ``python -m okg.optimizer.question_sets coverage --registry
<question_sets.yaml> --set <id>`` prints the question count, the digest and a
coverage table per scope tag (counts only, never question text).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml


QUESTION_SET_SCHEMA = "okg.question_set"
QUESTION_SET_VERSION = 1
REGISTRY_VERSION = 1
ORIGINS = ("operator", "resolved_ticket", "generated")
# Questions a person asked of the system for real. A generated question counts
# for coverage only on a scope tag no real question covers.
REAL_ORIGINS = frozenset({"operator", "resolved_ticket"})
DIGEST_PREFIX = "sha256:"
REGISTRY_FORMATS = ("okg.question_set", "golden_export_dir")

_DOC_REQUIRED = ("schema", "version", "id", "questions")
_DOC_KEYS = frozenset(_DOC_REQUIRED) | {"description"}
_QUESTION_REQUIRED = ("id", "question", "origin", "scope_tags", "held_out", "required_facts")
_QUESTION_KEYS = frozenset(_QUESTION_REQUIRED) | {"source_refs", "reference_answer", "as_of"}
_FACT_REQUIRED = ("id", "text", "required")
_FACT_KEYS = frozenset(_FACT_REQUIRED) | {"source_refs"}
_AS_OF_KEYS = frozenset({"snapshot", "version", "at"})


# --- errors ----------------------------------------------------------------


@dataclass(frozen=True)
class QuestionSetProblem:
    """One refusal, with a stable code and a JSON pointer into the input."""

    code: str
    message: str
    pointer: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "pointer": self.pointer}


class QuestionSetError(ValueError):
    def __init__(self, problems: Sequence[QuestionSetProblem]) -> None:
        self.problems = tuple(problems)
        first = self.problems[0]
        more = f" (+{len(self.problems) - 1} more)" if len(self.problems) > 1 else ""
        super().__init__(f"{first.code}: {first.message}{more}")

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(p.code for p in self.problems)


class HeldOutQuestionError(LookupError):
    """A caller holding the loop view asked for a held-out question."""

    code = "question_held_out"


# --- records ---------------------------------------------------------------


@dataclass(frozen=True)
class RequiredFact:
    id: str
    text: str
    required: bool
    source_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class AsOf:
    """The environment state a live-system answer is checked against."""

    snapshot: str | None = None
    version: str | None = None
    at: str | None = None


@dataclass(frozen=True)
class Question:
    id: str
    question: str
    origin: str
    scope_tags: tuple[str, ...]
    held_out: bool
    required_facts: tuple[RequiredFact, ...]
    source_refs: tuple[str, ...] = ()
    reference_answer: str | None = None
    as_of: AsOf | None = None


@dataclass(frozen=True)
class QuestionSet:
    id: str
    questions: tuple[Question, ...]
    digest: str
    description: str | None = None

    def question(self, question_id: str) -> Question:
        for q in self.questions:
            if q.id == question_id:
                return q
        raise KeyError(question_id)

    def loop_view(self) -> LoopView:
        return loop_view(self)


class LoopView:
    """The questions the optimizing loop may see: held-out ones removed.

    It keeps only the ids of held-out questions, never their text, and
    :meth:`question` refuses a held-out id with :class:`HeldOutQuestionError`
    instead of returning it.
    """

    def __init__(self, question_set: QuestionSet) -> None:
        self.set_id = question_set.id
        self.digest = question_set.digest
        self.questions: tuple[Question, ...] = tuple(
            q for q in question_set.questions if not q.held_out
        )
        self._held_out_ids = frozenset(q.id for q in question_set.questions if q.held_out)

    @property
    def held_out_count(self) -> int:
        return len(self._held_out_ids)

    def question(self, question_id: str) -> Question:
        if question_id in self._held_out_ids:
            raise HeldOutQuestionError(
                f"question {question_id!r} of set {self.set_id!r} is held out; "
                "the loop view does not hand out held-out questions"
            )
        for q in self.questions:
            if q.id == question_id:
                return q
        raise KeyError(question_id)

    def __iter__(self) -> Iterator[Question]:
        return iter(self.questions)

    def __len__(self) -> int:
        return len(self.questions)


def loop_view(question_set: QuestionSet) -> LoopView:
    """The non-held-out view the optimizing loop reads."""
    return LoopView(question_set)


# --- YAML / JSON reading ---------------------------------------------------


class _UniqueKeyLoader(yaml.SafeLoader):
    """safe_load that refuses a repeated mapping key instead of keeping the last."""


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False):
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark,
            )
        seen.add(key)
    return loader.construct_mapping(node, deep=deep)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping,
)


def _read_structured(path: Path, what: str) -> Any:
    """Read a YAML or JSON file (JSON is YAML), refusing repeated keys."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise QuestionSetError([
            QuestionSetProblem(f"{what}_file_missing", f"{what} file not found: {path}"),
        ]) from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise QuestionSetError([
            QuestionSetProblem(f"{what}_unreadable", f"{what} file cannot be read: {exc}"),
        ]) from exc
    try:
        return yaml.load(text, Loader=_UniqueKeyLoader)  # noqa: S506 - SafeLoader subclass
    except yaml.YAMLError as exc:
        # PyYAML's message quotes the offending line, which may be private or
        # held-out question text. Report only where it failed, and drop the
        # chained exception so a traceback cannot print it either.
        mark = getattr(exc, "problem_mark", None) or getattr(exc, "context_mark", None)
        where = f" at line {mark.line + 1}, column {mark.column + 1}" if mark is not None else ""
        raise QuestionSetError([
            QuestionSetProblem(
                f"{what}_parse", f"{what} file does not parse ({type(exc).__name__}){where}",
            ),
        ]) from None


# --- validation ------------------------------------------------------------


def _non_empty_str(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _token(value: Any) -> bool:
    """An id, tag or ref: a non-empty string with no surrounding whitespace."""
    return _non_empty_str(value) and value == value.strip()


def _is_int(value: Any, expected: int) -> bool:
    """Exactly the integer ``expected``: not True, not 1.0."""
    return type(value) is int and value == expected


def _check_keys(
    entry: Mapping[str, Any],
    allowed: frozenset[str],
    required: Iterable[str],
    pointer: str,
    out: list[QuestionSetProblem],
    *,
    missing_codes: Mapping[str, str] | None = None,
) -> None:
    for key in sorted(set(entry) - allowed, key=str):
        out.append(QuestionSetProblem("unknown_key", f"unknown key {key!r}", f"{pointer}/{key}"))
    for key in required:
        if key not in entry:
            code = (missing_codes or {}).get(key, "missing_key")
            out.append(QuestionSetProblem(code, f"required key {key!r} is missing", f"{pointer}/{key}"))


def _refs(value: Any, pointer: str, out: list[QuestionSetProblem]) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(_token(v) for v in value):
        out.append(QuestionSetProblem(
            "source_refs_invalid",
            "source_refs must be a list of non-empty strings without surrounding whitespace", pointer,
        ))
        return ()
    if len(set(value)) != len(value):
        out.append(QuestionSetProblem("source_refs_duplicate", "source_refs repeats a ref", pointer))
    return tuple(value)


def _parse_fact(
    raw: Any, pointer: str, seen: set[str], out: list[QuestionSetProblem],
) -> RequiredFact | None:
    if not isinstance(raw, Mapping):
        out.append(QuestionSetProblem("fact_not_mapping", "a required fact must be a mapping", pointer))
        return None
    before = len(out)
    _check_keys(raw, _FACT_KEYS, _FACT_REQUIRED, pointer, out)
    fact_id = raw.get("id")
    if "id" in raw and not _token(fact_id):
        out.append(QuestionSetProblem("fact_id_invalid", "fact id must be a non-empty string without surrounding whitespace", f"{pointer}/id"))
    elif _token(fact_id):
        if fact_id in seen:
            out.append(QuestionSetProblem(
                "duplicate_fact_id", f"fact id {fact_id!r} is used twice in one question", f"{pointer}/id",
            ))
        seen.add(fact_id)
    if "text" in raw and not _non_empty_str(raw["text"]):
        out.append(QuestionSetProblem("fact_text_invalid", "fact text must be a non-empty string", f"{pointer}/text"))
    if "required" in raw and not isinstance(raw["required"], bool):
        out.append(QuestionSetProblem(
            "fact_required_invalid", "fact required must be true or false", f"{pointer}/required",
        ))
    refs = _refs(raw["source_refs"], f"{pointer}/source_refs", out) if "source_refs" in raw else ()
    if len(out) != before:
        return None
    return RequiredFact(id=fact_id, text=raw["text"], required=raw["required"], source_refs=refs)


def _parse_as_of(raw: Any, pointer: str, out: list[QuestionSetProblem]) -> AsOf | None:
    if not isinstance(raw, Mapping) or not raw:
        out.append(QuestionSetProblem(
            "as_of_invalid", "as_of must be a mapping with at least one of snapshot, version, at", pointer,
        ))
        return None
    before = len(out)
    _check_keys(raw, _AS_OF_KEYS, (), pointer, out)
    for key in sorted(_AS_OF_KEYS & set(raw)):
        if not _non_empty_str(raw[key]):
            out.append(QuestionSetProblem(
                "as_of_invalid", f"as_of {key} must be a non-empty string", f"{pointer}/{key}",
            ))
    if len(out) != before:
        return None
    return AsOf(snapshot=raw.get("snapshot"), version=raw.get("version"), at=raw.get("at"))


def _parse_question(
    raw: Any, pointer: str, seen: set[str], out: list[QuestionSetProblem],
) -> Question | None:
    if not isinstance(raw, Mapping):
        out.append(QuestionSetProblem("question_not_mapping", "a question must be a mapping", pointer))
        return None
    before = len(out)
    _check_keys(
        raw, _QUESTION_KEYS, _QUESTION_REQUIRED, pointer, out,
        missing_codes={
            "origin": "origin_missing",
            "held_out": "held_out_missing",
            "required_facts": "required_facts_missing",
            "scope_tags": "scope_tags_missing",
        },
    )
    qid = raw.get("id")
    if "id" in raw and not _token(qid):
        out.append(QuestionSetProblem(
            "question_id_invalid", "question id must be a non-empty string", f"{pointer}/id",
        ))
    elif _token(qid):
        if qid in seen:
            out.append(QuestionSetProblem(
                "duplicate_question_id", f"question id {qid!r} is used twice", f"{pointer}/id",
            ))
        seen.add(qid)
    if "question" in raw and not _non_empty_str(raw["question"]):
        out.append(QuestionSetProblem(
            "question_text_invalid", "question text must be a non-empty string", f"{pointer}/question",
        ))
    if "origin" in raw and raw["origin"] not in ORIGINS:
        out.append(QuestionSetProblem(
            "origin_invalid",
            f"origin {raw['origin']!r} is not one of {', '.join(ORIGINS)}",
            f"{pointer}/origin",
        ))
    tags = raw.get("scope_tags")
    if "scope_tags" in raw:
        if not isinstance(tags, list) or not all(_token(t) for t in tags):
            out.append(QuestionSetProblem(
                "scope_tags_invalid",
                "scope_tags must be a list of non-empty strings without surrounding whitespace", f"{pointer}/scope_tags",
            ))
        elif not tags:
            out.append(QuestionSetProblem(
                "scope_tags_empty", "a question needs at least one scope tag", f"{pointer}/scope_tags",
            ))
        elif len(set(tags)) != len(tags):
            out.append(QuestionSetProblem(
                "scope_tag_duplicate", "scope_tags repeats a tag", f"{pointer}/scope_tags",
            ))
    if "held_out" in raw and not isinstance(raw["held_out"], bool):
        out.append(QuestionSetProblem(
            "held_out_invalid", "held_out must be true or false", f"{pointer}/held_out",
        ))
    facts: list[RequiredFact] = []
    if "required_facts" in raw:
        raw_facts = raw["required_facts"]
        if not isinstance(raw_facts, list):
            out.append(QuestionSetProblem(
                "required_facts_invalid", "required_facts must be a list", f"{pointer}/required_facts",
            ))
        elif not raw_facts:
            out.append(QuestionSetProblem(
                "required_facts_empty", "a question needs at least one required fact",
                f"{pointer}/required_facts",
            ))
        else:
            fact_ids: set[str] = set()
            for i, rf in enumerate(raw_facts):
                fact = _parse_fact(rf, f"{pointer}/required_facts/{i}", fact_ids, out)
                if fact is not None:
                    facts.append(fact)
            if len(facts) == len(raw_facts) and not any(f.required for f in facts):
                out.append(QuestionSetProblem(
                    "no_required_fact",
                    "every fact is marked required: false; a question needs at least one required fact",
                    f"{pointer}/required_facts",
                ))
    refs = _refs(raw["source_refs"], f"{pointer}/source_refs", out) if "source_refs" in raw else ()
    if "reference_answer" in raw and not _non_empty_str(raw["reference_answer"]):
        out.append(QuestionSetProblem(
            "reference_answer_invalid", "reference_answer must be a non-empty string",
            f"{pointer}/reference_answer",
        ))
    as_of = _parse_as_of(raw["as_of"], f"{pointer}/as_of", out) if "as_of" in raw else None
    if len(out) != before:
        return None
    return Question(
        id=qid,
        question=raw["question"],
        origin=raw["origin"],
        scope_tags=tuple(tags),
        held_out=raw["held_out"],
        required_facts=tuple(facts),
        source_refs=refs,
        reference_answer=raw.get("reference_answer"),
        as_of=as_of,
    )


def parse_question_set(raw: Any) -> QuestionSet:
    """Validate one parsed question-set document; raise QuestionSetError on refusal.

    Every problem in the document is collected before raising, so one run
    lists them all.
    """
    if not isinstance(raw, Mapping):
        raise QuestionSetError([
            QuestionSetProblem("document_not_mapping", "a question set must be a mapping"),
        ])
    out: list[QuestionSetProblem] = []
    _check_keys(raw, _DOC_KEYS, _DOC_REQUIRED, "", out)
    if "schema" in raw and raw["schema"] != QUESTION_SET_SCHEMA:
        out.append(QuestionSetProblem(
            "schema_invalid", f"schema must be {QUESTION_SET_SCHEMA!r}, got {raw['schema']!r}", "/schema",
        ))
    version = raw.get("version")
    if "version" in raw and not _is_int(version, QUESTION_SET_VERSION):
        out.append(QuestionSetProblem(
            "version_invalid", f"version must be the integer {QUESTION_SET_VERSION}, got {version!r}",
            "/version",
        ))
    if "id" in raw and not _token(raw["id"]):
        out.append(QuestionSetProblem("set_id_invalid", "set id must be a non-empty string", "/id"))
    if "description" in raw and not _non_empty_str(raw["description"]):
        out.append(QuestionSetProblem(
            "description_invalid", "description must be a non-empty string", "/description",
        ))
    questions: list[Question] = []
    if "questions" in raw:
        raw_questions = raw["questions"]
        if not isinstance(raw_questions, list):
            out.append(QuestionSetProblem("questions_invalid", "questions must be a list", "/questions"))
        elif not raw_questions:
            out.append(QuestionSetProblem(
                "questions_empty", "a question set needs at least one question", "/questions",
            ))
        else:
            seen: set[str] = set()
            for i, rq in enumerate(raw_questions):
                q = _parse_question(rq, f"/questions/{i}", seen, out)
                if q is not None:
                    questions.append(q)
    if out:
        raise QuestionSetError(out)
    ordered = tuple(sorted(questions, key=lambda q: q.id))
    shell = QuestionSet(id=raw["id"], questions=ordered, digest="", description=raw.get("description"))
    return QuestionSet(
        id=shell.id, questions=ordered, digest=question_set_digest(shell), description=shell.description,
    )


# --- canonical form and digest ---------------------------------------------


def _fact_document(fact: RequiredFact) -> dict[str, Any]:
    doc: dict[str, Any] = {"id": fact.id, "text": fact.text, "required": fact.required}
    if fact.source_refs:
        doc["source_refs"] = list(fact.source_refs)
    return doc


def _question_document(q: Question) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "id": q.id,
        "question": q.question,
        "origin": q.origin,
        "scope_tags": sorted(q.scope_tags),
        "held_out": q.held_out,
        "required_facts": [_fact_document(f) for f in q.required_facts],
    }
    if q.source_refs:
        doc["source_refs"] = list(q.source_refs)
    if q.reference_answer is not None:
        doc["reference_answer"] = q.reference_answer
    if q.as_of is not None:
        doc["as_of"] = {
            k: v
            for k, v in (("snapshot", q.as_of.snapshot), ("version", q.as_of.version), ("at", q.as_of.at))
            if v is not None
        }
    return doc


def question_set_document(question_set: QuestionSet) -> dict[str, Any]:
    """The canonical document: questions sorted by id, scope tags sorted.

    Re-parsing it yields the same set and the same digest.
    """
    doc: dict[str, Any] = {
        "schema": QUESTION_SET_SCHEMA,
        "version": QUESTION_SET_VERSION,
        "id": question_set.id,
    }
    if question_set.description is not None:
        doc["description"] = question_set.description
    doc["questions"] = [
        _question_document(q) for q in sorted(question_set.questions, key=lambda q: q.id)
    ]
    return doc


def question_set_digest(question_set: QuestionSet) -> str:
    """sha256 over the canonical JSON of the canonical document.

    Key order, file layout, question order and scope-tag order do not change
    it; any change to a question, fact, flag, ref or the set id does. This is
    the value a measurement record carries as ``question_set.digest``.
    """
    canonical = json.dumps(
        question_set_document(question_set), sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return DIGEST_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def load_question_set_file(path: str | Path) -> QuestionSet:
    """Read and validate one ``okg.question_set`` file (YAML or JSON)."""
    return parse_question_set(_read_structured(Path(path), "question_set"))


# --- registry --------------------------------------------------------------


@dataclass(frozen=True)
class RegistryEntry:
    id: str
    format: str
    path: str
    digest: str
    origin: str | None = None
    held_out: tuple[str, ...] | None = None
    member_root: str | None = None


@dataclass(frozen=True)
class Registry:
    """A deployment's declared question sets.

    Loading contract (the replay runner #4279 and the optimizer rely on it):
    :meth:`load` is the default and returns the :class:`LoopView`, so a
    caller that does not ask otherwise never receives held-out text. A
    scorer that must read held-out questions, and the coverage report, call
    :meth:`load_with_held_out` by name.
    """

    path: Path
    entries: Mapping[str, RegistryEntry]

    def load(self, set_id: str, environ: Mapping[str, str] | None = None) -> LoopView:
        """The loop view: held-out questions withheld."""
        return loop_view(load_registered_set(self, set_id, environ))

    def load_with_held_out(self, set_id: str, environ: Mapping[str, str] | None = None) -> QuestionSet:
        """The full set, held-out questions included: for scoring and coverage only."""
        return load_registered_set(self, set_id, environ)


_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ENTRY_KEYS: Mapping[str, tuple[frozenset[str], tuple[str, ...]]] = {
    "okg.question_set": (frozenset({"format", "path", "digest"}), ("format", "path", "digest")),
    "golden_export_dir": (
        frozenset({"format", "path", "digest", "origin", "held_out", "member_root"}),
        ("format", "path", "digest", "origin", "held_out"),
    ),
}


def load_registry(path: str | Path) -> Registry:
    """Read a deployment's ``question_sets.yaml``; raise QuestionSetError on refusal.

    Shape::

        version: 1
        question_sets:
          <id>:
            format: okg.question_set | golden_export_dir
            path: <file or dir; relative to this file, absolute, or ${ENV}>
            digest: sha256:<64 hex>       # canonical digest the set must have
            origin: operator              # golden_export_dir only
            held_out: []                  # golden_export_dir only: held-out ids
            member_root: <dir in zip>     # golden_export_dir from a .zip only
    """
    registry_path = Path(path)
    raw = _read_structured(registry_path, "registry")
    out: list[QuestionSetProblem] = []
    if not isinstance(raw, Mapping):
        raise QuestionSetError([
            QuestionSetProblem("registry_not_mapping", "the registry must be a mapping"),
        ])
    _check_keys(raw, frozenset({"version", "question_sets"}), ("version", "question_sets"), "", out)
    version = raw.get("version")
    if "version" in raw and not _is_int(version, REGISTRY_VERSION):
        out.append(QuestionSetProblem(
            "registry_version_invalid", f"registry version must be {REGISTRY_VERSION}, got {version!r}",
            "/version",
        ))
    entries: dict[str, RegistryEntry] = {}
    sets = raw.get("question_sets")
    if "question_sets" in raw and (not isinstance(sets, Mapping) or not sets):
        out.append(QuestionSetProblem(
            "registry_sets_invalid", "question_sets must be a non-empty mapping of id to entry",
            "/question_sets",
        ))
    elif isinstance(sets, Mapping):
        for set_id, entry in sets.items():
            pointer = f"/question_sets/{set_id}"
            if not _token(set_id):
                out.append(QuestionSetProblem("set_id_invalid", "set id must be a non-empty string", pointer))
                continue
            parsed = _parse_entry(set_id, entry, pointer, out)
            if parsed is not None:
                entries[set_id] = parsed
    if out:
        raise QuestionSetError(out)
    return Registry(path=registry_path, entries=MappingProxyType(entries))


def _parse_entry(
    set_id: str, entry: Any, pointer: str, out: list[QuestionSetProblem],
) -> RegistryEntry | None:
    if not isinstance(entry, Mapping):
        out.append(QuestionSetProblem(
            "registry_entry_not_mapping", "a registry entry must be a mapping", pointer,
        ))
        return None
    fmt = entry.get("format")
    if fmt not in _ENTRY_KEYS:
        out.append(QuestionSetProblem(
            "registry_format_invalid",
            f"format {fmt!r} is not one of {', '.join(REGISTRY_FORMATS)}",
            f"{pointer}/format",
        ))
        return None
    before = len(out)
    allowed, required = _ENTRY_KEYS[fmt]
    _check_keys(entry, allowed, required, pointer, out)
    if "path" in entry and not _non_empty_str(entry["path"]):
        out.append(QuestionSetProblem(
            "registry_path_invalid", "path must be a non-empty string", f"{pointer}/path",
        ))
    if "digest" in entry and not (isinstance(entry["digest"], str) and _DIGEST_RE.match(entry["digest"])):
        out.append(QuestionSetProblem(
            "registry_digest_invalid", "digest must be sha256:<64 lowercase hex>", f"{pointer}/digest",
        ))
    if "origin" in entry and entry["origin"] not in ORIGINS:
        out.append(QuestionSetProblem(
            "origin_invalid", f"origin {entry['origin']!r} is not one of {', '.join(ORIGINS)}",
            f"{pointer}/origin",
        ))
    held_out = entry.get("held_out")
    if "held_out" in entry and (
        not isinstance(held_out, list) or not all(_token(h) for h in held_out)
    ):
        out.append(QuestionSetProblem(
            "registry_held_out_invalid", "held_out must be a list of question ids (empty for none)",
            f"{pointer}/held_out",
        ))
    elif "held_out" in entry and len(set(held_out)) != len(held_out):
        out.append(QuestionSetProblem(
            "registry_held_out_duplicate", "held_out repeats a question id", f"{pointer}/held_out",
        ))
    if "member_root" in entry and not _non_empty_str(entry["member_root"]):
        out.append(QuestionSetProblem(
            "registry_member_root_invalid", "member_root must be a non-empty string",
            f"{pointer}/member_root",
        ))
    if len(out) != before:
        return None
    return RegistryEntry(
        id=set_id,
        format=fmt,
        path=entry["path"],
        digest=entry["digest"],
        origin=entry.get("origin"),
        held_out=tuple(held_out) if held_out is not None else None,
        member_root=entry.get("member_root"),
    )


def resolve_entry_path(
    registry: Registry, entry: RegistryEntry, environ: Mapping[str, str] | None = None,
) -> Path:
    """Expand ``${ENV}`` references and resolve relative to the registry file.

    Placeholders go through OKG's one shared expander
    (``okg.substrate.db.dsn.expand``), so a path follows the same rules as
    every other ``${NAME}`` value: defaults and commands are refused, unset
    and empty are reported. Imported here so loading this module stays free
    of substrate imports.
    """
    from okg.substrate.db import dsn

    result = dsn.expand(entry.path, os.environ if environ is None else environ)
    if result.unsupported:
        raise QuestionSetError([QuestionSetProblem(
            "question_set_path_env_unsupported",
            f"set {entry.id!r} path uses placeholder syntax that is not expanded: "
            f"{', '.join(result.unsupported)}; only ${{NAME}} expands",
            f"/question_sets/{entry.id}/path",
        )])
    missing = [*result.unset, *result.empty]
    if missing or result.value is None:
        raise QuestionSetError([QuestionSetProblem(
            "question_set_path_env_unset",
            f"set {entry.id!r} path needs environment variable(s) {', '.join(missing)}, "
            "which are unset or empty",
            f"/question_sets/{entry.id}/path",
        )])
    resolved = Path(result.value).expanduser()
    if not resolved.is_absolute():
        resolved = registry.path.parent / resolved
    return resolved


def load_registered_set(
    registry: Registry, set_id: str, environ: Mapping[str, str] | None = None,
) -> QuestionSet:
    """Load a set by id and check its digest against the registry's pin."""
    entry = registry.entries.get(set_id)
    if entry is None:
        known = ", ".join(sorted(registry.entries)) or "none"
        raise QuestionSetError([QuestionSetProblem(
            "question_set_unknown", f"set {set_id!r} is not in {registry.path} (known: {known})",
            f"/question_sets/{set_id}",
        )])
    path = resolve_entry_path(registry, entry, environ)
    if not path.exists():
        raise QuestionSetError([QuestionSetProblem(
            "question_set_file_missing", f"set {set_id!r} path does not exist: {path}",
            f"/question_sets/{set_id}/path",
        )])
    if entry.format == "okg.question_set":
        qs = load_question_set_file(path)
    else:
        from .question_set_import import import_golden_export

        if entry.origin is None or entry.held_out is None:  # _ENTRY_KEYS requires both
            raise QuestionSetError([QuestionSetProblem(
                "missing_key", f"set {set_id!r} needs origin and held_out", f"/question_sets/{set_id}",
            )])
        qs = import_golden_export(
            path,
            set_id=set_id,
            origin=entry.origin,
            held_out=entry.held_out,
            member_root=entry.member_root,
        )
    if qs.id != set_id:
        raise QuestionSetError([QuestionSetProblem(
            "question_set_id_mismatch", f"registry id {set_id!r} loads a set whose id is {qs.id!r}",
            f"/question_sets/{set_id}",
        )])
    if qs.digest != entry.digest:
        raise QuestionSetError([QuestionSetProblem(
            "question_set_digest_mismatch",
            f"set {set_id!r} has digest {qs.digest}, registry pins {entry.digest}",
            f"/question_sets/{set_id}/digest",
        )])
    return qs


# --- coverage --------------------------------------------------------------


@dataclass(frozen=True)
class TagCoverage:
    tag: str
    by_origin: Mapping[str, int]
    held_out: int
    total: int
    # Questions that count for this tag: the real ones if any exist,
    # otherwise the generated ones.
    counted: int
    counted_from: str  # "real" or "generated_only"
    generated_not_counted: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "tag": self.tag,
            "by_origin": dict(self.by_origin),
            "held_out": self.held_out,
            "total": self.total,
            "counted": self.counted,
            "counted_from": self.counted_from,
            "generated_not_counted": self.generated_not_counted,
        }


def coverage_report(question_set: QuestionSet) -> tuple[TagCoverage, ...]:
    """Per scope tag: counts by origin, held-out count, and counted questions.

    The rule: a generated question counts only where no operator or
    resolved_ticket question covers that scope tag. Held-out questions count
    toward coverage like any other, by their origin (a held-out operator
    question makes a tag "real"); ``held_out`` says how many of them the
    loop cannot see.
    """
    tags: dict[str, list[Question]] = {}
    for q in question_set.questions:
        for tag in q.scope_tags:
            tags.setdefault(tag, []).append(q)
    rows = []
    for tag in sorted(tags):
        qs = tags[tag]
        by_origin = {o: sum(1 for q in qs if q.origin == o) for o in ORIGINS}
        real = sum(by_origin[o] for o in REAL_ORIGINS)
        generated = by_origin["generated"]
        rows.append(TagCoverage(
            tag=tag,
            by_origin=MappingProxyType(by_origin),
            held_out=sum(1 for q in qs if q.held_out),
            total=len(qs),
            counted=real if real else generated,
            counted_from="real" if real else "generated_only",
            generated_not_counted=generated if real else 0,
        ))
    return tuple(rows)


def summary(question_set: QuestionSet) -> dict[str, Any]:
    """Counts and digest only: safe to print for a private set."""
    qs = question_set.questions
    facts = [f for q in qs for f in q.required_facts]
    return {
        "id": question_set.id,
        "digest": question_set.digest,
        "questions": len(qs),
        "by_origin": {o: sum(1 for q in qs if q.origin == o) for o in ORIGINS},
        "held_out": sum(1 for q in qs if q.held_out),
        "loop_view_questions": sum(1 for q in qs if not q.held_out),
        "facts": len(facts),
        "required_facts": sum(1 for f in facts if f.required),
        "facts_with_source_refs": sum(1 for f in facts if f.source_refs),
        "questions_with_source_refs": sum(1 for q in qs if q.source_refs),
        "questions_with_as_of": sum(1 for q in qs if q.as_of is not None),
        "scope_tags": len({t for q in qs for t in q.scope_tags}),
    }


def render_coverage(question_set: QuestionSet) -> str:
    s = summary(question_set)
    origins = ", ".join(f"{o} {n}" for o, n in s["by_origin"].items())
    lines = [
        f"question set {s['id']}: {s['questions']} questions, digest {s['digest']}",
        f"  origin: {origins}; held out {s['held_out']}; loop view {s['loop_view_questions']}",
        f"  facts {s['facts']} (required {s['required_facts']}, with source refs "
        f"{s['facts_with_source_refs']}); questions with source refs "
        f"{s['questions_with_source_refs']}; with as_of {s['questions_with_as_of']}",
        "",
    ]
    header = ("scope tag", "operator", "ticket", "generated", "held out", "counted", "counted from")
    rows = [header] + [
        (
            r.tag, str(r.by_origin["operator"]), str(r.by_origin["resolved_ticket"]),
            str(r.by_origin["generated"]), str(r.held_out), str(r.counted), r.counted_from,
        )
        for r in coverage_report(question_set)
    ]
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    for row in rows:
        lines.append("  " + "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip())
    return "\n".join(lines)


# --- entry point -----------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m okg.optimizer.question_sets",
        description="Validate a question set and print counts, digest and per-scope-tag coverage "
        "(never question text).",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_cov = sub.add_parser("coverage", help="print the digest and the coverage report per scope tag")
    src = p_cov.add_mutually_exclusive_group(required=True)
    src.add_argument("--registry", help="a deployment's question_sets.yaml (with --set)")
    src.add_argument("--file", help="one okg.question_set file")
    p_cov.add_argument("--set", help="set id in the registry")
    p_cov.add_argument("--json", action="store_true", help="print JSON instead of a table")
    args = parser.parse_args(argv)
    if args.registry and not args.set:
        parser.error("--registry needs --set")
    if args.file and args.set:
        parser.error("--set applies only with --registry")
    try:
        if args.file:
            qs = load_question_set_file(args.file)
        else:
            # Coverage counts held-out questions; it prints counts only.
            qs = load_registry(args.registry).load_with_held_out(args.set)
    except QuestionSetError as exc:
        if args.json:
            print(json.dumps({"ok": False, "problems": [p.as_dict() for p in exc.problems]}, indent=2))
        else:
            for p in exc.problems:
                print(f"{p.code}: {p.message} [{p.pointer}]", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(
            {"ok": True, "summary": summary(qs), "coverage": [r.as_dict() for r in coverage_report(qs)]},
            indent=2,
        ))
    else:
        print(render_coverage(qs))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    raise SystemExit(main())
