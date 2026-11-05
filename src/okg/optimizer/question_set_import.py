"""Import a golden-set export into the ``okg.question_set`` format.

A golden-set export ("export dir") is the folder format golden-set converters
write: ``benchmark.json`` (a list of items: id, question, answer, category,
expected_atoms [{id, text, required}], optional metadata.source_types),
``labels.json`` (question id to {tier, time_sensitive, atoms [{id}], ...}) and,
optionally, ``expected_sources.json`` ({items: question id to [source id]}).
The same folder may sit inside a zip under ``member_root``.

Mapping:
- origin comes from the caller (the registry entry); the export does not say
  who wrote its questions, so the importer never guesses.
- required facts are the expected atoms; ``required`` must be a boolean.
- scope tags: ``category:<c>`` (required), ``source_type:<s>`` per
  metadata.source_types, ``tier:<t>`` when the label is not null,
  ``time_sensitive`` when the label is true, ``domain:<d>`` and
  ``task_type:<t>`` when the labels carry them (null means no value; any
  other non-string is refused). metadata.question_type, where present, must
  equal the category and adds no tag: it is the upstream name of the same
  value, so a second tag would only double-count.
- expected sources become the question's ``source_refs``. The export names
  sources per question, not per atom, so facts carry no refs: copying every
  source onto every fact would claim a trace the export does not have.
- held-out ids come from the caller; an unknown or repeated id is refused.
- labels.json entries for ids not in benchmark.json, and labelled atoms a
  question does not have, are refused, as are unknown expected_sources ids.

Every gap is a named error, never a default.
"""
from __future__ import annotations

import ast
import json
import zipfile
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .question_sets import (
    QUESTION_SET_SCHEMA,
    QUESTION_SET_VERSION,
    QuestionSet,
    QuestionSetError,
    QuestionSetProblem,
    parse_question_set,
)


def _fail(code: str, message: str, pointer: str = "") -> QuestionSetError:
    return QuestionSetError([QuestionSetProblem(code, message, pointer)])


def _read_member(path: Path, member_root: str | None, name: str, *, optional: bool = False) -> Any:
    if path.is_dir():
        if member_root is not None:
            raise _fail("export_member_root_invalid", "member_root applies only to a zip export")
        target = path / name
        if not target.exists():
            if optional:
                return None
            raise _fail("export_file_missing", f"export has no {name}: {target}", f"/{name}")
        try:
            data = target.read_bytes()
        except OSError as exc:
            raise _fail("export_unreadable", f"cannot read {target}: {exc}", f"/{name}") from exc
    elif zipfile.is_zipfile(path):
        if member_root is None:
            raise _fail("export_member_root_missing", "a zip export needs member_root (the folder inside it)")
        member = member_root.rstrip("/") + "/" + name
        with zipfile.ZipFile(path) as z:
            if member not in z.namelist():
                if optional:
                    return None
                raise _fail("export_file_missing", f"zip has no member {member}", f"/{name}")
            data = z.read(member)
    else:
        raise _fail("export_not_dir_or_zip", f"export path is neither a folder nor a zip: {path}")
    try:
        return json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise _fail("export_parse", f"{name} is not valid JSON: {exc}", f"/{name}") from exc


def _atoms(raw: Any, qid: str) -> list[Any]:
    if isinstance(raw, str):
        try:
            raw = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise _fail("export_atoms_invalid", f"{qid}: expected_atoms text does not parse", f"/{qid}") from exc
    if not isinstance(raw, list):
        raise _fail("export_atoms_invalid", f"{qid}: expected_atoms must be a list", f"/{qid}")
    return raw


def _tags(item: Mapping[str, Any], label: Mapping[str, Any], qid: str) -> list[str]:
    category = item.get("category")
    if not isinstance(category, str) or not category.strip():
        raise _fail("export_category_missing", f"{qid}: item has no category", f"/{qid}/category")
    tags = [f"category:{category}"]
    metadata = item.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise _fail("export_metadata_invalid", f"{qid}: metadata must be a mapping", f"/{qid}/metadata")
    source_types = (metadata or {}).get("source_types")
    if source_types is not None:
        if not isinstance(source_types, list) or not all(isinstance(s, str) and s for s in source_types):
            raise _fail(
                "export_source_types_invalid", f"{qid}: metadata.source_types must be a list of strings",
                f"/{qid}/metadata/source_types",
            )
        tags.extend(f"source_type:{s}" for s in source_types)
    # metadata.question_type, where present, is the upstream name of the
    # category; it must agree and adds no tag of its own.
    question_type = (metadata or {}).get("question_type")
    if question_type is not None and question_type != category:
        raise _fail(
            "export_question_type_mismatch", f"{qid}: metadata.question_type differs from category",
            f"/{qid}/metadata/question_type",
        )
    tier = label["tier"]
    if tier is not None:
        if not ((type(tier) is int) or (isinstance(tier, str) and tier.strip() and tier == tier.strip())):
            raise _fail(
                "export_label_invalid", f"{qid}: labels tier must be null, an integer or a non-empty string",
                f"/{qid}/tier",
            )
        tags.append(f"tier:{tier}")
    if label["time_sensitive"] is True:
        tags.append("time_sensitive")
    elif label["time_sensitive"] is not False:
        raise _fail(
            "export_label_invalid", f"{qid}: labels time_sensitive must be true or false",
            f"/{qid}/time_sensitive",
        )
    for key in ("domain", "task_type"):
        value = label.get(key)
        if value is None:
            continue  # absent or null: the export states no value for this label
        if not (isinstance(value, str) and value.strip() and value == value.strip()):
            raise _fail(
                "export_label_invalid", f"{qid}: labels {key} must be null or a non-empty string",
                f"/{qid}/{key}",
            )
        tags.append(f"{key}:{value}")
    return tags


def import_golden_export(
    path: str | Path,
    *,
    set_id: str,
    origin: str,
    held_out: Iterable[str],
    member_root: str | None = None,
    description: str | None = None,
) -> QuestionSet:
    """Read a golden-set export and return it as a validated QuestionSet."""
    root = Path(path)
    bench = _read_member(root, member_root, "benchmark.json")
    labels = _read_member(root, member_root, "labels.json")
    sources_doc = _read_member(root, member_root, "expected_sources.json", optional=True)
    if not isinstance(bench, list):
        raise _fail("export_benchmark_invalid", "benchmark.json must be a list of items", "/benchmark.json")
    if not isinstance(labels, Mapping):
        raise _fail("export_labels_invalid", "labels.json must map question id to labels", "/labels.json")
    sources: Mapping[str, Any] = {}
    if sources_doc is not None:
        if not isinstance(sources_doc, Mapping) or not isinstance(sources_doc.get("items"), Mapping):
            raise _fail(
                "export_sources_invalid", "expected_sources.json must carry an items mapping",
                "/expected_sources.json",
            )
        sources = sources_doc["items"]
    held_list = list(held_out)
    held = set(held_list)
    if len(held) != len(held_list):
        raise _fail("held_out_duplicate_id", "held_out repeats a question id", "/held_out")
    ids = [item.get("id") if isinstance(item, Mapping) else None for item in bench]
    unknown_labels = sorted(set(labels) - set(i for i in ids if isinstance(i, str)), key=str)
    if unknown_labels:
        raise _fail(
            "export_labels_unknown_id",
            f"labels.json names ids not in benchmark.json: {', '.join(map(str, unknown_labels[:5]))}",
            "/labels.json",
        )
    unknown_held = sorted(held - set(i for i in ids if isinstance(i, str)))
    if unknown_held:
        raise _fail(
            "held_out_unknown_id", f"held_out names ids not in the export: {', '.join(unknown_held[:5])}",
            "/held_out",
        )
    unknown_sources = sorted(set(sources) - set(i for i in ids if isinstance(i, str)))
    if unknown_sources:
        raise _fail(
            "export_sources_unknown_id",
            f"expected_sources.json names ids not in benchmark.json: {', '.join(unknown_sources[:5])}",
            "/expected_sources.json",
        )

    questions: list[dict[str, Any]] = []
    for item in bench:
        if not isinstance(item, Mapping) or not isinstance(item.get("id"), str):
            raise _fail("export_item_invalid", "every benchmark item must be a mapping with a string id")
        qid = item["id"]
        label = labels.get(qid)
        if not isinstance(label, Mapping):
            raise _fail("export_label_missing", f"labels.json has no entry for {qid}", f"/{qid}")
        if "tier" not in label or "time_sensitive" not in label:
            raise _fail("export_label_missing", f"labels.json entry {qid} lacks tier or time_sensitive", f"/{qid}")
        label_atoms = label.get("atoms", [])
        if not isinstance(label_atoms, list) or not all(isinstance(a, Mapping) for a in label_atoms):
            raise _fail("export_label_invalid", f"{qid}: labels atoms must be a list of mappings", f"/{qid}/atoms")
        labelled_atoms = {a.get("id") for a in label_atoms}
        atoms = _atoms(item.get("expected_atoms"), qid)
        atom_ids = {a.get("id") for a in atoms if isinstance(a, Mapping)}
        extra = sorted(map(str, labelled_atoms - atom_ids))
        if extra:
            raise _fail(
                "export_label_atom_unknown",
                f"labels.json labels atom(s) {', '.join(extra[:5])} that {qid} does not have",
                f"/{qid}/atoms",
            )
        facts = []
        for atom in atoms:
            if not isinstance(atom, Mapping):
                raise _fail("export_atoms_invalid", f"{qid}: each atom must be a mapping", f"/{qid}")
            if atom.get("id") not in labelled_atoms:
                raise _fail(
                    "export_label_atom_missing", f"labels.json has no atom {atom.get('id')!r} for {qid}", f"/{qid}",
                )
            if not isinstance(atom.get("required"), bool):
                raise _fail(
                    "export_atom_required_invalid",
                    f"{qid}: atom {atom.get('id')!r} has no boolean required",
                    f"/{qid}",
                )
            facts.append({"id": atom["id"], "text": atom.get("text"), "required": atom["required"]})
        question: dict[str, Any] = {
            "id": qid,
            "question": item.get("question"),
            "origin": origin,
            "scope_tags": _tags(item, label, qid),
            "held_out": qid in held,
            "required_facts": facts,
        }
        answer = item.get("answer")
        if answer is not None:
            question["reference_answer"] = answer
        if qid in sources:
            question["source_refs"] = sources[qid]
        questions.append(question)

    doc: dict[str, Any] = {
        "schema": QUESTION_SET_SCHEMA,
        "version": QUESTION_SET_VERSION,
        "id": set_id,
        "questions": questions,
    }
    if description is not None:
        doc["description"] = description
    return parse_question_set(doc)
