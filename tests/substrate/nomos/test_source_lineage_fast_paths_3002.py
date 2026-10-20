"""The lineage helpers' fast paths give the old answers (issue #3002).

Every Nomos read rebuilds each returned row's source lineage and stamps the
row with its own copy, so ``_fingerprint`` encodes once and
``attrs_with_lineage`` copies JSON-shaped envelopes without ``deepcopy``.
"""

from __future__ import annotations

import copy
import json
from collections import OrderedDict
from typing import Any

import pytest

from okg.substrate.nomos import source_lineage
from okg.substrate.nomos.source_lineage import (
    LINEAGE_ATTR,
    attrs_with_lineage,
    lineage_for_row,
    normalize_source_lineage,
)


def _old_json_safe(value: Any) -> Any:
    try:
        json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value)
    return value


def _old_fingerprint(value: Any) -> str:
    return json.dumps(_old_json_safe(value), sort_keys=True, separators=(",", ":"))


class _Opaque:
    def __repr__(self) -> str:
        return "<opaque>"


VALUES: list[Any] = [
    None, True, 0, 1.5, "", " id ", "x", [], {}, (1, "a"),
    {"b": 1, "a": [1, {"z": None}]},
    {1: "int key", 2: "x"},
    {1: "mixed", "a": "keys"},  # sort_keys raises TypeError: falls back to str
    {"s": {1, 2}},
    _Opaque(),
    [{"source_record_id": {"id": "r"}, "source_revision": {"v": 1}}],
]


@pytest.mark.parametrize("value", VALUES, ids=repr)
def test_fingerprint_matches_the_two_encode_version(value: Any) -> None:
    assert source_lineage._fingerprint(value) == _old_fingerprint(value)


def test_fingerprint_still_raises_on_a_cycle() -> None:
    loop: list[Any] = []
    loop.append(loop)
    with pytest.raises(ValueError):
        source_lineage._fingerprint(loop)
    with pytest.raises(ValueError):
        _old_fingerprint(loop)


ENVELOPES: list[Any] = [
    None,
    "src-a",
    ["src-b", "src-a", "src-a"],
    {"source_ids": ["b", "a"]},
    {"sources": [{"source_id": "a", "source_records": [
        {"source_record_id": {"id": "r2"}, "source_revision": {"v": 2}},
        {"source_record_id": {"id": "r1"}, "source_revision": {"v": 1}},
        {"source_record_id": {"id": "r1"}, "source_revision": {"v": 1}},
    ], "policy_fingerprints": ["f2", "f1", "f2"]}]},
    {"sources": [{"source_id": "a", "source_record_ids": ["r1", "r2"],
                  "source_revisions": [1, 2], "record_detail_truncated": True}]},
    {"sources": [{"id": None}], "status": "ok"},
    {"sources": [{"source_id": "a", "source_records": [{"id": {1, 2}}]}]},
]


#: normalize_source_lineage(envelope, source_id="col-src", ...) for each of
#: ENVELOPES, computed with the code before issue #3002's change (dev
#: 6c29d520776), as canonical JSON.
BEFORE_3002 = [
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"col-src\"], \"sources\": [{\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"col-src\", \"src-a\"], \"sources\": [{\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}, {\"source_id\": \"src-a\"}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"col-src\", \"src-a\", \"src-b\"], \"sources\": [{\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}, {\"source_id\": \"src-a\"}, {\"source_id\": \"src-b\"}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"a\", \"b\", \"col-src\"], \"sources\": [{\"source_id\": \"a\"}, {\"source_id\": \"b\"}, {\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"a\", \"col-src\"], \"sources\": [{\"policy_fingerprints\": [\"f1\", \"f2\"], \"source_id\": \"a\", \"source_record_ids\": [{\"id\": \"r1\"}, {\"id\": \"r2\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"r1\"}, \"source_revision\": {\"v\": 1}}, {\"source_record_id\": {\"id\": \"r2\"}, \"source_revision\": {\"v\": 2}}], \"source_revisions\": [{\"v\": 1}, {\"v\": 2}]}, {\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
    "{\"record_detail\": \"truncated\", \"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"a\", \"col-src\"], \"sources\": [{\"record_detail_truncated\": true, \"source_id\": \"a\", \"source_record_ids\": [\"r1\", \"r2\"], \"source_records\": [{\"source_record_id\": \"r1\", \"source_revision\": 1}, {\"source_record_id\": \"r2\", \"source_revision\": 2}], \"source_revisions\": [1, 2]}, {\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"col-src\"], \"sources\": [{\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
    "{\"schema\": \"okg.nomos.source_lineage.v1\", \"source_ids\": [\"a\", \"col-src\"], \"sources\": [{\"source_id\": \"a\", \"source_record_ids\": [\"{1, 2}\"], \"source_records\": [{\"source_record_id\": \"{1, 2}\"}]}, {\"source_id\": \"col-src\", \"source_record_ids\": [{\"id\": \"c\"}], \"source_records\": [{\"source_record_id\": {\"id\": \"c\"}, \"source_revision\": {\"v\": 9}}], \"source_revisions\": [{\"v\": 9}]}], \"status\": \"ok\"}",
]


@pytest.mark.parametrize(("envelope", "before"), list(zip(ENVELOPES, BEFORE_3002)), ids=repr)
def test_normalize_is_unchanged_and_idempotent(envelope: Any, before: str) -> None:
    once = normalize_source_lineage(envelope, source_id="col-src",
                                    source_record_id={"id": "c"}, source_revision={"v": 9})
    assert json.dumps(once, sort_keys=True) == before
    assert normalize_source_lineage(once) == normalize_source_lineage(
        copy.deepcopy(once))
    row = lineage_for_row({LINEAGE_ATTR: envelope} if envelope is not None else {},
                          source_id="col-src", source_record_id={"id": "c"},
                          source_revision={"v": 9})
    assert row == normalize_source_lineage(
        envelope, **({} if envelope is not None else {
            "source_id": "col-src", "source_record_id": {"id": "c"},
            "source_revision": {"v": 9}}))


def test_attrs_with_lineage_copies_like_deepcopy_and_shares_nothing_mutable() -> None:
    lineage = normalize_source_lineage(ENVELOPES[4], source_id="x")
    stamped = attrs_with_lineage({"text": "t"}, lineage)
    assert stamped == {"text": "t", LINEAGE_ATTR: copy.deepcopy(lineage)}
    assert json.dumps(stamped[LINEAGE_ATTR], sort_keys=True) == json.dumps(
        lineage, sort_keys=True)
    stamped[LINEAGE_ATTR]["sources"][0]["source_records"].append("mutated")
    stamped[LINEAGE_ATTR]["source_ids"].append("mutated")
    assert "mutated" not in json.dumps(lineage)


def test_unfamiliar_values_go_to_deepcopy() -> None:
    class Tagged(dict):
        pass

    inner = Tagged(a=[1, 2])
    value = {"plain": [{"k": "v"}], "tagged": inner, "ordered": OrderedDict(b=[3]),
             "tuple": ([4], "s")}
    copied = source_lineage._copy_json_tree(value)
    assert copied == copy.deepcopy(value)
    assert type(copied["tagged"]) is Tagged and copied["tagged"] is not inner
    assert type(copied["ordered"]) is OrderedDict
    assert copied["tuple"][0] is not value["tuple"][0]
    copied["tagged"]["a"].append(9)
    copied["ordered"]["b"].append(9)
    assert inner == {"a": [1, 2]} and value["ordered"] == {"b": [3]}
