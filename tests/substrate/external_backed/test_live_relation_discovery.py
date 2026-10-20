"""`inspect` lists a subtype's live relations with their argument schemas.

task.lre.discover for issue #2013. Hermetic: the catalog read is faked, so
what is proved here is the CONTRACT -- that the published block is the
compiled declaration, that an agent can read an argument schema without
guessing, and that "declares none" is absent rather than empty.
"""
from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.external_backed.live_relations import (
    ANNOTATION_KEY,
    compile_args_model,
    parse_live_relations,
)
from okg.substrate.mcp.operator_models import OperatorResult

HITS = {
    "deadline_seconds": 5,
    "action": "external.opensearch.search",
    "sample_policy": "digest",
    "retention_class": "warm",
    "args": {
        "q": {"type": "string", "required": True},
        "size": {"type": "integer", "default": 10},
    },
}


class _FakeCursor:
    def __init__(self, metadata: dict[str, Any]) -> None:
        self._metadata = metadata
        self._rows: Any = None

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple) -> None:
        self._rows = [(self._metadata,)] if "okg.subtypes" in sql else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows or [])


class _FakeConn:
    def __init__(self, metadata: dict[str, Any]) -> None:
        self._metadata = metadata

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._metadata)


class _FakeSession:
    catalog_version_id = 1


def _shape(metadata: dict[str, Any]) -> dict[str, Any]:
    from okg.substrate.mcp.subtype_summary import catalog_subtype_shape

    return catalog_subtype_shape(
        _FakeConn(metadata), _FakeSession(), "search_index",
    )


def _stored() -> dict[str, Any]:
    """What the CATALOG stores: the whole compiled declaration.

    `inspect` renders the caller-facing subset from this. Storing only the
    published subset was the earlier shape and could not be dispatched from,
    because the deadline and the principal mode were lost.
    """
    relations = parse_live_relations(
        {ANNOTATION_KEY: {"hits": dict(HITS)}}, subtype="search_index",
    )
    return {name: rel.to_metadata() for name, rel in relations.items()}


def test_inspect_lists_live_relations_with_their_argument_schemas() -> None:
    shape = _shape({"live_relations": _stored()})
    assert "live_relations" in shape
    hits = shape["live_relations"]["hits"]
    assert hits["action"] == "external.opensearch.search"
    assert hits["args"]["q"] == {"type": "string", "required": True}
    assert hits["args"]["size"] == {"type": "integer", "default": 10}
    assert hits["sample_policy"] == "digest"
    assert hits["retention_class"] == "warm"


@pytest.mark.parametrize("metadata,why", [
    ({}, "the subtype declares nothing at all"),
    ({"live_relations": {}}, "an empty index is not a declaration"),
    ({"live_relations": "broken"}, "a non-mapping is not published"),
])
def test_a_subtype_declaring_none_omits_the_key_entirely(
    metadata: dict[str, Any], why: str,
) -> None:
    """Absent, never `{}`.

    "This subtype declares no live relations" and "this surface withholds
    them" have to stay distinguishable -- the same distinction
    `InspectKindNotServed` preserves one level up. An empty object says the
    first while looking like the second.
    """
    assert "live_relations" not in _shape(metadata), why


def test_the_published_schema_and_the_enforced_model_agree() -> None:
    """The advertised argument names ARE the enforced ones.

    Both are rendered from one `LiveRelation`, so this cannot drift without
    the object changing. Pinned anyway: an agent that reads an argument list
    from `inspect` and gets it refused by `expand` has been lied to, and the
    lie would be silent.
    """
    relations = parse_live_relations(
        {ANNOTATION_KEY: {"hits": dict(HITS)}}, subtype="search_index",
    )
    published = _shape({"live_relations": _stored()})["live_relations"]
    for name, relation in relations.items():
        model = compile_args_model(relation)
        assert set(model.model_fields) == set(published[name]["args"])


def test_the_inspect_output_schema_declares_live_relations() -> None:
    """An agent reading `inspect`'s outputSchema learns the field exists."""
    assert "live_relations" in OperatorResult.model_fields
    schema = OperatorResult.model_json_schema()
    assert "live_relations" in schema["properties"]


def test_the_published_block_withholds_the_operator_only_fields() -> None:
    """`principal_mode` and `deadline_seconds` are stored, not advertised.

    A caller cannot act on either, and publishing the deadline would invite
    an agent to treat it as a budget it can plan around when it is really an
    operator's bound on someone else's system.
    """
    stored = _stored()["hits"]
    assert stored["principal_mode"] == "service"
    assert stored["deadline_seconds"] == 5.0

    published = _shape({"live_relations": _stored()})["live_relations"]["hits"]
    assert "principal_mode" not in published
    assert "deadline_seconds" not in published


def test_a_stored_declaration_missing_a_field_refuses_rather_than_defaulting(
) -> None:
    """A catalog composed by another version is refused, not guessed at.

    Filling in a default deadline would run an external call unbounded under
    assumptions nobody declared.
    """
    from okg.substrate.external_backed.live_relations import (
        LiveRelation,
        LiveRelationInvalid,
    )

    partial = {k: v for k, v in _stored()["hits"].items()
               if k != "deadline_seconds"}
    with pytest.raises(LiveRelationInvalid) as exc:
        LiveRelation.from_metadata("hits", "search_index", partial)
    assert "deadline_seconds" in str(exc.value)


def test_bounds_survive_the_catalog_round_trip() -> None:
    """A bound the dispatcher does not rebuild is a bound nothing enforces.

    The constraints were validated at composition and published by
    `inspect`, then dropped by `from_metadata` -- so they existed everywhere
    except where they were checked. Through-the-tool tests caught it; this
    pins the mechanism.
    """
    from okg.substrate.external_backed.live_relations import (
        LiveRelation,
        compile_args_model,
    )

    bounded = parse_live_relations({ANNOTATION_KEY: {"hits": {
        "action": "external.x.y", "deadline_seconds": 5,
        "args": {
            "q": {"type": "string", "required": True, "maxLength": 12},
            "size": {"type": "integer", "default": 10, "minimum": 1,
                     "maximum": 100},
        },
    }}}, subtype="page")["hits"]

    rebuilt = LiveRelation.from_metadata("hits", "page", bounded.to_metadata())
    assert rebuilt.args == bounded.args

    model = compile_args_model(rebuilt)
    with pytest.raises(Exception):
        model(q="x", size=101)
    with pytest.raises(Exception):
        model(q="y" * 13)


def test_the_real_compose_to_dispatch_round_trip_is_covered_hermetically() -> None:
    """modules_compose -> metadata jsonb -> from_metadata, without a database.

    The Postgres tool test covers the real path end to end; this covers the
    SHAPE cheaply, so a change to what the composer stores is caught in the
    fast lane rather than only in the database lane.
    """
    import json

    import yaml

    from okg.substrate.external_backed.live_relations import (
        LiveRelation,
        compile_args_model,
    )

    schema = {"classes": {"SearchIndex": {
        "annotations": {
            "archetype": "thing",
            ANNOTATION_KEY: {"hits": {
                "action": "external.opensearch.search",
                "deadline_seconds": 5,
                "args": {
                    "q": {"type": "string", "required": True,
                          "maxLength": 32},
                    "size": {"type": "integer", "default": 10,
                             "minimum": 1, "maximum": 50},
                },
            }},
        },
    }}}
    # What the composer reads is raw YAML, so round-trip through it.
    annotations = yaml.safe_load(yaml.safe_dump(schema))["classes"][
        "SearchIndex"
    ]["annotations"]
    declared = parse_live_relations(annotations, subtype="search_index")
    stored = {n: r.to_metadata() for n, r in declared.items()}

    # ... and what the catalog writes is jsonb, so round-trip through that.
    reloaded = json.loads(json.dumps(stored))
    rebuilt = LiveRelation.from_metadata(
        "hits", "search_index", reloaded["hits"],
    )

    assert rebuilt.args == declared["hits"].args
    assert rebuilt.deadline_seconds == 5.0
    model = compile_args_model(rebuilt)
    with pytest.raises(Exception):
        model(q="x", size=51)
