"""A push-down declares its own join operators and its own empty-set error.

Issue #3666, PACT `live-graph-rucio-sites`. The engine holds no operator
symbol and no error text of any one system: `push_down.join` must name `and`
and `or`, and `push_down.empty_error` lists the substrings of the system's
error text that mean "the expression selected nothing". An error the
declaration does not name stays a failure.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.external_backed.live_dispatch import LiveAdapterError
from okg.substrate.external_backed.live_types import LiveTypeInvalid, LiveTypeRegistry
from okg.substrate.mcp import live_calls

FIXTURE = Path(__file__).parent / "fixtures" / "live_types_sites.yaml"
DECLARED = "No sites match the expression"


def _schema(**push_down: Any) -> dict[str, Any]:
    schema = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    sites = schema["classes"]["System"]["annotations"]["okg_live_type"]["edges"]["sites"]
    # The shared fixture declares an empty-set message for the live-graph
    # tests; these tests start from none and add their own.
    sites["push_down"].pop("empty_error", None)
    sites["push_down"].update(push_down)
    return schema


def _compose(tmp_path: Path, schema: dict[str, Any]):
    library = tmp_path / "library"
    (library / "live_example").mkdir(parents=True)
    (library / "live_example" / "schema.yaml").write_text(
        yaml.safe_dump(schema), encoding="utf-8",
    )
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps({"name": "fixture", "modules": ["live_example"]}), encoding="utf-8",
    )
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    return compose_catalog(deployment, modules_root=library, cross_links_root=cross_links)


def _push(tmp_path: Path, schema: dict[str, Any]):
    registry = LiveTypeRegistry.from_catalog(_compose(tmp_path, schema).subtype_metadata)
    return registry.by_subtype("system").edges["sites"].push_down


@pytest.mark.parametrize("missing", ["and", "or"])
def test_a_push_down_join_without_and_or_or_is_refused(tmp_path, missing) -> None:
    join = {"and": "&", "or": "|", "and_not": "\\"}
    del join[missing]
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, _schema(join=join))
    assert exc.value.data["reason"] == "join_operator_missing"
    assert missing in str(exc.value)


@pytest.mark.parametrize("bad", [[], "text", [""], [3]])
def test_a_malformed_empty_error_is_refused(tmp_path, bad) -> None:
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, _schema(empty_error=bad))
    assert exc.value.data["reason"] == "empty_error_invalid"


def test_empty_error_parses_and_round_trips(tmp_path) -> None:
    push = _push(tmp_path, _schema(empty_error=[DECLARED]))
    assert push.empty_error == (DECLARED,)
    assert push.to_metadata()["empty_error"] == [DECLARED]
    assert _push(tmp_path / "none", _schema()).empty_error == ()


def _failing(excerpt: str):
    def adapter(**_: Any) -> Any:
        raise LiveAdapterError("upstream_unavailable", excerpt=excerpt)

    return adapter


def test_a_declared_empty_error_is_read_as_zero_rows() -> None:
    wrapped = live_calls.empty_answer_adapter(
        _failing(f"SiteExpressionError: {DECLARED.upper()}."), (DECLARED,),
    )
    with pytest.raises(LiveAdapterError) as caught:
        wrapped()
    assert caught.value.failure == "upstream_empty_result"


def test_an_undeclared_error_message_stays_a_failure() -> None:
    wrapped = live_calls.empty_answer_adapter(
        _failing("SiteExpressionError: resulted in an empty set"), (DECLARED,),
    )
    with pytest.raises(LiveAdapterError) as caught:
        wrapped()
    assert caught.value.failure == "upstream_unavailable"


def test_with_no_declaration_no_error_is_read_as_empty() -> None:
    wrapped = live_calls.empty_answer_adapter(_failing(f"{DECLARED}."))
    with pytest.raises(LiveAdapterError) as caught:
        wrapped()
    assert caught.value.failure == "upstream_unavailable"
