"""view="list" together with select (PACT read-passage-rows-3666, D6).

A list row is its head line plus the keys the caller chose. `attrs.<key>`
names one attribute, as `where` spells it, beside the list's own fields; a
name a list row can never carry is still refused by name. Pure: no database.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp.search_views import (
    LIST_FIELDS,
    SearchViewUnsupported,
    resolve_search_view,
)


def _resolve(select):
    return resolve_search_view(
        "list", method="lexical", rank_by=None, max_per_parent_argument=False,
        limit=None, select=select,
    )


def test_a_list_takes_attribute_names_beside_its_fields() -> None:
    """Must-fail on f9634f77389: `attrs.status` was refused on a list."""
    limit, select = _resolve(["attrs.status", "attrs.updated"])
    assert limit == 100
    assert select == [*LIST_FIELDS, "attrs.status", "attrs.updated"]
    assert _resolve(["node_id", "attrs.status"])[1] == ["node_id", "attrs.status"]
    # The list's own narrowing is unchanged.
    assert _resolve(["node_id"])[1] == ["node_id"]
    assert _resolve(None)[1] == list(LIST_FIELDS)


@pytest.mark.parametrize("select", [["excerpt"], ["attrs."], [], "attrs.status"])
def test_a_name_a_list_row_never_carries_is_still_refused(select) -> None:
    with pytest.raises(SearchViewUnsupported):
        _resolve(select)
