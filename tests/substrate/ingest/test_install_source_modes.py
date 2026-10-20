"""A packaged install must retain each selected source's real profile semantics."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.ingest.service import IngestOptions, _run_mode_for_source, _validate_run_modes_by_source


def test_mixed_mode_map_is_immutable_and_profile_correct():
    modes = {"jira": "reconcile", "docs": "scope_complete", "releases": "release_new"}
    profiles = {"jira": "mutable_api", "docs": "discovery_crawl", "releases": "reference_catalog"}
    options = IngestOptions("unused", "test", Path("."), run_modes_by_source=modes)
    modes["jira"] = "scope_complete"
    plan = SimpleNamespace(ordered=tuple(profiles), runnable={
        name: SimpleNamespace(source_class=profile) for name, profile in profiles.items()
    })
    _validate_run_modes_by_source(options, plan)
    assert [_run_mode_for_source(options, plan.runnable[name], name, ()) for name in plan.ordered] == [
        "reconcile", "scope_complete", "release_new",
    ]
    with pytest.raises(TypeError):
        options.run_modes_by_source["jira"] = "cursor"


@pytest.mark.parametrize("modes", [
    {"docs": "scope_complete"},  # omitted required predecessor
    {"docs": "scope_complete", "releases": "release_new", "unknown": "append"},
    {"docs": "scope_complete", "releases": "scope_complete"},
    {"docs": "unknown", "releases": "release_new"},
])
def test_mode_map_refuses_incomplete_unknown_or_incompatible_selection(modes):
    plan = SimpleNamespace(ordered=("releases", "docs"), runnable={
        "releases": SimpleNamespace(source_class="reference_catalog"),
        "docs": SimpleNamespace(source_class="discovery_crawl"),
    })
    with pytest.raises(SubstrateError):
        _validate_run_modes_by_source(IngestOptions("unused", "test", Path("."), run_modes_by_source=modes), plan)


def test_empty_map_preserves_existing_global_mode():
    options = IngestOptions("unused", "test", Path("."), run_mode="cursor")
    assert _run_mode_for_source(options, SimpleNamespace(), "source", ()) == "cursor"
