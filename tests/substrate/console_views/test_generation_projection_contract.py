"""The bounded generation reader must project every key its callers read.

`ChronosGraphStore.recent_generations` returns rows whose `metadata` holds
ONLY the keys the caller asked for. That is what makes the read cheap -- a
projected key still moves its bytes, and a checkpoint's `blocked_reason` can
be tens of megabytes -- and it is also the trap: a composer that starts
reading a key nobody projected sees `None` and prints "not recorded" for a
fact the deployment did record. Nothing in the read path fails; the page just
becomes quietly wrong.

So the key set each composer declares is checked against the key literals in
the composer AND IN EVERY HELPER IT CALLS. A new `metadata.get("...")` fails
here until it is added to that composer's tuple, and a key outside the store's
contract is refused by the store itself.

The helpers are the point. This guard used to read `inspect.getsource(func)`
alone, which is the routed function's body and nothing else -- so every key
`compose_attempt_row` reads through `_display_generation_id`,
`_blocked_detail`, `_blocked_summary`, `_excluded_source_names`,
`_excluded_count` and `_view_runs_fields` was invisible to it, and so was the
whole of `console_views/attempt_detail.py`, whose sections read nine keys
through six helpers of their own. A guard that cannot see where a key is read
is a guard against nothing; it now walks the call graph inside each composer's
own module, transitively, and reads the source of every function it reaches.

These tests need no database: they read the composers' source.
"""
from __future__ import annotations

import ast
import importlib
import inspect
import sys
import textwrap

import pytest

from okg.substrate.console_views import (
    attempt_detail,
    attempts_by_day,
    generations,
    health,
    knowledge,
    producer_metrics,
    records,
)
from okg.substrate.graph_store_internal.generations import (
    RECENT_GENERATION_METADATA_KEYS,
    _generation_projection,
)
from okg.substrate.errors import SubstrateError

# The three keys the reader always fetches because it applies
# `list_generations`'s own scope filter in Python.
_ALWAYS = {"kind", "deployment_name", "status"}


def _module_functions(module) -> dict[str, object]:
    """Every function defined IN this module, by the name its callers use.

    Methods and imported names are deliberately out: a helper a composer
    reaches through an import belongs to the module that owns it, and its keys
    are that module's contract to declare.
    """
    return {
        name: value
        for name, value in vars(module).items()
        if inspect.isfunction(value)
        and getattr(value, "__module__", None) == module.__name__
    }


def _called_names(tree: ast.AST) -> set[str]:
    """Every plain name this source calls: `helper(...)`, not `x.helper(...)`.

    An attribute call is a method or another module's function, and neither is
    resolved here -- see `_module_functions`.
    """
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def _reachable_sources(func) -> list[str]:
    """`func`'s source and that of every helper it reaches in its own module.

    Transitive, and cycle-safe: a pair of mutually recursive helpers is walked
    once each.
    """
    module = inspect.getmodule(func)
    defined = _module_functions(module)
    seen: set[str] = set()
    sources: list[str] = []
    pending = [func]
    while pending:
        current = pending.pop()
        name = current.__name__
        if name in seen:
            continue
        seen.add(name)
        source = textwrap.dedent(inspect.getsource(current))
        sources.append(source)
        for called in _called_names(ast.parse(source)):
            helper = defined.get(called)
            if helper is not None and called not in seen:
                pending.append(helper)
    return sources


def _metadata_keys_read_by(func) -> set[str]:
    """Every literal metadata key `func` reads, directly or through a helper.

    Matches both shapes the composers use: ``metadata.get("x")`` /
    ``metadata["x"]`` on any name ending in ``metadata``, and the
    ``(generation.metadata or {}).get("x")`` form.
    """
    tree = ast.parse("\n".join(_reachable_sources(func)))
    found: set[str] = set()

    def _is_metadata(node: ast.AST) -> bool:
        if isinstance(node, ast.Name):
            return node.id.endswith("metadata")
        if isinstance(node, ast.Attribute):
            return node.attr == "metadata"
        if isinstance(node, ast.BoolOp):
            return any(_is_metadata(v) for v in node.values)
        return False

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and _is_metadata(node.func.value)
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            found.add(node.args[0].value)
        if (
            isinstance(node, ast.Subscript)
            and _is_metadata(node.value)
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            found.add(node.slice.value)
    return found


ROUTED = [
    pytest.param(
        records.compose_published_versions,
        records.VERSION_METADATA_KEYS,
        id="compose_published_versions",
    ),
    pytest.param(
        generations.compose_attempt_row,
        generations.ATTEMPT_ROW_METADATA_KEYS,
        id="compose_attempt_row",
    ),
    pytest.param(
        generations.compose_generations,
        generations.ATTEMPT_ROW_METADATA_KEYS,
        id="compose_generations",
    ),
    pytest.param(
        attempts_by_day.compose_attempts_by_day,
        attempts_by_day.ATTEMPTS_BY_DAY_METADATA_KEYS,
        id="compose_attempts_by_day",
    ),
    # The attempt DETAIL read. It reached the bounded reader in the same pass
    # that added it here: until then it called the unbounded
    # `store.list_generations`, which projects nothing, so every key it reads
    # was served and none of them was declared anywhere.
    pytest.param(
        attempt_detail.compose_attempt_detail,
        attempt_detail.ATTEMPT_DETAIL_METADATA_KEYS,
        id="compose_attempt_detail",
    ),
    # Issue #2113: the last three composers that called the unbounded
    # `list_generations`. Health reads two windows -- the recent attempts and
    # the previous published generation's counts -- so it declares both
    # tuples; the counts are read through `_metadata_int`, whose key argument
    # this scanner cannot see, and are pinned by behaviour instead
    # (`test_bounded_health_knowledge_metrics.py`).
    pytest.param(
        health.compose_health,
        health.HEALTH_HISTORY_METADATA_KEYS + health.HEALTH_PREVIOUS_METADATA_KEYS,
        id="compose_health",
    ),
    # The overview reads a COUNT, not rows: it declares no key, so any
    # metadata read that appears in it fails here until it is declared.
    pytest.param(
        knowledge._fetch_overview,
        knowledge.OVERVIEW_METADATA_KEYS,
        id="knowledge_overview",
    ),
    # `_run_from_metadata` lives in `enricher_diagnostics` and reads `status`
    # and `target_generation_id`; this scanner stays inside one module, so
    # those two are declared by hand and pinned by the Postgres field test.
    pytest.param(
        producer_metrics._selected_enricher_runs,
        producer_metrics.ENRICHER_RUN_METADATA_KEYS,
        id="producer_metrics_enricher_runs",
    ),
]


@pytest.mark.parametrize(("composer", "declared"), ROUTED)
def test_composer_reads_only_projected_metadata_keys(composer, declared) -> None:
    """Every metadata key the composer reads is one the reader projects."""
    read = _metadata_keys_read_by(composer)
    projected = set(declared) | _ALWAYS
    missing = sorted(read - projected)
    assert not missing, (
        f"{composer.__name__} reads metadata keys the bounded reader does not "
        f"project: {missing}. Add them to the composer's declared key tuple "
        f"(and to RECENT_GENERATION_METADATA_KEYS) -- otherwise the composer "
        f"reads None and prints 'not recorded' for a recorded fact."
    )


@pytest.mark.parametrize(("composer", "declared"), ROUTED)
def test_declared_keys_are_inside_the_store_contract(composer, declared) -> None:
    outside = sorted(set(declared) - set(RECENT_GENERATION_METADATA_KEYS))
    assert not outside, (
        f"{composer.__name__} declares keys the store contract does not hold: "
        f"{outside}"
    )


def test_reader_refuses_a_key_outside_the_contract() -> None:
    """An unknown key is a refusal, not a silently absent column."""
    with pytest.raises(SubstrateError) as excinfo:
        _generation_projection(("status", "a_key_no_checkpoint_records"))
    assert "a_key_no_checkpoint_records" in str(excinfo.value)


def test_projection_always_includes_the_filter_keys() -> None:
    """The reader filters on kind/deployment/status, so it always fetches them."""
    assert _ALWAYS.issubset(set(_generation_projection(("blocked_reason",))))


def test_the_guard_itself_sees_a_key_read_only_inside_a_helper(
    tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guard's own failure mode, pinned.

    `inspect.getsource` on the routed function alone passes a composer whose
    helper reads an undeclared key, which is how `_display_generation_id`,
    `_blocked_detail`, `_blocked_summary`, `_excluded_source_names`,
    `_excluded_count` and `_view_runs_fields` were outside the contract while
    the file that checks it was green. Asserted against a module written here
    rather than against a mutation of a real composer, so it keeps holding
    after the real helpers change.
    """
    module_path = tmp_path / "fake_composer_module.py"
    module_path.write_text(textwrap.dedent(
        '''
        def _helper(metadata):
            return metadata.get("a_key_only_the_helper_reads")

        def _deeper(metadata):
            return metadata["a_key_two_helpers_down"]

        def _helper_calling_a_helper(metadata):
            return _deeper(metadata)

        def compose(generation):
            metadata = generation.metadata or {}
            return [
                metadata.get("a_key_the_composer_reads"),
                _helper(metadata),
                _helper_calling_a_helper(metadata),
            ]
        '''
    ))
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("fake_composer_module")
    try:
        assert _metadata_keys_read_by(module.compose) == {
            "a_key_the_composer_reads",
            "a_key_only_the_helper_reads",
            "a_key_two_helpers_down",
        }
    finally:
        sys.modules.pop("fake_composer_module", None)
