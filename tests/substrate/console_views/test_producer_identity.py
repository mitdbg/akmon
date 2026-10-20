"""One configured producer must be one row, whatever its runtime key is.

No database: the defect is a join, and a join is decidable from a registry
document and a rollup mapping. `source_registry.yaml` declares two names per
source -- the document key (`bib-references`) and the `ownership_id` the
runtime attributes its records to (`okg-workspace.bib_references`). The
console's source composer builds its join key from the document key alone, so
a source whose `ownership_id` is not `<deployment>.<key>` never matches its own
counts: the configured row reports zero and the runtime key appears again as a
second, "not in registry" producer.

That is the Archiv3 double count -- `cmssw_releases` beside `cmssw-releases` --
and `deployments/okg-workspace/source_registry.yaml` ships two live instances of
the same shape (`bib-references`, `latex-project`).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.console_views import sources as sources_mod


DEPLOYMENT = "lane-fixture"
REGISTRY = """
sources:
  bib-references:
    ownership_id: lane-fixture.bib_references
"""

# What the runtime attributed its records to: the ownership_id, not the key.
RUNTIME_KEY = "lane-fixture.bib_references"


class _Cursor:
    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, *_: Any, **__: Any) -> None:
        return None

    def fetchall(self) -> list[tuple[str, Any]]:
        # source_progress records the ownership_id.
        return [(RUNTIME_KEY, None)]


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


@pytest.fixture
def registry_deployment(tmp_path, monkeypatch):
    """A deployment whose one source's ownership_id differs from its key."""
    dep_dir = tmp_path / DEPLOYMENT
    dep_dir.mkdir()
    (dep_dir / "source_registry.yaml").write_text(REGISTRY)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: dep_dir if name == DEPLOYMENT else None,
    )
    return dep_dir


@pytest.fixture
def rolled_counts(monkeypatch):
    """The maintained rollup, keyed by the runtime ownership_id."""
    monkeypatch.setattr(
        sources_mod,
        "require_console_count_rollup_enabled",
        lambda **_: None,
    )
    import okg.substrate.console_views.console_count_rollup_read as rollup

    monkeypatch.setattr(
        rollup,
        "read_console_count_rollup",
        lambda conn, **_: {
            (rollup.COUNT_KIND_SOURCE, RUNTIME_KEY): 41,
            (rollup.COUNT_KIND_EDGE_SOURCE, RUNTIME_KEY): 7,
        },
    )


def _compose(conn: Any) -> dict[str, Any]:
    return sources_mod.compose_sources(
        conn,
        deployment_name=DEPLOYMENT,
        session=SimpleNamespace(generation_id="gen:1", branch_name="default"),
    )


def test_one_configured_producer_is_one_row(registry_deployment, rolled_counts):
    """The source read must not list a producer twice under its two names."""
    payload = _compose(_Conn())
    names = [row["name"] for row in payload["sources"]]
    assert payload["source_count"] == 1, (
        f"one configured source produced {payload['source_count']} rows: {names}"
    )


def test_the_configured_row_carries_the_runtime_counts(
    registry_deployment, rolled_counts
):
    """The counts belong to the configured source, not to a phantom beside it."""
    payload = _compose(_Conn())
    rows = {row["name"]: row for row in payload["sources"]}
    assert "bib-references" in rows, f"configured name missing: {list(rows)}"
    row = rows["bib-references"]
    assert row["in_registry"] is True
    assert row["records_in_gen"] == 41
    assert row["edges_in_gen"] == 7


def test_no_row_is_reported_outside_the_registry(registry_deployment, rolled_counts):
    """The runtime key is the configured source, so nothing is unconfigured."""
    payload = _compose(_Conn())
    stray = [row["name"] for row in payload["sources"] if not row["in_registry"]]
    assert stray == [], f"runtime keys listed as unconfigured producers: {stray}"


# --- one rule, and no second one ------------------------------------------

CONSOLE_VIEWS = Path(
    "src/okg/substrate/console_views"
)

#: The one module allowed to turn a deployment into producer identities.
IDENTITY_MODULE = "producer_identity.py"

#: Parsing the registry is what makes a module an identity helper. A
#: docstring that names the file is prose, so the tokens below are the ones
#: that only appear in code that opens it.
#: `/ "source_registry.yaml"` is the path join that locates the file to open.
REGISTRY_READS = (
    "load_source_registry",
    "registry_path.read_text()",
    '/ "source_registry.yaml"',
)

#: There is no exception. `knowledge.py::_fetch_sources` was the last module
#: that parsed the registry itself, keyed its sources by the document name,
#: and swallowed a parse failure; it resolves through `producer_identity` now
#: (issue #2147).

#: Building a runtime key by joining a deployment name to a producer name.
#: `f"{deployment_name}."` alone is a prefix used for SHORTENING a recorded
#: key for display, which decides nothing; the forbidden shape is the one
#: that interpolates a name after the dot and calls the result a key.
KEY_CONSTRUCTIONS = ('f"{deployment_name}.{', 'f"{prefix}{')


def _console_view_sources() -> list[Path]:
    root = Path(__file__).resolve().parents[3] / CONSOLE_VIEWS
    assert root.is_dir(), root
    return sorted(
        path for path in root.glob("*.py") if path.name != "__init__.py"
    )


def test_console_views_declares_exactly_one_identity_helper():
    """`producer_identity` is the only module that reads the registry.

    A second module that parsed `source_registry.yaml` would be a second
    answer to "what is this producer called", and the two would drift. That
    drift is the defect this change exists to fix: `compose_sources` and
    `_health_source_breakdown` each had their own rule, and on the reported
    deployment they turned five sources into eight.
    """
    offenders = {
        path.name: [token for token in REGISTRY_READS if token in path.read_text()]
        for path in _console_view_sources()
    }
    reading = {
        name: tokens for name, tokens in offenders.items()
        if tokens and name != IDENTITY_MODULE
    }
    assert reading == {}, reading
    assert offenders[IDENTITY_MODULE]


def test_no_console_view_builds_a_runtime_key_from_a_name():
    """A key is declared or recorded. It is never constructed.

    `_health_source_breakdown` built `f"{deployment_name}.{name}"` for a
    producer the registry does not name, which is the same construction that
    made a source miss its own counts. A producer outside the registry has no
    declared id, so the row carries a key some read actually recorded.
    """
    built = {
        path.name: [
            shape for shape in KEY_CONSTRUCTIONS if shape in path.read_text()
        ]
        for path in _console_view_sources()
    }
    assert {name: shapes for name, shapes in built.items() if shapes} == {}


def test_the_one_name_resolver_that_is_not_an_identity_rule_is_named():
    """One exception, stated rather than hidden.

    `source_diagnostics._selected_source_name` matches what a CALLER
    typed against rows the read already returned, splitting on "." to try a
    shorter spelling. It resolves a request parameter against rows in hand;
    it never decides whose counts a recorded key carries, and it reaches no
    database. If it ever joins counts, this test is where that shows up.
    """
    text = (
        Path(__file__).resolve().parents[3]
        / CONSOLE_VIEWS
        / "source_diagnostics.py"
    ).read_text()
    assert 'requested.rsplit(".", 1)' in text, (
        "the documented exception moved; re-read whether it is still a "
        "request-parameter resolver and not an identity rule"
    )
    resolver = text.split("def _selected_source_name")[1].split("\ndef ")[0]
    assert "cursor" not in resolver and "execute" not in resolver, resolver
