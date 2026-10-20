"""M5b — library resolver + fragment loading + adapter protocol."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.invariants import load_invariants
from okg.substrate.library import (
    LibraryRefMissing,
    available_refs,
    resolve,
)
from okg.substrate.library.sources import available_adapters


# ---------- resolver ----------

def test_resolver_rejects_malformed_ref() -> None:
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError, match="malformed"):
        resolve("not_a_library_ref")
    with pytest.raises(SubstrateError, match="malformed"):
        resolve("okg.ticket")  # missing kind
    with pytest.raises(SubstrateError, match="malformed"):
        resolve("okg.bogus_kind.name")


def test_resolver_rejects_deleted_ontology_kind() -> None:
    """Per `unify-ontology-systems` (2026-04-26): the legacy
    `okg.ontology.*` and `okg.narrowings.*` kinds are removed."""
    from okg.substrate.errors import SubstrateError
    with pytest.raises(SubstrateError, match="malformed"):
        resolve("okg.ontology.ticket")
    with pytest.raises(SubstrateError, match="malformed"):
        resolve("okg.narrowings.ticket")


def test_resolver_missing_ref_lists_alternatives() -> None:
    with pytest.raises(LibraryRefMissing) as exc:
        resolve("okg.invariants.does_not_exist")
    assert exc.value.code == "library_ref_missing"


def test_available_refs_covers_all_kinds() -> None:
    for kind in ["invariants", "modules", "templates"]:
        refs = available_refs(kind)
        assert refs, f"{kind} returned empty"


# ---------- promoted ontologies load via the modules path ----------

PROMOTED_ONTOLOGY_NAMES = (
    "person", "document_starter", "service", "repo_starter",
    "incident", "ticket", "code_artifact", "dataset",
)


@pytest.mark.parametrize("name", PROMOTED_ONTOLOGY_NAMES)
def test_promoted_ontology_schema_loads(name: str) -> None:
    """Each fragment that was promoted under
    `unify-ontology-systems` SHALL ship a LinkML schema at
    `library/ontologies/<name>/schema.yaml`."""
    root = (
        Path(__file__).resolve().parents[2]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
    )
    schema_path = root / name / "schema.yaml"
    assert schema_path.is_file(), schema_path
    doc = yaml.safe_load(schema_path.read_text())
    classes = doc.get("classes") or {}
    # Every promoted ontology declares >=1 archetype-annotated
    # subtype + an EdgeNarrowings carrier (or has no narrowings,
    # in which case it omits EdgeNarrowings).
    archetypes = [
        c for c in classes.values()
        if isinstance(c, dict)
        and (c.get("annotations") or {}).get("archetype")
    ]
    assert len(archetypes) >= 1


# ---------- invariants pass the M5.1 loader ----------

@pytest.mark.parametrize("name", [
    "no_orphan_edges", "every_text_node_has_text",
    "subtype_counts_within_bounds", "monotonic_observation_at",
    "catalog_version_consistent",
])
def test_library_invariants_parse(name: str) -> None:
    p = resolve(f"okg.invariants.{name}")
    specs = load_invariants(p)
    assert len(specs) == 1
    spec = specs[0]
    assert spec.name == name
    assert spec.severity in ("error", "warn")
    assert spec.scope == "cross"
    # Shipped bodies are real SQL; the loader already asserted
    # they contain no write keywords.


# ---------- adapter protocol conformance ----------

# Post-Wave-A (delete-5-inert-M5b-skeletons) the library ships
# 4 fixture-backed protocol-conformant adapter skeletons:
# github, jira, confluence, docsite. The 5 deleted skeletons
# (gitlab, postgres_table, s3, webhook, openapi) had no callers,
# no test data, no production wire-up; the academic-researcher profile
# carries its own real adapters (apple_calendar_local, gmail_api,
# google_calendar_api, slack_api) which have separate tests.
ADAPTER_CLASSES = [
    ("github", "GitHubSource"),
    ("jira", "JiraSource"),
    ("confluence", "ConfluenceSource"),
    ("docsite", "DocsiteSource"),
]


@pytest.mark.parametrize("modname,classname", ADAPTER_CLASSES)
def test_adapter_loads_and_satisfies_protocol(
    modname: str, classname: str,
) -> None:
    import importlib
    mod = importlib.import_module(
        f"okg.substrate.library.sources.{modname}"
    )
    cls = getattr(mod, classname)
    adapter = cls()
    # name + profile are class/instance attrs.
    assert adapter.name == modname
    assert adapter.profile in (
        "reference_catalog", "mutable_api",
        "discovery_crawl", "append_feed", "live_overlay",
    )
    # SourceAdapter is runtime-checkable? It's a Protocol; it
    # doesn't have @runtime_checkable here, so we just assert
    # it has the `run` attribute.
    assert callable(getattr(adapter, "run", None))


def test_available_adapters_lists_post_sweep_set() -> None:
    """Post-Wave-A the 4 fixture-backed skeleton adapters MUST
    still be discoverable. The 5 deleted (gitlab/postgres_table/
    s3/webhook/openapi) MUST NOT reappear without explicit
    reintroduction via a follow-up PACT."""
    names = set(available_adapters())
    expected = {"github", "jira", "confluence", "docsite"}
    assert expected.issubset(names), (
        f"missing adapters: {expected - names}"
    )
    deleted = {"gitlab", "postgres_table", "s3", "webhook", "openapi"}
    assert deleted.isdisjoint(names), (
        f"deleted-skeleton adapters reappeared: {deleted & names} "
        f"— either remove them again or land a PACT reintroducing them"
    )
    # Private base modules (prefix `_`) are not listed.
    assert "_fixture" not in names


# ---------- fixture-backed adapter actually runs ----------

def test_fixture_adapter_runs(tmp_path: Path) -> None:
    from okg.substrate.library.sources.docsite import DocsiteSource
    fixture = tmp_path / "docs.json"
    raw_bytes = json.dumps({
        "nodes": [
            {"node_id": "d:1", "subtype": "document",
             "attrs": {"text": "hello"}},
        ],
        "edges": [],
    }).encode()
    fixture.write_bytes(raw_bytes)
    adapter = DocsiteSource(fixture_path=str(fixture))
    run = adapter.run("run-id-x")
    facts = list(run.facts)
    assert len(facts) == 1
    assert facts[0].node_id == "d:1"
    assert facts[0].subtype == "document"
    assert facts[0].attrs["text"] == "hello"
    assert run.applied_raw_token == adapter.change_probe.token_from_item_hashes({
        str(fixture): hashlib.sha256(raw_bytes).hexdigest(),
    })
    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"


def test_fixture_adapter_binds_emission_and_token_to_one_byte_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.docsite import DocsiteSource

    fixture = tmp_path / "docs.json"
    first_bytes = json.dumps({
        "nodes": [
            {"node_id": "d:first", "subtype": "document",
             "attrs": {"text": "first"}},
        ],
        "edges": [],
    }).encode()
    replacement_bytes = json.dumps({
        "nodes": [
            {"node_id": "d:replacement", "subtype": "document",
             "attrs": {"text": "replacement"}},
        ],
        "edges": [],
    }).encode()
    fixture.write_bytes(first_bytes)
    adapter = DocsiteSource(fixture_path=str(fixture))
    original_read_bytes = Path.read_bytes
    read_paths: list[Path] = []

    def _read_then_mutate(path: Path) -> bytes:
        captured = original_read_bytes(path)
        read_paths.append(path)
        if path == fixture:
            fixture.write_bytes(replacement_bytes)
        return captured

    monkeypatch.setattr(Path, "read_bytes", _read_then_mutate)

    run = adapter.run("run-id-snapshot")
    facts = list(run.facts)
    applied = adapter.change_probe.token_from_item_hashes({
        str(fixture): hashlib.sha256(first_bytes).hexdigest(),
    })

    assert read_paths == [fixture]
    assert [fact.node_id for fact in facts] == ["d:first"]
    assert run.applied_raw_token == applied
    assert not tokens_equal(applied, adapter.change_probe.build_token())


def test_fixture_adapter_scoped_run_withholds_global_token(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.docsite import DocsiteSource

    fixture = tmp_path / "docs.json"
    fixture.write_text(json.dumps({
        "nodes": [
            {"node_id": "d:1", "subtype": "document"},
            {"node_id": "d:2", "subtype": "document"},
        ],
        "edges": [],
    }))
    run = DocsiteSource(fixture_path=str(fixture)).run(
        "run-id-scoped",
        sync_scope={"scope_keys": ["d:1"]},
    )

    assert [fact.node_id for fact in run.facts] == ["d:1"]
    assert run.applied_raw_token is None
    assert run.applied_token_scope == "unavailable"
    assert run.applied_token_authority == "unavailable"


def test_fixture_adapter_read_error_is_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.library.sources.docsite import DocsiteSource

    fixture = tmp_path / "docs.json"
    fixture.write_text('{"nodes": [], "edges": []}')
    adapter = DocsiteSource(fixture_path=str(fixture))
    reads = 0

    def _unreadable(_path: Path) -> bytes:
        nonlocal reads
        reads += 1
        raise PermissionError("sensitive operating-system detail")

    monkeypatch.setattr(Path, "read_bytes", _unreadable)

    with pytest.raises(SubstrateError, match="could not be read") as exc_info:
        adapter.run("run-id-unreadable")

    assert reads == 1
    assert "sensitive operating-system detail" not in str(exc_info.value)


def test_fixture_adapter_invalid_json_never_builds_applied_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.library.sources.docsite import DocsiteSource

    fixture = tmp_path / "docs.json"
    fixture.write_bytes(b'{"nodes": [')
    adapter = DocsiteSource(fixture_path=str(fixture))

    def _unexpected_token(_hashes) -> dict:
        raise AssertionError("invalid bytes must never receive an applied token")

    monkeypatch.setattr(
        adapter.change_probe,
        "token_from_item_hashes",
        _unexpected_token,
    )

    with pytest.raises(SubstrateError, match="not valid JSON"):
        adapter.run("run-id-invalid-json")


# ---------- templates resolve + shape ----------

@pytest.mark.parametrize("name", [
    "devops", "docs-corpus", "empty", "okg-workspace",
])
def test_template_resolves_and_has_deployment_yaml(name: str) -> None:
    p = resolve(f"okg.templates.{name}")
    assert isinstance(p, Path)
    assert p.is_dir()
    deployment_yaml = p / "deployment.yaml"
    assert deployment_yaml.is_file()
    doc = yaml.safe_load(deployment_yaml.read_text())
    # Every starter takes the name given to `okg init` (issue #2816).
    assert doc["name"] == "{{ deployment_name }}"
    # Per `unify-ontology-systems` (2026-04-26): templates use
    # the canonical `modules:` shape. Each module name MUST
    # resolve to a real ontology dir under
    # `library/ontologies/<name>/schema.yaml`.
    root = (
        Path(__file__).resolve().parents[2]
        / "src" / "okg" / "substrate" / "library" / "ontologies"
    )
    for mod in doc.get("modules", []):
        assert (root / mod / "schema.yaml").is_file(), (
            f"{name} template references unknown module {mod!r}"
        )


def test_devops_template_fixtures_are_well_formed() -> None:
    p = resolve("okg.templates.devops")
    for fixture_name in ("jira.json", "github.json"):
        fx = Path(p) / "fixtures" / fixture_name
        assert fx.exists()
        doc = json.loads(fx.read_text())
        assert "nodes" in doc
