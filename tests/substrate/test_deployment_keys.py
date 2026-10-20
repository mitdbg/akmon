"""The deployment key table, and the loader and schema built from it (#2571).

`okg.substrate.deployments.keys.DEPLOYMENT_KEYS` declares every top-level
deployment.yaml key once. These tests hold the loader, the generated
`schemas/deployment.schema.json`, lint and bootstrap to that table, and check
the schema against what the real loader accepts rather than against the table
itself.

PACT: issue-2571-deployment-keys. Test names carry the requirement they prove
(table, types, null, schema, agreement, bootstrap, unread, sealed).
"""

from __future__ import annotations

import ast
import dataclasses
import importlib.util
import json
import os
import re
import runpy
import shutil
import subprocess
import sys
import warnings
from argparse import Namespace
from pathlib import Path
from typing import Any, Callable

import pytest
import yaml
from jsonschema import Draft202012Validator

from okg.substrate import deployments
from okg.substrate.deployment_bootstrap.workflow import (
    BootstrapDraftInvalid,
    _validate_artifact_payloads,
    validate_against_schema,
)
from okg.substrate.deployments import (
    DeploymentConfigError,
    DeploymentKeyTypeError,
    DeploymentManifest,
)
from okg.substrate.deployments.environment import (
    _DEFINITION_KEYS,
    _ENVIRONMENT_OWNED_DEFINITION_KEYS,
)
from okg.substrate.deployments.keys import (
    DEPLOYMENT_KEYS,
    DEPLOYMENT_KEYS_BY_NAME,
    JSON_TYPES,
    KNOWN_TOP_LEVEL_KEYS,
    READ_BY_DEPLOYMENT,
    READ_BY_NOTHING,
    READ_BY_OKG,
    REQUIRED_KEYS,
    render_deployment_schema,
)
from okg.substrate.history_compaction import parse_history_retention
from tests.mirror_scope import is_public_copy
from okg.substrate.schema_resources import AUTHORED_FILE_SCHEMAS

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "schemas" / "deployment.schema.json"
KEYS_PY = ROOT / "src" / "okg" / "substrate" / "deployments" / "keys.py"

# The 63 top-level keys the loader accepted on dev 87c3ae1eba, before the
# table existed. None may disappear: a manifest that used one would stop
# loading.
KEYS_ON_DEV_87C3AE1EBA = frozenset({
    "agent_surface", "agent_tools", "alias_resolver", "aliases",
    "audit_questions", "chat", "checkpoint_attestor_root",
    "checkpoint_attestors", "chronos", "consistency", "consistency_gates",
    "contract", "coverage", "dashboards", "deployment_up",
    "derived_producers", "description", "doctor_checks", "embedding",
    "embeddings", "enricher_registry", "enrichers", "extractor",
    "host_storage", "ir_consistency_checks", "lenses", "link_config",
    "live_capabilities", "maintained_derivations", "mcp", "mcp_instructions",
    "mcp_pre_join_tools", "mcp_trace_path", "module_providers", "modules",
    "modules_config", "name", "native_views", "nomos",
    "post_publish_consistency_checks", "postgres", "postgres_dsn", "profile",
    "profiling", "publisher", "readiness", "record_prefixes", "refreshers",
    "release", "resolvers", "runtime", "schema_dir", "search",
    "session_start_context", "skill_triggers", "skills_dir", "snapshot_path",
    "source_registry", "sync_root", "taxonomy", "tool_description_overrides",
    "viz", "viz_path",
})

# One value of each JSON type. The string is `schemas`, an existing directory
# in every probe deployment, so a path key can accept it.
SAMPLES: dict[str, Any] = {
    "null": None,
    "boolean": True,
    "integer": 1,
    "number": 1.5,
    "string": "schemas",
    "array": [],
    "object": {},
}

# The 9 loader keys a sealed definition does not place yet. Placing one means
# deciding whether it belongs to the definition or the environment; remove it
# from here in the same change.
AWAITING_SEALED_PLACEMENT = frozenset({
    "code_drift", "coverage", "deployment_up", "docker", "host_storage",
    "mcp", "publisher", "readiness", "search",
})


def _deployment(tmp_path: Path, extra: dict[str, Any], name: str = "probe") -> Path:
    """Write a minimal valid deployment plus ``extra`` and return its manifest."""

    base = tmp_path / name
    (base / "schemas").mkdir(parents=True, exist_ok=True)
    (base / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    document = {
        "name": name,
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        **extra,
    }
    manifest = base / "deployment.yaml"
    manifest.write_text(yaml.safe_dump(document, sort_keys=True), encoding="utf-8")
    return manifest


def _load(manifest: Path, *, lenient: bool = False) -> DeploymentManifest:
    loader = (
        DeploymentManifest.from_yaml_lenient if lenient
        else DeploymentManifest.from_yaml
    )
    with warnings.catch_warnings():
        # `embedding` is the deprecated spelling; its warning is not the point.
        warnings.simplefilter("ignore")
        return loader(manifest)


def _schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _schema_errors(document: Any) -> list[str]:
    validator = Draft202012Validator(_schema())
    return [error.message for error in validator.iter_errors(document)]


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.table
# ---------------------------------------------------------------------------

def test_table_declares_each_key_once():
    names = [key.name for key in DEPLOYMENT_KEYS]
    assert len(names) == len(set(names))
    assert set(names) == set(KNOWN_TOP_LEVEL_KEYS) == set(DEPLOYMENT_KEYS_BY_NAME)


def test_table_keeps_every_key_the_loader_accepted_before():
    assert KEYS_ON_DEV_87C3AE1EBA <= KNOWN_TOP_LEVEL_KEYS


def test_table_is_the_loaders_accepted_set():
    assert deployments._KNOWN_TOP_LEVEL_KEYS is KNOWN_TOP_LEVEL_KEYS


def test_table_rows_are_well_formed():
    for key in DEPLOYMENT_KEYS:
        assert key.summary.strip(), key.name
        assert key.types and set(key.types) <= set(JSON_TYPES), key.name
        assert key.read_by in {READ_BY_OKG, READ_BY_DEPLOYMENT, READ_BY_NOTHING}
        if key.read_by == READ_BY_OKG:
            assert key.reader and not key.reason, key.name
        else:
            assert key.reason and not key.reader, key.name
    for required in REQUIRED_KEYS:
        assert "null" not in DEPLOYMENT_KEYS_BY_NAME[required].types


def test_table_module_imports_no_okg_at_module_level():
    tree = ast.parse(KEYS_PY.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Import):
            assert not any(a.name.split(".")[0] == "okg" for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            assert (node.module or "").split(".")[0] != "okg"


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.types
# ---------------------------------------------------------------------------

_GENERIC_ROWS = [key for key in DEPLOYMENT_KEYS if not key.parser]
_PARSER_ROWS = [key for key in DEPLOYMENT_KEYS if key.parser]


def _disallowed(key) -> list[str]:
    allowed = set(key.types)
    if "number" in allowed:
        allowed.add("integer")
    return [label for label in SAMPLES if label not in allowed]


@pytest.mark.parametrize("key", _GENERIC_ROWS, ids=lambda key: key.name)
@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_types_refuse_every_disallowed_type(tmp_path, key, lenient):
    for label in _disallowed(key):
        manifest = _deployment(tmp_path / label, {key.name: SAMPLES[label]})
        with pytest.raises(DeploymentKeyTypeError) as caught:
            _load(manifest, lenient=lenient)
        assert caught.value.key == key.name
        assert caught.value.found == label
        assert f"key {key.name} must be" in str(caught.value)


@pytest.mark.parametrize("key", _PARSER_ROWS, ids=lambda key: key.name)
def test_types_parser_rows_keep_their_own_error(tmp_path, key):
    manifest = _deployment(tmp_path, {key.name: "not-a-block"})
    with pytest.raises(Exception) as caught:
        _load(manifest)
    assert not isinstance(caught.value, DeploymentKeyTypeError)


def _write_raw(tmp_path: Path, text: str) -> Path:
    base = tmp_path / "raw"
    (base / "schemas").mkdir(parents=True)
    (base / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    manifest = base / "deployment.yaml"
    manifest.write_text(
        "schema_dir: schemas\nsource_registry: source_registry.yaml\n" + text,
        encoding="utf-8",
    )
    return manifest


def test_types_yaml_boolean_is_not_a_name(tmp_path):
    manifest = _write_raw(tmp_path, "name: yes\n")
    with pytest.raises(DeploymentKeyTypeError, match="not a boolean"):
        _load(manifest)


def test_types_unquoted_date_is_refused_with_a_quote_hint(tmp_path):
    manifest = _write_raw(tmp_path, "description: 2026-09-26\n")
    with pytest.raises(DeploymentKeyTypeError, match="quote the value"):
        _load(manifest)


def test_types_boolean_key_is_named_not_crashed_on(tmp_path):
    manifest = _write_raw(tmp_path, "on: 1\nbogus_key: 2\n")
    with pytest.raises(DeploymentConfigError, match="not text"):
        _load(manifest)


def test_types_lint_reports_key_type_once(tmp_path):
    from okg.substrate.deployment_lint import lint_deployment

    manifest = _deployment(tmp_path, {"chronos": "retain everything"})
    report = lint_deployment(manifest.parent, env={})
    codes = [finding["code"] for finding in report["findings"]]
    assert codes.count("deployment.manifest.key_type") == 1
    assert "deployment.manifest.model_rejected" not in codes
    assert report["ok"] is False


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.null
# ---------------------------------------------------------------------------

def _model(manifest: DeploymentManifest) -> dict[str, Any]:
    # `effective_raw` is the raw mapping itself, so it records the null key.
    return {
        field.name: getattr(manifest, field.name)
        for field in dataclasses.fields(manifest)
        if field.name != "effective_raw"
    }


@pytest.mark.parametrize(
    "key",
    [key for key in DEPLOYMENT_KEYS if "null" in key.types],
    ids=lambda key: key.name,
)
@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_null_loads_like_an_absent_key(tmp_path, key, lenient):
    absent = _model(_load(_deployment(tmp_path / "absent", {}, "p"), lenient=lenient))
    null = _model(
        _load(_deployment(tmp_path / "null", {key.name: None}, "p"), lenient=lenient),
    )
    # Both deployments are named `p` but live in different directories.
    normalize = lambda model, where: json.loads(  # noqa: E731
        json.dumps(model, default=str).replace(str(where), "<dir>"),
    )
    assert normalize(null, tmp_path / "null") == normalize(absent, tmp_path / "absent")


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.schema
# ---------------------------------------------------------------------------

def test_schema_checked_in_file_is_generated():
    assert SCHEMA_PATH.read_text(encoding="utf-8") == render_deployment_schema(), (
        "run `python scripts/gen_deployment_schema.py`"
    )


def test_schema_check_flag_refuses_one_edited_byte(tmp_path, monkeypatch):
    script = runpy.run_path(str(ROOT / "scripts" / "gen_deployment_schema.py"))
    main = script["main"]
    target = tmp_path / "deployment.schema.json"
    target.write_text(render_deployment_schema(), encoding="utf-8")
    main.__globals__["TARGET"] = target
    main.__globals__["ROOT"] = tmp_path
    monkeypatch.setattr(sys, "argv", ["gen_deployment_schema.py", "--check"])
    assert main() == 0
    text = target.read_text(encoding="utf-8")
    target.write_text(text[:-2] + "}\n\n", encoding="utf-8")
    assert main() == 1


def test_schema_output_does_not_depend_on_hash_seed():
    code = (
        "from okg.substrate.deployments.keys import render_deployment_schema;"
        "import sys; sys.stdout.write(render_deployment_schema())"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True, text=True, check=True,
        ).stdout
        for seed in ("0", "1", "12345")
    }
    assert outputs == {render_deployment_schema()}


def test_schema_top_level_matches_the_table():
    schema = _schema()
    Draft202012Validator.check_schema(schema)
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["schema_dir", "source_registry"]
    assert set(schema["properties"]) == KNOWN_TOP_LEVEL_KEYS
    for key in DEPLOYMENT_KEYS:
        declared = schema["properties"][key.name]["type"]
        declared = [declared] if isinstance(declared, str) else declared
        assert set(declared) == set(key.types), key.name
    for stale in ("embedder", "retention", "workers"):
        assert stale not in schema["properties"]


def _closed_spots(node: Any, pointer: str = "") -> list[str]:
    spots: list[str] = []
    if isinstance(node, dict):
        if node.get("additionalProperties") is False and pointer:
            spots.append(pointer)
        for name, child in node.items():
            spots.extend(_closed_spots(child, f"{pointer}/{name}"))
    elif isinstance(node, list):
        for index, child in enumerate(node):
            spots.extend(_closed_spots(child, f"{pointer}/{index}"))
    return spots


def _via_loader(key: str, valid: dict[str, Any]) -> Callable:
    """Load a deployment whose ``key`` block is ``block``."""

    def run(tmp_path: Path, block: dict[str, Any]) -> None:
        _load(_deployment(tmp_path, {key: block}))

    run.valid = valid  # type: ignore[attr-defined]
    return run


def _history(valid: dict[str, Any], wrap: Callable[[dict], dict]) -> Callable:
    def run(tmp_path: Path, block: dict[str, Any]) -> None:
        parse_history_retention(
            wrap(block), catalog_node_subtypes=set(), observed_edge_types=set(),
        )

    run.valid = valid  # type: ignore[attr-defined]
    return run


def _code_drift(valid: dict[str, Any]) -> Callable:
    """The drift probe owns `code_drift`: it refuses an unknown key as
    `misconfigured` when it runs, while the loader only type-checks it."""

    def run(tmp_path: Path, block: dict[str, Any]) -> None:
        from okg.substrate.runtime.deployment_drift import _declared_mode

        _, _, refusal = _declared_mode({"code_drift": block})
        if refusal is not None:
            raise ValueError(refusal)

    run.valid = valid  # type: ignore[attr-defined]
    return run


def _budget(valid: dict[str, Any]) -> Callable:
    def run(tmp_path: Path, block: dict[str, Any]) -> None:
        _load(_deployment(tmp_path, {"publisher": {"first_publish_warning_budget": block}}))

    run.valid = valid  # type: ignore[attr-defined]
    return run


# Every closed spot in the generated schema, with a valid block and the parser
# that must refuse the same block plus one unknown key.
CLOSED_SPOT_PROBES: dict[str, Callable] = {
    "/properties/agent_surface": _via_loader("agent_surface", {"served": ["search"]}),
    "/properties/chat": _via_loader("chat", {"enabled": False}),
    "/properties/code_drift": _code_drift({"mode": "pinned"}),
    "/properties/coverage": _via_loader("coverage", {}),
    "/properties/embedding": _via_loader("embedding", {"enabled": False, "reason": "probe"}),
    "/properties/embeddings": _via_loader("embeddings", {"enabled": False, "reason": "probe"}),
    "/properties/mcp": _via_loader("mcp", {}),
    "/properties/postgres": _via_loader("postgres", {"dsn": "postgresql://h/db"}),
    "/properties/chronos": _via_loader("chronos", {"interval_continuation_percent": 95}),
    "/properties/publisher": _via_loader("publisher", {}),
    "/properties/publisher/properties/first_publish_warning_budget": _budget(
        {"node_count": 1, "edge_count": 1},
    ),
    "/properties/runtime": _via_loader("runtime", {}),
    "/properties/search": _via_loader("search", {}),
    "/properties/chronos/properties/history_retention": _history(
        {"default": "full", "audit_tail_generations": 1}, lambda block: block,
    ),
    "/$defs/historyRetentionRule/oneOf/1": _history(
        {"horizon": "1d"},
        lambda rule: {"default": rule, "audit_tail_generations": 1},
    ),
}


def test_schema_closes_a_block_only_where_its_parser_refuses_unknown_keys(tmp_path):
    assert sorted(_closed_spots(_schema())) == sorted(CLOSED_SPOT_PROBES)
    for index, (spot, probe) in enumerate(sorted(CLOSED_SPOT_PROBES.items())):
        probe(tmp_path / f"valid-{index}", dict(probe.valid))
        with pytest.raises(Exception) as caught:
            probe(tmp_path / f"extra-{index}", {**probe.valid, "zzz_unknown": 1})
        # Some parsers name the key in the message, history retention in its
        # data, and a retention rule refuses anything but `horizon`.
        said = str(caught.value) + repr(getattr(caught.value, "data", None))
        assert "zzz_unknown" in said or "horizon" in said, (spot, said)
        assert _schema_errors_at(spot, {**probe.valid, "zzz_unknown": 1}), spot
        assert not _schema_errors_at(spot, dict(probe.valid)), spot


def _schema_errors_at(pointer: str, value: Any) -> list[str]:
    schema = _schema()
    fragment: Any = schema
    for part in pointer.strip("/").split("/"):
        fragment = fragment[int(part)] if isinstance(fragment, list) else fragment[part]
    wrapped = {"$defs": schema["$defs"], **fragment}
    return [error.message for error in Draft202012Validator(wrapped).iter_errors(value)]


def test_schema_example_loads_and_validates(tmp_path):
    example = ROOT / "schemas" / "examples" / "deployment.yaml"
    document = yaml.safe_load(example.read_text(encoding="utf-8"))
    assert _schema_errors(document) == []
    base = tmp_path / "acme-ops"
    (base / document["schema_dir"]).mkdir(parents=True)
    (base / document["source_registry"]).write_text("sources: {}\n", encoding="utf-8")
    shutil.copy(example, base / "deployment.yaml")
    manifest = _load(base / "deployment.yaml")
    assert manifest.name == "acme-ops"


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.agreement
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("key", DEPLOYMENT_KEYS, ids=lambda key: key.name)
def test_agreement_loader_and_schema_on_every_type(tmp_path, key):
    """Loader accepts => schema accepts; row disallows => loader refuses."""

    allowed = set(key.types) | ({"integer"} if "number" in key.types else set())
    for label, value in SAMPLES.items():
        manifest = _deployment(tmp_path / label, {key.name: value})
        document = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        try:
            _load(manifest)
        except Exception:
            loaded = False
        else:
            loaded = True
        if loaded:
            assert _schema_errors(document) == [], (key.name, label)
        if label not in allowed:
            assert not loaded, (key.name, label)


def _tracked_manifests() -> list[Path]:
    listed = subprocess.run(
        ["git", "ls-files", "*deployment.yaml"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return [
        ROOT / rel for rel in listed
        if rel.endswith("/deployment.yaml")
        and not rel.startswith(("pact/archive/", "openspec/"))
    ]


def test_agreement_every_tracked_manifest_that_loads_validates():
    loaded = 0
    for manifest in _tracked_manifests():
        try:
            _load(manifest)
        except Exception:
            continue
        loaded += 1
        document = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        assert _schema_errors(document) == [], manifest.relative_to(ROOT)
    # 18 of the 25 tracked manifests loaded on dev 87c3ae1eba; the public copy
    # leaves this repository's own deployments out, and 13 of its manifests
    # load (export of 5038de8f6a).
    assert loaded >= (13 if is_public_copy() else 18)


@pytest.mark.parametrize("value", [0, 900000, -1, "5", True, 1.5])
def test_agreement_postgres_idle_bound(tmp_path, value):
    extra = {"postgres": {"idle_in_transaction_session_timeout_ms": value}}
    manifest = _deployment(tmp_path, extra)
    try:
        _load(manifest)
    except Exception:
        loaded = False
    else:
        loaded = True
    document = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    assert loaded == (not _schema_errors(document)), value


@pytest.mark.parametrize("policy", [
    {"default": "full", "audit_tail_generations": 1},
    {"default": {"horizon": "30d"}, "audit_tail_generations": 5,
     "nodes": {}, "edges": {}},
    {"default": "sometimes", "audit_tail_generations": 1},
    {"default": "full", "audit_tail_generations": 0},
    {"default": {"horizon": "30"}, "audit_tail_generations": 1},
    {"audit_tail_generations": 1},
])
def test_agreement_history_retention(policy):
    try:
        parse_history_retention(policy, catalog_node_subtypes=set(), observed_edge_types=set())
    except Exception:
        parsed = False
    else:
        parsed = True
    errors = _schema_errors_at(
        "/properties/chronos/properties/history_retention", policy,
    )
    assert parsed == (not errors), policy


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.bootstrap
# ---------------------------------------------------------------------------

_STAGED = {
    "name": "staged",
    "schema_dir": "schemas",
    "source_registry": "source_registry.yaml",
    "postgres": {"dsn": "${OKG_DSN}", "database": "okg_staged"},
    "embeddings": {"enabled": False, "reason": "staged fixture"},
    "host_storage": {"warn_available_pct": 20, "blocker_available_pct": 10},
}


def test_bootstrap_accepts_keys_the_loader_accepts():
    _validate_artifact_payloads({"deployment.yaml": yaml.safe_dump(_STAGED)})


def test_bootstrap_refuses_a_key_the_loader_refuses():
    staged = {**_STAGED, "workers": {"concurrency": {"ingest": 2}}}
    with pytest.raises(BootstrapDraftInvalid):
        _validate_artifact_payloads({"deployment.yaml": yaml.safe_dump(staged)})


@pytest.mark.parametrize("authored,schema_name", sorted(AUTHORED_FILE_SCHEMAS.items()))
def test_bootstrap_hint_names_a_working_schema_show(authored, schema_name, capsys):
    from okg.substrate.cli.main import _cmd_schema_show

    with pytest.raises(BootstrapDraftInvalid) as caught:
        validate_against_schema(42, schema_name, path=f"staged/{authored}")
    hint = caught.value.fix_hint
    match = re.search(r"`okg schema show ([^`]+)`", hint or "")
    assert match and match.group(1) == authored, hint
    assert _cmd_schema_show(Namespace(file=match.group(1)), as_json=False) == 0
    assert json.loads(capsys.readouterr().out)


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.unread
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "key",
    [key for key in DEPLOYMENT_KEYS if key.read_by == READ_BY_OKG],
    ids=lambda key: key.name,
)
def test_unread_reader_module_mentions_its_key(key):
    """A guard against the named reader disappearing, not proof the key works."""

    spec = importlib.util.find_spec(key.reader)
    assert spec is not None and spec.origin, key.reader
    source = Path(spec.origin).read_text(encoding="utf-8")
    quoted = re.search(rf"""(['"]){re.escape(key.name)}\1""", source)
    attribute = re.search(rf"\.{re.escape(key.name)}\b", source)
    assert quoted or attribute, f"{key.reader} does not mention {key.name!r}"


def _lint_findings(manifest: Path) -> tuple[bool, list[dict[str, Any]]]:
    from okg.substrate.deployment_lint import lint_deployment

    report = lint_deployment(manifest.parent, env={})
    return report["ok"], report["findings"]


def test_unread_lint_warns_and_still_passes(tmp_path):
    base_ok, base = _lint_findings(_deployment(tmp_path / "base", {}))
    ok, findings = _lint_findings(_deployment(tmp_path / "unread", {
        "snapshot_path": "schemas",
        "publisher": {"kickoff_interval": "30s"},
    }))
    unread = [f for f in findings if f["code"] == "deployment.manifest.unread_key"]
    assert sorted(f["observed"] for f in unread) == [
        "publisher.kickoff_interval", "snapshot_path",
    ]
    assert {f["severity"] for f in unread} == {"warning"}
    assert all("nothing in okg acts on" in f["message"] for f in unread)
    assert ok == base_ok
    other = lambda rows: sorted(  # noqa: E731
        f["code"] for f in rows if f["code"] != "deployment.manifest.unread_key"
    )
    assert other(findings) == other(base)


def test_unread_deployment_owned_key_gets_no_warning(tmp_path):
    _, findings = _lint_findings(
        _deployment(tmp_path, {"modules_config": {"cern_twiki": {"site": "x"}}}),
    )
    assert not [f for f in findings if f["code"] == "deployment.manifest.unread_key"]


# ---------------------------------------------------------------------------
# req.issue-2571-deployment-keys.sealed
# ---------------------------------------------------------------------------

def test_sealed_every_key_is_placed_or_listed_as_awaiting():
    placed = _DEFINITION_KEYS | _ENVIRONMENT_OWNED_DEFINITION_KEYS
    unplaced = KNOWN_TOP_LEVEL_KEYS - placed
    assert unplaced == AWAITING_SEALED_PLACEMENT, (
        "a table key must be placed in environment.py's definition or "
        "environment-owned set, or listed in AWAITING_SEALED_PLACEMENT"
    )


# ---------------------------------------------------------------------------
# Review follow-ups (code review of 6d94b8168e)
# ---------------------------------------------------------------------------

def test_types_error_survives_pickling():
    """Workflow runtimes store errors by pickling them."""

    import pickle

    error = DeploymentKeyTypeError(
        "chronos", allowed=("null", "object"), found="string",
        manifest_path=Path("deployment.yaml"),
    )
    restored = pickle.loads(pickle.dumps(error))
    assert isinstance(restored, DeploymentKeyTypeError)
    assert (restored.key, restored.found, str(restored)) == (
        error.key, error.found, str(error),
    )


@pytest.mark.parametrize("value", [5, True, 1.5, "prefix"])
def test_types_lint_reports_a_non_list_record_prefixes(tmp_path, value):
    """Lint used to crash iterating a non-list `record_prefixes`."""

    from okg.substrate.deployment_lint import lint_deployment

    manifest = _deployment(tmp_path, {"record_prefixes": value})
    codes = [f["code"] for f in lint_deployment(manifest.parent, env={})["findings"]]
    assert "deployment.manifest.key_type" in codes
    assert "deployment.manifest.model_load_error" not in codes


@pytest.mark.parametrize("key", ["checkpoint_attestors", "checkpoint_attestor_root"])
def test_null_refused_where_the_publisher_refuses_it(tmp_path, key):
    """The publisher reads these raw and refuses null, so the table does too."""

    assert "null" not in DEPLOYMENT_KEYS_BY_NAME[key].types
    with pytest.raises(DeploymentKeyTypeError):
        _load(_deployment(tmp_path, {key: None}))


# ---------------------------------------------------------------------------
# PACT issue-2634-closed-blocks: unknown keys inside `postgres` and `chronos`
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(("block", "value", "typo"), [
    ("postgres", {"dns": "postgresql://h/db"}, "dns"),
    ("postgres", {"dsn": "postgresql://h/db", "databse": "db"}, "databse"),
    ("chronos", {"interval_continuation_percnt": 95}, "interval_continuation_percnt"),
])
@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_closed_block_refuses_a_misspelled_key(tmp_path, block, value, typo, lenient):
    manifest = _deployment(tmp_path, {block: value})
    with pytest.raises(DeploymentConfigError) as caught:
        _load(manifest, lenient=lenient)
    message = str(caught.value)
    assert f"{block} block has unknown key(s): {typo!r}" in message
    known = {"postgres": deployments._POSTGRES_KEYS, "chronos": deployments._CHRONOS_KEYS}
    assert all(key in message for key in known[block])


@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_closed_block_accepts_every_known_key(tmp_path, lenient):
    manifest = _deployment(tmp_path, {
        "postgres": {
            "dsn": "postgresql://h/db",
            "database": "db",
            "self_hosted_authoritative": False,
            "idle_in_transaction_session_timeout_ms": 0,
        },
        "chronos": {
            "interval_continuation_percent": 95,
            "history_retention": {"default": "full", "audit_tail_generations": 1},
        },
    })
    loaded = _load(manifest, lenient=lenient)
    assert loaded.chronos["interval_continuation_percent"] == 95


@pytest.mark.parametrize("block", ["postgres", "chronos"])
@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_closed_block_names_a_key_that_is_not_text(tmp_path, block, lenient):
    manifest = _write_raw(tmp_path, f"{block}:\n  on: 1\n")
    with pytest.raises(DeploymentConfigError, match=r"unknown key\(s\): True"):
        _load(manifest, lenient=lenient)


def test_closed_block_lint_reports_a_blocker(tmp_path):
    from okg.substrate.deployment_lint import lint_deployment

    manifest = _deployment(tmp_path, {"chronos": {"interval_continuation_percnt": 95}})
    report = lint_deployment(manifest.parent, env={})
    rejected = [
        f for f in report["findings"] if f["code"] == "deployment.manifest.model_rejected"
    ]
    assert report["ok"] is False
    assert [f["severity"] for f in rejected] == ["blocker"]
    assert "interval_continuation_percnt" in rejected[0]["observed"]


# For each accepted key, one okg module and the lookup in it that reads the key
# from the deployment's block. The test looks for that exact lookup, so it
# fails when the reader is rewritten or removed and the key would stay
# accepted with nothing reading it.
CLOSED_BLOCK_READERS: dict[str, dict[str, tuple[str, str]]] = {
    "postgres": {
        "dsn": (
            "okg.substrate.cli.main", '(manifest.get("postgres") or {}).get("dsn")',
        ),
        "database": ("okg.substrate.deployment_authority", 'postgres.get("database")'),
        "self_hosted_authoritative": (
            "okg.substrate.deployment_authority",
            'raw["postgres"].get("self_hosted_authoritative")',
        ),
        # The loader's idle-bound reader names the key through a constant.
        "idle_in_transaction_session_timeout_ms": (
            "okg.substrate.deployments", "pg_block[_IDLE_IN_TRANSACTION_KEY]",
        ),
    },
    "chronos": {
        # `raw` there is the loaded manifest's `chronos` block.
        "interval_continuation_percent": (
            "okg.substrate.graph_store_internal.common",
            'raw["interval_continuation_percent"]',
        ),
        "history_retention": (
            "okg.substrate.publisher.deployment_stack",
            'chronos.get("history_retention")',
        ),
    },
}


def test_closed_block_readers_look_up_every_accepted_key():
    assert set(CLOSED_BLOCK_READERS["postgres"]) == deployments._POSTGRES_KEYS
    assert set(CLOSED_BLOCK_READERS["chronos"]) == deployments._CHRONOS_KEYS
    assert deployments._IDLE_IN_TRANSACTION_KEY == "idle_in_transaction_session_timeout_ms"
    for readers in CLOSED_BLOCK_READERS.values():
        for key, (module, lookup) in readers.items():
            spec = importlib.util.find_spec(module)
            assert spec is not None and spec.origin, module
            source = Path(spec.origin).read_text(encoding="utf-8")
            assert lookup in source, f"{module} no longer reads {key!r} with {lookup}"


@pytest.mark.parametrize("block", ["postgres", "chronos"])
@pytest.mark.parametrize("lenient", [False, True], ids=["strict", "lenient"])
def test_closed_block_null_loads(tmp_path, block, lenient):
    manifest = _deployment(tmp_path, {block: None})
    loaded = _load(manifest, lenient=lenient)
    assert loaded.chronos is None
    if block == "postgres":
        # No DSN is declared, so nothing resolves one.
        assert loaded.effective_raw["postgres"] is None


@pytest.mark.parametrize(("plan_postgres", "expected"), [
    (None, {"dsn": "${OKG_DSN}"}),
    ({"dsn_env": "OKG_FOO"}, {"dsn": "${OKG_FOO}"}),
    ({"dsn": "postgresql://h/db"}, {"dsn": "postgresql://h/db"}),
])
def test_closed_block_bootstrap_writes_only_keys_the_loader_accepts(
    tmp_path, plan_postgres, expected,
):
    """A plan may name the DSN's variable; the written block must still load."""

    from okg.substrate.deployment_bootstrap.workflow import _deployment_postgres_block

    block = _deployment_postgres_block(plan_postgres)
    assert block == expected
    _load(_deployment(tmp_path, {"postgres": block}))


def test_closed_block_bootstrap_draft_writes_dsn_env_as_dsn(tmp_path):
    """The draft step, not just the helper, turns a plan's `dsn_env` into `dsn`."""

    from okg.substrate.deployment_bootstrap.workflow import (
        _build_artifacts,
        _ensure_default_authoring_plan,
        create_plan,
        draft,
        load_plan,
        save_plan,
    )

    dep = tmp_path / "boot"
    brief = tmp_path / "brief.md"
    brief.write_text("A probe deployment.\n", encoding="utf-8")
    create_plan(deployment=dep, brief_file=brief, apply=True)
    plan = load_plan(dep)
    plan["deployment"]["postgres"] = {"dsn_env": "OKG_FOO"}
    save_plan(dep, plan)

    draft(deployment=dep)  # validates the staged files against their schemas
    plan = load_plan(dep)
    _ensure_default_authoring_plan(plan)
    written = yaml.safe_load(_build_artifacts(dep, plan)["deployment.yaml"])
    assert written["postgres"] == {"dsn": "${OKG_FOO}"}
    _load(_deployment(tmp_path / "load", {"postgres": written["postgres"]}))
