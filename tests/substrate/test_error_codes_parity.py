"""error_codes.yaml ↔ Python SubstrateError classes — parity in BOTH directions.

A drift in either direction is a contract bug:

- A YAML code with a `python_exception` field but no Python class:
  CLI / runbook references will dispatch on a code the Python layer
  cannot raise.
- A Python class with a code not in YAML: agents / runbooks will
  see a code that doesn't appear in the documented registry.
"""

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path

import pytest
import yaml

import okg.substrate
from okg.substrate.errors import all_error_codes

ERROR_CODES_YAML = (
    Path(__file__).resolve().parents[2] / "docs" / "reference" / "error_codes.yaml"
)


# Codes that appear in the YAML but are emitted from outside Python
# exception classes (e.g. CLI-level conditions, observability-only
# events). These are intentionally NOT in errors.py.
EMITTED_WITHOUT_EXCEPTION = {
    "dedupe_collision",  # observability event; UNIQUE constraint emits it
    "requires_confirm",  # CLI-level; dispatcher emits before any code runs
}

# Ad-hoc codes attached per-instance to SubstrateError instead of
# their own class. These appear at runtime (caller sets `.code`)
# but `all_error_codes()` does not list them by class.
ADHOC_CODES = {
    "dsn_not_set",  # connection.py:dsn_from_env
    "bootstrap_refused",  # bootstrap.py:bootstrap_snapshot
    # Test-only stubs intentionally not in YAML (Stride A.1).
    "test_backend_unavailable",
    # CLI-level codes attached per-instance (no exception class):
    "embedder_spec_invalid",  # embeddings/identity.py:build_embedder
    # Semantic-search readiness refusals (PACT reachable-semantic-search).
    # Attached per-instance rather than given classes because each carries a
    # measured `data` payload assembled at the raise site.
    "embedder_identity_undeclared",  # embeddings/identity.py
    "semantic_embedder_not_configured",  # cli/_read_side.py
    "semantic_index_empty",  # mcp/semantic_readiness.py
    "embedding_model_skew",  # mcp/semantic_readiness.py
    "semantic_coverage_unmeasurable",  # mcp/semantic_readiness.py
    "no_applied_catalog",  # cli/main.py:_cmd_catalog_reconcile
    "source_registry_invalid",  # cli/main.py:_cmd_ingest
    "source_registry_cycle",  # cli/main.py:_cmd_ingest
    "deployment_runtime_config_unresolved",  # cli/main.py:_deployment_runtime_enabled
    "deployment_not_found",  # jobs/worker.py:handle_ingest
    "source_sync_payload_invalid",  # jobs/worker.py:handle_ingest
    "source_sync_ingest_failed",  # jobs/worker.py:handle_ingest
    # `okg runtime cancel-workflow` refusals (issues #1257, #1254). Each
    # carries a payload read from the observed workflow row at the raise
    # site, so they are attached per-instance rather than given classes.
    "deployment_queues_unresolved",  # runtime/workflow_cancel.py
    "dbos_schema_absent",  # runtime/workflow_cancel.py
    "workflow_not_found",  # runtime/workflow_cancel.py
    "workflow_already_terminal",  # runtime/workflow_cancel.py
    "workflow_outside_deployment",  # runtime/workflow_cancel.py
    "workflow_status_changed",  # runtime/workflow_cancel.py
    # The read operator commands' own usage refusals (PACT
    # read-commands-3666); each names the flags or file it met.
    "read_args_invalid",  # cli/_read_operators.py
    "read_args_unreadable",  # cli/_read_operators.py
    "read_argument_given_twice",  # cli/_read_operators.py
    "search_flags_conflict",  # cli/_read_operators.py
    "read_command_failed",  # cli/_read_operators.py
    # Their shell forms' refusals (PACT read-shell-forms-3666).
    "read_where_invalid",  # cli/_read_shell.py
    "read_input_unresolved",  # cli/_read_shell.py
    "read_stdin_invalid",  # cli/_read_shell.py
    "read_all_incomplete",  # cli/_read_shell.py
    "read_select_text_unavailable",  # cli/_read_shell.py
    "read_output_flags_conflict",  # cli/_read_shell.py
    "read_lines_invalid",  # cli/_read_shell.py (PACT read-shell-texts-3666)
}


def _defines_error_code_class(file_path: Path) -> bool:
    """Return whether a file defines a class-level ``code`` candidate.

    Importing this mechanical superset lets ``all_error_codes()`` perform the
    runtime SubstrateError-subclass check.  Looking only for a literal
    ``SubstrateError`` base would miss indirect subclasses and aliased bases.
    """
    try:
        tree = ast.parse(file_path.read_bytes(), filename=str(file_path))
    except Exception as exc:
        raise AssertionError(f"Failed to parse Python file {file_path}: {exc}") from exc

    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for statement in node.body:
            targets = (
                statement.targets
                if isinstance(statement, ast.Assign)
                else [statement.target]
                if isinstance(statement, ast.AnnAssign)
                else []
            )
            if any(
                isinstance(target, ast.Name) and target.id == "code"
                for target in targets
            ):
                return True
    return False


def _module_name_for_file(file_path: Path, package_root: Path) -> str:
    """Map a package Python file to its importable dotted module name."""
    try:
        relative_path = file_path.relative_to(package_root)
    except ValueError as exc:
        raise AssertionError(
            f"Cannot map Python file outside package root: "
            f"file={file_path}, package_root={package_root}"
        ) from exc

    module_parts = list(relative_path.with_suffix("").parts)
    if module_parts[-1] == "__init__":
        module_parts.pop()
    invalid_parts = [part for part in module_parts if not part.isidentifier()]
    if invalid_parts:
        raise AssertionError(
            f"Cannot map Python file {file_path} to an importable module; "
            f"invalid path components: {invalid_parts}"
        )
    return ".".join((okg.substrate.__name__, *module_parts))


def _load_substrate_error_modules() -> None:
    """Mechanically discover and import every SubstrateError-defining module."""
    package_file = okg.substrate.__file__
    if package_file is None:
        raise AssertionError(
            f"Installed package {okg.substrate.__name__} has no filesystem root"
        )
    package_root = Path(package_file).resolve().parent
    if not package_root.is_dir():
        raise AssertionError(
            f"Installed package root is not a directory: "
            f"package={okg.substrate.__name__}, root={package_root}"
        )

    modules_by_name: dict[str, Path] = {}
    for file_path in sorted(package_root.rglob("*.py")):
        if not _defines_error_code_class(file_path):
            continue
        module_name = _module_name_for_file(file_path, package_root)
        previous_path = modules_by_name.setdefault(module_name, file_path)
        if previous_path != file_path:
            raise AssertionError(
                f"Multiple Python files map to module {module_name}: "
                f"{previous_path}, {file_path}"
            )

    for module_name in sorted(modules_by_name):
        file_path = modules_by_name[module_name]
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            raise AssertionError(
                f"Failed to import discovered error-code module "
                f"{module_name} from {file_path}: {exc}"
            ) from exc


_load_substrate_error_modules()


def _load_registry() -> dict:
    registry = yaml.safe_load(ERROR_CODES_YAML.read_text())
    seen: set[str] = set()
    for entry in registry["codes"]:
        code = entry["code"]
        assert code not in seen, f"Duplicate error-code registry entry: {code}"
        seen.add(code)
    return registry


def test_registry_codes_are_unique() -> None:
    _load_registry()


@pytest.mark.parametrize("conflicting", [False, True])
def test_registry_loader_refuses_duplicate_rows(
    tmp_path, monkeypatch, conflicting
) -> None:
    first = {"code": "duplicate_probe", "data_shape": []}
    second = {**first, "data_shape": ["different"]} if conflicting else dict(first)
    path = tmp_path / "registry.yaml"
    path.write_text(yaml.safe_dump({"codes": [first, second]}))
    monkeypatch.setattr(sys.modules[__name__], "ERROR_CODES_YAML", path)
    with pytest.raises(
        AssertionError, match="Duplicate error-code registry entry: duplicate_probe"
    ):
        _load_registry()


def test_registry_loader_accepts_unique_rows(tmp_path, monkeypatch) -> None:
    registry = {"codes": [{"code": "first"}, {"code": "second"}]}
    path = tmp_path / "registry.yaml"
    path.write_text(yaml.safe_dump(registry))
    monkeypatch.setattr(sys.modules[__name__], "ERROR_CODES_YAML", path)
    observed = _load_registry()
    assert observed == registry


def test_every_yaml_python_code_has_a_class() -> None:
    """YAML → Python: every code with `python_exception` set has a class."""
    registry = _load_registry()
    python_codes = all_error_codes()
    missing: list[str] = []
    for c in registry["codes"]:
        if c.get("python_exception") is None:
            continue
        if c["code"] in ADHOC_CODES:
            continue
        if c["code"] not in python_codes:
            missing.append(c["code"])
    assert not missing, (
        f"YAML codes lacking Python exception classes: {missing}. "
        f"Either add a SubstrateError subclass or set python_exception "
        f"to null in error_codes.yaml."
    )


def test_every_python_code_is_registered() -> None:
    """Python → YAML: every Python class code appears in the registry."""
    registry = _load_registry()
    yaml_codes = {c["code"] for c in registry["codes"]}
    extra: list[str] = []
    for code in all_error_codes():
        if code in ADHOC_CODES:
            continue
        if code not in yaml_codes:
            extra.append(code)
    assert not extra, (
        f"Python error classes have codes not in error_codes.yaml: "
        f"{extra}. Add them to the registry or remove the class."
    )


def test_emitted_without_exception_codes_are_in_yaml() -> None:
    """Codes documented as exception-less (DLQ collisions, CLI gates)
    must still appear in the registry."""
    registry = _load_registry()
    yaml_codes = {c["code"] for c in registry["codes"]}
    for code in EMITTED_WITHOUT_EXCEPTION:
        assert code in yaml_codes, (
            f"{code!r} is in EMITTED_WITHOUT_EXCEPTION allow-list but "
            f"missing from error_codes.yaml"
        )


def test_substrate_error_root_is_excluded() -> None:
    """The base class's code 'substrate_error' is the per-instance
    catch-all and is NOT supposed to appear in all_error_codes()."""
    python_codes = all_error_codes()
    assert "substrate_error" not in python_codes


def _documented_error(call, code: str) -> dict:
    """Measure a real emitter, then check its documented mandatory keys."""
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        call()
    error = raised.value
    assert error.code == code
    entry = next(row for row in _load_registry()["codes"] if row["code"] == code)
    assert set(entry["data_shape"]) <= error.data.keys()
    return error.data


def test_alias_configuration_payload_variants(tmp_path) -> None:
    from okg.substrate.library.sources.concept_aliases import (
        ConceptAliasesSource,
        parse_aliases_file,
    )

    code = "concept_aliases_path_missing"
    observed = _documented_error(lambda: ConceptAliasesSource(aliases_path=""), code)
    assert observed == {}
    missing = tmp_path / "missing.yaml"
    observed = _documented_error(lambda: parse_aliases_file(missing), code)
    assert observed == {"path": str(missing)}
    source = ConceptAliasesSource(aliases_path=str(missing))
    for call in (source._canonical_resolution_watermark, lambda: source.run("proof")):
        observed = _documented_error(call, "concept_aliases_no_dsn")
        assert observed == {}
    malformed = tmp_path / "aliases.yaml"
    malformed.write_text("not_aliases: []\n")
    observed = _documented_error(
        lambda: parse_aliases_file(malformed), "concept_aliases_parse_error"
    )
    assert observed == {"path": str(malformed)}


def test_persona_prompt_error_reports_path(tmp_path) -> None:
    from okg.substrate.mcp.review_as import _read_prompt

    missing = tmp_path / "missing.md"
    observed = _documented_error(
        lambda: _read_prompt(str(missing), tmp_path), "persona_prompt_missing"
    )
    assert observed == {"path": str(missing)}


@pytest.mark.parametrize(
    "args,extra",
    [({"max_hops": 4}, {"max_hops": 4}), ({"path": ["a"] * 4}, {"path_len": 4})],
)
def test_traversal_errors_share_only_tool_and_index(args, extra) -> None:
    from okg.substrate.mcp.framework.schema import _validate_traverse_path_args

    observed = _documented_error(
        lambda: _validate_traverse_path_args(args=args, tool_name="probe", index=2),
        "pre_join_tool_traverse_hops_exceeded",
    )
    assert observed == {"name": "probe", "index": 2, **extra}


def test_schema_errors_include_empty_and_disjoint_payloads() -> None:
    from okg.substrate.mcp.framework.resolvers import ResolverRegistry
    from okg.substrate.mcp.framework.schema import parse_predicate, parse_pre_join_tools

    code = "pre_join_tool_schema_error"
    observed = _documented_error(lambda: parse_predicate(""), code)
    assert observed == {}
    observed = _documented_error(lambda: parse_pre_join_tools({"tools": "wrong"}), code)
    assert observed == {"observed": "str"}
    observed = _documented_error(
        lambda: ResolverRegistry({}).register_from_manifest({"probe": None}), code
    )
    assert observed == {"slug": "probe"}


def test_generic_tool_errors_have_disjoint_contexts() -> None:
    from okg.substrate.mcp.framework.executor import execute_pre_join_tool
    from okg.substrate.mcp.framework.resolvers import ResolverRegistry
    from okg.substrate.mcp.framework.schema import PreJoinParam, PreJoinToolSpec
    from okg.substrate.mcp.review_as import build_review_neighborhood_bundle

    # Both public boundaries refuse before any connection/session access.
    spec = PreJoinToolSpec("probe", "", (PreJoinParam("required", "string", True),), ())
    code = "pre_join_tool_error"
    observed = _documented_error(
        lambda: execute_pre_join_tool(
            spec, conn=None, session=None, params={}, resolvers=ResolverRegistry({})
        ),
        code,
    )
    assert observed == {"name": "probe", "missing": ["required"]}
    observed = _documented_error(
        lambda: build_review_neighborhood_bundle(
            "node",
            conn=None,
            session=None,
            pre_join_tools={},
            resolvers=ResolverRegistry({}),
            tool_name="absent",
        ),
        code,
    )
    assert observed == {"tool": "absent"}


def test_result_and_predicate_errors_do_not_require_context() -> None:
    from okg.substrate.library.sources.experimental.bench_outputs import (
        BenchOutputsSource,
    )
    from okg.substrate.library.sources.experimental.predicates import Predicate

    observed = _documented_error(
        lambda: BenchOutputsSource("*.json", result_schema=None), "result_schema_error"
    )
    assert observed == {}
    observed = _documented_error(
        lambda: Predicate.parse("not a predicate"),
        "success_condition_predicate_parse_error",
    )
    assert observed == {}


def test_summary_template_error_is_a_warning(caplog) -> None:
    from okg.substrate.mcp.framework.executor import format_summary_template

    observed = format_summary_template(template="{missing}", params={}, bundles={})
    assert observed == "{missing}"
    assert "pre_join_tool_summary_template_error" in caplog.text
    row = next(
        row
        for row in _load_registry()["codes"]
        if row["code"] == "pre_join_tool_summary_template_error"
    )
    assert row["data_shape"] == []


def test_publisher_cycle_not_dequeued_payload(monkeypatch) -> None:
    """`okg run --once` with no worker consuming the queue (issue #2502):
    the real emitter's payload carries every documented key."""
    from types import SimpleNamespace

    from okg.substrate.runtime import admin
    from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult

    class _NeverDequeued:
        def list_workflows(self, *, workflow_ids=None, **_kwargs):
            if workflow_ids is None:
                return []  # nothing running ahead of the row
            return [SimpleNamespace(status="ENQUEUED", output=None, error=None)]

    monkeypatch.setattr(admin, "_build_client", lambda **_kwargs: _NeverDequeued())
    monkeypatch.setattr(admin, "registered_app_version", lambda _dsn: "app-version-1")
    monkeypatch.setattr(
        admin,
        "enqueue_deployment_workflow",
        lambda **_kwargs: DBOSWorkflowEnqueueResult(workflow_id="wf-2502"),
    )
    monkeypatch.setattr(admin, "cancel_deployment_workflow", lambda **_kwargs: None)

    data = _documented_error(
        lambda: admin.client_enqueue_publisher_cycle(
            deployment="demo",
            dsn="postgres://unused",
            timeout_seconds=3600.0,
            poll_interval_seconds=0.01,
            dequeue_timeout_seconds=0.05,
        ),
        "publisher_cycle_not_dequeued",
    )
    assert data["cancelled"] is True
    row = next(
        row
        for row in _load_registry()["codes"]
        if row["code"] == "publisher_cycle_not_dequeued"
    )
    assert row["python_exception"] == "PublisherCycleNotDequeued"
