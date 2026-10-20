"""IR-authored consistency checks: the authoring contract + loader.

PACT consistency-checks-as-ir, task.ir.check-contract. Covers: loading a
fixture deployment whose check is a transform-IR program; effect-typing
rejection of write-shaped programs at load (asserted by attempting one);
structural identity (hashes derive from the program — same program, same
hashes; changed predicate, changed hashes; no hand-pinning); deployment
surface registration; and the realism check — cern-twiki's snapshot_hygiene
predicates (missing title, empty body, conflict markers, unresolved internal
links) expressed as IR programs, lowered, and executed against a fixture
corpus in the ephemeral test database with exact expected offenders.
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import psycopg
import pytest

from okg.substrate.consistency import (
    IrCheckLoadError,
    load_ir_consistency_checks,
)

FIXTURE_PACKAGE = "ir_check_fixture_checks"


# --------------------------------------------------------------------------- #
# Fixture deployment: a package with IR programs + a deployment.yaml           #
# --------------------------------------------------------------------------- #

_PROGRAMS_SOURCE = '''
"""Fixture IR check programs (cern-twiki snapshot_hygiene shapes)."""
from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import (
    and_,
    col,
    eq,
    func,
    gt,
    json_get,
    json_member,
    like,
    lit,
    lt,
    ne,
)


def _check_program(program_id, where_expr, check_attrs, project=None):
    project = project or {
        "node_id": col("n.node_id"),
        "title": json_get(col("n.attrs"), "title"),
        "source_path": json_get(col("n.attrs"), "source_path"),
    }
    return TransformProgram(
        program_id=program_id,
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": dict(check_attrs)},
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.FROM, {"surface": "twiki_pages", "alias": "n"}),
                        output="n",
                    ),
                    Binding(
                        Op(OpKind.WHERE, {"expr": where_expr}),
                        output="bad",
                        inputs=("n",),
                    ),
                    Binding(
                        Op(OpKind.SELECT, {"project": project}),
                        output="offending",
                        inputs=("bad",),
                    ),
                    Binding(
                        Op(OpKind.EMIT, {"target": "assertion_input"}),
                        inputs=("offending",),
                    ),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )


def _text_attr(name):
    return func("coalesce", func("btrim", json_get(col("n.attrs"), name)), lit(""))


def missing_title_program():
    return _check_program(
        "fixture.twiki.missing_title",
        eq(_text_attr("title"), lit("")),
        {
            "predicate_key": "twiki_title_present",
            "normalized_predicate": "twiki_missing_title",
            "participant_roles": {"page": "node_id"},
            "severity": "medium",
            "normalized_value_columns": ["node_id"],
        },
    )


def empty_body_program():
    # Stripped body under 40 chars, redirects and navigation pages excluded.
    return _check_program(
        "fixture.twiki.empty_body",
        and_(
            lt(func("length", _text_attr("body")), lit(40)),
            ne(
                func("coalesce", json_get(col("n.attrs"), "twiki_is_redirect"), lit("false")),
                lit("true"),
            ),
            ne(
                func(
                    "coalesce",
                    json_get(col("n.attrs"), "twiki_is_navigation_page"),
                    lit("false"),
                ),
                lit("true"),
            ),
        ),
        {
            "predicate_key": "twiki_meaningful_body_present",
            "normalized_predicate": "twiki_empty_body",
            "participant_roles": {"page": "node_id"},
            "severity": "medium",
            "normalized_value_columns": ["node_id"],
        },
    )


def conflict_marker_program():
    return _check_program(
        "fixture.twiki.conflict_marker",
        like(json_get(col("n.attrs"), "body"), "%<<<<<<<%"),
        {
            "predicate_key": "twiki_no_conflict_markers",
            "normalized_predicate": "twiki_conflict_marker_present",
            "participant_roles": {"page": "node_id"},
            "severity": "high",
            "normalized_value_columns": ["node_id"],
        },
    )


def unresolved_links_program():
    return _check_program(
        "fixture.twiki.unresolved_links",
        gt(
            func(
                "jsonb_array_length",
                func(
                    "coalesce",
                    json_member(col("n.attrs"), "unresolved_internal_links"),
                    func("jsonb_build_array"),
                ),
            ),
            lit(0),
        ),
        {
            "predicate_key": "twiki_internal_link_resolves",
            "normalized_predicate": "twiki_unresolved_internal_link",
            "participant_roles": {"page": "node_id"},
            "severity": "medium",
            "normalized_value_columns": ["node_id"],
        },
    )


def write_effect_program():
    program = missing_title_program()
    program.effect = Effect.DELTA
    program.program_id = "fixture.twiki.write_effect"
    return program


def no_check_attrs_program():
    program = missing_title_program()
    program.attrs = {}
    program.program_id = "fixture.twiki.no_attrs"
    return program


def no_value_columns_program():
    # The emission contract without normalized_value_columns: identity would
    # have no declared assignment, so the loader must refuse it.
    program = missing_title_program()
    check = dict(program.attrs["check"])
    check.pop("normalized_value_columns")
    program.attrs = {"check": check}
    program.program_id = "fixture.twiki.no_value_columns"
    return program
'''

_DEPLOYMENT_YAML = """
name: ir-check-fixture
ir_consistency_checks:
  - name: twiki_missing_title_ir
    check_id: fixture.twiki.missing_title.v1
    predicate_family: twiki_snapshot_hygiene
    confidence_type: deterministic_boolean
    program_ref: {pkg}.missing_title_program
    surfaces:
      twiki_pages:
        binding: {binding}
  - name: twiki_empty_body_ir
    check_id: fixture.twiki.empty_body.v1
    predicate_family: twiki_snapshot_hygiene
    confidence_type: deterministic_boolean
    program_ref: {pkg}.empty_body_program
    surfaces:
      twiki_pages:
        binding: {binding}
  - name: twiki_conflict_marker_ir
    check_id: fixture.twiki.conflict_marker.v1
    predicate_family: twiki_snapshot_hygiene
    confidence_type: deterministic_boolean
    program_ref: {pkg}.conflict_marker_program
    surfaces:
      twiki_pages:
        binding: {binding}
  - name: twiki_unresolved_links_ir
    check_id: fixture.twiki.unresolved_links.v1
    predicate_family: twiki_snapshot_hygiene
    confidence_type: deterministic_boolean
    program_ref: {pkg}.unresolved_links_program
    surfaces:
      twiki_pages:
        binding: {binding}
"""


@pytest.fixture()
def fixture_deployment(tmp_path: Path):
    """A deployment dir + importable fixture-programs module."""
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    (module_dir / f"{FIXTURE_PACKAGE}.py").write_text(
        textwrap.dedent(_PROGRAMS_SOURCE), encoding="utf-8"
    )
    sys.path.insert(0, str(module_dir))
    try:
        yield tmp_path
    finally:
        sys.path.remove(str(module_dir))
        sys.modules.pop(FIXTURE_PACKAGE, None)


def _write_manifest(deployment_dir: Path, *, binding: str = "pg_temp.twiki_pages") -> None:
    (deployment_dir / "deployment.yaml").write_text(
        _DEPLOYMENT_YAML.format(pkg=FIXTURE_PACKAGE, binding=binding),
        encoding="utf-8",
    )


def _entry_yaml(program_ref_suffix: str) -> str:
    return textwrap.dedent(f"""
    name: ir-check-fixture
    ir_consistency_checks:
      - name: bad_check
        check_id: fixture.twiki.bad.v1
        predicate_family: twiki_snapshot_hygiene
        confidence_type: deterministic_boolean
        program_ref: {FIXTURE_PACKAGE}.{program_ref_suffix}
        surfaces:
          twiki_pages:
            binding: pg_temp.twiki_pages
    """)


# --------------------------------------------------------------------------- #
# Loader + contract                                                            #
# --------------------------------------------------------------------------- #

def test_loader_composes_all_four_checks(fixture_deployment: Path) -> None:
    _write_manifest(fixture_deployment)
    specs = load_ir_consistency_checks(fixture_deployment)
    assert [s.name for s in specs] == [
        "twiki_missing_title_ir",
        "twiki_empty_body_ir",
        "twiki_conflict_marker_ir",
        "twiki_unresolved_links_ir",
    ]
    for spec in specs:
        definition = spec.definition("ir-check-fixture")
        assert definition.evaluator_ref == "okg.evaluator.ir.v1"
        # every hash is structural (sha-shaped), none hand-pinned
        for h in (
            definition.candidate_rule_hash,
            definition.evaluator_hash,
            definition.compute_hash,
            definition.output_schema_hash,
            definition.dirty_rule_hash,
        ):
            assert isinstance(h, str) and len(h) >= 32


def test_write_effect_program_rejected_at_load(fixture_deployment: Path) -> None:
    (fixture_deployment / "deployment.yaml").write_text(
        _entry_yaml("write_effect_program"), encoding="utf-8"
    )
    with pytest.raises(IrCheckLoadError, match="READ_ONLY"):
        load_ir_consistency_checks(fixture_deployment)


def test_missing_check_attrs_rejected_at_load(fixture_deployment: Path) -> None:
    (fixture_deployment / "deployment.yaml").write_text(
        _entry_yaml("no_check_attrs_program"), encoding="utf-8"
    )
    with pytest.raises(IrCheckLoadError, match="attrs\\['check'\\]"):
        load_ir_consistency_checks(fixture_deployment)


def test_missing_normalized_value_columns_rejected_at_load(
    fixture_deployment: Path,
) -> None:
    # PACT consistency-finding-identity, req.identity.declared-or-refused: an
    # emission contract that names no identity columns is refused AT LOAD with
    # an authoring error naming the check and the missing field — never
    # defaulted to a projection-derived identity.
    (fixture_deployment / "deployment.yaml").write_text(
        _entry_yaml("no_value_columns_program"), encoding="utf-8"
    )
    with pytest.raises(
        IrCheckLoadError,
        match="'fixture.twiki.bad.v1'.*normalized_value_columns",
    ):
        load_ir_consistency_checks(fixture_deployment)


def test_unknown_surface_rejected_at_load(fixture_deployment: Path) -> None:
    # program reads twiki_pages but the entry declares no such surface ->
    # IR validation fails at load (unregistered surface), loudly.
    manifest = _entry_yaml("missing_title_program").replace(
        "      twiki_pages:\n        binding: pg_temp.twiki_pages\n", ""
    ).replace("    surfaces:\n", "")
    (fixture_deployment / "deployment.yaml").write_text(manifest, encoding="utf-8")
    with pytest.raises(IrCheckLoadError, match="unregistered_surface"):
        load_ir_consistency_checks(fixture_deployment)


def test_duplicate_check_id_rejected(fixture_deployment: Path) -> None:
    manifest = textwrap.dedent(f"""
    name: ir-check-fixture
    ir_consistency_checks:
      - name: bad_check
        check_id: fixture.twiki.bad.v1
        predicate_family: twiki_snapshot_hygiene
        confidence_type: deterministic_boolean
        program_ref: {FIXTURE_PACKAGE}.missing_title_program
        surfaces:
          twiki_pages:
            binding: pg_temp.twiki_pages
      - name: bad_check_two
        check_id: fixture.twiki.bad.v1
        predicate_family: twiki_snapshot_hygiene
        confidence_type: deterministic_boolean
        program_ref: {FIXTURE_PACKAGE}.empty_body_program
        surfaces:
          twiki_pages:
            binding: pg_temp.twiki_pages
    """)
    (fixture_deployment / "deployment.yaml").write_text(manifest, encoding="utf-8")
    with pytest.raises(IrCheckLoadError, match="duplicate check_id"):
        load_ir_consistency_checks(fixture_deployment)


def test_substrate_surface_name_cannot_be_rebound(fixture_deployment: Path) -> None:
    manifest = _entry_yaml("missing_title_program").replace(
        "      twiki_pages:", "      nodes_live:"
    )
    (fixture_deployment / "deployment.yaml").write_text(manifest, encoding="utf-8")
    with pytest.raises(IrCheckLoadError, match="nodes_live"):
        load_ir_consistency_checks(fixture_deployment)


def test_loaded_program_is_a_snapshot_immune_to_factory_mutation(
    fixture_deployment: Path,
) -> None:
    # A factory may hand out a shared module-level singleton; post-load
    # mutation of it must not change the loaded spec's identity.
    _write_manifest(fixture_deployment)
    specs = load_ir_consistency_checks(fixture_deployment)
    import importlib

    module = importlib.import_module(FIXTURE_PACKAGE)
    hash_before = specs[0].definition("d").candidate_rule_hash
    # mutate the loaded spec's source-of-truth sibling: rebuild + mutate
    specs[0].program.attrs  # loaded snapshot exists
    program = module.missing_title_program()
    program.attrs = {}  # mutate a freshly built one; snapshot must be isolated
    assert specs[0].definition("d").candidate_rule_hash == hash_before
    assert specs[0].program.attrs.get("check")  # snapshot retains its contract


def test_read_only_program_calling_effectful_kernel_is_rejected() -> None:
    # The IR type-system rule (not loader starvation): a declared-READ_ONLY
    # program CALLing a registered delta-effect kernel must fail validation.
    from okg.substrate.transform import (
        Binding,
        Block,
        Effect,
        FunctionRegistry,
        Op,
        OpKind,
        Registries,
        Terminator,
        TerminatorKind,
        TransformProgram,
    )
    from okg.substrate.transform import Determinism
    from okg.substrate.transform.registry import FunctionSpec

    functions = FunctionRegistry()
    functions.register(FunctionSpec(
        function_id="evil.mutator",
        effect=Effect.DELTA,
        determinism=Determinism.DETERMINISTIC,
    ))
    program = TransformProgram(
        program_id="fixture.readonly_calls_delta_kernel",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(
                        Op(OpKind.CALL, {"function_id": "evil.mutator"}),
                        output="offending",
                    ),
                ),
                terminator=Terminator(
                    TerminatorKind.RETURN, {"values": ["offending"]}
                ),
            ),
        ),
    )
    errors = program.validate(Registries(functions=functions))
    assert any(
        e.code == "read_only_program_calls_effectful_kernel" for e in errors
    ), [str(e) for e in errors]


def test_hashes_are_structural_not_pinned(fixture_deployment: Path) -> None:
    _write_manifest(fixture_deployment)
    a = load_ir_consistency_checks(fixture_deployment)
    b = load_ir_consistency_checks(fixture_deployment)
    # same program -> identical hashes across loads
    assert (
        a[0].definition("d").candidate_rule_hash
        == b[0].definition("d").candidate_rule_hash
    )
    # different programs -> different hashes (predicate participates)
    hashes = {s.definition("d").candidate_rule_hash for s in a}
    assert len(hashes) == len(a)
    # changing only the surface binding changes identity too
    _write_manifest(fixture_deployment, binding="pg_temp.other_pages")
    c = load_ir_consistency_checks(fixture_deployment)
    assert (
        c[0].definition("d").candidate_rule_hash
        != a[0].definition("d").candidate_rule_hash
    )


# --------------------------------------------------------------------------- #
# Realism: the twiki predicates run against a fixture corpus                   #
# --------------------------------------------------------------------------- #

_CORPUS = [
    # (node_id, attrs)
    ("twiki:CMS:Good", {
        "title": "Good", "body": "x" * 100, "source_path": "CMS/Good.txt",
    }),
    ("twiki:CMS:NoTitle", {
        "title": "  ", "body": "y" * 100, "source_path": "CMS/NoTitle.txt",
    }),
    ("twiki:CMS:Stub", {
        "title": "Stub", "body": "short", "source_path": "CMS/Stub.txt",
    }),
    ("twiki:CMS:Redirect", {
        "title": "Redirect", "body": "", "twiki_is_redirect": "true",
        "source_path": "CMS/Redirect.txt",
    }),
    ("twiki:CMS:Conflict", {
        "title": "Conflict", "source_path": "CMS/Conflict.txt",
        "body": "a\n<<<<<<< HEAD\nold\n=======\nnew\n>>>>>>> b\n" + "z" * 40,
    }),
    ("twiki:CMS:BadLinks", {
        "title": "BadLinks", "body": "w" * 100, "source_path": "CMS/BadLinks.txt",
        "unresolved_internal_links": ["https://twiki.cern.ch/CMS/Missing"],
    }),
]

_EXPECTED_OFFENDERS = {
    "twiki_missing_title_ir": {"twiki:CMS:NoTitle"},
    "twiki_empty_body_ir": {"twiki:CMS:Stub"},
    "twiki_conflict_marker_ir": {"twiki:CMS:Conflict"},
    "twiki_unresolved_links_ir": {"twiki:CMS:BadLinks"},
}


@pytest.mark.postgres
def test_twiki_predicates_lower_and_find_exact_offenders(
    fixture_deployment: Path,
    clean_runtime: str,
) -> None:
    import json

    _write_manifest(fixture_deployment)
    specs = load_ir_consistency_checks(fixture_deployment)

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE twiki_pages (node_id text PRIMARY KEY, attrs jsonb)"
            )
            for node_id, attrs in _CORPUS:
                cur.execute(
                    "INSERT INTO twiki_pages VALUES (%s, %s::jsonb)",
                    (node_id, json.dumps(attrs)),
                )
        conn.commit()

        for spec in specs:
            rows = spec.run_rows(conn)
            offenders = {row["node_id"] for row in rows}
            assert offenders == _EXPECTED_OFFENDERS[spec.name], spec.name
            # projected observation inputs ride along
            for row in rows:
                assert "source_path" in row
