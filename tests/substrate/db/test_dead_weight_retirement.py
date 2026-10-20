"""schema-dead-weight-retirement: the deletions, and the gates that hold them.

Covers:

  req.schema-dead-weight-retirement.delete-zero-consumer
    The audited zero-consumer relations are gone from the shipped DDL and
    from a freshly migrated catalog, and the retirement migration is wired
    into the ordered runner so an existing database converges to the same
    shape.

  req.schema-dead-weight-retirement.not-valid-resolved
    No CHECK constraint ships NOT VALID, and a freshly migrated catalog
    carries zero `convalidated = false` rows.

The gate tests are the load-bearing ones: they must be RED on a mutation
fixture, not merely green on the real tree. A gate proven only against the
tree it was written for is a gate proven against nothing --- so each one is
also run against a doctored copy of the input that reintroduces exactly the
defect it exists to catch.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "schema.sql"
MIGRATIONS_DIR = REPO_ROOT / "src" / "okg" / "substrate" / "db" / "migrations"
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_schema_relation_hygiene.py"

RETIREMENT_MIGRATION = "0025_dead_relation_and_constraint_convergence"

# The relations this change retires, as (catalog kind, identifier). Written
# out here rather than imported from the checker so a bad edit to the
# checker's own pin list cannot make these assertions vacuous.
#
# Spelled with explicit `+` rather than adjacent literals: implicit
# concatenation inside a tuple reads as a missing comma, to humans and to
# CodeQL (py/implicit-string-concatenation-in-list).
_COMPUTE_PAGERANK_SIG = (
    "okg.compute_pagerank(text, double precision, integer, double precision)"
)
_REFRESH_PAGERANK_SIG = (
    "okg.refresh_pagerank(text, text, double precision, integer, "
    + "double precision)"
)

RETIRED = (
    ("table", "okg.pagerank_scores"),
    ("table", "okg.verifier_runs"),
    ("function", _COMPUTE_PAGERANK_SIG),
    ("function", _REFRESH_PAGERANK_SIG),
)

# Six of the seven constraints that shipped NOT VALID: the ones this change
# converges to born-VALID. The seventh,
# admission_envelopes_target_generation_ref_nonempty_chk, is deliberately NOT
# here --- it stays NOT VALID under a registered exemption (see
# EXEMPT_CONSTRAINT below and not_valid_exemptions.yaml).
FORMERLY_NOT_VALID = (
    ("okg.federation_mailbox_messages",
     "federation_mailbox_payload_shape_chk"),
    ("okg.federation_mailbox_messages",
     "federation_mailbox_lifecycle_evidence_chk"),
    ("okg.federation_mailbox_messages",
     "federation_mailbox_lease_shape_chk"),
    ("okg.poros_inbox_messages",
     "poros_inbox_messages_status_chk"),
    ("okg.poros_detached_object_manifests",
     "poros_detached_manifest_classification_chk"),
    ("okg.poros_detached_object_manifests",
     "poros_detached_manifest_shape_chk"),
)

EXEMPT_RELATION = "okg.admission_envelopes"
EXEMPT_CONSTRAINT = "admission_envelopes_target_generation_ref_nonempty_chk"


def _load_checker():
    spec = importlib.util.spec_from_file_location(
        "check_schema_relation_hygiene", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_schema_relation_hygiene"] = module
    spec.loader.exec_module(module)
    return module


CHECKER = _load_checker()


def _strip_comments(text: str) -> str:
    return CHECKER._strip_line_comments(text)


# --------------------------------------------------------------------------
# 1. The deletions, statically (hermetic --- no DB).
# --------------------------------------------------------------------------


def test_retired_relations_are_absent_from_the_shipped_ddl() -> None:
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    for token in (
        "pagerank_scores",
        "compute_pagerank",
        "refresh_pagerank",
        "verifier_runs",
    ):
        assert token not in text, (
            f"{token} is still in schema.sql; a fresh install would create "
            "a relation this change retires"
        )


def test_schema_ships_no_unregistered_not_valid_constraint() -> None:
    """Born-VALID (§2.10 rev 4): a fresh install validates on creation.

    Exactly one `NOT VALID` survives, and it is registered. Asserting the
    COUNT as well as the registration keeps the exemption from becoming a
    door: a second unregistered site would fail the checker, and a second
    REGISTERED site would fail this count and force the reviewer to look.
    """
    text = _strip_comments(SCHEMA_SQL.read_text(encoding="utf-8"))
    assert text.count("NOT VALID") == 1, (
        "schema.sql NOT VALID site count changed; every one must be paired "
        "with a validation migration or registered in "
        "not_valid_exemptions.yaml"
    )
    # Keyed (relation, constraint): a constraint name is only unique per
    # relation, so a bare-name key would let one exemption cover a same-named
    # constraint on another table.
    exemptions = CHECKER.load_exemptions()
    key = f"{EXEMPT_RELATION}.{EXEMPT_CONSTRAINT}"
    assert set(exemptions) == {key}
    assert exemptions[key]["owner"] == "schema-dead-weight-retirement"
    assert exemptions[key]["reason"].strip()


def test_retirement_migration_validates_every_formerly_not_valid_constraint() -> None:
    """The upgrade path must cover all seven, not a subset.

    The snapshot's DROP+ADD sites do not re-run under migrate()'s checksum
    skip, and the two probe-guarded sites skip an already-present constraint,
    so an existing database converges only through this migration.
    """
    sql = (MIGRATIONS_DIR / f"{RETIREMENT_MIGRATION}.sql").read_text(
        encoding="utf-8"
    )
    for _relation, constraint in FORMERLY_NOT_VALID:
        assert constraint in sql, (
            f"{constraint} has no VALIDATE path in {RETIREMENT_MIGRATION}; "
            "existing databases would keep it unenforced forever"
        )


def test_retirement_migration_drops_every_retired_relation() -> None:
    sql = (MIGRATIONS_DIR / f"{RETIREMENT_MIGRATION}.sql").read_text(
        encoding="utf-8"
    )
    for token in (
        "okg.pagerank_scores",
        "okg.verifier_runs",
        "okg.compute_pagerank",
        "okg.refresh_pagerank",
    ):
        assert token in sql, f"{RETIREMENT_MIGRATION} does not drop {token}"
    # CASCADE would silently remove an unaudited dependent instead of
    # refusing --- the whole point of re-verifying consumers. Checked against
    # executable SQL only: the header prose explains why CASCADE is avoided,
    # and a check that read comments would fail on its own rationale.
    executable = CHECKER._strip_line_comments(sql)
    assert "CASCADE" not in executable.upper(), (
        "the drops must be RESTRICT (the default) so an unaudited dependency "
        "blocks the migration rather than being silently removed"
    )


def test_retirement_migration_is_registered_in_the_ordered_runner() -> None:
    """A migration file nobody runs converges nothing."""
    from okg.substrate.db.management import (
        current_known_migrations,
        load_migrations_manifest,
    )

    known = {m.migration_id: m for m in current_known_migrations()}
    assert RETIREMENT_MIGRATION in known
    # Destructive migrations are auto-SKIPPED; this one must auto-run or the
    # upgrade path never converges (see the manifest entry's argument).
    assert known[RETIREMENT_MIGRATION].destructive is False

    entry = load_migrations_manifest()[RETIREMENT_MIGRATION]
    assert entry["applicability"] == "always"
    assert entry["schema_diff"]["mode"] == "required"


# --------------------------------------------------------------------------
# 2. The gates are non-vacuous (hermetic --- mutation fixtures).
# --------------------------------------------------------------------------


def test_static_arms_pass_on_the_real_tree() -> None:
    """Green on the tree --- but only the static arms are certified here.

    Without a DSN the catalog arms examined nothing, so the envelope must
    still report NOT ok. A gate that passed on the half it could read would
    be absence dressed as agreement.
    """
    result = CHECKER.run_relation_hygiene_checks(dsn=None)
    assert result["violations"] == []
    assert result["catalog_status"] == "unavailable"
    assert result["ok"] is False, (
        "an uncertifiable catalog arm must fail the gate, not pass it"
    )

    # --static-only is the EXPLICIT half-certification the no-Postgres CI
    # runner asks for. It may pass, but it must still say the catalog arm
    # certified nothing -- otherwise it is the same silent pass wearing a flag.
    explicit = CHECKER.run_relation_hygiene_checks(dsn=None, static_only=True)
    assert explicit["ok"] is True
    assert explicit["catalog_status"] == "not_requested"
    assert "certif" in explicit["checks"]["catalog"]["detail"]


def test_static_only_still_fails_on_a_real_static_violation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--static-only must weaken the SCOPE, not the verdict."""
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nCREATE TABLE IF NOT EXISTS okg.verifier_runs (run_id TEXT);\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None, static_only=True)

    assert result["ok"] is False
    assert [
        v["relation"]
        for v in result["violations"]
        if v["check"] == "no_dead_relations"
    ] == ["verifier_runs"]


def test_born_valid_gate_is_red_on_an_unpaired_not_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The mutation fixture: a constraint added NOT VALID with no VALIDATE."""
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nALTER TABLE okg.admission_envelopes\n"
        "  ADD CONSTRAINT mutation_fixture_unpaired_chk\n"
        "  CHECK (deployment_name IS NOT NULL) NOT VALID;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    born_valid = [
        v for v in result["violations"] if v["check"] == "born_valid"
    ]
    assert born_valid, "the gate missed an unpaired NOT VALID constraint"
    assert born_valid[0]["constraint"] == "mutation_fixture_unpaired_chk"
    assert result["ok"] is False


def test_born_valid_gate_accepts_a_not_valid_paired_with_a_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The gate bans UNPAIRED NOT VALID, not the two-step pattern itself.

    Adding a constraint NOT VALID and validating it in the same migration is
    the legitimate way to add a CHECK to a large populated table without
    holding ACCESS EXCLUSIVE for the scan. A gate that banned it outright
    would push authors toward the locking form.
    """
    doctored_schema = tmp_path / "schema.sql"
    doctored_schema.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nALTER TABLE okg.admission_envelopes\n"
        "  ADD CONSTRAINT mutation_fixture_paired_chk\n"
        "  CHECK (deployment_name IS NOT NULL) NOT VALID;\n",
        encoding="utf-8",
    )
    doctored_migrations = tmp_path / "migrations"
    doctored_migrations.mkdir()
    (doctored_migrations / "0099_pairing_probe.sql").write_text(
        "ALTER TABLE okg.admission_envelopes\n"
        "  VALIDATE CONSTRAINT mutation_fixture_paired_chk;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored_schema)
    monkeypatch.setattr(CHECKER, "MIGRATIONS_DIR", doctored_migrations)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    assert [v for v in result["violations"] if v["check"] == "born_valid"] == []


def test_no_dead_relations_gate_is_red_when_a_retired_relation_returns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The mutation fixture: a revert or bad merge brings the table back."""
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nCREATE TABLE IF NOT EXISTS okg.pagerank_scores (\n"
        "  scope TEXT NOT NULL,\n"
        "  node_id TEXT NOT NULL,\n"
        "  PRIMARY KEY (scope, node_id)\n"
        ");\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    dead = [
        v for v in result["violations"] if v["check"] == "no_dead_relations"
    ]
    assert [v["relation"] for v in dead] == ["pagerank_scores"]
    assert result["ok"] is False


@pytest.mark.parametrize(
    "comment_form",
    [
        "-- a whole-line comment naming pagerank_scores\n",
        "SELECT 1; -- a TRAILING comment naming pagerank_scores\n",
        "/* a block comment\n   naming pagerank_scores */\n",
    ],
    ids=["whole_line", "trailing", "block"],
)
def test_gate_ignores_relation_names_inside_comments(
    comment_form: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Comment prose must not trip the pinned-absence arm.

    Exercised through the REAL check function on a doctored schema, not by
    asserting on the stripping helper: a helper-only assertion would keep
    passing even if the gate stopped calling it. All three comment forms
    matter -- the retirement migration and schema.sql both name these
    relations in prose, and an earlier version of the stripper only handled
    whole-line `--`.
    """
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8") + "\n" + comment_form,
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    assert [
        v for v in result["violations"] if v["check"] == "no_dead_relations"
    ] == [], f"comment prose tripped the gate: {comment_form!r}"


def test_unnamed_not_valid_cannot_borrow_a_registered_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A constraint added without a name must not inherit the exemption.

    `ADD CHECK (...) NOT VALID` lets Postgres generate the name. If the gate
    scanned backwards past the statement boundary it would attribute this to
    the previous statement's ADD CONSTRAINT -- which is the registered
    exemption -- and silently wave it through.
    """
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nALTER TABLE okg.admission_envelopes\n"
        "  ADD CHECK (producer_id IS NOT NULL) NOT VALID;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    born_valid = [
        v for v in result["violations"] if v["check"] == "born_valid"
    ]
    assert born_valid, (
        "an unnamed NOT VALID constraint borrowed a registered exemption's "
        "name and was silently accepted"
    )
    assert born_valid[0]["constraint"] is None


def test_validation_pairing_actually_recognizes_this_repos_migrations() -> None:
    """The "pair it with a validation migration" escape hatch must be real.

    `_validated_constraint_names()` originally matched only the LITERAL
    `VALIDATE CONSTRAINT foo` form, while every migration in this tree
    validates dynamically inside a DO block via
    `format('... VALIDATE CONSTRAINT %I', ...)`. The set came back empty, so
    the advice the gate prints was advice no migration here could follow --
    a check that silently matches nothing. Assert it is non-empty AND that it
    finds the six constraints 0025 really validates.
    """
    names = CHECKER._validated_constraint_names()
    assert names, (
        "no migration was recognized as validating any constraint; the "
        "pairing arm matches nothing and the escape hatch is unreachable"
    )
    for _relation, constraint in FORMERLY_NOT_VALID:
        assert constraint in names, (
            f"{constraint} is validated by {RETIREMENT_MIGRATION} but the "
            "pairing scan did not see it"
        )


def test_an_unregistered_not_valid_is_still_red_with_the_registry_loaded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exemption registry must be a keyhole, not an open door.

    A registry that suppressed the whole check once ANY row existed would be
    worse than no check. The named constraint is exempt; a different one is
    not.
    """
    doctored = tmp_path / "schema.sql"
    doctored.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8")
        + "\nALTER TABLE okg.admission_envelopes\n"
        "  ADD CONSTRAINT some_other_unregistered_chk\n"
        "  CHECK (deployment_name IS NOT NULL) NOT VALID;\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "SCHEMA_SQL", doctored)

    result = CHECKER.run_relation_hygiene_checks(dsn=None)

    born_valid = [
        v for v in result["violations"] if v["check"] == "born_valid"
    ]
    assert [v["constraint"] for v in born_valid] == [
        "some_other_unregistered_chk"
    ], "the registered exemption must not suppress an unregistered sibling"


def test_exemption_registry_refuses_an_entry_without_an_owner_or_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Silence is a gate failure, not a free pass."""
    doctored = tmp_path / "not_valid_exemptions.yaml"
    doctored.write_text(
        "version: 1\n"
        "exemptions:\n"
        "  - relation: okg.admission_envelopes\n"
        "    constraint: sneaky_chk\n"
        "    owner: ''\n"
        "    reason: ''\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(CHECKER, "EXEMPTIONS_YAML", doctored)

    with pytest.raises(ValueError, match="owner"):
        CHECKER.load_exemptions()


def test_lease_audit_is_deliberately_not_on_the_pin_list() -> None:
    """It gained a consumer since the audit; retiring it is views.sql work.

    Pinned as a test so a later sweep does not quietly add it back to the
    absence list and red every deployment that still publishes the surface.
    """
    pinned = {identifier for _kind, identifier in CHECKER.RETIRED_RELATIONS}
    assert "okg.lease_audit" not in pinned


# --------------------------------------------------------------------------
# 3. The catalog, against a real migrated database.
# --------------------------------------------------------------------------


@pytest.mark.postgres
def test_fresh_install_has_none_of_the_retired_relations(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        for kind, identifier in RETIRED:
            resolver = "to_regclass" if kind == "table" else "to_regprocedure"
            cur.execute(f"SELECT {resolver}(%s)", (identifier,))
            assert cur.fetchone()[0] is None, (
                f"{identifier} still exists on a migrated database"
            )


@pytest.mark.postgres
def test_migrated_catalog_carries_no_unregistered_unvalidated_constraint(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname || '.' || r.relname, c.conname
              FROM pg_constraint c
              JOIN pg_class r ON r.oid = c.conrelid
              JOIN pg_namespace n ON n.oid = r.relnamespace
             WHERE NOT c.convalidated
               AND n.nspname IN ('okg', 'okg_derived')
             ORDER BY 1, 2
            """
        )
        unvalidated = cur.fetchall()
    # The one registered exemption is expected here, and ONLY it. Asserting
    # the exact list (rather than filtering it out) means a future exemption
    # cannot slip in unnoticed.
    assert unvalidated == [(EXEMPT_RELATION, EXEMPT_CONSTRAINT)], (
        f"unexpected unvalidated constraints: {unvalidated}"
    )


@pytest.mark.postgres
def test_the_seven_constraints_exist_and_are_validated(
    okg_migrated: str,
) -> None:
    """Non-vacuity for the test above.

    "Zero invalid constraints" is also true of a database that dropped all
    seven. Assert each one is PRESENT and validated, so the gate cannot be
    satisfied by deletion.
    """
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        for relation, constraint in FORMERLY_NOT_VALID:
            cur.execute(
                """
                SELECT c.convalidated
                  FROM pg_constraint c
                 WHERE c.conrelid = %s::regclass
                   AND c.conname = %s
                """,
                (relation, constraint),
            )
            row = cur.fetchone()
            assert row is not None, (
                f"{relation}.{constraint} is missing; the born-VALID gate "
                "must not be satisfiable by dropping the constraint"
            )
            assert row[0] is True, f"{relation}.{constraint} is NOT VALID"


@pytest.mark.postgres
def test_the_exempt_constraint_is_present_and_still_enforces_new_rows(
    okg_migrated: str,
) -> None:
    """An exemption is not a deletion.

    The registered constraint must still EXIST and still refuse new blank
    refs --- that is the half of NOT VALID that earns the exemption. If it
    were gone, or not enforcing, the registry entry would be covering for a
    hole rather than describing a deliberate design.
    """
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.convalidated
              FROM pg_constraint c
             WHERE c.conrelid = %s::regclass
               AND c.conname = %s
            """,
            (EXEMPT_RELATION, EXEMPT_CONSTRAINT),
        )
        row = cur.fetchone()
    assert row is not None, "the exempt constraint must still exist"
    assert row[0] is False, "the exemption describes a NOT VALID constraint"

    with psycopg.connect(okg_migrated, autocommit=True) as conn:
        with conn.cursor() as cur, pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                """
                INSERT INTO okg.admission_envelopes (
                  deployment_name, producer_id, producer_kind, intent,
                  admission_mode, status, target_generation_ref,
                  idempotency_key
                )
                VALUES (
                  'test', 'producer:exempt-probe', 'source', 'publish',
                  'direct', 'published', '  ',
                  'dead-weight-exempt-probe'
                )
                """
            )


@pytest.mark.postgres
def test_relation_hygiene_gate_is_green_against_a_migrated_database(
    okg_migrated: str,
) -> None:
    result = CHECKER.run_relation_hygiene_checks(dsn=okg_migrated)
    assert result["catalog_status"] == "checked"
    assert result["violations"] == [], result["violations"]
    assert result["ok"] is True


@pytest.mark.postgres
def test_catalog_arm_is_red_when_a_retired_relation_is_present(
    okg_migrated: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-vacuity for the catalog arm: pin a relation that DOES exist.

    Rather than recreating a retired table on the shared session database
    (which would leak into every later test), point the pin list at a
    relation the schema legitimately ships. The arm must report it present.
    """
    monkeypatch.setattr(
        CHECKER,
        "RETIRED_RELATIONS",
        (("table", "okg.admission_envelopes"),),
    )
    result = CHECKER.run_relation_hygiene_checks(dsn=okg_migrated)

    dead = [
        v for v in result["violations"] if v["check"] == "no_dead_relations"
    ]
    assert [v["relation"] for v in dead] == ["okg.admission_envelopes"]
    assert result["ok"] is False


@pytest.mark.postgres
def test_catalog_arm_function_probe_can_actually_find_a_function(
    okg_migrated: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Non-vacuity for the `to_regprocedure` half specifically.

    Both retired functions are absent, so the function arm returns "absent"
    for every input on this database -- which is exactly what a BROKEN probe
    (wrong resolver, malformed signature, silently-NULL result) would also
    return. Point it at a function that really exists and require it to
    report present, so the arm is proved able to say yes at all.
    """
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT n.nspname || '.' || p.proname
                   || '(' || pg_get_function_identity_arguments(p.oid) || ')'
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
             ORDER BY 1
             LIMIT 1
            """
        )
        row = cur.fetchone()
    assert row is not None, "expected at least one okg function to probe"
    live_function = row[0]

    monkeypatch.setattr(
        CHECKER, "RETIRED_RELATIONS", (("function", live_function),)
    )
    result = CHECKER.run_relation_hygiene_checks(dsn=okg_migrated)

    dead = [
        v for v in result["violations"] if v["check"] == "no_dead_relations"
    ]
    assert [v["relation"] for v in dead] == [live_function], (
        "the to_regprocedure arm could not detect a function that exists; "
        "its 'absent' verdicts prove nothing"
    )
