from __future__ import annotations

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.lineage import (
    ENRICHER_LINEAGE_SCHEMA,
    WITHHELD_INPUT_SOURCE_HELD_STATUS_PREFIX,
    EnricherLineageInvalidation,
    LINEAGE_STATUS_AVAILABLE,
    LINEAGE_STATUS_UNAVAILABLE,
    MISSING_DEPENDENCY_DECLARATION,
    build_enricher_run_lineage_envelope,
    declare_enricher_dependencies,
    invalidations_from_chronos_dirty_scope,
    invalidations_from_source_delta_batches,
    plan_enricher_lineage,
    held_input_sources,
    resolve_declared_source_ids,
    source_alias_index,
    unknown_declared_source_status,
    withheld_input_source_status,
    withheld_reason_from_status,
)
from okg.substrate.publisher import Publisher


class _Cursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchall(self):
        # Issue #1528: `_run_enrichers` now asks the admission ledger whether a
        # declared input source is quarantined. This fake has no ledger, so it
        # answers "none held" and the tests below keep exercising the lineage
        # path they were written for.
        return []


class _Conn:
    def cursor(self):
        return _Cursor()


def test_enricher_registry_lineage_matches_node_subtype_selectors() -> None:
    class _PactEnricher:
        name = "pact"
        reads_subtypes = ("pact_change",)
        reads_edge_types = ()

    class _SourceFileEnricher:
        name = "source_file"
        reads_subtypes = ("source_file",)
        reads_edge_types = ()

    invalidations = invalidations_from_chronos_dirty_scope((
        {"pact_change": ["pact-change:1"]},
        {},
        True,
        None,
        {},
        None,
    ))
    pact_plan = plan_enricher_lineage(
        declare_enricher_dependencies(_PactEnricher()),
        invalidations,
    )
    source_file_plan = plan_enricher_lineage(
        declare_enricher_dependencies(_SourceFileEnricher()),
        invalidations,
    )

    assert pact_plan.lineage_status == LINEAGE_STATUS_AVAILABLE
    assert pact_plan.dirty is True
    assert pact_plan.dependency_keys == (
        {
            "dependency_kind": "graph_node",
            "dependency_ref": "pact-change:1",
        },
    )
    assert source_file_plan.lineage_status == LINEAGE_STATUS_AVAILABLE
    assert source_file_plan.dirty is False
    assert source_file_plan.dependency_keys == ()


def test_enricher_registry_lineage_matches_edge_type_selectors() -> None:
    class _MentionsEnricher:
        name = "mentions"
        reads_subtypes = ()
        reads_edge_types = ("mentions",)

    class _ContainsEnricher:
        name = "contains"
        reads_subtypes = ()
        reads_edge_types = ("contains",)

    invalidations = invalidations_from_chronos_dirty_scope((
        {},
        {"mentions": [42]},
        True,
        None,
        {},
        None,
    ))

    mentions_plan = plan_enricher_lineage(
        declare_enricher_dependencies(_MentionsEnricher()),
        invalidations,
    )
    contains_plan = plan_enricher_lineage(
        declare_enricher_dependencies(_ContainsEnricher()),
        invalidations,
    )

    assert mentions_plan.dirty is True
    assert mentions_plan.dependency_keys == (
        {"dependency_kind": "graph_edge", "dependency_ref": "42"},
    )
    assert contains_plan.dirty is False


def test_enricher_registry_lineage_matches_source_and_component_selectors() -> None:
    class _GitEnricher:
        name = "git"
        reads_subtypes = ()
        reads_edge_types = ()
        lineage_dependencies = {"source_ids": ["git-files"]}

    class _TicketEnricher:
        name = "tickets"
        reads_subtypes = ()
        reads_edge_types = ()
        lineage_dependencies = {"source_ids": ["tickets"]}

    git_decl = declare_enricher_dependencies(_GitEnricher())
    ticket_decl = declare_enricher_dependencies(_TicketEnricher())
    git_source_plan = plan_enricher_lineage(
        git_decl,
        (
            EnricherLineageInvalidation(
                "source_record",
                'git-files:{"path":"docs/a.md"}',
                source_id="git-files",
            ),
        ),
    )
    ticket_source_plan = plan_enricher_lineage(
        ticket_decl,
        (
            EnricherLineageInvalidation(
                "source_record",
                'git-files:{"path":"docs/a.md"}',
                source_id="git-files",
            ),
        ),
    )
    component_plan = plan_enricher_lineage(
        git_decl,
        (
            EnricherLineageInvalidation(
                "component_hash",
                git_decl.component_hash,
            ),
        ),
    )

    assert git_source_plan.dirty is True
    assert git_source_plan.dependency_keys == (
        {
            "dependency_kind": "source_record",
            "dependency_ref": 'git-files:{"path":"docs/a.md"}',
        },
    )
    assert ticket_source_plan.dirty is False
    assert component_plan.dirty is True
    assert component_plan.dependency_keys == (
        {
            "dependency_kind": "component_hash",
            "dependency_ref": git_decl.component_hash,
        },
    )


def test_enricher_registry_lineage_missing_declaration_forces_loud_full() -> None:
    class _LegacyEnricher:
        name = "legacy"
        reads_subtypes = ()
        reads_edge_types = ()

    declaration = declare_enricher_dependencies(_LegacyEnricher())
    plan = plan_enricher_lineage(declaration, ())
    envelope = build_enricher_run_lineage_envelope(
        _LegacyEnricher(),
        generation_id=123,
        catalog_version_id=7,
        plan=plan,
    ).to_dict()

    assert plan.lineage_status == LINEAGE_STATUS_UNAVAILABLE
    assert plan.fallback_reason == MISSING_DEPENDENCY_DECLARATION
    assert plan.force_full is True
    assert envelope["schema"] == ENRICHER_LINEAGE_SCHEMA
    assert envelope["output_kind"] == "edge_fact_stream"
    assert envelope["materialization"] == {
        "kind": "edge_facts",
        "projection": "project_new_edges",
    }
    assert envelope["lineage_status"] == LINEAGE_STATUS_UNAVAILABLE
    assert envelope["fallback_reason"] == MISSING_DEPENDENCY_DECLARATION
    assert envelope["force_full"] is True


def test_enricher_registry_lineage_rejects_selectorless_explicit_kinds() -> None:
    class _BroadGraphEnricher:
        name = "broad"
        reads_subtypes = ()
        reads_edge_types = ()
        lineage_dependencies = {"kinds": ["graph_node"]}

    declaration = declare_enricher_dependencies(_BroadGraphEnricher())
    plan = plan_enricher_lineage(
        declaration,
        (
            EnricherLineageInvalidation(
                "graph_node",
                "source-file:1",
                node_subtype="source_file",
            ),
        ),
    )

    assert plan.lineage_status == LINEAGE_STATUS_UNAVAILABLE
    assert plan.fallback_reason == MISSING_DEPENDENCY_DECLARATION
    assert plan.force_full is True
    assert plan.dependency_keys == ()


def test_enricher_registry_lineage_publisher_forces_full_on_missing_declaration(
    monkeypatch,
) -> None:
    class _LegacyCleanEnricher:
        name = "legacy_clean"
        publisher_skip_when_clean = True
        reads_subtypes = ()
        reads_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # governance/lineage, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "git_doc", "edge_type": "references", "dst_subtype": "git_doc"}],
            "nondeterministic": False,
        }

        def can_skip_clean(self, conn, ctx) -> bool:
            return True

        def enrich(self, conn, *, generation_id: int, incremental=None):
            calls.append({
                "generation_id": generation_id,
                "is_first_run": incremental.is_first_run,
                "change_basis": incremental.change_basis,
            })
            return EnrichResult(self.name, n_edges_emitted=0)

    calls: list[dict[str, object]] = []
    publisher = Publisher(
        "postgres://unused",
        "test",
        enrichers=[_LegacyCleanEnricher()],
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_progress_rows",
        lambda conn, names: {"legacy_clean": (5, 6, "success", None, 7)},
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_profile_fingerprints",
        lambda conn, producer_ids: {"_enricher:legacy_clean": "fp:legacy"},
    )
    monkeypatch.setattr(
        publisher,
        "_current_fact_watermarks",
        lambda conn: (5, 6),
    )
    monkeypatch.setattr(
        publisher,
        "_current_catalog_version_id",
        lambda conn: 7,
    )
    monkeypatch.setattr(
        publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        lambda conn, *, branch_name, previous=None, timing_sink=None, timing_prefix=None: (
            {},
            {},
            True,
            None,
            {},
            None,
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_expected_enricher_profile_fingerprint",
        lambda conn, *, enricher, producer_id, catalog_version_id: "fp:legacy",
    )
    monkeypatch.setattr(
        publisher,
        "_materialize_enricher_profile",
        lambda *args, **kwargs: "fp:legacy",
    )
    monkeypatch.setattr(
        publisher,
        "_admit_enricher_outputs",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        publisher,
        "_advance_enricher_progress",
        lambda *args, **kwargs: None,
    )

    runs = publisher._run_enrichers(
        _Conn(),
        generation_id=123,
        catalog_version_id=7,
    )

    run = runs[1]
    assert calls == [
        {
            "generation_id": 123,
            "is_first_run": True,
            "change_basis": MISSING_DEPENDENCY_DECLARATION,
        }
    ]
    assert run["status"] == "ok"
    assert run["lineage_status"] == LINEAGE_STATUS_UNAVAILABLE
    assert run["fallback_reason"] == MISSING_DEPENDENCY_DECLARATION
    assert run["force_full"] is True
    assert run["lineage_envelope"]["fallback_reason"] == (
        MISSING_DEPENDENCY_DECLARATION
    )


def test_enricher_registry_lineage_source_invalidation_controls_publisher_skip(
    monkeypatch,
) -> None:
    class _SourceEnricher:
        publisher_skip_when_clean = True
        reads_subtypes = ("git_doc",)
        reads_edge_types = ()
        requires_narrowings = ()
        # Explicit minimal envelope: the boot guard demands a declared
        # write surface; this fixture's behavior under test is
        # governance/lineage, not admission.
        write_envelope = {
            "edges": [{"src_subtype": "git_doc", "edge_type": "references", "dst_subtype": "git_doc"}],
            "nondeterministic": False,
        }

        def __init__(self, name: str, source_id: str) -> None:
            self.name = name
            self.lineage_dependencies = {"source_ids": [source_id]}

        def can_skip_clean(self, conn, ctx) -> bool:
            return True

        def enrich(self, conn, *, generation_id: int, incremental=None):
            calls.append((self.name, incremental.is_first_run,
                          incremental.change_basis))
            return EnrichResult(self.name, n_edges_emitted=0)

    calls: list[tuple[str, bool, str]] = []
    publisher = Publisher(
        "postgres://unused",
        "test",
        enrichers=[
            _SourceEnricher("git", "git-files"),
            _SourceEnricher("tickets", "tickets"),
        ],
        enricher_lineage_invalidations_hint=(
            invalidations_from_source_delta_batches((
                {
                    "source_id": "git-files",
                    "contract": "valid",
                    "changed_record_ids": ['{"path":"docs/a.md"}'],
                },
            ))
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.publisher.publisher.persist_transform_program",
        lambda conn, program: None,
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_progress_rows",
        lambda conn, names: {
            name: (5, 6, "success", None, 7) for name in names
        },
    )
    monkeypatch.setattr(
        publisher,
        "_load_enricher_profile_fingerprints",
        lambda conn, producer_ids: {
            producer_id: f"fp:{producer_id}" for producer_id in producer_ids
        },
    )
    monkeypatch.setattr(
        publisher,
        "_current_fact_watermarks",
        lambda conn: (5, 6),
    )
    monkeypatch.setattr(
        publisher,
        "_current_catalog_version_id",
        lambda conn: 7,
    )
    monkeypatch.setattr(
        publisher,
        "_query_dirty_from_chronos_current_segment_by_type",
        lambda conn, *, branch_name, previous=None, timing_sink=None, timing_prefix=None: (
            {"unrelated": ["unrelated:1"]},
            {},
            True,
            None,
            {},
            None,
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_expected_enricher_profile_fingerprint",
        lambda conn, *, enricher, producer_id, catalog_version_id: (
            f"fp:{producer_id}"
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_materialize_enricher_profile",
        lambda *args, **kwargs: "unused",
    )
    monkeypatch.setattr(
        publisher,
        "_admit_enricher_outputs",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        publisher,
        "_advance_enricher_progress",
        lambda *args, **kwargs: None,
    )

    runs = publisher._run_enrichers(
        _Conn(),
        generation_id=123,
        catalog_version_id=7,
    )

    assert calls == [("git", True, "lineage_dependency")]
    assert [run["status"] for run in runs[1:]] == ["ok", "skipped"]
    assert [run["lineage_envelope"]["dirty"] for run in runs[1:]] == [
        True,
        False,
    ]


# --- Issue #1528: withholding an enricher whose input source is quarantined ---


class _RecordingCursor:
    def __init__(self, owner: "_RecordingConn") -> None:
        self._owner = owner

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self._owner.queries.append((sql, params))

    def fetchall(self):
        return list(self._owner.rows)


class _RecordingConn:
    """Records every query so 'it did not ask' is a measured claim."""

    def __init__(self, rows=()) -> None:
        self.rows = list(rows)
        self.queries: list[tuple[str, object]] = []

    def cursor(self):
        return _RecordingCursor(self)


def test_reads_sources_declaration_carries_the_source_ids() -> None:
    class _SessionEnricher:
        name = "tool_use_symbol"
        reads_sources = ("codex_sessions", "claude_sessions")

    declaration = declare_enricher_dependencies(_SessionEnricher())

    assert declaration.available
    assert declaration.source_ids == frozenset({
        "codex_sessions",
        "claude_sessions",
    })
    assert "source_record" in declaration.kinds
    assert "source_content_hash" in declaration.kinds


def test_reads_source_ids_alias_carries_the_source_ids() -> None:
    class _AliasEnricher:
        name = "alias"
        reads_source_ids = ("git_commits",)

    declaration = declare_enricher_dependencies(_AliasEnricher())

    assert declaration.source_ids == frozenset({"git_commits"})


def test_held_input_sources_names_only_the_held_sources() -> None:
    class _SessionEnricher:
        name = "tool_use_symbol"
        reads_sources = ("codex_sessions", "claude_sessions")

    conn = _RecordingConn(rows=[("codex_sessions",)])

    held = held_input_sources(
        conn,
        declare_enricher_dependencies(_SessionEnricher()),
        deployment_name="default",
    )

    assert held == ("codex_sessions",)
    assert len(conn.queries) == 1
    sql, params = conn.queries[0]
    # The read is scoped to this deployment (issue #419), to source runs, and
    # to the LATEST envelope per source (issue #1528 F3) -- an older held run
    # must not pin the enricher forever.
    assert params["deployment"] == "default"
    assert params["sources"] == sorted(["claude_sessions", "codex_sessions"])
    assert params["held_modes"] == ["review", "quarantine"]
    assert params["undecided"] == ["pending", "quarantined"]
    assert params["dropped"] == ["rejected", "failed"]
    assert params["no_status_proof_mode"] == "quarantine"
    assert "intent = 'source_sync'" in sql
    assert "DISTINCT ON (source)" in sql
    assert "created_at DESC" in sql
    # Issue #1528 F1: the release conditions are terminal-status and
    # apply-proof, NOT "some status other than pending".
    assert "applied_to_default" in sql
    assert "node_facts_inserted" in sql


def test_an_enricher_without_a_source_declaration_is_not_gated() -> None:
    """The stated limit of this gate, pinned so it cannot become a surprise.

    An enricher that declares no input sources is NOT withheld, and the
    publisher does not even ask the admission ledger about it.
    """
    class _SubtypeOnlyEnricher:
        name = "subtype_only"
        reads_subtypes = ("source_file",)

    conn = _RecordingConn(rows=[("codex_sessions",)])

    held = held_input_sources(
        conn,
        declare_enricher_dependencies(_SubtypeOnlyEnricher()),
        deployment_name="default",
    )

    assert held == ()
    assert conn.queries == []


def test_undeclared_enricher_is_not_gated() -> None:
    class _UndeclaredEnricher:
        name = "undeclared"

    declaration = declare_enricher_dependencies(_UndeclaredEnricher())
    conn = _RecordingConn(rows=[("codex_sessions",)])

    assert not declaration.available
    assert held_input_sources(
        conn, declaration, deployment_name="default",
    ) == ()
    assert conn.queries == []


def test_withheld_status_names_the_sources_and_is_recognised() -> None:
    status = withheld_input_source_status(
        ("codex_sessions", "claude_sessions"),
    )

    assert status.startswith(WITHHELD_INPUT_SOURCE_HELD_STATUS_PREFIX)
    assert "codex_sessions" in status
    assert "claude_sessions" in status


# --- Issue #1528 F5: resolving a free-form declaration onto ownership ids -----


class _Entry:
    def __init__(self, name: str, ownership_id: str) -> None:
        self.name = name
        self.ownership_id = ownership_id


def test_alias_index_accepts_both_the_registry_name_and_the_ownership_id():
    """Issue #1528 F5. Ingest writes `admission_envelopes.source =
    entry.ownership_id` (`ingest/service.py:2914`), but an author writes the
    registry NAME in `reads_sources`. The two genuinely differ: a `code_repos`
    block generates a source named `git-files-<slug>` whose ownership id is
    `<slug>.git-files` (`sources/registry.py:806-812`). Both aliases must
    resolve, and both must normalize to the ownership id -- the only value
    the admission query can match.
    """
    index = source_alias_index([
        _Entry("git-files-okg", "okg.git-files"),
        _Entry("codex-sessions", "workspace.codex-sessions"),
    ])

    assert index["git-files-okg"] == "okg.git-files"
    assert index["okg.git-files"] == "okg.git-files"
    assert index["codex-sessions"] == "workspace.codex-sessions"
    assert index["workspace.codex-sessions"] == "workspace.codex-sessions"
    # Nothing else is invented.
    assert "git-files" not in index


def test_alias_index_accepts_name_ownership_pairs():
    index = source_alias_index([("codex-sessions", "ws.codex-sessions")])
    assert index["codex-sessions"] == "ws.codex-sessions"
    assert index["ws.codex-sessions"] == "ws.codex-sessions"


def test_alias_index_skips_an_entry_with_no_ownership_id():
    """A source with no ownership id writes no envelope `source`, so it can
    never be matched; indexing it under its name would resolve a declaration
    to a value the ledger never contains."""
    assert source_alias_index([_Entry("nameless", "")]) == {}


def test_a_declared_registry_name_resolves_to_its_ownership_id():
    class _E:
        name = "e"
        reads_sources = ("git-files-okg",)

    resolved, unknown = resolve_declared_source_ids(
        declare_enricher_dependencies(_E()),
        source_alias_index([_Entry("git-files-okg", "okg.git-files")]),
    )

    assert resolved == ("okg.git-files",)
    assert unknown == ()


def test_a_declared_ownership_id_resolves_to_itself():
    class _E:
        name = "e"
        reads_sources = ("okg.git-files",)

    resolved, unknown = resolve_declared_source_ids(
        declare_enricher_dependencies(_E()),
        source_alias_index([_Entry("git-files-okg", "okg.git-files")]),
    )

    assert resolved == ("okg.git-files",)
    assert unknown == ()


def test_an_unregistered_declaration_is_reported_as_unknown():
    """The refusal case. An id that resolves to nothing matches zero
    envelopes, so the hold query passes VACUOUSLY and the enricher ships
    ungated with no signal -- exactly the silence #1330 was made of."""
    class _E:
        name = "e"
        reads_sources = ("git-files-okg", "typoed-source")

    resolved, unknown = resolve_declared_source_ids(
        declare_enricher_dependencies(_E()),
        source_alias_index([_Entry("git-files-okg", "okg.git-files")]),
    )

    assert resolved == ("okg.git-files",)
    assert unknown == ("typoed-source",)


def test_resolved_ids_override_the_declaration_in_the_hold_query():
    """The resolved OWNERSHIP ids, not the declared names, are what the
    admission ledger is asked about."""
    class _E:
        name = "e"
        reads_sources = ("git-files-okg",)

    conn = _RecordingConn(rows=[])
    held_input_sources(
        conn,
        declare_enricher_dependencies(_E()),
        deployment_name="default",
        source_ids=["okg.git-files"],
    )

    assert conn.queries[0][1]["sources"] == ["okg.git-files"]


def test_every_withheld_reason_is_readable_from_its_status():
    """One reader for both writers, so `okg doctor` and the operator console
    cannot disagree about what a `withheld:` status means -- and so both
    stay outside the `failed:%` prefix that reddens enricher-liveness."""
    held = withheld_input_source_status(("b", "a"))
    unknown = unknown_declared_source_status(("typo",))

    assert withheld_reason_from_status(held) == (
        "withheld_source_held_for_admission", "a,b",
    )
    assert withheld_reason_from_status(unknown) == (
        "unknown_declared_source", "typo",
    )
    assert withheld_reason_from_status("success") is None
    assert withheld_reason_from_status("failed:boom") is None
    assert withheld_reason_from_status(None) is None
    # Both are admitted by the schema's single `withheld:%` constraint arm
    # and excluded from the `failed:%` liveness query.
    assert held.startswith("withheld:")
    assert unknown.startswith("withheld:")


def test_a_registry_that_yields_no_index_says_so_in_the_log(
    monkeypatch, caplog, tmp_path,
):
    """Issue #1528 F5 follow-up. `_source_alias_index` returning `None` means
    declared-source validation is SKIPPED and gating continues on the
    unnormalized id. The unreadable-registry path already warned; the two
    quiet paths -- no `source_registry.yaml` on disk, and a registry that
    parses to zero usable entries -- now say so too, naming the deployment,
    so an ungated publish is never silent."""
    import logging

    publisher = Publisher("postgres://unused", "dep-under-test")

    # 1. No registry file on disk.
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name, **kwargs: tmp_path,
    )
    with caplog.at_level(logging.INFO, "okg.substrate.publisher.publisher"):
        assert publisher._source_alias_index() is None
    assert any(
        "no source_registry.yaml on disk" in r.getMessage()
        and "dep-under-test" in r.getMessage()
        for r in caplog.records
    ), caplog.text

    # 2. A registry that parses to zero usable entries.
    registry_path = tmp_path / "source_registry.yaml"
    registry_path.write_text("sources: {}\n", encoding="utf-8")
    monkeypatch.setattr(
        "okg.substrate.sources.registry.load_source_registry",
        lambda path: {},
    )
    caplog.clear()
    # Same publisher, memo dropped: the index is cached for the object's life.
    publisher._source_alias_index_cache = Publisher._SOURCE_ALIAS_INDEX_UNSET
    with caplog.at_level(
        logging.WARNING, "okg.substrate.publisher.publisher",
    ):
        assert publisher._source_alias_index() is None
    assert any(
        "no usable entries" in r.getMessage()
        and "dep-under-test" in r.getMessage()
        for r in caplog.records
    ), caplog.text
