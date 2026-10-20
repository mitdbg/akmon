"""academic-researcher profile + okg init coverage.

Coverage matrix:

1. Profile discovery: `discover_profile('academic-researcher')` finds
   the substrate-shipped profile.
2. Profile shape: name + version + init questions + modules.yaml +
   source-defaults/ + invariants.yaml + dashboards/ load cleanly.
3. modules.yaml composes the research baseline (the former
   cs-researcher module set) + academic_researcher.
4. `okg init --profile academic-researcher` scaffolds a deployment
   that composes cleanly.
5. Invariants include the PI-tuned ones (advisee floor, grant
   milestone overdue, letter request outstanding, service load).
6. Dashboards include pi-overview and researcher-overview.

Per `codify-academic-pi-profile` (Child 3 of `add-academic-pi-profile`);
renamed from academic-pi and merged with cs-researcher on 2026-09-23.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.profile import (
    load_profile,
    load_profile_modules,
)
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    discover_profile,
    scaffold_deployment,
)


PROFILE_NAME = "academic-researcher"
PROFILE_DIR = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/profiles" / PROFILE_NAME
)


# ---------- 1. Profile shape ----------


def test_profile_directory_present() -> None:
    """The substrate-shipped profile directory exists with the
    expected files.
    """
    assert PROFILE_DIR.is_dir(), (
        f"academic-researcher profile missing at {PROFILE_DIR}"
    )
    assert (PROFILE_DIR / "profile.yaml").is_file()
    assert (PROFILE_DIR / "modules.yaml").is_file()
    assert (PROFILE_DIR / "source-defaults").is_dir()
    assert (PROFILE_DIR / "invariants.yaml").is_file()
    assert (PROFILE_DIR / "dashboards").is_dir()


def test_profile_loads_cleanly() -> None:
    """The profile.yaml parses through Layer 2A's loader."""
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    assert profile.name == "academic-researcher"
    assert profile.version == "0.1.0"
    assert profile.description, "description must be non-empty"


def test_profile_discovery_finds_academic_researcher() -> None:
    """Layer 2A's discover_profile cascade locates the profile."""
    profile = discover_profile(PROFILE_NAME)
    assert profile.name == PROFILE_NAME
    assert profile.modules_file is not None


def test_profile_init_questions_carry_required_ids() -> None:
    """Required init questions: deployment_name, postgres_dsn,
    orcid. Optional questions: papers_dir, cv_path, group_webpage,
    advisees_dir, grants_dir, letters_log, calendar_ics, notes_dir,
    mcp_port, pi_name, git_repos.
    """
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    qids = {q.id for q in profile.init_questions}
    expected_required = {"deployment_name", "postgres_dsn", "orcid"}
    expected_optional = {
        "papers_dir", "cv_path", "group_webpage", "advisees_dir",
        "grants_dir", "letters_log", "calendar_ics", "notes_dir",
        "mcp_port", "pi_name", "git_repos",
    }
    missing = (expected_required | expected_optional) - qids
    assert not missing, f"missing init_questions: {missing}"


def test_profile_required_init_questions() -> None:
    """deployment_name + postgres_dsn are required. ORCID is
    optional per `dead-simple-install` (2026-05-21): the
    `dblp_by_orcid` source has a name-based fallback (per
    `extend-dblp-by-orcid-with-name-fallback`), so the install
    can complete without ORCID."""
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    by_id = {q.id: q for q in profile.init_questions}
    assert by_id["deployment_name"].required is True
    assert by_id["postgres_dsn"].required is True
    assert by_id["orcid"].required is False, (
        "ORCID was made optional in `dead-simple-install` — the "
        "dblp_by_orcid source falls back to name-based search."
    )
    # Optional answers
    for opt in ("papers_dir", "cv_path", "group_webpage",
                "advisees_dir", "grants_dir", "letters_log",
                "calendar_ics", "notes_dir", "git_repos"):
        assert by_id[opt].required is False, (
            f"{opt} SHALL be optional (skippable for a minimal "
            f"first install)"
        )


def test_profile_mcp_port_defaults_to_5431() -> None:
    """Default MCP port is 5431."""
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    by_id = {q.id: q for q in profile.init_questions}
    assert by_id["mcp_port"].default == 5431


def test_profile_modules_yaml_composes_research_base_plus_academic_researcher() -> None:
    """modules.yaml SHALL include the research baseline (the whole
    module set of the retired cs-researcher profile, merged in on
    2026-09-23) plus academic_researcher.
    """
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    modules = load_profile_modules(profile)
    assert "academic_researcher" in modules, (
        "academic-researcher profile SHALL load the academic_researcher "
        "module"
    )
    assert "academic_pi" not in modules
    # The former cs-researcher modules.yaml, verbatim.
    expected_baseline = {
        "extraction", "concepts", "person", "views", "reasoning",
        "methodology", "experimental", "scholarly", "coordination",
        "ci", "quality_evidence", "bookkeeping", "agent_sessions",
        "compute_env", "agent_memory", "dataset", "git_graph",
        "code_graph", "openspec", "external_repos", "personas",
    }
    missing = expected_baseline - set(modules)
    assert not missing, (
        f"academic-researcher modules missing research baseline: "
        f"{missing}"
    )


def test_profile_source_defaults_present() -> None:
    """The profile ships every source default the retired
    cs-researcher profile shipped (its fifteen ids), plus the
    lab-management sources from `add-academic-pi-sources`.
    """
    sd_dir = PROFILE_DIR / "source-defaults"
    sources = {p.stem for p in sd_dir.glob("*.yaml")}
    former_cs_researcher = {
        "agent_sessions", "author_aliases", "bib_references",
        "code_structure", "concept_aliases", "dblp", "external_repos",
        "git_files", "git_history", "latex_project",
        "latex_project_doc_corpus", "latex_project_git_files",
        "openspec", "paper_pdf_inbox", "talks",
    }
    missing = former_cs_researcher - sources
    assert not missing, (
        f"academic-researcher profile missing carried-over sources: "
        f"{sorted(missing)}"
    )


def test_profile_dashboards_present() -> None:
    """Profile ships pi-overview and the researcher-overview
    dashboard carried over from cs-researcher."""
    dash_dir = PROFILE_DIR / "dashboards"
    dashboards = {p.stem for p in dash_dir.glob("*.yaml")}
    assert {"pi-overview", "researcher-overview"} <= dashboards, (
        f"academic-researcher profile missing a dashboard; "
        f"present: {sorted(dashboards)}"
    )
    for path in dash_dir.glob("*.yaml"):
        raw = yaml.safe_load(path.read_text())
        assert raw["name"] == path.stem
        assert raw["panels"], f"{path.name}: no panels"
        for panel in raw["panels"]:
            assert panel["id"] and panel["sql"].strip(), (
                f"{path.name}: panel without id or sql"
            )


def test_profile_invariants_carry_pi_tuned_set() -> None:
    """Profile invariants ship the PI-tuned set: core subtypes
    present, advisee floor, grant milestone overdue, letter request
    outstanding, service load threshold, DLQ zero.
    """
    invariants_text = (PROFILE_DIR / "invariants.yaml").read_text()
    raw = yaml.safe_load(invariants_text) or {}
    invariants = raw.get("invariants") or []
    names = {inv.get("name") for inv in invariants}
    expected = {
        "academic_researcher_core_subtypes_present",
        "academic_researcher_advisee_floor",
        "academic_researcher_grant_milestone_overdue_warn",
        "academic_researcher_letter_request_outstanding_warn",
        "academic_researcher_service_load_warn_threshold",
        "academic_researcher_source_sync_dlq_zero",
    }
    missing = expected - names
    assert not missing, (
        f"academic-researcher profile missing invariants: {missing}"
    )


# ---------- 2. End-to-end: build_plan + scaffold + compose ----------


def _answers() -> dict:
    return {
        "deployment_name": "test_pi",
        "postgres_dsn": (
            "postgres://postgres:okg@localhost:5435/okg_test_pi"
        ),
        "orcid": "0000-0001-2345-6789",
        "papers_dir": "/tmp/sample_papers",
        "cv_path": "",
        "group_webpage": "",
        "advisees_dir": "",
        "grants_dir": "",
        "letters_log": "",
        "calendar_ics": "",
        "notes_dir": "",
        "mcp_port": "5431",
        "pi_name": "",
        "git_repos": "",
    }


def test_build_plan_interpolates_answers(tmp_path: Path) -> None:
    """build_plan substitutes ${deployment_name} into the carried-
    over source defaults (bib_references uses ${papers_dir}).
    """
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="test_pi",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(),
    )
    # bib_references.yaml carries ${papers_dir} interpolation
    bib = plan.source_defaults.get("bib_references", "")
    assert "/tmp/sample_papers" in bib
    assert "${papers_dir}" not in bib
    assert "${deployment_name}" not in bib


def test_scaffold_deployment_writes_expected_files(
    tmp_path: Path,
) -> None:
    """The scaffolded deployment carries the profile artifacts."""
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="test_pi",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(),
    )
    result = scaffold_deployment(plan)
    dep_dir = result.deployment_dir
    assert dep_dir.is_dir()
    assert (dep_dir / "deployment.yaml").is_file()
    assert (dep_dir / "source_registry.yaml").is_file()
    assert (dep_dir / "invariants.yaml").is_file()
    assert (dep_dir / "dashboards" / "pi-overview.yaml").is_file()
    assert (dep_dir / "dashboards" / "researcher-overview.yaml").is_file()
    # deployment.yaml carries the profile-reference block.
    dep_yaml = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert dep_yaml.get("profile", {}).get("name") == "academic-researcher"
    assert "academic_researcher" in dep_yaml["modules"]
    assert "academic_pi" not in dep_yaml["modules"]
    # Every lab-management source names the renamed package.
    registry = (dep_dir / "source_registry.yaml").read_text()
    assert "sources.academic_researcher." in registry
    assert "sources.academic_pi." not in registry


def test_scaffolded_deployment_composes_cleanly(
    tmp_path: Path,
) -> None:
    """A fresh academic-researcher scaffold composes against the
    catalog without errors. Verifies the full module list (research
    baseline + academic_researcher) resolves all narrowings.
    """
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="test_pi",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(),
    )
    result = scaffold_deployment(plan)
    dep_dir = result.deployment_dir
    composed = compose_catalog(dep_dir)
    # academic_researcher subtypes appear, attributed to that module.
    # `meeting` migrated to `calendar.calendar_event` per
    # `refine-academic-pi-with-comms-and-calendar`.
    expected_subtypes = {
        "advisee", "lab", "grant", "deadline", "committee",
        "review", "course", "talk_engagement",
        "recommendation_letter", "action_item",
        "calendar_event", "calendar_feed",
        "email", "email_thread", "chat_channel", "chat_message",
    }
    missing = expected_subtypes - set(composed.subtypes)
    assert not missing, (
        f"scaffolded academic-researcher deployment compose missing "
        f"subtypes: {missing}"
    )
    assert composed.subtype_origin.get("advisee") == "academic_researcher"
    assert "academic_pi" not in composed.modules
    # Refinement closure works.
    assert composed.subtype_parent.get("advisee") == "person"
    assert composed.subtype_parent.get("lab") == "team"
    # Cross-module narrowings to scholarly resolve.
    triples = composed.narrowing_triples()
    assert ("advisee", "author_of", "paper") in triples
    assert ("grant", "acknowledged_in", "paper") in triples


def test_okg_init_writes_profile_reference_block_and_dashboards(
    tmp_path: Path, capsys,
) -> None:
    """End-to-end through the CLI: `okg init --profile
    academic-researcher --no-publish` writes the scaffold, copies both
    dashboards, and records {name, version, initialized_at,
    init_answers}. Carried over from the retired cs-researcher test."""
    from types import SimpleNamespace

    import okg.substrate.cli.main as cli

    papers = tmp_path / "papers"
    repo = tmp_path / "repo"
    papers.mkdir()
    repo.mkdir()
    deployments_root = tmp_path / "deployments"
    args = SimpleNamespace(
        command="init",
        profile_name=PROFILE_NAME,
        template=None,
        deployment_name="ref_block_test",
        deployments_root=str(deployments_root),
        force=False, yes=False, no_publish=True, non_interactive=True,
        target=None, repo_root=None, openspec_root=None,
        claude_projects_root=None, notes_root=None, since=None,
        answer_postgres_dsn="postgresql:///ref_block",
        answer_orcid=None,
        answer_papers_dir=str(papers),
        answer_git_repos=str(repo),
    )
    rc = cli._cmd_init(args, as_json=True)
    assert rc == 0, capsys.readouterr()
    dep_dir = deployments_root / "ref_block_test"
    for name in ("pi-overview", "researcher-overview"):
        assert (dep_dir / "dashboards" / f"{name}.yaml").is_file()
    dep_yaml = yaml.safe_load((dep_dir / "deployment.yaml").read_text())
    assert dep_yaml["profile"]["name"] == PROFILE_NAME
    assert dep_yaml["profile"]["version"] == "0.1.0"
    assert "initialized_at" in dep_yaml["profile"]
    answers = dep_yaml["profile"]["init_answers"]
    assert answers["deployment_name"] == "ref_block_test"
    assert answers["postgres_dsn"] == "postgresql:///ref_block"
    assert str(papers) in str(answers["papers_dir"])


def test_scaffold_interpolates_git_repos_and_never_authors_the_dsn(
    tmp_path: Path,
) -> None:
    """`${papers_dir}` / `${git_repos}` / `${deployment_name}` resolve
    from init answers, and no source default authors the DSN (issue
    #2126). Carried over from the retired cs-researcher test."""
    plan = build_plan(
        profile=discover_profile(PROFILE_NAME),
        deployment_name="interp_test",
        deployments_root=tmp_path,
        raw_answers={
            **_answers(),
            "deployment_name": "interp_test",
            "postgres_dsn": "postgresql:///interp",
            "papers_dir": "/var/data/papers",
            "git_repos": ["/var/code/repo"],
        },
    )
    sr_text = (
        scaffold_deployment(plan).deployment_dir / "source_registry.yaml"
    ).read_text()
    for placeholder in (
        "${papers_dir}", "${git_repos}", "${deployment_name}",
        "${postgres_dsn}",
    ):
        assert placeholder not in sr_text
    assert "/var/data/papers" in sr_text
    assert "/var/code/repo" in sr_text
    assert "postgresql:///interp" not in sr_text
    sources = yaml.safe_load(sr_text)["sources"]
    for sid in (
        "openspec", "bib_references", "git_files", "git_history",
        "code_structure", "agent_sessions",
    ):
        assert sid in sources, sorted(sources)


# ---------- 2b. Source defaults — enabled_by_default ----------


EXPECTED_ENABLED_BY_DEFAULT: set[str] = set()

EXPECTED_OFF_BY_DEFAULT = {
    # Operator-config-required.
    "gmail_api",
    "google_calendar_api",
    "slack_api",
    "notes_dir",
    "paper_pdf_inbox",
    "cv_ingest_llm",
    "group_webpage_scrape",
    "lab_webpage_talks",
    "calendar_ics",
    "apple_calendar_local",
    "mit_course_catalog",
    "bib_references",
    "talks",
    # Demoted per F-5 (see above).
    "author_aliases",
    "concept_aliases",
    "identity_aliases",
    "external_repos",
    "dblp",
    # Wizard-first install: even zero-config public identity
    # and broad okg-workspace sources require setup approval.
    "nsf_award_search",
    "doe_osti",
    "dblp_by_orcid",
    "agent_sessions",
    "git_files",
    "git_history",
    "code_structure",
    "openspec",
}


def test_every_source_default_declares_enabled_by_default() -> None:
    """Per `streamline-academic-pi-install`: every YAML under the
    profile's `source-defaults/` SHALL declare a top-level
    `enabled_by_default: bool` field. Sources requiring operator-
    provided config are `false`; zero-config sources are `true`.
    """
    sd_dir = PROFILE_DIR / "source-defaults"
    for yaml_path in sd_dir.glob("*.yaml"):
        text = yaml_path.read_text()
        parsed = yaml.safe_load(text)
        assert isinstance(parsed, dict) and len(parsed) == 1, (
            f"{yaml_path.name}: expected single top-level source-id "
            f"key, got {list(parsed.keys()) if parsed else None}"
        )
        source_id = next(iter(parsed))
        body = parsed[source_id]
        assert isinstance(body, dict) and "enabled_by_default" in body, (
            f"{yaml_path.name}: missing `enabled_by_default` field "
            f"under `{source_id}:`"
        )
        flag = body["enabled_by_default"]
        assert isinstance(flag, bool), (
            f"{yaml_path.name}: `enabled_by_default` must be bool, "
            f"got {flag!r}"
        )
        if source_id in EXPECTED_ENABLED_BY_DEFAULT:
            assert flag is True, (
                f"{yaml_path.name}: expected `enabled_by_default: "
                f"true` for zero-config source `{source_id}`"
            )
        elif source_id in EXPECTED_OFF_BY_DEFAULT:
            assert flag is False, (
                f"{yaml_path.name}: expected `enabled_by_default: "
                f"false` for operator-config source `{source_id}`"
            )


def test_scaffold_carries_enabled_by_default_into_source_registry(
    tmp_path: Path,
) -> None:
    """A scaffolded deployment's `source_registry.yaml` SHALL
    preserve each source's `enabled_by_default` flag from the
    profile's source-defaults.
    """
    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="test_pi",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(),
    )
    result = scaffold_deployment(plan)
    registry_path = result.deployment_dir / "source_registry.yaml"
    registry_raw = yaml.safe_load(registry_path.read_text())
    sources = registry_raw.get("sources") or {}
    for source_id in EXPECTED_ENABLED_BY_DEFAULT:
        entry = sources.get(source_id)
        if entry is None:
            continue  # source not in this profile — ok
        assert entry.get("enabled_by_default") is True, (
            f"source_registry.yaml: `{source_id}` should be "
            f"enabled_by_default=true but is "
            f"{entry.get('enabled_by_default')!r}"
        )
    for source_id in EXPECTED_OFF_BY_DEFAULT:
        entry = sources.get(source_id)
        if entry is None:
            continue
        assert entry.get("enabled_by_default") is False, (
            f"source_registry.yaml: `{source_id}` should be "
            f"enabled_by_default=false but is "
            f"{entry.get('enabled_by_default')!r}"
        )


def test_source_entry_loader_exposes_enabled_by_default(
    tmp_path: Path,
) -> None:
    """`load_source_registry` SHALL expose `enabled_by_default` on
    each `SourceEntry`. Missing field reads as `False`
    (conservative default for existing pre-flag deployments).
    """
    from okg.substrate.sources.registry import load_source_registry

    profile = load_profile(PROFILE_DIR / "profile.yaml")
    plan = build_plan(
        profile=profile,
        deployment_name="test_pi",
        deployments_root=tmp_path / "deployments",
        raw_answers=_answers(),
    )
    result = scaffold_deployment(plan)
    registry = load_source_registry(
        result.deployment_dir / "source_registry.yaml",
    )
    enabled = {
        name for name, e in registry.items() if e.enabled_by_default
    }
    disabled = {
        name for name, e in registry.items()
        if not e.enabled_by_default
    }
    assert enabled == set()
    assert "gmail_api" in disabled
    assert "dblp" in disabled
    assert "git_files" in disabled
