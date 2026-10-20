"""A profile-bearing package can ask for strict source admission (#1915).

`_build_source_registry_yaml` rendered the installed `source_registry.yaml`
with a top-level `sources:` key and nothing else, so no profile could put
`admission_contract: strict_v1` at the top of the registry it installs and
every packaged source was admitted through the legacy contract.

What the strict contract actually adds over legacy is NOT output-signature
narrowing: `admission_policy` is parsed under both contracts and
`_prepare_source_admission` materializes the producer profile from it either
way, so a fact outside a declared signature is already refused on the legacy
path. Strict adds document-level admission: the registry is validated against
`source_registry.schema.json`, its scalars must survive a strict JSON
boundary, duplicate `ownership_id` / `producer_id` are refused, entries are
frozen, and every runnable adapter is planned before any constructor runs
(`plan_source_registry_bindings`). The tests below distinguish the two
contracts by a document the legacy path admits and the strict path refuses.

They also pin that a profile declaring strict admission cannot scaffold a
registry that fails to load later, that a profile declaring nothing renders
what it rendered before, and that an unknown contract is refused.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.profile import ProfileError, load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    scaffold_deployment,
)
from okg.substrate.sources.registry import (
    SourceRegistryUnknownKeyError,
    load_source_registry_admission,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILES_ROOT = REPO_ROOT / "src/okg/substrate/library/profiles"
STRICT_DECLARATION = "source_admission_contract: strict_v1\n"


def _source_default(
    *,
    ownership_id: str = "archi.twiki",
    producer_id: str = "twiki_eos",
    extra: str = "",
) -> str:
    """One connector entry as a distribution ships it in `source-defaults/`:
    every key the strict registry schema requires, plus an `admission_policy`
    whose output signature admits one node subtype."""
    return (
        "module: archi.connectors.twiki\n"
        "class: TwikiEosConnector\n"
        f"ownership_id: {ownership_id}\n"
        "source_class: discovery_crawl\n"
        "record_identity_kind: scoped_locator\n"
        "record_identity_fields: [path]\n"
        "source_revision_kind: content_hash\n"
        "deletion_semantics: missing_from_completed_scope\n"
        "publication_mode: published_generation\n"
        "description: Archi TWiki crawl\n"
        "credential_refs: [ARCHI_TWIKI_TOKEN]\n"
        "sync:\n"
        "  triggers: poll\n"
        "  poll:\n"
        "    interval_seconds: 900\n"
        "admission_policy:\n"
        f"  producer_id: {producer_id}\n"
        "  producer_kind: source\n"
        "  trust_label: archi_operated\n"
        "  admission_mode: direct\n"
        "  output_signature:\n"
        "    nodes:\n"
        "      - subtype: Document\n"
        "        clause_id: allow_document\n"
        + extra
    )


def _write_profile(
    root: Path,
    *,
    declaration: str = "",
    sources: dict[str, str] | None = None,
) -> Path:
    """Write a minimal profile package and return its `profile.yaml`."""
    profile_dir = root / "profile"
    (profile_dir / "source-defaults").mkdir(parents=True)
    for stem, body in (sources or {"twiki_eos": _source_default()}).items():
        (profile_dir / "source-defaults" / f"{stem}.yaml").write_text(
            body,
            encoding="utf-8",
        )
    profile_path = profile_dir / "profile.yaml"
    profile_path.write_text(
        "name: strict-admission-probe\n"
        "version: 1.0.0\n"
        "source_defaults: source-defaults\n"
        + declaration,
        encoding="utf-8",
    )
    return profile_path


def _scaffold(profile_path: Path, tmp_path: Path) -> Path:
    plan = build_plan(
        profile=load_profile(profile_path),
        deployment_name="strict_probe",
        deployments_root=tmp_path / "deployments",
        raw_answers={},
    )
    result = scaffold_deployment(plan)
    return result.deployment_dir / "source_registry.yaml"


def test_the_loaders_contract_name_matches_the_registrys() -> None:
    """`catalog.profile` spells the contract itself rather than importing it
    from `sources.registry`, so the profile loader takes no dependency on the
    ingest stack of its own. (It does not yet shed that stack: importing
    `okg.substrate.catalog` pulls `catalog.reconcile`, and with it the
    registry, either way.) This pins the two spellings equal so they cannot
    drift."""
    from okg.substrate.catalog import profile as profile_module
    from okg.substrate.sources.registry import STRICT_ADMISSION_CONTRACT

    assert profile_module._STRICT_ADMISSION_CONTRACT == STRICT_ADMISSION_CONTRACT


def test_declared_strict_contract_reaches_the_rendered_registry(
    tmp_path: Path,
) -> None:
    registry_path = _scaffold(
        _write_profile(tmp_path, declaration=STRICT_DECLARATION),
        tmp_path,
    )

    document = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert document["admission_contract"] == "strict_v1"

    admission = load_source_registry_admission(registry_path)
    assert admission.contract == "strict_v1"
    assert admission.strict
    assert admission.entries["twiki_eos"].admission_contract == "strict_v1"


# A document the legacy contract admits and the strict contract refuses: it
# duplicates an owner across two sources. That is the difference the declared
# contract actually makes. (A key the registry schema does not allow on a
# source used to be a second difference; both contracts now refuse it, see
# test_both_contracts_refuse_a_key_outside_the_registry_schema.)
CONTRACT_DIFFERENCES = {
    "duplicate_ownership_id": (
        {
            "twiki_eos": _source_default(),
            "twiki_eos_mirror": _source_default(producer_id="twiki_mirror"),
        },
        "duplicate ownership_id",
    ),
}


def test_both_contracts_refuse_a_key_outside_the_registry_schema(
    tmp_path: Path,
) -> None:
    """An entry-level `fixture_path` is read by nothing. Both contracts
    refuse it at scaffold time, while the operator is still watching `okg
    init`; the legacy refusal names the key and where it belongs, and no
    registry is written."""
    sources = {
        "twiki_eos": _source_default(extra="fixture_path: /tmp/records\n"),
    }
    with pytest.raises(ProfileError) as legacy_refusal:
        _scaffold(
            _write_profile(tmp_path / "legacy", sources=sources),
            tmp_path / "legacy",
        )
    assert legacy_refusal.value.data["code"] == "source_registry_unknown_key"
    assert legacy_refusal.value.data["source"] == "twiki_eos"
    assert legacy_refusal.value.data["key"] == "fixture_path"
    assert "sources.twiki_eos.params.fixture_path" in str(legacy_refusal.value)
    assert isinstance(
        legacy_refusal.value.__cause__, SourceRegistryUnknownKeyError,
    )
    assert not (
        tmp_path / "legacy" / "deployments" / "strict_probe"
        / "source_registry.yaml"
    ).exists()

    with pytest.raises(ProfileError) as strict_refusal:
        _scaffold(
            _write_profile(
                tmp_path / "strict",
                declaration=STRICT_DECLARATION,
                sources=sources,
            ),
            tmp_path / "strict",
        )
    assert "Additional properties are not allowed" in str(strict_refusal.value)


@pytest.mark.parametrize("case", sorted(CONTRACT_DIFFERENCES))
def test_legacy_admits_what_strict_refuses_at_scaffold_time(
    case: str, tmp_path: Path,
) -> None:
    """The contracts must be distinguishable, and the strict refusal must
    land while the operator is still watching `okg init` -- not at the first
    ingest, hours after a packaged installer exited 0."""
    sources, expected = CONTRACT_DIFFERENCES[case]

    # Declaring nothing: scaffolds, and the rendered registry loads.
    legacy_registry = _scaffold(
        _write_profile(tmp_path / "legacy", sources=sources),
        tmp_path / "legacy",
    )
    assert load_source_registry_admission(legacy_registry).contract == "legacy_v1"

    # Declaring strict: refused at scaffold time, naming the cause.
    with pytest.raises(ProfileError) as refusal:
        _scaffold(
            _write_profile(
                tmp_path / "strict",
                declaration=STRICT_DECLARATION,
                sources=sources,
            ),
            tmp_path / "strict",
        )
    assert expected in str(refusal.value)
    assert refusal.value.data["key"] == "source_admission_contract"
    assert not (
        tmp_path / "strict" / "deployments" / "strict_probe"
        / "source_registry.yaml"
    ).exists()


def test_profile_declaring_nothing_renders_the_registry_it_rendered_before(
    tmp_path: Path,
) -> None:
    registry_path = _scaffold(_write_profile(tmp_path), tmp_path)

    rendered = registry_path.read_text(encoding="utf-8")
    assert rendered == yaml.safe_dump(
        {"sources": {"twiki_eos": yaml.safe_load(_source_default())}},
        sort_keys=False,
    )
    assert "admission_contract" not in yaml.safe_load(rendered)
    assert load_source_registry_admission(registry_path).contract == "legacy_v1"


def _shipped_profiles() -> list[str]:
    names = [
        path.parent.name
        for path in sorted(PROFILES_ROOT.glob("*/profile.yaml"))
    ]
    assert names
    return names


def _required_answers(profile, tmp_path: Path) -> dict:
    answers: dict = {}
    for question in profile.init_questions:
        if not question.required:
            continue
        if question.type == "slug":
            answers[question.id] = "strict_probe"
        elif question.type == "path":
            answers[question.id] = str(tmp_path)
        else:
            answers[question.id] = (
                "postgres://postgres:okg@localhost:5433/okg_probe"
            )
    return answers


@pytest.mark.parametrize("profile_name", _shipped_profiles())
def test_a_shipped_profile_renders_the_bytes_it_rendered_before(
    profile_name: str, tmp_path: Path,
) -> None:
    """No shipped profile declares the key, so each must render exactly what
    the previous renderer produced: `yaml.safe_dump({"sources": ...})` and
    nothing else."""
    profile = load_profile(PROFILES_ROOT / profile_name / "profile.yaml")
    assert profile.source_admission_contract is None
    plan = build_plan(
        profile=profile,
        deployment_name="strict_probe",
        deployments_root=tmp_path / "deployments",
        raw_answers=_required_answers(profile, tmp_path),
    )
    rendered = (
        scaffold_deployment(plan).deployment_dir / "source_registry.yaml"
    ).read_text(encoding="utf-8")

    document = yaml.safe_load(rendered)
    assert list(document) == ["sources"]
    assert rendered == yaml.safe_dump(
        {"sources": document["sources"]},
        sort_keys=False,
    )


@pytest.mark.parametrize("declared", ["strict_v2", "legacy_v1", "''", "true"])
def test_an_unknown_contract_is_refused_not_defaulted(
    declared: str, tmp_path: Path,
) -> None:
    profile_path = _write_profile(
        tmp_path,
        declaration=f"source_admission_contract: {declared}\n",
    )

    # Refused when the profile loads, before anything is scaffolded.
    with pytest.raises(ProfileError) as refusal:
        load_profile(profile_path)
    assert "source_admission_contract" in str(refusal.value)
    assert refusal.value.data["supported"] == ["strict_v1"]

    with pytest.raises(ProfileError):
        _scaffold(profile_path, tmp_path)
