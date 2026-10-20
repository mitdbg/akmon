from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FORMAL_ROOT = ROOT / "formal"

REQUIRED_SECTIONS = (
    "## System",
    "## Design Contract",
    "## Model Inventory",
    "## Transformation Map",
    "## Invariant Map",
    "## Verification Process",
    "## Model Boundary",
)
PATH_PREFIXES = ("src/", "deployments/", "tests/")
EXPENSIVE_TLC_MODEL_DIRS = {
    "admission": "AdmissionSmoke.cfg",
    "attributes": "AttributeStateSmoke.cfg",
    "consistency": "TransformGateDirtyScopeSmoke.cfg",
    "nomos": "NomosSourceLineage.cfg",
}


def _direct_model_dirs() -> list[Path]:
    return sorted(
        path
        for path in FORMAL_ROOT.iterdir()
        if path.is_dir() and (any(path.glob("*.tla")) or any(path.glob("*.py")))
    )


def _cfg_invariants(config: Path) -> list[str]:
    invariants: list[str] = []
    in_block = False
    for raw_line in config.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            if in_block:
                break
            continue
        if line == "INVARIANTS":
            in_block = True
            continue
        if in_block and line in {"CONSTANTS", "CONSTRAINT", "SPECIFICATION"}:
            break
        if in_block:
            invariants.append(line)
    return invariants


def _cfg_properties(config: Path) -> list[str]:
    """Return the temporal PROPERTIES declared in a .cfg (empty if none).

    A non-empty list means the config asks TLC to check a temporal property
    (e.g. ``<>Ready`` or ``[](workPending => <>Ready)``), not just safety
    INVARIANTS. This is the signal that a model carries a real liveness
    claim rather than reusing liveness vocabulary for a safety property.
    """
    properties: list[str] = []
    in_block = False
    for raw_line in config.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            if in_block:
                break
            continue
        if line == "PROPERTIES":
            in_block = True
            continue
        if in_block and line in {
            "CONSTANTS",
            "CONSTRAINT",
            "SPECIFICATION",
            "INVARIANTS",
        }:
            break
        if in_block:
            properties.append(line)
    return properties


# Vocabulary that specifically denotes temporal-logic / liveness claims, as
# opposed to the plain-English "progress"/"convergence" that safety-only
# models (e.g. enricher_backfill, pact) legitimately use for workflow steps.
_TEMPORAL_CLAIM_PATTERN = re.compile(
    r"\b(liveness|eventually|temporal\s+propert)", re.IGNORECASE
)


def _section_text(text: str, heading: str) -> str:
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    if next_heading == -1:
        return text[start:]
    return text[start:next_heading]


def _path_refs(text: str) -> list[str]:
    refs: list[str] = []
    for ref in re.findall(r"`([^`]+)`", text):
        if ref.startswith(PATH_PREFIXES):
            refs.append(ref.split("::", 1)[0])
    return refs


def test_formal_root_readme_lists_model_folders() -> None:
    readme = FORMAL_ROOT / "README.md"
    assert readme.is_file(), "formal/README.md must describe the formal surface"
    text = readme.read_text(encoding="utf-8")

    for heading in (
        "## Folder Index",
        "## Standard Folder Format",
        "## Verification Process",
    ):
        assert heading in text
    assert "formal/boundary-inventory.toml" in text
    for section in REQUIRED_SECTIONS:
        assert f"`{section.removeprefix('## ')}`" in text
    for model_dir in _direct_model_dirs():
        assert f"`{model_dir.name}`" in text


def test_each_formal_model_folder_has_standard_readme() -> None:
    for model_dir in _direct_model_dirs():
        readme = model_dir / "README.md"
        assert readme.is_file(), f"{readme} is required"
        text = readme.read_text(encoding="utf-8")

        for section in REQUIRED_SECTIONS:
            assert section in text, f"{readme} missing section {section}"
        transformation_map = _section_text(text, "## Transformation Map")
        invariant_map = _section_text(text, "## Invariant Map")
        for column in (
            "Runtime transformation",
            "Production code",
            "Implementation tests",
        ):
            assert column in transformation_map, (
                f"{readme} transformation map must include {column}"
            )
        assert "`src/" in transformation_map or "`deployments/" in transformation_map, (
            f"{readme} must bind the model to production source code"
        )
        assert "`tests/" in transformation_map or ".test." in transformation_map, (
            f"{readme} must bind the model to executable implementation tests"
        )
        for column in ("What it means", "Code/test signal"):
            assert column in invariant_map, (
                f"{readme} invariant map must include {column}"
            )
        for ref in _path_refs(transformation_map + invariant_map):
            assert (ROOT / ref).exists(), f"{readme} references missing path {ref}"
        for model in sorted(model_dir.glob("*.tla")):
            assert model.name in text, f"{readme} does not document {model.name}"
            assert model.name in transformation_map, (
                f"{readme} does not map {model.name} to a runtime transformation"
            )
        for model in sorted(model_dir.glob("*.py")):
            assert model.name in text, f"{readme} does not document {model.name}"
            assert model.name in transformation_map, (
                f"{readme} does not map {model.name} to a runtime transformation"
            )
        for config in sorted(model_dir.glob("*.cfg")):
            assert config.name in text, f"{readme} does not document {config.name}"
            for invariant in _cfg_invariants(config):
                assert invariant in invariant_map, (
                    f"{readme} does not document invariant {invariant} "
                    f"from {config.name}"
                )


def test_archive_only_formal_folders_are_not_listed_as_active_assurance() -> None:
    root_text = (FORMAL_ROOT / "README.md").read_text(encoding="utf-8")

    for model_dir in _direct_model_dirs():
        readme = model_dir / "README.md"
        text = readme.read_text(encoding="utf-8")
        archive_only = (
            "historical reference material" in text
            and "not verification gates" in text
        )
        if not archive_only:
            continue
        index_row = next(
            line
            for line in root_text.splitlines()
            if line.startswith(f"| `{model_dir.name}` |")
        )
        verification_process = _section_text(text, "## Verification Process")

        assert "archive-only" in index_row.lower()
        assert "--archived-helper" in verification_process
        assert "okg pact evidence ingest" not in verification_process


def test_expensive_tlc_readmes_document_execution_profiles() -> None:
    root_text = (FORMAL_ROOT / "README.md").read_text(encoding="utf-8")

    assert "--list-profiles --json" in root_text
    assert "proof_status: not_freshly_proven" in root_text
    assert "failure_class: invariant_violation" in root_text
    for folder, smoke_config in EXPENSIVE_TLC_MODEL_DIRS.items():
        readme = FORMAL_ROOT / folder / "README.md"
        text = readme.read_text(encoding="utf-8")
        profile_section = _section_text(text, "## Execution Profiles")

        assert "| `smoke` |" in profile_section
        assert "| `full` |" in profile_section
        assert smoke_config in profile_section
        assert "--profile smoke" in profile_section
        assert "--profile full" in profile_section
        assert "--list-profiles --json" in profile_section
        assert "timeout/not freshly proven" in profile_section


def test_liveness_language_is_backed_by_a_temporal_property() -> None:
    """Liveness/temporal prose in a formal README must name a real checker.

    The corpus is overwhelmingly safety. Where a model folder's README makes
    an explicit temporal claim -- it uses the words "liveness", "eventually",
    or "temporal property", which denote temporal logic rather than the
    plain-English "progress"/"convergence" that safety models also use -- that
    folder must actually carry a temporal PROPERTY: at least one .cfg in the
    folder declares a ``PROPERTIES`` block that TLC checks under fairness. This
    is the guard that keeps liveness words from being decorative.
    """
    for model_dir in _direct_model_dirs():
        readme = model_dir / "README.md"
        if not readme.is_file():
            continue
        text = readme.read_text(encoding="utf-8")
        if not _TEMPORAL_CLAIM_PATTERN.search(text):
            continue

        configs_with_properties = {
            config.name: _cfg_properties(config)
            for config in sorted(model_dir.glob("*.cfg"))
            if _cfg_properties(config)
        }
        assert configs_with_properties, (
            f"{readme} uses liveness/temporal language but no .cfg in "
            f"{model_dir.name} declares a PROPERTIES block; the claim is not "
            f"backed by a temporal property TLC checks"
        )
        # The README must name at least one declared temporal property so a
        # reader can find the checker behind the liveness claim.
        declared = {
            prop for props in configs_with_properties.values() for prop in props
        }
        assert any(prop in text for prop in declared), (
            f"{readme} makes a temporal claim but does not name any of its "
            f"declared temporal properties {sorted(declared)} so the prose "
            f"cannot be traced to a checker"
        )


def test_source_sync_readiness_has_a_real_liveness_proof() -> None:
    """source_sync must carry the corpus's non-archive readiness liveness proof.

    ReadinessConvergence proves only safety (it explicitly disclaims
    liveness). ReadinessConvergenceLiveness is the companion temporal proof
    over the SAME transition structure (wave 5 replaced the retired 5-state
    ReadinessLiveness toy): budgeted disruptions, WF on the completing run,
    SF on publish/exposure, and an eventual ready-or-parked property; the
    no-fairness spec is refuted (tests/formal/test_readiness_liveness_witness.py).
    This pins the proof so a future edit cannot quietly demote source-sync
    readiness back to safety-only while keeping the convergence prose.
    """
    model_dir = FORMAL_ROOT / "source_sync"
    module = model_dir / "ReadinessConvergenceLiveness.tla"
    config = model_dir / "ReadinessConvergenceLiveness.cfg"
    assert module.is_file(), (
        "formal/source_sync/ReadinessConvergenceLiveness.tla is required"
    )
    assert config.is_file(), (
        "formal/source_sync/ReadinessConvergenceLiveness.cfg is required"
    )

    module_text = module.read_text(encoding="utf-8")
    # A real fair temporal spec: fairness conjunct plus an <>/~> property.
    assert "WF_vars(" in module_text and "SF_vars(" in module_text, (
        "ReadinessConvergenceLiveness must assume fairness (WF + SF) so its "
        "eventual-readiness property is non-vacuous"
    )
    assert "<>" in module_text, (
        "ReadinessConvergenceLiveness must declare an eventual (<>) property"
    )

    properties = _cfg_properties(config)
    assert properties, (
        "ReadinessConvergenceLiveness.cfg must declare a PROPERTIES block so "
        "TLC checks a temporal property, not only safety invariants"
    )

    readme = (model_dir / "README.md").read_text(encoding="utf-8")
    assert "ReadinessConvergenceLiveness" in readme, (
        "source_sync README must document the ReadinessConvergenceLiveness model"
    )
    assert _TEMPORAL_CLAIM_PATTERN.search(readme), (
        "source_sync README must describe the readiness liveness claim"
    )
    assert any(prop in readme for prop in properties), (
        f"source_sync README must name a ReadinessConvergenceLiveness temporal "
        f"property from {properties}"
    )
