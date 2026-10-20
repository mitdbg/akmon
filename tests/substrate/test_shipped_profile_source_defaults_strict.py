"""Every shipped profile's source defaults must survive the strict path (#2126).

The defect this closes: 25 of the 72 source defaults shipped by the five
built-in profiles declared a `source_class` that contradicted their own reader
class, or authored a `params` key the substrate injects. Nothing caught them.

Why nothing caught them, which is the point of this file:

* The scaffold's own gate, `_refuse_inadmissible_strict_registry`, runs
  `admit_source_registry_document` and claims in its docstring that "the
  admitter that runs here is the same one the runtime runs, so the two cannot
  disagree". Admission is only half the runtime gate. The class-side rules live
  in `ingest/adapter_factory.py` and need the adapter class imported, which the
  admitter never does. The gate also never fired for a shipped profile, because
  no shipped `profile.yaml` declares `source_admission_contract`.
* `okg deployment lint` and `okg doctor` read the module by AST on purpose, so
  they could not see a class attribute either.
* `guided_add._save_definition` *promotes* the registry to `strict_v1` when it
  saves a new source. The next step's `okg ingest` then plans every entry, so
  the first `okg source add --apply` on a fresh install hit one latent defect
  per run and refused to add an unrelated source 18 times in a row.
* The one test that exercised the strict path over a profile
  (`tests/substrate/cli/test_profile_strict_source_admission.py`) builds
  synthetic profiles, so the shipped ones were never run through it.

So the guard is: compose each shipped profile exactly as the scaffold does,
promote the document the way the guided flow does, and plan every runnable
entry the way `okg ingest` does. `plan_source_registry_bindings` is the whole
strict gate and constructs nothing, so this needs no database and no network.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.profile import load_profile
from okg.substrate.deployment_bootstrap.profile_init import (
    build_plan,
    planned_writes,
)
from okg.substrate.nomos import audit_deployment_policy
from okg.substrate.ingest.adapter_factory import (
    RESERVED_ADAPTER_PARAM_NAMES,
    RUNNABLE_SOURCE_PROFILE_NAMES,
    plan_source_registry_bindings,
)
from okg.substrate.sources.registry import (
    STRICT_ADMISSION_CONTRACT,
    admit_source_registry_document,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILES_ROOT = REPO_ROOT / "src/okg/substrate/library/profiles"
LIBRARY_SOURCES_ROOT = REPO_ROOT / "src/okg/substrate/library/sources"

#: Answers for every init question any shipped profile asks. Values only have
#: to be well-formed: the strict gate reads the rendered registry, it does not
#: touch a path or open a connection.
PROFILE_ANSWERS = {
    "deployment_name": "strict_guard",
    "postgres_dsn": "postgresql://okg@127.0.0.1:5999/strict_guard",
    "orcid": "0000-0002-1825-0097",
    "papers_dir": "/strict-guard/papers",
    "repo_root": "/strict-guard/repo",
    "cv_path": "/strict-guard/cv.tex",
    "advisees_dir": "/strict-guard/advisees",
    "grants_dir": "/strict-guard/grants",
    "letters_log": "/strict-guard/letters.yaml",
    "notes_dir": "/strict-guard/notes",
    "paper_cut_manifest": "/strict-guard/paper-cuts.json",
    "venues_file": "/strict-guard/venues.yaml",
    "codex_sessions_root": "/strict-guard/sessions",
    "codex_archived_sessions_root": "/strict-guard/archived-sessions",
}


def _shipped_profiles() -> list[str]:
    return sorted(
        path.parent.name
        for path in PROFILES_ROOT.glob("*/profile.yaml")
    )


def _scaffolded_documents(profile_name: str, root: Path) -> dict[str, dict]:
    """Render one shipped profile's scaffold, by file name.

    Rendering goes through `planned_writes`, the scaffold's own pure renderer,
    so the documents under test are the ones an install writes -- placeholders
    expanded, `code_repos` blocks expanded, nothing hand-built.
    """
    profile = load_profile(PROFILES_ROOT / profile_name / "profile.yaml")
    asked = {question.id for question in profile.init_questions}
    plan = build_plan(
        profile=profile,
        deployment_name="strict_guard",
        deployments_root=root,
        raw_answers={
            key: value
            for key, value in PROFILE_ANSWERS.items()
            if key in asked
        },
    )
    rendered: dict[str, dict] = {}
    for write in planned_writes(plan):
        name = Path(write.path).name
        if name in ("deployment.yaml", "source_registry.yaml"):
            assert name not in rendered, (
                f"profile {profile_name!r} renders {name} more than once"
            )
            rendered[name] = yaml.safe_load(write.text) or {}
    for name in ("deployment.yaml", "source_registry.yaml"):
        assert name in rendered, (
            f"profile {profile_name!r} renders no {name}"
        )
    return rendered


def _installable_profiles() -> list[str]:
    """The profiles an operator can actually install.

    `_test` declares `classification: fixture` and ships no
    `deployment-defaults.yaml`, so it scaffolds a manifest with no
    `nomos.deployment_posture` and the policy audit has nothing to audit.
    Filtering on the declared classification rather than the directory name
    keeps a new fixture profile out without hiding a new real one.
    """
    kept = []
    for name in _shipped_profiles():
        declared = yaml.safe_load(
            (PROFILES_ROOT / name / "profile.yaml").read_text(encoding="utf-8")
        ) or {}
        if str(declared.get("classification") or "") != "fixture":
            kept.append(name)
    assert {"academic-researcher"} <= set(kept), (
        f"the profile whose source_class this change moved is not "
        f"installable: {kept}"
    )
    return kept


def _composed_registry_document(profile_name: str, root: Path) -> dict:
    """The rendered registry, marked strict the way the guided flow marks it.

    The `admission_contract` line is added here because no shipped profile
    declares one yet and `guided_add._save_definition` adds it on the
    operator's first `okg source add`.
    """
    document = _scaffolded_documents(profile_name, root)["source_registry.yaml"]
    return {
        "admission_contract": STRICT_ADMISSION_CONTRACT,
        **{
            key: value
            for key, value in document.items()
            if key != "admission_contract"
        },
    }


@pytest.mark.parametrize("profile_name", _shipped_profiles())
def test_shipped_profile_source_defaults_pass_strict_adapter_admission(
    profile_name: str,
    tmp_path: Path,
) -> None:
    """The gate the guided add-source flow applies, applied to every default.

    One refusal in one source default blocks adding ANY source to a deployment
    scaffolded from that profile, because the flow plans the whole registry.
    So this asserts on the complete set of refusals rather than the first one:
    a fix that repairs 17 of 18 defaults leaves the operator exactly as stuck.
    """
    document = _composed_registry_document(profile_name, tmp_path)
    registry_path = tmp_path / "strict_guard" / "source_registry.yaml"

    admission = admit_source_registry_document(
        document,
        registry_path=registry_path,
    )
    assert admission.strict, (
        f"profile {profile_name!r} was not admitted under "
        f"{STRICT_ADMISSION_CONTRACT}"
    )

    refusals: dict[str, str] = {}
    for name, entry in sorted(admission.entries.items()):
        if entry.is_registry_only:
            continue
        try:
            plan_source_registry_bindings(
                admit_source_registry_document(
                    {
                        "admission_contract": STRICT_ADMISSION_CONTRACT,
                        "sources": {name: dict(document["sources"][name])},
                    },
                    registry_path=registry_path,
                ),
                dsn=PROFILE_ANSWERS["postgres_dsn"],
                deployment="strict_guard",
            )
        except (ImportError, TypeError, ValueError) as exc:
            refusals[name] = f"{type(exc).__name__}: {exc}"

    assert refusals == {}, (
        f"profile {profile_name!r} ships source defaults the guided "
        "add-source flow refuses:\n"
        + "\n".join(f"  {name}: {cause}" for name, cause in refusals.items())
    )


@pytest.mark.parametrize("profile_name", _installable_profiles())
def test_shipped_profile_scaffold_passes_the_source_policy_audit(
    profile_name: str,
    tmp_path: Path,
) -> None:
    """Reclassifying `source_class` must not trade one refusal for another.

    `nomos.source_policy_defaults.by_source_class` is keyed by the very value
    these defaults changed, so moving thirteen academic-researcher sources off
    `discovery_crawl` left them with no policy and the scaffold's own
    source-registry audit then fails with `nomos.audit.source_policy_missing`
    -- one per source, on a deployment the operator has not touched yet. The
    `mutable_api` and `reference_catalog` blocks in the academic-researcher
    `deployment-defaults.yaml` are what keep this green, and
    without this test nothing notices if one is deleted: the strict-admission
    guard above reads the registry, never the manifest.

    Both documents come from `planned_writes`, so this is the pair an install
    writes, not a hand-built fixture.
    """
    documents = _scaffolded_documents(profile_name, tmp_path)

    audit = audit_deployment_policy(
        documents["deployment.yaml"],
        source_registry=documents["source_registry.yaml"],
        deployment=profile_name,
    )

    missing = [
        finding for finding in audit["findings"]
        if finding.get("code") == "nomos.audit.source_policy_missing"
    ]
    assert missing == [], (
        f"profile {profile_name!r} scaffolds sources with no "
        "nomos.source_policy_defaults entry: "
        + ", ".join(
            str(finding.get("source_name")) for finding in missing
        )
    )
    assert audit["ok"] is True, (
        f"profile {profile_name!r} scaffolds a deployment whose source-policy "
        "audit fails:\n"
        + "\n".join(
            f"  {finding.get('code')}: {finding.get('path')}"
            for finding in audit["findings"]
        )
    )
    # A policy block that covered nothing would satisfy the two assertions
    # above by vacuum, so pin that every runnable source resolved one.
    runnable = {
        name for name, body in
        (documents["source_registry.yaml"].get("sources") or {}).items()
        if isinstance(body, dict)
        and body.get("source_class") != "registry_only"
    }
    assert set(audit["source_policies"]) >= runnable, (
        f"profile {profile_name!r} resolved no policy for "
        f"{sorted(runnable - set(audit['source_policies']))}"
    )


@pytest.mark.parametrize("profile_name", _shipped_profiles())
def test_shipped_profile_source_defaults_author_no_reserved_params(
    profile_name: str,
    tmp_path: Path,
) -> None:
    """Named separately from the plan so the whole reserved set is reported.

    The plan stops at the first refusal per source, and the class-profile check
    runs before the params check, so a source with both defects reports only
    the first. This states the params rule over every default at once.
    """
    document = _composed_registry_document(profile_name, tmp_path)
    authored = {
        f"{name}.{key}": name
        for name, body in (document.get("sources") or {}).items()
        if isinstance(body, dict) and isinstance(body.get("params"), dict)
        for key in body["params"]
        if key in RESERVED_ADAPTER_PARAM_NAMES
    }
    assert authored == {}, (
        f"profile {profile_name!r} authors substrate-injected params "
        f"(reserved: {sorted(RESERVED_ADAPTER_PARAM_NAMES)}): "
        f"{sorted(authored)}"
    )


def test_every_library_source_class_declares_a_real_profile() -> None:
    """A reader whose `profile` is not in the matrix can never be admitted.

    Three library readers declared `profile = "artifact_import"`, a string that
    is in no profile the substrate defines, so `source_class_profile_invalid`
    refused them whatever their registry entry said. Two of the three shipped
    in `workspace-graph`; the other two were one registry entry away from the
    same refusal, which is why this reads the classes rather than the defaults.

    Read by import: these are first-party library modules, so unlike
    `okg deployment lint` -- which stays AST-only because it inspects a
    deployment's own Python -- this test may load them.
    """
    import importlib
    import inspect

    offenders: dict[str, str] = {}
    for path in sorted(LIBRARY_SOURCES_ROOT.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        module_name = (
            "okg.substrate.library.sources."
            + ".".join(path.relative_to(LIBRARY_SOURCES_ROOT).with_suffix("").parts)
        )
        try:
            module = importlib.import_module(module_name)
        except Exception:  # noqa: BLE001 - an unimportable module is not this
            continue          # test's subject; the lint/import paths report it.
        for class_name, obj in vars(module).items():
            if not inspect.isclass(obj):
                continue
            if getattr(obj, "__module__", None) != module_name:
                continue
            declared = inspect.getattr_static(obj, "profile", None)
            if not isinstance(declared, str):
                continue
            if declared not in RUNNABLE_SOURCE_PROFILE_NAMES:
                offenders[f"{module_name}.{class_name}"] = declared

    assert offenders == {}, (
        "library source classes declare a profile outside "
        f"{sorted(RUNNABLE_SOURCE_PROFILE_NAMES)}: {offenders}"
    )
