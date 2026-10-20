"""Module providers on the catalog module path (#1179 slices 1 and 2).

Slice 1 — composition resolves each `modules:` entry through a provider
mapping keyed by owner: a bare name is an `okg`-owned built-in, an
`owner:name` entry matches exactly, and nothing falls through to a
second provider. The golden test pins every shipped profile's composed
catalog to the fingerprint it had before providers existed.

Slice 2 — one subtype declared under two owners refuses before the
composed catalog is returned, with the `duplicate_semantic_ownership`
code the typed-asset path already uses. Same-owner cross-declaration
does not refuse.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from okg.substrate import fastyaml
from okg.substrate.catalog.composition import resolve
from okg.substrate.catalog.module_providers import (
    BUILTIN_OWNER,
    CrossOwnerSubtypeCollision,
    ModuleProvider,
    ModuleProviderError,
    builtin_module_providers,
    normalize_providers,
    parse_module_ref,
    providers_from_modules_root,
    resolve_modules,
)
from okg.substrate.catalog.modules_compose import (
    DeploymentSubtypeOverlayRefused,
    compose_catalog,
)
from okg.substrate.nomos.read_masking import (
    LEGACY_REDACTION_KEYS,
    MASK_TOKEN,
    READ_DROP_CLASSES,
    mask_attrs,
    subtype_attr_classifier,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
PROFILES_DIR = REPO_ROOT / "src/okg/substrate/library/profiles"
GOLDEN_PATH = (
    REPO_ROOT / "tests/substrate/fixtures/profile_catalog_fingerprints.json"
)


# ---------- helpers ----------


def _write_module(root: Path, name: str, classes: dict[str, str]) -> None:
    """A minimal ontology module: one schema.yaml, no manifest."""
    module_dir = root / name
    module_dir.mkdir(parents=True, exist_ok=True)
    body = ["classes:"]
    for cls_name, archetype in classes.items():
        body += [
            f"  {cls_name}:",
            "    annotations:",
            f"      archetype: {archetype}",
        ]
    (module_dir / "schema.yaml").write_text("\n".join(body) + "\n")


def _deployment(tmp_path: Path, modules: list[str]) -> Path:
    root = tmp_path / "deployment"
    root.mkdir(parents=True, exist_ok=True)
    (root / "deployment.yaml").write_text(
        json.dumps({"name": "fixture", "modules": modules}),
    )
    return root


def _empty_cross_links(tmp_path: Path) -> Path:
    path = tmp_path / "cross_links"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _profile_modules(name: str) -> list[str]:
    with (PROFILES_DIR / name / "modules.yaml").open() as handle:
        return list((fastyaml.safe_load(handle) or {}).get("modules") or [])


def _catalog_fingerprint(modules: list[str], tmp_path: Path) -> str:
    """Hash everything composition derives from the module path."""
    try:
        composed = compose_catalog(_deployment(tmp_path, modules))
    except Exception as exc:  # noqa: BLE001 - a refusal is part of the golden
        return f"ERROR:{type(exc).__name__}:{exc}"
    payload = {
        "modules": list(composed.modules),
        "subtypes": sorted(composed.subtypes),
        "subtype_origin": dict(sorted(composed.subtype_origin.items())),
        "subtype_parent": dict(sorted(composed.subtype_parent.items())),
        "subtype_metadata": json.loads(
            json.dumps(composed.subtype_metadata, sort_keys=True, default=str),
        ),
        "narrowings": sorted(
            [
                row.parent_subtype,
                row.edge_archetype,
                row.child_subtype,
                row.origin,
                row.narrowing_name,
                row.cardinality or "",
                json.dumps(
                    row.entity_merge_equivalence, sort_keys=True, default=str,
                ),
            ]
            for row in composed.narrowings
        ),
        "aliases": sorted(
            json.dumps(alias, sort_keys=True) for alias in composed.aliases
        ),
        "identifier_patterns": sorted(
            json.dumps(pattern, sort_keys=True, default=str)
            for pattern in composed.identifier_patterns
        ),
        "archetype_kind_overrides": dict(
            sorted(composed.archetype_kind_overrides.items()),
        ),
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


# ---------- slice 1: reference spelling and provider resolution ----------


def test_bare_name_is_an_okg_owned_builtin() -> None:
    assert parse_module_ref("reasoning") == (BUILTIN_OWNER, "reasoning")


def test_owner_qualified_reference_splits_on_the_single_colon() -> None:
    assert parse_module_ref("archi:physics") == ("archi", "physics")


@pytest.mark.parametrize(
    "ref",
    [
        "archi:physics:extra",
        "archi:",
        ":physics",
        "archi: physics",
        "a b",
        "..",
        ".",
        "archi:..",
        "../../etc",
    ],
)
def test_loose_or_escaping_module_reference_refuses(ref: str) -> None:
    """`.` and `..` match the segment pattern but leave the provider root."""
    with pytest.raises(ModuleProviderError) as excinfo:
        parse_module_ref(ref)
    assert excinfo.value.code == "module_provider_unresolved"


def test_bare_name_never_falls_through_to_another_owner(tmp_path: Path) -> None:
    """A second provider offering the same module name is not consulted."""
    other_root = tmp_path / "archi"
    _write_module(other_root, "physics", {"Detector": "entity"})
    providers = {
        BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, tmp_path / "builtin"),
        "archi": ModuleProvider("archi", other_root, kind="package"),
    }
    resolved = resolve_modules(["physics"], providers)
    assert resolved["physics"].owner == BUILTIN_OWNER
    assert resolved["physics"].module_dir == tmp_path / "builtin" / "physics"


def test_unknown_owner_refuses_and_names_the_available_owners() -> None:
    with pytest.raises(ModuleProviderError) as excinfo:
        resolve_modules(["archi:physics"], builtin_module_providers())
    assert excinfo.value.code == "module_provider_unresolved"
    assert excinfo.value.data["owner"] == "archi"
    assert excinfo.value.data["available_owners"] == [BUILTIN_OWNER]


def test_modules_root_is_the_single_okg_provider_shim(tmp_path: Path) -> None:
    providers = providers_from_modules_root(tmp_path)
    assert set(providers) == {BUILTIN_OWNER}
    assert providers[BUILTIN_OWNER].kind == "builtin"
    assert normalize_providers(None, tmp_path) == providers


def test_providers_and_modules_root_together_refuse(tmp_path: Path) -> None:
    with pytest.raises(ModuleProviderError):
        normalize_providers(providers_from_modules_root(tmp_path), tmp_path)


def test_provider_keyed_by_a_foreign_owner_refuses(tmp_path: Path) -> None:
    with pytest.raises(ModuleProviderError):
        normalize_providers({"archi": ModuleProvider("okg", tmp_path)})


def test_compose_catalog_loads_a_second_owner_module(tmp_path: Path) -> None:
    builtin_root = tmp_path / "builtin"
    archi_root = tmp_path / "archi"
    _write_module(builtin_root, "reasoning", {"Hypothesis": "entity"})
    _write_module(archi_root, "physics", {"Detector": "entity"})
    composed = compose_catalog(
        _deployment(tmp_path, ["reasoning", "archi:physics"]),
        providers={
            BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, builtin_root),
            "archi": ModuleProvider("archi", archi_root, kind="package"),
        },
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert composed.subtypes == {"hypothesis", "detector"}
    assert composed.subtype_origin["detector"] == "archi:physics"


def test_resolver_reads_the_manifest_from_the_owning_provider(
    tmp_path: Path,
) -> None:
    archi_root = tmp_path / "archi"
    _write_module(archi_root, "physics", {"Detector": "entity"})
    (archi_root / "physics" / "module.yaml").write_text(
        "name: physics\nversion: 1.2.3\nprovides:\n  subtypes_file: schema.yaml\n",
    )
    resolved = resolve(
        ["archi:physics"],
        providers={"archi": ModuleProvider("archi", archi_root, kind="package")},
    )
    assert resolved.manifests["archi:physics"].version == "1.2.3"
    assert resolved.subtypes_by_module["archi:physics"] == {"detector"}


# ---------- slice 1: every shipped profile composes byte-identically ----------


def test_shipped_profiles_match_their_pre_provider_fingerprints(
    tmp_path: Path,
) -> None:
    """Golden generated at 1475c87d5d, before module providers landed.

    Regenerated by PACT read-model-s1-catalog-fields: the only change is new
    `title_from` / `path_from` / `last_modified_at_from` metadata keys and
    `sub_agent_session` losing two date keys naming attributes it lacks.

    A change to composition that moves any subtype, origin, narrowing,
    alias, or identifier pattern for any shipped profile fails here.
    """
    golden = json.loads(GOLDEN_PATH.read_text())
    profiles = sorted(
        path.name
        for path in PROFILES_DIR.iterdir()
        if (path / "modules.yaml").is_file()
    )
    assert profiles == sorted(golden)
    observed = {
        name: _catalog_fingerprint(_profile_modules(name), tmp_path / name)
        for name in profiles
    }
    assert observed == golden


# ---------- slice 2: cross-owner collision refusal ----------


def test_cross_owner_subtype_collision_refuses_before_any_mutation(
    tmp_path: Path,
) -> None:
    builtin_root = tmp_path / "builtin"
    archi_root = tmp_path / "archi"
    _write_module(builtin_root, "dataset", {"Table": "entity"})
    _write_module(archi_root, "physics", {"Table": "entity"})
    with pytest.raises(CrossOwnerSubtypeCollision) as excinfo:
        compose_catalog(
            _deployment(tmp_path, ["dataset", "archi:physics"]),
            providers={
                BUILTIN_OWNER: ModuleProvider(BUILTIN_OWNER, builtin_root),
                "archi": ModuleProvider("archi", archi_root, kind="package"),
            },
            cross_links_root=_empty_cross_links(tmp_path),
        )
    error = excinfo.value
    assert error.code == "duplicate_semantic_ownership"
    assert error.data["phase"] == "pre_mutation"
    assert error.data["subtype"] == "table"
    assert error.data["owners"] == ["archi", BUILTIN_OWNER]
    assert "archi" in str(error) and BUILTIN_OWNER in str(error)


def test_same_owner_cross_declaration_is_not_refused(tmp_path: Path) -> None:
    """Thirteen built-in subtype names are cross-declared; keep merging."""
    builtin_root = tmp_path / "builtin"
    _write_module(builtin_root, "scholarly", {"Table": "entity"})
    _write_module(builtin_root, "dataset", {"Table": "entity"})
    composed = compose_catalog(
        _deployment(tmp_path, ["scholarly", "dataset"]),
        modules_root=builtin_root,
        cross_links_root=_empty_cross_links(tmp_path),
    )
    assert composed.subtypes == {"table"}
    assert composed.subtype_origin["table"] == "scholarly"


@pytest.mark.parametrize(
    ("profile", "subtype"),
    [
        ("academic-researcher", "table"),
        ("academic-researcher", "review"),
    ],
)
def test_shipped_profiles_with_overlapping_builtins_still_compose(
    tmp_path: Path, profile: str, subtype: str,
) -> None:
    composed = compose_catalog(
        _deployment(tmp_path, _profile_modules(profile)),
    )
    assert subtype in composed.subtypes


# ---------- issue #2176: a deployment schema file over a library subtype ----------


_LIBRARY_PERSON = """\
classes:
  Person:
    annotations:
      archetype: entity
      okg_searchable_text_fields: name
    attributes:
      name: {}
      email:
        annotations:
          pii_class: direct_identifier
      phone:
        annotations:
          pii_class: quasi_identifier
"""


def _compose_with_overlay(tmp_path: Path, overlay: str):
    library_root = tmp_path / "library"
    (library_root / "people").mkdir(parents=True)
    (library_root / "people" / "schema.yaml").write_text(_LIBRARY_PERSON)
    deployment = _deployment(tmp_path, ["people"])
    (deployment / "schemas").mkdir()
    (deployment / "schemas" / "person_overlay.yaml").write_text(overlay)
    return compose_catalog(
        deployment,
        modules_root=library_root,
        cross_links_root=_empty_cross_links(tmp_path),
    )


def _person_overlay(annotations: str, attributes: str = "") -> str:
    body = "classes:\n  Person:\n    annotations:\n      archetype: entity\n"
    body += annotations
    if attributes:
        body += "    attributes:\n" + attributes
    return body


_SSN = (
    "      ssn:\n"
    "        annotations:\n"
    "          pii_class: sensitive_attribute\n"
)


def test_overlay_redeclaring_pii_without_extends_is_refused(
    tmp_path: Path,
) -> None:
    """Before #2176 this silently replaced the library's whole PII map."""
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(tmp_path, _person_overlay("", _SSN))
    error = excinfo.value
    assert error.code == "deployment_subtype_overlay_refused"
    assert error.data["reason"] == "masking_without_extends"
    assert error.data["subtype"] == "person"
    assert error.data["key"] == "pii_classes"
    assert error.data["library_origin"] == "people"
    assert error.data["schema_file"].endswith("schemas/person_overlay.yaml")
    assert "okg_extends: people" in error.fix_hint


def test_overlay_with_extends_keeps_library_pii_and_adds_its_own(
    tmp_path: Path,
) -> None:
    composed = _compose_with_overlay(
        tmp_path, _person_overlay("      okg_extends: people\n", _SSN),
    )
    assert composed.subtype_metadata["person"]["pii_classes"] == {
        "email": "direct_identifier",
        "phone": "quasi_identifier",
        "ssn": "sensitive_attribute",
    }


def test_overlay_records_what_the_deployment_contributed(
    tmp_path: Path,
) -> None:
    composed = _compose_with_overlay(
        tmp_path,
        _person_overlay(
            "      okg_extends: people\n"
            "      okg_searchable_text_fields: name,nickname\n"
            "      okg_volatile_fields: seen_at\n",
            _SSN,
        ),
    )
    # The owner stays the library module; the deployment's part is recorded
    # per key, down to the entries it added.
    assert composed.subtype_origin["person"] == "people"
    assert composed.subtype_metadata["person"]["searchable_text_fields"] == [
        "name", "nickname",
    ]
    overlays = composed.subtype_overlays["person"]
    assert overlays["pii_classes"] == [{
        "origin": "deployment:fixture",
        "schema_file": "person_overlay.yaml",
        "extends": "people",
        "entries": ["ssn"],
    }]
    assert overlays["searchable_text_fields"][0]["entries"] == ["nickname"]
    assert overlays["volatile_fields"][0]["entries"] is None
    assert set(overlays) == {
        "pii_classes", "searchable_text_fields", "volatile_fields",
    }


def test_overlay_with_extends_cannot_weaken_a_library_pii_entry(
    tmp_path: Path,
) -> None:
    weaken = (
        "      email:\n"
        "        annotations:\n"
        "          pii_class: non_pii\n"
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(
            tmp_path, _person_overlay("      okg_extends: people\n", weaken),
        )
    error = excinfo.value
    assert error.data["reason"] == "entry_conflict"
    assert error.data["key"] == "pii_classes"
    assert error.data["entry"] == "email"
    assert error.data["library_value"] == "direct_identifier"
    assert error.data["file_value"] == "non_pii"


def test_overlay_with_extends_refuses_a_different_single_value(
    tmp_path: Path,
) -> None:
    overlay = (
        "classes:\n  Person:\n    annotations:\n"
        "      archetype: resource\n      okg_extends: people\n"
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(tmp_path, overlay)
    assert excinfo.value.data["reason"] == "value_conflict"
    assert excinfo.value.data["key"] == "archetype"


def test_overlay_extends_must_name_the_declaring_module(tmp_path: Path) -> None:
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(
            tmp_path, _person_overlay("      okg_extends: compute_env\n", _SSN),
        )
    assert excinfo.value.data["reason"] == "extends_target_invalid"
    assert excinfo.value.data["library_modules"] == ["people"]


def test_overlay_extends_without_archetype_is_refused(tmp_path: Path) -> None:
    """A marked class with no archetype would be skipped whole, masking included."""
    overlay = (
        "classes:\n  Person:\n    annotations:\n      okg_extends: people\n"
        "    attributes:\n" + _SSN
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(tmp_path, overlay)
    assert excinfo.value.data["reason"] == "extends_target_invalid"
    assert excinfo.value.data["subtype"] == "person"


def test_overlay_adding_only_unset_keys_needs_no_marker(tmp_path: Path) -> None:
    """The live-http-ci fixture's shape: restate archetype, add new keys."""
    composed = _compose_with_overlay(
        tmp_path, _person_overlay("      okg_alias_fields: nickname\n"),
    )
    metadata = composed.subtype_metadata["person"]
    assert metadata["alias_fields"] == ["nickname"]
    assert metadata["pii_classes"] == {
        "email": "direct_identifier",
        "phone": "quasi_identifier",
    }
    assert composed.subtype_overlays["person"] == {
        "alias_fields": [{
            "origin": "deployment:fixture",
            "schema_file": "person_overlay.yaml",
            "extends": None,
            "entries": None,
        }],
    }


# ---------- #2176 review: masking is never loosened ----------


_LIBRARY_MESSAGE = """\
classes:
  Message:
    annotations:
      archetype: resource
    attributes:
      content: {}
      title: {}
"""


def _compose_files(
    tmp_path: Path, files: dict[str, str], *, library: dict[str, str],
):
    """Compose a deployment with several schema files over a small library."""
    library_root = tmp_path / "library"
    for module, schema in library.items():
        (library_root / module).mkdir(parents=True)
        (library_root / module / "schema.yaml").write_text(schema)
    deployment = _deployment(tmp_path, sorted(library))
    (deployment / "schemas").mkdir()
    for name, text in files.items():
        (deployment / "schemas" / name).write_text(text)
    return compose_catalog(
        deployment,
        modules_root=library_root,
        cross_links_root=_empty_cross_links(tmp_path),
    )


def _message_overlay(annotations: str, attributes: str) -> str:
    return (
        "classes:\n  Message:\n    annotations:\n      archetype: resource\n"
        + annotations + "    attributes:\n" + attributes
    )


_CONTENT_NON_PII = (
    "      content:\n"
    "        annotations:\n"
    "          pii_class: non_pii\n"
)


def test_overlay_redeclaring_a_non_pii_key_without_extends_is_refused(
    tmp_path: Path,
) -> None:
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(
            tmp_path,
            _person_overlay("      okg_searchable_text_fields: nickname\n"),
        )
    error = excinfo.value
    assert error.data["reason"] == "redeclared_without_extends"
    assert error.data["key"] == "searchable_text_fields"
    assert error.data["conflict_source"] == "people"
    assert "okg_extends: people" in error.fix_hint


def test_overlay_revealing_a_default_masked_attribute_without_marker_is_refused(
    tmp_path: Path,
) -> None:
    """Library leaves pii_classes unset, so reads drop `content` by default."""
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_files(
            tmp_path,
            {"message.yaml": _message_overlay("", _CONTENT_NON_PII)},
            library={"chat": _LIBRARY_MESSAGE},
        )
    error = excinfo.value
    assert error.data["reason"] == "masking_without_extends"
    assert error.data["key"] == "pii_classes"
    assert error.data["conflict_source"] == "chat"
    assert error.data["conflict_schema_file"] is None
    assert "okg_extends: chat" in error.fix_hint


def test_overlay_revealing_a_default_masked_attribute_with_marker_is_refused(
    tmp_path: Path,
) -> None:
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_files(
            tmp_path,
            {"message.yaml": _message_overlay(
                "      okg_extends: chat\n", _CONTENT_NON_PII,
            )},
            library={"chat": _LIBRARY_MESSAGE},
        )
    error = excinfo.value
    assert error.data["reason"] == "masking_loosened"
    assert error.data["entry"] == "content"
    assert error.data["library_value"] == "free_text"
    assert error.data["file_value"] == "non_pii"


def test_overlay_with_marker_cannot_reveal_an_unlisted_attribute(
    tmp_path: Path,
) -> None:
    """Library Person lists email and phone; unlisted `name` reads masked."""
    reveal_name = (
        "      name:\n"
        "        annotations:\n"
        "          pii_class: non_pii\n"
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(
            tmp_path, _person_overlay("      okg_extends: people\n", reveal_name),
        )
    error = excinfo.value
    assert error.data["reason"] == "masking_loosened"
    assert error.data["entry"] == "name"
    assert error.data["library_value"] == "sensitive_attribute"
    assert error.data["file_value"] == "non_pii"


def test_overlay_with_marker_cannot_pseudonymize_an_unlisted_attribute(
    tmp_path: Path,
) -> None:
    """Pseudonymized reads are weaker than masked ones."""
    name_identifier = (
        "      name:\n"
        "        annotations:\n"
        "          pii_class: direct_identifier\n"
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_with_overlay(
            tmp_path,
            _person_overlay("      okg_extends: people\n", name_identifier),
        )
    assert excinfo.value.data["reason"] == "masking_loosened"
    assert excinfo.value.data["entry"] == "name"


def test_overlay_with_marker_may_add_stricter_masking(tmp_path: Path) -> None:
    """`title` is revealed by default; the deployment masks it."""
    mask_title = (
        "      title:\n"
        "        annotations:\n"
        "          pii_class: sensitive_attribute\n"
    )
    composed = _compose_files(
        tmp_path,
        {"message.yaml": _message_overlay("      okg_extends: chat\n", mask_title)},
        library={"chat": _LIBRARY_MESSAGE},
    )
    assert composed.subtype_metadata["message"]["pii_classes"] == {
        **{attr: "free_text" for attr in LEGACY_REDACTION_KEYS},
        "title": "sensitive_attribute",
    }
    assert composed.subtype_overlays["message"]["pii_classes"] == [{
        "origin": "deployment:fixture",
        "schema_file": "message.yaml",
        "extends": "chat",
        "entries": ["title"],
        "library_defaults": sorted(LEGACY_REDACTION_KEYS),
    }]


def _read_message(composed, attrs: dict[str, str]) -> dict[str, object]:
    """Apply the read filter's redact transform to one Message row's attrs.

    Mirrors `read_filter._filter_rows_evaluate`: the subtype's composed
    `pii_classes` (None when unset) through `subtype_attr_classifier`, then
    `mask_attrs` with `READ_DROP_CLASSES`.
    """
    pii_classes = composed.subtype_metadata.get("message", {}).get("pii_classes")
    return mask_attrs(
        attrs,
        classifier=subtype_attr_classifier(pii_classes),
        drop_classes=READ_DROP_CLASSES,
    )


def test_overlay_first_pii_classes_keeps_default_dropped_attributes_dropped(
    tmp_path: Path,
) -> None:
    """Library Message has no pii_classes, so reads drop `content` and the
    other default-dropped attributes. A marked deployment file that adds the
    first pii_classes (masking `title`) must not turn those drops into mask
    tokens, which would show the attribute names."""
    mask_title = (
        "      title:\n"
        "        annotations:\n"
        "          pii_class: sensitive_attribute\n"
    )
    library_only = _compose_files(
        tmp_path / "before", {}, library={"chat": _LIBRARY_MESSAGE},
    )
    composed = _compose_files(
        tmp_path / "after",
        {"message.yaml": _message_overlay("      okg_extends: chat\n", mask_title)},
        library={"chat": _LIBRARY_MESSAGE},
    )
    row = {attr: f"secret {attr}" for attr in LEGACY_REDACTION_KEYS}
    row.update({"title": "a title", "status": "open"})

    before = _read_message(library_only, row)
    after = _read_message(composed, row)
    for attr in LEGACY_REDACTION_KEYS:
        assert attr not in before, attr
        assert attr not in after, attr
    assert before["title"] == "a title"
    assert after["title"] == MASK_TOKEN
    assert after["status"] == MASK_TOKEN


@pytest.mark.parametrize("pii_class", ["sensitive_attribute", "direct_identifier"])
def test_overlay_first_pii_classes_cannot_mask_a_default_dropped_attribute(
    tmp_path: Path, pii_class: str,
) -> None:
    """A mask token (or pseudonym) shows the name of an attribute reads drop."""
    content_masked = (
        "      content:\n"
        "        annotations:\n"
        f"          pii_class: {pii_class}\n"
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_files(
            tmp_path,
            {"message.yaml": _message_overlay(
                "      okg_extends: chat\n", content_masked,
            )},
            library={"chat": _LIBRARY_MESSAGE},
        )
    error = excinfo.value
    assert error.data["reason"] == "masking_loosened"
    assert error.data["entry"] == "content"
    assert error.data["library_value"] == "free_text"
    assert error.data["file_value"] == pii_class


def test_overlay_first_pii_classes_may_classify_content_free_text(
    tmp_path: Path,
) -> None:
    content_free_text = (
        "      content:\n"
        "        annotations:\n"
        "          pii_class: free_text\n"
    )
    composed = _compose_files(
        tmp_path,
        {"message.yaml": _message_overlay(
            "      okg_extends: chat\n", content_free_text,
        )},
        library={"chat": _LIBRARY_MESSAGE},
    )
    rows = composed.subtype_overlays["message"]["pii_classes"]
    assert rows[0]["entries"] == ["content"]
    assert rows[0]["library_defaults"] == sorted(
        LEGACY_REDACTION_KEYS - {"content"}
    )
    read = _read_message(composed, {"content": "secret", "text": "secret"})
    assert "content" not in read
    assert "text" not in read


def test_overlay_provenance_lists_every_deployment_file(tmp_path: Path) -> None:
    ssn = _person_overlay("      okg_extends: people\n", _SSN)
    passport = _person_overlay(
        "      okg_extends: people\n",
        "      passport:\n"
        "        annotations:\n"
        "          pii_class: sensitive_attribute\n",
    )
    composed = _compose_files(
        tmp_path,
        {"a_ssn.yaml": ssn, "b_passport.yaml": passport},
        library={"people": _LIBRARY_PERSON},
    )
    assert composed.subtype_metadata["person"]["pii_classes"] == {
        "email": "direct_identifier",
        "phone": "quasi_identifier",
        "ssn": "sensitive_attribute",
        "passport": "sensitive_attribute",
    }
    rows = composed.subtype_overlays["person"]["pii_classes"]
    assert [(row["schema_file"], row["entries"]) for row in rows] == [
        ("a_ssn.yaml", ["ssn"]),
        ("b_passport.yaml", ["passport"]),
    ]


def test_overlay_conflict_between_deployment_files_names_the_earlier_file(
    tmp_path: Path,
) -> None:
    ssn = _person_overlay("      okg_extends: people\n", _SSN)
    ssn_free_text = _person_overlay(
        "      okg_extends: people\n",
        "      ssn:\n"
        "        annotations:\n"
        "          pii_class: free_text\n",
    )
    with pytest.raises(DeploymentSubtypeOverlayRefused) as excinfo:
        _compose_files(
            tmp_path,
            {"a_ssn.yaml": ssn, "b_ssn.yaml": ssn_free_text},
            library={"people": _LIBRARY_PERSON},
        )
    error = excinfo.value
    assert error.data["reason"] == "entry_conflict"
    assert error.data["entry"] == "ssn"
    assert error.data["schema_file"].endswith("schemas/b_ssn.yaml")
    assert error.data["conflict_source"] == "deployment:fixture"
    assert error.data["conflict_schema_file"] == "a_ssn.yaml"
    assert "a_ssn.yaml" in str(error)
    assert "a_ssn.yaml" in error.fix_hint
    assert "library module" not in error.fix_hint
