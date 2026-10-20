"""Connector registration metadata (#1181 slice 4).

The load-bearing test is the first one: an entry built from a
`ConnectorRegistration` and the same entry written by hand as YAML must
admit to the *same* `SourceEntry` under the landed strict admission. If
the mapping ever drifts from what an author would write, that test fails
rather than a packaged connector failing at install time.
"""
from __future__ import annotations

from types import MappingProxyType

import pytest

from okg.substrate import fastyaml
from okg.substrate.sources.registry import admit_source_registry_document

import okg.deployment as deployment


def _thaw(value):
    """Detach the registry's frozen views into plain comparable JSON."""
    if isinstance(value, (dict, MappingProxyType)):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw(item) for item in value]
    return value


def _registration(**overrides):
    kwargs = dict(
        connector_type="twiki_eos",
        version="2.1.0",
        module="archi.connectors.twiki",
        class_name="TwikiEosConnector",
        ownership_id="archi.twiki",
        source_class="discovery_crawl",
        trust_label="archi_operated",
        sdk_range=deployment.SdkVersionRange("1.0.0", "2.0.0"),
        config_schema={
            "type": "object",
            "properties": {"root": {"type": "string"}},
            "required": ["root"],
        },
        auth_modes=("env_credential",),
        capabilities=("checkpoint_resume", "record_set_authority"),
        required_modules=("archi:detector",),
        output_signature={"nodes": [{"subtype": "Document"}]},
        sync={"triggers": "poll", "poll": {"interval_seconds": 900}},
        credential_refs=("ARCHI_TWIKI_TOKEN",),
        record_identity_kind="scoped_locator",
        record_identity_fields=("path",),
        source_revision_kind="content_hash",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        description="Archi TWiki crawl",
    )
    kwargs.update(overrides)
    return deployment.ConnectorRegistration(**kwargs)


# The same connector as a human would author it in source_registry.yaml.
# Deliberately written as text, not as the mapping function's output, so
# the comparison below is a real one.
HAND_WRITTEN = """
admission_contract: strict_v1
sources:
  twiki_eos:
    module: archi.connectors.twiki
    class: TwikiEosConnector
    ownership_id: archi.twiki
    source_class: discovery_crawl
    record_identity_kind: scoped_locator
    record_identity_fields: [path]
    source_revision_kind: content_hash
    deletion_semantics: missing_from_completed_scope
    publication_mode: published_generation
    description: Archi TWiki crawl
    credential_refs: [ARCHI_TWIKI_TOKEN]
    sync:
      triggers: poll
      poll:
        interval_seconds: 900
    admission_policy:
      producer_id: twiki_eos
      producer_kind: source
      trust_label: archi_operated
      admission_mode: direct
      output_signature:
        nodes:
          - subtype: Document
    configuration_hints:
      okg_connector:
        connector_type: twiki_eos
        connector_version: "2.1.0"
        sdk_min_version: "1.0.0"
        sdk_max_version_exclusive: "2.0.0"
        auth_modes: [env_credential]
        capabilities: [checkpoint_resume, record_set_authority]
        required_modules: ["archi:detector"]
        config_schema:
          type: object
          properties:
            root:
              type: string
          required: [root]
"""


def _admit(document, tmp_path):
    return admit_source_registry_document(
        document, registry_path=tmp_path / "source_registry.yaml",
    )


def _projection(entry):
    """Everything admission decided about one entry, as plain JSON."""
    return {
        "ownership_id": entry.ownership_id,
        "source_class": entry.source_class,
        "record_identity_kind": entry.record_identity_kind,
        "record_identity_fields": list(entry.record_identity_fields),
        "source_revision_kind": entry.source_revision_kind,
        "deletion_semantics": entry.deletion_semantics,
        "publication_mode": entry.publication_mode,
        "credential_refs": list(entry.credential_refs),
        "record_authority_format": entry.record_authority_format,
        "admission_contract": entry.admission_contract,
        "admission_policy": entry.admission_policy.as_dict(),
        "raw": _thaw(entry.raw),
    }


def test_generated_entry_admits_like_a_hand_written_one(tmp_path):
    generated_document = {
        "admission_contract": "strict_v1",
        "sources": {
            "twiki_eos": deployment.registration_registry_entry(
                _registration()
            ),
        },
    }
    generated = _admit(generated_document, tmp_path)
    hand = _admit(fastyaml.safe_load(HAND_WRITTEN), tmp_path)

    assert generated.strict and hand.strict
    assert set(generated.entries) == set(hand.entries) == {"twiki_eos"}
    assert (
        _projection(generated.entries["twiki_eos"])
        == _projection(hand.entries["twiki_eos"])
    )


def test_generated_entry_carries_the_declared_sdk_metadata(tmp_path):
    """The registry has no field for a connector's own version, auth
    modes, capabilities, required modules or SDK window, so they ride in
    `configuration_hints` under one namespaced key — and survive strict
    admission unchanged rather than being dropped."""
    entry = deployment.registration_registry_entry(_registration())
    admitted = _admit(
        {"admission_contract": "strict_v1", "sources": {"twiki_eos": entry}},
        tmp_path,
    )
    hints = _thaw(
        admitted.entries["twiki_eos"].raw["configuration_hints"]
    )["okg_connector"]
    assert hints["connector_version"] == "2.1.0"
    assert hints["sdk_min_version"] == "1.0.0"
    assert hints["sdk_max_version_exclusive"] == "2.0.0"
    assert hints["auth_modes"] == ["env_credential"]
    assert hints["capabilities"] == [
        "checkpoint_resume", "record_set_authority",
    ]
    assert hints["required_modules"] == ["archi:detector"]
    assert hints["config_schema"]["required"] == ["root"]


# --- Refusals ----------------------------------------------------------------


def test_unknown_capability_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(capabilities=("teleportation",))
    assert caught.value.code == "connector_capability_unknown"
    assert caught.value.data["capability"] == "teleportation"
    assert "checkpoint_resume" in caught.value.data["allowed"]


def test_unknown_auth_mode_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(auth_modes=("mtls",))
    assert caught.value.code == "connector_auth_mode_unknown"
    assert caught.value.data["auth_mode"] == "mtls"


def test_malformed_output_signature_is_refused():
    """Refused by `strict_output_signature` itself, not a restatement:
    a signature node without `subtype` is what strict admission rejects."""
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(output_signature={"nodes": [{"id_prefix": "doc:"}]})
    assert caught.value.code == "connector_output_signature_invalid"
    assert caught.value.data["field"] == "output_signature"


def test_malformed_sync_block_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(sync={"triggers": "telepathy"})
    assert caught.value.code == "connector_output_signature_invalid"
    assert caught.value.data["field"] == "sync"


@pytest.mark.parametrize(
    "module_id", ["detector", "archi:sub:detector", ":detector", "archi:"],
)
def test_non_strict_required_module_id_is_refused(module_id):
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(required_modules=(module_id,))
    assert caught.value.code == "connector_required_module_unqualified"
    assert caught.value.data["module_id"] == module_id


def test_invalid_config_schema_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        _registration(config_schema={"type": "not-a-json-schema-type"})
    assert caught.value.code == "connector_config_schema_invalid"


def _live_sdk_module():
    """The package object `okg.deployment` resolves to right now.

    `test_connector_protocol.py` evicts `okg.deployment` from `sys.modules`
    to prove the lazy cache, so this module's import-time reference can be a
    stale object. Patching a stale object would patch a version nothing
    reads.
    """
    import importlib

    return importlib.import_module("okg.deployment")


def test_sdk_range_satisfied_by_the_running_version(monkeypatch):
    monkeypatch.setattr(_live_sdk_module(), "__version__", "1.4.0")
    entry = deployment.registration_registry_entry(_registration())
    assert entry["configuration_hints"]["okg_connector"][
        "sdk_min_version"
    ] == "1.0.0"


def test_sdk_range_unsatisfied_by_the_running_version(monkeypatch):
    monkeypatch.setattr(_live_sdk_module(), "__version__", "2.0.0")
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        deployment.registration_registry_entry(_registration())
    assert caught.value.code == "connector_sdk_version_unsupported"
    assert caught.value.data["sdk_version"] == "2.0.0"
    assert caught.value.data["max_version_exclusive"] == "2.0.0"


def test_a_prerelease_build_is_compared_by_its_base_release(monkeypatch):
    """`1.0.0rc2` must not be refused by `min_version: 1.0.0` — the very
    build carrying the feature. #1179's rule, kept here."""
    monkeypatch.setattr(_live_sdk_module(), "__version__", "1.0.0rc2")
    assert deployment.registration_registry_entry(_registration())


def test_deprecated_connector_is_refused_without_the_allow_flag():
    registration = _registration(deprecation="superseded by twiki_eos_v3")
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        deployment.registration_registry_entry(registration)
    assert caught.value.code == "connector_registration_deprecated"
    assert caught.value.data["deprecation"] == "superseded by twiki_eos_v3"


def test_deprecated_connector_is_admitted_with_the_allow_flag():
    registration = _registration(deprecation="superseded by twiki_eos_v3")
    entry = deployment.registration_registry_entry(
        registration, allow_deprecated=True,
    )
    hints = entry["configuration_hints"]["okg_connector"]
    assert hints["deprecation"] == "superseded by twiki_eos_v3"


def test_an_inverted_sdk_window_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError):
        deployment.SdkVersionRange("2.0.0", "1.0.0")


# --- Declaration and discovery ----------------------------------------------


def test_a_registration_round_trips_through_a_declared_asset():
    """Discovery is declaration: a package ships the registration as a
    package-v2 `connector` asset. Nothing is imported to find it."""
    registration = _registration()
    payload = registration.as_asset_payload()
    assert payload["kind"] == "connector"
    assert payload["schema"] == "okg.deployment_product.asset.connector/v1"
    assert (
        deployment.ConnectorRegistration.from_asset_payload(payload)
        == registration
    )


def test_a_foreign_asset_schema_is_refused():
    with pytest.raises(deployment.ConnectorRegistrationError):
        deployment.ConnectorRegistration.from_asset_payload(
            {"schema": "okg.deployment_product.asset.enricher/v1"}
        )


def test_every_declarable_capability_names_a_landed_runner_input():
    """The capability vocabulary is closed *because* each member is a
    field the landed protocol already carries. A capability with nothing
    behind it would be a promise the runner cannot keep."""
    context_fields = set(deployment.ConnectorContext.__dataclass_fields__)
    run_fields = set(deployment.ConnectorRun.__dataclass_fields__)
    backing = {
        "authoritative_replay": "authoritative_replay" in context_fields,
        "cancellation": hasattr(deployment.ConnectorContext, "cancelled"),
        "change_probe": "change_probe_verdict" in context_fields,
        "checkpoint_resume": "next_checkpoint" in run_fields,
        "partial_health": "partial" in deployment.HEALTH_STATUSES,
        "progress_fingerprints": "since_progress" in context_fields,
        "record_authority_stream": "record_authority_stream" in run_fields,
        "record_set_authority": "record_set" in run_fields,
        "scoped_replacement": "record_set_replacement_keys" in run_fields,
        "scoped_run": "sync_scope" in context_fields,
    }
    assert set(backing) == set(deployment.CONNECTOR_CAPABILITIES)
    assert [name for name, ok in backing.items() if not ok] == []


def test_every_declarable_auth_mode_names_a_landed_mechanism():
    """Each auth mode is checked against the mechanism it names.

    Restating the same five strings the constant holds would only prove
    the constant equals itself, so every member is bound to the landed
    callable that implements it. `none` is the absence of custody, so it
    is bound to the observable consequence: a registration declaring it
    produces a registry entry carrying no credential reference.
    """
    from okg.substrate.library.oauth import flow, static_token

    entry = deployment.registration_registry_entry(
        _registration(auth_modes=("none",), credential_refs=())
    )
    backing = {
        "none": "credential_refs" not in entry,
        "env_credential": callable(deployment.credential_preflight),
        "file_credential": callable(deployment.file_preflight),
        "keychain_token": callable(static_token.get_static_token)
        and callable(static_token.set_static_token),
        "oauth2_authorization_code": callable(flow.OAuthFlow.start),
    }
    assert set(backing) == set(deployment.AUTH_MODES)
    assert [name for name, ok in backing.items() if not ok] == []


# --- Reading a declaration back (#1181 slice 8) ------------------------------


def test_a_malformed_asset_body_refuses_with_a_code():
    """A bare `TypeError` out of the dataclass constructor was catchable
    only as `TypeError` and appeared in no error registry."""
    payload = _registration().as_asset_payload()
    payload["registration"]["retry_budget"] = 3
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        deployment.ConnectorRegistration.from_asset_payload(payload)
    assert caught.value.code == "connector_asset_payload_invalid"
    assert "retry_budget" in caught.value.data["declared_fields"]


@pytest.mark.parametrize(
    "bound", ["1.0.0rc1", "1.0.0.post1", "1.0.0.dev3", "1!1.0.0", "1.0.0+lab"]
)
def test_a_non_release_sdk_bound_is_refused_not_normalized(bound):
    """The window is compared base-release to base-release, so a bound
    carrying a pre/post/dev/local/epoch segment could never change a
    verdict; package v2 refuses one on `okg_compatibility` for the same
    reason."""
    with pytest.raises(deployment.ConnectorRegistrationError) as caught:
        deployment.SdkVersionRange(bound, "9.0.0")
    assert caught.value.code == "connector_sdk_bound_invalid"
    assert caught.value.data["field"] == "min_version"


def test_runnable_example_registration_and_schema_are_complete(tmp_path):
    """Validate the actual author example against production registry/catalog readers."""
    import importlib.util
    from pathlib import Path

    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.producer_authority import normalize_trust_label

    package = Path(__file__).resolve().parents[3] / (
        "examples/json-record-connector/src/okg_example_records"
    )
    spec = importlib.util.spec_from_file_location("example_registration_under_test", package / "reader.py")
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    declared = deployment.ConnectorRegistration.from_asset_payload(
        reader.REGISTRATION.as_asset_payload(),
    )
    assert normalize_trust_label(declared.trust_label) == declared.trust_label
    row = deployment.registration_registry_entry(declared)
    row["params"] = {"path": str(tmp_path / "records.json")}
    registry = _admit({"admission_contract": "strict_v1",
                       "sources": {declared.connector_type: row}}, tmp_path)
    entry = registry.entries[declared.connector_type]
    assert registry.strict
    assert entry.source_class == reader.RecordSource.profile
    assert entry.ownership_id == "example.records"
    assert entry.deletion_semantics == "missing_from_completed_scope"
    assert entry.raw["module"] == "okg_example_records.reader"
    assert entry.raw["class"] == "RecordSource"
    catalog = compose_catalog(package / "deployment")
    assert catalog.subtypes == {"example_collection", "example_record"}
    # An installed deployment's publication read-back refuses a generation with
    # no edges (install_execution.py:251-252), so the example declares the
    # containment relationship it really emits.
    assert declared.output_signature == {
        "nodes": [{"subtype": "example_collection"}, {"subtype": "example_record"}],
        "edges": [{
            "src_subtype": "example_collection",
            "edge_type": "contains",
            "dst_subtype": "example_record",
        }],
    }
    # A registry path is configuration, never a database/authority parameter.
    from inspect import signature

    assert set(signature(reader.RecordSource).parameters) == {"path"}


def _okg_imports_in(source):
    import ast

    imports = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def _example_okg_imports(path):
    return _okg_imports_in(path.read_text())


def test_runnable_example_code_uses_only_public_okg_imports():
    """The installed runtime modules stay on the public SDK.

    Scoped to the modules that run inside a deployment. `package.py` is a
    build-time tool and is checked separately below: it legitimately calls the
    public distribution producer, which is not part of `okg.deployment`.
    """
    from pathlib import Path

    package = Path(__file__).resolve().parents[3] / (
        "examples/json-record-connector/src/okg_example_records"
    )
    runtime = ("reader.py", "setup.py", "conformance.py")
    imports = []
    for name in runtime:
        imports.extend(_example_okg_imports(package / name))
    assert all(name == "okg.deployment" or name.startswith("okg.deployment.")
               for name in imports if name == "okg" or name.startswith("okg."))
    assert all("._internal" not in name for name in imports)


def test_runnable_example_packaging_uses_only_public_producer_imports():
    """The packaging module may reach the public producer, and nothing further."""
    from pathlib import Path

    package = Path(__file__).resolve().parents[3] / (
        "examples/json-record-connector/src/okg_example_records"
    )
    imports = _example_okg_imports(package / "package.py")
    okg_imports = [
        name for name in imports if name == "okg" or name.startswith("okg.")
    ]
    assert okg_imports
    assert all(
        name in ("okg.deployment", "okg.distributions")
        or name.startswith("okg.deployment.")
        or name.startswith("okg.distributions.")
        for name in okg_imports
    )
    assert all("._internal" not in name for name in imports)
    assert not any(name.startswith("okg.substrate") for name in imports)


def test_runnable_example_readme_recipe_uses_only_public_okg_imports():
    """The recipe an external author copies stays on the public SDK.

    The README's one Python block assembles the pinned release entry. It was
    the last step of this example to import `okg.substrate`, until #1953
    exported the producer as `okg.distributions.build_release_entry`.
    """
    import re
    from pathlib import Path

    readme = Path(__file__).resolve().parents[3] / (
        "examples/json-record-connector/README.md"
    )
    blocks = re.findall(r"^```python\n(.*?)^```", readme.read_text(), re.S | re.M)
    assert blocks, "the recipe no longer carries a Python block"
    imports = []
    for block in blocks:
        imports.extend(_okg_imports_in(block))
    okg_imports = [
        name for name in imports if name == "okg" or name.startswith("okg.")
    ]
    assert okg_imports
    assert all(
        name in ("okg.deployment", "okg.distributions")
        or name.startswith("okg.deployment.")
        or name.startswith("okg.distributions.")
        for name in okg_imports
    )
    assert all("._internal" not in name for name in imports)
    assert not any(name.startswith("okg.substrate") for name in imports)
    assert "build_release_entry(" in "\n".join(blocks)


def test_runnable_example_names_no_okg_release():
    """A version bump edits only okg's pyproject.toml and uv.lock.

    The example builds okg from the checkout, so it must follow whatever
    version that checkout declares: no exact `okg==` pin, no okg wheel file
    named by version, and an okg range equal to the release window its
    distribution package declares.
    """
    import re
    import tomllib
    from pathlib import Path

    example = Path(__file__).resolve().parents[3] / "examples/json-record-connector"
    project = tomllib.loads((example / "pyproject.toml").read_text())["project"]
    okg_requirements = [
        item for item in project["dependencies"]
        if re.match(r"okg\b(?![-_])", item)
    ]
    package = (example / "src/okg_example_records/package.py").read_text()
    window = dict(re.findall(
        r'^(OKG_MIN_VERSION|OKG_MAX_VERSION_EXCLUSIVE) = "([^"]+)"$',
        package,
        re.M,
    ))
    assert okg_requirements == [
        f"okg>={window['OKG_MIN_VERSION']},<{window['OKG_MAX_VERSION_EXCLUSIVE']}"
    ]

    pinned = re.compile(r"okg==|okg-\d[^\s\"']*\.whl|\d+\.\d+\.\d+ today")
    offenders = [
        f"{path.relative_to(example)}:{number}"
        for path in sorted(example.rglob("*"))
        if path.is_file() and path.suffix in {".md", ".toml", ".py"}
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if pinned.search(line)
    ]
    assert offenders == []
