"""The typed ``chat:`` manifest block (PACT chat-manifest-block).

Every test here is about a REFUSAL, because validation is this PACT's whole
deliverable: nothing consumes the block yet, so the only thing that can be
wrong is a manifest that loads when it should not have.

The absence arms are first-class, not afterthoughts. A membership test whose
reference set is empty passes vacuously — the deployment "has no undeclared
tools" because it has no admitted tools at all — so each rule is proved on
its own absence case too: no ``search:`` block, an empty admitted tool
surface, an empty ``models.builtin``, an absent ``builtin`` key.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.deployments import (
    _KNOWN_TOP_LEVEL_KEYS,
    DeploymentConfigError,
    DeploymentManifest,
)
from okg.substrate.deployments.chat_block import (
    CANDIDATE_UI_FEATURE_KEYS,
    CANDIDATE_UI_KEYS,
    SHIPPED_UI_FEATURE_KEYS,
    SHIPPED_UI_KEYS,
    _is_credential_shaped,
    UI_KNOB_CENSUS_FILE,
    UI_KNOB_CENSUS_PINNED_VERSION,
    ChatManifestError,
    ChatUiCensusUnavailable,
    ChatUnknownSearchProfile,
    VizChatRetired,
    load_ui_feature_census,
    load_ui_knob_census,
    parse_chat_block,
    schema_field_census,
    shipped_ui_field_set,
    validate_requested_tools,
)
from okg.substrate.mcp.operators import AKMON_READ_TOOLS, QUERY_ESCAPE_TOOLS
from tests.mirror_scope import private_path

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _valid_chat_block() -> dict:
    """The design.md section 4 example, reconciled against the census.

    No `theme`: the census measured both theme knobs as not drivable at the
    pinned version, so the field is not admitted.
    """
    return {
        "enabled": True,
        "ui": {
            "site_name": "CMS Computing Ops",
            "logo_ref": "chat/logo.png",
            "banner": "Research deployment — answers cite the published graph.",
            "features": {
                "context_compaction": True,
                "web_search": False,
                "arena": True,
                "byok": True,
            },
        },
        "models": {
            "builtin": ["anthropic/claude-sonnet-5", "openai/gpt-5"],
            "default": "anthropic/claude-sonnet-5",
        },
        "preset": {
            "display_name": "CMS Computing Ops",
            "description": "Ask the CMS computing operations graph",
            "system_prompt_ref": "chat/system_prompt.md",
            "suggestions": ["Which sites are draining?"],
        },
        "search_profile": "analyst",
        "mcp": {
            "port": 8101,
            "tools": ["inspect", "search", "expand"],
            "auth_token_env": "OKG_CHAT_MCP_TOKEN",
        },
        "access": {"groups": ["cms-operators"]},
        "deep_links": {
            "operator_ui_base": "http://127.0.0.1:5173",
            "chat_site_base": "http://127.0.0.1:8080",
        },
    }


def _raw_manifest(chat: dict | None = None, **extra) -> dict:
    raw: dict = {
        "name": "fixture-deployment",
        "search": {
            "default_profile": "analyst",
            "profiles": {
                "analyst": {"subtypes": ["chunk"]},
                "ops": {"subtypes": ["chunk"]},
            },
        },
    }
    if chat is not None:
        raw["chat"] = chat
    raw.update(extra)
    return raw


def _write_deployment(tmp_path: Path, raw: dict) -> Path:
    """Materialize a loadable deployment dir and return its manifest path."""
    dep = tmp_path / raw.get("name", "fixture-deployment")
    (dep / "schema").mkdir(parents=True, exist_ok=True)
    (dep / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    document = dict(raw)
    document.setdefault("schema_dir", "schema")
    document.setdefault("source_registry", "source_registry.yaml")
    manifest_path = dep / "deployment.yaml"
    manifest_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return manifest_path


# ---------------------------------------------------------------------------
# req.chat-manifest-block.typed-schema
# ---------------------------------------------------------------------------

def test_chat_block_is_a_known_top_level_key(tmp_path: Path) -> None:
    """A manifest carrying `chat:` loads instead of raising the
    unknown-top-level-key refusal that used to make the block unwritable."""
    assert "chat" in _KNOWN_TOP_LEVEL_KEYS
    manifest_path = _write_deployment(
        tmp_path, _raw_manifest(_valid_chat_block()),
    )
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.chat is not None
    assert manifest.chat.enabled is True


def test_chat_block_parses_non_ui_schema(tmp_path: Path) -> None:
    """Every non-ui design.md section 4 field round-trips onto the model."""
    manifest_path = _write_deployment(
        tmp_path, _raw_manifest(_valid_chat_block()),
    )
    chat = DeploymentManifest.from_yaml(manifest_path).chat

    assert chat.models.builtin == (
        "anthropic/claude-sonnet-5", "openai/gpt-5",
    )
    assert chat.models.default == "anthropic/claude-sonnet-5"
    assert chat.preset.display_name == "CMS Computing Ops"
    assert chat.preset.description == "Ask the CMS computing operations graph"
    assert chat.preset.system_prompt_ref == "chat/system_prompt.md"
    assert chat.preset.suggestions == ("Which sites are draining?",)
    assert chat.search_profile == "analyst"
    assert chat.mcp.port == 8101
    assert chat.mcp.tools == ("inspect", "search", "expand")
    assert chat.mcp.auth_token_env == "OKG_CHAT_MCP_TOKEN"
    assert chat.access.groups == ("cms-operators",)
    assert chat.deep_links.operator_ui_base == "http://127.0.0.1:5173"
    assert chat.deep_links.chat_site_base == "http://127.0.0.1:8080"
    # The census-admitted ui knobs ride here; `theme` is not one of them.
    assert chat.ui.site_name == "CMS Computing Ops"
    assert chat.ui.logo_ref == "chat/logo.png"
    assert chat.ui.banner.startswith("Research deployment")
    assert chat.ui.features == {
        "context_compaction": True,
        "web_search": False,
        "arena": True,
        "byok": True,
    }
    # The chat surface has no accent at this version, and says so plainly
    # rather than raising on the attribute.
    assert chat.accent is None


@pytest.mark.parametrize(
    "mutate, offending, path",
    [
        (
            lambda c: c["preset"].__setitem__("display_nam", "typo"),
            "display_nam",
            "chat.preset",
        ),
        (
            lambda c: c["models"].__setitem__("defalt", "x"),
            "defalt",
            "chat.models",
        ),
        (
            lambda c: c["mcp"].__setitem__("prot", 1),
            "prot",
            "chat.mcp",
        ),
        (
            lambda c: c["access"].__setitem__("grops", []),
            "grops",
            "chat.access",
        ),
        (
            lambda c: c["deep_links"].__setitem__("console_base", "http://x"),
            "console_base",
            "chat.deep_links",
        ),
        (
            lambda c: c["ui"]["features"].__setitem__("web_serch", True),
            "web_serch",
            "chat.ui.features",
        ),
        (
            lambda c: c["ui"].__setitem__("site_nam", "typo"),
            "site_nam",
            "chat.ui",
        ),
        (
            lambda c: c.__setitem__("serch_profile", "analyst"),
            "serch_profile",
            "chat",
        ),
    ],
)
def test_unknown_nested_chat_key_rejected(
    tmp_path: Path, mutate, offending: str, path: str,
) -> None:
    """A typo at ANY depth raises, naming the key and its path.

    The top-level census governs only the top-level key SET, so without a
    per-level check `display_nam:` would be silently dropped and the site
    would come up with a default display name.
    """
    chat = _valid_chat_block()
    mutate(chat)
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(DeploymentConfigError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    message = str(excinfo.value)
    assert offending in message, message
    assert path in message, message


def test_unknown_nested_chat_key_is_not_a_top_level_refusal(
    tmp_path: Path,
) -> None:
    """The nested refusal is its own rule, not the top-level census firing.

    Without this the previous test would pass even if `chat.preset` were
    never inspected — a manifest can fail for the wrong reason and look
    verified.
    """
    chat = _valid_chat_block()
    chat["preset"]["display_nam"] = "typo"
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert "unknown top-level key" not in str(excinfo.value)
    assert excinfo.value.data["path"] == "chat.preset"


def _chat_without(key: str) -> dict:
    chat = _valid_chat_block()
    del chat[key]
    return chat


def _chat_with_empty_builtin() -> dict:
    chat = _valid_chat_block()
    chat["models"] = {"builtin": []}
    return chat


@pytest.mark.parametrize(
    "chat, missing",
    [
        ({}, "enabled"),
        (_chat_without("ui"), "ui"),
        (_chat_without("preset"), "preset"),
        (_chat_without("models"), "models"),
        (_chat_without("search_profile"), "search_profile"),
        (_chat_without("mcp"), "mcp"),
        (_chat_with_empty_builtin(), "builtin"),
    ],
)
def test_chat_block_missing_required_subblock_raises(
    tmp_path: Path, chat: dict, missing: str,
) -> None:
    """A hollow block never degrades into a default-configured chat.

    Each arm removes exactly ONE required sub-block from an otherwise valid
    declaration, so the raised message must name that specific field — a
    block missing everything at once would let any of these assertions pass
    for the wrong reason.
    """
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(DeploymentConfigError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert missing in str(excinfo.value)


def test_empty_chat_mapping_names_what_an_enabled_chat_must_declare(
    tmp_path: Path,
) -> None:
    """`chat: {}` says what is missing, not merely that something is."""
    manifest_path = _write_deployment(tmp_path, _raw_manifest({}))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    message = str(excinfo.value)
    assert "enabled" in message
    for required in ("ui", "models", "preset", "search_profile", "mcp"):
        assert required in message, required


def test_chat_disabled_is_the_only_minimal_form(tmp_path: Path) -> None:
    """`enabled: false` alone loads; `enabled: false` plus site content does not."""
    manifest_path = _write_deployment(
        tmp_path, _raw_manifest({"enabled": False}),
    )
    chat = DeploymentManifest.from_yaml(manifest_path).chat
    assert chat is not None and chat.enabled is False

    manifest_path = _write_deployment(
        tmp_path,
        _raw_manifest({"enabled": False, "preset": {"display_name": "x"}}),
    )
    with pytest.raises(ChatManifestError):
        DeploymentManifest.from_yaml(manifest_path)


def test_viz_chat_beside_a_chat_block_is_still_refused(tmp_path: Path) -> None:
    """The old mutual-exclusion case, kept as one arm of the retirement.

    This was a mutual-exclusion guard while the viz.chat disposition was an
    open operator decision. Decision 6 resolved it as retire, so the refusal
    is no longer "pick one" — it is "that one is gone", and it no longer
    depends on a chat: block being present. Renamed accordingly: "mutually
    exclusive" would name a rule that no longer exists. Kept rather than
    deleted because absorbing a rule into a broader one is exactly when its
    case should stay covered. The full retirement contract lives in
    tests/substrate/test_retire_viz_chat.py.
    """
    manifest_path = _write_deployment(
        tmp_path,
        _raw_manifest(
            _valid_chat_block(),
            viz={"chat": {"system_hints": ["be brief"]}},
        ),
    )

    with pytest.raises(VizChatRetired) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert "viz.chat" in str(excinfo.value)


def test_manifest_without_chat_block_still_loads(tmp_path: Path) -> None:
    """A manifest with no chat block loads and reports chat as ABSENT.

    Absent must be its own state, not an empty-but-configured block: the
    difference is what `okg lint deployments` reports as not-declared rather
    than as a passing chat check.
    """
    manifest_path = _write_deployment(tmp_path, _raw_manifest())
    manifest = DeploymentManifest.from_yaml(manifest_path)
    assert manifest.chat is None



def test_the_workspace_chat_block_parses() -> None:
    """The in-repo arm, split out of the test above.

    chat-sync-projection landed the first in-repo chat block, on
    okg-workspace, so the assertion is that it PARSES rather than that it is
    absent. Keeping the old "is None" would have made the first real
    declaration read as a regression. okg-workspace is this repository's own
    deployment; the public copy skips this test (tests/mirror_scope.py).
    """
    workspace = private_path("deployments/okg-workspace/deployment.yaml")
    workspace_chat = DeploymentManifest.from_yaml(workspace).chat
    assert workspace_chat is not None and workspace_chat.enabled


def test_chat_schema_declares_no_credential_field() -> None:
    """No field in the schema could carry a secret.

    Structural, not aspirational: a provider key or admin key has nowhere to
    be written because every key outside this census is rejected. The one
    credential-ADJACENT field, `mcp.auth_token_env`, names an environment
    VARIABLE — the indirection the rule exists to force.
    """
    banned = (
        "api_key", "apikey", "secret", "password", "passwd", "credential",
        "admin_key", "access_key", "private_key", "token",
    )
    census = schema_field_census()
    assert census, "the schema field census must not be empty"
    for path, fields in census.items():
        for name in fields:
            lowered = name.lower()
            # One definition of the rule, applied twice. The module's own
            # predicate is the authority (it is what the schema is written
            # against); the needle sweep below is the independent check that
            # the predicate has not been widened into uselessness.
            assert not _is_credential_shaped(name), (
                f"{path}.{name} is credential-shaped"
            )
            if any(needle in lowered for needle in banned):
                # The ONLY admissible form of a credential-adjacent field:
                # an environment VARIABLE NAME. `mcp.auth_token_env` and
                # `sso.client_secret_env` are the two, and the `_env` suffix
                # is the indirection the no-secrets rule exists to force.
                assert lowered.endswith("_env"), (
                    f"{path}.{name} may carry a literal credential; only an "
                    f"environment VARIABLE NAME (*_env) is admissible"
                )


def test_chat_key_is_classified_in_the_fingerprint_census() -> None:
    """`chat` is classified IN or OUT for the manifest fingerprint.

    An unclassified top-level key reds the per-PR census gate; classifying it
    here keeps the fingerprint's coverage self-maintaining.
    """
    from okg.substrate.catalog.manifest_identity import (
        MANIFEST_FINGERPRINT_OUT,
        census_gap,
        census_overlap,
    )

    assert "chat" in MANIFEST_FINGERPRINT_OUT
    assert census_gap(_KNOWN_TOP_LEVEL_KEYS) == set()
    assert census_overlap() == set()


# ---------------------------------------------------------------------------
# req.chat-manifest-block.census-bounded-ui
# ---------------------------------------------------------------------------

def _census_artifact(root: Path) -> Path:
    return root / UI_KNOB_CENSUS_FILE


def _knob(field: str, *, drivable: bool) -> dict:
    """One census entry in the artifact's real measurement shape."""
    return {
        "knob": field.rpartition(".")[2],
        "manifest_field": field,
        "mechanism": "api" if drivable else "none",
        "persistent_config": drivable,
        "readback": "GET /api/config" if drivable else "none",
        "drivable": drivable,
    }


def _write_census(root: Path, knobs: list[dict], *, version: str | None = None) -> Path:
    artifact = _census_artifact(root)
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(
        json.dumps({
            "openwebui_version": version or UI_KNOB_CENSUS_PINNED_VERSION,
            "knobs": knobs,
        }),
        encoding="utf-8",
    )
    return artifact


def test_ui_schema_field_set_equals_census_admitted_set() -> None:
    """The shipped ui field set equals the census-ADMITTED set, exactly.

    Read from the recorded artifact, never restated inline. This is the whole
    census mechanism in one assertion: the schema may not admit a knob nobody
    measured as drivable, and may not omit one that was.
    """
    admitted = load_ui_knob_census()
    shipped = shipped_ui_field_set()

    assert shipped == admitted, (
        f"the shipped ui field set {sorted(shipped)} must equal the "
        f"census-admitted set {sorted(admitted)}"
    )
    # And the reconciliation is a real narrowing, not a coincidence: design.md
    # proposed a knob the census refused, so the shipped set is a STRICT
    # subset of the candidate list.
    assert shipped < CANDIDATE_UI_KEYS
    assert "theme" in CANDIDATE_UI_KEYS - shipped


def test_every_admitted_ui_knob_is_actually_parsed() -> None:
    """Each admitted knob reaches a ChatUi field — accepted is not enough.

    The equality test above proves the schema ADMITS the census set. This
    proves it does something with each one: a knob that parsed into nothing
    would satisfy the field-set comparison and still be a silent no-op, which
    is the exact failure the census exists to prevent.
    """
    sample: dict[str, object] = {
        "site_name": "Fixture Chat",
        "logo_ref": "chat/logo.png",
        "banner": "Fixture banner",
        "features": {"web_search": True},
    }
    assert set(sample) == SHIPPED_UI_KEYS, (
        "this test must exercise every admitted knob; update the sample when "
        "the census admits a new one"
    )

    chat = _valid_chat_block()
    chat["ui"] = sample
    block = parse_chat_block(_raw_manifest(chat))

    assert block.ui.site_name == "Fixture Chat"
    assert block.ui.logo_ref == "chat/logo.png"
    assert block.ui.banner == "Fixture banner"
    assert block.ui.features == {"web_search": True}


def test_ui_feature_set_equals_candidate_intersect_census() -> None:
    """The feature vocabulary is candidate ∩ census — the same rule as above.

    Without this, `features` would be an admitted container holding an
    unaudited key set: the census would bound the door and not the room.

    `memory` used to be this test's witness that the intersection BITES:
    measured drivable, yet inadmissible because design.md records per-user
    memory as off. The 2026-08-10 census correction removed it — the master
    key `memories.enable` (config.py:2809) was never actually driven, so
    `drivable` is now false and the leaf is no longer measured at all.

    With it gone the three sets COINCIDE, and this assertion on its own is
    vacuous: candidate ∩ measured equals candidate whenever measured ⊇
    candidate, so it would hold on a census that narrowed nothing. That fact
    is asserted here rather than left implicit, and the narrowing DIRECTION is
    proven by mechanism in the test below instead of by a data accident.
    """
    measured = load_ui_feature_census()

    assert SHIPPED_UI_FEATURE_KEYS == CANDIDATE_UI_FEATURE_KEYS & measured
    assert CANDIDATE_UI_FEATURE_KEYS <= measured, (
        "a candidate is no longer measured drivable — the sets have stopped "
        "coinciding, so re-read this test: the intersection now carries real "
        "information and the docstring above is stale"
    )


def test_a_candidate_the_census_does_not_admit_is_narrowed_out(
    tmp_path: Path,
) -> None:
    """The census NARROWS the candidate list, proven on the mechanism.

    The real artifact currently measures every candidate as drivable, so no
    live data distinguishes "the intersection works" from "the intersection
    is a no-op". A synthetic census does: mark one candidate not drivable and
    the derived set MUST lose exactly that leaf, with the rest untouched.
    """
    dropped = sorted(CANDIDATE_UI_FEATURE_KEYS)[0]
    kept = CANDIDATE_UI_FEATURE_KEYS - {dropped}

    _write_census(tmp_path, [
        _knob(f"chat.ui.features.{leaf}", drivable=(leaf != dropped))
        for leaf in sorted(CANDIDATE_UI_FEATURE_KEYS)
    ])

    narrowed = load_ui_feature_census(data_dir=tmp_path)

    assert dropped not in narrowed, (
        f"{dropped!r} was measured NOT drivable and still came back admitted"
    )
    assert narrowed == kept
    assert CANDIDATE_UI_FEATURE_KEYS & narrowed == kept

    # The control: the same shape with every candidate drivable admits them
    # all, so the arm above fails for the measurement and not the fixture.
    _write_census(tmp_path, [
        _knob(f"chat.ui.features.{leaf}", drivable=True)
        for leaf in sorted(CANDIDATE_UI_FEATURE_KEYS)
    ])
    assert load_ui_feature_census(data_dir=tmp_path) == CANDIDATE_UI_FEATURE_KEYS


def test_declaring_an_ungoverned_feature_is_refused(tmp_path: Path) -> None:
    """`features.memory` is refused even though the census measured it drivable."""
    chat = _valid_chat_block()
    chat["ui"]["features"] = {"memory": True}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert "memory" in str(excinfo.value)
    assert excinfo.value.data["path"] == "chat.ui.features"


@pytest.mark.parametrize("bad", ["yes", 1, 0, None, [], {"nested": True}])
def test_feature_values_must_be_literal_booleans(
    tmp_path: Path, bad: object,
) -> None:
    """A non-boolean toggle raises rather than being coerced.

    design.md calls features an "explicit ENABLE_* projection". `"yes"` and
    `1` are truthy in Python and meaningless as a declaration: coercing them
    would decide a capability the operator never actually stated.
    """
    chat = _valid_chat_block()
    chat["ui"]["features"] = {"web_search": bad}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["path"] == "chat.ui.features.web_search"


def test_banner_must_be_a_string(tmp_path: Path) -> None:
    """The banner is manifest identity text, not a pre-built banner object.

    The census drives `ui.banners`, whose objects carry
    id/type/title/content/dismissible/timestamp. Those are projection-owned
    (ids and timestamps are not deployment identity), so the manifest declares
    the TEXT and the sync verb builds the object. A mapping here is refused
    rather than passed through to an API that would reject it later.
    """
    chat = _valid_chat_block()
    chat["ui"]["banner"] = {"title": "hi", "content": "there"}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    assert excinfo.value.data["path"] == "chat.ui.banner"


def test_empty_features_map_is_declared_not_absent(tmp_path: Path) -> None:
    """`features: {}` parses to {}, distinct from an absent block.

    Absent means "leave the instance defaults alone"; empty means the
    operator declared a toggle map and put nothing in it. Collapsing them
    would destroy the distinction before any projection could act on it.
    """
    chat = _valid_chat_block()
    chat["ui"]["features"] = {}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))
    assert DeploymentManifest.from_yaml(manifest_path).chat.ui.features == {}

    chat = _valid_chat_block()
    del chat["ui"]["features"]
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))
    assert DeploymentManifest.from_yaml(manifest_path).chat.ui.features is None


def test_ui_feature_census_refuses_an_all_undrivable_feature_set(
    tmp_path: Path,
) -> None:
    """A census admitting no feature refuses rather than returning empty.

    The knob-side twin of this arm is tested above; without this one, an
    empty feature set would silently satisfy the intersection above — both
    sides being nothing is not agreement.
    """
    _write_census(tmp_path, [
        _knob("chat.ui.banner", drivable=True),
        _knob("chat.ui.features.web_search", drivable=False),
    ])
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_feature_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "empty_admitted_features"


def test_inadmissible_theme_field_is_refused_with_the_measured_reason(
    tmp_path: Path,
) -> None:
    """`chat.ui.theme` refuses, and says WHY — not "unknown key".

    The operator wrote a real, documented field. Reporting it as a typo would
    send them looking for a spelling mistake; the truth is that Open WebUI at
    the pinned version cannot apply it.
    """
    chat = _valid_chat_block()
    chat["ui"]["theme"] = {"base": "light", "accent": "#10b981"}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    message = str(error)
    assert "theme" in message
    assert "not drivable" in message.lower()
    assert UI_KNOB_CENSUS_PINNED_VERSION in message
    assert error.data["inadmissible"] == ["theme"]
    assert "unknown key" not in message


def test_ui_schema_refuses_when_census_missing(tmp_path: Path) -> None:
    """With no artifact there is no admitted set, and the loader says so.

    The schema constant is bound to the census by test rather than read at
    manifest-load time, so this guards the binding's input: a missing census must refuse, never silently
    hand back a default set that would let the binding test pass.
    """
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_knob_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "missing"
    assert str(tmp_path / UI_KNOB_CENSUS_FILE) in str(excinfo.value)

    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_feature_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "missing"


def test_ui_census_refuses_on_version_mismatch(tmp_path: Path) -> None:
    """A census recorded at another version proves nothing about the pin."""
    _write_census(
        tmp_path,
        [_knob("chat.ui.banner", drivable=True)],
        version="0.0.1",
    )
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_knob_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "version_mismatch"
    assert excinfo.value.data["expected"] == UI_KNOB_CENSUS_PINNED_VERSION


def test_ui_census_refuses_when_unreadable(tmp_path: Path) -> None:
    """A present-but-corrupt census is a hard failure, never a fallback."""
    artifact = _census_artifact(tmp_path)
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text("{not json", encoding="utf-8")
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_knob_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "unreadable"


def test_ui_census_refuses_an_all_undrivable_census(tmp_path: Path) -> None:
    """A census that admits NOTHING refuses rather than returning an empty set.

    An empty admitted set would silently satisfy every membership test — the
    schema would "equal the census" by both being nothing. That must be a
    refusal, not a passing comparison.
    """
    _write_census(tmp_path, [
        _knob("chat.ui.theme.accent", drivable=False),
        _knob("chat.ui.banner", drivable=False),
    ])
    with pytest.raises(ChatUiCensusUnavailable) as excinfo:
        load_ui_knob_census(data_dir=tmp_path)
    assert excinfo.value.data["reason"] == "empty_admitted_set"


def test_ui_census_admits_recorded_knobs(tmp_path: Path) -> None:
    """With a valid census the admitted set is DERIVED from the measurements.

    Proves the reading path works, so the refusals above are refusals and not
    a loader that can never succeed. Also pins the three derivation rules: a
    not-drivable knob is excluded, a knob naming a non-chat.ui field is
    excluded, and `features.<leaf>` collapses to the container `features`.
    """
    _write_census(tmp_path, [
        _knob("chat.ui.banner", drivable=True),
        _knob("chat.ui.site_name", drivable=True),
        _knob("chat.ui.theme.accent", drivable=False),
        _knob("chat.models.default", drivable=True),
        _knob("chat.ui.features.web_search", drivable=True),
        _knob("chat.ui.features.arena", drivable=False),
    ])

    assert load_ui_knob_census(data_dir=tmp_path) == frozenset({
        "banner", "site_name", "features",
    })
    assert load_ui_feature_census(data_dir=tmp_path) == frozenset({
        "web_search",
    })


# ---------------------------------------------------------------------------
# req.chat-manifest-block.cross-reference-validation
# ---------------------------------------------------------------------------

def test_cross_reference_unknown_search_profile_raises(tmp_path: Path) -> None:
    """An undeclared profile refuses rather than widening scope."""
    chat = _valid_chat_block()
    chat["search_profile"] = "ghost"
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatUnknownSearchProfile) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "unknown_search_profile"
    assert "ghost" in str(error)
    assert "analyst" in str(error) and "ops" in str(error)
    assert error.data["declared"] == ["analyst", "ops"]


def test_cross_reference_search_profile_without_search_block_raises(
    tmp_path: Path,
) -> None:
    """An EMPTY declared set is not permission.

    The manifest declares no `search:` block at all, so every membership test
    against it is vacuous. Passing here would be the classic absence-dressed-
    as-agreement failure: the chat surface would come up unscoped.
    """
    chat = _valid_chat_block()
    raw = _raw_manifest(chat)
    del raw["search"]
    manifest_path = _write_deployment(tmp_path, raw)

    with pytest.raises(ChatUnknownSearchProfile) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "unknown_search_profile"
    assert "analyst" in str(error)
    assert error.data["declared"] == []
    assert "no search" in str(error)


def test_cross_reference_tool_outside_known_universe_raises(
    tmp_path: Path,
) -> None:
    """An unrecognized tool is refused, never dropped from the tool set."""
    chat = _valid_chat_block()
    chat["mcp"]["tools"] = ["inspect", "teleport"]
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "chat_manifest_invalid"
    assert "teleport" in str(error)
    known = sorted(set(AKMON_READ_TOOLS) | set(QUERY_ESCAPE_TOOLS))
    assert error.data["admitted"] == known
    for tool in known:
        assert tool in str(error)


def test_cross_reference_empty_admitted_surface_refuses_every_tool() -> None:
    """Handed an EMPTY admitted set, the guard refuses every tool.

    The admitted surface is a parameter precisely so this arm is reachable:
    the module constants are non-empty by construction, so monkeypatching
    them would be testing a state the code can never be in. An empty
    reference set must refuse everything, not admit everything.
    """
    with pytest.raises(ChatManifestError) as excinfo:
        validate_requested_tools(["inspect"], admitted=[])

    error = excinfo.value
    assert error.code == "chat_manifest_invalid"
    assert "inspect" in str(error)
    assert error.data["admitted"] == []

    # And the same guard admits what a non-empty surface declares, so the
    # refusal above is a refusal rather than a function that always raises.
    assert validate_requested_tools(
        ["inspect"], admitted=["inspect", "search"],
    ) == ("inspect",)


def test_cross_reference_default_model_not_in_builtin_raises(
    tmp_path: Path,
) -> None:
    """A default outside the built-in list never falls back to the first."""
    chat = _valid_chat_block()
    chat["models"]["default"] = "meta/llama-4"
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "chat_manifest_invalid"
    assert "meta/llama-4" in str(error)
    assert "anthropic/claude-sonnet-5" in str(error)
    assert error.data["path"] == "chat.models.default"


def test_cross_reference_empty_builtin_list_refuses_every_default(
    tmp_path: Path,
) -> None:
    """An empty built-in list contains no default — so it refuses every one."""
    chat = _valid_chat_block()
    chat["models"] = {"builtin": [], "default": "anthropic/claude-sonnet-5"}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "chat_manifest_invalid"
    assert "anthropic/claude-sonnet-5" in str(error)
    assert error.data["builtin"] == []
    assert "empty" in str(error)


def test_cross_reference_builtin_absent_with_default_declared_raises(
    tmp_path: Path,
) -> None:
    """An ABSENT builtin key is refused too, with its own reason.

    Absent and empty are different mistakes; both must refuse, and neither
    may resolve the default from the instance.
    """
    chat = _valid_chat_block()
    chat["models"] = {"default": "anthropic/claude-sonnet-5"}
    manifest_path = _write_deployment(tmp_path, _raw_manifest(chat))

    with pytest.raises(ChatManifestError) as excinfo:
        DeploymentManifest.from_yaml(manifest_path)

    error = excinfo.value
    assert error.code == "chat_manifest_invalid"
    assert "anthropic/claude-sonnet-5" in str(error)
    assert "no builtin key" in str(error)


# ---------------------------------------------------------------------------
# req.chat-provider-url-no-credential.loader-refuses (#2344)
# ---------------------------------------------------------------------------

#: A sentinel that is NOT key-shaped, so only the userinfo/query rules can be
#: what catches it (and only identity can prove it was withheld).
URL_SECRET = "hunter2-Sentinel-2344"
PASTED_KEY = "sk-proj-" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4"
#: The part of PASTED_KEY after its prefix; the encoded-prefix rows use it.
KEY_TAIL = PASTED_KEY[len("sk-proj-"):]
#: Fake tokens with no key prefix (#2735): 64 hex digits, and 42 base62
#: characters that switch case and digit every character.
HEX_TOKEN = hashlib.sha256(b"okg-fake-token-2735").hexdigest()
MIXED_TOKEN = "".join(f"{chr(65 + i)}{i % 10}{chr(97 + i)}" for i in range(14))
#: Fake short keys in real key shapes, assembled so no literal is complete.
GOOGLE_KEY = "AI" + "za" + "SyFAKE0" + "a1B2c3D4e5F6g7H8i9J0k1L2m3N"
HF_KEY = "hf" + "_" + "FAKEsentinelAbCdEfGhIjKlMnOpQrStUv"
GITLAB_KEY = "gl" + "pat-" + "FAKE0a1B2c3D4e5F6g7H"
AWS_KEY = "AK" + "IA" + "FAKE0SENTINEL012"

CREDENTIAL_BASE_URLS = {
    "userinfo": (f"https://okg:{URL_SECRET}@api.provider.invalid/v1", "has_credentials", URL_SECRET),
    "query-api-key": (f"http://host.docker.internal:8000/v1?api_key={URL_SECRET}",
                      "credential_parameter", URL_SECRET),
    "fragment-token": (f"https://api.provider.invalid/v1#token={URL_SECRET}",
                       "credential_parameter", URL_SECRET),
    "key-in-path": (f"https://api.provider.invalid/{PASTED_KEY}/v1", "key_shaped_part", PASTED_KEY),
    "bare-pasted-key": (PASTED_KEY, "not_http", PASTED_KEY),
    # #2735: a `;name=value` path parameter, which urlsplit keeps in the path.
    "path-param-api-key": (f"https://api.provider.invalid/v1;api_key={URL_SECRET}",
                           "credential_parameter", URL_SECRET),
    "path-param-encoded-name": (f"https://api.provider.invalid/v1;api%5Fkey={URL_SECRET}",
                                "credential_parameter", URL_SECRET),
    "path-segment-assignment": (f"https://api.provider.invalid/token={URL_SECRET}/v1",
                                "credential_parameter", URL_SECRET),
    "path-param-key-value": (f"https://api.provider.invalid/v1;x={PASTED_KEY}",
                             "key_shaped_part", PASTED_KEY),
    # #2735: a percent-encoded key prefix (`%73` is `s`), once and twice.
    "encoded-key-prefix": (f"https://api.provider.invalid/%73k-proj-{KEY_TAIL}/v1",
                           "key_shaped_part", KEY_TAIL),
    "double-encoded-key-prefix": (f"https://api.provider.invalid/%2573k-proj-{KEY_TAIL}/v1",
                                  "key_shaped_part", KEY_TAIL),
    # #2735: a bare token with no known prefix in a path segment.
    "hex-token-segment": (f"https://api.provider.invalid/{HEX_TOKEN}/v1",
                          "token_shaped_part", HEX_TOKEN),
    "mixed-token-segment": (f"https://api.provider.invalid/{MIXED_TOKEN}/v1",
                            "token_shaped_part", MIXED_TOKEN),
    "encoded-mixed-token-segment": (
        f"https://api.provider.invalid/{MIXED_TOKEN[:20]}%2D{MIXED_TOKEN[20:]}/v1",
        "token_shaped_part", MIXED_TOKEN[20:],
    ),
    "hex-token-path-param-value": (f"https://api.provider.invalid/v1;session={HEX_TOKEN}",
                                   "token_shaped_part", HEX_TOKEN),
    # #2735 review: a key prefix after a boundary inside a part, not at its
    # start -- an encoded slash, a NUL, a dot, a word and hyphen, a plus.
    "key-after-encoded-slash": (f"https://api.provider.invalid/v1/x%2F{PASTED_KEY}",
                                "key_shaped_part", KEY_TAIL),
    "key-after-nul": (f"https://api.provider.invalid/%00{PASTED_KEY}", "key_shaped_part", KEY_TAIL),
    "key-after-dot": (f"https://api.provider.invalid/.{PASTED_KEY}", "key_shaped_part", KEY_TAIL),
    "key-after-word": (f"https://api.provider.invalid/key-{PASTED_KEY}", "key_shaped_part", KEY_TAIL),
    "key-after-plus": (f"https://api.provider.invalid/+{PASTED_KEY}", "key_shaped_part", KEY_TAIL),
    # Short keys with no other rule to catch them.
    "google-key-mid-part": (f"https://api.provider.invalid/x-{GOOGLE_KEY}", "key_shaped_part", GOOGLE_KEY),
    "hf-key-mid-part": (f"https://api.provider.invalid/x-{HF_KEY}", "key_shaped_part", HF_KEY),
    "gitlab-key-mid-part": (f"https://api.provider.invalid/x.{GITLAB_KEY}", "key_shaped_part", GITLAB_KEY),
    "aws-key-mid-part": (f"https://api.provider.invalid/x-{AWS_KEY}", "key_shaped_part", AWS_KEY),
    # #2735 review: camelCase and common credential names, any case.
    **{
        f"param-{name}": (f"https://api.provider.invalid/v1?{name}={URL_SECRET}",
                          "credential_parameter", URL_SECRET)
        for name in ("accessToken", "apiToken", "authToken", "sessionToken", "JWT",
                     "bearer", "credential", "passwd", "pwd", "dbPwd", "clientSecret")
    },
}


def _load_with_provider(tmp_path: Path, provider: dict) -> DeploymentManifest:
    chat = _valid_chat_block()
    chat["provider"] = provider
    return DeploymentManifest.from_yaml(_write_deployment(tmp_path, _raw_manifest(chat)))


@pytest.mark.parametrize("case", sorted(CREDENTIAL_BASE_URLS))
def test_a_hand_written_base_url_carrying_a_credential_is_refused_and_withheld(
    tmp_path: Path, case: str,
) -> None:
    url, shape, secret = CREDENTIAL_BASE_URLS[case]

    with pytest.raises(ChatManifestError) as excinfo:
        _load_with_provider(tmp_path, {"base_url": url, "api_key_env": "OKG_PROVIDER_KEY"})

    error = excinfo.value
    assert error.data["path"] == "chat.provider.base_url"
    assert error.data["shape"] == shape
    assert error.data["observed"] == "<withheld>"
    rendered = json.dumps(error.to_json(), default=str) + str(error)
    assert secret not in rendered
    assert url not in rendered
    # The operator is told which file and key to edit by hand: setup loads
    # this manifest first, so it cannot repair the value.
    manifest_file = str(next(tmp_path.glob("*/deployment.yaml")))
    assert "chat.provider.base_url" in str(error)
    assert manifest_file in str(error)
    if shape != "not_http":
        assert manifest_file in error.fix_hint
        assert "by hand" in error.fix_hint


def test_a_non_string_base_url_is_refused_without_echoing_it(tmp_path: Path) -> None:
    secret = 7302344123456789

    with pytest.raises(ChatManifestError) as excinfo:
        _load_with_provider(tmp_path, {"base_url": secret})

    error = excinfo.value
    assert error.data["path"] == "chat.provider.base_url"
    assert error.data["observed"] == "a value of type int"
    assert str(secret) not in json.dumps(error.to_json(), default=str) + str(error)


#: Real provider base URL shapes (#2735 survey): every base URL the repo's
#: deployments, docs, templates and tests use for a chat provider, and the
#: common providers' documented forms. Account, resource and project names
#: are placeholders of the real length and alphabet. None may be refused.
REAL_BASE_URLS = [
    # In this repository.
    "http://host.docker.internal:11434/v1",
    "http://host.docker.internal:8000/v1",
    "http://host.docker.internal:9000/v1",
    "http://gpu.example.invalid:8000/v1",
    "http://127.0.0.1:8080/v1",
    "https://localhost/v1",
    "https://fake-2344.openai.azure.invalid/openai/v1?api-version=2024-06-01",
    # OpenAI, Anthropic, and OpenAI-compatible hosted APIs.
    "https://api.openai.com/v1",
    "https://api.anthropic.com/v1",
    "https://openrouter.ai/api/v1",
    "https://api.together.xyz/v1",
    "https://api.groq.com/openai/v1",
    "https://api.mistral.ai/v1",
    "https://api.deepseek.com/v1",
    "https://api.x.ai/v1",
    "https://api.perplexity.ai",
    "https://api.fireworks.ai/inference/v1",
    "https://api.deepinfra.com/v1/openai",
    "https://oai.helicone.ai/v1",
    "https://api.portkey.ai/v1",
    "https://router.huggingface.co/v1",
    "https://api-inference.huggingface.co/models/meta-llama/Meta-Llama-3.1-8B-Instruct/v1",
    "https://api.runpod.ai/v2/abc123xyz4567/openai/v1",
    # Self-hosted: Ollama, vLLM, LiteLLM.
    "http://localhost:11434/v1",
    "http://localhost:8000/v1",
    "http://litellm:4000",
    # Azure OpenAI and Azure AI, including long deployment names and a
    # UUID-bearing resource path.
    "https://my-resource.openai.azure.com/openai/deployments/gpt-4o-mini",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "my-company-gpt-4o-production-eastus-deployment-2024",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "MyCompanyGPT4oProductionEastUS2024Deploy",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "Llama3p1x70BInstructAWQ4bitVllmServingEndpointA1",
    "https://my-resource.openai.azure.com/openai/v1?api-version=preview",
    "https://my-resource.services.ai.azure.com/models",
    "https://my-endpoint.eastus2.inference.ml.azure.com/v1",
    "https://management.azure.com/subscriptions/0b1f6471-1bf0-4dda-aec3-cb9272f09590" +
    "/resourceGroups/my-rg/providers/Microsoft.CognitiveServices/accounts/my-account",
    # AWS Bedrock, including a percent-encoded inference-profile ARN.
    "https://bedrock-runtime.us-east-1.amazonaws.com/model/" +
    "anthropic.claude-3-5-sonnet-20241022-v2:0/invoke",
    "https://bedrock-runtime.us-east-1.amazonaws.com/model/arn%3Aaws%3Abedrock%3Aus-east-1" +
    "%3A123456789012%3Ainference-profile%2Fus.anthropic.claude-3-7-sonnet-20250219-v1%3A0" +
    "/converse",
    "https://bedrock-runtime.us-west-2.amazonaws.com/openai/v1",
    # Google Vertex AI and Gemini.
    "https://us-central1-aiplatform.googleapis.com/v1/projects/my-project-123456" +
    "/locations/us-central1/endpoints/openapi",
    "https://us-central1-aiplatform.googleapis.com/v1beta1/projects/my-project" +
    "/locations/us-central1/endpoints/1234567890123456789",
    "https://generativelanguage.googleapis.com/v1beta/openai/",
    # Cloudflare: a 32-hex-digit account ID in the path.
    "https://gateway.ai.cloudflare.com/v1/0123456789abcdef0123456789abcdef/my-gateway/openai",
    "https://api.cloudflare.com/client/v4/accounts/0123456789abcdef0123456789abcdef/ai/v1",
    # Databricks, IBM watsonx (UUID deployment), Snowflake Cortex.
    "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com/serving-endpoints",
    "https://us-south.ml.cloud.ibm.com/ml/v1/deployments/" +
    "0b1f6471-1bf0-4dda-aec3-cb9272f09590/text/generation",
    "https://myaccount.snowflakecomputing.com/api/v2/cortex/inference:complete",
    # #2735 review: Azure deployment names split by `-` into several short
    # pieces, which mix cases and digits like a token would.
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "o3-mini-2025-01-31-DataZone-Std-EUW-Prod2",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "Llama-3-70B-FP8-AWQ-TP4-PP2-H100-X8-EU-W2",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "gpt-4o-2024-08-06-GlobalStd-WestEU-Prod-01",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "DeepSeek-R1-Distill-Qwen-32B-AWQ-4bit-A100x2",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "Phi-4-mini-Instruct-128k-Q4KM-GGUF-v2-Team7",
    "https://my-resource.openai.azure.com/openai/deployments/" +
    "Mistral_Large_2411_PTU_100_FranceC_Dev_A1b2",
    # Parts that hold a key prefix's letters inside a word, not a key.
    "https://api.provider.invalid/task-skills-and-more/v1",
    "https://api.provider.invalid/desk-assistant-production/v1",
    "https://api.provider.invalid/models/my-sk-learn-regression-model/v1",
    "https://api.provider.invalid/tenants/team-hf_hub_mirror/v1",
]


def test_an_overlong_base_url_is_refused_and_withheld_before_decoding(tmp_path: Path) -> None:
    """#2735 review: past the cap nothing is decoded or scanned."""
    from okg.substrate.chat.block_scaffold import (
        BASE_URL_MAX_LENGTH,
        base_url_masked_parts,
        masked_base_url,
    )

    url = "https://api.provider.invalid/v1;x=" + "%25" * BASE_URL_MAX_LENGTH + URL_SECRET

    with pytest.raises(ChatManifestError) as excinfo:
        _load_with_provider(tmp_path, {"base_url": url})

    assert excinfo.value.data["shape"] == "too_long"
    assert URL_SECRET not in json.dumps(excinfo.value.to_json(), default=str)
    assert masked_base_url(url) == "<withheld>"
    assert base_url_masked_parts(url) == [url]
    base = "https://api.provider.invalid/v1/"
    at_cap = (base + "models/" * BASE_URL_MAX_LENGTH)[:BASE_URL_MAX_LENGTH]
    assert len(at_cap) == BASE_URL_MAX_LENGTH
    assert _load_with_provider(tmp_path / "at-cap", {"base_url": at_cap}).chat.provider.base_url == at_cap


@pytest.mark.parametrize("url", REAL_BASE_URLS)
def test_a_clean_base_url_still_loads_unchanged(tmp_path: Path, url: str) -> None:
    from okg.substrate.chat.block_scaffold import masked_base_url

    manifest = _load_with_provider(tmp_path, {"base_url": url})

    assert manifest.chat.provider.base_url == url
    # Output masking withholds a query always, and nothing else of these.
    if "?" not in url:
        assert masked_base_url(url) == url
