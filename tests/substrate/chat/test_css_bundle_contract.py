"""The chat CSS bundle as an explicit byte-delivery contract (PACT
``chat-css-bundle-contract``).

The recorded knob census measured a floor failure: Open WebUI v0.11.0 exposes
no styling knob — no config key, no environment variable, no admin API, and no
value the instance can be asked for. The one lever is a static file the vendor
links unconditionally and reads nothing out of. The operator decision was to
admit that lever as an EXPLICIT contract over BYTES rather than pretend it is a
knob, and these tests hold the contract to exactly that claim: *the bytes named
by the manifest are the bytes the instance serves*, and nothing more.

Every arm drives the real
:func:`okg.substrate.chat.css_bundle.sync_css_bundle` against the measured
doubles — :class:`~tests.substrate.chat.fake_openwebui.FakeInstance` for the
HTTP readback and :class:`FakeContainerRuntime` for the container filesystem —
so the assertions are on the WIRE: which argv the container received, which
bytes the filesystem holds, and which digest ``GET /static/custom.css``
answers with.

Two properties of the doubles carry most of the weight here. The runtime
answers **127** to any argv it does not recognize, so a precondition or a
removal proof that stopped measuring anything would fail rather than pass; and
a write to the frontend build's ``build/static`` lands on the filesystem and is
served by nothing, exactly as the census measured, so a delivery aimed at the
wrong directory cannot pass here and fail live.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.data import chat_data_dir
from okg.substrate.chat.css_bundle import (
    CONTAINER_CSS_PATH,
    CUSTOM_NAME_VAR,
    MAX_BUNDLE_BYTES,
    REFUSAL_BUNDLE_TOO_LARGE,
    REFUSAL_BUNDLE_UNREADABLE,
    REFUSAL_CONTAINER_ABSENT,
    REFUSAL_CUSTOM_NAME_SET,
    REFUSAL_CUSTOM_NAME_UNDETERMINED,
    REFUSAL_NO_CONTAINER_RUNTIME,
    REFUSAL_REMOVAL_UNVERIFIED,
    REFUSAL_SHIP_STATE_UNREADABLE,
    REFUSAL_STALE_APPROVAL_VERSION,
    SERVED_CSS_PATH,
    STATE_NOT_DECLARED,
    STATE_RECONVERGED,
    STATE_REFUSED,
    STATE_RETRACTED,
    STATE_SHIPPED,
    STATE_VERIFIED,
    STATIC_DIR,
    ChatCssBundleError,
    read_ship_record,
    ship_state_path,
    sync_css_bundle,
)
from okg.substrate.chat.sync import (
    ChatSyncError,
    InstanceClient,
    SyncInputs,
    sync,
)
from okg.substrate.deployments.chat_block import (
    SHIPPED_UI_KEYS,
    ChatCssBundle,
    ChatManifestError,
    parse_chat_block,
    resolve_theme_accent,
)

from .fake_openwebui import BUILD_STATIC_DIR, FakeContainerRuntime, FakeInstance

REPO_ROOT = Path(__file__).resolve().parents[3]
TOKEN = "fake-admin-token-value-0123456789"
BASE_URL = "http://instance.test"
CONTAINER = "okg-chat-fixture"
PINNED = "0.11.4"

#: The declared bundle's bytes. ASCII on purpose: the fake's static route
#: carries assets through a text seam, so a binary payload would fail for a
#: reason that has nothing to do with the contract.
DECLARED_CSS = b":root { --okg-brand: #10b981; }\n.chat-shell { padding: 8px; }\n"
TAMPERED_CSS = b":root { --okg-brand: #ff0000; }\n"

#: The seven operators a real `okg mcp-serve` registers, measured live.
LIVE_TOOLS = ("aggregate", "expand", "filter", "inspect", "map", "query", "search")


# ---------------------------------------------------------------------------
# Fixture deployment
# ---------------------------------------------------------------------------

def _raw_manifest(*, css_bundle: dict[str, Any] | None) -> dict[str, Any]:
    chat: dict[str, Any] = {
        "enabled": True,
        "ui": {
            "site_name": "Fixture Site",
            "banner": "Answers are pinned to one published generation.",
            "features": {
                "web_search": False,
                "image_generation": False,
                "code_interpreter": False,
                "arena": False,
                "channels": False,
                "byok": False,
                "context_compaction": True,
            },
        },
        "models": {"builtin": ["base/model-a"], "default": "base/model-a"},
        "preset": {
            "display_name": "Fixture Preset",
            "description": "Ask the fixture graph.",
            "system_prompt_ref": "chat/system_prompt.md",
            "suggestions": ["What changed last week?"],
        },
        "search_profile": "analyst",
        "mcp": {
            "port": 8100,
            "tools": [
                "search", "inspect", "expand", "filter", "map",
                "aggregate", "query",
            ],
        },
        "access": {"groups": ["okg-users"]},
        "deep_links": {
            "operator_ui_base": "http://console.test",
            "chat_site_base": "http://chat.test",
        },
    }
    if css_bundle is not None:
        chat["css_bundle"] = css_bundle
    return {
        "name": "fixture",
        "nomos": {"runtime_enforcement": "enforce"},
        "search": {"profiles": {"analyst": {"subtypes": ["document"]}}},
        "chat": chat,
    }


def _deployment_dir(tmp_path: Path, *, css: bytes | None = DECLARED_CSS) -> Path:
    dep_dir = tmp_path / "deployment"
    (dep_dir / "chat").mkdir(parents=True, exist_ok=True)
    (dep_dir / "chat" / "system_prompt.md").write_text(
        "Use the OKG tools. Quote node ids.", encoding="utf-8",
    )
    if css is not None:
        (dep_dir / "chat" / "custom.css").write_bytes(css)
    return dep_dir


def _bundle(version: str = PINNED, ref: str = "chat/custom.css") -> ChatCssBundle:
    return ChatCssBundle(ref=ref, approved_for_version=version)


def _instance(**kwargs: Any) -> FakeInstance:
    instance = FakeInstance(**kwargs)
    instance.add_group("okg-users", "group-users")
    instance.install_function(
        "okg_review_queue",
        valve_fields=("deployment", "openwebui_api_base", "admin_group"),
    )
    return instance


def _client(instance: FakeInstance) -> InstanceClient:
    return InstanceClient(BASE_URL, TOKEN, opener=instance.opener)


def _served(instance: FakeInstance) -> bytes | None:
    return _client(instance).get_bytes(SERVED_CSS_PATH)


def _run(
    *,
    tmp_path: Path,
    instance: FakeInstance,
    runtime: Any,
    css_bundle: ChatCssBundle | None,
    dep_dir: Path | None = None,
    running_version: str = PINNED,
    state_root: Path | None = None,
) -> Any:
    client = _client(instance)
    return sync_css_bundle(
        deployment="fixture",
        deployment_dir=dep_dir if dep_dir is not None else _deployment_dir(tmp_path),
        css_bundle=css_bundle,
        running_version=running_version,
        container=CONTAINER,
        runtime=runtime,
        read_served=lambda: client.get_bytes(SERVED_CSS_PATH),
        state_root=state_root if state_root is not None else tmp_path / "state",
    )


@pytest.fixture()
def instance() -> FakeInstance:
    return _instance()


@pytest.fixture()
def runtime(instance: FakeInstance) -> FakeContainerRuntime:
    return FakeContainerRuntime(instance, container=CONTAINER)


# ---------------------------------------------------------------------------
# req.chat-css-bundle-contract.delivery
# scn.chat-css-bundle-contract.roundtrip
# ---------------------------------------------------------------------------

def test_declared_bytes_are_shipped_and_read_back_byte_identical(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The first run delivers, and the readback proves byte identity."""
    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(),
    )

    assert outcome.state == STATE_SHIPPED
    assert outcome.ok and outcome.delivered
    assert outcome.byte_identical is True
    assert outcome.drift is False
    # The claim is a DIGEST comparison, not a value readback.
    assert outcome.declared_sha256 == outcome.observed_sha256
    assert outcome.declared_bytes == len(DECLARED_CSS)
    # On the wire: the bytes are on the container filesystem at the served
    # directory, and the instance answers with them.
    assert runtime.files[CONTAINER_CSS_PATH] == DECLARED_CSS
    assert _served(instance) == DECLARED_CSS


def test_the_delivery_targets_the_served_directory_not_the_frontend_build(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The census GOTCHA, asserted rather than trusted.

    A 165-byte file written to the frontend build's ``build/static`` was
    measured serving nothing; the same file under ``STATIC_DIR`` served
    immediately. A delivery aimed at the wrong directory would look like a
    clean write and change nothing at all.
    """
    _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(),
    )

    assert CONTAINER_CSS_PATH.startswith(STATIC_DIR + "/")
    assert not any(
        path.startswith(BUILD_STATIC_DIR) for path in runtime.files
    ), "the delivery wrote into the frontend build directory, which serves nothing"


def test_a_second_run_with_no_change_writes_nothing(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """Idempotence, decided by the READBACK and not by the recorded state."""
    dep_dir = _deployment_dir(tmp_path)
    _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )
    writes_after_first = [c for c in runtime.calls if c[:2] == ("sh", "-c")]

    second = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )

    assert second.state == STATE_VERIFIED
    assert second.byte_identical is True
    assert second.delivered is False
    assert [c for c in runtime.calls if c[:2] == ("sh", "-c")] == writes_after_first


def test_a_tampered_file_is_reported_as_drift_and_reconverged(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """scn.roundtrip: ship, tamper, run again — drift is named, then closed."""
    dep_dir = _deployment_dir(tmp_path)
    first = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )
    assert first.byte_identical is True

    runtime.tamper(CONTAINER_CSS_PATH, TAMPERED_CSS)
    assert _served(instance) == TAMPERED_CSS

    second = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )

    assert second.state == STATE_RECONVERGED
    assert second.drift is True
    assert second.drift_detail and first.declared_sha256 in second.drift_detail
    # The report names WHAT was found, not just that something was wrong.
    assert second.observed_before_sha256 != second.declared_sha256
    assert second.byte_identical is True
    assert second.observed_sha256 == second.declared_sha256
    assert _served(instance) == DECLARED_CSS


def test_bytes_from_outside_this_contract_are_reported_before_being_replaced(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """A file this deployment never delivered is not overwritten silently."""
    runtime.tamper(CONTAINER_CSS_PATH, b".hand-applied { color: red; }\n")

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(),
    )

    assert outcome.state == STATE_RECONVERGED
    assert outcome.drift is True
    assert "no recorded delivery" in (outcome.drift_detail or "")
    assert outcome.recorded_ship_sha256 is None
    assert _served(instance) == DECLARED_CSS


# ---------------------------------------------------------------------------
# scn.chat-css-bundle-contract.retract
# ---------------------------------------------------------------------------

def test_retracting_the_declaration_removes_the_shipped_bytes(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """Declared-then-retracted removes the delivery and says so."""
    dep_dir = _deployment_dir(tmp_path)
    state_root = tmp_path / "state"
    shipped = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir, state_root=state_root,
    )
    assert shipped.byte_identical is True

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=None, dep_dir=dep_dir, state_root=state_root,
    )

    assert outcome.state == STATE_RETRACTED
    assert outcome.ok and outcome.removed
    assert outcome.vendor_default_restored is True
    # The report NAMES the retraction rather than leaving it to be inferred
    # from a missing field.
    assert any("retraction is not the same" in note for note in outcome.notes)
    assert outcome.recorded_ship_sha256 == shipped.declared_sha256
    # The vendor default is the absence of a delivered file: nothing on the
    # container filesystem, and nothing served.
    assert CONTAINER_CSS_PATH not in runtime.files
    assert _served(instance) is None
    # The record is forgotten, so a third run is a plain never-declared run.
    assert read_ship_record("fixture", state_root=state_root) is None


def test_a_removal_the_runtime_only_claims_is_refused(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """`rm` exiting zero is not evidence the file is gone.

    Two witnesses are required — the container filesystem and the served
    readback — because a readback that could not reach the instance also
    returns nothing, and that reading would turn a failed removal into a
    reported success.
    """
    state_root = tmp_path / "state"
    dep_dir = _deployment_dir(tmp_path)
    _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir, state_root=state_root,
    )
    runtime.removal_lies = True

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=None, dep_dir=dep_dir, state_root=state_root,
    )

    assert outcome.state == STATE_REFUSED
    assert outcome.refusal_code == REFUSAL_REMOVAL_UNVERIFIED
    assert outcome.vendor_default_restored is False
    assert "STILL PRESENT" in (outcome.refusal or "")
    # The record is deliberately KEPT while the removal is unproved, so the
    # next run retries the retraction instead of forgetting the bytes.
    assert read_ship_record("fixture", state_root=state_root) is not None


def test_never_declared_leaves_the_static_surface_untouched(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The one state that reads nothing and writes nothing."""
    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime, css_bundle=None,
    )

    assert outcome.state == STATE_NOT_DECLARED
    assert outcome.ok
    assert outcome.delivered is False and outcome.removed is False
    # Untouched, asserted on the wire in both directions: no command reached
    # the container, and the instance was never even asked for the file.
    assert runtime.calls == []
    assert not any(path == SERVED_CSS_PATH for _method, path in instance.requests)
    assert not ship_state_path("fixture", state_root=tmp_path / "state").exists()


def test_an_unreadable_ship_record_refuses_rather_than_reading_as_never_shipped(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """Absence and unreadability are different facts.

    Read as "this deployment never delivered anything", a corrupt record turns
    a retraction into a no-op and strands the delivered bytes on the instance.
    """
    state_root = tmp_path / "state"
    path = ship_state_path("fixture", state_root=state_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(ChatCssBundleError) as excinfo:
        _run(
            tmp_path=tmp_path, instance=instance, runtime=runtime,
            css_bundle=None, state_root=state_root,
        )

    assert excinfo.value.data["reason"] == REFUSAL_SHIP_STATE_UNREADABLE
    assert str(path) in excinfo.value.data["path"]


# ---------------------------------------------------------------------------
# req.chat-css-bundle-contract.fragility-gate
# scn.chat-css-bundle-contract.pin-bump
# ---------------------------------------------------------------------------

def test_a_vendor_pin_bump_refuses_and_clears_the_owned_bytes(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The refusal names the stale approval AND defines the served state."""
    dep_dir = _deployment_dir(tmp_path)
    state_root = tmp_path / "state"
    shipped = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(PINNED), dep_dir=dep_dir, state_root=state_root,
    )
    assert shipped.byte_identical is True

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(PINNED), dep_dir=dep_dir, state_root=state_root,
        running_version="0.12.0",
    )

    assert outcome.state == STATE_REFUSED
    assert outcome.ok is False
    assert outcome.refusal_code == REFUSAL_STALE_APPROVAL_VERSION
    # The stale approval version and the running one are both NAMED, so the
    # operator does not have to work out which pin the bytes were checked at.
    assert repr(PINNED) in (outcome.refusal or "")
    assert repr("0.12.0") in (outcome.refusal or "")
    # The refusal window has a defined SERVED state: the vendor default.
    assert outcome.removed is True
    assert outcome.vendor_default_restored is True
    assert CONTAINER_CSS_PATH not in runtime.files
    assert _served(instance) is None
    # Re-approving against the new pin is the whole remedy, and the run says so.
    assert "0.12.0" in (outcome.fix_hint or "")


def test_a_pin_bump_with_nothing_delivered_refuses_without_inventing_a_removal(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """No recorded delivery means there are no owned bytes to clear."""
    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(PINNED), running_version="0.12.0",
    )

    assert outcome.refusal_code == REFUSAL_STALE_APPROVAL_VERSION
    assert outcome.removed is False
    assert "no recorded delivery" in (outcome.refusal or "")


def test_re_approving_against_the_new_pin_ships_again(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """Editing ``approved_for_version`` IS the re-approval act."""
    dep_dir = _deployment_dir(tmp_path)
    state_root = tmp_path / "state"
    _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(PINNED), dep_dir=dep_dir, state_root=state_root,
        running_version="0.12.0",
    )

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle("0.12.0"), dep_dir=dep_dir, state_root=state_root,
        running_version="0.12.0",
    )

    assert outcome.state == STATE_SHIPPED
    assert outcome.byte_identical is True
    assert _served(instance) == DECLARED_CSS


# ---------------------------------------------------------------------------
# scn.chat-css-bundle-contract.custom-name
# ---------------------------------------------------------------------------

def test_a_set_custom_name_refuses_and_names_the_overwrite_hazard(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    """A third party writing into the same directory makes delivery ungoverned."""
    hostile = FakeContainerRuntime(
        instance, container=CONTAINER, env={CUSTOM_NAME_VAR: "acme"},
    )

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=hostile,
        css_bundle=_bundle(),
    )

    assert outcome.state == STATE_REFUSED
    assert outcome.refusal_code == REFUSAL_CUSTOM_NAME_SET
    assert CUSTOM_NAME_VAR in (outcome.refusal or "")
    assert "api.openwebui.com" in (outcome.refusal or "")
    assert STATIC_DIR in (outcome.refusal or "")
    # Refused BEFORE anything was written, and nothing was removed either.
    assert outcome.delivered is False and outcome.removed is False
    assert CONTAINER_CSS_PATH not in hostile.files


def test_an_empty_custom_name_is_the_inert_case_and_delivery_proceeds(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    """The vendor's branding fetch is skipped when the value is empty."""
    benign = FakeContainerRuntime(
        instance, container=CONTAINER, env={CUSTOM_NAME_VAR: ""},
    )

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=benign,
        css_bundle=_bundle(),
    )

    assert outcome.state == STATE_SHIPPED
    assert any("inert" in note for note in outcome.notes)


def test_an_undeterminable_custom_name_refuses_rather_than_assuming_unset(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    """"I could not find out" is never "there is nothing there"."""

    class _Mute(FakeContainerRuntime):
        def execute(self, container: str, argv: Any) -> tuple[int, str, str]:
            if list(argv)[:1] == ["printenv"]:
                return 126, "", "OCI runtime exec failed"
            return super().execute(container, argv)

    mute = _Mute(instance, container=CONTAINER)

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=mute, css_bundle=_bundle(),
    )

    assert outcome.refusal_code == REFUSAL_CUSTOM_NAME_UNDETERMINED
    assert outcome.delivered is False
    assert CONTAINER_CSS_PATH not in mute.files


# ---------------------------------------------------------------------------
# The remaining refusals: no fallback anywhere
# ---------------------------------------------------------------------------

def test_a_declared_bundle_with_no_container_runtime_refuses(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    """A skipped delivery reads exactly like a completed one everywhere else."""
    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=None,
        css_bundle=_bundle(),
    )

    assert outcome.refusal_code == REFUSAL_NO_CONTAINER_RUNTIME
    assert STATIC_DIR in (outcome.refusal or "")


def test_an_absent_container_refuses(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    gone = FakeContainerRuntime(instance, container=CONTAINER, present=False)

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=gone, css_bundle=_bundle(),
    )

    assert outcome.refusal_code == REFUSAL_CONTAINER_ABSENT


def test_a_declared_file_that_cannot_be_read_refuses(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """A declared asset that is missing is a broken declaration, not an absent one."""
    dep_dir = _deployment_dir(tmp_path, css=None)

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )

    assert outcome.refusal_code == REFUSAL_BUNDLE_UNREADABLE
    assert CONTAINER_CSS_PATH not in runtime.files


def test_a_bundle_over_the_delivery_limit_refuses_rather_than_truncating(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The bytes ride in an exec argv, which the kernel bounds."""
    dep_dir = _deployment_dir(tmp_path, css=b"/*" + b"x" * (MAX_BUNDLE_BYTES + 1) + b"*/")

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(), dep_dir=dep_dir,
    )

    assert outcome.refusal_code == REFUSAL_BUNDLE_TOO_LARGE
    assert CONTAINER_CSS_PATH not in runtime.files


def test_a_write_that_does_not_read_back_is_never_counted_as_delivered(
    tmp_path: Path, instance: FakeInstance,
) -> None:
    """The census GOTCHA as a FAILURE, not as a passing test.

    A delivery that reported success while the served bytes stayed empty is
    exactly what writing to the frontend build directory produces.
    """

    class _WrongDirectory(FakeContainerRuntime):
        def _write(self, path: str, payload: bytes) -> None:
            super()._write(f"{BUILD_STATIC_DIR}/custom.css", payload)

    wrong = _WrongDirectory(instance, container=CONTAINER)

    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=wrong, css_bundle=_bundle(),
    )

    assert outcome.state == STATE_REFUSED
    assert outcome.refusal_code == "ship_unverified"
    assert outcome.byte_identical is False
    assert _served(instance) is None


# ---------------------------------------------------------------------------
# The report says BYTES, never a styling value
# ---------------------------------------------------------------------------

def test_the_report_describes_byte_delivery_and_claims_no_styling_value(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The whole reason this surface has its own name.

    The census measured that the instance cannot be asked what it is styled
    like, so a report that used the vocabulary of a styling knob would be
    claiming a readback that does not exist.
    """
    outcome = _run(
        tmp_path=tmp_path, instance=instance, runtime=runtime,
        css_bundle=_bundle(),
    )
    payload = outcome.to_dict()
    rendered = json.dumps(payload, sort_keys=True).lower()

    assert payload["contract"] == "byte_delivery"
    assert "bytes" in payload["claim"]
    assert payload["semantic_readback"].startswith("none")
    for banned in ("theme", "accent", "colour", "color"):
        assert banned not in rendered, (
            f"the delivery report used {banned!r}; this surface delivers "
            "bytes and can never report a styling value"
        )
    # And the fields that ARE there are all facts about bytes.
    assert payload["declared_sha256"] == payload["observed_sha256"]
    assert payload["declared_bytes"] == len(DECLARED_CSS)


# ---------------------------------------------------------------------------
# SEAM with chat-manifest-block: the schema, and the refusal that stays
# ---------------------------------------------------------------------------

def _manifest_with(css_bundle: Any) -> dict[str, Any]:
    return _raw_manifest(css_bundle=css_bundle)


def test_the_css_bundle_sub_block_parses_into_the_chat_block() -> None:
    block = parse_chat_block(
        _manifest_with({"ref": "chat/custom.css", "approved_for_version": "0.11.0"})
    )
    assert block is not None and block.css_bundle is not None
    assert block.css_bundle.ref == "chat/custom.css"
    assert block.css_bundle.approved_for_version == "0.11.0"


def test_an_absent_sub_block_is_the_never_declared_state() -> None:
    """No default is manufactured: ``None`` is the state the sync side needs."""
    block = parse_chat_block(_manifest_with(None))
    assert block is not None and block.css_bundle is None


@pytest.mark.parametrize(
    "declared",
    [
        pytest.param({"ref": "chat/custom.css"}, id="no-approval-version"),
        pytest.param({"approved_for_version": "0.11.0"}, id="no-ref"),
        pytest.param(
            {
                "ref": "chat/custom.css",
                "approved_for_version": "0.11.0",
                "approved_for_versoin": "0.11.0",
            },
            id="typo'd-key",
        ),
        pytest.param(
            {"ref": "/etc/passwd.css", "approved_for_version": "0.11.0"},
            id="absolute-path",
        ),
        pytest.param(
            {"ref": "../../secrets.css", "approved_for_version": "0.11.0"},
            id="escaping-path",
        ),
        pytest.param(
            {"ref": "chat/logo.png", "approved_for_version": "0.11.0"},
            id="not-a-stylesheet",
        ),
        pytest.param(
            {"ref": "chat/custom.css", "approved_for_version": ""},
            id="empty-approval-version",
        ),
        pytest.param("chat/custom.css", id="not-a-mapping"),
    ],
)
def test_the_sub_block_is_strict_at_every_level(declared: Any) -> None:
    with pytest.raises(ChatManifestError):
        parse_chat_block(_manifest_with(declared))


def test_chat_ui_theme_still_refuses_by_presence(tmp_path: Path) -> None:
    """The byte contract does NOT reopen the knob the census refused.

    ``chat.ui.theme`` is refused in both paths that can see it — the parser
    rejects the field, and the resolver refuses to hand any consumer a value
    from it — and the arrival of a byte-delivery surface changes neither.
    """
    manifest = _manifest_with(
        {"ref": "chat/custom.css", "approved_for_version": "0.11.0"}
    )
    manifest["chat"]["ui"]["theme"] = {"accent": "#10b981"}

    with pytest.raises(ChatManifestError) as parsed:
        parse_chat_block(manifest)
    assert "not drivable" in str(parsed.value).lower()

    with pytest.raises(ChatManifestError) as resolved:
        resolve_theme_accent(manifest)
    assert "could never render" in str(resolved.value)

    # And the census-bound ui field set is unchanged by this change.
    assert "theme" not in SHIPPED_UI_KEYS
    assert "css_bundle" not in SHIPPED_UI_KEYS


# ---------------------------------------------------------------------------
# The wiring: `okg chat sync` carries the surface
# ---------------------------------------------------------------------------

def _sync_inputs(tmp_path: Path, *, css_bundle: dict[str, Any] | None) -> SyncInputs:
    raw = _raw_manifest(css_bundle=css_bundle)
    dep_dir = _deployment_dir(tmp_path)
    return SyncInputs(
        deployment="fixture",
        deployment_dir=dep_dir,
        raw_manifest=raw,
        chat=parse_chat_block(raw, manifest_path=dep_dir / "deployment.yaml"),
        mcp_instructions_path=None,
        chat_data_dir=chat_data_dir(),
        mcp_probe_url="http://127.0.0.1:8100/mcp",
        mcp_instance_url="http://host.docker.internal:8100/mcp",
        preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
        state_home=tmp_path / "state",
    )


def _probe(_endpoint: str, _token_env: str | None) -> tuple[str, ...]:
    return LIVE_TOOLS


def test_a_full_sync_run_delivers_the_bundle_and_reports_it_separately(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """The surface is reachable through the verb, and is not one of the five.

    ``written_surfaces`` counts surfaces that write a VALUE and read that
    value back. This one can only ever assert byte identity, so it is reported
    under its own key — folding it in would let "the file matches" read as
    "the styling is what the manifest says".
    """
    inputs = _sync_inputs(
        tmp_path,
        css_bundle={"ref": "chat/custom.css", "approved_for_version": PINNED},
    )

    report = sync(
        inputs,
        _client(instance),
        probe_tools=_probe,
        container_runtime=runtime,
        container=CONTAINER,
    )

    assert report.css_bundle is not None
    assert report.css_bundle.state == STATE_SHIPPED
    assert report.css_bundle.byte_identical is True
    assert runtime.files[CONTAINER_CSS_PATH] == DECLARED_CSS
    payload = report.to_dict()
    assert payload["css_bundle"]["contract"] == "byte_delivery"
    assert "css_bundle" not in payload["written_surfaces"]
    assert "css_bundle" not in report.surfaces


def test_a_full_sync_run_without_a_declaration_reports_the_untouched_surface(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """Present on every run: a surface that vanishes cannot be audited."""
    report = sync(
        _sync_inputs(tmp_path, css_bundle=None),
        _client(instance),
        probe_tools=_probe,
        container_runtime=runtime,
        container=CONTAINER,
    )

    assert report.css_bundle is not None
    assert report.css_bundle.state == STATE_NOT_DECLARED
    assert report.to_dict()["css_bundle"]["state"] == STATE_NOT_DECLARED
    assert runtime.calls == []


def test_a_sync_run_fails_when_the_declared_bundle_is_refused(
    tmp_path: Path, instance: FakeInstance, runtime: FakeContainerRuntime,
) -> None:
    """A declared surface that could not be applied fails the run.

    And the partial report still carries what the refusal DID to the instance,
    because the pin-bump arm removes bytes as part of refusing.
    """
    inputs = _sync_inputs(
        tmp_path,
        css_bundle={"ref": "chat/custom.css", "approved_for_version": "0.9.0"},
    )

    with pytest.raises(ChatSyncError) as excinfo:
        sync(
            inputs,
            _client(instance),
            probe_tools=_probe,
            container_runtime=runtime,
            container=CONTAINER,
        )

    assert excinfo.value.data["reason"] == REFUSAL_STALE_APPROVAL_VERSION
    assert excinfo.value.data["surface"] == "css_bundle"
    partial = getattr(excinfo.value, "partial_report", None)
    assert partial is not None
    assert partial.css_bundle.refusal_code == REFUSAL_STALE_APPROVAL_VERSION
    assert partial.ok is False
