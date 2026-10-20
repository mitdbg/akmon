"""The chat layer's own lifecycle (PACT ``chat-layer-lifecycle``, slice S5 of #1183).

Two halves, both hermetic.

**The manifest fields.** ``chat.image`` and ``chat.instance_port`` are parsed
by the real ``parse_chat_block`` and resolved by the real instance layer, so a
schema that stopped admitting either fails here rather than in a container.
The tested-image set is an INJECTION point in every arm that needs a second
version: the shipped set holds one entry, and an arm that could only be
written by editing the shipped constant would be a test of the edit.

**The receipts.** Every arm drives the real builders against
:class:`tests.substrate.chat.fake_openwebui.FakeInstance`, and the retire arms
run against an instance ``okg chat sync`` really configured through the same
fake — so "the surfaces are gone" is measured after they were genuinely there,
not asserted about an instance that never had them.

Three assertions here could not be written another way:

* no receipt starts or stops anything. The fake records method and path for
  every call; the upgrade and rollback arms assert the request log is empty.
* an unreachable instance must not read as a retired one. That arm makes the
  connection listing fail on the wire and requires a refusal.
* no secret reaches a receipt. The MCP token is built at run time and searched
  for in the written bytes.
"""

from __future__ import annotations

import json
import uuid
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.chat.instance import (
    PINNED_IMAGE_REF,
    PINNED_VERSION,
    TESTED_IMAGES,
    ChatImageNotTested,
    ChatInstanceError,
    PgTarget,
    resolve_instance_image,
    resolve_instance_port,
)
from okg.substrate.chat.instance import (
    # Aliased: pytest collects any module-level name beginning `test`, so the
    # imported helper would be gathered as a test function and error on its
    # required argument.
    tested_image_version as version_of_tested_image,
)
from okg.substrate.chat.lifecycle import (
    CHAT_LIFECYCLE_SCHEMA,
    KIND_RETIRE,
    KIND_ROLLBACK,
    KIND_UPGRADE,
    ChatLifecycleAppDatabaseChanged,
    ChatLifecycleBaselineUnreconciled,
    ChatLifecycleError,
    ChatLifecycleOutputPathExists,
    ChatLifecycleSurfaceRemains,
    load_lifecycle_receipt,
    observe_okg_surfaces,
    retire_receipt,
    rollback_receipt,
    upgrade_receipt,
    write_lifecycle_receipt,
)
from okg.substrate.chat.plan_apply import (
    chat_readback,
    load_chat_plan,
    write_chat_plan,
)
from okg.substrate.chat.sync import sync
from okg.substrate.deployments.chat_block import (
    ChatManifestError,
    parse_chat_block,
)
from tests.substrate.chat.fake_openwebui import FakeInstance
from tests.substrate.test_chat_plan_apply_readback import (
    BASE_URL,
    MCP_TOKEN_ENV,
    TOKEN,
    _client,
    _instance,
    _make_inputs,
    _probe,
    _raw_manifest,
)

#: A SECOND tested image, supplied to the arms that need a version change.
#: The shipped set has one entry, honestly, so every two-version arm injects
#: its own catalogue rather than editing the measured constant.
NEXT_IMAGE = "ghcr.io/open-webui/open-webui:v0.12.0"
NEXT_VERSION = "0.12.0"
TWO_TESTED = {PINNED_IMAGE_REF: PINNED_VERSION, NEXT_IMAGE: NEXT_VERSION}

APP_DB = PgTarget(host="127.0.0.1", port=5432, database="okg_chat_fixture")
OTHER_DB = PgTarget(host="127.0.0.1", port=5432, database="okg_chat_moved")
DEPLOYMENT = "fixture"


@pytest.fixture
def mcp_token(monkeypatch: pytest.MonkeyPatch) -> str:
    """A credential built at run time, so no literal can match it by accident."""
    value = "mcp-" + uuid.uuid4().hex
    monkeypatch.setenv(MCP_TOKEN_ENV, value)
    return value


def _block(**chat_overrides: Any):
    raw = _raw_manifest(**chat_overrides)
    return parse_chat_block(raw, manifest_path=None)


# ---------------------------------------------------------------------------
# (a) chat.image, chat.instance_port and the tested-image allow-list
# ---------------------------------------------------------------------------

def test_the_manifest_declares_the_image_and_the_port() -> None:
    """req.chat-layer-lifecycle.manifest-fields."""
    block = _block(image=PINNED_IMAGE_REF, instance_port=8080)
    assert block.image == PINNED_IMAGE_REF
    assert block.instance_port == 8080
    # And absence stays absence, not a manufactured default.
    bare = _block()
    assert bare.image is None
    assert bare.instance_port is None


@pytest.mark.parametrize(
    "bad", ["ghcr.io/open-webui/open-webui:latest", "open-webui", "", 7],
)
def test_an_unpinned_image_refuses_at_load(bad: Any) -> None:
    """req.chat-layer-lifecycle.manifest-fields."""
    with pytest.raises(ChatManifestError) as caught:
        _block(image=bad)
    assert caught.value.data["path"] == "chat.image"


@pytest.mark.parametrize("bad", [0, 70000, True, "8080"])
def test_a_port_that_is_not_a_tcp_port_refuses_at_load(bad: Any) -> None:
    with pytest.raises(ChatManifestError) as caught:
        _block(instance_port=bad)
    assert caught.value.data["path"] == "chat.instance_port"


def test_an_image_outside_the_tested_set_refuses_naming_it() -> None:
    """req.chat-layer-lifecycle.image-allow-list.

    The refusal must NAME the tested set: "not tested" without saying what is
    leaves an operator guessing which reference to write.
    """
    with pytest.raises(ChatImageNotTested) as caught:
        resolve_instance_image(DEPLOYMENT, _block(image=NEXT_IMAGE))
    assert caught.value.code == "chat_image_not_tested"
    assert caught.value.data["reason"] == "image_not_tested"
    assert caught.value.data["tested"] == sorted(TESTED_IMAGES)
    assert PINNED_IMAGE_REF in caught.value.data["tested"]
    # Control: the same image resolves against a set that has measured it.
    plan = resolve_instance_image(
        DEPLOYMENT, _block(image=NEXT_IMAGE), tested_images=TWO_TESTED,
    )
    assert (plan.image, plan.version, plan.source) == (
        NEXT_IMAGE, NEXT_VERSION, "manifest",
    )


def test_the_pin_is_the_allow_lists_default_not_a_hardcode() -> None:
    """req.chat-layer-lifecycle.image-allow-list."""
    plan = resolve_instance_image(DEPLOYMENT, _block())
    assert plan.image == PINNED_IMAGE_REF
    # And the source SAYS it was defaulted; a report claiming `manifest` here
    # would credit the deployment with a decision it never made.
    assert plan.source == "default_pin"
    assert plan.version == TESTED_IMAGES[PINNED_IMAGE_REF]


def test_an_empty_tested_set_admits_nothing() -> None:
    """A vacuous allow-list must refuse, not wave everything through."""
    with pytest.raises(ChatImageNotTested) as caught:
        version_of_tested_image(PINNED_IMAGE_REF, tested_images={})
    assert caught.value.data["reason"] == "tested_set_empty"


def test_an_image_flag_disagreeing_with_the_manifest_refuses() -> None:
    with pytest.raises(ChatInstanceError) as caught:
        resolve_instance_image(
            DEPLOYMENT,
            _block(image=PINNED_IMAGE_REF),
            override=NEXT_IMAGE,
            tested_images=TWO_TESTED,
        )
    assert caught.value.data["reason"] == "image_disagreement"


def test_stand_up_takes_the_port_from_the_manifest_when_no_flag_is_passed() -> None:
    """req.chat-layer-lifecycle.port-precedence."""
    plan = resolve_instance_port(DEPLOYMENT, _block(instance_port=8080))
    assert (plan.instance_port, plan.source) == (8080, "manifest")


def test_the_flag_alone_still_settles_the_port() -> None:
    """req.chat-layer-lifecycle.port-precedence."""
    plan = resolve_instance_port(DEPLOYMENT, _block(), override=8080)
    assert (plan.instance_port, plan.source) == (8080, "operator_flag")


def test_an_agreeing_flag_and_manifest_resolve_and_record_both() -> None:
    plan = resolve_instance_port(
        DEPLOYMENT, _block(instance_port=8080), override=8080,
    )
    assert (plan.instance_port, plan.source) == (8080, "manifest_and_flag_agree")


def test_a_port_flag_disagreeing_with_the_manifest_refuses() -> None:
    """req.chat-layer-lifecycle.port-precedence.

    Neither input silently wins. A site whose address depended on which of
    two disagreeing inputs a reader happened to read has no address.
    """
    with pytest.raises(ChatInstanceError) as caught:
        resolve_instance_port(DEPLOYMENT, _block(instance_port=8080), override=9090)
    assert caught.value.data["reason"] == "instance_port_disagreement"
    assert caught.value.data["declared"] == 8080
    assert caught.value.data["override"] == 9090


# ---------------------------------------------------------------------------
# Shared fixtures for the receipts
# ---------------------------------------------------------------------------

def _reconciled(tmp_path: Path) -> tuple[Any, FakeInstance, dict[str, Any], dict[str, Any]]:
    """A synced instance, its plan, and a clean readback of it."""
    inputs = _make_inputs(tmp_path)
    plan = load_chat_plan(write_chat_plan(inputs, tmp_path / "plan")["plan_path"])
    instance = _instance()
    report = sync(inputs, _client(instance), probe_tools=_probe, expected_plan=plan)
    assert report.ok
    document = chat_readback(inputs, _client(instance), plan)
    assert document["ok"], document["deviations"]
    return inputs, instance, plan, document


def _upgrade(plan: dict[str, Any], readback: dict[str, Any], **over: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "baseline_image": PINNED_IMAGE_REF,
        "candidate_image": NEXT_IMAGE,
        "baseline_app_database": APP_DB,
        "candidate_app_database": APP_DB,
        "plan_digest": plan["plan_digest"],
        "readback": readback,
        "tested_images": TWO_TESTED,
    }
    kwargs.update(over)
    return upgrade_receipt(DEPLOYMENT, **kwargs)


# ---------------------------------------------------------------------------
# (b) the upgrade receipt
# ---------------------------------------------------------------------------

def test_upgrade_receipt_records_both_images_and_an_unchanged_database(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.upgrade-receipt."""
    _inputs, instance, plan, readback = _reconciled(tmp_path)
    before = len(instance.requests)
    receipt = _upgrade(plan, readback)
    assert receipt["schema"] == CHAT_LIFECYCLE_SCHEMA
    assert receipt["kind"] == KIND_UPGRADE
    assert receipt["baseline"] == {
        "image": PINNED_IMAGE_REF, "version": PINNED_VERSION, "tested": True,
    }
    assert receipt["candidate"] == {
        "image": NEXT_IMAGE, "version": NEXT_VERSION, "tested": True,
    }
    assert receipt["app_database"] == APP_DB.as_dict()
    assert receipt["app_database_unchanged"] is True
    assert receipt["plan_digest"] == plan["plan_digest"]
    assert receipt["readback"]["ok"] is True
    assert receipt["readback"]["plan_digest"] == plan["plan_digest"]
    # Nothing ran. Asserted on the wire, not inferred from the absence of a
    # container in this process.
    assert receipt["container_operation"]["performed_by_this_verb"] is False
    assert "okg chat-instance up" in (
        receipt["container_operation"]["expected_operator_command"]
    )
    assert len(instance.requests) == before


def test_upgrade_refuses_a_candidate_outside_the_tested_set(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.upgrade-receipt."""
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    with pytest.raises(ChatLifecycleError) as caught:
        _upgrade(plan, readback, tested_images={PINNED_IMAGE_REF: PINNED_VERSION})
    assert caught.value.code == "chat_image_not_tested"
    assert caught.value.data["role"] == "candidate"
    assert caught.value.data["tested"] == [PINNED_IMAGE_REF]


def test_upgrade_refuses_when_the_app_database_identity_would_change(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.upgrade-receipt."""
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    with pytest.raises(ChatLifecycleAppDatabaseChanged) as caught:
        _upgrade(plan, readback, candidate_app_database=OTHER_DB)
    assert caught.value.code == "chat_lifecycle_app_database_changed"
    assert caught.value.data["baseline"]["database"] == APP_DB.database
    assert caught.value.data["candidate"]["database"] == OTHER_DB.database


def test_upgrade_refuses_a_baseline_that_deviates_from_its_plan(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.upgrade-receipt.

    The deviation is REAL: the site name is changed on the instance behind
    the plan's back, and the readback that then refuses is the one handed to
    the receipt.
    """
    inputs, instance, plan, _clean = _reconciled(tmp_path)
    instance.webui_name = "Renamed Out Of Band"
    drifted = chat_readback(inputs, _client(instance), plan)
    assert not drifted["ok"]
    with pytest.raises(ChatLifecycleBaselineUnreconciled) as caught:
        _upgrade(plan, drifted)
    assert caught.value.code == "chat_lifecycle_baseline_unreconciled"
    assert caught.value.data["reason"] == "baseline_deviates_from_plan"
    assert caught.value.data["deviations"] >= 1
    assert "instance_ui" in caught.value.data["surfaces"]


def test_upgrade_refuses_a_candidate_identical_to_the_baseline(
    tmp_path: Path, mcp_token: str,
) -> None:
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    with pytest.raises(ChatLifecycleError) as caught:
        _upgrade(plan, readback, candidate_image=PINNED_IMAGE_REF)
    assert caught.value.data["reason"] == "candidate_not_distinct"


def test_upgrade_refuses_a_readback_taken_against_another_plan(
    tmp_path: Path, mcp_token: str,
) -> None:
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    with pytest.raises(ChatLifecycleError) as caught:
        _upgrade(plan, readback, plan_digest="sha256:not-the-plan-that-was-read")
    assert caught.value.data["reason"] == "readback_plan_mismatch"


def test_upgrade_records_a_baseline_whose_image_left_the_tested_set(
    tmp_path: Path, mcp_token: str,
) -> None:
    """Leaving a retired image is exactly what an allow-list should permit.

    The baseline is recorded as untested rather than refused; refusing would
    strand a deployment on the version it most needs to leave.
    """
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    receipt = _upgrade(plan, readback, tested_images={NEXT_IMAGE: NEXT_VERSION})
    assert receipt["baseline"] == {
        "image": PINNED_IMAGE_REF, "version": None, "tested": False,
    }


# ---------------------------------------------------------------------------
# (c) the rollback receipt
# ---------------------------------------------------------------------------

def test_rollback_receipt_reverses_a_recorded_upgrade(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.rollback-receipt."""
    _inputs, instance, plan, readback = _reconciled(tmp_path)
    upgrade = _upgrade(plan, readback)
    before = len(instance.requests)
    receipt = rollback_receipt(
        DEPLOYMENT,
        upgrade=upgrade,
        current_app_database=APP_DB,
        tested_images=TWO_TESTED,
    )
    assert receipt["kind"] == KIND_ROLLBACK
    assert receipt["candidate"]["image"] == NEXT_IMAGE
    assert receipt["baseline"] == {
        "image": PINNED_IMAGE_REF, "version": PINNED_VERSION, "tested": True,
    }
    assert receipt["app_database"] == APP_DB.as_dict()
    assert receipt["app_database_unchanged"] is True
    assert receipt["reverses_upgrade"] == upgrade["receipt_digest"]
    assert receipt["container_operation"]["performed_by_this_verb"] is False
    assert len(instance.requests) == before


def test_rollback_refuses_a_baseline_retired_from_the_tested_set(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.rollback-receipt.

    The upgrade receipt says the baseline WAS tested; the allow-list is asked
    again at rollback time, and a version retired in between is not a place
    to go back to.
    """
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    upgrade = _upgrade(plan, readback)
    assert upgrade["baseline"]["tested"] is True
    with pytest.raises(ChatLifecycleError) as caught:
        rollback_receipt(
            DEPLOYMENT,
            upgrade=upgrade,
            current_app_database=APP_DB,
            tested_images={NEXT_IMAGE: NEXT_VERSION},
        )
    assert caught.value.code == "chat_image_not_tested"
    assert caught.value.data["role"] == "baseline"


def test_rollback_refuses_a_document_that_is_not_an_upgrade_receipt(
    tmp_path: Path, mcp_token: str,
) -> None:
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    retired = _retire(instance)
    with pytest.raises(ChatLifecycleError) as caught:
        rollback_receipt(
            DEPLOYMENT, upgrade=retired, current_app_database=APP_DB,
        )
    assert caught.value.data["reason"] == "upgrade_receipt_expected"


def test_rollback_refuses_when_the_app_database_moved(
    tmp_path: Path, mcp_token: str,
) -> None:
    _inputs, _instance_, plan, readback = _reconciled(tmp_path)
    with pytest.raises(ChatLifecycleAppDatabaseChanged):
        rollback_receipt(
            DEPLOYMENT,
            upgrade=_upgrade(plan, readback),
            current_app_database=OTHER_DB,
            tested_images=TWO_TESTED,
        )


# ---------------------------------------------------------------------------
# (d) the retire receipt
# ---------------------------------------------------------------------------

def _retire(instance: FakeInstance, **over: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "preset_id": "okg-fixture",
        "mcp_server_id": "okg-fixture",
        "app_database": APP_DB,
    }
    kwargs.update(over)
    return retire_receipt(DEPLOYMENT, _client(instance), **kwargs)


def test_retire_receipt_records_an_absence_it_measured(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.retire-receipt.

    The instance really held the surfaces first — they are removed here the
    way an operator removes them, through the same admin API — so the
    absence is a change that was observed, not a fixture that never had them.
    """
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    present = observe_okg_surfaces(
        DEPLOYMENT, _client(instance), preset_id="okg-fixture",
        mcp_server_id="okg-fixture",
    )
    assert present["preset"] and present["connection"]

    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()

    receipt = _retire(instance)
    assert receipt["kind"] == KIND_RETIRE
    assert receipt["surfaces_absent"] == [
        "preset", "connection", "write_connection", "prompt",
    ]
    assert receipt["app_database"] == APP_DB.as_dict()
    # The graph is untouched, and the receipt says so rather than implying it.
    assert receipt["graph_deployment"]["touched"] is False
    assert receipt["container_operation"]["performed_by_this_verb"] is False
    assert "okg chat-instance down" in (
        receipt["container_operation"]["expected_operator_command"]
    )


def test_retire_refuses_while_an_owned_surface_remains(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.retire-receipt."""
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    instance.tool_servers.clear()
    instance.prompts.clear()
    with pytest.raises(ChatLifecycleSurfaceRemains) as caught:
        _retire(instance)
    assert caught.value.code == "chat_lifecycle_surface_remains"
    assert caught.value.data["remaining"] == ["preset"]


def test_retire_refuses_an_instance_it_could_not_read(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.retire-receipt.

    The failure mode this arm exists for: an unanswered instance looks
    exactly like an emptied one to anything that reads absence off a
    swallowed error.
    """
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    instance.fail_paths.add("/api/v1/configs/tool_servers")
    with pytest.raises(ChatLifecycleError) as caught:
        _retire(instance)
    assert caught.value.data["reason"] == "instance_unreadable"
    assert caught.value.data["underlying_code"]


# ---------------------------------------------------------------------------
# (e) every receipt: inert, deterministic, secret-free
# ---------------------------------------------------------------------------

def _all_three(tmp_path: Path) -> list[dict[str, Any]]:
    _inputs, instance, plan, readback = _reconciled(tmp_path)
    upgrade = _upgrade(plan, readback)
    rollback = rollback_receipt(
        DEPLOYMENT, upgrade=upgrade, current_app_database=APP_DB,
        tested_images=TWO_TESTED,
    )
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    return [upgrade, rollback, _retire(instance)]


def test_every_receipt_is_byte_identical_on_a_rerun(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.receipts-deterministic."""
    first = _all_three(tmp_path / "one")
    second = _all_three(tmp_path / "two")
    for left, right in zip(first, second, strict=True):
        a = write_lifecycle_receipt(left, tmp_path / f"out-a-{left['kind']}")
        b = write_lifecycle_receipt(right, tmp_path / f"out-b-{right['kind']}")
        assert Path(a["receipt_path"]).read_bytes() == (
            Path(b["receipt_path"]).read_bytes()
        )
        assert a["receipt_digest"] == b["receipt_digest"]


def test_a_receipt_directory_that_exists_refuses(
    tmp_path: Path, mcp_token: str,
) -> None:
    """req.chat-layer-lifecycle.receipts-deterministic."""
    receipt = _all_three(tmp_path)[0]
    out = tmp_path / "receipt"
    write_lifecycle_receipt(receipt, out)
    with pytest.raises(ChatLifecycleOutputPathExists) as caught:
        write_lifecycle_receipt(receipt, out)
    assert caught.value.code == "chat_lifecycle_output_path_exists"


def test_no_receipt_carries_a_secret(tmp_path: Path, mcp_token: str) -> None:
    """req.chat-layer-lifecycle.receipts-secret-free."""
    for index, receipt in enumerate(_all_three(tmp_path)):
        written = write_lifecycle_receipt(receipt, tmp_path / f"secret-{index}")
        raw = Path(written["receipt_path"]).read_bytes()
        assert mcp_token.encode("utf-8") not in raw
        assert TOKEN.encode("utf-8") not in raw
        assert BASE_URL.encode("utf-8") not in raw
        # The app database appears as an identity, never as a DSN.
        assert b"postgres" not in raw.lower()


def test_an_edited_receipt_is_refused_on_load(tmp_path: Path, mcp_token: str) -> None:
    """req.chat-layer-lifecycle.receipts-deterministic."""
    written = write_lifecycle_receipt(_all_three(tmp_path)[0], tmp_path / "out")
    path = Path(written["receipt_path"])
    doctored = json.loads(path.read_text())
    doctored["candidate"]["image"] = PINNED_IMAGE_REF
    path.write_text(json.dumps(doctored))
    with pytest.raises(ChatLifecycleError) as caught:
        load_lifecycle_receipt(path)
    assert caught.value.data["reason"] == "receipt_digest_self_mismatch"


def test_a_round_tripped_receipt_loads_unchanged(
    tmp_path: Path, mcp_token: str,
) -> None:
    receipt = _all_three(tmp_path)[0]
    written = write_lifecycle_receipt(receipt, tmp_path / "out")
    assert load_lifecycle_receipt(written["receipt_path"]) == receipt


# ---------------------------------------------------------------------------
# (f) the CLI seam
# ---------------------------------------------------------------------------

def test_the_lifecycle_receipt_verb_refuses_a_kind_missing_its_inputs() -> None:
    """req.chat-layer-lifecycle.receipts-deterministic.

    ``okg chat-instance lifecycle-receipt`` takes a ``--kind``, and
    ``argparse`` cannot say "required for THIS choice", so the per-kind
    inputs are checked in the handler. Without the check an upgrade with no
    plan would compare against nothing and still write a document.
    """
    import argparse

    from okg.substrate.chat.instance_cli import _require_receipt_flags

    for kind, given, missing in (
        ("upgrade", {}, ["--plan", "--readback"]),
        ("upgrade", {"plan": "p.json"}, ["--readback"]),
        ("rollback", {}, ["--upgrade-receipt"]),
    ):
        args = argparse.Namespace(
            **{"kind": kind, "plan": None, "readback": None,
               "upgrade_receipt": None, **given},
        )
        with pytest.raises(ChatLifecycleError) as caught:
            _require_receipt_flags(args)
        assert caught.value.data["missing"] == missing
        assert caught.value.data["kind"] == kind
    # `retire` needs neither: its evidence is the live instance, not a file.
    _require_receipt_flags(argparse.Namespace(kind="retire"))


def _receipt_with_target(
    tmp_path: Path,
    path: str,
    target: Any,
) -> tuple[dict[str, Any], FakeInstance]:
    """Exercise each public identity input with otherwise legitimate receipts."""
    _inputs, instance, plan, readback = _reconciled(tmp_path)
    if path.startswith("upgrade"):
        overrides = {}
        if path in ("upgrade-baseline", "upgrade-both"):
            overrides["baseline_app_database"] = target
        if path in ("upgrade-candidate", "upgrade-both"):
            overrides["candidate_app_database"] = target
        return _upgrade(plan, readback, **overrides), instance
    if path.startswith("rollback"):
        upgrade = _upgrade(plan, readback)
        if path in ("rollback-recorded", "rollback-both"):
            upgrade["app_database"] = target
        current = target if path in ("rollback-current", "rollback-both") else APP_DB
        return rollback_receipt(
            DEPLOYMENT,
            upgrade=upgrade,
            current_app_database=current,
            tested_images=TWO_TESTED,
        ), instance
    assert path == "retire"
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    return _retire(instance, app_database=target), instance


@pytest.mark.parametrize(
    "path",
    [
        "upgrade-baseline",
        "upgrade-candidate",
        "upgrade-both",
        "rollback-recorded",
        "rollback-current",
        "rollback-both",
        "retire",
    ],
)
@pytest.mark.parametrize(
    "target",
    [
        pytest.param({"host": None, "port": None, "database": None}, id="null-mapping"),
        pytest.param(PgTarget(host="", port=0, database=""), id="empty-pg-target"),
        pytest.param({"host": APP_DB.host, "port": APP_DB.port}, id="missing-database"),
    ],
)
def test_lifecycle_refuses_unusable_identity_on_every_public_path(
    tmp_path: Path,
    mcp_token: str,
    path: str,
    target: Any,
) -> None:
    original = deepcopy(target)
    output = tmp_path / "receipt-output"
    with pytest.raises(ChatLifecycleError) as caught:
        receipt, _instance_read = _receipt_with_target(tmp_path, path, target)
        write_lifecycle_receipt(receipt, output)
    assert caught.value.code == "chat_lifecycle_refused"
    assert caught.value.data == {"reason": "app_database_identity_missing"}
    assert target == original
    assert not output.exists()


@pytest.mark.parametrize(
    "field,bad",
    [
        ("host", None),
        ("host", ""),
        ("host", " \t"),
        ("host", 42),
        ("host", []),
        ("database", None),
        ("database", ""),
        ("database", " \t"),
        ("database", 42),
        ("database", []),
        ("port", None),
        ("port", False),
        ("port", True),
        ("port", "5432"),
        ("port", 5432.0),
        ("port", 0),
        ("port", -1),
        ("port", 65536),
    ],
)
@pytest.mark.parametrize("representation", ["mapping", "pg-target"])
def test_lifecycle_target_fields_must_be_usable_without_coercion(
    tmp_path: Path,
    mcp_token: str,
    field: str,
    bad: Any,
    representation: str,
) -> None:
    fields = APP_DB.as_dict() | {field: bad}
    target = PgTarget(**fields) if representation == "pg-target" else fields
    if representation == "mapping":
        target["credential"] = "caller-secret-do-not-echo"
    original = deepcopy(target)
    with pytest.raises(ChatLifecycleError) as caught:
        _receipt_with_target(tmp_path, "upgrade-both", target)
    error = caught.value
    assert error.code == "chat_lifecycle_refused"
    assert error.data == {"reason": "app_database_identity_missing"}
    assert "caller-secret-do-not-echo" not in json.dumps(error.to_json())
    assert APP_DB.database not in json.dumps(error.to_json())
    assert target == original


@pytest.mark.parametrize("field", ["host", "port", "database"])
def test_lifecycle_target_requires_every_identity_key(
    tmp_path: Path,
    mcp_token: str,
    field: str,
) -> None:
    target = APP_DB.as_dict()
    del target[field]
    with pytest.raises(ChatLifecycleError) as caught:
        _receipt_with_target(tmp_path, "upgrade-both", target)
    assert caught.value.data == {"reason": "app_database_identity_missing"}


@pytest.mark.parametrize("target", [None, {}, "not-an-identity", []])
def test_lifecycle_target_refuses_absent_or_wrong_input_container(
    tmp_path: Path,
    mcp_token: str,
    target: Any,
) -> None:
    with pytest.raises(ChatLifecycleError) as caught:
        _receipt_with_target(tmp_path, "upgrade-both", target)
    assert caught.value.data == {"reason": "app_database_identity_missing"}


@pytest.mark.parametrize(
    "host,port,database",
    [
        ("127.0.0.1", 5432, "okg_chat_fixture"),
        ("db.internal", 1, "chat"),
        ("2001:db8::1", 65535, "chat"),
        ("/var/run/postgresql", 5432, "chat"),
        ("db.internal", 5432, " chat database "),
    ],
)
def test_valid_target_representations_preserve_exact_receipt_identity(
    tmp_path: Path,
    mcp_token: str,
    host: str,
    port: int,
    database: str,
) -> None:
    _inputs, instance, plan, readback = _reconciled(tmp_path)
    typed = PgTarget(host=host, port=port, database=database)
    mapping = typed.as_dict() | {"credential": "not-an-identity-field"}
    original = deepcopy(mapping)
    first = _upgrade(
        plan, readback, baseline_app_database=typed, candidate_app_database=typed
    )
    second = _upgrade(
        plan, readback, baseline_app_database=mapping, candidate_app_database=typed
    )
    assert first == second
    assert first["app_database"] == typed.as_dict()
    for target in (typed, mapping):
        rollback = rollback_receipt(
            DEPLOYMENT,
            upgrade=first,
            current_app_database=target,
            tested_images=TWO_TESTED,
        )
        assert rollback["app_database"] == typed.as_dict()
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    assert _retire(instance, app_database=typed) == _retire(
        instance, app_database=mapping
    )
    assert mapping == original
    assert "not-an-identity-field" not in json.dumps(first)


@pytest.mark.parametrize("failure", ["surface-present", "unreadable"])
def test_retire_preserves_surface_refusal_before_invalid_identity(
    tmp_path: Path,
    mcp_token: str,
    failure: str,
) -> None:
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    if failure == "unreadable":
        instance.fail_paths.add("/api/v1/configs/tool_servers")
    before = len(instance.requests)
    with pytest.raises(ChatLifecycleError) as caught:
        _retire(instance, app_database=None)
    assert caught.value.data["reason"] == (
        "instance_unreadable" if failure == "unreadable" else "owned_surface_present"
    )
    assert instance.requests[before:]
    assert all(method == "GET" for method, _path in instance.requests[before:])


def test_retire_reads_empty_surfaces_but_never_writes_an_invalid_identity_receipt(
    tmp_path: Path,
    mcp_token: str,
) -> None:
    _inputs, instance, _plan, _readback = _reconciled(tmp_path)
    instance.models.clear()
    instance.tool_servers.clear()
    instance.prompts.clear()
    before = len(instance.requests)
    output = tmp_path / "invalid-retirement"
    with pytest.raises(ChatLifecycleError) as caught:
        document = _retire(instance, app_database=None)
        write_lifecycle_receipt(document, output)
    assert caught.value.data == {"reason": "app_database_identity_missing"}
    assert instance.requests[before:]
    assert all(method == "GET" for method, _path in instance.requests[before:])
    assert not output.exists()
