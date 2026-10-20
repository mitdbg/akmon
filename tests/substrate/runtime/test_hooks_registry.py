"""Tests for the hook registry (Phase 2 §1.3).

The registry is in-process module-level state. Tests here exercise
the API surface without spinning up DBOS — that integration is
covered by an end-to-end test in Phase 2 §2.2.6.

Each test clears the registry on entry so order-independent.
"""
from __future__ import annotations

import pytest

from okg.substrate.runtime.hooks import (
    HOOK_EVENT_ESCALATION_DECIDED,
    HOOK_EVENT_GENERATION_BLOCKED,
    HOOK_EVENT_GENERATION_PUBLISHED,
    _registry_snapshot,
    clear_hooks,
    iter_hooks,
    register_hook,
)


@pytest.fixture(autouse=True)
def reset_registry():
    """Each test starts and ends with an empty registry."""
    clear_hooks()
    yield
    clear_hooks()


def test_register_and_iter_returns_in_order():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="promote_annotations",
    )
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="notify_external_system",
    )

    hooks = iter_hooks(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    )
    assert hooks == ["promote_annotations", "notify_external_system"]


def test_iter_returns_a_fresh_list():
    """Caller mutation does not affect the registry."""
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="promote_annotations",
    )
    h = iter_hooks(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    )
    h.append("evil_mutation")

    hooks_after = iter_hooks(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    )
    assert hooks_after == ["promote_annotations"]


def test_duplicate_registration_is_idempotent():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="promote_annotations",
    )
    # Re-import of the same module (which happens during worker
    # recovery) should not duplicate the hook.
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="promote_annotations",
    )

    hooks = iter_hooks(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    )
    assert hooks == ["promote_annotations"]


def test_different_deployments_isolated():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="afp_hook",
    )
    register_hook(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="lp_hook",
    )

    assert iter_hooks(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["afp_hook"]
    assert iter_hooks(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["lp_hook"]


def test_different_event_types_isolated():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="on_pub_hook",
    )
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_BLOCKED,
        workflow_name="on_blocked_hook",
    )
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_ESCALATION_DECIDED,
        workflow_name="on_decided_hook",
    )

    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["on_pub_hook"]
    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_BLOCKED,
    ) == ["on_blocked_hook"]
    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_ESCALATION_DECIDED,
    ) == ["on_decided_hook"]


def test_register_rejects_unknown_event_type():
    with pytest.raises(ValueError, match="unknown hook event type"):
        register_hook(
            deployment="af_pubs",
            event_type="generation_partially_published",
            workflow_name="bogus",
        )


def test_iter_rejects_unknown_event_type():
    with pytest.raises(ValueError, match="unknown hook event type"):
        iter_hooks(
            deployment="af_pubs",
            event_type="generation_partially_published",
        )


def test_clear_hooks_narrowing():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="a",
    )
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_BLOCKED,
        workflow_name="b",
    )
    register_hook(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="c",
    )

    # Clear a specific (deployment, event_type) pair.
    clear_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    )

    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == []
    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_BLOCKED,
    ) == ["b"]
    assert iter_hooks(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["c"]


def test_clear_hooks_by_deployment():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="a",
    )
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_BLOCKED,
        workflow_name="b",
    )
    register_hook(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="c",
    )

    clear_hooks(deployment="af_pubs")

    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == []
    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_BLOCKED,
    ) == []
    assert iter_hooks(
        deployment="okg-workspace",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["c"]


def test_registry_snapshot_returns_fresh_copy():
    register_hook(
        deployment="af_pubs",
        event_type=HOOK_EVENT_GENERATION_PUBLISHED,
        workflow_name="a",
    )
    snap = _registry_snapshot()
    snap[("af_pubs", HOOK_EVENT_GENERATION_PUBLISHED)].append("evil")

    # Subsequent iter_hooks call still sees the original list.
    assert iter_hooks(
        deployment="af_pubs", event_type=HOOK_EVENT_GENERATION_PUBLISHED,
    ) == ["a"]
