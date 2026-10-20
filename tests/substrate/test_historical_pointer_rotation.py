"""A release made invalid by a LATER rule must still be replaceable.

`_validate_historical_pointer_release` says in its own docstring that a
prior pointer need only be "internally consistent, immutable, and stored
under the same artifact root". It also called `load_environment`, which
re-parses an already-sealed, already-superseded manifest through the
CURRENT validation set.

That deadlocks. Once any manifest rule tightens, the outgoing release can
no longer be loaded, so it can never be rotated out — the release that
most needs replacing is exactly the one that fails.

okg-workspace-production hit this: its 2026-07-12 release declares a dark
`viz.theme.background` with no `viz.theme.surface`, which the 2026-08-18
theme-coherence rule rejects. Activating any newer release refused with
`viz_theme_incoherent`, naming the OLD artifact's manifest.

Immutability is not relaxed by the fix, and these tests pin that: a
tampered or misplaced artifact must still refuse.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate import release_activation
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.errors import SealedReleaseInvalid

ARTIFACT_ID = "sha256:" + "a" * 64


class _Store:
    def __init__(self, root: Path) -> None:
        self._root = root

    def resolve(self) -> Path:
        return self._root


class _Release:
    def __init__(self, root: Path) -> None:
        self.artifact_root = root
        self.repo_root = root / "repo"


def _environment(store: Path) -> Any:
    class _Rel:
        artifact_store = _Store(store)

    class _Env:
        release = _Rel()

    return _Env()


@pytest.fixture()
def wired(tmp_path: Path, monkeypatch) -> dict[str, Any]:
    """A pointer whose artifact verifies but whose manifest no longer loads."""
    store = tmp_path / "artifacts"
    root = store / ARTIFACT_ID.removeprefix("sha256:")
    (root / "repo").mkdir(parents=True)
    descriptor = root / "repo" / "environment.yaml"
    descriptor.write_text("id: x\n", encoding="utf-8")

    calls: dict[str, Any] = {"verified": False, "descriptor_checked": False}
    release = _Release(root)

    monkeypatch.setattr(
        release_activation, "verify_sealed_release",
        lambda *a, **k: calls.__setitem__("verified", True) or release,
    )
    monkeypatch.setattr(
        release_activation, "_candidate_descriptor_path",
        lambda *a, **k: descriptor,
    )
    monkeypatch.setattr(
        release_activation, "_validate_descriptor_file",
        lambda *a, **k: calls.__setitem__("descriptor_checked", True),
    )
    return {
        "calls": calls,
        "store": store,
        "root": root,
        "pointer": {
            "artifact_id": ARTIFACT_ID,
            "artifact_root": str(root),
        },
        "environment": _environment(store),
    }


def test_a_manifest_invalidated_by_a_later_rule_can_still_rotate_out(
    wired: dict[str, Any], monkeypatch,
) -> None:
    """The deadlock: this is what blocked okg-workspace-production."""
    def _refuse(*_a, **_k):
        raise DeploymentConfigError(
            "viz.theme.background is dark without viz.theme.surface"
        )

    monkeypatch.setattr(release_activation, "load_environment", _refuse)

    result = release_activation._validate_historical_pointer_release(
        wired["environment"], wired["pointer"], environ={},
    )

    assert result is not None, "a superseded release could not be rotated out"
    # Immutability was still proven, and the descriptor still checked.
    assert wired["calls"]["verified"] is True
    assert wired["calls"]["descriptor_checked"] is True


def test_a_loadable_manifest_is_still_fully_validated(
    wired: dict[str, Any], monkeypatch,
) -> None:
    """The relaxation must not skip checks for a manifest that DOES load."""
    seen: dict[str, bool] = {"fields": False, "binding": False}
    monkeypatch.setattr(
        release_activation, "load_environment", lambda *a, **k: object(),
    )
    monkeypatch.setattr(
        release_activation, "_validate_active_environment_fields",
        lambda *a, **k: seen.__setitem__("fields", True),
    )
    monkeypatch.setattr(
        release_activation, "_validate_candidate_binding",
        lambda *a, **k: seen.__setitem__("binding", True),
    )

    release_activation._validate_historical_pointer_release(
        wired["environment"], wired["pointer"], environ={},
    )

    assert seen["fields"] is True
    assert seen["binding"] is True


def test_a_tampered_artifact_still_refuses(
    wired: dict[str, Any], monkeypatch,
) -> None:
    """Immutability is NOT relaxed — this is the safety half."""
    def _tampered(*_a, **_k):
        raise SealedReleaseInvalid("artifact digest does not match its id")

    monkeypatch.setattr(release_activation, "verify_sealed_release", _tampered)
    monkeypatch.setattr(
        release_activation, "load_environment",
        lambda *a, **k: pytest.fail("must not reach load after tamper check"),
    )

    with pytest.raises(SealedReleaseInvalid, match="digest"):
        release_activation._validate_historical_pointer_release(
            wired["environment"], wired["pointer"], environ={},
        )


def test_an_artifact_outside_the_store_still_refuses(
    wired: dict[str, Any], tmp_path: Path,
) -> None:
    """Location is NOT relaxed either."""
    pointer = dict(wired["pointer"])
    pointer["artifact_root"] = str(tmp_path / "elsewhere" / "x")

    with pytest.raises(SealedReleaseInvalid, match="artifact store"):
        release_activation._validate_historical_pointer_release(
            wired["environment"], pointer, environ={},
        )
