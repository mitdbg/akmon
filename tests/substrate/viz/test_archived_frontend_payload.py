"""The retired /viz frontend routes answer 410 and point at git, not a path.

PACT layout-history-openspec deleted ``archive/`` from the working tree. The
410 payload used to name ``archive/ui/legacy-viz-2026-06-17`` as a path; it
now names the git object ``<commit>:<path>`` where the archived frontend can
still be read. No database: these routes never touch the viz context.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


def _no_context():
    raise AssertionError("the archived-frontend routes must not read the context")


@pytest.mark.parametrize("route", ["/", "/viz", "/viz/", "/viz/old/page"])
def test_archived_route_payload_names_the_git_object(route: str) -> None:
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from okg.viz.app import (
        LEGACY_VIZ_ARCHIVE_COMMIT,
        LEGACY_VIZ_ARCHIVE_PATH,
        build_app,
    )

    client = TestClient(build_app(ctx_provider=_no_context, is_ready=lambda: True))
    response = client.get(route)

    assert response.status_code == 410
    body = response.json()
    assert body["archived"] is True
    assert body["archive"] == f"{LEGACY_VIZ_ARCHIVE_COMMIT}:{LEGACY_VIZ_ARCHIVE_PATH}"
    commit, _, path = body["archive"].partition(":")
    assert re.fullmatch(r"[0-9a-f]{40}", commit)
    assert path == "archive/ui/legacy-viz-2026-06-17"
    # The object is in git, not in the working tree.
    assert not (REPO_ROOT / "archive").exists()


def test_archived_api_path_is_still_a_plain_404() -> None:
    """The 410 is for the retired pages only; an unknown API route stays 404."""
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from okg.viz.app import build_app

    client = TestClient(build_app(ctx_provider=_no_context, is_ready=lambda: True))
    assert client.get("/viz/api/no-such-route").status_code == 404
