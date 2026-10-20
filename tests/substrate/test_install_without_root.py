"""Installing environment services must not require root on macOS.

The only owned service artifact is the postgres password file, chowned to
the container's uid so the container can read the secret. Host ownership
is the means; the container reading it is the end.

On Linux those coincide — Docker shares the host kernel, uids are real,
and without the chown the container cannot read the file.

On macOS they do not. Docker Desktop presents a bind mount through a
translation layer that reports the container's own uid regardless of the
host owner. Measured on okg-workspace-production: the secret is 501:20 on
the host and the container reads it as uid=1000 gid=1000, and Postgres has
consumed it for weeks.

So on macOS the chown bought nothing and cost everything: changing a file
to a different uid needs root, which made `service install` root-only —
and because `restart` requires a current install, a dead deployment could
not be recovered without sudo.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from okg.substrate import environment_services as svc
from okg.substrate.deployments import DeploymentConfigError

OTHER_UID = 1000
OTHER_GID = 1000


def test_macos_does_not_chown_the_host_file(tmp_path: Path, monkeypatch) -> None:
    """The whole point: no root needed, and no chown attempted."""
    monkeypatch.setattr(svc.sys, "platform", "darwin")

    def _forbidden(*_a, **_k):
        pytest.fail("attempted a host chown on macOS, which needs root")

    monkeypatch.setattr(svc.os, "fchown", _forbidden)

    target = tmp_path / "postgres-password"
    svc._atomic_write(target, b"pw\n", mode=0o600, owner=(OTHER_UID, OTHER_GID))

    assert target.read_bytes() == b"pw\n"
    # Mode is still enforced — only OWNERSHIP is delegated to the runtime.
    assert oct(target.stat().st_mode)[-3:] == "600"
    # And it stays owned by whoever ran the install.
    assert target.stat().st_uid == os.getuid()


def test_linux_still_chowns(tmp_path: Path, monkeypatch) -> None:
    """The safety half — on Linux the chown is load-bearing."""
    monkeypatch.setattr(svc.sys, "platform", "linux")
    seen: dict[str, tuple[int, int]] = {}

    def _record(_fd, uid, gid):
        seen["owner"] = (uid, gid)

    monkeypatch.setattr(svc.os, "fchown", _record)

    # The recording stub does not really change the uid, so the post-write
    # verification then fails — which is itself the proof that the Linux
    # path both attempts the chown AND still checks the result.
    target = tmp_path / "postgres-password"
    with pytest.raises(DeploymentConfigError, match="owner mismatch"):
        svc._atomic_write(
            target, b"pw\n", mode=0o600, owner=(OTHER_UID, OTHER_GID),
        )

    assert seen["owner"] == (OTHER_UID, OTHER_GID), (
        "Linux must still attempt the chown to the container uid"
    )


def test_unowned_artifacts_are_untouched_on_both_platforms(
    tmp_path: Path, monkeypatch,
) -> None:
    """Artifacts with no declared owner never chown, anywhere."""
    for platform in ("darwin", "linux"):
        monkeypatch.setattr(svc.sys, "platform", platform)
        monkeypatch.setattr(
            svc.os, "fchown",
            lambda *_a, **_k: pytest.fail(f"chown attempted on {platform}"),
        )
        target = tmp_path / f"plist-{platform}"
        svc._atomic_write(target, b"<plist/>", mode=0o644, owner=None)
        assert target.read_bytes() == b"<plist/>"
        assert oct(target.stat().st_mode)[-3:] == "644"


def test_the_predicate_is_platform_declared(monkeypatch) -> None:
    """Stated as a rule, not inferred from a caught PermissionError.

    Swallowing the error would also skip the chown on Linux, leaving a
    secret the container cannot read, silently.
    """
    monkeypatch.setattr(svc.sys, "platform", "darwin")
    assert svc._host_owner_reaches_container() is False

    monkeypatch.setattr(svc.sys, "platform", "linux")
    assert svc._host_owner_reaches_container() is True
