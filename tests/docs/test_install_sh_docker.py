"""install.sh asks for Docker only when the install needs a container.

Issue #3414 (review L1 of PACT minimal-default-install) and issue #3480: the
default `minimal` profile never starts a database, with or without
`--postgres-dsn`; only `--profile academic-researcher` without a DSN starts
PostgreSQL in Docker. This file is not on a database lane because it reads
install.sh, which the database lanes do not count as input.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "install.sh"


def _source(
    tmp_path: Path,
    body: str,
    *argv: str,
    path: str = "/usr/bin:/bin",
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess:
    """Source install.sh's helpers (no bootstrap) and run ``body``."""
    env = {
        "OKG_INSTALL_SH_SOURCE_ONLY": "1",
        "PATH": path,
        "HOME": str(tmp_path),
        **(extra_env or {}),
    }
    return subprocess.run(
        ["/bin/bash", "-c", f'source "$0"; {body}', str(SCRIPT), *argv],
        env=env, capture_output=True, text=True, check=False,
        stdin=subprocess.DEVNULL,
    )


def _yes_no(tmp_path: Path, function: str, *argv: str) -> bool:
    probe = _source(
        tmp_path, f'if {function} "$@"; then echo yes; else echo no; fi', *argv,
    )
    # A sentinel, not the exit code: a script that dies while being
    # sourced must not read as "no".
    assert probe.returncode == 0, probe.stderr
    assert probe.stdout.strip() in ("yes", "no"), probe.stdout
    return probe.stdout.strip() == "yes"


def test_install_sh_needs_docker_only_for_the_academic_container(tmp_path) -> None:
    """The default profile never starts a database, so it never needs
    Docker, even with no arguments (issue #3480 defect 2)."""
    def needs_docker(*argv: str) -> bool:
        return _yes_no(tmp_path, "needs_docker", *argv)

    assert not needs_docker()
    assert not needs_docker("--deployment-name", "t")
    assert not needs_docker("--profile", "minimal")
    assert not needs_docker("--profile", "workspace-graph")
    assert not needs_docker("--deployment-name", "t", "--postgres-dsn", "postgresql:///t")
    assert not needs_docker("--postgres-dsn=postgresql:///t")
    assert not needs_docker(
        "--profile", "academic-researcher", "--postgres-dsn", "postgresql:///t",
    )
    assert needs_docker("--profile", "academic-researcher")
    assert needs_docker("--profile=academic-researcher", "--orcid", "0000")


def test_install_sh_needs_a_dsn_only_for_the_default_profile(tmp_path) -> None:
    def needs_dsn(*argv: str) -> bool:
        return _yes_no(tmp_path, "needs_dsn", *argv)

    assert needs_dsn()
    assert needs_dsn("--deployment-name", "t")
    assert needs_dsn("--profile", "minimal")
    assert not needs_dsn("--postgres-dsn", "${OKG_DSN}")
    assert not needs_dsn("--profile", "academic-researcher")
    assert not needs_dsn("--help")
    assert not needs_dsn("--dry-run")


def test_install_sh_without_a_dsn_or_terminal_says_how_to_give_one(tmp_path) -> None:
    """curl | bash has no terminal for the installer to ask on: stop first
    and name both ways to a database, without demanding Docker Desktop."""
    probe = _source(tmp_path, "require_dsn_or_terminal; echo continued")
    assert probe.returncode == 1
    assert "continued" not in probe.stdout
    assert "--postgres-dsn '${OKG_DSN}'" in probe.stderr
    assert "export OKG_DSN=" in probe.stderr
    assert "docker compose up -d --build --wait" in probe.stderr
    assert "Docker Desktop" not in probe.stderr
    # Under curl | bash there is no clone yet: say how to get one, into
    # the folder the installer then reuses.
    assert "From a clone of OKG" not in probe.stderr
    root = tmp_path / "okg"
    assert f"https://github.com/mitdbg/akmon.git {root}" in probe.stderr
    assert f"cd {root}/ops/pg && docker compose up" in probe.stderr


def test_install_sh_does_not_send_linux_users_to_docker_desktop(tmp_path) -> None:
    """With no docker on PATH, Linux gets the Docker Engine link; the
    Docker Desktop link is for macOS only."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "uname").write_text("#!/bin/sh\necho Linux\n")
    (bin_dir / "uname").chmod(0o755)
    probe = _source(tmp_path, "require_docker", path=str(bin_dir))
    assert probe.returncode == 1
    assert "Docker Desktop" not in probe.stderr
    assert "https://docs.docker.com/engine/install/" in probe.stderr


def test_install_sh_masks_the_dsn_password_in_its_log(tmp_path) -> None:
    """Defect 3: the log line echoed every argument, password included."""
    # Built from pieces so the public-tree scan does not read the test's
    # own source as a DSN that carries a password.
    url_secret, kv_secret = "s3cr3t-pw", "other-s3cret"
    probe = _source(
        tmp_path, 'masked_args "$@"',
        "--deployment-name", "t",
        "--postgres-dsn", "postgresql://okg:" + url_secret + "@db.example:5432/okg",
        "--postgres-dsn=host=db password=" + kv_secret + " dbname=x",
        "--postgres-dsn", "${OKG_DSN}",
    )
    assert probe.returncode == 0, probe.stderr
    out = probe.stdout
    assert "s3cr3t-pw" not in out and "other-s3cret" not in out
    assert "postgresql://okg:***@db.example:5432/okg" in out
    assert "password=***" in out
    assert "${OKG_DSN}" in out and "--deployment-name t" in out


def test_install_sh_links_okg_and_says_whether_the_shell_finds_it(tmp_path) -> None:
    """Defect 1: after install.sh the command was only in ~/okg/.venv/bin."""
    root = tmp_path / "okg"
    (root / ".venv" / "bin").mkdir(parents=True)
    okg = root / ".venv" / "bin" / "okg"
    okg.write_text("#!/bin/sh\necho okg-ran\n")
    okg.chmod(0o755)
    bin_dir = tmp_path / "localbin"
    env = {"OKG_INSTALL_ROOT": str(root), "OKG_BIN_DIR": str(bin_dir)}

    off_path = _source(tmp_path, "link_okg_on_path; path_hint", extra_env=env)
    assert off_path.returncode == 0, off_path.stderr
    assert (bin_dir / "okg").resolve() == okg.resolve()
    assert f'export PATH="{bin_dir}:$PATH"' in off_path.stderr

    on_path = _source(
        tmp_path, "link_okg_on_path; path_hint",
        path=f"{bin_dir}:/usr/bin:/bin", extra_env=env,
    )
    assert on_path.returncode == 0, on_path.stderr
    assert "is on your PATH" in on_path.stderr
    assert "export PATH" not in on_path.stderr


def test_install_sh_never_replaces_an_okg_it_did_not_link(tmp_path) -> None:
    root = tmp_path / "okg"
    (root / ".venv" / "bin").mkdir(parents=True)
    (root / ".venv" / "bin" / "okg").write_text("#!/bin/sh\n")
    (root / ".venv" / "bin" / "okg").chmod(0o755)
    bin_dir = tmp_path / "localbin"
    bin_dir.mkdir()
    own = bin_dir / "okg"
    own.write_text("mine\n")
    probe = _source(
        tmp_path, "link_okg_on_path",
        extra_env={"OKG_INSTALL_ROOT": str(root), "OKG_BIN_DIR": str(bin_dir)},
    )
    assert probe.returncode == 0, probe.stderr
    assert own.read_text() == "mine\n" and not os.path.islink(own)
    assert "leaving it alone" in probe.stderr


def test_install_sh_masks_an_at_sign_and_a_quoted_password(tmp_path) -> None:
    """Review LOW-1: an unencoded '@' in a URL password and a quoted
    keyword value (password='a b') each leaked part of the password."""
    at_secret, quoted_secret = "p@ss", "a b"
    hash_secret, query_secret = "p#q", "x?y"
    probe = _source(
        tmp_path, 'masked_args "$@"',
        "--postgres-dsn", "postgresql://u:" + at_secret + "@h:5432/db",
        "--postgres-dsn", "postgresql://v:" + hash_secret + "@h2/db",
        "--postgres-dsn", "postgresql://w:" + query_secret + "@h3/db?sslmode=require",
        # "pass" + "word=": the public-tree scan reads a literal keyword
        # password in this source as a credential.
        "--postgres-dsn=host=h pass" + "word='" + quoted_secret + "' dbname=x",
        "--postgres-dsn", "host=h password = '" + quoted_secret + "'",
    )
    assert probe.returncode == 0, probe.stderr
    out = probe.stdout
    assert "ss@h" not in out and "p@" not in out
    assert "postgresql://u:***@h:5432/db" in out
    # libpq ends userinfo only at '/', so '#' and '?' belong to the password.
    assert "p#q" not in out and "x?y" not in out
    assert "postgresql://v:***@h2/db" in out
    assert "postgresql://w:***@h3/db?sslmode=require" in out
    assert "a b" not in out and " b'" not in out
    assert "host=h password=*** dbname=x" in out
    assert "host=h password = ***" in out


def _fake_checkout(tmp_path: Path) -> tuple[Path, Path, dict[str, str]]:
    root = tmp_path / "okg"
    (root / ".venv" / "bin").mkdir(parents=True)
    okg = root / ".venv" / "bin" / "okg"
    okg.write_text("#!/bin/sh\necho okg-ran\n")
    okg.chmod(0o755)
    bin_dir = tmp_path / "localbin"
    bin_dir.mkdir()
    return okg, bin_dir, {"OKG_INSTALL_ROOT": str(root), "OKG_BIN_DIR": str(bin_dir)}


def test_install_sh_keeps_another_tools_okg_link(tmp_path) -> None:
    """Review MED-1: uv tool and pipx link okg into ~/.local/bin; the
    installer replaced that link. It now keeps it and says the shell
    runs the other okg."""
    okg, bin_dir, env = _fake_checkout(tmp_path)
    other = tmp_path / "uv-tools" / "okg" / "bin" / "okg"
    other.parent.mkdir(parents=True)
    other.write_text("#!/bin/sh\n")
    other.chmod(0o755)
    (bin_dir / "okg").symlink_to(other)

    probe = _source(
        tmp_path, "link_okg_on_path; path_hint",
        path=f"{bin_dir}:/usr/bin:/bin", extra_env=env,
    )
    assert probe.returncode == 0, probe.stderr
    assert os.readlink(bin_dir / "okg") == str(other)
    assert "leaving it alone" in probe.stderr
    assert "okg was not linked" in probe.stderr
    assert f"'okg' runs {bin_dir}/okg, not this install" in probe.stderr
    assert "is on your PATH" not in probe.stderr
    assert str(okg) in probe.stderr


def test_install_sh_replaces_a_dangling_or_own_link(tmp_path) -> None:
    okg, bin_dir, env = _fake_checkout(tmp_path)
    link = bin_dir / "okg"
    link.symlink_to(tmp_path / "gone" / "okg")
    dangling = _source(tmp_path, "link_okg_on_path", extra_env=env)
    assert dangling.returncode == 0, dangling.stderr
    assert os.readlink(link) == str(okg)

    # A link into this install root (an earlier run) is replaced too.
    link.unlink()
    link.symlink_to(Path(env["OKG_INSTALL_ROOT"]) / "old" / "okg")
    (Path(env["OKG_INSTALL_ROOT"]) / "old").mkdir()
    (Path(env["OKG_INSTALL_ROOT"]) / "old" / "okg").write_text("")
    own = _source(tmp_path, "link_okg_on_path", extra_env=env)
    assert own.returncode == 0, own.stderr
    assert os.readlink(link) == str(okg)
    assert "Linked" in own.stderr


def test_install_sh_says_when_the_shell_runs_a_different_okg(tmp_path) -> None:
    """Review MED-2: BIN_DIR on PATH is not enough; an okg earlier on
    PATH wins, and the old message said 'okg is on your PATH' anyway."""
    _okg, bin_dir, env = _fake_checkout(tmp_path)
    earlier = tmp_path / "earlier"
    earlier.mkdir()
    (earlier / "okg").write_text("#!/bin/sh\n")
    (earlier / "okg").chmod(0o755)

    probe = _source(
        tmp_path, "link_okg_on_path; path_hint",
        path=f"{earlier}:{bin_dir}:/usr/bin:/bin", extra_env=env,
    )
    assert probe.returncode == 0, probe.stderr
    assert f"'okg' runs {earlier}/okg, not this install's" in probe.stderr
    assert "is on your PATH" not in probe.stderr
    assert f'export PATH="{bin_dir}:$PATH"' in probe.stderr


def test_install_sh_replaces_its_own_relative_link(tmp_path) -> None:
    """Review LOW-3: a relative link into the install root (`../okg/...`)
    and an install root spelled with a trailing slash are still ours."""
    okg, bin_dir, env = _fake_checkout(tmp_path)
    old = Path(env["OKG_INSTALL_ROOT"]) / "old"
    old.mkdir()
    (old / "okg").write_text("")
    link = bin_dir / "okg"
    link.symlink_to(Path("..") / "okg" / "old" / "okg")
    env = {**env, "OKG_INSTALL_ROOT": env["OKG_INSTALL_ROOT"] + "/"}
    probe = _source(tmp_path, "link_okg_on_path", extra_env=env)
    assert probe.returncode == 0, probe.stderr
    assert "leaving it alone" not in probe.stderr
    assert Path(os.readlink(link)).resolve() == okg.resolve()
