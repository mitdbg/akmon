"""Per `harden-okg-network-and-secrets`: the academic-researcher install must
not ship a universal hardcoded Postgres password on a 0.0.0.0-bound
port, and must not leak the DSN password through logs/CLI output.

Closes a CRITICAL found by security audit (2026-06-09): a bare
`PORT:5432` mapping published Postgres on every interface with the
same password `okg` on every install worldwide.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from okg.substrate.cli.profile_cli_loader import get_profile_cli

_install = get_profile_cli("academic-researcher").install

_PROFILE_DIR = Path(
    "src/okg/substrate/library/profiles/academic-researcher"
)


# ---------- per-deployment password ----------


def test_default_dsn_threads_password() -> None:
    dsn = _install._default_dsn("dep", 5435, "S3cretPw")
    assert "://postgres:S3cretPw@localhost:5435/okg_dep" in dsn


def test_default_dsn_default_password_is_only_for_direct_callers() -> None:
    # The bare default still exists for tests/manual use, but the
    # install flow overrides it (see resolve test below).
    assert ":okg@" in _install._default_dsn("dep", 5435)


def test_password_from_dsn() -> None:
    assert _install._password_from_dsn(
        "postgres://postgres:s3cr3t@h:5/db"
    ) == "s3cr3t"
    assert _install._password_from_dsn("postgres://postgres@h/db") is None
    assert _install._password_from_dsn("") is None


def test_resolve_pg_password_generates_fresh_random_when_no_deployment() -> None:
    with mock.patch(
        "okg.substrate.deployments.find_deployment_dir",
        return_value=None,
    ):
        a = _install._resolve_pg_password("brand-new-1")
        b = _install._resolve_pg_password("brand-new-2")
    assert a != "okg" and b != "okg"
    assert a != b              # fresh per call
    assert len(a) >= 20        # not trivially guessable


def test_resolve_pg_password_reuses_existing_deployment(tmp_path: Path) -> None:
    """A re-run of `okg install` against an existing deployment SHALL
    reuse the password already baked into its volume — minting a new
    one would make the persisted DSN fail to authenticate."""
    dep_dir = tmp_path / "existing"
    dep_dir.mkdir()
    (dep_dir / "deployment.yaml").write_text(
        "postgres:\n"
        "  dsn: postgres://postgres:ALREADYbaked123@localhost:5435/okg_x\n"
    )
    with mock.patch(
        "okg.substrate.deployments.find_deployment_dir",
        return_value=dep_dir,
    ):
        pw = _install._resolve_pg_password("existing")
    assert pw == "ALREADYbaked123"


def test_resolve_pg_password_reads_init_answers_dsn(tmp_path: Path) -> None:
    dep_dir = tmp_path / "existing2"
    dep_dir.mkdir()
    (dep_dir / "deployment.yaml").write_text(
        "profile:\n"
        "  init_answers:\n"
        "    postgres_dsn: postgres://postgres:fromAnswers9@localhost:5/okg_y\n"
    )
    with mock.patch(
        "okg.substrate.deployments.find_deployment_dir",
        return_value=dep_dir,
    ):
        assert _install._resolve_pg_password("existing2") == "fromAnswers9"


def test_resolve_pg_password_prefers_postgres_block_over_redacted_answers(
    tmp_path: Path,
) -> None:
    dep_dir = tmp_path / "existing3"
    dep_dir.mkdir()
    (dep_dir / "deployment.yaml").write_text(
        "postgres:\n"
        "  dsn: postgres://postgres:fromCanonical9@localhost:5/okg_y\n"
        "profile:\n"
        "  init_answers:\n"
        "    postgres_dsn: postgres://postgres:***@localhost:5/okg_y\n"
    )
    with mock.patch(
        "okg.substrate.deployments.find_deployment_dir",
        return_value=dep_dir,
    ):
        assert _install._resolve_pg_password("existing3") == "fromCanonical9"


# ---------- compose: loopback bind + env-driven password ----------


def test_compose_binds_postgres_to_loopback_only() -> None:
    """A bare `PORT:5432` publishes on 0.0.0.0. The published port
    SHALL be prefixed 127.0.0.1 in every shipped compose file."""
    for rel in (
        _PROFILE_DIR / "compose.yaml",
        Path("ops/pg/docker-compose.yaml"),
    ):
        text = rel.read_text()
        # The `- "...:5432"` host mapping line must be loopback-bound.
        mapping_lines = [
            ln.strip() for ln in text.splitlines()
            if ":5432\"" in ln and ln.strip().startswith("-")
        ]
        assert mapping_lines, f"no 5432 mapping found in {rel}"
        for ln in mapping_lines:
            assert "127.0.0.1:" in ln, f"{rel}: not loopback-bound: {ln}"


def test_compose_password_is_env_driven_not_hardcoded() -> None:
    text = (_PROFILE_DIR / "compose.yaml").read_text()
    assert "POSTGRES_PASSWORD: ${POSTGRES_PASSWORD" in text
    assert "POSTGRES_PASSWORD: okg\n" not in text


def test_bundled_database_port_name_and_password_are_settable() -> None:
    """Issue #3581: a newcomer whose port 5433 is taken, or who already runs
    an okg database, had to edit the tracked compose file. Each fixed value
    reads an OKG_PG_* variable whose default is today's value, and the host
    side of the port stays the literal loopback address. An empty password
    is passed through (Postgres refuses it) instead of becoming `okg`. The
    image name reads OKG_PG_IMAGE and defaults to `okg-pg17:local`: the
    academic-researcher install builds the image from this file and then
    looks for that default name, so the default must not move."""
    import yaml

    service = yaml.safe_load(
        Path("ops/pg/docker-compose.yaml").read_text()
    )["services"]["db"]
    assert service["image"] == "${OKG_PG_IMAGE:-okg-pg17:local}"
    assert service["container_name"] == "${OKG_PG_CONTAINER:-okg-pg}"
    assert service["environment"]["POSTGRES_PASSWORD"] == "${OKG_PG_PASSWORD-okg}"
    assert service["ports"] == ["127.0.0.1:${OKG_PG_PORT:-5433}:5432"]
    # The database name stays fixed: cron.database_name must match it.
    assert service["environment"]["POSTGRES_DB"] == "okg"
    assert "cron.database_name=okg" in service["command"]


# Compose files that run without a top-level `name:` because every caller
# passes `-p`, or that only ever layer onto a named base file.
_COMPOSE_WITHOUT_NAME = {
    # Overlays: merged onto ops/pg/docker-compose.yaml, whose name applies.
    "ops/pg/docker-compose.bench.yaml": "overlay; CI passes -p okg-bench",
    "ops/pg/docker-compose.ci.yaml": "overlay on ops/pg/docker-compose.yaml",
    "ops/pg/docker-compose.golden.yaml": "overlay; no live caller (CI fixture only)",
    # Started only by CI with -p "${OKG_REQUIRED_CI_PG_PROJECT}".
    "ops/pg/docker-compose.required-ci.yaml": "CI passes -p per lane",
    # The academic-researcher install starts it with -p okg-<deployment>.
    "src/okg/substrate/library/profiles/academic-researcher/compose.yaml":
        "install passes -p okg-<deployment>",
}


def test_every_compose_file_names_its_own_project() -> None:
    """Issue #3639: without `name:` Compose names the project after the
    folder (`pg`, `opensearch`) and joins any other app's project of that
    name, recreating or (`down -v`) removing that app's containers and
    volumes. Every compose file a person runs by hand names its project;
    the rest are listed above with the reason they need no name."""
    import re
    import subprocess

    import yaml

    repo = Path(__file__).resolve().parents[3]
    compose_name = re.compile(r"^(docker-)?compose[^/]*\.ya?ml$")
    files = [
        f for f in subprocess.run(
            ["git", "ls-files"], capture_output=True, text=True, check=True,
            cwd=repo,
        ).stdout.split()
        if compose_name.match(Path(f).name) and not f.startswith("history/")
    ]
    assert "ops/pg/docker-compose.yaml" in files
    unnamed = sorted(
        f for f in files
        if f not in _COMPOSE_WITHOUT_NAME
        and not (yaml.safe_load((repo / f).read_text()) or {}).get("name")
    )
    assert unnamed == []
    assert set(_COMPOSE_WITHOUT_NAME) <= set(files), "stale exemption"


def test_the_bundled_database_project_is_okg_pg() -> None:
    import yaml

    for rel, name in (
        ("ops/pg/docker-compose.yaml", "okg-pg"),
        ("deployments/opensearch-reference/opensearch/docker-compose.yaml",
         "okg-opensearch-reference"),
    ):
        path = Path(__file__).resolve().parents[3] / rel
        assert yaml.safe_load(path.read_text())["name"] == name, rel


# ---------- DSN redaction in `okg deployment show` ----------


def test_deployment_show_uses_redactor() -> None:
    """The deployment-show payload SHALL run the DSN through the
    password redactor (shared with the rest of the substrate).

    `_cmd_deployment_show` calls the module binding `redact_dsn`; this
    test pins that binding, so making it a pass-through goes red.
    """
    from okg.substrate.cli import deployment as dep_cli

    assert dep_cli.redact_dsn(
        "postgres://postgres:supersecret@localhost:5435/okg_x"
    ) == "postgres://postgres:***@localhost:5435/okg_x"
