"""A refused deployment.yaml fails closed on every reader (#2713).

The loader refuses a misspelled key with ``DeploymentConfigError``. Each
test below feeds a manifest with a misspelled top-level key through one
path that used to skip, crash on, or swallow that refusal. Decision D1 of
#2573: exit 1 with a one-line named reason, never a traceback and never a
silent ``None``.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.graph_store_internal.common import (
    CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV,
    deployment_interval_continuation_percent,
)

TYPO_KEY = "postgress"


@pytest.fixture(autouse=True)
def _isolated_env(tmp_path, monkeypatch):
    for name in (
        "OKG_DSN",
        "OKG_DEPLOYMENT",
        "OKG_ENVIRONMENT_DESCRIPTOR",
        "OKG_RELEASE_ROOT",
        CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))


def _typo_manifest(tmp_path: Path) -> Path:
    d = tmp_path / "typo"
    d.mkdir()
    path = d / "deployment.yaml"
    path.write_text(
        "name: typo\n"
        "postgres:\n"
        "  dsn: ${OKG_DSN}\n"
        f"{TYPO_KEY}:\n"
        "  dsn: postgresql://u:p@127.0.0.1:1/x\n"
    )
    return path


def _assert_one_line_refusal(stderr: str) -> None:
    assert "Traceback" not in stderr, stderr
    lines = [line for line in stderr.splitlines() if line.strip()]
    assert len(lines) == 1, stderr
    assert "unknown top-level key" in lines[0], stderr
    assert repr(TYPO_KEY) in lines[0], stderr


def test_deployment_env_refuses_misspelled_key(tmp_path, capsys) -> None:
    manifest = _typo_manifest(tmp_path)

    rc = main(["deployment", "env", str(manifest)])

    captured = capsys.readouterr()
    assert rc == 1, captured
    assert "postgres.dsn" not in captured.out, captured.out
    _assert_one_line_refusal(captured.err)


def test_deployment_env_json_refuses_misspelled_key(tmp_path, capsys) -> None:
    manifest = _typo_manifest(tmp_path)

    rc = main(["deployment", "env", str(manifest), "--json"])

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line_refusal(captured.err)
    payload = json.loads(captured.err)
    assert TYPO_KEY in payload["error"]["message"]


def test_deployment_show_refuses_misspelled_key(tmp_path, capsys) -> None:
    manifest = _typo_manifest(tmp_path)

    rc = main(["deployment", "show", str(manifest)])

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line_refusal(captured.err)


def test_deployment_show_json_refuses_misspelled_key(tmp_path, capsys) -> None:
    manifest = _typo_manifest(tmp_path)

    rc = main(["deployment", "show", str(manifest), "--json"])

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line_refusal(captured.err)
    assert TYPO_KEY in json.loads(captured.err)["error"]["message"]


def test_doctor_refuses_misspelled_key_before_connecting(
    tmp_path, capsys, monkeypatch,
) -> None:
    manifest = _typo_manifest(tmp_path)

    def _no_connect(*_args, **_kwargs):
        raise AssertionError("doctor connected before refusing the manifest")

    monkeypatch.setattr("okg.substrate.db.connection.connect", _no_connect)

    rc = main([
        "doctor",
        "--deployment", str(manifest),
        "--dsn", "postgresql://u:p@127.0.0.1:1/x",
    ])

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line_refusal(captured.err)



def test_doctor_json_reports_refusal_not_dsn_error_without_dsn_flag(
    tmp_path, capsys,
) -> None:
    # With no --dsn, the refusal must win over any DSN-resolution error, so
    # the operator sees the misspelled key rather than a missing postgres.dsn.
    manifest = _typo_manifest(tmp_path)

    rc = main(["doctor", "--deployment", str(manifest), "--json"])

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line_refusal(captured.err)
    assert TYPO_KEY in json.loads(captured.err)["error"]["message"]

def test_interval_reader_raises_on_refused_manifest(tmp_path) -> None:
    manifest = _typo_manifest(tmp_path)

    with pytest.raises(DeploymentConfigError, match=TYPO_KEY):
        deployment_interval_continuation_percent(str(manifest))


def test_interval_reader_none_when_deployment_not_found(
    tmp_path, monkeypatch,
) -> None:
    # Graph storage runs outside any deployment directory; a name that no
    # search root holds is "no deployment default", not a refused manifest.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))

    assert deployment_interval_continuation_percent("no-such-deployment-2713") is None


def test_interval_reader_raises_when_sealed_resolution_fails(
    tmp_path, monkeypatch,
) -> None:
    # A sealed process has one pinned environment; a resolution error there
    # (here, an incomplete descriptor) must not read as "no default".
    from okg.substrate.deployments import DeploymentResolutionError

    monkeypatch.setenv("OKG_RELEASE_ROOT", str(tmp_path))

    with pytest.raises(DeploymentResolutionError, match="incomplete"):
        deployment_interval_continuation_percent("any-deployment")



# The loader refuses in other ways besides DeploymentConfigError: a typo in
# the search, mcp or agent_surface block raises a typed SubstrateError, a
# typo inside the postgres or chronos block raises DeploymentConfigError, a
# manifest with no schema_dir raises ValueError, a list or scalar top level
# raises DeploymentManifestNotMappingError, a file that is not YAML raises
# yaml.YAMLError, and an unreadable file raises OSError. Each must still be
# one line naming the key, the parse position or the OS error.
_VALID_HEAD = (
    "name: p\n"
    "schema_dir: schema\n"
    "source_registry: source_registry.yaml\n"
    "postgres:\n"
    "  dsn: postgresql://u:p@127.0.0.1:1/x\n"
)
_OTHER_REFUSALS = {
    "search_typo": (
        _VALID_HEAD + "search:\n  defualt_profile: x\n",
        ("defualt_profile",),
    ),
    "mcp_typo": (
        _VALID_HEAD + "mcp:\n  tool_profile: {}\n",
        ("'tool_profile'",),
    ),
    # #2657: keys inside the postgres and chronos blocks.
    "postgres_block_typo": (
        _VALID_HEAD + "  databse: x\n",
        ("postgres block", "'databse'"),
    ),
    "chronos_block_typo": (
        _VALID_HEAD + "chronos:\n  interval_continuation_percnt: 10\n",
        ("chronos block", "'interval_continuation_percnt'"),
    ),
    "agent_surface_typo": (
        _VALID_HEAD + "agent_surface:\n  instructons: x\n",
        ("'instructons'",),
    ),
    "missing_schema_dir": (
        "name: p\npostgres:\n  dsn: postgresql://u:p@127.0.0.1:1/x\n",
        ("schema_dir",),
    ),
    "unclosed_flow_list": (
        "name: p\npostgres:\n  dsn: [unclosed\n",
        ("not valid YAML", "deployment.yaml", "started at line 3"),
    ),
    "tab_indent": (
        "name: p\npostgres:\n\tdsn: x\n",
        ("not valid YAML", "deployment.yaml", "line 3, column 1"),
    ),
    "top_level_list": (
        "- name: p\n- schema_dir: schema\n",
        ("top level must be a mapping", "not a list"),
    ),
    "top_level_string": (
        "just a string\n",
        ("top level must be a mapping", "not a string"),
    ),
    "unreadable": (
        _VALID_HEAD,
        ("cannot read deployment manifest", "deployment.yaml",
         "Permission denied"),
    ),
    "not_utf8": (
        _VALID_HEAD.encode() + b"description: caf\xe9\n",
        ("deployment.yaml is not UTF-8 text", "at byte"),
    ),
}

# `okg deployment env` accepts a partial manifest, so a missing schema_dir
# is not a refusal there.
_ENV_REFUSALS = sorted(set(_OTHER_REFUSALS) - {"missing_schema_dir"})


def _other_manifest(
    tmp_path: Path, case: str, monkeypatch,
) -> tuple[Path, tuple[str, ...]]:
    text, needles = _OTHER_REFUSALS[case]
    d = tmp_path / case
    d.mkdir()
    (d / "schema").mkdir()
    (d / "source_registry.yaml").write_text("sources: []\n")
    path = d / "deployment.yaml"
    if isinstance(text, bytes):
        path.write_bytes(text)
    else:
        path.write_text(text)
    if case == "unreadable":
        # chmod 000 does not stop root (CI containers), so refuse the read
        # where the loader opens the file instead.
        import builtins

        import okg.substrate.deployments as deployments

        real_open = builtins.open

        def _open(file, *args, **kwargs):
            if Path(file) == path:
                raise PermissionError(13, "Permission denied", str(path))
            return real_open(file, *args, **kwargs)

        monkeypatch.setattr(deployments, "open", _open, raising=False)
    return path, needles


def _assert_one_line(stderr: str, needles: tuple[str, ...]) -> None:
    assert "Traceback" not in stderr, stderr
    lines = [line for line in stderr.splitlines() if line.strip()]
    assert len(lines) == 1, stderr
    for needle in needles:
        assert needle in lines[0], stderr
    assert "local env file" not in lines[0], stderr


def _no_connect(monkeypatch) -> None:
    def _refuse(*_args, **_kwargs):
        raise AssertionError("doctor connected before refusing the manifest")

    monkeypatch.setattr("okg.substrate.db.connection.connect", _refuse)


@pytest.mark.parametrize("case", sorted(_OTHER_REFUSALS))
@pytest.mark.parametrize("as_json", [False, True])
def test_deployment_show_other_refusals_are_one_line(
    tmp_path, capsys, monkeypatch, case, as_json,
) -> None:
    manifest, needles = _other_manifest(tmp_path, case, monkeypatch)

    rc = main(
        ["deployment", "show", str(manifest)] + (["--json"] if as_json else [])
    )

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line(captured.err, needles)


@pytest.mark.parametrize("case", sorted(_OTHER_REFUSALS))
@pytest.mark.parametrize("as_json", [False, True])
def test_doctor_other_refusals_are_one_line(
    tmp_path, capsys, monkeypatch, case, as_json,
) -> None:
    manifest, needles = _other_manifest(tmp_path, case, monkeypatch)
    _no_connect(monkeypatch)

    rc = main(
        ["doctor", "--deployment", str(manifest)]
        + (["--json"] if as_json else [])
    )

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line(captured.err, needles)


@pytest.mark.parametrize("case", _ENV_REFUSALS)
@pytest.mark.parametrize("as_json", [False, True])
def test_deployment_env_other_refusals_are_one_line(
    tmp_path, capsys, monkeypatch, case, as_json,
) -> None:
    manifest, needles = _other_manifest(tmp_path, case, monkeypatch)

    rc = main(
        ["deployment", "env", str(manifest)] + (["--json"] if as_json else [])
    )

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line(captured.err, needles)


@pytest.mark.parametrize("as_json", [False, True])
def test_doctor_refuses_missing_manifest_path(
    tmp_path, capsys, monkeypatch, as_json,
) -> None:
    # A manifest path that does not exist used to be ignored: doctor went on
    # to check whatever database --dsn named.
    missing = tmp_path / "nowhere" / "deployment.yaml"
    _no_connect(monkeypatch)

    rc = main(
        [
            "doctor",
            "--deployment", str(missing),
            "--dsn", "postgresql://u:p@127.0.0.1:1/x",
        ]
        + (["--json"] if as_json else [])
    )

    captured = capsys.readouterr()
    assert rc == 1, captured
    _assert_one_line(captured.err, ("not found", str(missing)))


def test_refusal_names_a_declared_path_it_cannot_read(tmp_path) -> None:
    # A schema_dir inside a directory okg cannot enter raises OSError for the
    # schema path, not the manifest; the line must not blame the manifest.
    # Built directly because chmod 000 does not stop root in CI containers.
    from okg.substrate.cli.deployment import manifest_refusal

    manifest, _ = _other_manifest(tmp_path, "search_typo", None)
    schema = manifest.parent / "locked" / "schema"
    exc = PermissionError(13, "Permission denied", str(schema))

    message = manifest_refusal(exc, str(manifest)).message

    assert message == (
        f"cannot read {schema} while loading deployment manifest "
        f"{manifest.resolve()}: Permission denied"
    )


def test_refusal_names_the_manifest_when_it_is_the_unreadable_file(
    tmp_path,
) -> None:
    from okg.substrate.cli.deployment import manifest_refusal

    manifest, _ = _other_manifest(tmp_path, "search_typo", None)
    exc = PermissionError(13, "Permission denied", str(manifest))

    message = manifest_refusal(exc, str(manifest)).message

    assert message == (
        f"cannot read deployment manifest {manifest}: Permission denied"
    )


def test_doctor_bare_unknown_name_still_reaches_the_database(
    tmp_path, capsys, monkeypatch,
) -> None:
    # Only a path-shaped --deployment is refused when missing. A bare name the
    # lookup cannot find keeps the old behavior: doctor checks the database
    # --dsn names. A name may contain a slash (a nested deployment under a
    # search root), so doctor does not refuse on the separator alone.
    empty_root = tmp_path / "deployments"
    empty_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty_root))
    dsn = "postgresql://u:p@127.0.0.1:1/x"
    seen: list[str] = []

    def _record_connect(target, *_args, **_kwargs):
        seen.append(target)
        raise RuntimeError("stop after the connect attempt")

    monkeypatch.setattr("okg.substrate.db.connection.connect", _record_connect)

    main(["doctor", "--deployment", "no-such-deployment", "--dsn", dsn])

    captured = capsys.readouterr()
    assert seen == [dsn], captured
    assert "not found" not in captured.err, captured
