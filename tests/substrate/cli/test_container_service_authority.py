"""The supervisor adapter is declared per target, never inferred from the host.

`okg-workspace` runs two ways at once: a launchd worker on the operator's
machine and a container in the Kubernetes lifecycle lane. Inferring the adapter
from `platform.system()` gives the in-cluster instantiation a host-supervisor
identity it can never satisfy, which is how deployment-profile-kubernetes went
red on every branch whose lifecycle rows execute.
"""

from __future__ import annotations

import platform
from pathlib import Path

import pytest

from okg.substrate import deployment_apply as da
from okg.substrate.errors import SubstrateError

REPO_ROOT = Path(__file__).resolve().parents[3]
TARGET = {"host": "h", "port": 5432, "dbname": "d"}


# ---------------------------------------------------------------------------
# declared
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("adapter", da.SUPERVISOR_ADAPTERS)
def test_declared_adapter_is_honoured(adapter: str) -> None:
    assert da.resolve_supervisor_adapter(
        declared=adapter, deployment="d", runtime_enabled=True
    ) == adapter


@pytest.mark.parametrize("host", ["Darwin", "Linux"])
def test_declared_container_survives_any_planning_host(
    host: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Planning inside a pod must not select systemd.

    This is the exact rebuild-the-bug case: the desired adapter must follow the
    declaration, not the OS the planning process happens to run on.
    """

    monkeypatch.setattr(platform, "system", lambda: host)
    authority = da._tracking_service_authority(
        deployment="okg-workspace",
        deployment_dir=Path("/tmp/x"),
        sync_root=None,
        target=TARGET,
        supervisor="container",
    )
    assert authority["adapter"] == "container"
    assert authority["working_directory"] == str(da.CONTAINER_ROOT)


def test_declared_overlay_overrides_the_baked_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The chart's declaration wins over the manifest baked into the image.

    `COPY . .` puts the same deployment.yaml in the image that the host reads,
    so the manifest cannot be truthful for both targets. The overlay is the
    narrower, target-scoped declaration.
    """

    monkeypatch.setenv(da.SUPERVISOR_OVERLAY_ENV, "container")
    assert da.resolve_supervisor_adapter(
        declared="launchd", deployment="okg-workspace", runtime_enabled=True
    ) == "container"


def test_declared_manifest_stands_when_no_overlay_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(da.SUPERVISOR_OVERLAY_ENV, raising=False)
    assert da.resolve_supervisor_adapter(
        declared="launchd", deployment="okg-workspace", runtime_enabled=True
    ) == "launchd"


def test_declared_blank_overlay_is_not_a_declaration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty env var must not be read as a declaration of anything."""

    monkeypatch.setenv(da.SUPERVISOR_OVERLAY_ENV, "   ")
    assert da.supervisor_overlay() is None
    assert da.resolve_supervisor_adapter(
        declared="launchd", deployment="d", runtime_enabled=True
    ) == "launchd"


def test_declared_unknown_overlay_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad overlay is refused, not silently ignored in favour of the manifest."""

    monkeypatch.setenv(da.SUPERVISOR_OVERLAY_ENV, "docker")
    with pytest.raises(SubstrateError):
        da.resolve_supervisor_adapter(
            declared="launchd", deployment="d", runtime_enabled=True
        )


def test_declared_unknown_adapter_raises() -> None:
    with pytest.raises(SubstrateError) as exc:
        da.resolve_supervisor_adapter(
            declared="kubernetes", deployment="d", runtime_enabled=True
        )
    assert "unsupported" in str(exc.value)


def test_declared_absent_on_new_contract_raises_naming_the_field() -> None:
    with pytest.raises(SubstrateError) as exc:
        da.resolve_supervisor_adapter(
            declared=None,
            deployment="d",
            runtime_enabled=True,
            contract_version=da.SUPERVISOR_REQUIRED_CONTRACT_VERSION,
        )
    message = str(exc.value)
    assert "runtime.supervisor" in message


def test_declared_absent_on_old_contract_keeps_host_inference() -> None:
    """A live host worker must not hit a surprise raise mid-upgrade."""

    assert da.resolve_supervisor_adapter(
        declared=None, deployment="d", runtime_enabled=True, contract_version=1
    ) is None
    assert da.resolve_supervisor_adapter(
        declared=None, deployment="d", runtime_enabled=True, contract_version=None
    ) is None


def test_declared_absent_without_runtime_does_not_raise() -> None:
    assert da.resolve_supervisor_adapter(
        declared=None,
        deployment="d",
        runtime_enabled=False,
        contract_version=da.SUPERVISOR_REQUIRED_CONTRACT_VERSION,
    ) is None


# ---------------------------------------------------------------------------
# identity
# ---------------------------------------------------------------------------

HOST_IDENTITY_FIELDS = (
    "working_directory",
    "process_executable",
    "interpreter",
    "import_root",
    "code_revision",
    "database",
)


def test_identity_container_carries_every_field_the_host_adapter_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remapped, not relaxed: no field may be dropped for the container."""

    monkeypatch.setenv("OKG_CODE_REVISION", "abc123")
    authority = da._container_service_authority(deployment="d", target=TARGET)
    for field in HOST_IDENTITY_FIELDS:
        assert field in authority, field
        assert authority[field] is not None, field


def test_identity_container_paths_are_rooted_in_the_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_CODE_REVISION", "abc123")
    a = da._container_service_authority(deployment="d", target=TARGET)
    assert a["working_directory"] == "/opt/okg"
    assert a["interpreter"] == a["process_executable"]


WHEEL_SITE_PACKAGES = "/opt/okg/.venv/lib/python3.12/site-packages"


def test_identity_container_import_root_is_the_images_site_packages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The image installs okg from a wheel into .venv, not at /opt/okg/src.

    With deployment_apply imported from the image's site-packages, the desired
    import_root is that site-packages, and the worker's observed authority
    (the same process layout) reports the same path, so the gate matches. A
    worker importing okg from /opt/okg/src still refuses.
    """

    monkeypatch.setenv("OKG_CODE_REVISION", "abc123")
    monkeypatch.setattr(
        da, "__file__", f"{WHEEL_SITE_PACKAGES}/okg/substrate/deployment_apply.py",
    )
    desired = da._container_service_authority(deployment="d", target=TARGET)
    assert desired["import_root"] == WHEEL_SITE_PACKAGES

    observed = da._observed_runtime_authority(
        deployment="d",
        deployment_dir="/unused",
        dsn="postgresql://okg@h:5432/d",
    )
    assert observed["import_root"] == WHEEL_SITE_PACKAGES

    mismatches = da._worker_ack_mismatches(
        {"runtime_enabled": True, "service_authority": desired},
        {"observed_runtime_authority": dict(desired, import_root="/opt/okg/src")},
    )
    assert any(reason.startswith("identity import_root differs") for reason in mismatches)


def test_identity_interpreter_matches_the_acking_containers_invocation() -> None:
    """The spelling is load-bearing and depends on HOW the process was started.

    sys.executable preserves the exec'd name rather than resolving the symlink,
    and both spellings occur in this image: `command: ["okg"]` goes through a
    shebang of /opt/okg/.venv/bin/python3, while `command: ["python","-c"]`
    resolves via PATH to .../python. The process that records the ack is the
    `worker` container, which uses `command: ["okg"]` -- so python3 is correct.
    Measured in the built image, not inferred from a developer machine, where
    the shebang differs.
    """

    a = da._container_service_authority(deployment="d", target=TARGET)
    assert a["process_executable"] == "/opt/okg/.venv/bin/python3"
    assert a["interpreter"] == a["process_executable"]


def test_identity_declaration_is_coupled_to_the_chart_invocation() -> None:
    """Pin the coupling that makes python3 correct.

    If the worker container stops using `command: ["okg"]`, sys.executable
    changes spelling and the identity gate refuses with no other signal. This
    fails loudly at that edit instead.
    """

    chart = (
        REPO_ROOT
        / "ops" / "kubernetes" / "okg" / "templates" / "runtime-statefulset.yaml"
    ).read_text(encoding="utf-8")
    worker = chart.split("- name: worker\n", 1)[-1].split("- name: ", 1)[0]
    assert 'command: ["okg"]' in worker, (
        "the worker container no longer invokes the okg console script; "
        "CONTAINER_INTERPRETER's python3 spelling depends on that shebang"
    )


def test_identity_unknown_build_revision_is_not_a_revision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`ARG VCS_REF=unknown` must never satisfy the gate.

    A build path that omits --build-arg bakes the literal "unknown". Both sides
    would then read it and compare EQUAL -- a false convergence that passes the
    gate having verified nothing. Absent is the honest answer.
    """

    monkeypatch.setenv("OKG_CODE_REVISION", da.UNKNOWN_BUILD_REVISION)
    assert da.container_code_revision() is None
    a = da._container_service_authority(deployment="d", target=TARGET)
    assert a["code_revision"] is None
    desired = {"runtime_enabled": True, "service_authority": a}
    ack = {"observed_runtime_authority": dict(a)}
    assert da._worker_ack_matches(desired, ack) is False


def test_identity_revision_comes_from_the_build_stamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_CODE_REVISION", "deadbeef")
    assert da.container_code_revision() == "deadbeef"
    a = da._container_service_authority(deployment="d", target=TARGET)
    assert a["code_revision"] == "deadbeef"


def test_identity_absent_revision_stays_none_so_the_gate_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown revision is never agreement."""

    monkeypatch.delenv("OKG_CODE_REVISION", raising=False)
    assert da.container_code_revision() is None
    a = da._container_service_authority(deployment="d", target=TARGET)
    assert a["code_revision"] is None
    # And the gate refuses on it rather than treating absence as a match.
    # Both sides absent is agreement about nothing: this is the None == None
    # hole req.absent-identity-still-refuses closes.
    desired = {"runtime_enabled": True, "service_authority": a}
    ack = {"observed_runtime_authority": dict(a)}
    assert da._worker_ack_matches(desired, ack) is False


def test_identity_blank_revision_is_treated_as_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_CODE_REVISION", "   ")
    assert da.container_code_revision() is None


# ---------------------------------------------------------------------------
# mismatch
# ---------------------------------------------------------------------------


def test_mismatch_host_desired_against_container_observed_refuses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dual-target failure: a host manifest observed inside a container."""

    monkeypatch.setattr(platform, "system", lambda: "Linux")
    host = da._tracking_service_authority(
        deployment="d",
        deployment_dir=Path("/srv/checkout"),
        sync_root=Path("/srv/checkout"),
        target=TARGET,
        supervisor="systemd",
    )
    monkeypatch.setenv("OKG_CODE_REVISION", "abc123")
    container = da._container_service_authority(deployment="d", target=TARGET)
    desired = {"runtime_enabled": True, "service_authority": host}
    ack = {"observed_runtime_authority": container}
    assert da._worker_ack_matches(desired, ack) is False
    # The paths genuinely differ, which is why a shared identity model cannot
    # serve both targets.
    assert host["working_directory"] != container["working_directory"]
    assert host["import_root"] != container["import_root"]
