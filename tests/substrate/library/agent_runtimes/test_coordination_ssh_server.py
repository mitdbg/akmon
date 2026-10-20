from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
from dataclasses import asdict, replace
from pathlib import Path
import pwd
import signal
import socket
import stat
import subprocess
import sys
import textwrap
import time
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from okg.substrate.library.agent_runtimes import coordination_ssh_server as server


ROUTE = "coordination-v1"
PRINCIPAL = "operator:alice"
SESSION_ID = "session:2026-08-30"
SOURCE_ADDRESS = "127.0.0.1"
TARGET_ADDRESS = "127.0.0.1"
REPORT = ".scratch/junit/coordination-ssh-server.xml"
# The workflow job that carries the blocking proof, and the one runner class
# that can run it: the docker pod class is the only one with passwordless sudo.
PROOF_JOB = "coordination-ssh-proof"
PROOF_RUNNER = "okg-arc-docker"
TEST_MODULE = "tests/substrate/library/agent_runtimes/test_coordination_ssh_server.py"
TEST_CLASS = "tests.substrate.library.agent_runtimes.test_coordination_ssh_server"
# Every binary the sudo-unshare isolation backend needs on the runner. The
# blocking step must name each one; dropping one from its loop is a weakening.
REQUIRED_PREREQUISITE_BINARIES = (
    "/usr/sbin/sshd",
    "/usr/bin/ssh",
    "/usr/bin/ssh-keygen",
    "/usr/bin/sudo",
    "/usr/bin/unshare",
    "/usr/bin/mount",
)
# The step has exactly two fail-closed arms that raise `missing`: an absent
# binary and an unusable passwordless sudo. Both must survive. /run/sshd is
# deliberately NOT one of them: the loopback test creates it itself under the
# sudo it has already proven (PR #1525), so gating the step on it would fail
# closed on hosts the test would have repaired.
FAIL_CLOSED_ARMS = ("absent binary", "unusable passwordless sudo")
REQUIRED_NODEIDS = (
    f"{TEST_CLASS}::test_authenticates_only_the_explicit_enrolled_identity",
    f"{TEST_CLASS}::test_refuses_commands_connections_and_open_enrollments",
    f"{TEST_CLASS}::test_refuses_unsafe_replaced_or_mutated_authentication_evidence",
    f"{TEST_CLASS}::test_refuses_malformed_or_nonunique_public_key_bindings",
    f"{TEST_CLASS}::test_real_loopback_sshd_force_command_binds_identity_and_refuses_command",
    f"{TEST_CLASS}::test_blocking_ci_step_resists_omission_and_weakening",
)


def _public_key(algorithm: str = "ssh-ed25519", payload: bytes = b"K" * 32) -> str:
    algorithm_bytes = algorithm.encode("ascii")
    blob = (
        len(algorithm_bytes).to_bytes(4, "big")
        + algorithm_bytes
        + len(payload).to_bytes(4, "big")
        + payload
    )
    return f"{algorithm} {base64.b64encode(blob).decode('ascii')} fixture"


def _write_auth_info(path: Path, public_key: str | None = None) -> Path:
    path.write_text(f"publickey {public_key or _public_key()}\n", encoding="ascii")
    path.chmod(0o600)
    return path


def _enrollment(
    public_key: str | None = None,
) -> server.CoordinationSshEnrollment:
    return server.CoordinationSshEnrollment(
        credential_id=server.coordination_ssh_public_key_credential_id(
            public_key or _public_key()
        ),
        principal=PRINCIPAL,
        session_id=SESSION_ID,
        allowed_routes=(ROUTE,),
        allowed_source_addresses=(SOURCE_ADDRESS,),
        allowed_target_addresses=(TARGET_ADDRESS,),
    )


def _environment(auth_info: Path) -> dict[str, str]:
    return {
        "SSH_USER_AUTH": str(auth_info),
        "SSH_CONNECTION": f"{SOURCE_ADDRESS} 41000 {TARGET_ADDRESS} 2222",
        "operator_principal": "forged:principal",
        "operator_session_id": "forged:session",
        "OKG_COORDINATION_ROUTE": "forged-route",
    }


def _authenticate(
    auth_info: Path,
    *,
    environ: dict[str, str] | None = None,
    route: str = ROUTE,
    enrollments: tuple[server.CoordinationSshEnrollment, ...] | None = None,
) -> server.CoordinationSshServerIdentity:
    return server.authenticate_coordination_ssh_session(
        _environment(auth_info) if environ is None else environ,
        route=route,
        expected_auth_file_uid=os.getuid(),
        enrollments=(_enrollment(),) if enrollments is None else enrollments,
    )


def _assert_refused(
    auth_info: Path,
    *,
    reason_code: str,
    environ: dict[str, str] | None = None,
    route: str = ROUTE,
    enrollments: tuple[server.CoordinationSshEnrollment, ...] | None = None,
) -> None:
    with pytest.raises(server.CoordinationSshAuthenticationError) as raised:
        _authenticate(
            auth_info,
            environ=environ,
            route=route,
            enrollments=enrollments,
        )
    assert raised.value.reason_code == reason_code


def test_authenticates_only_the_explicit_enrolled_identity(tmp_path: Path) -> None:
    auth_info = _write_auth_info(tmp_path / "auth-info")

    identity = _authenticate(auth_info)

    assert identity == server.CoordinationSshServerIdentity(
        credential_id=server.coordination_ssh_public_key_credential_id(_public_key()),
        principal=PRINCIPAL,
        session_id=SESSION_ID,
        route=ROUTE,
        source_address=SOURCE_ADDRESS,
        source_port=41000,
        target_address=TARGET_ADDRESS,
        target_port=2222,
    )
    assert "forged" not in repr(identity)

    auth_info.chmod(0o400)
    assert _authenticate(auth_info) == identity

    ipv6_auth_info = _write_auth_info(tmp_path / "auth-info-v6")
    ipv6_environment = _environment(ipv6_auth_info)
    ipv6_environment["SSH_CONNECTION"] = "2001:0db8::1 41000 ::ffff:192.0.2.1 2222"
    ipv6_enrollment = replace(
        _enrollment(),
        allowed_source_addresses=("2001:db8::1",),
        allowed_target_addresses=("::ffff:c000:201",),
    )
    ipv6_identity = _authenticate(
        ipv6_auth_info,
        environ=ipv6_environment,
        enrollments=(ipv6_enrollment,),
    )
    assert ipv6_identity.source_address == "2001:db8::1"
    assert ipv6_identity.target_address == "::ffff:192.0.2.1"


def test_refuses_commands_connections_and_open_enrollments(tmp_path: Path) -> None:
    auth_info = _write_auth_info(tmp_path / "auth-info")

    for command in ("", "status"):
        environment = _environment(auth_info)
        environment["SSH_ORIGINAL_COMMAND"] = command
        _assert_refused(
            auth_info,
            reason_code="caller_command_refused",
            environ=environment,
        )

    invalid_connections = (
        "127.0.0.1 41000 127.0.0.1",
        "localhost 41000 127.0.0.1 2222",
        "127.0.0.1 +41000 127.0.0.1 2222",
        "127.0.0.1 04100 127.0.0.1 2222",
        "127.0.0.1 0 127.0.0.1 2222",
        "127.0.0.1 41000 127.0.0.1 65536",
        f"127.0.0.1 {'9' * 10000} 127.0.0.1 2222",
        "fe80::1%lo 41000 127.0.0.1 2222",
    )
    for connection in invalid_connections:
        environment = _environment(auth_info)
        environment["SSH_CONNECTION"] = connection
        _assert_refused(
            auth_info,
            reason_code="invalid_connection_evidence",
            environ=environment,
        )

    _assert_refused(auth_info, reason_code="invalid_route", route=" route")
    _assert_refused(
        auth_info,
        reason_code="credential_not_uniquely_enrolled",
        enrollments=(),
    )
    _assert_refused(
        auth_info,
        reason_code="authentication_evidence_missing",
        environ={},
    )
    for field in ("principal", "session_id"):
        for value in (None, "", " \t\n "):
            enrollment = replace(_enrollment(), **{field: value})
            _assert_refused(
                auth_info,
                reason_code="invalid_enrollment",
                enrollments=(enrollment,),
            )
    for field in (
        "allowed_routes",
        "allowed_source_addresses",
        "allowed_target_addresses",
    ):
        enrollment = replace(_enrollment(), **{field: ()})
        _assert_refused(
            auth_info,
            reason_code="invalid_enrollment",
            enrollments=(enrollment,),
        )
    duplicate_addresses = replace(
        _enrollment(),
        allowed_source_addresses=("2001:db8::1", "2001:0db8:0:0:0:0:0:1"),
    )
    _assert_refused(
        auth_info,
        reason_code="invalid_enrollment",
        enrollments=(duplicate_addresses,),
    )
    for enrollment, reason in (
        (replace(_enrollment(), allowed_routes=("other-route",)), "route_not_enrolled"),
        (
            replace(_enrollment(), allowed_source_addresses=("192.0.2.1",)),
            "source_not_enrolled",
        ),
        (
            replace(_enrollment(), allowed_target_addresses=("192.0.2.2",)),
            "target_not_enrolled",
        ),
    ):
        _assert_refused(
            auth_info,
            reason_code=reason,
            enrollments=(enrollment,),
        )

    with pytest.raises(server.CoordinationSshAuthenticationError) as raised:
        server.authenticate_coordination_ssh_session(
            _environment(auth_info),
            route=ROUTE,
            expected_auth_file_uid=os.getuid() + 1,
            enrollments=(_enrollment(),),
        )
    assert raised.value.reason_code == "invalid_expected_uid"


def test_refuses_unsafe_replaced_or_mutated_authentication_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    auth_info = _write_auth_info(tmp_path / "auth-info")

    auth_info.chmod(0o640)
    _assert_refused(auth_info, reason_code="unsafe_authentication_evidence")
    auth_info.chmod(0o600)

    hard_link = tmp_path / "hard-link"
    os.link(auth_info, hard_link)
    _assert_refused(auth_info, reason_code="unsafe_authentication_evidence")
    hard_link.unlink()

    symlink = tmp_path / "symlink"
    symlink.symlink_to(auth_info)
    _assert_refused(symlink, reason_code="unsafe_authentication_evidence")

    observed = auth_info.lstat()
    wrong_owner = SimpleNamespace(
        st_mode=observed.st_mode,
        st_uid=os.getuid() + 1,
        st_nlink=observed.st_nlink,
        st_size=observed.st_size,
    )
    real_lstat = server.Path.lstat
    monkeypatch.setattr(server.Path, "lstat", lambda self: wrong_owner)
    _assert_refused(auth_info, reason_code="unsafe_authentication_evidence")
    monkeypatch.setattr(server.Path, "lstat", real_lstat)

    replacement = _write_auth_info(tmp_path / "replacement")
    real_open = os.open

    def replace_before_open(path: str | os.PathLike[str], flags: int) -> int:
        replacement.replace(auth_info)
        return real_open(path, flags)

    monkeypatch.setattr(server.os, "open", replace_before_open)
    _assert_refused(auth_info, reason_code="changed_authentication_evidence")
    monkeypatch.setattr(server.os, "open", real_open)

    auth_info = _write_auth_info(tmp_path / "auth-info-after-open")
    real_read = os.read
    changed = False

    def mutate_after_read(descriptor: int, size: int) -> bytes:
        nonlocal changed
        data = real_read(descriptor, size)
        if data and not changed:
            os.fchmod(descriptor, 0o400)
            changed = True
        return data

    monkeypatch.setattr(server.os, "read", mutate_after_read)
    _assert_refused(auth_info, reason_code="changed_authentication_evidence")


def test_refuses_malformed_or_nonunique_public_key_bindings(tmp_path: Path) -> None:
    auth_info = _write_auth_info(tmp_path / "auth-info")
    duplicate = _enrollment()
    _assert_refused(
        auth_info,
        reason_code="credential_not_uniquely_enrolled",
        enrollments=(duplicate, duplicate),
    )
    _assert_refused(
        auth_info,
        reason_code="credential_not_uniquely_enrolled",
        enrollments=(replace(_enrollment(), credential_id="ssh-key:sha256:other"),),
    )

    malformed_records = (
        "password accepted\n",
        f"publickey {_public_key()}\npublickey {_public_key()}\n",
        "publickey ssh-ed25519 not-base64\n",
        f"publickey ssh-rsa {_public_key().split()[1]}\n",
        f"publickey {_public_key()}\x7f\n",
    )
    for index, record in enumerate(malformed_records):
        candidate = tmp_path / f"malformed-{index}"
        candidate.write_text(record, encoding="ascii")
        candidate.chmod(0o600)
        _assert_refused(candidate, reason_code="invalid_authentication_record")


def _reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind((SOURCE_ADDRESS, 0))
        return int(listener.getsockname()[1])


def _wait_for_sshd(process: subprocess.Popen[str], port: int, log_path: Path) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            with socket.create_connection((SOURCE_ADDRESS, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    detail = log_path.read_text(encoding="utf-8", errors="replace")[-4000:]
    raise AssertionError(f"loopback sshd did not become ready: {detail}")


def _run_ssh(
    *, port: int, user: str, private_key: Path, known_hosts: Path, command: str | None
) -> subprocess.CompletedProcess[str]:
    argv = [
        "/usr/bin/ssh",
        "-F",
        "/dev/null",
        "-T",
        "-p",
        str(port),
        "-i",
        str(private_key),
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "IdentityAgent=none",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "KbdInteractiveAuthentication=no",
        f"{user}@{SOURCE_ADDRESS}",
    ]
    if command is not None:
        argv.append(command)
    return subprocess.run(argv, text=True, capture_output=True, timeout=15, check=False)


# `Popen.terminate()` cannot end the sudo-unshare fixture. The direct child is
# `sudo`, which makes its own real uid root before it forks `unshare`, so a
# SIGTERM from the unprivileged runner account is refused with EPERM: the
# `unshare` and `sshd` pair survives the test, inside the GitHub runner
# service's cgroup, for the life of the host. Those leaked pairs are what made
# the runner unit a needrestart target during the nightly unattended upgrade,
# and needrestart then restarted it mid-job (issues #1553, #1595).
#
# So signal the whole process GROUP -- the fixture is started with
# `start_new_session=True`, so the child's pid is its group id and the
# `unshare` child and its `sshd` are in it -- escalate TERM to KILL, and reap
# the direct child either way. The privileged arm runs the same `os.killpg`
# through sudo, using the /usr/bin/sudo and /usr/bin/python3 the sudo-unshare
# fixture already requires, so it adds no prerequisite and leaves no room for
# a shell to reinterpret the negative group id.
_KILLPG_SOURCE = (
    "import os, signal, sys\n"
    "try:\n"
    "    os.killpg(int(sys.argv[1]), getattr(signal, 'SIG' + sys.argv[2]))\n"
    "except ProcessLookupError:\n"
    "    pass\n"
)


def _signal_isolated_group(pgid: int, signal_name: str, *, privileged: bool) -> None:
    """Send ``signal_name`` to every process in the group ``pgid``."""

    if not privileged:
        try:
            os.killpg(pgid, getattr(signal, f"SIG{signal_name}"))
        except ProcessLookupError:
            # The group is already gone, which is the state this asks for.
            pass
        return
    subprocess.run(
        [
            "/usr/bin/sudo",
            "-n",
            "/usr/bin/python3",
            "-c",
            _KILLPG_SOURCE,
            str(pgid),
            signal_name,
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        timeout=15,
        check=False,
    )


def _terminate_loopback_sshd(
    process: subprocess.Popen[str], *, privileged: bool
) -> None:
    """End the loopback sshd fixture and leave nothing of it behind.

    Runs from a ``finally``, so it must work when the test body failed as well
    as when it passed, and it must never raise a second exception over the
    first one.
    """

    if process.poll() is None:
        _signal_isolated_group(process.pid, "TERM", privileged=privileged)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            # It ignored TERM; the KILL below is exactly this case's answer.
            pass
    if process.poll() is None:
        _signal_isolated_group(process.pid, "KILL", privileged=privileged)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            # Unreapable after KILL means the kernel holds it (uninterruptible
            # sleep). Nothing a test can do; the sweep below still runs, and
            # the caller's own assertions report the leak rather than hanging.
            pass
    # A KILL to the group reaches the direct child too, but the group may hold
    # survivors the child never waited for, so sweep it once more. This is a
    # no-op when the group is already empty.
    _signal_isolated_group(process.pid, "KILL", privileged=privileged)


# Real OpenSSH fixture: owned by the explicit blocking workflow proof step.
@pytest.mark.integration
def test_real_loopback_sshd_force_command_binds_identity_and_refuses_command(
    tmp_path: Path,
) -> None:
    configured_backend = os.environ.get("OKG_COORDINATION_SSH_TEST_ISOLATION")
    # Only the blocking proof job (PROOF_JOB) names a backend; it installs
    # OpenSSH and runs this module under assert-no-skips, so there an absent
    # sshd still fails below. A lane that runs this test without naming a
    # backend -- the nightly coverage-full sweep, whose docker pod has no
    # OpenSSH server -- reports a visible skip instead of a false failure.
    if configured_backend is None and not Path("/usr/sbin/sshd").is_file():
        pytest.skip(
            "/usr/sbin/sshd is absent on this runner; the real-sshd proof is "
            f"owned by the {PROOF_JOB} job, which installs OpenSSH and refuses skips"
        )
    isolation_backend = configured_backend or "bwrap"
    assert isolation_backend in {"bwrap", "sudo-unshare"}
    required_binaries = [
        "/bin/sh",
        "/usr/sbin/sshd",
        "/usr/bin/ssh",
        "/usr/bin/ssh-keygen",
    ]
    if isolation_backend == "bwrap":
        required_binaries.append("/usr/bin/bwrap")
    else:
        required_binaries.extend(
            ("/usr/bin/sudo", "/usr/bin/unshare", "/usr/bin/mount")
        )
    for binary in required_binaries:
        assert Path(binary).is_file(), f"required OpenSSH binary is absent: {binary}"
    if isolation_backend == "sudo-unshare":
        sudo_probe = subprocess.run(
            ["/usr/bin/sudo", "-n", "true"],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        assert sudo_probe.returncode == 0, sudo_probe.stderr
        # OpenSSH refuses to start without its privilege separation directory:
        # "Missing privilege separation directory: /run/sshd". Ubuntu 24.04
        # socket-activates ssh and lets systemd-tmpfiles create it at boot, so
        # a host that installed openssh-server AFTER boot has never had one --
        # and this test launches sshd directly, so it never triggers the unit
        # that would. State the precondition here rather than depending on how
        # the host was provisioned. install -d is idempotent.
        privsep = subprocess.run(
            ["/usr/bin/sudo", "-n", "install", "-d", "-m", "0755", "/run/sshd"],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        assert privsep.returncode == 0, privsep.stderr
        assert Path("/run/sshd").is_dir()

    user = pwd.getpwuid(os.getuid()).pw_name
    private_key = tmp_path / "client-key"
    host_key = tmp_path / "host-key"
    for key_path in (private_key, host_key):
        generated = subprocess.run(
            [
                "/usr/bin/ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                str(key_path),
            ],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        assert generated.returncode == 0, generated.stderr

    client_public_key = (
        private_key.with_suffix(".pub").read_text(encoding="ascii").strip()
    )
    account_home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
    assert account_home.is_dir()
    runtime_environment = tmp_path / "runtime-environment"
    runtime_environment.mkdir(mode=0o700)
    trusted_key_directory = runtime_environment / ".ssh"
    trusted_key_directory.mkdir(mode=0o700)
    trusted_key_directory.chmod(0o700)
    authorized_keys = trusted_key_directory / "authorized_keys"
    authorized_keys.write_text(client_public_key + "\n", encoding="ascii")
    authorized_keys.chmod(0o600)
    # Generic runners create the service account without a password, which
    # leaves it locked before sshd can evaluate its authorized key. Keep the
    # host account untouched and expose an unlocked-but-unusable password
    # record only inside the test daemon's private mount namespace.
    assert ":" not in user and "\n" not in user
    isolated_shadow = tmp_path / "shadow"
    isolated_shadow.write_text(
        f"{user}:x:20000:0:99999:7:::\n",
        encoding="ascii",
    )
    isolated_shadow.chmod(0o600)
    passwd_records = []
    matching_passwd_records = 0
    for passwd_record in Path("/etc/passwd").read_text(encoding="ascii").splitlines():
        fields = passwd_record.split(":")
        if len(fields) == 7 and fields[0] == user:
            assert fields[2] == str(os.getuid())
            fields[6] = "/bin/sh"
            matching_passwd_records += 1
        passwd_records.append(":".join(fields))
    assert matching_passwd_records <= 1
    if matching_passwd_records == 0:
        account = pwd.getpwuid(os.getuid())
        passwd_records.append(
            f"{user}:x:{account.pw_uid}:{account.pw_gid}::{account_home}:/bin/sh"
        )
    isolated_passwd = tmp_path / "passwd"
    isolated_passwd.write_text("\n".join(passwd_records) + "\n", encoding="ascii")
    isolated_passwd.chmod(0o600)

    port = _reserve_loopback_port()
    known_hosts = tmp_path / "known_hosts"
    host_public_fields = (
        host_key.with_suffix(".pub").read_text(encoding="ascii").split()
    )
    known_hosts.write_text(
        f"[{SOURCE_ADDRESS}]:{port} {host_public_fields[0]} {host_public_fields[1]}\n",
        encoding="ascii",
    )

    credential_id = server.coordination_ssh_public_key_credential_id(client_public_key)
    server_module_path = Path(server.__file__).resolve()
    assert server_module_path.is_file()
    server_module_sha256 = hashlib.sha256(server_module_path.read_bytes()).hexdigest()
    isolated_server_module_path = tmp_path / "coordination_ssh_server.py"
    isolated_server_module_path.touch(mode=0o600)
    python_runtime_root = Path(sys.base_prefix).resolve()
    assert (python_runtime_root / "bin" / "python3").is_file()
    isolated_python_runtime = tmp_path / "python-runtime"
    isolated_python_runtime.mkdir(mode=0o700)
    force_command_python = isolated_python_runtime / "bin" / "python3"
    launcher = tmp_path / "coordination-force-command.py"
    launcher.write_text(
        textwrap.dedent(
            f"""\
            #!{force_command_python}
            import hashlib
            import importlib.util
            import json
            import os
            import pwd
            import stat
            import sys
            from dataclasses import asdict

            spec = importlib.util.spec_from_file_location(
                "coordination_ssh_server_force_command",
                {str(isolated_server_module_path)!r},
            )
            if spec is None or spec.loader is None:
                raise RuntimeError("coordination_ssh_server_module_unavailable")
            server = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = server
            spec.loader.exec_module(server)

            enrollment = server.CoordinationSshEnrollment(
                credential_id={credential_id!r},
                principal={PRINCIPAL!r},
                session_id={SESSION_ID!r},
                allowed_routes=({ROUTE!r},),
                allowed_source_addresses=({SOURCE_ADDRESS!r},),
                allowed_target_addresses=({TARGET_ADDRESS!r},),
            )
            identity = server.authenticate_coordination_ssh_session(
                os.environ,
                route=sys.argv[1],
                expected_auth_file_uid=os.getuid(),
                enrollments=(enrollment,),
            )
            result = asdict(identity)
            auth_file_stat = os.stat(os.environ["SSH_USER_AUTH"])
            result["auth_file_uid"] = auth_file_stat.st_uid
            result["auth_file_mode"] = stat.S_IMODE(auth_file_stat.st_mode)
            result["home"] = os.environ["HOME"]
            result["runtime_directory"] = os.environ["XDG_RUNTIME_DIR"]
            result["server_module_sha256"] = hashlib.sha256(
                open({str(isolated_server_module_path)!r}, "rb").read()
            ).hexdigest()
            login_account = pwd.getpwuid(os.getuid())
            login_home = login_account.pw_dir
            result["login_shell"] = login_account.pw_shell
            result["login_home_is_runtime"] = os.path.samefile(
                login_home,
                os.environ["HOME"],
            )
            result["login_home_has_bashrc"] = os.path.exists(
                os.path.join(login_home, ".bashrc")
            )
            print(json.dumps(result, sort_keys=True), flush=True)
            """
        ),
        encoding="utf-8",
    )
    launcher.chmod(0o700)

    sshd_log = tmp_path / "sshd.log"
    sshd_config = tmp_path / "sshd_config"
    sshd_config.write_text(
        "\n".join(
            (
                f"Port {port}",
                f"ListenAddress {SOURCE_ADDRESS}",
                "AddressFamily inet",
                f"HostKey {host_key}",
                f"PidFile {tmp_path / 'sshd.pid'}",
                "AuthorizedKeysFile .ssh/authorized_keys",
                "StrictModes yes",
                "PubkeyAuthentication yes",
                "AuthenticationMethods publickey",
                "PasswordAuthentication no",
                "KbdInteractiveAuthentication no",
                "ChallengeResponseAuthentication no",
                "UsePAM no",
                "PermitRootLogin no",
                f"AllowUsers {user}",
                "ExposeAuthInfo yes",
                f"ForceCommand {force_command_python} {launcher} {ROUTE}",
                "DisableForwarding yes",
                "PermitTTY no",
                "PermitUserEnvironment no",
                "PermitUserRC no",
                # A real forced command starts in sshd's login environment, not
                # pytest's import environment. Pin the checked-out production
                # module and a writable home/runtime directory from server-owned
                # configuration; no client environment is accepted.
                (
                    "SetEnv "
                    f"BASH_ENV=/dev/null ENV=/dev/null HOME={runtime_environment} "
                    f"XDG_RUNTIME_DIR={runtime_environment} PYTHONNOUSERSITE=1 "
                    f"PYTHONHOME={isolated_python_runtime}"
                ),
                "LogLevel VERBOSE",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    validated = subprocess.run(
        ["/usr/sbin/sshd", "-t", "-f", str(sshd_config)],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert validated.returncode == 0, validated.stderr

    if isolation_backend == "bwrap":
        sshd_command = [
            "/usr/bin/bwrap",
            "--die-with-parent",
            "--dev-bind",
            "/",
            "/",
            "--ro-bind",
            str(server_module_path),
            str(isolated_server_module_path),
            "--ro-bind",
            str(python_runtime_root),
            str(isolated_python_runtime),
            "--ro-bind",
            str(isolated_passwd),
            "/etc/passwd",
            "--ro-bind",
            str(isolated_shadow),
            "/etc/shadow",
            "--bind",
            str(runtime_environment),
            str(account_home),
            "--",
            "/usr/sbin/sshd",
            "-D",
            "-e",
            "-f",
            str(sshd_config),
        ]
    else:
        namespace_launcher = tmp_path / "launch-sshd-mount-namespace.py"
        namespace_launcher.write_text(
            textwrap.dedent(
                """\
                import os
                import stat
                import subprocess
                import sys

                (
                    user,
                    expected_uid_text,
                    passwd_source,
                    shadow_source,
                    account_home,
                    runtime_home,
                    module_source,
                    module_target,
                    python_source,
                    python_target,
                    config,
                ) = sys.argv[1:]
                expected_uid = int(expected_uid_text)
                if not user or ":" in user or "\\n" in user:
                    raise RuntimeError("invalid isolated account name")
                isolated_sources = {}
                try:
                    for label, source in (
                        ("passwd", passwd_source),
                        ("shadow", shadow_source),
                    ):
                        source_stat = os.stat(source, follow_symlinks=False)
                        if not (
                            stat.S_ISREG(source_stat.st_mode)
                            and source_stat.st_uid == expected_uid
                            and stat.S_IMODE(source_stat.st_mode) == 0o600
                            and source_stat.st_nlink == 1
                        ):
                            raise RuntimeError(f"unsafe isolated {label} source")
                        descriptor = os.open(
                            source,
                            os.O_RDWR | os.O_NOFOLLOW,
                        )
                        opened_source_stat = os.fstat(descriptor)
                        if (
                            opened_source_stat.st_dev != source_stat.st_dev
                            or opened_source_stat.st_ino != source_stat.st_ino
                        ):
                            os.close(descriptor)
                            raise RuntimeError(f"isolated {label} source changed")
                        isolated_sources[label] = descriptor
                    passwd_fd = isolated_sources["passwd"]
                    os.fchown(passwd_fd, 0, 0)
                    os.fchmod(passwd_fd, 0o644)
                    os.fsync(passwd_fd)
                    shadow_fd = isolated_sources["shadow"]
                    os.fchmod(shadow_fd, 0o600)
                    os.fchown(shadow_fd, 0, 0)
                    os.ftruncate(shadow_fd, 0)
                    shadow_record = f"{user}:x:20000:0:99999:7:::\\n".encode(
                        "ascii"
                    )
                    if os.write(shadow_fd, shadow_record) != len(shadow_record):
                        raise RuntimeError("isolated shadow write was incomplete")
                    os.fsync(shadow_fd)
                finally:
                    for descriptor in isolated_sources.values():
                        os.close(descriptor)
                subprocess.run(
                    ["/usr/bin/mount", "--make-rprivate", "/"], check=True
                )
                subprocess.run(
                    ["/usr/bin/mount", "--bind", passwd_source, "/etc/passwd"],
                    check=True,
                )
                subprocess.run(
                    [
                        "/usr/bin/mount",
                        "-o",
                        "remount,bind,ro",
                        "/etc/passwd",
                    ],
                    check=True,
                )
                subprocess.run(
                    ["/usr/bin/mount", "--bind", shadow_source, "/etc/shadow"],
                    check=True,
                )
                subprocess.run(
                    [
                        "/usr/bin/mount",
                        "-o",
                        "remount,bind,ro",
                        "/etc/shadow",
                    ],
                    check=True,
                )
                subprocess.run(
                    ["/usr/bin/mount", "--bind", module_source, module_target],
                    check=True,
                )
                subprocess.run(
                    [
                        "/usr/bin/mount",
                        "-o",
                        "remount,bind,ro",
                        module_target,
                    ],
                    check=True,
                )
                subprocess.run(
                    ["/usr/bin/mount", "--bind", python_source, python_target],
                    check=True,
                )
                subprocess.run(
                    [
                        "/usr/bin/mount",
                        "-o",
                        "remount,bind,ro",
                        python_target,
                    ],
                    check=True,
                )
                # The account home is bound LAST. The module and interpreter
                # sources can live under that home -- on an ARC pod the
                # checkout is /home/runner/_work/... -- and binding the home
                # first would hide them from the two binds above.
                subprocess.run(
                    ["/usr/bin/mount", "--bind", runtime_home, account_home],
                    check=True,
                )
                os.execv(
                    "/usr/sbin/sshd",
                    ["/usr/sbin/sshd", "-D", "-e", "-f", config],
                )
                """
            ),
            encoding="utf-8",
        )
        namespace_launcher.chmod(0o700)
        sshd_command = [
            "/usr/bin/sudo",
            "-n",
            "/usr/bin/unshare",
            "--mount",
            "--fork",
            "--kill-child=TERM",
            "/usr/bin/python3",
            str(namespace_launcher),
            user,
            str(os.getuid()),
            str(isolated_passwd),
            str(isolated_shadow),
            str(account_home),
            str(runtime_environment),
            str(server_module_path),
            str(isolated_server_module_path),
            str(python_runtime_root),
            str(isolated_python_runtime),
            str(sshd_config),
        ]

    with sshd_log.open("w", encoding="utf-8") as log_stream:
        process = subprocess.Popen(
            sshd_command,
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=log_stream,
            start_new_session=True,
        )
        try:
            _wait_for_sshd(process, port, sshd_log)
            if isolation_backend == "sudo-unshare":
                isolated_passwd_stat = isolated_passwd.stat()
                assert isolated_passwd_stat.st_uid == 0
                assert stat.S_IMODE(isolated_passwd_stat.st_mode) == 0o644
                isolated_shadow_stat = isolated_shadow.stat()
                assert isolated_shadow_stat.st_uid == 0
                assert stat.S_IMODE(isolated_shadow_stat.st_mode) == 0o600
            accepted = _run_ssh(
                port=port,
                user=user,
                private_key=private_key,
                known_hosts=known_hosts,
                command=None,
            )
            assert accepted.returncode == 0, (
                f"{accepted.stderr}\nsshd log:\n"
                f"{sshd_log.read_text(encoding='utf-8', errors='replace')}"
            )
            assert accepted.stderr == ""
            result = json.loads(accepted.stdout)
            assert result == {
                **asdict(
                    server.CoordinationSshServerIdentity(
                        credential_id=credential_id,
                        principal=PRINCIPAL,
                        session_id=SESSION_ID,
                        route=ROUTE,
                        source_address=SOURCE_ADDRESS,
                        source_port=result["source_port"],
                        target_address=TARGET_ADDRESS,
                        target_port=port,
                    )
                ),
                "auth_file_uid": os.getuid(),
                "auth_file_mode": 0o600,
                "home": str(runtime_environment),
                "runtime_directory": str(runtime_environment),
                "server_module_sha256": server_module_sha256,
                "login_shell": "/bin/sh",
                "login_home_is_runtime": True,
                "login_home_has_bashrc": False,
            }
            assert 1 <= result["source_port"] <= 65535

            refused = _run_ssh(
                port=port,
                user=user,
                private_key=private_key,
                known_hosts=known_hosts,
                command="caller-command-must-be-refused",
            )
            assert refused.returncode != 0
            assert "caller_command_refused" in refused.stderr
            assert refused.stdout == ""
        finally:
            _terminate_loopback_sshd(
                process, privileged=isolation_backend == "sudo-unshare"
            )


def test_absent_sshd_skips_outside_the_proof_job_and_fails_inside_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """KILLS: the nightly coverage-full skip leaking into the blocking proof.

    With no isolation backend named (the coverage-full sweep) an absent
    /usr/sbin/sshd is a visible skip. With a backend named (the proof job sets
    one) the same absence still fails on the binary check.
    """
    sshd = Path("/usr/sbin/sshd")
    real_is_file = Path.is_file

    def without_sshd(self: Path) -> bool:
        return False if self == sshd else real_is_file(self)

    monkeypatch.setattr(Path, "is_file", without_sshd)
    real_test = test_real_loopback_sshd_force_command_binds_identity_and_refuses_command

    monkeypatch.delenv("OKG_COORDINATION_SSH_TEST_ISOLATION", raising=False)
    with pytest.raises(pytest.skip.Exception, match="/usr/sbin/sshd is absent"):
        real_test(tmp_path)

    for backend in ("sudo-unshare", "bwrap"):
        monkeypatch.setenv("OKG_COORDINATION_SSH_TEST_ISOLATION", backend)
        with pytest.raises(
            AssertionError, match="required OpenSSH binary is absent: /usr/sbin/sshd"
        ):
            real_test(tmp_path)


def _still_running(pid: int) -> bool:
    """Whether ``pid`` names a process that has not ended.

    ``os.kill(pid, 0)`` also succeeds for a zombie: a process that has
    already ended and is only waiting for its parent to collect its exit
    status. Teardown ends the grandchild; collecting it is the job of
    whatever adopts the orphan, which is PID 1 inside a container. A pod
    whose PID 1 is the runner's own start script (an ARC heavy pod, a
    submit-host runner) never collects it, so the ended grandchild stays a
    zombie there, while a VM's systemd or the docker pod's docker-init
    collects it at once. The question this test asks is whether the process
    was ended, so a zombie counts as ended. Where there is no /proc (macOS),
    the signal probe is the only answer, and launchd collects orphans.
    """

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    stat = Path(f"/proc/{pid}/stat")
    if not Path("/proc/self/stat").is_file():
        return True
    try:
        text = stat.read_text(encoding="utf-8")
    except (FileNotFoundError, ProcessLookupError):
        # Collected between the probe and the read. Collected before the
        # open, the file is gone (ENOENT); collected after the open and
        # before the read, the read itself fails with ESRCH.
        return False
    # Field 3 of /proc/<pid>/stat, after the parenthesised command name
    # (which may itself contain spaces or parentheses).
    state = text.rsplit(")", 1)[1].split()[0]
    return state != "Z"


def test_still_running_counts_a_zombie_as_ended() -> None:
    """A child that exited but was never waited for is not running."""

    if not Path("/proc/self/stat").is_file():
        pytest.skip("no /proc: zombie state is not observable on this platform")
    process = subprocess.Popen(
        ["/bin/sh", "-c", "exit 0"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 10
        state = ""
        while time.monotonic() < deadline:
            text = Path(f"/proc/{process.pid}/stat").read_text(encoding="utf-8")
            state = text.rsplit(")", 1)[1].split()[0]
            if state == "Z":
                break
            time.sleep(0.01)
        assert state == "Z", state
        assert _still_running(process.pid) is False
    finally:
        process.wait(timeout=10)
    live = subprocess.Popen(
        ["/bin/sh", "-c", "sleep 30"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        assert _still_running(live.pid) is True
    finally:
        live.kill()
        live.wait(timeout=10)


def test_still_running_counts_a_process_collected_mid_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A process collected between opening and reading its stat has ended.

    The kernel resolves the process again on each read, so a stat file opened
    while the process lived and read after it was collected fails with ESRCH
    (ProcessLookupError), not ENOENT. Teardown collects the grandchild at its
    own pace, so the poll in the teardown test can land in that gap; this
    forces the gap with a real child instead of waiting for it to happen.
    """

    if not Path("/proc/self/stat").is_file():
        pytest.skip("no /proc: the stat read this guards does not happen here")
    live = subprocess.Popen(
        ["/bin/sh", "-c", "sleep 30"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    target = Path(f"/proc/{live.pid}/stat")
    original_read_text = Path.read_text
    collected_mid_read: list[bool] = []

    def _collect_then_read(self: Path, *args: Any, **kwargs: Any) -> str:
        if self != target:
            return original_read_text(self, *args, **kwargs)
        with self.open(encoding="utf-8") as handle:
            live.kill()
            live.wait(timeout=10)
            collected_mid_read.append(True)
            return handle.read()

    monkeypatch.setattr(Path, "read_text", _collect_then_read)
    try:
        assert _still_running(live.pid) is False
    finally:
        if live.poll() is None:
            live.kill()
            live.wait(timeout=10)
    assert collected_mid_read == [True], "the stat read never happened"


def test_loopback_sshd_teardown_reaps_a_child_that_ignores_term(
    tmp_path: Path,
) -> None:
    """Teardown must end the whole isolated group, not just the direct child.

    The real fixture's direct child ignores nothing, but its `unshare`/`sshd`
    descendants outlive a TERM that only the direct child receives. Stand in a
    cheap process group that traps TERM and holds a grandchild: if teardown
    stopped at TERM, or signalled the pid instead of the group, this leaks the
    same way the runner hosts did (issue #1595).
    """

    marker = tmp_path / "grandchild.pid"
    script = textwrap.dedent(
        f"""
        trap '' TERM
        sleep 600 &
        echo $! > {marker}
        wait
        """
    )
    process = subprocess.Popen(
        ["/bin/sh", "-c", script],
        text=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # ``echo $! > marker`` creates the file before it writes the pid, so a poll
    # that lands between the two sees an empty file: wait for the content, not
    # for the path.
    deadline = time.monotonic() + 10
    marker_text = ""
    while time.monotonic() < deadline and not marker_text:
        if marker.exists():
            marker_text = marker.read_text(encoding="utf-8").strip()
        if not marker_text:
            time.sleep(0.05)
    assert marker_text, "the stand-in never reported its grandchild"
    grandchild = int(marker_text)

    _terminate_loopback_sshd(process, privileged=False)

    assert process.poll() is not None, "the direct child was never reaped"
    gone_deadline = time.monotonic() + 10
    while time.monotonic() < gone_deadline:
        if not _still_running(grandchild):
            break
        time.sleep(0.05)
    else:  # pragma: no cover - only reached when the sweep failed
        os.kill(grandchild, signal.SIGKILL)
        raise AssertionError("teardown left the grandchild running")


def test_privileged_teardown_signals_the_group_through_sudo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sudo-unshare arm must escalate, and must address the group.

    `Popen.terminate()` on a root-owned `sudo` raises EPERM, and a signal to
    the pid alone leaves the `unshare` child behind, so the privileged path
    goes through sudo and names the negated group id. Both are pinned here
    because the real fixture only runs on a Linux runner with passwordless
    sudo.
    """

    calls: list[list[str]] = []

    def _record(argv: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", _record)
    stubborn = SimpleNamespace(
        pid=4242,
        poll=lambda: None,
        wait=lambda timeout=None: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("sudo", timeout or 0)
        ),
    )

    _terminate_loopback_sshd(stubborn, privileged=True)

    assert [call[:3] for call in calls] == [
        ["/usr/bin/sudo", "-n", "/usr/bin/python3"],
    ] * 3
    assert all(call[3] == "-c" and call[4] == _KILLPG_SOURCE for call in calls)
    assert [call[5:] for call in calls] == [
        ["4242", "TERM"],
        ["4242", "KILL"],
        ["4242", "KILL"],
    ]
    assert "os.killpg" in _KILLPG_SOURCE


# The three arms that can raise `missing`, each pinned as the exact statement
# sequence it must be. A substring contract cannot tell `if [ -x "$path" ]`
# from `if [ -x "$path" ] || [ 1 ]`, and it cannot see an `else` rewritten to
# `elif false; then`. Both keep every literal the token scan looks for while
# reporting every prerequisite present, so the arms are matched whole.
PREREQUISITE_BINARY_ARM = (
    'if [ -x "$path" ]; then',
    'echo "present: $path"',
    "else",
    'echo "MISSING prerequisite: $path" >&2',
    "missing=1",
    "fi",
)
PREREQUISITE_SUDO_ARM = (
    "if ! sudo -n true; then",
    'echo "MISSING prerequisite: passwordless sudo for $(id -un)" >&2',
    "missing=1",
    "else",
    'echo "present: passwordless sudo"',
    "fi",
)
# Shapes that leave every required literal in place and still make the step a
# no-op. `; true` and `--deselect` are also caught at run time by junit_gate,
# but the contract is what this test is for.
NEUTERING_FRAGMENTS = ("|| true", "|| :", "; true", "--deselect")


def _shell_statements(command: str) -> list[str]:
    """Return the step's executable statements.

    Backslash continuations are joined, comment-only lines and blanks are
    dropped, and each statement is stripped, so the result is what the shell
    would actually run rather than how it happens to be laid out.
    """

    joined: list[str] = []
    for line in command.splitlines():
        if joined and joined[-1].endswith("\\"):
            joined[-1] = joined[-1][:-1].rstrip() + " " + line.strip()
            continue
        joined.append(line.strip())
    return [line for line in joined if line and not line.startswith("#")]


def _contains_statements(statements: list[str], block: tuple[str, ...]) -> bool:
    """Whether ``block`` appears as a consecutive run of statements."""

    width = len(block)
    return any(
        tuple(statements[index : index + width]) == block
        for index in range(len(statements) - width + 1)
    )


def _prerequisite_paths(statements: list[str]) -> list[str]:
    """Return the absolute paths the step's prerequisite loop actually checks.

    The step used to be a bare `test -x <path>` chain. It is now a loop that
    names the missing binary, so the contract reads the loop's own path list
    rather than a fixed literal: a path dropped from the list is a prerequisite
    that is no longer proven, whatever the rest of the step says. A comment
    inside the loop header would leave every path in the text while the shell
    checked none of them, so the header must carry no comment at all.
    """

    headers = [line for line in statements if line.startswith("for path in ")]
    assert len(headers) == 1, "prerequisite loop is absent"
    header = headers[0]
    assert "#" not in header, header
    assert header.endswith("; do"), header
    listed = header[len("for path in ") : -len("; do")].split()
    assert listed, "prerequisite loop lists no paths"
    return listed


def _workflow_contract(document: dict[str, Any]) -> None:
    jobs = document["jobs"]
    # The proof is its own job on the docker pod class (PACT
    # fast-linux-off-docker): that class is the only one with the passwordless
    # sudo `unshare --mount` needs. It must stay unconditional and blocking --
    # a job-level `if` or `continue-on-error` would drop the proof as surely
    # as deleting the step -- and it must stay in the merge gate's needs.
    job = jobs[PROOF_JOB]
    assert job.get("runs-on") == PROOF_RUNNER, job.get("runs-on")
    assert "if" not in job
    assert "continue-on-error" not in job
    assert "strategy" not in job
    assert PROOF_JOB in jobs["test-results-gate"]["needs"]
    # Nothing else may carry the proof; a second copy on another runner class
    # would be the one a later edit quietly keeps.
    carriers = sorted(
        job_id
        for job_id, candidate in jobs.items()
        if isinstance(candidate, dict)
        and any(
            isinstance(step, dict)
            and step.get("name") == "Prove coordination SSH server admission"
            for step in candidate.get("steps") or ()
        )
    )
    assert carriers == [PROOF_JOB], carriers
    steps = job["steps"]
    matches = [
        step
        for step in steps
        if step.get("name") == "Prove coordination SSH server admission"
    ]
    assert len(matches) == 1
    step = matches[0]
    index = steps.index(step)
    assert steps[index - 1].get("name") == "Prepare artifact directories"
    assert "if" not in step
    assert "continue-on-error" not in step
    assert step.get("env") == {"OKG_COORDINATION_SSH_TEST_ISOLATION": "sudo-unshare"}
    command = step.get("run", "")
    statements = _shell_statements(command)
    # The prerequisite loop must still prove every binary the isolation backend
    # needs, still probe passwordless sudo, still require the sshd privilege
    # separation directory, and still fail closed on any gap.
    listed_paths = _prerequisite_paths(statements)
    for binary in REQUIRED_PREREQUISITE_BINARIES:
        assert binary in listed_paths, binary
    for arm in (PREREQUISITE_BINARY_ARM, PREREQUISITE_SUDO_ARM):
        assert _contains_statements(statements, arm), arm[0]
    assert command.count("missing=1") == len(FAIL_CLOSED_ARMS), "missing=1"
    # errexit is what makes the failing statements above stop the step, and the
    # first statement is the only place it may be set.
    assert statements[0] == "set -euo pipefail", statements[0]
    for statement in statements[1:]:
        assert not statement.startswith("set "), statement
        for fragment in NEUTERING_FRAGMENTS:
            assert fragment not in statement, statement
        assert statement != "exit 0", statement
    # The gate must actually stop the step: nothing may run between it and the
    # proof it guards.
    assert 'test "$missing" -eq 0' in statements, "prerequisite gate"
    gate = statements.index('test "$missing" -eq 0')
    assert gate + 1 < len(statements), "the gate guards nothing"
    assert statements[gate + 1].startswith("rm -f "), statements[gate + 1]
    required_tokens = (
        "set -euo pipefail",
        "missing=0",
        '[ -x "$path" ]',
        "sudo -n true",
        'test "$missing" -eq 0',
        f"rm -f {REPORT}",
        f"python -m pytest {TEST_MODULE}",
        "-o addopts=''",
        f"--junitxml={REPORT}",
        f"python .github/scripts/assert-no-skips.py {REPORT}",
        f"python scripts/ci/junit_gate.py {REPORT}",
        "--step-outcome success",
        "--lane coordination-ssh-server",
    ) + tuple(f"--require-nodeid {nodeid}" for nodeid in REQUIRED_NODEIDS)
    for token in required_tokens:
        assert token in command, token
    uploads = [
        candidate
        for candidate in steps[index + 1 :]
        if candidate.get("name") == "Upload coordination-ssh-proof artifacts"
    ]
    assert len(uploads) == 1
    assert ".scratch/junit/*.xml" in uploads[0]["with"]["path"]


def test_blocking_ci_step_resists_omission_and_weakening() -> None:
    workflow_path = Path(".github/workflows/test.yml")
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))
    _workflow_contract(workflow)

    steps = workflow["jobs"][PROOF_JOB]["steps"]
    step_index = next(
        index
        for index, step in enumerate(steps)
        if step.get("name") == "Prove coordination SSH server admission"
    )
    mutations = []

    omitted = copy.deepcopy(workflow)
    del omitted["jobs"][PROOF_JOB]["steps"][step_index]
    mutations.append(omitted)

    conditional = copy.deepcopy(workflow)
    conditional["jobs"][PROOF_JOB]["steps"][step_index]["if"] = "false"
    mutations.append(conditional)

    softened = copy.deepcopy(workflow)
    softened["jobs"][PROOF_JOB]["steps"][step_index]["continue-on-error"] = True
    mutations.append(softened)

    # The job-level escapes the split into its own job opened: a job guard, a
    # soft job, a runner class without the sudo the proof needs, a job the
    # merge gate no longer waits for, and a second copy on another job.
    guarded_job = copy.deepcopy(workflow)
    guarded_job["jobs"][PROOF_JOB]["if"] = "false"
    mutations.append(guarded_job)

    soft_job = copy.deepcopy(workflow)
    soft_job["jobs"][PROOF_JOB]["continue-on-error"] = True
    mutations.append(soft_job)

    for runner in ("okg-arc-heavy", "okg-arc-light", ["okg-arc-docker"]):
        moved = copy.deepcopy(workflow)
        moved["jobs"][PROOF_JOB]["runs-on"] = runner
        mutations.append(moved)

    ungated = copy.deepcopy(workflow)
    ungated["jobs"]["test-results-gate"]["needs"].remove(PROOF_JOB)
    mutations.append(ungated)

    duplicated = copy.deepcopy(workflow)
    duplicated["jobs"]["fast-linux"]["steps"].append(
        copy.deepcopy(workflow["jobs"][PROOF_JOB]["steps"][step_index])
    )
    mutations.append(duplicated)

    alternate_isolation = copy.deepcopy(workflow)
    alternate_isolation["jobs"][PROOF_JOB]["steps"][step_index]["env"][
        "OKG_COORDINATION_SSH_TEST_ISOLATION"
    ] = "bwrap"
    mutations.append(alternate_isolation)

    for token in (
        "-o addopts=''",
        "assert-no-skips.py",
        "--require-nodeid",
        "rm -f",
        "junit_gate.py",
        "sudo -n true",
        'test "$missing" -eq 0',
        '[ -x "$path" ]',
        "missing=0",
        "missing=1",
        *REQUIRED_PREREQUISITE_BINARIES,
    ):
        weakened = copy.deepcopy(workflow)
        command = weakened["jobs"][PROOF_JOB]["steps"][step_index]["run"]
        weakened["jobs"][PROOF_JOB]["steps"][step_index]["run"] = command.replace(
            token, "OMITTED", 1
        )
        mutations.append(weakened)

    # Present-but-neutered guards. Each of these keeps every literal the token
    # scan looks for and still turns the step into a no-op.
    for original, neutered in (
        ('test "$missing" -eq 0', 'test "$missing" -eq 0 || true'),
        ("if ! sudo -n true; then", "if ! { sudo -n true || true; }; then"),
        ("if ! sudo -n true; then", "if ! sudo -n true && false; then"),
        ("set -euo pipefail", "set -euo pipefail\nexit 0"),
        ("set -euo pipefail", "set -euo pipefail\nset +e"),
        ("set -euo pipefail", "set -euo pipefail\nset +o errexit"),
        # A comment in the loop header leaves every path in the text while the
        # shell checks only the first one.
        ("for path in /usr/sbin/sshd", "for path in /bin/sh # /usr/sbin/sshd"),
        # An unconditionally true test reports every binary present.
        ('if [ -x "$path" ]; then', 'if [ -x "$path" ] || [ 1 ]; then'),
        # A dead else arm never raises `missing`.
        (
            'else\n    echo "MISSING prerequisite: $path" >&2',
            'elif false; then\n    echo "MISSING prerequisite: $path" >&2',
        ),
        # A pytest run that cannot fail, and one that skips the real proof.
        ("--junitxml=", "; true --junitxml="),
        (
            "test_coordination_ssh_server.py -q",
            "test_coordination_ssh_server.py --deselect x -q",
        ),
        (
            'test "$missing" -eq 0\n',
            'test "$missing" -eq 0\necho "gate is not the last word"\n',
        ),
    ):
        neutralized = copy.deepcopy(workflow)
        command = neutralized["jobs"][PROOF_JOB]["steps"][step_index]["run"]
        replaced = command.replace(original, neutered, 1)
        assert replaced != command, original
        neutralized["jobs"][PROOF_JOB]["steps"][step_index]["run"] = replaced
        mutations.append(neutralized)

    for mutation in mutations:
        with pytest.raises(AssertionError):
            _workflow_contract(mutation)
