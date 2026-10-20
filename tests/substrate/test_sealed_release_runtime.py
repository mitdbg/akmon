from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sys
from typing import Any, Mapping, cast

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from okg.substrate import _sealed_release_runtime as runtime
from okg.substrate import sealed_release as sealed_release_impl
from okg.substrate._sealed_release_integrity import (
    executable_tree_digest,
    is_bytecode_relative_name,
    seal_permissions,
)
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.sealed_release import SealedRelease, SealedReleaseInputs


def _sha(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _canonical_digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(runtime._canonical_json(value)).hexdigest()


def _public_bytes(key: Ed25519PrivateKey) -> bytes:
    return key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _locked_wheelhouse(
    root: Path,
    repo: Path,
    *,
    dependency_source: str = (
        'source = { registry = "https://packages.invalid/simple" }'
    ),
) -> dict[str, str]:
    wheel_name = "fixture_dependency-1.0.0-py3-none-any.whl"
    wheel_payload = b"fixture locked wheel bytes"
    wheel_ref = runtime._raw_blob(wheel_payload)
    (repo / "uv.lock").write_text(
        "\n".join(
            [
                "version = 1",
                "",
                "[[package]]",
                'name = "fixture-project"',
                'version = "1.0.0"',
                'source = { editable = "." }',
                "",
                "[[package]]",
                'name = "fixture-dependency"',
                'version = "1.0.0"',
                dependency_source,
                (
                    "wheels = [{ url = "
                    f'"https://packages.invalid/{wheel_name}", '
                    f'hash = "{wheel_ref["digest"]}", size = {len(wheel_payload)} }}]'
                ),
                "",
            ]
        ),
        encoding="utf-8",
    )
    root.mkdir()
    (root / wheel_name).write_bytes(wheel_payload)
    manifest = {
        "schema_version": runtime.RUNTIME_WHEELHOUSE_MANIFEST_SCHEMA,
        "dependency_lock_raw_blob": runtime._raw_blob((repo / "uv.lock").read_bytes()),
        "wheels": [{"relative_name": wheel_name, "raw_blob": wheel_ref}],
    }
    (root / "manifest.json").write_bytes(runtime._canonical_json(manifest))
    for path in root.iterdir():
        path.chmod(0o444)
    root.chmod(0o555)
    return {
        runtime.RUNTIME_WHEELHOUSE_ROOT_ENV: str(root),
        runtime.RUNTIME_WHEELHOUSE_MANIFEST_DIGEST_ENV: _canonical_digest(manifest),
    }


def _producer_proof(
    root: Path,
    *,
    reuse_executor: bool = False,
    rewritten_archive_manifest: bool = False,
    absolute_loader_search: bool = False,
    prefix_source: Path | None = None,
    platform_key: tuple[str, str, str, str] = ("linux", "x86_64", "glibc", "cp312"),
) -> tuple[dict[str, object], runtime.VerifiedRuntimeDistribution]:
    """Sign a fixture producer proof over fixture bytes or a real prefix.

    ``prefix_source`` is an extracted runtime prefix (for example a
    python-build-standalone ``install_only`` tree). Its bytes become the proved
    complete prefix; the signatures remain fixture keys, not a producer's.
    """

    profile = dict(runtime._RUNTIME_PLATFORM_PROFILES[platform_key])
    prefix = root / "python" / "install"
    payloads = {
        "bin/python3.12": b"fixture enrolled interpreter",
        "lib/libtcl9.0.so": b"fixture tcl",
        "lib/libtk9.0.so": b"fixture tk",
        "lib/libpython3.12.a": b"fixture static python archive",
        "lib/python3.12/lib-dynload/_tkinter.cpython-312.so": b"fixture tkinter",
    }
    if prefix_source is not None:
        prefix.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(prefix_source, prefix, symlinks=True)
        payloads = {}
    for name, payload in payloads.items():
        path = prefix / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        if name == "bin/python3.12":
            path.chmod(0o755)
    archive_payload = b"fixture normalized complete archive"
    (root / "runtime-distribution.tar.zst").write_bytes(archive_payload)
    archive = runtime._raw_blob(archive_payload)
    distribution_tree = {
        "schema_version": runtime.RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA,
        "entries": runtime._runtime_prefix_members(prefix),
    }
    distribution_tree_digest = _canonical_digest(distribution_tree)
    member_by_name = {
        str(item["relative_name"]): item for item in distribution_tree["entries"]
    }
    native_names = sorted(
        name
        for name, member in member_by_name.items()
        if member["kind"] == "file"
        and (member["mode"] == 0o555 or name.endswith((".a", ".so")) or ".so." in name)
    )
    native_manifest = {
        "schema_version": runtime.RUNTIME_NATIVE_OUTPUT_MANIFEST_SCHEMA,
        "entries": [
            {"relative_name": name, "raw_blob": member_by_name[name]["raw_blob"]}
            for name in native_names
        ],
    }
    archive_manifest = copy.deepcopy(native_manifest)
    if rewritten_archive_manifest:
        archive_manifest["entries"][0]["raw_blob"] = runtime._raw_blob(b"changed")
    transcript: dict[str, object] = {
        "schema_version": runtime.RUNTIME_ORIGINAL_LINK_TRANSCRIPT_SCHEMA,
        "entries": [
            {
                "relative_name": "lib/libtcl9.0.so",
                "literal_search": "/tools/deps/lib"
                if absolute_loader_search
                else "$ORIGIN:$ORIGIN/..",
            },
            {
                "relative_name": "lib/libtk9.0.so",
                "literal_search": "$ORIGIN:$ORIGIN/..",
            },
            {
                "relative_name": ("lib/python3.12/lib-dynload/_tkinter.cpython-312.so"),
                "literal_search": "$ORIGIN/../..",
            },
        ],
    }
    if prefix_source is not None:
        transcript["entries"] = [
            {"relative_name": name, "literal_search": search}
            for name, search in sorted(
                (
                    name,
                    "$ORIGIN/../.."
                    if PurePosixPath(name).name.startswith("_tkinter")
                    else "$ORIGIN:$ORIGIN/..",
                )
                for name in member_by_name
                if PurePosixPath(name).name.startswith(("_tkinter", "libtcl", "libtk"))
            )
        ]
    process_trace = {
        "schema_version": runtime.RUNTIME_PROCESS_TRACE_SCHEMA,
        "commands": [
            {
                "executable": "pinned-tools/uv",
                "argv": [
                    "--no-config",
                    "--offline",
                    "--no-python-downloads",
                    "run",
                    "--frozen",
                    "--no-dev",
                    "--python",
                    "pinned-tools/driver-python/bin/python3.12",
                    "build.py",
                    "--serial",
                    "--python",
                    "cpython-3.12",
                    "--options",
                    "noopt",
                    "--target-triple",
                    profile["target_triple"],
                ],
            }
        ],
        "environment": dict(runtime._RUNTIME_BUILD_ENVIRONMENT),
        "network": "disabled",
    }
    process_trace_digest = _canonical_digest(process_trace)
    source_manifest = {
        "schema_version": runtime.RUNTIME_SOURCE_MANIFEST_SCHEMA,
        "upstream": {
            "repository": "https://github.com/astral-sh/python-build-standalone",
            "release_tag": "20260814",
            "commit": "38d35dcf0e212ca02eed8ebc11d0c92906387d56",
            "git_tree": "9d623a68052ad503c8d72085fb1823c206cf329a",
            "source_archive_raw_blob": (
                "sha256:66c2ba43e778ee55b3a2f4c925525e3c4352b13dbd77b3bc479478f7a7dcd7c3"
            ),
        },
        "cpython": {
            "version": "3.12.14",
            "source_raw_blob": (
                "sha256:5c8462af5790baf43a321a1559dbe0db06d1be4300fb85fb53c40060668e548a"
            ),
        },
        "driver_tools": [
            {
                "logical_name": name,
                "executable_raw_blob": digest,
            }
            for name, digest in sorted(
                {
                    "uv": "sha256:b65f23a420c4acc96427efb30e5ed9bc0f7e25d2d712000f6ede77c1a0de5f46",
                    "driver_python": "sha256:9b1522a474140400cc6f64d475ad49c045f8034696e4df09c3f9792dd41bdf2c",
                    "gnu_make": "sha256:0a5e3a88ba6c2b6a8dbb785a7bb3e3b4ee292992d41bdbf905211bb5e7831f3c",
                    "docker_buildx": "sha256:48af8a397ebd60178778bf63611dbcebe5f5e7a9be90eb9147b24b9587455778",
                }.items()
            )
        ],
        "builder_base_manifest_digest": (
            "sha256:32ad5050caffb2c7e969dac873bce2c370015c2256ff984b70c1c08b3a2816a0"
        ),
        "builder_snapshot_release_roots": sorted(
            [
                "sha256:ff4cbf89abb58afa73b08f1b21ebaeed9a513e5d848c8c146fdddfa6ca39429a",
                "sha256:63bc2e5739f84e2a4a967cc76d5e04ff4a3020c25934fa21d81f8202f381edf7",
                "sha256:cc346ba2752ef276d8f60266ade45a19075e16fb54e3c4ceb9e280b3077675d6",
            ]
        ),
        "dependency_lock_raw_blob": (
            "sha256:4ca49fe030b09ad2a70fef0149a45c8d052e5d79043fbb9b64cf6979bacf248b"
        ),
        "downloads_manifest_raw_blob": (
            "sha256:b739831dc701afa9b3f2de5f39c7ed82dd79859c3ff2c04c521d71ca279bf97a"
        ),
        "sorted_acquired_inputs": [
            {
                "logical_name": "cpython-source",
                "source_url_digest": _sha("https cpython source url"),
                "size": 20820300,
                "raw_blob": (
                    "sha256:5c8462af5790baf43a321a1559dbe0db06d1be4300fb85fb53c40060668e548a"
                ),
                "parent_index_raw_blob": None,
            }
        ],
        "package_chain_status": "every-index-rooted-and-every-package-indexed",
    }
    source_manifest_digest = _canonical_digest(source_manifest)
    profile_digest = _canonical_digest(profile)
    producer_key = Ed25519PrivateKey.generate()
    builder_keys = [Ed25519PrivateKey.generate(), Ed25519PrivateKey.generate()]
    builders = []
    for index, key in enumerate(builder_keys):
        builders.append(
            {
                "builder_role_id": runtime._RUNTIME_BUILD_ROLES[index],
                "authenticated_executor_identity_digest": _sha(
                    "executor-0" if reuse_executor else f"executor-{index}"
                ),
                "ephemeral_attestor_public_key_digest": runtime._raw_blob(
                    _public_bytes(key)
                )["digest"],
                "clean_builder_root_creation_receipt_digest": _sha(f"root-{index}"),
                "acquisition_worker_identity_digest": _sha(f"worker-{index}"),
            }
        )
    session = {
        "schema_version": runtime.RUNTIME_REPRODUCIBILITY_SESSION_SCHEMA,
        "release_producer_attestor_identity_digest": runtime._raw_blob(
            _public_bytes(producer_key)
        )["digest"],
        "source_manifest_digest": source_manifest_digest,
        "build_recipe_digest": runtime.RUNTIME_BUILD_RECIPE_DIGEST,
        "platform_profile_digest": profile_digest,
        "builders": builders,
        "status": "two-pairwise-distinct-build-authorities-enrolled",
    }
    session_digest = _canonical_digest(session)
    session_signature = producer_key.sign(runtime._canonical_json(session))
    attestations = []
    for index, key in enumerate(builder_keys):
        record = {
            "schema_version": runtime.RUNTIME_BUILD_ATTESTATION_SCHEMA,
            "reproducibility_session_digest": session_digest,
            "source_manifest_digest": source_manifest_digest,
            "build_recipe_digest": runtime.RUNTIME_BUILD_RECIPE_DIGEST,
            "platform_profile_digest": profile_digest,
            **builders[index],
            "builder_image_manifest_digest": _sha("builder-image"),
            "builder_layer_digests": [_sha("builder-layer")],
            "docker_executor_attestation_digest": _sha(f"docker-{index}"),
            "acquisition_receipt_digest": _sha(f"acquisition-{index}"),
            "clean_builder_nonce_raw_blob": _sha(f"nonce-{index}"),
            "normalized_archive": archive,
            "distribution_prefix_tree_digest": distribution_tree_digest,
            "original_link_search_transcript_digest": _canonical_digest(transcript),
            "native_link_output_manifest_digest": _canonical_digest(native_manifest),
            "native_archive_output_manifest_digest": _canonical_digest(native_manifest),
            "ordered_process_execution_trace_digest": process_trace_digest,
        }
        signature = key.sign(runtime._canonical_json(record))
        record["attestation_signature_raw_blob"] = runtime._raw_blob(signature)
        attestations.append(
            {
                "record": record,
                "authentication": {
                    "public_key_base64": base64.b64encode(_public_bytes(key)).decode(),
                    "signature_base64": base64.b64encode(signature).decode(),
                },
            }
        )
    attestation_digests = sorted(
        _canonical_digest(value["record"]) for value in attestations
    )
    reproducibility_proof = {
        "schema_version": runtime.RUNTIME_REPRODUCIBILITY_PROOF_SCHEMA,
        "reproducibility_session_digest": session_digest,
        "source_manifest_digest": source_manifest_digest,
        "build_recipe_digest": runtime.RUNTIME_BUILD_RECIPE_DIGEST,
        "platform_profile_digest": profile_digest,
        "build_attestation_digests": attestation_digests,
        "pairwise_distinct_fields": list(runtime._RUNTIME_DISTINCT_BUILD_FIELDS),
        "normalized_archive": archive,
        "distribution_prefix_tree_digest": distribution_tree_digest,
        "original_link_search_transcript_digest": _canonical_digest(transcript),
        "native_output_manifest_digest": _canonical_digest(native_manifest),
        "status": "two-authenticated-independent-builds-identical",
    }
    acquisition = {
        "schema_version": runtime.RUNTIME_DISTRIBUTION_ACQUISITION_SCHEMA,
        "distribution_enrollment_digest": _canonical_digest(
            runtime.RUNTIME_DISTRIBUTION_ENROLLMENT
        ),
        "build_recipe_digest": runtime.RUNTIME_BUILD_RECIPE_DIGEST,
        "reproducibility_proof_digest": _canonical_digest(reproducibility_proof),
        "platform_profile_digest": profile_digest,
        "normalized_archive": archive,
        "distribution_prefix_tree_digest": distribution_tree_digest,
        "ordered_acquisition_transcript_digest": _sha("acquisition-transcript"),
    }
    document = {
        "schema_version": runtime.RUNTIME_DISTRIBUTION_BUNDLE_SCHEMA,
        "distribution_enrollment": copy.deepcopy(
            runtime.RUNTIME_DISTRIBUTION_ENROLLMENT
        ),
        "source_manifest": source_manifest,
        "build_recipe_digest": runtime.RUNTIME_BUILD_RECIPE_DIGEST,
        "platform_profile": profile,
        "reproducibility_session": session,
        "session_authentication": {
            "public_key_base64": base64.b64encode(_public_bytes(producer_key)).decode(),
            "signature_base64": base64.b64encode(session_signature).decode(),
        },
        "build_attestations": attestations,
        "reproducibility_proof": reproducibility_proof,
        "distribution_acquisition_receipt": acquisition,
        "normalized_archive": archive,
        "distribution_prefix_tree": distribution_tree,
        "original_link_search_transcript": transcript,
        "native_link_output_manifest": native_manifest,
        "native_archive_output_manifest": archive_manifest,
        "process_traces": [process_trace],
    }
    proof_digest = _canonical_digest(document)
    (root / "proof.json").write_bytes(runtime._canonical_json(document))
    for path in sorted(
        root.rglob("*"), key=lambda value: len(value.parts), reverse=True
    ):
        if path.is_symlink():
            continue
        if path.is_dir():
            path.chmod(0o555)
        else:
            path.chmod(0o555 if path.stat().st_mode & 0o111 else 0o444)
    root.chmod(0o555)
    verified = runtime._verify_authorized_runtime_distribution(
        document,
        root,
        expected_profile=profile,
        proof_digest=proof_digest,
    )
    return document, verified


def _sealed_release_fixture(
    tmp_path: Path,
    monkeypatch,
    *,
    changed_prefix_copy_proof: bool = False,
) -> tuple[SealedRelease, dict[str, object]]:
    draft = tmp_path / "draft"
    repo = draft / "repo"
    venv = draft / "venv"
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    repo.mkdir(parents=True)
    distribution.copy_prefix_to(venv)
    (venv / "bin" / "python3.12").chmod(0o755)
    verifier = (
        venv
        / "lib"
        / "python3.12"
        / "site-packages"
        / f"{runtime.RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
    )
    verifier.parent.mkdir(parents=True)
    verifier.write_bytes(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)
    runtime_root_fd = os.open(venv, os.O_RDONLY | os.O_DIRECTORY)
    repo_root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    try:
        document = dict(
            runtime.write_runtime_construction_receipt(
                repo_root_fd=repo_root_fd,
                runtime_root_fd=runtime_root_fd,
                python_request=b"3.12",
                interpreter_relative_name="bin/python3.12",
                python_description={
                    "major": 3,
                    "minor": 12,
                    "python_abi": "cp312",
                    "builtin_modules": ["_abc"],
                    "frozen_modules": ["_frozen_importlib"],
                    "os": "linux",
                    "architecture": "x86_64",
                    "libc": "glibc",
                },
                distribution=distribution,
                dependency_wheelhouse_manifest_digest=_sha("wheelhouse"),
            )
        )
    finally:
        os.close(runtime_root_fd)
        os.close(repo_root_fd)
    if changed_prefix_copy_proof:
        receipt = dict(cast(Mapping[str, object], document["receipt"]))
        receipt["prefix_copy_proof_digest"] = _sha("changed prefix copy proof")
        document["receipt"] = receipt
        document["receipt_digest"] = runtime._canonical_ref(
            runtime.RUNTIME_CONSTRUCTION_SCHEMA, receipt
        )
        (repo / runtime.RUNTIME_CONSTRUCTION_FILENAME).write_bytes(
            runtime._canonical_json(document)
        )

    seal_permissions(repo)
    seal_permissions(venv)
    tree_digest = executable_tree_digest(draft)
    inputs = SealedReleaseInputs(
        code_revision="7" * 40,
        dependency_lock_digest=_sha("lock"),
        schema_checksum=_sha("schema"),
        catalog_fingerprint=_sha("catalog"),
        definition_digest=_sha("definition"),
        skill_bundle_digest=None,
    )
    artifact_id = sealed_release_impl._release_artifact_id(inputs, tree_digest)
    preparing_release = SealedRelease(
        artifact_id,
        draft,
        inputs,
        tree_digest,
        "2026-08-23T00:00:00+00:00",
    )
    sealed_release_impl._write_release_metadata(preparing_release)
    final_root = tmp_path / artifact_id.removeprefix("sha256:")
    draft.rename(final_root)
    seal_permissions(final_root)
    release = SealedRelease(
        artifact_id,
        final_root,
        inputs,
        tree_digest,
        preparing_release.prepared_at,
    )
    monkeypatch.setattr(
        sealed_release_impl,
        "_descriptor_startup_proof",
        lambda *_args, **_kwargs: {
            "schema_version": "okg.sealed_release.runtime_descriptor_startup_proof/v2",
            "status": "descriptor-exec-verified",
        },
    )
    monkeypatch.setattr(
        sealed_release_impl,
        "_runtime_tk_native_members",
        lambda *_args, **_kwargs: (
            "lib/libtcl9.0.so",
            "lib/libtk9.0.so",
            "lib/python3.12/lib-dynload/_tkinter.cpython-312.so",
        ),
    )
    return release, document


def test_dependency_sync_requires_exact_authorized_reproducible_distribution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")
    wheelhouse_env = _locked_wheelhouse(tmp_path / "wheelhouse", repo)
    commands: list[tuple[list[str], dict[str, object]]] = []

    def record_command(command, **kwargs):
        commands.append((command, kwargs))
        if command[0] == "enrolled-python":
            return json.dumps(
                {
                    "major": 3,
                    "minor": 12,
                    "python_abi": "cp312",
                    "builtin_modules": ["_abc"],
                    "frozen_modules": ["_frozen_importlib"],
                    "os": "linux",
                    "architecture": "x86_64",
                    "libc": "glibc",
                }
            )
        return ""

    monkeypatch.setattr(runtime, "_run_release_command", record_command)

    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(repo, tmp_path / "venv", {})

    assert refusal.value.data == {"code": "sealed_release_runtime_authority_missing"}
    assert commands == []

    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    with pytest.raises(SealedReleaseInvalid) as missing_installer:
        runtime.sync_dependencies(repo, tmp_path / "venv-without-uv", wheelhouse_env)
    assert missing_installer.value.data == {
        "code": "sealed_release_runtime_authority_missing"
    }
    assert commands == []

    _document, distribution = _producer_proof(tmp_path / "second-producer-proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    monkeypatch.setattr(
        runtime,
        "_open_enrolled_release_driver",
        lambda *_args, **_kwargs: (os.open(__file__, os.O_RDONLY), _sha("uv")),
    )
    runtime.sync_dependencies(
        repo,
        tmp_path / "venv-with-uv",
        {
            **wheelhouse_env,
            runtime.RUNTIME_UV_EXECUTABLE_ENV: "/coordinator/enrolled-uv",
            "PATH": "/untrusted/bin",
            "PIP_INDEX_URL": "https://untrusted.invalid/simple",
            "UV_CACHE_DIR": "/untrusted/cache",
        },
    )
    command, command_kwargs = commands[0]
    assert command[:5] == [
        "enrolled-uv",
        "sync",
        "--offline",
        "--no-cache",
        "--no-config",
    ]
    command_environment = cast(dict[str, str], command_kwargs["environ"])
    project_environment = command_environment["UV_PROJECT_ENVIRONMENT"]
    assert isinstance(project_environment, str)
    assert project_environment.startswith("/proc/self/fd/")
    assert command_environment == {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PYTHONDONTWRITEBYTECODE": "1",
        "UV_LINK_MODE": "copy",
        "UV_PROJECT_ENVIRONMENT": project_environment,
    }
    assert command_kwargs["executable_fd"] is not None
    assert command_kwargs["expected_executable_digest"] == _sha("uv")
    assert command_kwargs["require_live_executable_verification"] is True
    retained_fds = cast(tuple[int, ...], command_kwargs["retained_fds"])
    assert retained_fds[0] == int(project_environment.removeprefix("/proc/self/fd/"))
    assert command_kwargs["write_root_fds"] == (retained_fds[0],)
    assert command[5:8] == [
        "--no-index",
        "--find-links",
        f"/proc/self/fd/{retained_fds[1]}",
    ]
    assert command[-1].startswith("/proc/self/fd/")
    assert int(command[-1].removeprefix("/proc/self/fd/")) in retained_fds
    assert "--locked" in command
    assert "--frozen" not in command
    interpreter_command, interpreter_kwargs = commands[1]
    assert interpreter_command[0] == "enrolled-python"
    assert interpreter_kwargs["executable_fd"] is not None
    assert interpreter_kwargs["require_live_executable_verification"] is True


@pytest.mark.skipif(sys.platform != "linux", reason="Landlock is a Linux API")
def test_landlock_write_confinement_refuses_descendant_symlink_escape(
    tmp_path: Path,
) -> None:
    admitted = tmp_path / "admitted"
    outside = tmp_path / "outside"
    admitted.mkdir()
    outside.mkdir()
    (admitted / "escape").symlink_to(outside, target_is_directory=True)
    admitted_fd = os.open(admitted, os.O_RDONLY | os.O_DIRECTORY)
    pid = os.fork()
    if pid == 0:  # pragma: no cover - asserted through the parent status
        try:
            runtime._install_landlock_write_confinement((admitted_fd,))
            local_fd = os.open(
                "local",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=admitted_fd,
            )
            os.close(local_fd)
            try:
                escaped_fd = os.open(
                    admitted / "escape" / "escaped",
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except PermissionError:
                os._exit(0)
            else:
                os.close(escaped_fd)
                os._exit(2)
        except BaseException:
            os._exit(3)
    os.close(admitted_fd)
    waited, status = os.waitpid(pid, 0)
    assert waited == pid
    assert os.WIFEXITED(status)
    assert os.WEXITSTATUS(status) == 0
    assert (admitted / "local").read_bytes() == b""
    assert not (outside / "escaped").exists()


def test_dependency_sync_refuses_local_path_authority_in_exact_lock(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")
    wheelhouse_env = _locked_wheelhouse(
        tmp_path / "wheelhouse",
        repo,
        dependency_source='source = { directory = "../outside" }',
    )
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(repo, tmp_path / "venv", wheelhouse_env)

    assert refusal.value.data == {"code": "sealed_release_runtime_acquisition_mismatch"}


def test_enrolled_dependency_driver_requires_exact_readonly_bytes(
    tmp_path: Path,
) -> None:
    driver_payload = b"\x7fELF fixture enrolled driver"
    driver_digest = str(runtime._raw_blob(driver_payload)["digest"])
    distribution = runtime.VerifiedRuntimeDistribution(
        proof_document={
            "source_manifest": {
                "driver_tools": [
                    {"logical_name": "uv", "executable_raw_blob": driver_digest}
                ]
            }
        },
        proof_digest=_sha("proof"),
        distribution_members=(),
        platform_profile={},
        source_manifest_digest=_sha("source"),
        reproducibility_proof_digest=_sha("reproducibility"),
        distribution_acquisition_receipt_digest=_sha("acquisition"),
        distribution_archive={},
        _prefix_fd=-1,
    )
    driver = tmp_path / "enrolled-uv"
    driver.write_bytes(driver_payload)
    driver.chmod(0o555)

    driver_fd, digest = runtime._open_enrolled_release_driver(
        str(driver), distribution, logical_name="uv"
    )
    try:
        assert digest == driver_digest
        assert os.pread(driver_fd, len(driver_payload), 0) == driver_payload
    finally:
        os.close(driver_fd)

    driver.chmod(0o755)
    with pytest.raises(SealedReleaseInvalid) as writable:
        runtime._open_enrolled_release_driver(
            str(driver), distribution, logical_name="uv"
        )
    assert writable.value.data == {"code": "sealed_release_runtime_recipe_mismatch"}

    driver.write_bytes(b"\x7fELF changed enrolled driver")
    driver.chmod(0o555)
    with pytest.raises(SealedReleaseInvalid) as changed:
        runtime._open_enrolled_release_driver(
            str(driver), distribution, logical_name="uv"
        )
    assert changed.value.data == {"code": "sealed_release_runtime_recipe_mismatch"}
    distribution.close()


def test_release_command_rechecks_enrolled_executable_after_child(
    tmp_path: Path, monkeypatch
) -> None:
    payload = b"\x7fELF fixture executable"
    executable = tmp_path / "enrolled-tool"
    executable.write_bytes(payload)
    executable.chmod(0o555)
    executable_fd = os.open(executable, os.O_RDONLY)

    class MutatingProcess:
        returncode = 0

        def poll(self):
            return None

        def communicate(self):
            executable.chmod(0o755)
            executable.write_bytes(b"changed executable")
            return "", ""

    monkeypatch.setattr(
        runtime.subprocess, "Popen", lambda *_args, **_kwargs: MutatingProcess()
    )
    try:
        with pytest.raises(SealedReleaseInvalid) as refusal:
            runtime._run_release_command(
                ["enrolled-tool"],
                cwd=tmp_path,
                environ={},
                executable_fd=executable_fd,
                expected_executable_digest=str(runtime._raw_blob(payload)["digest"]),
                require_live_executable_verification=True,
            )
    finally:
        os.close(executable_fd)

    assert refusal.value.data == {"code": "sealed_release_runtime_recipe_mismatch"}


def test_release_command_kills_child_when_live_executable_check_refuses(
    tmp_path: Path, monkeypatch
) -> None:
    payload = b"\x7fELF fixture executable"
    executable = tmp_path / "enrolled-tool"
    executable.write_bytes(payload)
    executable.chmod(0o555)
    executable_fd = os.open(executable, os.O_RDONLY)

    class RunningProcess:
        returncode = -9
        killed = False
        communicated = False

        def poll(self):
            return self.returncode if self.killed else None

        def kill(self):
            self.killed = True

        def communicate(self):
            self.communicated = True
            return "", ""

    process = RunningProcess()

    def spawn(*_args, **_kwargs):
        executable.chmod(0o755)
        executable.write_bytes(b"changed while child is alive")
        return process

    monkeypatch.setattr(runtime.subprocess, "Popen", spawn)
    try:
        with pytest.raises(SealedReleaseInvalid) as refusal:
            runtime._run_release_command(
                ["enrolled-tool"],
                cwd=tmp_path,
                environ={},
                executable_fd=executable_fd,
                expected_executable_digest=str(runtime._raw_blob(payload)["digest"]),
                require_live_executable_verification=True,
            )
    finally:
        os.close(executable_fd)

    assert refusal.value.data == {"code": "sealed_release_runtime_recipe_mismatch"}
    assert process.killed is True
    assert process.communicated is True


def test_dependency_sync_retains_destination_through_final_receipt(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")
    wheelhouse_env = _locked_wheelhouse(tmp_path / "wheelhouse", repo)
    _document, distribution = _producer_proof(tmp_path / "proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    monkeypatch.setattr(
        runtime,
        "_open_enrolled_release_driver",
        lambda *_args, **_kwargs: (os.open(__file__, os.O_RDONLY), _sha("uv")),
    )
    destination = tmp_path / "venv"
    retained = tmp_path / "venv-retained"
    outside = tmp_path / "outside"
    outside.mkdir()

    def swap_root(command, **_kwargs):
        assert command[0] == "enrolled-uv"
        destination.rename(retained)
        destination.symlink_to(outside, target_is_directory=True)
        return ""

    monkeypatch.setattr(runtime, "_run_release_command", swap_root)
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(
            repo,
            destination,
            {
                **wheelhouse_env,
                runtime.RUNTIME_UV_EXECUTABLE_ENV: "/coordinator/enrolled-uv",
            },
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_closure_mismatch"}
    assert list(outside.iterdir()) == []


def test_dependency_sync_refuses_transient_project_source_substitution(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")
    (repo / "pyproject.toml").write_text(
        '[project]\nname="fixture"\nversion="1"\n', encoding="utf-8"
    )
    wheelhouse_env = _locked_wheelhouse(tmp_path / "wheelhouse", repo)
    _document, distribution = _producer_proof(tmp_path / "proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    monkeypatch.setattr(
        runtime,
        "_open_enrolled_release_driver",
        lambda *_args, **_kwargs: (os.open(__file__, os.O_RDONLY), _sha("uv")),
    )

    def swap_lock(command, **_kwargs):
        assert command[0] == "enrolled-uv"
        lock = repo / "uv.lock"
        original = lock.read_bytes()
        replacement = repo / "uv.lock.replacement"
        replacement.write_bytes(
            b'version = 1\nrevision = 3\nrequires-python = ">=3.12"\n'
        )
        replacement.replace(lock)
        restored = repo / "uv.lock.restored"
        restored.write_bytes(original)
        restored.replace(lock)
        return ""

    monkeypatch.setattr(runtime, "_run_release_command", swap_lock)
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(
            repo,
            tmp_path / "venv",
            {
                **wheelhouse_env,
                runtime.RUNTIME_UV_EXECUTABLE_ENV: "/coordinator/enrolled-uv",
            },
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_closure_mismatch"}


def test_dependency_sync_refuses_transient_runtime_descendant_rebinding(
    tmp_path: Path, monkeypatch
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")
    wheelhouse_env = _locked_wheelhouse(tmp_path / "wheelhouse", repo)
    _document, distribution = _producer_proof(tmp_path / "proof")
    monkeypatch.setattr(
        runtime,
        "_load_authorized_runtime_distribution",
        lambda _environ: distribution,
    )
    monkeypatch.setattr(
        runtime,
        "_open_enrolled_release_driver",
        lambda *_args, **_kwargs: (os.open(__file__, os.O_RDONLY), _sha("uv")),
    )
    destination = tmp_path / "venv"
    outside = tmp_path / "outside"
    outside.mkdir()

    def swap_lib(command, **_kwargs):
        assert command[0] == "enrolled-uv"
        assert command[-1].startswith("/proc/self/fd/")
        library = destination / "lib"
        retained = destination / "lib-retained"
        library.rename(retained)
        library.symlink_to(outside, target_is_directory=True)
        library.unlink()
        retained.rename(library)
        return ""

    monkeypatch.setattr(runtime, "_run_release_command", swap_lib)
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(
            repo,
            destination,
            {
                **wheelhouse_env,
                runtime.RUNTIME_UV_EXECUTABLE_ENV: "/coordinator/enrolled-uv",
            },
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_closure_mismatch"}
    assert list(outside.iterdir()) == []


def test_dependency_sync_refuses_loader_environment_before_proof(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".python-version").write_text("3.12\n", encoding="utf-8")

    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime.sync_dependencies(
            repo,
            tmp_path / "venv",
            {"LD_LIBRARY_PATH": "/tools/deps/lib"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_runtime_construction_receipt_is_canonical_and_path_free(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    venv = tmp_path / "venv"
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    interpreter = venv / "bin" / "python3.12"
    verifier = (
        venv
        / "lib"
        / "python3.12"
        / "site-packages"
        / f"{runtime.RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
    )
    repo.mkdir()
    distribution.copy_prefix_to(venv)
    interpreter.chmod(0o755)
    verifier.parent.mkdir(parents=True, exist_ok=True)
    verifier.write_bytes(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)

    runtime_root_fd = os.open(venv, os.O_RDONLY | os.O_DIRECTORY)
    repo_root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    try:
        document = runtime.write_runtime_construction_receipt(
            repo_root_fd=repo_root_fd,
            runtime_root_fd=runtime_root_fd,
            python_request=b"3.12",
            interpreter_relative_name="bin/python3.12",
            python_description={
                "major": 3,
                "minor": 12,
                "python_abi": "cp312",
                "builtin_modules": ["_abc"],
                "frozen_modules": ["_frozen_importlib"],
                "os": "linux",
                "architecture": "x86_64",
                "libc": "glibc",
            },
            distribution=distribution,
            dependency_wheelhouse_manifest_digest=_sha("wheelhouse"),
        )
    finally:
        os.close(runtime_root_fd)
        os.close(repo_root_fd)

    payload = (repo / runtime.RUNTIME_CONSTRUCTION_FILENAME).read_bytes()
    assert (
        payload
        == json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    )
    assert str(tmp_path).encode() not in payload
    receipt = document["receipt"]
    assert isinstance(receipt, Mapping)
    assert receipt["schema_version"] == runtime.RUNTIME_CONSTRUCTION_SCHEMA
    assert receipt["build_recipe_digest"] == runtime.RUNTIME_BUILD_RECIPE_DIGEST
    assert receipt["prefix_configuration"] == {
        "profile": "standalone-base-prefix/v1",
        "pyvenv_config": None,
    }
    assert receipt["environment_profile"] == "fixed-empty-authority/v1"
    assert receipt["cwd_profile"] == "unrelated-empty-directory/v1"
    assert receipt["execution_profile"] == "descriptor-exec/v1"


def test_runtime_authority_v2_exposes_verified_prefix_copy_proof_digest(
    tmp_path: Path, monkeypatch
) -> None:
    release, document = _sealed_release_fixture(tmp_path, monkeypatch)
    construction_receipt = document["receipt"]
    assert isinstance(construction_receipt, Mapping)

    with release.extension_runtime_authority() as authority:
        receipt = authority.receipt
        second_receipt = authority.receipt

        assert receipt["schema_version"] == ("okg.sealed_release.runtime_authority/v2")
        assert isinstance(authority._receipt, bytes)
        assert receipt == second_receipt
        assert receipt is not second_receipt
        assert (
            receipt["prefix_copy_proof_digest"]
            == (construction_receipt["prefix_copy_proof_digest"])
        )
        assert receipt["prefix_copy_proof_digest"] == _canonical_digest(
            document["prefix_copy_proof"]
        )
        assert (
            authority.receipt_digest
            == sealed_release_impl._runtime_canonical_digest(receipt)
        )
        with pytest.raises(AttributeError):
            authority.receipt_digest = "sha256:" + "0" * 64  # type: ignore[misc]
        with pytest.raises(TypeError):
            authority.platform_identity["os"] = "changed"  # type: ignore[index]
        receipt["python_abi"] = "changed"
        assert authority.receipt["python_abi"] == "cp312"
        assert str(tmp_path) not in json.dumps(receipt, sort_keys=True)


def test_descriptor_startup_holds_interpreter_and_verifier_until_live_recheck(
    tmp_path: Path, monkeypatch
) -> None:
    compile(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE, "runtime-verifier", "exec")
    root = tmp_path / "release"
    interpreter = root / "venv/bin/python3.12"
    verifier = root / "venv/lib/python3.12/site-packages/okg_release_runtime_verify.py"
    interpreter.parent.mkdir(parents=True)
    verifier.parent.mkdir(parents=True)
    interpreter_payload = b"fixture interpreter"
    interpreter.write_bytes(interpreter_payload)
    interpreter.chmod(0o555)
    verifier.write_bytes(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)
    verifier.chmod(0o444)
    native_names = (
        "venv/lib/libtcl9.0.so",
        "venv/lib/libtk9.0.so",
        "venv/lib/python3.12/lib-dynload/_tkinter.cpython-312.so",
    )
    for name in native_names:
        member = root / name
        member.parent.mkdir(parents=True, exist_ok=True)
        member.write_bytes(name.encode("utf-8"))
        member.chmod(0o444)

    interpreter_member = sealed_release_impl.RuntimeAuthorityMember(
        "bin/python3.12",
        0o555,
        len(interpreter_payload),
        str(runtime._raw_blob(interpreter_payload)["digest"]),
    )
    verifier_member = sealed_release_impl.RuntimeAuthorityMember(
        "lib/python3.12/site-packages/okg_release_runtime_verify.py",
        0o444,
        len(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE),
        str(runtime._raw_blob(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)["digest"]),
    )
    native_members = tuple(
        (
            name,
            sealed_release_impl.RuntimeAuthorityMember(
                name.removeprefix("venv/"),
                0o444,
                len(name.encode("utf-8")),
                str(runtime._raw_blob(name.encode("utf-8"))["digest"]),
            ),
        )
        for name in native_names
    )

    class DescriptorExecSupport:
        fexecve = object()

    monkeypatch.setattr(
        sealed_release_impl.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: DescriptorExecSupport(),
    )

    def fake_exec(_fd, argv, _environ):
        # A relative argv[0] in the empty cwd sends CPython to its build-time
        # prefix (#2352); a failed assertion here exits the child nonzero.
        assert argv[0] == "/proc/self/fd/6"
        request = os.read(3, 65536).decode("utf-8").splitlines()
        assert request[2:] == list(native_names)
        fields = [
            "okg.sealed_release.runtime_descriptor_startup_wire/v1",
            "cp312",
            *("1" for _index in range(11)),
            "3,4,5,6,8,9,10,11,12",
            "_abc",
            "_frozen_importlib",
            "\0".join(native_names),
        ]
        os.write(5, "\n".join(fields).encode("utf-8"))
        os.close(5)
        os.read(8, 1)
        os._exit(0)

    monkeypatch.setattr(sealed_release_impl, "_runtime_exec_fd", fake_exec)
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        proof = sealed_release_impl._descriptor_startup_proof(
            root_fd,
            "venv/bin/python3.12",
            python_abi="cp312",
            interpreter_member=interpreter_member,
            verifier_relative_name=(
                "venv/lib/python3.12/site-packages/okg_release_runtime_verify.py"
            ),
            verifier_member=verifier_member,
            tk_native_members=native_members,
        )
        assert proof["inherited_fds"] == [3, 4, 5, 6, 8, 9, 10, 11, 12]
        assert proof["tk_native_members"] == list(native_names)

        original_verify = sealed_release_impl._verify_runtime_member_fd
        verification_count = 0

        def tamper_during_child(
            root_fd,
            relative_name,
            member_fd,
            expected,
            *,
            expected_token=None,
        ):
            nonlocal verification_count
            verification_count += 1
            if verification_count == 7:
                verifier.chmod(0o644)
                verifier.write_bytes(runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE)
                verifier.chmod(0o444)
            return original_verify(
                root_fd,
                relative_name,
                member_fd,
                expected,
                expected_token=expected_token,
            )

        monkeypatch.setattr(
            sealed_release_impl, "_verify_runtime_member_fd", tamper_during_child
        )
        with pytest.raises(SealedReleaseInvalid) as refusal:
            sealed_release_impl._descriptor_startup_proof(
                root_fd,
                "venv/bin/python3.12",
                python_abi="cp312",
                interpreter_member=interpreter_member,
                verifier_relative_name=(
                    "venv/lib/python3.12/site-packages/okg_release_runtime_verify.py"
                ),
                verifier_member=verifier_member,
                tk_native_members=native_members,
            )
        assert refusal.value.data == {"code": "sealed_release_runtime_closure_mismatch"}
        assert verification_count == 7

        verification_count = 0
        native_path = root / native_names[0]

        def tamper_native_during_child(
            root_fd,
            relative_name,
            member_fd,
            expected,
            *,
            expected_token=None,
        ):
            nonlocal verification_count
            verification_count += 1
            if verification_count == 8:
                payload = native_path.read_bytes()
                native_path.chmod(0o644)
                native_path.write_bytes(payload)
                native_path.chmod(0o444)
            return original_verify(
                root_fd,
                relative_name,
                member_fd,
                expected,
                expected_token=expected_token,
            )

        monkeypatch.setattr(
            sealed_release_impl,
            "_verify_runtime_member_fd",
            tamper_native_during_child,
        )
        with pytest.raises(SealedReleaseInvalid) as native_refusal:
            sealed_release_impl._descriptor_startup_proof(
                root_fd,
                "venv/bin/python3.12",
                python_abi="cp312",
                interpreter_member=interpreter_member,
                verifier_relative_name=(
                    "venv/lib/python3.12/site-packages/okg_release_runtime_verify.py"
                ),
                verifier_member=verifier_member,
                tk_native_members=native_members,
            )
        assert native_refusal.value.data == {
            "code": "sealed_release_runtime_closure_mismatch"
        }
        assert verification_count == 8
    finally:
        interpreter.chmod(0o555)
        os.close(root_fd)


def test_runtime_authority_v2_refuses_changed_prefix_copy_proof(
    tmp_path: Path, monkeypatch
) -> None:
    release, _document = _sealed_release_fixture(
        tmp_path,
        monkeypatch,
        changed_prefix_copy_proof=True,
    )

    with pytest.raises(SealedReleaseInvalid) as refusal:
        release.extension_runtime_authority()

    assert refusal.value.data == {"code": "sealed_release_runtime_closure_mismatch"}


def test_runtime_distribution_verifies_two_signed_independent_builds(
    tmp_path: Path,
) -> None:
    document, verified = _producer_proof(tmp_path / "proof")

    assert verified.proof_digest == _canonical_digest(document)
    assert verified.reproducibility_proof_digest == _canonical_digest(
        document["reproducibility_proof"]
    )
    assert verified.platform_profile["profile_id"] == (
        "cpython-3.12.14-linux-x86_64-gnu-okg-relocatable-v1"
    )

    omitted_native = copy.deepcopy(document)
    link_manifest = cast(
        dict[str, object], omitted_native["native_link_output_manifest"]
    )
    archive_manifest = cast(
        dict[str, object], omitted_native["native_archive_output_manifest"]
    )
    cast(list[object], link_manifest["entries"]).pop()
    cast(list[object], archive_manifest["entries"]).pop()
    with pytest.raises(SealedReleaseInvalid) as incomplete_native:
        runtime._verify_authorized_runtime_distribution(
            omitted_native,
            None,
            expected_profile=verified.platform_profile,
        )
    assert incomplete_native.value.data == {
        "code": "sealed_release_runtime_recipe_mismatch"
    }

    off_recipe = copy.deepcopy(document)
    process_traces = cast(list[dict[str, object]], off_recipe["process_traces"])
    commands = cast(list[dict[str, object]], process_traces[0]["commands"])
    commands[0]["argv"] = ["build.py"]
    with pytest.raises(SealedReleaseInvalid) as changed_command:
        runtime._verify_authorized_runtime_distribution(
            off_recipe,
            None,
            expected_profile=verified.platform_profile,
        )
    assert changed_command.value.data == {
        "code": "sealed_release_runtime_recipe_mismatch"
    }

    changed_environment = copy.deepcopy(document)
    environment_traces = cast(
        list[dict[str, object]], changed_environment["process_traces"]
    )
    environment = cast(dict[str, str], environment_traces[0]["environment"])
    environment["PATH"] = "/attacker/bin"
    with pytest.raises(SealedReleaseInvalid) as changed_environment_refusal:
        runtime._verify_authorized_runtime_distribution(
            changed_environment,
            None,
            expected_profile=verified.platform_profile,
        )
    assert changed_environment_refusal.value.data == {
        "code": "sealed_release_runtime_non_relocatable"
    }


@pytest.mark.parametrize(
    ("options", "code"),
    [
        (
            {"reuse_executor": True},
            "sealed_release_runtime_reproducibility_mismatch",
        ),
        (
            {"rewritten_archive_manifest": True},
            "sealed_release_runtime_recipe_mismatch",
        ),
        (
            {"absolute_loader_search": True},
            "sealed_release_runtime_non_relocatable",
        ),
    ],
)
def test_runtime_distribution_refuses_invalid_producer_proof(
    tmp_path: Path,
    options: dict[str, bool],
    code: str,
) -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        _producer_proof(tmp_path / "proof", **cast(Any, options))

    assert refusal.value.data == {"code": code}


def test_runtime_distribution_refuses_pruned_complete_prefix(tmp_path: Path) -> None:
    document, verified = _producer_proof(tmp_path / "proof")
    library_root = tmp_path / "proof" / "python" / "install" / "lib"
    library_root.chmod(0o755)
    (library_root / "libtcl9.0.so").unlink()

    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._verify_authorized_runtime_distribution(
            document,
            tmp_path / "proof",
            expected_profile=verified.platform_profile,
        )

    assert refusal.value.data == {
        "code": "sealed_release_runtime_reproducibility_mismatch"
    }
    with pytest.raises(SealedReleaseInvalid) as retained_copy:
        verified.copy_prefix_to(tmp_path / "changed-copy")
    assert retained_copy.value.data == {
        "code": "sealed_release_runtime_reproducibility_mismatch"
    }


def test_runtime_distribution_copy_refuses_destination_directory_swap(
    tmp_path: Path, monkeypatch
) -> None:
    _document, verified = _producer_proof(tmp_path / "proof")
    destination = tmp_path / "copy"
    outside = tmp_path / "outside"
    outside.mkdir()
    original_open = runtime._open_directory_member_at
    swapped = False

    def swap_after_open(root_fd, relative_name, *, code, require_readonly):
        nonlocal swapped
        fd = original_open(
            root_fd,
            relative_name,
            code=code,
            require_readonly=require_readonly,
        )
        if not require_readonly and relative_name == "lib" and not swapped:
            swapped = True
            (destination / "lib").rename(destination / "lib-retained")
            (destination / "lib").symlink_to(outside, target_is_directory=True)
        return fd

    monkeypatch.setattr(runtime, "_open_directory_member_at", swap_after_open)
    with pytest.raises(SealedReleaseInvalid) as refusal:
        verified.copy_prefix_to(destination)

    assert refusal.value.data == {
        "code": "sealed_release_runtime_reproducibility_mismatch"
    }
    assert swapped is True
    assert list(outside.iterdir()) == []


def test_runtime_closure_refuses_absolute_host_toolcache_search() -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "bin/python3",
            ("libpython3.12.so.1.0",),
            ("/opt/hostedtoolcache/Python/3.12.14/x64/lib",),
            {"lib/libpython3.12.so.1.0"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


@pytest.mark.parametrize("origin_token", ["$ORIGIN", "${ORIGIN}"])
def test_runtime_closure_resolves_contained_origin_relative_dependency(
    origin_token: str,
) -> None:
    dependency = f"{origin_token}/../lib/libpython3.12.so.1.0"

    assert runtime._resolve_elf_dependencies(
        "lib/libpython3.so",
        (dependency,),
        (),
        {"lib/libpython3.so", "lib/libpython3.12.so.1.0"},
    ) == [
        {
            "dependency": dependency,
            "resolution": "lib/libpython3.12.so.1.0",
        }
    ]


@pytest.mark.parametrize(
    "dependency",
    [
        "/lib/libpython3.12.so.1.0",
        "lib/libpython3.12.so.1.0",
        "../lib/libpython3.12.so.1.0",
        "$ORIGIN/..\\lib/libpython3.12.so.1.0",
        "$ORIGIN/../../lib/libpython3.12.so.1.0",
    ],
)
def test_runtime_closure_refuses_noncontained_dependency_locator(
    dependency: str,
) -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "lib/libpython3.so",
            (dependency,),
            (),
            {"lib/libpython3.12.so.1.0"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_runtime_closure_refuses_missing_origin_relative_target_without_fallback() -> (
    None
):
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "bin/python3",
            ("$ORIGIN/../lib/libc.so.6",),
            ("$ORIGIN/../lib",),
            {"bin/python3"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_runtime_closure_requires_declared_search_for_nonplatform_dependency() -> None:
    assert runtime._resolve_elf_dependencies(
        "bin/python3",
        ("libpython3.12.so.1.0",),
        ("$ORIGIN/../lib",),
        {"bin/python3", "lib/libpython3.12.so.1.0"},
    ) == [
        {
            "dependency": "libpython3.12.so.1.0",
            "resolution": "lib/libpython3.12.so.1.0",
        }
    ]
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "bin/python3",
            ("libpython3.12.so.1.0",),
            (),
            {"bin/python3", "lib/libpython3.12.so.1.0"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_runtime_closure_refuses_escaping_declared_search() -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "bin/python3",
            ("libpython3.12.so.1.0",),
            ("$ORIGIN/../../lib",),
            {"lib/libpython3.12.so.1.0"},
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_runtime_closure_allows_shallow_platform_dependency() -> None:
    assert runtime._resolve_elf_dependencies(
        "bin/python3",
        ("libc.so.6",),
        (),
        {"bin/python3"},
    ) == [{"dependency": "libc.so.6", "resolution": "platform-allow-list"}]


def test_runtime_closure_refuses_unresolved_nonplatform_dependency() -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        runtime._resolve_elf_dependencies(
            "bin/python3",
            ("libprivate-runtime.so",),
            ("$ORIGIN/../lib",),
            set(),
        )

    assert refusal.value.data == {"code": "sealed_release_runtime_non_relocatable"}


def test_prefix_walk_uses_whole_name_byte_order(tmp_path: Path) -> None:
    # Issue #2352: a depth-first walk listed "pip/__init__.py" before its
    # sibling "pip-26.2.1.dist-info" ("-" sorts before "/"), so every real
    # python-build-standalone prefix differed from its proved tree.
    root = tmp_path / "prefix"
    for name in (
        "lib/tcl9/init.tcl",
        "lib/tcl9.0/tclConfig.sh",
        "site-packages/pip/__init__.py",
        "site-packages/pip-26.2.1.dist-info/METADATA",
    ):
        member = root / name
        member.parent.mkdir(parents=True, exist_ok=True)
        member.write_bytes(name.encode())
    expected = [item["relative_name"] for item in runtime._runtime_prefix_members(root)]
    assert expected == sorted(expected, key=lambda value: str(value).encode("utf-8"))
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        walked = runtime._runtime_prefix_members_at(root_fd, require_readonly=False)
    finally:
        os.close(root_fd)
    assert [item["relative_name"] for item in walked] == expected
    assert walked == runtime._runtime_prefix_members(root)


def test_construction_receipt_excludes_bytecode(tmp_path: Path) -> None:
    # Sealing removes __pycache__, .pyc and .pyo and verification refuses them
    # (#2857); the construction receipt must agree or the sealed runtime never
    # matches it (#2352). The filter is the shared case-insensitive one.
    repo = tmp_path / "repo"
    venv = tmp_path / "venv"
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    repo.mkdir()
    distribution.copy_prefix_to(venv)
    (venv / "bin" / "python3.12").chmod(0o755)
    site = venv / "lib" / "python3.12" / "site-packages"
    site.mkdir(parents=True, exist_ok=True)
    (site / f"{runtime.RUNTIME_RELOCATION_VERIFIER_MODULE}.py").write_bytes(
        runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE
    )
    cache = venv / "lib" / "python3.12" / "encodings" / "__pycache__"
    cache.mkdir(parents=True)
    (cache / "utf_8.cpython-312.pyc").write_bytes(b"bytecode")
    (site / "stray.pyc").write_bytes(b"bytecode")
    (site / "LOUD.PYO").write_bytes(b"bytecode")
    (site / "kept.py").write_bytes(b"VALUE = 1\n")
    runtime_root_fd = os.open(venv, os.O_RDONLY | os.O_DIRECTORY)
    repo_root_fd = os.open(repo, os.O_RDONLY | os.O_DIRECTORY)
    try:
        document = runtime.write_runtime_construction_receipt(
            repo_root_fd=repo_root_fd,
            runtime_root_fd=runtime_root_fd,
            python_request=b"3.12",
            interpreter_relative_name="bin/python3.12",
            python_description={
                "major": 3,
                "minor": 12,
                "python_abi": "cp312",
                "builtin_modules": ["_abc"],
                "frozen_modules": ["_frozen_importlib"],
                "os": "linux",
                "architecture": "x86_64",
                "libc": "glibc",
            },
            distribution=distribution,
            dependency_wheelhouse_manifest_digest=_sha("wheelhouse"),
        )
    finally:
        os.close(runtime_root_fd)
        os.close(repo_root_fd)
    names = [
        str(item["relative_name"])
        for item in cast(list[dict[str, object]], document["runtime_members"])
    ]
    assert "lib/python3.12/site-packages/kept.py" in names
    assert not [name for name in names if is_bytecode_relative_name(name)]
    assert "lib/python3.12/site-packages/LOUD.PYO" not in names
    assert "lib/python3.12/encodings" in names


def test_relocation_verifier_scans_descriptors_before_tcl_starts() -> None:
    # Tcl opens /dev/null on closed descriptors 0-2 and a notifier pipe when
    # _tkinter loads, so a scan after the import never matches (#2352).
    source = runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE
    scan = source.index(b"for fd in range(64):")
    assert source.count(b"for fd in range(64):") == 1
    assert scan < source.index(b"import tkinter,_tkinter")


def _tk_loader_graph(tk_library: str) -> dict[str, object]:
    return {
        "objects": [
            {
                "relative_name": "venv/lib/python3.12/lib-dynload/_tkinter.so",
                "resolutions": [
                    {"dependency": "libtcl9.0.so", "resolution": "venv/lib/libtcl9.0.so"},
                    {"dependency": tk_library, "resolution": f"venv/lib/{tk_library}"},
                    {"dependency": "libc.so.6", "resolution": "platform-allow-list"},
                ],
            }
        ]
    }


@pytest.mark.parametrize("tk_library", ["libtcl9tk9.0.so", "libtk8.6.so"])
def test_runtime_tk_closure_accepts_tk8_and_tk9_library_names(
    tk_library: str,
) -> None:
    # python-build-standalone 20260814 ships Tk 9 as libtcl9tk9.0.so (#2352).
    graph = _tk_loader_graph(tk_library)
    members = {
        name: object()
        for name in (
            "venv/lib/libtcl9.0.so",
            f"venv/lib/{tk_library}",
            "venv/lib/python3.12/lib-dynload/_tkinter.so",
        )
    }
    assert sealed_release_impl._runtime_tk_native_members(graph, members) == tuple(
        sorted(members, key=lambda value: value.encode("utf-8"))
    )


@pytest.mark.parametrize("tk_library", ["libtclfoo9.0.so", "libfoo9tk9.0.so"])
def test_runtime_tk_closure_refuses_names_outside_the_tk_patterns(
    tk_library: str,
) -> None:
    graph = _tk_loader_graph(tk_library)
    graph["objects"][0]["resolutions"][1]["dependency"] = tk_library  # type: ignore[index]
    members = {
        name: object()
        for name in (
            "venv/lib/libtcl9.0.so",
            f"venv/lib/{tk_library}",
            "venv/lib/python3.12/lib-dynload/_tkinter.so",
        )
    }
    with pytest.raises(SealedReleaseInvalid):
        sealed_release_impl._runtime_tk_native_members(graph, members)
