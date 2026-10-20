"""Frozen, relocatable Python runtime construction for sealed releases."""

from __future__ import annotations

import base64
import csv
import ctypes
import hashlib
import io
import json
import os
import platform
import posixpath
import re
import stat
import struct
import subprocess
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
from typing import Mapping, Sequence
from urllib.parse import unquote, urlparse

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from okg.substrate._sealed_release_integrity import (
    is_bytecode_relative_name,
    private_pycache_prefix,
)
from okg.substrate.errors import SealedReleaseInvalid


_PYTHON_SHEBANG = re.compile(rb"^#![^\n]*python(?:\d+(?:\.\d+)*)?$")
_RELOCATABLE_PYTHON_HEADER = (
    b"#!/bin/sh\n"
    b"'''exec' \"$(dirname -- \"$(realpath -- \"$0\")\")\"/'python3' "
    b'"$0" "$@"\n'
    b"' '''\n"
)
RUNTIME_CONSTRUCTION_FILENAME = ".okg-release-runtime.json"
RUNTIME_CONSTRUCTION_SCHEMA = "okg.sealed_release.runtime_construction_receipt/v2"
RUNTIME_CONSTRUCTION_DOCUMENT_SCHEMA = (
    "okg.sealed_release.runtime_construction_document/v2"
)
RUNTIME_PREFIX_TREE_SCHEMA = "okg.sealed_release.runtime_prefix_tree/v1"
RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA = (
    "okg.sealed_release.runtime_distribution_prefix_tree/v1"
)
RUNTIME_DISTRIBUTION_BUNDLE_SCHEMA = "okg.sealed_release.runtime_distribution_bundle/v1"
RUNTIME_DISTRIBUTION_ENROLLMENT_SCHEMA = (
    "okg.sealed_release.runtime_distribution_enrollment/v1"
)
RUNTIME_SOURCE_MANIFEST_SCHEMA = "okg.sealed_release.runtime_source_manifest/v2"
RUNTIME_BUILD_RECIPE_SCHEMA = "okg.sealed_release.runtime_build_recipe/v2"
RUNTIME_PLATFORM_PROFILE_SCHEMA = "okg.sealed_release.runtime_platform_profile/v1"
RUNTIME_REPRODUCIBILITY_SESSION_SCHEMA = (
    "okg.sealed_release.runtime_reproducibility_session/v1"
)
RUNTIME_BUILD_ATTESTATION_SCHEMA = "okg.sealed_release.runtime_build_attestation/v2"
RUNTIME_REPRODUCIBILITY_PROOF_SCHEMA = (
    "okg.sealed_release.runtime_reproducibility_proof/v2"
)
RUNTIME_DISTRIBUTION_ACQUISITION_SCHEMA = (
    "okg.sealed_release.runtime_distribution_acquisition/v1"
)
RUNTIME_PREFIX_COPY_PROOF_SCHEMA = "okg.sealed_release.runtime_prefix_copy_proof/v1"
# Upstream bytecode is listed as excluded, not copied: sealing removes it and
# verification refuses any that appears (#2857).
RUNTIME_PREFIX_COPY_STATUS = "exact-prefix-without-bytecode-plus-declared-additions"
RUNTIME_RELEASE_ADDITIONS_SCHEMA = (
    "okg.sealed_release.runtime_release_additions_manifest/v1"
)
RUNTIME_ORIGINAL_LINK_TRANSCRIPT_SCHEMA = (
    "okg.sealed_release.runtime_original_link_search_transcript/v1"
)
RUNTIME_NATIVE_OUTPUT_MANIFEST_SCHEMA = (
    "okg.sealed_release.runtime_native_output_manifest/v1"
)
RUNTIME_PROCESS_TRACE_SCHEMA = "okg.sealed_release.runtime_process_execution_trace/v2"
RUNTIME_NATIVE_CLOSURE_SCHEMA = "okg.sealed_release.runtime_native_closure/v1"
RUNTIME_LOADER_GRAPH_SCHEMA = "okg.sealed_release.runtime_loader_graph/v1"
RUNTIME_LOADER_PROOF_SCHEMA = "okg.sealed_release.runtime_loader_relocation_proof/v1"
RUNTIME_PLATFORM_ALLOW_LIST_SCHEMA = (
    "okg.sealed_release.runtime_platform_loader_allow_list/v1"
)
RUNTIME_RELOCATION_VERIFIER_MODULE = "okg_release_runtime_verify"
RUNTIME_BUILD_RECIPE_DIGEST = (
    "sha256:41e3a7e40e51145ea6ba8aef9fcc83b9296f4add8b297aa70a234e41ef3d6145"
)
RUNTIME_PROOF_ROOT_ENV = "OKG_SEALED_RUNTIME_PROOF_ROOT"
RUNTIME_PROOF_DIGEST_ENV = "OKG_SEALED_RUNTIME_PROOF_DIGEST"
RUNTIME_UV_EXECUTABLE_ENV = "OKG_SEALED_RUNTIME_UV_EXECUTABLE"
RUNTIME_WHEELHOUSE_ROOT_ENV = "OKG_SEALED_RUNTIME_WHEELHOUSE_ROOT"
RUNTIME_WHEELHOUSE_MANIFEST_DIGEST_ENV = "OKG_SEALED_RUNTIME_WHEELHOUSE_MANIFEST_DIGEST"
RUNTIME_WHEELHOUSE_MANIFEST_SCHEMA = (
    "okg.sealed_release.locked_dependency_wheelhouse/v1"
)
_RUNTIME_PROOF_FILENAME = "proof.json"
_RUNTIME_WHEELHOUSE_MANIFEST_FILENAME = "manifest.json"
_RUNTIME_ARCHIVE_FILENAME = "runtime-distribution.tar.zst"
_RUNTIME_PREFIX_RELATIVE = "python/install"
_RUNTIME_FORBIDDEN_ENVIRONMENT = frozenset(
    {
        "LD_LIBRARY_PATH",
        "DYLD_LIBRARY_PATH",
        "DYLD_FALLBACK_LIBRARY_PATH",
        "PYTHONHOME",
        "PYTHONPATH",
    }
)
_RUNTIME_FORBIDDEN_MUTATION_TOOLS = frozenset(
    {"chrpath", "install_name_tool", "llvm-bolt", "objcopy", "patchelf", "strip"}
)
_RUNTIME_DISTINCT_BUILD_FIELDS = (
    "builder_role_id",
    "authenticated_executor_identity_digest",
    "ephemeral_attestor_public_key_digest",
    "clean_builder_root_creation_receipt_digest",
    "clean_builder_nonce_raw_blob",
    "acquisition_worker_identity_digest",
    "acquisition_receipt_digest",
)
_RUNTIME_BUILD_ROLES = (
    "independent-builder-a/v1",
    "independent-builder-b/v1",
)
_RUNTIME_BUILD_ENVIRONMENT = {
    "DOCKER_CONFIG": "recipe-root/pinned-tools/docker-config",
    "DOCKER_HOST": "unix:///var/run/docker.sock",
    "HOME": "recipe-root/build-home",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "NUM_CPUS": "1",
    "NUM_JOBS_AGGRESSIVE": "3",
    "PATH": "/usr/bin:/bin",
    "PYTHONHASHSEED": "0",
    "SOURCE_DATE_EPOCH": "1704067200",
    "TMPDIR": "recipe-root/build-tmp",
    "TZ": "UTC",
    "UV_CACHE_DIR": "recipe-root/pinned-inputs/uv-cache",
}
_RUNTIME_PLATFORM_PROFILES: dict[tuple[str, str, str, str], dict[str, str]] = {
    ("linux", "x86_64", "glibc", "cp312"): {
        "schema_version": RUNTIME_PLATFORM_PROFILE_SCHEMA,
        "profile_id": "cpython-3.12.14-linux-x86_64-gnu-okg-relocatable-v1",
        "target_triple": "x86_64-unknown-linux-gnu",
        "os": "linux",
        "architecture": "x86_64",
        "libc": "glibc",
        "minimum_glibc": "2.17",
        "kernel_contract": "descriptor-exec-v1",
        "python_abi": "cp312",
    },
    ("linux", "aarch64", "glibc", "cp312"): {
        "schema_version": RUNTIME_PLATFORM_PROFILE_SCHEMA,
        "profile_id": "cpython-3.12.14-linux-aarch64-gnu-okg-relocatable-v1",
        "target_triple": "aarch64-unknown-linux-gnu",
        "os": "linux",
        "architecture": "aarch64",
        "libc": "glibc",
        "minimum_glibc": "2.17",
        "kernel_contract": "descriptor-exec-v1",
        "python_abi": "cp312",
    },
}
RUNTIME_DISTRIBUTION_ENROLLMENT: dict[str, object] = {
    "schema_version": RUNTIME_DISTRIBUTION_ENROLLMENT_SCHEMA,
    "upstream_repository": "https://github.com/astral-sh/python-build-standalone",
    "upstream_release_tag": "20260814",
    "upstream_commit": "38d35dcf0e212ca02eed8ebc11d0c92906387d56",
    "upstream_tree": "9d623a68052ad503c8d72085fb1823c206cf329a",
    "upstream_source_raw_blob": (
        "sha256:66c2ba43e778ee55b3a2f4c925525e3c4352b13dbd77b3bc479478f7a7dcd7c3"
    ),
    "cpython_version": "3.12.14",
    "cpython_source_raw_blob": (
        "sha256:5c8462af5790baf43a321a1559dbe0db06d1be4300fb85fb53c40060668e548a"
    ),
    "build_recipe_digest": RUNTIME_BUILD_RECIPE_DIGEST,
    "platform_profiles": sorted(
        (dict(profile) for profile in _RUNTIME_PLATFORM_PROFILES.values()),
        key=lambda profile: str(profile["profile_id"]),
    ),
}
RUNTIME_RELOCATION_VERIFIER_SOURCE = b"""import _imp,os,sys
def inside(value,root):
 try: relative=os.path.relpath(os.path.realpath(value),root)
 except ValueError: return False
 return relative!=".." and not relative.startswith(".."+os.sep)
def member_stat(root_fd,name):
 parts=name.split("/")
 if not parts or any(not part or part in (".","..") for part in parts): raise ValueError("invalid member")
 current=os.dup(root_fd)
 try:
  for part in parts[:-1]:
   next_fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=current)
   os.close(current); current=next_fd
  member=os.open(parts[-1],os.O_RDONLY|os.O_CLOEXEC|getattr(os,"O_NOFOLLOW",0),dir_fd=current)
  try: return os.fstat(member)
  finally: os.close(member)
 finally: os.close(current)
wire=os.read(3,65536).decode("utf-8").splitlines()
if len(wire)<3: raise ValueError("incomplete request")
interpreter_member,verifier_member=wire[:2]
native_members=wire[2:]
executed=os.fstat(6)
sealed=member_stat(4,interpreter_member)
verifier=os.fstat(9)
sealed_verifier=member_stat(4,verifier_member)
descriptor_match=(executed.st_mode&0o170000)==0o100000 and (sealed.st_mode&0o170000)==0o100000 and (executed.st_dev,executed.st_ino)==(sealed.st_dev,sealed.st_ino)
verifier_match=(verifier.st_mode&0o170000)==0o100000 and (sealed_verifier.st_mode&0o170000)==0o100000 and (verifier.st_dev,verifier.st_ino)==(sealed_verifier.st_dev,sealed_verifier.st_ino)
fds=[]
for fd in range(64):
 try: os.fstat(fd); fds.append(fd)
 except OSError: pass
import tkinter,_tkinter
prefix=os.path.realpath(sys.prefix)
origins=[]
for module in tuple(sys.modules.values()):
 value=getattr(module,"__file__",None)
 if value: origins.append(value)
maps=open("/proc/self/maps","rb").read().splitlines()
def mapped(member_fd):
 wanted=os.fstat(member_fd)
 for line in maps:
  fields=line.split(None,5)
  if len(fields)>=5 and b":" in fields[3]:
   major,minor=fields[3].split(b":",1)
   if int(major,16)==os.major(wanted.st_dev) and int(minor,16)==os.minor(wanted.st_dev) and int(fields[4])==wanted.st_ino: return True
 return False
native_fds=list(range(10,10+len(native_members)))
loaded_native=sorted(member for member,member_fd in zip(native_members,native_fds) if mapped(member_fd))
tkinter_stat=os.stat(_tkinter.__file__)
tkinter_exact=any((tkinter_stat.st_dev,tkinter_stat.st_ino)==(os.fstat(member_fd).st_dev,os.fstat(member_fd).st_ino) for member,member_fd in zip(native_members,native_fds) if os.path.basename(member).startswith("_tkinter"))
values=["okg.sealed_release.runtime_descriptor_startup_wire/v1","cp%d%d"%(sys.version_info.major,sys.version_info.minor),str(int(bool(sys.flags.isolated))),str(int(bool(sys.flags.ignore_environment))),str(int(bool(sys.flags.no_user_site))),str(int(bool(sys.dont_write_bytecode))),str(int(descriptor_match)),str(int(verifier_match)),str(int(inside(sys.base_prefix,prefix))),str(int(all(inside(value,prefix) for value in origins))),str(int(not os.listdir("."))),"1",str(int(tkinter_exact and loaded_native==sorted(native_members))),",".join(str(fd) for fd in fds),"\\0".join(sorted(sys.builtin_module_names)),"\\0".join(sorted(name for name in sys.stdlib_module_names if _imp.is_frozen(name))),"\\0".join(loaded_native)]
os.write(5,"\\n".join(values).encode("utf-8"))
os.close(5)
os.read(8,1)
"""

_LINUX_PLATFORM_LIBRARIES = frozenset(
    {
        "ld-linux-aarch64.so.1",
        "ld-linux-x86-64.so.2",
        "libbz2.so.1.0",
        "libc.so.6",
        "libcrypt.so.1",
        "libdl.so.2",
        "libexpat.so.1",
        "libffi.so.8",
        "libgcc_s.so.1",
        "liblzma.so.5",
        "libm.so.6",
        "libnsl.so.1",
        "libpthread.so.0",
        "libresolv.so.2",
        "librt.so.1",
        "libutil.so.1",
        "libuuid.so.1",
        "libz.so.1",
    }
)


@dataclass(slots=True)
class VerifiedMaterializedRuntime:
    """Copied runtime retained by descriptor through construction and receipt."""

    locator: Path
    distribution_members: tuple[Mapping[str, object], ...]
    _root_fd: int
    _root_identity: tuple[int, int]
    _closed: bool = False

    @property
    def root_fd(self) -> int:
        if self._closed or self._root_fd < 0:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "materialized runtime authority is closed",
            )
        return self._root_fd

    def assert_path_binding(self) -> None:
        observed_fd = _open_absolute_directory_nofollow(
            self.locator, code="sealed_release_runtime_closure_mismatch"
        )
        try:
            observed = os.fstat(observed_fd)
            retained = os.fstat(self.root_fd)
            if (observed.st_dev, observed.st_ino) != self._root_identity or (
                retained.st_dev,
                retained.st_ino,
            ) != self._root_identity:
                raise _invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "materialized runtime path no longer names the retained root",
                )
        finally:
            os.close(observed_fd)

    def members(self) -> list[dict[str, object]]:
        return _runtime_prefix_members_at(self.root_fd, require_readonly=False)

    def assert_distribution_unchanged(self) -> None:
        _assert_distribution_prefix_unchanged(self.root_fd, self.distribution_members)

    def close(self) -> None:
        if self._closed:
            return
        root_fd = self._root_fd
        self._root_fd = -1
        self._closed = True
        if root_fd >= 0:
            os.close(root_fd)

    def __del__(self) -> None:  # pragma: no cover - best-effort descriptor cleanup
        if hasattr(self, "_closed"):
            self.close()


@dataclass(slots=True)
class VerifiedRuntimeDistribution:
    proof_document: Mapping[str, object]
    proof_digest: str
    distribution_members: tuple[Mapping[str, object], ...]
    platform_profile: Mapping[str, str]
    source_manifest_digest: str
    reproducibility_proof_digest: str
    distribution_acquisition_receipt_digest: str
    distribution_archive: Mapping[str, object]
    _prefix_fd: int
    _closed: bool = False

    def copy_prefix_to(self, destination: Path) -> VerifiedMaterializedRuntime:
        if self._closed or self._prefix_fd < 0:
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "verified runtime distribution is closed",
            )
        expected = tuple(self.distribution_members)
        if tuple(_runtime_prefix_members_at(self._prefix_fd)) != expected:
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "retained runtime distribution changed before copy",
            )
        destination_fd = _copy_runtime_prefix_from_fd(
            self._prefix_fd, destination, expected
        )
        try:
            if tuple(_runtime_prefix_members_at(self._prefix_fd)) != expected:
                raise _invalid(
                    "sealed_release_runtime_reproducibility_mismatch",
                    "retained runtime distribution changed during copy",
                )
            root = os.fstat(destination_fd)
            return VerifiedMaterializedRuntime(
                destination,
                expected,
                destination_fd,
                (root.st_dev, root.st_ino),
            )
        except BaseException:
            os.close(destination_fd)
            raise

    def close(self) -> None:
        if self._closed:
            return
        prefix_fd = self._prefix_fd
        self._prefix_fd = -1
        self._closed = True
        if prefix_fd >= 0:
            os.close(prefix_fd)

    def __del__(self) -> None:  # pragma: no cover - best-effort descriptor cleanup
        self.close()


def sync_dependencies(
    repo_root: Path,
    venv_root: Path,
    environ: Mapping[str, str],
) -> None:
    """Materialize only an exact authorized reproducible CPython distribution."""

    forbidden = sorted(_RUNTIME_FORBIDDEN_ENVIRONMENT.intersection(environ))
    if forbidden:
        raise _invalid(
            "sealed_release_runtime_non_relocatable",
            "sealed runtime construction refuses inherited loader or Python authority",
        )
    repo_root_fd = _open_absolute_directory_nofollow(
        repo_root, code="sealed_release_runtime_closure_mismatch"
    )
    try:
        _sync_dependencies_retained(repo_root, repo_root_fd, venv_root, environ)
    finally:
        os.close(repo_root_fd)


def _sync_dependencies_retained(
    repo_root: Path,
    repo_root_fd: int,
    venv_root: Path,
    environ: Mapping[str, str],
) -> None:
    repo_source_state = _retained_tree_state_at(
        repo_root_fd, code="sealed_release_runtime_closure_mismatch"
    )
    try:
        python_request = (
            _read_regular_member_at(
                repo_root_fd,
                ".python-version",
                code="sealed_release_runtime_authority_missing",
                require_readonly=False,
            )
            .decode("utf-8")
            .strip()
        )
    except (OSError, UnicodeDecodeError) as exc:
        raise SealedReleaseInvalid(
            "sealed release requires a checked-in .python-version"
        ) from exc
    if not python_request or any(character.isspace() for character in python_request):
        raise SealedReleaseInvalid("sealed release .python-version is invalid")
    if python_request not in {"3.12", "3.12.14"}:
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "sealed release Python request has no enrolled distribution",
        )

    distribution = _load_authorized_runtime_distribution(environ)
    materialized: VerifiedMaterializedRuntime | None = None
    try:
        materialized = distribution.copy_prefix_to(venv_root)
    finally:
        distribution.close()
    interpreter_fd = -1
    try:
        if materialized is None:  # pragma: no cover - copy either returns or raises
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "runtime copy did not retain its destination",
            )
        materialized.assert_path_binding()
        materialized.assert_distribution_unchanged()
        _assert_directory_path_binding(
            repo_root,
            repo_root_fd,
            code="sealed_release_runtime_closure_mismatch",
        )
        protected_names = {
            str(item["relative_name"]) for item in distribution.distribution_members
        }

        interpreter_relative_name = "bin/python3.12"
        interpreter_expected = next(
            item
            for item in distribution.distribution_members
            if item.get("relative_name") == interpreter_relative_name
        )
        interpreter_fd = _open_verified_runtime_file_fd(
            materialized.root_fd,
            interpreter_relative_name,
            interpreter_expected,
            code="sealed_release_runtime_closure_mismatch",
        )
        site_packages_relative_name = "lib/python3.12/site-packages"
        if any(
            str(item["relative_name"]).startswith(f"{site_packages_relative_name}/")
            for item in distribution.distribution_members
        ):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "enrolled base runtime contains mutable site-package members",
            )
        _ensure_runtime_directories_at(
            materialized.root_fd,
            site_packages_relative_name,
            code="sealed_release_runtime_closure_mismatch",
        )
        protected_runtime_directories = _protected_runtime_directory_state_at(
            materialized.root_fd,
            distribution.distribution_members,
            mutable_directories={"bin", site_packages_relative_name},
        )

        runtime_fd_path = f"/proc/self/fd/{materialized.root_fd}"
        sync_env = {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONDONTWRITEBYTECODE": "1",
            "UV_LINK_MODE": "copy",
            "UV_PROJECT_ENVIRONMENT": runtime_fd_path,
        }
        extras_raw = environ.get("OKG_SEALED_PYTHON_EXTRAS", "[]")
        try:
            extras = json.loads(extras_raw)
        except ValueError as exc:
            raise SealedReleaseInvalid(
                "sealed release Python extras are invalid"
            ) from exc
        if (
            not isinstance(extras, list)
            or any(not isinstance(value, str) or not value.strip() for value in extras)
            or len(set(extras)) != len(extras)
        ):
            raise SealedReleaseInvalid(
                "sealed release Python extras must be unique names"
            )
        wheelhouse_fd, wheelhouse_digest, wheelhouse_state = (
            _open_locked_dependency_wheelhouse(repo_root_fd, environ)
        )
        try:
            uv_fd, uv_digest = _open_enrolled_release_driver(
                environ.get(RUNTIME_UV_EXECUTABLE_ENV),
                distribution,
                logical_name="uv",
            )
            command = [
                "enrolled-uv",
                "sync",
                "--offline",
                "--no-cache",
                "--no-config",
                "--no-index",
                "--find-links",
                f"/proc/self/fd/{wheelhouse_fd}",
                "--locked",
                "--active",
                "--no-editable",
                "--no-dev",
                "--python",
                f"/proc/self/fd/{interpreter_fd}",
            ]
            for extra in extras:
                command.extend(["--extra", extra])
            try:
                _run_release_command(
                    command,
                    cwd=Path(f"/proc/self/fd/{repo_root_fd}"),
                    environ=sync_env,
                    executable_fd=uv_fd,
                    retained_fds=(
                        materialized.root_fd,
                        wheelhouse_fd,
                        repo_root_fd,
                        interpreter_fd,
                    ),
                    expected_executable_digest=uv_digest,
                    require_live_executable_verification=True,
                    write_root_fds=(materialized.root_fd,),
                )
            finally:
                os.close(uv_fd)
            if (
                _retained_tree_state_at(
                    repo_root_fd, code="sealed_release_runtime_closure_mismatch"
                )
                != repo_source_state
            ):
                raise _invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "release project inputs changed during dependency installation",
                )
            if (
                _retained_tree_state_at(
                    wheelhouse_fd, code="sealed_release_runtime_acquisition_mismatch"
                )
                != wheelhouse_state
            ):
                raise _invalid(
                    "sealed_release_runtime_acquisition_mismatch",
                    "dependency wheelhouse changed during installation",
                )
        finally:
            os.close(wheelhouse_fd)
        if (
            _protected_runtime_directory_state_at(
                materialized.root_fd,
                distribution.distribution_members,
                mutable_directories={"bin", site_packages_relative_name},
            )
            != protected_runtime_directories
        ):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "protected runtime directories changed during dependency installation",
            )
        materialized.assert_path_binding()
        members_after_sync = materialized.members()
        if any(item["relative_name"] == "pyvenv.cfg" for item in members_after_sync):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "standalone runtime construction created a host-bound pyvenv configuration",
            )
        materialized.assert_distribution_unchanged()
        _canonicalize_direct_urls(
            repo_root_fd,
            materialized.root_fd,
            protected_names=protected_names,
        )
        _rewrite_python_shebangs(materialized.root_fd, protected_names=protected_names)
        _refresh_record_hashes(materialized.root_fd, protected_names=protected_names)
        materialized.assert_distribution_unchanged()
        try:
            description = _describe_interpreter(
                "enrolled-python",
                cwd=Path(f"/proc/self/fd/{repo_root_fd}"),
                environ={
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                executable_fd=interpreter_fd,
                retained_fds=(materialized.root_fd, repo_root_fd),
                sealed_root_fds=(materialized.root_fd, repo_root_fd),
                expected_executable_digest=str(
                    _strict_mapping(
                        interpreter_expected.get("raw_blob"),
                        code="sealed_release_runtime_closure_mismatch",
                    )["digest"]
                ),
                require_live_executable_verification=True,
            )
        finally:
            os.close(interpreter_fd)
            interpreter_fd = -1
        observed_profile = _selected_platform_profile(description)
        if observed_profile != dict(distribution.platform_profile):
            raise _invalid(
                "sealed_release_runtime_distribution_unsupported",
                "constructed interpreter differs from the enrolled platform profile",
            )
        verifier_relative_name = (
            f"lib/python{description['major']}.{description['minor']}/site-packages/"
            f"{RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
        )
        _write_new_runtime_file_at(
            materialized.root_fd,
            verifier_relative_name,
            RUNTIME_RELOCATION_VERIFIER_SOURCE,
            mode=0o600,
            code="sealed_release_runtime_closure_mismatch",
        )
        write_runtime_construction_receipt(
            repo_root_fd=repo_root_fd,
            runtime_root_fd=materialized.root_fd,
            python_request=python_request.encode("utf-8"),
            interpreter_relative_name=interpreter_relative_name,
            python_description=description,
            distribution=distribution,
            dependency_wheelhouse_manifest_digest=wheelhouse_digest,
        )
        materialized.assert_distribution_unchanged()
        materialized.assert_path_binding()
        _assert_directory_path_binding(
            repo_root,
            repo_root_fd,
            code="sealed_release_runtime_closure_mismatch",
        )
    finally:
        if interpreter_fd >= 0:
            os.close(interpreter_fd)
        if materialized is not None:
            materialized.close()


def _load_authorized_runtime_distribution(
    environ: Mapping[str, str],
) -> VerifiedRuntimeDistribution:
    root_value = environ.get(RUNTIME_PROOF_ROOT_ENV)
    expected_digest = environ.get(RUNTIME_PROOF_DIGEST_ENV)
    if not root_value or not expected_digest:
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "sealed release requires an exact authorized runtime producer proof",
        )
    if not _is_sha256(expected_digest):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "authorized runtime producer proof digest is invalid",
        )
    proof_root = Path(root_value).expanduser()
    if not proof_root.is_absolute():
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "authorized runtime producer proof root is not absolute",
        )
    try:
        proof_root_fd = _open_readonly_absolute_directory(
            proof_root,
            code="sealed_release_runtime_acquisition_mismatch",
        )
    except OSError as exc:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "authorized runtime producer proof root is unavailable",
        ) from exc
    try:
        payload = _read_regular_member_at(
            proof_root_fd,
            _RUNTIME_PROOF_FILENAME,
            code="sealed_release_runtime_acquisition_mismatch",
            require_readonly=True,
        )
        document = _strict_json_document(
            payload,
            code="sealed_release_runtime_acquisition_mismatch",
        )
        if _canonical_digest(document) != expected_digest:
            raise _invalid(
                "sealed_release_runtime_acquisition_mismatch",
                "authorized runtime producer proof digest differs",
            )
        return _verify_authorized_runtime_distribution(
            document,
            None,
            expected_profile=_host_platform_profile(),
            proof_digest=expected_digest,
            proof_root_fd=proof_root_fd,
        )
    finally:
        os.close(proof_root_fd)


def _verify_authorized_runtime_distribution(
    document: Mapping[str, object],
    proof_root: Path | None,
    *,
    expected_profile: Mapping[str, str],
    proof_digest: str | None = None,
    proof_root_fd: int | None = None,
) -> VerifiedRuntimeDistribution:
    if proof_root is not None:
        if proof_root_fd is not None:
            raise TypeError("proof root path and descriptor are mutually exclusive")
        owned_root_fd = _open_readonly_absolute_directory(
            proof_root,
            code="sealed_release_runtime_acquisition_mismatch",
        )
        try:
            return _verify_authorized_runtime_distribution(
                document,
                None,
                expected_profile=expected_profile,
                proof_digest=proof_digest,
                proof_root_fd=owned_root_fd,
            )
        finally:
            os.close(owned_root_fd)
    _exact_keys(
        document,
        {
            "schema_version",
            "distribution_enrollment",
            "source_manifest",
            "build_recipe_digest",
            "platform_profile",
            "reproducibility_session",
            "session_authentication",
            "build_attestations",
            "reproducibility_proof",
            "distribution_acquisition_receipt",
            "normalized_archive",
            "distribution_prefix_tree",
            "original_link_search_transcript",
            "native_link_output_manifest",
            "native_archive_output_manifest",
            "process_traces",
        },
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if document.get("schema_version") != RUNTIME_DISTRIBUTION_BUNDLE_SCHEMA:
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime distribution proof has the wrong schema",
        )
    enrollment = _strict_mapping(
        document.get("distribution_enrollment"),
        code="sealed_release_runtime_recipe_mismatch",
    )
    if enrollment != RUNTIME_DISTRIBUTION_ENROLLMENT:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime distribution enrollment differs from the approved recipe",
        )
    if document.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime build recipe digest differs from the approved recipe",
        )
    profile = _strict_string_mapping(
        document.get("platform_profile"),
        code="sealed_release_runtime_distribution_unsupported",
    )
    enrolled_profile = _RUNTIME_PLATFORM_PROFILES.get(
        (
            profile.get("os", ""),
            profile.get("architecture", ""),
            profile.get("libc", ""),
            profile.get("python_abi", ""),
        )
    )
    if enrolled_profile is None or profile != enrolled_profile:
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "runtime distribution platform profile is not enrolled",
        )
    if profile != dict(expected_profile):
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "runtime distribution does not match the active producer platform",
        )

    source_manifest = _strict_mapping(
        document.get("source_manifest"),
        code="sealed_release_runtime_acquisition_mismatch",
    )
    _verify_source_manifest(source_manifest)
    source_manifest_digest = _canonical_digest(source_manifest)
    profile_digest = _canonical_digest(profile)

    archive = _strict_raw_blob(
        document.get("normalized_archive"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if proof_root_fd is not None:
        _verify_regular_file_raw_blob_at(
            proof_root_fd,
            _RUNTIME_ARCHIVE_FILENAME,
            archive,
            code="sealed_release_runtime_reproducibility_mismatch",
        )
    distribution_tree = _strict_mapping(
        document.get("distribution_prefix_tree"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    _exact_keys(
        distribution_tree,
        {"schema_version", "entries"},
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if (
        distribution_tree.get("schema_version")
        != RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime distribution prefix tree has the wrong schema",
        )
    entries_value = distribution_tree.get("entries")
    if isinstance(entries_value, (str, bytes)) or not isinstance(
        entries_value, Sequence
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime distribution prefix entries are invalid",
        )
    distribution_members = tuple(
        _normalize_prefix_member(
            value,
            code="sealed_release_runtime_reproducibility_mismatch",
        )
        for value in entries_value
    )
    names = [str(item["relative_name"]) for item in distribution_members]
    if names != sorted(set(names), key=lambda value: value.encode("utf-8")):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime distribution prefix members are not one sorted complete tree",
        )
    distribution_tree_digest = _canonical_digest(distribution_tree)

    transcript = _strict_mapping(
        document.get("original_link_search_transcript"),
        code="sealed_release_runtime_recipe_mismatch",
    )
    _verify_original_link_transcript(transcript, distribution_members)
    transcript_digest = _canonical_digest(transcript)
    link_manifest = _strict_mapping(
        document.get("native_link_output_manifest"),
        code="sealed_release_runtime_recipe_mismatch",
    )
    archive_manifest = _strict_mapping(
        document.get("native_archive_output_manifest"),
        code="sealed_release_runtime_recipe_mismatch",
    )
    _verify_native_output_manifest(link_manifest, distribution_members)
    _verify_native_output_manifest(archive_manifest, distribution_members)
    if link_manifest != archive_manifest:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime native output bytes changed after original link",
        )
    native_manifest_digest = _canonical_digest(link_manifest)
    process_traces = document.get("process_traces")
    if isinstance(process_traces, (str, bytes)) or not isinstance(
        process_traces, Sequence
    ):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime process traces are invalid",
        )
    trace_by_digest: dict[str, Mapping[str, object]] = {}
    for value in process_traces:
        trace = _strict_mapping(
            value,
            code="sealed_release_runtime_recipe_mismatch",
        )
        _verify_process_trace(trace, platform_profile=profile)
        digest = _canonical_digest(trace)
        if digest in trace_by_digest:
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime build traces are not independently identified",
            )
        trace_by_digest[digest] = trace

    session = _strict_mapping(
        document.get("reproducibility_session"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    session_digest = _canonical_digest(session)
    _verify_reproducibility_session(
        session,
        source_manifest_digest=source_manifest_digest,
        platform_profile_digest=profile_digest,
    )
    _verify_session_authentication(
        session,
        _strict_mapping(
            document.get("session_authentication"),
            code="sealed_release_runtime_reproducibility_mismatch",
        ),
    )
    attestations_value = document.get("build_attestations")
    if (
        isinstance(attestations_value, (str, bytes))
        or not isinstance(attestations_value, Sequence)
        or len(attestations_value) != 2
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime proof requires exactly two build attestations",
        )
    attestation_records: list[Mapping[str, object]] = []
    attestation_digests: list[str] = []
    for value in attestations_value:
        envelope = _strict_mapping(
            value,
            code="sealed_release_runtime_reproducibility_mismatch",
        )
        _exact_keys(
            envelope,
            {"record", "authentication"},
            code="sealed_release_runtime_reproducibility_mismatch",
        )
        record = _strict_mapping(
            envelope.get("record"),
            code="sealed_release_runtime_reproducibility_mismatch",
        )
        _verify_build_attestation(
            record,
            _strict_mapping(
                envelope.get("authentication"),
                code="sealed_release_runtime_reproducibility_mismatch",
            ),
            session_digest=session_digest,
            source_manifest_digest=source_manifest_digest,
            platform_profile_digest=profile_digest,
            archive=archive,
            distribution_tree_digest=distribution_tree_digest,
            transcript_digest=transcript_digest,
            native_manifest_digest=native_manifest_digest,
            trace_by_digest=trace_by_digest,
        )
        attestation_records.append(record)
        attestation_digests.append(_canonical_digest(record))
    _verify_build_independence(session, attestation_records)

    proof = _strict_mapping(
        document.get("reproducibility_proof"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    _verify_reproducibility_proof(
        proof,
        session_digest=session_digest,
        source_manifest_digest=source_manifest_digest,
        platform_profile_digest=profile_digest,
        attestation_digests=attestation_digests,
        archive=archive,
        distribution_tree_digest=distribution_tree_digest,
        transcript_digest=transcript_digest,
        native_manifest_digest=native_manifest_digest,
    )
    reproducibility_proof_digest = _canonical_digest(proof)
    acquisition = _strict_mapping(
        document.get("distribution_acquisition_receipt"),
        code="sealed_release_runtime_acquisition_mismatch",
    )
    _verify_distribution_acquisition(
        acquisition,
        reproducibility_proof_digest=reproducibility_proof_digest,
        platform_profile_digest=profile_digest,
        archive=archive,
        distribution_tree_digest=distribution_tree_digest,
    )
    prefix_fd = -1
    try:
        if proof_root_fd is not None:
            prefix_fd = _open_directory_member_at(
                proof_root_fd,
                _RUNTIME_PREFIX_RELATIVE,
                code="sealed_release_runtime_reproducibility_mismatch",
                require_readonly=True,
            )
            observed_members = tuple(_runtime_prefix_members_at(prefix_fd))
            if observed_members != distribution_members:
                raise _invalid(
                    "sealed_release_runtime_reproducibility_mismatch",
                    "runtime distribution prefix differs from the proved complete tree",
                )
        return VerifiedRuntimeDistribution(
            document,
            proof_digest or _canonical_digest(document),
            distribution_members,
            profile,
            source_manifest_digest,
            reproducibility_proof_digest,
            _canonical_digest(acquisition),
            archive,
            prefix_fd,
        )
    except BaseException:
        if prefix_fd >= 0:
            os.close(prefix_fd)
        raise


def _verify_source_manifest(manifest: Mapping[str, object]) -> None:
    _exact_keys(
        manifest,
        {
            "schema_version",
            "upstream",
            "cpython",
            "driver_tools",
            "builder_base_manifest_digest",
            "builder_snapshot_release_roots",
            "dependency_lock_raw_blob",
            "downloads_manifest_raw_blob",
            "sorted_acquired_inputs",
            "package_chain_status",
        },
        code="sealed_release_runtime_acquisition_mismatch",
    )
    if manifest.get("schema_version") != RUNTIME_SOURCE_MANIFEST_SCHEMA:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime source manifest has the wrong schema",
        )
    upstream = _strict_mapping(
        manifest.get("upstream"), code="sealed_release_runtime_acquisition_mismatch"
    )
    expected_upstream = {
        "repository": "https://github.com/astral-sh/python-build-standalone",
        "release_tag": "20260814",
        "commit": "38d35dcf0e212ca02eed8ebc11d0c92906387d56",
        "git_tree": "9d623a68052ad503c8d72085fb1823c206cf329a",
        "source_archive_raw_blob": (
            "sha256:66c2ba43e778ee55b3a2f4c925525e3c4352b13dbd77b3bc479478f7a7dcd7c3"
        ),
    }
    if upstream != expected_upstream:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime upstream source identity differs",
        )
    cpython = _strict_mapping(
        manifest.get("cpython"), code="sealed_release_runtime_acquisition_mismatch"
    )
    if cpython != {
        "version": "3.12.14",
        "source_raw_blob": (
            "sha256:5c8462af5790baf43a321a1559dbe0db06d1be4300fb85fb53c40060668e548a"
        ),
    }:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime CPython source identity differs",
        )
    expected_driver_digests = {
        "uv": "sha256:b65f23a420c4acc96427efb30e5ed9bc0f7e25d2d712000f6ede77c1a0de5f46",
        "driver_python": "sha256:9b1522a474140400cc6f64d475ad49c045f8034696e4df09c3f9792dd41bdf2c",
        "gnu_make": "sha256:0a5e3a88ba6c2b6a8dbb785a7bb3e3b4ee292992d41bdbf905211bb5e7831f3c",
        "docker_buildx": "sha256:48af8a397ebd60178778bf63611dbcebe5f5e7a9be90eb9147b24b9587455778",
    }
    driver_values = manifest.get("driver_tools")
    if isinstance(driver_values, (str, bytes)) or not isinstance(
        driver_values, Sequence
    ):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime driver tool manifest is invalid",
        )
    observed_drivers: dict[str, str] = {}
    for value in driver_values:
        item = _strict_mapping(
            value, code="sealed_release_runtime_acquisition_mismatch"
        )
        logical_name = item.get("logical_name")
        executable = item.get("executable_raw_blob")
        if not isinstance(logical_name, str) or not _is_sha256(executable):
            raise _invalid(
                "sealed_release_runtime_acquisition_mismatch",
                "runtime driver tool identity is invalid",
            )
        observed_drivers[logical_name] = str(executable)
    if observed_drivers != expected_driver_digests:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime driver executable bytes differ",
        )
    if manifest.get("builder_base_manifest_digest") != (
        "sha256:32ad5050caffb2c7e969dac873bce2c370015c2256ff984b70c1c08b3a2816a0"
    ):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime builder base manifest differs",
        )
    roots = manifest.get("builder_snapshot_release_roots")
    expected_roots = sorted(
        {
            "sha256:ff4cbf89abb58afa73b08f1b21ebaeed9a513e5d848c8c146fdddfa6ca39429a",
            "sha256:63bc2e5739f84e2a4a967cc76d5e04ff4a3020c25934fa21d81f8202f381edf7",
            "sha256:cc346ba2752ef276d8f60266ade45a19075e16fb54e3c4ceb9e280b3077675d6",
        }
    )
    if not isinstance(roots, list) or sorted(roots) != expected_roots:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime Debian Release roots differ",
        )
    if manifest.get("dependency_lock_raw_blob") != (
        "sha256:4ca49fe030b09ad2a70fef0149a45c8d052e5d79043fbb9b64cf6979bacf248b"
    ) or manifest.get("downloads_manifest_raw_blob") != (
        "sha256:b739831dc701afa9b3f2de5f39c7ed82dd79859c3ff2c04c521d71ca279bf97a"
    ):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime lock or downloads manifest differs",
        )
    if manifest.get("package_chain_status") != (
        "every-index-rooted-and-every-package-indexed"
    ):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime package acquisition is not digest-rooted",
        )
    acquired = manifest.get("sorted_acquired_inputs")
    if not isinstance(acquired, list) or not acquired:
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime acquired-input manifest is empty",
        )
    names: list[str] = []
    for value in acquired:
        item = _strict_mapping(
            value, code="sealed_release_runtime_acquisition_mismatch"
        )
        logical_name = item.get("logical_name")
        size = item.get("size")
        if (
            not isinstance(logical_name, str)
            or not logical_name
            or not _is_sha256(item.get("source_url_digest"))
            or not _is_sha256(item.get("raw_blob"))
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or (
                item.get("parent_index_raw_blob") is not None
                and not _is_sha256(item.get("parent_index_raw_blob"))
            )
        ):
            raise _invalid(
                "sealed_release_runtime_acquisition_mismatch",
                "runtime acquired-input identity is invalid",
            )
        names.append(logical_name)
    if names != sorted(set(names), key=lambda value: value.encode("utf-8")):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime acquired inputs are not one closed sorted set",
        )


def _verify_original_link_transcript(
    transcript: Mapping[str, object],
    members: Sequence[Mapping[str, object]],
) -> None:
    _exact_keys(
        transcript,
        {"schema_version", "entries"},
        code="sealed_release_runtime_recipe_mismatch",
    )
    if transcript.get("schema_version") != RUNTIME_ORIGINAL_LINK_TRANSCRIPT_SCHEMA:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime original-link transcript has the wrong schema",
        )
    values = transcript.get("entries")
    if not isinstance(values, list):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime original-link transcript is invalid",
        )
    member_names = {str(item["relative_name"]) for item in members}
    expected_by_name: dict[str, str] = {}
    for member_name in member_names:
        basename = PurePosixPath(member_name).name
        if basename.startswith("_tkinter"):
            expected_by_name[member_name] = "$ORIGIN/../.."
        elif basename.startswith(("libtcl", "libtk")):
            expected_by_name[member_name] = "$ORIGIN:$ORIGIN/.."
    observed: list[tuple[str, str]] = []
    for value in values:
        item = _strict_mapping(value, code="sealed_release_runtime_recipe_mismatch")
        _exact_keys(
            item,
            {"relative_name", "literal_search"},
            code="sealed_release_runtime_recipe_mismatch",
        )
        name = item.get("relative_name")
        search = item.get("literal_search")
        if not isinstance(name, str) or name not in member_names:
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "runtime original-link transcript names an unproved member",
            )
        basename = PurePosixPath(name).name
        if basename.startswith("_tkinter"):
            expected = "$ORIGIN/../.."
        elif basename.startswith("libtcl"):
            expected = "$ORIGIN:$ORIGIN/.."
        elif basename.startswith("libtk"):
            expected = "$ORIGIN:$ORIGIN/.."
        else:
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "runtime original-link transcript contains an unrelated object",
            )
        if search != expected:
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "runtime Tcl/Tk loader search is not the enrolled relative value",
            )
        observed.append((name, str(search)))
    if (
        not expected_by_name
        or observed != sorted(expected_by_name.items())
        or observed != sorted(set(observed))
    ):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime original-link transcript is incomplete or unordered",
        )


def _verify_native_output_manifest(
    manifest: Mapping[str, object],
    members: Sequence[Mapping[str, object]],
) -> None:
    _exact_keys(
        manifest,
        {"schema_version", "entries"},
        code="sealed_release_runtime_recipe_mismatch",
    )
    if manifest.get("schema_version") != RUNTIME_NATIVE_OUTPUT_MANIFEST_SCHEMA:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime native output manifest has the wrong schema",
        )
    member_by_name = {str(item["relative_name"]): item for item in members}
    values = manifest.get("entries")
    if not isinstance(values, list) or not values:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime native output manifest is empty",
        )
    names: list[str] = []
    for value in values:
        item = _strict_mapping(value, code="sealed_release_runtime_recipe_mismatch")
        _exact_keys(
            item,
            {"relative_name", "raw_blob"},
            code="sealed_release_runtime_recipe_mismatch",
        )
        name = item.get("relative_name")
        member = member_by_name.get(str(name))
        if (
            not isinstance(name, str)
            or member is None
            or member.get("kind") != "file"
            or item.get("raw_blob") != member.get("raw_blob")
        ):
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "runtime native output differs from the complete prefix",
            )
        names.append(name)
    expected_names = sorted(
        (
            name
            for name, member in member_by_name.items()
            if member.get("kind") == "file"
            and (
                member.get("mode") == 0o555
                or name.endswith((".a", ".so"))
                or ".so." in PurePosixPath(name).name
            )
        ),
        key=lambda value: value.encode("utf-8"),
    )
    if names != expected_names or names != sorted(
        set(names), key=lambda value: value.encode("utf-8")
    ):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime native output manifest does not cover every native output",
        )


def _verify_process_trace(
    trace: Mapping[str, object],
    *,
    platform_profile: Mapping[str, str],
) -> None:
    _exact_keys(
        trace,
        {"schema_version", "commands", "environment", "network"},
        code="sealed_release_runtime_recipe_mismatch",
    )
    if (
        trace.get("schema_version") != RUNTIME_PROCESS_TRACE_SCHEMA
        or trace.get("network") != "disabled"
    ):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime build trace did not use the enrolled offline profile",
        )
    environment = _strict_mapping(
        trace.get("environment"), code="sealed_release_runtime_recipe_mismatch"
    )
    if environment != _RUNTIME_BUILD_ENVIRONMENT:
        raise _invalid(
            "sealed_release_runtime_non_relocatable",
            "runtime build trace differs from the enrolled fixed environment",
        )
    target = platform_profile.get("target_triple")
    expected_commands = [
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
                target,
            ],
        }
    ]
    commands = trace.get("commands")
    if commands != expected_commands:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime build trace differs from the enrolled command",
        )
    for value in expected_commands:
        item = _strict_mapping(value, code="sealed_release_runtime_recipe_mismatch")
        _exact_keys(
            item,
            {"executable", "argv"},
            code="sealed_release_runtime_recipe_mismatch",
        )
        executable = item.get("executable")
        argv = item.get("argv")
        if (
            not isinstance(executable, str)
            or not executable
            or not isinstance(argv, list)
            or any(not isinstance(argument, str) for argument in argv)
            or PurePosixPath(executable).name in _RUNTIME_FORBIDDEN_MUTATION_TOOLS
        ):
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "runtime build trace executed an unapproved mutation tool",
            )


def _verify_reproducibility_session(
    session: Mapping[str, object],
    *,
    source_manifest_digest: str,
    platform_profile_digest: str,
) -> None:
    _exact_keys(
        session,
        {
            "schema_version",
            "release_producer_attestor_identity_digest",
            "source_manifest_digest",
            "build_recipe_digest",
            "platform_profile_digest",
            "builders",
            "status",
        },
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if (
        session.get("schema_version") != RUNTIME_REPRODUCIBILITY_SESSION_SCHEMA
        or session.get("source_manifest_digest") != source_manifest_digest
        or session.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST
        or session.get("platform_profile_digest") != platform_profile_digest
        or session.get("status") != "two-pairwise-distinct-build-authorities-enrolled"
        or not _is_sha256(session.get("release_producer_attestor_identity_digest"))
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime reproducibility session identity differs",
        )
    builders = session.get("builders")
    if not isinstance(builders, list) or len(builders) != 2:
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime reproducibility session does not enroll two builders",
        )
    roles = []
    for value in builders:
        item = _strict_mapping(
            value, code="sealed_release_runtime_reproducibility_mismatch"
        )
        _exact_keys(
            item,
            {
                "builder_role_id",
                "authenticated_executor_identity_digest",
                "ephemeral_attestor_public_key_digest",
                "clean_builder_root_creation_receipt_digest",
                "acquisition_worker_identity_digest",
            },
            code="sealed_release_runtime_reproducibility_mismatch",
        )
        if any(
            not _is_sha256(item.get(field))
            for field in (
                "authenticated_executor_identity_digest",
                "ephemeral_attestor_public_key_digest",
                "clean_builder_root_creation_receipt_digest",
                "acquisition_worker_identity_digest",
            )
        ):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime reproducibility session builder identity is invalid",
            )
        roles.append(item.get("builder_role_id"))
    if roles != list(_RUNTIME_BUILD_ROLES):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime reproducibility session roles differ",
        )


def _verify_session_authentication(
    session: Mapping[str, object],
    authentication: Mapping[str, object],
) -> None:
    _exact_keys(
        authentication,
        {"public_key_base64", "signature_base64"},
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    public_key = _decode_base64(
        authentication.get("public_key_base64"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    signature = _decode_base64(
        authentication.get("signature_base64"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if _raw_blob(public_key)["digest"] != session.get(
        "release_producer_attestor_identity_digest"
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime reproducibility session signer differs",
        )
    _verify_ed25519(public_key, signature, _canonical_json(session))


def _verify_build_attestation(
    record: Mapping[str, object],
    authentication: Mapping[str, object],
    *,
    session_digest: str,
    source_manifest_digest: str,
    platform_profile_digest: str,
    archive: Mapping[str, object],
    distribution_tree_digest: str,
    transcript_digest: str,
    native_manifest_digest: str,
    trace_by_digest: Mapping[str, Mapping[str, object]],
) -> None:
    expected_keys = {
        "schema_version",
        "reproducibility_session_digest",
        "source_manifest_digest",
        "build_recipe_digest",
        "platform_profile_digest",
        "builder_role_id",
        "authenticated_executor_identity_digest",
        "ephemeral_attestor_public_key_digest",
        "clean_builder_root_creation_receipt_digest",
        "acquisition_worker_identity_digest",
        "builder_image_manifest_digest",
        "builder_layer_digests",
        "docker_executor_attestation_digest",
        "acquisition_receipt_digest",
        "clean_builder_nonce_raw_blob",
        "normalized_archive",
        "distribution_prefix_tree_digest",
        "original_link_search_transcript_digest",
        "native_link_output_manifest_digest",
        "native_archive_output_manifest_digest",
        "ordered_process_execution_trace_digest",
        "attestation_signature_raw_blob",
    }
    _exact_keys(
        record,
        expected_keys,
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if (
        record.get("schema_version") != RUNTIME_BUILD_ATTESTATION_SCHEMA
        or record.get("reproducibility_session_digest") != session_digest
        or record.get("source_manifest_digest") != source_manifest_digest
        or record.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST
        or record.get("platform_profile_digest") != platform_profile_digest
        or record.get("normalized_archive") != archive
        or record.get("distribution_prefix_tree_digest") != distribution_tree_digest
        or record.get("original_link_search_transcript_digest") != transcript_digest
        or record.get("native_link_output_manifest_digest") != native_manifest_digest
        or record.get("native_archive_output_manifest_digest") != native_manifest_digest
        or record.get("ordered_process_execution_trace_digest") not in trace_by_digest
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime build attestation differs from the proved outputs",
        )
    for field in (
        "authenticated_executor_identity_digest",
        "ephemeral_attestor_public_key_digest",
        "clean_builder_root_creation_receipt_digest",
        "acquisition_worker_identity_digest",
        "builder_image_manifest_digest",
        "docker_executor_attestation_digest",
        "acquisition_receipt_digest",
        "clean_builder_nonce_raw_blob",
    ):
        if not _is_sha256(record.get(field)):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime build attestation identity is invalid",
            )
    layers = record.get("builder_layer_digests")
    if (
        not isinstance(layers, list)
        or not layers
        or layers != sorted(set(layers))
        or any(not _is_sha256(value) for value in layers)
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime builder layer identity is invalid",
        )
    _exact_keys(
        authentication,
        {"public_key_base64", "signature_base64"},
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    public_key = _decode_base64(
        authentication.get("public_key_base64"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    signature = _decode_base64(
        authentication.get("signature_base64"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if _raw_blob(public_key)["digest"] != record.get(
        "ephemeral_attestor_public_key_digest"
    ) or _raw_blob(signature) != record.get("attestation_signature_raw_blob"):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime build attestation authentication differs",
        )
    unsigned = dict(record)
    unsigned.pop("attestation_signature_raw_blob")
    _verify_ed25519(public_key, signature, _canonical_json(unsigned))


def _verify_build_independence(
    session: Mapping[str, object],
    attestations: Sequence[Mapping[str, object]],
) -> None:
    roles = [str(item.get("builder_role_id")) for item in attestations]
    if sorted(roles) != sorted(_RUNTIME_BUILD_ROLES):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime build roles are incomplete",
        )
    for field in _RUNTIME_DISTINCT_BUILD_FIELDS:
        values = [item.get(field) for item in attestations]
        if len(set(str(value) for value in values)) != 2:
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime build independence identity was reused",
            )
    builders = {
        str(item["builder_role_id"]): item
        for item in session.get("builders", [])  # type: ignore[union-attr]
    }
    for attestation in attestations:
        enrolled = builders.get(str(attestation.get("builder_role_id")))
        if enrolled is None or any(
            enrolled.get(field) != attestation.get(field)
            for field in (
                "authenticated_executor_identity_digest",
                "ephemeral_attestor_public_key_digest",
                "clean_builder_root_creation_receipt_digest",
                "acquisition_worker_identity_digest",
            )
        ):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime build attestation is outside the signed session",
            )


def _verify_reproducibility_proof(
    proof: Mapping[str, object],
    *,
    session_digest: str,
    source_manifest_digest: str,
    platform_profile_digest: str,
    attestation_digests: Sequence[str],
    archive: Mapping[str, object],
    distribution_tree_digest: str,
    transcript_digest: str,
    native_manifest_digest: str,
) -> None:
    _exact_keys(
        proof,
        {
            "schema_version",
            "reproducibility_session_digest",
            "source_manifest_digest",
            "build_recipe_digest",
            "platform_profile_digest",
            "build_attestation_digests",
            "pairwise_distinct_fields",
            "normalized_archive",
            "distribution_prefix_tree_digest",
            "original_link_search_transcript_digest",
            "native_output_manifest_digest",
            "status",
        },
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if (
        proof.get("schema_version") != RUNTIME_REPRODUCIBILITY_PROOF_SCHEMA
        or proof.get("reproducibility_session_digest") != session_digest
        or proof.get("source_manifest_digest") != source_manifest_digest
        or proof.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST
        or proof.get("platform_profile_digest") != platform_profile_digest
        or proof.get("build_attestation_digests") != sorted(attestation_digests)
        or proof.get("pairwise_distinct_fields") != list(_RUNTIME_DISTINCT_BUILD_FIELDS)
        or proof.get("normalized_archive") != archive
        or proof.get("distribution_prefix_tree_digest") != distribution_tree_digest
        or proof.get("original_link_search_transcript_digest") != transcript_digest
        or proof.get("native_output_manifest_digest") != native_manifest_digest
        or proof.get("status") != "two-authenticated-independent-builds-identical"
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime reproducibility proof differs",
        )


def _verify_distribution_acquisition(
    receipt: Mapping[str, object],
    *,
    reproducibility_proof_digest: str,
    platform_profile_digest: str,
    archive: Mapping[str, object],
    distribution_tree_digest: str,
) -> None:
    _exact_keys(
        receipt,
        {
            "schema_version",
            "distribution_enrollment_digest",
            "build_recipe_digest",
            "reproducibility_proof_digest",
            "platform_profile_digest",
            "normalized_archive",
            "distribution_prefix_tree_digest",
            "ordered_acquisition_transcript_digest",
        },
        code="sealed_release_runtime_acquisition_mismatch",
    )
    if (
        receipt.get("schema_version") != RUNTIME_DISTRIBUTION_ACQUISITION_SCHEMA
        or receipt.get("distribution_enrollment_digest")
        != _canonical_digest(RUNTIME_DISTRIBUTION_ENROLLMENT)
        or receipt.get("build_recipe_digest") != RUNTIME_BUILD_RECIPE_DIGEST
        or receipt.get("reproducibility_proof_digest") != reproducibility_proof_digest
        or receipt.get("platform_profile_digest") != platform_profile_digest
        or receipt.get("normalized_archive") != archive
        or receipt.get("distribution_prefix_tree_digest") != distribution_tree_digest
        or not _is_sha256(receipt.get("ordered_acquisition_transcript_digest"))
    ):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "runtime distribution acquisition receipt differs",
        )


def _host_platform_profile() -> dict[str, str]:
    os_name = platform.system().lower()
    architecture = platform.machine().lower()
    if architecture in {"amd64", "x86-64"}:
        architecture = "x86_64"
    elif architecture == "arm64":
        architecture = "aarch64"
    libc_name, libc_version = platform.libc_ver()
    libc = "glibc" if libc_name.lower() in {"glibc", "libc"} else libc_name.lower()
    profile = _RUNTIME_PLATFORM_PROFILES.get((os_name, architecture, libc, "cp312"))
    if profile is None or not _version_at_least(libc_version, "2.17"):
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "active release producer platform has no enrolled runtime distribution",
        )
    return dict(profile)


def _selected_platform_profile(description: Mapping[str, object]) -> dict[str, str]:
    major = description.get("major")
    minor = description.get("minor")
    if (
        not isinstance(major, int)
        or isinstance(major, bool)
        or not isinstance(minor, int)
        or isinstance(minor, bool)
    ):
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "constructed interpreter version is invalid",
        )
    if (major, minor) != (3, 12) or description.get("python_abi") != "cp312":
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "constructed interpreter ABI is not enrolled",
        )
    identity = _platform_identity(description)
    architecture = identity["architecture"]
    if architecture == "arm64":
        architecture = "aarch64"
    libc = identity["libc"]
    if libc == "libc":
        libc = "glibc"
    profile = _RUNTIME_PLATFORM_PROFILES.get(
        (identity["os"], architecture, libc, "cp312")
    )
    if profile is None:
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "constructed interpreter platform is not enrolled",
        )
    return dict(profile)


def _version_at_least(observed: str, minimum: str) -> bool:
    try:
        observed_parts = tuple(int(part) for part in observed.split("."))
        minimum_parts = tuple(int(part) for part in minimum.split("."))
    except ValueError:
        return False
    width = max(len(observed_parts), len(minimum_parts))
    return observed_parts + (0,) * (width - len(observed_parts)) >= minimum_parts + (
        0,
    ) * (width - len(minimum_parts))


def _assert_distribution_prefix_unchanged(
    runtime_root_fd: int,
    expected_members: Sequence[Mapping[str, object]],
) -> None:
    observed = {
        str(item["relative_name"]): item
        for item in _runtime_prefix_members_at(runtime_root_fd, require_readonly=False)
    }
    for expected in expected_members:
        name = str(expected["relative_name"])
        if observed.get(name) != expected:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "release construction changed or pruned a distribution prefix member",
            )


def _normalize_prefix_member(
    value: object,
    *,
    code: str,
) -> Mapping[str, object]:
    item = _strict_mapping(value, code=code)
    kind = item.get("kind")
    if kind == "file":
        _exact_keys(
            item,
            {"relative_name", "kind", "mode", "size", "raw_blob"},
            code=code,
        )
        name = _safe_runtime_relative_name(item.get("relative_name"), code=code)
        mode = item.get("mode")
        size = item.get("size")
        raw = _strict_raw_blob(item.get("raw_blob"), code=code)
        if (
            mode not in {0o444, 0o555}
            or isinstance(mode, bool)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or raw.get("size") != size
        ):
            raise _invalid(code, "runtime prefix file identity is invalid")
        return {
            "relative_name": name,
            "kind": "file",
            "mode": mode,
            "size": size,
            "raw_blob": raw,
        }
    if kind == "directory":
        _exact_keys(
            item,
            {"relative_name", "kind", "mode", "size", "raw_blob"},
            code=code,
        )
        name = _safe_runtime_relative_name(item.get("relative_name"), code=code)
        if (
            item.get("mode") != 0o555
            or item.get("size") != 0
            or item.get("raw_blob") is not None
        ):
            raise _invalid(code, "runtime prefix directory identity is invalid")
        return {
            "relative_name": name,
            "kind": "directory",
            "mode": 0o555,
            "size": 0,
            "raw_blob": None,
        }
    if kind == "symlink":
        _exact_keys(
            item,
            {"relative_name", "kind", "mode", "normalized_relative_target"},
            code=code,
        )
        name = _safe_runtime_relative_name(item.get("relative_name"), code=code)
        target = item.get("normalized_relative_target")
        if not isinstance(target, str) or not target or "\\" in target:
            raise _invalid(code, "runtime prefix symlink target is invalid")
        resolved = posixpath.normpath(posixpath.join(posixpath.dirname(name), target))
        if (
            target.startswith("/")
            or resolved == ".."
            or resolved.startswith("../")
            or target != posixpath.normpath(target)
            or item.get("mode") != 0o777
        ):
            raise _invalid(code, "runtime prefix symlink escapes its authority")
        return {
            "relative_name": name,
            "kind": "symlink",
            "mode": 0o777,
            "normalized_relative_target": target,
        }
    raise _invalid(code, "runtime prefix member kind is invalid")


def _strict_json_document(payload: bytes, *, code: str) -> Mapping[str, object]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _invalid(code, "runtime producer proof is not strict JSON") from exc
    document = _strict_mapping(value, code=code)
    if payload != _canonical_json(document):
        raise _invalid(code, "runtime producer proof is not canonical JSON")
    return document


def _strict_mapping(value: object, *, code: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise _invalid(code, "runtime proof value is not one closed object")
    return value


def _strict_string_mapping(value: object, *, code: str) -> dict[str, str]:
    item = _strict_mapping(value, code=code)
    if any(
        not isinstance(key, str) or not isinstance(item_value, str)
        for key, item_value in item.items()
    ):
        raise _invalid(code, "runtime proof string mapping is invalid")
    return {str(key): str(item_value) for key, item_value in item.items()}


def _exact_keys(value: Mapping[str, object], expected: set[str], *, code: str) -> None:
    if set(value) != expected:
        raise _invalid(code, "runtime proof object is not closed")


def _strict_raw_blob(value: object, *, code: str) -> Mapping[str, object]:
    item = _strict_mapping(value, code=code)
    size = item.get("size")
    if (
        set(item) != {"digest_domain", "digest", "size"}
        or item.get("digest_domain") != "raw-bytes/v1"
        or not _is_sha256(item.get("digest"))
        or not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
    ):
        raise _invalid(code, "runtime proof raw-blob reference is invalid")
    return dict(item)


def _runtime_stat_token(
    value: os.stat_result,
) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_nlink,
    )


def _retained_tree_state_at(
    root_fd: int, *, code: str
) -> tuple[tuple[int, int, int, int, int, int, int], tuple[tuple[object, ...], ...]]:
    """Bind one retained tree's bytes, entries, and change-detecting metadata."""

    root_before = os.fstat(root_fd)
    members = _runtime_prefix_members_at(root_fd, require_readonly=False)
    entries: list[tuple[object, ...]] = []
    for item in members:
        relative_name = _safe_runtime_relative_name(
            item.get("relative_name"), code=code
        )
        parent_fd, leaf = _open_runtime_parent_at(root_fd, relative_name, code=code)
        try:
            observed = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            raise _invalid(code, "retained tree member became unavailable") from exc
        finally:
            os.close(parent_fd)
        entries.append(
            (
                relative_name,
                item.get("kind"),
                _runtime_stat_token(observed),
                _canonical_json(item),
            )
        )
    root_after = os.fstat(root_fd)
    if _runtime_stat_token(root_before) != _runtime_stat_token(root_after):
        raise _invalid(code, "retained tree changed while its state was captured")
    return _runtime_stat_token(root_after), tuple(entries)


def _protected_runtime_directory_state_at(
    root_fd: int,
    members: Sequence[Mapping[str, object]],
    *,
    mutable_directories: set[str],
) -> tuple[tuple[str, tuple[int, int, int, int, int, int, int]], ...]:
    """Retain all directory identities except uv's two admitted write roots."""

    code = "sealed_release_runtime_closure_mismatch"
    mutable = {
        _safe_runtime_relative_name(value, code=code) for value in mutable_directories
    }
    names = {"."}
    names.update(
        _safe_runtime_relative_name(item.get("relative_name"), code=code)
        for item in members
        if item.get("kind") == "directory"
        and not any(
            str(item.get("relative_name")) == value
            or str(item.get("relative_name")).startswith(f"{value}/")
            for value in mutable
        )
    )
    state: list[tuple[str, tuple[int, int, int, int, int, int, int]]] = []
    for name in sorted(names, key=lambda value: value.encode("utf-8")):
        directory_fd = (
            os.dup(root_fd)
            if name == "."
            else _open_directory_member_at(
                root_fd, name, code=code, require_readonly=False
            )
        )
        try:
            state.append((name, _runtime_stat_token(os.fstat(directory_fd))))
        finally:
            os.close(directory_fd)
    return tuple(state)


def _open_readonly_absolute_directory(path: Path, *, code: str) -> int:
    if not path.is_absolute() or any(part == ".." for part in path.parts):
        raise _invalid(code, "runtime proof root is not absolute and normalized")
    current = os.open(
        path.anchor,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        for component in path.relative_to(path.anchor).parts:
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        metadata = os.fstat(current)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o222:
            raise _invalid(code, "runtime proof root is writable")
        return current
    except BaseException:
        os.close(current)
        raise


def _open_directory_member_at(
    root_fd: int,
    relative_name: str,
    *,
    code: str,
    require_readonly: bool,
) -> int:
    safe = _safe_runtime_relative_name(relative_name, code=code)
    root_device = os.fstat(root_fd).st_dev
    current = os.dup(root_fd)
    try:
        for component in safe.split("/"):
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
            metadata = os.fstat(current)
            if metadata.st_dev != root_device or (
                require_readonly and stat.S_IMODE(metadata.st_mode) & 0o222
            ):
                raise _invalid(code, "runtime proof directory authority differs")
        return current
    except BaseException:
        os.close(current)
        raise


def _read_regular_member_at(
    root_fd: int,
    relative_name: str,
    *,
    code: str,
    require_readonly: bool,
) -> bytes:
    safe = _safe_runtime_relative_name(relative_name, code=code)
    parts = safe.split("/")
    parent_fd = (
        os.dup(root_fd)
        if len(parts) == 1
        else _open_directory_member_at(
            root_fd,
            "/".join(parts[:-1]),
            code=code,
            require_readonly=require_readonly,
        )
    )
    try:
        before = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        member_fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            opened = os.fstat(member_fd)
            if (
                _runtime_stat_token(before) != _runtime_stat_token(opened)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or opened.st_dev != os.fstat(root_fd).st_dev
                or (require_readonly and stat.S_IMODE(opened.st_mode) & 0o222)
            ):
                raise _invalid(code, "runtime proof member authority differs")
            chunks: list[bytes] = []
            while True:
                chunk = os.read(member_fd, 1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            if _runtime_stat_token(opened) != _runtime_stat_token(os.fstat(member_fd)):
                raise _invalid(code, "runtime proof member changed while read")
            return b"".join(chunks)
        finally:
            os.close(member_fd)
    except OSError as exc:
        raise _invalid(code, "runtime proof member is unavailable") from exc
    finally:
        os.close(parent_fd)


def _verify_regular_file_raw_blob_at(
    root_fd: int,
    relative_name: str,
    expected: Mapping[str, object],
    *,
    code: str,
) -> None:
    payload = _read_regular_member_at(
        root_fd,
        relative_name,
        code=code,
        require_readonly=True,
    )
    if _raw_blob(payload) != expected:
        raise _invalid(code, "runtime proof member bytes differ")


def _safe_runtime_relative_name(value: object, *, code: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise _invalid(code, "runtime proof member name is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise _invalid(code, "runtime proof member name escapes its prefix")
    return value


def _decode_base64(value: object, *, code: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise _invalid(code, "runtime proof authentication bytes are invalid")
    try:
        payload = base64.b64decode(value, validate=True)
    except ValueError as exc:
        raise _invalid(code, "runtime proof authentication bytes are invalid") from exc
    if not payload:
        raise _invalid(code, "runtime proof authentication bytes are empty")
    return payload


def _open_locked_dependency_wheelhouse(
    repo_root_fd: int, environ: Mapping[str, str]
) -> tuple[
    int,
    str,
    tuple[tuple[int, int, int, int, int, int, int], tuple[tuple[object, ...], ...]],
]:
    code = "sealed_release_runtime_acquisition_mismatch"
    root_value = environ.get(RUNTIME_WHEELHOUSE_ROOT_ENV)
    expected_digest = environ.get(RUNTIME_WHEELHOUSE_MANIFEST_DIGEST_ENV)
    if not root_value or not expected_digest:
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "sealed release requires an exact lock-backed dependency wheelhouse",
        )
    if not _is_sha256(expected_digest):
        raise _invalid(code, "dependency wheelhouse manifest digest is invalid")
    try:
        lock_payload = _read_regular_member_at(
            repo_root_fd,
            "uv.lock",
            code=code,
            require_readonly=False,
        )
        lock_document = tomllib.loads(lock_payload.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise _invalid(code, "sealed release dependency lock is unavailable") from exc
    lock_hashes: set[str] = set()
    packages = lock_document.get("package")
    if not isinstance(packages, list):
        raise _invalid(code, "sealed release dependency lock has no package set")
    editable_root_count = 0
    for package_value in packages:
        package = _strict_mapping(package_value, code=code)
        source = _strict_mapping(package.get("source"), code=code)
        if source == {"editable": "."}:
            editable_root_count += 1
        elif set(source) != {"registry"} or not isinstance(source.get("registry"), str):
            raise _invalid(
                code,
                "sealed release dependency lock contains non-wheel local authority",
            )
    if editable_root_count != 1:
        raise _invalid(
            code, "sealed release dependency lock must name one exact root project"
        )

    def collect_hashes(value: object) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key == "hash" and _is_sha256(nested):
                    lock_hashes.add(str(nested))
                else:
                    collect_hashes(nested)
        elif isinstance(value, list):
            for nested in value:
                collect_hashes(nested)

    collect_hashes(lock_document)
    if not lock_hashes:
        raise _invalid(code, "sealed release dependency lock has no artifact hashes")
    wheelhouse_fd = _open_readonly_absolute_directory(Path(root_value), code=code)
    try:
        manifest_payload = _read_regular_member_at(
            wheelhouse_fd,
            _RUNTIME_WHEELHOUSE_MANIFEST_FILENAME,
            code=code,
            require_readonly=True,
        )
        manifest = _strict_json_document(manifest_payload, code=code)
        if _canonical_digest(manifest) != expected_digest:
            raise _invalid(code, "dependency wheelhouse manifest digest differs")
        _exact_keys(
            manifest,
            {"schema_version", "dependency_lock_raw_blob", "wheels"},
            code=code,
        )
        if manifest.get("schema_version") != RUNTIME_WHEELHOUSE_MANIFEST_SCHEMA:
            raise _invalid(code, "dependency wheelhouse schema is unsupported")
        lock_ref = _strict_raw_blob(manifest.get("dependency_lock_raw_blob"), code=code)
        if lock_ref != _raw_blob(lock_payload):
            raise _invalid(code, "dependency wheelhouse names a different lock")
        wheels_value = manifest.get("wheels")
        if not isinstance(wheels_value, list) or not wheels_value:
            raise _invalid(code, "dependency wheelhouse has no locked wheels")
        wheels: list[Mapping[str, object]] = []
        for value in wheels_value:
            item = _strict_mapping(value, code=code)
            _exact_keys(item, {"relative_name", "raw_blob"}, code=code)
            name = _safe_runtime_relative_name(item.get("relative_name"), code=code)
            if "/" in name or not name.endswith(".whl"):
                raise _invalid(code, "dependency wheelhouse member is not a root wheel")
            raw_blob = _strict_raw_blob(item.get("raw_blob"), code=code)
            if raw_blob.get("digest") not in lock_hashes:
                raise _invalid(code, "dependency wheel is absent from the exact lock")
            _verify_regular_file_raw_blob_at(wheelhouse_fd, name, raw_blob, code=code)
            wheels.append({"relative_name": name, "raw_blob": raw_blob})
        names = [str(item["relative_name"]) for item in wheels]
        if names != sorted(set(names), key=lambda value: value.encode("utf-8")):
            raise _invalid(code, "dependency wheelhouse members are not one sorted set")
        observed = _runtime_prefix_members_at(wheelhouse_fd)
        observed_files = {
            str(item["relative_name"])
            for item in observed
            if item.get("kind") == "file"
        }
        if any(item.get("kind") != "file" for item in observed) or observed_files != {
            _RUNTIME_WHEELHOUSE_MANIFEST_FILENAME,
            *names,
        }:
            raise _invalid(code, "dependency wheelhouse contains undeclared members")
        return (
            wheelhouse_fd,
            expected_digest,
            _retained_tree_state_at(wheelhouse_fd, code=code),
        )
    except BaseException:
        os.close(wheelhouse_fd)
        raise


def _open_enrolled_release_driver(
    path_value: object,
    distribution: VerifiedRuntimeDistribution,
    *,
    logical_name: str,
) -> tuple[int, str]:
    if not isinstance(path_value, str) or not path_value:
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "sealed release requires an exact enrolled dependency installer",
        )
    source_manifest = _strict_mapping(
        distribution.proof_document.get("source_manifest"),
        code="sealed_release_runtime_recipe_mismatch",
    )
    tools = source_manifest.get("driver_tools")
    if not isinstance(tools, list):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime driver-tool enrollment is invalid",
        )
    matching = [
        _strict_mapping(item, code="sealed_release_runtime_recipe_mismatch")
        for item in tools
        if isinstance(item, Mapping) and item.get("logical_name") == logical_name
    ]
    if len(matching) != 1 or not _is_sha256(matching[0].get("executable_raw_blob")):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime dependency installer is not exactly enrolled",
        )
    path = Path(path_value)
    if not path.is_absolute() or any(part == ".." for part in path.parts):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime dependency installer locator is not absolute",
        )
    current = os.open(
        path.anchor,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        parts = path.relative_to(path.anchor).parts
        if not parts:
            raise OSError("missing dependency installer member")
        for part in parts[:-1]:
            next_fd = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        executable_fd = os.open(
            parts[-1],
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=current,
        )
    except OSError as exc:
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "runtime dependency installer is unavailable",
        ) from exc
    finally:
        os.close(current)
    try:
        before = os.fstat(executable_fd)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(executable_fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(executable_fd)
        executable_payload = b"".join(chunks)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not before.st_mode & 0o111
            or stat.S_IMODE(before.st_mode) & 0o222
            or (
                before.st_dev,
                before.st_ino,
                before.st_mode,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            != (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            or _raw_blob(executable_payload)["digest"]
            != matching[0]["executable_raw_blob"]
            or not executable_payload.startswith(b"\x7fELF")
        ):
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "runtime dependency installer bytes differ from enrollment",
            )
        os.lseek(executable_fd, 0, os.SEEK_SET)
        return executable_fd, str(matching[0]["executable_raw_blob"])
    except BaseException:
        os.close(executable_fd)
        raise


def _verify_ed25519(public_key: bytes, signature: bytes, payload: bytes) -> None:
    try:
        Ed25519PublicKey.from_public_bytes(public_key).verify(signature, payload)
    except (ValueError, InvalidSignature) as exc:
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime proof signature is invalid",
        ) from exc


def runtime_prefix_copy_entries(
    distribution_members: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], list[object]]:
    """Split proved distribution members into copied and excluded bytecode.

    Sealing removes upstream bytecode and verification refuses any in the
    tree (#2857), so the copy proof names it as excluded rather than copied.
    Construction and verification both call this, so they cannot drift.
    """

    copied: list[dict[str, object]] = []
    excluded: list[object] = []
    for item in distribution_members:
        if is_bytecode_relative_name(str(item.get("relative_name"))):
            excluded.append(item.get("relative_name"))
            continue
        copied.append(
            {
                "source_relative_name": item.get("relative_name"),
                "runtime_relative_name": item.get("relative_name"),
                "kind": item.get("kind"),
                "mode": item.get("mode"),
                "size": item.get("size"),
                "raw_blob": item.get("raw_blob"),
                "normalized_relative_target": item.get("normalized_relative_target"),
            }
        )
    return copied, excluded


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and re.fullmatch(r"sha256:[0-9a-f]{64}", value) is not None
    )


def write_runtime_construction_receipt(
    *,
    repo_root_fd: int,
    runtime_root_fd: int,
    python_request: bytes,
    interpreter_relative_name: str,
    python_description: Mapping[str, object],
    distribution: VerifiedRuntimeDistribution,
    dependency_wheelhouse_manifest_digest: str,
) -> Mapping[str, object]:
    """Seal the path-free v2 reproducible-runtime construction document."""

    if not _is_sha256(dependency_wheelhouse_manifest_digest):
        raise _invalid(
            "sealed_release_runtime_acquisition_mismatch",
            "dependency wheelhouse manifest digest is invalid",
        )
    # Sealing removes bytecode and verification refuses it (#2857), so the
    # receipt and the sealed runtime tree (members, digest, additions, loader
    # graph) never list it; otherwise the runtime never matches its receipt.
    members = [
        item
        for item in _runtime_prefix_members_at(runtime_root_fd, require_readonly=False)
        if not is_bytecode_relative_name(str(item["relative_name"]))
    ]
    member_by_name = {str(item["relative_name"]): item for item in members}
    interpreter = member_by_name.get(interpreter_relative_name)
    if interpreter is None or interpreter.get("kind") != "file":
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "enrolled Python interpreter is absent from the copied runtime",
        )
    if interpreter_relative_name != "bin/python3.12":
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "runtime interpreter is not the enrolled regular executable",
        )
    if "pyvenv.cfg" in member_by_name:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch",
            "standalone base-prefix authority forbids pyvenv configuration",
        )
    _assert_distribution_prefix_unchanged(
        runtime_root_fd,
        distribution.distribution_members,
    )
    distribution_prefix_tree = {
        "schema_version": RUNTIME_DISTRIBUTION_PREFIX_TREE_SCHEMA,
        "entries": list(distribution.distribution_members),
    }
    distribution_prefix_tree_digest = _canonical_digest(distribution_prefix_tree)
    declared_distribution_tree = _strict_mapping(
        distribution.proof_document.get("distribution_prefix_tree"),
        code="sealed_release_runtime_reproducibility_mismatch",
    )
    if (
        declared_distribution_tree != distribution_prefix_tree
        or distribution_prefix_tree_digest
        != _canonical_digest(declared_distribution_tree)
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "distribution prefix differs from its authenticated two-build proof",
        )
    runtime_prefix_tree = {
        "schema_version": RUNTIME_PREFIX_TREE_SCHEMA,
        "entries": members,
    }
    runtime_prefix_tree_digest = _canonical_digest(runtime_prefix_tree)
    distribution_names = {
        str(item["relative_name"]) for item in distribution.distribution_members
    }
    additions = [
        item for item in members if str(item["relative_name"]) not in distribution_names
    ]
    release_additions_manifest = {
        "schema_version": RUNTIME_RELEASE_ADDITIONS_SCHEMA,
        "entries": additions,
    }
    release_additions_manifest_digest = _canonical_digest(release_additions_manifest)
    copied_entries, excluded_bytecode_entries = runtime_prefix_copy_entries(
        distribution.distribution_members
    )
    prefix_copy_proof = {
        "schema_version": RUNTIME_PREFIX_COPY_PROOF_SCHEMA,
        "distribution_prefix_tree_digest": distribution_prefix_tree_digest,
        "copied_entries": copied_entries,
        "excluded_bytecode_entries": excluded_bytecode_entries,
        "release_additions_manifest_digest": release_additions_manifest_digest,
        "runtime_prefix_tree_digest": runtime_prefix_tree_digest,
        "status": RUNTIME_PREFIX_COPY_STATUS,
    }
    prefix_copy_proof_digest = _canonical_digest(prefix_copy_proof)
    platform_identity = _platform_identity(python_description)
    if _selected_platform_profile(python_description) != dict(
        distribution.platform_profile
    ):
        raise _invalid(
            "sealed_release_runtime_distribution_unsupported",
            "runtime platform has no exact enrolled distribution",
        )
    loader_graph = _runtime_loader_graph(
        runtime_root_fd,
        members,
        platform_identity=platform_identity,
    )
    loader_graph_digest = _canonical_digest(loader_graph)
    allow_list = {
        "schema_version": RUNTIME_PLATFORM_ALLOW_LIST_SCHEMA,
        "platform_identity": platform_identity,
        "libraries": sorted(_LINUX_PLATFORM_LIBRARIES)
        if platform_identity["os"] == "linux"
        else ["/System/Library/", "/usr/lib/"],
    }
    allow_list_digest = _canonical_digest(allow_list)
    loader_proof = {
        "schema_version": RUNTIME_LOADER_PROOF_SCHEMA,
        "platform_identity": platform_identity,
        "loader_graph_digest": loader_graph_digest,
        "platform_loader_allow_list_digest": allow_list_digest,
        "status": "closed",
    }
    loader_proof_digest = _canonical_digest(loader_proof)
    builtin_modules = _runtime_module_names(python_description, "builtin_modules")
    frozen_modules = _runtime_module_names(python_description, "frozen_modules")
    native_closure = {
        "schema_version": RUNTIME_NATIVE_CLOSURE_SCHEMA,
        "runtime_prefix_tree_digest": runtime_prefix_tree_digest,
        "loader_graph_digest": loader_graph_digest,
        "builtin_modules": builtin_modules,
        "frozen_modules": frozen_modules,
    }
    native_closure_digest = _canonical_digest(native_closure)
    verifier_ref = _raw_blob(RUNTIME_RELOCATION_VERIFIER_SOURCE)
    proof_document = distribution.proof_document
    preimage = {
        "schema_version": RUNTIME_CONSTRUCTION_SCHEMA,
        "source_profile": "digest-pinned-reproducible-cpython/v1",
        "python_request_raw_blob": _raw_blob(python_request),
        "distribution_enrollment_digest": _canonical_digest(
            RUNTIME_DISTRIBUTION_ENROLLMENT
        ),
        "source_manifest_digest": distribution.source_manifest_digest,
        "build_recipe_digest": RUNTIME_BUILD_RECIPE_DIGEST,
        "reproducibility_proof_digest": distribution.reproducibility_proof_digest,
        "distribution_acquisition_receipt_digest": (
            distribution.distribution_acquisition_receipt_digest
        ),
        "platform_profile_digest": _canonical_digest(
            dict(distribution.platform_profile)
        ),
        "distribution_archive": dict(distribution.distribution_archive),
        "dependency_wheelhouse_manifest_digest": (
            dependency_wheelhouse_manifest_digest
        ),
        "distribution_prefix_tree_digest": distribution_prefix_tree_digest,
        "prefix_copy_proof_digest": prefix_copy_proof_digest,
        "release_additions_manifest_digest": release_additions_manifest_digest,
        "runtime_prefix_tree_digest": runtime_prefix_tree_digest,
        "interpreter": interpreter,
        "prefix_configuration": {
            "profile": "standalone-base-prefix/v1",
            "pyvenv_config": None,
        },
        "stdlib_native_closure_digest": native_closure_digest,
        "loader_relocation_proof_digest": loader_proof_digest,
        "relocation_verifier_raw_blob": verifier_ref,
        "isolated_flags": ["-I", "-s", "-E", "-B"],
        "environment_profile": "fixed-empty-authority/v1",
        "cwd_profile": "unrelated-empty-directory/v1",
        "execution_profile": "descriptor-exec/v1",
    }
    document: dict[str, object] = {
        "schema_version": RUNTIME_CONSTRUCTION_DOCUMENT_SCHEMA,
        "receipt": preimage,
        "receipt_digest": _canonical_ref(RUNTIME_CONSTRUCTION_SCHEMA, preimage),
        "runtime_members": members,
        "distribution_members": list(distribution.distribution_members),
        "distribution_proof": proof_document,
        "prefix_copy_proof": prefix_copy_proof,
        "release_additions_manifest": release_additions_manifest,
        "loader_graph": loader_graph,
        "loader_proof": loader_proof,
        "platform_loader_allow_list": allow_list,
        "native_closure": native_closure,
        "platform_identity": platform_identity,
        "python_abi": str(python_description["python_abi"]),
    }
    _write_new_runtime_file_at(
        repo_root_fd,
        RUNTIME_CONSTRUCTION_FILENAME,
        _canonical_json(document),
        mode=0o600,
        code="sealed_release_runtime_closure_mismatch",
    )
    return document


def _describe_interpreter(
    interpreter: Path | str,
    *,
    cwd: Path,
    environ: Mapping[str, str],
    executable_fd: int | None = None,
    retained_fds: Sequence[int] = (),
    sealed_root_fds: Sequence[int] = (),
    expected_executable_digest: str | None = None,
    require_live_executable_verification: bool = False,
) -> Mapping[str, object]:
    source = (
        "import _imp,json,platform,sys;"
        "print(json.dumps({'major':sys.version_info.major,'minor':sys.version_info.minor,"
        "'python_abi':'cp%d%d'%(sys.version_info.major,sys.version_info.minor),"
        "'builtin_modules':sorted(sys.builtin_module_names),"
        "'frozen_modules':sorted(n for n in sys.stdlib_module_names if _imp.is_frozen(n)),"
        "'os':sys.platform,'architecture':platform.machine().lower(),"
        "'libc':(platform.libc_ver()[0] or 'system').lower()},sort_keys=True,separators=(',',':')));"
        "__import__('time').sleep(0.1)"
    )
    try:
        with private_pycache_prefix(outside_fds=sealed_root_fds) as pycache_flags:
            output = _run_release_command(
                [
                    str(interpreter),
                    "-I",
                    "-s",
                    "-E",
                    "-B",
                    *pycache_flags,
                    "-c",
                    source,
                ],
                cwd=cwd,
                environ=environ,
                executable_fd=executable_fd,
                retained_fds=retained_fds,
                expected_executable_digest=expected_executable_digest,
                require_live_executable_verification=(
                    require_live_executable_verification
                ),
            )
        value = json.loads(output)
    except (TypeError, ValueError) as exc:
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "managed Python description is invalid",
        ) from exc
    if not isinstance(value, dict):
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "managed Python description is invalid",
        )
    return value


def _platform_identity(description: Mapping[str, object]) -> dict[str, str]:
    os_name = str(description.get("os") or "").lower()
    if os_name.startswith("linux"):
        os_name = "linux"
    elif os_name.startswith("darwin"):
        os_name = "darwin"
    result = {
        "os": os_name,
        "architecture": str(description.get("architecture") or "").lower(),
        "libc": str(description.get("libc") or "system").lower(),
        "kernel_contract": "descriptor-exec-v1",
    }
    if any(
        not value or any(character.isspace() for character in value)
        for value in result.values()
    ):
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "managed Python platform identity is invalid",
        )
    return result


def _runtime_module_names(
    description: Mapping[str, object],
    field: str,
) -> list[str]:
    values = description.get(field)
    if (
        not isinstance(values, list)
        or any(not isinstance(value, str) or not value for value in values)
        or values != sorted(set(values))
    ):
        raise _invalid(
            "sealed_release_runtime_authority_missing",
            "managed Python module inventory is invalid",
        )
    return list(values)


def _runtime_prefix_members(root: Path) -> list[dict[str, object]]:
    if not root.is_dir() or root.is_symlink():
        raise _invalid(
            "sealed_release_runtime_closure_mismatch",
            "runtime prefix root is not one retained directory",
        )
    members: list[dict[str, object]] = []
    for path in sorted(
        root.rglob("*"), key=lambda value: value.relative_to(root).as_posix()
    ):
        relative_name = path.relative_to(root).as_posix()
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            target = os.readlink(path)
            normalized = posixpath.normpath(target)
            resolved = posixpath.normpath(
                posixpath.join(posixpath.dirname(relative_name), normalized)
            )
            if (
                target.startswith("/")
                or "\\" in target
                or target != normalized
                or resolved == ".."
                or resolved.startswith("../")
            ):
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "copied runtime contains an escaping symbolic link",
                )
            members.append(
                {
                    "relative_name": relative_name,
                    "kind": "symlink",
                    "mode": 0o777,
                    "normalized_relative_target": normalized,
                }
            )
            continue
        if stat.S_ISDIR(metadata.st_mode):
            members.append(
                {
                    "relative_name": relative_name,
                    "kind": "directory",
                    "mode": 0o555,
                    "size": 0,
                    "raw_blob": None,
                }
            )
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "copied runtime contains a special file",
            )
        payload = path.read_bytes()
        members.append(
            {
                "relative_name": relative_name,
                "kind": "file",
                "mode": 0o555 if metadata.st_mode & 0o111 else 0o444,
                "size": len(payload),
                "raw_blob": _raw_blob(payload),
            }
        )
    return members


def _runtime_prefix_members_at(
    root_fd: int, *, require_readonly: bool = True
) -> list[dict[str, object]]:
    root_before = os.fstat(root_fd)
    if not stat.S_ISDIR(root_before.st_mode) or (
        require_readonly and stat.S_IMODE(root_before.st_mode) & 0o222
    ):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime prefix root is writable or not a directory",
        )
    root_device = root_before.st_dev
    members: list[dict[str, object]] = []

    def walk(directory_fd: int, prefix: str) -> None:
        directory_before = os.fstat(directory_fd)
        names = os.listdir(directory_fd)
        if names != list(dict.fromkeys(names)):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime prefix directory names collide",
            )
        for name in sorted(names, key=lambda value: value.encode("utf-8")):
            if (
                not name
                or name in {".", ".."}
                or "/" in name
                or "\\" in name
                or "\x00" in name
            ):
                raise _invalid(
                    "sealed_release_runtime_reproducibility_mismatch",
                    "runtime prefix member name is invalid",
                )
            relative_name = f"{prefix}/{name}" if prefix else name
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if before.st_dev != root_device:
                raise _invalid(
                    "sealed_release_runtime_reproducibility_mismatch",
                    "runtime prefix crosses a mount boundary",
                )
            if stat.S_ISLNK(before.st_mode):
                target = os.readlink(name, dir_fd=directory_fd)
                after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                normalized = posixpath.normpath(target)
                resolved = posixpath.normpath(
                    posixpath.join(posixpath.dirname(relative_name), normalized)
                )
                if (
                    _runtime_stat_token(before) != _runtime_stat_token(after)
                    or target.startswith("/")
                    or "\\" in target
                    or target != normalized
                    or resolved == ".."
                    or resolved.startswith("../")
                ):
                    raise _invalid(
                        "sealed_release_runtime_non_relocatable",
                        "runtime prefix symbolic link differs or escapes",
                    )
                members.append(
                    {
                        "relative_name": relative_name,
                        "kind": "symlink",
                        "mode": 0o777,
                        "normalized_relative_target": normalized,
                    }
                )
                continue
            child_fd = os.open(
                name,
                os.O_RDONLY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(child_fd)
                if _runtime_stat_token(before) != _runtime_stat_token(opened):
                    raise _invalid(
                        "sealed_release_runtime_reproducibility_mismatch",
                        "runtime prefix member changed while opened",
                    )
                if require_readonly and stat.S_IMODE(opened.st_mode) & 0o222:
                    raise _invalid(
                        "sealed_release_runtime_reproducibility_mismatch",
                        "runtime prefix member is writable",
                    )
                if stat.S_ISDIR(opened.st_mode):
                    members.append(
                        {
                            "relative_name": relative_name,
                            "kind": "directory",
                            "mode": 0o555,
                            "size": 0,
                            "raw_blob": None,
                        }
                    )
                    walk(child_fd, relative_name)
                elif stat.S_ISREG(opened.st_mode) and opened.st_nlink == 1:
                    chunks: list[bytes] = []
                    while True:
                        chunk = os.read(child_fd, 1024 * 1024)
                        if not chunk:
                            break
                        chunks.append(chunk)
                    if _runtime_stat_token(opened) != _runtime_stat_token(
                        os.fstat(child_fd)
                    ):
                        raise _invalid(
                            "sealed_release_runtime_reproducibility_mismatch",
                            "runtime prefix member changed while read",
                        )
                    payload = b"".join(chunks)
                    members.append(
                        {
                            "relative_name": relative_name,
                            "kind": "file",
                            "mode": 0o555 if opened.st_mode & 0o111 else 0o444,
                            "size": len(payload),
                            "raw_blob": _raw_blob(payload),
                        }
                    )
                else:
                    raise _invalid(
                        "sealed_release_runtime_non_relocatable",
                        "runtime prefix contains a special or multiply linked file",
                    )
            finally:
                os.close(child_fd)
        if _runtime_stat_token(directory_before) != _runtime_stat_token(
            os.fstat(directory_fd)
        ):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime prefix directory changed while enumerated",
            )

    walk(root_fd, "")
    if _runtime_stat_token(root_before) != _runtime_stat_token(os.fstat(root_fd)):
        raise _invalid(
            "sealed_release_runtime_reproducibility_mismatch",
            "runtime prefix root changed while enumerated",
        )
    # The walk is depth-first, so "pip/..." would precede its sibling
    # "pip-1.0.dist-info"; proved trees use whole-name byte order (#2352).
    members.sort(key=lambda item: str(item["relative_name"]).encode("utf-8"))
    return members


def _read_symlink_member_at(root_fd: int, relative_name: str) -> str:
    parts = relative_name.split("/")
    parent_fd = (
        os.dup(root_fd)
        if len(parts) == 1
        else _open_directory_member_at(
            root_fd,
            "/".join(parts[:-1]),
            code="sealed_release_runtime_reproducibility_mismatch",
            require_readonly=True,
        )
    )
    try:
        before = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        target = os.readlink(parts[-1], dir_fd=parent_fd)
        after = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISLNK(before.st_mode) or _runtime_stat_token(
            before
        ) != _runtime_stat_token(after):
            raise _invalid(
                "sealed_release_runtime_reproducibility_mismatch",
                "runtime prefix symbolic link changed while copied",
            )
        return target
    finally:
        os.close(parent_fd)


def _copy_runtime_prefix_from_fd(
    root_fd: int,
    destination: Path,
    members: Sequence[Mapping[str, object]],
) -> int:
    code = "sealed_release_runtime_reproducibility_mismatch"
    if (
        not destination.is_absolute()
        or destination.name in {"", ".", ".."}
        or any(part == ".." for part in destination.parts)
    ):
        raise _invalid(code, "runtime copy destination is not absolute and normalized")

    parent_fd = _open_absolute_directory_nofollow(destination.parent, code=code)
    destination_fd = -1
    retain_destination = False
    try:
        os.mkdir(destination.name, mode=0o700, dir_fd=parent_fd)
        before = os.stat(destination.name, dir_fd=parent_fd, follow_symlinks=False)
        destination_fd = os.open(
            destination.name,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(destination_fd)
        if not stat.S_ISDIR(opened.st_mode) or _runtime_stat_token(
            before
        ) != _runtime_stat_token(opened):
            raise _invalid(code, "runtime copy destination authority differs")

        directories = sorted(
            (
                _safe_runtime_relative_name(member.get("relative_name"), code=code)
                for member in members
                if member.get("kind") == "directory"
            ),
            key=lambda value: (value.count("/"), value.encode("utf-8")),
        )
        for relative_name in directories:
            _create_runtime_directory_at(destination_fd, relative_name, code=code)

        for member in members:
            relative_name = _safe_runtime_relative_name(
                member.get("relative_name"), code=code
            )
            kind = member.get("kind")
            if kind == "directory":
                continue
            parent_member_fd, leaf = _open_runtime_parent_at(
                destination_fd, relative_name, code=code
            )
            try:
                if kind == "symlink":
                    observed_target = _read_symlink_member_at(root_fd, relative_name)
                    if observed_target != member.get("normalized_relative_target"):
                        raise _invalid(
                            code, "runtime prefix symbolic link target differs"
                        )
                    os.symlink(observed_target, leaf, dir_fd=parent_member_fd)
                    linked = os.stat(
                        leaf, dir_fd=parent_member_fd, follow_symlinks=False
                    )
                    if not stat.S_ISLNK(linked.st_mode) or (
                        os.readlink(leaf, dir_fd=parent_member_fd) != observed_target
                    ):
                        raise _invalid(
                            code, "runtime prefix symbolic link differs while created"
                        )
                    continue

                payload = _read_regular_member_at(
                    root_fd,
                    relative_name,
                    code=code,
                    require_readonly=True,
                )
                if (
                    kind != "file"
                    or _raw_blob(payload) != member.get("raw_blob")
                    or len(payload) != member.get("size")
                ):
                    raise _invalid(code, "runtime prefix member differs while copied")
                member_mode = member.get("mode")
                if not isinstance(member_mode, int) or isinstance(member_mode, bool):
                    raise _invalid(
                        code, "runtime prefix member mode is invalid while copied"
                    )
                _write_runtime_file_at(
                    parent_member_fd,
                    leaf,
                    payload,
                    mode=member_mode,
                    code=code,
                )
            finally:
                os.close(parent_member_fd)

        os.fchmod(destination_fd, 0o755)
        if tuple(
            _runtime_prefix_members_at(destination_fd, require_readonly=False)
        ) != tuple(members):
            raise _invalid(code, "runtime copy destination tree differs")
        rebound_fd = _open_absolute_directory_nofollow(destination, code=code)
        try:
            rebound = os.fstat(rebound_fd)
            retained = os.fstat(destination_fd)
            if (rebound.st_dev, rebound.st_ino) != (retained.st_dev, retained.st_ino):
                raise _invalid(code, "runtime copy destination was replaced")
        finally:
            os.close(rebound_fd)
        retain_destination = True
        return destination_fd
    except OSError as exc:
        raise _invalid(code, "runtime copy destination is unavailable") from exc
    finally:
        if destination_fd >= 0 and not retain_destination:
            os.close(destination_fd)
        os.close(parent_fd)


def _open_absolute_directory_nofollow(path: Path, *, code: str) -> int:
    if not path.is_absolute() or any(part == ".." for part in path.parts):
        raise _invalid(code, "runtime directory locator is not absolute and normalized")
    current = -1
    try:
        current = os.open(
            path.anchor,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        for component in path.relative_to(path.anchor).parts:
            next_fd = os.open(
                component,
                os.O_RDONLY
                | os.O_DIRECTORY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        return current
    except OSError as exc:
        if current >= 0:
            os.close(current)
        raise _invalid(code, "runtime directory locator is unavailable") from exc


def _assert_directory_path_binding(path: Path, retained_fd: int, *, code: str) -> None:
    observed_fd = _open_absolute_directory_nofollow(path, code=code)
    try:
        observed = os.fstat(observed_fd)
        retained = os.fstat(retained_fd)
        if (observed.st_dev, observed.st_ino) != (retained.st_dev, retained.st_ino):
            raise _invalid(code, "retained directory path was replaced")
    finally:
        os.close(observed_fd)


def _open_runtime_parent_at(
    root_fd: int, relative_name: str, *, code: str
) -> tuple[int, str]:
    safe = _safe_runtime_relative_name(relative_name, code=code)
    parts = safe.split("/")
    parent_fd = (
        os.dup(root_fd)
        if len(parts) == 1
        else _open_directory_member_at(
            root_fd,
            "/".join(parts[:-1]),
            code=code,
            require_readonly=False,
        )
    )
    return parent_fd, parts[-1]


def _create_runtime_directory_at(
    root_fd: int, relative_name: str, *, code: str
) -> None:
    parent_fd, leaf = _open_runtime_parent_at(root_fd, relative_name, code=code)
    child_fd = -1
    try:
        os.mkdir(leaf, mode=0o700, dir_fd=parent_fd)
        before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        child_fd = os.open(
            leaf,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(child_fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or opened.st_dev != os.fstat(root_fd).st_dev
            or _runtime_stat_token(before) != _runtime_stat_token(opened)
        ):
            raise _invalid(code, "runtime copy directory authority differs")
        os.fchmod(child_fd, 0o755)
    finally:
        if child_fd >= 0:
            os.close(child_fd)
        os.close(parent_fd)


def _write_runtime_file_at(
    parent_fd: int,
    leaf: str,
    payload: bytes,
    *,
    mode: int,
    code: str,
) -> None:
    member_fd = os.open(
        leaf,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | os.O_CLOEXEC
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=parent_fd,
    )
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(member_fd, remaining)
            if written <= 0:
                raise OSError("short runtime prefix write")
            remaining = remaining[written:]
        os.fchmod(member_fd, mode)
        observed = os.fstat(member_fd)
        rebound = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_nlink != 1
            or observed.st_size != len(payload)
            or stat.S_IMODE(observed.st_mode) != mode
            or _runtime_stat_token(observed) != _runtime_stat_token(rebound)
        ):
            raise _invalid(code, "runtime copy file authority differs")
    finally:
        os.close(member_fd)


def _ensure_runtime_directories_at(
    root_fd: int, relative_directory: str, *, code: str
) -> None:
    safe = _safe_runtime_relative_name(relative_directory, code=code)
    root_device = os.fstat(root_fd).st_dev
    current = os.dup(root_fd)
    try:
        for component in safe.split("/"):
            try:
                next_fd = os.open(
                    component,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=current,
                )
            except FileNotFoundError:
                os.mkdir(component, mode=0o700, dir_fd=current)
                next_fd = os.open(
                    component,
                    os.O_RDONLY
                    | os.O_DIRECTORY
                    | os.O_CLOEXEC
                    | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=current,
                )
            observed = os.fstat(next_fd)
            if not stat.S_ISDIR(observed.st_mode) or observed.st_dev != root_device:
                os.close(next_fd)
                raise _invalid(code, "runtime directory authority differs")
            os.close(current)
            current = next_fd
    except OSError as exc:
        raise _invalid(code, "runtime directory could not be created") from exc
    finally:
        os.close(current)


def _write_new_runtime_file_at(
    root_fd: int, relative_name: str, payload: bytes, *, mode: int, code: str
) -> None:
    safe = _safe_runtime_relative_name(relative_name, code=code)
    parent = PurePosixPath(safe).parent.as_posix()
    if parent != ".":
        _ensure_runtime_directories_at(root_fd, parent, code=code)
    parent_fd, leaf = _open_runtime_parent_at(root_fd, safe, code=code)
    try:
        _write_runtime_file_at(parent_fd, leaf, payload, mode=mode, code=code)
    except OSError as exc:
        raise _invalid(code, "runtime member could not be created") from exc
    finally:
        os.close(parent_fd)


def _open_verified_runtime_file_fd(
    root_fd: int,
    relative_name: str,
    expected: Mapping[str, object],
    *,
    code: str,
) -> int:
    parent_fd, leaf = _open_runtime_parent_at(root_fd, relative_name, code=code)
    member_fd = -1
    try:
        before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        member_fd = os.open(
            leaf,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(member_fd)
        chunks: list[bytes] = []
        while True:
            chunk = os.read(member_fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(member_fd)
        payload = b"".join(chunks)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_dev != os.fstat(root_fd).st_dev
            or _runtime_stat_token(before) != _runtime_stat_token(opened)
            or _runtime_stat_token(opened) != _runtime_stat_token(after)
            or stat.S_IMODE(after.st_mode) != expected.get("mode")
            or len(payload) != expected.get("size")
            or _raw_blob(payload) != expected.get("raw_blob")
        ):
            raise _invalid(
                code, "runtime executable differs from its retained authority"
            )
        os.lseek(member_fd, 0, os.SEEK_SET)
        result = member_fd
        member_fd = -1
        return result
    except OSError as exc:
        raise _invalid(code, "runtime executable is unavailable") from exc
    finally:
        if member_fd >= 0:
            os.close(member_fd)
        os.close(parent_fd)


def _runtime_loader_graph(
    root_fd: int,
    members: list[dict[str, object]],
    *,
    platform_identity: Mapping[str, str],
) -> dict[str, object]:
    regular_names = {
        str(item["relative_name"]) for item in members if item["kind"] == "file"
    }
    objects: list[dict[str, object]] = []
    for relative_name in sorted(regular_names):
        payload = _read_regular_member_at(
            root_fd,
            relative_name,
            code="sealed_release_runtime_closure_mismatch",
            require_readonly=False,
        )
        if payload.startswith(b"\x7fELF"):
            needed, search = _elf_dynamic_metadata(payload)
            resolutions = _resolve_elf_dependencies(
                relative_name,
                needed,
                search,
                regular_names,
            )
            objects.append(
                {
                    "relative_name": relative_name,
                    "raw_blob": _raw_blob(payload),
                    "loader_kind": "elf",
                    "dependency_tokens": list(needed),
                    "relative_search_tokens": list(search),
                    "resolutions": resolutions,
                }
            )
        elif payload[:4] in {
            b"\xcf\xfa\xed\xfe",
            b"\xfe\xed\xfa\xcf",
            b"\xca\xfe\xba\xbe",
            b"\xbe\xba\xfe\xca",
        }:
            dependencies, search = _macho_loader_metadata(payload)
            objects.append(
                {
                    "relative_name": relative_name,
                    "raw_blob": _raw_blob(payload),
                    "loader_kind": "mach-o",
                    "dependency_tokens": list(dependencies),
                    "relative_search_tokens": list(search),
                    "resolutions": _resolve_macho_dependencies(dependencies),
                }
            )
    return {
        "schema_version": RUNTIME_LOADER_GRAPH_SCHEMA,
        "platform_identity": dict(platform_identity),
        "objects": objects,
    }


def _elf_dynamic_metadata(payload: bytes) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if len(payload) < 16:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "ELF header is truncated"
        )
    elf_class = payload[4]
    byte_order = payload[5]
    endian = "<" if byte_order == 1 else ">" if byte_order == 2 else ""
    if elf_class == 2:
        header_format = endian + "HHIQQQIHHHHHH"
        program_format = endian + "IIQQQQQQ"
        dynamic_format = endian + "QQ"
    elif elf_class == 1:
        header_format = endian + "HHIIIIIHHHHHH"
        program_format = endian + "IIIIIIII"
        dynamic_format = endian + "II"
    else:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "ELF class is unsupported"
        )
    if not endian:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "ELF byte order is unsupported"
        )
    header_size = struct.calcsize(header_format)
    if len(payload) < 16 + header_size:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "ELF header is truncated"
        )
    header = struct.unpack_from(header_format, payload, 16)
    program_offset = int(header[4])
    program_entry_size = int(header[8])
    program_count = int(header[9])
    loads: list[tuple[int, int, int]] = []
    dynamic: tuple[int, int] | None = None
    expected_program_size = struct.calcsize(program_format)
    if program_entry_size < expected_program_size:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "ELF program header is invalid"
        )
    for index in range(program_count):
        offset = program_offset + index * program_entry_size
        if offset + expected_program_size > len(payload):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "ELF program header is truncated",
            )
        values = struct.unpack_from(program_format, payload, offset)
        if elf_class == 2:
            kind, file_offset, virtual_address, file_size = (
                values[0],
                values[2],
                values[3],
                values[5],
            )
        else:
            kind, file_offset, virtual_address, file_size = (
                values[0],
                values[1],
                values[2],
                values[4],
            )
        if kind == 1:
            loads.append((int(virtual_address), int(file_offset), int(file_size)))
        elif kind == 2:
            dynamic = (int(file_offset), int(file_size))
    if dynamic is None:
        return (), ()
    tags: list[tuple[int, int]] = []
    dynamic_entry_size = struct.calcsize(dynamic_format)
    for offset in range(dynamic[0], dynamic[0] + dynamic[1], dynamic_entry_size):
        if offset + dynamic_entry_size > len(payload):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "ELF dynamic table is truncated",
            )
        tag, value = struct.unpack_from(dynamic_format, payload, offset)
        if tag == 0:
            break
        tags.append((int(tag), int(value)))
    string_address = next((value for tag, value in tags if tag == 5), None)
    if string_address is None:
        return (), ()
    string_offset = None
    for virtual_address, file_offset, file_size in loads:
        if virtual_address <= string_address < virtual_address + file_size:
            string_offset = file_offset + string_address - virtual_address
            break
    if string_offset is None:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch",
            "ELF string table escapes load segments",
        )

    def string_at(index: int) -> str:
        start = string_offset + index
        end = payload.find(b"\0", start)
        if start < 0 or end < 0 or end > len(payload):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "ELF dynamic string is invalid",
            )
        try:
            return payload[start:end].decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "ELF dynamic string is invalid",
            ) from exc

    needed = tuple(string_at(value) for tag, value in tags if tag == 1)
    search_values = tuple(string_at(value) for tag, value in tags if tag in {15, 29})
    search: list[str] = []
    for value in search_values:
        for token in value.split(":"):
            if not token or token.startswith("/"):
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF loader search contains an empty or absolute host entry",
                )
            if not token.startswith(("$ORIGIN", "${ORIGIN}")):
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF loader search is not relative to the runtime object",
                )
            search.append(token.replace("${ORIGIN}", "$ORIGIN"))
    return tuple(needed), tuple(search)


def _resolve_elf_dependencies(
    object_name: str,
    needed: tuple[str, ...],
    search: tuple[str, ...],
    regular_names: set[str],
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    origin = posixpath.dirname(object_name)

    def candidate_for(token: str, dependency: str) -> str:
        directory = posixpath.normpath(token.replace("$ORIGIN", origin))
        return posixpath.normpath(posixpath.join(directory, dependency))

    def escapes_runtime(candidate: str) -> bool:
        return (
            candidate == ".."
            or candidate.startswith("../")
            or candidate.startswith("/")
        )

    for dependency in needed:
        if "\\" in dependency or not dependency:
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "ELF dependency token contains a host locator",
            )
        if "/" in dependency:
            origin_prefix = next(
                (
                    prefix
                    for prefix in ("$ORIGIN/", "${ORIGIN}/")
                    if dependency.startswith(prefix)
                ),
                None,
            )
            if origin_prefix is None:
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF dependency token contains a host locator",
                )
            candidate = posixpath.normpath(
                posixpath.join(origin, dependency[len(origin_prefix) :])
            )
            if escapes_runtime(candidate):
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF origin-relative dependency escapes the runtime prefix",
                )
            if candidate not in regular_names:
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF origin-relative dependency is absent from the runtime closure",
                )
            result.append({"dependency": dependency, "resolution": candidate})
            continue
        candidates: set[str] = set()
        for token in search:
            candidate = candidate_for(token, dependency)
            if escapes_runtime(candidate):
                raise _invalid(
                    "sealed_release_runtime_non_relocatable",
                    "ELF loader search escapes the runtime prefix",
                )
            if candidate in regular_names:
                candidates.add(candidate)
        if len(candidates) == 1:
            result.append(
                {"dependency": dependency, "resolution": next(iter(candidates))}
            )
        elif len(candidates) > 1:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "ELF dependency resolves to multiple runtime members",
            )
        elif dependency in _LINUX_PLATFORM_LIBRARIES:
            result.append(
                {"dependency": dependency, "resolution": "platform-allow-list"}
            )
        else:
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "ELF dependency is absent from the runtime closure",
            )
    return result


def _macho_loader_metadata(payload: bytes) -> tuple[tuple[str, ...], tuple[str, ...]]:
    magic = payload[:4]
    if magic not in {b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf"}:
        return (), ()
    endian = "<" if magic == b"\xcf\xfa\xed\xfe" else ">"
    if len(payload) < 32:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "Mach-O header is truncated"
        )
    _cpu, _subcpu, _type, command_count, _command_size, _flags, _reserved = (
        struct.unpack_from(endian + "IIIIIII", payload, 4)
    )
    cursor = 32
    dependencies: list[str] = []
    searches: list[str] = []
    for _index in range(command_count):
        if cursor + 8 > len(payload):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch", "Mach-O command is truncated"
            )
        command, command_size = struct.unpack_from(endian + "II", payload, cursor)
        if command_size < 8 or cursor + command_size > len(payload):
            raise _invalid(
                "sealed_release_runtime_closure_mismatch", "Mach-O command is invalid"
            )
        base_command = command & 0x7FFFFFFF
        if base_command in {0xC, 0x18, 0x1F, 0x23}:
            name_offset = struct.unpack_from(endian + "I", payload, cursor + 8)[0]
            dependencies.append(
                _macho_command_string(payload, cursor, command_size, name_offset)
            )
        elif base_command == 0x1C:
            name_offset = struct.unpack_from(endian + "I", payload, cursor + 8)[0]
            searches.append(
                _macho_command_string(payload, cursor, command_size, name_offset)
            )
        cursor += command_size
    for token in searches:
        if not token.startswith(("@loader_path", "@executable_path")):
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "Mach-O loader search is not relative to the runtime object",
            )
    return tuple(dependencies), tuple(searches)


def _macho_command_string(payload: bytes, cursor: int, size: int, offset: int) -> str:
    start = cursor + offset
    end = payload.find(b"\0", start, cursor + size)
    if offset < 8 or end < 0:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "Mach-O loader string is invalid"
        )
    try:
        return payload[start:end].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _invalid(
            "sealed_release_runtime_closure_mismatch", "Mach-O loader string is invalid"
        ) from exc


def _resolve_macho_dependencies(dependencies: tuple[str, ...]) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for dependency in dependencies:
        if dependency.startswith(("@rpath/", "@loader_path/", "@executable_path/")):
            result.append({"dependency": dependency, "resolution": "runtime-relative"})
        elif dependency.startswith(("/System/Library/", "/usr/lib/")):
            result.append(
                {"dependency": dependency, "resolution": "platform-allow-list"}
            )
        else:
            raise _invalid(
                "sealed_release_runtime_non_relocatable",
                "Mach-O dependency names a non-platform host location",
            )
    return result


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_digest(value: object) -> str:
    return f"sha256:{hashlib.sha256(_canonical_json(value)).hexdigest()}"


def _canonical_ref(schema: str, value: object) -> dict[str, str]:
    return {
        "digest_domain": "canonical-json/v1",
        "schema_version": schema,
        "digest": _canonical_digest(value),
    }


def _raw_blob(payload: bytes) -> dict[str, object]:
    return {
        "digest_domain": "raw-bytes/v1",
        "digest": f"sha256:{hashlib.sha256(payload).hexdigest()}",
        "size": len(payload),
    }


def _invalid(code: str, message: str) -> SealedReleaseInvalid:
    return SealedReleaseInvalid(message, data={"code": code})


def _run_release_command(
    command: list[str],
    *,
    cwd: Path,
    environ: Mapping[str, str],
    executable_fd: int | None = None,
    retained_fds: Sequence[int] = (),
    expected_executable_digest: str | None = None,
    require_live_executable_verification: bool = False,
    write_root_fds: Sequence[int] = (),
) -> str:
    write_roots = tuple(sorted(set(write_root_fds)))
    if any(fd not in retained_fds for fd in write_roots):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "write-confined command did not retain every admitted write root",
        )
    if expected_executable_digest is not None:
        if executable_fd is None:
            raise _invalid(
                "sealed_release_runtime_recipe_mismatch",
                "executable digest verification requires a retained descriptor",
            )
        verified_executable_fd = executable_fd
        _verify_open_executable_digest(
            verified_executable_fd, expected_executable_digest
        )
    else:
        verified_executable_fd = None
    temporary: tempfile.TemporaryDirectory[str] | None = None
    temporary_fd = -1
    try:
        command_environment = dict(environ)
        confined_roots = write_roots
        if write_roots:
            if platform.system() != "Linux":
                raise _invalid(
                    "sealed_release_runtime_distribution_unsupported",
                    "sealed dependency installation requires Linux write confinement",
                )
            temporary = tempfile.TemporaryDirectory(prefix="okg-sealed-uv-")
            temporary_fd = _open_absolute_directory_nofollow(
                Path(temporary.name),
                code="sealed_release_runtime_closure_mismatch",
            )
            command_environment["TMPDIR"] = f"/proc/self/fd/{temporary_fd}"
            confined_roots = (*write_roots, temporary_fd)
        pass_fds = tuple(
            sorted(
                {
                    *retained_fds,
                    *confined_roots,
                    *(() if executable_fd is None else (executable_fd,)),
                }
            )
        )
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            env=command_environment,
            executable=(
                None if executable_fd is None else f"/proc/self/fd/{executable_fd}"
            ),
            pass_fds=pass_fds,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=(
                None
                if not confined_roots
                else lambda: _install_landlock_write_confinement(confined_roots)
            ),
        )
        if expected_executable_digest is not None:
            assert verified_executable_fd is not None
            if require_live_executable_verification and proc.poll() is not None:
                proc.communicate()
                raise _invalid(
                    "sealed_release_runtime_recipe_mismatch",
                    "enrolled executable exited before its live identity was verified",
                )
            try:
                _verify_open_executable_digest(
                    verified_executable_fd, expected_executable_digest
                )
            except BaseException:
                if proc.poll() is None:
                    proc.kill()
                proc.communicate()
                raise
        stdout, stderr = proc.communicate()
        if expected_executable_digest is not None:
            assert verified_executable_fd is not None
            _verify_open_executable_digest(
                verified_executable_fd, expected_executable_digest
            )
        if proc.returncode != 0:
            rendered = " ".join(command)
            raise SealedReleaseInvalid(
                "sealed release command failed",
                data={
                    "command": rendered,
                    "observed": (stderr or stdout)[-4000:],
                },
            )
        return stdout.strip()
    finally:
        if temporary_fd >= 0:
            os.close(temporary_fd)
        if temporary is not None:
            temporary.cleanup()


class _LandlockRulesetAttr(ctypes.Structure):
    _fields_ = [("handled_access_fs", ctypes.c_uint64)]


class _LandlockPathBeneathAttr(ctypes.Structure):
    _fields_ = [
        ("allowed_access", ctypes.c_uint64),
        ("parent_fd", ctypes.c_int32),
    ]


def _install_landlock_write_confinement(write_root_fds: Sequence[int]) -> None:
    """Restrict this child and its descendants to admitted write subtrees."""

    create_ruleset = 444
    add_rule = 445
    restrict_self = 446
    create_version = 1
    path_beneath_rule = 1
    pr_set_no_new_privs = 38
    write_access = (1 << 1) | sum(1 << bit for bit in range(4, 15))
    libc = ctypes.CDLL(None, use_errno=True)
    abi = int(libc.syscall(create_ruleset, None, 0, create_version))
    if abi < 3:
        error = ctypes.get_errno()
        raise OSError(
            error or 95,
            "Linux Landlock ABI v3 is required for sealed writes",
        )
    ruleset = _LandlockRulesetAttr(write_access)
    ruleset_fd = int(
        libc.syscall(create_ruleset, ctypes.byref(ruleset), ctypes.sizeof(ruleset), 0)
    )
    if ruleset_fd < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    try:
        for root_fd in write_root_fds:
            rule = _LandlockPathBeneathAttr(write_access, root_fd)
            if (
                int(
                    libc.syscall(
                        add_rule,
                        ruleset_fd,
                        path_beneath_rule,
                        ctypes.byref(rule),
                        0,
                    )
                )
                != 0
            ):
                error = ctypes.get_errno()
                raise OSError(error, os.strerror(error))
        if int(libc.prctl(pr_set_no_new_privs, 1, 0, 0, 0)) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        if int(libc.syscall(restrict_self, ruleset_fd, 0)) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
    finally:
        os.close(ruleset_fd)


def _verify_open_executable_digest(executable_fd: int, expected_digest: str) -> None:
    before = os.fstat(executable_fd)
    chunks: list[bytes] = []
    offset = 0
    while True:
        chunk = os.pread(executable_fd, 1024 * 1024, offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
    after = os.fstat(executable_fd)
    payload = b"".join(chunks)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not before.st_mode & 0o111
        or stat.S_IMODE(before.st_mode) & 0o222
        or _runtime_stat_token(before) != _runtime_stat_token(after)
        or _raw_blob(payload)["digest"] != expected_digest
        or not payload.startswith(b"\x7fELF")
    ):
        raise _invalid(
            "sealed_release_runtime_recipe_mismatch",
            "executed tool differs from its enrolled immutable bytes",
        )


def _canonicalize_direct_urls(
    repo_root_fd: int,
    runtime_root_fd: int,
    *,
    protected_names: set[str],
) -> None:
    members = _runtime_prefix_members_at(runtime_root_fd, require_readonly=False)
    names = [
        str(item["relative_name"])
        for item in members
        if item.get("kind") == "file"
        and PurePosixPath(str(item["relative_name"])).name == "direct_url.json"
        and PurePosixPath(str(item["relative_name"])).parent.name.endswith(".dist-info")
    ]
    for relative_name in names:
        try:
            payload = _read_regular_member_at(
                runtime_root_fd,
                relative_name,
                code="sealed_release_runtime_closure_mismatch",
                require_readonly=False,
            )
            raw = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeDecodeError, ValueError) as exc:
            raise SealedReleaseInvalid(
                "installed package has invalid direct_url metadata",
                data={"observed": relative_name},
            ) from exc
        if not isinstance(raw, dict):
            raise SealedReleaseInvalid(
                "installed package has invalid direct_url metadata",
                data={"observed": relative_name},
            )
        parsed = urlparse(str(raw.get("url") or ""))
        if parsed.scheme != "file" or parsed.path.startswith("/__okg_release__/"):
            continue
        if relative_name in protected_names:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "release metadata rewrite would change the proved distribution prefix",
            )
        source_path = Path(unquote(parsed.path))
        if not source_path.is_absolute():
            raise SealedReleaseInvalid(
                "frozen environment contains a non-absolute local package source",
                data={"observed": parsed.path},
            )
        source_fd = -1
        try:
            if str(source_path) == f"/proc/self/fd/{repo_root_fd}":
                source_fd = os.dup(repo_root_fd)
            else:
                source_fd = _open_absolute_directory_nofollow(
                    source_path,
                    code="sealed_release_runtime_closure_mismatch",
                )
            source = os.fstat(source_fd)
            retained = os.fstat(repo_root_fd)
            if (source.st_dev, source.st_ino) != (retained.st_dev, retained.st_ino):
                raise _invalid(
                    "sealed_release_runtime_closure_mismatch",
                    "installed local package does not name the retained release repo",
                )
        finally:
            if source_fd >= 0:
                os.close(source_fd)
        raw["url"] = "file:///__okg_release__/repo"
        _write_existing_runtime_file_at(
            runtime_root_fd,
            relative_name,
            (json.dumps(raw, sort_keys=True, separators=(",", ":")) + "\n").encode(
                "utf-8"
            ),
        )


def _rewrite_python_shebangs(
    runtime_root_fd: int,
    *,
    protected_names: set[str],
) -> None:
    members = _runtime_prefix_members_at(runtime_root_fd, require_readonly=False)
    names = [
        str(item["relative_name"])
        for item in members
        if item.get("kind") == "file"
        and str(item["relative_name"]).startswith("bin/")
        and str(item["relative_name"]).count("/") == 1
    ]
    for relative_name in names:
        payload = _read_regular_member_at(
            runtime_root_fd,
            relative_name,
            code="sealed_release_runtime_closure_mismatch",
            require_readonly=False,
        )
        line_end = payload.find(b"\n")
        first_line = payload if line_end < 0 else payload[: line_end + 1]
        if len(first_line) > 4096 or not _PYTHON_SHEBANG.fullmatch(
            first_line.rstrip(b"\r\n")
        ):
            continue
        if relative_name in protected_names:
            raise _invalid(
                "sealed_release_runtime_closure_mismatch",
                "script rewrite would change the proved distribution prefix",
            )
        remainder = b"" if line_end < 0 else payload[line_end + 1 :]
        _write_existing_runtime_file_at(
            runtime_root_fd,
            relative_name,
            _RELOCATABLE_PYTHON_HEADER + remainder,
        )


def _refresh_record_hashes(
    runtime_root_fd: int,
    *,
    protected_names: set[str],
) -> None:
    members = _runtime_prefix_members_at(runtime_root_fd, require_readonly=False)
    regular_names = {
        str(item["relative_name"]) for item in members if item.get("kind") == "file"
    }
    records = sorted(
        name
        for name in regular_names
        if PurePosixPath(name).name == "RECORD"
        and PurePosixPath(name).parent.name.endswith(".dist-info")
    )
    for record_name in records:
        if record_name in protected_names:
            continue
        payload = _read_regular_member_at(
            runtime_root_fd,
            record_name,
            code="sealed_release_runtime_closure_mismatch",
            require_readonly=False,
        )
        try:
            rows = list(csv.reader(io.StringIO(payload.decode("utf-8")), strict=True))
        except (UnicodeDecodeError, csv.Error) as exc:
            raise SealedReleaseInvalid(
                "installed package has invalid RECORD metadata",
                data={"observed": record_name},
            ) from exc
        site_packages = PurePosixPath(record_name).parent.parent.as_posix()
        updated: list[list[str]] = []
        for row in rows:
            if not row:
                continue
            while len(row) < 3:
                row.append("")
            candidate = posixpath.normpath(posixpath.join(site_packages, row[0]))
            if (
                not row[0]
                or "\\" in row[0]
                or "\x00" in row[0]
                or candidate.startswith("/")
                or candidate == ".."
                or candidate.startswith("../")
            ):
                raise SealedReleaseInvalid(
                    "installed package RECORD escapes the sealed environment",
                    data={"observed": row[0]},
                )
            if candidate in regular_names and candidate != record_name:
                content = _read_regular_member_at(
                    runtime_root_fd,
                    candidate,
                    code="sealed_release_runtime_closure_mismatch",
                    require_readonly=False,
                )
                encoded = base64.urlsafe_b64encode(
                    hashlib.sha256(content).digest(),
                ).rstrip(b"=")
                row[1] = f"sha256={encoded.decode('ascii')}"
                row[2] = str(len(content))
            elif candidate == record_name:
                row[1] = ""
                row[2] = ""
            updated.append(row)
        output = io.StringIO(newline="")
        csv.writer(output, lineterminator="\n").writerows(updated)
        _write_existing_runtime_file_at(
            runtime_root_fd,
            record_name,
            output.getvalue().encode("utf-8"),
        )


def _write_existing_runtime_file_at(
    root_fd: int, relative_name: str, payload: bytes
) -> None:
    code = "sealed_release_runtime_closure_mismatch"
    parent_fd, leaf = _open_runtime_parent_at(root_fd, relative_name, code=code)
    member_fd = -1
    try:
        before = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        member_fd = os.open(
            leaf,
            os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        opened = os.fstat(member_fd)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_dev != os.fstat(root_fd).st_dev
            or _runtime_stat_token(before) != _runtime_stat_token(opened)
        ):
            raise _invalid(code, "runtime member authority differs before rewrite")
        mode = stat.S_IMODE(opened.st_mode)
        os.ftruncate(member_fd, 0)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(member_fd, remaining)
            if written <= 0:
                raise OSError("short runtime member write")
            remaining = remaining[written:]
        os.fchmod(member_fd, mode)
        os.fsync(member_fd)
        after = os.fstat(member_fd)
        rebound = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        if after.st_size != len(payload) or _runtime_stat_token(
            after
        ) != _runtime_stat_token(rebound):
            raise _invalid(code, "runtime member authority differs after rewrite")
    except OSError as exc:
        raise _invalid(code, "runtime member could not be rewritten") from exc
    finally:
        if member_fd >= 0:
            os.close(member_fd)
        os.close(parent_fd)
