"""#2857: a sealed release never reads, and never admits, Python bytecode.

The release tree digest skips ``__pycache__``, ``.pyc`` and ``.pyo``, so a
bytecode file planted after sealing would run unverified. Two defences:
every sealed interpreter starts with ``-X pycache_prefix=<fresh private
dir>`` (no read from the tree's ``__pycache__``), and every verification
refuses bytecode anywhere in the executable tree (which also covers
source-less ``.pyc`` modules that ``pycache_prefix`` cannot stop).
"""

from __future__ import annotations

import json
import os
import py_compile
import stat
import subprocess
import sys
import tempfile
from importlib.util import cache_from_source
from pathlib import Path

import pytest

from okg.substrate import _sealed_release_runtime as runtime
from okg.substrate import sealed_release as sealed_release_impl
from okg.substrate._sealed_release_integrity import (
    SEALED_BYTECODE_REASON,
    executable_tree_digest,
    is_bytecode_relative_name,
    private_pycache_prefix,
    remove_runtime_caches,
    seal_permissions,
)
from okg.substrate.errors import SealedReleaseInvalid
from okg.substrate.release_activation import (
    activate_environment_release,
    launcher_descriptor_path,
)
from okg.substrate.runtime import release_launcher
from okg.substrate.sealed_release import verify_sealed_release
from tests.substrate.test_sealed_releases import (
    _fake_sync,
    _fixture_repo,
    _prepare,
    _prepare_environment_fixture,
)

_MARKER = "OKG-PLANTED-BYTECODE-RAN"
_SEALED_FLAGS = ["-I", "-s", "-E", "-B"]


def _plant_unchecked_pyc(source: Path, *, pyc: Path) -> None:
    """Compile hostile code into an unchecked-hash .pyc at ``pyc``."""

    hostile = pyc.parent / "hostile_source.py"
    pyc.parent.mkdir(parents=True, exist_ok=True)
    hostile.write_text(f"print({_MARKER!r})\nVALUE = 'planted'\n", encoding="utf-8")
    py_compile.compile(
        str(hostile),
        cfile=str(pyc),
        doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH,
    )
    hostile.unlink()


def _import_probe(library: Path, module: str) -> str:
    return (
        f"import sys; sys.path.insert(0, {str(library)!r}); "
        f"import {module}; print({module}.VALUE)"
    )


def _unseal_directories(root: Path) -> None:
    """Let the test play an attacker with write access to the release."""

    for item in (root, *root.rglob("*")):
        if item.is_dir() and not item.is_symlink():
            item.chmod(stat.S_IMODE(item.stat().st_mode) | stat.S_IWUSR)


# --- defence 1: a real interpreter never reads the tree's __pycache__ -------


def test_planted_pycache_bytecode_is_not_read_with_private_prefix(
    tmp_path: Path,
) -> None:
    library = tmp_path / "lib"
    library.mkdir()
    source = library / "victim.py"
    source.write_text("VALUE = 'source'\n", encoding="utf-8")
    _plant_unchecked_pyc(source, pyc=Path(cache_from_source(str(source))))
    probe = _import_probe(library, "victim")

    # The attack is real: -B alone still reads the planted file.
    exposed = subprocess.run(
        [sys.executable, *_SEALED_FLAGS, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
    )
    assert exposed.stdout.split() == [_MARKER, "planted"]

    with private_pycache_prefix() as flags:
        prefix = Path(flags[1].removeprefix("pycache_prefix="))
        assert flags[0] == "-X"
        assert stat.S_IMODE(prefix.stat().st_mode) == 0o700
        protected = subprocess.run(
            [sys.executable, *_SEALED_FLAGS, *flags, "-c", probe],
            capture_output=True,
            text=True,
            check=True,
        )
        assert list(prefix.iterdir()) == []
    assert protected.stdout.split() == ["source"]
    assert _MARKER not in protected.stdout + protected.stderr
    assert not prefix.exists()


def test_sourceless_pyc_still_loads_under_prefix_so_verification_must_refuse(
    tmp_path: Path,
) -> None:
    """pycache_prefix does not stop a source-less ``module.pyc`` on sys.path.

    This is why verification refuses every ``.pyc`` in the tree, not only
    ``__pycache__`` directories.
    """

    library = tmp_path / "lib"
    library.mkdir()
    _plant_unchecked_pyc(library / "ghost.py", pyc=library / "ghost.pyc")
    with private_pycache_prefix() as flags:
        loaded = subprocess.run(
            [
                sys.executable,
                *_SEALED_FLAGS,
                *flags,
                "-c",
                _import_probe(library, "ghost"),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    assert loaded.stdout.split() == [_MARKER, "planted"]


def test_private_prefix_refuses_a_directory_inside_the_sealed_tree(
    tmp_path: Path, monkeypatch
) -> None:
    tree = tmp_path / "tree"
    (tree / "tmp").mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(tree / "tmp"))
    tree_fd = os.open(tree, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(SealedReleaseInvalid) as refusal:
            with private_pycache_prefix(outside_fds=(tree_fd,)):
                pytest.fail("a prefix inside the sealed tree was admitted")
    finally:
        os.close(tree_fd)
    assert refusal.value.data["code"] == SEALED_BYTECODE_REASON
    assert list((tree / "tmp").iterdir()) == []


def test_private_prefix_refuses_bytecode_written_during_the_launch() -> None:
    with pytest.raises(SealedReleaseInvalid) as refusal:
        with private_pycache_prefix() as flags:
            prefix = Path(flags[1].removeprefix("pycache_prefix="))
            (prefix / "planted.cpython-312.pyc").write_bytes(b"same-user plant")
    assert refusal.value.data == {
        "code": SEALED_BYTECODE_REASON,
        "observed": ["planted.cpython-312.pyc"],
    }
    assert not prefix.exists()


def test_private_prefix_reports_a_failed_removal(monkeypatch, caplog) -> None:
    import shutil

    def refuse(_path, *args, **kwargs):
        raise PermissionError("fixture: cannot remove")

    monkeypatch.setattr(shutil, "rmtree", refuse)
    with pytest.raises(SealedReleaseInvalid) as refusal:
        with private_pycache_prefix() as flags:
            prefix = Path(flags[1].removeprefix("pycache_prefix="))
    assert refusal.value.data["code"] == SEALED_BYTECODE_REASON
    assert "could not be removed" in str(refusal.value)

    # A failure while another error propagates is logged, not masked.
    with caplog.at_level("WARNING"):
        with pytest.raises(RuntimeError, match="launch failed"):
            with private_pycache_prefix():
                raise RuntimeError("launch failed")
    assert "could not be removed" in caplog.text
    monkeypatch.undo()
    for leftover in Path(tempfile.gettempdir()).glob("okg-sealed-pycache-*"):
        if leftover == prefix or leftover.name in caplog.text:
            shutil.rmtree(leftover)


def test_interpreter_description_refuses_prefix_inside_the_runtime_tree(
    tmp_path: Path, monkeypatch
) -> None:
    tree = tmp_path / "runtime"
    (tree / "tmp").mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(tree / "tmp"))
    monkeypatch.setattr(
        runtime,
        "_run_release_command",
        lambda *_a, **_k: pytest.fail("interpreter started"),
    )
    tree_fd = os.open(tree, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(SealedReleaseInvalid) as refusal:
            runtime._describe_interpreter(
                "enrolled-python",
                cwd=tmp_path,
                environ={},
                sealed_root_fds=(tree_fd,),
            )
    finally:
        os.close(tree_fd)
    assert refusal.value.data["code"] == SEALED_BYTECODE_REASON


def test_runtime_interpreter_description_launch_sets_private_pycache_prefix(
    tmp_path: Path, monkeypatch
) -> None:
    seen: list[list[str]] = []

    def record(command, **_kwargs):
        seen.append(list(command))
        index = command.index("-X")
        prefix = Path(command[index + 1].removeprefix("pycache_prefix="))
        assert prefix.is_dir() and list(prefix.iterdir()) == []
        assert stat.S_IMODE(prefix.stat().st_mode) == 0o700
        return json.dumps({"major": 3})

    monkeypatch.setattr(runtime, "_run_release_command", record)
    assert runtime._describe_interpreter(
        "enrolled-python", cwd=tmp_path, environ={}
    ) == {"major": 3}
    (command,) = seen
    assert command[:5] == ["enrolled-python", *_SEALED_FLAGS]
    assert command[5] == "-X"
    assert command[6].startswith("pycache_prefix=")
    assert command[7] == "-c"
    assert not Path(command[6].removeprefix("pycache_prefix=")).exists()


def test_descriptor_startup_child_argv_sets_private_pycache_prefix(
    tmp_path: Path, monkeypatch
) -> None:
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

    def member(relative: str, payload: bytes, mode: int):
        return sealed_release_impl.RuntimeAuthorityMember(
            relative,
            mode,
            len(payload),
            str(runtime._raw_blob(payload)["digest"]),
        )

    native_members = tuple(
        (name, member(name.removeprefix("venv/"), name.encode("utf-8"), 0o444))
        for name in native_names
    )

    class DescriptorExecSupport:
        fexecve = object()

    monkeypatch.setattr(
        sealed_release_impl.ctypes,
        "CDLL",
        lambda *_args, **_kwargs: DescriptorExecSupport(),
    )
    argv_record = tmp_path / "child-argv.json"

    def fake_exec(_fd, argv, _environ):
        prefix = Path(argv[argv.index("-X") + 1].removeprefix("pycache_prefix="))
        argv_record.write_text(
            json.dumps(
                {
                    "argv": argv,
                    "prefix_empty": prefix.is_dir() and not any(prefix.iterdir()),
                    "prefix_mode": stat.S_IMODE(prefix.stat().st_mode),
                }
            ),
            encoding="utf-8",
        )
        os.read(3, 65536)
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
        sealed_release_impl._descriptor_startup_proof(
            root_fd,
            "venv/bin/python3.12",
            python_abi="cp312",
            interpreter_member=member("bin/python3.12", interpreter_payload, 0o555),
            verifier_relative_name=(
                "venv/lib/python3.12/site-packages/okg_release_runtime_verify.py"
            ),
            verifier_member=member(
                "lib/python3.12/site-packages/okg_release_runtime_verify.py",
                runtime.RUNTIME_RELOCATION_VERIFIER_SOURCE,
                0o444,
            ),
            tk_native_members=native_members,
        )
    finally:
        os.close(root_fd)
    record = json.loads(argv_record.read_text(encoding="utf-8"))
    argv = record["argv"]
    assert argv[1:5] == _SEALED_FLAGS
    assert argv[5] == "-X" and argv[6].startswith("pycache_prefix=")
    assert argv[7] == "/proc/self/fd/9"
    prefix = Path(argv[6].removeprefix("pycache_prefix="))
    assert record["prefix_empty"] is True
    assert record["prefix_mode"] == 0o700
    assert root.resolve() not in prefix.parents
    assert not prefix.exists()


# --- defence 2: verification refuses bytecode anywhere in the tree ----------


@pytest.mark.parametrize(
    "planted",
    [
        "venv/lib/python3.12/encodings/__pycache__/utf_8.cpython-312.pyc",
        "venv/lib/python3.12/site-packages/sitecustomize.pyc",
        "repo/src/pkg/__pycache__",
        "repo/src/pkg/module.PYO",
    ],
)
def test_verification_refuses_bytecode_planted_after_sealing(
    tmp_path: Path, planted: str
) -> None:
    repo, commit = _fixture_repo(tmp_path)
    release = _prepare(repo, commit, tmp_path / "artifacts", _fake_sync("one", []))
    assert verify_sealed_release(release.artifact_root) == release

    target = release.artifact_root / planted
    _unseal_directories(release.artifact_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    if planted.endswith("__pycache__"):
        target.mkdir()
    else:
        target.write_bytes(b"planted bytecode")

    with pytest.raises(SealedReleaseInvalid) as refusal:
        verify_sealed_release(release.artifact_root)
    assert refusal.value.data["code"] == SEALED_BYTECODE_REASON
    assert planted in refusal.value.data["observed"]
    with pytest.raises(SealedReleaseInvalid) as direct:
        executable_tree_digest(release.artifact_root)
    assert direct.value.data["code"] == SEALED_BYTECODE_REASON


def test_sealing_removes_bytecode_the_dependency_sync_wrote(tmp_path: Path) -> None:
    repo, commit = _fixture_repo(tmp_path)
    inner = _fake_sync("one", [])

    def sync_with_bytecode(repo_root: Path, venv_root: Path, environ) -> None:
        inner(repo_root, venv_root, environ)
        package = venv_root / "lib/python3.12/site-packages/okg"
        cache = package / "__pycache__"
        cache.mkdir()
        (cache / "__init__.cpython-312.pyc").write_bytes(b"upstream bytecode")
        (repo_root / "stray.pyc").write_bytes(b"stray")
        # Case variants and non-directory __pycache__ entries (#2857 review).
        # A separate directory: macOS folds __PYCACHE__ onto __pycache__.
        (venv_root / "lib/__PYCACHE__").mkdir()
        (venv_root / "lib/__PYCACHE__/x.pyc").write_bytes(b"upper")
        (package / "Mixed.PYO").write_bytes(b"upper suffix")
        (repo_root / "__pycache__").write_bytes(b"a file named __pycache__")
        (repo_root / "linked").mkdir()
        (repo_root / "linked/__pycache__").symlink_to("..")

    release = _prepare(repo, commit, tmp_path / "artifacts", sync_with_bytecode)
    leftovers = [
        path.relative_to(release.artifact_root).as_posix()
        for path in release.artifact_root.rglob("*")
        if is_bytecode_relative_name(path.name)
    ]
    assert leftovers == []
    assert (release.artifact_root / "venv/lib/python3.12").is_dir()
    assert verify_sealed_release(release.artifact_root) == release


def test_fd_tree_snapshot_refuses_bytecode(tmp_path: Path) -> None:
    repo, commit = _fixture_repo(tmp_path)
    release = _prepare(repo, commit, tmp_path / "artifacts", _fake_sync("one", []))
    cache = release.artifact_root / "venv/lib/python3.12/site-packages/okg"
    _unseal_directories(release.artifact_root)
    (cache / "__pycache__").mkdir()
    root_fd = os.open(release.artifact_root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(SealedReleaseInvalid) as refusal:
            sealed_release_impl._release_tree_snapshot_at(root_fd)
    finally:
        os.close(root_fd)
    assert refusal.value.data["code"] == SEALED_BYTECODE_REASON
    assert refusal.value.data["observed"] == [
        "venv/lib/python3.12/site-packages/okg/__pycache__"
    ]


def test_launcher_refuses_bytecode_planted_after_activation(tmp_path: Path) -> None:
    repo, commit = _fixture_repo(tmp_path)
    environment, release, values = _prepare_environment_fixture(
        repo, commit, tmp_path / "state"
    )
    activate_environment_release(
        environment=environment,
        release=release,
        source_repo=repo,
        activated_by="test",
        environ=values,
    )
    assert release_launcher._executable_tree_digest(release.artifact_root)
    planted = (
        release.artifact_root
        / "venv/lib/python3.12/encodings/__pycache__/utf_8.cpython-312.pyc"
    )
    _unseal_directories(release.artifact_root)
    planted.parent.mkdir(parents=True)
    planted.write_bytes(b"planted bytecode")
    launcher = Path(release_launcher.__file__).resolve()

    proc = subprocess.run(
        [
            sys.executable,
            "-I",
            str(launcher),
            "--descriptor",
            str(launcher_descriptor_path(environment)),
            "--verify-only",
            "--json",
        ],
        env=values,
        check=False,
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 2
    error = json.loads(proc.stderr)["error"]
    assert "sealed_runtime_bytecode_present" in error["message"]
    assert "utf_8.cpython-312.pyc" in error["message"]


def test_prefix_copy_proof_lists_upstream_bytecode_as_excluded_not_copied() -> None:
    members = [
        {"relative_name": "bin/python3.12", "kind": "file", "mode": 0o555},
        {"relative_name": "lib/python3.12/encodings", "kind": "directory"},
        {
            "relative_name": "lib/python3.12/encodings/__pycache__",
            "kind": "directory",
        },
        {
            "relative_name": (
                "lib/python3.12/encodings/__pycache__/utf_8.cpython-312.pyc"
            ),
            "kind": "file",
            "mode": 0o644,
        },
        {"relative_name": "lib/python3.12/encodings/utf_8.py", "kind": "file"},
    ]

    copied, excluded = runtime.runtime_prefix_copy_entries(members)

    assert [item["runtime_relative_name"] for item in copied] == [
        "bin/python3.12",
        "lib/python3.12/encodings",
        "lib/python3.12/encodings/utf_8.py",
    ]
    assert excluded == [
        "lib/python3.12/encodings/__pycache__",
        "lib/python3.12/encodings/__pycache__/utf_8.cpython-312.pyc",
    ]
    assert runtime.RUNTIME_PREFIX_COPY_STATUS == (
        "exact-prefix-without-bytecode-plus-declared-additions"
    )


@pytest.mark.parametrize(
    "name",
    [
        "a/__pycache__",
        "a/__PyCache__/b.txt",
        "a/b.pyc",
        "a/b.PYC",
        "a/b.opt-1.pyo",
        "a/b.py",
        "a/pycache",
        "a/b.pyc.txt",
        "__pycache__",
    ],
)
def test_launcher_and_integrity_share_one_bytecode_predicate(name: str) -> None:
    """The stdlib-only launcher cannot import okg, so it carries a copy."""

    assert release_launcher._is_bytecode_relative_name(
        name
    ) == is_bytecode_relative_name(name)


def test_construction_receipt_excludes_upstream_bytecode(
    tmp_path: Path, monkeypatch
) -> None:
    """A real upstream prefix ships __pycache__; the receipt must not list it."""

    from tests.substrate.test_sealed_release_runtime import _producer_proof, _sha

    upstream = "lib/python3.12/encodings/__pycache__/utf_8.cpython-312.pyc"
    original = runtime._runtime_prefix_members

    def plant_then_list(prefix: Path):
        if prefix.as_posix().endswith("python/install"):
            planted = prefix / upstream
            planted.parent.mkdir(parents=True, exist_ok=True)
            planted.write_bytes(b"upstream bytecode")
            (prefix / "lib/python3.12/encodings/utf_8.py").write_bytes(b"# codec\n")
        return original(prefix)

    monkeypatch.setattr(runtime, "_runtime_prefix_members", plant_then_list)
    _document, distribution = _producer_proof(tmp_path / "producer-proof")
    monkeypatch.undo()
    distribution_names = [
        str(item["relative_name"]) for item in distribution.distribution_members
    ]
    assert upstream in distribution_names

    repo = tmp_path / "draft/repo"
    venv = tmp_path / "draft/venv"
    repo.mkdir(parents=True)
    distribution.copy_prefix_to(venv)
    (venv / "bin/python3.12").chmod(0o755)
    verifier = (
        venv
        / "lib/python3.12/site-packages"
        / f"{runtime.RUNTIME_RELOCATION_VERIFIER_MODULE}.py"
    )
    verifier.parent.mkdir(parents=True)
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

    runtime_names = [str(item["relative_name"]) for item in document["runtime_members"]]
    assert not any(is_bytecode_relative_name(name) for name in runtime_names)
    assert "lib/python3.12/encodings/utf_8.py" in runtime_names
    proof = document["prefix_copy_proof"]
    assert proof["excluded_bytecode_entries"] == [
        "lib/python3.12/encodings/__pycache__",
        upstream,
    ]
    assert proof["status"] == runtime.RUNTIME_PREFIX_COPY_STATUS
    assert document["release_additions_manifest"]["entries"] == [
        item
        for item in document["runtime_members"]
        if item["relative_name"] not in distribution_names
    ]

    # Sealing removes the bytecode; the sealed prefix then matches the receipt
    # member for member, and its tree digest is the one the receipt records.
    remove_runtime_caches(venv)
    seal_permissions(venv)
    sealed_fd = os.open(venv, os.O_RDONLY | os.O_DIRECTORY)
    try:
        sealed_members = runtime._runtime_prefix_members_at(
            sealed_fd, require_readonly=False
        )
    finally:
        os.close(sealed_fd)
    assert sealed_members == document["runtime_members"]
    assert document["receipt"]["runtime_prefix_tree_digest"] == (
        runtime._canonical_digest(
            {
                "schema_version": runtime.RUNTIME_PREFIX_TREE_SCHEMA,
                "entries": sealed_members,
            }
        )
    )
