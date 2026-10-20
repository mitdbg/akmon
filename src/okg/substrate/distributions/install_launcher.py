"""Stdlib bootstrap for the existing ``okg install`` command.

The interpreter, uv, dependency lock and Python startup environment are trusted
bootstrap inputs. Release tooling exports the hash-locked requirements from
uv.lock; an operator does not maintain a second dependency list. Selected
framework and consumer wheels never resolve their own dependencies.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile

_MAX_INPUT = 512 * 1024 * 1024


def _open_regular(path: Path):
    """Open through non-symlink ancestors; read the same descriptor we check."""
    path = path.absolute()
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        opened = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    info = os.fstat(opened)
    if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_INPUT:
        os.close(opened)
        raise ValueError("bootstrap input must be a bounded regular file")
    return os.fdopen(opened, "rb")


def _retain(path: Path, pin: str, directory: Path) -> Path:
    """Verify bytes while copying into a private, exclusive retained file."""
    if re.fullmatch(r"sha256:[0-9a-f]{64}", pin) is None:
        raise ValueError("a full independently supplied SHA256 is required")
    target = directory / path.name
    digest = hashlib.sha256()
    size = 0
    with _open_regular(path) as source, target.open("xb") as destination:
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            if size > _MAX_INPUT:
                raise ValueError("bootstrap input exceeds its limit")
            digest.update(chunk)
            destination.write(chunk)
    if "sha256:" + digest.hexdigest() != pin:
        raise ValueError("bootstrap input differs from supplied pin")
    target.chmod(0o400)
    return target


def _record(marker: Path, ownership: dict[str, str], state: str) -> None:
    descriptor, name = tempfile.mkstemp(prefix=".okg-owner-", dir=marker.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump({"ownership": ownership, "state": state}, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(marker)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _environment_lock(environment: Path):
    if ".." in environment.parts:
        raise ValueError("environment path must not contain parent traversal")
    # The sibling lock exists before the environment does and survives retries.
    key = hashlib.sha256(str(environment).encode()).hexdigest()
    path = environment.parent / (".okg-install-" + key + ".lock")
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("installation lock is foreign")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("installation environment is busy") from exc
        yield
    finally:
        os.close(descriptor)


def _uv(command: list[str]) -> None:
    # Installer stderr may contain private index URLs. Never echo it, including
    # through CalledProcessError. The public refusal exposes only the stage.
    result = subprocess.run(command, capture_output=True, check=False)
    if result.returncode:
        raise ValueError("dependency or environment installation failed")


def main(argv: list[str] | None = None) -> int:
    # Match the child install parser's option recognition: an abbreviated
    # update selector must reach the ownership guard before environment setup.
    parser = argparse.ArgumentParser(allow_abbrev=True, description=__doc__)
    for name in ("framework-wheel", "framework-digest", "consumer-wheel", "consumer-digest",
                 "dependency-lock", "dependency-digest", "installer-wheel", "installer-digest", "environment"):
        parser.add_argument("--" + name, required=True)
    for name in ("dependency-wheelhouse", "wheelhouse-manifest", "wheelhouse-digest"):
        parser.add_argument("--" + name)
    parser.add_argument("--update-source")
    parser.add_argument("--search-query")
    parser.add_argument("--search-generation")
    for name in ("dry-run", "scaffold-only", "no-publish"):
        parser.add_argument("--" + name, action="store_true")
    options, remaining = parser.parse_known_args(argv)
    try:
        read_intent = options.search_query is not None or options.search_generation is not None
        staging = options.dry_run or options.scaffold_only or options.no_publish
        if read_intent and (not options.search_query or not options.search_query.strip()
                or options.update_source is not None or staging
                or (options.search_generation is not None and not options.search_generation.strip())):
            raise ValueError("unsupported installed search combination")
        if options.update_source is not None and staging:
            raise ValueError("unsupported installed update combination")
        existing_only = options.update_source is not None or read_intent
        for name in ("dry_run", "scaffold_only", "no_publish"):
            if getattr(options, name):
                remaining = ["--" + name.replace("_", "-"), *remaining]
        environment = Path(options.environment).absolute()
        for parent in (environment.parent, *environment.parent.parents):
            if parent.is_symlink():
                raise ValueError("environment ancestor is a symlink")
        ownership = {"schema": "okg.install-environment/v1", "framework": options.framework_digest,
                     "consumer": options.consumer_digest, "dependencies": options.dependency_digest,
                     "installer": options.installer_digest}
        wheelhouse_inputs = (options.dependency_wheelhouse, options.wheelhouse_manifest, options.wheelhouse_digest)
        if any(wheelhouse_inputs) and not all(wheelhouse_inputs):
            raise ValueError("wheelhouse directory and pinned manifest are required together")
        if all(wheelhouse_inputs):
            ownership["wheelhouse"] = options.wheelhouse_digest
        marker = environment / ".okg-install-owner.json"
        installed = False
        # Retain inputs before creating or changing the selected environment.
        with _environment_lock(environment), tempfile.TemporaryDirectory(
            prefix="okg-pinned-install-", dir=environment.parent,
        ) as scratch:
            retained = Path(scratch)
            for label in ("framework", "consumer", "dependencies", "installer"):
                (retained / label).mkdir(mode=0o700)
            framework = _retain(Path(options.framework_wheel).absolute(), options.framework_digest, retained / "framework")
            consumer = _retain(Path(options.consumer_wheel).absolute(), options.consumer_digest, retained / "consumer")
            dependencies = _retain(Path(options.dependency_lock).absolute(), options.dependency_digest, retained / "dependencies")
            installer = _retain(Path(options.installer_wheel).absolute(), options.installer_digest, retained / "installer")
            wheelhouse_args = []
            if all(wheelhouse_inputs):
                (retained / "wheelhouse-manifest").mkdir(mode=0o700)
                manifest_path = _retain(Path(options.wheelhouse_manifest).absolute(), options.wheelhouse_digest, retained / "wheelhouse-manifest")
                manifest = json.loads(manifest_path.read_text())
                if not isinstance(manifest, dict) or manifest.get("requirements_digest") != options.dependency_digest:
                    raise ValueError("wheelhouse requirements identity differs")
                entries = manifest.get("entries")
                if not isinstance(entries, list) or not entries or len(entries) > 1024:
                    raise ValueError("wheelhouse inventory invalid")
                wheelhouse = retained / "wheelhouse"
                wheelhouse.mkdir(mode=0o700)
                names = set()
                for entry in entries:
                    if not isinstance(entry, dict):
                        raise ValueError("wheelhouse entry invalid")
                    name = entry["wheel"]
                    if type(name) is not str or Path(name).name != name or not name.endswith(".whl") or name in names:
                        raise ValueError("wheelhouse member name invalid")
                    names.add(name)
                    _retain(Path(options.dependency_wheelhouse).absolute() / name, entry["wheel_digest"], wheelhouse)
                wheelhouse_args = ["--no-index", "--find-links", str(wheelhouse)]
            if framework.suffix != ".whl" or consumer.suffix != ".whl" or installer.suffix != ".whl":
                raise ValueError("framework and consumer inputs must be wheels")
            if environment.exists() or environment.is_symlink():
                if environment.is_symlink() or not environment.is_dir():
                    raise ValueError("environment is foreign")
                with _open_regular(marker) as stream:
                    recorded = json.load(stream)
                if (not isinstance(recorded, dict) or set(recorded) != {"ownership", "state"}
                        or recorded["ownership"] != ownership
                        or recorded["state"] not in ("preparing", "installed")):
                    raise ValueError("environment is foreign or its package pins changed")
                installed = recorded["state"] == "installed"
            else:
                if existing_only:
                    raise ValueError("operation requires an installed owned environment")
                environment.mkdir(mode=0o700)
                _record(marker, ownership, "preparing")
            if existing_only:
                if not installed:
                    raise ValueError("source update requires an installed owned environment")
                for name in ("update_source", "search_query", "search_generation"):
                    if getattr(options, name) is not None:
                        remaining = ["--" + name.replace("_", "-") + "=" + getattr(options, name), *remaining]
            python = environment / "bin/python"
            if not python.exists():
                if installed:
                    raise ValueError("owned environment lost its interpreter")
                _uv(["uv", "--no-config", "venv", "--allow-existing", "--python", sys.executable,
                     str(environment)])
            if not installed:
                # uv seeds only the independently pinned installer. pip owns
                # framework/consumer installation so uv_cache.json is not added
                # to their strictly checked distribution metadata directories.
                _uv(["uv", "--no-config", "pip", "install", "--no-compile", "--no-deps",
                     "--python", str(python), str(installer)])
                common = [str(python), "-I", "-B", "-m", "pip", "--isolated", "install", "--no-compile"]
                _uv([*common, *wheelhouse_args, "--require-hashes", "--only-binary", ":all:", "-r", str(dependencies)])
                _uv([*common, "--no-deps", str(framework), str(consumer)])
                _record(marker, ownership, "installed")
            # -B precedes every framework import; --no-compile alone is not enough.
            command = [str(python), "-I", "-B", "-c",
                       "from okg.substrate.distributions.install_execution import main; raise SystemExit(main())",
                       "--framework-wheel", str(framework), "--framework-digest", options.framework_digest,
                       "--consumer-wheel", str(consumer), "--consumer-digest", options.consumer_digest, *remaining]
            return subprocess.run(command, check=False).returncode
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as exc:
        print(json.dumps({"ok": False, "reason": type(exc).__name__,
                          "message": "pinned install launcher refused; inspect package pins and owned environment"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
