"""Assemble a trusted producer's retained wheels into one operator entry point.

The producer supplies independently pinned framework/consumer wheels and a
hash manifest for its locked dependency wheelhouse. Dependency source builds
are an explicit upstream producer step, never installer-time execution. The
retained wheelhouse manifest records original source hashes, build tools and
any derived wheel hashes; this assembly makes no reproducibility or publisher
signature claim.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import zipfile

from .install_launcher import _open_regular
from .package import DistributionPackageError


def _copy(source: Path, pin: str, destination: Path, logical: str) -> None:
    with _open_regular(source.absolute()) as stream:
        body = stream.read()
    if "sha256:" + hashlib.sha256(body).hexdigest() != pin:
        raise DistributionPackageError(
            "release_artifact_digest_mismatch", logical,
            "release artifact digest differs from its pin")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with destination.open("xb") as stream:
        stream.write(body)
    destination.chmod(0o600)


def build_bundle(*, framework: Path, framework_digest: str, consumer: Path,
                 consumer_digest: str, consumer_package: str, wheelhouse: Path,
                 wheelhouse_digest: str, installer: Path, installer_digest: str,
                 output: Path) -> Path:
    """Publish an entirely new local release directory; never adopt one."""
    if not consumer_package.isidentifier() or consumer_package == "okg":
        raise DistributionPackageError(
            "release_consumer_package_invalid", "consumer",
            "a distinct consumer package is required")
    output = output.absolute()
    if output.exists() or output.is_symlink():
        raise DistributionPackageError(
            "release_output_exists", "", "release output already exists")
    output.mkdir(mode=0o700)
    temporary = Path(tempfile.mkdtemp(prefix=".install-release-", dir=output))
    completed = False
    try:
        _copy(framework, framework_digest, temporary / "framework" / framework.name, "framework")
        _copy(consumer, consumer_digest, temporary / "consumer" / consumer.name, "consumer")
        _copy(installer, installer_digest, temporary / "installer" / installer.name, "installer")
        _copy(wheelhouse / "manifest.json", wheelhouse_digest,
              temporary / "dependencies/manifest.json", "dependencies/manifest.json")
        manifest = json.loads((temporary / "dependencies/manifest.json").read_text())
        if not isinstance(manifest, dict) or not isinstance(manifest.get("entries"), list):
            raise DistributionPackageError(
                "release_wheelhouse_manifest_malformed", "dependencies/manifest.json",
                "dependency wheelhouse manifest malformed")
        dependency_digest = manifest.get("requirements_digest")
        _copy(wheelhouse / "runtime-wheels.txt", dependency_digest,
              temporary / "dependencies/runtime-wheels.txt", "dependencies/runtime-wheels.txt")
        names = set()
        for entry in manifest["entries"]:
            if not isinstance(entry, dict):
                raise DistributionPackageError(
                    "release_wheelhouse_entry_malformed", "dependencies/manifest.json",
                    "dependency wheelhouse entry malformed")
            name = entry.get("wheel")
            if not isinstance(name, str) or Path(name).name != name or not name.endswith(".whl") or name in names:
                raise DistributionPackageError(
                    "release_wheel_name_invalid", "dependencies/wheels",
                    "dependency wheel filename malformed or duplicate")
            names.add(name)
            _copy(wheelhouse / "wheels" / name, entry.get("wheel_digest"),
                  temporary / "dependencies/wheels" / name, "dependencies/wheels/" + name)
        if not names:
            raise DistributionPackageError(
                "release_wheelhouse_empty", "dependencies/wheels",
                "empty dependency wheelhouse")
        with zipfile.ZipFile(temporary / "framework" / framework.name) as wheel:
            launcher = wheel.read("okg/substrate/distributions/install_launcher.py")
        (temporary / "install_launcher.py").write_bytes(launcher)
        launcher_digest = hashlib.sha256(launcher).hexdigest()
        arguments = {
            "framework-wheel": "framework/" + framework.name, "framework-digest": framework_digest,
            "consumer-wheel": "consumer/" + consumer.name, "consumer-digest": consumer_digest,
            "consumer-package": consumer_package,
            "dependency-lock": "dependencies/runtime-wheels.txt", "dependency-digest": dependency_digest,
            "dependency-wheelhouse": "dependencies/wheels", "wheelhouse-manifest": "dependencies/manifest.json",
            "wheelhouse-digest": wheelhouse_digest, "installer-wheel": "installer/" + installer.name,
            "installer-digest": installer_digest,
        }
        paths = {key for key in arguments if key.endswith("wheel") or key in {"dependency-lock", "dependency-wheelhouse", "wheelhouse-manifest"}}
        entry = '''# Trusted release bootstrap; Python, host and this release entry are trusted.
import argparse, hashlib, os, pathlib, sys
root = pathlib.Path(__file__).resolve().parent
fixed = FIXED
paths = PATHS
parser = argparse.ArgumentParser(description="Install this pinned release through the existing OKG install flow", allow_abbrev=False)
parser.add_argument("--target", required=True)
options, remaining = parser.parse_known_args()
reserved = set(fixed) | {"environment"}
if any(arg.startswith("--") and any(name.startswith(arg[2:].split("=", 1)[0]) for name in reserved) for arg in remaining):
    parser.error("release pins and environment ownership cannot be overridden")
launcher = root / "install_launcher.py"
if launcher.is_symlink() or hashlib.sha256(launcher.read_bytes()).hexdigest() != LAUNCHER:
    parser.error("release launcher bytes changed")
arguments = []
for key, value in fixed.items():
    arguments += ["--" + key, str(root / value) if key in paths else value]
target = pathlib.Path(options.target).absolute()
environment = target.with_name(target.name + ".environment")
os.execv(sys.executable, [sys.executable, "-I", "-B", str(launcher), *arguments,
    "--environment", str(environment), "--target", str(target), *remaining])
'''.replace("FIXED", repr(arguments)).replace("PATHS", repr(sorted(paths))).replace("LAUNCHER", repr(launcher_digest))
        (temporary / "install.py").write_text(entry)
        (temporary / "release.json").write_text(json.dumps({"schema": "okg.install-release/v1", "pins": arguments,
            "install_entry_sha256": hashlib.sha256(entry.encode()).hexdigest(), "launcher_sha256": launcher_digest,
            "trust": "host, interpreter, release producer and declared dependency build tools"}, indent=2) + "\n")
        for item in temporary.iterdir():
            os.rename(item, output / item.name)
        temporary.rmdir()
        completed = True
        return output / "install.py"
    finally:
        if not completed:
            shutil.rmtree(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("framework", "framework-digest", "consumer", "consumer-digest", "consumer-package",
                 "wheelhouse", "wheelhouse-digest", "installer", "installer-digest", "output"):
        parser.add_argument("--" + name, required=True)
    values = vars(parser.parse_args())
    for key in ("framework", "consumer", "wheelhouse", "installer", "output"):
        values[key] = Path(values[key])
    print(build_bundle(**values))


if __name__ == "__main__":
    main()
