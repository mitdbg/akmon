#!/usr/bin/env python3
"""Install the pinned TLC runner for the admission formal model."""
from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.7.4"
URL = (
    "https://github.com/tlaplus/tlaplus/releases/download/"
    f"v{VERSION}/tla2tools.jar"
)
SHA256 = "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
DEFAULT_DEST = REPO_ROOT / ".tools" / "tla2tools.jar"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)

    dest = args.dest.resolve()
    if dest.exists() and not args.force:
        actual = _sha256(dest.read_bytes())
        if actual == SHA256:
            print(f"formal-admission-bootstrap: {dest} already installed")
            return 0
        print(
            f"formal-admission-bootstrap: {dest} exists but has "
            f"sha256={actual}; use --force to replace it",
            file=sys.stderr,
        )
        return 2

    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(URL, timeout=60) as response:
        payload = response.read()
    actual = _sha256(payload)
    if actual != SHA256:
        print(
            "formal-admission-bootstrap: checksum mismatch for "
            f"TLC {VERSION}: expected {SHA256}, got {actual}",
            file=sys.stderr,
        )
        return 2
    dest.write_bytes(payload)
    print(f"formal-admission-bootstrap: installed TLC {VERSION} at {dest}")
    return 0


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
