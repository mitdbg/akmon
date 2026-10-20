#!/usr/bin/env python3
"""Build AKMON's documentation as one release bundle."""

from __future__ import annotations

import hashlib
import html
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath

import yaml


ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "documentation.yaml"
SCRATCH = (ROOT / ".scratch").resolve()


def _load_inventory() -> dict:
    data = yaml.safe_load(INVENTORY.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("version") != 1:
        raise RuntimeError("documentation.yaml must use schema version 1")
    return data


def _scratch_path(raw: str) -> Path:
    path = (ROOT / raw).resolve()
    if SCRATCH not in path.parents:
        raise RuntimeError(f"refusing to write outside .scratch: {path}")
    return path


def _reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def _build(config: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "-f", config, "--strict", "--clean"],
        cwd=ROOT,
        check=True,
    )


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _rewrite_public_url(root: Path, placeholder: str, public_url: str) -> None:
    public_url = public_url.rstrip("/") + "/"
    for pattern in ("*.html", "*.xml"):
        for path in root.rglob(pattern):
            text = path.read_text(encoding="utf-8")
            if placeholder in text:
                path.write_text(text.replace(placeholder, public_url), encoding="utf-8")


def _published_url(markdown_path: str) -> str:
    path = PurePosixPath(markdown_path)
    if path.suffix == ".md":
        path = path.with_suffix("")
    parts = list(path.parts)
    if parts and parts[-1].lower() in {"index", "readme"}:
        parts.pop()
    clean = "/" + "/".join(part for part in parts if part != "/")
    return clean.rstrip("/") + "/"


def _redirect_output(bundle: Path, old_path: str) -> Path:
    path = PurePosixPath(old_path)
    if path.suffix == ".md":
        path = path.with_suffix("")
    parts = list(path.parts)
    if parts and parts[-1].lower() in {"index", "readme"}:
        parts.pop()
    return bundle.joinpath(*parts, "index.html")


def _write_redirects(bundle: Path, redirects: dict[str, str]) -> None:
    for old_path, target in sorted(redirects.items()):
        output = _redirect_output(bundle, old_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        url = _published_url(target)
        escaped = html.escape(url, quote=True)
        output.write_text(
            "<!doctype html>\n"
            '<meta charset="utf-8">\n'
            f'<link rel="canonical" href="{escaped}">\n'
            f'<meta http-equiv="refresh" content="0; url={escaped}">\n'
            f'<p>This page moved to <a href="{escaped}">{escaped}</a>.</p>\n'
            f'<script>location.replace({json.dumps(url)});</script>\n',
            encoding="utf-8",
        )


def build_bundle() -> Path:
    inventory = _load_inventory()
    publications = inventory["publications"]
    atomic = inventory["atomic_bundle"]
    product = publications["product"]

    product_dir = _scratch_path(product["build_dir"])
    bundle = _scratch_path(atomic["site_dir"])

    _build(product["config"])
    _rewrite_public_url(
        product_dir,
        "https://docs.example.invalid/",
        os.environ.get("DOCS_SITE_URL", "https://docs.example.invalid/"),
    )
    _reset(bundle)

    shutil.copytree(product_dir, bundle, dirs_exist_ok=True)
    shutil.copy2(ROOT / product["machine_index"], bundle / "llms.txt")
    _write_redirects(bundle, inventory["redirects"])

    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    manifest = {
        "schema": "akmon.docs.release/v1",
        "commit": commit,
        "site": {
            "mount": atomic["product_mount"],
            "pages": len(product["pages"]),
            "sha256": _tree_digest(product_dir),
        },
        "redirects": len(inventory["redirects"]),
    }
    (bundle / "release-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return bundle


def main() -> int:
    bundle = build_bundle()
    print(f"built {bundle.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
