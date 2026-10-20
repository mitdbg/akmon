"""The generated documentation index is a fresh, valid artifact."""
from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
LLMS_TXT = ROOT / "okg-llms.txt"


def _published_page(root: Path, target: str) -> Path:
    relative = target.strip("/")
    if not relative:
        return root / "README.md" if root.name == "docs" else root / "index.md"
    direct = root / f"{relative}.md"
    if direct.exists():
        return direct
    for name in ("index.md", "README.md"):
        candidate = root / relative / name
        if candidate.exists():
            return candidate
    return root / relative / "index.md"


def _gen():
    path = ROOT / "scripts/gen_llms_txt.py"
    spec = importlib.util.spec_from_file_location("gen_llms_txt", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_okg_llms_txt_exists_and_in_sync() -> None:
    assert LLMS_TXT.exists()
    result = subprocess.run(
        [sys.executable, "scripts/gen_llms_txt.py", "--check"],
        cwd=ROOT, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_okg_llms_txt_links_all_resolve() -> None:
    text = LLMS_TXT.read_text(encoding="utf-8")
    targets = re.findall(r"\]\(([^)]+)\)", text)
    assert targets, "expected linked pages"
    for target in targets:
        assert target.startswith("/") and target.endswith("/")
        path = _published_page(ROOT / "docs", target)
        assert path.exists(), f"dead link in okg-llms.txt: {target}"


def test_okg_llms_txt_grouped_by_audience() -> None:
    text = LLMS_TXT.read_text(encoding="utf-8")
    for heading in ("## Users", "## Data owners", "## Operators", "## Developers"):
        assert heading in text


def test_okg_llms_txt_excludes_historical_pages() -> None:
    text = LLMS_TXT.read_text(encoding="utf-8")
    targets = re.findall(r"\]\((/[^)]+/)\)", text)
    for target in targets:
        path = _published_page(ROOT / "docs", target)
        raw = path.read_text(encoding="utf-8")
        match = re.match(r"\A---\n(.*?)\n---\n", raw, re.DOTALL)
        front = yaml.safe_load(match.group(1)) if match else {}
        assert front.get("status") != "historical", target
    introduction = text.split("## ", 1)[0].lower()
    assert "bitemporal" not in introduction


def test_index_covers_product_and_runbook_routes_without_legacy_wrappers() -> None:
    text = LLMS_TXT.read_text(encoding="utf-8")
    assert text.startswith("# AKMON documentation\n")
    assert re.search(r"^- \[[^]]+\]\(/\):", text, re.MULTILINE)
    assert "- [The AKMON model](/start/concepts/):" in text
    assert "- [Runbooks](/runbooks/):" in text
    assert "- [Evaluate AKMON from source](/start/evaluate-from-source/):" in text
    assert "/handbook/" not in text
    for legacy in ("/start-here/", "/add-update-data/", "/run-akmon/"):
        assert legacy not in text


def test_no_llms_full_txt_shipped() -> None:
    # Red-team cut: the drift-prone full bundle is not shipped.
    assert not (ROOT / "llms-full.txt").exists()
    assert not (ROOT / "okg-llms-full.txt").exists()


def test_generator_is_deterministic() -> None:
    gen = _gen()
    inventory = gen._inventory()["publications"]
    assert gen.build_index("product", inventory["product"]) == gen.build_index(
        "product", inventory["product"]
    )
