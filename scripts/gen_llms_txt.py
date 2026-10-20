#!/usr/bin/env python3
"""Generate AKMON's machine-readable documentation index."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
INVENTORY = ROOT / "documentation.yaml"
FRONT_MATTER_RE = re.compile(r"\A---\n(.*?)\n---\n", re.DOTALL)
H1_RE = re.compile(r"^#\s+(.+?)\s*$", re.MULTILINE)
AUDIENCE_SECTIONS = [
    ("user", "Users"),
    ("data owner", "Data owners"),
    ("operator", "Operators"),
    ("developer", "Developers"),
    ("maintainer", "Maintainers"),
    ("coding agent", "Coding agents"),
]


def _inventory() -> dict:
    value = yaml.safe_load(INVENTORY.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("version") != 1:
        raise RuntimeError("documentation.yaml must use schema version 1")
    return value


def _split_front_matter(text: str) -> tuple[dict, str]:
    match = FRONT_MATTER_RE.match(text)
    if not match:
        return {}, text
    data = yaml.safe_load(match.group(1)) or {}
    return (data if isinstance(data, dict) else {}), text[match.end() :]


def _one_line_purpose(body: str) -> str:
    started = False
    for raw in body.splitlines():
        line = raw.strip()
        if not started:
            if line.startswith("# "):
                started = True
            continue
        if not line or line.startswith((">", "<!--", "---", "#", "|", "```")):
            continue
        if re.match(r"\*\*[^*]+:\*\*", line):
            continue
        line = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", line)
        sentence = re.split(r"(?<=[.!?])\s", line, maxsplit=1)[0]
        sentence = re.sub(r"\s+", " ", sentence).strip()
        return sentence if len(sentence) <= 160 else sentence[:157].rstrip() + "…"
    return ""


def _site_url(name: str, page: str) -> str:
    """Return the public site URL for an inventory page.

    The machine indexes are consumed by agents as links into the published
    documentation site, not as links into the repository checkout.  Keep the
    MkDocs' index-page rules in one place so generated links use one URL
    convention.
    """
    relative = page.removesuffix(".md")
    if relative in {"README", "index"}:
        return "/"
    if relative.endswith("/README") or relative.endswith("/index"):
        relative = relative.rsplit("/", 1)[0]
    return f"/{relative}/"


def _entry(name: str, source_dir: str, page: str) -> dict:
    path = ROOT / source_dir / page
    text = path.read_text(encoding="utf-8")
    front, body = _split_front_matter(text)
    h1 = H1_RE.search(body)
    return {
        "path": f"{source_dir}/{page}",
        "url": _site_url(name, page),
        "title": h1.group(1).strip() if h1 else page,
        "audiences": [
            item.strip()
            for item in str(front.get("audience", "")).split(",")
            if item.strip()
        ],
        "type": front.get("type", ""),
        "canonical_id": front.get("canonical_id", ""),
        "purpose": _one_line_purpose(body),
    }


def build_index(name: str, publication: dict) -> str:
    entries = [
        _entry(name, publication["source_dir"], page)
        for page in publication["pages"]
    ]
    title = "AKMON documentation"
    lines = [
        f"# {title}",
        "",
        "> Generated from documentation.yaml and page metadata.",
        "",
    ]
    for key, heading in AUDIENCE_SECTIONS:
        section = sorted(
            (entry for entry in entries if key in entry["audiences"]),
            key=lambda entry: entry["path"],
        )
        if not section:
            continue
        lines.append(f"## {heading}")
        for entry in section:
            purpose = f": {entry['purpose']}" if entry["purpose"] else ""
            lines.append(
                f"- [{entry['title']}]({entry['url']}){purpose} "
                f"(type: {entry['type']}, id: {entry['canonical_id']})"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    check = "--check" in argv
    publication = _inventory()["publications"]["product"]
    outputs = {
        ROOT / publication["machine_index"]: build_index("product", publication),
    }
    stale: list[Path] = []
    for path, content in outputs.items():
        if check:
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            if current != content:
                stale.append(path)
        else:
            path.write_text(content, encoding="utf-8")
            print(f"wrote {path.relative_to(ROOT)}")
    if stale:
        names = ", ".join(path.relative_to(ROOT).as_posix() for path in stale)
        print(
            f"ERROR: stale documentation indexes: {names}; "
            "run uv run python scripts/gen_llms_txt.py",
            file=sys.stderr,
        )
        return 1
    if check:
        print("documentation machine indexes: in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
