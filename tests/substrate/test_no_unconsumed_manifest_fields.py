"""Deployment manifests must not carry unconsumed fields.

Per `deployment-qa` capability (proposal `qa-stabilize-main`,
requirement "Deployment Manifests Have No Unconsumed Fields"):
a top-level key in `deployments/<name>/deployment.yaml` is a leak
when its name appears as a string literal nowhere in `src/okg/`
outside the manifest parser. Such fields lie to the operator about
deployment behavior.

Round-3 audit seed example: `health_gates: cms` /
`health_gates: knowledge_work` — both removed; this test prevents
recurrence.

Approach: any consumer somewhere in `src/okg/` either accesses
`manifest.<field>` (where `<field>` is named identically to the YAML
key by convention in DeploymentManifest.from_yaml) OR pulls the key
directly out of the parsed YAML via `raw.get("<key>")` /
`raw["<key>"]`. Both of those leave the key name as a literal in the
source. So we conservatively check: does the key name appear as a
quoted string literal anywhere in src/okg/ outside the parser file?
"""
from __future__ import annotations

import re
from pathlib import Path

import yaml

from okg.substrate.deployments.keys import DEPLOYMENT_KEYS, READ_BY_OKG

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENTS_DIR = REPO_ROOT / "deployments"
SRC_ROOT = REPO_ROOT / "src" / "okg"
PARSING_FILE = SRC_ROOT / "substrate" / "deployments" / "__init__.py"
# The key table names every key as a string, so it cannot witness a reader.
KEY_TABLE_FILE = SRC_ROOT / "substrate" / "deployments" / "keys.py"

# Keys the table in `okg.substrate.deployments.keys` records as read by the
# deployment's own code or by nothing. The table carries the reason for each,
# and `okg deployment lint` warns on the ones nothing reads; this test only
# guards the keys the table says okg reads.
PARSED_BUT_NOT_VIA_MANIFEST = frozenset(
    key.name for key in DEPLOYMENT_KEYS if key.read_by != READ_BY_OKG
)


def _key_appears_as_consumer(key: str) -> bool:
    """Conservatively True iff `key` appears somewhere in src/okg/
    outside the parser file as either:
      - a quoted string literal `'key'` or `"key"` (e.g. for
        `raw.get("key")` or `raw["key"]`), OR
      - an attribute access `.key` (e.g. `manifest.key`).

    The attribute-access path catches consumers that read through
    the DeploymentManifest dataclass; the string-literal path
    catches consumers that read directly from the parsed YAML or
    that reference the key by name in docs / log messages /
    arg-parser help.
    """
    pat_literal = re.compile(rf"""(['"]){re.escape(key)}\1""")
    # Attribute access: `.<key>` followed by a non-identifier char.
    # Use \b to anchor the end so `.modules_config` doesn't match
    # `.modules_configs` or similar.
    pat_attr = re.compile(rf"""\.{re.escape(key)}\b""")
    for path in SRC_ROOT.rglob("*.py"):
        if path in (PARSING_FILE, KEY_TABLE_FILE):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if pat_literal.search(text) or pat_attr.search(text):
            return True
    return False


def test_every_manifest_yaml_key_has_a_consumer() -> None:
    findings: list[str] = []

    for manifest in sorted(DEPLOYMENTS_DIR.glob("*/deployment.yaml")):
        with open(manifest) as f:
            raw = yaml.safe_load(f) or {}
        for key in raw.keys():
            if key in PARSED_BUT_NOT_VIA_MANIFEST:
                continue
            if not _key_appears_as_consumer(key):
                findings.append(
                    f"{manifest.relative_to(REPO_ROOT)}: top-level key "
                    f"{key!r} appears nowhere in src/okg/ outside the "
                    f"parser — leak (if intentionally parsed-but-not-"
                    f"read-via-manifest, add to "
                    f"PARSED_BUT_NOT_VIA_MANIFEST with a comment)"
                )

    if findings:
        raise AssertionError(
            "Unconsumed deployment-manifest field(s):\n  "
            + "\n  ".join(findings)
        )
