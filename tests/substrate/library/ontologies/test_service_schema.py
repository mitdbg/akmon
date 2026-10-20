"""The service module's `Service` is findable by name (#2158 part 1).

`Service` declared no `okg_searchable_text_fields`, so the projector stamped
no `attrs.text` for a service node and `search` could reach it only by id
(the OpenSearch reference walk had to enter at the index instead). It now
declares `name`. No database, no container.
"""
from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


def test_service_declares_its_name_as_searchable_text(tmp_path: Path) -> None:
    root = tmp_path / "service-only"
    (root / "schemas").mkdir(parents=True)
    (root / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "service-test", "modules": ["service"]}),
        encoding="utf-8",
    )
    composed = compose_catalog(root)

    assert composed.subtype_metadata["service"]["searchable_text_fields"] == ["name"]
