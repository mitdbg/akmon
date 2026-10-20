from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest

from okg.substrate.deployments import inject_deployment_path


def repo_root_from_test(path: Path) -> Path:
    return path.resolve().parents[2]


def deployment_path(repo_root: Path, deployment: str) -> Path:
    return repo_root / "deployments" / deployment


def optional_deployment_available(repo_root: Path, deployment: str) -> bool:
    return deployment_path(repo_root, deployment).is_dir()


def inject_optional_deployment_path(
    repo_root: Path,
    deployment: str,
) -> Path | None:
    path = deployment_path(repo_root, deployment)
    if not path.exists():
        return None
    inject_deployment_path(path)
    return path


def skip_if_optional_deployment_missing(
    repo_root: Path,
    deployment: str,
) -> Path:
    path = deployment_path(repo_root, deployment)
    if not path.exists():
        pytest.skip(f"deployments/{deployment} is not present in this checkout")
    return path


def should_ignore_optional_deployment_test(
    collection_path: Path,
    *,
    repo_root: Path,
    deployment: str,
    filenames: set[str] | None = None,
) -> bool:
    if optional_deployment_available(repo_root, deployment):
        return False
    path = Path(collection_path)
    if filenames is not None:
        return path.name in filenames
    return path.suffix == ".py" and path.name.startswith("test_")


def mark_optional_deployment_items(
    items: Iterable[pytest.Item],
    *,
    deployment: str,
    test_root: Path,
    filenames: set[str] | None = None,
) -> None:
    marker = pytest.mark.optional_deployment(
        reason=f"requires deployments/{deployment}"
    )
    root = Path(test_root).resolve()
    for item in items:
        path = Path(str(item.fspath)).resolve()
        if not path.is_relative_to(root):
            continue
        if filenames is not None and path.name not in filenames:
            continue
        item.add_marker(marker)
