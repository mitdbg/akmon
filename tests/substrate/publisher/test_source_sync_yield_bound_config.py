"""``publisher.max_source_sync_yield_seconds`` in deployment.yaml.

PACT publisher-starvation-4039 (issue #4039, decision D10). The scheduled
publisher yields to source-sync workflow rows only until its freshness is older
than this bound. The value is a number >= 0 (default 600); a malformed value is
refused when the manifest loads and is never replaced by the default.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.deployments import (
    DEFAULT_MAX_SOURCE_SYNC_YIELD_SECONDS,
    DeploymentConfigError,
    PublisherConfig,
    _publisher_config_from_manifest,
)

_PATH = Path("deployment.yaml")


def _load(block):
    return _publisher_config_from_manifest(
        {"publisher": block}, manifest_path=_PATH,
    )


def test_the_default_is_ten_minutes() -> None:
    assert DEFAULT_MAX_SOURCE_SYNC_YIELD_SECONDS == 600.0
    assert PublisherConfig().max_source_sync_yield_seconds == 600.0
    assert _publisher_config_from_manifest({}, manifest_path=_PATH) == PublisherConfig()
    assert _load({}).max_source_sync_yield_seconds == 600.0


@pytest.mark.parametrize("value", [0, 0.0, 1, 90, 600.5, 86400])
def test_a_number_at_or_above_zero_is_loaded(value) -> None:
    assert _load({"max_source_sync_yield_seconds": value}).max_source_sync_yield_seconds == float(value)


@pytest.mark.parametrize(
    "value",
    [-1, -0.001, True, False, "600", "10m", None, [], {}, float("nan"), float("inf")],
)
def test_a_malformed_value_is_refused_not_defaulted(value) -> None:
    with pytest.raises(DeploymentConfigError, match="max_source_sync_yield_seconds"):
        _load({"max_source_sync_yield_seconds": value})


def test_the_key_is_in_the_printed_schema() -> None:
    from okg.substrate.deployments.keys import _PUBLISHER_SCHEMA

    schema = _PUBLISHER_SCHEMA["properties"]["max_source_sync_yield_seconds"]
    assert schema["type"] == "number" and schema["minimum"] == 0
    assert schema["default"] == 600
