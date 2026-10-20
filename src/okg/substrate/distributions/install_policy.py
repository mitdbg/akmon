"""Explicit deployment-owned source policies for packaged installation."""
from __future__ import annotations

from copy import deepcopy
import os
from pathlib import Path

import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.deployment_audit import audit_deployment_policy
from okg.substrate.nomos.vocabulary import PII_CLASSES


def read_source_policy(path: Path) -> dict:
    """Read one bounded, duplicate-free snapshot without following symlinks."""
    from .package import _open_anchor
    from .installed_wheel import _read_file
    parent = _open_anchor(path.absolute().parent, directory=True)
    try:
        content = _read_file(parent, path.name, limit=1024 * 1024)
    finally:
        os.close(parent)

    class UniqueLoader(yaml.SafeLoader):
        pass

    def mapping(loader, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if type(key) is not str or key in result:
                raise SubstrateError("source policy requires unique string keys")
            result[key] = loader.construct_object(value_node, deep=deep)
        return result

    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    try:
        policy = yaml.load(content, Loader=UniqueLoader)
    except yaml.YAMLError as exc:
        raise SubstrateError("source policy YAML is invalid") from exc
    if type(policy) is not dict or set(policy) != {"sources"} or type(policy["sources"]) is not dict:
        raise SubstrateError("source policy must contain only a sources mapping")
    return policy


def apply_source_policy(files: dict[str, bytes], policy: dict, *, require_redaction: bool) -> dict[str, bytes]:
    """Apply exact operator declarations; no name-based defaults or merging."""
    manifest = yaml.safe_load(files["deployment.yaml"])
    registry = yaml.safe_load(files["source_registry.yaml"])
    if type(policy) is not dict or set(policy) != {"sources"} or type(policy["sources"]) is not dict:
        raise SubstrateError("source policy must contain only a sources mapping")
    sources = registry["sources"]
    if set(policy["sources"]) != set(sources):
        raise SubstrateError("source policy must declare exactly the final registry sources")
    for name, declaration in policy["sources"].items():
        if type(name) is not str or type(declaration) is not dict or set(declaration) != {"source_policy"}:
            raise SubstrateError("each source declaration must contain only source_policy")
        value = declaration["source_policy"]
        if type(value) is not dict:
            raise SubstrateError("source_policy must be a mapping")
        classes = value.get("pii_classes")
        if type(classes) is not dict or any(type(key) is not str or not key or type(item) is not str or item not in PII_CLASSES for key, item in classes.items()):
            raise SubstrateError("source policy requires explicit valid pii_classes")
        if require_redaction and value.get("store_raw") is not False:
            raise SubstrateError("private redaction requires explicit store_raw: false")
        sources[name]["source_policy"] = deepcopy(value)
    audit = audit_deployment_policy(manifest, source_registry=registry)
    if audit["ok"] is not True:
        raise SubstrateError("deployment source policy audit refused", data={"finding_codes": [item["code"] for item in audit["findings"]]})
    return {**files,
            "source-policy.yaml": yaml.safe_dump(policy, sort_keys=True).encode(),
            "source_registry.yaml": yaml.safe_dump(registry, sort_keys=True).encode()}
