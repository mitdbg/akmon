"""Materialize inert configuration from the installed example's declaration."""

import argparse
import json
from importlib.resources import files
from pathlib import Path
from shutil import copyfile

import yaml

from okg.deployment import ConnectorRegistration, registration_registry_entry

from .reader import REGISTRATION


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=False)
    assets = files("okg_example_records")
    for directory, pattern in (("data", "*.json"), ("deployment", "*.yaml"),
                               ("deployment/schemas", "*.yaml"),
                               ("deployment/schemas/bridges", "*.yaml")):
        target = root / ("schemas/bridges" if directory.endswith("bridges") else
                         "schemas" if directory.endswith("schemas") else
                         "data" if directory == "data" else ".")
        target.mkdir(parents=True, exist_ok=True)
        for source in Path(str(assets.joinpath(directory))).glob(pattern):
            copyfile(source, target / source.name)
    copyfile(root / "data/baseline.json", root / "records.json")
    payload = REGISTRATION.as_asset_payload()
    (root / "connector.json").write_text(json.dumps(payload, indent=2) + "\n")
    # Use the same inert payload a package-v2 connector asset contains.
    declared = ConnectorRegistration.from_asset_payload(payload)
    entry = registration_registry_entry(declared)
    entry["params"] = {"path": str(root / "records.json")}
    (root / "source_registry.yaml").write_text(yaml.safe_dump({
        "admission_contract": "strict_v1", "sources": {declared.connector_type: entry},
    }, sort_keys=False))
    print(root / "deployment.yaml")


if __name__ == "__main__":
    main()
