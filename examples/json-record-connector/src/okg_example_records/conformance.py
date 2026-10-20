"""The conformance kit drives the same reader as production ingestion."""

import json
from importlib.resources import files

from okg.deployment.conformance import ConnectorCase

from .reader import REGISTRATION, RecordReader


def case():
    path = files("okg_example_records").joinpath("data/conformance.json")
    dataset = json.loads(path.read_text(encoding="utf-8"))
    return ConnectorCase(
        connector=RecordReader(), registration=REGISTRATION,
        dataset={row["id"]: row for row in dataset["records"]},
        config={"path": str(path)}, expected_rejections=("excluded",),
    )
