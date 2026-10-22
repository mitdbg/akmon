"""Evidence-artifact templates: `okg pact evidence template --kind <kind>`.

Per `polish-pact-first-run-dx` req.pactdx.evidence-template (issue #643):
the evidence artifact YAML shapes (`--graph-report`, `--formal-report`)
were learn-by-example only — authors reverse-engineered them from sibling
repos. This module emits a commented, ingest-valid skeleton for each
canonical evidence kind that takes a file artifact, matching exactly what
`okg pact evidence ingest` accepts (see `ingest_graph_report` /
`ingest_formal_report` in operations.py — the parsers are the contract).

An unknown kind errors with the canonical kind list from the evidence-kind
registry (code defaults + `pact/project.yaml` `evidence_kinds:` extensions).
"""

from __future__ import annotations

from pathlib import Path

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import envelope
from okg.substrate.pact.evidence_kinds import load_registry


# Graph-shaped artifacts share one parser (`ingest_graph_report`): a
# top-level `evidence:` list of rows. The template stamps the requested
# canonical kind into the row.
_GRAPH_SHAPED_KINDS = {"graph_report", "published_generation"}

_GRAPH_TEMPLATE = """\
# Evidence artifact for:
#   okg pact evidence ingest <change> --graph-report <this-file>
# Shape: a top-level `evidence:` list; each row becomes one evidence row
# on the PACT. A row's omitted `refs` default to the change id.
evidence:
- kind: {kind}
  # One of: pass | fail | missing | planned | waived
  status: pass
  # Requirement / task / test-plan ids this evidence proves. Omit (or
  # delete) to default to the change id; ids listed here must exist in
  # pact.yaml or `gate post-edit` errors with unknown_evidence_ref.
  # refs:
  # - req.example
  # - task.example
  # One line: the substrate signal this run showed.
  summary: 'TODO: e.g. generation <g> published; nodes_live +<n>'
  # The command that produced the numbers.
  command: 'okg generation describe <generation-id> --json'
  # The published generation this evidence pins (optional).
  # generation_id: 123
  # Free-form structured payload preserved on the evidence row.
  data:
    nodes_live: 0
    edges_live: 0
"""

_FORMAL_TEMPLATE = """\
# Evidence artifact for:
#   okg pact evidence ingest <change> --formal-report <this-file>
# STRICT shape: every row must reference a formal binding already declared
# in pact.yaml (`formal:` — see `okg pact evidence bind-tla`), report a
# FRESH PASSING run, and pin the exact bytes it checked. Stale, partial,
# or digest-mismatched rows fail ingest loudly.
results:
  # Must equal a `formal[].property_id` (or binding id) in pact.yaml.
- property_id: TODO.property.id
  # Only `pass` ingests; anything else is formal_report_nonpassing_status.
  status: pass
  # Must be `true`: the run happened against the current repo bytes.
  fresh: true
  # The exact checker invocation executed for THIS run.
  command: 'TODO: e.g. uv run python scripts/check_property.py --json'
  # Required (and must match the binding) when the binding declares a
  # checker_command.
  # checker_command: 'uv run python scripts/check_property.py --json'
  # Checker output file; must exist INSIDE this report's directory.
  artifact: TODO-checker-output.txt
  # sha256 of the binding's module file, e.g. "sha256:<hex>". Compute:
  #   python -c "import hashlib,sys;print('sha256:'+hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" <module>
  module_digest: sha256:TODO
  # Required when the binding declares a config file.
  # config_digest: sha256:TODO
  # Must cover ALL of the binding's declared refinement tests.
  refinement_tests:
  - tests/TODO/test_refinement.py::test_case
  summary: 'TODO: property + what the checker proved, one line'
"""

# Kinds whose proof enters through a different door than a YAML artifact
# file — point authors at the right verb instead of emitting a template
# that ingest would reject.
_NON_FILE_KIND_HINTS = {
    "executed_test": (
        "okg pact evidence ingest <change> --pytest-report junit.xml "
        "(produce it with `pytest --junitxml=junit.xml`)"
    ),
    "planned_test": "declare a test_plan entry in pact.yaml",
    "waiver": "okg pact evidence waive <change> --scope <id> --reason ...",
}
# No change is named here, so the hint takes the default version (v5). `kind`
# is already a v5 kind (see `_attach_hint`).
_ATTACH_HINT = (
    "okg pact evidence attach <change> --kind {kind} --requirement <req> "
    "[--status pass|fail] [--command ...] [--artifact <path>] [--note ...]"
)


def _attach_hint(kind: str) -> str:
    from okg.substrate.pact.repair import v5_attach_kind

    return _ATTACH_HINT.format(kind=v5_attach_kind(kind))


def evidence_template(kind: str, *, repo_root: Path | None = None) -> dict:
    """The template payload for one evidence kind, or a loud refusal.

    Aliases resolve through the registry (`graph` -> `graph_report`,
    `formal` -> `formal_check`); the emitted row carries the canonical kind.
    """
    registry = load_registry(repo_root)
    if not registry.is_known(kind):
        raise SubstrateError(
            f"unknown evidence kind {kind!r}",
            data={
                "kind": kind,
                "canonical_kinds": sorted(registry.canonical),
                "aliases": dict(sorted(registry.aliases.items())),
            },
            fix_hint=(
                "pick a canonical kind (or alias) from the registry; "
                "templated kinds: "
                + ", ".join(sorted(_GRAPH_SHAPED_KINDS | {"formal_check"}))
            ),
        )
    canonical = registry.canonicalize(kind)
    if canonical in _GRAPH_SHAPED_KINDS:
        text = _GRAPH_TEMPLATE.format(kind=canonical)
        ingest_flag = "--graph-report"
    elif canonical == "formal_check":
        text = _FORMAL_TEMPLATE
        ingest_flag = "--formal-report"
    else:
        hint = _NON_FILE_KIND_HINTS.get(
            canonical, _attach_hint(canonical)
        )
        raise SubstrateError(
            f"evidence kind {canonical!r} does not take a YAML artifact "
            "file; no template to emit",
            data={
                "kind": canonical,
                "templated_kinds": sorted(
                    _GRAPH_SHAPED_KINDS | {"formal_check"}
                ),
                "canonical_kinds": sorted(registry.canonical),
            },
            fix_hint=hint,
        )
    return envelope.recorded(
        # Human (non --json) mode prints `text` raw so
        # `okg pact evidence template --kind graph_report > report.yaml`
        # yields an editable artifact directly — so the envelope's one-line
        # summary is not what this verb puts there.
        text=text,
        kind=canonical,
        requested_kind=kind,
        ingest_flag=ingest_flag,
        ingest_command=(
            f"okg pact evidence ingest <change> {ingest_flag} <this-file>"
        ),
        template=text,
    )
