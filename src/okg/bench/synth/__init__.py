"""KG-first synthetic corpus lab (ring 1a').

Generate a ground-truth knowledge graph from a frozen fresh-seed Synthea
FHIR export, corrupt it across three layers (KG-fork / render / post-render),
render clinical documents from it, and seal exact gold.

Core modules (``prf``, ``worldschema``, ``fhir_map``, ``config``,
``corruption``, ``render``, ``goldseal``) import no okg substrate code —
the ground truth never passes through the system under test. The
``runner`` and CLI glue are the substrate-facing side.
"""
