"""Substrate-side composers for the Console's typed view payloads.

Each view's read-side composer lives in its own module
(`today_brief.py`, `triage_brief.py`, ...). The composer:

  1. Reads from the substrate via SQL views (`okg.v_*`) or via
     AKMON operators (inspect / aggregate / etc).
  2. Composes the typed payload defined by the matching Pydantic model and
     endpoint contract tests.
  3. Returns the payload as a Pydantic model. The viz API layer
     serializes it to JSON for the future single operator app.

The retired TypeScript schema parity tests are archived with the legacy UI.
Do not restore the archived console package as the active schema authority.
"""
from __future__ import annotations
