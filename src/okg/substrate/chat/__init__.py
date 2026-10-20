"""okg.substrate.chat — the chat-surface substrate package.

Owns the pieces the chat frontend needs from the OKG side. Two module
classes live here, with different import contracts:

- **Contracts and artifacts**: plain library code, importable from
  ``okg.*``, with no Open WebUI, console-bundle, or database dependency —
  testable entirely on their own. ``deep_links`` is the ONE place a
  console URL is built; ``audit_record`` owns the versioned, append-only
  audit-record artifact; ``observability_verdict`` records whether the
  audit was ever measured against the bytes the model actually received.
- **Open WebUI function modules** (``review_queue_pipe``,
  ``audit_filter``): self-contained files copied verbatim into an Open
  WebUI instance by ``okg chat sync`` and run inside the Open WebUI
  process, which does not import ``okg`` as a library. These modules may
  not import from ``okg.*`` at runtime; they reach the substrate through
  the ``okg`` CLI over a subprocess argv list. They live in the repo so
  the test suite can exercise their rendering and refusal logic against
  captured CLI JSON and recorded instance behavior.
"""
