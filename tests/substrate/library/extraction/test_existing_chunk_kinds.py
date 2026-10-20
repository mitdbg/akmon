from __future__ import annotations

import json

from okg.substrate.library.extraction.parse.markdown import MarkdownParser
from okg.substrate.library.extraction.parse.notebook import NotebookParser


def test_markdown_fence_is_code_while_prose_keeps_paragraph_default() -> None:
    parsed = MarkdownParser().parse("Prose.\n\n```python\nprint('x')\n```\n")
    assert [(c.text, c.chunk_kind) for c in parsed.chunks] == [
        ("Prose.", "paragraph"),
        ("print('x')", "code_block"),
    ]


def test_notebook_code_and_markdown_cells_are_typed() -> None:
    notebook = json.dumps({
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {},
        "cells": [
            {
                "id": "markdown-cell", "cell_type": "markdown",
                "metadata": {}, "source": ["hello"],
            },
            {
                "id": "code-cell", "cell_type": "code", "metadata": {},
                "source": ["x = 1"],
                "execution_count": None, "outputs": [],
            },
        ],
    })
    parsed = NotebookParser().parse(notebook)
    assert [c.chunk_kind for c in parsed.chunks] == ["paragraph", "code_block"]
