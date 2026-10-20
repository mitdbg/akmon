from __future__ import annotations

import pytest

from okg.views.slot_embeds import (
    VIEW_SLOT_EMBED_MAX_LIMIT,
    ViewSlotEmbedError,
    parse_view_slot_embed_directive,
    view_slot_embed_directive,
)


def test_view_slot_embed_directive_round_trips() -> None:
    directive = view_slot_embed_directive(
        slot_name="clinical_summary",
        scope_key="fhir:Patient:patient-smoke-1",
        limit=8,
        title="Clinical Summary",
        open=True,
    )

    parsed = parse_view_slot_embed_directive(directive)

    assert parsed.slot_name == "clinical_summary"
    assert parsed.kind == "relation"
    assert parsed.scope_key == "fhir:Patient:patient-smoke-1"
    assert parsed.limit == 8
    assert parsed.title == "Clinical Summary"
    assert parsed.open is True


@pytest.mark.parametrize(
    "directive",
    [
        '<!--okg:view-slot {"kind":"relation"} -->',
        '<!--okg:view-slot {"slot_name":"clinical_summary","kind":"json"} -->',
        '<!--okg:view-slot {"slot_name":"clinical_summary","sql":"select 1"} -->',
        '<!--okg:view-slot {"slot_name":"clinical_summary","schema":"okg"} -->',
        '<!--okg:view-slot {"slot_name":"clinical_summary","relation":"r"} -->',
        '<!--okg:view-slot {"slot_name":"clinical_summary","limit":0} -->',
        (
            '<!--okg:view-slot {"slot_name":"clinical_summary","limit":'
            f"{VIEW_SLOT_EMBED_MAX_LIMIT + 1}"
            "} -->"
        ),
        '<!--okg:view-slot {"slot_name":"select patient"} -->',
        (
            '<!--okg:view-slot {"slot_name":"clinical_summary",'
            '"title":"select * from patients"} -->'
        ),
    ],
)
def test_view_slot_embed_directive_rejects_unsafe_payloads(
    directive: str,
) -> None:
    with pytest.raises(ViewSlotEmbedError):
        parse_view_slot_embed_directive(directive)


def test_view_slot_embed_directive_rejects_non_directive_text() -> None:
    with pytest.raises(ViewSlotEmbedError):
        parse_view_slot_embed_directive("{{ sql_view:clinical_summary }}")
