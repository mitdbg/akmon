"""Small real PDFs for tests, built without any PDF-writing dependency.

`make_pdf(["page one text", None, "page three"])` returns the bytes of a PDF
with one page per entry: a string is drawn as that page's text layer, and
`None` is a blank page with no text, which is what a scanned page looks like to
a text extractor.
"""
from __future__ import annotations


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def make_pdf(pages: list[str | None]) -> bytes:
    objects: list[bytes] = []

    def add(body: str | bytes) -> int:
        objects.append(body.encode("latin-1") if isinstance(body, str) else body)
        return len(objects)

    catalog = add("")  # filled in below, once the page tree exists
    tree = add("")
    font = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    kids: list[int] = []
    for text in pages:
        stream = (
            f"BT /F1 12 Tf 72 700 Td ({_escape(text)}) Tj ET" if text else ""
        )
        content = add(
            f"<< /Length {len(stream)} >>\nstream\n{stream}\nendstream",
        )
        kids.append(add(
            "<< /Type /Page /Parent %d 0 R /MediaBox [0 0 612 792] "
            "/Resources << /Font << /F1 %d 0 R >> >> /Contents %d 0 R >>"
            % (tree, font, content),
        ))
    objects[catalog - 1] = f"<< /Type /Catalog /Pages {tree} 0 R >>".encode("latin-1")
    objects[tree - 1] = (
        "<< /Type /Pages /Kids [%s] /Count %d >>"
        % (" ".join(f"{k} 0 R" for k in kids), len(kids))
    ).encode("latin-1")

    out = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += b"%d 0 obj\n" % number + body + b"\nendobj\n"
    xref = len(out)
    out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objects) + 1)
    for offset in offsets:
        out += b"%010d 00000 n \n" % offset
    out += (
        b"trailer\n<< /Size %d /Root %d 0 R >>\nstartxref\n%d\n%%%%EOF\n"
        % (len(objects) + 1, catalog, xref)
    )
    return bytes(out)
