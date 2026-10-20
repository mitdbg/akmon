"""Plain-function tests: normalize."""
from __future__ import annotations


# ---- normalize ----------------------------------------------------


def test_unicode_clean_fixes_mojibake() -> None:
    """Canonical mojibake: UTF-8 bytes mis-decoded as CP1252.
    ftfy fixes the round-trip; it ALSO uncurls smart quotes by
    default, so `’` becomes `'` — that's expected behavior."""
    from okg.substrate.library.extraction.normalize import unicode_clean
    bad = "Itâ€™s broken"
    cleaned = unicode_clean(bad)
    # Mojibake repaired AND smart-quote uncurled.
    assert cleaned == "It's broken"


def test_unicode_clean_passes_clean_text() -> None:
    """Even already-clean text gets ftfy's smart-quote uncurl."""
    from okg.substrate.library.extraction.normalize import unicode_clean
    assert unicode_clean("hello world") == "hello world"


def test_strip_html_basic() -> None:
    from okg.substrate.library.extraction.normalize import strip_html
    assert strip_html("<p>hi <b>there</b></p>") == "hi there"


def test_strip_html_drops_script() -> None:
    from okg.substrate.library.extraction.normalize import strip_html
    res = strip_html("<p>visible</p><script>alert(1)</script>")
    assert "visible" in res
    assert "alert" not in res


def test_strip_markdown_basic() -> None:
    from okg.substrate.library.extraction.normalize import strip_markdown
    md = "# Heading\n\nbody **bold** text\n"
    res = strip_markdown(md)
    # Heading marker is gone; body is present.
    assert "#" not in res
    assert "body" in res
