"""Unit tests for okg.views.assembly — Stage 1.8 +
add-portal-fragment-annotations Stage 1."""
from __future__ import annotations

from okg.views.assembly import (
    AssembledWithMarkers,
    FragmentOffset,
    MarkeredAssembly,
    assemble,
    assemble_with_markers,
)
from okg.views.types import Fragment, View


def _f(id: str, content: str) -> Fragment:
    return Fragment(
        id=id,
        kind="quote",
        sources=("a",),
        source_hashes={"a": "0" * 16},
        content=content,
    )


class TestAssemble:
    def test_empty_view_assembles_to_empty_string(self):
        v = View(view_id="x", view_def_hash="abc")
        assert assemble(v) == ""

    def test_single_fragment(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[_f("a", "hello")],
        )
        assert assemble(v) == "hello"

    def test_blank_line_separator(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[_f("a", "alpha"), _f("b", "beta")],
        )
        assert assemble(v) == "alpha\n\nbeta"

    def test_order_preserved(self):
        # Planner-emitted order is the assembly order; assemble
        # does not reorder.
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("z", "Z"), _f("y", "Y"), _f("x", "X"),
            ],
        )
        assert assemble(v) == "Z\n\nY\n\nX"

    def test_empty_fragments_skipped(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("a", "alpha"),
                _f("blank", ""),
                _f("b", "beta"),
            ],
        )
        # Blank fragments don't produce a stray paragraph break.
        assert assemble(v) == "alpha\n\nbeta"


class TestAssembleWithMarkers:
    """`assemble_with_markers` — per
    add-portal-fragment-annotations spec.

    Three invariants:
      1. byte-clean field equals `assemble(view)` for the same view
      2. `markdown[start:start+length]` for each FragmentOffset
         equals that fragment's content
      3. markered field contains <!--frag:{id} start--> and
         <!--frag:{id} end--> around each fragment's content
    """

    def test_byte_clean_matches_assemble(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("a", "alpha"),
                _f("b", "beta"),
                _f("c", "gamma"),
            ],
        )
        result = assemble_with_markers(v)
        assert isinstance(result, AssembledWithMarkers)
        assert result.markdown == assemble(v)

    def test_offsets_slice_byte_clean_output(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("a", "alpha"),
                _f("b", "beta"),
                _f("c", "gamma"),
            ],
        )
        result = assemble_with_markers(v)
        assert len(result.fragment_offsets) == 3
        for fragment, off in zip(v.fragments, result.fragment_offsets):
            assert off.fragment_id == fragment.id
            assert (
                result.markdown[off.start:off.start + off.length]
                == fragment.content
            )

    def test_markered_wraps_each_fragment(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("row:42", "hello"),
                _f("row:43", "world"),
            ],
        )
        result = assemble_with_markers(v)
        assert (
            "<!--frag:row:42 start-->hello<!--frag:row:42 end-->"
            in result.markered
        )
        assert (
            "<!--frag:row:43 start-->world<!--frag:row:43 end-->"
            in result.markered
        )

    def test_empty_fragments_skipped(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[
                _f("a", "alpha"),
                _f("blank", ""),
                _f("b", "beta"),
            ],
        )
        result = assemble_with_markers(v)
        assert result.markdown == "alpha\n\nbeta"
        # Only emitted fragments get offsets.
        assert [o.fragment_id for o in result.fragment_offsets] == [
            "a", "b",
        ]
        # Blank fragment does NOT appear in the markered variant.
        assert "blank" not in result.markered

    def test_empty_view_returns_empty_artifact(self):
        v = View(view_id="x", view_def_hash="abc")
        result = assemble_with_markers(v)
        assert result.markdown == ""
        assert result.markered == ""
        assert result.fragment_offsets == ()

    def test_single_fragment_offset_at_zero(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[_f("only", "hello")],
        )
        result = assemble_with_markers(v)
        assert result.fragment_offsets == (
            FragmentOffset(fragment_id="only", start=0, length=5),
        )

    def test_offsets_account_for_separator(self):
        v = View(
            view_id="x",
            view_def_hash="abc",
            fragments=[_f("a", "alpha"), _f("b", "beta")],
        )
        result = assemble_with_markers(v)
        # "alpha" at [0:5], "\n\n" at [5:7], "beta" at [7:11].
        assert result.fragment_offsets[0].start == 0
        assert result.fragment_offsets[0].length == 5
        assert result.fragment_offsets[1].start == 7
        assert result.fragment_offsets[1].length == 4

    def test_fragment_offset_round_trips_via_dict(self):
        off = FragmentOffset(fragment_id="row:42", start=12, length=340)
        assert FragmentOffset.from_dict(off.to_dict()) == off


class TestMarkeredAssemblyBuilder:
    """`MarkeredAssembly` for custom assemblers — write plain
    text + write_fragment chunks and build a coherent artifact.
    """

    def test_plain_write_only_produces_no_offsets(self):
        b = MarkeredAssembly()
        b.write("# Header\n\n")
        b.write("Some prose.\n")
        result = b.build()
        assert result.markdown == "# Header\n\nSome prose.\n"
        assert result.markered == "# Header\n\nSome prose.\n"
        assert result.fragment_offsets == ()

    def test_fragment_offsets_address_byte_clean_only(self):
        b = MarkeredAssembly()
        b.write("# Header\n\n")
        b.write_fragment("row:1", "alpha")
        b.write("\n\n")
        b.write_fragment("row:2", "beta")
        result = b.build()
        # Byte-clean: "# Header\n\nalpha\n\nbeta"
        assert result.markdown == "# Header\n\nalpha\n\nbeta"
        # Markered output has comments around each fragment.
        assert "<!--frag:row:1 start-->alpha<!--frag:row:1 end-->" in result.markered
        assert "<!--frag:row:2 start-->beta<!--frag:row:2 end-->" in result.markered
        # Offsets point at byte-clean output.
        off1, off2 = result.fragment_offsets
        assert off1.fragment_id == "row:1"
        assert (
            result.markdown[off1.start:off1.start + off1.length]
            == "alpha"
        )
        assert off2.fragment_id == "row:2"
        assert (
            result.markdown[off2.start:off2.start + off2.length]
            == "beta"
        )

    def test_fragment_id_can_repeat(self):
        # A fragment whose rendered chunks span multiple
        # structural locations (e.g. header + body) emits one
        # offset per write_fragment call.
        b = MarkeredAssembly()
        b.write_fragment("row:1", "first chunk")
        b.write("\n\nINTERLUDE\n\n")
        b.write_fragment("row:1", "second chunk")
        result = b.build()
        ids = [off.fragment_id for off in result.fragment_offsets]
        assert ids == ["row:1", "row:1"]
        # Both ranges resolve to the right text.
        a, b_off = result.fragment_offsets
        assert (
            result.markdown[a.start:a.start + a.length]
            == "first chunk"
        )
        assert (
            result.markdown[b_off.start:b_off.start + b_off.length]
            == "second chunk"
        )

    def test_empty_fragment_skipped(self):
        b = MarkeredAssembly()
        b.write("X")
        b.write_fragment("blank", "")
        b.write("Y")
        result = b.build()
        assert result.markdown == "XY"
        assert result.markered == "XY"
        assert result.fragment_offsets == ()
