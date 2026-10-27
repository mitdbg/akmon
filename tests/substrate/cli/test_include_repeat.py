"""A repeated ``--include`` or ``--exclude`` adds values instead of replacing (#4016).

Parser-level only: no database, no deployment.
"""

from __future__ import annotations

import pytest

from okg.substrate.cli.main import _build_parser


def _parse(argv: list[str]):
    return _build_parser().parse_args(argv)


def _ingest(*include_args: str):
    return _parse(["ingest", "--deployment", "d", *include_args])


# okg ingest: one comma-separated string; repeats join with commas.


def test_ingest_repeat_keeps_every_value() -> None:
    args = _ingest("--include", "a", "--include", "b")
    assert args.include.split(",") == ["a", "b"]


def test_ingest_repeat_still_splits_commas_in_each_value() -> None:
    args = _ingest("--include", "a,b", "--include", "c")
    assert args.include.split(",") == ["a", "b", "c"]


def test_ingest_repeat_drops_duplicates_in_first_seen_order() -> None:
    args = _ingest("--include", "b,a", "--include", "a,c", "--include", "b")
    assert args.include.split(",") == ["b", "a", "c"]


def test_ingest_single_comma_form_unchanged() -> None:
    assert _ingest("--include", "a,b").include == "a,b"


def test_ingest_without_include_is_none() -> None:
    assert _ingest().include is None


def test_ingest_include_without_a_value_is_refused() -> None:
    with pytest.raises(SystemExit):
        _ingest("--include")


# nargs="*" commands: provision, deployment up, up.

_NARGS_COMMANDS = {
    "provision": ["provision", "--deployment", "d"],
    "deployment-up": ["deployment", "up", "d"],
    "up": ["up", "d"],
}


@pytest.mark.parametrize("name", sorted(_NARGS_COMMANDS))
def test_nargs_repeat_keeps_every_value(name: str) -> None:
    args = _parse([*_NARGS_COMMANDS[name], "--include", "a", "--include", "b"])
    assert args.include == ["a", "b"]


@pytest.mark.parametrize("name", sorted(_NARGS_COMMANDS))
def test_nargs_space_separated_form_unchanged(name: str) -> None:
    args = _parse([*_NARGS_COMMANDS[name], "--include", "a", "b"])
    assert args.include == ["a", "b"]


@pytest.mark.parametrize("name", sorted(_NARGS_COMMANDS))
def test_nargs_repeat_mixes_with_space_separated(name: str) -> None:
    args = _parse(
        [*_NARGS_COMMANDS[name], "--include", "a", "b", "--include", "c"]
    )
    assert args.include == ["a", "b", "c"]


@pytest.mark.parametrize("name", sorted(_NARGS_COMMANDS))
def test_nargs_without_include_stays_empty(name: str) -> None:
    args = _parse(_NARGS_COMMANDS[name])
    assert not args.include


# okg ingest --exclude: same comma-string shape as --include.


def test_exclude_repeat_keeps_every_value() -> None:
    args = _ingest("--exclude", "a", "--exclude", "b")
    assert args.exclude.split(",") == ["a", "b"]


def test_exclude_repeat_still_splits_commas_in_each_value() -> None:
    args = _ingest("--exclude", "a,b", "--exclude", "c")
    assert args.exclude.split(",") == ["a", "b", "c"]


def test_exclude_repeat_drops_duplicates_in_first_seen_order() -> None:
    args = _ingest("--exclude", "b,a", "--exclude", "a,c")
    assert args.exclude.split(",") == ["b", "a", "c"]


def test_exclude_single_comma_form_unchanged() -> None:
    assert _ingest("--exclude", "a,b").exclude == "a,b"


def test_exclude_without_value_is_none_and_needs_a_value() -> None:
    assert _ingest().exclude is None
    with pytest.raises(SystemExit):
        _ingest("--exclude")


def test_include_and_exclude_repeats_stay_separate() -> None:
    args = _ingest(
        "--include", "a", "--exclude", "x", "--include", "b", "--exclude", "y",
    )
    assert args.include.split(",") == ["a", "b"]
    assert args.exclude.split(",") == ["x", "y"]


# Empty and whitespace-only items are skipped; when nothing is left the value
# is "", which every consumer reads as "no filter" (as on origin/dev).


@pytest.mark.parametrize("flag", ["include", "exclude"])
def test_empty_repeat_is_skipped(flag: str) -> None:
    args = _ingest(f"--{flag}", "a", f"--{flag}", "")
    assert getattr(args, flag) == "a"


@pytest.mark.parametrize("flag", ["include", "exclude"])
def test_empty_first_repeat_is_skipped(flag: str) -> None:
    args = _ingest(f"--{flag}", "", f"--{flag}", "a")
    assert getattr(args, flag) == "a"


@pytest.mark.parametrize("flag", ["include", "exclude"])
def test_trailing_comma_is_skipped(flag: str) -> None:
    assert getattr(_ingest(f"--{flag}", "a,"), flag) == "a"
    assert getattr(_ingest(f"--{flag}", "a,,b, ,c"), flag) == "a,b,c"


@pytest.mark.parametrize("flag", ["include", "exclude"])
def test_whitespace_only_value_is_skipped(flag: str) -> None:
    assert getattr(_ingest(f"--{flag}", "a", f"--{flag}", "  "), flag) == "a"


@pytest.mark.parametrize("flag", ["include", "exclude"])
def test_only_empty_values_leave_no_filter(flag: str) -> None:
    for argv in ([f"--{flag}", ""], [f"--{flag}", "", f"--{flag}", ","]):
        assert getattr(_ingest(*argv), flag) == ""
