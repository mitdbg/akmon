import copy
from pathlib import Path
import pytest
from okg.substrate.library.sources._file_walk import FileWalk


@pytest.mark.parametrize("operation", ["stat", "read_bytes"])
@pytest.mark.parametrize("error", [PermissionError, FileNotFoundError])
def test_strict_refuses_enumerated_failures_and_legacy_still_skips(
    tmp_path, monkeypatch, operation, error
):
    path = tmp_path / "a.txt"
    path.write_text("data")
    cursor = {"files": {"a.txt": "old"}}
    before = copy.deepcopy(cursor)
    method = getattr(Path, operation)

    def failed(p, *args, **kwargs):
        if p == path:
            raise error("fault after enumeration")
        return method(p, *args, **kwargs)

    monkeypatch.setattr(Path, operation, failed)
    strict = FileWalk(
        prior_cursor=cursor, repo_root=tmp_path, read_as="bytes", strict_io=True
    )
    with pytest.raises(error):
        list(strict.iter([path]))
    legacy = FileWalk(prior_cursor=cursor, repo_root=tmp_path, read_as="bytes")
    assert list(legacy.iter([path])) == []
    assert cursor == before


def test_strict_successful_noop_retains_cursor(tmp_path):
    path = tmp_path / "a.txt"
    path.write_text("data")
    first = FileWalk(
        prior_cursor=None, repo_root=tmp_path, read_as="bytes", strict_io=True
    )
    assert list(first.iter([path]))[0].is_changed
    cursor = first.next_cursor()
    second = FileWalk(
        prior_cursor=cursor, repo_root=tmp_path, read_as="bytes", strict_io=True
    )
    assert not list(second.iter([path]))[0].is_changed
    assert second.next_cursor() == cursor
