"""Read torch-saved INTEGER tensors without torch.

STaRK ships its Prime SKB as `node_types.pt`, `edge_types.pt` and
`edge_index.pt` — `torch.save` output. The prior in-repo adapter reached
for `import torch` to read them, which puts a ~2 GB deep-learning stack in
the dependency graph of a source that needs three flat int64 arrays. This
repo carries no torch and the substrate is deliberately free of ML runtime
deps, so the arrays are read from the container directly.

A `torch.save` file is a ZIP archive holding

    <stem>/data.pkl      the pickled tensor-rebuild call
    <stem>/data/<key>    the raw, uncompressed storage bytes
    <stem>/version

`data.pkl` reduces to ``torch._utils._rebuild_tensor_v2(storage, offset,
size, stride, ...)`` where `storage` arrives through the pickle's
`persistent_id` protocol as ``('storage', <StorageType>, key, location,
numel)``. Unpickling with a `find_class` that never imports torch, plus a
`persistent_load` that records the storage descriptor, recovers shape,
dtype and byte offset; `struct.unpack` does the rest.

FAIL-LOUD BY CONSTRUCTION. Every assumption above is checked and every
violation raises `StarkTensorError`: a non-zip file, a missing `data.pkl`,
a rebuild call this reader does not implement, a float or otherwise
unsupported storage type, a non-zero storage offset, a non-contiguous
stride, or a storage whose byte length disagrees with its declared element
count. There is no "best effort" path and no default shape — a release
whose layout changed must stop the build, not be silently misread.

SECURITY. `find_class` returns inert stubs and never imports or calls
anything, so no code in the pickle can execute. Only the four names this
reader implements are accepted; anything else raises.
"""

from __future__ import annotations

import io
import pickle
import struct
import zipfile
from pathlib import Path

from okg.substrate.errors import SubstrateError

__all__ = ["StarkTensorError", "read_int_tensor"]


class StarkTensorError(SubstrateError):
    """A torch-saved tensor could not be read under this reader's
    assumptions. Always names the file and the assumption that failed."""


#: storage class name -> (struct format code, bytes per element). Only
#: integer storages are supported: the three Prime tensors are all int64,
#: and a float storage would mean the release changed shape in a way that
#: must be looked at rather than coerced.
_INT_STORAGE: dict[str, tuple[str, int]] = {
    "LongStorage": ("q", 8),
    "IntStorage": ("i", 4),
    "ShortStorage": ("h", 2),
    "CharStorage": ("b", 1),
    "ByteStorage": ("B", 1),
}

#: the rebuild entry points this reader implements. `_rebuild_tensor_v2`
#: is what `torch.save` emits for a plain dense tensor.
_REBUILD_NAMES = frozenset({"_rebuild_tensor_v2", "_rebuild_tensor"})


class _Stub:
    """An inert stand-in for a class the pickle names. Calling it records
    the call instead of performing it; nothing is imported or executed."""

    __slots__ = ("module", "name")

    def __init__(self, module: str, name: str) -> None:
        self.module = module
        self.name = name

    def __call__(self, *args: object, **kwargs: object) -> tuple:
        return ("call", self.module, self.name, args, kwargs)

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"<stub {self.module}.{self.name}>"


class _InertUnpickler(pickle.Unpickler):
    """Unpickler that resolves every global to an inert stub and every
    persistent id to the raw storage descriptor."""

    def find_class(self, module: str, name: str) -> _Stub:  # type: ignore[override]
        return _Stub(module, name)

    def persistent_load(self, pid: object) -> tuple:  # type: ignore[override]
        if not (isinstance(pid, tuple) and pid and pid[0] == "storage"):
            raise StarkTensorError(
                f"unsupported persistent id in a torch-saved tensor: {pid!r}"
            )
        return ("storage", pid)


def read_int_tensor(path: Path) -> tuple[tuple[int, ...], list[int]]:
    """Return ``(shape, values)`` for a torch-saved integer tensor.

    ``values`` is the flat row-major contents. Raises `StarkTensorError`
    rather than returning a partial or defaulted result.
    """
    path = Path(path)
    if not path.is_file():
        raise StarkTensorError(f"tensor file is missing: {path}")
    if not zipfile.is_zipfile(path):
        raise StarkTensorError(
            f"{path} is not a torch>=1.6 zip-format tensor; this reader does "
            "not implement the legacy pickle container"
        )
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        pickles = [n for n in names if n.endswith("/data.pkl")]
        if len(pickles) != 1:
            raise StarkTensorError(
                f"{path} holds {len(pickles)} data.pkl entries "
                f"(expected exactly 1): {names[:8]}"
            )
        prefix = pickles[0][: -len("data.pkl")]
        try:
            obj = _InertUnpickler(io.BytesIO(archive.read(pickles[0]))).load()
        except pickle.UnpicklingError as exc:
            raise StarkTensorError(
                f"{path}: {pickles[0]} is not readable as a pickle: {exc}"
            ) from exc
        shape, storage_name, key, numel, offset, stride = _describe(obj, path)
        if storage_name not in _INT_STORAGE:
            raise StarkTensorError(
                f"{path} holds a {storage_name} tensor; this reader supports "
                f"integer storages only ({sorted(_INT_STORAGE)}). A float "
                "tensor here means the release layout changed"
            )
        if offset != 0:
            raise StarkTensorError(
                f"{path} declares storage offset {offset}; this reader reads "
                "whole storages only"
            )
        expected_stride = _contiguous_stride(shape)
        if stride != expected_stride:
            raise StarkTensorError(
                f"{path} declares stride {stride}, which is not the row-major "
                f"contiguous stride {expected_stride} for shape {shape}"
            )
        code, width = _INT_STORAGE[storage_name]
        try:
            raw = archive.read(prefix + "data/" + str(key))
        except KeyError as exc:
            raise StarkTensorError(
                f"{path} names storage {key!r} but the archive has no such "
                f"entry: {names[:8]}"
            ) from exc
    if len(raw) != numel * width:
        raise StarkTensorError(
            f"{path} storage {key!r} is {len(raw)} bytes but declares "
            f"{numel} elements of {width} bytes ({numel * width} expected)"
        )
    count = 1
    for dim in shape:
        count *= dim
    if count != numel:
        raise StarkTensorError(
            f"{path} declares shape {shape} ({count} elements) over a storage "
            f"of {numel} elements"
        )
    values = list(struct.unpack(f"<{numel}{code}", raw))
    return shape, values


def _describe(
    obj: object, path: Path
) -> tuple[tuple[int, ...], str, object, int, int, tuple[int, ...]]:
    """Pull (shape, storage class, storage key, numel, offset, stride) out
    of the recorded rebuild call."""
    if not (isinstance(obj, tuple) and len(obj) == 5 and obj[0] == "call"):
        raise StarkTensorError(
            f"{path}: data.pkl does not reduce to a tensor-rebuild call "
            f"(got {type(obj).__name__})"
        )
    _, module, name, args, _kwargs = obj
    if name not in _REBUILD_NAMES:
        raise StarkTensorError(
            f"{path}: data.pkl calls {module}.{name}, which this reader does "
            f"not implement (expected one of {sorted(_REBUILD_NAMES)})"
        )
    if len(args) < 4:
        raise StarkTensorError(
            f"{path}: {name} called with {len(args)} arguments; expected at "
            "least (storage, offset, size, stride)"
        )
    storage, offset, size, stride = args[0], args[1], args[2], args[3]
    if not (isinstance(storage, tuple) and len(storage) == 2
            and storage[0] == "storage"):
        raise StarkTensorError(
            f"{path}: {name}'s first argument is not a persistent storage "
            f"reference (got {storage!r})"
        )
    pid = storage[1]
    if len(pid) != 5:
        raise StarkTensorError(
            f"{path}: storage descriptor has {len(pid)} fields, expected 5 "
            f"('storage', type, key, location, numel): {pid!r}"
        )
    _, storage_type, key, _location, numel = pid
    storage_name = getattr(storage_type, "name", None) or str(storage_type)
    try:
        shape = tuple(int(d) for d in size)
        stride_t = tuple(int(d) for d in stride)
        return shape, storage_name, key, int(numel), int(offset), stride_t
    except (TypeError, ValueError) as exc:
        raise StarkTensorError(
            f"{path}: size {size!r} / stride {stride!r} are not integer "
            f"sequences: {exc}"
        ) from exc


def _contiguous_stride(shape: tuple[int, ...]) -> tuple[int, ...]:
    stride: list[int] = []
    running = 1
    for dim in reversed(shape):
        stride.append(running)
        running *= dim
    return tuple(reversed(stride))
