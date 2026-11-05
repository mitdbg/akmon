"""OKG test package."""

#: End-of-process cleanups, run by ``pytest_sessionfinish`` in tests/conftest.py.
#: Each is a no-argument callable returning ``(name, ok, error)`` rows; a
#: test-support module appends its own when it is imported. The list lives in
#: this package, which every test imports, so tests/conftest.py never imports
#: (or names) the modules that register, and a run that never imports them
#: runs nothing.
SESSION_CLEANUPS: list = []
