# Package marker: makes tests/deployments and its per-deployment
# subpackages importable so duplicate test basenames across deployments
# (e.g. test_public_seed.py, test_registry.py) resolve to distinct module
# paths under pytest's default `prepend` import mode instead of colliding
# with an "import file mismatch" error.
