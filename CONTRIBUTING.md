# Contributing to AKMON

Thank you for your interest in AKMON.

This repository is published as a series of release snapshots. Issues and
pull requests are welcome; accepted changes are applied upstream and appear
in the next snapshot.

## Before you open a pull request

1. Read [Develop AKMON](docs/develop/index.md) and, for changes to the import,
   publication, or query path, the
   [code architecture](docs/develop/architecture/pipeline.md).
2. Set up a development environment:

   ```bash
   uv sync --locked --extra dev
   ```

   `uv sync` removes extras it does not name. If you followed
   [Evaluate AKMON from source](docs/start/evaluate-from-source.md), use
   `uv sync --locked --extra dev --extra code --extra quality` to keep the
   packages it installed.

3. Run the fast test lane and the documentation build:

   ```bash
   uv run python scripts/mirror_fast_lane.py
   uv run --extra docs mkdocs build --strict
   ```

4. Keep each pull request to one change, with tests for the behavior it adds
   or fixes.

## Reporting a security problem

Do not open a public issue for a security problem. Email the maintainers at
mohoney@mit.edu instead.

## License

By contributing, you agree that your contribution is licensed under the
[MIT License](LICENSE).
