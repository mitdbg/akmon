# Testing

The canonical testing guide lives in [`tests/README.md`](tests/README.md):
the four test classes (functional / database / viz / performance), the
`scripts/qa.py` lane runner, the directory inventory, coverage floors, and
how to add a test.

Quick start:

```sh
docker compose -f ops/pg/docker-compose.yaml up -d   # dev Postgres
uv run python scripts/qa.py smoke                     # collection + sanity
uv run python scripts/qa.py substrate-db              # Postgres-backed lane
```

This file is intentionally a pointer so the test layout has one source of
truth. Add or change testing guidance in [`tests/README.md`](tests/README.md).
