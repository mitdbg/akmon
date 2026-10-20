# Operator handbook

How to run the deployment day to day. Read **Home** first; it says whether the
last update published and what needs attention.

## Daily check

1. Open Home and read the status line.
2. If an attempt is blocked, open it in Updates and read the recorded error.
3. Open Sources for the producer it stopped on.

## Producers

| Producer | Kind | Cadence |
|---|---|---|
| `source-07-git` | source | every 6 h |
| `semantic-linker` | enricher | after every update |
| `daily-brief` | view | daily |

## Publishing by hand

```bash
okg update run --deployment workspace-mixed
okg generation describe <id> --invariants
```

- A version publishes only when every invariant holds.
- Reads keep serving the previous version until then.

See the [status page](https://docs.example.invalid/status) for the live view.
