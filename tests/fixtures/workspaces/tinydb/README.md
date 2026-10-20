# TinyDB real-history fixture

`tinydb-v4.8.1-v4.8.2.bundle` is an offline Git bundle created from
<https://github.com/msiemens/tinydb>. It contains complete upstream history
for the two release refs exercised by the workspace-profile execution test:

- `v4.8.1` — `ad63e2e670b02ac1e9ed7f3130f6de53ec8d5bce`
- `v4.8.2` — `10644a0e07ad180c5b756aba272ee6b0dbd12df8`

The test verifies the bundle checksum and both commit identities before using
it. PACT, OpenSpec, test, coverage, security, CodeQL, profiling, and sanitized
Codex-session artifacts are generated in a separately labelled temporary
overlay; none are represented as TinyDB upstream content.
