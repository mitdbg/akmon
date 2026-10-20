---
audience: developer, operator
type: explanation
canonical_id: graph-anonymization
---
# Graph anonymization


AKMON provides controls that hide personal data from graph reads and prevent
selected sources from storing raw identifying values while preserving graph
structure.

A knowledge graph makes privacy harder than a flat store: the same person recurs
across thousands of nodes and edges, so redacting names to `<PERSON>` collapses
everyone onto one token and destroys the graph. AKMON therefore uses stable
pseudonyms: `Alice` becomes the same token everywhere, so joins survive while
the raw value does not.

## The model

Anonymization is a classification-driven transform applied at two points, per
data class:

| Posture | Where | What it does |
|---|---|---|
| **At rest** | ingest write path (`okg.substrate.ingest.pii_sanitize`) | For a *not-store-raw* source, direct identifiers are pseudonymized before storage and free-text spans are masked, so raw PII never lands in the store. |
| **At read** | Nomos read filter (`okg.substrate.nomos.read_masking`, applied by `read_filter` / the export path) | The same stored graph projects differently per principal: identifiers pseudonymize, sensitive/free-text mask, `non_pii` reveals. |

Both call one function, `mask_attrs`, keyed on each attribute's **privacy
class**.

### Privacy classes (`pii_class`)

Every attribute has a class from a fixed taxonomy (declared in the Nomos ontology
enum `NomosPiiClass` and the Python `PII_CLASSES` frozenset; the two are
drift-checked):

| `pii_class` | Read behavior | Example |
|---|---|---|
| `direct_identifier` | pseudonymize (or mask if no key) | email, full name, phone |
| `quasi_identifier` | pseudonymize / mask | location, exact timestamp |
| `sensitive_attribute` | mask | diagnosis, salary |
| `free_text` | drop at read; span-mask at ingest | body, notes, transcript |
| `non_pii` | reveal | node id, subtype, created_at |

**Fail-safe is the invariant.** An attribute that carries a value but has no
resolved `pii_class` resolves to *sensitive* (masked), never `non_pii`.
Defaulting unclassified data to reveal is treated as a P0 bug. Structural
governance keys (`classification`, source lineage, `nomos_redacted`) are exempt
from masking — the policy engine keys on them, so masking them would fail open.

### The pseudonym vault

`okg.substrate.alias.pseudonymize` maps a canonical entity (resolved through the
existing alias/canonicalization spine) to a stable, keyed pseudonym:

- **Injective by construction** — a 128-bit truncated HMAC-SHA256, so distinct
  entities never collide onto one pseudonym at graph scale.
- **Keyed, no reverse map** — the HMAC key comes from the deployment secret
  store (`OKG_PSEUDONYM_HMAC_KEY`); there is no stored raw→pseudonym map and no
  inverse. A missing key fails loud (`pseudonym_key_unavailable`) — never a
  silent default key. AKMON does not provide de-anonymization.
- **Rotation is an explicit epoch** — the pseudonym is bound to an epoch label,
  so rotating the key means bumping the epoch (a visible, deterministic fork),
  never a silent re-key.
- **Fail-closed** — an entity the alias cascade cannot resolve is **held**
  (`unresolved_entity_held`, raw-free), never surface-keyed or stored raw.

### The free-text detector

Structured attributes classify by ontology declaration. Free text (findings,
transcripts, commit messages) needs a detector: the `privacy` extra installs
Microsoft Presidio (`presidio-analyzer`, under the `data-privacy-stack` GitHub
org) plus a GLiNER recognizer, wired as
`okg.substrate.ingest.presidio_detector.PresidioFreeTextDetector`. It is an
**optional** heavy dependency — if a not-store-raw source has free text and the
detector is absent, ingest **holds** the fact (`presidio_unavailable`) rather
than skipping detection silently.

Statistical disclosure control (k-anonymity, differential privacy over
aggregates) is out of scope.

## Configuring anonymization for a source

Anonymizing a source `<src>` is four steps. Both halves — read-time masking and
at-rest pseudonymization — are live; the at-rest half binds automatically from a
per-source posture (step 4b).

**1. Provision the pseudonym key** (once per deployment). Set
`OKG_PSEUDONYM_HMAC_KEY` to ≥16 bytes of high-entropy material from the
deployment secret store. Without it, identifier classes *mask* instead of
pseudonymizing (fail-safe, but joins are lost for those attributes).

**2. Install the free-text detector** (only if `<src>` has free-text PII):

```
uv pip install -e ".[privacy]"
```

Then register it once at startup:
`register_free_text_detector(maybe_build_presidio_detector())`.

**3. Classify the source's attributes.** **Two** classification surfaces
protect **different stages** — declare both for end-to-end coverage, not
either/or:

- **Read masking** keys on the attribute's **subtype** `pii_class`, stored in
  `okg.subtypes.metadata.pii_classes` (populated from the ontology subtype
  definitions). It masks a `redact` principal's read (step 4a).
- **At-rest** keys on a **per-source** `pii_classes` map on the source's
  `source_policy` (step 4b). It pseudonymizes or masks a `store_raw: false`
  source's facts before storage.

A subtype the ontology has not annotated falls back to the legacy read behavior;
a source with no `pii_classes` map masks everything unlisted at rest (fail-safe).
Anything left unclassified fails safe to masked on either side, so declare the
`non_pii` structural fields too if you want them revealed.

**4a. Read-time masking (live).** Add a Nomos policy rule that redacts `<src>`
for the principals who must not see its PII:

```yaml
- rule_id: redact_<src>_for_readers
  effect: redact
  actions: [graph.read.mcp]
  subjects: [subject:<unprivileged-principal>]
  match: {deployment: <dep>, scope: deployment:<dep>, source_id: <src>}
  obligations: [{type: redaction}, {type: audit}]
```

A principal with an `allow` rule sees the values policy permits; a `redact`
principal gets the classification-driven transform (free-text dropped,
identifiers pseudonymized, `non_pii` revealed). No raw value reaches the
decision-sink payload.

**4b. At-rest pseudonymization.** To store pseudonyms instead of raw
identifiers, set `store_raw: false` on `<src>`'s `source_policy` (it defaults to
`true`, so every existing source is untouched). The source runner then transforms
each fact at admission — before canonicalize/lineage/COPY, so no downstream stage
sees raw. Declare which attributes to pseudonymize/reveal with a
`pii_classes` map (undeclared attributes fail safe to masked):

```yaml
# source_registry.yaml — sources.<src>.source_policy
source_policy:
  sensitivity: restricted
  data_classification: restricted
  store_raw: false            # opt <src> into at-rest anonymization
  pii_classes:                # attr -> pii_class (undeclared -> fail-safe mask)
    owner_email: direct_identifier   # pseudonymized (joins survive across identical values)
    display_name: quasi_identifier   # pseudonymized
    bio: free_text                   # detected spans masked (needs the detector)
    kind: non_pii                    # revealed
```

Nomos `deployment_audit` validates both fields; a not-store-raw source with no
`OKG_PSEUDONYM_HMAC_KEY` fails loudly (never a silent raw store), and one whose
facts carry free text with no detector installed **holds** the source watermark
(`pii_facts_held`) rather than storing raw.

Three limits apply to a `store_raw: false` source:

- **Join fidelity.** Identifiers are pseudonymized on their *raw stored value*, so
  joins survive only across byte-identical values — two surface variants of the
  same entity (`Alice@x` vs `alice@x`) get *different* pseudonyms.
- **Identity scope.** The transform rewrites attribute *values*. It does not
  pseudonymize a raw identifier used as a `node_id` or edge endpoint. Do not use
  such identifiers for a source that promises not to store raw identifiers.
- **Raw-document relationship builders.** A builder that reads a raw source
  document does not pass captured text through this at-rest transform. Do not
  enable that builder for a source whose contract forbids storing raw text.

## Status

| Capability | State |
|---|---|
| `pii_class` taxonomy + fail-safe resolver | live |
| Keyed pseudonym vault (no reverse map, epoch, fail-closed) | live |
| Read-time masking (one `mask_attrs` path; the old `_redacted_attrs` blocklist is retired) | live |
| Presidio free-text detector (`privacy` extra) | live (optional) |
| At-rest transform + detector-hold + published-generation proof | live |
| Auto-binding the at-rest transform into the source runner (`source_policy.store_raw` posture, bound at both write chokepoints, frozen fact-writer inventory) | live |
| Ontology-wide `pii_class` annotation | not provided; deployments must classify the fields they use |
| At-rest masking for raw-document relationship builders | not provided; do not combine those builders with a no-raw-storage promise |
| Pseudonymized `node_id` and edge endpoint identities | not provided |
| Existing raw data already in Chronos history | read masking only; AKMON provides no automatic history backfill |

Implementation and tests live under `src/okg/substrate/nomos/{pii_classification,read_masking}.py`,
`src/okg/substrate/alias/pseudonymize.py`,
`src/okg/substrate/ingest/{pii_sanitize,presidio_detector}.py`, and
`tests/substrate/{nomos,alias,ingest}/`.
