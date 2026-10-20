# Poros Formal Models

## System

The Poros model covers end-to-end exchange between OKG deployments: sender
outbox, receiver inbox, preflight verification, replay protection, receipts,
Nomos decisions, admission, quarantine/review, publication, imported live rows,
promotion, and public proof views.

## Design Contract

- Safety property: cross-OKG imports cannot become live without receiver-side
  verification, Nomos decision, admission, promotion authority, and complete
  provenance/proof evidence.
- Authority boundary: receiver exchange code and receiver Nomos/admission state
  own publication authority; sender policy cannot authorize receiver visibility.
- Valid runtime paths: sender outbox export, receiver inbox submit, delivery
  callback recording, replay detection, receipt handling, quarantine/review,
  promotion, rejection, and live proof-row publication.
- Out of scope: cryptographic primitive correctness, transport reliability, and
  deployment-specific moderation UX remain implementation-test concerns.
- Preconditions/refinement: fixture refinement covers the model vocabulary,
  and `make verify-poros` requires positive DB refinement evidence from a
  sender outbox export through receiver publication. Rejection-state runtime
  behavior is bound to the implementation tests in this README; any future
  missing formal coverage must be added to `formal/gap-inventory.toml` before
  it can be treated as an active gap.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `PorosExchange.tla` | `PorosExchange.cfg` | Positive cross-OKG exchange contract. |
| `PorosExchange.tla` | `PorosSenderAuthorityWitness.cfg` | Buggy-toggle witness: `BuggySenderHintGrantsAllow = TRUE` lets a sender-hinted, receiver-unauthorized message be allowed and admitted, refuting `NoSenderPolicyAuthority` by name (the toggle is `FALSE` on the positive config, where the receiver's decision is authoritative). |
| `PorosExchangeCounterexample.tla` | `PorosExchangeCounterexample.cfg` | Expected-failing receipt/publication negative coverage. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `PorosExchange.tla` | Sender outbox data becomes a signed receiver inbox message, and the receiver records the preflight result. The DB refinement also checks sender-side callback evidence. | `src/okg/substrate/exchange/outbox.py`, `src/okg/substrate/exchange/inbox.py`, `src/okg/substrate/exchange/http.py`, `src/okg/substrate/exchange/receiver.py`, `src/okg/substrate/exchange/refinement_db.py` | `tests/substrate/exchange/test_poros_http.py::test_http_receiver_service_exported_outbox_message_refines_published_flow`, `tests/substrate/exchange/test_poros_inbox_and_authority.py` |
| `PorosExchange.tla` | Verified receiver inbox data moves through Nomos decision, admission, quarantine/review, promotion, and live graph visibility. | `src/okg/substrate/exchange/admission.py`, `src/okg/substrate/exchange/receiver.py`, `src/okg/substrate/exchange/refinement.py`, `src/okg/substrate/exchange/refinement_db.py` | `tests/substrate/exchange/test_poros_http.py::test_http_receiver_service_exported_outbox_message_refines_published_flow`, `tests/substrate/exchange/test_poros_admission.py`, `test_poros_message_board.py` of a private deployment (not in this copy) |
| `PorosExchange.tla` (`NoSenderPolicyAuthority`) | `receiver_authorized` is derived from the receiver's recorded Nomos decision (`poros_incorporation_events` `nomos_decided`/`promotion_decided` effect `allow`), and `sender_hint` from the sender's declared claim-shaped `graph_profile` -- so a sender's authorization hint can never stand in for the receiver's decision at the refinement boundary. | `src/okg/substrate/exchange/refinement_db.py`, `src/okg/substrate/exchange/receiver.py` | `tests/substrate/exchange/test_poros_http.py::test_http_sender_hinted_denial_refines_receiver_unauthorized`, `tests/substrate/exchange/test_poros_refinement.py::test_poros_refinement_rejects_sender_hint_broadening_policy` |
| `PorosExchangeCounterexample.tla` | A broken receiver treats a receipt as publication; TLC must find the counterexample. | `src/okg/substrate/exchange/inbox.py`, `src/okg/substrate/exchange/receiver.py` | `tests/substrate/exchange/test_poros_inbox_and_authority.py`, `tests/substrate/exchange/test_poros_admission.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `ConstantSetsOK`, `TypeOK` | Message ids, outbox rows, inbox rows, receipts, policy decisions, admissions, promotions, and proof rows keep the expected shape. | Exchange fixture setup in `tests/substrate/exchange/test_poros_inbox_and_authority.py` and DB trace mapping in `tests/substrate/exchange/test_poros_http.py::test_http_receiver_service_exported_outbox_message_refines_published_flow`. |
| `NoUnverifiedAdmission`, `NoReplayDoubleEffect`, `ReceiptIsNotPublication` | Unverified messages cannot enter admission, replay does not apply twice, and a receipt is not publication. | Inbox and replay tests in `tests/substrate/exchange/test_poros_inbox_and_authority.py`; receiver DB refinement in `tests/substrate/exchange/test_poros_http.py`. |
| `NomosDecisionRequired`, `QuarantineNotLive`, `PromotionRequiresReceiverPolicy`, `NoUnauthorizedPromotion`, `NoSenderPolicyAuthority` | Receiver policy controls admission, quarantine, promotion, and visibility; sender policy cannot authorize receiver publication. `NoSenderPolicyAuthority` has a named buggy-toggle witness (`PorosSenderAuthorityWitness.cfg`, `BuggySenderHintGrantsAllow = TRUE`) and its refinement input `receiver_authorized` is now derived from the receiver's real Nomos decision, so a sender hint cannot broaden receiver policy. | Admission and promotion tests in `tests/substrate/exchange/test_poros_admission.py` and `test_poros_message_board.py` of a private deployment (not in this copy); sender-hint denial in `tests/substrate/exchange/test_poros_http.py::test_http_sender_hinted_denial_refines_receiver_unauthorized`. |
| `GenerationPinPreserved`, `LargeObjectHashBound`, `PrivatePayloadNotPublic`, `ProofCompleteness` | Imported rows keep origin generation/hash evidence, private payloads stay out of public views, and proof views include the required outbox, inbox, receipt, admission, policy, publication, and live-row facts. | `tests/substrate/exchange/test_poros_http.py::test_http_receiver_service_exported_outbox_message_refines_published_flow`, `tests/substrate/exchange/test_poros_graph_shape.py`, and deployment message-board tests. |
| `BrokenReceiptImpliesPublication` | The negative model must find a trace where receipt-before-publication disproves the broken claim. | `make formal-poros-counterexample` must produce the expected counterexample. |

## Verification Process

Run:

```sh
make formal-poros-bootstrap
make formal-poros
make formal-poros-counterexample
make formal-poros-refinement
make formal-poros-db-refinement \
  SENDER_DSN=postgres://... \
  RECEIVER_DSN=postgres://... \
  MESSAGE_ID=...
make verify-poros \
  SENDER_DSN=postgres://... \
  RECEIVER_DSN=postgres://... \
  MESSAGE_ID=...
```

The wrapper uses `.tools/tla2tools.jar`, `$TLA2TOOLS_JAR`, or
`tools/tla2tools.jar`; missing Java/TLC fails the run. `make formal-poros`
writes TLC logs plus a strict `.scratch/formal/poros/formal-report.yaml` row
for `formal.poros.exchange-safety`. `make formal-poros-counterexample` writes
the expected non-vacuity report for
`formal.poros.receipt-publication-nonvacuity` and succeeds only when TLC reports
`BrokenReceiptImpliesPublication`.

The fixture refinement target checks `formal/poros/fixtures/two-okg-trace.json`
and writes a mapped state under `.scratch/formal/poros-refinement`; it is a
required runtime-to-model helper, not a TLC invariant. The positive DB
refinement mapping is exercised by
`tests/substrate/exchange/test_poros_http.py::test_http_receiver_service_exported_outbox_message_refines_published_flow`;
that runtime check also verifies sender-side delivery attempt rows and their
body/envelope hashes. Delivery attempts are DB evidence for the sender callback
path, not a separate TLA+ state variable. The DB refinement target fails unless
`SENDER_DSN`, `RECEIVER_DSN`, and `MESSAGE_ID` identify a completed sender
outbox to receiver publication trace.

## Model Boundary

Implementation tests must connect model variables to sender outbox rows,
receiver inbox rows, replay-cache rows, signed receipts, Nomos decision rows,
admission evidence, blocked-state evidence, receiver generations, live graph
rows, and public proof/read-side views. Cryptographic implementations, object
stores, HTTP, operating systems, and Postgres are outside the model and need
runtime tests plus library-level guarantees.
