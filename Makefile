# OKG Makefile — convenience targets for the local-dev workflow.
#
# This is a minimal Makefile: most operator commands run through the
# `okg` CLI directly. Targets here cover the dev-loop ergonomics that
# `sdk-design.md` and Phase 1 §2.6.2 specify need a one-shot
# affordance.

.PHONY: reset-dbos formal-admission formal-admission-bootstrap formal-admission-counterexample formal-pact-mutation formal-pact-projection formal-pact-formal-evidence formal-pact-workflow-authority formal-pact-v4-decision-ledger formal-console formal-console-vocabulary-lookup formal-console-vocabulary-lookup-counterexample formal-console-focus formal-console-offline-replay formal-console-offline-replay-counterexample verify-admission verify-console formal-nomos formal-nomos-bootstrap formal-nomos-counterexample verify-nomos formal-poros formal-poros-bootstrap formal-poros-counterexample formal-poros-refinement formal-poros-db-refinement-env formal-poros-db-refinement verify-poros formal-suite-bootstrap formal-suite-smoke formal-suite-counterexamples formal-suite lint-cli

# DEPLOYMENT defaults to okg-workspace for the dev dogfood;
# override on the command line: `make reset-dbos DEPLOYMENT=af_pubs`.
DEPLOYMENT ?= okg-workspace
FORMAL_ADMISSION_OUT ?= .scratch/formal/admission
FORMAL_ADMISSION_COUNTEREXAMPLE_OUT ?= .scratch/formal/admission-counterexample
FORMAL_NOMOS_OUT ?= .scratch/formal/nomos-control-plane
FORMAL_NOMOS_COUNTEREXAMPLE_OUT ?= .scratch/formal/nomos-counterexample
FORMAL_POROS_OUT ?= .scratch/formal/poros
FORMAL_POROS_COUNTEREXAMPLE_OUT ?= .scratch/formal/poros-counterexample
FORMAL_POROS_REFINEMENT_OUT ?= .scratch/formal/poros-refinement
FORMAL_POROS_DB_REFINEMENT_OUT ?= .scratch/formal/poros-db-refinement
FORMAL_PACT_MUTATION_OUT ?= .scratch/formal/pact-mutation-safety
FORMAL_PACT_FORMAL_EVIDENCE_OUT ?= .scratch/formal/pact-formal-evidence
FORMAL_PACT_WORKFLOW_AUTHORITY_OUT ?= .scratch/formal/workflow-authority
FORMAL_PACT_V4_DECISION_LEDGER_OUT ?= .scratch/formal/pact-v4-decision-ledger
FORMAL_CONSOLE_VOCAB_OUT ?= .scratch/formal/console-vocabulary-lookup
FORMAL_CONSOLE_VOCAB_CE_OUT ?= .scratch/formal/console-vocabulary-lookup-counterexample
FORMAL_CONSOLE_FOCUS_OUT ?= .scratch/formal/console-focus
FORMAL_CONSOLE_OFFLINE_OUT ?= .scratch/formal/console-offline-replay
FORMAL_CONSOLE_OFFLINE_CE_OUT ?= .scratch/formal/console-offline-replay-counterexample
FORMAL_PACT_PROJECTION_OUT ?= .scratch/formal/pact-projection

reset-dbos:
	@echo "→ resetting DBOS state for deployment=$(DEPLOYMENT)"
	uv run okg runtime reset --deployment $(DEPLOYMENT) --confirm

formal-admission:
	uv run python scripts/check_formal_admission.py --profile full --out $(FORMAL_ADMISSION_OUT)

formal-admission-bootstrap:
	uv run python scripts/bootstrap_formal_admission.py

formal-admission-counterexample:
	uv run python scripts/check_formal_admission.py \
		--model AdmissionCounterexample.tla \
		--config AdmissionCounterexample.cfg \
		--out $(FORMAL_ADMISSION_COUNTEREXAMPLE_OUT) \
		--expect-counterexample

formal-pact-mutation:
	uv run python scripts/check_formal_pact_mutation.py --out $(FORMAL_PACT_MUTATION_OUT)

formal-pact-projection:
	uv run python scripts/check_formal_pact_projection.py --out $(FORMAL_PACT_PROJECTION_OUT)

formal-pact-formal-evidence:
	uv run --extra dev python scripts/check_formal_pact_formal_evidence.py --out $(FORMAL_PACT_FORMAL_EVIDENCE_OUT)

formal-pact-workflow-authority:
	uv run python scripts/check_formal_workflow_authority.py --out $(FORMAL_PACT_WORKFLOW_AUTHORITY_OUT)

formal-pact-v4-decision-ledger:
	uv run python scripts/check_formal_pact_v4_decision_ledger.py --out $(FORMAL_PACT_V4_DECISION_LEDGER_OUT)

verify-admission: formal-admission formal-admission-counterexample

formal-console-vocabulary-lookup:
	uv run python scripts/check_formal_console.py \
		--archived-helper \
		--model vocabulary-lookup \
		--out $(FORMAL_CONSOLE_VOCAB_OUT) \
		--checker-command "make formal-console-vocabulary-lookup"

formal-console-vocabulary-lookup-counterexample:
	uv run python scripts/check_formal_console.py \
		--archived-helper \
		--model vocabulary-lookup-counterexample \
		--out $(FORMAL_CONSOLE_VOCAB_CE_OUT)

formal-console-focus:
	uv run python scripts/check_formal_console.py \
		--archived-helper \
		--model console-focus \
		--out $(FORMAL_CONSOLE_FOCUS_OUT) \
		--checker-command "make formal-console-focus"

formal-console-offline-replay:
	uv run python scripts/check_formal_console.py \
		--archived-helper \
		--model offline-replay \
		--out $(FORMAL_CONSOLE_OFFLINE_OUT) \
		--checker-command "make formal-console-offline-replay"

formal-console-offline-replay-counterexample:
	uv run python scripts/check_formal_console.py \
		--archived-helper \
		--model offline-replay-counterexample \
		--out $(FORMAL_CONSOLE_OFFLINE_CE_OUT) \
		--checker-command "make formal-console-offline-replay-counterexample"

formal-console: formal-console-vocabulary-lookup formal-console-focus formal-console-offline-replay

verify-console: formal-console formal-console-vocabulary-lookup-counterexample formal-console-offline-replay-counterexample

formal-nomos:
	uv run python scripts/check_formal_nomos.py --profile full --out $(FORMAL_NOMOS_OUT)

formal-nomos-bootstrap:
	uv run python scripts/bootstrap_formal_admission.py

formal-nomos-counterexample:
	uv run python scripts/check_formal_nomos.py \
		--model NomosCounterexample.tla \
		--config NomosCounterexample.cfg \
		--out $(FORMAL_NOMOS_COUNTEREXAMPLE_OUT) \
		--expect-counterexample

verify-nomos: formal-nomos formal-nomos-counterexample

formal-poros:
	uv run python scripts/check_formal_poros.py --out $(FORMAL_POROS_OUT)

formal-poros-bootstrap:
	uv run python scripts/bootstrap_formal_admission.py

formal-poros-counterexample:
	uv run python scripts/check_formal_poros.py \
		--model PorosExchangeCounterexample.tla \
		--config PorosExchangeCounterexample.cfg \
		--out $(FORMAL_POROS_COUNTEREXAMPLE_OUT) \
		--expect-counterexample

formal-poros-refinement:
	uv run python scripts/check_poros_refinement_fixture.py \
		--out $(FORMAL_POROS_REFINEMENT_OUT)

formal-poros-db-refinement-env:
	@test -n "$(SENDER_DSN)" || (echo "SENDER_DSN is required for Poros DB refinement" >&2; exit 1)
	@test -n "$(RECEIVER_DSN)" || (echo "RECEIVER_DSN is required for Poros DB refinement" >&2; exit 1)
	@test -n "$(MESSAGE_ID)" || (echo "MESSAGE_ID is required for Poros DB refinement" >&2; exit 1)

formal-poros-db-refinement: formal-poros-db-refinement-env
	uv run python scripts/check_poros_refinement_from_db.py \
		--sender-dsn "$(SENDER_DSN)" \
		--receiver-dsn "$(RECEIVER_DSN)" \
		--message-id "$(MESSAGE_ID)" \
		--out $(FORMAL_POROS_DB_REFINEMENT_OUT)

verify-poros: formal-poros formal-poros-counterexample formal-poros-refinement formal-poros-db-refinement

# Aggregate formal gate — the single gating run path over the whole corpus.
# `formal-suite-smoke` runs every checker's fast configuration; `-counterexamples`
# runs every non-vacuity witness. Both exit non-zero on any non-pass, so a model
# that regresses to vacuous, unsound, or stale turns the gate RED.
formal-suite-bootstrap:
	uv run python scripts/bootstrap_formal_admission.py

formal-suite-smoke:
	uv run python scripts/run_formal_suite.py --mode smoke

formal-suite-counterexamples:
	uv run python scripts/run_formal_suite.py --mode counterexamples

formal-suite: formal-suite-smoke formal-suite-counterexamples

# CLI surface gate: `lint-cli` runs the full argparse ↔ docs/manifest
# cross-check + the docs/source scan.

lint-cli:
	uv run python3 scripts/lint_cli_references.py --all
