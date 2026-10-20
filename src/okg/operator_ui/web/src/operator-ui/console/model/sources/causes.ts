// What the convergence read recorded, in words.
//
// `_source_convergence_row` in `src/okg/substrate/source_sync_convergence.py`
// decides a source's state and its reason together: `blocked` with
// `completed_scope_watermark_stale` is one verdict, not a state plus a note.
// The producer page printed the state and dropped the code, so it could say a
// source was blocked and could not say by what.
//
// Every sentence below says what the check measured. None of them says what to
// do about it: no read here carries a recovery instruction, and a page that
// invented one would be guessing at a runtime it cannot see.
//
// A code with no entry is printed as its own words -- underscores for spaces --
// and nothing else. That is the honest fallback: the reader gets the code the
// read returned, made readable, and this file does not pretend to know a
// meaning it was never given.

/** Every reason `_source_convergence_row` and its two fallbacks can record,
 *  in the words the page prints. The Python test
 *  `test_every_recorded_convergence_reason_has_words` holds this map against
 *  the literals in `source_sync_convergence.py`, so a reason added there and
 *  not added here fails rather than silently falling back. */
export const CONVERGENCE_CAUSES: Readonly<Record<string, string>> = {
  // Fresh. Printed only where the page prints a cause at all.
  source_not_required_for_readiness:
    "this source is not required for readiness",
  source_progress_freshness_fresh:
    "the source's recorded progress is within the freshness window",
  completed_scope_watermark_fresh:
    "the last completed run's watermark is within the freshness window",

  // Stale, missing, or never evaluated.
  // Printed beside the word Overdue. "Watermark" is the runtime's word for
  // the progress mark a run leaves; the reader's question is when it last
  // ran (review 2026-09-22).
  // No window length: no read serves one (every Archiv3 source declares no
  // schedule), and "the freshness window" named a window no page could state
  // (review 2026-09-23).
  completed_scope_watermark_stale:
    "its last completed run is older than the runtime's freshness limit",
  completed_scope_watermark_missing:
    "no completed run has recorded a watermark",
  completed_scope_watermark_not_evaluated:
    "no completed-scope watermark was evaluated for this source",
  source_progress_freshness_stale:
    "its recorded progress is older than the runtime's freshness limit",
  source_progress_freshness_missing:
    "the source has recorded no progress",
  source_progress_freshness_not_evaluated:
    "the source's progress was not evaluated",

  // Waiting on something that is not the source.
  published_generation_not_observed:
    "the source is fresh, and no published version was observed",
  published_generation_not_visible_to_read_side:
    "the source is fresh, and the published version is not yet visible to the read side",

  // Waiting on a person.
  manual_or_credential_constraint:
    "needs a manual run or a credential to refresh",

  // Not wired.
  source_sync_not_declared_for_readiness_source:
    "no source-sync policy is declared for this source",
  source_not_in_convergence_status:
    "the convergence read returned no row for this source",
  source_registry_missing:
    "the deployment's source registry could not be read",

  // Work is running.
  completed_scope_recovery_active:
    "a completed-scope recovery is running",
  source_progress_recovery_active:
    "a progress recovery is running",
  completed_scope_recovery_stale:
    "the completed-scope recovery that is running has gone stale",
  source_progress_recovery_stale:
    "the progress recovery that is running has gone stale",
  active_incomplete_work_does_not_satisfy_completed_freshness:
    "the work now running covers part of the source, which does not satisfy the "
    + "completed-scope freshness this source requires",

  // The convergence read's own fallbacks.
  stale_started_pending_workflow_blocks_completed_scope_recovery:
    "a started workflow has gone stale and is holding up the completed-scope recovery",
  stale_protected_waiting_workflow_blocks_completed_scope_recovery:
    "a protected waiting workflow has gone stale and is holding up the completed-scope recovery",
  stale_replaceable_waiting_workflow_blocks_completed_scope_recovery:
    "a replaceable waiting workflow has gone stale and is holding up the completed-scope recovery",
  credential_or_manual_source_cannot_be_auto_recovered:
    "this source needs a credential or a manual step, so it cannot be recovered automatically",
  source_fresh_or_not_readiness_required:
    "the source is fresh, or readiness does not require it",
  source_fresh_downstream_readiness_blocked:
    "the source is fresh, and something after it is blocking readiness",
  unknown_readiness_gap:
    "the convergence read found a readiness gap it has no code for",

  // `_deployment_exposure`'s verdict on the DEPLOYMENT, not on one source. It
  // reaches a producer row only where a read carries the deployment-level
  // convergence in a source's place, and the words say which subject each one
  // is about so a reader is never told a source is what the deployment is.
  runtime_disabled:
    "the deployment's source-sync runtime is not enabled",
  readiness_sources_use_incomplete_default_event_work:
    "the deployment has readiness sources whose default work covers only part "
    + "of the source, so a stale watermark can go unnoticed",
  readiness_sources_have_completed_scope_default_or_no_runtime_gap:
    "the deployment's readiness sources default to completed-scope work, so "
    + "none is exposed to a stale watermark",
  no_readiness_required_sources_declared:
    "the deployment declares no source that readiness requires",
};

/** The states whose cause the producer page prints. A fresh producer's reason
 *  is not printed: "fresh, because the watermark is within the window" is the
 *  state said twice. */
export const STATES_WITH_A_PRINTED_CAUSE: readonly string[] = [
  "blocked", "manual_required", "overdue", "stale", "stuck", "unsupported",
];

/**
 * One recorded reason code, in words.
 *
 * An unmapped code becomes its own words and nothing more. Inventing a
 * sentence for a code this file has never seen would be the page asserting
 * something no read recorded.
 */
export function causeWords(reason: string | null | undefined): string | null {
  if (!reason) return null;
  const code = reason.trim();
  if (!code) return null;
  return CONVERGENCE_CAUSES[code] ?? code.replace(/_/g, " ");
}
