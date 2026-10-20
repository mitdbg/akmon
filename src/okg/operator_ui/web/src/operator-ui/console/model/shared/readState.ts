// Where a read has got to.
//
// `pending` is not `failed` and neither is `answered`, and the page says a
// different thing for each. Kept out of `home.ts` so the model that lists a
// failed read (`attention.ts`) and the model that builds the page from it do
// not import each other.

/** Where the producers read has got to. A calm chip, an all-clear sentence
 *  and a producer count are each a claim about every producer, so each may be
 *  printed only in the first of these. */
export type ProducersRead = "answered" | "pending" | "failed";

/**
 * True once the producers read has answered.
 *
 * ONE read. It used to be three -- the source, enricher and view diagnostics
 * -- and this function existed because two of three landing would undercount
 * every state on the page with nothing saying so. Home reads
 * `console/producers` now, which composes all three server-side and returns
 * every producer once, so there is one answer to wait for and the partial
 * state is the read's own `omitted_data` rather than this browser's
 * bookkeeping.
 */
export function producersAnswered(reads: { producers: unknown }): boolean {
  return Boolean(reads.producers);
}

/** Which of the three states the producers read is in. `producersPending`
 *  alone could not tell "still reading" from "the read failed". */
export function producersState(reads: {
  producers: unknown; producersPending?: boolean;
}): ProducersRead {
  if (producersAnswered(reads)) return "answered";
  return reads.producersPending ? "pending" : "failed";
}
