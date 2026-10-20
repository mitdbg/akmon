// The failure a console view raises when a read it needs is not there.
//
// It lives in the console tree, not the design tree, because LoudFailure
// (shipping code) has to recognise it, and shipping code must import
// nothing under @operator-ui/design-harness: the production build resolves the
// harness to a stub, and one stray import would pull the fixtures in.

/** Raised when a view asks the design fixture for a read it does not define.
 *  Carries the key it looked for and the keys it has for that route, because
 *  the first question after this failure is always "then what DID you
 *  define?". The design fixture client is the only thing that throws it. */
export class FixtureReadMissing extends Error {
  readonly fixture: string;
  readonly key: string;
  readonly available: string[];

  constructor(fixture: string, key: string, available: string[]) {
    super(
      `The design fixture "${fixture}" defines no read for ${key}. `
      + (available.length
        ? `It defines ${available.length} read(s) for that route: ${available.slice(0, 6).join(", ")}`
        : "It defines no read for that route at all.")
      + " Add it in scripts/operator-ui-design/lib/deployments.mjs and regenerate; do not render without it.",
    );
    this.name = "FixtureReadMissing";
    this.fixture = fixture;
    this.key = key;
    this.available = available;
  }
}
