// A seeded pseudo-random generator, so every fixture regenerates byte for
// byte. Math.random() would make `generate-fixtures.mjs --check` a coin toss
// and the captured screenshots un-diffable between runs.
//
// mulberry32: 32-bit state, one multiply-xorshift round per draw. Chosen for
// being four lines and exactly reproducible across Node versions -- not for
// statistical quality, which a fixture does not need.
export function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** An integer in [lo, hi]. */
export function intBetween(rand, lo, hi) {
  return lo + Math.floor(rand() * (hi - lo + 1));
}

/** One element of `items`. Refuses an empty list rather than returning
 *  undefined, which would reach a fixture as a null field. */
export function pick(rand, items) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error("pick() was given no items");
  }
  return items[Math.floor(rand() * items.length) % items.length];
}
