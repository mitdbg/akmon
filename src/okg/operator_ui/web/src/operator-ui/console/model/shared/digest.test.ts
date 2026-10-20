// The digest against the published SHA-1 test vectors. A title suffix built
// on this has to be the same eight characters in every build and browser, so
// the function is checked against the standard's own answers rather than
// against itself.
import { describe, expect, it } from "vitest";
import { sha1Hex } from "@operator-ui/console/model/shared/digest";

describe("sha1Hex", () => {
  it("answers the published vectors", () => {
    expect(sha1Hex("")).toBe("da39a3ee5e6b4b0d3255bfef95601890afd80709");
    expect(sha1Hex("abc")).toBe("a9993e364706816aba3e25717850c26c9cd0d89d");
    expect(sha1Hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"))
      .toBe("84983e441c3bd26ebaae4aa1f95129e5e54670f1");
    // 1,000,000 "a" is the third vector; this is the 55/56/64-byte boundary
    // instead, which is where a padding mistake actually shows up.
    expect(sha1Hex("a".repeat(55))).toBe("c1c8bbdc22796e28c0e15163d20899b65621d65a");
    expect(sha1Hex("a".repeat(56))).toBe("c2db330f6083854c99d4b5bfb6e8f29f201be699");
    expect(sha1Hex("a".repeat(64))).toBe("0098ba824b5c16427bd7a1122a5a442a25ec644d");
  });

  it("reads the same bytes a browser would, for a non-ASCII id", () => {
    // UTF-8, not UTF-16 code units: an id carrying a non-ASCII path must
    // digest the same here as in the browser.
    expect(sha1Hex("é")).toBe("bf15be717ac1b080b4f1c456692825891ff5073d");
  });
});
