// A synchronous SHA-1, for naming a record in a title without printing the
// record's whole internal id.
//
// Written out here rather than called from `crypto.subtle`, which is async:
// a list's titles are computed while it renders, and a promise there would
// mean rows that draw without their suffix and then change under the reader.
// Nothing here is a security claim -- SHA-1 is used only because it gives the
// same eight characters for the same id in every build, run and browser, so
// two rows named apart today are named apart tomorrow.

/** `a + b` as a 32-bit unsigned value. */
function add32(a: number, b: number): number {
  return (a + b) >>> 0;
}

function rotl(value: number, by: number): number {
  return ((value << by) | (value >>> (32 - by))) >>> 0;
}

/** The SHA-1 of a string's UTF-8 bytes, as 40 lowercase hex characters. */
export function sha1Hex(input: string): string {
  const bytes = new TextEncoder().encode(input);
  const bitLength = bytes.length * 8;
  // The padded message: the bytes, a 0x80 byte, zeroes, then the length as a
  // 64-bit big-endian count of bits.
  const padded = new Uint8Array((((bytes.length + 8) >> 6) + 1) << 6);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  // The high word of the length: a title's id is never 512 MB of text, so it
  // is always zero, and it is written rather than assumed.
  view.setUint32(padded.length - 8, Math.floor(bitLength / 0x100000000));
  view.setUint32(padded.length - 4, bitLength >>> 0);

  let h0 = 0x67452301;
  let h1 = 0xefcdab89;
  let h2 = 0x98badcfe;
  let h3 = 0x10325476;
  let h4 = 0xc3d2e1f0;
  const words = new Uint32Array(80);

  for (let at = 0; at < padded.length; at += 64) {
    for (let index = 0; index < 16; index += 1) {
      words[index] = view.getUint32(at + index * 4);
    }
    for (let index = 16; index < 80; index += 1) {
      words[index] = rotl(
        words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16], 1,
      );
    }
    let a = h0; let b = h1; let c = h2; let d = h3; let e = h4;
    for (let index = 0; index < 80; index += 1) {
      let f: number;
      let k: number;
      if (index < 20) { f = (b & c) | (~b & d); k = 0x5a827999; }
      else if (index < 40) { f = b ^ c ^ d; k = 0x6ed9eba1; }
      else if (index < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8f1bbcdc; }
      else { f = b ^ c ^ d; k = 0xca62c1d6; }
      const next = add32(add32(add32(add32(rotl(a, 5), f >>> 0), e), k), words[index]);
      e = d; d = c; c = rotl(b, 30); b = a; a = next;
    }
    h0 = add32(h0, a); h1 = add32(h1, b); h2 = add32(h2, c);
    h3 = add32(h3, d); h4 = add32(h4, e);
  }

  return [h0, h1, h2, h3, h4].map((word) => word.toString(16).padStart(8, "0")).join("");
}
