// @vitest-environment jsdom
//
// The HTML viewer's policy injection, asserted on the parsed document rather
// than on the string. The four bypass documents below came from an
// adversarial review of the earlier string-splice version: a comment before
// the head, a link before the head tag, a title containing "<head>", and a
// meta refresh. Each one left a resource that fetched from a foreign origin
// through a sandboxed frame. The policy has to be the first child of the
// real head, and nothing else in the document may redirect it (meta
// http-equiv) or rebase its URLs (base).
import { describe, expect, it } from "vitest";

import { FRAME_CSP, withPolicy } from "@operator-ui/console/viewers/HtmlFrame";

function parse(html: string): Document {
  return new DOMParser().parseFromString(html, "text/html");
}

function expectPolicyFirstAndNothingElse(out: string): Document {
  const doc = parse(out);
  const first = doc.head.firstElementChild;
  expect(first, "the head has a first child").not.toBeNull();
  expect(first?.tagName).toBe("META");
  expect(first?.getAttribute("http-equiv")).toBe("Content-Security-Policy");
  expect(first?.getAttribute("content")).toBe(FRAME_CSP);
  expect(doc.querySelectorAll("meta[http-equiv]").length, "only the policy meta carries http-equiv").toBe(1);
  expect(doc.querySelectorAll("base").length).toBe(0);
  return doc;
}

const BYPASSES: Record<string, string> = {
  "a comment containing <head> before the real head":
    '<!doctype html><!-- <head> --><html><head><link rel="stylesheet" href="https://probe-a-comment.invalid/a.css"></head><body><img src="https://probe-a-img.invalid/a.png">A</body></html>',
  "a link before the head tag":
    '<link rel="stylesheet" href="https://probe-b-early-link.invalid/b.css"><html><head><title>b</title></head><body>B</body></html>',
  "a meta refresh in the body":
    '<html><head></head><body><meta http-equiv="refresh" content="0;url=https://probe-c-refresh.invalid/"></body></html>',
  "a title containing <head>":
    '<!doctype html><html><title>&lt;head&gt; is text here but <head> is not</title><head></head><body><img src="https://probe-d-title.invalid/d.png">D</body></html>',
};

describe("withPolicy", () => {
  it("puts the policy first in the head of an ordinary page and keeps the page", () => {
    const out = withPolicy("<html><head><title>t</title></head><body><p>body text</p></body></html>");
    const doc = expectPolicyFirstAndNothingElse(out);
    expect(doc.title).toBe("t");
    expect(doc.body.textContent).toBe("body text");
    expect(out.startsWith("<!doctype html><html>")).toBe(true);
  });

  it("gives a page with no head one, and a bare fragment a whole document", () => {
    expectPolicyFirstAndNothingElse(withPolicy("<html><body>x</body></html>"));
    const doc = expectPolicyFirstAndNothingElse(withPolicy("<p>bare</p>"));
    expect(doc.body.textContent).toBe("bare");
  });

  for (const [name, html] of Object.entries(BYPASSES)) {
    it(`closes ${name}`, () => {
      expectPolicyFirstAndNothingElse(withPolicy(html));
    });
  }

  it("removes every base element and every other http-equiv meta, wherever they are", () => {
    const out = withPolicy([
      "<html><head>",
      '<meta http-equiv="refresh" content="0;url=https://x.invalid/">',
      '<base href="https://x.invalid/">',
      '<META HTTP-EQUIV="Content-Security-Policy" content="default-src *">',
      "</head><body>",
      '<base href="https://y.invalid/">',
      '<noscript><meta http-equiv="refresh" content="0;url=https://z.invalid/"></noscript>',
      "text</body></html>",
    ].join(""));
    const doc = expectPolicyFirstAndNothingElse(out);
    expect(doc.body.textContent).toBe("text");
    expect(out).not.toContain("refresh");
    expect(out).not.toContain("default-src *");
  });

  it("carries a policy that loads nothing from anywhere", () => {
    expect(FRAME_CSP).toContain("default-src 'none'");
  });
});
