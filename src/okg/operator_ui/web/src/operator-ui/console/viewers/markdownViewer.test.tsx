// The Markdown viewer draws an image only from the artifact's own origin.
// GitHub's sanitize schema allows http(s) image sources, and an ingested
// document saying `![](https://evil/x.png)` would fetch from the operator's
// browser. A relative image resolves under the artifact and is kept; a
// remote, a data: and a protocol-relative one are withheld and named.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { RenderedMarkdown, sameOriginImage } from "@operator-ui/console/viewers/MarkdownViewer";

const PAGE = "http://console.test/";
const ARTIFACT = "/viz/api/records/artifact?record=paper:okg-2026&path=README.md";

function render(text: string): string {
  return renderToStaticMarkup(RenderedMarkdown({ text, artifactUrl: ARTIFACT, pageHref: PAGE }));
}

describe("sameOriginImage", () => {
  it("resolves a relative image under the artifact and keeps it", () => {
    expect(sameOriginImage("figures/a.png", ARTIFACT, PAGE)).toBe("http://console.test/viz/api/records/figures/a.png");
    expect(sameOriginImage("/static/b.png", ARTIFACT, PAGE)).toBe("http://console.test/static/b.png");
  });

  it("refuses another origin, a data: URL, a protocol-relative host and nothing", () => {
    expect(sameOriginImage("https://evil.invalid/x.png", ARTIFACT, PAGE)).toBeNull();
    expect(sameOriginImage("data:image/png;base64,iVBORw0KGgo=", ARTIFACT, PAGE)).toBeNull();
    expect(sameOriginImage("//evil.invalid/x.png", ARTIFACT, PAGE)).toBeNull();
    expect(sameOriginImage("http://console.test:8443/x.png", ARTIFACT, PAGE)).toBeNull();
    expect(sameOriginImage(undefined, ARTIFACT, PAGE)).toBeNull();
  });
});

describe("RenderedMarkdown", () => {
  it("strips a remote image and says so", () => {
    const html = render("before ![the figure](https://evil.invalid/x.png) after");
    expect(html).not.toContain("<img");
    expect(html).not.toContain("evil.invalid");
    expect(html).toContain("the figure (not shown: not from this deployment)");
    expect(html).toContain("data-image-withheld");
  });

  it("strips a data: image", () => {
    const html = render("![](data:image/png;base64,iVBORw0KGgo=)");
    expect(html).not.toContain("<img");
    expect(html).not.toContain("base64");
    expect(html).toContain("image (not shown: not from this deployment)");
  });

  it("strips a protocol-relative image, which has no scheme for the schema to refuse", () => {
    const html = render("![](//evil.invalid/x.png)");
    expect(html).not.toContain("<img");
    expect(html).not.toContain("evil.invalid");
  });

  it("keeps a relative image, resolved under the artifact", () => {
    const html = render("![figure one](figures/a.png)");
    expect(html).toContain('<img src="http://console.test/viz/api/records/figures/a.png" alt="figure one"/>');
    expect(html).not.toContain("not shown");
  });

  it("still opens a link in a new tab and drops a javascript: one", () => {
    const html = render("[docs](https://docs.invalid/) [bad](javascript:alert(1))");
    expect(html).toContain('<a href="https://docs.invalid/" target="_blank" rel="noopener noreferrer">docs</a>');
    expect(html).not.toContain("javascript:");
  });
});
