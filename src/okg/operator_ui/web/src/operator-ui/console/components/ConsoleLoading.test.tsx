// The loading line must sit in the page's own column, not against the window.
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { ConsoleLoading } from "@operator-ui/console/components/ConsoleLoading";

describe("ConsoleLoading", () => {
  it("renders inside the console's page column", () => {
    // It was a bare <p> outside `.okg-console`, so on first paint the line
    // landed at x=0 against the window edge in the browser's default type,
    // while the page it stands in for sits in a 960px column.
    const markup = renderToStaticMarkup(<ConsoleLoading what="this deployment" />);
    expect(markup).toContain('class="okg-console"');
    expect(markup).toContain('data-testid="console-loading"');
    expect(markup.indexOf('class="okg-console"'))
      .toBeLessThan(markup.indexOf("Reading this deployment"));
  });

  it("names what it is reading", () => {
    expect(renderToStaticMarkup(<ConsoleLoading what="deployment_configs" />))
      .toContain("Reading deployment_configs…");
  });
});
