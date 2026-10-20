import { Button, theme as antdTheme } from "antd";
import { readFileSync } from "node:fs";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ANTD_TOKEN_SOURCES,
  buildAntdTheme,
  OperatorAntdProvider,
  readResolvedTokens,
} from "@operator-ui/shared/ui/antdTheme";

const tokensCss = readFileSync(
  new URL("../../styles/tokens.css", import.meta.url),
  "utf8",
);

describe("Ant Design theme bridge", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reads every Ant Design colour and font from a token the stylesheet defines", () => {
    for (const property of Object.values(ANTD_TOKEN_SOURCES)) {
      expect(tokensCss, `${property} must be defined in tokens.css`).toContain(`${property}:`);
    }
  });

  it("copies the resolved token values and omits the ones the element does not define", () => {
    const values: Record<string, string> = {
      "--okg-color-accent": " #0a4f88 ",
      "--okg-color-bad": "#b23a2a",
      "--okg-font-mono": "\"SF Mono\", monospace",
    };
    vi.stubGlobal("getComputedStyle", () => ({
      getPropertyValue: (property: string) => values[property] ?? "",
    }));

    const tokens = readResolvedTokens({} as Element);

    expect(tokens).toEqual({
      colorPrimary: "#0a4f88",
      colorError: "#b23a2a",
      fontFamilyCode: "\"SF Mono\", monospace",
    });
  });

  it("uses the dark algorithm only for dark firings", () => {
    const light = buildAntdTheme({ colorPrimary: "#0a4f88" }, false);
    const dark = buildAntdTheme({ colorPrimary: "#e07b1a" }, true);

    expect(light.algorithm).toBe(antdTheme.defaultAlgorithm);
    expect(dark.algorithm).toBe(antdTheme.darkAlgorithm);
    expect(light.token?.colorPrimary).toBe("#0a4f88");
    expect(dark.token?.colorPrimary).toBe("#e07b1a");
  });

  it("renders Ant Design components inside the provider", () => {
    const html = renderToStaticMarkup(
      <OperatorAntdProvider>
        <Button type="primary">Search</Button>
      </OperatorAntdProvider>,
    );

    expect(html).toContain("ant-btn");
    expect(html).toContain("Search");
  });
});
