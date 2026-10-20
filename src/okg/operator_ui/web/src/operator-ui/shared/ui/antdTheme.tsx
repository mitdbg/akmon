import { useEffect, useMemo, useState, type PropsWithChildren } from "react";
import { App as AntApp, ConfigProvider, theme as antdTheme, type ThemeConfig } from "antd";
import { THEME_IS_DARK, useTheme } from "@operator-ui/styles/theme";

/**
 * Ant Design reads its colours from the operator UI's own design tokens.
 *
 * tokens.css stays the single source of truth: each firing (and any
 * deployment-declared override applied to the document root) sets the
 * `--okg-*` custom properties, and this bridge copies their COMPUTED values
 * into Ant Design's theme. Ant Design derives hover and active shades from a
 * seed colour, so it needs the resolved value, not a `var(...)` reference.
 */

/** Ant Design seed tokens and the design token each one is read from. */
export const ANTD_TOKEN_SOURCES = {
  colorPrimary: "--okg-color-accent",
  colorLink: "--okg-color-link",
  colorSuccess: "--okg-color-ok",
  colorWarning: "--okg-color-warn",
  colorError: "--okg-color-bad",
  colorInfo: "--okg-color-info",
  colorBgLayout: "--okg-color-bg",
  colorBgContainer: "--okg-color-surface",
  colorFillAlter: "--okg-color-surface-muted",
  colorBorder: "--okg-color-line",
  colorBorderSecondary: "--okg-color-line-soft",
  colorText: "--okg-color-text",
  colorTextHeading: "--okg-color-strong",
  colorTextSecondary: "--okg-color-muted",
  colorTextTertiary: "--okg-color-subtle",
  fontFamily: "--okg-font-sans",
  fontFamilyCode: "--okg-font-mono",
} as const;

export type AntdTokenName = keyof typeof ANTD_TOKEN_SOURCES;
export type ResolvedTokens = Partial<Record<AntdTokenName, string>>;

/**
 * Read the resolved design tokens from an element's computed style.
 *
 * A token the stylesheet does not define is left out, so Ant Design keeps its
 * own value for it. In the browser tokens.css defines every one; the gap only
 * shows in environments that load no stylesheet, such as unit tests.
 */
export function readResolvedTokens(element: Element): ResolvedTokens {
  const style = globalThis.getComputedStyle?.(element);
  if (!style) return {};
  const resolved: ResolvedTokens = {};
  for (const [name, property] of Object.entries(ANTD_TOKEN_SOURCES) as [AntdTokenName, string][]) {
    const value = style.getPropertyValue(property).trim();
    if (value) resolved[name] = value;
  }
  return resolved;
}

export function buildAntdTheme(tokens: ResolvedTokens, dark: boolean): ThemeConfig {
  return {
    algorithm: dark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
    token: {
      ...tokens,
      borderRadius: 6,
      fontSize: 14,
    },
    components: {
      Table: { headerBg: tokens.colorFillAlter, cellPaddingBlockSM: 6 },
      Descriptions: { labelBg: tokens.colorFillAlter },
    },
  };
}

function documentTokens(): ResolvedTokens {
  return typeof document === "undefined" ? {} : readResolvedTokens(document.documentElement);
}

/**
 * Re-read the tokens whenever the root's theme or inline overrides change. The
 * first render already reads them, so Ant Design never paints its own default
 * colours before the design tokens arrive.
 */
function useResolvedTokens(): ResolvedTokens {
  const firing = useTheme();
  const [tokens, setTokens] = useState<ResolvedTokens>(documentTokens);

  useEffect(() => {
    if (typeof document === "undefined") return;
    const root = document.documentElement;
    const read = () => setTokens(readResolvedTokens(root));
    read();
    if (typeof MutationObserver === "undefined") return;
    const observer = new MutationObserver(read);
    observer.observe(root, { attributes: true, attributeFilter: ["data-theme", "style"] });
    return () => observer.disconnect();
  }, [firing]);

  return tokens;
}

/** Wrap a view so every Ant Design component inside it uses the active firing. */
export function OperatorAntdProvider({ children }: PropsWithChildren) {
  const firing = useTheme();
  const tokens = useResolvedTokens();
  const config = useMemo(
    () => buildAntdTheme(tokens, THEME_IS_DARK[firing]),
    [tokens, firing],
  );
  return (
    <ConfigProvider theme={config} componentSize="middle">
      <AntApp component={false}>{children}</AntApp>
    </ConfigProvider>
  );
}
