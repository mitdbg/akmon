// Text widths for the graph's label placement, measured with the page's own
// fonts on a canvas.
//
// A browser always has a 2D canvas. jsdom (the unit-test DOM) has none: there
// the width is estimated from the character count, which is what a test
// needs to lay the tree out and nothing a reader ever sees.
//
// The labels are re-placed on every zoom step, so widths are remembered: the
// fonts are read once per theme (a theme may change them), and each text's
// width once per font.
import type { MeasureText } from "@operator-ui/console/model/graph/typeMapLayout";

let context: CanvasRenderingContext2D | null | undefined;
let fontsFor: string | null = null;
let fontCache: { sans: string; mono: string } = { sans: "sans-serif", mono: "monospace" };
const widths = new Map<string, number>();
let listening = false;

function fonts(): { sans: string; mono: string } {
  const theme = document.documentElement.getAttribute("data-theme") ?? "";
  if (fontsFor !== theme) {
    const style = getComputedStyle(document.documentElement);
    fontCache = {
      sans: style.getPropertyValue("--okg-font-sans").trim() || "sans-serif",
      mono: style.getPropertyValue("--okg-font-mono").trim() || "monospace",
    };
    fontsFor = theme;
    widths.clear();
  }
  return fontCache;
}

export const measureText: MeasureText = (text, px, mono) => {
  if (context === undefined) {
    try {
      context = document.createElement("canvas").getContext("2d");
    } catch {
      context = null;
    }
  }
  if (!context) return text.length * px * (mono ? 0.62 : 0.55);
  if (!listening && typeof document.fonts?.addEventListener === "function") {
    // A web font that arrives late changes every width measured before it.
    document.fonts.addEventListener("loadingdone", () => widths.clear());
    listening = true;
  }
  const f = fonts();
  const key = `${mono ? 1 : 0}|${px}|${text}`;
  const known = widths.get(key);
  if (known !== undefined) return known;
  context.font = `${mono ? 600 : 400} ${px}px ${mono ? f.mono : f.sans}`;
  const w = context.measureText(text).width;
  if (widths.size > 5000) widths.clear();
  widths.set(key, w);
  return w;
};
