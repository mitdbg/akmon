import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { chromium } from "playwright";

const viewerPath = resolve(process.argv[2] ?? "bench/results/flamegraph-viewer/bench-flamegraph.html");
const viewerUrl = pathToFileURL(viewerPath).href;
const failureScreenshot = resolve(dirname(viewerPath), "bench-flamegraph-smoke-failure.png");

const pageErrors = [];
const consoleErrors = [];

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });

page.on("pageerror", (error) => {
  pageErrors.push(error.stack || error.message || String(error));
});
page.on("console", (message) => {
  if (message.type() === "error") {
    consoleErrors.push(message.text());
  }
});

try {
  await page.goto(viewerUrl, { waitUntil: "load", timeout: 15_000 });
  await page.locator(".okg-bench-flamegraph-shell").waitFor({ state: "visible", timeout: 10_000 });
  await page.locator(".okg-bench-flamegraph-back").waitFor({ state: "visible", timeout: 10_000 });
  await page.locator(".okg-perf-profile-flamegraph").waitFor({ state: "visible", timeout: 10_000 });
  await page.locator(".okg-perf-profile-flamegraph__chart svg").waitFor({
    state: "visible",
    timeout: 10_000,
  });
  await page.waitForFunction(() => document.querySelectorAll("g.frame").length > 0, null, {
    timeout: 10_000,
  });

  const result = await page.evaluate(() => ({
    shellCount: document.querySelectorAll(".okg-bench-flamegraph-shell").length,
    backCount: document.querySelectorAll(".okg-bench-flamegraph-back").length,
    pickerCount: document.querySelectorAll(".okg-bench-flamegraph-picker select").length,
    pickerOptionCount: document.querySelectorAll(".okg-bench-flamegraph-picker option").length,
    manifestEntryCount: (() => {
      const raw = document.getElementById("okg-flamegraph-manifest")?.textContent?.trim();
      if (!raw) return 0;
      try {
        const parsed = JSON.parse(raw);
        return Array.isArray(parsed) ? parsed.length : 0;
      } catch {
        return -1;
      }
    })(),
    backHref: document.querySelector(".okg-bench-flamegraph-back")?.getAttribute("href"),
    profileCount: document.querySelectorAll(".okg-perf-profile-flamegraph").length,
    svgCount: document.querySelectorAll(".okg-perf-profile-flamegraph__chart svg").length,
    frameCount: document.querySelectorAll("g.frame").length,
    text: document.body.innerText.slice(0, 240),
  }));
  const meaningfulConsoleErrors = consoleErrors.filter((text) => !text.includes("favicon.ico"));
  if (pageErrors.length > 0 || meaningfulConsoleErrors.length > 0) {
    throw new Error(
      JSON.stringify({ pageErrors, consoleErrors: meaningfulConsoleErrors }, null, 2),
    );
  }
  if (
    result.shellCount !== 1
    || result.backCount !== 1
    || result.profileCount !== 1
    || result.svgCount !== 1
  ) {
    throw new Error(`unexpected flamegraph DOM counts: ${JSON.stringify(result)}`);
  }
  if (result.manifestEntryCount < 0) {
    throw new Error(`invalid flamegraph manifest: ${JSON.stringify(result)}`);
  }
  if (result.manifestEntryCount > 1 && result.pickerCount !== 1) {
    throw new Error(`missing flamegraph picker: ${JSON.stringify(result)}`);
  }
  if (result.manifestEntryCount > 1 && result.pickerOptionCount !== result.manifestEntryCount) {
    throw new Error(`unexpected flamegraph picker options: ${JSON.stringify(result)}`);
  }
  console.log(JSON.stringify({ ok: true, viewer: viewerPath, ...result }, null, 2));
} catch (error) {
  await mkdir(dirname(failureScreenshot), { recursive: true });
  await page.screenshot({ path: failureScreenshot, fullPage: true });
  console.error(JSON.stringify({ ok: false, viewer: viewerPath, failureScreenshot }, null, 2));
  throw error;
} finally {
  await browser.close();
}
