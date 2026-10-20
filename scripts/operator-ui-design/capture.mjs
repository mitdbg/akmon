#!/usr/bin/env node
// Capture every view of every fixture at both widths.
//
//   node scripts/operator-ui-design/capture.mjs
//
// Writes pact/changes/operator-ui-redesign-mocks/ui/rendered/<fixture>/<view>-1440.png
// for the 1440 captures, which are the tracked proof. The 390 captures and the
// hover state are still taken and measured, but go to
// .tmp/operator-ui-design-captures/<fixture>/ (ignored), so the repository
// carries no capture at all (ui/rendered/ is ignored); ui/evidence/capture.json
// records every capture's repo-relative path, size and sha256, and the audit
// measures 390 on the live page.
// -- six fixtures by four views by two widths, 48 files -- plus one attempt
// detail per fixture that has an attempt, at 1440, as
// <fixture>/updates-detail-1440.png (the attempt is the newest blocked one
// where there is one, otherwise the newest published one; `empty` has none and
// gets no file), plus one producer page per fixture at 1440 as
// <fixture>/sources-detail-1440.png (the failed producer where one exists,
// and on workspace-mixed the withheld enricher as well), plus one hover
// state on archiv3-small -- the pointer over the producer name's copy icon --
// as archiv3-small/sources-detail-hover-1440.png under .tmp/, because the copy icons are
// drawn at 40 per cent and come up on hover, and a still capture cannot
// otherwise show the state the operator judges; plus Explore's other two page
// states at 1440 -- the results for the fixture's one query as
// <fixture>/explore-results-1440.png, the featured record's page as
// <fixture>/explore-record-1440.png (on Related where the record's rows are
// capped), and the record page's other tabs where the fixture carries the
// state: Content with each rendered artifact format, a refused and a missing
// artifact, a chunk's page, Provenance, and Raw with a refused and with an
// oversize payload (see `exploreTargetsFor` in lib/harness.mjs). A Content
// capture is refused unless every viewer on it reports "ready", and any page
// that requests a URL off the dev server fails the run.
//
// Fails, rather than writing a file, when:
//   - the dev server does not come up,
//   - the harness root for the fixture asked for never appears,
//   - the page issues ANY request to /viz/api/. A mock that fell through to a
//     real read would be approved as a mock, which is the whole thing this
//     harness exists to prevent.
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { chromium } from "playwright";
import {
  currentDetailAttemptFor,
  detailAttemptFor,
  exploreTargetsFor,
  fixtureNames,
  foreignRequestListener,
  viewerStates,
  readableDetailAttemptFor,
  sourcesDetailTargetsFor,
  openDetail,
  openExplore,
  openSourcesDetail,
  openView,
  PACT_UI_DIR,
  requirePactUiDir,
  REPO_ROOT,
  startDevServer,
  VIEWS,
  WIDTHS,
} from "./lib/harness.mjs";

/** The one fixture that gets the hover capture. */
const HOVER_FIXTURE = "archiv3-small";

async function main() {
  requirePactUiDir();
  const fixtures = fixtureNames();
  const trackedRoot = join(PACT_UI_DIR, "rendered");
  const untrackedRoot = join(REPO_ROOT, ".tmp", "operator-ui-design-captures");
  const rootFor = (width) => (width === 1440 ? trackedRoot : untrackedRoot);
  const relative = (target) => target.slice(REPO_ROOT.length + 1);
  // No capture is tracked; the manifest's path, size and digest per capture are
  // the record of what was reviewed, and `node scripts/operator-ui-design/capture.mjs` regenerates it.
  const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
  const server = await startDevServer();
  const browser = await chromium.launch();
  const failures = [];
  const written = [];
  const manifest = [];

  try {
    for (const fixture of fixtures) {
      mkdirSync(join(trackedRoot, fixture), { recursive: true });
      mkdirSync(join(untrackedRoot, fixture), { recursive: true });
      for (const { width, height } of WIDTHS) {
        const context = await browser.newContext({
          viewport: { width, height },
          deviceScaleFactor: 1,
          // A fixed zone, or a capture taken in a different month renders
          // different ages and the mismatch ledger chases a clock.
          timezoneId: "UTC",
          colorScheme: "light",
        });
        const page = await context.newPage();
        for (const view of VIEWS) {
          const target = join(rootFor(width), fixture, `${view.toLowerCase()}-${width}.png`);
          try {
            const apiRequests = await openView(page, server.url, view, fixture);
            if (apiRequests.length) {
              failures.push(
                `${fixture}/${view}@${width}: the page read ${apiRequests.length} URL(s) over `
                + `the network, starting with ${apiRequests[0]}`,
              );
              continue;
            }
            // The harness's fixture switcher is fixed to the corner of the
            // viewport, so in a full-page screenshot it lands at the fold and
            // paints over a row of the thing being reviewed. It is design-only
            // scaffolding; hide it for the capture, not for the session.
            await page.evaluate(() => {
              document.documentElement.classList.add("okg-capture");
            });
            const shot = await page.screenshot({ fullPage: true });
            writeFileSync(target, shot);
            const box = await page.evaluate(() => ({
              scrollWidth: document.documentElement.scrollWidth,
              clientWidth: document.documentElement.clientWidth,
              scrollHeight: document.documentElement.scrollHeight,
            }));
            written.push(target);
            manifest.push({
              fixture,
              view,
              width,
              path: relative(target),
              bytes: shot.length,
              sha256: sha256(shot),
              document_height: box.scrollHeight,
              horizontal_overflow: box.scrollWidth - box.clientWidth,
            });
          } catch (error) {
            failures.push(`${fixture}/${view}@${width}: ${error.message}`);
          }
        }

        // The attempt detail, at 1440 only. It is a full-width page in the
        // same column, so the phone capture would say nothing the desktop one
        // does not, and the operator judges 1440.
        const details = width === 1440
          ? [
            ["updates-detail", detailAttemptFor(fixture)],
            ["updates-detail-published", readableDetailAttemptFor(fixture)],
            ["updates-detail-current", currentDetailAttemptFor(fixture)],
          ].filter(([, id]) => Boolean(id))
          : [];
        for (const [name, detail] of details) {
          const target = join(rootFor(width), fixture, `${name}-${width}.png`);
          try {
            const apiRequests = await openDetail(page, server.url, "Updates", fixture, detail);
            if (apiRequests.length) {
              failures.push(
                `${fixture}/Updates-detail@${width}: the page read ${apiRequests.length} `
                + `URL(s) over the network, starting with ${apiRequests[0]}`,
              );
            } else {
              await page.evaluate(() => {
                document.documentElement.classList.add("okg-capture");
              });
              const shot = await page.screenshot({ fullPage: true });
              writeFileSync(target, shot);
              const box = await page.evaluate(() => ({
                scrollWidth: document.documentElement.scrollWidth,
                clientWidth: document.documentElement.clientWidth,
                scrollHeight: document.documentElement.scrollHeight,
              }));
              written.push(target);
              manifest.push({
                fixture,
                view: `Updates detail (${name.replace("updates-detail", "").replace(/^-/, "") || "blocked"})`,
                attempt: detail,
                width,
                path: relative(target),
                bytes: shot.length,
                sha256: sha256(shot),
                document_height: box.scrollHeight,
                horizontal_overflow: box.scrollWidth - box.clientWidth,
              });
            }
            // Story S16 ends with the switch, not with the button: capture the
            // page again once "Read this version" has been pressed, so the
            // reviewer sees the top bar move rather than trusting the ledger.
            if (name === "updates-detail-published") {
              const after = join(rootFor(width), fixture, `${name}-after-${width}.png`);
              const button = page.locator("button", { hasText: "Read this version" });
              if (await button.count()) {
                await button.first().click();
                await page.waitForFunction(
                  (id) => {
                    const bar = document.querySelector('[data-testid="console-top-bar"]');
                    return Boolean(bar && bar.textContent && bar.textContent.includes(id));
                  },
                  detail,
                  { timeout: 15000 },
                );
                const shot = await page.screenshot({ fullPage: true });
                writeFileSync(after, shot);
                written.push(after);
                manifest.push({
                  fixture,
                  view: "Updates detail (published, after Read this version)",
                  attempt: detail,
                  width,
                  path: relative(after),
                  bytes: shot.length,
                  sha256: sha256(shot),
                  document_height: await page.evaluate(
                    () => document.documentElement.scrollHeight,
                  ),
                  horizontal_overflow: await page.evaluate(
                    () => document.documentElement.scrollWidth
                      - document.documentElement.clientWidth,
                  ),
                });
              } else {
                failures.push(
                  `${fixture}/${name}@${width}: no "Read this version" control to press`,
                );
              }
            }
          } catch (error) {
            failures.push(`${fixture}/Updates-detail@${width}: ${error.message}`);
          }
        }
        // The producer page, at 1440 only, for the same reason.
        const producers = width === 1440 ? sourcesDetailTargetsFor(fixture) : [];
        for (const [name, producer] of producers) {
          const target = join(rootFor(width), fixture, `${name}-${width}.png`);
          try {
            const apiRequests = await openSourcesDetail(page, server.url, fixture, producer);
            if (apiRequests.length) {
              failures.push(
                `${fixture}/Sources-detail@${width}: the page read ${apiRequests.length} `
                + `URL(s) over the network, starting with ${apiRequests[0]}`,
              );
              continue;
            }
            const failed = await page.locator('[data-testid="design-read-failed"]').count();
            if (failed) {
              failures.push(
                `${fixture}/${name}@${width}: the producer page for ${producer} rendered a `
                + "read failure",
              );
              continue;
            }
            await page.evaluate(() => {
              document.documentElement.classList.add("okg-capture");
            });
            const shot = await page.screenshot({ fullPage: true });
            writeFileSync(target, shot);
            const box = await page.evaluate(() => ({
              scrollWidth: document.documentElement.scrollWidth,
              clientWidth: document.documentElement.clientWidth,
              scrollHeight: document.documentElement.scrollHeight,
            }));
            written.push(target);
            manifest.push({
              fixture,
              view: `Sources detail (${name === "sources-detail" ? "producer" : "enricher"})`,
              producer,
              width,
              path: relative(target),
              bytes: shot.length,
              sha256: sha256(shot),
              document_height: box.scrollHeight,
              horizontal_overflow: box.scrollWidth - box.clientWidth,
            });
            if (fixture === HOVER_FIXTURE && name === "sources-detail") {
              const hover = join(untrackedRoot, fixture, `sources-detail-hover-${width}.png`);
              const icon = page.locator(
                '[data-testid="sources-detail-header"] .okg-console__copy--icon',
              );
              if (!(await icon.count())) {
                failures.push(`${fixture}/${name}@${width}: no copy icon on the name to hover`);
                continue;
              }
              await icon.first().hover();
              // Let the opacity transition finish before the shot.
              await page.waitForTimeout(300);
              const opacity = await icon.first().evaluate(
                (element) => getComputedStyle(element).opacity,
              );
              if (opacity !== "1") {
                failures.push(
                  `${fixture}/${name}@${width}: the hovered copy icon is at opacity ${opacity}, not 1`,
                );
                continue;
              }
              const hoverShot = await page.screenshot({ fullPage: true });
              writeFileSync(hover, hoverShot);
              written.push(hover);
              manifest.push({
                fixture,
                view: "Sources detail (producer, hover on the name's copy icon)",
                producer,
                width,
                path: relative(hover),
                bytes: hoverShot.length,
                sha256: sha256(hoverShot),
                document_height: box.scrollHeight,
                horizontal_overflow: box.scrollWidth - box.clientWidth,
                hovered_icon_opacity: opacity,
              });
            }
          } catch (error) {
            failures.push(`${fixture}/${name}@${width}: ${error.message}`);
          }
        }
        // Explore's results and record pages, at 1440 only, for the same
        // reason again.
        const explores = width === 1440 ? exploreTargetsFor(fixture) : [];
        for (const [name, params] of explores) {
          const target = join(rootFor(width), fixture, `${name}-${width}.png`);
          try {
            const foreign = foreignRequestListener(page, server.url);
            const apiRequests = await openExplore(page, server.url, fixture, params);
            foreign.stop();
            if (apiRequests.length) {
              failures.push(
                `${fixture}/${name}@${width}: the page read ${apiRequests.length} `
                + `URL(s) over the network, starting with ${apiRequests[0]}`,
              );
              continue;
            }
            if (foreign.foreign.length) {
              failures.push(
                `${fixture}/${name}@${width}: the page requested ${foreign.foreign.length} `
                + `URL(s) off the dev server, starting with ${foreign.foreign[0]}`,
              );
              continue;
            }
            const failed = await page.locator('[data-testid="design-read-failed"]').count();
            if (failed) {
              failures.push(
                `${fixture}/${name}@${width}: the Explore page for ${JSON.stringify(params)} `
                + "rendered a read failure",
              );
              continue;
            }
            // A Content capture must show the artifact drawn, not a viewer
            // that is still loading or that failed and said so.
            const viewers = await viewerStates(page);
            const notReady = viewers.filter((pane) => pane.state !== "ready");
            if (notReady.length) {
              failures.push(
                `${fixture}/${name}@${width}: the ${notReady[0].viewer} viewer is ${notReady[0].state}`
                + (notReady[0].reason ? ` (${notReady[0].reason})` : ""),
              );
              continue;
            }
            await page.evaluate(() => {
              document.documentElement.classList.add("okg-capture");
            });
            const shot = await page.screenshot({ fullPage: true });
            writeFileSync(target, shot);
            const box = await page.evaluate(() => ({
              scrollWidth: document.documentElement.scrollWidth,
              clientWidth: document.documentElement.clientWidth,
              scrollHeight: document.documentElement.scrollHeight,
            }));
            written.push(target);
            manifest.push({
              fixture,
              view: `Explore ${name.replace("explore-", "")}`,
              explore: params,
              width,
              path: relative(target),
              bytes: shot.length,
              sha256: sha256(shot),
              document_height: box.scrollHeight,
              horizontal_overflow: box.scrollWidth - box.clientWidth,
              ...(viewers.length ? { viewers } : {}),
            });
          } catch (error) {
            failures.push(`${fixture}/${name}@${width}: ${error.message}`);
          }
        }
        await context.close();
      }
    }
  } finally {
    await browser.close();
    server.stop();
  }

  mkdirSync(join(PACT_UI_DIR, "evidence"), { recursive: true });
  writeFileSync(
    join(PACT_UI_DIR, "evidence", "capture.json"),
    `${JSON.stringify({
      captured_at_clock: "2026-09-18T09:00:00.000Z",
      expected: fixtures.length * VIEWS.length * WIDTHS.length
        + fixtures.filter((name) => detailAttemptFor(name)).length
        + fixtures.filter((name) => readableDetailAttemptFor(name)).length
        + fixtures.filter((name) => currentDetailAttemptFor(name)).length
        // One more: the published detail again, after "Read this version".
        + fixtures.filter((name) => readableDetailAttemptFor(name)).length
        // The producer pages.
        + fixtures.reduce((sum, name) => sum + sourcesDetailTargetsFor(name).length, 0)
        // The hover state on archiv3-small's producer page.
        + (fixtures.includes(HOVER_FIXTURE) ? 1 : 0)
        // Explore's results and record pages.
        + fixtures.reduce((sum, name) => sum + exploreTargetsFor(name).length, 0),
      written: written.length,
      failures,
      captures: manifest,
    }, null, 2)}\n`,
  );

  if (failures.length) {
    for (const failure of failures) console.error(`FAILED: ${failure}`);
    console.error(`${written.length} captured, ${failures.length} failed`);
    return 1;
  }
  console.log(
    `ok: ${written.length} captures written; 1440 under ${trackedRoot}, `
    + `390 and hover under ${untrackedRoot}`,
  );
  return 0;
}

process.exit(await main());
