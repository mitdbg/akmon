import { createReadStream, existsSync, realpathSync, statSync } from "node:fs";
import { basename, extname, join, normalize } from "node:path";
import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import {
  OPERATOR_UI_BASE_ENV,
  ROOT_MOUNT_BASE,
  resolveOperatorUiBase,
// Explicit .ts extension: Vite's native config loader refuses an extensionless
// relative import, and this config is never fed to tsc (tsconfig.operator-ui.json
// includes only src/okg/operator_ui/web/src).
} from "./src/okg/operator_ui/web/src/operator-ui/shared/api/mountBase.ts";

const apiTarget = process.env.OKG_OPERATOR_API_BASE || process.env.VITE_OKG_API_BASE;

// Where this bundle will be served. Unset builds the console at the origin
// root exactly as before; OKG_OPERATOR_UI_BASE=/console/ builds the SAME
// source for a path prefix, emitting prefixed asset URLs and (via Vite's
// injected import.meta.env.BASE_URL) prefixed /viz/api reads. A malformed
// value throws out of resolveOperatorUiBase rather than reverting to root.
const mountBase = resolveOperatorUiBase(process.env);
const isPrefixed = mountBase !== ROOT_MOUNT_BASE;
if (isPrefixed) {
  // Loud, because the mount silently changes every emitted URL in the bundle.
  console.log(`[operator-ui] ${OPERATOR_UI_BASE_ENV}=${mountBase} — building for a prefixed mount`);
}

// The console's own API surface, addressed under its mount. `mountBase` always
// ends in "/", so this is "/viz/api" at the root and "/console/viz/api" under
// a "/console/" mount.
const apiPrefix = `${mountBase}viz/api`;
const apiProxy = apiTarget
  ? {
      [apiPrefix]: {
        target: apiTarget,
        changeOrigin: true,
        // The backend serves /viz/api; the mount is a front-door concern, so
        // strip it before forwarding.
        ...(isPrefixed
          ? { rewrite: (path: string) => path.replace(mountBase.slice(0, -1), "") }
          : {}),
      },
    }
  : undefined;

const repoRoot = fileURLToPath(new URL(".", import.meta.url));
const nodeModulesRoot = existsSync("node_modules")
  ? realpathSync("node_modules")
  : fileURLToPath(new URL("./node_modules", import.meta.url));
const serverFs = {
  allow: [repoRoot, nodeModulesRoot],
};

// The design harness (PACT operator-ui-redesign-mocks) must not ship.
//
// Gating it on `import.meta.env.DEV` alone is not enough: Rolldown still
// followed the dynamic import and emitted an 831 kB DesignHarness chunk
// carrying all six fixtures, because the constant fold happens after the module
// graph is built. So the gate is a resolver: in a production build the harness
// module resolves to a stub that throws, no fixture is reachable from it, and
// the chunk is never created. `OKG_OPERATOR_UI_DESIGN_MOCKS=1` builds a design
// bundle on purpose; scripts/operator-ui-design/check-bundle-clean.mjs refuses to report such a
// build clean.
const DESIGN_MOCKS_ENV = "OKG_OPERATOR_UI_DESIGN_MOCKS";
const designBuild = process.env[DESIGN_MOCKS_ENV] === "1";
// The design build lands beside the production one, never over it: the
// bundle-clean check reads .tmp/operator-ui-dist and must never find a
// harness there, and the bundle budget reads the design build for the
// chunks only the harness's views load (explore_js).
const outDir = fileURLToPath(new URL(
  designBuild ? "./.tmp/operator-ui-dist-design" : "./.tmp/operator-ui-dist",
  import.meta.url,
));
const DESIGN_HARNESS_ID = "@operator-ui/design-harness/DesignHarness";
const DESIGN_STUB_ID = "\0okg-design-harness-stub";

function designHarnessGate() {
  let include = true;
  return {
    name: "okg-design-harness-gate",
    enforce: "pre" as const,
    configResolved(resolved: { command: string }) {
      include = resolved.command === "serve" || process.env[DESIGN_MOCKS_ENV] === "1";
      if (!include) {
        console.log("[operator-ui] design harness excluded from this build");
      }
    },
    resolveId(source: string) {
      if (include) return null;
      // Both spellings. Vite prepends its own alias plugin AHEAD of user `pre`
      // plugins, so by the time this hook runs the `@operator-ui/...` specifier
      // has usually already become an absolute path; matching only the alias
      // would silently never fire, which is exactly what it did first time.
      const isHarness = source === DESIGN_HARNESS_ID
        || /[\\/]operator-ui[\\/]design-harness[\\/]DesignHarness(\.tsx)?$/.test(source);
      return isHarness ? DESIGN_STUB_ID : null;
    },
    load(id: string) {
      if (id !== DESIGN_STUB_ID) return null;
      // Throws rather than rendering null: a production bundle should never
      // reach this, and if a future edit does reach it, the operator gets a
      // named failure instead of a blank page.
      return "export function DesignHarness() {\n"
        + "  throw new Error('The design harness is not built into this bundle.');\n"
        + "}\n";
    },
  };
}

// The design fixtures' artifacts (the files the record page's Content tab
// renders) live with the design tooling under scripts/operator-ui-design/,
// outside Vite's root. The fixtures name them by the URL below, and the
// URL stays so the fixture JSON stays byte-identical; the dev server maps
// it to the folder. Serve only: a production build has no such route and
// no fixture to name it.
const ARTIFACT_URL_PREFIX = "/design/fixtures/artifacts/";
const ARTIFACT_DIR = fileURLToPath(new URL("./scripts/operator-ui-design/fixtures/artifacts/", import.meta.url));
const ARTIFACT_TYPES: Record<string, string> = {
  ".pdf": "application/pdf",
  ".md": "text/markdown; charset=utf-8",
  ".rs": "text/plain; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".png": "image/png",
  ".json": "application/json",
};

function designArtifacts() {
  return {
    name: "okg-design-fixture-artifacts",
    apply: "serve" as const,
    configureServer(server: { middlewares: { use: (path: string, handler: (req: { url?: string }, res: { statusCode: number; setHeader: (k: string, v: string) => void; end: (body?: string) => void }, next: () => void) => void) => void } }) {
      server.middlewares.use(ARTIFACT_URL_PREFIX, (req, res, next) => {
        // One flat folder: the basename only, so no path can climb out of it.
        const name = basename(normalize((req.url ?? "").split("?")[0]));
        const file = join(ARTIFACT_DIR, name);
        if (!name || !existsSync(file) || !statSync(file).isFile()) {
          next();
          return;
        }
        res.statusCode = 200;
        res.setHeader("content-type", ARTIFACT_TYPES[extname(name)] ?? "application/octet-stream");
        res.setHeader("content-length", String(statSync(file).size));
        createReadStream(file).pipe(res as unknown as NodeJS.WritableStream);
      });
    },
  };
}

export default defineConfig({
  base: mountBase,
  root: "src/okg/operator_ui/web",
  plugins: [
    designHarnessGate(),
    designArtifacts(),
    react(),
    {
      name: "okg-operator-api-boundary",
      configureServer(server) {
        if (apiTarget) {
          return;
        }
        server.middlewares.use(apiPrefix, (req, res) => {
          const path = `${apiPrefix}${req.url ?? ""}`;
          res.statusCode = 200;
          res.setHeader("content-type", "application/json");
          res.end(
            JSON.stringify({
              ok: false,
              ready: false,
              status: 502,
              path,
              error: "operator UI dev server has no OKG API target",
              next_check:
                "start the OKG viz server and set OKG_OPERATOR_API_BASE, or open the operator UI from the viz server",
            }),
          );
        });
      },
    },
  ],
  server: {
    fs: serverFs,
    ...(apiProxy ? { proxy: apiProxy } : {}),
  },
  preview: apiProxy ? { proxy: apiProxy } : undefined,
  build: {
    outDir,
    emptyOutDir: true,
    sourcemap: true,
    // .vite/manifest.json: which chunk each module became and what it
    // imports, so audit/bundle-budget.mjs can sum a lazy closure exactly.
    manifest: true,
  },
  resolve: {
    alias: {
      "@operator-ui": fileURLToPath(
        new URL("./src/okg/operator_ui/web/src/operator-ui", import.meta.url),
      ),
      // The six mock deployments, imported only by design-harness/fixtures.ts,
      // which the gate above keeps out of a production build.
      "@operator-ui-design-fixtures": fileURLToPath(
        new URL("./scripts/operator-ui-design/fixtures", import.meta.url),
      ),
    },
  },
});
