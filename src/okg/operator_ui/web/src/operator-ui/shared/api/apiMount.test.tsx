import { createServer, request as httpRequest, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { fileURLToPath, URL as NodeURL } from "node:url";
import { runnerImport } from "vite";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { ApiRequestError, buildApiUrl, createApiClient } from "@operator-ui/shared/api/apiClient";
import { ROOT_MOUNT_BASE } from "@operator-ui/shared/api/mountBase";

// These arms assert on requests OBSERVED AT A SERVER, never on the string
// buildApiUrl returned. That distinction is the whole point: the defect is not
// that the console computes a wrong-looking path, it is that a root-absolute
// /viz/api read LANDS on whatever owns the origin root. Only a server that
// receives the request can tell you where it landed.
//
// Topology, mirroring the intended one hostname per deployment:
//
//   front proxy (the hostname)
//     ├── /console/*  ──► console backend   (inside the mount)
//     └── /*          ──► chat site catcher (owns the origin root)

const MOUNT = "/console/";

const API_CLIENT = fileURLToPath(new NodeURL("./apiClient.ts", import.meta.url));
const OPERATOR_UI_SRC = fileURLToPath(new NodeURL("../../", import.meta.url)).replace(/\/$/, "");

type ApiClientModule = typeof import("@operator-ui/shared/api/apiClient");

/**
 * Load apiClient the way a BUILD loads it: re-transformed by Vite under a
 * declared `base`, so `import.meta.env.BASE_URL` is injected exactly as it is
 * in the shipped bundle.
 *
 * Every other arm in this file hands `createApiClient` an explicit `mountBase`,
 * which means none of them ever reaches the default at apiClient.ts:123
 * (`mountBase === undefined ? builtMountBase() : mountBase`) — and that default
 * is the entire production path, because the application never passes a mount.
 * A statically-imported client cannot cover it either: vitest transforms this
 * tree with Vite's own default base, so the injected BASE_URL is "/" and a
 * hardcoded `"/"` fallback — the exact silent failure this change removes —
 * would be indistinguishable from reading the build input. Re-transforming the
 * module under a non-root base is what tells the two apart.
 */
async function clientBuiltFor(mount: string): Promise<ApiClientModule> {
  const { module } = await runnerImport<ApiClientModule>(API_CLIENT, {
    base: mount,
    configFile: false,
    logLevel: "silent",
    resolve: { alias: { "@operator-ui": OPERATOR_UI_SRC } },
  });
  return module;
}

interface Recorder {
  server: Server;
  origin: string;
  observed: string[];
}

async function listen(server: Server): Promise<string> {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address() as AddressInfo;
  return `http://127.0.0.1:${port}`;
}

/** A backend that records every path it is asked for. */
async function startRecorder(label: string, status = 200): Promise<Recorder> {
  const observed: string[] = [];
  const server = createServer((req, res) => {
    observed.push(req.url ?? "");
    res.statusCode = status;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ ok: status === 200, served_by: label, path: req.url }));
  });
  const origin = await listen(server);
  return { server, origin, observed };
}

async function stop(server: Server) {
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())));
}

let consoleBackend: Recorder;
let chatSite: Recorder;
let frontProxy: Server;
let frontOrigin = "";
let rootArmBackend: Recorder;

describe("operator console API reads under a declared mount", () => {
  beforeAll(async () => {
    consoleBackend = await startRecorder("console-backend");
    // The chat site owns / in this topology and answers 404 for /viz/api,
    // exactly as the read-handshake spike's nginx root location did.
    chatSite = await startRecorder("chat-site", 404);
    rootArmBackend = await startRecorder("root-arm-console-backend");

    frontProxy = createServer((clientReq, clientRes) => {
      const path = clientReq.url ?? "/";
      const upstream = path.startsWith(MOUNT) ? consoleBackend.origin : chatSite.origin;
      const target = new URL(path, upstream);
      const proxied = httpRequest(
        {
          hostname: target.hostname,
          port: target.port,
          path: `${target.pathname}${target.search}`,
          method: clientReq.method,
          headers: clientReq.headers,
        },
        (upstreamRes) => {
          clientRes.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
          upstreamRes.pipe(clientRes);
        },
      );
      proxied.on("error", () => {
        clientRes.statusCode = 502;
        clientRes.end();
      });
      clientReq.pipe(proxied);
    });
    frontOrigin = await listen(frontProxy);
  });

  afterAll(async () => {
    await Promise.all([
      stop(consoleBackend.server),
      stop(chatSite.server),
      stop(rootArmBackend.server),
      stop(frontProxy),
    ]);
  });

  beforeEach(() => {
    consoleBackend.observed.length = 0;
    chatSite.observed.length = 0;
    rootArmBackend.observed.length = 0;
  });

  it("prefixed client requests arrive under the console mount", async () => {
    const api = createApiClient({ baseUrl: frontOrigin, mountBase: MOUNT });

    await expect(api.get("/viz/api/console/health", { focus: "cms" })).resolves.toMatchObject({
      served_by: "console-backend",
    });

    expect(consoleBackend.observed).toEqual(["/console/viz/api/console/health?focus=cms"]);
  });

  it("nothing outside the prefix receives a viz api request", async () => {
    const api = createApiClient({ baseUrl: frontOrigin, mountBase: MOUNT });

    await api.get("/viz/api/console/health");
    await api.get("/viz/api/deployments");

    // Vacuity guard: a catcher that saw nothing because the console issued
    // nothing proves nothing, so the same run asserts a non-zero count INSIDE.
    expect(consoleBackend.observed.length).toBe(2);
    expect(chatSite.observed.filter((path) => path.includes("/viz/api"))).toEqual([]);
    expect(chatSite.observed).toEqual([]);
  });

  it("a client given no mount reads through the mount its build declared", async () => {
    // No mountBase argument: this is the call the console itself makes, so the
    // mount can only come from the build input.
    const { createApiClient: createBuiltClient } = await clientBuiltFor(MOUNT);
    const api = createBuiltClient({ baseUrl: frontOrigin });

    await expect(api.get("/viz/api/console/health")).resolves.toMatchObject({
      served_by: "console-backend",
    });

    expect(consoleBackend.observed).toEqual(["/console/viz/api/console/health"]);
    expect(chatSite.observed).toEqual([]);
  }, 60_000);

  it("a root build given no mount keeps todays path", async () => {
    // The same source, the same missing argument, a different declared mount:
    // together with the arm above this shows the default TRACKS the build input
    // rather than returning a constant.
    const { createApiClient: createBuiltClient } = await clientBuiltFor(ROOT_MOUNT_BASE);
    const api = createBuiltClient({ baseUrl: rootArmBackend.origin });

    await api.get("/viz/api/console/health", { focus: "cms ops" });

    expect(rootArmBackend.observed).toEqual(["/viz/api/console/health?focus=cms+ops"]);
  }, 60_000);

  it("root client requests keep todays path", async () => {
    const api = createApiClient({ baseUrl: rootArmBackend.origin, mountBase: "/" });

    await api.get("/viz/api/console/health", { focus: "cms ops", limit: 3 });

    // Byte for byte the path the backend serves today.
    expect(rootArmBackend.observed).toEqual([
      "/viz/api/console/health?focus=cms+ops&limit=3",
    ]);
  });

  it("a non viz api route still raises", async () => {
    const api = createApiClient({ baseUrl: frontOrigin, mountBase: MOUNT });

    await expect(api.get("/api/health" as "/viz/api/health")).rejects.toBeInstanceOf(
      ApiRequestError,
    );
    // The guard survives the mount: it is applied to the route BEFORE the
    // mount is joined on, so it cannot be sidestepped by a prefixed console.
    expect(() => buildApiUrl("/console/viz/api/health" as "/viz/api/health", {}, undefined, MOUNT))
      .toThrow(ApiRequestError);

    // And the refused route reached no server.
    expect(consoleBackend.observed).toEqual([]);
    expect(chatSite.observed).toEqual([]);
  });

  it("negative control: a root-addressed read from a prefixed console lands on the chat site", async () => {
    // This is the pre-fix behaviour — buildApiUrl's old root-absolute path —
    // reproduced against the same topology. It proves the assertions above can
    // fail: the read leaves the console's mount and is answered by whatever
    // owns the origin root.
    const preFix = createApiClient({ baseUrl: frontOrigin, mountBase: "/" });

    await expect(preFix.get("/viz/api/console/health")).rejects.toBeInstanceOf(ApiRequestError);

    expect(chatSite.observed).toEqual(["/viz/api/console/health"]);
    expect(consoleBackend.observed).toEqual([]);
  });
});
