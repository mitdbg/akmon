import { fileURLToPath } from "node:url";
import { dirname, join, resolve, sep } from "node:path";
import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const tsconfig = join(repoRoot, "tsconfig.operator-ui.json");
const tsc = join(
  repoRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc",
);
const operatorUiRoot = resolve(repoRoot, "src/okg/operator_ui/web/src") + sep;

const result = spawnSync(
  tsc,
  ["--project", tsconfig, "--noEmit", "--listFiles", "--pretty", "false"],
  { cwd: repoRoot, encoding: "utf8", shell: process.platform === "win32" },
);

if (result.error) {
  console.error(`operator-ui:typecheck could not start tsc: ${result.error.message}`);
  process.exit(1);
}

if (result.stderr) process.stderr.write(result.stderr);

const outputLines = (result.stdout ?? "")
  .split(/\r?\n/)
  .map((line) => line.trim())
  .filter((line) => line.length > 0);
const listedFiles = new Set(
  outputLines
    .map((line) => resolve(repoRoot, line))
    .filter((file) => existsSync(file)),
);
const visited = [...listedFiles]
  .filter(
    (file) =>
      file.startsWith(operatorUiRoot) &&
      /\.(?:ts|tsx)$/.test(file),
  );

const diagnostics = outputLines.filter(
  (line) => !listedFiles.has(resolve(repoRoot, line)),
);
if (diagnostics.length > 0) {
  process.stdout.write(`${diagnostics.join("\n")}\n`);
}

if (visited.length === 0) {
  console.error(
    "operator-ui:typecheck guard: tsc visited zero operator-UI source files",
  );
  process.exit(1);
}

console.log(`operator-ui:typecheck guard: visited ${visited.length} source files`);
process.exit(typeof result.status === "number" ? result.status : 1);
