// The real files the record page's Content tab renders, built here byte for
// byte so `generate-fixtures.mjs --check` can prove nothing drifted.
//
// Six artifacts, one per media type the viewer routes: a two-page PDF, a
// Markdown document, a Rust source file, an HTML page, a PNG, and a 300 KB
// JSON export that is too large to inline in a raw read. Every byte comes
// from this file and Node's standard library -- no PDF library, no image
// library, no compressor -- so the same bytes come out on every machine.
import { createHash } from "node:crypto";

export const ARTIFACT_DIR_NAME = "artifacts";

/** Where the dev server serves the artifacts from: Vite's root is
 *  src/okg/operator_ui/web, so design/fixtures/artifacts/<name> is a URL. */
export function artifactUrl(name) {
  return `/design/fixtures/${ARTIFACT_DIR_NAME}/${name}`;
}

export function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

// --- the PDF ----------------------------------------------------------------

function pdfEscape(text) {
  return text.replace(/\\/g, "\\\\").replace(/\(/g, "\\(").replace(/\)/g, "\\)");
}

/** A page of Helvetica lines: `[size, text]`; an empty text is a blank line. */
function pdfPageStream(lines) {
  const ops = ["BT", "/F1 11 Tf", "14 TL", "72 740 Td"];
  for (const [size, text] of lines) {
    if (size !== 11) ops.push(`/F1 ${size} Tf`);
    ops.push(text ? `(${pdfEscape(text)}) Tj T*` : "T*");
    if (size !== 11) ops.push("/F1 11 Tf");
  }
  ops.push("ET");
  return ops.join("\n");
}

/** A minimal PDF 1.4 file: catalog, pages, N pages sharing one Helvetica
 *  font, one content stream each, and a correct xref table. */
function buildPdf(pages) {
  const objects = [];
  const add = (body) => { objects.push(body); return objects.length; };
  const catalog = add(null);
  const pagesObj = add(null);
  const font = add("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>");
  const pageIds = [];
  for (const lines of pages) {
    const stream = pdfPageStream(lines);
    const content = add(`<< /Length ${Buffer.byteLength(stream, "latin1")} >>\nstream\n${stream}\nendstream`);
    const page = add(
      `<< /Type /Page /Parent ${pagesObj} 0 R /MediaBox [0 0 612 792] `
      + `/Resources << /Font << /F1 ${font} 0 R >> >> /Contents ${content} 0 R >>`,
    );
    pageIds.push(page);
  }
  objects[catalog - 1] = `<< /Type /Catalog /Pages ${pagesObj} 0 R >>`;
  objects[pagesObj - 1] = `<< /Type /Pages /Kids [${pageIds.map((id) => `${id} 0 R`).join(" ")}] /Count ${pageIds.length} >>`;

  let out = "%PDF-1.4\n%âãÏÓ\n";
  const offsets = [];
  objects.forEach((body, index) => {
    offsets.push(Buffer.byteLength(out, "latin1"));
    out += `${index + 1} 0 obj\n${body}\nendobj\n`;
  });
  const xref = Buffer.byteLength(out, "latin1");
  out += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  for (const offset of offsets) out += `${String(offset).padStart(10, "0")} 00000 n \n`;
  out += `trailer\n<< /Size ${objects.length + 1} /Root ${catalog} 0 R >>\nstartxref\n${xref}\n%%EOF\n`;
  return Buffer.from(out, "latin1");
}

/** The paper's text, per chunk, as the chunker would extract it. Exported so
 *  the fixture's document_chunk records carry the same words the PDF does. */
export const PAPER_CHUNKS = [
  {
    page: 1,
    heading: "Abstract",
    text: "Operational knowledge graphs record what an engineering organisation did, "
      + "decided and observed, so that the next question can be answered from evidence "
      + "rather than from memory. We describe a substrate that ingests sources into a "
      + "versioned graph, publishes each version only after its invariants hold, and "
      + "serves reads pinned to one published version.",
  },
  {
    page: 1,
    heading: "1 Introduction",
    text: "A deployment is a set of producers: sources that read external systems, "
      + "enrichers that add relationships, and views that derive tables. An update "
      + "attempt runs every producer, checks the result and publishes a version or "
      + "blocks with a stated cause. Readers always see one published version at a "
      + "time and can name which one they read.",
  },
  {
    page: 2,
    heading: "2 Retained payloads and artifacts",
    text: "A source may retain the canonical record it ingested as JSON, chunked at one "
      + "mebibyte with a byte count and a SHA-256. The original artifact, when the source "
      + "keeps a corpus root, stays on disk under the path the record names. Both are "
      + "served under the policy that masks the record's attributes, so an original file "
      + "never bypasses the masking applied to its fields.",
  },
];

function wrap(text, width) {
  const lines = [];
  let line = "";
  for (const word of text.split(" ")) {
    if (`${line} ${word}`.trim().length > width) { lines.push(line.trim()); line = word; } else { line = `${line} ${word}`; }
  }
  if (line.trim()) lines.push(line.trim());
  return lines;
}

function paperPdf() {
  const page1 = [
    [18, "Operational knowledge graphs"],
    [11, ""],
    [11, "M. Curie, Radium Institute, 1921"],
    [11, ""],
    [13, PAPER_CHUNKS[0].heading],
    ...wrap(PAPER_CHUNKS[0].text, 84).map((line) => [11, line]),
    [11, ""],
    [13, PAPER_CHUNKS[1].heading],
    ...wrap(PAPER_CHUNKS[1].text, 84).map((line) => [11, line]),
    [11, ""],
    [9, "Page 1 of 2"],
  ];
  const page2 = [
    [13, PAPER_CHUNKS[2].heading],
    ...wrap(PAPER_CHUNKS[2].text, 84).map((line) => [11, line]),
    [11, ""],
    [11, "Table 1. The three raw layers of a record."],
    [11, ""],
    [11, "layer        holds                                 refusable by policy"],
    [11, "row          the stored node row                   no, masked in place"],
    [11, "edges        the stored edge rows                  no"],
    [11, "payload      the canonical record as JSON          yes"],
    [11, ""],
    [9, "Page 2 of 2"],
  ];
  return buildPdf([page1, page2]);
}

// --- the Markdown document --------------------------------------------------

export const HANDBOOK_MARKDOWN = `# Operator handbook

How to run the deployment day to day. Read **Home** first; it says whether the
last update published and what needs attention.

## Daily check

1. Open Home and read the status line.
2. If an attempt is blocked, open it in Updates and read the recorded error.
3. Open Sources for the producer it stopped on.

## Producers

| Producer | Kind | Cadence |
|---|---|---|
| \`source-07-git\` | source | every 6 h |
| \`semantic-linker\` | enricher | after every update |
| \`daily-brief\` | view | daily |

## Publishing by hand

\`\`\`bash
okg update run --deployment workspace-mixed
okg generation describe <id> --invariants
\`\`\`

- A version publishes only when every invariant holds.
- Reads keep serving the previous version until then.

See the [status page](https://docs.example.invalid/status) for the live view.
`;

// --- the Rust source ----------------------------------------------------------

export const MAIN_RS = `//! Entry point for the ingest worker: read one source, emit records.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Instant;

/// One record as the source emitted it, before the graph assigns ids.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub source_record_id: String,
    pub subtype: String,
    pub attrs: BTreeMap<String, String>,
}

/// Why a run stopped before publishing.
#[derive(Debug)]
pub enum StopReason {
    SourceIncomplete { expected: usize, got: usize },
    PolicyRefused(String),
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopReason::SourceIncomplete { expected, got } => {
                write!(f, "ingest_source_incomplete: {got} of {expected} rows")
            }
            StopReason::PolicyRefused(policy) => write!(f, "policy {policy} refused the read"),
        }
    }
}

/// A run over one source: counts what it read and what it emitted.
pub struct Run {
    started: Instant,
    read: usize,
    emitted: Vec<Record>,
}

impl Run {
    pub fn new() -> Self {
        Run { started: Instant::now(), read: 0, emitted: Vec::new() }
    }

    /// Accept one raw row. Rows without a stable id are counted and dropped,
    /// never given a made-up id: a guessed id would merge two records.
    pub fn accept(&mut self, row: BTreeMap<String, String>) {
        self.read += 1;
        let Some(id) = row.get("id").cloned() else { return };
        let subtype = row.get("kind").cloned().unwrap_or_else(|| "document".to_string());
        self.emitted.push(Record { source_record_id: id, subtype, attrs: row });
    }

    /// Finish the run. An incomplete read is a stop, not a partial publish.
    pub fn finish(self, expected: usize) -> Result<Vec<Record>, StopReason> {
        if self.read < expected {
            return Err(StopReason::SourceIncomplete { expected, got: self.read });
        }
        eprintln!("read {} rows in {:?}", self.read, self.started.elapsed());
        Ok(self.emitted)
    }
}

fn main() {
    let mut run = Run::new();
    for index in 0..3 {
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), format!("doc-{index}"));
        row.insert("kind".to_string(), "document".to_string());
        run.accept(row);
    }
    match run.finish(3) {
        Ok(records) => println!("emitted {} records", records.len()),
        Err(reason) => eprintln!("stopped: {reason}"),
    }
}
`;

// --- the HTML page ------------------------------------------------------------

/** A docsite page. It carries an inline script and a remote script on
 *  purpose: the Content tab's frame is sandboxed with scripts off, so neither
 *  may run or load, and scripts/operator-ui-design/audit-rules.mjs checks that they did not. */
export const STATUS_HTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Deployment status</title>
<script src="https://cdn.invalid/analytics.js"></script>
<style>
  body { font: 15px/1.5 Georgia, serif; margin: 32px; color: #1c1c1c; }
  h1 { font-size: 24px; }
  table { border-collapse: collapse; }
  td, th { border: 1px solid #bbb; padding: 4px 10px; text-align: left; }
  #script-ran { color: #b00; font-weight: bold; }
</style>
</head>
<body>
<h1>Deployment status</h1>
<p>This page is rebuilt after every published version.</p>
<table>
  <tr><th>Producer</th><th>State</th></tr>
  <tr><td>source-07-git</td><td>updated</td></tr>
  <tr><td>semantic-linker</td><td>updated</td></tr>
  <tr><td>contributor-graph</td><td>failed</td></tr>
</table>
<p id="script-ran" hidden>A script ran inside this frame.</p>
<script>
  document.getElementById("script-ran").hidden = false;
  document.title = "SCRIPT RAN";
</script>
</body>
</html>
`;

// --- the PNG --------------------------------------------------------------------

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[n] = c >>> 0;
  }
  return table;
})();

function crc32(bytes) {
  let crc = 0xffffffff;
  for (const byte of bytes) crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

function adler32(bytes) {
  let a = 1;
  let b = 0;
  for (const byte of bytes) { a = (a + byte) % 65521; b = (b + a) % 65521; }
  return ((b << 16) | a) >>> 0;
}

/** A zlib stream of stored (uncompressed) deflate blocks: no compressor, so
 *  the bytes do not depend on which zlib Node was built with. */
function zlibStored(data) {
  const blocks = [];
  for (let at = 0; at < data.length; at += 65535) {
    const slice = data.subarray(at, Math.min(at + 65535, data.length));
    const last = at + 65535 >= data.length ? 1 : 0;
    const head = Buffer.alloc(5);
    head[0] = last;
    head.writeUInt16LE(slice.length, 1);
    head.writeUInt16LE(slice.length ^ 0xffff, 3);
    blocks.push(head, slice);
  }
  const trailer = Buffer.alloc(4);
  trailer.writeUInt32BE(adler32(data));
  return Buffer.concat([Buffer.from([0x78, 0x01]), ...blocks, trailer]);
}

function pngChunk(type, data) {
  const length = Buffer.alloc(4);
  length.writeUInt32BE(data.length);
  const typed = Buffer.concat([Buffer.from(type, "latin1"), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(typed));
  return Buffer.concat([length, typed, crc]);
}

/** A 160 by 100 indexed-colour PNG: three boxes joined by lines, the shape
 *  of a producer graph, in four palette colours. */
function diagramPng() {
  const width = 160;
  const height = 100;
  const palette = [
    [0xf7, 0xf6, 0xf2], // canvas
    [0x0e, 0x2b, 0x45], // ink
    [0x4a, 0xa3, 0xd8], // accent
    [0xc8, 0x37, 0x37], // failed
  ];
  const pixels = new Uint8Array(width * height);
  const box = (x, y, w, h, colour) => {
    for (let yy = y; yy < y + h; yy += 1) {
      for (let xx = x; xx < x + w; xx += 1) {
        const edge = xx === x || xx === x + w - 1 || yy === y || yy === y + h - 1;
        pixels[yy * width + xx] = edge ? 1 : colour;
      }
    }
  };
  const hline = (x1, x2, y) => { for (let xx = x1; xx <= x2; xx += 1) pixels[y * width + xx] = 1; };
  box(10, 40, 36, 20, 2);
  box(62, 40, 36, 20, 2);
  box(114, 40, 36, 20, 3);
  hline(46, 62, 50);
  hline(98, 114, 50);
  box(62, 10, 36, 16, 0);
  for (let yy = 26; yy < 40; yy += 1) pixels[yy * width + 80] = 1;

  const raw = Buffer.alloc((width + 1) * height);
  for (let y = 0; y < height; y += 1) {
    raw[y * (width + 1)] = 0; // filter: none
    raw.set(pixels.subarray(y * width, (y + 1) * width), y * (width + 1) + 1);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 3; // indexed colour
  ihdr[10] = 0;
  ihdr[11] = 0;
  ihdr[12] = 0;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk("IHDR", ihdr),
    pngChunk("PLTE", Buffer.from(palette.flat())),
    pngChunk("IDAT", zlibStored(raw)),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

// --- the large JSON export --------------------------------------------------------

/** A canonical record set a session source retains: about 300 KB, past the
 *  256 KB the raw read inlines, so the Raw tab must show the head and offer
 *  "Open raw". Seeded arithmetic, no RNG library. */
function sessionExportJson() {
  const turns = [];
  let seed = 0x5e55;
  const next = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed; };
  const tools = ["okg_search", "okg_record_detail", "okg_health", "okg_sources", "okg_generation_describe"];
  for (let index = 0; index < 1400; index += 1) {
    const tool = tools[next() % tools.length];
    turns.push({
      turn: index,
      at: new Date(Date.parse("2026-09-17T22:04:00Z") + index * 2600).toISOString(),
      role: index % 2 === 0 ? "assistant" : "tool",
      tool: index % 2 === 0 ? null : tool,
      text: index % 2 === 0
        ? `Checking which sources failed last night; step ${index / 2} of the review.`
        : `${tool} returned ${next() % 400} rows in ${next() % 900} ms; nothing was substituted.`,
      tokens_in: next() % 4000,
      tokens_out: next() % 1200,
    });
  }
  return `${JSON.stringify({
    schema: "okg.session.canonical.v1",
    session_id: "sess-1",
    question_label: "which sources failed last night",
    started_at: "2026-09-17T22:04:00Z",
    turns,
  }, null, 2)}\n`;
}

/** Every artifact by file name: bytes, media type, byte count, SHA-256 and
 *  the URL the harness serves it at. Built once per process. */
export function buildArtifacts() {
  const files = [
    { name: "okg-2026.pdf", mediaType: "application/pdf", bytes: paperPdf() },
    { name: "handbook.md", mediaType: "text/markdown", bytes: Buffer.from(HANDBOOK_MARKDOWN, "utf8") },
    { name: "main.rs", mediaType: "text/x-rust", bytes: Buffer.from(MAIN_RS, "utf8") },
    { name: "status.html", mediaType: "text/html", bytes: Buffer.from(STATUS_HTML, "utf8") },
    { name: "diagram.png", mediaType: "image/png", bytes: diagramPng() },
    { name: "session-sess-1.json", mediaType: "application/json", bytes: Buffer.from(sessionExportJson(), "utf8") },
  ];
  return Object.fromEntries(files.map((file) => [file.name, {
    ...file,
    byteCount: file.bytes.length,
    sha256: sha256(file.bytes),
    url: artifactUrl(file.name),
  }]));
}
