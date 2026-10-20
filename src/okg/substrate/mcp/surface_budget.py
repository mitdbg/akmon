"""Measure what one MCP session pays at startup, hermetically.

Per `mcp-tool-surface-profiles` (issue #1457). Every session pays for the
`tools/list` schemas and the `initialize` instructions before it asks a single
question, and nothing measured that cost, so nothing could stop it growing.

What is measured is the JSON the SDK actually puts on the wire, not a proxy:
each tool is serialized with `Tool.model_dump_json(by_alias=True,
exclude_none=True)` — name, title, description, inputSchema, outputSchema,
annotations — which is what a client receives. Instruction bytes are the
`instructions` string from the initialize result.

`OKG_MCP_SURFACE_PROBE=1` is what makes this hermetic: `build_server` then
constructs both connection pools UNOPENED, so the real tool surface registers
without a database. Nothing here connects to Postgres; the DSN is an
unreachable loopback port by construction.

Tokens are counted with tiktoken's `cl100k_base` encoding over exactly the
strings the byte figures measure: each tool's wire JSON and the instructions
text. cl100k is a PROXY for Claude's own tokenizer, which is not available
offline; the two disagree by some percent, so a token count here is a
comparison between surfaces and commits, not a billing number. The budget
tests gate on these tokens (Jason, 2026-10-07, `tokenbudget A`); bytes stay
in the measurement as reported context only.

The count is hermetic. The cl100k ranks are read from the copy litellm
already ships, verified against tiktoken's published sha256, and never
downloaded: `tiktoken.get_encoding` is not called because it may fetch the
file. A missing or altered file raises `SurfaceTokenizerUnavailable`; there
is no fallback to a download or to a bytes-per-token estimate.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import tiktoken

#: Unreachable by construction (port 1). Under `OKG_MCP_SURFACE_PROBE` the
#: pools are built but never opened, so this is never dialed; it is spelled
#: unreachable anyway so a probe that LOSES its hermetic flag fails fast
#: rather than quietly measuring against somebody's real database.
PROBE_DSN = "postgresql://okg:okg@127.0.0.1:1/okg_surface_probe"


#: Where litellm ships tiktoken's cl100k_base ranks, and the sha256 tiktoken
#: itself pins for that file (`tiktoken_ext.openai_public.cl100k_base`). The
#: file is located through the installed distribution's metadata rather than
#: `importlib.resources`, because importing `litellm` can fetch its model
#: price map over the network.
CL100K_RANKS_DISTRIBUTION = "litellm"
CL100K_RANKS_PACKAGE = "litellm.litellm_core_utils.tokenizers"
CL100K_RANKS_FILE = "9b5ad71b2ce5302211f9c61530b329a4922fc6a4"
CL100K_RANKS_SHA256 = (
    "223921b76ee99bde995b7ff738513eef100fb51d18c93597a113bcffe865b2a7"
)

_ENCODING: "tiktoken.Encoding | None" = None
_ENCODING_LOCK = threading.Lock()


class SurfaceTokenizerUnavailable(RuntimeError):
    """The hermetic cl100k ranks could not be loaded.

    Raised instead of downloading the file or estimating from bytes: a
    budget measured with a different counter than its pins is not a
    measurement of the same thing.
    """


def _load_cl100k_ranks() -> dict[bytes, int]:
    relative = CL100K_RANKS_PACKAGE.replace(".", "/") + "/" + CL100K_RANKS_FILE
    try:
        ranks_file = metadata.distribution(CL100K_RANKS_DISTRIBUTION).locate_file(
            relative
        )
        contents = Path(str(ranks_file)).read_bytes()
    except (metadata.PackageNotFoundError, OSError) as exc:
        raise SurfaceTokenizerUnavailable(
            f"the cl100k_base ranks file {CL100K_RANKS_FILE} is missing from "
            f"package {CL100K_RANKS_PACKAGE}; the surface token count reads "
            "only that local copy and never downloads it"
        ) from exc
    digest = hashlib.sha256(contents).hexdigest()
    if digest != CL100K_RANKS_SHA256:
        raise SurfaceTokenizerUnavailable(
            f"the cl100k_base ranks file {CL100K_RANKS_FILE} in "
            f"{CL100K_RANKS_PACKAGE} has sha256 {digest}, not the "
            f"{CL100K_RANKS_SHA256} tiktoken pins; refusing to count tokens "
            "with an altered vocabulary"
        )
    ranks: dict[bytes, int] = {}
    for line in contents.splitlines():
        if not line:
            continue
        token, rank = line.split()
        ranks[base64.b64decode(token)] = int(rank)
    return ranks


def _build_cl100k() -> "tiktoken.Encoding":
    try:
        import tiktoken
        from tiktoken_ext import openai_public
    except ImportError as exc:
        raise SurfaceTokenizerUnavailable(
            "tiktoken is not importable; the surface token count needs it"
        ) from exc
    ranks = _load_cl100k_ranks()

    # cl100k's split pattern and special tokens live only inside
    # `openai_public.cl100k_base()`, which would load the ranks from a URL.
    # Its loader is swapped for one that returns the verified local ranks,
    # and refuses if the definition ever asks for a different file.
    def _local_ranks(_url: str, expected_hash: str | None = None) -> dict[bytes, int]:
        if expected_hash != CL100K_RANKS_SHA256:
            raise SurfaceTokenizerUnavailable(
                f"tiktoken's cl100k_base now pins sha256 {expected_hash}, not "
                f"{CL100K_RANKS_SHA256}; the local ranks no longer match it"
            )
        return ranks

    original = openai_public.load_tiktoken_bpe
    openai_public.load_tiktoken_bpe = _local_ranks
    try:
        definition = openai_public.cl100k_base()
    finally:
        openai_public.load_tiktoken_bpe = original
    return tiktoken.Encoding(
        name=definition["name"],
        pat_str=definition["pat_str"],
        mergeable_ranks=definition["mergeable_ranks"],
        special_tokens=definition["special_tokens"],
    )


def cl100k_encoding() -> "tiktoken.Encoding":
    """The process-wide cl100k_base encoding, built once from local ranks."""
    global _ENCODING
    with _ENCODING_LOCK:
        if _ENCODING is None:
            _ENCODING = _build_cl100k()
        return _ENCODING


def count_tokens(text: str) -> int:
    """cl100k_base tokens in `text`, a proxy for Claude's tokenizer.

    Special-token spellings in `text` are counted as ordinary text, because
    that is what a client receives them as.
    """
    return len(cl100k_encoding().encode(text, disallowed_special=()))


@dataclass(frozen=True)
class ToolBytes:
    """One tool's contribution to the `tools/list` payload.

    `wire_tokens` counts the same wire JSON `wire_bytes` measures, with
    cl100k_base (a proxy for Claude's tokenizer).
    """

    name: str
    wire_bytes: int
    description_bytes: int
    input_schema_bytes: int
    wire_tokens: int


@dataclass(frozen=True)
class SurfaceMeasurement:
    """What one launch configuration serves at startup."""

    deployment: str
    allow_writes: bool
    tool_profile: str | None
    instructions_bytes: int
    tools: tuple[ToolBytes, ...] = field(default_factory=tuple)
    #: The generated "not available here" part of `instructions_bytes` (#3151),
    #: budgeted separately from okg's own text (decision 12).
    generated_instructions_bytes: int = 0
    #: cl100k_base tokens of the whole instructions string, and of its
    #: authored and generated parts (`split_generated_instructions`).
    instructions_tokens: int = 0
    authored_instructions_tokens: int = 0
    generated_instructions_tokens: int = 0

    @property
    def tool_names(self) -> tuple[str, ...]:
        return tuple(sorted(t.name for t in self.tools))

    @property
    def tools_bytes(self) -> int:
        return sum(t.wire_bytes for t in self.tools)

    @property
    def total_bytes(self) -> int:
        return self.tools_bytes + self.instructions_bytes

    @property
    def tools_tokens(self) -> int:
        return sum(t.wire_tokens for t in self.tools)

    @property
    def total_tokens(self) -> int:
        """The gated figure: cl100k tokens of every tool plus instructions."""
        return self.tools_tokens + self.instructions_tokens

    def breakdown(self) -> str:
        """Per-tool table, largest first.

        A budget failure must name the offender. A bare "4,861 > 4,856"
        tells a contributor that something grew but not what, so every
        assertion message in the budget tests carries this. `tok` is
        cl100k_base tokens, the gated figure; the byte columns are context.
        """
        lines = [
            f"profile={self.tool_profile or 'full'} "
            f"allow_writes={self.allow_writes} "
            f"deployment={self.deployment} n_tools={len(self.tools)}",
            f"{'tool':<26}{'tok':>7}{'wire_B':>9}{'desc_B':>9}{'schema_B':>10}",
        ]
        for tool in sorted(self.tools, key=lambda t: -t.wire_tokens):
            lines.append(
                f"{tool.name:<26}{tool.wire_tokens:>7}{tool.wire_bytes:>9}"
                f"{tool.description_bytes:>9}{tool.input_schema_bytes:>10}"
            )
        lines.append(
            f"{'TOOLS TOTAL':<26}{self.tools_tokens:>7}{self.tools_bytes:>9}"
        )
        lines.append(
            f"{'instructions':<26}{self.instructions_tokens:>7}"
            f"{self.instructions_bytes:>9}"
        )
        lines.append(
            f"{'GRAND TOTAL':<26}{self.total_tokens:>7}{self.total_bytes:>9}"
        )
        return "\n".join(lines)


async def measure_surface(
    *,
    deployment: str = "default",
    allow_writes: bool = False,
    tool_profile: str | None = None,
) -> SurfaceMeasurement:
    """Build a server hermetically and measure exactly what it would serve.

    Requires `OKG_MCP_SURFACE_PROBE` to be set by the caller (a test fixture
    or a diagnostic entry point). It is NOT set here: a measurement helper
    that silently enables a probe mode would let a caller believe it measured
    a normal server when it measured a probe one.
    """
    if os.environ.get("OKG_MCP_SURFACE_PROBE") not in {"1", "true", "TRUE", "yes"}:
        raise RuntimeError(
            "measure_surface requires OKG_MCP_SURFACE_PROBE=1, which builds "
            "the connection pools unopened so the surface can be measured "
            "without a database. Set it in the caller so the probe mode is "
            "visible where the measurement is requested."
        )

    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.capability_probe import split_generated_instructions

    # A NAMED deployment resolves its DSN through deployment authority, which
    # needs a live environment. The measurement only needs the REGISTERED
    # surface, so DSN resolution is stubbed to the unreachable probe DSN. This
    # does not change which manifest is read: instructions, tool profiles and
    # upload provisioning still come from the real deployment.
    previous = server_mod._server_dsn_resolution
    server_mod._server_dsn_resolution = lambda _dsn, _name: {
        "dsn": PROBE_DSN,
        "authority": {"source": "surface_probe_stub"},
    }
    try:
        mcp = build_server(
            dsn=PROBE_DSN,
            deployment_name=deployment,
            allow_writes=allow_writes,
            tool_profile=tool_profile,
        )
    finally:
        server_mod._server_dsn_resolution = previous

    try:
        instructions = mcp.instructions or ""
        rows: list[ToolBytes] = []
        for tool in await mcp.list_tools():
            blob = tool.model_dump_json(by_alias=True, exclude_none=True)
            schema = json.dumps(tool.inputSchema, separators=(",", ":"))
            rows.append(
                ToolBytes(
                    name=tool.name,
                    wire_bytes=len(blob.encode("utf-8")),
                    description_bytes=len((tool.description or "").encode("utf-8")),
                    input_schema_bytes=len(schema.encode("utf-8")),
                    wire_tokens=count_tokens(blob),
                )
            )
        authored, generated = split_generated_instructions(instructions)
        return SurfaceMeasurement(
            deployment=deployment,
            allow_writes=allow_writes,
            tool_profile=tool_profile,
            instructions_bytes=len(instructions.encode("utf-8")),
            tools=tuple(rows),
            generated_instructions_bytes=len(generated.encode("utf-8")),
            instructions_tokens=count_tokens(instructions),
            authored_instructions_tokens=count_tokens(authored),
            generated_instructions_tokens=count_tokens(generated),
        )
    finally:
        mcp._okg_pool.close()
        write_pool: Any = getattr(mcp, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()
