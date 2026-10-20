# OpenWebUI MCP structured-result compatibility image

This directory defines a derived image for the repository's pinned OpenWebUI
v0.11.4 client. The upstream base is immutable:

- reference: `ghcr.io/open-webui/open-webui:v0.11.4`
- digest: `sha256:9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f`
  (the multi-architecture index; upstream revision
  `8bd8b4fac5e059578ac0c74b3c18d11139f88b7d`)
- native MCP client source:
  `backend/open_webui/utils/mcp/client.py`
- upstream client SHA-256:
  `f46c5e7e01cf44bad0e75716f707776437aaf8e2aa7927e511185a014d0e64bb`
- patched client SHA-256:
  `12fac6a236a4eb6464ab18b3970c76be3b030355cb0a30e6a6ee2667d94e9813`

The v0.11.0 base (`sha256:72c0ba64…`, client `516bc5e4…`) was replaced by
v0.11.4. Between the two tags the client's result-selection block is
byte-identical; upstream only made `list_tools` read every page of a
paginated tool list. The patch is therefore unchanged and only the two
whole-file digests moved.

The build changes only the MCP result selection. A non-null
`structuredContent` value wins, including `{}`; legacy results whose
`structuredContent` is null continue through `content`. The build refuses
any upstream source drift and runs the real patched client plus OpenWebUI's
downstream result formatter before an image can exist. The formatter import
loads OpenWebUI's broader application module graph, which creates upload and
cache directories and constructs database engine objects. The build disables
database migrations, authentication, plugins, version checks, custom-branding
fetches, model downloads, and audit file logging; it enables offline mode and
redirects data, SQLite, home, model cache, and audit paths into one temporary directory. A shell exit trap removes that directory on success or failure, so verifier-created database, config, cache,
or secret state is not retained in the image.

Pull-request builds verify only: they build the image, run the guards and the
compatibility verifier, and delete the local image. Nothing is pushed and no
registry credential is used.

The container image is not published. Build it from this source with
`ops/openwebui/Dockerfile`; the build context is the repository root,
because the Dockerfile copies files by their repository paths. Push it
to your registry.

In this copy the pinned chat image is a placeholder under the reserved
`your-registry.invalid` domain, so it cannot be pulled. The Dockerfile
builds on Open WebUI v0.11.4. Only an image listed in `TESTED_IMAGES` in
`src/okg/substrate/chat/instance.py` is admitted, mapped to the version
it answers at `/api/version`; `PINNED_VERSION` in the same file is the
Open WebUI version the chat measurements were taken on. Set `chat.image`
to `<your-registry>/okg-openwebui-compat@sha256:<digest>` and add that
reference to `TESTED_IMAGES` mapped to `"0.11.4"`. If `PINNED_VERSION`
is not `0.11.4`, the measurements were taken on an older build and were
not repeated on the one you build. On `dev` it builds only when a file the Dockerfile copies, the
Dockerfile, `.dockerignore`, or the job's own definition changed since the
image `dev` points at; otherwise `dev` stays on that image and no
`sha-<commit>` tag is written. A manual run with `force_rebuild: openwebui`
rebuilds anyway.

## Adoption

`okg.substrate.chat.instance` pins one build of this image by digest (in
this copy a placeholder; see above), and it is the ONLY tested image; the upstream reference now refuses with
`chat_image_not_tested`, naming this image and the reason.

- pinned reference (v0.11.4 base):
  `your-registry.invalid/okg-openwebui-compat@sha256:2561170716571aaf7e41417decd7e23b43c390a00395c957b362a17e80557c11`
- published by dev `46ef2e92a7` (PR #2930), container-images run 36531821115,
  job 109286977916; tags `dev` and `sha-46ef2e92a7ea6b16c5a0211af17c1f876c8e5f77`
- adopted by PACT `openwebui-0114-pin-and-cuts`, which re-measured okg's
  vendor pins at v0.11.4

The v0.11.0-based build (`sha256:e0b1f475…`, dev `6623262a24`, run
35549288823) left the tested set with that change and now refuses with
`chat_image_not_tested` (reason `image_not_tested`).

One consequence of the v0.11.4 site cuts: `okg chat sync` writes the okg
preset's whole `meta.builtinTools` map (site default, then the preset's own
choices, then `knowledge: false`). After that first write the preset keeps a
frozen copy of the site defaults and no longer follows later site-wide
changes, because v0.11.4 applies the site default only to a model with no
`builtinTools` of its own (`backend/open_webui/utils/models.py:339-356`).
A later sync does not refresh it either, since the preset's own copy
overlays the site default: change a site-wide category on the preset too.

**Architecture.** The upstream pin
`ghcr.io/open-webui/open-webui@sha256:9591b13f…` is a multi-architecture index
carrying `linux/amd64` and `linux/arm64`. This derived image is built
`linux/amd64` ONLY. An Apple-silicon operator who previously ran the upstream
image natively on arm64 will now run this one under emulation, which is slower
to start and uses more memory, and needs Rosetta or QEMU enabled in Docker
Desktop. Publishing an arm64 variant would remove that cost and is not part of
this change.

One upstream behaviour is deliberately left alone. When a tool result is an
error, the client raises with the selected object only, so an error carrying
both `structuredContent` and `content` loses the text half. That is upstream's
own shape: this patch changes which object is selected, never how a refusal is
raised. Carrying both would change the patched client's byte digest, and with
it the reviewed provenance labels, the frozen source excerpt, and the
in-image verifier expectations, so it belongs in its own reviewed change.

Since okg issue #3059 an okg result carries no `structuredContent` at all:
every result, success or refusal, is ONE text block in `content`. The patched
client therefore selects `content` for it, as upstream does. A success is
returned as that content list, and a refusal (`isError: true`) is raised as
`Exception(<the content list>)`, whose one text block holds the whole refusal:
its status line, code and fix-it fields. The verifier checks both forms.

The frozen source excerpt comes from the official OpenWebUI v0.11.4 client.
OpenWebUI's v0.11.4 top-level `LICENSE` is the Open WebUI License and says
materials governed by prior licenses retain those terms through
`LICENSE_HISTORY`. Both official notices are retained byte-for-byte in
`ops/openwebui/upstream-notices/` and copied into the derived image; both
files are byte-identical at v0.11.0 and v0.11.4.
