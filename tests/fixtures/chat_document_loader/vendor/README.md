# Pinned Open WebUI document-loader client

`external_document.py.txt` is a byte-identical copy of
`/app/backend/open_webui/retrieval/loaders/external_document.py`, read out of
a running `ghcr.io/open-webui/open-webui:v0.11.0` container on 2026-08-11.
The `.txt` suffix keeps it out of lint and test collection; the bytes are
unchanged.

`okg.substrate.chat.document_loader.VENDOR_CONTRACT` records its sha256.
`tests/substrate/chat/test_document_loader.py` executes this file's
`ExternalDocumentLoader.load()` against okg's loader endpoint, so the contract
test drives the vendor's own client rather than a transcription of it. A pin
bump changes the file, the digest stops matching, and the test fails.

The file is Open WebUI source, distributed under the license in
`LICENSE-open-webui-v0.11.0.txt`.
