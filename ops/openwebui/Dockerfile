FROM ghcr.io/open-webui/open-webui@sha256:9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f AS verifier

USER root

COPY ops/openwebui/apply_mcp_client_patch.py /tmp/okg-apply-mcp-client-patch.py
COPY ops/openwebui/verify_compatibility.py /tmp/okg-verify-mcp-compatibility.py
COPY ops/openwebui/run_compatibility_verifier.py /tmp/okg-run-mcp-compatibility.py
COPY ops/openwebui/upstream-notices/LICENSE /usr/share/licenses/okg-openwebui-compat/OpenWebUI-LICENSE
COPY ops/openwebui/upstream-notices/LICENSE_HISTORY /usr/share/licenses/okg-openwebui-compat/OpenWebUI-LICENSE_HISTORY

RUN set -eu; \
    verify_root="$(mktemp -d /tmp/okg-openwebui-verify.XXXXXX)"; \
    trap 'rm -rf "$verify_root"' 0; \
    export \
        OKG_OPENWEBUI_VERIFY_ROOT="$verify_root" \
        DATA_DIR="$verify_root/data" \
        DATABASE_URL="sqlite:///$verify_root/data/webui.db" \
        HOME="$verify_root/home" \
        PYTHONPATH=/app/backend \
        STATIC_DIR="$verify_root/static" \
        XDG_CACHE_HOME="$verify_root/cache/xdg" \
        HF_HOME="$verify_root/cache/huggingface" \
        SENTENCE_TRANSFORMERS_HOME="$verify_root/cache/sentence-transformers" \
        WHISPER_MODEL_DIR="$verify_root/cache/whisper" \
        TIKTOKEN_CACHE_DIR="$verify_root/cache/tiktoken" \
        AUDIT_LOGS_FILE_PATH="$verify_root/data/audit.log" \
        ENABLE_AUDIT_LOGS_FILE=False \
        ENABLE_DB_MIGRATIONS=False \
        ENABLE_PLUGINS=False \
        ENABLE_VERSION_UPDATE_CHECK=False \
        CUSTOM_NAME= \
        HF_HUB_OFFLINE=1 \
        OFFLINE_MODE=True \
        OAUTH_SESSION_TOKEN_ENCRYPTION_KEY=okg-public-verifier-fixture \
        WEBUI_AUTH=False; \
    mkdir -p "$DATA_DIR" "$HOME" "$STATIC_DIR" "$XDG_CACHE_HOME"; \
    python /tmp/okg-apply-mcp-client-patch.py; \
    python /tmp/okg-run-mcp-compatibility.py; \
    rm -f \
        /tmp/okg-apply-mcp-client-patch.py \
        /tmp/okg-run-mcp-compatibility.py \
        /tmp/okg-verify-mcp-compatibility.py

FROM ghcr.io/open-webui/open-webui@sha256:9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f

ARG VCS_REF=unknown

COPY --from=verifier /app/backend/open_webui/utils/mcp/client.py /app/backend/open_webui/utils/mcp/client.py
COPY --from=verifier /usr/share/licenses/okg-openwebui-compat/ /usr/share/licenses/okg-openwebui-compat/

LABEL org.opencontainers.image.source="https://github.com/mitdbg/akmon" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.version="0.11.4-okg-mcp-structured-content.1" \
      org.opencontainers.image.base.name="ghcr.io/open-webui/open-webui:v0.11.4" \
      org.opencontainers.image.base.digest="sha256:9591b13f13843c7721c2b8eaf7382846c81b3ffe126526d1888d1fed50c6a33f" \
      org.mitdbg.okg.openwebui.client-source.sha256="f46c5e7e01cf44bad0e75716f707776437aaf8e2aa7927e511185a014d0e64bb" \
      org.mitdbg.okg.openwebui.client-patched.sha256="12fac6a236a4eb6464ab18b3970c76be3b030355cb0a30e6a6ee2667d94e9813"
