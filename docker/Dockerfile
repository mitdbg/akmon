# Generic OKG MCP base image.
#
# Serves any OKG deployment's read surface (the AKMON MCP operators) over
# streamable-http, so clients that reach the server over the network -- e.g. a
# chatbot running in another container -- can consume it. Deployment-specific
# state is NOT baked in: the deployment package, the Postgres DSN, and any
# per-deployment secrets are supplied at run time via mounts + env, exactly like
# the rucio-mcp sidecar. One image, many deployments.
#
# Build (context = repo root):
#   docker build -f docker/Dockerfile -t okg-mcp:0.1.0 .
#
# Run (CMS example, host-network so it reaches Postgres on localhost:5437;
# replace the host paths with your own deployments dir + env file):
#   docker run --rm --network host \
#     -e CMS_DSN=postgres://postgres:okg@localhost:5437/cms \
#     -e OKG_DEPLOYMENT=cms \
#     -e OKG_MCP_PORT=8100 \
#     -v /path/to/okg-deployments:/deployments:ro \
#     -v /path/to/okg-env-file:/secrets/okg.env:ro \
#     -e OKG_ENV_FILE=/secrets/okg.env \
#     okg-mcp:0.1.0
FROM python:3.14-slim

# System deps:
#   - build-essential + libkrb5-dev: build requests-kerberos (the `cern` extra)
#     if no wheel is available for the target platform.
#   - git: in-process git source adapters at runtime (pygit2 bundles libgit2
#     in its manylinux wheel, so no apt libgit2 is needed).
#   - curl: debugging (no HEALTHCHECK is defined; the MCP endpoint needs a
#     protocol handshake, not a plain GET).
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      libkrb5-dev \
      krb5-user \
      git \
      curl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only what the package build needs. NOTE: `src` is copied before the
# install, so editing framework source busts the dependency layer too -- a
# multi-stage / deps-first split (or a pip cache mount) would avoid that; left
# as a deferred size/build optimization.
# LICENSE and NOTICE* are the package's declared license-files; without them
# the installed package carries no licence text.
COPY pyproject.toml README.md LICENSE NOTICE* ./
COPY src ./src
RUN pip install --no-cache-dir ".[cern]"

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Env-driven, per-deployment. The entrypoint translates these into
# `okg mcp-serve` flags. Defaults target a host-network sidecar.
# OKG_MCP_HOST defaults to loopback (safe under --network host); override to
# 0.0.0.0 for bridge networking with published ports. See entrypoint.sh.
ENV OKG_MCP_TRANSPORT=streamable-http \
    OKG_MCP_HOST=127.0.0.1 \
    OKG_MCP_PORT=8100 \
    OKG_DEPLOYMENT=cms \
    OKG_DEPLOYMENTS_DIR=/deployments

EXPOSE 8100
ENTRYPOINT ["/entrypoint.sh"]
