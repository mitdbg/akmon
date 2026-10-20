FROM ghcr.io/astral-sh/uv@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS wheel

# okg is installed into the runtime image from a wheel, not as an editable
# checkout. An editable install runs /opt/okg/src, and release authority then
# needs okg's Git checkout, which the image cannot have (.dockerignore drops
# .git): every publish from a deployment repository outside /opt/okg refused
# with "framework release authority is unavailable". A wheel install is named
# by the wheel itself: its installed files are checked against the wheel's
# RECORD, and the generation records `repository_provenance.status:
# distribution` (PACT publish-from-installed-okg, image-installed-wheel).
#
# The build has no .git either, so setup.py cannot read the source. The
# builder supplies it: VCS_REF names the commit (OKG_BUILD_SOURCE_REVISION),
# and OKG_BUILD_SOURCE_TREE / OKG_BUILD_SOURCE_DIRTY the tree and whether the
# checkout was clean. R4's release workflow will compute those two from its
# checkout; until then CI images pass only VCS_REF and record them as
# unknown. setup.py
# refuses a malformed value. Anything not supplied stays `unknown` and is
# recorded as null, never as clean; the prod channel refuses a wheel whose
# commit is unknown or whose build was not clean.
#
# The build backend is pinned to the setuptools version uv.lock locks, so a
# rebuild of one commit yields the same wheel payload (and payload_digest,
# which a prod pin names) instead of whatever setuptools is newest that day.
ARG VCS_REF=unknown
ARG OKG_BUILD_SOURCE_TREE=unknown
ARG OKG_BUILD_SOURCE_DIRTY=unknown
WORKDIR /build
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    printf 'setuptools==83.0.0\n' > /tmp/build-constraints.txt \
    && OKG_BUILD_SOURCE_REVISION="${VCS_REF}" uv build --wheel \
        --build-constraints /tmp/build-constraints.txt --out-dir /dist \
    && test "$(ls /dist/okg-*.whl | wc -l)" = 1

FROM ghcr.io/astral-sh/uv@sha256:e5b65587bce7de595f299855d7385fe7fca39b8a74baa261ba1b7147afa78e58 AS runtime

ARG OKG_VERSION=dev
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="OKG runtime" \
      org.opencontainers.image.description="Operational Knowledge Graph runtime and deployment assets" \
      org.opencontainers.image.version="${OKG_VERSION}" \
      org.opencontainers.image.source="https://github.com/mitdbg/akmon"

# UV_COMPILE_BYTECODE_TIMEOUT: uv gives each bytecode-compile worker 60 s to
# start Python by default. The linux/arm64 half of the published image builds
# under QEMU emulation on a 1-CPU okg-arc-docker pod, where that startup ran
# past 60 s and `uv sync` failed with "Python startup timed out (60s)" on dev
# runs 36531821115, 36558249590 and 36564666345. 300 s keeps compilation on
# and gives the emulated workers room; a real hang still fails the build.
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_COMPILE_BYTECODE_TIMEOUT=300 \
    UV_CONCURRENT_DOWNLOADS=2 \
    UV_HTTP_TIMEOUT=900 \
    UV_LINK_MODE=copy \
    UV_NO_SYNC=1 \
    OKG_FRAMEWORK_TREE=/opt/okg \
    PATH="/opt/okg/.venv/bin:${PATH}"

# procps supplies `ps`, which the dogfood readiness profile shells out to in
# order to prove exactly one runtime worker process root is active
# (`runtime.worker_processes`). Without it the audit reports "unavailable" and
# fails closed, so the container can never satisfy dogfood readiness and its
# startup probe never passes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        gh \
        git \
        libmagic1 \
        openssh-client \
        procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/okg

COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --extra quality --no-install-project

# The source tree stays at /opt/okg for what is read from it as files:
# deployments/, ops/, scripts/, docs and the lifecycle lane's copy of src/.
# Python imports okg from the installed wheel in .venv, not from src/: there
# is no PYTHONPATH, and UV_NO_SYNC keeps a `uv run` here from reinstalling
# okg as an editable checkout. OKG_FRAMEWORK_TREE tells okg where that tree
# is, since a wheel install cannot find it from its own path. The import
# check below fails the build unless okg imports from a wheel whose RECORD
# verifies.
#
# `--python .../python3` is load-bearing: uv writes the interpreter it was
# given into the `okg` console script's shebang, and the worker container runs
# `okg`, so that spelling is the worker's sys.executable. The container
# service authority declares .venv/bin/python3 (deployment_apply
# CONTAINER_INTERPRETER), as the editable `uv sync` install used to write;
# with `--python .../python` the worker identity gate refuses every worker
# ("identity process_executable differs"). The shebang check below holds it.
#
# The environment in .venv stays owned by root: the runtime user can read and
# run the installed wheel but not edit it. The rest of /opt/okg stays owned by
# the runtime user, as before, because okg writes run-time state beneath its
# working directory (profiles/, .okg/).
COPY . .
COPY --from=wheel /dist/ /tmp/okg-wheel/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python /opt/okg/.venv/bin/python3 --no-deps /tmp/okg-wheel/okg-*.whl \
    && rm -rf /tmp/okg-wheel \
    && test "$(head -n 1 /opt/okg/.venv/bin/okg)" = "#!/opt/okg/.venv/bin/python3" \
    && cd / \
    && /opt/okg/.venv/bin/python -c "from okg.substrate.distributions.installed_framework import installed_framework_distribution as f; i = f(); assert i is not None, 'okg is not a verified wheel install'; print('okg wheel', i.version, i.payload_digest, 'source_revision', i.source_revision, 'source_dirty', i.source_dirty)" \
    && cd /opt/okg \
    && groupadd --gid 10001 okg \
    && useradd --uid 10001 --gid okg --home-dir /var/lib/okg --create-home okg \
    && mkdir -p /var/lib/okg /tmp/okg \
    && chown okg:okg /opt/okg \
    && find /opt/okg -mindepth 1 -maxdepth 1 ! -name .venv -exec chown -R okg:okg {} + \
    && chown -R okg:okg /var/lib/okg /tmp/okg

USER 10001:10001
WORKDIR /opt/okg
STOPSIGNAL SIGTERM

CMD ["okg", "--help"]

# Keep the per-build revision at the end so changing the SHA does not invalidate
# the expensive apt and uv dependency layers above.
#
# The env var carries the same fact as the label into the running process. A
# label is only readable through the container runtime API; the service-authority
# identity gate runs *inside* the container, where `git rev-parse` cannot answer
# because .dockerignore drops .git. Without this the revision is unknowable
# in-image and the gate refuses forever.
LABEL org.opencontainers.image.revision="${VCS_REF}"
ENV OKG_CODE_REVISION="${VCS_REF}"
