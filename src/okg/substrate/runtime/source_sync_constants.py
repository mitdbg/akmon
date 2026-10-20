"""Lightweight source-sync runtime constants."""
from __future__ import annotations


SOURCE_SYNC_WORKFLOW_NAME = "okg_source_sync"

# DBOS uses one application version for the whole worker process.  This
# explicit marker is the replay boundary for the setup -> observed_at ->
# ingest_cycle topology; bump it when that topology changes so old histories
# are not silently replayed under a new workflow shape.
SOURCE_SYNC_APPLICATION_VERSION = "okg-runtime-source-sync-v2"
