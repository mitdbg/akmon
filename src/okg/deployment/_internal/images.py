"""Image value builder for connector facts (#3046).

``ImageRef`` is an **alias**: the substrate object itself, re-exported under
the public import path, like the fact names in ``connectors``. It lives in its
own module so resolving it does not pull the connector surface's heavier
import chain. ``ImageRef.file(path)`` and ``ImageRef.url(url)`` return plain
JSON dicts; ``okg.substrate.images.values`` holds every rule about them.
"""
from __future__ import annotations

from okg.substrate.images.values import ImageRef

__all__ = ["ImageRef"]
