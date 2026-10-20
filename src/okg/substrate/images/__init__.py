"""Images as a first-class okg value (#3046).

`values` defines the image value and every rule about it; `config` parses the
`agent_surface.images` block. Both are dependency-light so the source runner,
the SDK, the read side and blob cleanup can import them freely.
"""
from __future__ import annotations

from okg.substrate.images.config import (
    BudgetConfig,
    ImagesConfig,
    ImagesConfigInvalid,
    RenderConfig,
    UrlConfig,
    UrlPrefix,
)
from okg.substrate.images.values import (
    IMAGE_ERROR_REASONS,
    IMAGE_TAG,
    POST_HASH_REASONS,
    PRE_HASH_REASONS,
    ClassifiedImage,
    ImageRef,
    ImageRefInvalid,
    ImageValueForged,
    ImageValueSite,
    StoredPng,
    check_incoming,
    classify,
    file_value,
    is_image_value,
    iter_image_values,
    url_value,
    with_error,
    with_png,
)

__all__ = [
    "BudgetConfig",
    "ClassifiedImage",
    "IMAGE_ERROR_REASONS",
    "IMAGE_TAG",
    "ImageRef",
    "ImageRefInvalid",
    "ImageValueForged",
    "ImageValueSite",
    "ImagesConfig",
    "ImagesConfigInvalid",
    "POST_HASH_REASONS",
    "PRE_HASH_REASONS",
    "RenderConfig",
    "StoredPng",
    "UrlConfig",
    "UrlPrefix",
    "check_incoming",
    "classify",
    "file_value",
    "is_image_value",
    "iter_image_values",
    "url_value",
    "with_error",
    "with_png",
]
