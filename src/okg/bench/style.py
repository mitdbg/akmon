"""Matplotlib + seaborn style + color palette for the bench plots.

Defines `apply_paper_style()` (call once per plot script) and the
canonical colors/line styles used across every figure so the same
repo always has the same hue and the same source set always has
the same dash pattern.
"""
from __future__ import annotations

try:
    import matplotlib.pyplot as plt
    import matplotlib as mpl
except ImportError as _plot_exc:  # pragma: no cover - wheel test
    raise ImportError(
        "okg.bench.style needs matplotlib — install the bench extra: "
        "pip install 'okg[bench]'"
    ) from _plot_exc


# One color per repo. Picked from a colorblind-safe palette
# (Wong 2011 / IBM Carbon design system).
REPO_COLOR = {
    "tinydb": "#0072B2",  # blue
    "marius": "#009E73",  # green
    "quake":  "#D55E00",  # vermilion
    "pandas": "#785EF0",  # purple
}

# One line style per source set. Solid = primary, dashed/dotted =
# variants. Keep `files+history` solid because it's the "full"
# config most plots show.
SOURCE_SET_LINESTYLE = {
    "files+history": "-",
    "files-only":    "--",
    "history-only":  ":",
}

SOURCE_SET_LABEL = {
    "files+history": "files + history",
    "files-only":    "files only",
    "history-only":  "history only",
}


def apply_paper_style() -> None:
    """Configure matplotlib for publication-quality output.

    Call once at the top of any plot script. Sets the seaborn paper
    theme, embeds fonts in saved PDFs (so a reader without the font
    installed still sees the right glyphs), and bumps a few defaults
    to be print-friendly.
    """
    try:
        plt.style.use("seaborn-v0_8-paper")
    except OSError:
        # older matplotlib may not have the v0_8 prefix
        plt.style.use("seaborn-paper")

    mpl.rcParams.update({
        # Make PDFs embed type-3 → type-42 (TrueType) for portability.
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "font.family": "sans-serif",
        "font.sans-serif": [
            "Helvetica", "Arial", "DejaVu Sans",
        ],
        "font.size":      10,
        "axes.titlesize": 12,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "figure.figsize": (6.0, 4.0),
        "figure.dpi":     120,
        "savefig.dpi":    300,
        "savefig.bbox":   "tight",
        "savefig.pad_inches": 0.05,
        # Axis spines: keep all four for paper style.
        "axes.spines.top":   True,
        "axes.spines.right": True,
        # Grid: subtle horizontal only.
        "axes.grid":      True,
        "grid.alpha":     0.3,
        "grid.linestyle": "-",
        "grid.linewidth": 0.4,
    })


def color_for_repo(repo_name: str) -> str:
    return REPO_COLOR.get(repo_name, "#666666")


def linestyle_for_source_set(source_set: str) -> str:
    return SOURCE_SET_LINESTYLE.get(source_set, "-")


def label_for_source_set(source_set: str) -> str:
    return SOURCE_SET_LABEL.get(source_set, source_set)
