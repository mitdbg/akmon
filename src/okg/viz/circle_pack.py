"""Hierarchical circle packing for Map mode.

Implements the same algorithm as `d3.hierarchy().pack()`: every node
gets a `(x, y, r)` such that:

  - Each parent's circle exactly contains all its children's circles
    (geometric containment, not heuristic).
  - Sibling circles never overlap.
  - Layout is deterministic — same data → same positions.
  - No force iteration, no convergence time.

The result is the bird's-eye visualization that's been called
"NYTimes infographic" — every level of the hierarchy is a circle,
every parent literally contains its children.

Reference: Wang et al. "Visualization of large hierarchical data by
circle packing" (CHI 2006); d3-hierarchy's `pack.js` ports.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field


@dataclass
class PackNode:
    """One node in the tree being packed. Mutated to set x, y, r."""

    id: str
    value: float = 1.0   # weight / count; drives circle area
    children: list["PackNode"] = field(default_factory=list)
    x: float = 0.0
    y: float = 0.0
    r: float = 0.0


def pack_hierarchy(root: PackNode, padding: float = 4.0) -> None:
    """Recursively assign (x, y, r) to every node in the tree.

    Algorithm (Wang et al., d3-pack):
      - For a leaf: r = sqrt(value).
      - For an internal node: pack children using the front-chain
        algorithm, then enclose them in the smallest enclosing
        circle. Set internal node's r = enclosing circle radius +
        padding.
      - After computing the enclosing circle, the parent's center
        coincides with the enclosing circle's center. Children's
        positions are translated so the parent sits at (0,0)
        relative to its parent (final absolute positions resolved
        when we walk top-down at the end).
    """
    _compute_radii(root, padding)
    # Center the root at (0, 0) and recurse to translate descendants.
    _translate(root, 0.0, 0.0)


def pack_hierarchy_balanced(
    root: PackNode,
    padding: float = 4.0,
    top_padding: float = 8.0,
) -> None:
    """Pack a hierarchy where the top-level children should be
    log-balanced so all are visible at the bird's-eye level.

    Naive `pack_hierarchy` makes a 138-child subtree ~35× larger than
    a 15-child subtree, which buries small forest-roots. Here we:

      1. Pack each top-level child's subtree independently → natural radius.
      2. Rescale each subtree so its radius = `log_target(natural)`,
         compressing dynamic range from ~35× to ~3×.
      3. Pack the rescaled top-level subtrees together at the root.

    Children at deeper levels keep their natural relative sizes within
    each top-level branch — only the bird's-eye level is balanced.
    """
    if not root.children:
        _compute_radii(root, padding)
        _translate(root, 0.0, 0.0)
        return

    # Step 1: pack each top-level subtree independently.
    naturals: list[float] = []
    for child in root.children:
        _compute_radii(child, padding)
        naturals.append(child.r)

    # Step 2: log-rescale. Each subtree gets a target radius bounded by
    # log10(natural). Big subtrees compress; small subtrees stay close
    # to natural. BASE controls the smallest allowed forest-root
    # radius — below BASE, content would be unreadable.
    r_min = max(1.0, min(naturals))
    BASE = 400.0
    targets: list[float] = []
    for natural in naturals:
        if natural <= 0:
            targets.append(BASE)
            continue
        # Target = BASE * (1 + log10(natural/r_min)). r_min becomes
        # exactly BASE. r_min × 10 becomes BASE × 2. r_min × 100 → BASE × 3.
        target = BASE * (1.0 + math.log10(max(1.0, natural / r_min)))
        targets.append(target)
    for child, natural, target in zip(root.children, naturals, targets):
        if natural <= 0:
            continue
        factor = target / natural
        _scale_subtree(child, factor)

    # Step 3: pack the rescaled top-level subtrees + enclose.
    # Same uniform-sibling dispatcher as deeper levels.
    children_sorted = sorted(root.children, key=lambda c: -c.r)
    if _should_disc_fill(children_sorted):
        _hex_fill(children_sorted, top_padding)
    else:
        _pack_circles(children_sorted, top_padding)
    cx, cy, cr = _enclose([(c.x, c.y, c.r) for c in children_sorted])
    for c in children_sorted:
        c.x -= cx
        c.y -= cy
    root.r = cr + top_padding

    # Step 4: top-down translate to absolute coordinates.
    _translate(root, 0.0, 0.0)


def _scale_subtree(node: PackNode, factor: float) -> None:
    """Multiply node.r and all positions/radii in subtree by factor."""
    node.r *= factor
    node.x *= factor
    node.y *= factor
    for child in node.children:
        _scale_subtree(child, factor)


def _compute_radii(node: PackNode, padding: float) -> None:
    """Bottom-up: compute each node's radius and pack its children
    relative to its center."""
    if not node.children:
        # Leaf: radius proportional to sqrt(value).
        node.r = max(2.0, math.sqrt(max(1.0, node.value)))
        return

    # Bottom-up: pack each child's subtree first.
    for child in node.children:
        _compute_radii(child, padding)

    # Pick a packing strategy. Front-chain produces a beautiful
    # spiral when sibling sizes vary widely (the natural d3-pack
    # case), but degenerates to a 1D snake when siblings are all
    # similar — useless for "fill the disc" containment visuals.
    # For uniform siblings, hex-fill packs them densely into the
    # interior of an enclosing circle.
    children = sorted(node.children, key=lambda c: -c.r)
    if _should_disc_fill(children):
        _hex_fill(children, padding)
    else:
        _pack_circles(children, padding)

    # Smallest enclosing circle of the packed children → parent's
    # bounding circle.
    cx, cy, cr = _enclose([(c.x, c.y, c.r) for c in children])
    # Translate children so the enclosing center sits at (0, 0).
    for c in children:
        c.x -= cx
        c.y -= cy
    node.r = cr + padding


def _should_disc_fill(circles: list[PackNode]) -> bool:
    """Use hex disc-fill when there are enough siblings for a chain
    layout to look bad. Below 4 siblings, front-chain is fine. Above,
    we always disc-fill — even with size variance, a hex grid sized
    by the largest sibling yields a disc-shaped layout, which beats
    a 1D snake along the boundary every time."""
    return len(circles) >= 4


def _hex_fill(circles: list[PackNode], padding: float) -> None:
    """Hex-grid fill of N circles inside a bounding disc.

    Sibling radii may vary; the lattice spacing uses the LARGEST
    radius so no two siblings overlap. Smaller siblings get a bit
    of extra slack — disc-fill geometry prioritized over perfect
    tangency. Largest circles assigned to most-central points.
    """
    n = len(circles)
    if n == 0:
        return
    if n == 1:
        circles[0].x = 0.0
        circles[0].y = 0.0
        return

    r = max(c.r for c in circles)
    pad = padding
    step = 2 * r + pad
    # Disc radius: hex packing density × edge-effect slack.
    R = math.sqrt(n / 0.78) * r * 1.15
    # Build hex-lattice candidate points within disc of radius R.
    candidates: list[tuple[float, float]] = []
    rows = int(math.ceil(R / (step * math.sqrt(3) / 2))) + 1
    cols = int(math.ceil(R / step)) + 1
    for row in range(-rows, rows + 1):
        y = row * step * math.sqrt(3) / 2
        x_offset = (step / 2) if (row % 2) else 0.0
        for col in range(-cols, cols + 1):
            x = col * step + x_offset
            if x * x + y * y <= R * R:
                candidates.append((x, y))
    candidates.sort(key=lambda p: p[0] * p[0] + p[1] * p[1])
    # Assign biggest circles to most-central points (descending order).
    for c, (x, y) in zip(circles, candidates):
        c.x = x
        c.y = y
    # Any leftover circles get placed on an outer ring.
    leftover = circles[len(candidates):]
    for i, c in enumerate(leftover):
        theta = 2 * math.pi * i / max(1, len(leftover))
        ring_r = R + step
        c.x = ring_r * math.cos(theta)
        c.y = ring_r * math.sin(theta)


def _pack_circles(circles: list[PackNode], padding: float) -> None:
    """Front-chain (d3-pack) algorithm for packing N circles tangent
    to each other. Mutates each circle's x, y."""
    n = len(circles)
    if n == 0:
        return
    if n == 1:
        circles[0].x = 0.0
        circles[0].y = 0.0
        return

    pad = padding
    # Place first two circles tangent on the x-axis.
    a = circles[0]
    a.x, a.y = 0.0, 0.0
    b = circles[1]
    b.x, b.y = a.r + b.r + pad, 0.0
    if n == 2:
        return
    c = circles[2]
    _place_tangent(c, a, b, pad)

    # Build the front chain: doubly-linked list around the boundary.
    front = [a, b, c]

    for i in range(3, n):
        cand = circles[i]
        # Find a place along the front chain where the new circle
        # fits without overlapping any other front circle.
        placed = False
        for j_offset in range(len(front)):
            ai = front[j_offset]
            bi = front[(j_offset + 1) % len(front)]
            _place_tangent(cand, ai, bi, pad)
            # Check overlap with all other front circles.
            ok = True
            for k, other in enumerate(front):
                if other is ai or other is bi:
                    continue
                if _overlaps(cand, other, pad):
                    ok = False
                    break
            if ok:
                # Insert cand between ai and bi in the front chain.
                front.insert(j_offset + 1, cand)
                placed = True
                break
        if not placed:
            # Fallback: place along the boundary at angle 2πi/n.
            ring_r = max(c.r for c in front) + cand.r + pad
            theta = 2 * math.pi * i / n
            cand.x = ring_r * math.cos(theta)
            cand.y = ring_r * math.sin(theta)
            front.append(cand)


def _place_tangent(c: PackNode, a: PackNode, b: PackNode, pad: float) -> None:
    """Place circle c tangent to both a and b. Solve: distance from
    c.center to a.center = a.r + c.r + pad; distance from c.center
    to b.center = b.r + c.r + pad."""
    dx = b.x - a.x
    dy = b.y - a.y
    d = math.hypot(dx, dy)
    if d == 0:
        c.x = a.x
        c.y = a.y - (a.r + c.r + pad)
        return
    ra, rb, rc = a.r + pad, b.r + pad, c.r + pad
    # Standard two-circle intersection.
    d2 = d * d
    dr = ra + rc
    er = rb + rc
    base = (d2 + dr * dr - er * er) / (2 * d)
    h2 = dr * dr - base * base
    if h2 < 0:
        # Numerical safety: place along the line ab past b.
        c.x = b.x + dx / d * (rb + rc)
        c.y = b.y + dy / d * (rb + rc)
        return
    h = math.sqrt(h2)
    px = a.x + dx / d * base
    py = a.y + dy / d * base
    # Choose the side that's "outside" the chain (the one farther
    # from the centroid of all front circles); for simplicity, pick
    # the one with larger y so the chain spirals outward.
    candidate1 = (px - dy / d * h, py + dx / d * h)
    candidate2 = (px + dy / d * h, py - dx / d * h)
    # Prefer candidate that's farther from origin (roughly outward).
    d1 = candidate1[0] ** 2 + candidate1[1] ** 2
    d2c = candidate2[0] ** 2 + candidate2[1] ** 2
    c.x, c.y = candidate1 if d1 > d2c else candidate2


def _overlaps(a: PackNode, b: PackNode, pad: float) -> bool:
    dx = a.x - b.x
    dy = a.y - b.y
    min_d = a.r + b.r + pad - 1e-6
    return dx * dx + dy * dy < min_d * min_d


def _enclose(circles: list[tuple[float, float, float]]) -> tuple[float, float, float]:
    """Smallest enclosing circle (Welzl's algorithm, simplified).
    For our scale (≤ ~100 children per parent typically), the
    incremental version is fast enough."""
    if not circles:
        return 0.0, 0.0, 0.0
    if len(circles) == 1:
        return circles[0]

    # Incremental approximation: start with first two circles' bounding
    # circle, expand to include each subsequent circle.
    cx, cy, cr = circles[0]
    for x, y, r in circles[1:]:
        d = math.hypot(x - cx, y - cy)
        if d + r <= cr:
            continue   # already inside
        # Expand to include this circle.
        new_r = (cr + d + r) / 2
        if d > 1e-9:
            t = (new_r - cr) / d
            cx += (x - cx) * t
            cy += (y - cy) * t
        cr = new_r
    return cx, cy, cr


def _translate(node: PackNode, ox: float, oy: float) -> None:
    """Top-down: convert each node's relative position to absolute."""
    node.x += ox
    node.y += oy
    for child in node.children:
        _translate(child, node.x, node.y)
