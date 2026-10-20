"""Measure coherence of the built-in library and the deployment tree.

Emits machine-readable counts per coherence dimension so that CI can compare
the live library state rather than relying on a point-in-time audit page.

Dimensions:
  ontology     duplicate class names, declared `extends:` vs the cross-module
               narrowing endpoints each schema actually references, archetype
               vocabulary spread and near-duplicate spellings, module versions.
  profiles     composed module sets, pairwise overlap, and closure: does a
               profile's module set contain every module that owns a class the
               set's narrowings reference?
  sources      inventory plus `output_signature` declaration coverage — the
               machine-checkable source->ontology binding contract.
  deployments  declared profile, artifacts that shadow a library equivalent,
               and last-touch date per deployment.

The deployments tree lives in a separate repo. When it is absent the probe
reports `status: unavailable` for that dimension; it never substitutes a
default (a default-value fallback would misreport the audit as complete).

Usage:
    uv run python scripts/audit_library_coherence.py [--json] [--deployments PATH]
"""
from __future__ import annotations

import argparse
import collections
import difflib
import json
import pathlib
import subprocess
import sys

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
LIBRARY = REPO_ROOT / "src/okg/substrate/library"
ONTOLOGIES = LIBRARY / "ontologies"
PROFILES = LIBRARY / "profiles"
SOURCES = LIBRARY / "sources"
ENRICHERS = LIBRARY / "enrichers"
DEFAULT_DEPLOYMENTS = pathlib.Path.home() / "projects/okg-deployments"

# Library subdirectories a deployment may shadow with a local copy.
SHADOWABLE = ("schemas", "enrichers", "sources", "checks", "extractors", "concepts")


def _has_output_signature(entry: object) -> bool:
    """True when a source registry entry declares an output signature.

    The key is normally nested under the entry's `admission_policy` block;
    a top-level spelling is also accepted so the count does not depend on
    which of the two shapes a registry happens to use.
    """
    if not isinstance(entry, dict):
        return False
    if entry.get("output_signature"):
        return True
    policy = entry.get("admission_policy")
    return bool(isinstance(policy, dict) and policy.get("output_signature"))


def _load_yaml(path: pathlib.Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError:
        return {}


def _is_narrowing_class(cdef: dict) -> bool:
    ann = (cdef or {}).get("annotations") or {}
    return str(ann.get("okg_edge_narrowings", "")).lower() == "true"


def _split_subtypes(value: object) -> list[str]:
    return str(value or "").replace(",", " ").split()


def scan_ontology() -> dict:
    """Inventory classes, narrowings, and cross-module references."""
    mod_dirs = [
        d for d in sorted(ONTOLOGIES.iterdir())
        if d.is_dir() and not d.name.startswith("__")
    ]
    classes: dict[str, list[str]] = collections.defaultdict(list)
    owner: dict[str, str] = {}
    node_archetypes: collections.Counter = collections.Counter()
    edge_archetypes: collections.Counter = collections.Counter()
    versions: dict[str, str] = {}
    declared: dict[str, set[str]] = {}
    narrowing_names: dict[str, list[str]] = collections.defaultdict(list)
    raw: dict[str, dict] = {}

    for d in mod_dirs:
        meta = _load_yaml(d / "module.yaml")
        versions[d.name] = str(meta.get("version"))
        declared[d.name] = set((meta.get("extends") or {}).keys())
        doc = _load_yaml(d / "schema.yaml")
        raw[d.name] = doc
        for cname, cdef in (doc.get("classes") or {}).items():
            cdef = cdef or {}
            if _is_narrowing_class(cdef):
                for nname, ndef in (cdef.get("attributes") or {}).items():
                    narrowing_names[nname].append(d.name)
                    nann = ((ndef or {}).get("annotations") or {})
                    if nann.get("edge_archetype"):
                        edge_archetypes[str(nann["edge_archetype"])] += 1
                continue
            classes[cname].append(d.name)
            owner.setdefault(cname, d.name)
            ann = cdef.get("annotations") or {}
            if ann.get("archetype"):
                node_archetypes[str(ann["archetype"])] += 1

    # Which other modules does each module's narrowings actually reference?
    #
    # A reference is cross-module only when the referencing module does NOT
    # itself define the endpoint class. 9 class names are defined by two
    # modules each, so resolving an endpoint through a single global owner map
    # would report a module as depending on a twin that merely shares the name
    # — inflating both the dependency and profile-closure counts.
    defines: dict[str, set[str]] = collections.defaultdict(set)
    for cname, mods in classes.items():
        for m in mods:
            defines[m].add(cname)

    referenced: dict[str, set[str]] = {}
    needs_classes: dict[str, set[str]] = {}
    unresolved: dict[str, list[str]] = collections.defaultdict(list)
    for name, doc in raw.items():
        targets: set[str] = set()
        needed: set[str] = set()
        for _c, cdef in (doc.get("classes") or {}).items():
            if not _is_narrowing_class(cdef or {}):
                continue
            for nname, ndef in ((cdef or {}).get("attributes") or {}).items():
                nann = ((ndef or {}).get("annotations") or {})
                for side in ("src_subtypes", "dst_subtypes"):
                    for st in _split_subtypes(nann.get(side)):
                        if st in defines[name]:
                            continue  # satisfied locally; not a dependency
                        holders = classes.get(st)
                        if not holders:
                            unresolved[name].append(f"{nname}:{st}")
                            continue
                        needed.add(st)
                        targets.update(holders)
        referenced[name] = targets
        needs_classes[name] = needed

    undeclared = {
        m: sorted(referenced[m] - declared.get(m, set()))
        for m in sorted(raw)
        if referenced[m] - declared.get(m, set())
    }
    # `extends:` entries no module reference backs — declaration-only edges.
    unreferenced = {
        m: sorted(declared.get(m, set()) - referenced[m])
        for m in sorted(raw)
        if declared.get(m, set()) - referenced[m]
    }
    duplicate_classes = {
        k: sorted(v) for k, v in sorted(classes.items()) if len(v) > 1
    }
    duplicate_narrowings = {
        k: sorted(v) for k, v in sorted(narrowing_names.items()) if len(v) > 1
    }

    # Near-duplicate archetype spellings, e.g. `derived_from` vs
    # `derives_from`. The difference is an internal verb inflection, so
    # suffix-stripping does not catch it; compare token similarity instead.
    near_dupes: list[list[str]] = []
    for group in (node_archetypes, edge_archetypes):
        toks = sorted(group)
        for i, a in enumerate(toks):
            for b in toks[i + 1:]:
                if a == b:
                    continue
                ratio = difflib.SequenceMatcher(None, a, b).ratio()
                # Also treat active/passive pairs (verifies / verified_by,
                # contains / contained_by) as candidates for the same relation.
                stem_a = a.replace("_by", "").rstrip("sd")
                stem_b = b.replace("_by", "").rstrip("sd")
                if ratio >= 0.85 or (stem_a and stem_a == stem_b):
                    near_dupes.append([a, b, round(ratio, 3)])

    return {
        "module_count": len(mod_dirs),
        "class_count": len(classes),
        "narrowing_count": len(narrowing_names),
        "duplicate_classes": duplicate_classes,
        "duplicate_class_count": len(duplicate_classes),
        "duplicate_narrowings": duplicate_narrowings,
        "undeclared_cross_module_deps": undeclared,
        "undeclared_cross_module_dep_count": sum(
            len(v) for v in undeclared.values()
        ),
        "unreferenced_extends": unreferenced,
        "unreferenced_extends_count": sum(len(v) for v in unreferenced.values()),
        "node_archetypes": dict(node_archetypes.most_common()),
        "edge_archetypes": dict(edge_archetypes.most_common()),
        "edge_archetype_count": len(edge_archetypes),
        "singleton_edge_archetypes": sorted(
            k for k, v in edge_archetypes.items() if v == 1
        ),
        "near_duplicate_archetypes": near_dupes,
        "unresolved_narrowing_endpoints": dict(unresolved),
        "unresolved_narrowing_endpoint_count": sum(
            len(v) for v in unresolved.values()
        ),
        "versions": versions,
        "distinct_versions": sorted(set(versions.values())),
        "_owner": owner,
        "_referenced": referenced,
        "_needs_classes": {k: sorted(v) for k, v in needs_classes.items()},
        "_class_holders": {k: sorted(v) for k, v in classes.items()},
    }


def scan_profiles(ont: dict) -> dict:
    """Module sets, pairwise overlap, and closure under narrowing endpoints."""
    prof_dirs = [
        d for d in sorted(PROFILES.iterdir())
        if d.is_dir() and not d.name.startswith("__")
    ]
    sets: dict[str, set[str]] = {}
    for d in prof_dirs:
        mods = _load_yaml(d / "modules.yaml").get("modules") or []
        sets[d.name] = {str(m) for m in mods}

    known = set(ont["versions"])
    missing_modules = {
        p: sorted(s - known) for p, s in sets.items() if s - known
    }
    # Closure: for every endpoint class a member module's narrowings reference
    # but do not define, at least one module that DOES define it must be in the
    # set. Resolution is per class, not per module: a duplicated class is
    # satisfied by any one of its holders.
    needs = ont["_needs_classes"]
    holders = ont["_class_holders"]
    closure_gaps = {}
    for p, s in sets.items():
        gap = {}
        for m in s:
            for cls in needs.get(m, []):
                if not (set(holders.get(cls, [])) & s):
                    gap.setdefault(cls, {
                        "needed_by": [], "satisfied_by_any_of": holders.get(cls, []),
                    })
                    gap[cls]["needed_by"].append(m)
        if gap:
            closure_gaps[p] = gap

    overlap = {}
    names = sorted(sets)
    for i, a in enumerate(names):
        for b in names[i + 1:]:
            sa, sb = sets[a], sets[b]
            if not (sa | sb):
                continue
            j = len(sa & sb) / len(sa | sb)
            overlap[f"{a}|{b}"] = {
                "jaccard": round(j, 3),
                "shared": sorted(sa & sb),
                "only_" + a: sorted(sa - sb),
                "only_" + b: sorted(sb - sa),
            }

    return {
        "profile_count": len(prof_dirs),
        "module_sets": {p: sorted(s) for p, s in sets.items()},
        "missing_modules": missing_modules,
        "closure_gaps": closure_gaps,
        "closure_gap_count": sum(len(v) for v in closure_gaps.values()),
        "pairwise_overlap": overlap,
        "unused_modules": sorted(
            known - set().union(*sets.values()) if sets else known
        ),
    }


def scan_sources() -> dict:
    """Source inventory and `output_signature` binding-declaration coverage."""
    single = sorted(
        p.stem for p in SOURCES.glob("*.py")
        if not p.stem.startswith("_") and p.stem != "__init__"
    )
    pkgs = sorted(
        d.name for d in SOURCES.iterdir()
        if d.is_dir() and not d.name.startswith(("_", "."))
    )
    helpers = sorted(
        p.stem for p in SOURCES.glob("_*.py") if p.stem != "__init__"
    )
    registries = sorted(LIBRARY.glob("templates/*/source_registry.yaml"))
    declared_sigs = 0
    registry_sources = 0
    per_registry = {}
    for r in registries:
        doc = _load_yaml(r)
        srcs = doc.get("sources") or {}
        sigs = sum(1 for v in srcs.values() if _has_output_signature(v))
        per_registry[str(r.relative_to(REPO_ROOT))] = {
            "sources": len(srcs), "with_output_signature": sigs,
        }
        registry_sources += len(srcs)
        declared_sigs += sigs

    enr_single = sorted(
        p.stem for p in ENRICHERS.glob("*.py")
        if not p.stem.startswith("_") and p.stem != "__init__"
    )
    enr_pkgs = sorted(
        d.name for d in ENRICHERS.iterdir()
        if d.is_dir() and not d.name.startswith(("_", "."))
    )
    return {
        "single_file_sources": single,
        "package_sources": pkgs,
        "helper_modules": helpers,
        "source_count": len(single) + len(pkgs),
        "enricher_single_file": enr_single,
        "enricher_packages": enr_pkgs,
        "enricher_count": len(enr_single) + len(enr_pkgs),
        "template_registries": per_registry,
        "registry_source_count": registry_sources,
        "registry_with_output_signature": declared_sigs,
    }


def _local_class_names(dep_dir: pathlib.Path) -> set[str]:
    """Class names a deployment defines in its own schemas/ tree."""
    names: set[str] = set()
    schemas = dep_dir / "schemas"
    if not schemas.is_dir():
        return names
    for y in schemas.rglob("*.yaml"):
        doc = _load_yaml(y)
        for cname, cdef in (doc.get("classes") or {}).items():
            if not _is_narrowing_class(cdef or {}):
                names.add(cname)
    return names


def scan_deployments(root: pathlib.Path, library_classes: set[str]) -> dict:
    """Declared profile, local-vs-library class collisions, last-touch per deployment.

    A local `schemas/` tree is not by itself redundancy — a deployment is
    expected to extend the ontology with domain subtypes. Redundancy is a local
    class that RE-DEFINES a name the library already owns, so that is measured
    separately from mere directory presence.
    """
    if not root.is_dir():
        return {
            "status": "unavailable",
            "reason": f"deployments tree not found at {root}",
        }
    dirs = [
        d for d in sorted(root.iterdir())
        if d.is_dir() and not d.name.startswith((".", "_")) and d.name != "scripts"
    ]
    out = {}
    for d in dirs:
        prof = _load_yaml(d / "profile.yaml")
        dep = _load_yaml(d / "deployment.yaml")
        try:
            last = subprocess.run(
                ["git", "-C", str(root), "log", "-1", "--format=%cs", "--", d.name],
                capture_output=True, text=True, timeout=30,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            last = "unknown"
        local = _local_class_names(d)
        declared = (
            prof.get("name") or prof.get("profile") or dep.get("profile") or None
        )
        reg = _load_yaml(d / "source_registry.yaml").get("sources") or {}
        reg_sigs = sum(1 for v in reg.values() if _has_output_signature(v))
        out[d.name] = {
            "declared_profile": declared,
            "registry_sources": len(reg),
            "registry_with_output_signature": reg_sigs,
            "defines_own_profile": declared == d.name,
            "library_modules": sorted(str(m) for m in (dep.get("modules") or [])),
            "local_class_count": len(local),
            "collides_with_library": sorted(local & library_classes),
            "local_dirs": [s for s in SHADOWABLE if (d / s).is_dir()],
            "has_source_registry": (d / "source_registry.yaml").is_file(),
            "last_commit": last or "unknown",
        }
    no_profile = sorted(k for k, v in out.items() if not v["declared_profile"])
    return {
        "status": "ok",
        "root": str(root),
        "deployment_count": len(dirs),
        "deployments": out,
        "without_declared_profile": no_profile,
        "defining_own_profile": sorted(
            k for k, v in out.items() if v["defines_own_profile"]
        ),
        "with_library_collisions": {
            k: v["collides_with_library"] for k, v in out.items()
            if v["collides_with_library"]
        },
        "collision_count": sum(
            len(v["collides_with_library"]) for v in out.values()
        ),
        "registry_source_total": sum(v["registry_sources"] for v in out.values()),
        "registry_with_output_signature_total": sum(
            v["registry_with_output_signature"] for v in out.values()
        ),
    }


def build_report(deployments_root: pathlib.Path) -> dict:
    ont = scan_ontology()
    prof = scan_profiles(ont)
    src = scan_sources()
    dep = scan_deployments(deployments_root, set(ont["_class_holders"]))
    ont_public = {k: v for k, v in ont.items() if not k.startswith("_")}
    return {
        "ontology": ont_public,
        "profiles": prof,
        "sources": src,
        "deployments": dep,
    }


def _print_summary(rep: dict) -> None:
    o, p, s, d = rep["ontology"], rep["profiles"], rep["sources"], rep["deployments"]
    print("== ontology ==")
    print(f"  modules                      {o['module_count']}")
    print(f"  classes                      {o['class_count']}")
    print(f"  narrowings                   {o['narrowing_count']}")
    print(f"  duplicate class names        {o['duplicate_class_count']}")
    print(f"  undeclared cross-module deps {o['undeclared_cross_module_dep_count']}")
    print(f"  unreferenced `extends`       {o['unreferenced_extends_count']}")
    print(f"  edge archetypes              {o['edge_archetype_count']}"
          f" ({len(o['singleton_edge_archetypes'])} used once)")
    print(f"  near-duplicate archetypes    {len(o['near_duplicate_archetypes'])}")
    print(f"  distinct module versions     {o['distinct_versions']}")
    print("== profiles ==")
    print(f"  profiles                     {p['profile_count']}")
    print(f"  closure gaps                 {p['closure_gap_count']}")
    for k, v in p["closure_gaps"].items():
        print(f"    {k}: unresolvable endpoint classes {sorted(v)}")
    print(f"  modules in no profile        {len(p['unused_modules'])}")
    print("== sources ==")
    print(f"  sources                      {s['source_count']}")
    print(f"  enrichers                    {s['enricher_count']}")
    print(f"  registry sources w/ signature"
          f" {s['registry_with_output_signature']}/{s['registry_source_count']}")
    print("== deployments ==")
    if d["status"] != "ok":
        print(f"  UNAVAILABLE: {d['reason']}")
        return
    print(f"  deployments                  {d['deployment_count']}")
    print(f"  without declared profile     {len(d['without_declared_profile'])}")
    print(f"  defining their own profile   {len(d['defining_own_profile'])}")
    print(f"  registry sources w/ signature"
          f" {d['registry_with_output_signature_total']}/{d['registry_source_total']}")
    print(f"  local classes colliding with library"
          f" {d['collision_count']} across {len(d['with_library_collisions'])} deployments")
    for k, v in d["with_library_collisions"].items():
        print(f"    {k}: {v}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--json", action="store_true", help="emit the full report as JSON")
    ap.add_argument(
        "--deployments", type=pathlib.Path, default=DEFAULT_DEPLOYMENTS,
        help="path to the okg-deployments tree",
    )
    args = ap.parse_args(argv)
    rep = build_report(args.deployments)
    if args.json:
        print(json.dumps(rep, indent=2, sort_keys=True))
    else:
        _print_summary(rep)
    return 0


if __name__ == "__main__":
    sys.exit(main())
