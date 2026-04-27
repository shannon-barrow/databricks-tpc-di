"""Render Jinja + Python-builder for the same inputs and assert equivalence.

Run from the repo root with:

    python -m src.tools.workflow_builders._diff_harness

Each test case calls the builder, calls `render_dag` against the
corresponding Jinja template, and prints a deep-diff. Exits non-zero if
any case mismatches.
"""
from __future__ import annotations

import json
import sys
import os
from typing import Any

# Make sibling modules importable when run as a script
_THIS = os.path.dirname(os.path.abspath(__file__))
_TOOLS = os.path.dirname(_THIS)
sys.path.insert(0, _TOOLS)

from _workflow_utils import render_dag  # noqa: E402


def _diff(a: Any, b: Any, path: str = "$") -> list[str]:
    """Deep-diff two JSON-like structures. Returns a list of `path: msg` strings."""
    out: list[str] = []
    if type(a) is not type(b):
        out.append(f"{path}: type mismatch {type(a).__name__} != {type(b).__name__} | {a!r} vs {b!r}")
        return out
    if isinstance(a, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"{path}.{k}: only in builder | {b[k]!r}")
            elif k not in b:
                out.append(f"{path}.{k}: only in jinja | {a[k]!r}")
            else:
                out.extend(_diff(a[k], b[k], f"{path}.{k}"))
        return out
    if isinstance(a, list):
        if len(a) != len(b):
            out.append(f"{path}: list length {len(a)} != {len(b)}")
        for i, (x, y) in enumerate(zip(a, b)):
            out.extend(_diff(x, y, f"{path}[{i}]"))
        if len(a) > len(b):
            for i, x in enumerate(a[len(b):], start=len(b)):
                out.append(f"{path}[{i}]: only in jinja | {x!r}")
        elif len(b) > len(a):
            for i, x in enumerate(b[len(a):], start=len(a)):
                out.append(f"{path}[{i}]: only in builder | {x!r}")
        return out
    if a != b:
        out.append(f"{path}: {a!r} != {b!r}")
    return out


def assert_equivalent(name: str, jinja_template_path: str, jinja_args: dict,
                      builder_dict: dict) -> bool:
    """Render Jinja + compare to builder_dict. Print results, return True if equal."""
    rendered = render_dag(jinja_template_path, jinja_args)
    diffs = _diff(rendered, builder_dict)
    if not diffs:
        print(f"[OK]   {name}: jinja == builder")
        return True
    print(f"[FAIL] {name}: {len(diffs)} differences")
    for d in diffs[:50]:
        print(f"   {d}")
    if len(diffs) > 50:
        print(f"   ... and {len(diffs) - 50} more")
    return False


def _project_root() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(here, "..", "..", ".."))


def jinja_template(name: str) -> str:
    """Path to a Jinja template file in src/tools/jinja_templates/."""
    return os.path.join(_project_root(), "src", "tools", "jinja_templates", name)


# ---------- Test cases (registered per builder cutover) ----------


def run_all() -> int:
    failures = 0

    # ----- datagen_workflow_digen ----- (already cut over; Jinja removed)

    # ----- datagen_workflow_digen / datagen_workflow ----- already cut over

    # ----- dlt_pipeline ----- already cut over

    print()
    if failures:
        print(f"FAILED: {failures} case(s) mismatched")
    else:
        print("ALL CASES PASS")
    return failures


if __name__ == "__main__":
    sys.exit(run_all())
