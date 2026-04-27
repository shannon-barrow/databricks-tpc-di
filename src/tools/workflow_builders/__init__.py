"""Python-native workflow JSON builders.

Each module in this package exposes a `build()` function that returns a
dict matching what its corresponding Jinja template would produce. The
goal is to replace the Jinja templating layer with Python conditionals
that are easier to reason about, refactor, and unit-test.

Cutover is one template at a time; each builder is paired with a diff
harness in `_diff_harness.py` that renders both the Jinja template and
the new builder for the same inputs and asserts deep equality.
"""
