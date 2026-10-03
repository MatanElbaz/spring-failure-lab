#!/usr/bin/env python3
"""Check the structure and provenance of every scenario in the repository.

A scenario is any directory scenarios/<area>/<name>/ that contains scenario.yaml.
Standard library only: scenario.yaml keys are checked with line-based matching.

Usage: python3 scripts/check_scenarios.py [repo-root]
Exit status is 0 when clean and 1 when any error is printed (one per line).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import List

REQUIRED_HEADINGS = (
    "## Symptom",
    "## Signature",
    "## Root cause",
    "## Fix",
    "## Sources",
    "## What this does not prove",
)
REQUIRED_KEYS = (
    "id",
    "area",
    "symptom",
    "signature_logs",
    "versions_tested",
    "sources",
    "reproduced",
)
PLACEHOLDERS = ("TODO", "TBD", "FIXME")
REQUIRED_TARGETS = ("break", "fix", "repeat")


def find_scenarios(root: Path) -> List[Path]:
    return sorted(p.parent for p in (root / "scenarios").glob("*/*/scenario.yaml"))


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def heading_lines(text: str) -> List[str]:
    return [line.rstrip() for line in text.splitlines()]


def signature_quotes(readme: str) -> List[str]:
    """Non-empty lines inside fenced blocks of the '## Signature' section."""
    quotes: List[str] = []
    in_section = False
    in_fence = False
    for line in readme.splitlines():
        stripped = line.strip()
        if not in_fence and line.startswith("## "):
            in_section = line.rstrip() == "## Signature"
            continue
        if not in_section:
            continue
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence and stripped:
            quotes.append(stripped)
    return quotes


def makefile_targets(text: str) -> set:
    targets = set()
    for line in text.splitlines():
        match = re.match(r"^([^\s#.=:][^:=]*?)\s*:(?!=)", line)
        if match:
            targets.update(match.group(1).split())
    return targets


def check_scenario(root: Path, directory: Path) -> List[str]:
    label = directory.relative_to(root).as_posix()
    errors: List[str] = []

    def err(message: str) -> None:
        errors.append("%s: %s" % (label, message))

    readme_path = directory / "README.md"
    yaml_path = directory / "scenario.yaml"
    signature_path = directory / "expected-signature.txt"
    makefile_path = directory / "Makefile"

    readme = read(readme_path) if readme_path.is_file() else None
    yaml_text = read(yaml_path)

    if readme is None:
        err("README.md not found")
    else:
        headings = set(heading_lines(readme))
        for heading in REQUIRED_HEADINGS:
            if heading not in headings:
                err("missing heading '%s'" % heading)

    for key in REQUIRED_KEYS:
        if not re.search(r"^%s\s*:" % re.escape(key), yaml_text, re.MULTILINE):
            err("scenario.yaml missing key '%s'" % key)

    signature_lines = None
    if signature_path.is_file():
        signature_lines = read(signature_path).splitlines()
    else:
        err("expected-signature.txt not found")

    if readme is not None and signature_lines is not None:
        for quote in signature_quotes(readme):
            if not any(quote in line for line in signature_lines):
                err("signature line not found in expected-signature.txt: %s" % quote)

    searched = yaml_text if readme is None else readme + "\n" + yaml_text
    for placeholder in PLACEHOLDERS:
        if re.search(r"\b%s\b" % placeholder, searched):
            err("contains placeholder '%s'" % placeholder)

    targets = makefile_targets(read(makefile_path)) if makefile_path.is_file() else set()
    for target in REQUIRED_TARGETS:
        if target not in targets:
            err("Makefile missing target '%s'" % target)

    return errors


def check(root: Path) -> List[str]:
    """Return a list of error strings; an empty list means every scenario is clean."""
    root = Path(root)
    errors: List[str] = []
    for directory in find_scenarios(root):
        errors.extend(check_scenario(root, directory))
    return errors


def main(argv: List[str]) -> int:
    root = Path(argv[1]) if len(argv) > 1 else Path(__file__).resolve().parent.parent
    errors = check(root)
    for error in errors:
        print(error)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
