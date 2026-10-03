"""Tests for scripts/check_scenarios.py. Standard library only."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import check_scenarios  # noqa: E402

README_OK = """# Example scenario

## Symptom

Records go missing after a deploy.

## Signature

```
committed offset: 200
lost: 164
```

## Root cause

The offset is committed before the work is done.

## Fix

Acknowledge after the work.

## Sources

- https://example.org/docs

## What this does not prove

A real crash.
"""

YAML_OK = """id: kafka/example
area: kafka
symptom: ["records missing"]
signature_logs: ["committed offset"]
versions_tested: {spring-boot: "4.1.1"}
sources: [{title: "Docs", url: "https://example.org/docs"}]
reproduced: {by: "someone", date: "2026-10-03", status: confirmed}
"""

SIGNATURE_OK = "12:00:00.000 committed offset: 200\n12:00:00.001 lost: 164 of 200\n"

MAKEFILE_OK = """.PHONY: break fix repeat

break:
\techo break

fix:
\techo fix

repeat:
\techo repeat
"""


class ScenarioTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)

    def make_scenario(self, area="kafka", name="example", **overrides):
        """Create a valid scenario; overrides map file name -> text or None (omit)."""
        files = {
            "README.md": README_OK,
            "scenario.yaml": YAML_OK,
            "expected-signature.txt": SIGNATURE_OK,
            "Makefile": MAKEFILE_OK,
        }
        files.update(overrides)
        directory = self.root / "scenarios" / area / name
        directory.mkdir(parents=True)
        for file_name, text in files.items():
            if text is not None:
                (directory / file_name).write_text(text, encoding="utf-8")
        return "scenarios/%s/%s" % (area, name)


class CheckTests(ScenarioTestCase):
    def test_repo_without_scenarios_is_clean(self):
        self.assertEqual(check_scenarios.check(self.root), [])

    def test_valid_scenario_is_clean(self):
        self.make_scenario()
        self.assertEqual(check_scenarios.check(self.root), [])

    def test_directory_without_scenario_yaml_is_not_a_scenario(self):
        path = self.make_scenario(**{"scenario.yaml": None, "README.md": None})
        self.assertEqual(check_scenarios.check(self.root), [])
        self.assertTrue((self.root / path).is_dir())

    def test_missing_readme_heading(self):
        path = self.make_scenario(
            **{"README.md": README_OK.replace("## What this does not prove", "## Limits")}
        )
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: missing heading '## What this does not prove'" % path],
        )

    def test_missing_readme_file(self):
        path = self.make_scenario(**{"README.md": None})
        self.assertEqual(
            check_scenarios.check(self.root), ["%s: README.md not found" % path]
        )

    def test_missing_yaml_key(self):
        path = self.make_scenario(
            **{"scenario.yaml": YAML_OK.replace("reproduced:", "reproduced_by:")}
        )
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: scenario.yaml missing key 'reproduced'" % path],
        )

    def test_indented_key_does_not_count_as_top_level(self):
        yaml_text = YAML_OK.replace("area: kafka\n", "nested:\n  area: kafka\n")
        path = self.make_scenario(**{"scenario.yaml": yaml_text})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: scenario.yaml missing key 'area'" % path],
        )

    def test_missing_expected_signature_file(self):
        path = self.make_scenario(**{"expected-signature.txt": None})
        errors = check_scenarios.check(self.root)
        self.assertIn("%s: expected-signature.txt not found" % path, errors)

    def test_signature_line_not_in_expected_signature(self):
        readme = README_OK.replace("lost: 164", "lost: 999")
        path = self.make_scenario(**{"README.md": readme})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: signature line not found in expected-signature.txt: lost: 999" % path],
        )

    def test_signature_lines_outside_fences_are_ignored(self):
        readme = README_OK.replace(
            "## Signature\n", "## Signature\n\nThis prose is not quoted.\n"
        )
        self.make_scenario(**{"README.md": readme})
        self.assertEqual(check_scenarios.check(self.root), [])

    def test_fenced_blocks_outside_signature_section_are_ignored(self):
        readme = README_OK.replace(
            "Acknowledge after the work.\n", "```\nnot a signature line\n```\n"
        )
        self.make_scenario(**{"README.md": readme})
        self.assertEqual(check_scenarios.check(self.root), [])

    def test_placeholder_in_readme(self):
        readme = README_OK.replace("A real crash.", "TODO write this")
        path = self.make_scenario(**{"README.md": readme})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: contains placeholder 'TODO'" % path],
        )

    def test_placeholder_in_yaml(self):
        yaml_text = YAML_OK.replace("Docs", "TBD")
        path = self.make_scenario(**{"scenario.yaml": yaml_text})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: contains placeholder 'TBD'" % path],
        )

    def test_fixme_placeholder(self):
        readme = README_OK.replace("A real crash.", "FIXME later")
        path = self.make_scenario(**{"README.md": readme})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: contains placeholder 'FIXME'" % path],
        )

    def test_missing_makefile(self):
        path = self.make_scenario(**{"Makefile": None})
        self.assertEqual(
            check_scenarios.check(self.root),
            [
                "%s: Makefile missing target 'break'" % path,
                "%s: Makefile missing target 'fix'" % path,
                "%s: Makefile missing target 'repeat'" % path,
            ],
        )

    def test_makefile_missing_one_target(self):
        makefile = MAKEFILE_OK.replace("repeat:\n\techo repeat\n", "")
        path = self.make_scenario(**{"Makefile": makefile})
        self.assertEqual(
            check_scenarios.check(self.root),
            ["%s: Makefile missing target 'repeat'" % path],
        )

    def test_phony_line_alone_does_not_define_targets(self):
        path = self.make_scenario(**{"Makefile": ".PHONY: break fix repeat\n"})
        self.assertEqual(len(check_scenarios.check(self.root)), 3)
        self.assertTrue(path)

    def test_errors_are_reported_per_scenario_in_sorted_order(self):
        bad = self.make_scenario(name="b-bad", **{"Makefile": None})
        self.make_scenario(name="a-good")
        errors = check_scenarios.check(self.root)
        self.assertEqual(len(errors), 3)
        self.assertTrue(all(e.startswith(bad + ": ") for e in errors))


class CliTests(ScenarioTestCase):
    def run_cli(self):
        return subprocess.run(
            [sys.executable, str(REPO / "scripts" / "check_scenarios.py"), str(self.root)],
            capture_output=True,
            text=True,
        )

    def test_exit_zero_when_clean(self):
        self.make_scenario()
        result = self.run_cli()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout, "")

    def test_exit_one_and_one_error_per_line(self):
        path = self.make_scenario(**{"Makefile": None})
        result = self.run_cli()
        self.assertEqual(result.returncode, 1)
        lines = result.stdout.splitlines()
        self.assertEqual(len(lines), 3)
        self.assertIn("%s: Makefile missing target 'break'" % path, lines)


class RepositoryTests(unittest.TestCase):
    def test_this_repository_passes_its_own_linter(self):
        self.assertEqual(check_scenarios.check(REPO), [])


if __name__ == "__main__":
    unittest.main()
