## What this changes

<!-- One or two sentences. For a new scenario, give its id, for example kafka/example-name. -->

## Checklist

For a new or changed scenario:

- [ ] The scenario is one self-contained Maven module under `scenarios/<area>/<name>/`.
- [ ] `make break` reproduces the failure and `make fix` shows the fix, and both pass.
- [ ] I ran `make repeat` and it reported all runs passing in both directions (the determinism rule: 10 of 10).
- [ ] `ReproductionTest` has the two tests `broken_reproduces_the_failure` and `fixed_survives_the_same_conditions`, and both print a narrated timeline.
- [ ] Every line quoted in the README `## Signature` section appears in `expected-signature.txt`, which the test run produced, and quotes no counts that change between runs.
- [ ] Every non-trivial claim in the README cites a primary source listed in `scenario.yaml`. Quotes are short and attributed.
- [ ] The README has `## What this does not prove` and it is honest about the limits of the test.
- [ ] `reproduced` in `scenario.yaml` was filled in by a human who ran the scenario.
- [ ] `python3 scripts/check_scenarios.py` passes.
- [ ] No code, data, names or incident details from an employer or any non-public source.

For any change:

- [ ] `python3 -m unittest discover -s tests` passes.
