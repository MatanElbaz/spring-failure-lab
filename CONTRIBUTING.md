# Contributing

Thank you for considering a contribution. This project is small and early, so the rules below are short and meant to keep every scenario trustworthy.

## What makes a good scenario

A scenario reproduces one named backend failure that an engineer on Spring Boot, Kafka or PostgreSQL could plausibly meet, shows the fix, and proves both with a test.

A scenario is accepted when it has:

1. **A public primary source** that states the mechanism: official documentation, a KIP, source code, vendor documentation, or a public postmortem. A postmortem is cited as the basis only where it states the mechanism itself; otherwise the scenario says "inspired by".
2. **A deterministic trigger.** Use explicit pauses, small timeouts, latches or a fault-injection proxy, not luck.
3. **A passing two-way test.** The failure appears in `broken` and never in `fixed`.

### The determinism rule

A scenario is merged only if it reproduces **10 of 10 runs**, in both directions. Run `make repeat` before opening a pull request. A scenario that later turns out to be flaky is moved out of the headline count until it is fixed.

## Structure of a scenario

One scenario is one self-contained Maven project at `scenarios/<area>/<name>/`. It does not depend on another scenario.

```
scenarios/<area>/<name>/
  README.md                 Symptom, Signature, Root cause, Fix, Sources, What this does not prove
  scenario.yaml             metadata
  expected-signature.txt    written by the test run
  Makefile                  targets: break, fix, test, repeat
  pom.xml
  src/main/java/...         broken, fixed and shared code
  src/test/java/...         ReproductionTest
```

`ReproductionTest` has two tests, `broken_reproduces_the_failure` and `fixed_survives_the_same_conditions`, which use the same trigger. Both print a narrated timeline (timestamps and counters) so a reader can observe the failure.

`make break` runs the first test only. `make fix` runs the second only. `make repeat` runs both several times and fails on the first failure.

### `scenario.yaml`

Required top-level keys: `id`, `area`, `symptom`, `signature_logs`, `versions_tested`, `sources`, `reproduced`.

- `reproduced` is filled in by a human who ran the scenario. Please do not fill it in on someone else's behalf.
- `sources` lists primary sources only.
- `versions_tested` records the versions the scenario was actually run with.

### README rules

- Every line quoted in `## Signature` must appear in `expected-signature.txt`. Do not edit that file by hand; it comes from the test run.
- Every non-trivial claim cites a source from `scenario.yaml`.
- Quote sources sparingly: at most one short quote (under 15 words) per source, with attribution. Prefer a paraphrase and a link.
- `## What this does not prove` must say honestly what the test does not show.
- No `TODO`, `TBD` or `FIXME` placeholders.
- Do not claim that a tool, including an AI tool, improves anything unless a measurement backs it.

## Provenance

- Do not contribute code, data, names, architecture or incident details that belong to an employer or to anyone who has not agreed to publish them. Scenarios come from public documentation and your own experiments.
- A scenario written or drafted with a language model is welcome only if a human has run it, read the sources, and can vouch for the mechanism. A passing test is required either way.

## Working on the repository

Requirements: JDK 21, a running Docker daemon, `make`, and Python 3 for the checks.

```
make lint    # check scenario structure and provenance
make test    # run the linter's own tests
```

CI runs both, then runs `make break` and `make fix` for every scenario.

## Proposing a scenario

Open an issue with the "Propose a scenario" form: the symptom, the stack versions, how to trigger it, a public source URL, and why it is not already covered. Discussing the idea first avoids wasted work.

To keep review manageable, please have at most one scenario pull request open at a time.

## Pull requests

Use the pull request template and tick the checklist honestly. Smaller changes (typos, source links, clearer wording, dependency updates) are welcome without a prior issue.

By contributing you agree that your contribution is licensed under the Apache License 2.0, as described in `LICENSE`.
