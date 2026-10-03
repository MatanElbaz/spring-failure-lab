# spring-failure-lab

[![CI](https://github.com/MatanElbaz/spring-failure-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/MatanElbaz/spring-failure-lab/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Reproduce real backend failures on Spring Boot, Kafka and PostgreSQL with one command. Each scenario ships the symptom, the root cause, the fix, and a test that proves it.

**Status: early.** One scenario at first, more planned. Only scenarios that exist and pass are listed.

## Run a scenario

Each scenario is a self-contained Maven project under `scenarios/<area>/<name>/`.

```
git clone https://github.com/MatanElbaz/spring-failure-lab.git
cd spring-failure-lab/scenarios/<area>/<name>
make break
```

`make break` runs the failing implementation and prints a timestamped timeline, so you can watch the failure happen. `make fix` runs the corrected implementation under the same conditions. `make repeat` runs both several times to check that the result is stable.

## Scenarios

The first scenario is being added: kafka/offset-committed-before-work-done

## Requirements

- JDK 21
- A running Docker daemon (scenarios start Kafka and PostgreSQL with Testcontainers)
- `make`

Python 3 is needed only to run the repository's own checks.

## Why this exists

Similar projects exist. Several small repositories already demonstrate individual Kafka or Spring failures, and there are long-standing collections of incident write-ups and chaos-engineering tools. This project does not claim a new idea.

What it tries to offer is a consistent shape across many scenarios:

- one format for every scenario: symptom, log signature, root cause, fix, sources, and what the test does not prove
- documentation in English
- a permissive license (Apache-2.0)
- a test per scenario that passes in both directions: the failure appears in `broken` and does not appear in `fixed`
- current dependency versions, with scenarios checked in CI

Whether that is useful to anyone is not yet known.

## How scenarios are checked

- A scenario is accepted only if it reproduces in 10 of 10 runs in both directions.
- Every log line quoted in a scenario README must appear in the `expected-signature.txt` file produced by the test run. A linter (`scripts/check_scenarios.py`) enforces this, along with the required README sections and metadata.
- Claims cite primary sources: official documentation, source code, or a public postmortem that states the mechanism.
- Each README ends with a section on what the test does not prove.

## Contributing

Scenario proposals and corrections are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the rules, or open a "Propose a scenario" issue.

## License

[Apache-2.0](LICENSE).
