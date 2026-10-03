.PHONY: lint test

# Check scenario structure and provenance.
lint:
	python3 scripts/check_scenarios.py

# Run the linter's own tests.
test:
	python3 -m unittest discover -s tests
