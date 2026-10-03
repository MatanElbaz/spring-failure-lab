# Scenario: kafka/offset-committed-before-work-done
# Requires JDK 21, Maven 3.9 and a running Docker daemon.

MVN ?= mvn
N ?= 10

.PHONY: break fix test repeat

# Run the failing implementation: records are lost on graceful shutdown. Prints a timeline and
# writes the key lines to expected-signature.txt.
break:
	$(MVN) -q test -Dtest='ReproductionTest#broken_reproduces_the_failure'

# Run the corrected implementation under the same conditions: nothing is lost.
fix:
	$(MVN) -q test -Dtest='ReproductionTest#fixed_survives_the_same_conditions'

# Run both tests.
test:
	$(MVN) -q test

# Run break then fix N times (default 10); stop at the first failure; print "reproduced N/N".
repeat:
	@mkdir -p target/repeat; ok=0; \
	for i in $$(seq 1 $(N)); do \
	  for target in break fix; do \
	    log=target/repeat/run-$$i-$$target.log; start=$$(date +%s); \
	    if ! $(MAKE) --no-print-directory $$target > $$log 2>&1; then \
	      cat $$log; echo "run $$i/$(N) $$target FAILED after $$ok/$(N) complete runs (log: $$log)"; exit 1; \
	    fi; \
	    echo "run $$i/$(N) $$target ok in $$(( $$(date +%s) - start )) s: $$(grep -h SUMMARY $$log | sed 's/^\[[^]]*\] //')"; \
	  done; \
	  ok=$$((ok + 1)); \
	done; \
	echo "reproduced $$ok/$(N)"
