# kafka/offset-committed-before-work-done

A Spring Kafka listener hands each record to a thread pool and returns. When the application shuts down gracefully, the records still waiting in the pool are never processed, and the new instance never sees them, because Spring had already committed their offsets.

## Run it

Requirements: JDK 21, Maven 3.9, a running Docker daemon (the test starts Kafka with Testcontainers; the first run pulls `apache/kafka:4.2.1`). The first run also downloads the Maven dependencies; because the Makefile runs Maven with `-q`, nothing is printed until the test starts, which can take a few minutes.

```
make break    # the failing implementation: prints a timeline, then the loss
make fix      # the corrected implementation under the same conditions
make repeat   # break then fix, 10 times; prints "reproduced 10/10" when every run behaves as described
```

`make repeat N=3` changes the number of rounds. `make break` also writes the key lines of its run to `expected-signature.txt`. This is a tracked file and the counts in it change between runs; `git checkout -- expected-signature.txt` undoes the change if you do not mean to commit it.

What `make break` prints (one real run; the counts vary by a few records between runs, see the end of the Signature section):

```
[  210 ms] kafka ready, topic 'orders' (1 partition) created
[  387 ms] produced 200 records (ids 0..199)
[  776 ms] consumer #1 started (group orders-consumer-group, variant=broken)
[  954 ms] first 4 records started processing
[  954 ms] graceful shutdown requested: stopping the listener container ...
[ 1508 ms] consumer #1 down | processed=36 | dropped-in-queue=160 | interrupted-in-flight=4 | committed offset=200
[ 1573 ms] replacement consumer started (same group), waiting until it is idle ...
[ 3754 ms] replacement idle | processed by replacement=0 | committed offset=200
[ 4141 ms] SUMMARY variant=broken | produced=200 | committed offset after shutdown=200 | processed=36 | dropped-in-queue=160 | lost=164 | duplicates=0
```

The test produces 200 records to a one-partition topic. Each record takes 50 ms of simulated work on a pool of 4 threads. Consumer #1 is shut down as soon as 4 records have started. A replacement consumer in the same group then runs until it is idle, and the test counts which of the 200 records were processed by either consumer.

## Symptom

- After a deploy, restart or scale-down, some records were never processed. No error is logged.
- Consumer lag for the group reads zero (the committed offset equals the end of the log), yet the work for some records was never done.
- The records are lost when an instance stops, so the problem can go unnoticed in a service that rarely restarts.

## Signature

The lines below appear in `expected-signature.txt` on every run of `make break`. They are quoted in part: the counts that change from run to run (`processed`, `dropped-in-queue`, `lost`) are left out; a full run is shown under "Run it" above. The offset after shutdown equals the number of records produced, while fewer records were processed, and the difference is lost.

```
replacement idle | processed by replacement=0 | committed offset=200
SUMMARY variant=broken | produced=200 | committed offset after shutdown=200
```

In eleven runs on 2026-10-03 (ten with `make repeat` and one review run) the broken variant processed 34 to 37 of the 200 records and lost 163 to 166; the committed offset after shutdown was 200 every time. The exact split varies because the four worker threads keep finishing records while the container stops.

## Root cause

The listener in `src/main/java/lab/broken/BrokenListener.java` submits the work to an executor and returns:

```java
@KafkaListener(id = "orders-listener", topics = "${lab.topic}", groupId = "${lab.group}")
public void onOrder(ConsumerRecord<String, String> record) {
    workers.submit(() -> work.process(record));   // returns before the work is done
}
```

1. With the default acknowledgment mode, `BATCH`, the container treats the records returned by one `poll()` as processed once the listener has returned for each of them, and then commits the offset. The reference documentation describes `BATCH` as committing the offset when all the records returned by `poll()` have been processed. Here "processed" means "the listener method returned", which happens immediately.
2. The commit is made per polled batch, by the consumer thread, after the batch has been handed to the listener. It is not tied to the moment each record's work finishes. In this test all 200 records arrive in one poll (`max.poll.records=500`), so the offset jumps to 200 as soon as the 200 hand-offs are done, while only a few records have been worked on.
3. A normal stop of the container lets the consumer thread finish the records from the previous poll before it stops (the `stopImmediate` property, off by default, changes that). So the whole batch is handed off and its offset, 200, is committed, even though the shutdown was requested after only four records had started.
4. The records that had been handed off but not yet started sit in the pool's in-memory queue (`dropped-in-queue`), and the four in flight are interrupted (`interrupted-in-flight`). In the test this is `shutdownNow()` on the pool, standing in for the process ending.
5. The replacement consumer joins the same group and starts at the committed offset, 200. It receives nothing. The records between the last finished one and offset 200 are lost, and Kafka has no record that anything is wrong.

Kafka's consumer documentation describes the same hazard for automatic commits: if a consumer does not finish processing the data returned by a poll before the next poll or before closing, the committed offset can get ahead of the consumed position, which results in missing records. Spring Kafka sets `enable.auto.commit` to `false` unless it is configured explicitly, so the mechanism here is the container's acknowledgment mode, not the Kafka client's auto-commit timer.

## Fix

Keep the hand-off to the pool, but acknowledge each record only after its work is done. `src/main/resources/application-fixed.properties`:

```properties
spring.kafka.listener.ack-mode=manual
spring.kafka.listener.async-acks=true
```

`src/main/java/lab/fixed/FixedListener.java`:

```java
@KafkaListener(id = "orders-listener", topics = "${lab.topic}", groupId = "${lab.group}")
public void onOrder(ConsumerRecord<String, String> record, Acknowledgment ack) {
    workers.submit(() -> {
        if (work.process(record)) {
            ack.acknowledge();   // only after the work is done
        }
    });
}
```

With `ack-mode=manual` the container commits only what the listener acknowledges. In the test the fixed variant loses nothing: in ten runs of `make repeat` on 2026-10-03 `lost` was 0 every time. This is what `make fix` printed in one of them (these lines are not part of `expected-signature.txt`):

```
consumer #1 down | processed=36 | dropped-in-queue=160 | interrupted-in-flight=4 | committed offset=none
replacement idle | processed by replacement=200 | committed offset=200
SUMMARY variant=fixed | produced=200 | committed offset after shutdown=none | processed=200 | dropped-in-queue=160 | lost=0 | duplicates=36
```

**Why `async-acks=true` is required.** Kafka stores one committed offset per partition, not one per record. The reference documentation says that with `MANUAL` acknowledgments must normally be made in order, and that `asyncAcks` allows them in any order: the container defers out-of-order commits until the missing acknowledgments arrive. A pool finishes records in no fixed order. If a later record is acknowledged while an earlier one is still running and `asyncAcks` is off, the commit can move past the unfinished record, which brings the original loss back for that record. This repository's test does not exercise the case with `async-acks=false`; the reason given is from the documentation. The documentation also notes that while out-of-order acknowledgments are pending, the consumer is paused until the offsets of the previous poll have been committed.

**The price: duplicates.** After the shutdown in the fixed run the committed offset was still `none`: the 36 records that consumer #1 had finished had not been committed by the time the container stopped, so the replacement received all 200 and processed those 36 a second time (`duplicates=36`; 35 or 36 in the ten runs mentioned above). Any record that is delivered but not yet acknowledged when a consumer stops is redelivered. The delivery guarantee becomes at-least-once, so the handler must be idempotent: for example an upsert keyed by a business id, a unique constraint that turns the second insert into a no-op, or a table of processed message ids checked inside the same transaction as the side effect.

**Alternative: do the work synchronously.** Remove the pool and process the record inside the listener. With the default `BATCH` mode the offset is then committed only after the batch has been processed, so the committed offset does not run ahead of the work. Two details matter on shutdown:

- `stop()` blocks for at most `shutdownTimeout`, 10 seconds by default. If the polled batch takes longer than that, the stop can return while the consumer thread is still in the middle of the batch.
- Setting `spring.kafka.listener.immediate-stop=true` makes the container stop after the current record instead of after the whole polled batch, and the remaining records are redelivered to the next consumer.

Keeping `max.poll.records` small also bounds how much work one batch holds. This synchronous variant is described here as an alternative and is not implemented or tested in this repository.

## Sources

- Spring for Apache Kafka reference, [Message Listener Containers](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/message-listener-container.html): the `AckMode` list (`BATCH` is the default, `MANUAL`), and the statement that the framework sets `enable.auto.commit` to `false` unless configured.
- Spring for Apache Kafka reference, [Manually Committing Offsets](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/ooo-commits.html): in-order acknowledgments and the `asyncAcks` property.
- Spring for Apache Kafka reference, [Container Properties](https://docs.spring.io/spring-kafka/reference/kafka/container-props.html): `ackMode`, `asyncAcks`, `shutdownTimeout` (10000 ms) and `stopImmediate`.
- Apache Kafka, [KafkaConsumer javadoc (4.2)](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html): the sections on automatic and manual offset control, including the warning that a committed offset can get ahead of the consumed position.

Pages accessed on 2026-10-03.

## What this does not prove

- **A real crash.** The test requests a graceful shutdown, stops the listener container, then calls `shutdownNow()` on the pool. It does not kill a process (`kill -9`, an out-of-memory kill, a node loss). What a crash loses depends on when the last commit happened relative to the crash, which this test does not measure.
- **What a real JVM does with its pool on SIGTERM.** `shutdownNow()` is a stand-in for the process ending. A pool configured to finish its queue before the process exits would lose fewer records, as long as the shutdown wait is long enough. That was not tested.
- **Plain Kafka client auto-commit.** The scenario is about the Spring listener container's acknowledgment mode. A plain `KafkaConsumer` with `enable.auto.commit=true` is not covered; the Kafka javadoc linked above describes that case.
- **The `async-acks=false` failure.** The need for `async-acks=true` comes from the documentation. The test shows only that the configuration in this repository loses nothing.
- **The synchronous alternative.** It is described from the documentation and not run here.
- **Scale and generality.** One broker, one partition, one consumer instance at a time, no rebalance, no transactions, no retries or error handlers. The 50 ms of work, the pool of 4 and the single 200-record batch are chosen to make the failure deterministic. "164 of 200 lost" is not a loss rate for a production service; the amount lost depends on the poll size, the queue and when the shutdown arrives, and with a small `max.poll.records` it would be smaller (not measured here).
- **Other versions.** Only the versions in `scenario.yaml` were run, on one laptop (timings in this README are from that machine).
