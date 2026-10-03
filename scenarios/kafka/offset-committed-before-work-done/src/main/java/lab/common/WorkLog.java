package lab.common;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Thread-safe record of what the "business logic" actually did, so the test can count lost and duplicated records. */
@Component
public class WorkLog {

    private final long workMillis;
    private final CountDownLatch startedGate;
    private final Set<Integer> processed = ConcurrentHashMap.newKeySet();
    private final Map<Integer, AtomicInteger> timesProcessed = new ConcurrentHashMap<>();
    private final AtomicInteger started = new AtomicInteger();
    private final AtomicInteger interrupted = new AtomicInteger();

    public WorkLog(@Value("${lab.work-millis}") long workMillis,
                   @Value("${lab.gate-after-started:4}") int gateAfterStarted) {
        this.workMillis = workMillis;
        this.startedGate = new CountDownLatch(gateAfterStarted);
    }

    /**
     * The simulated work: sleep, then record the record id as processed.
     * If the thread is interrupted (executor shutdownNow) the work does not count as done.
     *
     * @return true when the work completed
     */
    public boolean process(ConsumerRecord<String, String> record) {
        int id = Integer.parseInt(record.value());
        started.incrementAndGet();
        startedGate.countDown();
        try {
            Thread.sleep(workMillis);
        }
        catch (InterruptedException e) {
            interrupted.incrementAndGet();
            Thread.currentThread().interrupt();
            return false;
        }
        processed.add(id);
        timesProcessed.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
        return true;
    }

    /** Opens once the configured number of records has started processing. */
    public CountDownLatch startedGate() {
        return startedGate;
    }

    /** Distinct record ids whose work completed. */
    public Set<Integer> processed() {
        return processed;
    }

    public int started() {
        return started.get();
    }

    public int interrupted() {
        return interrupted.get();
    }

    /** Completed executions, counting a record processed twice as two. */
    public int totalExecutions() {
        return timesProcessed.values().stream().mapToInt(AtomicInteger::get).sum();
    }
}
