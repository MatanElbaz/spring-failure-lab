package lab.broken;

import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import lab.common.WorkLog;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * BUG: hands the record to an executor and returns immediately.
 * With the default ack mode (BATCH) Spring treats the record as handled when this method returns,
 * and commits the offsets of the whole polled batch from the consumer thread, whether or not the pool has finished.
 */
@Component
@Profile("broken")
public class BrokenListener {

    private final ExecutorService workers;
    private final WorkLog work;

    BrokenListener(ExecutorService workers, WorkLog work) {
        this.workers = workers;
        this.work = work;
    }

    @KafkaListener(id = "orders-listener", topics = "${lab.topic}", groupId = "${lab.group}")
    public void onOrder(ConsumerRecord<String, String> record) {
        workers.submit(() -> work.process(record));   // returns before the work is done
    }
}
