package lab.fixed;

import java.util.concurrent.ExecutorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import lab.common.WorkLog;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * FIX: still hands the record to the pool, but acknowledges its offset only after the work finished.
 * Needs spring.kafka.listener.ack-mode=manual and, because pool threads finish out of order,
 * spring.kafka.listener.async-acks=true (see application-fixed.properties).
 */
@Component
@Profile("fixed")
public class FixedListener {

    private final ExecutorService workers;
    private final WorkLog work;

    FixedListener(ExecutorService workers, WorkLog work) {
        this.workers = workers;
        this.work = work;
    }

    @KafkaListener(id = "orders-listener", topics = "${lab.topic}", groupId = "${lab.group}")
    public void onOrder(ConsumerRecord<String, String> record, Acknowledgment ack) {
        workers.submit(() -> {
            if (work.process(record)) {
                ack.acknowledge();   // only after the work is done
            }
        });
    }
}
