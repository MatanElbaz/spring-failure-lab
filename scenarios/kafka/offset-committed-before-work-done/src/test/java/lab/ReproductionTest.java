package lab;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import lab.common.LabApp;
import lab.common.WorkLog;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Scenario: the listener hands each record to a thread pool and returns. A graceful shutdown then loses the
 * records still waiting in the pool's queue, because their offsets were already committed.
 *
 * Both tests use identical conditions: 200 records in one partition, 50 ms of work per record, a pool of 4 threads,
 * and a shutdown requested as soon as exactly 4 records have started. They differ only in the listener
 * (profile "broken" or "fixed"). A replacement consumer in the same group then runs until it is idle, and the test
 * counts which of the 200 records were processed by either consumer.
 */
@Testcontainers
class ReproductionTest {

    static final String TOPIC = "orders";
    static final String GROUP = "orders-consumer-group";
    static final int PRODUCED = 200;
    static final int GATE = 4;   // shutdown is requested as soon as exactly this many records have STARTED
    static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    static final Path SIGNATURE_FILE =
            Path.of(System.getProperty("basedir", "."), "expected-signature.txt");

    /** One Kafka container (KRaft) shared by every test in this class. */
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.1");

    record Outcome(int processed, int lost, int duplicates, long committedAfterShutdown, int droppedInQueue,
                   List<String> keyLines) {
    }

    // ------------------------------------------------------------------ the two tests

    @Test
    void broken_reproduces_the_failure() throws Exception {
        Outcome outcome = run("broken");

        writeSignature(outcome.keyLines());
        assertThat(outcome.lost()).as("records lost").isGreaterThan(0);
        assertThat(outcome.committedAfterShutdown()).as("committed offset after shutdown").isEqualTo(PRODUCED);
    }

    @Test
    void fixed_survives_the_same_conditions() throws Exception {
        Outcome outcome = run("fixed");

        assertThat(outcome.lost()).as("records lost").isZero();
        // Duplicates are expected here: records that were in flight at shutdown are redelivered.
    }

    // ------------------------------------------------------------------ the scenario

    Outcome run(String variant) throws Exception {
        long t0 = System.nanoTime();
        List<String> keyLines = new ArrayList<>();
        String bootstrap = KAFKA.getBootstrapServers();

        try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            resetTopic(admin);
            log(t0, "kafka ready, topic '%s' (1 partition) created", TOPIC);
            produce(bootstrap);
            log(t0, "produced %d records (ids 0..%d)", PRODUCED, PRODUCED - 1);

            // ---- consumer #1: the instance that is shut down
            ConfigurableApplicationContext ctx1 = start(variant, bootstrap);
            WorkLog work1 = ctx1.getBean(WorkLog.class);
            ThreadPoolExecutor pool1 = (ThreadPoolExecutor) ctx1.getBean(ExecutorService.class);
            log(t0, "consumer #1 started (group %s, variant=%s)", GROUP, variant);

            if (!work1.startedGate().await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("the first " + GATE + " records never started processing");
            }
            log(t0, "first %d records started processing", GATE);

            log(t0, "graceful shutdown requested: stopping the listener container ...");
            MessageListenerContainer container = ctx1.getBean(KafkaListenerEndpointRegistry.class)
                    .getListenerContainer("orders-listener");
            container.stop();                                  // blocks until the consumer thread has finished
            List<Runnable> dropped = pool1.shutdownNow();      // queued work is dropped, running tasks interrupted
            pool1.awaitTermination(10, TimeUnit.SECONDS);
            ctx1.close();
            // The commit happens per polled batch on the consumer thread, so read it only after the stop.
            long committedAfter = committed(admin);
            Set<Integer> processed1 = new HashSet<>(work1.processed());
            int executions1 = work1.totalExecutions();
            log(t0, keyLines, "consumer #1 down | processed=%d | dropped-in-queue=%d | interrupted-in-flight=%d"
                    + " | committed offset=%s", processed1.size(), dropped.size(), work1.interrupted(),
                    show(committedAfter));

            // ---- replacement consumer: a new instance in the same group
            ConfigurableApplicationContext ctx2 = start(variant, bootstrap);
            WorkLog work2 = ctx2.getBean(WorkLog.class);
            ThreadPoolExecutor pool2 = (ThreadPoolExecutor) ctx2.getBean(ExecutorService.class);
            log(t0, "replacement consumer started (same group), waiting until it is idle ...");
            awaitIdle(admin, pool2);
            Set<Integer> processed2 = new HashSet<>(work2.processed());
            int executions2 = work2.totalExecutions();
            log(t0, keyLines, "replacement idle | processed by replacement=%d | committed offset=%s",
                    processed2.size(), show(committed(admin)));
            ctx2.close();

            Set<Integer> all = new HashSet<>(processed1);
            all.addAll(processed2);
            int lost = PRODUCED - all.size();
            int duplicates = executions1 + executions2 - all.size();
            log(t0, keyLines, "SUMMARY variant=%s | produced=%d | committed offset after shutdown=%s | processed=%d"
                    + " | dropped-in-queue=%d | lost=%d | duplicates=%d", variant, PRODUCED, show(committedAfter),
                    all.size(), dropped.size(), lost, duplicates);
            return new Outcome(all.size(), lost, duplicates, committedAfter, dropped.size(), keyLines);
        }
    }

    ConfigurableApplicationContext start(String variant, String bootstrap) {
        return new SpringApplicationBuilder(LabApp.class)
                .web(WebApplicationType.NONE)
                .profiles(variant)                       // "broken" or "fixed": picks the listener and its config
                .properties(
                        "logging.level.root=WARN",
                        "logging.level.org.apache.kafka=ERROR",
                        "lab.gate-after-started=" + GATE,
                        "spring.kafka.bootstrap-servers=" + bootstrap)
                .run();
    }

    // ------------------------------------------------------------------ helpers

    /**
     * The replacement is idle when the group's committed offset covers all records, the pool has nothing queued or
     * running, and that has held for 2 seconds. "No new records for a while" would not be safe: joining the group
     * can take longer than that.
     */
    static void awaitIdle(AdminClient admin, ThreadPoolExecutor pool) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        long idleSince = -1;
        while (System.nanoTime() < deadline) {
            boolean idle = committed(admin) >= PRODUCED && pool.getQueue().isEmpty() && pool.getActiveCount() == 0;
            if (idle) {
                if (idleSince < 0) {
                    idleSince = System.nanoTime();
                }
                if (System.nanoTime() - idleSince > Duration.ofSeconds(2).toNanos()) {
                    return;
                }
            }
            else {
                idleSince = -1;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("the replacement consumer never became idle; committed offset=" + committed(admin));
    }

    static long committed(AdminClient admin) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(GROUP)
                .partitionsToOffsetAndMetadata().get();
        OffsetAndMetadata offset = offsets.get(PARTITION);
        return offset == null ? -1 : offset.offset();
    }

    static String show(long offset) {
        return offset < 0 ? "none" : String.valueOf(offset);
    }

    /** The topic and group are reset before each run so that the tests do not depend on their order. */
    static void resetTopic(AdminClient admin) throws Exception {
        if (admin.listTopics().names().get().contains(TOPIC)) {
            try {
                admin.deleteConsumerGroups(List.of(GROUP)).all().get();
            }
            catch (Exception ignored) {
                // the group may not exist yet, or may still be shutting down; the topic reset below is what matters
            }
            admin.deleteTopics(List.of(TOPIC)).all().get();
            while (admin.listTopics().names().get().contains(TOPIC)) {
                Thread.sleep(100);
            }
        }
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
    }

    static void produce(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < PRODUCED; i++) {
                producer.send(new ProducerRecord<>(TOPIC, 0, null, String.valueOf(i)));
            }
            producer.flush();
        }
    }

    /** Prints a timeline line with the elapsed milliseconds since the start of the run. */
    static void log(long t0, String format, Object... args) {
        System.out.printf("[%5d ms] %s%n", (System.nanoTime() - t0) / 1_000_000, String.format(format, args));
    }

    /** Same, and also remembers the line (without the timestamp) as a key line for expected-signature.txt. */
    static void log(long t0, List<String> keyLines, String format, Object... args) {
        log(t0, format, args);
        keyLines.add(String.format(format, args));
    }

    static void writeSignature(List<String> keyLines) throws IOException {
        Files.writeString(SIGNATURE_FILE, String.join("\n", keyLines) + "\n", StandardCharsets.UTF_8);
    }
}
