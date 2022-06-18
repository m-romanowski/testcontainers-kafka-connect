package dev.marcinromanowski.testcontainers

import dev.marcinromanowski.testutils.MockConsumers
import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

@Slf4j
@CompileStatic
@SuppressWarnings("unused")
class KafkaEventListener implements Closeable {

    private static final Duration POLL_DURATION = Duration.ofMillis(100)

    private final KafkaConsumer<String, String> consumer
    private final ExecutorService executorService
    private final AtomicBoolean started
    private final List<String> topics

    KafkaEventListener(String bootstrapServers, List<String> topics) {
        this.consumer = new KafkaConsumer<>(prepareProperties(bootstrapServers))
        this.executorService = Executors.newSingleThreadExecutor()
        this.started = new AtomicBoolean()
        this.topics = new ArrayList<>(topics)
    }

    private static Properties prepareProperties(String bootstrapServers) {
        Properties props = new Properties()
        props.setProperty("bootstrap.servers", bootstrapServers)
        props.setProperty("group.id", "test")
        props.setProperty("enable.auto.commit", "true")
        props.setProperty("auto.commit.interval.ms", "1000")
        props.setProperty("auto.offset.reset", "earliest")
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        return props
    }

    void start() {
        if (started.get()) {
            throw new IllegalStateException("Kafka listener is already started")
        }

        started.compareAndSet(false, true)
        executorService.execute(() -> {
            consumer.subscribe(topics)
            try {
                while (started.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION)
                    for (ConsumerRecord<String, String> record : records) {
                        def topic = record.topic()
                        def event = record.value()
                        log.info("Handled event in Kafka listener. Topic: $topic. Event: ${System.lineSeparator()} ${JsonOutput.prettyPrint(event)}")
                        MockConsumers.getMockConsumers(topic).each {
                            it.consumed(event)
                        }
                    }
                }
            } catch (WakeupException e) {
                if (!started.get()) {
                    throw e
                }
            } finally {
                consumer.close()
            }
        })
    }

    @Override
    void close() throws IOException {
        if (!started.get()) {
            throw new IllegalStateException("Kafka listener is already closed")
        }

        started.compareAndSet(true, false)
        consumer.wakeup()
        executorService.shutdown()
    }

}
