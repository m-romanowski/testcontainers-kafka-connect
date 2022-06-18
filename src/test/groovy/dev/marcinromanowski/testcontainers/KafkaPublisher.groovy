package dev.marcinromanowski.testcontainers

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import java.util.concurrent.TimeUnit

@Slf4j
@CompileStatic
class KafkaPublisher {

    private final String sourceTopic
    private final KafkaProducer<String, String> kafkaProducer

    KafkaPublisher(String sourceTopic, String bootStrapServers) {
        this.sourceTopic = sourceTopic
        this.kafkaProducer = new KafkaProducer<>(prepareProperties(bootStrapServers))
    }

    private static Properties prepareProperties(String bootstrapServers) {
        Properties props = new Properties()
        props.put("bootstrap.servers", bootstrapServers)
        props.put("acks", "all")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        return props
    }

    private static String articleEntityAsJson(ArticleEntity entity) {
        return """
            {
                "id": "$entity.id",
                "title": "$entity.title",
                "description": "$entity.description"
            }
        """
    }

    void sendArticle(ArticleEntity entity) {
        send(sourceTopic, entity.title, articleEntityAsJson(entity))
    }

    private void send(String topic, String key, String event) {
        log.info("Sending $key : $event to $topic")
        kafkaProducer.send(new ProducerRecord<>(topic, key, event)).get(10, TimeUnit.SECONDS)
    }

}
