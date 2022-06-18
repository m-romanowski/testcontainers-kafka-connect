package dev.marcinromanowski.testcontainers

import groovy.transform.CompileStatic
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

@CompileStatic
class Kafka {

    static final int DEFAULT_PORT = 9092
    static final String DEFAULT_SERVICE_NAME = "quickstart-tests-kafka"

    static String getNetworkBootstrapServers() {
        return "$DEFAULT_SERVICE_NAME:$DEFAULT_PORT"
    }

    static String startContainer(Network network) {
        def container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.4"))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withNetwork(network)
                .withNetworkAliases(DEFAULT_SERVICE_NAME)
                .waitingFor(Wait.forListeningPort())
        container.start()
        return container.getBootstrapServers()
    }

}
