package dev.marcinromanowski.testcontainers

import groovy.transform.CompileStatic
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

@CompileStatic
class KafkaConnect {

    static void startContainer(String bootstrapServers, Network network, List<KafkaConnectContainer.ConnectorConfiguration> connectors) {
        def container = new KafkaConnectContainer(DockerImageName.parse("confluentinc/cp-kafka-connect:6.1.4"), bootstrapServers)
                .withPlugin("kafka-connect-plugins/mongodb-kafka-connect-mongodb-1.7.0")
                .withNetwork(network)
                .withNetworkAliases(KafkaConnectContainer.DEFAULT_SERVICE_NAME)
        container.start()
        connectors.forEach { container.withConnector(it) }
    }

}
