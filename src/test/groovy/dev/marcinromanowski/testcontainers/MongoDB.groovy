package dev.marcinromanowski.testcontainers

import groovy.transform.CompileStatic
import org.testcontainers.containers.MongoDBContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

@CompileStatic
class MongoDB {

    static final int DEFAULT_PORT = 27017
    static final String DEFAULT_DATABASE = "test"
    static final String DEFAULT_SERVICE_NAME = "quickstart-tests-mongo"

    static String getNetworkBootstrapServerUri() {
        return "mongodb://$DEFAULT_SERVICE_NAME:$DEFAULT_PORT/$DEFAULT_DATABASE"
    }

    static String startContainer(Network network) {
        def container = new MongoDBContainer(DockerImageName.parse("mongo:5.0.7"))
                .withNetwork(network)
                .withNetworkAliases(DEFAULT_SERVICE_NAME)
        container.start()
        return container.getReplicaSetUrl()
    }

}
