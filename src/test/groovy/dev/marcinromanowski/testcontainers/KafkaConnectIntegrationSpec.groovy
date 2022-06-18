package dev.marcinromanowski.testcontainers

import dev.marcinromanowski.testutils.JsonPathConfiguration
import dev.marcinromanowski.testutils.MockConsumer
import dev.marcinromanowski.testutils.MockConsumers
import org.testcontainers.containers.Network
import spock.lang.Shared
import spock.lang.Specification

import static dev.marcinromanowski.testutils.PredefinedPollingConditions.WAIT
import static org.mockito.ArgumentMatchers.argThat
import static org.mockito.Mockito.verify

class KafkaConnectIntegrationSpec extends Specification {

    private static final String ARTICLES_COLLECTION = "articles"
    private static final String ARTICLES_SINK_TOPIC = ARTICLES_COLLECTION
    private static final String ARTICLES_SOURCE_TOPIC = "$MongoDB.DEFAULT_DATABASE.$ARTICLES_COLLECTION" // MongoDB <database>.<collection>
    private static String kafkaBootstrapServers = null, mongoDBUri = null

    static {
        Network.newNetwork().with {
            kafkaBootstrapServers = Kafka.startContainer(it)
            mongoDBUri = MongoDB.startContainer(it)
            KafkaConnect.startContainer(Kafka.getNetworkBootstrapServers(), it, [
                    articlesSinkConnectorConfiguration(MongoDB.getNetworkBootstrapServerUri()),
                    articlesSourceConnectorConfiguration(MongoDB.getNetworkBootstrapServerUri())
            ])
        }
    }

    @Shared
    private ArticlesRepository articlesRepository
    @Shared
    private KafkaPublisher kafkaPublisher
    @Shared
    private KafkaEventListener kafkaEventListener
    private MockConsumer articlesTopicConsumer

    def setupSpec() {
        articlesRepository = new ArticlesMongoRepository(mongoDBUri, MongoDB.DEFAULT_DATABASE, ARTICLES_COLLECTION)
        kafkaPublisher = new KafkaPublisher(ARTICLES_SINK_TOPIC, kafkaBootstrapServers)
        kafkaEventListener = new KafkaEventListener(kafkaBootstrapServers, [ARTICLES_SINK_TOPIC, ARTICLES_SOURCE_TOPIC])
        kafkaEventListener.start()
    }

    def setup() {
        articlesTopicConsumer = MockConsumers.registerMockConsumer(ARTICLES_SOURCE_TOPIC)
    }

    def cleanupSpec() {
        kafkaEventListener.close()
    }

    def "Kafka connect sink acceptance test"() {
        when:
            def article = new ArticleEntity(UUID.randomUUID().toString(), "first article title", "first article description")
            kafkaPublisher.sendArticle(article)
        then:
            WAIT.eventually {
                def foundArticle = articlesRepository.findByTitle(article.title)
                assert foundArticle.isPresent()
                verifyAll(foundArticle.get()) {
                    it.title == article.title
                    it.description == article.description
                }
            }
    }

    def "Kafka connect source acceptance test"() {
        when:
            def article = new ArticleEntity(UUID.randomUUID().toString(), "second article title", "second article description")
            articlesRepository.save(article)
        then:
            WAIT.eventually {
                verify(articlesTopicConsumer).consumed(argThat { String event ->
                    def parsedEvent = JsonPathConfiguration.parser().parse(event)
                    return parsedEvent.read('$.title') == article.title
                            && parsedEvent.read('$.description') == article.description
                })
            }
    }

    private static KafkaConnectContainer.ConnectorConfiguration articlesSinkConnectorConfiguration(String connectionUri) {
        def connectorName = "mongo-sink-articles"
        def configuration = [
                "name"           : connectorName,
                "tasks.max"      : "1",
                "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
                "connection.uri" : connectionUri,
                "database"       : MongoDB.DEFAULT_DATABASE,
                "collection"     : ARTICLES_COLLECTION,
                "topics"         : ARTICLES_SINK_TOPIC,
                "key.converter"  : "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.storage.StringConverter"
        ]
        return new KafkaConnectContainer.ConnectorConfiguration(connectorName, configuration)
    }

    private static KafkaConnectContainer.ConnectorConfiguration articlesSourceConnectorConfiguration(String connectionUri) {
        def connectorName = "mongo-source-articles"
        def configuration = [
                "name"                      : connectorName,
                "tasks.max"                 : "1",
                "connector.class"           : "com.mongodb.kafka.connect.MongoSourceConnector",
                "connection.uri"            : connectionUri,
                "database"                  : MongoDB.DEFAULT_DATABASE,
                "collection"                : ARTICLES_COLLECTION,
                "topics"                    : ARTICLES_SINK_TOPIC,
                "publish.full.document.only": "true",
                "key.converter"             : "org.apache.kafka.connect.storage.StringConverter",
                "value.converter"           : "org.apache.kafka.connect.storage.StringConverter",
                "output.json.formatter"     : "com.mongodb.kafka.connect.source.json.formatter.ExtendedJson"
        ]
        return new KafkaConnectContainer.ConnectorConfiguration(connectorName, configuration)
    }

}
