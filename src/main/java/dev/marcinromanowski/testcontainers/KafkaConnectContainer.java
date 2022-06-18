package dev.marcinromanowski.testcontainers;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

    static final String DEFAULT_SERVICE_NAME = "quickstart-tests-kafka-connect";
    static final String DEFAULT_PLUGINS_PATH = "/usr/share/java";
    static final int DEFAULT_EXPOSED_PORT = 28082;
    static final int DEFAULT_INTERNAL_TOPIC_REPLICATION_FACTOR = 1;
    static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.ofMinutes(1);

    private final ObjectMapper objectMapper;

    public KafkaConnectContainer(final DockerImageName dockerImageName, String bootstrapServers) {
        super(dockerImageName);
        this.objectMapper = getObjectMapper();

        withExposedPorts(DEFAULT_EXPOSED_PORT);
        withNetworkAliases(DEFAULT_SERVICE_NAME);

        // https://docs.confluent.io/platform/current/installation/docker/config-reference.html#kconnect-long-configuration
        withEnv("CONNECT_BOOTSTRAP_SERVERS", bootstrapServers);
        withEnv("CONNECT_GROUP_ID", DEFAULT_SERVICE_NAME);
        withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "quickstart-tests-kafka-connect-config");
        withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", String.valueOf(DEFAULT_INTERNAL_TOPIC_REPLICATION_FACTOR));
        withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "quickstart-tests-kafka-connect-offsets");
        withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", String.valueOf(DEFAULT_INTERNAL_TOPIC_REPLICATION_FACTOR));
        withEnv("CONNECT_STATUS_STORAGE_TOPIC", "quickstart-tests-kafka-connect-status");
        withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", String.valueOf(DEFAULT_INTERNAL_TOPIC_REPLICATION_FACTOR));
        withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
        withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", DEFAULT_SERVICE_NAME);
        withEnv("CONNECT_REST_PORT", String.valueOf(DEFAULT_EXPOSED_PORT));
        withEnv("CONNECT_PLUGIN_PATH", DEFAULT_PLUGINS_PATH);

        waitingFor(Wait.forHttp("/").withStartupTimeout(DEFAULT_STARTUP_TIMEOUT));
    }

    private static ObjectMapper getObjectMapper() {
        return new ObjectMapper()
            .registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES))
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.ANY)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true)
            .configure(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS, true)
            .configure(MapperFeature.AUTO_DETECT_FIELDS, true);
    }

    public KafkaConnectContainer withPlugin(String plugin) {
        MountableFile mountableFile = MountableFile.forClasspathResource(plugin);
        Path pluginsPath = Paths.get(mountableFile.getResolvedPath());
        File pluginsFile = pluginsPath.toFile();

        if (!pluginsFile.exists()) {
            throw new KafkaConnectContainerException("Resource with path %s couldn't be found".formatted(pluginsPath));
        }

        String containerPath = pluginsFile.isDirectory()
            ? DEFAULT_PLUGINS_PATH + "/" + pluginsPath.getFileName()
            : DEFAULT_PLUGINS_PATH + "/" + pluginsPath.getParent().getFileName() + "/" + pluginsPath.getFileName();

        addFileSystemBind(mountableFile.getResolvedPath(), containerPath, BindMode.READ_ONLY);
        return this;
    }

    /**
     * Should be used after container startup.
     */
    public KafkaConnectContainer withConnector(ConnectorConfiguration configuration) {
        HttpURLConnection connection;
        try {
            URL url = new URL(getUrl() + "/" + "connectors");
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accept", "application/json");
            connection.setDoOutput(true);
        } catch (IOException e) {
            throw new KafkaConnectContainerException("Cannot open connection to Kafka Connect", e);
        }

        try (OutputStream os = connection.getOutputStream()) {
            String serializedConfiguration = objectMapper.writeValueAsString(configuration);
            byte[] input = serializedConfiguration.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);

            int statusCode = connection.getResponseCode();
            if (statusCode != 201) {
                InputStream stream = connection.getErrorStream();
                String cause = IOUtils.toString(stream, "UTF-8");
                throw new KafkaConnectContainerException("Cannot create Kafka Connect connector. Got %s code. Cause %s".formatted(statusCode, cause));
            }
        } catch (IOException e) {
            throw new KafkaConnectContainerException("Unexpected error while creating Kafka Connect connector", e);
        }

        return this;
    }

    public String getUrl() {
        return "http://" + getHost() + ":" + getMappedPort(DEFAULT_EXPOSED_PORT);
    }

    @Override
    public void close() {
        stop();
    }

    public static class ConnectorConfiguration {

        String name;
        Map<String, Object> config;

        public ConnectorConfiguration(String name) {
            this.name = name;
            this.config = new HashMap<>();
        }

        public ConnectorConfiguration(String name, Map<String, Object> config) {
            this.name = name;
            this.config = config;
        }

        public ConnectorConfiguration add(String key, Object value) {
            config.put(key, value);
            return this;
        }

        public ConnectorConfiguration addAll(Map<String, Object> configuration) {
            config.putAll(configuration);
            return this;
        }

        public String getName() {
            return name;
        }

        public Map<String, Object> getConfig() {
            return config;
        }

    }

    public static class KafkaConnectContainerException extends RuntimeException {

        public KafkaConnectContainerException(String message) {
            super(message);
        }

        public KafkaConnectContainerException(String message, Throwable cause) {
            super(message, cause);
        }

    }

}
