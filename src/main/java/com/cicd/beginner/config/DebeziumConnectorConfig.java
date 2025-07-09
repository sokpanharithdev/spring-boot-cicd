package com.cicd.beginner.config;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class DebeziumConnectorConfig {

    @Value("${debezium.name}")
    private String connectorName;
    @Value("${debezium.topic.prefix}")
    private String connectorTopicPrefix;
    @Value("${debezium.connector.class}")
    private String connectorClass;
    @Value("${debezium.offset.storage}")
    private String offsetStorage;
    @Value("${debezium.offset.storage.file.filename}")
    private String offsetStorageFile;
    @Value("${debezium.offset.flush.interval.ms}")
    private String flushIntervalMs;
    @Value("${debezium.database.hostname}")
    private String databaseHostname;
    @Value("${debezium.database.port}")
    private String databasePort;
    @Value("${debezium.database.user}")
    private String databaseUser;
    @Value("${debezium.database.password}")
    private String databasePassword;
    @Value("${debezium.database.dbname}")
    private String databaseDbname;
    @Value("${debezium.database.server.name}")
    private String databaseServerName;
    @Value("${debezium.schema.history.internal}")
    private String schemaHistoryInternal;
    @Value("${debezium.schema.history.internal.file.filename}")
    private String schemaHistoryFile;
    @Value("${debezium.table.include.list}")
    private String tableIncludeList;

    private DebeziumEngine<io.debezium.engine.ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    @Autowired
    private StreamBridge streamBridge;

    public Configuration db2Connector() {
        Properties props = new Properties();
        props.setProperty("name", connectorName);
        props.setProperty("topic.prefix", connectorTopicPrefix);
        props.setProperty("connector.class", connectorClass);
        props.setProperty("offset.storage", offsetStorage);
        props.setProperty("offset.storage.file.filename", offsetStorageFile);
        props.setProperty("offset.flush.interval.ms", flushIntervalMs);
        props.setProperty("database.hostname", databaseHostname);
        props.setProperty("database.port", databasePort);
        props.setProperty("database.user", databaseUser);
        props.setProperty("database.password", databasePassword);
        props.setProperty("database.dbname", databaseDbname);
        props.setProperty("database.server.name", databaseServerName);
        props.setProperty("schema.history.internal", schemaHistoryInternal);
        props.setProperty("schema.history.internal.file.filename", schemaHistoryFile);
        props.setProperty("table.include.list", tableIncludeList);

        return Configuration.from(props);
    }

    @Bean
    public DebeziumEngine<io.debezium.engine.ChangeEvent<String, String>> embeddedEngine() {
        engine = DebeziumEngine.create(Json.class)
                .using(db2Connector().asProperties())
                .notifying(record -> {
                    try {
                        System.out.println("Sending change event to RabbitMQ: " + record);
                        streamBridge.send("debezium-out", "master-db.PUBLIC.USERS", record);
                    } catch (Exception e) {
                        System.err.println("Error sending change event to RabbitMQ: " + e.getMessage());
                    }
                })
                .build();

        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        return engine;
    }

    @Bean(destroyMethod = "close")
    public DebeziumEngine<io.debezium.engine.ChangeEvent<String, String>> shutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (engine != null) {
                try {
                    engine.close();
                } catch (Exception e) {
                    System.err.println("Error closing Debezium engine: " + e.getMessage());
                }
            }
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }));
        return engine;
    }
}
