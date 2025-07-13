package com.cicd.beginner.config;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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

  @Value("${debezium.table.include.list}")
  private String tableIncludeList;

  private EmbeddedEngine engine;

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

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
    props.setProperty("table.include.list", tableIncludeList);

    return Configuration.from(props);
  }

  @Bean
  public EmbeddedEngine embeddedEngine() {
    engine = EmbeddedEngine.create()
            .using(db2Connector())
            .notifying(this::handleEvent)
            .build();

    // Start the engine
    new Thread(engine::run).start();
    return engine;
  }

  private void handleEvent(SourceRecord record) {
    System.out.println("Received CDC Event: " + record);
    if (record.value() != null) {
      kafkaTemplate.send("users-cdc-topic", record.value().toString());
      System.out.println("Sent to Kafka: " + record.value());
      Struct value = (Struct) record.value();
      String operation = value.getString("op"); // c=CREATE, u=UPDATE, d=DELETE
      Struct after = value.getStruct("after"); // New row data
      Struct before = value.getStruct("before"); // Old row data (for updates/deletes)

      System.out.println("Operation: " + operation);
      if (after != null) {
//        System.out.println("New Data: id=" + after.getInt32("id") +
//                ", first_name=" + after.getString("first_name") +
//                ", last_name=" + after.getString("last_name") +
//                ", email=" + after.getString("email"));
      }
      if (before != null) {
//        System.out.println("Old Data: id=" + before.getInt32("id") +
//                ", first_name=" + before.getString("first_name") +
//                ", last_name=" + before.getString("last_name") +
//                ", email=" + before.getString("email"));
      }
    }
  }

  @PreDestroy
  public void stop() {
    if (engine != null) {
      engine.stop();
    }
  }
}
