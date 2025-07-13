package com.cicd.beginner.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class UsersListenerConfig {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Bean
  public java.util.function.Consumer<ChangeEvent<String, String>> usersListener() {
    return (event) -> {
      try {
        log.debug(
            "Received CDC event from RabbitMQ - Key: {}, Value: {}", event.key(), event.value());
        JsonNode valueNode = objectMapper.readTree(event.value());
        String operation = valueNode.path("payload").path("op").asText();
        JsonNode after = valueNode.path("payload").path("after");
        JsonNode before = valueNode.path("payload").path("before");

        switch (operation) {
          case "c":
            log.info("Create Operation: {}", after);
            break;
          case "u":
            log.info("Update Operation - Before: {}, After: {}", before, after);
            break;
          case "d":
            log.info("Delete Operation: {}", before);
            break;
          default:
            log.warn("Unknown operation: {}", operation);
        }
      } catch (Exception e) {
        log.error("Error processing CDC event: {}", e.getMessage(), e);
      }
    };
  }
}
