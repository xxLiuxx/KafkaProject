package com.xxliuxx.libraryeventsproducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * @author Yuchen Liu
 */

@Configuration
public class AutoCreateConfig {

  @Bean
  public NewTopic libraryEvent() {
    return TopicBuilder.name("library-events")
            .partitions(3)
            .replicas(3)
            .build();
  }
}
