package com.xxliuxx.libraryeventsconsumer.config;

import com.xxliuxx.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Yuchen Liu
 */

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

  @Autowired
  private LibraryEventsService libraryEventsService;

  @Bean
  public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ConsumerFactory<Object, Object> kafkaConsumerFactory) {
    ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(factory, kafkaConsumerFactory);

    // create 3 consumers
    factory.setConcurrency(3);

    // custom error handling
    factory.setErrorHandler(((thrownException, data) -> {
      log.info("Error in consumerConfig is {} and the record is {}", thrownException, data);
    }));

    // set recover
    factory.setRecoveryCallback(context -> {
      if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
        log.info("inside recoverable logic");
        ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context.getAttribute("record");
        this.libraryEventsService.handleRecovery(consumerRecord);
      } else {
        log.info("inside non-recoverable logic");
        throw new RuntimeException(context.getLastThrowable().getMessage());
      }

      return null;
    });
    return factory;
  }
}
