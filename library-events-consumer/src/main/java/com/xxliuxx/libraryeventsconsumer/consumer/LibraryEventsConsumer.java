package com.xxliuxx.libraryeventsconsumer.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xxliuxx.libraryeventsconsumer.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * @author Yuchen Liu
 */
@Component
@Slf4j
public class LibraryEventsConsumer {

  @Autowired
  private LibraryEventsService libraryEventsService;

  @RetryableTopic(attempts = "5", backoff = @Backoff(500L))
  @KafkaListener(topics = {"library-events"})
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord)
      throws JsonProcessingException {
    log.info("Consumer record: {}", consumerRecord);
    this.libraryEventsService.processLibraryEvents(consumerRecord);
  }
}
