package com.xxliuxx.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Yuchen Liu
 */

@Component
@Slf4j
public class LibraryEventProducer {

  private static final String TOPIC = "library-events";

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  private ObjectMapper mapper;

  public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
    // get the key and value of the message
    Integer key = libraryEvent.getLibraryEventId();
    String value = this.mapper.writeValueAsString(libraryEvent);

    // using sendDefault
    ListenableFuture<SendResult<Integer, String>> listenableFuture = this.kafkaTemplate.sendDefault(
        key,
        value);

    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }

      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, value, ex);
      }
    });
  }

  public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
      throws JsonProcessingException {
    Integer key = libraryEvent.getLibraryEventId();
    String value = this.mapper.writeValueAsString(libraryEvent);
    SendResult<Integer, String> sendResult = null;

    try {
      sendResult = this.kafkaTemplate.sendDefault(key, value)
          .get();
    } catch (ExecutionException | InterruptedException e) {
      log.error(
          "ExecutionException/InterruptedException fails to send message and the exception is {}",
          e.getMessage());
    } catch (Exception e) {
      log.error("Exception fails to send message and the exception is {}", e.getMessage());
    }

    return sendResult;
  }

  public void sendLibraryEventWithHeader(LibraryEvent libraryEvent)
      throws JsonProcessingException {
    // get the key and value of the message
    Integer key = libraryEvent.getLibraryEventId();
    String value = this.mapper.writeValueAsString(libraryEvent);

    // using sendDefault
    ProducerRecord<Integer, String> producerRecord = buildProducerRecord(TOPIC, key, value);
    ListenableFuture<SendResult<Integer, String>> listenableFuture = this.kafkaTemplate.send(
        producerRecord);

    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, value, result);
      }

      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, value, ex);
      }
    });
  }

  private ProducerRecord<Integer, String> buildProducerRecord(String topic, Integer key,
      String value) {
    List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));

    return new ProducerRecord<>(topic, null, key, value, headers);
  }

  private void handleFailure(Integer key, String value, Throwable ex) {
    log.error("Fail to send message and the exception is {}", ex.getMessage());

    try {
      throw ex;
    } catch (Throwable e) {
      log.error("Error in onFailure: {}", e.getMessage());
    }
  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
    log.info("Message sent successfully for key: {}, value is {}, partition is {}", key, value,
        result.getRecordMetadata());
  }
}
