package com.xxliuxx.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsconsumer.entity.LibraryEvent;
import com.xxliuxx.libraryeventsconsumer.mapper.LibraryEventsRepository;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * @author Yuchen Liu
 */
@Service
@Slf4j
public class LibraryEventsService {

  @Autowired
  private LibraryEventsRepository libraryEventRepository;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;


  public void processLibraryEvents(ConsumerRecord<Integer, String> consumerRecord)
      throws JsonProcessingException {
    LibraryEvent libraryEvent = this.objectMapper.readValue(consumerRecord.value(),
        LibraryEvent.class);

    // retry only when it's network issue
    if (libraryEvent.getLibraryEventId() == 000) {
      throw new RecoverableDataAccessException("Temporary Network Issue");
    }

    // if status is NEW, save the libraryEvent
    // if status is UPDATE, update the libraryEvent
    switch (libraryEvent.getLibraryEventType()) {
      case NEW:
        saveLibraryEvent(libraryEvent);
        break;
      case UPDATE:
        validateLibraryEvent(libraryEvent);
        saveLibraryEvent(libraryEvent);
        break;
      default:
        log.info("Invalid Library Event Type");
    }

  }

  private void saveLibraryEvent(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    this.libraryEventRepository.save(libraryEvent);
    log.info("LibraryEvent saved successfully");
  }

  private void validateLibraryEvent(LibraryEvent libraryEvent) {
    if (libraryEvent.getLibraryEventId() == null) {
      throw new IllegalArgumentException("Library Event Id is missing");
    }

    Optional<LibraryEvent> libraryEventOptional = this.libraryEventRepository.findById(
        libraryEvent.getLibraryEventId());

    if (!libraryEventOptional.isPresent()) {
      throw new IllegalArgumentException("Library Event is invalid");
    }

    log.info("Library Event has been validated successfully");
  }


  /**
   * handle the recovery, send the message back to the original topic
   */
  public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord) {
    Integer key = consumerRecord.key();
    String message = consumerRecord.value();
    ListenableFuture<SendResult<Integer, String>> listenableFuture = this.kafkaTemplate.sendDefault(
        key, message);
    listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
      @Override
      public void onSuccess(SendResult<Integer, String> result) {
        handleSuccess(key, message, result);
      }

      @Override
      public void onFailure(Throwable ex) {
        handleFailure(key, message, ex);
      }
    });
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
