package com.xxliuxx.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsconsumer.entity.LibraryEvent;
import com.xxliuxx.libraryeventsconsumer.mapper.LibraryEventsRepository;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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

  public void processLibraryEvents(ConsumerRecord<Integer, String> consumerRecord)
      throws JsonProcessingException {
    LibraryEvent libraryEvent = this.objectMapper.readValue(consumerRecord.value(),
        LibraryEvent.class);

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
      throw new IllegalArgumentException("Library Event is missing");
    }

    Optional<LibraryEvent> libraryEventOptional = this.libraryEventRepository.findById(
        libraryEvent.getLibraryEventId());

    if (!libraryEventOptional.isPresent()) {
      throw new IllegalArgumentException("Library Event is invalid");
    }

    log.info("Library Event has been validated successfully");
  }


}
