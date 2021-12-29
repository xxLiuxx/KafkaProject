package com.xxliuxx.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEventType;
import com.xxliuxx.libraryeventsproducer.producer.LibraryEventProducer;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Yuchen Liu
 */

@RestController
public class LibraryEventController {

  @Autowired
  private LibraryEventProducer libraryEventproducer;

  @PostMapping("/v1/libraryEvent")
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
      throws JsonProcessingException {

    // invoke kafka producer
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    this.libraryEventproducer.sendLibraryEventWithHeader(libraryEvent);

    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PutMapping("/v1/libraryEvent")
  public ResponseEntity<?> putLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent)
      throws JsonProcessingException {

    // validity check
    if (libraryEvent.getLibraryEventId() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please provide the libraryEventId");
    }

    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    this.libraryEventproducer.sendLibraryEventWithHeader(libraryEvent);

    return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
  }
}
