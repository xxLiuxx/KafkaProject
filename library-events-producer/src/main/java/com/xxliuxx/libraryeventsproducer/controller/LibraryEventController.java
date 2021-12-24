package com.xxliuxx.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEventType;
import com.xxliuxx.libraryeventsproducer.producer.LibraryEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
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
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
      throws JsonProcessingException {

    // invoke kafka producer
    libraryEvent.setLibraryEventType(LibraryEventType.NEW);
    this.libraryEventproducer.sendLibraryEventWithHeader(libraryEvent);

    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }
}
