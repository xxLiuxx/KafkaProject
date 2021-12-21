package com.xxliuxx.libraryeventsproducer.controller;

import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
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

  @PostMapping("/v1/libraryEvent")
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) {

    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }
}
