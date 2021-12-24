package com.xxliuxx.libraryeventsproducer.domain;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Yuchen Liu
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class LibraryEvent {

  private Integer libraryEventId;
  private LibraryEventType libraryEventType;

  @NotNull(message = "Book is mandatory for LibraryEvent")
  @Valid
  private Book book;
}
