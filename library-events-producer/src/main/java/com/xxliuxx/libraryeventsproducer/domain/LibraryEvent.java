package com.xxliuxx.libraryeventsproducer.domain;

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
  private Book book;
}
