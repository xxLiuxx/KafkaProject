package com.xxliuxx.libraryeventsproducer.controller;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsproducer.domain.Book;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
import com.xxliuxx.libraryeventsproducer.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

/**
 * @author Yuchen Liu
 */
@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @MockBean
  private LibraryEventProducer libraryEventProducer;

  @Test
  public void postLibraryEvent() throws Exception {
    // given
    Book book = Book.builder()
        .bookId(123)
        .bookName("Kafka")
        .bookAuthor("Udemy")
        .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    // expect
    String json = objectMapper.writeValueAsString(libraryEvent);
    when(libraryEventProducer.sendLibraryEventWithHeader(isA(LibraryEvent.class))).thenReturn(null);

    mockMvc.perform(post("/v1/libraryEvent").content(json).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().isCreated());

  }


  @Test
  public void postLibraryEvent_4xx() throws Exception {
    // given
    Book book = Book.builder()
        .bookId(null)
        .bookName(null)
        .bookAuthor("Udemy")
        .build();

    LibraryEvent libraryEvent = LibraryEvent.builder()
        .libraryEventId(null)
        .book(book)
        .build();

    // expect
    String json = objectMapper.writeValueAsString(libraryEvent);
    when(libraryEventProducer.sendLibraryEventWithHeader(isA(LibraryEvent.class))).thenReturn(null);

    mockMvc.perform(post("/v1/libraryEvent").content(json).contentType(MediaType.APPLICATION_JSON))
        .andExpect(status().is4xxClientError());

  }
}
