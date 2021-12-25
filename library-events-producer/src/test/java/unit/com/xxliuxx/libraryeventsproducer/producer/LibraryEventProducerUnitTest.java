package com.xxliuxx.libraryeventsproducer.producer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsproducer.domain.Book;
import com.xxliuxx.libraryeventsproducer.domain.LibraryEvent;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;


/**
 * @author Yuchen Liu
 */
@ExtendWith(MockitoExtension.class)
public class LibraryEventProducerUnitTest {
  @Mock
  private KafkaTemplate<Integer, String> KafkaTemplate;

  @Spy
  private ObjectMapper mapper;

  @InjectMocks
  private LibraryEventProducer libraryEventProducer;

  @Test
  public void testSendLibraryEventWithHeaderOnFailure() {
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

    // when
    SettableListenableFuture future = new SettableListenableFuture();
    future.setException(new RuntimeException("Exception calling kafka"));
    when(KafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

    // then
    assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventWithHeader(libraryEvent).get());
  }

  @Test
  public void testSendLibraryEventWithHeaderOnSuccess()
      throws JsonProcessingException, ExecutionException, InterruptedException {
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

    String record = mapper.writeValueAsString(libraryEvent);
    SettableListenableFuture future = new SettableListenableFuture();
    ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), record);
    RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, 342, System.currentTimeMillis(), 1, 2);
    SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,
        recordMetadata);
    future.set(sendResult);

    // when
    when(KafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);
    ListenableFuture<SendResult<Integer, String>> listenableFuture = libraryEventProducer.sendLibraryEventWithHeader(
        libraryEvent);

    // then
    SendResult<Integer, String> actualSendResult = listenableFuture.get();
    assert actualSendResult.getRecordMetadata().partition() == 1;
  }
}
