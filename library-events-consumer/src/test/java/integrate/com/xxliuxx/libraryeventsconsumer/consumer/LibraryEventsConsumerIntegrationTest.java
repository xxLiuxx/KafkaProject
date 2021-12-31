package com.xxliuxx.libraryeventsconsumer.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxliuxx.libraryeventsconsumer.entity.Book;
import com.xxliuxx.libraryeventsconsumer.entity.LibraryEvent;
import com.xxliuxx.libraryeventsconsumer.entity.LibraryEventType;
import com.xxliuxx.libraryeventsconsumer.mapper.LibraryEventsRepository;
import com.xxliuxx.libraryeventsconsumer.service.LibraryEventsService;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Yuchen Liu
 */
@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
    "spring.kafka.producer.bootstrap-servers = ${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.bootstrap-servers = ${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

  @Autowired
  private EmbeddedKafkaBroker embeddedKafkaBroker;

  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @SpyBean
  private LibraryEventsConsumer libraryEventsConsumerSpy;

  @SpyBean
  private LibraryEventsService libraryEventsServiceSpy;

  @Autowired
  private LibraryEventsRepository libraryEventsRepository;

  @Autowired
  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
      ContainerTestUtils.waitForAssignment(messageListenerContainer,
          embeddedKafkaBroker.getPartitionsPerTopic());
    }
  }

  @AfterEach
  void tearDown() {
    libraryEventsRepository.deleteAll();
  }

  @Test
  public void publishNewLibraryEvent()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka\",\"bookAuthor\":\"Udemy\"}}";
    kafkaTemplate.sendDefault(json).get();

    CountDownLatch countDownLatch = new CountDownLatch(1);
    countDownLatch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));

    List<LibraryEvent> allRecords = (List<LibraryEvent>) libraryEventsRepository.findAll();
    assert allRecords.size() == 1;
    allRecords.forEach(record -> {
      assertEquals(123, record.getBook().getBookId());
      assert record.getLibraryEventId() != null;
    });
  }

  @Test
  public void updateLibraryEvent()
      throws ExecutionException, InterruptedException, JsonProcessingException {
    // build the updated content
    String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka\",\"bookAuthor\":\"Udemy\"}}";
    LibraryEvent libraryEvent = mapper.readValue(json, LibraryEvent.class);
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEventsRepository.save(libraryEvent);

    Book updatedBook = Book.builder().bookId(123).bookName("Kafka 2").bookAuthor("Udemy").build();
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEvent.setBook(updatedBook);

    // send the request
    String updatedJson = mapper.writeValueAsString(libraryEvent);
    kafkaTemplate.sendDefault(updatedJson).get();

    CountDownLatch countDownLatch = new CountDownLatch(1);
    countDownLatch.await(3, TimeUnit.SECONDS);

    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));

    LibraryEvent persistedLibraryEvent = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();

    assertEquals("Kafka 2", persistedLibraryEvent.getBook().getBookName());

  }

  @Test
  void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
    //given
    Integer libraryEventId = 123;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();
    //when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);


    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(1)).processLibraryEvents(isA(ConsumerRecord.class));

    Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
    assertFalse(libraryEventOptional.isPresent());
  }

  @Test
  void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
    //given
    Integer libraryEventId = null;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();

    //when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);


    verify(libraryEventsConsumerSpy, times(5)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(5)).processLibraryEvents(isA(ConsumerRecord.class));
  }

  @Test
  void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
    //given
    Integer libraryEventId = 000;
    String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
    kafkaTemplate.sendDefault(libraryEventId, json).get();

    //when
    CountDownLatch latch = new CountDownLatch(1);
    latch.await(3, TimeUnit.SECONDS);


    verify(libraryEventsConsumerSpy, times(5)).onMessage(isA(ConsumerRecord.class));
    verify(libraryEventsServiceSpy, times(5)).processLibraryEvents(isA(ConsumerRecord.class));
  }
}
