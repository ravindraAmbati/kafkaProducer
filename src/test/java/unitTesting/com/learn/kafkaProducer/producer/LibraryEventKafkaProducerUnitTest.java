package com.learn.kafkaProducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafkaProducer.domain.Book;
import com.learn.kafkaProducer.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventKafkaProducerUnitTest {

    @InjectMocks
    LibraryEventKafkaProducer libraryEventKafkaProducer;

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    private LibraryEvent libraryEvent;
    private Book book;

    @Value("${spring.local.kafka.topic}")
    private String kafkaTopic;

    @BeforeEach
    void setUp() {

        book = Book.builder()
                .id(123)
                .name("aBook")
                .author("anUnknown")
                .build();
        libraryEvent = LibraryEvent.builder()
                .id(null)
                .book(book)
                .build();
    }

    @AfterEach
    void tearDown() {

        libraryEvent = null;
        book = null;
        objectMapper = null;
        libraryEventKafkaProducer = null;
    }

    @Test
    void sendLibraryEvents_onFailure() {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        listenableFuture.setException(new RuntimeException("Exception caused by kafka"));
        when(kafkaTemplate.sendDefault(isA(Integer.class), isA(String.class))).thenReturn(listenableFuture);
        assertThrows(RuntimeException.class, () -> libraryEventKafkaProducer.sendLibraryEvents(libraryEvent).get());
    }

    @Test
    void sendLibraryEventsSynchronous_onFailure() {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        listenableFuture.setException(new RuntimeException("Exception caused by kafka"));
        when(kafkaTemplate.sendDefault(isA(Integer.class), isA(String.class))).thenReturn(listenableFuture);
        assertThrows(RuntimeException.class, () -> libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent));
    }

    @Test
    void sendLibraryEventsForTopic_onFailure() throws JsonProcessingException {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        listenableFuture.setException(new RuntimeException("Exception caused by kafka"));
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("test-topic", key, value);
//        when(kafkaTemplate.send(producerRecord)).thenReturn(listenableFuture);
        assertThrows(RuntimeException.class, () -> libraryEventKafkaProducer.sendLibraryEventsForTopic(libraryEvent));
    }

    @Test
    void sendLibraryEvents_onSuccess() throws JsonProcessingException {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("test-topic", key, value);
        TopicPartition topicPartition = new TopicPartition("test-topic", 3);
        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, 0, 0, 0, 0L, 0, 0);
        SendResult<Integer, String> sendResults = new SendResult<>(producerRecord, recordMetadata);
        listenableFuture.set(sendResults);
        when(kafkaTemplate.sendDefault(key, value)).thenReturn(listenableFuture);
        assertEquals(listenableFuture, libraryEventKafkaProducer.sendLibraryEvents(libraryEvent));
    }

    @Test
    void sendLibraryEventsSynchronous_onSuccess() throws JsonProcessingException, ExecutionException, InterruptedException {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<Integer, String>("test-topic", key, value);
        TopicPartition topicPartition = new TopicPartition("test-topic", 3);
        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, 0, 0, 0, 0L, 0, 0);
        SendResult<Integer, String> sendResults = new SendResult<>(producerRecord, recordMetadata);
        listenableFuture.set(sendResults);
        when(kafkaTemplate.sendDefault(key, value)).thenReturn(listenableFuture);
        assertEquals(listenableFuture.get(), libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent));
    }

    @Test
    void sendLibraryEventsForTopic_onSuccess() throws JsonProcessingException {
        SettableListenableFuture<SendResult<Integer, String>> listenableFuture = new SettableListenableFuture<>();
        Integer key = libraryEvent.getId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        String topic = "library-event";
        List<Header> headers = List.of(new RecordHeader(topic, "book-barcode-reading".getBytes()));
        Timestamp now = Timestamp.valueOf(LocalDateTime.now());
        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, null, key, value, headers);
        TopicPartition topicPartition = new TopicPartition("library-event", 3);
        RecordMetadata recordMetadata = new RecordMetadata(topicPartition, 0, 0, now.getTime(), 0L, 0, 0);
        SendResult<Integer, String> sendResults = new SendResult<>(producerRecord, recordMetadata);
        listenableFuture.set(sendResults);
        libraryEventKafkaProducer.kafkaTopic = topic;
        when(kafkaTemplate.send(producerRecord)).thenReturn(listenableFuture);
        assertEquals(listenableFuture, libraryEventKafkaProducer.sendLibraryEventsForTopic(libraryEvent));
    }
}