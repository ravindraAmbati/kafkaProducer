package com.learn.kafkaProducer.controller;

import com.learn.kafkaProducer.domain.Book;
import com.learn.kafkaProducer.domain.LibraryEvent;
import com.learn.kafkaProducer.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"${spring.kafka.template.default-topic}", "${spring.local.kafka.topic}"}, partitions = 3)
@TestPropertySource(properties =
        {
                "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"
        })
public class LibraryEventsControllerIntegrationTest {

    Map<String, Object> config;
    @Autowired
    private TestRestTemplate testRestTemplate;
    private RequestEntity<LibraryEvent> requestEntity;
    private LibraryEvent libraryEvent;
    private Book book;
    private ResponseEntity<LibraryEvent> actualResponseEntity;
    private ResponseEntity<?> actualRawResponseEntity;
    private HttpHeaders httpHeaders;
    private Consumer<Integer, String> consumer;
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Value("${spring.kafka.template.default-topic}")
    private String defaultKafkaTopic;
    @Value("${spring.local.kafka.topic}")
    private String kafkaTopic;

    @BeforeEach
    void setUp() {
        book = Book.builder()
                .bookId(123)
                .name("aBook")
                .author("anUnknown")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        httpHeaders = new HttpHeaders();
        httpHeaders.setContentType(MediaType.APPLICATION_JSON);
        config = KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker);
        consumer = new DefaultKafkaConsumerFactory<>(config, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {

        testRestTemplate = null;
        requestEntity = null;
        libraryEvent = null;
        book = null;
        actualResponseEntity = null;
        httpHeaders = null;
        consumer.close();
        config = null;
        defaultKafkaTopic = null;
        kafkaTopic = null;
        actualRawResponseEntity = null;
    }

    @Test
    @Timeout(5)
    void createLibraryEvent() {
        LibraryEvent expectedLibraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();
        String expectedConsumerRecord = "{\"id\":null,\"type\":\"NEW\",\"book\":{\"id\":123,\"name\":\"aBook\",\"author\":\"anUnknown\"}}";
        ResponseEntity<LibraryEvent> expectedResponseEntity = new ResponseEntity<>(expectedLibraryEvent, httpHeaders, HttpStatus.CREATED);
        //given
        requestEntity = new RequestEntity<>(libraryEvent, HttpMethod.POST, URI.create("/create/libraryEvent"));
        //when
        actualResponseEntity = testRestTemplate.exchange(requestEntity, LibraryEvent.class);
        //then
        assertEquals(expectedResponseEntity.getStatusCode(), actualResponseEntity.getStatusCode());
        assertEquals(expectedResponseEntity.getStatusCodeValue(), actualResponseEntity.getStatusCodeValue());
        assertEquals(expectedResponseEntity.getBody(), actualResponseEntity.getBody());
        assertEquals(expectedResponseEntity.getHeaders().getContentType(), actualResponseEntity.getHeaders().getContentType());
        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, defaultKafkaTopic);
        assertNull(consumerRecord.key());
        assertNotNull(consumerRecord.value());
        assertEquals(expectedConsumerRecord, consumerRecord.value());
    }

    @Test
    @Timeout(5)
    void createLibraryEventSynchronous() throws InterruptedException {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        ResponseEntity<LibraryEvent> expectedResponseEntity = new ResponseEntity<>(libraryEvent, httpHeaders, HttpStatus.CREATED);
        //given
        requestEntity = new RequestEntity<>(libraryEvent, HttpMethod.POST, URI.create("/create/libraryEvent/waitForResponse"));
        //when
        actualResponseEntity = testRestTemplate.exchange(requestEntity, LibraryEvent.class);
        //then
        assertEquals(expectedResponseEntity.getStatusCode(), actualResponseEntity.getStatusCode());
        assertEquals(expectedResponseEntity.getStatusCodeValue(), actualResponseEntity.getStatusCodeValue());
        assertEquals(expectedResponseEntity.getBody(), actualResponseEntity.getBody());
        assertEquals(expectedResponseEntity.getHeaders().getContentType(), actualResponseEntity.getHeaders().getContentType());
        assertNotNull(KafkaTestUtils.getSingleRecord(consumer, defaultKafkaTopic).value());
    }

    @Test
    @Timeout(5)
    void createLibraryEventForTopic() throws InterruptedException {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        ResponseEntity<LibraryEvent> expectedResponseEntity = new ResponseEntity<>(libraryEvent, httpHeaders, HttpStatus.CREATED);
        //given
        requestEntity = new RequestEntity<>(libraryEvent, HttpMethod.POST, URI.create("/create/libraryEvent/topic"));
        //when
        actualResponseEntity = testRestTemplate.exchange(requestEntity, LibraryEvent.class);
        //then
        assertEquals(expectedResponseEntity.getStatusCode(), actualResponseEntity.getStatusCode());
        assertEquals(expectedResponseEntity.getStatusCodeValue(), actualResponseEntity.getStatusCodeValue());
        assertEquals(expectedResponseEntity.getBody(), actualResponseEntity.getBody());
        assertEquals(expectedResponseEntity.getHeaders().getContentType(), actualResponseEntity.getHeaders().getContentType());
        assertNotNull(KafkaTestUtils.getSingleRecord(consumer, kafkaTopic).value());
    }

    @Test
    @Timeout(5)
    void updateLibraryEvent() {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();

        ResponseEntity<LibraryEvent> expectedResponseEntity = new ResponseEntity<>(libraryEvent, httpHeaders, HttpStatus.OK);
        //given
        requestEntity = new RequestEntity<>(libraryEvent, HttpMethod.PUT, URI.create("/update/libraryEvent"));
        //when
        actualResponseEntity = testRestTemplate.exchange(requestEntity, LibraryEvent.class);
        //then
        assertEquals(expectedResponseEntity.getStatusCode(), actualResponseEntity.getStatusCode());
        assertEquals(expectedResponseEntity.getStatusCodeValue(), actualResponseEntity.getStatusCodeValue());
        assertEquals(expectedResponseEntity.getBody(), actualResponseEntity.getBody());
        assertEquals(expectedResponseEntity.getHeaders().getContentType(), actualResponseEntity.getHeaders().getContentType());
        assertNotNull(KafkaTestUtils.getSingleRecord(consumer, defaultKafkaTopic).value());
    }

    @Test
    @Timeout(5)
    void updateLibraryEvent_nullId() {
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.UPDATE)
                .book(book)
                .build();

        ResponseEntity<?> expectedResponseEntity = new ResponseEntity<>("Library Event Id shouldn't be null", httpHeaders, HttpStatus.BAD_REQUEST);
        //given
        requestEntity = new RequestEntity<>(libraryEvent, HttpMethod.PUT, URI.create("/update/libraryEvent"));
        //when
        actualRawResponseEntity = testRestTemplate.exchange(requestEntity, String.class);
        //then
        assertEquals(expectedResponseEntity.getStatusCode(), actualRawResponseEntity.getStatusCode());
        assertEquals(expectedResponseEntity.getStatusCodeValue(), actualRawResponseEntity.getStatusCodeValue());
        assertEquals(expectedResponseEntity.getBody(), actualRawResponseEntity.getBody());
        assertEquals("text/plain;charset=UTF-8", Objects.requireNonNull(actualRawResponseEntity.getHeaders().getContentType()).toString());
    }
}