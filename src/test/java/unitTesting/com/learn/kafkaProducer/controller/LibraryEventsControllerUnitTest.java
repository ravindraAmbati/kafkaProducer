package com.learn.kafkaProducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafkaProducer.domain.Book;
import com.learn.kafkaProducer.domain.LibraryEvent;
import com.learn.kafkaProducer.producer.LibraryEventKafkaProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
@AutoConfigureMockMvc
public class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;
    @Autowired
    ObjectMapper objectMapper;
    @MockBean
    LibraryEventKafkaProducer libraryEventKafkaProducer;
    private LibraryEvent libraryEvent;
    private LibraryEvent.LibraryEventBuilder emptyLibraryEventBuilder;
    private Book book;
    private Book.BookBuilder emptyBookBuilder;
    private String libraryEventJson;
    private String emptyLibraryEventJson;

    @BeforeEach
    void setUp() throws JsonProcessingException {

        book = Book.builder()
                .bookId(123)
                .name("aBook")
                .author("anUnknown")
                .build();
        emptyBookBuilder = Book.builder();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        emptyLibraryEventBuilder = LibraryEvent.builder();
        libraryEventJson = objectMapper.writeValueAsString(libraryEvent);

    }

    @AfterEach
    void tearDown() {

        libraryEvent = null;
        book = null;
        objectMapper = null;
        libraryEventJson = null;
        libraryEventKafkaProducer = null;
    }

    @Test
    void createLibraryEvent() throws Exception {

        when(libraryEventKafkaProducer.sendLibraryEvents(libraryEvent)).thenReturn(null);
        mockMvc.perform(
                post("/create/libraryEvent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().isCreated()
        );
    }

    @Test
    void createLibraryEventSynchronous() throws Exception {

        when(libraryEventKafkaProducer.sendLibraryEvents(libraryEvent)).thenReturn(null);
        mockMvc.perform(
                post("/create/libraryEvent/waitForResponse").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().isCreated()
        );
    }

    @Test
    void createLibraryEventForTopic() throws Exception {

        when(libraryEventKafkaProducer.sendLibraryEvents(libraryEvent)).thenReturn(null);
        mockMvc.perform(
                post("/create/libraryEvent/topic").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().isCreated()
        );
    }

    @Test
    void updateLibraryEvent_null() throws Exception {
        book = Book.builder()
                .bookId(null)
                .name("aBook")
                .author("anUnknown")
                .build();
        libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        libraryEventJson = objectMapper.writeValueAsString(libraryEvent);
        mockMvc.perform(
                put("/update/libraryEvent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().isBadRequest()
        ).andExpect(content().string("Book Id shouldn't be null"));
    }

    @Test
    void updateLibraryEvent() throws Exception {

        libraryEvent = LibraryEvent.builder()
                .libraryEventId(999)
                .book(book)
                .build();
        libraryEventJson = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent)).thenReturn(null);
        mockMvc.perform(
                put("/update/libraryEvent").content(libraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().isOk()
        );
    }

    @Test
    void createLibraryEvent_4xx() throws Exception {

        emptyLibraryEventJson = objectMapper.writeValueAsString(emptyLibraryEventBuilder.build());
        emptyLibraryEventJson = objectMapper.writeValueAsString(emptyLibraryEventBuilder.book(emptyBookBuilder.author("anUnknown").build()).build());
        mockMvc.perform(
                post("/create/libraryEvent").content(emptyLibraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().is4xxClientError()
        ).andExpect(content().string("book.name: must not be blank"));
    }

    @Test
    void createLibraryEventSynchronous_4xx() throws Exception {

        emptyLibraryEventJson = objectMapper.writeValueAsString(emptyLibraryEventBuilder.book(emptyBookBuilder.name("aBook").build()).build());
        mockMvc.perform(
                post("/create/libraryEvent/waitForResponse").content(emptyLibraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().is4xxClientError()
        ).andExpect(content().string("book.author: must not be blank"));
    }

    @Test
    void createLibraryEventForTopic_4xx() throws Exception {

        emptyLibraryEventJson = objectMapper.writeValueAsString(emptyLibraryEventBuilder.book(emptyBookBuilder.bookId(123).build()).build());
        mockMvc.perform(
                post("/create/libraryEvent/topic").content(emptyLibraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().is4xxClientError()
        ).andExpect(content().string("book.author: must not be blank || book.name: must not be blank"));
    }

    @Test
    void updateLibraryEvent_4xx() throws Exception {

        emptyLibraryEventJson = objectMapper.writeValueAsString(emptyLibraryEventBuilder.build());
        mockMvc.perform(
                put("/update/libraryEvent").content(emptyLibraryEventJson).contentType(MediaType.APPLICATION_JSON)
        ).andExpect(
                status().is4xxClientError()
        ).andExpect(content().string("book: must not be null"));
    }
}