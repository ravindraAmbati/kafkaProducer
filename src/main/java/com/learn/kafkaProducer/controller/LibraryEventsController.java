package com.learn.kafkaProducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaProducer.domain.LibraryEvent;
import com.learn.kafkaProducer.domain.LibraryEventType;
import com.learn.kafkaProducer.producer.LibraryEventKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventKafkaProducer libraryEventKafkaProducer;

    @PostMapping("/create/libraryEvent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("###Before message sent###");
        libraryEvent.setType(LibraryEventType.NEW);
        libraryEventKafkaProducer.sendLibraryEvents(libraryEvent);
        log.info("###After message sent and it should be called after success message###");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/create/libraryEvent/waitForResponse")
    public ResponseEntity<LibraryEvent> createLibraryEventSynchronous(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("###Before message sent###");
        libraryEvent.setType(LibraryEventType.NEW);
        SendResult<Integer, String> sendResult = libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent);
        log.info("sendResult:{}", sendResult);
        log.info("###After message sent and it should be called after success message###");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/create/libraryEvent/topic")
    public ResponseEntity<LibraryEvent> createLibraryEventForTopic(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("###Before message sent###");
        libraryEvent.setType(LibraryEventType.NEW);
        libraryEventKafkaProducer.sendLibraryEventsForTopic(libraryEvent);
        log.info("###After message sent and it should be called after success message###");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/update/libraryEvent")
    public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        if (null == libraryEvent.getId()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Library Event Id shouldn't be null");
        }
        libraryEvent.setType(LibraryEventType.UPDATE);
        SendResult<Integer, String> sendResult = libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent);
        log.info("sendResult:{}", sendResult);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
