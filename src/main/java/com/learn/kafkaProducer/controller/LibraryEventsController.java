package com.learn.kafkaProducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaProducer.domain.LibraryEvent;
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

@RestController
@Slf4j
public class LibraryEventsController {

    @Autowired
    LibraryEventKafkaProducer libraryEventKafkaProducer;

    @PostMapping("/create/libraryEvent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("###Before message sent###");
        libraryEventKafkaProducer.sendLibraryEvents(libraryEvent);
        log.info("###After message sent and it should be called after success message###");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PostMapping("/create/libraryEvent/waitForResponse")
    public ResponseEntity<LibraryEvent> createLibraryEventSynchronous(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {
        log.info("###Before message sent###");
        SendResult<Integer, String> sendResult = libraryEventKafkaProducer.sendLibraryEventsSynchronous(libraryEvent);
        log.info("sendResult:{}", sendResult);
        log.info("###After message sent and it should be called after success message###");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/update/libraryEvent")
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        //todo: invoke kafka producer
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
