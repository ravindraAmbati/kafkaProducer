package com.learn.kafkaProducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learn.kafkaProducer.domain.LibraryEvent;
import com.learn.kafkaProducer.producer.LibraryEventKafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        try {
            libraryEventKafkaProducer.sendLibraryEvents(libraryEvent);
        } catch (JsonProcessingException e) {
            log.error("failed to parse {} " + e.getMessage(), libraryEvent);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(libraryEvent);
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/update/libraryEvent")
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        //todo: invoke kafka producer
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
