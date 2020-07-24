package com.learn.kafkaProducer.controller;

import com.learn.kafkaProducer.domain.LibraryEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LibraryEventsController {

    @PostMapping("/create/libraryEvent")
    public ResponseEntity<LibraryEvent> createLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        //todo: invoke kafka producer
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/update/libraryEvent")
    public ResponseEntity<LibraryEvent> updateLibraryEvent(@RequestBody LibraryEvent libraryEvent) {
        //todo: invoke kafka producer
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }
}
