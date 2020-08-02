package com.learn.kafkaProducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Book {

    private Integer bookId;
    @NotBlank
    private String name;
    @NotBlank
    private String author;
}
