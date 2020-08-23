package com.learn.kafkaProducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
//@Profile("local")
public class AutoKafkaTopicCreationConfig {

    @Bean
    public NewTopic createKafkaTopic() {
        return TopicBuilder
                .name("library-events-topic")
                .partitions(3)
                .build();
    }
}
