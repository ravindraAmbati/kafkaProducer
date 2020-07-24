package com.learn.kafkaProducer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Profile("local")
public class AutoKafkaTopicCreationConfig {

    private static final String DEFAULT_KAFKA_TOPIC = "default-kafka-topic";

    @Bean
    public NewTopic createKafkaTopic() {
        return TopicBuilder
                .name(DEFAULT_KAFKA_TOPIC)
                .partitions(3)
                .build();
    }

    @Bean
    public NewTopic createKafkaTopic(String topic) {
        if (null == topic || topic.length() < 1 || "".equals(topic.trim())) {
            return createKafkaTopic();
        }
        return TopicBuilder
                .name(topic)
                .partitions(3)
                .build();
    }
}
