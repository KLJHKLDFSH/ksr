package com.store.ksr.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Properties;

public class TestStreams {

    private static final Logger log = LoggerFactory.getLogger(TestStreams.class);
    private StreamsBuilder builder;
    @Autowired
    private KafkaAdmin kafkaAdmin;
    private String one = "one";
    private String two = "two";

    @Autowired
    private Properties kafkaStreamsConfig;

    public TestStreams() {

        this.builder = new StreamsBuilder();
    }

    public void start(){
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(one).build());
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(two).build());
        one();
        two();
        new KafkaStreams(builder.build(),kafkaStreamsConfig).start();
    }

    void one(){
        builder.stream(one)
                .peek((key, value) -> log.info("one value:{}", value));
    }

    void two(){
        builder.stream(two)
                .peek((key, value) -> log.info("two value:{}", value));
    }
}
