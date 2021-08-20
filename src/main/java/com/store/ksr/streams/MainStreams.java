package com.store.ksr.streams;

import com.store.ksr.store.RedisDbKeyValueStoreSupplier;
import com.store.ksr.store.RedisStoreTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class MainStreams {

    private static final Logger log = LoggerFactory.getLogger(MainStreams.class);

    @Autowired
    private Properties kafkaStreamsConfig;
    @Autowired
    private RedisStoreTemplate redisStoreTemplate;

    @Bean("sunStreams")
    public KafkaStreams sunStreams(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer,String> stream = builder.stream("test");
        stream
                .groupBy( (key, value) -> value , Grouped.with(new Serdes.StringSerde(),new Serdes.StringSerde()))
                .count(Materialized.as("sunStore"));
        return new KafkaStreams(builder.build(),kafkaStreamsConfig);
    }

    @Bean("distributedStreams")
    public KafkaStreams distributedStreams(){
        String topic = "dist";
        StreamsBuilder builder = new StreamsBuilder();
        TimestampExtractor timestampExtractor = ( (ConsumerRecord<Object, Object> record, long partitionTime)->{
//            log.info("record:{}, partitionTime:{}",record.toString(),new DateFormatter().print(new Date(partitionTime), Locale.CHINA) );
            return record.timestamp();
        });
        KStream<Integer, String> stream = builder.stream(topic,Consumed.with(timestampExtractor));
        stream
                .groupByKey()
                .count(Materialized.as(new RedisDbKeyValueStoreSupplier("distributedCount", redisStoreTemplate )));

        return new KafkaStreams(builder.build(), kafkaStreamsConfig);

    }
}
