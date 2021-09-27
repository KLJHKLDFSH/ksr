package com.store.ksr.streams;

import com.store.ksr.config.KafkaConfig;
import com.store.ksr.store.RedisDbKeyValueStoreSupplier;
import com.store.ksr.store.RedisStoreTemplate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

@Component
public class MainStreams {

    private static final Logger log = LoggerFactory.getLogger(MainStreams.class);

    @Autowired
    private Properties kafkaStreamsConfig;
    @Autowired
    private RedisStoreTemplate redisStoreTemplate;

    @Autowired
    private KafkaAdmin kafkaAdmin;


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

    /***
     * join 操作测试
     *
     * @return
     */
//    @Bean
    public KafkaStreams joinOperationTest(){
        //topic
        String one = "one";
        String two = "two";
        TimestampExtractor te = (record, partitionTime)-> {
            log.info("topic:{}, partition:{},key:{}, value:{}",
                    record.topic(), record.partition(), record.key(), record.value());
            return record.timestamp();
        };
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(one).partitions(2).build());
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(two).partitions(2).build());
        //builder streams
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String,String> oneStreams = sb.stream(one, Consumed.with(te));
        KStream<String,String> twoStreams = sb.stream(two, Consumed.with(te));

        oneStreams
                .selectKey( (key, value) -> value)
                .repartition()
                .peek((key, value) -> log.info("one: key:{},value:{}", key, value))
                .join(
                    twoStreams
                            .peek((key, value) -> log.info("two: key:{},value:{}", key, value)),
                    (v1,v2) -> {
                        log.info("join, v1:{}, v2:{}",v1, v2);
                        return v1 + v2;
                    },
                    JoinWindows.of(Duration.ofSeconds(10)))
                .peek((key, value) -> log.info("key:{}, value:{}", key, value));


        String applicationId = UUID.randomUUID().toString();
        log.info("applicationId:{}",applicationId);
        kafkaStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "joinOperationTest");
        kafkaStreamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaStreamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        kafkaStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConfig.STREAMS_STATE_DATA_DIR+applicationId);
        return new KafkaStreams(sb.build(), kafkaStreamsConfig);
    }

}
