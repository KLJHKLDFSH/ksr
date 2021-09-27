package com.store.ksr;

import com.google.gson.Gson;
import com.store.ksr.config.KafkaConfig;
import com.store.ksr.model.IpSession;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class StreamsTests extends KsrApplicationTests{
    private static final Logger log = LoggerFactory.getLogger(StreamsTests.class);
    @Autowired
    private KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    private ThreadPoolTaskExecutor threadPool;
    @Autowired
    private KafkaAdmin kafkaAdmin;
    @Autowired
    private Gson gson;
    @Autowired
    private Properties kafkaStreamsConfig;
    @Test
    void twoStreamsConsumerCommonTopic(){
        String topic = "common";
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).build());
        //builder streams
        one(topic).start();
        two(topic).start();
        //send data
        sendDataToTopic(topic);
        threadWait();
    }

    @Test
    void timeTest(){
        long  eventTime = new Date(1627265623000L).getTime();
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH,-2);
        c.add(Calendar.DAY_OF_MONTH, 8);
        long currentTIme = c.getTime().getTime();
        log.info("event Time:{} ,{}, currentTime:{},{}", format(eventTime), eventTime, format(currentTIme), currentTIme);
    }

    KafkaStreams one(String topic){
        StreamsBuilder sb = new StreamsBuilder();
        sb.<Integer,String>stream(topic).peek(this::print);
        kafkaStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsConfig.get(StreamsConfig.APPLICATION_ID_CONFIG)+"_one");
        return new KafkaStreams(sb.build(), kafkaStreamsConfig);
    }
    KafkaStreams two(String topic){
        StreamsBuilder sb = new StreamsBuilder();
        sb.<Integer,String>stream(topic).peek(this::print);
        kafkaStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaStreamsConfig.get(StreamsConfig.APPLICATION_ID_CONFIG)+"_two");
        return new KafkaStreams(sb.build(), kafkaStreamsConfig);
    }

    @Test
    void timeWindowTest(){
        String topic = "timeWindow";
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).build());
        StreamsBuilder sb = new StreamsBuilder();
        sb.<Integer,String>stream(topic)
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10))
                        .advanceBy(Duration.ofSeconds(1))
                        .grace(Duration.ZERO))
                .count(Materialized
                        .<Integer, Long, WindowStore<Bytes, byte[]>>as("countMaterialized")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> log.info("key:{}, start:{}, end:{},value:{}",key,format(key.window().start()),format(key.window().end()),value));
        kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
        kafkaStreamsConfig.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 10 * 1000L);

        new KafkaStreams(sb.build(), kafkaStreamsConfig).start();
        //send data
        sendDataToTopic(topic);
        threadWait();

    }

    @Test
    void windowStoreTime(){
        String topic = "retention";
        kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).build());
        StreamsBuilder sb = new StreamsBuilder();
        sb.<Integer,String>stream(topic)
                .repartition()
                .groupByKey(Grouped.with(Serdes.Integer(),Serdes.String()))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1))
                        .advanceBy(Duration.ofMinutes(1))
                        .grace(Duration.ZERO))
                .count(Materialized.with(Serdes.Integer(),Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> log.info("key:{}, start:{}, end:{},value:{}",key,format(key.window().start()),format(key.window().end()),value));

        kafkaStreamsConfig.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 10 * 1000L);
        kafkaStreamsConfig.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 10000L);
        kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
        kafkaStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"windowStoreTime");
        new KafkaStreams(sb.build(), kafkaStreamsConfig).start();
        //send data
        sendDataToTopic(topic);
        threadWait();


    }

    void print(Integer key, String value){
        log.info("key:{}, value:{}", key,value);
    }


    private void threadWait(){
        while (true) {
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private String format(Long time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        return sdf.format(new Date(time));
    }

    void sendDataToTopic( String topic ){
        int id = new Random().nextInt();
        log.info("send data id:{}", id);
        long timestamp = new Date().getTime();
        // ipseesion
        IpSession i1 = new IpSession("a", 1.0d);
        ProducerRecord<Integer, String> iRecord1 = new ProducerRecord<>(topic, 0, timestamp,id, gson.toJson(i1));
        IpSession i2 = new IpSession("b", 2.0d);
		ProducerRecord<Integer, String> iRecord2 = new ProducerRecord<>(topic, 1, timestamp,1,gson.toJson(i2));
        IpSession i3 = new IpSession("a", 3.0d);
        ProducerRecord<Integer, String> iRecord3 = new ProducerRecord<>(topic, 0,timestamp,id, gson.toJson(i3));
        IpSession i4 = new IpSession("b", 4.0d);
		ProducerRecord<Integer, String> iRecord4 = new ProducerRecord<>(topic, 1,timestamp,0, gson.toJson(i4));
        kafkaTemplate.send(iRecord1);
		kafkaTemplate.send(iRecord2);
        kafkaTemplate.send(iRecord3);
    }
}
