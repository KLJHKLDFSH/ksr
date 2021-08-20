package com.store.ksr;

import com.store.ksr.store.RedisDbKeyValueStoreSupplier;
import com.store.ksr.store.RedisStoreTemplate;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.*;

@SpringBootTest
class KsrApplicationTests {

	private static final Logger log = LoggerFactory.getLogger(KsrApplicationTests.class);

	@Autowired
	private StringRedisTemplate stringRedisTemplate;

	@Autowired
	private KafkaTemplate<Integer,String> kafkaTemplate;

	@Autowired
	private KafkaStreams sunStreams;

	@Autowired
	private ThreadPoolTaskExecutor threadPool;

	@Autowired
	private RedisStoreTemplate redisStoreTemplate;

	@Test
	void contextLoads() {
		log.info("success");
	}
	@Test
	void redisConnectTest(){
		stringRedisTemplate.opsForValue().set("test","success");
		assert(Objects.equals(stringRedisTemplate.opsForValue().get("test"), "success"));
	}

	@Test
	void bytesRedisSetTest(){
		byte[] setValue = "success".getBytes();
		System.out.println("=================== set value ===================");
		for (int i = 0; i < setValue.length; i++)  {
			System.out.print(setValue[i]);
		}
		System.out.println();
		redisStoreTemplate.<Bytes,byte[]>opsForHash().put("redis,test", new Bytes("bytes".getBytes()), setValue);
		byte[] getValue = redisStoreTemplate.opsForValue().get( new Bytes("bytes".getBytes()) );
//		redisTemplate.opsForValue().set("123","123123");
//		String getValue = redisTemplate.opsForValue().get("123");
		System.out.println("=================== get value ===================");
		for (int i = 0; i < getValue.length; i++)  {
			System.out.print(getValue[i]);
		}
		assert(Arrays.equals(setValue, getValue));
	}

	@Test
	void kafkaConnectTest(){
		log.info("producer");
		ProducerRecord<Integer,String> producerRecord = new ProducerRecord<>("test",1,"success");
		kafkaTemplate.send(producerRecord);
		log.info("consumer");
		Consumer<Integer,String> consumer = new KafkaConsumer<Integer, String>(consumerProp());
		consumer.subscribe(Collections.singleton("test"));
		ConsumerRecords<Integer,String> records = consumer.poll(Duration.ofMinutes(1));
		Iterator<ConsumerRecord<Integer, String>> iterator = records.iterator();
		while (iterator.hasNext()){
			String value = iterator.next().value();
			log.info("consumer:{}",value);
			assert(Objects.equals("success",value));
		}
	}

	@Test
	void sumStreamsTest(){
		keepAliveSend("test");
		sunStreams.start();
		while (sunStreams.state().isRunningOrRebalancing()){
			try {
				Thread.sleep(5000);
				ReadOnlyKeyValueStore<Integer, Long> countStore =
						sunStreams
								.store(StoreQueryParameters.fromNameAndType("sunStore", QueryableStoreTypes.keyValueStore()));
				countStore.all().forEachRemaining(kv-> log.info("store, key:{},value:{}",kv.key,kv.value));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	void redisKeyValueStoreStreams(){
		String topic = "redisTest";
		keepAliveSend(topic);
		KafkaStreams kafkaStreams = redisStoreStreamsTest(topic);
		kafkaStreams.start();
		while (kafkaStreams.state().isRunningOrRebalancing()){
			try {
				Thread.sleep(5000);
				ReadOnlyKeyValueStore<Integer, Long> countStore =
						kafkaStreams
								.store(StoreQueryParameters.fromNameAndType("redisDBTest", QueryableStoreTypes.keyValueStore()));
				countStore.all().forEachRemaining(kv-> log.info("store, key:{},value:{}",kv.key,kv.value));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	void localStoreWindowStreams(){
		String topic = "windowTest";
		String storeName = "windowStoreInLocal";
		keepAliveSend(topic);
		KafkaStreams windowStreams = localStoreWindowStreams(topic, storeName);
		windowStreams.start();
		while (windowStreams.state().isRunningOrRebalancing()){
			ReadOnlyWindowStore<Integer, String> winCount = windowStreams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore() ));
			winCount.all().forEachRemaining( e -> log.info("window:{}, value:{}", e.key.toString(), e.value) );
		}
	}


	@Autowired
	private Properties kafkaStreamsConfig;

	private KafkaStreams redisStoreStreamsTest(String topic){
		StreamsBuilder sb = new StreamsBuilder();
		sb.stream(topic)
				.groupByKey()
				.count( Materialized.as(new RedisDbKeyValueStoreSupplier("redisDBTest",redisStoreTemplate)));
		return new KafkaStreams(sb.build(),kafkaStreamsConfig);
	}

	private KafkaStreams localStoreWindowStreams(String topic, String storeName){
		StreamsBuilder sb = new StreamsBuilder();
		sb.stream(topic)
				.groupByKey()
				.windowedBy(SlidingWindows
						.withTimeDifferenceAndGrace(Duration.ofMinutes(1),Duration.ofSeconds(10)))
				.count(Materialized.as(storeName));
		return new KafkaStreams(sb.build(), kafkaStreamsConfig);
	}


	private void sendRecordTopic(String topic){
		while (true){
			int mod = 10;
			int key = new Random().nextInt() % mod;
			ProducerRecord<Integer,String> producerRecord = new ProducerRecord<>(topic,key,"success");
			kafkaTemplate.send(producerRecord)	;
		}
	}

	private void keepAliveSend(String topic){
		threadPool.execute(()-> sendRecordTopic(topic));
	}

	private Map<String,Object> consumerProp(){
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000 );
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,20);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000 );
		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,3000);
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"test");
		return props;
	}


}

