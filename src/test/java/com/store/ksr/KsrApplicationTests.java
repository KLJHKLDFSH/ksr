package com.store.ksr;

import com.google.gson.Gson;
import com.store.ksr.config.KafkaConfig;
import com.store.ksr.model.IpSession;
import com.store.ksr.model.Line;
import com.store.ksr.model.Queue;
import com.store.ksr.store.*;
import com.store.ksr.streams.processor.PrintInfoProcessorSupplier;
import com.store.ksr.utli.ObjectWithByte;
import joptsimple.util.KeyValuePair;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.StringUtils;

import java.awt.geom.QuadCurve2D;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Supplier;

@SpringBootTest
class KsrApplicationTests {

	private static final Logger log = LoggerFactory.getLogger(KsrApplicationTests.class);

	@Autowired
	private StringRedisTemplate stringRedisTemplate;

	@Autowired
	private KafkaTemplate<Integer,String> kafkaTemplate;

	@Autowired
	private ThreadPoolTaskExecutor threadPool;

	@Autowired
	private RedisStoreTemplate redisStoreTemplate;
	@Autowired
	private RedisWindowStoreTemplate redisWindowStoreTemplate;
	@Autowired
	private KafkaAdmin kafkaAdmin;
	@Value("${broker}")
	private List<String> broker;
	@Autowired
	private Gson gson;

	@Test
	void contextLoads() {
		log.info("success");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH,-2);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		log.info(sdf.format(calendar.getTime()));
		String keyword = "IDC,万国,";
		String[] keywords = keyword.split(",");
		for (int i = 0; i < keywords.length; i++) {
			log.info("keyword:{}", keywords[i]);
		}
	}

	@Test
	void listTypeYamlConfigParse(){
//		log.info(broker);
		for(String str: broker){
			log.info(str);
		}
	}

	@Test
	void redisConnectTest(){
		stringRedisTemplate.opsForValue().set("test","success");
		assert(Objects.equals(stringRedisTemplate.opsForValue().get("test"), "success"));
	}
//	@TestjoinOperationTest
//	void segmentSerializer(){
//		Bytes key = Bytes.wrap("key".getBytes());
//		byte[] value = "value".getBytes();
//		Segment segment = new Segment(key, value);
//		byte[] ser = ObjectWithByte.toByteArray(segment);
//		//des
//		Segment des = (Segment) ObjectWithByte.toObject(ser);
//		log.info("key:{}", new String(des.key().get()));
//		log.info("value:{}", new String(des.value()) );
//	}
//
//	@Test
//	void redisStoreMapObj(){
//		long time = System.currentTimeMillis();
//		Bytes key = new Bytes("key".getBytes());
//		byte[] value = "value".getBytes();
//		log.info("key:{}", key);
//		log.info("value:{}", value);
//		Segment segment = new Segment(key, value);
//		redisWindowStoreTemplate.opsForHash().put("cmp",time,segment);
//		Segment result =
//				redisWindowStoreTemplate.<Long, Segment>opsForHash().get("cmp",time);
//		log.info("rKey:{}",segment.key() );
//		log.info("rValue:{}", segment.value() );
//		assert Objects.equals(value, segment.value());
//	}

	@Test
	void hashCodePartitionTest(){
		String centerName = "上海中心";
		String branchSh = "上海分公司";
		log.info("once:{}",Objects.hash(centerName, branchSh));

		String centerName_1 = "上海中心";
		String branchSh_2 = "北京分公司";
		log.info("two:{}",Objects.hash(centerName_1,branchSh_2));


	}

	/**
	 * 无法实现仅按时间获取数据功能
	 */
	@Test
	void redisWindowStoreTest(){
		long time = System.currentTimeMillis();
		Bytes key = new Bytes("key".getBytes());
		byte[] value = "value".getBytes();
		log.info("key:{}", key);
		log.info("value:{}", new String(value));
		ConcurrentNavigableMap<BytesWrapper, byte[]> map = new ConcurrentSkipListMap<>();
		map.put(BytesWrapper.wrap(key), value);
		redisWindowStoreTemplate.opsForHash().put("redisWindowStoreTest", time, map);
		ConcurrentNavigableMap<BytesWrapper, byte[]> result = redisWindowStoreTemplate.<Long,ConcurrentNavigableMap<BytesWrapper, byte[]>>opsForHash().get("redisWindowStoreTest", time);
		byte[] rValue = result.get(BytesWrapper.wrap(key));
		log.info("rValue:{}",new String(value));
		assert Arrays.equals(value,rValue);
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
		StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer,String> stream = builder.stream("test");
		stream
				.groupBy( (key, value) -> value , Grouped.with(new Serdes.StringSerde(),new Serdes.StringSerde()))
				.count(Materialized.as("sunStore"));
		KafkaStreams sunStreams = new KafkaStreams(builder.build(),kafkaStreamsConfig);
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
		StreamsBuilder sb = new StreamsBuilder();
		sb.<Integer,String>stream(topic)
				.groupByKey()
				.count( Materialized.as(new RedisDbKeyValueStoreSupplier("redisDBTest",redisStoreTemplate)));
		KafkaStreams kafkaStreams = new KafkaStreams(sb.build(),kafkaStreamsConfig);
		kafkaStreams.cleanUp();
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
		long start = new Date().getTime();
		long keep =  10 * 1000;
		while (windowStreams.state().isRunningOrRebalancing() && new Date().getTime() - keep <= start ){
			if(windowStreams.state() == KafkaStreams.State.RUNNING){
				ReadOnlyWindowStore<Integer, String> winCount = windowStreams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.windowStore() ));
				winCount.all().forEachRemaining( e -> log.info("window:{}, value:{}", e.key.toString(), e.value) );
			}
		}
	}

	/**
	 * 测试两个有两个分区的topic，再同一个streams task 中，task 获取到的partition情况
	 */
	@Test
	void taskProcessorSamePartitionTest(){
		// create topic session and segment ,partition is 3 ;
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name("session").partitions(3).build());
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name("segment").partitions(3).build());
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name("result").partitions(3).build());
		//send data to topic
		String key1 = "上海中心 - 上海分公司";
		String key2 = "上海中心 - 北京分公司";
		String key3 = "上海中心 - 昆明分公司";
		String key4 = "北京中心 - 昆明分公司";
		String sessionValue = "123,";

		log.info("hash code : key1:{}, key2:{}, key3{}, key4{}",key1.hashCode(), key2.hashCode(), key3.hashCode(), key4.hashCode());

		String segmentValue = "123|";
		ProducerRecord<Integer, String> session1 = new ProducerRecord<>("session",key1.hashCode(),sessionValue );
		ProducerRecord<Integer, String> session2 = new ProducerRecord<>("session",key2.hashCode(),sessionValue );
		ProducerRecord<Integer, String> session3 = new ProducerRecord<>("session",key3.hashCode(),sessionValue );
		ProducerRecord<Integer, String> session4 = new ProducerRecord<>("session",key4.hashCode(),sessionValue );
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss:SSS");
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, -10);
		long timestamp = calendar.getTime().getTime();
//		long timestamp = new Date().getTime();
		log.info("===========timestamp:{}===========", sdf.format(new Date(timestamp)));
		int partitions = kafkaTemplate.partitionsFor("segment").size();
		ProducerRecord<Integer, String> segment1 = new ProducerRecord<>("segment",0,timestamp,1,segmentValue );
		ProducerRecord<Integer, String> segment2 = new ProducerRecord<>("segment",0,timestamp,1 ,segmentValue );
		ProducerRecord<Integer, String> segment3 = new ProducerRecord<>("segment",0,timestamp,1 ,segmentValue );
		ProducerRecord<Integer, String> segment4 = new ProducerRecord<>("segment",0,timestamp,1 ,segmentValue );

		//builder streams DSL application
		TimestampExtractor te = (  record, partitionTime) -> {
			log.info("topic:{}, timestamp:{}, partitionTime:{}", record.topic(),sdf.format(new Date( record.timestamp())), sdf.format(new Date(partitionTime)));
			return record.timestamp();
		};
		StreamsBuilder sb = new StreamsBuilder();
//		KStream<Integer,String>  sessionStreams = sb.stream("session", Consumed.with(te));
		KStream<Integer,String>  segmentStreams = sb.stream("segment", Consumed.with(te));
//		segmentStreams
//				.join(sessionStreams, (l ,r )-> l +"|"+ r ,JoinWindows.of(Duration.ofMinutes(1)))
//				.peek((key, value) -> log.info("key:{}, value", key.))
//				.process( new PrintInfoProcessorSupplier());
//		segmentStreams.groupByKey()
//						.windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(5)))
//						.aggregate(String::new,
//								(key, value, agg) -> {
//									agg = agg + value;
//									return agg;
//								}
//						)
//						.toStream()
//						.peek((key, value) -> log.info("window key:{}, start:{}, end:{} , value:{}", key.key(), sdf.format(new Date(key.window().start())),sdf.format(new Date(key.window().end())) , value))
//						.selectKey( (key,value) ->key.key() )
//						.process(new PrintInfoProcessorSupplier());

		kafkaStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConfig.STREAMS_STATE_DATA_DIR+"taskProcessorSamePartitionTest");
		kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
		KafkaStreams streams = new KafkaStreams(sb.build(),kafkaStreamsConfig);
		streams.cleanUp();
		streams.start();
		log.info("streams application start");
		if (streams.state().isRunningOrRebalancing()){
			log.info("send data to topic");
			kafkaTemplate.send(session1);
			kafkaTemplate.send(session2);
			kafkaTemplate.send(session3);
			kafkaTemplate.send(session4);

			kafkaTemplate.send(segment1);
			kafkaTemplate.send(segment2);
			kafkaTemplate.send(segment3);
			kafkaTemplate.send(segment4);
			log.info("send data to topic success");
		}
		while (true) {
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	void redisLineTest(){
		//kafka streams real-time update redis database line;
		//redis store line window data
		//72 line
		List<String> lineList = lineList();
		//add value;
		Random random = new Random();

		// realtime line , object map to json, use string type store json data, with append Prefix on the line name left;
//		String realTime = "REALTIME_";
//		lineList.parallelStream()
//				.map(this::builderLine)
//				.forEach(line -> {
//					stringRedisTemplate.opsForValue().set(realTime + line.getName(),gson.toJson(line));
//				});
//		lineList.parallelStream()
//				.forEach(lineName-> {
//					Line line = gson.fromJson(stringRedisTemplate.opsForValue().<Line>get(realTime + lineName), Line.class) ;
//					log.info("line:{}", gson.toJson(line));
//				} );
		//history
		//use redis list type

		String preFix = "HIS_";
		for (String name: lineList){
			List<Line> ll = historyList(name);
			stringRedisTemplate.opsForList().rightPush(preFix+name, gson.toJson(ll));
			log.info("name:{},data:{}",name, stringRedisTemplate.opsForList().leftPop(preFix+name) );;
		}
	}

	@Test
	void kafkaWindowChangelogTest(){
		//create topic
		String topic = "kafkaWindowChangelogTest";
		String resultTopic = "aggregationResultTopic";
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(1).build());
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(resultTopic).partitions(1).build());

		//builder state store
		String storeName = "successStore";
//		WindowStore<Integer, String> successStore = Stores.windowStoreBuilder(
//				Stores.persistentTimestampedWindowStore(storeName,Duration.ofMinutes(1),Duration.ofSeconds(10),true),
//						new Serdes.IntegerSerde(), new Serdes.StringSerde())
//				.withLoggingDisabled()
//				.build();
//		builder streams
		Materialized<Integer, String, WindowStore<Bytes, byte[]>> materialized = Materialized.<Integer,String,WindowStore<Bytes, byte[]> >as(storeName)
				.withKeySerde(new Serdes.IntegerSerde())
				.withValueSerde(new Serdes.StringSerde());
//				.withCachingDisabled()
//				.withLoggingDisabled();


		StreamsBuilder sb = new StreamsBuilder();
		sb.<Integer, String>stream(topic)
				.selectKey( (key, value) -> key + 1)
				.repartition()
				.groupByKey()
				.windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(10)))
				.aggregate(String::new,
						(key, value, agg) -> {
							agg = agg + value;
							return agg;
						},
						materialized
				).toStream()
				.selectKey((key, value) -> key.key())
				.to(resultTopic);
		kafkaStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConfig.STREAMS_STATE_DATA_DIR+"taskProcessorSamePartitionTest");
		kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
		kafkaStreamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkaWindowChangelogTest");
		kafkaStreamsConfig.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 10 * 1000L);
		KafkaStreams streams = new KafkaStreams(sb.build(),kafkaStreamsConfig);
		streams.cleanUp();
		//start
		streams.start();
		//send data
		keepAliveSend(topic);
		threadWait();
	}


	@Test
	void windowTimestampTest(){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

		//create topic
		String topic = "windowTimestamp";
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(topic).partitions(10).build());
		//builder data
		String prValue = "q|";
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.SECOND, -120 );
		log.info("-120 :{}",sdf.format(calendar.getTime()));
		ProducerRecord<Integer, String> pr0 = new ProducerRecord<>(topic,0, calendar.getTime().getTime(),1,prValue);
		calendar.add(Calendar.SECOND, 60 );
		ProducerRecord<Integer, String> pr1 = new ProducerRecord<>(topic,0, calendar.getTime().getTime(),1,prValue);
		ProducerRecord<Integer, String> pr2 = new ProducerRecord<>(topic,1,prValue);
		ProducerRecord<Integer, String> pr3 = new ProducerRecord<>(topic,1,prValue);
		ProducerRecord<Integer, String> pr4 = new ProducerRecord<>(topic,1,prValue);

		//builder streams
		TimestampExtractor te = (record, partitionTime)-> {
			log.info("timestamp:{}, partitionTime:{}",sdf.format(new Date(record.timestamp())) , sdf.format(new Date(partitionTime)));
			return record.timestamp();
		};
		StreamsBuilder sb = new StreamsBuilder();
		sb.<Integer, String>stream(topic)
				.groupByKey()
				.windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(1)).grace(Duration.ZERO))
				.aggregate(String::new ,
						(key, value, agg) -> agg += value,
						Named.as("windowTimestamp"),
						Materialized.with(new Serdes.IntegerSerde(), new Serdes.StringSerde() ) )
//				.suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10),Suppressed.BufferConfig.maxRecords(2L)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				.peek((key, value) -> log.info("window start:{}, end:{},key:{} value:{}",sdf.format(new Date(key.window().start())),sdf.format(new Date(key.window().end())),key.key(),value));

		kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
		KafkaStreams kafkaStreams = new KafkaStreams(sb.build(), kafkaStreamsConfig);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		//send data
		if (kafkaStreams.state().isRunningOrRebalancing()){
			log.info("send Date:{}", sdf.format(new Date()));
			kafkaTemplate.send(pr1);
			kafkaTemplate.send(pr2);
			kafkaTemplate.send(pr3);
			kafkaTemplate.send(pr4);
			kafkaTemplate.send(pr0);

		}
		threadWait();
	}

	@Test
	void queueTestStreams(){
		//create topic
		String queue = "queue";
		String ipsession =  "ipsession";
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(queue).partitions(2).build());
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(ipsession).partitions(2).build());

		//builder streams topology
		StreamsBuilder sb = new StreamsBuilder();


		KStream<Integer, String> ipSessionStreams =
				ipSessionTable(sb, ipsession).toStream()
				//压平
				.map((key, value) -> {
					IpSession tmp = gson.fromJson(value, IpSession.class);
					tmp.setTime(key.window().start(), key.window().end());
					log.info("ip session: key:{}, value:{}, start:{}, end:{}", key, value, format(tmp.getStart()), format(tmp.getEnd()));
					return KeyValue.pair(key.key(),gson.toJson(tmp));
				});
		sb.<Integer,String>stream(queue)
				.peek((key, value) -> log.info("queue: key:{}, value:{}", key, value))
				.leftJoin(ipSessionStreams,
						(v1, v2) ->  {
//							log.info("JOIN START... v1:{}, v2:{}", v1, v2);
							if(v2 == null) {
								return v1;
							}
							IpSession ipSession = gson.fromJson(v2,IpSession.class);
							Queue tmp = gson.fromJson(v1, Queue.class);
							log.info("join window: queue time :{}, ipSession start time:{}, end time:{}",
									format(tmp.getTime()), format(ipSession.getStart()),format(ipSession.getEnd()) );
							if( tmp.getTime() >= ipSession.getStart() && tmp.getTime() <= ipSession.getEnd()){

								tmp.setBitps(ipSession.getBitps());
							}
							return gson.toJson(tmp);
						},
						JoinWindows.of(Duration.ofSeconds(1) ))
				.mapValues(value ->  gson.fromJson(value, Queue.class))
				.filter((key, value) -> value.getBitps() != null)
				.peek((key, value) -> log.info("join: key:{},value:{}",key, gson.toJson(value)));


		KafkaStreams kafkaStreams = new KafkaStreams(sb.build(), kafkaStreamsConfig);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		//send data
		while (kafkaStreams.state().isRunningOrRebalancing()){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if(kafkaStreams.state().compareTo(KafkaStreams.State.RUNNING)==0){
				log.info("streams is running");
				break;
			}
		}
		log.info("send data");
		threadPool.execute(this::sendIpSession);
		threadPool.execute(this::sendQueue);
		//monitor table status
//		while (kafkaStreams.state().isRunningOrRebalancing()){
//			if(kafkaStreams.state() == KafkaStreams.State.RUNNING){
//				String storeName = ipSessionStreams.queryableStoreName();
//				ReadOnlyWindowStore<Integer, Double> store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(storeName,
//						QueryableStoreTypes.windowStore()));
//				store.all()
//						.forEachRemaining( c -> {
//							log.info("table: key:{}, value:{}",c.key,c.value);
//						});
//			}
//		}

		threadWait();
	}

	public KTable<Windowed<Integer>,String> ipSessionTable(StreamsBuilder sb,String ipsession){
		Materialized<Integer, String, WindowStore<Bytes, byte[]>> materialized =
				Materialized.<Integer, String, WindowStore<Bytes, byte[]>>as("ipsessionTable")
						.withKeySerde(new Serdes.IntegerSerde() )
						.withValueSerde( new Serdes.StringSerde())
						.withLoggingDisabled();
//		KTable<Windowed<Integer>,String> ipSessionStreams =
		return sb.<Integer,String>stream(ipsession)
						.groupByKey()
						.windowedBy( TimeWindows.of(Duration.ofSeconds(5))
								.advanceBy(Duration.ofSeconds(1))
								.grace(Duration.ZERO))
						.aggregate( String::new,
								(key, value ,sum) -> {
									String result = sum;
									if(StringUtils.hasText(sum)){
										IpSession tmp = gson.fromJson(value, IpSession.class);
										IpSession sumIp  = gson.fromJson(sum, IpSession.class);
										sumIp.setBitps(tmp.getBitps() + sumIp.getBitps());
										result = gson.toJson(sumIp);
									}else {
										result = value;
									}
									return result;
								},
								materialized
						)
						.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
	}

	@Test
	void streamsJoinWindowTable(){
//create topic
		String queue = "queue";
		String ipsession =  "ipsession";
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(queue).partitions(2).build());
		kafkaAdmin.createOrModifyTopics(TopicBuilder.name(ipsession).partitions(2).build());

		//builder streams topology
		StreamsBuilder sb = new StreamsBuilder();
		KTable<Windowed<Integer>,String> ipSessionTable = ipSessionTable(sb, ipsession);

		sb.<Integer,String>stream(queue)
				.map((key, value) -> {
					Queue q = gson.fromJson(value, Queue.class);
					Windowed<Integer> k = new Windowed<>(key,new TimeWindow(q.getTime() - 500L, q.getTime() + 500L ));
					return KeyValue.pair(k, value);
				})
				.leftJoin(ipSessionTable, (l,r)->{
					log.info("l:{}, r:{}",l,r);
					if(r == null) {
						return l;
					}
					IpSession ipSession = gson.fromJson(r,IpSession.class);
					Queue tmp = gson.fromJson(l, Queue.class);

					if( tmp.getTime() >= ipSession.getStart() && tmp.getTime() <= ipSession.getEnd()){
						log.info("join window: queue time :{}, ipSession start time:{}, end time:{}",
								format(tmp.getTime()), format(ipSession.getStart()),format(ipSession.getEnd()) );
						tmp.setBitps(ipSession.getBitps());
					}
					return gson.toJson(tmp);
				}, Joined.keySerde(new WindowedSerdes.TimeWindowedSerde<Integer>()) )
				.mapValues(value ->  gson.fromJson(value, Queue.class))
				.filter((key, value) -> value.getBitps() != null)
				.peek((key, value) -> log.info("join: key:{},value:{}",key, gson.toJson(value)));

		KafkaStreams kafkaStreams = new KafkaStreams(sb.build(), kafkaStreamsConfig);
		kafkaStreams.cleanUp();
		kafkaStreams.start();
		//send data
		threadPool.execute(this::sendIpSession);
		threadPool.execute(this::sendQueue);

		threadWait();
	}

	@Test
	void size(){
		String data = "{\"concurrent_flow_count\":0,\"create_flow_count\":0,\"downlink_tcp_effective_payload_packet\":0,\"endpoint1_packet_retrans_rate\":-2.0,\"endpoint1_tx_bitps\":816.0,\"endpoint1_tx_tcp_retransmission_packet\":18446744073709551614,\"endpoint2_packet_retrans_rate\":-2.0,\"endpoint2_tx_bitps\":816.0,\"endpoint2_tx_tcp_retransmission_packet\":18446744073709551614,\"ip_endpoint1\":\"144.72.252.2\",\"ip_endpoint2\":\"20.4.33.36\",\"tcp_connect_failure_rate\":0.0,\"tcp_connect_noresponse_rate\":0.0,\"tcp_retransmission_packet\":18446744073709551614,\"total_bitps\":1632.0,\"total_packet_retrans_rate\":0.0,\"uplink_tcp_effective_payload_packet\":0}";
		log.info("data size:{}", data.getBytes().length);
		String comp = "{\"endpoint1_tx_bitps\": 816.0,\"line\":{\"center\":\"上海\",\"branch\":\"云南\"},\"queue\":\"asdfasfjaslkdfh\"}";
		log.info("comp size:{}",comp.getBytes().length);

	}

	private void sendIpSession(){

	}
	private void sendQueue(){
		Integer id = new Random().nextInt();
		log.info("id:{}",id);
		//builder Data
//builder Data
		String ipSession = "ipsession";
//		String topic, Integer partition, Long timestamp, K key, V value

//		while (true){
//			try {
//				Calendar c = Calendar.getInstance();
//				c.add(Calendar.MONTH, -1 );
//				Long timestamp = c.getTime().getTime();
//		long timestamp = new Date().getTime();
		long  timestamp = 1627265623000L;
		// ipseesion
		IpSession i1 = new IpSession("a", 1.0d);
		ProducerRecord<Integer, String> iRecord1 = new ProducerRecord<>(ipSession, 0, timestamp,id, gson.toJson(i1));
		IpSession i2 = new IpSession("b", 2.0d);
//				ProducerRecord<Integer, String> iRecord2 = new ProducerRecord<>(ipSession, 1, timestamp,1,gson.toJson(i2));
		IpSession i3 = new IpSession("a", 3.0d);
		ProducerRecord<Integer, String> iRecord3 = new ProducerRecord<>(ipSession, 0,timestamp,id, gson.toJson(i3));
		IpSession i4 = new IpSession("b", 4.0d);
//				ProducerRecord<Integer, String> iRecord4 = new ProducerRecord<>(ipSession, 1,timestamp,0, gson.toJson(i4));
		kafkaTemplate.send(iRecord1);
//				kafkaTemplate.send(iRecord2);
		kafkaTemplate.send(iRecord3);
//				kafkaTemplate.send(iRecord4);
//				Thread.sleep( 1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		while (true){
//			try {
				String queue = "queue";
				Queue q1 = new Queue("Q1");
				ProducerRecord<Integer, String> queueRecord1 = new ProducerRecord<>(queue, 0,timestamp,id, gson.toJson(q1));
				Queue q2 = new Queue("Q2" );
//				ProducerRecord<Integer, String> queueRecord2 = new ProducerRecord<>(queue, 1,timestamp,1, gson.toJson(q2));
				Queue q3 = new Queue("Q1" );
				ProducerRecord<Integer, String> queueRecord3 = new ProducerRecord<>(queue, 0,timestamp,id, gson.toJson(q3));
				Queue q4 = new Queue("Q2" );
//				ProducerRecord<Integer, String> queueRecord4 = new ProducerRecord<>(queue, 1, timestamp, 0,gson.toJson(q4));


				kafkaTemplate.send(queueRecord1);
//				kafkaTemplate.send(queueRecord2);
				kafkaTemplate.send(queueRecord3);
//				kafkaTemplate.send(queueRecord4);
//				Thread.sleep( 1 * 60 * 1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
	}



	private Line builderLine(String name){
		Random random = new Random();
		return new Line(name,random.nextDouble(),random.nextDouble(),random.nextDouble(), random.nextDouble(), System.currentTimeMillis());
	}

	private List<Line> historyList(String name){
		List<Line> list = new ArrayList<>();
		for (int i = 0; i < 50; i++) {
			Line line = builderLine(name);
			list.add(line);
		}
		return list;
	}

	@Autowired
	private Properties kafkaStreamsConfig;



	/**
	 * window count in local store
	 * @param topic
	 * @param storeName
	 * @return
	 */
	private KafkaStreams localStoreWindowStreams(String topic, String storeName){
		StreamsBuilder sb = new StreamsBuilder();
//		sb.stream(topic)
//				.groupByKey()
////				.windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
//				.windowedBy(SlidingWindows.withTimeDifferenceAndGrace(Duration.ofMinutes(1),Duration.ZERO))
//				.count(Materialized.as(storeName));
//		kafkaStreamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, KafkaConfig.STREAMS_STATE_DATA_DIR+storeName);
//		kafkaStreamsConfig.put(StreamsConfig.CLIENT_ID_CONFIG, KafkaConfig.randomClientId());
		return new KafkaStreams(sb.build(), kafkaStreamsConfig);
	}


	private void sendRecordTopic(String topic){
		for (int i = 0; i < 1000; i++) {
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
	private List<String> branchList(){
		List<String> list = new ArrayList<>();
		list.add("上海分公司");
		list.add("广东分公司");
		list.add("江苏分公司");
		list.add("浙江分公司");
		list.add("福建分公司");
		list.add("山东分公司");
		list.add("辽宁分公司");
		list.add("湖北分公司");
		list.add("湖南分公司");
		list.add("河南分公司");
		list.add("北京分公司");
		list.add("海南分公司");
		list.add("天津分公司");
		list.add("云南分公司");
		list.add("大连分公司");
		list.add("厦门分公司");
		list.add("青岛分公司");
		list.add("深圳分公司");
		list.add("四川分公司");
		list.add("安徽分公司");
		list.add("新疆分公司");
		list.add("甘肃分公司");
		list.add("河北分公司");
		list.add("黑龙江分公司");
		list.add("山西分公司");
		list.add("江西分公司");
		list.add("贵州分公司");
		list.add("重庆分公司");
		list.add("陕西分公司");
		list.add("广西分公司");
		list.add("宁夏分公司");
		list.add("青海分公司");
		list.add("吉林分公司");
		list.add("西藏分公司");
		return list;
	}

	private List<String> centerList(){
		List<String> list = new ArrayList<>();
		list.add("上海中心");
		list.add("北京中心");
		return list;
	}

	private List<String> lineList(){
		List<String> lineList = new ArrayList<>();
		List<String> branchList = branchList();
		List<String> centerList = centerList();
		for (String center : centerList){
			for (String branch: branchList){
				lineList.add(center + " - " + branch);
			}
		}
		return lineList;
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
}

