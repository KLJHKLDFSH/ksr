package com.store.ksr;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
public class KsrApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(KsrApplication.class);
	@Autowired
	private KafkaStreams sunStreams;
	@Autowired
	private KafkaStreams distributedStreams;
	@Autowired
	private KafkaTemplate<Integer,String> kafkaTemplate;
	@Autowired
	private ThreadPoolTaskExecutor threadPool;

	public static void main(String[] args) {
		SpringApplication.run(KsrApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		distStreamsRunning(args);
	}

	private void distStreamsRunning(String... args){
		Integer key = Integer.parseInt(args[0]);
		String value = args[1];
		syncSend("dist", key,value);
		distributedStreams.start();
		while (distributedStreams.state().isRunningOrRebalancing()){
			try {
				Thread.sleep(5000);
				log.info("check state store");
				ReadOnlyKeyValueStore<Integer, Long> countStore =
						distributedStreams.store(StoreQueryParameters.fromNameAndType("distributedCount", QueryableStoreTypes.keyValueStore()));
				countStore.all().forEachRemaining(kv-> log.info("store, key:{},value:{}",kv.key,kv.value));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}


	}

	public void sunStreamsRunning(String... args){
		//		sunStreams.start();
//		if(args.length > 2 && Objects.equals(args[0], "send")){
//			log.info("send data");
//			keepAliveSend("test",args[1]);
//		}
//		while (args.length > 2 && args[2].equals("check") && sunStreams.state().isRunningOrRebalancing()){
//
//			try {
//				Thread.sleep(5000);
//				log.info("check state store");
////				sunStreams.allMetadata().forEach(metadata -> {
////					log.info("host:{},port:{}",metadata.host(),metadata.port());
////				});
//				ReadOnlyKeyValueStore<Integer, Long> countStore =
//						sunStreams.store(StoreQueryParameters.fromNameAndType("sunStore", QueryableStoreTypes.keyValueStore()));
//				countStore.all().forEachRemaining(kv-> log.info("store, key:{},value:{}",kv.key,kv.value));
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
	}

	private void sendRecordTopic(String topic,Integer key,String value){
		for (int i = 0; i < 1000; i++){
			try {
				ProducerRecord<Integer,String> producerRecord = new ProducerRecord<>(topic,key, key,value);
				kafkaTemplate.send(producerRecord);
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	private void syncSend(String topic,Integer key,String value){
		threadPool.execute(()-> sendRecordTopic(topic, key, value));
	}
}
