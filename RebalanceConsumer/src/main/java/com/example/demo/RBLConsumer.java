package com.example.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class RBLConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"RBL_Consumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		
		Consumer<String, String> consumer= new KafkaConsumer<String, String>(props);
		
		RebalanceListener list = new RebalanceListener(consumer);
		
		consumer.subscribe(Arrays.asList("rbl2"));
		try {
			
			while (true) {
				ConsumerRecords<String, String> records= consumer.poll(10);
				for(ConsumerRecord<String, String> record : records) {
					System.out.println("Received :: "+ record.value());
				}
				consumer.commitAsync();
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			consumer.commitSync();
			consumer.close();
		}
	}
}
