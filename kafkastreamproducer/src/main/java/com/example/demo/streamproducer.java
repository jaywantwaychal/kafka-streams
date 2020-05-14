package com.example.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class streamproducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "client");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
		
		for (int i=0 ;i < 100; i++) {
			producer.send(new ProducerRecord<Integer, String>("trial", i, "trial value:" +i));
		}
		
		producer.close();
	}

}
