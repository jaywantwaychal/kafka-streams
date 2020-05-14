package com.example.demo;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public class KtableSimpleProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "client");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		String[] records = new String[]{"HDFC:200.00", "LIC:536.14","HDFC:204.00", "LIC:","HDFC:2010.00", "AXIS:123"};
		Random r = new Random();
		
		for(int i=0; i<100; i++) {
			 
			producer.send(new ProducerRecord<String, String>("ktable", i+"-record" , records[r.nextInt(6)]));
		}
		
		producer.close();
	}

}
