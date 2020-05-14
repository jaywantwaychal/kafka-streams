package com.example.demo;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ScaledMultiThreadedProducer {

	public static void main(String[] args) {

		ArrayList<String> fileName = new ArrayList<String>();
		fileName.add("data/cab-flink.txt");
		fileName.add("data/cab-flink2.txt");
		
		String topicName = "multi";
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);   //exactly one sematics
		
		KafkaProducer<Integer, String> producer= new KafkaProducer<Integer, String>(props);
		Thread[] threads = new Thread[fileName.size()];
		
		
		for(int i=0 ;i < fileName.size(); i++ ) {
			threads[i] = new Thread(new ScalingRunner(producer, topicName, fileName.get(i)));
			threads[i].start();
		}
		
		try {
			for (Thread t: threads) {
				t.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			
		}finally {
			producer.close();
		}
	}

}
