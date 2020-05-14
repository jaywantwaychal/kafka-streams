package com.example.demo;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.datagenerator.InvoiceGenerator;
import com.example.types.PosInvoice;

public class MiniProjectProducer {

	public static void main(String[] args) {
		
		String topicName = "minip4";
		int numPOS =10; 
		long producerSpeed = 1500;
		
		Properties props = new Properties();
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "stateStore");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		
		KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(props);
		
	//	KafkaProducer<String, String> producer1 = new KafkaProducer<String, String>(props);
		
		//producer1.send(new ProducerRecord<String, String>(topicName, null, "testing"));
		
		ExecutorService executor = Executors.newFixedThreadPool(numPOS);
		
		InvoiceGenerator genrator = InvoiceGenerator.getInstance();
		
		Thread[] threads = new Thread[10];
		try {
			int i = 10;
			while (i> 0) {
				
				i--;
			//	using threads array
			//	threads[i] = new Thread(new ProducerExecutor(producer, topicName, genrator, producerSpeed));
			//	threads[i].start();
				
				executor.submit(new ProducerExecutor(producer, topicName, genrator, producerSpeed));
			//	PosInvoice invoice = genrator.getNextInvoice();
			//	producer.send(new ProducerRecord<String, PosInvoice>(topicName, invoice.getStoreID(), invoice));
				
			}
			/*
			 * for(int i=0; i < numPOS; i++) { executor.submit(new
			 * ProducerExecutor(producer1, topicName, InvoiceGenerator.getInstance(),
			 * producerSpeed)); }
			 */
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
	}
}
