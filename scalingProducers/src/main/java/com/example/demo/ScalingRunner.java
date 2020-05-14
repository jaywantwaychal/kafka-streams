package com.example.demo;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ScalingRunner implements Runnable {

	KafkaProducer<Integer, String> producer;
	String topicName;
	String fileName;
	
	
	public ScalingRunner(KafkaProducer<Integer, String> producer, String topicName, String fileName) {
		super();
		this.producer = producer;
		this.topicName = topicName;
		this.fileName = fileName;
	}


	@Override
	public void run() {
		
		File file = new File(fileName);
		
		try(Scanner scan = new Scanner(file)) {
			
			while(scan.hasNext()) {
				String line = scan.next();
				producer.send(new ProducerRecord<Integer, String>(topicName, line));
			}
			
		}catch(IOException e) {
			System.out.println(e);
		}
		
	}

}
