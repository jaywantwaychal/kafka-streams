package com.example.demo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.example.types.PosInvoice;

public class MinipeojectConsumer {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonSerDesrializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"MiniProjectGrp");
		props.put("key.serdes.class", StringDeserializer.class);
		props.put("value.serdes.class", PosInvoice.class);
		
		Properties produprops = new Properties();
		produprops.put(ProducerConfig.CLIENT_ID_CONFIG, "client");
		produprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		produprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		produprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerDesrializer.class);
		
		KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(props);
		consumer.subscribe(Arrays.asList("minip"));
		
		KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(produprops);
		
		while(true) {
			ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));
			for( ConsumerRecord<String, PosInvoice> record: records) {
				if(isValid(record)) {
					producer.send(new ProducerRecord<String, PosInvoice>("valid", record.value().getStoreID(), record.value()));
				}else {
					producer.send(new ProducerRecord<String, PosInvoice>("invalid", record.value().getStoreID(), record.value()));
				}
			}
			consumer.commitAsync();
		}
	}

	private static boolean isValid(ConsumerRecord<String, PosInvoice> record) {
		if(record.value().getDeliveryType().equalsIgnoreCase("HOME-DELIVERY") && record.value().getDeliveryAddress().getContactNumber() != null) {
			return true;
		}
		return false;
	}

}
