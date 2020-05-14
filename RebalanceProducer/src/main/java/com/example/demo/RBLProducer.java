package com.example.demo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

public class RBLProducer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		//props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, new CustomPartition());
		
		Producer<String, String> producer = null;
		
		try {
			
			producer = new KafkaProducer<String, String>(props);
			
			ProducerRecord<String, String> record = null;
			SimpleDateFormat pattern =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			
			while (true) {
				
				record = new ProducerRecord<String, String>("rbl2", "recordsKey", pattern.format(new Date()));
				RecordMetadata meta =producer.send(record).get();
				Thread.sleep(1000);
				System.out.println("*********Partition : "+meta.partition() + "**** Data : "+meta.offset());
			}
			
			
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			producer.close();
		}
	}
}

class CustomPartition implements Partitioner{

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitionsInfo = cluster.availablePartitionsForTopic(topic);
		int partitions = partitionsInfo.size();
		return partitions%2;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
