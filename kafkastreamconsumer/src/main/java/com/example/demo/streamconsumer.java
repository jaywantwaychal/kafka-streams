package com.example.demo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class streamconsumer {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "client");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<Integer, String> kstream = builder.stream("trial");
		kstream.foreach((k,v) -> System.out.println("Key: "+k+"  value: "+v));
		Topology topology = builder.build();
		
		KafkaStreams kafkastream = new KafkaStreams(topology, props);
		kafkastream.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() ->  {
			kafkastream.close();
		}));

	}

}
