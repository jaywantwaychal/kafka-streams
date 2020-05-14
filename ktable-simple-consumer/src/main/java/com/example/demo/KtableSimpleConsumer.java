package com.example.demo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;

public class KtableSimpleConsumer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
		
		StreamsBuilder builder = new StreamsBuilder();
		KTable<String, String> KT0 = builder.table("ktable", Consumed.with(Serdes.String(), Serdes.String()));
		
		KT0.toStream().print(Printed.<String, String>toSysOut().withLabel("KT0"));
		
		KTable<String, String> KT1 = KT0.filter((k,v) -> v != null && v.startsWith("HDFC"));
		KT1.toStream().print(Printed.<String, String>toSysOut().withLabel("KT1"));
		
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
		
		
		Runtime.getRuntime().addShutdownHook(new Thread(() ->  {
			streams.close();
		}));

	}

}
