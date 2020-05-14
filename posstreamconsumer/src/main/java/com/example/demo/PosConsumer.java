package com.example.demo;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import serde.AppSerdes;

public class PosConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "POSApplication");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, PosInvoice> KS0 = builder.stream("minip3", Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));

		KS0.filter((k,v) -> v.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY))
			.to(AppConfigs.shipmentTopicName, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice()));
		
		KS0.filter((k,v) -> v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
			.mapValues(v -> RecordBuilder.getNotification(v))
			.to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));
		
		KS0.mapValues(v -> RecordBuilder.getMaskedInvoice(v))
			.flatMapValues(v -> RecordBuilder.getHadoopRecords(v))
			.to(AppConfigs.hadoopTopic, Produced.with(AppSerdes.String(), AppSerdes.HadoopRecord()));
		
		
		Topology topology = builder.build(props);
		KafkaStreams kstreams = new KafkaStreams(topology, props);
		kstreams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			kstreams.close();
		}));
		
	}

}
