package com.example.demo;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.example.demo.types.Notification;
import com.example.demo.types.PosInvoice;

import serde.AppSerdes;

public class StreamStateStoreConsumer {

	public static void main(String[] args) {
		
		Properties props  = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateStore");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
		
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, PosInvoice> KS0 = builder.stream("minip4", Consumed.with(AppSerdes.String(), AppSerdes.PosInvoice()));
		
		KStream<String, PosInvoice> KS1 = KS0.filter((k,v) -> v.getCustomerType().equalsIgnoreCase(AppConfigs.CUSTOMER_TYPE_PRIME))
			.through(AppConfigs.TEMP_TOPIC, Produced.with(AppSerdes.String(), AppSerdes.PosInvoice(), new POSPartitioner()));
		
		
		//state store
		StoreBuilder kvIMstorebuilder = Stores.keyValueStoreBuilder(
				Stores.inMemoryKeyValueStore(AppConfigs.STATE_STORE_NAME),
				AppSerdes.String(), AppSerdes.Double());
		
		builder.addStateStore(kvIMstorebuilder);
		
		//do transform
		
		KStream<String, Notification> KS2 = KS1.transformValues(new RewardsTransformerSupplier(), AppConfigs.STATE_STORE_NAME);
			
		KS2.to(AppConfigs.notificationTopic, Produced.with(AppSerdes.String(), AppSerdes.Notification()));
		
		KS2.mapValues(v -> ReportGenerator.getReport(v))
			.to(AppConfigs.REPORT, Produced.with(AppSerdes.String(), AppSerdes.Report()));
		
		Topology topology = builder.build(props);
		KafkaStreams streams = new KafkaStreams(topology, props);
		streams.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(()-> { 
			streams.close();
		}));
		
		
		
		
	}

}
