package com.example.demo;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener implements ConsumerRebalanceListener{

	public RebalanceListener(Consumer<String, String> consumer) {
		
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		
		partitions.stream().forEach(tp -> System.out.println("partition from revoked: "+tp.partition()));
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		partitions.stream().forEach(tp -> System.out.println("partition from assigned: "+tp.partition()));
	}

}
