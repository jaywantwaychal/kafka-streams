package com.example.demo;

import org.apache.kafka.streams.processor.StreamPartitioner;

import com.example.demo.types.PosInvoice;

public class POSPartitioner implements StreamPartitioner<String, PosInvoice> {

	@Override
	public Integer partition(String topic, String key, PosInvoice value, int numPartitions) {
		int partitionNumber = value.getCustomerCardNo().hashCode() % numPartitions;
		if(partitionNumber >= 0)
			return partitionNumber;
		else
			return 0;
	}

}
