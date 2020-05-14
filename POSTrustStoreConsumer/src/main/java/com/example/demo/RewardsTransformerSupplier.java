package com.example.demo;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

import com.example.demo.types.Notification;
import com.example.demo.types.PosInvoice;

public class RewardsTransformerSupplier implements ValueTransformerSupplier<PosInvoice, Notification> {

	@Override
	public ValueTransformer<PosInvoice, Notification> get() {
		return new RewardsNotificationTransformer();
	}

}
