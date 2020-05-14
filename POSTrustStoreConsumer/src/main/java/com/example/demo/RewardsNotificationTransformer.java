package com.example.demo;

import javax.script.Invocable;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import com.example.demo.types.Notification;
import com.example.demo.types.PosInvoice;

public class RewardsNotificationTransformer implements ValueTransformer<PosInvoice, Notification> {

	private KeyValueStore<String, Double> store;
	
	@Override
	public void init(ProcessorContext context) {
		this.store = (KeyValueStore<String, Double>) context.getStateStore(AppConfigs.STATE_STORE_NAME);
	}

	@Override
	public Notification transform(PosInvoice value) {
		Notification notification = new Notification();
		notification.setCustomerCardNo(value.getCustomerCardNo());
		notification.setInvoiceNumber(value.getInvoiceNumber());
		notification.setTotalAmount(value.getTotalAmount());
		notification.setEarnedLoyaltyPoints(value.getTotalAmount() * AppConfigs.LOYALTY_FACTOR);
		
		Double prevLoyaltyPoint = this.store.get(value.getCustomerCardNo());
		Double totalLoyaltyPoint;
		if(null != prevLoyaltyPoint) {
			totalLoyaltyPoint = prevLoyaltyPoint + notification.getEarnedLoyaltyPoints();
		}else {
			totalLoyaltyPoint = notification.getEarnedLoyaltyPoints();
		}
		
		notification.setTotalEarnedLoyaltyPoints(totalLoyaltyPoint);
		this.store.put(value.getCustomerCardNo(), totalLoyaltyPoint);
		
		return notification;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}


}
