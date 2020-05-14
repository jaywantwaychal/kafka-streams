package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.example.datagenerator.InvoiceGenerator;
import com.example.types.PosInvoice;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ProducerExecutor implements Runnable {

	private KafkaProducer<String, PosInvoice> producer;
	private String topicName;
	private InvoiceGenerator invoiceGenerator;
	private long producerSpeed;
	GsonBuilder builder = new GsonBuilder();
	Gson gson = null;
	

	/*
	 * public ProducerExecutor(KafkaProducer<String, PosInvoice> producer, String
	 * topicName, InvoiceGenerator InvoiceGenerator, long producerSpeed) { super();
	 * this.producer = producer; this.topicName = topicName; this.invoiceGenerator =
	 * InvoiceGenerator; this.producerSpeed = producerSpeed; this.gson =
	 * builder.create(); }
	 */
	
	public ProducerExecutor(KafkaProducer<String, PosInvoice> producer2, String topicName,
			InvoiceGenerator InvoiceGenerator, long producerSpeed) {
		super();
		this.producer = producer2;
		this.topicName = topicName;
		this.invoiceGenerator = InvoiceGenerator;
		this.producerSpeed = producerSpeed;
		this.gson = builder.create();
	}

	@Override
	public void run() {
		while (true) {
			try {
			PosInvoice invoice = invoiceGenerator.getNextInvoice();
			String jsonmsg = gson.toJson(invoice);
		//	System.out.println("Invoice : " + gson.toJson(jsonmsg));
			
			producer.send(new ProducerRecord<>(topicName, invoice.getStoreID(), invoice));
			
				Thread.sleep(producerSpeed);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
