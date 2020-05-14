package com.example.demo;

import java.util.ArrayList;
import java.util.List;

import guru.learningjournal.kafka.examples.types.HadoopRecord;
import guru.learningjournal.kafka.examples.types.LineItem;
import guru.learningjournal.kafka.examples.types.Notification;
import guru.learningjournal.kafka.examples.types.PosInvoice;

/**
 * Transform Invoice to other Objects
 *
 * @author prashant
 * @author www.learningjournal.guru
 */

class RecordBuilder {
    /**
     * Create a flattened List of HadoopRecords from Invoice
     * Each Line Item is denormalized into an independent record
     *
     * @param invoice PosInvoice object
     * @return List of HadoopRecord
     */
    static List<HadoopRecord> getHadoopRecords(PosInvoice invoice) {
        List<HadoopRecord> records = new ArrayList<>();
        for (LineItem i : invoice.getInvoiceLineItems()) {
            HadoopRecord record = new HadoopRecord();
            record.setInvoiceNumber(invoice.getInvoiceNumber());
                    record.setCreatedTime(invoice.getCreatedTime());
                    record.setStoreID(invoice.getStoreID());
                    record.setPosID(invoice.getPosID());
                    record.setCustomerType(invoice.getCustomerType());
                    record.setPaymentMethod(invoice.getPaymentMethod());
                    record.setDeliveryType(invoice.getDeliveryType());
                    record.setItemCode(i.getItemCode());
                    record.setItemDescription(i.getItemDescription());
                    record.setItemPrice(i.getItemPrice());
                    record.setItemQty(i.getItemQty());
                    record.setTotalValue(i.getTotalValue());
            if (invoice.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY)) {
                record.setCity(invoice.getDeliveryAddress().getCity());
                record.setState(invoice.getDeliveryAddress().getState());
                record.setPinCode(invoice.getDeliveryAddress().getPinCode());
            }
            records.add(record);
        }
        return records;
    }

    /**
     * Set personally identifiable values to null
     *
     * @param invoice PosInvoice object
     * @return masked PosInvoice object
     */
    static PosInvoice getMaskedInvoice(PosInvoice invoice) {
        invoice.setCustomerCardNo(null);
        if (invoice.getDeliveryType().equalsIgnoreCase(AppConfigs.DELIVERY_TYPE_HOME_DELIVERY)) {
            invoice.getDeliveryAddress().setAddressLine(null);
            invoice.getDeliveryAddress().setContactNumber(null);
        }
        return invoice;
    }

    /**
     * Transform PosInvoice to Notification
     *
     * @param invoice PosInvoice Object
     * @return Notification Object
     */
    static Notification getNotification(PosInvoice invoice) {
        Notification notification = new Notification();
        notification.setInvoiceNumber(invoice.getInvoiceNumber());
        notification.setCustomerCardNo(invoice.getCustomerCardNo());
        notification.setTotalAmount(invoice.getTotalAmount());
        notification.setEarnedLoyaltyPoints(invoice.getTotalAmount() * AppConfigs.LOYALTY_FACTOR);
        return notification;
    }
}
