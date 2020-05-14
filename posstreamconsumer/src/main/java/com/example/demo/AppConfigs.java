package com.example.demo;

public class AppConfigs {
    public final static String applicationID = "PosFanout";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String posTopicName = "pos";
    public final static String shipmentTopicName = "ship";
    public final static String notificationTopic = "loy";
    public final static String hadoopTopic = "hsink";
    public final static String DELIVERY_TYPE_HOME_DELIVERY = "HOME-DELIVERY";
    public final static String CUSTOMER_TYPE_PRIME = "PRIME";
    public final static Double LOYALTY_FACTOR = 0.02;
}
