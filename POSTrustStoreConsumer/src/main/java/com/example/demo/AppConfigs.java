package com.example.demo;

public class AppConfigs {
    public final static String applicationID = "StateStore";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String posTopicName = "pos";
    public final static String notificationTopic = "loy";
    public final static String CUSTOMER_TYPE_PRIME = "PRIME";
    public final static Double LOYALTY_FACTOR = 0.02;
    
    public final static String STATE_STORE_NAME = "InMemoryKeyValueStateStore";
    public final static String TEMP_TOPIC = "temp";
    public final static String REPORT = "report";
}
