package com.example.demo;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Json Serializer
 */
public class JsonSerDesrializer<T> implements Serializer<T>, Deserializer<T>{
    private final ObjectMapper objectMapper = new ObjectMapper();
    private  Class<T> classname;
    private final static String KEY_SERDES_CLASS_NAME = "key.serdes.class";
    private final static String VALUE_SERDES_CLASS_NAME = "value.serdes.class";
    		

    public JsonSerDesrializer() {

    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        if(isKey) {
        	classname = (Class<T>) config.get(KEY_SERDES_CLASS_NAME);
        }else {
        	classname = (Class<T>) config.get(VALUE_SERDES_CLASS_NAME);
        }
    }

    /**
     * Serialize JsonNode
     *
     * @param topic Kafka topic name
     * @param data  data as JsonNode
     * @return byte[]
     */
    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }

	@Override
	public T deserialize(String topic, byte[] data) {
		if(null == data) {
			return null;
		}else {
			try {
				return objectMapper.readValue(data, classname);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
}
