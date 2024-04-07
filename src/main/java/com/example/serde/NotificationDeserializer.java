package com.example.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;
import com.example.model.*;

public class NotificationDeserializer implements Deserializer<Notification> {

     private final ObjectMapper objectMapper = new ObjectMapper();

     @Override
     public void configure(Map<String, ?> configs, boolean isKey) {}
 
     @Override
     public Notification deserialize(String topic, byte[] data) {
         // Deserialize bytes to Invoice object
         // Implement deserialization logic here
         try {
            return objectMapper.readValue(data, Notification.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
     }
 
     @Override
     public void close() {}
 }