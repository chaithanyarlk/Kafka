package com.example.serde;

import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.model.*;

public class InvoiceSerializer implements Serializer<Invoice> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, Invoice data) {
        // Serialize CustomKey object to bytes
        // Implement serialization logic here

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void close() {}
}



// Similar custom serializer and deserializer classes for CustomValue
