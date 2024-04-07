package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.example.model.Invoice;
import com.example.serde.*;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker addresses
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer for message keys
        properties.put("value.serializer", InvoiceSerializer.class.getName()); // Serializer for message values

        Producer<String,Invoice> producer = new KafkaProducer<>(properties);


        int numThreads = 10;

        Dispatcher dispatcher = new Dispatcher(producer, "topic");

        Thread[] threads = new Thread[numThreads];

        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Dispatcher(producer, "topic"));
            threads[i].start();
        }

        try {
            for (int i = 0; i < numThreads; i++) {
                threads[i].join(); // Wait for each thread to complete
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("All threads have completed.");

        producer.close();

    }
}