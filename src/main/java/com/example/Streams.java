package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.example.model.Invoice;
import com.example.model.Notification;
import com.example.serde.*;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class Streams {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker addresses
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // Serializer for message keys
        properties.put("value.serializer", InvoiceSerializer.class.getName()); // Serializer for message values

        StreamsBuilder builder = new StreamsBuilder();

        Serde<Invoice> valueSerde = Serdes.serdeFrom(new InvoiceSerializer(), new InvoiceDeserializer());

        Serde<Notification> notifSerde = Serdes.serdeFrom(new NotificationSerializer(), new NotificationDeserializer());

        KStream<String, Invoice> inputStream = builder.stream("input-topic", Consumed.with(Serdes.String(), valueSerde));

        // Example in which all the items which have name not null needs to go for other topic ship
        inputStream.filter((key,value)->{
          if(Objects.isNull(value.getName())){
               return false;
          }
          return true;
        }).to("ship",Produced.with(Serdes.String(), valueSerde));

        // Example in which the Items are serialized and their sum is calculated and sent to notif topic for warehouse
        inputStream.filter((key,value)->{
          if(Objects.isNull(value.getName())){
               return false;
          }
          return true;
        }).mapValues( value -> {
          Notification notification = new Notification();
          int total = 0;
          for(int i = 0 ;i < value.getItems().size();i++){
               total += value.getItems().get(i).qty;
          }
          notification.setTotal(total);
          notification.setName(value.getName());
          return notification;
        }).to("notif",Produced.with(Serdes.String(), notifSerde));

        // State example  
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("stateStoreName"),
                        Serdes.String(),
                        Serdes.Integer()
                );

        // Add the state store builder to the builder
        builder.addStateStore(storeBuilder);

        inputStream.filter((key,value)->{
          if(Objects.isNull(value.getName())){
               return false;
          }
          return true;
        }).process(null, args)


        // Build the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        // Start the Kafka Streams application
        streams.start();

        // Add shutdown hook to gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    



    }
}