package com.example;

import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.example.model.*;


class Dispatcher extends Thread {
     Producer<String, Invoice>producer;
     String topic;
     ObjectMapper objectMapper;
     public Dispatcher(Producer<String, Invoice>producer,String topic){
          this.producer = producer;
          this.topic = topic;
          this.objectMapper =  new ObjectMapper();
     }

     public Invoice getRecord(String name){
          Invoice invoice = new Invoice();
          invoice.setName(name);
          int randomNumber = (int) (Math.random() * 100);
          invoice.setPrice(randomNumber);
          List<Item> items = new ArrayList<Item>();
          for(int i = 0;i<2;i++){
               Item item = new Item();
               item.itemName = name + "item"+String.valueOf(i);
               item.qty = i + randomNumber;
               items.add(item);
               System.out.println("tem name item.itemName "+item.itemName); 
          }
          invoice.setItems(items);

          return invoice;
          
     }
     public void run(){
          for (int i = 0;i < 2;i++){
               String name = Thread.currentThread().getName();
               
               Invoice invoice = getRecord(name);

               producer.send(new ProducerRecord<>(this.topic, "key", invoice));
          }
     }
}