package com.example.model;

import java.util.List;

public class Invoice {
     String name;

     float price;

     List<Item> items;

     public String getName(){
          return this.name;
     }

     public float getPricee(){
          return this.price;
     }

     public void setName(String name){
          this.name = name;
     }

     public void setPrice(float price){
          this.price = price;
     }

     public void setItems(List<Item> items){
          this.items= items;
     }

     public List<Item> getItems(){
          return this.items;
     }
}
