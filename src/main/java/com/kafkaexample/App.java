package com.kafkaexample;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

public class App {
  private static KafkaProducer<String, String> producer = null;
  private static KafkaConsumer<String, String> consumer = null;

  private static void createProducer() {
    System.out.println("Creating producer...");
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    producer = new KafkaProducer<>(properties);
    System.out.println("Done!");
  }

  private static void createConsumer() {
    System.out.println("Creating consumer...");
    var properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "host.docker.internal:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumer = new KafkaConsumer<>(properties);
    System.out.println("Done!");
  }

  static {
    createProducer();
    createConsumer();

  }

  public static KafkaProducer<String, String> getProducer() {
    return producer;
  }

  public static KafkaConsumer<String, String> getConsumer() {
    return consumer;
  }

  public static void dispatchMessage(String topic, String message) {
    getProducer().send(new ProducerRecord<>(topic, message));
  }

  public static void main(String[] args) {
    try {
      System.out.println("Running...");
      dispatchMessage("my-topic", "Hello from Kafka!");
      System.out.println("Message dispatched.");

      var consumer = getConsumer();
      consumer.subscribe(Collections.singletonList("my-topic"));
      System.out.println("Polling for message...");
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
      for (var record : records) {
        System.out.println(record.value());
      }
    } finally {
      consumer.close();
      producer.close();
    }
  }
}