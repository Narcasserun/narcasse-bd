package com.narcasse.kafka.demo;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class WordProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "todo:");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer(props);

        boolean stop = false;
        Random random = new Random();

        System.out.println("send message to kafka ....");

        while (!stop) {
            try {
                String i = String.valueOf(random.nextInt(100) % 3);
                String message = "hello" + i + " world" + i;
                ProducerRecord<String, String> record = new ProducerRecord("to", i, message);
                RecordMetadata rm = producer.send(record).get();
                System.out.println("message: " + message + ", partition=" + rm.partition() + ", offset=" + rm.offset());
                Thread.sleep(1000);
            } catch (Exception e) {
                stop = true;
                producer.close();
                System.err.println(e);
            }
        }
    }
}
