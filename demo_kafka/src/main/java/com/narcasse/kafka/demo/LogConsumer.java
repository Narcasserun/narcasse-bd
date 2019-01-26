package com.narcasse.kafka.demo;


import com.narcasse.kafka.dao.MySqlClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu1:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "nc_test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test"));

        try {
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(100);

                Thread.sleep(1000);

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String logStr = new String(record.value());

 //time|level|thread_name|method_name|error_msg
 //2018-05-01 23:40:26.704|ERROR|main|com.gupao.kafka.LogProducer|hello Log4jToKafka test8
                    System.out.println(logStr);


                    String[] splits = logStr.split("\\|");

                    Map<String, String> logEntry = new HashMap();
                    logEntry.put("time",splits[0]);
                    logEntry.put("level",splits[1]);
                    logEntry.put("thread_name",splits[2]);
                    logEntry.put("method_name",splits[3]);
                    logEntry.put("error_msg",splits[4]);

                    MySqlClient sqlClient = new MySqlClient();
                    sqlClient.insertLog(logEntry);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
