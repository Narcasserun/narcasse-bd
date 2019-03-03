package com.narcasse.kafka.demo;


import com.narcasse.kafka.serde.Customer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {

        //第一步：配置参数
        Properties props = new Properties();

        //broker地址：建立与集群的连接，通过一个broker会自动发现集群中其他的broker,不用把集群中所有的broker地址列全，一般配置2-3个即可
        props.put("bootstrap.servers", "ubuntu1:9092");

        //consumer_group id
        props.put("group.id", "nc_test");

        //自动提交offset
//        props.put("enable.auto.commit", "true");
        props.put("enable.auto.commit", "false");
//        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "latest");

        //consumer每次发起fetch请求时,从服务器读取的数据量
        props.put("max.partition.fetch.bytes", "1000");

        //一次最多poll多少个record
        props.put("max.poll.records", "5");

        //跟producer端一致(bytes->object)
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //跟producer端一致
        props.put("value.deserializer", "com.narcasse.kafka.serde.CustomerDeserializer");

        //第二步：创建KafkaConsumer
        Consumer<String, Customer> consumer = new KafkaConsumer(props);

        //第三步：订阅消息,指定topic
        consumer.subscribe(Arrays.asList("test"));
        TopicPartition topicPartition = new TopicPartition("test",0);

        boolean stop = false;
        // 是从指定offset进行消费
        //consumer.poll(100);
        //consumer.seek(topicPartition,100);
        while (!stop) {
            try {
                //下一次的offset
                //System.out.println("下一次的offset:"+consumer.position(topicPartition));
                //第四步：消费消息
                ConsumerRecords<String, Customer> records = consumer.poll(100);

                System.out.println("获取数据量：" + records.count());

                for (ConsumerRecord<String, Customer> record : records) {
                    System.out.printf("开始消费数据:offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }

                consumer.commitSync();

                Thread.sleep(2000);
            } catch (Exception e) {
                //异常处理
                stop = true;
                consumer.close();
                System.err.println(e);
            }
        }

    }
}
