package com.narcasse.kafka.demo;


import com.narcasse.kafka.serde.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MyProducer {

    public static void main(String[] args) {

        //第一步：配置参数，影响producer的生产行为
        Properties props = new Properties();

        //broker地址：建立与集群的连接，通过一个broker会自动发现集群中其他的broker,不用把集群中所有的broker地址列全，一般配置2-3个即可
        props.put("bootstrap.servers", "ubuntu1:9092");

        //是否确认broker完全接收消息：[all, -1, 0, 1]
        props.put("acks", "0");

        //失败后消息重发的次数：可能导致消息的次序发生变化
        props.put("retries", 3);

        //单批发送的数据条数
        props.put("batch.size", 16384);

        //数据在producer端停留的时间：设置为0，则立即发送
        props.put("linger.ms", 1);

        //数据缓存大小
        props.put("buffer.memory", 33554432);

        //key序列化方式
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //value序列化方式
        props.put("value.serializer", "com.narcasse.kafka.serde.CustomerSerializer");

        //重新设置分区器
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.gupao.kafka.partition.CustomerPartitioner");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.narcasse.kafka.partition.MyPartitioner");
        //第二步：创建KafkaProducer
        KafkaProducer<String, Customer> producer = new KafkaProducer(props);

        boolean stop = false;
        int i = 0;

        while (!stop) {
            try {

                i++;
                Customer customer = new Customer(i, "name" + i);

                //第三步：构建一条消息
                ProducerRecord<String, Customer> record = new ProducerRecord<>("test", Integer.toString(i), customer);

                //第四步：向broker发送消息
                RecordMetadata rm = producer.send(record).get();

                System.out.println("send message:" + customer + ", partition=" + rm.partition() + ", offset=" + rm.offset());

                Thread.sleep(1000);

            } catch (Exception e) {
                stop = true;
                //关闭KafkaProducer，将尚未发送完成的消息全部发送出去
                producer.close();
                System.err.println(e);
            }
        }
    }
}
