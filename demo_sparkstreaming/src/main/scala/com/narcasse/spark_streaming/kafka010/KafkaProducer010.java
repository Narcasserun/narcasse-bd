package com.narcasse.spark_streaming.kafka010;

import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;
/*
  1. 分区

  2. 数据本地性

  3. 公平调度
 */
public class KafkaProducer010 {
	public static void main(String [] args)  {

		Properties props = new Properties();
		props.put("bootstrap.servers", "ubuntu1:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
		Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
		java.util.Random points =new java.util.Random(2);

		List list = new ArrayList<String>();
		list.add("hadoop");
		list.add("hive");
		list.add("spark");
		list.add("flink");
		list.add("kafka");
		list.add("kylin");
		list.add("redis");

//		for(int j = 0;j < 100;j++)
		for(int i = 0; i < 6; i++){

//			String send = points.nextInt(2)+" "+points.nextInt(2);
			JSONObject json = new JSONObject();
			json.put("id",i);
			json.put("timestamp",System.currentTimeMillis());
			producer.send(new ProducerRecord<String, String>("upstate", json.toString()));
//
			System.out.println("");
//			producer.send(new ProducerRecord<String, String>("alluxioTest", (String) list.get(points.nextInt(7))));

			try {
				sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		producer.close();
	}
}
