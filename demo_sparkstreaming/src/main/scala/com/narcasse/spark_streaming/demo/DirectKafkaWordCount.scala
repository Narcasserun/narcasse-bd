package com.narcasse.spark_streaming.demo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 功能：从Kafka消费数据
  **/
object DirectKafkaWordCount extends StreamingExample {

  val Array(brokers, groupId, topics) = args

  // Create context with 2 second batch interval
  val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  // Create direct kafka stream with brokers and topics
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    //三种调度consumer的策略(分区分配)：1、PreferConsistent 2、PreferBrokers 3、PreferFixed
    LocationStrategies.PreferConsistent,

    //三种消费策略：1、Subscribe：指定topic 2、SubscribePattern:通过正则来匹配topic(动态新增要消费的topic) 3、Assign:指定topic和partition
    ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

  // Get the lines, split them into words, count the words and print
  val lines = messages.map(_.value)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
  wordCounts.print()

  // Start the computation
  ssc.start()
  ssc.awaitTermination()
}
