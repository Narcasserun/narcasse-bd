package com.narcasse.spark_streaming.kafka082

import java.util

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object directStream {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[7]")
      //.set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances", "localhost")
      .set("spark.driver.bindAddress", "localhost")
      .set("spark.executor.memory", "512m")
      .set("spark.driver.memory", "512m")
      .set("spark.default.parallelism","6")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        , "/opt/jars/kafka_2.11-0.8.2.2.jar"
        , "/opt/jars/kafka-clients-0.8.2.2.jar"
        , "/opt/jars/metrics-core-2.2.0.jar"
        , "/opt/jars/spark-streaming-kafka-0-8_2.11-2.3.1.jar"
      ))

    val sc = new SparkContext(sparkConf)
   // sc.addJar("/opt/jars/kafka_2.11-0.8.2.2.jar")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> "ubuntu1:9092"
      , "zookeeper.connect" -> "ubuntu1:2181/kaf"
      , "zookeeper.connection.timeout.ms" -> "10000")
    val topicsSet = Set("test2")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    // 获取分区仅在第一次转换，所以浪尖建议大家使用foreachRDD，然后使用rdd的操作算子进行计算。
    lines.foreachRDD(rdd => {


      //代表原始kafkardd分区 1
      println("kafkardd partitions num is  "+rdd.partitions.size)

//      代表map转换操作分区变华 1
      val rddmap = rdd.map(_._2).flatMap(_.split(" ")).map((_,1))
      println("rdd map partition num is "+rddmap.partitions.size)

//      非shuffle 分区变化 2
      val rddunion = rdd.union(rdd)
      println("union partition size is "+ rddunion.partitions.size)

//      shuffle分区变化，默认并行度 12设置并行度的话分区数是并行度
      val reducerdd = rddmap.repartition(12).reduceByKey((_+_))
      println("shuffle partition size " + reducerdd.partitions.size)

//      指定fhuffle分区数 2
      val rddgiven = rddmap.reduceByKey((_+_),2)
      println("rddgiven partition size " + rddgiven.partitions.size)

//      直接重分区 4
      val repartitionrdd = rdd.repartition(4)
      println("repartitionrdd partition size " + repartitionrdd.partitions.size)
    // 2
      val coalescerdd = repartitionrdd.coalesce(2)
      println("coalescerdd partition size " + coalescerdd.partitions.size)

    })
    ssc.start()
    ssc.awaitTermination()
  }
}
