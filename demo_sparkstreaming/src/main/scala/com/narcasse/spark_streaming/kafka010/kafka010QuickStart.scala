package com.narcasse.spark_streaming.kafka010

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
1, 复制配置文件到 resource目录下
2，配置环境变量
3，spark-defaults.conf
 */
object kafka010QuickStart {

   def main(args: Array[String]) {
      //    创建一个批处理时间是2s的context 要增加环境变量
      val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("yarn-client")
        .set("yarn.resourcemanager.hostname", "mt-mdh.local")
        .set("spark.executor.instances","2")
        .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
          ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
          ,"/opt/jars/kafka-clients-0.10.2.2.jar"
          ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
      val ssc = new StreamingContext(sparkConf, Seconds(5))


      //    使用broker和topic创建DirectStream
      val topicsSet = "test".split(",").toSet
      val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer"-> classOf[StringDeserializer],
        "group.id"->"test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit"->(false: java.lang.Boolean))

      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

      // Get the lines, split them into words, count the words and print
      val lines = messages.map(_.value)
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
      wordCounts.print()

      //    启动流
      ssc.start()
      ssc.awaitTermination()
    }
}
