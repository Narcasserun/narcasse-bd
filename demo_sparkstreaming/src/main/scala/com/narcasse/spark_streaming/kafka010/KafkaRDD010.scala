package com.narcasse.spark_streaming.kafka010

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaRDD010 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances","2")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/kafka_2.11-0.10.2.2.jar"
        ,"/opt/jars/kafka-clients-0.10.2.2.jar"
        ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
      ))
    val sc = new SparkContext(sparkConf)
    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange("test", 0, 0, 100)
//      ,
//      OffsetRange("test", 1, 0, 100)
    )
    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->"test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean))
    import scala.collection.JavaConverters._
    val kafkardd = KafkaUtils.createRDD[String, String](sc, new java.util.HashMap[String, Object](kafkaParams.asJava), offsetRanges, LocationStrategies.PreferConsistent)
    println(kafkardd.count())
    sc.stop()
  }
}
