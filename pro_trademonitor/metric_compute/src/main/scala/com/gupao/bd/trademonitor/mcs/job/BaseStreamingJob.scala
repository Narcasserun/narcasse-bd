package com.gupao.bd.trademonitor.mcs.job

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

/**
  * 功能：Spark Streaming类job的基类
  **/
abstract class BaseStreamingJob(jobConf: JobConf) extends Runnable with Serializable {

  val streamingJobName = "default"
  val duration = Seconds(5)

  def createStreamingContext(appName: String, batchInterval: Duration) = {
    val conf = new SparkConf().setAppName(appName).setMaster(jobConf.SPARK_MASTER)
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1000")
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer", "16mb")
    val ssc = new StreamingContext(conf, batchInterval)
    ssc.sparkContext.setLogLevel("WARN")
    ssc
  }

  def getKafkaRDD(ssc: StreamingContext, topics: List[String], groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    val _consumerParams = Map[String, Object](
      "bootstrap.servers" -> jobConf.KAFKA_BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> java.lang.Boolean.FALSE,
      "max.partition.fetch.bytes" -> "15000060"
    )

    val input = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, _consumerParams)
    )

    input
  }

  def getKafkaRDD(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    getKafkaRDD(ssc, List(topic), groupId)
  }

  override def run(): Unit = {
    val ssc = createStreamingContext(streamingJobName, duration)
    process(ssc)
    ssc.start()
    ssc.awaitTermination()
  }

  def process(streamingContext: StreamingContext)
}
