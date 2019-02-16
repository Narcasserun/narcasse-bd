package com.narcasse.spark_streaming.kafka010

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._
/*
   
*/

object Kafka010ManualOffsets2 {

  def main(args: Array[String]): Unit = {

    //1.创建配置项
    var  conf = new SparkConf().setAppName("Kafka010ManualOffsets2").setMaster("local[4]")
    //创建streamingcontext
    var sc = new StreamingContext(new SparkContext(conf),Seconds(5))

    var topic = "test".split(",").toSet

     val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer"-> classOf[StringDeserializer],
        "group.id"->"test4",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit"->(false: java.lang.Boolean))

    //创建inputDstream
    val message = KafkaUtils.createDirectStream(sc,LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String,String](topic,kafkaParams,getLastOffsets(kafkaParams,topic))
    )
    message.foreachRDD(
      rdd=>{
        // 漂亮的代码 转换rdd为Array[OffsetRange]

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          iter.foreach(each=>{
            each.offset();
            each.value();
            println(each)
          });
        }

        println(rdd.count())
        println(offsetRanges)
        // 手动提交offset ，前提是禁止自动提交
        message.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    )


  }

  def getLastOffsets(kafkaParams : Map[String, Object],topics:Set[String]): Map[TopicPartition, Long] ={
    val props = new Properties()
    props.putAll(kafkaParams.asJava)
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(topics.asJavaCollection)
    paranoidPoll(consumer)
    val map = consumer.assignment().asScala.map { tp =>
      println(tp+"---" +consumer.position(tp))
      tp -> (consumer.position(tp))
    }.toMap
    println(map)
    consumer.close()
    map
  }
  def paranoidPoll(c: Consumer[String, String]): Unit = {
    val msgs = c.poll(0)
    if (!msgs.isEmpty) {
      // position should be minimum offset per topicpartition
      msgs.asScala.foldLeft(Map[TopicPartition, Long]()) { (acc, m) =>
        val tp = new TopicPartition(m.topic, m.partition)
        val off = acc.get(tp).map(o => Math.min(o, m.offset)).getOrElse(m.offset)
        acc + (tp -> off)
      }.foreach { case (tp, off) =>
        c.seek(tp, off)
      }
    }
  }

}
