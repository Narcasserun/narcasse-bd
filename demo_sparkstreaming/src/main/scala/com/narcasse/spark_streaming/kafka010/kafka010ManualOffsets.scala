package com.narcasse.spark_streaming.kafka010

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.JavaConverters._

/*
 测试：
   100条数据

   处理一个批次，停掉，从最大offset恢复
 */
object kafka010ManualOffsets {
   def main(args: Array[String]) {
      //    创建一个批处理时间是2s的context 要增加环境变量
      val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[4]")
        //.set("yarn.resourcemanager.hostname", "mt-mdh.local")
        //.set("spark.executor.instances","2")
       // .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
         // ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
         // ,"/opt/jars/kafka-clients-0.10.2.2.jar"
         // ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
      val ssc = new StreamingContext(sparkConf, Seconds(5))


      //    使用broker和topic创建DirectStream
      val topicsSet = "test".split(",").toSet
      val kafkaParams = Map[String, Object]("bootstrap.servers" -> "ubuntu1:9092",
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer"-> classOf[StringDeserializer],
        "group.id"->"test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit"->(false: java.lang.Boolean))

     // 没有接口提供 offset
      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams,getLastOffsets(kafkaParams ,topicsSet)))//

     messages.foreachRDD(rdd=>{
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
       messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
     })
      //    启动流
      ssc.start()
      ssc.awaitTermination()
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
