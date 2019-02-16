# spark streaming提交偏移到zookeeper

标签（空格分隔）： kafka

---
```
package com.dianru.tzq.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkException
import org.apache.spark.streaming.dstream.InputDStream
import scala.collection.mutable.HashMap

object KafkaTest01 {
  def getKafkaStream(kafkaParams: Map[String, String], topics: Set[String], ssc: StreamingContext) = {
    val kafkaCluster = new KafkaCluster(kafkaParams)
    var kafkaStream: InputDStream[(String, String)] = null
    
    val kafkaPartitionsE = kafkaCluster.getPartitions(topics)
    if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")

    val kafkaPartitions = kafkaPartitionsE.right.get
    //从zookeeper中获取offset信息
    val consumerOffsetsE = kafkaCluster.getConsumerOffsets("1", kafkaPartitions)
    //如果在zookeeper中没有记录，就从最小的offset开始消费
    if (consumerOffsetsE.isLeft) {
      kafkaParams + ("auto.offset.reset" -> "smallest")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      val consumerOffsets = consumerOffsetsE.right.get

      val latestOffests = kafkaCluster.getLatestLeaderOffsets(kafkaPartitions).right.get
      val earliestOffests = kafkaCluster.getEarliestLeaderOffsets(kafkaPartitions).right.get
      var newConsumerOffsets: HashMap[TopicAndPartition, Long] = HashMap()

      consumerOffsets.foreach(f => {
        val max = latestOffests.get(f._1).get.offset
        val min = earliestOffests.get(f._1).get.offset
        newConsumerOffsets.put(f._1, f._2)
        //如果zookeeper中记录的offset在kafka中不存在(已经过期),就指定其现有kafka的最小offset位置开始消费
        if (f._2 < min) {
          newConsumerOffsets.put(f._1, min)
        }
        println(max + "-----" + f._2 + "--------" + min)
      })

      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, newConsumerOffsets.toMap, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))

    }
    
    kafkaStream.foreachRDD(rdd => {
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      for (offsets <- offsetsList) {
        val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
        // 更新offset到kafkaCluster  
        val o = kafkaCluster.setConsumerOffsets("1", Map(topicAndPartition -> offsets.untilOffset))
        if (o.isLeft) {
          println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
        }
      }
    })
    kafkaStream
  }
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Test01").setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topics = Set("tzq")
    val brokers = "192.168.1.32:9092,192.168.1.33:9092,192.168.1.34:9092" 
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder") //, "auto.offset.reset" -> "smallest"

    var kafkaStream = getKafkaStream(kafkaParams: Map[String, String], topics: Set[String], ssc: StreamingContext)

    val events = kafkaStream.flatMap(line => {
      Some(line._2)
    })
    events.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
```

问题：获取KafkaCluster报错：
```
val brokers = "192.168.1.32:9092,192.168.1.33:9092,192.168.1.34:9092" 
//"192.168.101.124:9092"    val kafkaParams = Map[String, String](      "metadata.broker.list" -> brokers, "serializer.class" -> "kafka.serializer.StringEncoder") //, "auto.offset.reset" -> "smallest"    val kafkaCluster = new KafkaCluster(kafkaParams)
```
调用new KafkaCluster(kafkaParams)方法的时候报错，说找不到这个类。
报错信息为：not found: type KafkaCluster 
但是查看jar包中确实是有这个类：


原因：该类是私有的所以不能外部访问，将该类的源码拷出来贴到复制到自己的工程下。并将代码中的private[spark]都删掉

更新zookeeper中的信息：
```
kafkaStream.foreachRDD(rdd => {

      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges


      for (offsets <- offsetsList) {

        val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)

        // 更新offset到kafkaCluster  

        val o = kafkaCluster.setConsumerOffsets("1", Map(topicAndPartition -> offsets.untilOffset))

        if (o.isLeft) {

          println(s"Error updating the offset to Kafka cluster: ${o.left.get}")

        }

      }

    })
```



