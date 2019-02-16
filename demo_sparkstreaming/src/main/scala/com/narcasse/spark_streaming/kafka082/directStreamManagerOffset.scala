package com.narcasse.spark_streaming.kafka082

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.mutable.HashMap

object directStreamManagerOffset {
  val groupid = "test082"

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances", "2")
      .set("spark.driver.bindAddress", "mt-mdh.local")
      .set("spark.executor.memory", "512m")
      .set("spark.driver.memory", "512m")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        , "/opt/jars/kafka_2.11-0.8.2.2.jar"
        , "/opt/jars/kafka-clients-0.8.2.2.jar"
        , "/opt/jars/metrics-core-2.2.0.jar"
        , "/opt/jars/spark-streaming-kafka-0-8_2.11-2.3.1.jar"
      ))
    val sc = new SparkContext(sparkConf)
    sc.addJar("/opt/jars/kafka_2.11-0.8.2.2.jar")
    val ssc = new StreamingContext(sc, Seconds(5))

    val kafkaParams = Map("metadata.broker.list" -> "mt-mdh.local:9092"
      , "zookeeper.connect" -> "mt-mdh.local:2181/kafka082"
      , "zookeeper.connection.timeout.ms" -> "10000")
    val topicsSet = Set("page_visits")
    // kafka 分区和生成rdd分区一一对应。
    // 同一个topic 创建多个stream 然后union没必要
    val lines = getKafkaStream(kafkaParams,topicsSet,ssc)

    // 获取分区仅在第一次转换，所以浪尖建议大家使用foreachRDD，然后使用rdd的操作算子进行计算。
    lines.foreachRDD(rdd => {
      //处理

      // 结果提交后
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val kc = new KafkaCluster(kafkaParams)

      offsetRanges.foreach(o => {
        val topicAndPartition = TopicAndPartition("page_visits", o.partition)
        val res = kc.setConsumerOffsets(groupid, Map((topicAndPartition, o.untilOffset)))
        if (res.isLeft) {
          println(s"Error updating the offset to Kafka cluster: ${res.left.get}")
        }
        // page_visits 0 699 699
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      })
//    0
      println(rdd.count())
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaStream(kafkaParams: Map[String, String], topics: Set[String], ssc: StreamingContext) = {
    val kafkaCluster = new KafkaCluster(kafkaParams)
    var kafkaStream: InputDStream[(String, String)] = null
    val kafkaPartitionsE = kafkaCluster.getPartitions(topics)
    //left代表空，right代表有值
    if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")

    val kafkaPartitions = kafkaPartitionsE.right.get
    //从zookeeper中获取offset信息
    val consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupid, kafkaPartitions)
    //如果在zookeeper中没有记录，就从最小的offset开始消费
    if (consumerOffsetsE.isLeft) {
      kafkaParams + ("auto.offset.reset" -> "largest")
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    } else {
      val consumerOffsets = consumerOffsetsE.right.get
      val latestOffests = kafkaCluster.getLatestLeaderOffsets(kafkaPartitions).right.get
      val earliestOffests = kafkaCluster.getEarliestLeaderOffsets(kafkaPartitions).right.get
      var newConsumerOffsets: HashMap[TopicAndPartition, Long] = HashMap[TopicAndPartition, Long]()
      consumerOffsets.foreach(f => {
        val max = latestOffests.get(f._1).get.offset
        val min = earliestOffests.get(f._1).get.offset

        newConsumerOffsets .put(f._1,f._2)
        //如果zookeeper中记录的offset在kafka中不存在(已经过期),就指定其现有kafka的最小offset位置开始消费
        if (f._2 < min) {
          newConsumerOffsets.put(f._1, min)
        }
        //首次启动 699 699 424
        println(max + "-----" + f._2 + "--------" + min)
      })
      import scala.collection.JavaConverters._
//      val messageHandler = (mmd : MessageAndMetadata[String, String]) => Tuple2(String,String)(mmd.topic, mmd.message())
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,(String,String)](
        ssc, kafkaParams, newConsumerOffsets.toMap, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }
    kafkaStream
  }
}
