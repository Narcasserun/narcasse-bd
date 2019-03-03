package com.narcasse.spark_streaming.kafka010

import java.io.File
import java.nio.charset.Charset

import com.google.common.io.Files
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


/*
  updateStateByKey 注意事项：
  1，checkpoint

  2. 错误 处理逻辑要在方法中
  Exception in thread "main" org.apache.spark.SparkException: org.apache.spark.streaming.dstream.StateDStream@47f2c722 has not been initialized

  3，手动发送若干数据
 */
object kafka010Checkpoint {
   def main(args: Array[String]) {

     val checkpointDirectory = "/opt/checkpoint"

     val ssc = StreamingContext.getOrCreate(checkpointDirectory,
       () => createContext(checkpointDirectory))

      //    启动流
      ssc.start()
      ssc.awaitTermination()
    }

  def createContext(checkpointDirectory: String)
  : StreamingContext = {

    //    创建一个批处理时间是2s的context 要增加环境变量
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances","2")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
        ,"/opt/jars/kafka-clients-0.10.2.2.jar"
        ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val sc = new SparkContext(sparkConf)
    val aar1 = Array(1,2)
    sc.broadcast(aar1)

    ssc.checkpoint(checkpointDirectory)
    //    使用broker和topic创建DirectStream
    val topicsSet = "test1".split(",").toSet
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> "mt-mdh.local:9093",
      "key.deserializer"->classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->"test1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit"->(false: java.lang.Boolean))
    // 没有接口提供 offset

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))


    // 会报错的哦,object not serializable (class: org.apache.kafka.clients.consumer.ConsumerRecord
    //     messages.checkpoint(Seconds(20))

    /* 超时*/
    messages.persist()
    def updateFn(newVal :Seq[Int], stateVal : Option[Int]) :Option[Int] = {
      stateVal match {
        case Some(state) =>
          val res = state + newVal.sum
          if (res < 2) {
            return None
          } else {
            Some(res)
          }
        case None =>
          Some(newVal.sum)
      }
    }
    messages.map(_.value()).flatMap(_.split(" ")).map((_,1)).
      updateStateByKey(updateFn _).checkpoint( Seconds(20)) //只需要checkpoint状态更新流产生的rdd
      .foreachRDD(rdd=>{

      rdd.keys.collect().foreach(println)
      println("============")
    })
    ssc
  }
}
