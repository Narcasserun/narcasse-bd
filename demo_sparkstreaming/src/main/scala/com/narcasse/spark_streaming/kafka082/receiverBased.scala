package com.narcasse.spark_streaming.kafka082

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/*

WARN Utils: Service 'sparkDriver' could not bind on a random free port. You may check whether configuring an appropriate binding address.
配置: set("spark.driver.bindAddress","127.0.0.1")


 */
object receiverBased {

  def main(args: Array[String]): Unit = {

    //    创建一个批处理时间是2s的context 要增加环境变量
    val sparkConf = new SparkConf().setAppName(this.getClass.getName).setMaster("yarn-client")
      .set("yarn.resourcemanager.hostname", "mt-mdh.local")
      .set("spark.executor.instances","2")
      .set("spark.driver.bindAddress","mt-mdh.local")
      .set("spark.executor.memory","512m")
      .set("spark.driver.memory","512m")
      .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
        ,"/opt/jars/kafka_2.11-0.8.2.2.jar"
        ,"/opt/jars/kafka-clients-0.8.2.2.jar"
        ,"/opt/jars/metrics-core-2.2.0.jar"
        ,"/opt/jars/spark-streaming-kafka-0-8_2.11-2.3.1.jar"))

    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(5))

    // 数值代表单个receiver线程数
    val topicMap = Map("page_visits" -> 1)

    // 每次调用createStream只会生成一个receiver 要为receiver预留cpu的
    // 创建的多个流要进行union
    val lines = KafkaUtils.createStream(ssc, "localhost:2181/kafka082", "page_visits", topicMap).map(_._2)

//    val msgs = (0 to 9).map(num=>{KafkaUtils.createStream(ssc, "localhost:2181/kafka082", "page_visits", topicMap).map(_._2)})
//    ssc.union(msgs)

    val words = lines.flatMap(_.split(" "))
    val counts = words.map((_, 1L)).reduceByKey(_ + _)
    counts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
