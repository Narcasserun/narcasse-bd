package com.narcasse.spark_streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * $ nc -lk 9999
  * 功能：从SocketInputDStream消费数据（指定IP和端口)
  **/
object NetworkWordCount {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("set host and port")
      System.exit(1)
    }

    val host: String = args(0)
    val port: Int = args(1).toInt

    //1、配置SparkConf
    val sparkConf = new SparkConf().setAppName("NetworkWordCount")

    //2、构造StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    ssc.sparkContext.setLogLevel("WARN")

    //3、输入
    val lines = ssc.socketTextStream(host, port, StorageLevel.MEMORY_AND_DISK_SER)

    //4、计算
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    val distDir = args(2)

    //5、输出
    wordCounts.saveAsTextFiles(distDir, "txt")

    if (sparkConf.get("spark.master").contains("local")) {
      wordCounts.print()
    }

    //6、启动
    ssc.start()

    //7、终止
    ssc.awaitTermination()

  }
}
