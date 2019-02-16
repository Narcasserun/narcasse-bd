package com.narcasse.spark_streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * 功能：从SocketInputDStream消费数据，计算累计WordCount
  **/
object StatefulNetworkWordCount extends StreamingExample {

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("StatefulNetworkWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")

  // Initial state RDD for mapWithState operation
  val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

  val lines = ssc.socketTextStream(hostname, port)
  //滑动时间间隔：前一个窗口到后一个窗口所经过的时间长度。必须是批处理时间间隔的倍数
  //checkpoint 频率设置一般是滑动时间的5-10倍，而不是batch时间
  lines.checkpoint(Seconds(1/10))
  val words = lines.flatMap(_.split(" "))
  val wordDstream = words.map(x => (x, 1))

  // 计算Word的累计数据，通过State保留历史值
  val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  val stateDstream = wordDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD))
  stateDstream.print()
  ssc.start()
  ssc.awaitTermination()
}
