package com.narcasse.spark_streaming.homework


import com.narcasse.spark_streaming.demo.StreamingExample
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

/**
  * nc -lk 9999
  **/
object MapWithStateVSUpdateStateByKey extends StreamingExample {

  val sparkConf = new SparkConf().setMaster("local[3]").setAppName("NetworkWordCount")
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint(checkpointDir)

  val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

  val words = lines.flatMap(_.split(" "))

  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

  // 1. mapWithState操作
  val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }

  val state = StateSpec.function(mappingFunc)
  wordCounts.mapWithState(state).print()

  // 2. updateStateByKey
  val addFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
    //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
    val currentCount = currValues.sum
    // 已累加的值
    val previousCount = prevValueState.getOrElse(0)
    // 返回累加后的结果，是一个Option[Int]类型
    Some(currentCount + previousCount)
  }

  wordCounts.updateStateByKey(addFunc, 2).print()

  ssc.start()
  ssc.awaitTermination()
}
