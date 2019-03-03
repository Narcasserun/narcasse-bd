package com.narcasse.flink.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * counter 累加器
  * Flink Broadcast和Accumulators的区别
  * Broadcast(广播变量)允许程序员将一个只读的变量缓存在每台机器上，
  * 而不用在任务之间传递变量。广播变量可以进行共享，但是不可以进行修改
  * Accumulators(累加器)是可以在不同任务中对同一个变量进行累加操作。
  *
  */
object BatchDemoCounterScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = env.fromElements("a","b","c","d")

    val res = data.map(new RichMapFunction[String,String] {
      //1：定义累加器
      val numLines = new IntCounter

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //2:注册累加器
        getRuntimeContext.addAccumulator("num-lines",this.numLines)
      }

      override def map(value: String) = {
        this.numLines.add(1)
        value
      }

    }).setParallelism(4)


    res.writeAsText("d:\\data\\count21")
    val jobResult = env.execute("BatchDemoCounterScala")
    //3：获取累加器
    val num = jobResult.getAccumulatorResult[Int]("num-lines")
    println("num:"+num)

  }

}
