package com.narcasse.flink.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/*
   
*/

object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = env.readTextFile("C:\\environment\\大数据\\hello.txt")

    import org.apache.flink.api.scala._
    val counts = text.flatMap(line=>line.split("\\W+")).map(e=>(e,1)).groupBy(0).sum(1)

    counts.setParallelism(1).print()


  }

}
