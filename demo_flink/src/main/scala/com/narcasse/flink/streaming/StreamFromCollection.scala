package com.narcasse.flink.streaming

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
   
*/

object StreamFromCollection {

  def main(args: Array[String]): Unit = {

  //  获取运行时环境
    //隐式转换
    import org.apache.flink.api.scala._
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var  data =List(1,2,3,4,5,6,7)
    val darastream: DataStream[Int] = env.fromCollection(data)

      val value = darastream.map(_+1)
    value.print().setParallelism(1)
    env.execute("StreamFromCollection")

}
}
