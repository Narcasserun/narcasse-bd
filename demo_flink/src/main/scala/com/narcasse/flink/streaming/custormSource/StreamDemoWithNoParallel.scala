package com.narcasse.flink.streaming.custormSource



import org.apache.flink.streaming.api.windowing.time.Time

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
   
*/

object StreamDemoWithNoParallel {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //隐式转换
    import org.apache.flink.api.scala._
    val streamSource: DataStream[Long] = env.addSource(new MyNoParallelSource).setParallelism(1)


    val  mapData = streamSource.map(line=>{
      println("接受到的数据是："+line)
      line
    })
    val res: DataStream[Long] = mapData.timeWindowAll(Time.seconds(2)).sum(0)
    res.print().setParallelism(1)


    env.execute("StreamDemoWithNoParallel")


  }

}
