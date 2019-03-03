package com.narcasse.flink.streaming.custormSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/*
   
*/

object StreamDemoWithParallelscala {
  def main(args: Array[String]): Unit = {


          val env = StreamExecutionEnvironment.getExecutionEnvironment

          import org.apache.flink.api.scala._
          val text = env.addSource(new MyParallelSourceScala).setParallelism(2)
          val data =text.map(line=>{
            println("接收到的数据为:"+line)
            line
          })
        val value = data.timeWindowAll(Time.seconds(2)).sum(0)
        value.print()
        env.execute("StreamDemoWithParallelscala")




  }

}
