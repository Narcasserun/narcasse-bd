package com.narcasse.flink.streaming.streamAPI

import com.narcasse.flink.streaming.custormSource.MyNoParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoFilterScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text = env.addSource(new MyNoParallelSource)

    val mapData = text.map(line=>{
      println("原始接收到的数据："+line)
      line
    }).filter(_ % 2 == 0)

    val sum = mapData.map(line=>{
      println("过滤之后的数据："+line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)


    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")



  }

}
