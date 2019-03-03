package com.narcasse.flink.streaming.streamAPI

import com.narcasse.flink.streaming.custormSource.MyNoParallelSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoConnectScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    val text1 = env.addSource(new MyNoParallelSource)
    val text2 = env.addSource(new MyNoParallelSource)

    val text2_str = text2.map("str" + _)

    val connectedStreams = text1.connect(text2_str)

    val result = connectedStreams.map(line1=>{line1},line2=>{line2})

    result.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")



  }

}
