package com.narcasse.flink.streaming

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


/*
* 滑动窗口计算
*
* 每隔1秒统计最近2秒内的数据，打印到控制台
*/

object SocketWindowWordCountScala {

  def main(args: Array[String]): Unit = {

      val port:Int=try {
        ParameterTool.fromArgs(args).getInt("port")
      }catch {
        case e:Exception=>
          println("no port input ,use default port")
          9000
      }
    //获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val hostname= "ubuntu1"
    //从指定端口获取数据
   val text = env.socketTextStream(hostname,port)
    import org.apache.flink.api.scala._
    //解析数据，使用窗口计算，并计算出当前秒，最近两秒单词出现的次数
    val windowscount: DataStream[(String, Int)] = text.flatMap(line => line.split("\\s")).map(w => (w, 1))
      .keyBy(0).timeWindow(Time.seconds(2), Time.seconds(1))
      .sum(1)
    windowscount.print().setParallelism(1)
    env.execute("socket windows count")
  }

}
