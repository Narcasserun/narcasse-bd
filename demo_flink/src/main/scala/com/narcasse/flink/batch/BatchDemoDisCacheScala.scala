package com.narcasse.flink.batch

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * Distributed Cache
  *
  */
object BatchDemoDisCacheScala {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._


    //1:注册文件
    env.registerCachedFile("C:\\environment\\flink\\a.txt","b.txt")

    val data = env.fromElements("a","b","c","d")

    val result = data.map(new RichMapFunction[String,String] {

      import java.util

      private val dataList = new util.ArrayList[String]
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val myFile = getRuntimeContext.getDistributedCache.getFile("b.txt")
        val lines = FileUtils.readLines(myFile)
        val it = lines.iterator()
        while (it.hasNext){
          val line = it.next()
          dataList.add(line)
          println("line:"+line)
        }
      }
      override def map(value: String) = {
        //在这里就可以使用dataList
        value+":"+dataList
      }
    })

    result.print()

  }

}
