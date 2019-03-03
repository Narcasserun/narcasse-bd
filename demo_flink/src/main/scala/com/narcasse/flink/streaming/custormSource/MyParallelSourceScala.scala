package com.narcasse.flink.streaming.custormSource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * 创建自定义并行度为1的source
  *
  * 实现从1开始产生递增数字
  *
  */
class MyParallelSourceScala extends ParallelSourceFunction[Long]{

  private var flag = true
  private var count = 1l
  override def run(ctx: SourceFunction.SourceContext[Long]){

    while (flag){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }

  }

  override def cancel(){

    flag=false
  }
}
