package com.narcasse.flink.streaming.custormSource

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/*
   
*/

class MyNoParallelSource extends SourceFunction[Long]{

  private var isrunning = true
  private var count = 1l

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit ={

    while (isrunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }

  }
  override def cancel(): Unit ={
    isrunning=false
  }
}
