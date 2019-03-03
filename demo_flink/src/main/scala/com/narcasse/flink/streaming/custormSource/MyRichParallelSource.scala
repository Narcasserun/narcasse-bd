package com.narcasse.flink.streaming.custormSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/*
   
*/

class MyRichParallelSource extends RichParallelSourceFunction[Long]{

  private var isRunning = true
  private var count = 1l


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit ={

    while (isRunning){
      ctx.collect(count)
      count+=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning=false
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def close(): Unit = super.close()
}
