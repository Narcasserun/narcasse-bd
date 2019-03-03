package com.narcasse.flink.streaming.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/*
   
*/

object StreamingDataToRedis {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data = env.socketTextStream("ubuntu1",9999)

    val words_data=data.map(line=>("1_words_scala",line))
    val config = new FlinkJedisPoolConfig.Builder().setHost("ubuntu1").setPort(6379).setPassword("123456").build()
    val redisSink: RedisSink[(String, String)] = new RedisSink[Tuple2[String,String]](config,new MyRedisMapper)
    words_data.addSink(redisSink)

    //执行任务
    env.execute("Socket window count")


  }

  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{

    override def getKeyFromData(data: (String, String)) = {
      data._1
    }

    override def getValueFromData(data: (String, String)) = {
      data._2
    }

    override def getCommandDescription = {
      new RedisCommandDescription(RedisCommand.LPUSH)
    }
  }

}
