package com.narcasse.spark_streaming.kafka010

import net.sf.json.JSONObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, TaskContext}


/*
  updateStateByKey 注意事项：
  1，key超时
  2，checkpoint
  3，初始值

 */
object kafka010UpstateBykey {
   def main(args: Array[String]) {
      //    创建一个批处理时间是2s的context 要增加环境变量
      val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[4]")
//        .set("yarn.resourcemanager.hostname", "mt-mdh.local")
//        .set("spark.executor.instances","2")
//        .setJars(List("/Users/meitu/Desktop/sparkjar/bigdata.jar"
//          ,"/opt/jars/spark-streaming-kafka-0-10_2.11-2.3.1.jar"
//          ,"/opt/jars/kafka-clients-0.10.2.2.jar"
//          ,"/opt/jars/kafka_2.11-0.10.2.2.jar"))
      val ssc = new StreamingContext(sparkConf, Seconds(5))

     ssc.checkpoint("C:\\logs\\streaming\\update")

      //    使用broker和topic创建DirectStream
      val topicsSet = "upstate".split(",").toSet
      val kafkaParams = Map[String, Object]("bootstrap.servers" -> "ubuntu1:9092",
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer"-> classOf[StringDeserializer],
        "group.id"->"test2",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit"->(false: java.lang.Boolean))

     // 没有接口提供 offset
      val messages = KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
     // 会报错的哦,object not serializable (class: org.apache.kafka.clients.consumer.ConsumerRecord
    //messages.checkpoint(Seconds(20))

     /* 超时 前提条件是每个批次无论状态里的key是否有最新的值，都会为所有已存在的key调用update函数*/
     /* key 超过20s没出现就删除*/
     def updateFn(newVals :Seq[JSONObject], stateVal : Option[JSONObject]) :Option[JSONObject] = {
        stateVal match {
          case Some(state) =>
            if(newVals.nonEmpty){
              val newVal = newVals.head
              Some(newVal)
            }else{
              // 无新值，就判断key是否超时
              val stateTime = state.getLong("timestamp")
              val diffTime = System.currentTimeMillis()-stateTime
              if(diffTime > 20000){
                None
              }else{
                stateVal
              }
            }
          case None =>
            Some(newVals.head)
        }
    }
      messages.map(_.value()).map(each =>{
        val json = JSONObject.fromObject(each)
        val id = json.getInt("id")
        (id,json)
      }).updateStateByKey(updateFn _).checkpoint(Seconds(20)) //只需要checkpoint状态更新流产生的rdd
        .foreachRDD(rdd=>{
          rdd.keys.collect().foreach(println)
          println("=============>")
        })


     /*给定初始RDD
     * 有时候需要给每一个key以初始值
     * 初始值RDD必然会被删除
     **/
//     def updateFn(newVal :Seq[Int], stateVal : Option[Int]) :Option[Int] = {
//       stateVal match {
//         case Some(state) =>
//           val res = state + newVal.sum
//           if (res <= 2) {
//              None
//           } else {
//             Some(res)
//           }
//         case None =>
//           Some(newVal.sum)
//       }
//     }
//     val initRDD = ssc.sparkContext.parallelize(Array(("1",2), ("30",1), ("20",14)))
//     messages.map(_.value()).flatMap(_.split(" ")).map((_,1))
//       .updateStateByKey(updateFn _,new HashPartitioner(2),initRDD).checkpoint( Seconds(20)) //只需要checkpoint状态更新流产生的rdd
//       .foreachRDD(rdd=>{
//       rdd.keys.collect().foreach(println)
//       println("======>")
//     })

      //    启动流
      ssc.start()
      ssc.awaitTermination()
    }
}
