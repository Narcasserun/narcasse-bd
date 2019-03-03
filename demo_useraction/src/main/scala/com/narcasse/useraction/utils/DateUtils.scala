package com.narcasse.useraction.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/*
     create by mask on  2018/11/29 10:15
*/

object DateUtils {

  //输入文件日期时间格式
  //10/Nov/2016:00:01:02 +0800

  val SOURCE_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)


  //目标日期格式
  val TARGET_TIME_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss")

  /**
    * 获取时间：yyyy-MM-dd HH:mm:ss
    */
  def parse(time: String) = {
    TARGET_TIME_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志时间：long类型
    *
    * time: [10/Nov/2016:00:01:02 +0800]
    */
  def getTime(time: String) = {
    try {
      SOURCE_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception => {
        0l
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getTime("10/Jul/2018:00:09:04 +0800"))

  }
}
