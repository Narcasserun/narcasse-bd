package com.narcasse.useraction.utils

import org.apache.commons.lang3.time.FastDateFormat

object FilterUtils {

  private val dataFormat: FastDateFormat =
    FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  // 以时间作为过滤条件
  def filterByTime(fields: Array[String], startTime: Long, endTime: Long): Boolean ={
    val time = fields(1)
    val time_long = dataFormat.parse(time).getTime
    time_long >= startTime && time_long < endTime
  }

  // 以事件类型作为过滤条件
  def filterByType(fields: Array[String], eventType: String): Boolean = {
    val _type = fields(0)
    _type == eventType
  }

  // 以类型和时间作为过滤条件
  def filterByTypeAndTime(fields: Array[String], eventType: String,
                          beginTime: Long, endTime: Long): Boolean = {
    val _type = fields(0)
    val time = fields(1)
    val time_long = dataFormat.parse(time).getTime
    _type == eventType && time_long >= beginTime && time_long < endTime
  }

  // 以多个类型来进行过滤
  def filterByTypes(fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for (et <- eventTypes){
      if (_type == et)
        return true
    }
    false
  }

}
