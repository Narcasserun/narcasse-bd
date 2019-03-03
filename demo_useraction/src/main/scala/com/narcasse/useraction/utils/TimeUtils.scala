package com.narcasse.useraction.utils

import java.util.Calendar

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {
  private val dataFormat: FastDateFormat =
    FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  private val calendar: Calendar = Calendar.getInstance()

  // 用apply注入方法把传过来的String类型的时间转换成Long类型的时间
  def apply(time: String) = {
    calendar.setTime(dataFormat.parse(time))
    calendar.getTimeInMillis()
  }

  // 通过Calendar去改变日期
  def getCertainDayTime(amout: Int): Long = {
    // 改变日期
    calendar.add(Calendar.DATE, amout)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amout)
    time
  }

}
