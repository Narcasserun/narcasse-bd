package com.gupao.bd.trademonitor.mcs.metric

import com.alibaba.fastjson.JSONObject

/**
  * 功能：全站累计成交量
  **/
case class TotalPrice(dt: String, price: Double) {
  val json: JSONObject = new JSONObject()
  json.put("metric", "TotalPrice")
  json.put("dt", dt)
  json.put("price", price)

  override def toString: String = json.toJSONString
}
