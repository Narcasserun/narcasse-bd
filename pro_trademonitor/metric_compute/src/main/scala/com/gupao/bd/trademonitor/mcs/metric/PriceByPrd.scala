package com.gupao.bd.trademonitor.mcs.metric

import com.alibaba.fastjson.JSONObject

/**
  * 功能：分时段各产品实时成交量
  **/
case class PriceByPrd(dt: String, pName: String, price: Double) {
  val json: JSONObject = new JSONObject()
  json.put("metric", "PriceByPrd")
  json.put("dt", dt)
  json.put("pName", pName)
  json.put("price", price)

  override def toString: String = json.toJSONString
}
