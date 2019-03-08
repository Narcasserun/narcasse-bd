package com.gupao.bd.trademonitor.mcs.model

import com.alibaba.fastjson.JSONObject

/**
  * 功能：对应order_detail表中的数据
  **/
case class OrderDetail(json: JSONObject) {

  val data: JSONObject = json.getJSONObject("after")
  val id = data.getString("id").toInt
  val orderId = data.getString("order_id").toInt
  val buyCount = data.getString("buy_count").toInt
  val pName = data.getString("p_name")
  val totalPrice: Double = data.getString("total_price").toDouble

  //
  //  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
  //  `user_id` bigint(20) DEFAULT NULL COMMENT '用户ID',
  //  `order_status` varchar(11) DEFAULT NULL COMMENT '订单状态',
  //  `order_time` datetime DEFAULT NULL COMMENT '下单时间',
  //  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  //  `update_time` datetime DEFAULT NULL COMMENT '更新时间',

  override def toString: String = data.toJSONString
}
