package com.bluehonour.spark.streaming

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

object sparkHandleUtils {

  // 用户订单信息（OrderAndUser）
  //用户编号、订单编号、地址编号、订单时间、商品总额、运费、优惠金额、成交金额
  // userId、orderid、addressId、ordertime、 goodsamount、carriage、discountamount、amount
  // 订单投递信息（OrderDelivery）
  // 用户编号、地址编号、送货地址、联系人、联系电话
  // userId、addressId、address、linkman、phonenum
  // 支付信息（OrderPayment）
  // 订单编号、付款方式、支付时间、积分奖励、账单分类
  // orderid、payment、paymenttime、bonus、paymenttype

  case class OrderAndUser(
                           userId: String,
                           orderid: String,
                           addressId: String,
                           ordertime: String,
                           goodsamount: String,
                           carriage: String,
                           discountamount: String,
                           amount: String
                         )
  case class Orderids(
                       orderid: String,
                       payment: String,
                       paymenttime: String,
                       bonus: String,
                       paymenttype: String
                     )
  case class Tele(
                   userId: String,
                   addressId: String,
                   address: String,
                   linkman: String,
                   phonenum: String
                 )
  val customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val zone = ZoneOffset.of("+8")
  def filterProduceOrderInfoRDD(str: String): Boolean = {
    if(str == "produceOrderInfo" || str == "order2"){
      true
    }else{
      false
    }
  }
  def filterProducerOrderidsRDD(str: String): Boolean = {
    if(str == "ProducerOrderids" || str == "pay2"){
      true
    }else{
      false
    }
  }
  def String2DataTime(str: String): LocalDateTime = {
    LocalDateTime.parse(str, customFormatter)
  }
  implicit def LocalDataTime2Long(t: LocalDateTime): Long = {
    t.toInstant(zone).toEpochMilli()
  }
  def timehandle(t1: String, t2: String): Long = {
    val tNow = String2DataTime(t1)
    val tOld = String2DataTime(t2)
    val timeDiff = tNow - tOld
    timeDiff
  }
  def main(args: Array[String]): Unit = {
    val str = "2019-09-01 12:05:00"
    val str1 = "2019-09-01 12:00:00"
    val s = String2DataTime(str)
    val s1 = String2DataTime(str1)
    if((s - s1) == 5*60*1000){
      println(">>>>>>")
    }
  }
}
