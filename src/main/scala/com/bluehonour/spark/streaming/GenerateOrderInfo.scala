package com.bluehonour.spark.streaming

import java.time.{LocalDate, LocalDateTime}
import java.util.UUID
import scala.util.Random
import OrderInfoUtils._
import sparkStreaming.ProducerInfo._
import scala.collection.mutable.ArrayBuffer

object GenerateOrderInfo {
  def main(args: Array[String]): Unit = {
    val random = new Random(1000)
    // 临时存放orderid、ordertime。供后期生成 支付信息
    var orderIDBuffer = new ArrayBuffer[String]()
    // 用于生成订单信息
    val date = LocalDate.now().toString.replace("-", "")
    var i = 0

    while (true) {
      // 用户订单信息(逐条处理)
      //用户编号、订单编号、地址编号、订单时间、商品总额、运费、优惠金额、成交金额
      val num = random.nextInt(100000)
      val userId = s"user${num}"
      val orderId = UUID.randomUUID.toString
      val addressId = getAddressID
      val orderTime = getNow
      val goodsAmount = getGoodsAmount
      val carriage = getCarriage(goodsAmount)
      val discountAmount = getDiscount(goodsAmount)
      val amount = goodsAmount + carriage - discountAmount
      orderIDBuffer += orderId
      val orderInfo = s"$userId,$orderId,$addressId,$orderTime,$goodsAmount,$carriage,$discountAmount,$amount"
      //println(s"orderInfo=$orderInfo")
      /*val topicA = "produceOrderInfo"
      producerA(topicA, orderInfo)*/
      val filePathA = "/home/zcl/data/log/order.log"
      saveOrder(filePathA, orderInfo)

      // 送货地址信息（逐条处理）
      //用户编号、地址编号、送货地址、联系人、联系电话
      val address = getAddress
      val linkman = getUsername
      val phonenum = getPhoneNum
      val deliveryInfo = s"$userId,$addressId,$address,$linkman,$phonenum"
      //println(s"deliveryInfo=$deliveryInfo")
      /*val topicC =  "produceTele"
      producerA(topicA, orderInfo)*/

      // 支付信息(逐条处理)
      // 程序运行30秒之后(30秒,每秒1000条记录)开始生成支付信息
      // 始终让队列保持3W的长度，超出的全部处理
      //val bufferLength = 30*1000
      val bufferLength = 10
      if (orderIDBuffer.length > bufferLength) {
        val paymentNumber = orderIDBuffer.length - bufferLength
        orderIDBuffer = random.shuffle(orderIDBuffer) //打乱顺序 random.shuffle有返回值吗
        val orderids = orderIDBuffer.takeRight(paymentNumber)
        orderIDBuffer.trimEnd(paymentNumber)
        //val topicB = "producerOrderids"
        val filePathB = "/home/zcl/data/log/pay.log"
        orderids.foreach(id => {
          val paymentInfo = getPaymentInfo(id)
          //producerA(topicB, paymentInfo)
          savePay(filePathB, paymentInfo)
          //获得支付信息
          /*val paymentInfo = getPaymentInfo(id)*/
          //println(s"paymentInfo=$paymentInfo")
        })
      }
      // 控制消息发送的频率(大约每秒1000条)
      i += 1
      if (i%1000 == 0) {
        i = 0
        Thread.sleep(1000)
      }
    }
  }
}

// 用户订单信息（OrderAndUser）
//用户编号、订单编号、地址编号、订单时间、商品总额、运费、优惠金额、成交金额
// userId、orderid、addressId、ordertime、 goodsamount、carriage、discountamount、amount
// 订单投递信息（OrderDelivery）
// 用户编号、地址编号、送货地址、联系人、联系电话
// userId、addressId、address、linkman、phonenum
// 支付信息（OrderPayment）
// 订单编号、付款方式、支付时间、积分奖励、账单分类
// orderid、payment、paymenttime、bonus、paymenttype