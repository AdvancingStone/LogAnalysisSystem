package com.bluehonour.spark.streaming

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, LocalTime}
import scala.util.Random

object OrderInfoUtils {
  private val random = new Random(1000)
  private val warehouseId = (1 to 20).map(idx => f"warehouse$idx%2s".replace(" ", "0"))
  private val warehouseLength = warehouseId.length
  private val telHead = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",")
  private val telHeadLength = telHead.length
  private val payment = "支付宝,工商银行,建设银行,中国银行,农业银行,交通银行,招商银行,中信银行,浦发银行,兴业银行,民生银行,邮储银行,光大银行,平安银行,华夏银行,北京银行,上海银行,广发银行,银联,其他".split(",")
  private val paymentLength = payment.length
  private val paymentType = (1 to 10).map(idx => f"type$idx%2s".replace(" ", "0"))
  private val paymentTypeLength = paymentType.length
  private val simpleFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")

  // 将当前日期格式化为自定义字符串
  def getNow: String = simpleFormatter.format(LocalDateTime.now)

  // 得到商品总额
  def getGoodsAmount: Double = {
    val num = random.nextInt(200)
    val amount = num match {
      case _ if num < 120 => num
      case _ if num < 180 => random.nextInt(500)
      case _ if num < 200 => random.nextInt(2000)
      case _ => random.nextInt(5000)
    }
    amount
  }

  // 得到运费
  def getCarriage(amount: Double): Double =
    amount match {
      case _ if amount < 48 => 10
      case _ if amount < 98 => 5
      case _ => 0
    }

  // 得到优惠金额
  def getDiscount(amount: Double): Double = {
    val num = random.nextInt(100)
    val discount = num match {
      case _ if num < 80 => 0.0
      case _ if num < 85 => 0.15
      case _ if num < 90 => 0.10
      case _ if num < 95 => 0.05
      case _ => 0.0
    }
    (amount * discount).formatted("%.2f").toDouble
  }

  // 得到送货地址
  def getAddress: String = f"address${random.nextInt(10000000)}%8s".replace(" ", "0")

  // 得到地址编号
  def getAddressID:String = f"addressId${random.nextInt(10)+1}%2s".replace(" ", "0")

  // 得到用户姓名
  def getUsername: String = f"username${random.nextInt(10000000)}%8s".replace(" ", "0")

  // 得到telephone number. 有10%的概率，手机号会缺数
  def getPhoneNum: String = {
    telHead(random.nextInt(telHeadLength)) + random.nextInt(100000000)
  }

  // 得到支付方式, 银行 + 后4位银行卡号
  def getPayment: String = {
    val paymentHead = payment(random.nextInt(paymentLength))
    val paymentTail = (1 to 4).map(x=>random.nextInt(10)).mkString("")
    s"$paymentHead($paymentTail)"
  }

  // 积分奖励。应该与成交金额有关系，这里简化处理了
  def getBonus: String = (random.nextInt(20) + 1).toString

  // 支付类型。用于事后的交易类型统计，这里简化处理
  def getPaymentType: String = paymentType(random.nextInt(paymentTypeLength))

  // 生成一条payment信息
  // 订单编号、付款方式、支付时间、积分奖励、账单分类
  // orderid、payment、paymenttime、bonus、paymentType
  def getPaymentInfo(id: String): String = {
    val payment = getPayment
    val bonus = getBonus
    val paymentType = getPaymentType
    val time = getNow
    s"$id,$payment,$time,$bonus,$paymentType"
  }

  def main(args: Array[String]): Unit = {
    println(getNow)
    val time1 = LocalTime.now
    Thread.sleep(2000)
    val time2 = LocalTime.now
    println(Duration.between(time1, LocalTime.now).getSeconds)
    val str = "strfgdsdfg"
    val str1 = "ssdfds"
    val s = Option[(String)](str1)
    println(s.get)

    if(str == 0 && str != 1){
      println(">>")
    }else{
      println(">>>>>>>>")
    }
  }
}