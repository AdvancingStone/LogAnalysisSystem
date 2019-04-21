package com.bluehonour.spark.utils

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern
import scala.language.implicitConversions
import scala.io.Source

case class Weblog(ip:         String,          // 客户端的ip地址
                  sessionid:  String,          // sessionid
                  time:       LocalDateTime,    // 访问时间
                  url:        String,   // 请求的url、http协议等
                  status:     String,   // 请求状态；成功是200
                  sentBytes:  String,   // 发送给客户端文件主体内容大小
                  referer:    String,   // 记录从那个页面链接访问过来的
                  userAgent:  String)    // 客户浏览器的相关信息


case class LogInfo(ip:          String,   // 远程ip地址
                   district:    String,   // 区域
                   sessionid:   String,   // sessionid
                   datetime:    String,   // 访问时间
                   daystr:      String,   // 年月日时分秒
                   url:         String,   // 请求的url、http协议等
                   status:      String,   // 请求状态；成功是200
                   sentBytes:   String,   // 发送给客户端文件主体内容大小
                   referal:     String,   // 记录从那个页面链接访问过来的
                   userAgent:   String,   // 客户浏览器的相关信息
                   yearmonthday:String)   // 分区信息(按年月日分区)

case class NewLogInfo(ip:          String,   // 客户端的ip地址
                      district:    String,   // 区域
                      sessionid:   String,   // sessionid
                      datetime:    String,   // 访问时间
                      url:         String,   // 请求的url、http协议等
                      columnname:  String,   // column id
                      sentBytes:   Int,      // 发送给客户端文件主体内容大小
                      referal:     String,   // 记录从那个页面链接访问过来的
                      referalhost: String,   // referal网址
                      userAgent:   String,   // 客户浏览器的相关信息
                      devicetype:  String,   //设备类型
                      yearmonthday:String)   // 分区信息(按年月分区)

case class LogInfo2(ip:          String,   // 客户端的ip地址
                    district:    String,   // 区域
                    sessionid:   String,   // session
                    datetime:    String,   // 访问时间
                    daystr:      String,    //年月日时分秒
                    columnName:  String,   // 栏目
                    refererHost: String,   //引用主机
                    deviceType:  String,   //设备类型
                    year:        Int,
                    month:       Int,
                    day:         Int)



// 访问信息
case class VisitInfo(var sessionID: String,
                     datetime:  String,
                     distID:    Byte,
                     columnID:  Byte,
                     refererID: Byte,
                     deviceID:  Byte)

// IP信息
case class IpInfo(startIp: Long,
                  endIp: Long,
                  district: String)

// 栏目信息
case class ColumnInfo(url: String, context: String)

object LogUtils {
  // 匹配日志的正则表达式
  private val logPattern = Pattern.compile("""(\S+) (.+) \[(.+)\] "(.*)" (\d+) (\d+) "(.*)" "(.*)"""")

  // 匹配 4xx / 5xx  等状态
  private val statusPattern = Pattern.compile("""[4|5]\d{2}""")

  // 匹配 .css / .png / .jpg / .js / .ico / .gif
  private val staticResource = Pattern.compile(""".*\.(css|png|jpg|js|ico|gif).*""")




  // 要使用java8的时间日期类型，这种类型是线程安全的
  private val customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")

  implicit def localDateTime2Long(time: LocalDateTime): Long =
    time.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond()

  // 将字符串转为LocalDateTime
  def string2DateTime(str: String): LocalDateTime =
    LocalDateTime.parse(str, customFormatter)

  def getTimeInterval(time1: String, time2: String): Long =
    string2DateTime(time1) - string2DateTime(time2)

  // 检查状态是否为：4xx / 5xx
  def isValidStatus(loginfo: String): Boolean = statusPattern.matcher(loginfo).matches()

  // 未使用正则
  // 检查URL中是否包含静态资源：.css .png .jpg .js .ico .gif
  def isStaticResource1(url: String): Boolean =
    url.contains(""".css""") || url.contains(""".png""") ||
      url.contains(""".jpg""") || url.contains(""".js""") ||
      url.contains(""".ico""") || url.contains(""".gif""")

  // 使用正则
  // 检查URL中是否包含静态资源：.css .png .jpg .js .ico .gif
  def isStaticResource(url: String): Boolean = {
    val matcher = staticResource.matcher(url)
    matcher.matches()
  }

  // 日志解析(将ip转换为区域id)
  def LogParse(str: String): Option[LogInfo] = {
    val matcher = logPattern.matcher(str)
    if (matcher.matches()) {
      val datetime = matcher.group(3)
      // 该字段用于分区，只取年月
      val yearmonth = datetime.replaceAll("-", "").take(6)

      Some(LogInfo(matcher.group(1), "", matcher.group(2), datetime, yearmonth, matcher.group(4),
        matcher.group(5), matcher.group(6), matcher.group(7), matcher.group(8), yearmonth))
    }
    else None
  }

  // 日志解析 (将ip转换为区域)
  // 做了两件事：正则表达式的解析、ip转地址
  def LogParse(str: String, ips: Array[IpInfo]): Option[LogInfo] = {
    val matcher = logPattern.matcher(str)
    if (matcher.matches()) {
      val ip = matcher.group(1)
      val district = getDistrict(ip, ips)
      val datetime = matcher.group(3) //2017-01-08 00:38:04 +0800
      //取年月日时分秒
      val daystr = datetime.replaceAll("-","").replaceAll(" ","").replaceAll(":","").take(14)
      // 该字段用于分区，只取年月日
      val yearmonthday = datetime.replaceAll("-", "").take(8)

      Some(LogInfo(ip, district, matcher.group(2), datetime, daystr, matcher.group(4),
        matcher.group(5), matcher.group(6), matcher.group(7), matcher.group(8), yearmonthday))
    }
    else None
  }

  // 将ip地址转换为Long
  def ipv4ToLong(str: String): Long = {
    val n = 3
    // 拆分、拉拢、转换、计算
    str.split("\\.").zipWithIndex
      .map{case (ip, index) => (ip.toLong, n-index)}
      .foldLeft(0L){
        case (result, (ip, index)) =>
          result | ip << (index * 8)
      }
  }

  // 还有其他两种相对简洁的办法。
  // 思路相同，一个用了循环，一个用了fold
  def ipv4ToLong1(str: String): Long = {
    val ip = str.split("\\.").map(_.toLong)
    var long = 0L
    for (i <- ip.indices) {
      long = (long << 8) | ip(i)
    }
    long
  }

  // 最简洁，相对难理解
  def ipv4ToLong2(str: String): Long = {
    // 拆分、转换、计算
    str.split("\\.")
      .map(_.toLong)
      .fold(0L)((x,y) => (x << 8) | y)
  }

  // 使用折半查找, 将ip转换为地域信息
  def getDistrict(ip: String, ipArr: Array[IpInfo]): String = {
    val ipLong = ipv4ToLong(ip)
    var start = 0
    var end = ipArr.length - 1
    var num = -1

    // 满足条件，才能继续。找不到时，返回-1
    while (start <= end && num == -1) {
      val middle = (start + end)/2
      num = if (ipLong >= ipArr(middle).startIp && ipLong <= ipArr(middle).endIp) middle
      else -1

      // 重新赋值
      if (ipLong < ipArr(middle).startIp)
        end = middle - 1
      else
        start = middle + 1
    }
    if (num == -1) "unknown" else ipArr(num).district
  }

  // 得到栏目信息(找不到返回空，这里没有进一步做校验。具体的校验放在后面了)
  def getColumn(url: String): String = {
    if (3==url.split("\\s+").length) {
      url.split(" ")(1).split("/")(1).split("\\.")(0)
    }
    else ""
  }

  // 得到设备类型：PC端、移动端、
  def getDeviceType(userAgent: String): String = {
    val str = userAgent.toLowerCase()
    if(str.contains("android") || str.contains("windows phone") || str.contains("iphone")
      || str.contains("ipad") || str.contains("ipod")){
      "移动端"
    } else if(str.contains("windows nt")){
      "PC端"
    } else if(str.contains("DNSPod-Monitor/1.0")){
      "域名监控"
    } else{
      "unknown"
    }
  }
  def getDeviceType(userAgent: String, userAgentMap: collection.Map[String, String]): String = {
    //    println(userAgent)
    userAgentMap.getOrElse(userAgent, "unknown")
  }

  // 得到referal网址
  def getReferalHost(referal: String): String = {
    val infoarr = referal.split("/")
    if (infoarr.length >=3) infoarr(2)
    else "-"
  }

  // 对ip、url、referal、useragent进行转换
  def logInfo2NewLogInfo(loginfo: LogInfo,
                         userAgentMap: collection.Map[String, String]): NewLogInfo = {
    val LogInfo(ip, district, sessionid, datetime, daystr, url, status, sentBytes, referal, userAgent, yearmonday) = loginfo
    val columnname = getColumn(url)
    val deviceType = getDeviceType(userAgent, userAgentMap)
    val referalhost = getReferalHost(referal)

    NewLogInfo(ip, district, sessionid, datetime, url, columnname, sentBytes.toInt,
      referal, referalhost, userAgent, deviceType, yearmonday)
  }

  // 以下是测试方法。都是私有的，不允许外部调用
  private def regexTest1(str: String): Unit = {
    val matcher = staticResource.matcher(str)
    if (matcher.matches())
      println(s"Matcher!!! str = $str")
    else
      println("No matcher !!!")
  }

  private def regexTest2(): Unit = {
    import java.io.PrintWriter
    import scala.io.Source

    val lines = Source.fromFile("/home/spark/data/access.log").getLines()
    val out = new PrintWriter("/home/spark/nomatcher.log")

    lines.foreach(line => {
      val matcher = staticResource.matcher(line)
      if (matcher.matches())
        out.println(line)
    })
  }

  // 将ip转为long，并按起始ip排序
  private def getIps(filePath: String): Array[IpInfo] = {
    Source.fromFile(filePath)
      .getLines()
      .map(_.trim.split("\\s+"))
      .filter(_.length == 3)
      .map(x => IpInfo(ipv4ToLong(x(0)), ipv4ToLong(x(1)), x(2)))
      .toArray
      .sortBy(_.startIp)
  }

  // 校验ip相关的方法。检查是否有不能解析的ip
  private def verifyIps(): Unit = {
    val ips = getIps("/home/spark/data/ipaddress.dat")

    // 打印不能转换的ip地址
    val datalines = Source.fromFile("/home/spark/data/access.log").getLines()
    var i = 0
    var j = 0
    datalines.foreach(line => {
      val log = LogParse(line)
      if (log.isDefined) {
        val ip = log.get.ip
        val a = getDistrict(ip, ips)
        i += 1
        if (i % 10000 == 0) println(s"num = $i")
        if (a == "未知区域") {
          j += 1
          println(s"ip = $ip")
        }
      }
    })
    if (j == 0) println("全部ip地址均合法！")
  }

  def main(args: Array[String]): Unit = {
    val str = "54.32.33.14"
    val a = ipv4ToLong(str)
    val b = ipv4ToLong1(str)
    val c = ipv4ToLong2(str)
    println(s"a = $a")
    println(s"b = $b")
    println(s"c = $c")

    verifyIps()

    val str1 = "GET/auto.xxx.cn/tag/waitoutputthreads/images/stories/cr0t.php?conf HTTP/1.1"
    println(s"column = ${getColumn(str1)}")
  }
}