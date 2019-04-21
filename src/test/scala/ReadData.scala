import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
//    日志信息：
//    ip:         String,          // 客户端的ip地址
//    sessionid:  String,          // sessionid
//    time:       LocalDateTime,   // 访问时间
//    url:        String,   // 请求的url、http协议等
//    status:     String,   // 请求状态；成功是200
//    sentBytes:  String,   // 发送给客户端文件主体内容大小
//    referer:    String,   // 记录从那个页面链接访问过来的
//    userAgent:  String    // 客户浏览器的相关信息

//    61.169.62.163
//    80b378fb-b846-439d-8e0c-8835b08d66bb
//    [2017-01-08 12:01:20 +0800]
//    "GET /wp-content/uploads/2016/07/ggplot2.png HTTP/1.1"
//    200
//    817
//    "-"
//    "Mozilla/5.0 (Windows; U; Windows NT 6.1; ) AppleWebKit/534.12 (KHTML, like Gecko) Maxthon/3.0 Safari/534.12"

object ReadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getCanonicalName)
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val lines = sc.textFile("file:///home/liushuai/data/test1WData.log")
    println(lines.count())
    val  data1 = lines.collect()

    val pattern = Pattern.compile("""(\S+) (\S+) \[(.+)\] "(\S+ \S+ \S+)" (\d+) (\d+) "(\S+)" "(.+)"""")
    var num = 0
    data1.map(line => {
      val m = pattern.matcher(line)
      if (m.matches()) {
        for (i <- 0 to m.groupCount()) {
          println(s"group(${i}) = ${m.group(i)}")
        }
        num+=1
      } else {
        println("no no no")
        println(line)
      }
    })

    println()
    println(s"lines count is ${num}")
  }
}
