package com.bluehonour.spark.etl

import com.bluehonour.spark.utils.{IpInfo, LogUtils}
import org.apache.spark.sql.SparkSession

object LogAnalyze {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
//      .master("spark://master:7077")
        .master("local[4]")
      .enableHiveSupport()
      .getOrCreate()


    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    sc.addJar("/home/liushuai/IdeaProjects/logAnalysisProject/out/artifacts/logAnalysisProject_jar/logAnalysisProject.jar")

    // 将ip的信息排序后转变为广播变量
      val ipArr: Array[IpInfo] = sc.textFile("hdfs://master:9000/user/spark/data/logAnalysisProject/ipaddress.dat")
      .map(_.trim.split("\\s+"))
      .filter(_.length == 3)
      .map(x => IpInfo(LogUtils.ipv4ToLong(x(0)), LogUtils.ipv4ToLong(x(1)), x(2)))
      .collect
      .sortBy(_.startIp)
    //转变为广播变量
    val ipBC = sc.broadcast(ipArr)

    // 日志解析。做了5件事：正则表达式的解析、ip转地址、图片等资源的过滤,过滤 4xx 5xx的状态码,将yearmonthday进行拆分成年月日
    val logInfoRDD = sc.textFile("hdfs://master:9000/user/spark/data/logAnalysisProject/access.log")
      .mapPartitions(iter => {
        val ipInfo = ipBC.value
        //ip转地址
        val iter1 = iter.map(line => LogUtils.LogParse(line, ipInfo))
        iter1
      })
      .filter(_.isDefined)
      .map(_.get)
      //  过滤 4xx, 5xx的状态码
      .filter(x => !LogUtils.isValidStatus(x.status))
      // 过滤下载资源的信息
      .filter(x => !LogUtils.isStaticResource(x.url))
      //将yearmonthday进行拆分成年月日，根据年月日进行分区存表
      .map(x=>{
        val year = x.yearmonthday.substring(0,4)
        val month = x.yearmonthday.substring(4,6)
        val day = x.yearmonthday.substring(6)
        (x.ip,x.district,x.sessionid,x.datetime,x.daystr, x.url,x.status,x.sentBytes,x.referal, x.userAgent,year,month,day)
      })

//    logInfoRDD.collect
//      .foreach(println)

    //保存到hive中
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addJar("/home/liushuai/IdeaProjects/logAnalysisProject/out/artifacts/logAnalysisProject_jar/logAnalysisProject.jar")

    //将rdd转换成DF，然后创建临时表，根据年月日进行分区将临时表中的数据存入hive中
    spark.sql("use loganalysis")
    import spark.implicits._
    val df = logInfoRDD.toDF("ip","district","sessionid","datetime","daystr","url","status","sentBytes"  ,"referal","userAgent","year","month","day")
    df.createOrReplaceTempView("temp")
    spark.sql("select count(*) from temp").show
//    spark.sql("select * from temp ").show(500,false)

    //开启动态分区功能
    spark.sql("set hive.exec.dynamic.partition=true")
    //所有分区都是动态的（动态分区的模式）
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //  设置最大动态分区个数1000
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=1000 ")

    //927030
    spark.sql("insert overwrite table loginfo  partition(year,month,day) select * from  temp")


    spark.stop()
  }
}