package com.bluehonour.spark.etl

import com.bluehonour.spark.utils.{LogInfo2, LogUtils}
import org.apache.spark.sql.{Dataset, SparkSession}

object LogAnalysis2 {
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

    spark.sql("use loganalysis")
    val df = spark.sql("select ip, district, sessionid, datetime, daystr, url, referal, useragent  from loginfo")
    //    df.show()
    import spark.implicits._
    val logInfoDF: Dataset[LogInfo2] = df.map(x => {
      val ip = x.getString(0)
      val district = x.getString(1)
      val sessionid = x.getString(2)
      val datetime = x.getString(3)
      val daystr = x.getString(4) //年月日时分秒: 20170108003804
      //根据请求方式获取栏目信息
      val columnName = LogUtils.getColumn(x.getString(5))
      //获取用户来源信息
      val refererHost = LogUtils.getReferalHost(x.getString(6))
      //将用户设备转换成设备类型（移动端，PC端，unknown）
      val deviceType = LogUtils.getDeviceType(x.getString(7))
      //      val dateArr = datetime.split("\\s+")(0).split("-")
      //      val year = dateArr(0).toInt
      //      val month = dateArr(1).toInt
      //      val day = dateArr(2).toInt
      val year = daystr.substring(0,4).toInt
      val month = daystr.substring(4,6).toInt
      val day = daystr.substring(6,8).toInt
      //封装成对象
      LogInfo2(ip, district, sessionid, datetime,daystr, columnName, refererHost, deviceType, year, month, day)
    })


    //保存到hive中
    //    spark.sql("use logAnalysis")
    //    import spark.implicits._
    //    val logInfoDF = logInfoRDD.toDF()
    logInfoDF.createOrReplaceTempView("logInfo2DFTemp")
    spark.sql("select count(*) from logInfo2DFTemp").show
    spark.sql("select * from logInfo2DFTemp ").show(500,false)

    //开启动态分区功能
    spark.sql("set hive.exec.dynamic.partition=true")
    //所有分区都是动态的（动态分区的模式）
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //  设置最大动态分区个数1000
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=1000 ")

    //// 将查到的数据保存到hive中(927030)
    spark.sql("insert overwrite table loginfo2  partition(year,month,day) select * from  logInfo2DFTemp")


    spark.stop()
  }
}
