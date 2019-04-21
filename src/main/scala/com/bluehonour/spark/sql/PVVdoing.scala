package com.bluehonour.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  维度表，根据每小时或每天
  *  按终端，栏目，地域，来源求PV
  */
object PVVdoing {
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

    //broadcast hash join：将其中一张小表广播分发到另一张大表所在的分区节点上，
    // 分别并发地与其上的分区记录进行hash join（实际上是map操作）
    // broadcast适用于小表很小，可以直接广播的场景

    spark.sql("use loganalysis")

    //timeid: 201701080037
      val df: DataFrame = spark.sql(
        """
          |SELECT
          |	timeid,
          |	districtid,
          |	refererhostid,
          |	devicetypeid,
          |	columnid
          |FROM
          |	loginfo2
          |	JOIN dateTimeInfo ON ( substring(loginfo2.datstr, 0, 12) = dateTimeInfo.timeid )
          |	JOIN columninfo ON ( loginfo2.columnName = columninfo.keyword )
          |	JOIN devicetypeinfo ON ( loginfo2.deviceType = devicetypeinfo.deviceTypeName )
          |	JOIN districtinfo ON ( loginfo2.district = districtinfo.districtName )
          |	JOIN refererhostinfo ON ( loginfo2.refererHost = refererhostinfo.url )
        """.stripMargin)

    df.show(1000,false)

    df.createOrReplaceTempView("logAnalysis3Temp")


    //按终端，栏目，地域，来源求一天的pv
    //timeid: 201701080037
    val dimensionByDay: DataFrame = spark.sql(
      """
        |SELECT
        |	substring( timeid, 0, 8 ) AS yearmonthday,
        |	districtid,
        |	refererhostid,
        |	devicetypeid,
        |	columnid,
        |	count( * ) AS pv
        |FROM
        |	logAnalysis3Temp
        |GROUP BY
        |	yearmonthday,
        |	districtid,
        |	refererhostid,
        |	devicetypeid,
        |	columnid
        |ORDER BY
        |	pv DESC
      """.stripMargin)

    dimensionByDay.show()
    dimensionByDay.createOrReplaceTempView("dimensionByDayTemp")
    //14013
    spark.sql("insert overwrite table pvVdoingDayInfo select * from  dimensionByDayTemp")


    //按终端，栏目，地域，来源求每小时的pv
    //timeid: 201701080037
    val dimensionByHour: DataFrame = spark.sql(
      """
        |SELECT
        |	substring( timeid, 0, 10 ) AS yearmonthdayhour,
        |	districtid,
        |	refererhostid,
        |	devicetypeid,
        |	columnid,
        |	count( * ) AS pv
        |FROM
        |	logAnalysis3Temp
        |GROUP BY
        |	yearmonthdayhour,
        |	districtid,
        |	refererhostid,
        |	devicetypeid,
        |	columnid
        |ORDER BY
        |	pv DESC
      """.stripMargin)

    dimensionByHour.show()
    dimensionByHour.createOrReplaceTempView("dimensionByHourTemp")
    //72798
    spark.sql("insert overwrite table pvVdoingHourInfo select * from  dimensionByHourTemp")
    spark.stop()
  }

}
