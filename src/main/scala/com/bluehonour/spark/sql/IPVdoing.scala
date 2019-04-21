package com.bluehonour.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 维度表，求一天或一小时的独立ip访问量
  */
object IPVdoing {
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
    //
    val ipVdoingDF1: DataFrame = spark.sql(
      """
        |SELECT
        |	timeid,
        |	ip,
        |	districtid,
        |	refererhostid,
        |	devicetypeid,
        |	columnid
        |FROM
        |	loginfo2
        |	JOIN dateTimeInfo ON ( substring( loginfo2.datstr, 0, 12 ) = dateTimeInfo.timeid )
        |	JOIN columninfo ON ( loginfo2.columnName = columninfo.keyword )
        |	JOIN devicetypeinfo ON ( loginfo2.deviceType = devicetypeinfo.deviceTypeName )
        |	JOIN districtinfo ON ( loginfo2.district = districtinfo.districtName )
        |	JOIN refererhostinfo ON ( loginfo2.refererHost = refererhostinfo.url )
      """.stripMargin)

    ipVdoingDF1.createOrReplaceTempView("ipVdoingTemp")
//    ipVdoingDF1.show(100)
    val ipVdoingDF: DataFrame = spark.sql(
      """
         |SELECT
         |	substring( timeid, 0, 8 ) AS yearmonthday,
         |	ip,
         |	districtid,
         |	refererhostid,
         |	devicetypeid,
         |	columnid
         |FROM
         |	ipVdoingTemp
         |GROUP BY
         |  ip,
         |	yearmonthday,
         |	districtid,
         |	refererhostid,
         |	devicetypeid,
         |	columnid
      """.stripMargin)
    ipVdoingDF.show()
    ipVdoingDF.createOrReplaceTempView("ipVdoingTemp2")
    spark.sql(
      """
        |insert overwrite table ipVdoingInfo select * from ipVdoingTemp2
      """.stripMargin)

    spark.stop()
  }

}
