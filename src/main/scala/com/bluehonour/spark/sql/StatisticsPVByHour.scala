package com.bluehonour.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 按终端，栏目，地域，来源求每小时的pv
  */
object StatisticsPVByHour {
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
    //    按终端类型日统计pv
    spark.sql(
      """
        |SELECT
        |	timeid,
        | deviceTypeName,
        |	sum( pv ) AS pv
        |FROM
        |	deviceTypeInfo
        |	JOIN pvVdoingHourInfo ON ( deviceTypeInfo.devicetypeid = pvVdoingHourInfo.devicetypeid )
        |GROUP BY
        |	timeid,
        | deviceTypeName
      """.stripMargin).show
    //    按栏目类型日统计pv
    val columnDF: DataFrame = spark.sql(
      """
        |SELECT
        |	timeid,
        |	columnName,
        |	sum( pv ) AS pv
        |FROM
        |	columninfo
        |	JOIN pvVdoingHourInfo ON ( columninfo.columnid = pvVdoingHourInfo.columnid )
        |GROUP BY
        |	timeid,
        |	columnName
      """.stripMargin)

    //    按来源类型日统计pv
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	HOST,
        |	sum( pv ) AS pv
        |FROM
        |	refererHostInfo
        |	JOIN pvVdoingHourInfo ON ( refererHostInfo.refererHostId = pvVdoingHourInfo.refererHostId )
        |GROUP BY
        |	timeid,
        |	HOST
      """.stripMargin).show(100)

    //    按地域类型日统计pv
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	districtName,
        |	sum( pv ) AS pv
        |FROM
        |	districtInfo
        |	JOIN pvVdoingHourInfo ON ( districtInfo.districtid = pvVdoingHourInfo.districtid )
        |GROUP BY
        |	timeid,
        |	districtName
      """.stripMargin).show(100)


    //    每天最热门的栏目Top3
//      import spark.implicits._
//    columnDF.sort($"pv".desc).show
    columnDF.createOrReplaceTempView("columnTempView")

    //使用分析函数，排名函数
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	columnName,
        |	pv,
        |	ROW_NUMBER() OVER ( PARTITION BY timeid ORDER BY pv DESC ) AS rank
        |FROM
        |	columnTempView
        |HAVING
        | rank <= 3
      """.stripMargin).show(100)
    spark.stop()
  }

}
