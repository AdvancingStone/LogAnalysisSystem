package com.bluehonour.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticsUVByDay {
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

    //    按终端类型日统计uv
    spark.sql(
      """
        |SELECT
        |	timeid,
        | deviceTypeName,
        |	sum( uv ) AS uv
        |FROM
        |	deviceTypeInfo
        |	JOIN uvVdoingInfo ON ( deviceTypeInfo.devicetypeid = uvVdoingInfo.devicetypeid )
        |GROUP BY
        |	timeid,
        | deviceTypeName
      """.stripMargin).show
    //    按栏目类型日统计uv
    val columnDF: DataFrame = spark.sql(
      """
        |SELECT
        |	timeid,
        |	columnName,
        |	sum( uv ) AS uv
        |FROM
        |	columninfo
        |	JOIN uvVdoingInfo ON ( columninfo.columnid = uvVdoingInfo.columnid )
        |GROUP BY
        |	timeid,
        |	columnName
      """.stripMargin)
    columnDF.show()
    //    按来源类型日统计uv
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	HOST,
        |	sum( uv ) AS uv
        |FROM
        |	refererHostInfo
        |	JOIN uvVdoingInfo ON ( refererHostInfo.refererHostId = uvVdoingInfo.refererHostId )
        |GROUP BY
        |	timeid,
        |	HOST
      """.stripMargin).show(100)
    //    按地域类型日统计uv
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	districtName,
        |	sum( uv ) AS uv
        |FROM
        |	districtInfo
        |	JOIN uvVdoingInfo ON ( districtInfo.districtid = uvVdoingInfo.districtid )
        |GROUP BY
        |	timeid,
        |	districtName
      """.stripMargin).show(100)

    //    每天最热门的栏目Top3
    columnDF.createOrReplaceTempView("columnTempView")
    //使用分析函数，排名函数
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	columnName,
        |	uv,
        |	ROW_NUMBER() OVER ( PARTITION BY timeid ORDER BY uv DESC ) AS rank
        |FROM
        |	columnTempView
        |HAVING
        | rank <= 3
      """.stripMargin).show()
    spark.stop()
  }
}
