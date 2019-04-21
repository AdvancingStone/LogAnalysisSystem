package com.bluehonour.spark.sql

import org.apache.spark.sql.SparkSession

object StaticticsIPByDay {
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
    //ipvdoinginfo
    //    统计一天的独立ip
    spark.sql("select timeid, count(ip) from ipvdoinginfo group by timeid").show()

    //    按栏目类型统计一天的独立ip
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	columnname,
        |	count( * )
        |FROM
        |	columninfo
        |	JOIN ipvdoinginfo ON ( columninfo.columnid = ipvdoinginfo.columnid )
        |GROUP BY
        | timeid,
        |	columnname
      """.stripMargin).show()
    //    按地域类型统计一天的独立ip
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	districtname,
        |	count( * )
        |FROM
        |	districtInfo
        |	JOIN ipvdoinginfo ON ( districtInfo.districtid = ipvdoinginfo.districtid )
        |GROUP BY
        | timeid,
        |	districtname
      """.stripMargin).show()
    //    按终端类型统计一天的独立ip
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	deviceTypeName,
        |	count( * )
        |FROM
        |	deviceTypeInfo
        |	JOIN ipvdoinginfo ON ( deviceTypeInfo.deviceTypeId = ipvdoinginfo.deviceTypeId )
        |GROUP BY
        | timeid,
        |	deviceTypeName
      """.stripMargin).show()
    //    按来源类型统计一天的独立ip
    spark.sql(
      """
        |SELECT
        |	timeid,
        |	host,
        |	count( * )
        |FROM
        |	refererHostInfo
        |	JOIN ipvdoinginfo ON ( refererHostInfo.refererHostId = ipvdoinginfo.refererHostId )
        |GROUP BY
        | timeid,
        |	host
      """.stripMargin).show()
  }
}
