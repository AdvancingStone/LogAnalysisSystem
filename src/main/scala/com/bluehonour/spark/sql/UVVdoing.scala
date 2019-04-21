package com.bluehonour.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * 维度表，求一天或一小时的uv
  */
object UVVdoing {
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

    val df = spark.sql(
      """
        |SELECT
        |	timeid,
        | unix_timestamp(timeid ,'yyyyMMddHHmm') as ts,
        |	sessionid,
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
        |	JOIN refererhostinfo ON ( loginfo2.refererHost = refererhostinfo.url)
      """.stripMargin)

//    df.show()
    df.createOrReplaceTempView("vdoingTemp1")

    val df2 = spark.sql(
      """
         |SELECT
         |	substring( timeid, 0, 8 ) as date,
         |	sessionid,
         |	districtid,
         |	refererhostid,
         |	devicetypeid,
         |	columnid,
         |	count( * ) as uv
         |FROM
         |	(
         |	SELECT
         |		timeid,
         |		ts,
         |		sessionid,
         |		districtid,
         |		refererhostid,
         |		devicetypeid,
         |		columnid,
         |		fts
         |	FROM
         |		(
         |			SELECT
         |				timeid,
         |				ts,
         |				sessionid,
         |				districtid,
         |				refererhostid,
         |				devicetypeid,
         |				columnid,
         |				lag(ts) over ( PARTITION BY sessionid ORDER BY ts ) AS fts
         |			FROM
         |				vdoingTemp1
         |		)
         |	WHERE
         |		fts IS NULL
         |		OR (
         |			fts IS NOT NULL
         |			AND (
         |			( ( ts - fts ) >= 1800 AND devicetypeid = 2 )
         |			OR ( ( ts - fts ) >= 300 AND devicetypeid = 1 )
         |			)
         |		)
         |	)
         |GROUP BY
         |	date,
         |	sessionid,
         |	districtid,
         |	refererhostid,
         |	devicetypeid,
         |	columnid
      """.stripMargin)
    df2.show()
    df2.createOrReplaceTempView("vdoingTemp2")

    spark.sql("insert overwrite table uvVdoingInfo select * from vdoingTemp2")


    spark.stop()
  }
}
