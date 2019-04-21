import org.apache.spark.sql.SparkSession

object HiveDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Hive Demo")
      .master("spark://master:7077")
//        .master("local[5]")
      .enableHiveSupport()	// 支持hive，这个是关键，没有不行！
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addJar("/home/liushuai/IdeaProjects/logAnalysisProject/out/artifacts/logAnalysisProject_jar/logAnalysisProject.jar")

    spark.sql("use sparktest")
    spark.sql("select * from userinfo").show(false)

    spark.stop()
  }
}
