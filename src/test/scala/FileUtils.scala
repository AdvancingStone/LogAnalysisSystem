import java.time.LocalDateTime
import java.util.regex.Pattern

import com.bluehonour.spark.utils.Weblog
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 读取文件，封装对象成Weblog
  */
object FileUtils {



  val conf = new SparkConf()
    .setAppName(this.getClass.getCanonicalName)
    .setMaster("local[4]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  /**
    * 读取文件，封装对象成Weblog
    * @param source 文件
    * @return RDD[ Option[WebLog] ]
    */
  def readFileToWeblog(source:  String):  RDD[Option[Weblog]] = {
    val lines = sc.textFile(source)

    val pattern = Pattern.compile("""(\S+) (\S+) \[(.+)\] "(\S+ \S+ \S+)" (\d+) (\d+) "(\S+)" "(.+)"""")

    lines.map(line => {
      val m = pattern.matcher(line)

      if (m.matches()) {
        val time: LocalDateTime = DataUtils.string2DateTime(m.group(3))
        Some(Weblog(m.group(1), m.group(2), time, m.group(4), m.group(5), m.group(6), m.group(7), m.group(8)))
      } else {
        None
      }
    })
  }

}
