import com.bluehonour.spark.utils.Weblog
import org.apache.spark.rdd.RDD

object testfile {
  def main(args: Array[String]): Unit = {
//    val a:RDD[Option[Weblog]] = FileUtils.readFileToWeblog("file:///home/liushuai/data/test1WData.log")
//    a.collect().foreach(println)

    val datetime = "2017-01-08 00:37:52 +0800"
    val dateArr = datetime.split("\\s+")(0).split("-")
    val year  = dateArr(0).toInt
    val month = dateArr(1).toInt
    val day = dateArr(2).toInt
    println(year+" "+month+" "+day)
  }
}
