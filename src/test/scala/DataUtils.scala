import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

object DataUtils {
  val customFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")
  val zone = ZoneOffset.of("+8")

  def string2DateTime(str: String): LocalDateTime = {
    LocalDateTime.parse(str, customFormatter)
  }

  implicit def LocalDateTime2Long(t: LocalDateTime): Long = {
    t.toInstant(zone).toEpochMilli
  }

  def main(args: Array[String]): Unit = {
    val str1 = "2019-01-01 23:09:07 +0800"
    val str2 = "2019-01-01 23:19:07 +0800"
    val t1 = string2DateTime(str1)
    val t2 = string2DateTime(str2)
    println(t1, t2)
    println(t1.isAfter(t2))
    println(t1.isBefore(t2))
    println(t1.toInstant(zone).toEpochMilli)
    println(t2.toInstant(zone).toEpochMilli)

    println(t1 < t2)
  }
}
