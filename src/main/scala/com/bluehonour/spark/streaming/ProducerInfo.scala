package com.bluehonour.spark.streaming

import java.io.{File, FileWriter, PrintWriter}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random

object ProducerInfo {
  // brokers的地址，可以写多个。至少写2个避免单点故障
  val brokers = "slave:9092, slave1:9092"
  //val topic = "mykafka"
  // Properties类实现了Map接口，本质上是一种简单的Map容器
  val kafkaProps = new Properties()

  // 要往Kafka写入消息，首先要创建一个生产者对象，并设置一些属性。Kafka生产者有3个必选的属性
  // bootstrap.servers，指定broker的地址清单。不需要包含所有的broker地址，生产者会从给定的broker里查找其他broker的信息，建议至少提供2个broker的信息
  // key.serializer 将指定的值序列化。必选设置，即使key为空。 value.serializer 将value的值序列化
  kafkaProps.put("bootstrap.servers", brokers)
  kafkaProps.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer")

  // 使用字符串常量更好，避免输入错误
  //    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      "node1:9092, node2:9092")
  //    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   "org.apache.kafka.common.serialization.StringSerializer")
  //    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")


  def producerA(topic: String, message: String) = {
    // KafkaProducer是一个泛型，必须指定key、value的类型
    val producer = new KafkaProducer[String, String](kafkaProps)
    // topic, key, value（可以不指定类型）
    val num = Random.nextInt(3)
    println(num)
    val groupId = topic + num
    val record = new ProducerRecord[String, String](topic, groupId, message)
    producer.send(record)
    producer.close()
  }

  //存储到本地中的方法
  def saveOrder(filePath: String, message: String):Unit = {
    val writer: FileWriter = new FileWriter(filePath, true);
    writer.write(message);
    writer.write("\n")
    writer.close()
  }

  def savePay(filePath: String, message: String):Unit = {
    val writer: FileWriter = new FileWriter(filePath, true);
    writer.write(message);
    writer.write("\n")
    writer.close()
  }
}
