package com.bluehonour.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils, OffsetRange}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object KafkaDirectWithHBase {
  private val hbaseTableName = "kafka_offsets"
  val brokers = "slave:9092,slave1:9092"
  // largest  : 表示接受接收最大的offset，即最新消息 (defaults)
  // smallest : 表示最小offset，即从topic的开始位置消费所有消息
  val kafkaParams = Map(("metadata.broker.list", brokers),
    ("auto.offset.reset", "smallest"))
  val topics = Set("produceOrderInfo", "producerOrderids", "order2", "pay2")
  val groupId = ""

  // 保存Offset, 支持批量插入
  def saveOffsets(topics: Set[String], groupId: String,
                  offsetRanges: Array[OffsetRange], batchTime: Time): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val putArray = ArrayBuffer[Put]()
    topics.foreach(topic => {
      val rowkey = s"$topic:$groupId:${String.valueOf(batchTime.milliseconds)}"
      val put = new Put(rowkey.getBytes)
      for (offset <- offsetRanges) {
        put.addColumn(Bytes.toBytes("offsets"),
          Bytes.toBytes(offset.partition.toString), Bytes.toBytes(offset.untilOffset.toString))
      }
      putArray += put
    })

    // 批量插入
    import scala.collection.JavaConversions._
    table.put(putArray)
    table.close()
    conn.close()
  }

  // 从HBase中获得Map[TopicAndPartition, Long]. 这是保存在外部存储中的Offsets值
  private def getOffsetsFromHBase(topics: Set[String], groupId: String): Map[TopicAndPartition, Long] = {
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf(hbaseTableName))
    val scan = new Scan()
    var scanner: ResultScanner = null

    // 用可变集合存放最终的结果. 结果类型为：Map[TopicAndPartition, Long]
    var externalOffsets = mutable.Map[TopicAndPartition, Long]()
    // 对每个topic进行一次循环
    topics.foreach(topic => {
      // 定义scanner
      val startRow = s"$topic:$groupId:${String.valueOf(System.currentTimeMillis())}"
      val stopRow  = s"$topic:$groupId:0"
      scanner = table.getScanner(scan.withStartRow(startRow.getBytes).withStopRow(stopRow.getBytes).setReversed(true))
      val resultScanner = scanner.next()

      // 如果有对应的信息，将结果加到externalOffsets中. 无对应的信息不管，后面再去调整
      // externalOffsets中offsets的数量 < 实际存在的offsets的数量
      // 原因：新增的topic 或 原有topic新增了partition
      if (resultScanner != null) {
        resultScanner.rawCells().foreach(cell => {
          val partition = Bytes.toString(CellUtil.cloneQualifier(cell)).toInt
          val offset = Bytes.toString(CellUtil.cloneValue(cell)).toLong
          val tp = TopicAndPartition(topic, partition)
          externalOffsets += ((tp, offset))
        })
      }

      externalOffsets
    })

    scanner.close()
    conn.close()

    externalOffsets.toMap
  }

  // 考虑各种可能的情况，调整Offsets
  // 1、首次启动时，外部存储没有保存offsets，最小值
  // 2、如 offsets 越界，调整为最大或最小值
  private def resetOffsets(topics: Set[String],
                           hbaseOffsets: Map[TopicAndPartition, Long],
                           samllestOffsets: Map[TopicAndPartition, Long],
                           largestOffsets: Map[TopicAndPartition, Long],
                           kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    val offsetParam = kafkaParams.getOrElse("auto.offset.reset", "").toLowerCase
    var offsets = mutable.Map[TopicAndPartition, Long]()

    samllestOffsets.foreach { case (tp, minOffset) =>
      val maxOffset = largestOffsets(tp)

      // 不包含说明没有，赋最小值
      if (!hbaseOffsets.contains(tp)) {
        offsets += (tp -> minOffset)
      }
      else {
        // offset 合法性检查
        val offset = hbaseOffsets(tp)
        val tempOffset =
          if (offset < minOffset) minOffset
          else if (offset > maxOffset) maxOffset
          else offset
        offsets += (tp -> tempOffset)
      }
    }

    val result = offsetParam match {
      case "largest" => largestOffsets
      case "smallest" => samllestOffsets
      case _ => offsets.toMap
    }
    result
  }

  // 从HBase中获取保存的offset。如果HBase中没有信息，则将offset置为0
  private def getOffsets(kafkaParams: Map[String, String], topics: Set[String],
                         groupId: String): Map[TopicAndPartition, Long] = {
    // 需要逐个topic处理
    // 每个处理过程都需要完成：获取3个Offsets; 根据情况完成Offsets的赋值
    val kc = new KafkaCluster(kafkaParams)
    val topicAndPartitons = kc.getPartitions(topics).right.get
    val maxOffsets:  Map[TopicAndPartition, Long] = kc.getLatestLeaderOffsets(topicAndPartitons).right.get.mapValues(x => x.offset)
    val minOffsets:  Map[TopicAndPartition, Long] = kc.getEarliestLeaderOffsets(topicAndPartitons).right.get.mapValues(x => x.offset)
    val realOffsets: Map[TopicAndPartition, Long] = getOffsetsFromHBase(topics, groupId)

    resetOffsets(topics, realOffsets, minOffsets, maxOffsets, kafkaParams)
  }

  def createKafkaDirectStream (ssc: StreamingContext, kafkaParams: Map[String, String],
                               topics: Set[String], groupId: String): InputDStream[(String, String)] = {
    val fromOffsets = getOffsets(kafkaParams, topics, groupId)
    val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    val kafkaDS = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
      (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    kafkaDS
  }

  /*def main(args: Array[String]): Unit = {
    val brokers = "node1:9092"
    val kafkaParams1 = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest")
    val kafkaParams2 = Map[String, String]("metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest")
    val kafkaParams3 = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = Set("mykafka1", "mytopic1", "mytopic2")
    //    val groupId = "mycg1"

    val hOffsets = Map(
      TopicAndPartition("mykafka1", 0) -> 10L,
      TopicAndPartition("mykafka1", 1) -> 99999L)

    val sOffsets = Map(
      TopicAndPartition("mykafka1", 0) -> 100L,
      TopicAndPartition("mykafka1", 1) -> 90L,
      TopicAndPartition("mykafka1", 2) -> 80L,
      TopicAndPartition("mytopic1", 0) -> 0L,
      TopicAndPartition("mytopic1", 1) -> 0L,
      TopicAndPartition("mytopic1", 2) -> 0L,
      TopicAndPartition("mytopic1", 3) -> 0L,
      TopicAndPartition("mytopic2", 0) -> 100L,
      TopicAndPartition("mytopic2", 1) -> 100L,
      TopicAndPartition("mytopic2", 2) -> 100L,
      TopicAndPartition("mytopic2", 3) -> 100L,
      TopicAndPartition("mytopic2", 4) -> 100L)

    val lOffsets = Map(
      TopicAndPartition("mykafka1", 0) -> 1000L,
      TopicAndPartition("mykafka1", 1) -> 990L,
      TopicAndPartition("mykafka1", 2) -> 990L,
      TopicAndPartition("mytopic1", 0) -> 1000L,
      TopicAndPartition("mytopic1", 1) -> 2000L,
      TopicAndPartition("mytopic1", 2) -> 3000L,
      TopicAndPartition("mytopic1", 3) -> 4000L,
      TopicAndPartition("mytopic2", 0) -> 10000L,
      TopicAndPartition("mytopic2", 1) -> 20000L,
      TopicAndPartition("mytopic2", 2) -> 30000L,
      TopicAndPartition("mytopic2", 3) -> 40000L,
      TopicAndPartition("mytopic2", 4) -> 50000L)

    println("auto.offset.reset = smallest")
    val a = resetOffsets(topics, hOffsets, sOffsets, lOffsets, kafkaParams1)
    a.toArray.map(_.toString()).sorted.foreach(println)
    println("\n*************************************************************\n")

    println("auto.offset.reset = largest")
    val b = resetOffsets(topics, hOffsets, sOffsets, lOffsets, kafkaParams2)
    b.toArray.map(_.toString()).sorted.foreach(println)
    println("\n*************************************************************\n")

    println("auto.offset.reset = \"\"")
    val c = resetOffsets(topics, hOffsets, sOffsets, lOffsets, kafkaParams3)
    c.toArray.map(_.toString()).sorted.foreach(println)
  }*/
}
