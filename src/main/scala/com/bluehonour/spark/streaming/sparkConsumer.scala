package com.bluehonour.spark.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import sparkStreaming.sparkHandleUtils._
import OrderInfoUtils._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import sparkStreaming.ProducerInfo.{kafkaProps, producerA}
import sparkStreaming.KafkaDirectWithHBase._

import scala.util.Random

object sparkConsumer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getCanonicalName}")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    //创建messageHandle接受信息，并能够从hbase中读取offset值，获得ds
    val kafkaDS = createKafkaDirectStream(ssc, kafkaParams, topics, groupId)
    //对ds进行处理
    kafkaDS.foreachRDD((rdd, time) => {
      if (!rdd.isEmpty()){
        // 打印每批次接收到的总数据量 和 时间戳
        println(s"************** rdd.count = ${rdd.count()}; time = $time **************")
        //打印offset的情况(topic, partition, from, until)topicB
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
          println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        }

        //获取订单信息topic and 未满足发送信息的topic
        val orderRdd1 = rdd.filter(x => filterProduceOrderInfoRDD(x._1))
          .map(line => {
            val arr = line._2.split(""",""")
            (arr(1), (arr(0), arr(3)))
          })
        //获取支付信息topic and 先接受到支付信息的topic
        val payRdd1 = rdd.filter(x => filterProducerOrderidsRDD(x._1))
          .map(line => {
            val arr = line._2.split(""",""")
            (arr(0), arr(1))
          })
        //两种情况进行数据的全外连接
        val rddunion: RDD[(String, (Option[(String, String)], Option[String]))] = orderRdd1.fullOuterJoin(payRdd1)
        //数据的判度
        rddunion.foreachPartition(iter => {
          // KafkaProducer是一个泛型，必须指定key、value的类型
          val producer = new KafkaProducer[String, String](kafkaProps)
          // topic, key, value（可以不指定类型）
          val num = Random.nextInt(3)
          println(num)
          iter.foreach{case (key, value) => {
            val x = value._1.getOrElse(0)
            val y = value._2.getOrElse(0)
            //已下订单的情况
            if(x != 0 && y == 0){
              val timeNow = getNow
              val l = value._1.get._2
              //时间处理
              val timeDiff = timehandle(timeNow, l)
              //time达到5分钟，进行信息的发送
              if(timeDiff > 5*60*1000){
                //时间到达发信息
                val sendmessage = value._1.get._1
                println("*******************************")
                println("*******************************")
                println("*******************************")
                println("*******************************")
                println("*******************************")
                println(s"messageUserId = ${sendmessage}")
              }else{
                //时间没到达放入到kafkaA-
                val topicA = "order2"
                val mess = value._1.get._1 + "," + key + "," + "nothing"+ "," + value._1.get._2
                println(mess)
                val groupId = key + num
                val record = new ProducerRecord[String, String](key, groupId, mess)
                producer.send(record)
                producer.close()
              }
            }else if(x == 0 && y != 0){
              //放入到kafka中B-
              val topicB = "pay2"
              val mess = key + "," + value._2.get
              val groupId = key + num
              val record = new ProducerRecord[String, String](key, groupId, mess)
              producer.send(record)
              producer.close()
            }else{
              //订单已经支付的情况不做任何处理
            }
          }}
        })
        //保存offset值
        saveOffsets(topics: Set[String], "", offsetRanges, time)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
