package com.vc.tutorial.spark

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-8-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-10-integration.html
  *
  * Only Receiver Based Approach Until 1.3.0
  * https://spark.apache.org/docs/1.2.2/streaming-kafka-integration.html
  *
  * Receiver Based + Direct Stream Approach
  * https://spark.apache.org/docs/1.3.0/streaming-kafka-integration.html
  *
  * https://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming/
  */
object KafkaStreamOffsetIntoZk08 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaStreamOffsetIntoZk08").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))


    // Direct stream approach
    val kafkaParam = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "auto.offset.reset" -> "smallest"
    )
    val groupId = "KafkaStreamOffsetIntoZk08"
    val fromOffset = readOffsetFromZookeeper(Seq("test"), groupId)
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
      ssc,
      kafkaParam,
      fromOffset,
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
    )

    //Processing Stream
    stream.foreachRDD(rdd => {
      //Do processing
      rdd.foreach(r => {
        println(s"value: ${r._2}")
      })
      //Update consumer offset in zookeeper
      writeOffsetToZookeeper(rdd.asInstanceOf[HasOffsetRanges].offsetRanges, groupId)
    })

    //Starting StreamingContext and awaitTermination
    ssc.start()
    ssc.awaitTermination()
  }


  private def writeOffsetToZookeeper(offsetRanges: Array[OffsetRange], groupId: String): Unit = {
    println("Writing offset to zookeeper")
    val zkClient = new ZkClient("localhost:2181", Integer.MAX_VALUE, 10000, ZKStringSerializer)
    offsetRanges.foreach(x=>{
      ZkUtils.updatePersistentPath(zkClient, s"${ZkUtils.ConsumersPath}/$groupId/${x.topic}/${x.partition}/${x.untilOffset}", "")
    })
  }

  private def readOffsetFromZookeeper(topicNames: Seq[String], groupId: String): Map[TopicAndPartition, Long] = {
    val fromOffset = collection.mutable.HashMap.empty[TopicAndPartition, Long]
    val zkClient = new ZkClient("localhost:2181", Integer.MAX_VALUE, 10000, ZKStringSerializer)
    val output = ZkUtils.getPartitionsForTopics(zkClient, topicNames)
    output.foreach(x=> {
      val topicName = x._1
      val partitions = x._2
      partitions.foreach(p=>{
        ZkUtils.readDataMaybeNull(zkClient, s"${ZkUtils.ConsumersPath}/$groupId/$topicName/$p")._1 match {
          case Some(data) => fromOffset.put(TopicAndPartition(topicName, p), data.toLong)
          case None => fromOffset.put(TopicAndPartition(topicName, p), 0l)
        }
      })
    })
    fromOffset.toMap
  }

}
