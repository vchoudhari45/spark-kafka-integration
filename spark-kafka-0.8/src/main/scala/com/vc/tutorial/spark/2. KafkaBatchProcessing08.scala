package com.vc.tutorial.spark

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{KafkaUtils, OffsetRange}
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
  */
object KafkaBatchProcessing08 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaStreamOffset08")
    val sc = new SparkContext(sparkConf)

    val kafkaParam = Map("bootstrap.servers" -> "localhost:9092")
    val offsetRange = Array(
      OffsetRange("test", 0, 0, 2),
      OffsetRange("test", 1, 0, 2),
      OffsetRange("test", 2, 1, 2)
    )
    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](sc, kafkaParam, offsetRange)
    rdd.foreach(r=>{println(s"value: ${r._2}")})
  }
}
