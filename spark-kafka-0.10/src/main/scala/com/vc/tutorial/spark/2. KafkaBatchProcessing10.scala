package com.vc.tutorial.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

/**
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-8-integration.html
  * https://spark.apache.org/docs/2.0.1/streaming-kafka-0-10-integration.html
  * https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
  *
  * https://blog.cloudera.com/blog/2015/03/exactly-once-spark-streaming-from-apache-kafka/
  */
object KafkaBatchProcessing10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaStreamOffset10")
      .setMaster("local[*]")


    val sc = new SparkContext(sparkConf)
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "kafkaStreamOffset10"
    ).asJava

    val offsetRange = Array(
      OffsetRange("test", 0, 0, 2),
      OffsetRange("test", 1, 0, 0)
    )
    val rdd = KafkaUtils.createRDD(sc, kafkaParams, offsetRange, LocationStrategies.PreferConsistent)
    rdd.foreach(r=>println(s"partition: ${r.partition()} offset: ${r.offset()} value: ${r.value()}"))
  }
}
