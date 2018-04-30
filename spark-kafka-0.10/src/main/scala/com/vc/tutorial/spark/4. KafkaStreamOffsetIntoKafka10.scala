package com.vc.tutorial.spark

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
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
object KafkaStreamOffsetIntoKafka10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaStreamOffsetIntoZk08").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))


    // Direct stream approach
    val kafkaParam = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG  -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      ConsumerConfig.GROUP_ID_CONFIG -> "KafkaStreamOffsetIntoZk08"
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Seq("test"),
        kafkaParam
      )
    )
    stream.foreachRDD { rdd =>
      rdd.foreach(r => {
        println(s"value: ${r.value}")
      })
      //Commit offset to Kafka
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    //Starting StreamingContext and awaitTermination
    ssc.start()
    ssc.awaitTermination()
  }
}
