package com.vc.tutorial.spark

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
  * https://blog.cloudera.com/blog/2017/06/offset-management-for-apache-kafka-with-apache-spark-streaming
  */
object KafkaStreamOffsetIntoKafka08 {
  def main(args: Array[String]): Unit = {
    println("Option is only available in kafka-0.10 and higher and Spark 2.1.X higher")
  }
}
