package com.spark22.structuredstreaming.fmt

import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCount {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
//    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = words.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println

