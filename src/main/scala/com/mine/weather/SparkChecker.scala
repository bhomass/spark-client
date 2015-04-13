package com.mine.weather

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkChecker {
  val hdfsHost = "devserver5/"

  def main(args: Array[String]): Unit = {
    //    val subdirectory = args(0)
    val sparkConf = new SparkConf().setAppName("Spark Checker")
    val rawDataDirectory = "/tmp/weather/[0-25]"

    val sc = new SparkContext(sparkConf)

    val rawData = sc.textFile("hdfs://" + hdfsHost + rawDataDirectory, 4)
    val lumpedData = rawData.coalesce(50)

    lumpedData.cache
    println("lumpedData partition size" + lumpedData.partitions.size)
    
    println("rdd count " + lumpedData.count)

  }

}