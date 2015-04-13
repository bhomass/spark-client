package com.mine.weather

import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer

object NOAAParser {
  val hdfsHost = "devserver5/"
  val rawDataDirectory = "/tmp/weather/ghcnd_us"
  val formattedDataDirectory = "/tmp/weather/data1"
  val desiredElements = Array[String]("TMAX", "TMIN", "PRCP", "SNOW", "SNWD")
  val TMAX = "TMAX"
  val TMIN = "TMIN"
  val PRCP = "PRCP"
  val SNOW = "SNOW"
  val SNWD = "SNWD"
  val format = new SimpleDateFormat("yyyy-mm-dd")

  def main(args: Array[String]): Unit = {
//    val subdirectory = args(0)
    val sparkConf = new SparkConf().setAppName("NOAA parser")

    val sc = new SparkContext(sparkConf)

    val rawData = sc.textFile("hdfs://" + hdfsHost + rawDataDirectory , 4)
//    rawData.coalesce(3000, false)
    val groupedData = rawData.coalesce(100)
//    groupedData.cache
    
    println("groupedData partition size = " + groupedData.partitions.size)

    val keyNameValueArray = groupedData.map {
      line =>
        val keyNameValueArray = ArrayBuffer[(String, String)]() // stationId-day, element:value

        val stationId = line.substring(0, 11)
        val year = line.substring(11, 15)
//        println("year = " + year)

        if (year.startsWith("200")) {		// only keep data from the 200* decade
          val month = line.substring(15, 17)
          println("month = " + month)
          val element = line.substring(17, 21)
          println("element = " + element)

          if (desiredElements.contains(element)) {
            val valueList = fillDayValues(line)

            var day = 0
            for (dayValue <- valueList) {
              // can do a valid day check (months that have fewer than 31 days
              val rowKey = stationId + ":" + year + "-" + month + "-" + "%02d".format(day)
              val nameValue = element + ":" + dayValue
              println("nameValue = " + nameValue)
              keyNameValueArray.append( (rowKey, nameValue) )
              day += 1
              println("day = " +day)
            }
          }
        } // else return empty array
        keyNameValueArray.toArray[(String, String)]
    }
    val flatted =  keyNameValueArray.flatMap(knv => knv)
    val grouped =  flatted.groupByKey()
    val stationTuple =   grouped.map {
        keyArray =>
          val nvArray = keyArray._2

          var tmax = -9999
          var tmin = -9999
          var prcp = -9999
          var snow = -9999
          var snwd = -9999

          for (nv <- nvArray) {
            val elementValue = nv.split(":")
            val element = elementValue(0)
            val value = elementValue(1)
            println("element = " + element)
            if (element.equals(TMAX)) {
              tmax = value.trim().toInt
            } else if (element.equals(TMIN)) {
              tmin = value.trim().toInt
            } else if (element.equals(PRCP)) {
              prcp = value.trim().toInt
            } else if (element.equals(SNOW)) {
              snow = value.trim().toInt
            } else if (element.equals(SNWD)) {
              snwd = value.trim().toInt
            }
          }

          val stationYearMonthDay = keyArray._1.split(":")
          val stationId = stationYearMonthDay(0)
          val yearMonthDay = stationYearMonthDay(1)
          println("stationId = " + stationId +", yearMonthDay = " + yearMonthDay + ", tmax" + tmax) 
          stationId + "," + yearMonthDay + "," + tmax +"," + tmin +"," + prcp +","+ snow +"," + snwd
      }
      stationTuple.saveAsTextFile("hdfs://" + hdfsHost + formattedDataDirectory)
  }

  def fillDayValues(line: String): scala.collection.mutable.MutableList[String] = {
    val lineLength = line.length()
    println("line length = " + lineLength)
    var startingIndex = 21
    val valueList = new scala.collection.mutable.MutableList[String]()

    while (startingIndex < lineLength) {
//      println("startingIndex = " + startingIndex)
      val value = line.substring(startingIndex, startingIndex + 5)
      valueList += value
      startingIndex += 8

    }
    valueList
  }

}