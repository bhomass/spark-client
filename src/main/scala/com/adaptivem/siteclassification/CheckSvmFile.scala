package com.adaptivem.siteclassification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CheckSvmFile {

    val hdfsHost = "devserver2:8020"
  val sparkConf = new SparkConf().setAppName("CheckSvmFile").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
    
  def main(args: Array[String]): Unit = {
//    val input = sc.textFile("hdfs://" + hdfsHost + "/user/adaptive/libsvm/4", 4)
    val input = sc.textFile("hdfs://devserver2/user/adaptive/libsvm/", 4)
    
    val min = input.map{
      line => 
//        println("line = " + line)
        val words = line.split("\\s")
//        val size = words.length
        val last = words(1)
        val min = last.substring(0, last.indexOf(":"))
        println("min = " + min)
        min
        
    }
    
    min.foreach{
      min =>
        if (min.toInt == 0){
          println("bad min")
        }
    }
  }
    
}