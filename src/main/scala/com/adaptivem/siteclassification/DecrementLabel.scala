package com.adaptivem.siteclassification

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object DecrementLabel {
  val conf = new Configuration()
      val hdfsCoreSitePath = new Path("core-site.xml")
      conf.addResource(hdfsCoreSitePath)

  val fs = FileSystem.get(conf);
  println("fs scheme: " + fs.getScheme())

  val hdfsHost = "devserver2.office.onescreeninc.com:8020"
  val inPath = "hdfs://" + hdfsHost + "/user/adaptive/libsvm/"

  val sparkConf = new SparkConf().setAppName("DecrementLabel").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 23) {
      val parsed = sc.textFile(inPath + i, 4)
        //      .map(_.trim)
        .filter(line => !(line.isEmpty || line.startsWith("#")))
        .map { line =>
          val firstSpace = line.indexOf(" ")
          val label = line.substring(0, firstSpace).toInt
          val newLabel = label - 1
          println("label = " + label)
          val tail = line.substring(firstSpace + 1)
          (newLabel + " " + tail + "\n")
        }

      //    parsed.foreach(println)
      //    val outPath = "hdfs://" + hdfsHost + s"/user/adaptive/libsvm1/$i"
      //    println("outpath = " + outPath)
      val collected = parsed.collect
      saveOneLabel(collected, i.toString)
    }
  }

  def saveOneLabel(labeledPointStringArray: Array[String], label: String) {
    println("save One Label : " + label + ", string buffer size = " + labeledPointStringArray.size)
    val filenamePath = new Path(s"/user/adaptive/libsvm1/$label");
    val fout = fs.create(filenamePath)
    for (labeledPoint <- labeledPointStringArray) {
      //      println("writing labeledPoint = " + labeledPoint)
      fout.write((labeledPoint).getBytes())

    }
    fout.close()

  }

}