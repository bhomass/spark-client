/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

object HBaseTest {
  def main(args: Array[String]) {
    val master = "spark://devserver5";
    val table = "test";
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("spark://devserver5.office.onescreeninc.com:7077").set("spark.executor.memory", "1g")
    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    	conf.set("hbase.zookeeper.quorum", "devserver5")
		conf.set("hbase.zookeeper.property.clientPort","2181")
		conf.set("hbase.master", "devserver5:60000")
    // Other options for configuring scan behavior are available. More information available at 
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    conf.set(TableInputFormat.INPUT_TABLE, table)
    

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(table)) {
      val tableDesc = new HTableDescriptor(args(1))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], 
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.count()

    System.exit(0)
  }
}
