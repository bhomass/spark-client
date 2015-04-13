package com.mine.hbase

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import java.security.MessageDigest
import scala.util.Random
import org.apache.commons.lang.RandomStringUtils
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Result

object HBaseConnector {
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val random = new Random()
  val ALLINONE_FAMILY = "allinone"
  val CUSTOMERORDERTYPE = 1
  val ORDERRECORDTYPE = 2
  val SHIPPINGRECORDTYPE = 3
  val LINEITEMRECORDTYPE = 4
  val THREEYEARINMSEC = 3 * 365 * 24 * 60 * 60 * 1000

  val hbaseConfiguration = { // this is executed only once per partition
    val hbaseSitePath = new Path("hbase-site.xml")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.addResource(hbaseSitePath)
    hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConf
  }

  val customerOrderTable = new HTable(hbaseConfiguration, "customerorder")

  def insertStringRecord(rowkey: String, column: String, value: String) {
    val thePut = new Put(Bytes.toBytes(rowkey))
    thePut.add(Bytes.toBytes(ALLINONE_FAMILY), Bytes.toBytes(column), Bytes.toBytes(value));
    customerOrderTable.put(thePut);
  }

  def insertFloatRecord(rowkey: String, column: String, value: Float) {
    val thePut = new Put(Bytes.toBytes(rowkey))
    thePut.add(Bytes.toBytes(ALLINONE_FAMILY), Bytes.toBytes(column), Bytes.toBytes(value));
    customerOrderTable.put(thePut);
  }

  def insertIntRecord(rowkey: String, column: String, value: Int) {
    val thePut = new Put(Bytes.toBytes(rowkey))
    thePut.add(Bytes.toBytes(ALLINONE_FAMILY), Bytes.toBytes(column), Bytes.toBytes(value));
    customerOrderTable.put(thePut);
  }

  def run() {
    val customerNumber = (Math.random() * 1000000).toInt
    var customerId = "customer_" + customerNumber
    val baseKey = customerId + customerNumber
    var rowKey = customerId + ":" + CUSTOMERORDERTYPE
    
    val name = RandomStringUtils.random(10, true, false)
    insertStringRecord(rowKey, "name", name)	
    
    val addr = RandomStringUtils.random(20, true, false)
    insertStringRecord(rowKey, "address", addr)	
    
    val phone = RandomStringUtils.random(8, true, false)
    insertStringRecord(rowKey, "phone", phone)
    
    val orderNumber = RandomStringUtils.random(8, true, false)
    val orderRowKey = customerId + ":" + ORDERRECORDTYPE + ":" + orderNumber
    
    val salesDate = simpleDateFormat.format(new Date(simpleDateFormat.parse("2012-01-01").getTime() + random.nextLong() % THREEYEARINMSEC))
    insertStringRecord(orderRowKey, "salesdate", salesDate)
    
    var locationNumber = 1
    var locationRowKey = orderRowKey + ":" + SHIPPINGRECORDTYPE + ":" + locationNumber
    val shipToLine1 = RandomStringUtils.random(8, true, false)
    insertStringRecord(locationRowKey, "shipToLine1", shipToLine1)
    
    var lineItemNumber = 1
    var lineItemRowKey = orderRowKey + ":" + LINEITEMRECORDTYPE + ":" + locationNumber +":" + lineItemNumber
    var itemNumber = RandomStringUtils.random(8, true, false)
    insertStringRecord(lineItemRowKey, "itemnumber", itemNumber)
    var quantity = random.nextInt
    insertIntRecord(lineItemRowKey, "quantity", quantity)
    var price = "%.2f".format(random.nextFloat()).toFloat
    insertFloatRecord(lineItemRowKey, "price", price)
    
    lineItemNumber = 2
    lineItemRowKey = orderRowKey + ":" + LINEITEMRECORDTYPE + ":" + locationNumber +":" + lineItemNumber
    itemNumber = RandomStringUtils.random(8, true, false)
    insertStringRecord(lineItemRowKey, "itemnumber", itemNumber)
    quantity = random.nextInt
    insertIntRecord(lineItemRowKey, "quantity", quantity)
    price = "%.2f".format(random.nextFloat()).toFloat
    insertFloatRecord(lineItemRowKey, "price", price)
     
    locationNumber = 2
    locationRowKey = orderRowKey + ":" + SHIPPINGRECORDTYPE + ":" + locationNumber
    val shipToLine2 = RandomStringUtils.random(8, true, false)
    insertStringRecord(locationRowKey, "shipToLine1", shipToLine2)
  
    lineItemNumber = 1
    lineItemRowKey = orderRowKey + ":" + LINEITEMRECORDTYPE + ":" + locationNumber +":" + lineItemNumber
    itemNumber = RandomStringUtils.random(8, true, false)
    insertStringRecord(lineItemRowKey, "itemnumber", itemNumber)
    quantity = random.nextInt
    insertIntRecord(lineItemRowKey, "quantity", quantity)
    price = "%.2f".format(random.nextFloat()).toFloat
    insertFloatRecord(lineItemRowKey, "price", price)

    
    val newCustomerNumber = customerNumber + 1
    customerId = "customer_" + customerNumber
    rowKey = customerId + ":" + CUSTOMERORDERTYPE
    
    // partial key range works
    var endRow = "customer_" + newCustomerNumber
    var rsIt = scanRows(baseKey, endRow)
    var result = rsIt.next()
    
    var startRow = baseKey +":" + 1
    rsIt = scanRowsOpenEnd(startRow)
    result = rsIt.next()
    
//    returns 0 rows
    endRow = baseKey +":" + 2
    rsIt = scanRows(startRow, endRow)
    result = rsIt.next()
    
    // all order rows
    rsIt = scanRowsOpenEnd(orderRowKey)
    
    // all order location rows
    val allLocationKey = orderRowKey + ":" + SHIPPINGRECORDTYPE
    rsIt = scanRowsOpenEnd(allLocationKey)
    
  }
  
  def scanRows(startRow : String, endRow : String): java.util.Iterator[Result] = {
    val theScan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow))
    val rs = customerOrderTable.getScanner(theScan)
    val rsIt = rs.iterator
    rsIt
  }
  
  def scanRowsOpenEnd(startRow : String): java.util.Iterator[Result] = {
    val theScan = new Scan(Bytes.toBytes(startRow))
    val rs = customerOrderTable.getScanner(theScan)
    val rsIt = rs.iterator
    rsIt
  }

  def md5(s: String) = {
    new String(MessageDigest.getInstance("MD5").digest(s.getBytes))
  }

}