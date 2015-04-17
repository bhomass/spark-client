package com.mine.hbase;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.mine.hbase.model.DataRecord;

/**
 * HBase bulk import example
 * <p>
 * Parses Facebook and Twitter messages from CSV files and outputs
 * <ImmutableBytesWritable, KeyValue>.
 * <p>
 * The ImmutableBytesWritable key is used by the TotalOrderPartitioner to map it
 * into the correct HBase table region.
 * <p>
 * The KeyValue value holds the HBase mutation information (column family,
 * column, and value)
 */
public class HBaseKVMapper extends
    Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

  final static byte[] CATS_COL_FAM = "allinone".getBytes();
  final static int NUM_FIELDS = 6;

  CustomerOrderKeyValueParser csvParser = new CustomerOrderKeyValueParser();
  int tipOffSeconds = 0;
  String tableName = "";

  ImmutableBytesWritable hKey = new ImmutableBytesWritable();
  KeyValue kv;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    Configuration c = context.getConfiguration();

    tipOffSeconds = c.getInt("epoch.seconds.tipoff", 0);
    tableName = c.get("hbase.table.name");
  }

  /** {@inheritDoc} */
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] fields = null;
    System.out.println("line = " + value);

    try {
      fields = csvParser.parseLine(value.toString());
    } catch (Exception ex) {
      context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
      return;
    }

    String customerId = fields[1];
	String orderNumber = fields[5];
	String locationNumber = fields[7];
    // Get game offset in seconds from tip-off
    String compositeKey = null;

    try {
    	Integer type = Integer.parseInt(fields[0]);
    	switch (type){
    		case DataRecord.CUSTOMERORDERTYPE:
    			compositeKey = customerId + ":" + DataRecord.CUSTOMERORDERTYPE;
    		    hKey.set(compositeKey.getBytes());
    			String name = fields[2];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
    			          HColumnEnum.SRV_COL_NAME.getColumnName(), name.getBytes());
    			context.write(hKey, kv);
    			String address = fields[3];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
  			          HColumnEnum.SRV_COL_ADDRESS.getColumnName(), address.getBytes());
	  			context.write(hKey, kv);
	  			String phone = fields[4];
				kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_PHONE.getColumnName(), phone.getBytes());
				context.write(hKey, kv);
    			break;
    		case DataRecord.ORDERRECORDTYPE:
     			compositeKey = customerId + ":" + DataRecord.ORDERRECORDTYPE + ":" + orderNumber;
    		    hKey.set(compositeKey.getBytes());
     			// salesdate
     			String salesdate = fields[6];
				kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SALESDATE.getColumnName(), salesdate.getBytes());
				context.write(hKey, kv);
    			break;
    		case DataRecord.SHIPPINGRECORDTYPE:
    			compositeKey = customerId + ":" + DataRecord.ORDERRECORDTYPE + ":" + orderNumber +":" + DataRecord.SHIPPINGRECORDTYPE +":" + locationNumber;
    		    hKey.set(compositeKey.getBytes());
    		    String shiptoline1 = fields[8];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SHIPTOLINE1.getColumnName(), shiptoline1.getBytes());
				context.write(hKey, kv);
    			String shiptoline2 = fields[9]; 
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SHIPTOLINE2.getColumnName(), shiptoline2.getBytes());
				context.write(hKey, kv);
    			String shiptocity = fields[10];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SHIPTOCITY.getColumnName(), shiptocity.getBytes());
				context.write(hKey, kv);
    			String shiptostate = fields[11];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SHIPTOSTATE.getColumnName(), shiptostate.getBytes());
				context.write(hKey, kv);
    			String shiptozip = fields[12];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_SHIPTOZIP.getColumnName(), shiptozip.getBytes());
				context.write(hKey, kv);
    			break;
    		case DataRecord.LINEITEMRECORDTYPE:
    			String lineItemNumber = fields[13];
    			compositeKey = customerId + ":" + DataRecord.ORDERRECORDTYPE + ":" + orderNumber +":" + DataRecord.LINEITEMRECORDTYPE +":" + locationNumber + ":" + lineItemNumber;
    		    hKey.set(compositeKey.getBytes());
    			String itemnumber = fields[14];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_ITEMNUMBER.getColumnName(), itemnumber.getBytes());
				context.write(hKey, kv);
    			String quantity = fields[15];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_QUANTITY.getColumnName(), quantity.getBytes());
				context.write(hKey, kv);
    			String price = fields[16];
    			kv = new KeyValue(hKey.get(), CATS_COL_FAM,
				          HColumnEnum.SRV_COL_PRICE.getColumnName(), price.getBytes());
				context.write(hKey, kv);
    			break;
    	}
    			
    	System.out.println("key = " + compositeKey);
    } catch (Exception ex) {
      context.getCounter("HBaseKVMapper", "INVALID_DATE").increment(1);
      return;
    }

    // Key: e.g. "1200:twitter:jrkinley"
    hKey.set(compositeKey
        .getBytes());

    if (!fields[1].equals("")) {
      kv = new KeyValue(hKey.get(), CATS_COL_FAM,
          HColumnEnum.SRV_COL_CUSTOMERID.getColumnName(), fields[1].getBytes());
      context.write(hKey, kv);
    }

  }
}
