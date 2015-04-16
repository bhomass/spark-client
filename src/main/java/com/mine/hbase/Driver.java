package com.mine.hbase;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
public class Driver {
//	private static final String HOUR = "2014-09-18";
	private static final String INPUTFILEPATH = "/user/bruce/customerorder.csv";
	private static final String OUTPUTFILEPATH = "/user/bruce/hfiles/";
	private static final String TABLENAME = "customerorder";

  public static void main(String[] args) throws Exception {	
	InputStream is = Driver.class.getClassLoader().getResourceAsStream(
			"config.properties");

	Properties props = new Properties();
	String hdfsHost = null;
	try {
		props.load(is);
		hdfsHost = props.getProperty("hdfs.host");
	} catch (IOException e) {
		e.printStackTrace();
	}
	
    Job job = Job.getInstance();
    Configuration conf = job.getConfiguration();
    conf.set("hbase.table.name", TABLENAME);
    job.setJobName("Customer Order Bulkload");
//    conf.set("fs.defaultFS", "hdfs://" + hdfsHost + ":8020"); // need this for ambari cluster

    conf.set("fs.hdfs.impl", 
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
    conf.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
//    conf.set("zookeeper.znode.parent", "/hbase-unsecure");
    // Load hbase-site.xml 
    HBaseConfiguration.addHbaseResources(conf);

    job.setJarByClass(HBaseKVMapper.class);

    job.setMapperClass(HBaseKVMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(KeyValue.class);

    job.setInputFormatClass(CombineTextInputFormat.class);

    HTable hTable = new HTable(conf, TABLENAME);
    
    // Auto configure partitioner and reducer
    HFileOutputFormat2.configureIncrementalLoad(job, hTable);

    FileInputFormat.addInputPath(job, new Path(INPUTFILEPATH));
    Path outputPath = new Path(OUTPUTFILEPATH);
    FileOutputFormat.setOutputPath(job, outputPath);
    
    boolean success = job.waitForCompletion(true);
    
    if (success) {
    	LoadIncrementalHFiles lihf = null;
    	try {
    		lihf = new LoadIncrementalHFiles (conf);
    		lihf.doBulkLoad(outputPath, hTable);

    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
  }
}
