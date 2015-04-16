package com.mine.hbase.model;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;


public class DataRecord {
	public static final int CUSTOMERORDERTYPE = 1;
	public static final int ORDERRECORDTYPE = 2;
	public static final int SHIPPINGRECORDTYPE = 3;
	public static final int LINEITEMRECORDTYPE = 4;
			  
	private String recordType = "";
	private String customerId = "";
	private String name = "";
	private String address = "";
	private String phone = "";
	private String ordernumber = "";
	private String salesdate = "";
	private String locationnumber = "";
	private String shiptoLine1 = "";
	private String shiptoLine2 = "";
	private String shiptoCity = "";
	private String shiptoState = "";
	private String shiptoZip = "";
	private String lineItemNumber = "";
	private String itemnumber = "";
	private String quantity = "";
	private String price = "";
	
	public DataRecord(int recordType){
		this.recordType = recordType + "";
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(recordType);
		sb.append(",");
		sb.append(customerId);
		sb.append(",");
		sb.append(name);
		sb.append(",");
		sb.append(address);
		sb.append(",");
		sb.append(phone);
		sb.append(",");
		sb.append(ordernumber);
		sb.append(",");
		sb.append(salesdate);
		sb.append(",");
		sb.append(locationnumber);
		sb.append(",");
		sb.append(shiptoLine1);
		sb.append(",");
		sb.append(shiptoLine2);
		sb.append(",");
		sb.append(shiptoCity);
		sb.append(",");
		sb.append(shiptoState);
		sb.append(",");
		sb.append(shiptoZip);
		sb.append(",");
		sb.append(lineItemNumber);
		sb.append(",");
		sb.append(itemnumber);
		sb.append(",");
		sb.append(quantity);
		sb.append(",");
		sb.append(price);

		return sb.toString();
	}

	public static void main(String[] args) {
		List<DataRecord> records = new ArrayList<DataRecord>();
		int sets = 3;
		for (int i = 0; i < sets; i++){
			genOneDataSet(records);
		}
		writeCsv(records);

	}
	
	private static void genOneDataSet(List<DataRecord> records){
//		List<DataRecord> records = new ArrayList<DataRecord>();
		Customer customer = Customer.generateCustomer();
		DataRecord customerRecord = DataRecord.genCustomerRecord(customer);
		records.add(customerRecord);
		
		Order order = Order.generateOrder();
		records.add(DataRecord.genOrderRecord(customer, order));
		
		records.add(DataRecord.genLocationRecord(customer, order, Location.generateLocation(), 1));
		
		records.add(DataRecord.genLineItemRecord(customer, order, 1, LineItem.generateLineItem(), 1));
		records.add(DataRecord.genLineItemRecord(customer, order, 1, LineItem.generateLineItem(), 2));
		records.add(DataRecord.genLineItemRecord(customer, order, 1, LineItem.generateLineItem(), 3));
		
		records.add(DataRecord.genLocationRecord(customer, order, Location.generateLocation(), 2));
		records.add(DataRecord.genLineItemRecord(customer, order, 2, LineItem.generateLineItem(), 1));
		records.add(DataRecord.genLineItemRecord(customer, order, 2, LineItem.generateLineItem(), 2));

	}
	
	private static void writeCsv(List<DataRecord> records){
		FileOutputStream fout;		

		try
		{
		    // Open an output stream
		    fout = new FileOutputStream ("/home/bruce/customerorder.csv");
		    PrintStream printStream = new PrintStream(fout);
		    for (DataRecord record : records){
		    	printStream.println (record.toString());
		    }
		    // Close our output stream
		    fout.close();		
		}
		// Catches any error conditions
		catch (IOException e)
		{
			System.err.println ("Unable to write to file");
			System.exit(-1);
		}
	}
	
	public static DataRecord genCustomerRecord(Customer customer){
		DataRecord record = new DataRecord(CUSTOMERORDERTYPE);
//		String rowKey = customer.getCustomerid() + ":" + CUSTOMERORDERTYPE;
		record.customerId = customer.getCustomerid();
		record.name = customer.getName();
		record.address = customer.getAddress();
		record.phone = customer.getPhone();
		
		return record;
	}
	
	public static DataRecord genOrderRecord(Customer customer, Order order){
		DataRecord record = new DataRecord(ORDERRECORDTYPE);
		record.customerId = customer.getCustomerid();
		record.salesdate = order.getSalesDate();
		record.ordernumber = order.getOrderNumber();
		
		return record;
	}
	
	public static DataRecord genLocationRecord(Customer customer, Order order, Location location, int locationNumber){
		DataRecord record = new DataRecord(SHIPPINGRECORDTYPE);
		record.customerId = customer.getCustomerid();
		record.ordernumber = order.getOrderNumber();
		record.locationnumber = locationNumber + "";
		record.shiptoLine1 = location.getShiptoLine1();
		record.shiptoCity = location.getShiptoCity();
		record.shiptoState = location.getShiptoState();
		record.shiptoZip = location.getShiptoZip();
		
		return record;
	}

	public static DataRecord genLineItemRecord(Customer customer, Order order, int locationNumber, LineItem lineItem, int lineItemNumber){
		DataRecord record = new DataRecord(LINEITEMRECORDTYPE);
		record.customerId = customer.getCustomerid();
		record.ordernumber = order.getOrderNumber();
		record.locationnumber = locationNumber + "'";
		record.lineItemNumber = lineItemNumber + "";
		record.itemnumber = lineItem.getItemNumber();
		record.quantity = lineItem.getQuantity() + "";
		record.price = lineItem.getPrice() + "";
		
		return record;
	}

}
