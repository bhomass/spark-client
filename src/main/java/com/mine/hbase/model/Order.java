package com.mine.hbase.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class Order {
	private String salesDate;
	private String orderNumber;
	
	private static final int THREEYEARINMSEC = 3 * 365 * 24 * 60 * 60 * 1000;
	private static final Random random = new Random();
	private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public Order(){
		
	}

	public static Order generateOrder(){
		Order order = new Order();
		String salesDate = "2014-09-18";
		try {
			salesDate = simpleDateFormat.format(new Date(simpleDateFormat.parse("2012-01-01").getTime() + random.nextLong() % THREEYEARINMSEC));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		order.salesDate = salesDate;
		
		String orderNumber = RandomStringUtils.random(8, true, false);
		order.orderNumber = orderNumber;
		
		return order;
	}
	
	public String getOrderNumber() {
		return orderNumber;
	}

	public String getSalesDate() {
		return salesDate;
	}

}
