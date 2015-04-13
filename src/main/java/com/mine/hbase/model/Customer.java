package com.mine.hbase.model;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class Customer {
	private int recordtype = 1;
	private String customerid;
	private String name;
	private String address;
	private String phone;
	

	public static Customer generateCustomer() {
		Customer customer = new Customer();
		Random random = new Random();
		int customerNumber = (int)(Math.random() * 1000000);
		customer.customerid = "customer_" + customerNumber;
		String name = RandomStringUtils.random(10, true, false);
		customer.name = name;
		String address = RandomStringUtils.random(20, true, false);
		customer.address = address;
		String phone = RandomStringUtils.random(8, true, false);
		customer.phone = phone;
		
		return customer;
	}


	public int getRecordtype() {
		return recordtype;
	}


	public String getCustomerid() {
		return customerid;
	}


	public String getName() {
		return name;
	}


	public String getAddress() {
		return address;
	}


	public String getPhone() {
		return phone;
	}
	
}
