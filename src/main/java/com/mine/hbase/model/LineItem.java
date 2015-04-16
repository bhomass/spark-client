package com.mine.hbase.model;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class LineItem {
	private static final Random random = new Random();
	
	private String itemNumber;
	private int quantity;
	private float price;
	
	public static LineItem generateLineItem(){
		LineItem item = new LineItem();
		item.itemNumber = RandomStringUtils.random(8, true, false);
		item.quantity = random.nextInt();
		item.price = Float.parseFloat(String.format("%.2f", random.nextFloat()));
		
		return item;
	}

	public LineItem(){
		
	}

	public String getItemNumber() {
		return itemNumber;
	}

	public int getQuantity() {
		return quantity;
	}

	public float getPrice() {
		return price;
	}

}
