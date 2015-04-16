package com.mine.hbase.model;

import org.apache.commons.lang3.RandomStringUtils;

public class Location {
	private String shiptoLine1;
	private String shiptoLine2;
	private String shiptoCity;
	private String shiptoState;
	private String shiptoZip;
	
	public static Location generateLocation(){
		Location location = new Location();
		location.shiptoLine1 = RandomStringUtils.random(8, true, false);
		location.shiptoLine2 = RandomStringUtils.random(8, true, false);
		location.shiptoCity = RandomStringUtils.random(8, true, false);
		location.shiptoState = RandomStringUtils.random(6, true, false);
		location.shiptoZip = RandomStringUtils.random(5, true, false);
		
		return location;
	}

	public Location(){
		
	}

	public String getShiptoLine1() {
		return shiptoLine1;
	}

	public String getShiptoLine2() {
		return shiptoLine2;
	}

	public String getShiptoCity() {
		return shiptoCity;
	}

	public String getShiptoState() {
		return shiptoState;
	}

	public String getShiptoZip() {
		return shiptoZip;
	}

}
