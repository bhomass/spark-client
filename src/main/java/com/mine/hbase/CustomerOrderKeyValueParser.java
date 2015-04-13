package com.mine.hbase;



public class CustomerOrderKeyValueParser {
	private static final int MAX = 15;
	
	public String[] parseLine(String line){
		String[] fields = new String[MAX + 1];
		
		int beg = -1;
		for (int i = 0; i < MAX; i++){
			int comma = line.indexOf(",", beg + 1);
			fields[i] = line.substring(beg + 1, comma);
			beg = comma;
		}
		
		fields[MAX] = line.substring(beg);
		
		return fields;
	}
	

}
