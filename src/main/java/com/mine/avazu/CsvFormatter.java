package com.mine.avazu;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class CsvFormatter {

	public static void main(String[] args) {
		FileOutputStream fout;	
		try{
			fout = new FileOutputStream ("test.csv");
			PrintStream pr = new PrintStream(fout);
			String header  = "id,click,hour,C1,banner_pos,site_id,site_domain,site_category,app_id,app_domain,app_category,device_id,device_ip,device_model,device_type,device_conn_type,C14,C15,C16,C17,C18,C19,C20,C21";
			pr.println(header);
			
			BufferedReader br = new BufferedReader(new FileReader("/home/bruce/Documents/avazu/test.txt"));
		    for(String line; (line = br.readLine()) != null; ) {
		        String outLine = line.replace("\t", ",");
		        pr.println(outLine);
		    }
		    // line is not visible here.
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
