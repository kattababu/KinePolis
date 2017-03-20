 package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Sample {

	public Sample() {
		// TODO Auto-generated constructor stub
	}

	
	
	/**
	 * @param args
	 */
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static String namesC=null;
	static String namesC2=null;
	
	static String rownames=null,family=null,qualifier=null;
	static String content=null;
	
	static HTable ht=null;
	static Scan sc=null;
	
	
	static 
	{
		
		file=new File("/katta/KinePole/SampleCont.txt");
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			fos = new FileOutputStream(file,true);
			ps = new PrintStream(fos);
			 System.setOut(ps);
			
			
		Configuration config=HBaseConfiguration.create();
		ht=new HTable(config,"kinepolies_webpage");
		sc=new Scan();
		ResultScanner rescan=ht.getScanner(sc);
		
		for(Result res = rescan.next(); (res != null); res=rescan.next())
		{
			for(KeyValue kv:res.list())
			{
				
				rownames=Bytes.toString(kv.getRow());
				
				family=Bytes.toString(kv.getFamily());
				qualifier=Bytes.toString(kv.getQualifier());
				
				
				
				if(rownames.equals("fr.kinepolis:https/cinemas/forvm-centre-nimes/infos"))
				{
					if(family.equals("f") && qualifier.equals("cnt"))
					{
						content=Bytes.toString(kv.getValue());
											
						System.out.println(content);
						
						//System.out.println("\n\n\n\n");
					}
					
				}
				
				
				
								
				
			}
		}
		
		
		ht.close();
		rescan.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		
		
		
		

	}
	
	
	

}