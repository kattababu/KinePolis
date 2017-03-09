package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import us.codecraft.xsoup.Xsoup;

public class KinePolisRateCNT {

	public KinePolisRateCNT() {
		// TODO Auto-generated constructor stub
	}

	
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	static String Raturl=null;
	static String IMDBRat=null;
	static String UserRat=null;
	
	
	public void KinePolisRatNT(String names)
	{
		try
		{
			
			//fos = new FileOutputStream(file,true);
			//ps = new PrintStream(fos);
			//System.setOut(ps);
			
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"kinepolies_webpage");
			sc=new Scan();
			rescan=ht.getScanner(sc);
			
			for(Result res = rescan.next(); (res != null); res=rescan.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					
					if(rownames.equals(names))
					{
						if(family.equals("f")&& qualifier.equals("cnt"))
						{
									
							System.out.println("\n");
							System.out.println(rownames);
							System.out.println("\n");
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
						
							
							Raturl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							
							SplitRaturl(Raturl);
							
							
							
							String ImDB=Xsoup.compile("//div[@class='clearfix-field field field-name-imdb-score field-type-ds field-label-inline clearfix']/div[@class='field-items']//span[@class='kinepolis-imdb-movie-score']/text()").evaluate(document).get();
							if(ImDB!=null)
							{
								IMDBRat=ImDB.trim();
								String rating_type="imdb";
								RatingTab(IMDBRat,rating_type);
								
							}
							
							
							String Usr=Xsoup.compile("//div[@class='clearfix-field field field-name-visitor-score field-type-ds field-label-inline clearfix']/div[@class='field-items']//span[@class='movie-visitor-score-score']/text()").evaluate(document).get();
							if(Usr!=null)
							{
								UserRat=Usr.trim();
								
								//System.out.println(UserRat);
								String rating_type="user";
							RatingTab(UserRat,rating_type);
								
							}
							
							
							
							
						}
					}
				}
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		finally
		{
			try
			{
				ht.close();
				rescan.close();
			}
			catch(Exception e)
			{
				e.getMessage();
			}
			
		}
	}
	
	
	
	
	public void RatingTab(String rate,String rate_type)
	{
		/////////////Program_SK//////////////////
		System.out.print(splitter_SK.trim()+"#<>#");
		
		
/////////////Program_Type/////////////////
		System.out.print("movie"+"#<>#");
		
		
		
/////////////Rating//////////////////
		System.out.print(rate.trim()+"#<>#");
		
		
		
		
/////////////Rating_Type//////////////////
		System.out.print(rate_type.trim()+"#<>#");
		
		
		
		
/////////////Rating_Reason//////////////////
		System.out.print("#<>#");
		
		
		
/////////////Created_At//////////////////
		System.out.print("#<>#");
		
		
		
		
/////////////Modified_At//////////////////
		System.out.print("#<>#");
		
		
/////////////Last_seen//////////////////
		System.out.print("#<>#");
		
		
/////////////New Line//////////////////
		System.out.print("\n");
		
		
		
		
		
		
		
		
		
		
	
	}
	
	
	
	
	
	public void SplitRaturl(String name)
	{
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	
}
