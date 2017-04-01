package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


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

public class KinePolisPRCNT {

	public KinePolisPRCNT() {
		// TODO Auto-generated constructor stub
	}

	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null,splitter_PRDY=null,splitter_Count=null;
	String CrewDateCon=null;
	/*
		
	final String mainhost="https://kinepolis.fr";
	
	static String MImg=null;
	
	static String Mvurl=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	MSDigest msd=new MSDigest();
	*/
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	
	static String PRurl=null;
	
	static String PRDate=null;
	String symb34="";
	static String country=null;
	
	
	static 
	{
		
		
		FileStore.ProgramReleaseTable("release");
	}
	
	

	public void KinePolisPNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.filePR,true);
			ps = new PrintStream(fos);
			System.setOut(ps);
			
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"kinepolies_webpage");
			sc=new Scan();
			//sc.setCaching(10);
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
							/*
									
						System.out.println("\n");
				System.out.println(rownames);
							System.out.println("\n");
							
							*/
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
						
							
							
							
							/*
							
				//////////// Program_Sk////////////////
							System.out.print(splitter_SK.trim()+"#<>#");
							
							
				////////////Program_Type////////////////
							System.out.print("movie"+"#<>#");
							
					////////////Company_name////////////////
							System.out.print("#<>#");
							
							
					////////////Region////////////////
							System.out.print("#<>#");
							
							
					////////////Relation///////////////
							System.out.print("#<>#");
							
							
					////////////Company_Rights////////////////
							System.out.print("#<>#");
							
							*/

							
						
							//clearfix-field field field-name-field-movie-release-date field-type-date field-label-inline clearfix
							
							String releaseDate=Xsoup.compile("//div[@class='clearfix-field field field-name-field-movie-release-date field-type-date field-label-inline clearfix']/div[@class='field-items']/div[@class='odd first last']/span[@class='date-display-single']/text()").evaluate(document).get();
							//System.out.println(releaseDate);
							if(releaseDate!=null)
								
								
							{
								//System.out.println("Inside"+releaseDate);
								
								PRurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								
								SplitPRurl(PRurl);
								//System.out.println(releaseDate);
								
								PRDate=releaseDate.replace("/", "-");
								CrewDateFormat(PRDate);
								
								SplitPRYurl(PRDate);
								
								
								String country=Xsoup.compile("//div[@class='clearfix-field field field-name-field-country field-type-taxonomy-term-reference field-label-inline clearfix']//div[@class='field-items']/*/text()").evaluate(document).get();
								if(country!=null)
								{
											
								
								List<String> clist=Xsoup.compile("//div[@class='clearfix-field field field-name-field-country field-type-taxonomy-term-reference field-label-inline clearfix']//div[@class='field-items']/*/text()").evaluate(document).list();
								
								if(clist!=null)
								{
								 for(String Cot_PR:clist)
								 {
										
										country=Cot_PR.trim();
										SplitCountry(country);
											
											ReleaseTab(CrewDateCon,splitter_Count);
																			
									 }
								 	 
								 }
								
								
								}
														
								else 
								
								{
									splitter_Count="".trim();
									
									ReleaseTab(CrewDateCon,splitter_Count);
								}


																		
								
							}
							
							
							
							
							
													/*	
						 * 
						 * 
							System.out.print("#<>#");
							
							
							
							
				////////////Studio////////////////
							System.out.print("#<>#");
							
							
							
					////////////IS_Max////////////////
							System.out.print("#<>#");
							
							
					////////////IS_Gaint_Screens////////////////
							System.out.print("#<>#");
							
							
					////////////Aux_Info////////////////
							System.out.print("#<>#");
							
							
					////////////Created-At////////////////
							System.out.print("#<>#");
							
							
							
					////////////Modified_At////////////////
							System.out.print("#<>#");
							
							
					////////////Last_Seen////////////////
							System.out.print("#<>#");
							
					////////////New Line////////////////
							System.out.print("\n");
							
							
							*/
							
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
				ps.close();
				fos.close();
			}
			catch(Exception e)
			{
				e.getMessage();
			}
			
		}
	}
	
	
	
	public void ReleaseTab(String rdate,String prcounty)
	{
		
////////////Program_Sk////////////////
		System.out.print(splitter_SK.trim()+"#<>#");
		
		
////////////Program_Type////////////////
		System.out.print("movie"+"#<>#");
		
////////////Company_name////////////////
		System.out.print("#<>#");
		
		
////////////Region////////////////
		System.out.print("#<>#");
		
		
////////////Relation///////////////
		System.out.print("#<>#");
		
		
////////////Company_Rights////////////////
		System.out.print("#<>#");
		
		
////////////Release_Date////////////////
	
		System.out.print(rdate.trim());
		System.out.print("#<>#");
		
		
		////////////Release_Year////////////////
		
		System.out.print(splitter_PRDY.trim());
		System.out.print("#<>#");
		
		
		
		
		
		/////////////// Country_Name/////////////////
		
		System.out.print(prcounty.trim());
		System.out.print("#<>#");
		
		
		
		
		////////////Studio////////////////
					System.out.print("#<>#");
					
					
					
			////////////IS_Max////////////////
					System.out.print("#<>#");
					
					
			////////////IS_Gaint_Screens////////////////
					System.out.print("#<>#");
					
					
			////////////Aux_Info////////////////
					System.out.print("#<>#");
					
					
			////////////Created-At////////////////
					System.out.print("#<>#");
					
					
					
			////////////Modified_At////////////////
					System.out.print("#<>#");
					
					
			////////////Last_Seen////////////////
					System.out.print("#<>#");
					
			////////////New Line////////////////
					System.out.print("\n");
				
		

		

		
	}
	
	public void SplitPRurl(String name)
	{
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	public void SplitPRYurl(String name)
	{
		String[] split=name.split("\\-");
		splitter_PRDY=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	
	public void SplitCountry(String name)
	{
		String[] split=name.split("\\(|\\,");
		splitter_Count=split[0];
		//Splitter_count=split[1];
		//System.out.println(splitter);
	}
	
	
	
	public void CrewDateFormat(String name)
	{
		try
		{
			
			
		
		Date formatter = (Date) new SimpleDateFormat("dd-MM-yyyy").parse(name);
		  //  Date date;
		    //date = (Date)formatter.parse(fecha.toString());
		  //  System.out.println(date);        

		    Calendar cal = Calendar.getInstance();
		    cal.setTime(formatter);
		    CrewDateCon = cal.get(Calendar.YEAR) + "-" + 
		            (cal.get(Calendar.MONTH) + 1) + 
		            "-" +         cal.get(Calendar.DATE);
		   // System.out.println("formatedDate : " + formatedDate);

	}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
}

