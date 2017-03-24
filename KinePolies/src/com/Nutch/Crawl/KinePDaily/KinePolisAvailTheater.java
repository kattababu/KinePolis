package com.Nutch.Crawl.KinePDaily;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
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

public class KinePolisAvailTheater {

	public KinePolisAvailTheater() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static 
	{
		
		file=new File("/NutchCrawl/Nutch/runtime/local/Kinepshowurls/urls");
	}
	
	
	public void KinePolisATTCNT()
	{
		try
		{
			
			fos = new FileOutputStream(file,true);
			ps = new PrintStream(fos);
			 System.setOut(ps);
			
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
					
					
					if(rownames.contains("/cinemas/") && !rownames.endsWith("/infos")&&!rownames.endsWith("/aujourdhui"))
					{
						if(family.equals("f") && qualifier.equals("cnt"))
						{
					
						//System.out.println(rownames);
						SplitUrl(rownames);
						content=Bytes.toString(kv.getValue());
						Document document = Jsoup.parse(content);
						
						 if (rownames.contains(splitter_SK))
						 {
							 
							 if(splitter_SK.equals("forvm-centre-nimes"))
							 {
							 String  FORVM=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/FORVM/";
							 
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(FORVM+fdate);
									}
									
									
								}
							 //System.out.println(FORVM);
							 }
							 
							 
							 if(splitter_SK.equals("kinepolis-bourgoin-jallieu"))
							 {
							 String  KBOUR=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KBOUR/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KBOUR+fdate);
									}
									
									
								}
							
							// System.out.println(KBOUR);
							 }
							 
							 if(splitter_SK.equals("kinepolis-fenouillet"))
							 {
							 String  KFEN=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KFEN/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KFEN+fdate);
									}
									
									
								}
							
							 //System.out.println(KFEN);
							 }
							 
							 if(splitter_SK.equals("kinepolis-lomme"))
							 {
							 String  KLOM=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KLOM/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KLOM+fdate);
									}
									
									
								}
							
							 
							 //System.out.println(KLOM);
							 }
							 
							 if(splitter_SK.equals("kinepolis-longwy"))
							 {
							 String  ULONG=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/ULONG/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(ULONG+fdate);
									}
									
									
								}
							
							 //System.out.println(ULONG);
							 }
							 
							 
							 if(splitter_SK.equals("kinepolis-mulhouse"))
							 {
							 String  KMUL=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KMUL/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KMUL+fdate);
									}
									
									
								}
							
							 //System.out.println(KMUL);
							 }
							 
							 if(splitter_SK.equals("kinepolis-nancy"))
							 {
							 String  KNCY=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KNCY/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KNCY+fdate);
									}
									
									
								}
							
							// System.out.println(KNCY);
							 }
							 
							 if(splitter_SK.equals("kinepolis-nimes"))
							 {
							 String  KNIM=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KNIM/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KNIM+fdate);
									}
									
									
								}
							
							 
							 //System.out.println(KNIM);
							 }
							 
							 if(splitter_SK.equals("kinepolis-rouen"))
							 {
							 String  KROU=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KROU/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KROU+fdate);
									}
									
									
								}
							
							 //System.out.println(KROU);
							 }
							 				 
							 
							 if(splitter_SK.equals("kinepolis-st-julien-les-metz"))
							 {
							 String  KMETZ=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KMETZ/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KMETZ+fdate);
									}
									
									
								}
							
							 
							 //System.out.println(KMETZ);
							 }
							 if(splitter_SK.equals("kinepolis-thionville"))
							 {
							 String  KTHIO=	"https://kinepolis.fr/kinepolis_movie_filter/load-content/presales/KTHIO/";
							 List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
								for(String fdate:FilterDates)
									
								{
									
									
									if(!fdate.equals("all") && fdate!=null)
									{
										
										
									System.out.println(KTHIO+fdate);
									}
									
									
								}
							
							 
							// System.out.println(KTHIO);
							 }
						 }
						
						
						
					
						/*
						content=Bytes.toString(kv.getValue());
						Document document = Jsoup.parse(content);
						
						List<String> FilterDates=Xsoup.compile("//select[@id='filter-select-date']/option/@value").evaluate(document).list();
						for(String fdate:FilterDates)
							
						{
							
							
							if(!fdate.equals("all") && fdate!=null)
							{
								
								
							System.out.println(fdate);
							}
							
							
						}
						
						*/	
							//KinePolisATTDATECNT(rownames);
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
					
		//////////////////////////////////////////////
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void SplitUrl(String name)
	{
		//System.out.println(name);
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	
		

}
