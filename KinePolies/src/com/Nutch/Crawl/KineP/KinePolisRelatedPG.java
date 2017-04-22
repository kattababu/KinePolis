/**
 * 
 */
package com.Nutch.Crawl.KineP;

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

/**
 * @author surendra
 *
 */
public class KinePolisRelatedPG {

	/**
	 * 
	 */
	public KinePolisRelatedPG() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null,splitter_Count=null,Desclang=null,splitter_MSK=null;
	String symb34="";
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static String comma=null;
	int i=1;
	
	

	
	static 
	{
		
				
				FileStore.RelatedPrgTable("relatedprograms");
		 

	}

	

	public void KinePolisRPRGCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRPG,true);
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
					
					if(rownames.equals(names))
					{
						if(family.equals("f")&& qualifier.equals("cnt"))
						{
									
							//System.out.println("\n");
							//System.out.println(rownames);
							//System.out.println("\n");
							
							
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
						 List<String> releated_SKs=Xsoup.compile("//div[@id='block-kinepolis-movie-filter-kinepolis-movie-filter-related']//div[@class='movie-container-info']/p[@class='movie-overview-title']/a/@href").evaluate(document).list();
							for(String releated_SK:releated_SKs)
							{
								if(releated_SK!=null)
								{
									//System.out.println(releated_SK);
									
									//////////////// Program_SK////////////////
									
									String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
									SplitUrl(url);
									//System.out.print(splitter_SK.trim()+"#<>#");
									
									
									
									
									
									
									SplitUrlSK(releated_SK);
									
									RelatedTab(splitter_SK,splitter_MSK,i);
									i=i+1;
									//System.out.println(splitter_SK);
									
								}
								
								
								
								
								
							}
							//System.out.println(url.toString());
							
							
							
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
	
	
	
	public void RelatedTab(String Mname,String Lname,int inc)
	{
		///////////////////////  Program_Sk////////////
				
		System.out.print(Mname.trim()+"#<>#");		
		
		//////////////// Program_Type////////////////
		
		System.out.print("movie".trim()+"#<>#");
		
		
		//////////////////// Releated_Sk/////////////
		
		
		System.out.print(Lname.trim()+"#<>#");
		
		
		/////////////// Releated_Rank//////////////
		
		System.out.print(inc+"#<>#");
				
		
		//////////////////// Created_At///////////////
		
		
		System.out.print("#<>#");
		
		
		////////////////// Modified_At///////////////////
		
		System.out.print("#<>#");
		
		///////////////////// Last_Seen//////////////
		
		
		System.out.print("#<>#");
		
		//////////////////New Line/////////////
		
		
		System.out.print("\n");
		
		
		
		
		
		
		
	}
	
	
	public void SplitUrl(String name)
	{
		//System.out.println(name);
		if(name!=null)
		{
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		}
		//System.out.println(splitter);
	}
	
	
	public void SplitUrlSK(String name)
	{
		//System.out.println(name);
		if(name!=null)
		{
		String[] split=name.split("\\/");
		splitter_MSK=split[split.length - 1];
		}
		//System.out.println(splitter);
	}
	


}
