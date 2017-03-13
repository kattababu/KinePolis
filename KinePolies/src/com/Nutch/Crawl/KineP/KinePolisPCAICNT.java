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
public class KinePolisPCAICNT {

	/**
	 * 
	 */
	public KinePolisPCAICNT() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_PSK=null,splitter_UName=null;
	static String CrewAurl=null;
	int i=1;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	
	
	
	//final String mainhost="https://kinepolis.fr";
	public void KinePolisCrewPrgACTNT(String names)
	{
		try
		{
			
		fos = new FileOutputStream(FileStore.filePC,true);
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
									
						//	System.out.println("\n");
							//System.out.println(rownames);
							//System.out.println("\n");
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
							
							
							
							List<String> CrewActors=Xsoup.compile("//div[@class='clearfix-field field field-name-movie-cast-list field-type-ds field-label-inline clearfix']//div[@class='field field-name-title field-type-ds field-label-hidden']//div[@class='field-items']//a/@href").evaluate(document).list();
							if(CrewActors!=null)
							{
								for(String CrewActor:CrewActors)
								{
								
								CrewAurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								SplitPSK(CrewAurl);
							//System.out.println(mainhost+CrewDirector);
							 String CrewAQL=CrewActor.trim();
							 SplitUrlNames(CrewAQL);
							//KinePolisCrewQLsNT(CrewDQL);
							 PActCrewTabs();
							 i++;
								}
							 
							 
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
				ps.close();
				fos.close();
			}
			catch(Exception e)
			{
				e.getMessage();
			}
			
		}
	}
	
	
	public void SplitUrlNames(String name)
	{
		String[] split=name.split("\\/");
		splitter_UName=split[split.length - 1];
		//System.out.println(splitter_UName);
	}
	
	
	public void SplitPSK(String name)
	{
		String[] split=name.split("\\/");
		splitter_PSK=split[split.length - 1];
		//System.out.println(splitter_UName);
	}
	
	
	
	
	
	public void PActCrewTabs()
	{
		////////// Program_SK///////////////////////
		System.out.print(splitter_PSK.trim()+"#<>#");
		
		
//////////Program_Type///////////////////////
		System.out.print("movie"+"#<>#");
		
		
//////////Crew_SK///////////////////////
		System.out.print(splitter_UName.trim()+"#<>#");
		
		
		
//////////Role///////////////////////
		System.out.print("actor"+"#<>#");
		
		
		
		
//////////Description///////////////////////
		System.out.print("#<>#");
		
		
		
//////////Role_Title///////////////////////
		System.out.print("#<>#");
		
		
//////////Rank///////////////////////
		System.out.print(i+"#<>#");
		
//////////aux_info///////////////////////
		System.out.print("#<>#");
		
		
//////////Created_At///////////////////////
		System.out.print("#<>#");
		
//////////Modified_At///////////////////////
		System.out.print("#<>#");
		
		
//////////Last_Seen///////////////////////
		System.out.print("#<>#");
		
		
		
//////////New Line//////////////////////
		System.out.print("\n");
	}

}



