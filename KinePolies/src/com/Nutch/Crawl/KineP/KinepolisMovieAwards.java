/**
 * 
 */
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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class KinepolisMovieAwards {

	/**
	 * 
	 */
	public KinepolisMovieAwards() {
		// TODO Auto-generated constructor stub
	}

	
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_UName=null,splitter_WName=null,splitter_Year=null;
	//String symb34="";
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	//static String comma=null;
	
	
	public void KinePolisMovieAwardCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileCA,true);
			ps = new PrintStream(fos);
			 System.setOut(ps);
			
			Configuration config=HBaseConfiguration.create();
			ht=new HTable(config,"kinepolies_webpage");
			sc=new Scan();
			//sc.setCaching(10);
			//sc.setBatch(100);
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
							
							
							Elements el=Xsoup.compile("//div[@class='award-contest-title']").evaluate(document).getElements();
							for(Element xel:el)
							{
								if(xel.text().isEmpty()||xel.text()==""||xel.text()==null||xel==null)
								{
									
								//System.out.println(Vimgurl3);
									break;
									
									
								}
								if(xel!=null)
								{
									
									
									String CrewAwardNames=Xsoup.compile("/h3/text()").evaluate(xel).get();
									if(CrewAwardNames!=null)
									{
									
										
										Elements el2=Xsoup.compile("//ul/li").evaluate(xel).getElements();
										for(Element xel2:el2)
										{
										
											
									
											
										
										
										/////////////////////// Program_SK/////////////////////////
										
										 String CrewAwardWins_SK=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
											if(CrewAwardWins_SK!=null)
											{
												SplitWinNames(CrewAwardWins_SK);
												System.out.print(splitter_WName.trim()+"#<>#");
												
											}
											
									
									
									/////////////// Program_Type////////////////
									
									System.out.print("movie".trim()+"#<>#");
								
									
									///////////////////////////  Crew Awards Names////////////////////
								
										String filterCrewAwardNames=CrewAwardNames.replace(",", "").trim();
										System.out.print(filterCrewAwardNames.trim());
										System.out.print("#<>#");
									
										
									
									////////////////// Crew Awards Category////////////////////////
										

										String CrewAwardCategory=Xsoup.compile("//span[@class='award-category']/text()").evaluate(xel2).get();
										
											
												
												if(CrewAwardCategory!=null)
												{
										
									
										System.out.print(CrewAwardCategory.trim());
												}
										System.out.print("#<>#");
									
									
									//////////////////////  Crew Award Year/////////////////////
									
									if(CrewAwardNames!=null)
									{
										SplitAwardYear(CrewAwardNames);
										System.out.print(splitter_Year.trim());
										
										System.out.print("#<>#");
									}
									
									
									
									
									
									///////////////////// Crew AwardWinNames////////////////////////
									
									String CrewAwardWinNames=Xsoup.compile("//div[@class='field field-name-title field-type-ds field-label-hidden']/div[@class='field-items']//h1/text()").evaluate(document).get();
									if(CrewAwardWinNames!=null)
									{
										System.out.print(CrewAwardWinNames.trim()+"#<>#");
									
									}
									else
									{
										
										System.out.print("#<>#");
									}
									
									///////////////////////////// Crew Winner_Sk////////////////////////////
									
									
									 String Prog_SK=Xsoup.compile("//span[@class='award-actor']/a/@href").evaluate(xel2).get();
										if(Prog_SK!=null)
										{
											SplitUrlNames(Prog_SK.trim());
											System.out.print(splitter_UName.trim()+"#<>#");
											
										}
										
										else
										{
										System.out.print("#<>#");
										}
								
									
									
										
									
									
									///////////////Winner_Type/////////////////
								
									System.out.print("actor".trim()+"#<>#");
								
									///////////////Winner_Flag////////////////
									//System.out.println("\n");
									
									 String CrewAwardWins_Flag=Xsoup.compile("//div[@class='clearfix-field field field-name-kinepolis-awards-nominations field-type-ds field-label-inline clearfix']/h2[@class='label-inline field-label ds-normal-text']/text()").evaluate(document).get();
										//System.out.println(CrewAwardWins_Flag);
									 
									 if(CrewAwardWins_Flag.contains("Nominations"))
									 {
										 System.out.print("0");
									 }
									
									 else
									 {
										 System.out.print("1");
									 }
									
								
									System.out.print("#<>#");
								
									///////////////aux_info/////////////////
								
									System.out.print("#<>#");
								
									/////////////// Created_At/////////////////
								
									System.out.print("#<>#");
								
								
									///////////////MOdified_At/////////////////
								
									System.out.print("#<>#");
								
								
									///////////////Last_Seen/////////////////
								
									System.out.print("#<>#");

									
									//String att=xel.className();
									System.out.print("\n");
										}

										
										
										
										
									
								}
									
									
									
									
									
								
																	
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
		//System.out.println(splitter);
	}
	
	public void SplitWinNames(String name)
	{
		String[] split=name.split("\\/");
		splitter_WName=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	
	//SplitAwardYear(CrewAwardName);
	
	
	public void SplitAwardYear(String name)
	{
		String[] split=name.split("\\-");
		splitter_Year=split[split.length - 2];
		//System.out.println(splitter_Year);
	}
	
							
	
}
