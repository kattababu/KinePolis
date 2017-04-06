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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class KinePolisCrewAwardsCNT {

	/**
	 * 
	 */
	public KinePolisCrewAwardsCNT() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_UName=null,Movie_url=null,splitter_WName=null;
	
	static String splitter_Year=null;
	
	
	final String mainhost="https://kinepolis.fr";
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	
	
	
	static 
	{
		FileStore.AwardsTable("award");
		
		//file=new File("/katta/KinePole/CrewDTCNT.txt");
	}
	
	
	
	
	
	
	
	public void KinePolisCrewActAwdCNT(String names)
	{
		try
		{
			
		//	fos = new FileOutputStream(file,true);
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
							
							//////////////////// Program_SK///////////////////
							
							
							Movie_url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							
							
							List<String> CrewDirectors=Xsoup.compile("//div[@class='clearfix-field field field-name-field-movie-person-director field-type-node-reference field-label-inline clearfix']/div[@class='field-items']//a/@href").evaluate(document).list();
							if(CrewDirectors!=null)
							{
								for(String CrewDirector:CrewDirectors)
								{
									
									
								String CrewDirect=mainhost+CrewDirector.trim();
								//System.out.println("Crew Director:\t"+CrewDirect);
								KinePolisCrewAwdDctQLNT(CrewDirect);
								}
								 
							}
							
							
							List<String> CrewActors=Xsoup.compile("//div[@class='clearfix-field field field-name-movie-cast-list field-type-ds field-label-inline clearfix']//div[@class='field field-name-title field-type-ds field-label-hidden']//div[@class='field-items']//a/@href|//div[@id='movie-cast-list-downer']//a/@href").evaluate(document).list();
							if(CrewActors!=null)
							{
								for(String  CrewActor:CrewActors)
								{
							 String CrewAwdQL=mainhost+CrewActor.trim();
							 
							//System.out.println("Crew Actors:\t"+CrewAwdQL);
							KinePolisCrewAwdActQLNT(CrewAwdQL);
							 
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
			}
			catch(Exception e)
			{
				e.getMessage();
			}
			
		}
	}
	
	
	//////////////////////////// Crew Qualifier List Director////////////////////////
	
	
	public void KinePolisCrewAwdDctQLNT(String names)
	{
		try
		{
			
		//	fos = new FileOutputStream(file,true);
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
					
					
					if(family.equals("ol"))
					{
					if(qualifier.equals(names))
					{
						SplitUrlNames(names);
						 if(rownames.contains(splitter_UName) && rownames.endsWith(splitter_UName))
						 {
							 System.out.println("\n");
							System.out.println(rownames);
							System.out.println("\n");
							
							
							String Win_type="director";
							
							KinePolisCrewAwdCNT(rownames,Win_type);
							 
							//KinePolisCrewRPCACNT(rownames);
							 //KinePolisCrewRCNT(rownames);
							
							//CrewAwardName=Xsoup.compile("//div[@class='field field-name-title field-type-ds field-label-hidden']/div[@class='field-items']//h1/text()").evaluate(document).get();
							
							
							
							
							
							
							
							
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
	
	
	
	
	
	
	
	
	
	
//////////////////////////// Crew Qualifier List Actors////////////////////////
	
	
	public void KinePolisCrewAwdActQLNT(String names)
	{
		try
		{
			
		//	fos = new FileOutputStream(file,true);
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
					
					
					if(family.equals("ol"))
					{
					if(qualifier.equals(names))
					{
						SplitUrlNames(names);
						 if(rownames.contains(splitter_UName) && rownames.endsWith(splitter_UName))
						 {
							 System.out.println("\n");
							System.out.println(rownames);
							System.out.println("\n");
							
							String Win_type="actor";
							
							KinePolisCrewAwdCNT(rownames,Win_type);
							 
							//KinePolisCrewRPCACNT(rownames);
							 //KinePolisCrewRCNT(rownames);
							
							//CrewAwardName=Xsoup.compile("//div[@class='field field-name-title field-type-ds field-label-hidden']/div[@class='field-items']//h1/text()").evaluate(document).get();
							
							
							
							
							
							
							
							
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
	
	
	
	
	
	
	
	/////////////////////// Crew Award Rows CNT Data//////////////////////////////
	
	
	
	public void KinePolisCrewAwdCNT(String names,String Win_type)
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
						//System.out.println("Content Rows"+rownames);
							//System.out.println("\n");
							
							
							
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
							
/*
							CrewAwardNames=Xsoup.compile("//div[@class='award-contest-title']/h3/text()").evaluate(document).list();
							if(CrewAwardNames!=null)
							{
								for(String CrewAwardName:CrewAwardNames)
								{
									//SplitAwardYear(CrewAwardName);
									
									
									System.out.print("\n");
										
										
									
									
							
						
							
								}
							}
					
					*/
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
							/////////////// Program_SK/////////////////
							//SplitUrlNames(Movie_url.trim());
							
							//System.out.print(splitter_UName.trim()+"#<>#");
							
							
							
							 String Prog_SK=Xsoup.compile("//span[@class='award-actor']/a/@href").evaluate(xel).get();
								if(Prog_SK!=null)
								{
									SplitUrlNames(Prog_SK.trim());
									System.out.print(splitter_UName.trim());
									
								}
								
								System.out.print("#<>#");
						
							/////////////// Program_Type////////////////
							
							System.out.print("movie".trim()+"#<>#");
						
							
							///////////////////////////  Crew Awards Names////////////////////
						
							String CrewAwardNames=Xsoup.compile("/h3/text()").evaluate(xel).get();
							if(CrewAwardNames!=null)
							{
								String filterCrewAwardNames=CrewAwardNames.replace(",", "").trim();
								System.out.print(filterCrewAwardNames.trim()+"#<>#");
							}
							else
							{
								System.out.print("#<>#");
							}
								
							
							////////////////// Crew Awards Category////////////////////////
							
							String CrewAwardCategorys=Xsoup.compile("//span[@class='award-category']/text()").evaluate(xel).get();
							if(CrewAwardCategorys!=null)
							{
								System.out.print(CrewAwardCategorys.trim()+"#<>#");
							}
							else
							{
								
								System.out.print("#<>#");
							}
							
							
							//////////////////////  Crew Award Year/////////////////////
							
							if(CrewAwardNames!=null)
							{
								SplitAwardYear(CrewAwardNames);
								System.out.print(splitter_Year.trim()+"#<>#");
							}
							
							else
							{
								
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
							
							
							 String CrewAwardWins_SK=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								if(CrewAwardWins_SK!=null)
								{
									SplitWinNames(CrewAwardWins_SK);
									System.out.print(splitter_WName.trim()+"#<>#");
									
								}
								else
								{
									
									System.out.print("#<>#");
								}
								
							
							
							///////////////Winner_Type/////////////////
						
							System.out.print(Win_type.trim()+"#<>#");
						
							///////////////Winner_Flag////////////////
							//System.out.println("\n");
							
							 String CrewAwardWins_Flag=Xsoup.compile("//div[@class='field field-name-kinepolis-awards-nominations field-type-ds field-label-inline clearfix']/h2[@class='label-inline field-label ds-normal-text']/text()").evaluate(document).get();
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
					
					
							
						/*
							
							CrewAwardCategorys=Xsoup.compile("//div[@class='award-contest-title']//span[@class='award-category']/text()").evaluate(document).list();
							if(CrewAwardCategorys!=null)
							{
								for(String CrewAwardCategory:CrewAwardCategorys)
								{
							
							System.out.print(CrewAwardCategory+"#<>#");
								}
							}
							
							
							
							CrewAwardWinNames=Xsoup.compile("//div[@class='award-contest-title']//span[@class='award-actor']/a/text()").evaluate(document).list();
							if(CrewAwardWinNames!=null)
							{
								for(String CrewAwardWinName:CrewAwardWinNames)
								{
							
							System.out.print(CrewAwardWinName+"#<>#");
								}
							}
							
							
							
							CrewAwardWins_SK=Xsoup.compile("//div[@class='award-contest-title']//span[@class='award-actor']/a/@href").evaluate(document).list();
							if(CrewAwardWins_SK!=null)
							{
								for(String CrewAwardWin_SK:CrewAwardWins_SK)
								{
							
							//System.out.println(CrewAwardWin_SK);
							
							SplitWinNames(CrewAwardWin_SK);
							System.out.println(splitter_WName+"#<>");
							
							
								}
							}
							*/
							
									//System.out.println("\n\n");
							
							
						//ProgramCrewTabs();
							//ProgramCrewActorsTabs();
						}
					}
					
				}
			}
		}
		
		catch(Exception e)
		{
			e.getMessage();
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
