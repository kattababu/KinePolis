/**
 * 
 */
package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;




/**
 * @author surendra
 *
 */
public class KinePolisJsonShowData {

	/**
	 * 
	 */
	public KinePolisJsonShowData() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,rownames1=null,content=null,splitter_SK=null,Df=null,splitter_SK2=null;
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	
	static 
	{
		FileStore.TheaterAvailTable("theateravailability");
		
		
	}
	
	
	public  void KinePolisJSD()
	{
		try
		{
			
			
			fos = new FileOutputStream(FileStore.fileTHRAVL,true);
			ps = new PrintStream(fos);
			System.setOut(ps);
			
		Configuration config=HBaseConfiguration.create();
		ht=new HTable(config,"kinepshow_webpage");
		sc=new Scan();
		rescan=ht.getScanner(sc);

			
			for(Result res = rescan.next(); (res != null); res=rescan.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					if(rownames.contains("/presales/"))
					{
						if(family.equals("f") && qualifier.equals("cnt"))
						{
							content=Bytes.toString(kv.getValue());
							
						//	System.out.println("\n\n"+rownames+"\n\n");
							
							
							//splitter_SK2="FORVM";
							
							
								
								JSONParser parser = new JSONParser();
								Object obj = parser.parse(content);								
								JSONArray tag = (JSONArray)obj;							
								JSONObject jsonObject1 = (JSONObject) tag.get(3);								
								String structure = (String) jsonObject1.get("data");
								Document document = Jsoup.parse(structure);
								
								
								Elements el=Xsoup.compile("//div[@class='movie-container-info']").evaluate(document).getElements();
								for(Element xel:el)
								{
									if(xel!=null)
									{
										
										Elements el2=Xsoup.compile("//div[@class='purchase_item']").evaluate(xel).getElements();
										for(Element xel2:el2)
										{
										
											if(xel2!=null)
											{
												
												
												
												///////////// IS_3D/////////////////
												List<String> formatD=Xsoup.compile("//span[@class='purchase_item_version']/text()").evaluate(xel2).list();
												for(String Dform:formatD)
												{
												
													
													
													Elements el3=Xsoup.compile("//a[@target='_blank']").evaluate(xel2).getElements();
													for(Element xel3:el3)
													{
													
														if(xel3!=null)
														{
															
															
															
															/////////////////////////////  for Theater_Sk///////////
															
															String showtimelink=Xsoup.compile("/@href").evaluate(xel3).get();
															SplitUrl2(showtimelink);
												
										//// Program _SK///////////////
											
											
											String urllist=Xsoup.compile("//a[@class='movie-container-action kinepolis-ajaxify-get']/@href").evaluate(xel).get();
											SplitUrl(urllist);
											System.out.print(splitter_SK.trim()+"#<>#");
											
											
											//// Program_type////////////
											System.out.print("movie".trim()+"#<>#");
											
											/////////// Theater_Sk//////////////////////
											
											if(splitter_SK2.equals("FORVM".trim()))
											{
												String name="forvm-centre-nimes".trim();
												System.out.print(name.trim());
											}
											
											else if(splitter_SK2.equals("KBOUR".trim()))
											{
												String name="kinepolis-bourgoin-jallieu".trim();
												System.out.print(name.trim());
											}
											
											else if(splitter_SK2.equals("KFEN".trim()))
											{
												String name="kinepolis-fenouillet".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KLOM".trim()))
											{
												String name="kinepolis-lomme".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("ULONG".trim()))
											{
												String name="kinepolis-longwy".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KMUL".trim()))
											{
												String name="kinepolis-mulhouse".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KNCY".trim()))
											{
												String name="kinepolis-nancy".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KNIM".trim()))
											{
												String name="kinepolis-nimes".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KROU".trim()))
											{
												String name="kinepolis-rouen".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KMETZ".trim()))
											{
												String name="kinepolis-st-julien-les-metz".trim();
												System.out.print(name.trim());
											}
											else if(splitter_SK2.equals("KTHIO".trim()))
											{
												String name="kinepolis-thionville".trim();
												System.out.print(name.trim());
											}
											
											
											
											System.out.print("#<>#");
										
											///////////// Showtime/////////////////
											String showtime=Xsoup.compile("/text()").evaluate(xel3).get();
											System.out.print(showtime.trim()+"#<>#");
											
											
											///////////// Ticket_Booking_Link/////////////////
											
											System.out.print("https://kinepolis.fr"+showtimelink.trim()+"#<>#");
											
											
											///////////// IS_3D/////////////////
											DFormat(Dform);
											
											if(Df.equals("3"))
											{
											System.out.print("1".trim()+"#<>#");	
											}
											else
											{
											System.out.print("#<>#");
											}
											
											/////////////// Created_At//////////////
											System.out.print("#<>#");
											
											/////////////// Modified_At//////////////
											System.out.print("#<>#");
											
											
											///////////////Last_Seen//////////////
											System.out.print("#<>#");
										
										
											System.out.print("\n");
														}
													}
												}
											}
										
											
										
											
										}
									
										
										}
										
								
										
									}
								
								
							
							
							//System.exit(0);
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
			
	public void SplitUrl(String name)
	{
		//System.out.println(name);
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		//System.out.println(splitter);
	}			
				
	
	
	public void SplitUrl2(String name)
	{
		//System.out.println(name);
		String[] split=name.split("\\/");
		splitter_SK2=split[split.length - 1];
		//System.out.println(splitter_SK2);
	}			
				
	
	
	public void DFormat(String name)
	{
		
			
			String pattern="(\\d+)";
			
			Pattern r = Pattern.compile(pattern);

		      // Now create matcher object.
		      Matcher m = r.matcher(name);
		      if (m.find( )) {
		    	  Df=  m.group(0) ;
		    	 // System.out.println(Df);
		           }
		        
		      }
		      


}
