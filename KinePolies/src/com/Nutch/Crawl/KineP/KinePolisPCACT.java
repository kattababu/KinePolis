/**
 * 
 */
package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;

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
public class KinePolisPCACT {

	/**
	 * 
	 */
	public KinePolisPCACT() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null,splitter_UName=null;
	final String mainhost="https://kinepolis.fr";
	
	
	static String CrewDurl=null;
	static String CrewName=null;
	static String CrewDOB=null;
	static String CrewCountry=null;
	static String CrewDateCon=null;
	static String CrewImage=null;
	static String CrewImg=null;
	List<String> RFlist=null;
	static String symb="";
	
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	/////////////////////////////////////////////////// Crew For Actors//////////////////////////////
	
	
	public void KinePolisCrewPANT(String names)
	{
		try
		{
			
	
			
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
									
						
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
							
							List<String> CrewActors=Xsoup.compile("//div[@class='clearfix-field field field-name-movie-cast-list field-type-ds field-label-inline clearfix']//div[@class='field field-name-title field-type-ds field-label-hidden']//div[@class='field-items']//a/@href").evaluate(document).list();
							if(CrewActors!=null)
							{
								for(String  CrewActor:CrewActors)
								{
							//System.out.println(mainhost+CrewDirector);
							 String CrewAQL=mainhost+CrewActor.trim();
							 
							 if(CrewAQL.contains("/films/")||CrewAQL.contains("/evenements/"))
							 {
								 break;
							 }
							 else
							 {
								 KinePolisCrewPAQLNT(CrewAQL);
																	 
							 }
							 
							// System.out.println(CrewAQL);
							 
							 	
								}
							//KinePolisCrewQLsNT(CrewDQL);
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
	
	
	
	
	//////////////////////////// Crew Actors List Qualifier Names////////////////////////////
	
	
	public void KinePolisCrewPAQLNT(String names)
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
						 if(rownames.contains(splitter_UName) && rownames.endsWith(splitter_UName) && rownames.contains("/personnes/"))
						 {
							// System.out.println("\n");
							//System.out.println(rownames);
							//System.out.println("\n");
							 
							new KinePolisPCACT().KinePolisCrewRPCACNT(rownames);
							
							
							
							new KinePolisRMCNT().KinePolisCDCANT(rownames);
							 //KinePolisCrewRCNT(rownames);
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
	
	
	////////////////////////////// CNT OF  ROWS PCACNT////////////////////////////////////////
	
	public void KinePolisCrewRPCACNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileC,true);
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
						
							
							CrewDurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							//System.out.println(CrewDurl);
							
							SplitUrlNames(CrewDurl);
							
							//System.out.println(CrewDurl);
							
							
							CrewName=Xsoup.compile("//div[@class='field field-name-title field-type-ds field-label-hidden']/div[@class='field-items']//h1/text()").evaluate(document).get();
							//System.out.println(CrewName);
							 
							//System.out.println(CrewName);
							CrewDOB=Xsoup.compile("//div[@class='field field-name-field-person-birthday field-type-date field-label-inline clearfix']/div[@class='field-items']//span[@class='date-display-single']/text()").evaluate(document).get();
							//System.out.println(CrewDOB);
							
							
							//System.out.println(CrewDOB);
							
							
							CrewCountry=Xsoup.compile("//div[@class='field field-name-field-country field-type-taxonomy-term-reference field-label-inline clearfix']/div[@class='field-items']//*/text()").evaluate(document).get();
							//System.out.println(CrewCountry);
							
							CrewImg=Xsoup.compile("//div[@class='field field-name-field-person-picture field-type-image field-label-hidden']/div[@class='field-items']//img/@src").evaluate(document).get();
						
							CrewImage=mainhost+CrewImg;
							//System.out.println(CrewImage);
							
							
							RFlist=Xsoup.compile("//div[@id='block-kinepolis-movie-filter-kinepolis-movie-filter-related']/div[@class='inner-block']//div[@class='movie-container-image-wrapper']/a/@href").evaluate(document).list();
							
							//System.out.println(CrewCountry);
							
							
						//ProgramCrewTabs();
							ProgramCrewActorsTabs();
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

	
	
	 public void ProgramCrewActorsTabs()
	 {
		 /////////////////////// Crew_SK///////////////////////////
		 System.out.print(splitter_UName.trim()+"#<>#");
		 
	///////////////////////Crew_ Name///////////////////////////
		 String FilterName=CrewName.replace("-", " ").trim();
		 System.out.print(FilterName.trim()+"#<>#");
		 
	/////////////////////// Crew OriginalName///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew Description///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew aka///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Gender///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew Age ///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew Blood_group///////////////////////////
		 System.out.print("#<>#");
		 
		 
	///////////////////////Crew_BirthDate///////////////////////////
		 if(CrewDOB!=null)
		 {
			 CrewDateFormat(CrewDOB);
		 System.out.print(CrewDateCon.trim()+"#<>#");
		 }
		 else
		 {
			 System.out.print("#<>#");
			 
		 }
		 
		 
		 
	/////////////////////// Crew_BirthPlace///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Death_Date///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Death_Place///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Constellation///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Country///////////////////////////
		 if(CrewCountry!=null)
		 {
		 System.out.print(CrewCountry.trim()+"#<>#");
		 }
		 else
		 {
			 System.out.print("#<>#");
		 }
		 
		 
	/////////////////////// Crew_Occuption///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Biography///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_height///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Weight//////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Rating///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Top_rated_Works///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew_Num_of Ratings///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew_Family_Numbers///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Recent_Films///////////////////////////
		 
		 if(RFlist!=null)
		 {
			 for(String RFL:RFlist)
			 {
				 SplitUrlNames(RFL);
				 System.out.print(symb+splitter_UName.trim());
				 symb="<>";
				 
				 
			 }
			 System.out.print("#<>#");
			 
			 symb="";
		 }
		 else
		 {
		 System.out.print("#<>#");
		 }
		 
		 
	/////////////////////// Crew_Image///////////////////////////
		 if(CrewImg!=null)
		 {
		 System.out.print(CrewImage.trim()+"#<>#");
		 }
		 else
		 {
			 System.out.print("#<>#");
		 }
		 
		 
	/////////////////////// Crew_Videos///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew_Refernce_Url///////////////////////////
		 System.out.print(CrewDurl.trim()+"#<>#");
		 
		 
	/////////////////////// Crew_Aux_Info///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew_Created_At///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Modified_At///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Last_Seen///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_New Line///////////////////////////
		 System.out.print("\n");
		 
		 
		 
		 
		 
	 }
		


	public void SplitUrlNames(String name)
	{
		String[] split=name.split("\\/");
		splitter_UName=split[split.length - 1];
		//System.out.println(splitter);
	}
	
	public void CrewDateFormat(String name)
	{
		try
		{
			
			
		
		Date formatter = (Date) new SimpleDateFormat("EEE MMM dd yyyy", Locale.FRENCH).parse(name.replace(",", ""));
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
