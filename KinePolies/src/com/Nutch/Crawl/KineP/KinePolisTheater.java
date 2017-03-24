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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class KinePolisTheater {

	/**
	 * 
	 */
	public KinePolisTheater() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,rownames1=null,content=null,splitter_SK=null;
	 List<String> sam=null;
	 String finalrooms=null;
	 int totalseats=0;
	 String add1=null,add2=null,add3=null,add4=null,add5=null,add6=null,add7=null,add8=null,add9=null;
	 static FileOutputStream fos=null;
		static PrintStream ps=null;
		
		static File file=null;
	

	 
	 static 
		{
			FileStore.TheaterTable("theater");
			
			//file=new File("/katta/KinePole/CrewDTCNT.txt");
		}
		
	 
	 
	
	
	
	public void KinePolisCMH()
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
					
					if(rownames.endsWith("/infos"))
					{
						if(family.equals("f") && qualifier.equals("cnt"))
						{
						
							new KinePolisTheater().KinePolisCMH2R(rownames);
						//System.out.println(rownames);
						}
					
					
				}
			
					
					
					/*
					if(family.equals("ol"))
					{
						if(qualifier.equals("https://kinepolis.fr/"))
						{
							
							if(rownames.endsWith("/infos"))
							{
								sam = new ArrayList<String>(Arrays.asList(rownames));
								
								
								
								for(String one:sam)
								{
									System.out.println("Value:"+one);
									new KinePolisTheater().KinePolisCMH2R(one);
									
								}
								
								
							
							}
							
							
							
							
							
						
							
						}
						
						
						
						
						
						
					}
					*/
					
					
					
					
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
	
	
	
	
	/*
	
	public void KinePolisCMH2()
	{
		try
		{
			
			
			String last=null;
			
			
			
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
					//rownames1=Bytes.toString(kv.getRow());
					qualifier=Bytes.toString(kv.getQualifier());
					
					
					if(family.equals("il"))
					{
					
						
					//System.out.println(qualifier);
					if(qualifier.equals("https://kinepolis.fr/cinek"))
					{
						if(rownames.endsWith("/infos"))
						{
							last=rownames;
							System.out.println("rowlast"+last);
							
							
							
							
							
							
						}
						
					
					
					}
				
					
					}
	


					
					
					
					
					
					
					
					}
		}
			
			new KinePolisTheater().KinePolisCMH2R(last);
			//System.out.println("last"+last);
			
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
	
	
	
	*/

	//////////////////////////////
	
	
	
	public void KinePolisCMH2R(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileTHR,true);
			ps = new PrintStream(fos);
			System.setOut(ps);
		//System.out.println("incoming row"+names);
			
			
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
							
							///////////////////////////  Theater_SK//////////////////
							
						
							String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							SplitUrl(url);
							System.out.print(splitter_SK.trim()+"#<>#");	
							
							///////////////////////////  Theater_Name//////////////////
							
							String title=Xsoup.compile("//meta[@property='og:title']/@content").evaluate(document).get();
							
							System.out.print(title.trim()+"#<>#");
							
							///////////////////////////  Theater_Screen//////////////////
							
							System.out.print("#<>#");
							
							///////////////////////////  Theater_Location//////////////////
							
							
							
							String location=Xsoup.compile("//div[@class='addressfield-container-inline locality-block country-FR']/span[@class='locality']/text()").evaluate(document).get();
							System.out.print(location.trim()+"#<>#");
							
							
							///////////////////////////  Theater_FirmName//////////////////
							
							System.out.print("#<>#");
							
							
							///////////////////////////  Theater_IS_3D//////////////////
							
							System.out.print("#<>#");
							
							///////////////////////////  Theater_No_of Rooms//////////////////
							
							
							
							
							List<String>rooms=Xsoup.compile("//table[@class='sticky-enabled']/tbody/tr/td[1]/text()").evaluate(document).list();
							for(String room:rooms)
							{
								finalrooms=room;
							}
							
							System.out.print(finalrooms.trim()+"#<>#");
							
	///////////////////////////  Theater_No_of Seats//////////////////
							
							List<String>seats=Xsoup.compile("//table[@class='sticky-enabled']/tbody/tr/td[2]/text()").evaluate(document).list();
							for(String seat:seats)
							{
							
								totalseats+=Integer.parseInt(seat.trim());
							}
							System.out.print(totalseats+"#<>#");
							
							///////////////////////////  Theater_Contact_NUmbers//////////////////
							
							
							
							String contact_num=Xsoup.compile("//div[@class='field field-name-field-theater-phone field-type-text field-label-hidden']//div[@class='field-item even']/text()").evaluate(document).get();
							System.out.print(contact_num.trim()+"#<>#");
							
							///////////////////////////  Theater_ZipCode//////////////////
							
							
							String zipcode=Xsoup.compile("//div[@class='padding-fields field field-name-field-theater-address field-type-addressfield field-label-above']//span[@class='postal-code']/text()").evaluate(document).get();
							System.out.print(zipcode.trim()+"#<>#");
							
							
							
							
							//////////////////////////  Theater_Latitude//////////////////
							Element el=document.select("script[type=\'application/ld+json\']").first();
							String scriptContent = el.html();
							
							//System.out.println(scriptContent);
							JSONParser parser = new JSONParser();
							//
							Object object = parser.parse(scriptContent); 
							JSONObject jsonObject = (JSONObject) object;
							//JSONArray array = (JSONArray) object;
							JSONArray tag = (JSONArray)jsonObject.get("@graph");
							
							JSONObject jsonObject1 = (JSONObject) tag.get(1);
							
							//System.out.println(jsonObject1);
							
							JSONObject structure = (JSONObject) jsonObject1.get("geo");
							
							
							
							
							
							String lon=structure.get("longitude").toString();
							
							String lat=structure.get("latitude").toString();
							
							
							
							System.out.print(lon.trim()+"#<>#");
							
							///////////////////////////  Theater_Longitude//////////////////
							
							System.out.print(lat.trim()+"#<>#");
							
							
							///////////////////////////  Theater_Address//////////////////
						
							
							String address1=Xsoup.compile("//div[@class='padding-fields field field-name-field-theater-address field-type-addressfield field-label-above']/div[@class='addressfield-container-inline name-block']/*/text()").evaluate(document).get();
							if(address1!=null)
							{
								add1=address1;
								System.out.print(add1.trim());
							}
							
							String address2=Xsoup.compile("//div[@class='padding-fields field field-name-field-theater-address field-type-addressfield field-label-above']/div[@class='street-block']/*/text()").evaluate(document).get();
							if(address2!=null)
							{
								add2=address2;
								System.out.print(add2.trim());
							}
							
							String address3=Xsoup.compile("//div[@class='padding-fields field field-name-field-theater-address field-type-addressfield field-label-above']//span[@class='postal-code']/text()").evaluate(document).get();
							if(address3!=null)
							{
								add3=address3;
								System.out.print(add3.trim());
							}
							
							String address4=Xsoup.compile("//div[@class='addressfield-container-inline locality-block country-FR']/span[@class='locality']/text()").evaluate(document).get();
							if(address4!=null)
							{
								add4=address4;
								System.out.print(add4.trim());
							}
							
							String address5=Xsoup.compile("//div[@class='padding-fields field field-name-field-theater-address field-type-addressfield field-label-above']//span[@class='country']/text()").evaluate(document).get();
							if(address5!=null)
							{
								add5=address5;
								System.out.print(add5.trim());
							}
							
							String address6=Xsoup.compile("//div[@class='field field-name-field-theater-phone field-type-text field-label-hidden']//div[@class='field-item even']/span/text()").evaluate(document).get();
							if(address6!=null)
							{
								add6=address6;
								System.out.print(add6.trim());
							}
							
							
							String address7=Xsoup.compile("//div[@class='field field-name-field-theater-phone field-type-text field-label-hidden']//div[@class='field-item even']/text()").evaluate(document).get();
							if(address7!=null)
							{
								add7=address7;
								System.out.print(add7.trim());
							}
						
							
							
							String address8=Xsoup.compile("//div[@class='field field-name-field-theater-fax field-type-text field-label-hidden']//div[@class='field-item even']/span/text()").evaluate(document).get();
							if(address8!=null)
							{
								add8=address8;
								System.out.print(add8.trim());
							}
							
							String address9=Xsoup.compile("//div[@class='field field-name-field-theater-fax field-type-text field-label-hidden']//div[@class='field-item even']/text()").evaluate(document).get();
							if(address9!=null)
							{
								add9=address9;
								System.out.print(add9.trim());
							}
							
						
							
							System.out.print("#<>#");
							
						///////////////////////////  Theater_Theater_URL//////////////////
							
							System.out.print(url.trim()+"#<>#");							
							
					///////////////////////////  Theater_Created_AT//////////////////
							
							System.out.print("#<>#");
							
					///////////////////////////  Theater_Modified_AT//////////////////
							
							System.out.print("#<>#");
							
					///////////////////////////  Theater_Last_Seen//////////////////
							
							System.out.print("#<>#");
							
							System.out.print("\n");
							
							
							
							 
							
							
							
							
							
							
							
							
							
							
						
							
							
							
							
							
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
	
	
	
	
	public void TheaterTab()
	{
		
		

		

		

		

		
		
		

		

		
		System.out.println("#<>#");
		

		
	}
	
	public void SplitUrl(String name)
	{
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 2];
		//System.out.println(splitter);
	}


}
