

/**
 * 
 */
package com.Nutch.Crawl.KineP;

//import java.util.List;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Locale;
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
import org.apache.tika.language.LanguageIdentifier;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

/**
 * @author surendra
 *
 */
public class KinePolisMVNCT {

	/**
	 * 
	 */
	public KinePolisMVNCT() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null,splitter_Count=null,data=null,splitter_Title=null;
	String symb34="";
	String ElCrewDes=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static String comma=null;
	

	static 
	{
		
		//file=new File("/katta/KinePole/MovieCNT.txt");
				
				FileStore.MovieTable("movie");
		 

	}
	
	

	public void KinePolisCNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileM,true);
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
							
							//Element elm=document.body();
							
								
								//////////Movie SK _ Value//////////////
							
							String url=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							SplitUrl(url);
							System.out.print(splitter_SK.trim()+"#<>#");							
							//System.out.println(url);
							
							///////////////////////////Movie _ title/////////////////////////
							
							String title=Xsoup.compile("//div[@class='field-item even']/h1/text()").evaluate(document).get();
							String filtertitle=title.replace(",", "").replace("... ", " ").replace("...", " ").replace("/", "").replace("(", "").replace(")", "").replace(".", "").replace("%", "").replace("!", "");//.replace("-", "");
							if(filtertitle.contains(":"))
							{
							
							SplitTitle(filtertitle);
							System.out.print(splitter_Title.trim());
							}
							else
							{
								System.out.print(filtertitle.trim());
							}
							System.out.print("#<>#");
							//System.out.println("\n\n\n\n");
							
							//////////////// Original_Title///////////////////
							
							System.out.print("#<>#");
							
							/////////////////// Other _Titles///////////////////
							System.out.print("#<>#");
							
							///////////////////Description///////////////////
							
							
							

							Elements elsdesc=document.select("div");
							
							for(Element eld:elsdesc)
							{
								String attr=eld.attr("class");
								//System.out.println(attr);
								
								if(attr.equals("field-item even"))
								{
									
								
								
								Elements elspg=eld.getElementsByTag("p");
								for(Element elpg:elspg)
									
									
								{
									data=elpg.ownText();
									if(data!=null)
									{
										
									
									if(data.contains("Chorégraphie :") || data.contains("Direction Musicale :")|| data.contains("Compositeur:")||data.contains("Distribution: ")|| data.contains("Mise en scène:")|| data.contains("Direction musicale:")||data.contains("Auteur:")||data.contains("Musique :")||data.contains("Direction Musicale:")||data.contains("Distribution :"))
									{
										elpg.remove();
									}
									else
									{
									
									System.out.print(data);
									}
									}
									
									
									
									
								}
								
								}
							
							}
							
							System.out.print("#<>#");
							
							
							/*
							List<String> descript=Xsoup.compile("//div[@class='field-item even']/p/text()").evaluate(document).list();
							if(descript!=null)
								
							{
								//System.out.println(descript.);
								
								for(String desc:descript)
								{
									Desclang=desc;
									
									Pattern p = Pattern.compile(":([^:]*):");
								    Matcher m = p.matcher(Desclang);
								    while(m.find())
								    {
								    	m.replaceAll("");
								    	
								    	//System.out.println("Welcome to India");
								    	
								    	//System.out.println(m.group().trim());
								    	//int min=Integer.parseInt(m.group().trim());
								     // System.out.print((60*min)+"#<>#");
								    }
								  
									
									
									
							
								//String filterDesc=Desclang.replace("…", ".").replace("...", ".").replace("... ", ".");
							System.out.print(Desclang.trim());
									
								}
								
								
								
								System.out.print("#<>#");
							}
							else
							{
								System.out.print("#<>#");
							}
							*/
							///////////////////Genres///////////////////
							
							String genres=Xsoup.compile("//div[@class='clearfix-field field field-name-field-movie-genre field-type-taxonomy-term-reference field-label-inline clearfix']/h2/text()").evaluate(document).get();
							 //System.out.println(genres);
							
							if(genres!=null)
							{
								//System.out.println("Welcome to India");
								 
								String genresvalue=Xsoup.compile("//div[@class='odd first last'][1]/text()").evaluate(document).get();
								
								
								
								System.out.print(genresvalue.replace(", ", "<>").replace("/", "<>").trim()+"#<>#");
								//*[@id="content"]/div[2]/div[1]/div[5]/div
								
								
							}
							
							else 
							{
								System.out.print("#<>#");
							}
							

														 
							 
							
							///////////////////Sub_Genres///////////////////
							System.out.print("#<>#");
							
							///////////////////Category///////////////////
							System.out.print("#<>#");
							
							///////////////////Duration///////////////////
							
							
							String mintime=Xsoup.compile("//div[@class='clearfix-field field field-name-field-movie-length field-type-number-integer field-label-inline clearfix']/h2/text()").evaluate(document).get();
							 //System.out.println(genres);
							
							if(mintime!=null)
							{
								//System.out.println("Welcome to India");
								 
								String mtvalue=Xsoup.compile("//div[@class='clearfix-field field field-name-field-movie-length field-type-number-integer field-label-inline clearfix']//div[@class='odd first last']/text()").evaluate(document).get();
								
								Pattern p = Pattern.compile("\\d+");
							    Matcher m = p.matcher(mtvalue);
							    while(m.find())
							    {
							    	//System.out.println(m.group().trim());
							    	int min=Integer.parseInt(m.group().trim());
							      System.out.print((60*min)+"#<>#");
							    }
							  
								
								//System.out.print(mtvalue.trim()+"#<>#");
								//*[@id="content"]/div[2]/div[1]/div[5]/div
								
								
							}
							
							else 
							{
								System.out.print("#<>#");
							}
							
							//System.out.print("#<>#");
							
							///////////////////Language///////////////////
							System.out.print("#<>#");
							
							///////////////////Original_Language///////////////////
							System.out.print("#<>#");
							
							///////////////////MetaData_Language///////////////////
							LanguageIdentifier identifier = new LanguageIdentifier(data);
							String lang=identifier.getLanguage();
							Locale loc =new Locale(lang);
							String namevalue=loc.getISO3Language();
							if(identifier!=null)
							{
								//fra --> Language Code =French
								namevalue="french";
							System.out.print(namevalue.substring(0, 1).toUpperCase()+namevalue.substring(1).trim());
							}
							
							System.out.print("#<>#");
							
							///////////////////Aka///////////////////
							System.out.print("#<>#");
														
							///////////////////Production_Country///////////////////
							
							
							Elements divts=document.getElementsByTag("div");
							
							
							for(Element edas:divts)
							{
								
								String attrs=edas.attr("class");
								
								if(attrs.equals("clearfix-field field field-name-field-country field-type-taxonomy-term-reference field-label-inline clearfix"))
									
								{
									Element div2s=edas.select("div.field-items").first();
									
									Elements mltdivs=div2s.children();
									
									for(Element singdiv:mltdivs)
									{
										String attrbs=singdiv.ownText();
										SplitCountry(attrbs);
										
										System.out.print(symb34+splitter_Count.trim());
										
										symb34="<>";
										
										
										
										
									}
									
									
									
									
									
									
								}
								
							}
							
						
							
							
							System.out.print("#<>#");
							//System.out.println("\n\n");
							
							///////////////////Aux_Info///////////////////
							
							String moveFormatD=Xsoup.compile("//div[@class='field field-name-movie-technologies field-type-ds field-label-above']//ul/li/span[@class='technology-available']/text()").evaluate(document).get();	
							
							String cinelistD=Xsoup.compile("//div[@class='field field-name-movie-technologies field-type-ds field-label-above']//ul/li/span[@class='technology-available-list']/text()").evaluate(document).get();	
							if(moveFormatD!=null&& cinelistD!=null)
							{
							
							Elements el=Xsoup.compile("//div[@class='field field-name-movie-technologies field-type-ds field-label-above']//ul/li").evaluate(document).getElements();
							comma="";
								
								
								if(el!=null)
								{
									System.out.print("{");
									
									
									for(Element xel:el)
									{
										
									
								
								
									String moveFormat=Xsoup.compile("/span[@class='technology-available']/text()").evaluate(xel).get();	
									
									String cinelist=Xsoup.compile("/span[@class='technology-available-list']/text()").evaluate(xel).get();
									
									
									System.out.print(comma+"\""+moveFormat.replace(": ", "")+"\""+":\""+cinelist.replace(", ", "<>").trim()+"\"");
									comma=",";
									
									//
									
									
									
									
								}
								
									System.out.print("}");
							
							}
								
								
							}
							
							
							
							
							System.out.print("#<>#");
							
							//System.out.println("\n\n");
							///////////////////Reference_URL///////////////////
							System.out.print(url.trim()+"#<>#");
							
							///////////////////Created_AT///////////////////
							System.out.print("#<>#");							
							
							///////////////////Modified_AT///////////////////
							System.out.print("#<>#");
							
							///////////////////Last_Seen///////////////////
							System.out.print("#<>#");
							
							///////////////// New Line////////////
							
							System.out.print("\n");
							
							
														
							
						}
					}
					
					
			
				}
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
			//e.getMessage();
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
		
		public void SplitTitle(String name)
		{
			//System.out.println(name);
			if(name!=null)
			{
				//int len=name.length();
			/*	
				Matcher m = Pattern.compile("(([a-zA-Z])*):").matcher(name);
				 if(m.find()) {
				    splitter_Title=m.group(1);//.replace(":", "").trim();
				    
				   System.out.println(splitter_Title);
				 }
				 */
				String regex=",";
				
				String filtername=name.replaceFirst("(([^0-9])):", ",");
				//System.out.println(filtername);
				
				if(filtername.contains(regex))
				{
					
					
					String[] split=filtername.split("\\,");
					splitter_Title=split[1];
					
				}
				else 
				{
					//System.out.println("Welcome to India");
					splitter_Title=name;
				}
			//splitter_Title=filtername;
			
			 
			
			}
			//System.out.println(splitter);
		}
		
		
		public void SplitCountry(String name)
		{
			String[] split=name.split("\\(|\\,");
			splitter_Count=split[0];
			//Splitter_count=split[1];
			//System.out.println(splitter);
		}
	
		
				
}
