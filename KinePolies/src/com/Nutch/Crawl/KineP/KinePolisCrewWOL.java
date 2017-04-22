package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import us.codecraft.xsoup.Xsoup;

public class KinePolisCrewWOL {

	public KinePolisCrewWOL() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,data=null,splitter_UName=null,Mvurl=null,splitter_MName=null,splitter_MCName=null;
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	
	MSDigest msd=new MSDigest();
	
	
	int i=1;
	
	
	public void KinePolisCrewWOLNT(String names)
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
									
							//System.out.println("\n");
							//System.out.println(rownames);
							//System.out.println("\n");
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
							Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
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
										
										
										String namess=elpg.toString().replaceAll("<p>", "").replaceAll("</p>", "");
										String [] splitter=namess.split("<br>");
										for(String split:splitter)
										{
											
											if(split.contains(":"))
											{
												SplitMCNames(split);
												
											String[] splitm=split.split(":");
											 String splitcrew=splitm[splitm.length-1];
											 
											 String[] SplitterCrews=splitcrew.split("\\,| and |;| et ");
											 for(String splitcrews:SplitterCrews)
											 {
												 //System.out.println(splitcrews);
												 SplitUrlNames(splitcrews);
												 SplitMNames(splitcrews);
												 
												new KinePolisCrewWOL().ProgramCrewWOLTabs(splitter_UName,Mvurl,splitter_MName);
												
																												 
													 new KinePolisProgCrewWOL().PCrewWOLTabs(splitter_UName,Mvurl,splitter_MCName,i++);
																		 
																		
																													 
											 }
											 
											 
											}
										}
										
										
										
										//System.out.println(namess);
										
										
									}
									
									
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
				//ps.close();
				//fos.close();
			}
			catch(Exception e)
			{
				e.getMessage();
			}
			
		}
	}
							
	
	
	
	
	public void ProgramCrewWOLTabs(String names,String url,String movies)
	 {
		try
		{
		
		fos = new FileOutputStream(FileStore.fileC,true);
		ps = new PrintStream(fos);
		System.setOut(ps);
		
		
		 /////////////////////// Crew_SK///////////////////////////
		msd.MD5(names.trim());
		 //System.out.print(splitter_UName.trim()+"#<>#");
		System.out.print(msd.md5s.trim());
		System.out.print("#<>#");
		 
	///////////////////////Crew_ Name///////////////////////////
		// System.out.println(CrewName.length());
		String FilterName=names.replace("-", " ").replace("(", "").replace(")", "").replace("/", " ").trim();
		System.out.print(FilterName.trim());
		System.out.print("#<>#");
		 
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
		 System.out.print("#<>#"); 
		 
		 
	/////////////////////// Crew_BirthPlace///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Death_Date///////////////////////////
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Death_Place///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Constellation///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Country///////////////////////////
		 System.out.print("#<>#"); 
		 
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
		 
		 System.out.print("#<>#"); 
		 
	/////////////////////// Crew_Image///////////////////////////
		 System.out.print("#<>#");
		  
	/////////////////////// Crew_Videos///////////////////////////
		 System.out.print("#<>#");
		 
		 
		 
	/////////////////////// Crew_Refernce_Url///////////////////////////
		 System.out.print(url.trim());
		 System.out.print("#<>#");
		 
	/////////////////////// Crew_Aux_Info///////////////////////////
		 if(movies!=null)
		 {
		  
			 
		 System.out.print("{\"movie\""+":\""+movies.trim().replace("/", "<>")+"\"}"+"#<>#");
		 }
		 else
		 {
		 System.out.print("#<>#");
		 }
		 
	/////////////////////// Crew_Created_At///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Modified_At///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_Last_Seen///////////////////////////
		 System.out.print("#<>#");
		 
		 
	/////////////////////// Crew_New Line///////////////////////////
		 System.out.print("\n");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			
		}
		 
		finally
		{
			try
			{
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
		String[] split=name.split("\\(");
		splitter_UName=split[split.length - 1];
		
		//System.out.println(splitter_UName);
	}
	
		
public void SplitMNames(String name)
{
	 Matcher m = Pattern.compile("\\((.*?)\\)").matcher(name);
	 if(m.find()) {
	    splitter_MName=m.group(1);
	 }
	
}



public void SplitMCNames(String name)
{
	 Matcher m = Pattern.compile("(.*):").matcher(name);
	 if(m.find()) {
	    splitter_MCName=m.group(0).replace(":", "").trim();
	    
	   // System.out.println(splitter_MName);
	 }
	
}


}
