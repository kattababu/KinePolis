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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import us.codecraft.xsoup.Xsoup;

public class KinePolisRMCNT {

	
	/**
	 * @return 
	 * 
	 */
	public  KinePolisRMCNT() {
		// TODO Auto-generated constructor stub
	}
	
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,content=null,splitter_SK=null,ImgDimes=null;
	
	final String mainhost="https://kinepolis.fr";
	
	static String MImg=null;
	
	static String Mvurl=null;
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	MSDigest msd=new MSDigest();
	
	
	
	static 
	{
		
		
		FileStore.RichMediaTable("richmedia");
	}
	
	

	public void KinePolisRNT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRM,true);
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
									
							
							content=Bytes.toString(kv.getValue());
							final Document document = Jsoup.parse(content);
							
							//Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							
							//SplitMurl(Mvurl);
							
							 
							 /////////////////////////////////// Basic Image//////////////////////////
							 final AnonymousInner ai=new AnonymousInner()
							 {
									
										
								
								String Prog_types="movie".trim();
							/*
							////////////////////////// Personnes/////////////// Images//////////
							List<String>alist=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@src").evaluate(document).list();
							for(String actsimgurl2:alist)
							{
								if(actsimgurl2.isEmpty()||actsimgurl2==""||actsimgurl2==null)
								{
									
								//System.out.println(Vimgurl3);
									break;
									
									
								}
								
							if(actsimgurl2!=null)
							{
							
								//System.out.println(mainhost+actsimgurl2);
								Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								
								MImg=mainhost+actsimgurl2.trim();
								msd.MD5(MImg.trim());
								String Iwidth=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@width").evaluate(document).get();
								String Iheight=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@height").evaluate(document).get();
								
								String Image_Type="small";
								String Dimen_size=Iwidth.trim()+"x"+Iheight.trim();
								
								ImagTab(Image_Type,Dimen_size,Mvurl);
								
							}
							
							
							}
							*/
							
							
							
							
									//List<String> list = Xsoup.compile("//tr/td/text()").evaluate(document).list();
							
									/////////////////////////// Image Sliders/////////////////////////////
									
							 
								
							
							
							
							/////////////////////////////////////  Video Posters//////////////////////////////////
							
								 
									
							
							
							@Override
							public void Medium() throws Exception {
								// TODO Auto-generated method stub
								
								
								String imgurl=Xsoup.compile("//*[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@src").evaluate(document).get();
								
								
								if(imgurl!=null)
								{
									Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
									
									
									String Dimen_size="";
							
								//System.out.println(mainhost+imgurl);
									
									MImg=mainhost+imgurl.trim();
									msd.MD5(MImg.trim());
									
									String Img_typ="medium";
									
									if(MImg.contains("x"))
									{
										ImageDes(MImg);
										Dimen_size=ImgDimes;
										
										//System.out.print(ImgDimes+"#<>#");
									}
									else
									{
										Dimen_size="";
										
									}
									
									
									//System.out.println("Welcome to India");
									
									ImagTab(Img_typ,Dimen_size,Mvurl,Prog_types);
								}
								
							
								
							}



							@Override
							public void Poster() throws Exception {
								// TODO Auto-generated method stub
								
								
								String  Vimgurl3=Xsoup.compile("//div[@class='video-js-wrapper']//video/@poster").evaluate(document).get();
								
								
								
								
								if(Vimgurl3!=null)
								{
								//System.out.println(Vimgurl3);
									Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
									
									MImg=Vimgurl3.trim();
									msd.MD5(MImg.trim());
									
									String Iwidth=Xsoup.compile("//div[@class='video-js-wrapper']//video/@width").evaluate(document).get();
									String Iheight=Xsoup.compile("//div[@class='video-js-wrapper']//video/@height").evaluate(document).get();
									
									
									String Image_Type="poster";
									String Dimen_size=Iwidth.trim()+"x"+Iheight.trim();
									
									ImagTab(Image_Type,Dimen_size,Mvurl,Prog_types);
									
									
								}
								



								
							}



							@Override
							public void Large() throws Exception {
								// TODO Auto-generated method stub
								
								List<String>list=Xsoup.compile("//ul[@class='kinepolis-slider-list']//li//img/@src").evaluate(document).list();
								for(String Slideimgurl3:list)
								{
									
									
									if(Slideimgurl3!=null)
									{
									//System.out.println("Happy with Posters");
									
								//System.out.println(mainhost+Slideimgurl3);
									Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
									
									MImg=mainhost+Slideimgurl3.trim();
									msd.MD5(MImg.trim());
									
									String Iwidth=Xsoup.compile("//ul[@class='kinepolis-slider-list']//li//img/@width").evaluate(document).get();
									String Iheight=Xsoup.compile("//ul[@class='kinepolis-slider-list']//li//img/@height").evaluate(document).get();
									
									
									String Image_Type="large";
									String Dimen_size=Iwidth.trim()+"x"+Iheight.trim();
									
									ImagTab(Image_Type,Dimen_size,Mvurl,Prog_types);
									
								}
								
								
								
								}
								
								
								
							}
							 
							 
							 };
							
										ai.Medium();
																					
										ai.Poster();
																									
										ai.Large();
																				 
							 
							 
							 }
					}
				}
			}
		}
		catch(Exception e)
		{
			//e.getMessage();
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
	
	
	
	
	//////////////////////////////////// FOR Crew Director AND Crew Actor RICHMEDIA ///////////////////////////
	
	public void KinePolisCDCANT(String names)
	{
		try
		{
			
			fos = new FileOutputStream(FileStore.fileRM,true);
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
									
						
							content=Bytes.toString(kv.getValue());
							Document document = Jsoup.parse(content);
							
							
							String actsimgurl2=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@src").evaluate(document).get();
								
							if(actsimgurl2!=null)
							{
							
								//System.out.println(mainhost+actsimgurl2);
								Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
								
								MImg=mainhost+actsimgurl2.trim();
								msd.MD5(MImg.trim());
								String Iwidth=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@width").evaluate(document).get();
								String Iheight=Xsoup.compile("//div[contains(@class, 'field field-name-field-person-picture field-type-image field-label-hidden')]/div[contains(@class, 'field-items')]/div[contains(@class, 'field-item') and contains(@class, 'even')]/img/@height").evaluate(document).get();
								
								String Image_Type="small";
								String Prog_types="crew".trim();
								String Dimen_size=Iwidth.trim()+"x"+Iheight.trim();
								
								ImagTab(Image_Type,Dimen_size,Mvurl,Prog_types);
								
							}
							
							
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
		
							
	
	
	
	
	
	
	
	
	
	/////////////////////////////////////////////////////////END//////////////////////						
							
		public void ImagTab(String Image_Type,String Dimension_Image,String Murl,String Prog_types)
		{
			///////////// Image SK/////////////////
			SplitMurl(Murl);
			
			System.out.print(msd.md5s+"#<>#");
			
///////////// Program SK/////////////////
			
			System.out.print(splitter_SK.trim()+"#<>#");
			
///////////// Program_Type/////////////////
			
			System.out.print(Prog_types.trim()+"#<>#");
			
///////////// Media_ Type/////////////////
			
			System.out.print("image"+"#<>#");
			
			
///////////// Image_Type/////////////////
			
			System.out.print(Image_Type.trim()+"#<>#");
			
			
/////////////Size_Image/////////////////
			
			System.out.print("#<>#");
			
///////////// Dimensions_Image/////////////////
			
			System.out.print(Dimension_Image.trim()+"#<>#");
			
			
/////////////Description_Image/////////////////
			
			
			System.out.print("#<>#");
			
			
///////////// Image_Url/////////////////
			
			System.out.print(MImg.trim()+"#<>#");
			
			
///////////// Reference_Url/////////////////
			
			System.out.print(Murl.trim()+"#<>#");
			
			
///////////// Aux_Info/////////////////
			
			System.out.print("#<>#");
			
			
			
///////////// Created _At/////////////////
			
			System.out.print("#<>#");
			
/////////////Modified_At/////////////////
			
			System.out.print("#<>#");
			
///////////// Last_Seen////////////////
			
			System.out.print("#<>#");
			////////////// New Line////////////////
			
			System.out.print("\n");
			
			
			
			
			
			
			
			
			
			
			
			
		}
	
		public void SplitMurl(String name)
		{
			String[] split=name.split("\\/");
			splitter_SK=split[split.length - 1];
			//System.out.println(splitter);
		}
	
		
		public void ImageDes(String name)
		{
			
				String[] split=name.split("\\/");
				String splitterIMD=split[split.length - 1];
				//System.out.println("\n");
				
				String pattern="(\\d+)(x)(\\d+)";
				
				Pattern r = Pattern.compile(pattern);

			      // Now create matcher object.
			      Matcher m = r.matcher(splitterIMD);
			      if (m.find( )) {
			    	  ImgDimes=  m.group(0) ;
			           }else {
			        	   ImgDimes="";
			        // System.out.println("NO MATCH");
			      }
			      /*
				String dsp[]=splitterIMD.split("x");
				String fn=dsp[0];
				System.out.println(num);
				
				
				String nn=dsp[1];
				String lastn=nn.substring(0, num);
				
				ImgDimes=fn+"x"+lastn;
				*/
			
			//System.out.println(dsp);
				
				//System.out.println("\n");
				
				
			}
		
		

		
		
		

	
}
	
	abstract class AnonymousInner {
		   public abstract void Medium() throws Exception;
		   public abstract void Poster() throws Exception;
		   public abstract void Large() throws Exception;
		   
		}
