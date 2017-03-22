 package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

public class Sample {

	public Sample() {
		// TODO Auto-generated constructor stub
	}

	
	
	/**
	 * @param args
	 */
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	static String namesC=null;
	static String namesC2=null;
	
	static String rownames=null,family=null,qualifier=null,splitter_SK=null,ImgDimes=null,MImg=null;
	static String content=null;
	
	static HTable ht=null;
	static Scan sc=null;
	static String Mvurl=null;
	static MSDigest msd=new MSDigest();
	
	
	
	/*
	static 
	{
		
		file=new File("/katta/KinePole/SampleCont.txt");
	}
	*/
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try
		{
			//fos = new FileOutputStream(file,true);
			//ps = new PrintStream(fos);
			// System.setOut(ps);
			
			
		Configuration config=HBaseConfiguration.create();
		ht=new HTable(config,"kinepolies_webpage");
		sc=new Scan();
		ResultScanner rescan=ht.getScanner(sc);
		
		for(Result res = rescan.next(); (res != null); res=rescan.next())
		{
			for(KeyValue kv:res.list())
			{
				
				rownames=Bytes.toString(kv.getRow());
				
				family=Bytes.toString(kv.getFamily());
				qualifier=Bytes.toString(kv.getQualifier());
				
				
				
				if(rownames.equals("fr.kinepolis:https/personnes/ted-demme"))
				{
					if(family.equals("f") && qualifier.equals("cnt"))
					{
						content=Bytes.toString(kv.getValue());
											
					//	System.out.println(content);
						Document document = Jsoup.parse(content);
						
						//System.out.println("\n\n\n\n");
						
						
						
						
						////////////////////// Sample Data/////////////////
						
						
						
						
						String Vimgurl3=Xsoup.compile("//div[@class='video-js-wrapper']//video/@poster").evaluate(document).get();
						if(Vimgurl3.isEmpty()||Vimgurl3==""||Vimgurl3==null)
						{
							
						//System.out.println(Vimgurl3);
							break;
							
							
						}
						
						else if(Vimgurl3!=null)
						{
							Mvurl=Xsoup.compile("//meta[@property='og:url']/@content").evaluate(document).get();
							//System.out.println(Mvurl);
							
							
							MImg=Vimgurl3.trim();
							msd.MD5(MImg.trim());
							
							String Iwidth=Xsoup.compile("//div[@class='video-js-wrapper']//video/@width").evaluate(document).get();
							String Iheight=Xsoup.compile("//div[@class='video-js-wrapper']//video/@height").evaluate(document).get();
							
							
							String Image_Type="poster";
							String Dimen_size=Iwidth.trim()+"x"+Iheight.trim();
							
							ImagTab(Image_Type,Dimen_size,Mvurl);
							
						}
					}
					
				}
				
				
				
								
				
			}
		}
		
		
		ht.close();
		rescan.close();
		}
		catch(Exception e)
		{
			e.getMessage();
			//e.printStackTrace();
		}
		
		
		
		
		

	}
	
	
	
	
	
	public static void ImagTab(String Image_Type,String Dimension_Image,String Murl)
	{
		///////////// Image SK/////////////////
		SplitMurl(Murl);
		
		System.out.print(msd.md5s+"#<>#");
		
///////////// Program SK/////////////////
		
		System.out.print(splitter_SK.trim()+"#<>#");
		
///////////// Program_Type/////////////////
		
		System.out.print("movie"+"#<>#");
		
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

	public static void SplitMurl(String name)
	{
		String[] split=name.split("\\/");
		splitter_SK=split[split.length - 1];
		//System.out.println(splitter);
	}

	
	public static void ImageDes(String name)
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