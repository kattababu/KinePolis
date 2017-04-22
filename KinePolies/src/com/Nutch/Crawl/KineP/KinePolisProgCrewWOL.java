/**
 * 
 */
package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

/**
 * @author surendra
 *
 */
public class KinePolisProgCrewWOL {

	/**
	 * 
	 */
	public KinePolisProgCrewWOL() {
		// TODO Auto-generated constructor stub
	}
	
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	
	static File file=null;
	
	
	String splitter_UName=null,Rname=null;
	MSDigest msd=new MSDigest();
	
	
	
	public void PCrewWOLTabs(String Crew_sk,String Prog_sk,String Role,int inc)
	
	{
		try
		{
		fos = new FileOutputStream(FileStore.filePC,true);
		ps = new PrintStream(fos);
		System.setOut(ps);
		
		
		////////// Program_SK///////////////////////
		
		SplitUrlNames(Prog_sk);
		System.out.print(splitter_UName.trim()+"#<>#");
		
		
//////////Program_Type///////////////////////
		System.out.print("movie"+"#<>#");
		
		
//////////Crew_SK///////////////////////
		
		msd.MD5(Crew_sk.trim());
		
		System.out.print(msd.md5s.trim()+"#<>#");
		
		
		
//////////Role///////////////////////
		SplitConvertNames(Role);
		System.out.print(Rname.trim()+"#<>#");
		
		//System.out.print(Role.trim()+"#<>#");
		
		
//////////Description///////////////////////
		System.out.print("#<>#");
		
		
		
//////////Role_Title///////////////////////
		System.out.print("#<>#");
		
		
//////////Rank///////////////////////
		System.out.print(inc+"#<>#");
		
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
		String[] split=name.split("\\/");
		splitter_UName=split[split.length - 1];
		
		//System.out.println(splitter_UName);
	}
	
	
	public void SplitConvertNames(String Cnames)
	{
		if(Cnames.equals("Chorégraphie"))
		{
			Rname="choreography";
		}
		else if(Cnames.equals("Musique"))
		{
			Rname="music";
		}
		else if(Cnames.equals("Direction Musicale")||Cnames.equals("Direction musicale"))
		{
			Rname="musical direction";			
			
		}
		else if(Cnames.equals("Mise en scène"))
		{
			Rname="director";
		}
		else if(Cnames.equals("Distribution"))
		{
			Rname="actor";
		}
		
		else if(Cnames.equals("Auteur"))
		{
			Rname="author";
		}
		
		else if(Cnames.equals("Compositeur"))
		{
			Rname="composer";
		}
		
		
		
	}
	

}
