/**
 * 
 */
package com.Nutch.Crawl.KineP;

/**
 * @author surendra
 *
 */
public class KinePolisMain {

	/**
	 * 
	 */
	public KinePolisMain() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws Throwable 
	 */
	public static void main(String[] args)  {
		// TODO Auto-generated method stub
		
	//KinePolisTheater kpt=new KinePolisTheater();
	//kpt.KinePolisCMH();

		new KinePolisTheater().KinePolisCMH();
		
		KinePolisCNRows kpcr=new KinePolisCNRows();
		kpcr.KinePolisCR();
		
		//KinePolisJsonShowData kpjsd=new KinePolisJsonShowData();
		//kpjsd.KinePolisJSD();
		
		
		//new KinePolisJsonShowData().KinePolisJSD();
	
		
	SplitFiles.FileSplitS();
		
		
	}
	
	
	
	
}

