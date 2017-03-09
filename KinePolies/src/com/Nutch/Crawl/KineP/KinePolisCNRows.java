
/**
 * 
 */
package com.Nutch.Crawl.KineP;



//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author surendra
 *
 */
public class KinePolisCNRows {

	/**
	 * 
	 */
	public KinePolisCNRows() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null;
	
	/*
	static FileOutputStream fos=null;
	static PrintStream ps=null;
	static File file=null;
	
	static 
	{
		
		file=new File("/katta/KinePole/Rows.txt");
	}
	
	
*/
	//@SuppressWarnings("deprecation")
	public void KinePolisCR()
	{
		try
		{
			
			//fos = new FileOutputStream(file,true);
			//ps = new PrintStream(fos);
			// System.setOut(ps);
			
			
			Configuration config=HBaseConfiguration.create();
			
			//HTablePool pool = new HTablePool(config, 1000);  
	         //ht = (HTable) pool.getTable("kinepolies_webpage");
			ht=new HTable(config,"kinepolies_webpage");
		sc=new Scan();
		//sc.setMaxVersions(1);
	    //sc.setCaching(50);
	    //sc.setBatch(100);
		rescan=ht.getScanner(sc);
	int c=0;
			
			for(Result res = rescan.next(); (res != null); res=rescan.next())
			{
				for(KeyValue kv:res.list())
				{
					
					rownames=Bytes.toString(kv.getRow());
					family=Bytes.toString(kv.getFamily());
					qualifier=Bytes.toString(kv.getQualifier());
					
					if(family.equals("ol"))
					{
						if(qualifier.equals("https://kinepolis.fr/"))
						{
												
							//System.out.println(rownames);
							
							if(rownames.contains("/films/") ||rownames.contains("/evenements/"))
							{
								c=c+1;
							//System.out.println(rownames);
								
							if (c>0 && c<=5)
								{
								
								new KinePolisRateCNT().KinePolisRatNT(rownames);
								//new KinePolisPRCNT().KinePolisPNT(rownames);
								//new KinePolisRMCNT().KinePolisRNT(rownames);
								//new KinePolisMVNCT().KinePolisCNT(rownames);
								}
								
							
							if (c>5 && c<=10)
							{
								
								new KinePolisRateCNT().KinePolisRatNT(rownames);
								//new KinePolisPRCNT().KinePolisPNT(rownames);
								//new KinePolisRMCNT().KinePolisRNT(rownames);
								//new KinePolisMVNCT().KinePolisCNT(rownames);
								
							}
						
								
							}
							
							
							
						}
						
						
						
						
						
						
						
					}
					
					/*
					
					if(family.equals("il"))
					{
					if(qualifier.equals("https://kinepolis.fr/prochainement"))
						{
											
						System.out.println(rownames);
						
						
						if(rownames.contains("/films/") ||rownames.contains("/evenements/"))
							{
								new KinePolisCNCT().KinePolisCNT(rownames);
							}
							
							
						}
						
						
						//System.out.println("\n\n\n\n");
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

}
