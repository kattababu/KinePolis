
/**
 * 
 */
package com.Nutch.Crawl.KineP;





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
	String rownames=null,family=null,qualifier=null,rownames1=null;
	
	
	//@SuppressWarnings("deprecation")
	public void KinePolisCR()
	{
		try
		{
			
			
			
			
			Configuration config=HBaseConfiguration.create();
			
			
			ht=new HTable(config,"kinepolies_webpage");
		sc=new Scan();
		
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
												
						
							
							if(rownames.contains("/films/") ||rownames.contains("/evenements/"))
							{
								/*
								
								
								if(rownames.contains("/evenements/"))
								{
									
									System.out.println(rownames);
									//new KinePolisMVNCT().KinePolisCNT(rownames);	
									new  KinePolisCrewWOL().KinePolisCrewWOLNT(rownames);
									
									
									
									
									}
								
								
								*/
								
								
								
					
							
								c=c+1;
							if (c>0 && c<=250)
								{
								
								
							
								if(rownames.endsWith("/marathons-0"))
								{
									break;
								}
								else 
									
								{
									new KinePolisMVNCT().KinePolisCNT(rownames);								
									new KinePolisRMCNT().KinePolisRNT(rownames);
									new KinePolisPRCNT().KinePolisPNT(rownames);
									new KinePolisRateCNT().KinePolisRatNT(rownames);
									new KinePolisCrewCNT().KinePolisCrewNT(rownames);
									new KinePolisPCDCT().KinePolisCrewPrgNT(rownames);
									new KinePolisPCACT().KinePolisCrewPANT(rownames);
									new KinePolisPCAICNT().KinePolisCrewPrgACTNT(rownames);
									new KinePolisCrewAwardsCNT().KinePolisCrewActAwdCNT(rownames);
									new KinePolisRelatedPG().KinePolisRPRGCNT(rownames);
									new  KinePolisCrewWOL().KinePolisCrewWOLNT(rownames);
									
								}
								}
								
							
								
							
							/*
							if (c>500 && c<=1000)
							{
								
								//System.out.println(rownames);
								
								new KinePolisMVNCT().KinePolisCNT(rownames);
							new KinePolisRMCNT().KinePolisRNT(rownames);
								new KinePolisPRCNT().KinePolisPNT(rownames);
								new KinePolisRateCNT().KinePolisRatNT(rownames);
								new KinePolisCrewCNT().KinePolisCrewNT(rownames);
								new KinePolisPCDCT().KinePolisCrewPrgNT(rownames);
								new KinePolisPCACT().KinePolisCrewPANT(rownames);
								new KinePolisPCAICNT().KinePolisCrewPrgACTNT(rownames);
								new KinePolisCrewAwardsCNT().KinePolisCrewActAwdCNT(rownames);
								


										
							}
							*/
						
								
							}
							
							
							
						}
						
						
						
						
						
						
						
					}
					
			
					
					
					
						/*

							if(rownames.endsWith("/infos"))
							{
								if(family.equals("f") && qualifier.equals("cnt"))
								{
								
								System.out.println(rownames);
								}
									
						
								
								
								
							//System.out.println(rownames);
						
							
						}
					*/
					
					
				
					
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
	
	//////////////////////////////////////////////////////////////RM/////////////////////////////
	
	
		

}
