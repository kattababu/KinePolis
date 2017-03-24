/**
 * 
 */
package com.Nutch.Crawl.KinePDaily;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author surendra
 *
 */
public class KinePolisJsonShowData {

	/**
	 * 
	 */
	public KinePolisJsonShowData() {
		// TODO Auto-generated constructor stub
	}
	
	HTable ht;
	Scan sc;
	ResultScanner rescan;
	String rownames=null,family=null,qualifier=null,rownames1=null;
	

}
