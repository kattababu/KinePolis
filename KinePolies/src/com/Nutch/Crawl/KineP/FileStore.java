/**
 * 
 */
package com.Nutch.Crawl.KineP;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
//import java.util.Scanner;

/**
 * @author surendra
 *
 */
public class FileStore {

	/**
	 * 
	 */
	public FileStore() {
		// TODO Auto-generated constructor stub
	}
	
	
	final  static String filePath ="/katta/KinePole";
	final  static String filename="KinePolis_Terminal_";
	static File fileM=null;
	static File fileC=null;
	static File filePC=null;
	static File fileRM=null;
	static File filePR=null;
	static File fileTvshow=null;
	static File fileTvshowEps=null;
	//static int count=0;
	/*
	final static String movietable="movie_";
	final static String crewtable="crew_";
	final static String programcrew="programcrew_";
	final static String richmediatable="richmedia_";
	final static String programrelease="release_";
	
	
	public static String GetCurrentTimeStamp1() {
        SimpleDateFormat sdfDate = new SimpleDateFormat(
                "yyyyMMdd'T'HHmmssSSSSS");// dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }
	 */
	
	
	public static void MovieTable(String table) {
        //get current project path
      
        //create a new file with Time Stamp
        fileM = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");
        
       
        try {
        	  if (!fileM.exists()) {
                  fileM.createNewFile();
                  System.out.println("File is created; file name is " + fileM.getName());
                  
                  
                   
                  
                 
              } else {
            	  
            	    System.out.println("File already exist");
              }
        	  
         
        	
        	 
           } catch (IOException e) {
            e.printStackTrace();
        }
    }
	
	//////////////////////////////////////////////////  TVSHOWS//////////////////////////////
	
	
	public static void TVShowTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTvshow = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTvshow.exists()) {
                fileTvshow.createNewFile();
                System.out.println("File is created; file name is " + fileTvshow.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	//////////////////////////////////////////////////  EPSIODES////////////////////////////
	
	public static void EpsiodeTable(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileTvshowEps = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileTvshowEps.exists()) {
                fileTvshowEps.createNewFile();
                System.out.println("File is created; file name is " + fileTvshowEps.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
	/////////////////////////////////////////////////////////////////////// ProgramRelease///////////////
	
	
	public static void ProgramRelease(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        filePR = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!filePR.exists()) {
                filePR.createNewFile();
                System.out.println("File is created; file name is " + filePR.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	//////////////////////////////////////ProgramCrew//////////////////////////
	
	
	
	public static void ProgramCrew(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        filePC = new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!filePC.exists()) {
                filePC.createNewFile();
                System.out.println("File is created; file name is " + filePC.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
////////////////////////////////////////////////////////////////Crew///////////////////////////////
	
	public static void Crew(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileC= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileC.exists()) {
                fileC.createNewFile();
                System.out.println("File is created; file name is " + fileC.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	
	////////////////////////////////////////RichMedia//////////////////////////
	
	public static void RichMedia(String table) {
        //get current project path
       // String filePath = ;
        //create a new file with Time Stamp
        fileRM= new File(filePath + "/" + filename+table+"_"+GetCurrentTimeStamp().replace(":","").replace(".","")+".queries");

        try {
            if (!fileRM.exists()) {
                fileRM.createNewFile();
                System.out.println("File is created; file name is " + fileRM.getName());
            } else {
                System.out.println("File already exist");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




	
	
	
	
	
	
	
	
	
        // Get current system time
    public static String GetCurrentTimeStamp() {
        SimpleDateFormat sdfDate = new SimpleDateFormat(
                "yyyyMMdd'T'HH:mm:ss.SSSSS");// dd/MM/yyyy
        Date now = new Date();
        String strDate = sdfDate.format(now);
        return strDate;
    }
    
    
    
    /*
    // Get Current Host Name
    public static String GetCurrentTestHostName() throws UnknownHostException {
        InetAddress localMachine = InetAddress.getLocalHost();
        String hostName = localMachine.getHostName();
        return hostName;
    }

    // Get Current User Name
    public static String GetCurrentTestUserName() {
        return System.getProperty("user.name");
    }
    
*/
	 


}