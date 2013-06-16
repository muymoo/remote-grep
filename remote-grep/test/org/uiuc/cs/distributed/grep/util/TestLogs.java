package org.uiuc.cs.distributed.grep.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.uiuc.cs.distributed.grep.RemoteGrepApplication;

public class TestLogs {
	private static final int numberOfLocalLogFiles = 1;
	private static final int numberOfLines = 100;
	private static final long numberOfMillisecondsPerDay = 86400000;
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		createLogFiles();
	}
	
	public static void createLogFiles() {
		try {
			FileWriter fileWriter = null;
			BufferedWriter bufferedWriter = null;
		
			for (int fileIndex = 0; fileIndex < numberOfLocalLogFiles; fileIndex++) {
				File dummyLogFile = new File(RemoteGrepApplication.logLocation+File.separator+"machine." + fileIndex + ".log");
				
				
				fileWriter = new FileWriter(dummyLogFile.getAbsoluteFile());
	
				
				BufferedWriter writer = new BufferedWriter(fileWriter);
				
				String logType = "";
				Date logDate = new Date();
				long time = 1362117600; // 1362114000 Mar 01,2013 0:0:0 CST    
				
				System.out.println("Time: "+logDate.getTime());
						
				for(int lineIndex = 0; lineIndex< numberOfLines;lineIndex++ )
				{
	                // adjust the logType and the day of the date based on the log number
	                if(fileIndex < numberOfLines/10) { // Rare
	                    logType = "SEVERE";
	                } else if( fileIndex >= numberOfLines/10 && fileIndex < numberOfLines/5) { // Infrequent
	                    logType = "WARNING";
	                } else if( fileIndex >= numberOfLines/5) { // Frequent
	                    logType = "INFO";
	                }
					logDate.setTime((time * 1000) + (fileIndex * numberOfMillisecondsPerDay)+(lineIndex * 60000));
					String line = logType + " " + logDate.toString() + " " + "Danger will robinson.\n";
					writer.write(line);
				}
	
				writer.close();
				fileWriter.close();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
