package org.uiuc.cs.distributed.grep.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.uiuc.cs.distributed.grep.RemoteGrepApplication;

public class TestLogs {
	private static final long numberOfMillisecondsPerDay = 86400000;
	
	// this property has the assumption of 1 char = 1 byte storage in text file, and 100 char lines of text
	private static final int numberOfLinesInMB = 10000;
			
	// this property has the assumption that each line is the same length: 63 chars
	// this creates predictable line lengths and predictable file size
	private static final String errorMessages[] = 
		{"To the dark, dark seas comes the only whale; Watching ships go\n",
		 "It's a Casio on a platic beach; It's a Casio on plastic beach;\n",
		 "It's a Sytrofoam deep sea landfill; It's a Styrofoam deep sea \n",
		};
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length > 1) {
			createLogFile(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
		} else {
			createLogFile(5, 10000);
		}
	}
	
	/*
	 * creates a log file named "machine.i.log" with i as the input, and is stored at the given log location
	 * all the dates/times within the logs start at March 01, 2013 CST
	 * 
	 * NOTES:
	 *  Frequent log lines   = 80%
	 *  Infrequent log lines = 19%
	 *  Rare log lines       = 1%
	 */
	public static void createLogFile(int fileIndex, int numberOfLines) {
		try {
			FileWriter fileWriter = null;
			BufferedWriter bufferedWriter = null;
		
			File dummyLogFile = new File(RemoteGrepApplication.logLocation+File.separator+"machine." + fileIndex + ".log");
			
			
			fileWriter = new FileWriter(dummyLogFile.getAbsoluteFile());

			
			bufferedWriter = new BufferedWriter(fileWriter);
			
			String logType = "";
			Date logDate = new Date();
			long time = 1362117600; // 1362114000 Mar 01,2013 0:0:0 CST    
			
			System.out.println("Time: "+logDate.getTime());
					
			for(int lineIndex = 0; lineIndex< numberOfLines;lineIndex++ )
			{
                // adjust the logType and the time based on the line number
                if(lineIndex < numberOfLines/100) { // Rare
                    logType = "SEVERE "; // padding necessary for consistent line length
                } else if( lineIndex >= numberOfLines/100 && lineIndex < (numberOfLines/5)) { // Infrequent
                    logType = "WARNING";
                } else if( lineIndex >= (numberOfLines/5)) { // Frequent
                    logType = "INFO   "; // padding necessary for consistent line length
                }
				logDate.setTime((time * 1000) + (lineIndex * 60000));
				
				// select a random error message
				int min = 0;
				int max = errorMessages.length;
				int errorMessageIndex = min + (int)(Math.random() * (max - min));
				String line = logType + " " + logDate.toString() + "-" + errorMessages[errorMessageIndex];
				bufferedWriter.write(line);
			}
			
			// Extra lines for testing
			bufferedWriter.write("There should be 2 of me.\n");
			bufferedWriter.write("There should be 2 of me.\n");
			
			bufferedWriter.close();
			fileWriter.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

}
