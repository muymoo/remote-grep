package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

/**
 * Recieves maple input from other nodes
 * 
 * @author matt
 * 
 */
public class MapleCollectorThread extends Thread {
	private String intermediateFilePrefix;
	private ArrayList<String> sdfsSourceFiles;
	
	public MapleCollectorThread(String _intermediateFilePrefix, ArrayList<String> _sdfsSourceFiles) {
		super("MapleCollectorThread");
		this.intermediateFilePrefix = _intermediateFilePrefix;
		this.sdfsSourceFiles = _sdfsSourceFiles;
	}

	@Override
	public void run() {
		getAllCompletedFiles();
		processAllCompletedFiles();
		
	}
	
	private String getCompletedFileName(String prefix, String source_file)
	{
		return prefix+"_OUTPUT_"+source_file;
	}
	
	private String getLocalKeyFileName(String prefix, String key_name)
	{
		return Application.SCRATCH_DIR+File.separator+prefix+"_"+key_name+".key";
	}
	
	private String extractKeyFromLocalKeyFileName(String keyFileName)
	{
		String[] nameParts = keyFileName.split("_");
		String lastPart = nameParts[nameParts.length - 1];
		String[] lastParts = lastPart.split("[.]");
		return lastParts[0];
	}
	
	public void getAllCompletedFiles()
	{
		for(int i=0;i<this.sdfsSourceFiles.size();i++)
		{
			String currentFile = getCompletedFileName(this.intermediateFilePrefix,this.sdfsSourceFiles.get(i));
			if(!Application.getInstance().dfsClient.hasFile(currentFile))
			{
				String localFileName = Application.getInstance().dfsClient.generateNewFileName("file.scratch");
				Application.getInstance().dfsClient.get(currentFile,localFileName);
			}
		}
	}
	
	public void processAllCompletedFiles()
	{
		Set<String> keyFileNames = new HashSet<String>(); 
		
		for(int i=0;i<this.sdfsSourceFiles.size();i++)
		{
			String sdfsCompletedTask = getCompletedFileName(this.intermediateFilePrefix,this.sdfsSourceFiles.get(i));
			String currentFile = Application.getInstance().dfsClient.getFileLocation(sdfsCompletedTask);
			
			FileWriter fw;

			try {
				// Open up file to read in words
				Scanner fileScanner = null;
				try {
					fileScanner = new Scanner(new File(currentFile));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

				// Loop through each line
				while (fileScanner.hasNextLine()) {
					// In each line, loop through each word. Scanner breaks on \s by
					// default
					String currentLine = fileScanner.nextLine();
					Scanner lineScanner = new Scanner(currentLine);
					
					String wordLine = lineScanner.next();
					String word = wordLine.split(":")[0];
					
					lineScanner.close();
					
					
					String keyFileName = getLocalKeyFileName(this.intermediateFilePrefix, word);
					
					
					// if file doesnt exists, then create it
					File keyFile = new File(keyFileName);
					if (!keyFile.exists()) {
						keyFile.createNewFile();
						keyFileNames.add(keyFileName);
					}
					
					// We're going to write to this file
					fw = new FileWriter(keyFile.getAbsoluteFile(),true);
					BufferedWriter bw = new BufferedWriter(fw);
					bw.write(currentLine);
					bw.newLine();
					bw.close();
				}
				fileScanner.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		
		// put all the new key files in sdfs
		Iterator<String> keyFileNameIterator = keyFileNames.iterator();
		while(keyFileNameIterator.hasNext())
		{
			String currName = keyFileNameIterator.next();
			Application.getInstance().dfsClient.put(currName, this.intermediateFilePrefix+"_"+extractKeyFromLocalKeyFileName(currName));
		}
		
		// send original client maplecomplete
		Application.getInstance().mapleJuiceClient.sendMapleComplete(this.intermediateFilePrefix);
	}
}
