package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Runs maple executable locally
 * 
 * @author matt
 * 
 */
public class MapleTask extends Thread {
	private MapleJuiceNode mapleJuiceNode;

	public MapleTask(MapleJuiceNode _mapleJuiceNode) {
		this.mapleJuiceNode = _mapleJuiceNode;
	}

	@Override
	public void run() {
		getMapleExecutable();
		String localSourceFilePath = getSdfsSourceFile();
		executeMaple(localSourceFilePath);
		/*
		String localSourceFilePath = getSdfsSourceFile();
		BufferedReader in = executeMaple(localSourceFilePath);

		// Read input from maple's output stream
		String inputLine;
		try {
			while ((inputLine = in.readLine()) != null) {
				System.out.println("Found key:value: " + inputLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		// TODO: Put output in intermediate file to sdfs
	}

	/**
	 * Pulls down the maple executable from SDFS and renames it to maple.jar. It
	 * is stored in the current directory so it can be run from the program.
	 */
	private void getMapleExecutable() {
		if(!Application.getInstance().dfsClient.hasFile(mapleJuiceNode.mapleExecutableSdfsKey))
		{
			Application.getInstance().dfsClient.get(mapleJuiceNode.mapleExecutableSdfsKey,
				Application.getInstance().dfsClient.generateNewFileName(mapleJuiceNode.mapleExecutableSdfsKey));
		}
	}

	/**
	 * Pull down source file into scratch directory. This is the file we will
	 * run maple.jar on.
	 * 
	 * @return Local file path to the source file
	 */
	private String getSdfsSourceFile() {
		if(!Application.getInstance().dfsClient.hasFile(mapleJuiceNode.sdfsSourceFile))
		{
			String localFileName = Application.getInstance().dfsClient.generateNewFileName("file.scratch");
			Application.getInstance().dfsClient.get(mapleJuiceNode.sdfsSourceFile, localFileName);
		}
		return Application.getInstance().dfsClient.getFileLocation(mapleJuiceNode.sdfsSourceFile);
	}

	/**
	 * Runs the maple.jar command on the given local file
	 * 
	 * @param localSourceFilePath
	 *            File path to input file
	 * @return Stream to read output of maple.jar from (usually for capturing
	 *         the key:value pairs)
	 */
	private void executeMaple(String localSourceFilePath) {
		//BufferedReader in = null;
		String outputFileName = Application.getInstance().dfsClient.generateNewFileName("file.scratch");
		System.out.println("Executing maple task: "+"java -jar " + mapleJuiceNode.mapleExe + " " + localSourceFilePath+" "+outputFileName);
		try {
			Process p = Runtime.getRuntime()
					.exec("java -jar " + mapleJuiceNode.mapleExe + " " + localSourceFilePath+" "+outputFileName)
					;
			p.waitFor();
			Application.getInstance().dfsClient.put(outputFileName, mapleJuiceNode.intermediateFilePrefix+"_OUTPUT_"+mapleJuiceNode.sdfsSourceFile);
			Application.getInstance().mapleJuiceClient.sendMapleDone(mapleJuiceNode.intermediateFilePrefix, mapleJuiceNode.sdfsSourceFile);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Maple task done.");
		//	in = new BufferedReader(new InputStreamReader();
		//} catch (IOException e) {
		//	e.printStackTrace();
		//}
		return;
	}
}
