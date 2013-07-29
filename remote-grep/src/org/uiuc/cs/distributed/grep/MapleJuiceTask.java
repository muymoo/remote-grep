package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.util.Random;

/**
 * Runs maple executable locally
 * 
 * @author matt
 * 
 */
public class MapleJuiceTask implements Runnable {
	private MapleJuiceNode mapleJuiceNode;
	private String outputSdfsKey;
	private String type;

	public MapleJuiceTask(String type, MapleJuiceNode _mapleJuiceNode,
			String _outputSdfsKey) {
		this.mapleJuiceNode = _mapleJuiceNode;
		this.outputSdfsKey = _outputSdfsKey;
		this.type = type;
	}

	@Override
	public void run() {
		String exe = getExecutable();
		String localSourceFilePath = getSdfsSourceFile();
		execute(exe, localSourceFilePath);
	}

	/**
	 * Pulls down the executable from SDFS. It is stored in the current
	 * directory so it can be run from the program.
	 */
	private String getExecutable() {
		if (!Application.getInstance().dfsClient
				.hasFile(mapleJuiceNode.executableSdfsKey)) {
			System.out.println("Does not contain the executable - getting it.");
			String localFileName = Application.getInstance().dfsClient
					.generateNewFileName("file.jar");
			Application.getInstance().dfsClient
					.get(mapleJuiceNode.executableSdfsKey,localFileName);
			
		}
		String location = Application.getInstance().dfsClient
				.getFileLocation(mapleJuiceNode.executableSdfsKey);
		System.out.println("MapleJuiceTask - getExecutable - location: "+location);
		return location;
	}

	/**
	 * Pull down source file into scratch directory. This is the file we will
	 * run maple.jar on.
	 * 
	 * @return Local file path to the source file
	 */
	private String getSdfsSourceFile() {
		if (!Application.getInstance().dfsClient
				.hasFile(mapleJuiceNode.sdfsSourceFile)) {
			String localFileName = Application.getInstance().dfsClient
					.generateNewFileName("file.scratch");
			Application.getInstance().dfsClient.get(
					mapleJuiceNode.sdfsSourceFile, localFileName);
		}
		return Application.getInstance().dfsClient
				.getFileLocation(mapleJuiceNode.sdfsSourceFile);
	}

	/**
	 * Runs the maple.jar command on the given local file
	 * 
	 * @param localSourceFilePath
	 *            File path to input file
	 * @return Stream to read output of maple.jar from (usually for capturing
	 *         the key:value pairs)
	 */
	private void execute(String localExe, String localSourceFilePath) {
		// BufferedReader in = null;
		if(localExe == null)
		{
			System.out.println("ERROR - MapleJuiceTask - execute() - localExe is null");
			Application.LOGGER.warning("ERROR - MapleJuiceTask - execute() - localExe is null");
		}
		String outputFileName = Application.getInstance().dfsClient
				.generateNewFileName("file.scratch");
		System.out.println("Executing task: " + "java -jar " + localExe + " "
				+ localSourceFilePath + " " + outputFileName);
		try {
			Process p = Runtime.getRuntime().exec(
					"java -jar " + localExe + " " + localSourceFilePath + " "
							+ outputFileName);
			p.waitFor();

			
			// sleep while an election is in progress
			boolean isElection = Application.getInstance().group.electionInProgress;
			while(isElection)
			{
				Thread.sleep(5000);
				isElection = Application.getInstance().group.electionInProgress;
			}
			
			System.out.println("Putting after execute: " + outputFileName
					+ " key: " + outputSdfsKey);
			
			try {
				Random rand = new Random();
				int max = 5;
				int min = 1;
				int randomNum = rand.nextInt(max - min + 1) + min;
				Thread.sleep(randomNum * 100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Application.getInstance().dfsClient.put(outputFileName,
					outputSdfsKey);
			if (type.equals("maple")) {
				Application.getInstance().mapleClient.sendMapleDone(
						mapleJuiceNode.intermediateFilePrefix,
						mapleJuiceNode.sdfsSourceFile);
			} else {
				Application.getInstance().juiceClient.sendJuiceDone(
						mapleJuiceNode.intermediateFilePrefix,
						mapleJuiceNode.sdfsSourceFile);
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			System.out.println("Execution interrupted");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Could not find file.");
			e.printStackTrace();
		}
		return;
	}
}
