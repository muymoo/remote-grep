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
	private String mapleExecutableSdfsKey;
	private String intermediateFilePrefix;
	private String sdfsSourceFile;

	private final String MAPLE_EXE = "maple.jar";
	private final String SCRATCH_DIR = "/tmp/momont2/scratch/";

	public MapleTask(String mapleExecutableSdfsKey,
			String intermediateFilePrefix, String sdfsSourceFile) {
		this.mapleExecutableSdfsKey = mapleExecutableSdfsKey;
		this.intermediateFilePrefix = intermediateFilePrefix;
		this.sdfsSourceFile = sdfsSourceFile;
	}

	@Override
	public void run() {
		getMapleExecutable();
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
		}
		
		// TODO: Put output in intermediate file to sdfs
	}

	/**
	 * Pulls down the maple executable from SDFS and renames it to maple.jar. It
	 * is stored in the current directory so it can be run from the program.
	 */
	private void getMapleExecutable() {
		Application.getInstance().dfsClient.get(mapleExecutableSdfsKey,
				MAPLE_EXE);
	}

	/**
	 * Pull down source file into scratch directory. This is the file we will
	 * run maple.jar on.
	 * 
	 * @return Local file path to the source file
	 */
	private String getSdfsSourceFile() {
		String localSourceFilePath = SCRATCH_DIR + System.currentTimeMillis()
				+ ".txt";
		Application.getInstance().dfsClient.get(sdfsSourceFile,
				localSourceFilePath);
		return localSourceFilePath;
	}

	/**
	 * Runs the maple.jar command on the given local file
	 * 
	 * @param localSourceFilePath
	 *            File path to input file
	 * @return Stream to read output of maple.jar from (usually for capturing
	 *         the key:value pairs)
	 */
	private BufferedReader executeMaple(String localSourceFilePath) {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(Runtime.getRuntime()
					.exec("java -jar " + MAPLE_EXE + " " + localSourceFilePath)
					.getInputStream()));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return in;
	}
}
