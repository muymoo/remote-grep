package org.uiuc.cs.distributed.grep;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Recieves maple input from other nodes
 * 
 * @author matt
 * 
 */
public class JuiceCollectorThread extends Thread {
	private String intermediateFilePrefix;
	private ArrayList<String> sdfsSourceFiles;
	private String destinationFileName;

	public JuiceCollectorThread(String _intermediateFilePrefix,
			ArrayList<String> _sdfsSourceFiles, String _destinationFileName) {
		super("JuiceCollectorThread");
		this.intermediateFilePrefix = _intermediateFilePrefix;
		this.sdfsSourceFiles = _sdfsSourceFiles;
		this.destinationFileName = _destinationFileName;
	}

	@Override
	public void run() {
		System.out.println("JuiceCollectorThread starting");
		getAllCompletedFiles();
		processAllCompletedFiles();

	}

	private String getCompletedFileName(String prefix, String source_file) {
		return prefix + "_DESTINATION_" + source_file;
	}

	private String getLocalDestinationFileName(String prefix, String key_name) {
		return Application.SCRATCH_DIR + File.separator + prefix + "_"
				+ key_name + ".juicekey";
	}

	public void getAllCompletedFiles() {
		for (int i = 0; i < this.sdfsSourceFiles.size(); i++) {
			String currentFile = getCompletedFileName(
					this.intermediateFilePrefix, this.sdfsSourceFiles.get(i));
			if (!Application.getInstance().dfsClient.hasFile(currentFile)) {
				String localFileName = Application.getInstance().dfsClient
						.generateNewFileName("file.scratch");
				Application.getInstance().dfsClient.get(currentFile,
						localFileName);
			}
		}
	}

	public void processAllCompletedFiles() {
		// if file doesnt exists, then create it
		String localDestinationFile = getLocalDestinationFileName(
				intermediateFilePrefix, destinationFileName);
		File destinationFile = new File(localDestinationFile);
		if (!destinationFile.exists()) {
			try {
				destinationFile.createNewFile();
			} catch (IOException e) {
				System.out
						.println("Could not create destination file. Do you have permission?");
				e.printStackTrace();
				return;
			}
		}

		// We're going to append to this file
		FileWriter fw;
		try {
			fw = new FileWriter(destinationFile.getAbsoluteFile(), true);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			System.out
					.println("Could not create a file writer. Does this file exist?");
			e2.printStackTrace();
			return;

		}
		BufferedWriter bw = new BufferedWriter(fw);
		for (int i = 0; i < this.sdfsSourceFiles.size(); i++) {
			String sdfsCompletedFile = getCompletedFileName(
					this.intermediateFilePrefix, this.sdfsSourceFiles.get(i));
			String currentFile = Application.getInstance().dfsClient
					.getFileLocation(sdfsCompletedFile);

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
					String currentLine = fileScanner.nextLine();
					// Append the line to the output file
					bw.write(currentLine);
					bw.newLine();
				}
				fileScanner.close();
			} catch (IOException e1) {
				System.out.println("Could not read intermediate file.");
				e1.printStackTrace();
			}
		}
		try {
			bw.close();
		} catch (IOException e) {
			System.out.println("Could not close writer.");
			e.printStackTrace();
		}

		Application.getInstance().dfsClient.put(localDestinationFile,
				destinationFileName);

		Application.getInstance().juiceClient
				.sendJuiceComplete(intermediateFilePrefix);

		Application.getInstance().mapleJuiceServer.resetJobLists();
	}
}
