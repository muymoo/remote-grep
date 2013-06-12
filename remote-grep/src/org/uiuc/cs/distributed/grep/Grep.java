package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class Grep {

	public Grep() {
		createDummyLogFiles();
	}

	/**
	 * Creates dummy log files to test with
	 */
	private void createDummyLogFiles() {
		String logLine = "14:53 [ERROR] Cannot read machine code.";
		for (int i = 0; i < 5; i++) {
			File dummyLogFile = new File("/tmp/machine." + i + ".log");
			try {
				Files.write(dummyLogFile.toPath(), logLine.getBytes(),
						StandardOpenOption.CREATE);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Runs grep on /tmp/machine.1.log
	 * 
	 * @param regex
	 *            - Regular expression to search with
	 * @return The results of the grep command
	 */
	public String search(String regex) {
		Process process = null;
		String result = "";
		try {
			process = new ProcessBuilder("grep", "-rni", regex,
					"/tmp/machine.1.log").start();
			process.waitFor();

			BufferedReader br = new BufferedReader(new InputStreamReader(
					process.getInputStream()));
			String line = "";
			while ((line = br.readLine()) != null) {
				result += line;
			}
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}

		return result;
	}
}
