package org.uiuc.cs.momont2;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class RemoteGrep {

	public RemoteGrep() {
		createDummyLogFile();
	}

	private void createDummyLogFile() {
		String logLine = "14:53 [ERROR] Cannot read machine code.";
		File dummyLogFile = new File("/tmp/machine.1.log");
		try {
			Files.write(dummyLogFile.toPath(), logLine.getBytes(),
					StandardOpenOption.CREATE);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteDummyFiles() {
		try {
			Files.delete(new File("/tmp/machine.1.log").toPath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String grep(String command) {
		Process process = null;
		String result = "";
		try {
			process = new ProcessBuilder("grep", "-rni", "[ERROR]",
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
