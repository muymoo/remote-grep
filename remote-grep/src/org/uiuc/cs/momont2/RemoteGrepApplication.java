package org.uiuc.cs.momont2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RemoteGrepApplication {

	public void prompt() {
		System.out.print(">>");
	}

	public void readInput() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String grepCommand = null;
		
		try {
			grepCommand = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		RemoteGrep remoteGrep = new RemoteGrep();
		System.out.print(remoteGrep.grep(grepCommand));
	}
}
