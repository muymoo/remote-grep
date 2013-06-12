package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The grep server listens for regular expressions (regex) coming into port 4444
 * and returns the results of the grep command using the given regex on the
 * machine.1.log file.
 * 
 * @author matt
 * 
 */
public class GrepServer extends Thread {
	private Socket socket = null;

	public GrepServer() {
		super("GrepServerThread");
	}

	public void run() {
		try {
			try {
				// Start listening on port 4444
				this.socket = new ServerSocket(4444).accept();
			} catch (IOException e) {
				System.err.println("Could not listen on port: 4444.");
				System.exit(-1);
			}

			// Setup our input/output streams
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			String inputLine, outputLine;
			Grep grep = new Grep();

			// Loop until grep doesn't return any more results
			while ((inputLine = in.readLine()) != null) {

				// Run grep after receiving a regex from the client
				outputLine = grep.search(inputLine);

				// Send results back to client
				out.println(outputLine);
			}

			out.close();
			in.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
