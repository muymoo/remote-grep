package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * This grep task class is used to handle the client side grep request. It will
 * connect to a grep server and send a regex to run on the server. It will then
 * print out the grep results returned by the server
 * 
 * @author matt
 */
public class GrepTask extends Thread {
	private String IP;
	private int port;
	private String regex;

	public GrepTask(String ipAddress, int port, String regex) {
		this.IP = ipAddress;
		this.port = port;
		this.regex = regex;
	}

	/**
	 * Runs the client side grep commands. This function calls the remote grep
	 * server for this task with a regex and then prints off the result to the
	 * console.
	 */
	public void run() {
		Socket grepSocket = null;
		PrintWriter out = null;
		BufferedReader in = null;

		System.out.println("grep task accessing: " + IP + ":" + port);

		try {
			// Open socket to grep server
			grepSocket = new Socket(IP, port);

			// Setup our input and output streams
			out = new PrintWriter(grepSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(
					grepSocket.getInputStream()));

		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: " + IP);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: " + IP);
			System.exit(1);
		}

		String fromServer;
		String grepResult = "Grep Result: ";

		try {
			// Send regular expression to grep server
			out.println(this.regex);

			// Read grep results from server
			while ((fromServer = in.readLine()) != null) {
				grepResult += fromServer; // Store grep result
				break;
			}

			// Print results all at once so they are not mixed with other
			// threads
			System.out.println(grepResult);

			// Clean up socket
			out.close();
			in.close();
			grepSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
