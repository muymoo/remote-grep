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
	private Node node;
	private String regex;
	private String result;

	public GrepTask(Node _node) {
		this.node = _node;
	}
	
	public void setRegex(String _regex) {
		this.regex = _regex;
	}
	
	public String getResult() {
		return this.result;
	}

	/**
	 * Runs the client side grep commands. This function calls the remote grep
	 * server for this task with a regex and then prints off the result to the
	 * console.
	 */
	public void run() {
		Socket clientSocket = null;
		PrintWriter out = null;
		BufferedReader in = null;

		System.out.println("grep task accessing: " + this.node.toString());

		try {
			clientSocket = new Socket(this.node.getIP(), this.node.getPort());

			// Setup our input and output streams
			out = new PrintWriter(clientSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));

		} catch (UnknownHostException e) {
			System.err.println("Don't know about host: " + this.node.getIP());
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to: " + this.node.getIP());
			System.exit(1);
		}

		String fromServer;

		try {
			// Send regular expression to grep server
			out.println(this.regex);
			this.result = "";

			// Read grep results from server
			while ((fromServer = in.readLine()) != null) {
				this.result += fromServer; // Store grep result
				break;
			}

			// Clean up socket
			out.close();
			in.close();
			clientSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
