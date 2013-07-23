package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * MapleMaster delegates the maple commands to all nodes. This should only run
 * on the master/leader node.
 * 
 * @author matt
 * 
 */
public class MapleMaster extends Thread {
	private String executable;
	private String intermediateFilePrefix;
	private List<String> sdfsSourceFiles;
	private Application app;
	private final String MAPLE_SDFS_KEY = "maple_exe";

	/**
	 * Expects command line input per MP's requirement.
	 * 
	 * @param commands
	 *            Takes maple maple_exe intermediate_file_prefix sdfs_file_1...n
	 */
	public MapleMaster(String[] commands) {
		super("MapleServerThread");
		executable = commands[1];
		intermediateFilePrefix = commands[2];
		sdfsSourceFiles = Arrays.asList(commands).subList(3, commands.length);
		app = Application.getInstance();
	}

	@Override
	public void run() {
		putExecutableInSdfs();
		distributeMapleJobs();
		collectMapleResponses();

		// We can then loop through all intermediate prefixes in the global file
		// map! :) to find output files for juicing.
		System.out.println("Maple complete.");
	}

	/**
	 * Evenly divide maple task + SDFS input files across nodes.
	 */
	private void distributeMapleJobs() {
		synchronized (app.group.list) {
			int numberOfNodes = Application.getInstance().group.list.size();
			int index = 0;

			for (String sdfsSourceFile : sdfsSourceFiles) {
				// Divide tasks evenly across nodes
				Node nodeToRunOn = Application.getInstance().group.list
						.get(index % numberOfNodes);

				// Send task to node
				delegateMapleTask(nodeToRunOn, sdfsSourceFile);

				index++;
			}
		}
	}

	/**
	 * Places the executable into SDFS so other nodes can access it locally when
	 * they run it. Each node will just rename it to maple.jar so they can
	 * access the main class.
	 */
	private void putExecutableInSdfs() {
		app.dfsClient.put(executable, MAPLE_SDFS_KEY);
	}

	/**
	 * Delegate maple command to a node
	 * 
	 * @param nodeToRunOn
	 * @param sdfsSourceFile
	 */
	private void delegateMapleTask(Node nodeToRunOn, String sdfsSourceFile) {
		sendMessage(nodeToRunOn, "maple:" + MAPLE_SDFS_KEY + ":"
				+ intermediateFilePrefix + ":" + sdfsSourceFile);
	}

	/**
	 * Send a message to a node
	 * 
	 * @param destinationNode
	 * @param message
	 */
	private void sendMessage(Node destinationNode, String message) {
		// Connect to node via TCP
		Socket clientSocket;
		try {
			// Connect to the destination node
			clientSocket = new Socket(destinationNode.getIP(),
					Application.TCP_MAPLE_PORT);
			// Setup our output stream
			PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
					true);
			// Send the message
			out.println(message);
			// Cleanup
			out.close();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Wait for all nodes to respond
	 */
	private void collectMapleResponses() {
		ServerSocket mapleResponseSocket;
		try {
			mapleResponseSocket = new ServerSocket(Application.TCP_MAPLE_PORT);

			Socket clientSocket = mapleResponseSocket.accept();

			BufferedReader in = new BufferedReader(new InputStreamReader(
					clientSocket.getInputStream()));

			String inputLine;

			while ((inputLine = in.readLine()) != "<END>") {
				System.out.println("Maple received: " + inputLine);
			}

			in.close();
			clientSocket.close();
			mapleResponseSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
