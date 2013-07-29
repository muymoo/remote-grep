package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class JuiceClient {
	
	private String executable;
	private String intermediateFilePrefix;
	private List<String> sdfsSourceFiles;
	private String JUICE_SDFS_KEY;
	private String numberOfJuices;
	private String destinationFile;
	
	public JuiceClient()
	{
	}
	
	
	public void juice(String[] commands)
	{
		
		executable = commands[1];
		numberOfJuices = commands[2];
		intermediateFilePrefix = commands[3];
		destinationFile = commands[4];
		sdfsSourceFiles = Arrays.asList(commands).subList(3, commands.length);
		JUICE_SDFS_KEY = executable;
		putExecutableInSdfs();
		distributeJuiceJobs();
	}
	
	/**
	 * Places the executable into SDFS so other nodes can access it locally when
	 * they run it.
	 */
	private void putExecutableInSdfs() {
		Application.getInstance().dfsClient.put(executable, JUICE_SDFS_KEY);
	}
	
	/**
	 * Evenly divide maple task + SDFS input files across nodes.
	 * 
	 * wherejuice:intermediate_file_prefix:num_juice:destination
	 */
	private void distributeJuiceJobs() {
		try {
			Socket clientSocket = new Socket(Application.getInstance().group.getLeader().getIP(), Application.TCP_MAPLE_PORT);
			
            // Setup our input and output streams
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            
            out.println("wherejuice:"+JUICE_SDFS_KEY+":"+intermediateFilePrefix+":"+numberOfJuices+":"+destinationFile);
            System.out.println("wherejuice:"+JUICE_SDFS_KEY+":"+intermediateFilePrefix+":"+numberOfJuices+":"+destinationFile);
            /*
            String input = "";
            while(!(input = in.readLine()).equals("<END>"))
            {
            	System.out.println("wherejuiceRESPONSE: "+input);
            	delegateJuiceTask(input);
            }*/
            
            in.close();
            out.close();
            clientSocket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * report back to the leader node that a Juice task has completed.
	 * 
	 * Message is sent using format   JUICEDONE:<intermediate_file_prefix>:<sdfs_source_file>
	 * 
	 * @param intermediateFilePrefix
	 * @param sdfsSourceFile
	 */
	public void sendJuiceDone(String intermediateFilePrefix, String sdfsSourceFile) {
		String juiceDoneMessage = "juicedone:"+intermediateFilePrefix+":"+sdfsSourceFile;
		sendMessage(Application.getInstance().group.getLeader().getIP(),juiceDoneMessage);
	}
	
	/**
	 * report back to the original node that a Juice job is fully complete
	 * 
	 * Message is sent using format   JUICECOMPLETE:<intermediate_file_prefix>
	 * 
	 * @param IP
	 * @param intermediateFilePrefix
	 */
	public void sendJuiceComplete(String intermediateFilePrefix) {
		String juiceCompleteMessage = "juicecomplete:"+intermediateFilePrefix;
		sendMessage(Application.getInstance().mapleJuiceServer.originatorIP,juiceCompleteMessage);
	}


	/**
	 * Send a message to a node
	 * 
	 * @param destinationNode
	 * @param message
	 */
	private void sendMessage(String IP, String message) {
		// Connect to node via TCP
		Socket clientSocket;
		try {
			// Connect to the destination node
			clientSocket = new Socket(IP,
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
}
