package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class MapleClient {
	
	private String executable;
	private String intermediateFilePrefix;
	private List<String> sdfsSourceFiles;
	private String MAPLE_SDFS_KEY;
	
	public MapleClient()
	{
	}
	
	
	public void maple(String[] commands)
	{
		
		executable = commands[1];
		intermediateFilePrefix = commands[2];
		sdfsSourceFiles = Arrays.asList(commands).subList(3, commands.length);
		MAPLE_SDFS_KEY = executable;
		putExecutableInSdfs();
		distributeMapleJobs();
	}
	
	/**
	 * Places the executable into SDFS so other nodes can access it locally when
	 * they run it. Each node will just rename it to maple.jar so they can
	 * access the main class.
	 */
	private void putExecutableInSdfs() {
		Application.getInstance().dfsClient.put(executable, MAPLE_SDFS_KEY);
	}
	
	/**
	 * Evenly divide maple task + SDFS input files across nodes.
	 * 
	 * wheremaple:intermediate_file_prefix:input_file1;input_file2;input_file3
	 */
	private void distributeMapleJobs() {
		try {
			Socket clientSocket = new Socket(Application.getInstance().group.getLeader().getIP(), Application.TCP_MAPLE_PORT);
			
            // Setup our input and output streams
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            String sdfsString = "";
            for(int i=0;i<sdfsSourceFiles.size();i++)
            	sdfsString += sdfsSourceFiles.get(i)+":";
            
            out.println("wheremaple:"+intermediateFilePrefix+":"+sdfsString);
            String input = "";
            while(!(input = in.readLine()).equals("<END>"))
            {
            	delegateMapleTask(input);
            }
            
            in.close();
            out.close();
            clientSocket.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * report back to the leader node that a Maple task has completed.
	 * 
	 * Message is sent using format   MAPLEDONE:<intermediate_file_prefix>:<sdfs_source_file>
	 * 
	 * @param intermediateFilePrefix
	 * @param sdfsSourceFile
	 */
	public void sendMapleDone(String intermediateFilePrefix, String sdfsSourceFile) {
		String mapleDoneMessage = "mapledone:"+intermediateFilePrefix+":"+sdfsSourceFile;
		sendMessage(Application.getInstance().group.getLeader().getIP(),mapleDoneMessage);
	}
	
	/**
	 * report back to the original node that a Maple job is fully complete
	 * 
	 * Message is sent using format   MAPLECOMPLETE:<intermediate_file_prefix>
	 * 
	 * @param IP
	 * @param intermediateFilePrefix
	 */
	public void sendMapleComplete(String intermediateFilePrefix) {
		String mapleCompleteMessage = "maplecomplete:"+intermediateFilePrefix;
		sendMessage(Application.getInstance().mapleJuiceServer.originatorIP,mapleCompleteMessage);
	}



	/**
	 * Delegate maple command to a node
	 * 
	 * @param nodeToRunOn
	 * @param sdfsSourceFile
	 */
	private void delegateMapleTask(String input) {
		String[] inputCommands = input.split(":");
		String mapleCommand = "maple:" + MAPLE_SDFS_KEY + ":"
				+ intermediateFilePrefix + ":" + inputCommands[1];
		System.out.println("maple: "+mapleCommand);
		sendMessage(inputCommands[0], mapleCommand);
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
