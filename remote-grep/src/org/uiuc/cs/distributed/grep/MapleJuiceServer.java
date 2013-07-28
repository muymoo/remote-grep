package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Set;

/**
 * MapleMaster delegates the maple commands to all nodes. This should only run
 * on the master/leader node.
 * 
 * @author matt
 * 
 */
public class MapleJuiceServer {
	/**
	 * key: intermediateFilePrefix value: sdfs_input_file
	 */
	protected static ArrayList<String> jobFilesLeft;
	protected static ArrayList<String> jobFilesCompleted;
	public String originatorIP;

	protected static ArrayList<MapleJuiceTask> mapleTaskThreads;
	private Thread producer;
	private Thread collector;
	protected ServerSocket mapleServerSocket;

	/**
	 * Expects command line input per MP's requirement.
	 * 
	 * @param commands
	 *            Takes maple maple_exe intermediate_file_prefix sdfs_file_1...n
	 */
	public MapleJuiceServer() {
		mapleTaskThreads = new ArrayList<MapleJuiceTask>();
		jobFilesLeft = new ArrayList<String>();
		jobFilesCompleted = new ArrayList<String>();
		try {
			mapleServerSocket = new ServerSocket(Application.TCP_MAPLE_PORT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		producer = new MapleJuiceListener();
	}

	public void start() {
		producer.start();
	}

	public void stop() {
		producer.stop();
	}

	public void resetJobLists() {
		jobFilesLeft.clear();
		jobFilesCompleted.clear();
	}

	/**
	 * Main thread for listening for maple/juice requests
	 * 
	 * @author de065366
	 * 
	 */
	public class MapleJuiceListener extends Thread {

		private String destinationFile;

		public MapleJuiceListener() {
			super("MapleServerThread");
		}

		@Override
		public void run() {
			System.out.println("MJServer started");
			while(true)
			{
				try {
		            Socket clientSocket = mapleServerSocket.accept();
		            
		            System.out.println("MJServer received connection");
		            // Setup our input and output streams
		            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		            
		            String inputLine = in.readLine();
		            
		            // make sure the input line is formatted correctly
		            if(inputLine.split(":").length < 2) {
		            	continue;
		            }
		            String command = inputLine.split(":")[0];
		            String[] commandParts = inputLine.split(":");
		            System.out.println("MJServer - command: "+command);
		            if(command.equals("wheremaple"))
		            {
		            	originatorIP = clientSocket.getInetAddress().getHostAddress();
		            	if(commandParts.length < 3)
		            	{
		            		out.println("<END>");
		            	}

			            int nodeIndex = 0;
		            	
			            synchronized(Application.getInstance().group.list)
			            {
				            for(int i=2;i<commandParts.length;i++)
				            {
				            	out.println(Application.getInstance().group.list.get(nodeIndex).getIP()+":"+commandParts[i]);
				            	jobFilesLeft.add(commandParts[i]);
				            	
				            	nodeIndex = (nodeIndex + 1) % Application.getInstance().group.list.size();
				            }
			            }
			            out.println("<END>");
		            } else if(command.equals("maple"))
		            {
		            	if(commandParts.length < 4)
		            	{
		            		System.out.println("ERROR: maple command has too few arguments ("+commandParts+")");
		            	}
		            	String mapleExeSdfsKey = commandParts[1];
		            	String intermediateFilePrefix = commandParts[2];
		            	String sourceFile = commandParts[3];
		            	MapleJuiceNode mjNode = new MapleJuiceNode(mapleExeSdfsKey,intermediateFilePrefix,sourceFile);
		            	MapleJuiceTask mapleTask = new MapleJuiceTask("maple", mjNode, mjNode.intermediateFilePrefix+"_OUTPUT_"+mjNode.sdfsSourceFile);
		            	mapleTaskThreads.add(mapleTask);
		            	mapleTask.start();
		            } else if(command.equals("mapledone"))
		            {
		            	System.out.println("mapledone:"+commandParts);
		            	String intermediateFilePrefix = commandParts[1];
		            	String sdfsSourceFile = commandParts[2];
		            	if(jobFilesLeft.contains(sdfsSourceFile))
		            	{
		            		jobFilesLeft.remove(sdfsSourceFile);
		            		jobFilesCompleted.add(sdfsSourceFile);
		            	} else {
		            		System.out.println("ERROR - incorrect file sent in");
		            	}
		            	
		            	if(jobFilesLeft.size() == 0)
		            	{
		            		// all Maple tasks are done, call collector to resort based on key
		            		collector = new MapleCollectorThread(intermediateFilePrefix, jobFilesCompleted);
		            		collector.start();
		            	}
		            }else if(command.equals("maplecomplete"))
		            {
		            	System.out.println("maplecomplete:"+commandParts[1]);
		            	// send message to console to not block anymore
		            }else if(command.equals("wherejuice"))
		            {
		            	originatorIP = clientSocket.getInetAddress().getHostAddress();
		            	if(commandParts.length < 3)
		            	{
		            		out.println("<END>");
		            	}
		            	String[] inputTokens =  inputLine.split(":");
			            String intermediate_file_key =inputTokens[1];
			            int num_juice = Integer.parseInt(inputTokens[2]);
			            destinationFile = inputTokens[3]; // Store for later use
			            int nodeIndex = 0;
			            
			            Set<String> keys = Application.getInstance().dfsServer.globalFileMap.keySet();
			            for(String key : keys) {
			            	if (key.startsWith(intermediate_file_key) && !key.startsWith(intermediate_file_key+"_OUTPUT")) {
			            		System.out.println("Selected Key: " + key);
					            synchronized(Application.getInstance().group.list)
					            {
					            	out.println(Application.getInstance().group.list.get(nodeIndex).getIP()+":"+key);
					            }
				            	jobFilesLeft.add(key);
				            	
				            	nodeIndex = (nodeIndex + 1) % num_juice;
			            	}
		            	}
			            out.println("<END>");
		            } else if(command.equals("juice"))
		            {
		            	if(commandParts.length < 4)
		            	{
		            		System.out.println("ERROR: juice command has too few arguments ("+commandParts+")");
		            	}
		            	String juiceExeSdfsKey = commandParts[1];
		            	String intermediateFilePrefix = commandParts[2];
		            	String sourceFile = commandParts[3];
		            	MapleJuiceNode mjNode = new MapleJuiceNode(juiceExeSdfsKey,intermediateFilePrefix,sourceFile);
		            	MapleJuiceTask juiceTask = new MapleJuiceTask("juice", mjNode,  mjNode.intermediateFilePrefix+"_DESTINATION_"+mjNode.sdfsSourceFile);
		            	mapleTaskThreads.add(juiceTask);
		            	juiceTask.start();
		            } else if(command.equals("juicedone"))
		            {
		            	System.out.println("juicedone:"+commandParts);
		            	String intermediateFilePrefix = commandParts[1];
		            	String sdfsSourceFile = commandParts[2];
		            	if(jobFilesLeft.contains(sdfsSourceFile))
		            	{
		            		jobFilesLeft.remove(sdfsSourceFile);
		            		jobFilesCompleted.add(sdfsSourceFile);
		            	} else {
		            		System.out.println("ERROR - incorrect file sent in");
		            	}
		            	
		            	if(jobFilesLeft.size() == 0)
		            	{
		            		// all Juice tasks are done, call collector to populate destination file
		            		collector = new JuiceCollectorThread(intermediateFilePrefix, jobFilesCompleted, destinationFile );
		            		collector.start();
		            	}
		            }else if(command.equals("juicecomplete"))
		            {
		            	System.out.println("juicecomplete:"+commandParts[1]);
		            	// send message to console to not block anymore
		            }
					
					in.close();
					clientSocket.close();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			

	
			// We can then loop through all intermediate prefixes in the global file
			// map! :) to find output files for juicing.
			//System.out.println("Maple complete.");
		}
	}
}
