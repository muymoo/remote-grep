package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
	protected static ArrayList<String> globalMapleJobInputs;
	protected String localMapleIntermediatePrefix;
	protected static ArrayList<String> localMapleJobInputs;
	protected static ArrayList<String> localMapleJobsCompleted;
	protected static int globalMapleDoneMessages;
	
	protected static ArrayList<String> jobFilesLeft;
	protected static ArrayList<String> jobFilesCompleted;
	
	public String originatorIP;

	protected static ArrayList<MapleJuiceTask> mapleTaskThreads;
	private Thread producer;
	private Thread collector;
	protected ServerSocket mapleServerSocket;
	protected ExecutorService executor = Executors.newFixedThreadPool(5);

	/**
	 * Expects command line input per MP's requirement.
	 * 
	 * @param commands
	 *            Takes maple maple_exe intermediate_file_prefix sdfs_file_1...n
	 */
	public MapleJuiceServer() {
		mapleTaskThreads = new ArrayList<MapleJuiceTask>();
		this.globalMapleJobInputs = new ArrayList<String>();
		this.localMapleJobInputs = new ArrayList<String>();
		globalMapleDoneMessages = 0;
		
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
		this.globalMapleJobInputs = new ArrayList<String>();
		this.localMapleJobInputs = new ArrayList<String>();
		globalMapleDoneMessages = 0;
		jobFilesLeft = new ArrayList<String>();
		jobFilesCompleted = new ArrayList<String>();
	}
	

	public void populateGlobalMapleJobInputs()
	{
		System.out.println("Populating Global Maple Job Inputs");
		
		MapleJuiceServer.globalMapleJobInputs.clear();
		
		// add local jobs
		System.out.println("adding local jobs");
		for(String job : MapleJuiceServer.localMapleJobInputs)
		{
			MapleJuiceServer.globalMapleJobInputs.add(job);
			globalMapleDoneMessages++;
		}
		
		System.out.println("adding remote jobs");
		try {
			// contact all other nodes to get their maps, and add to global map
			List<Node> allOtherNodes = Application.getInstance().group
					.getOtherNodes();
			for (Node node : allOtherNodes) {
				
				Socket cSocket = new Socket(node.getIP(),
						Application.TCP_MAPLE_PORT);
				// Setup our input and output streams
				PrintWriter out = new PrintWriter(cSocket.getOutputStream(),
						true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						cSocket.getInputStream()));

				out.println("getmaplejobs:filler");
				System.out.println("sending \"getmaplejobs:\" to node:"+node.getIP());
				String inputLine = "";
				
				while (!(inputLine = in.readLine()).equals("<END>")) {
					String curr_sdfs_key = inputLine;
					System.out.println("got job: "+curr_sdfs_key);
					synchronized(MapleJuiceServer.globalMapleJobInputs)
					{
						MapleJuiceServer.globalMapleJobInputs.add(curr_sdfs_key);
					}
				}
				out.close();
				in.close();
				cSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(globalMapleDoneMessages > 0 &&
				globalMapleDoneMessages == MapleJuiceServer.globalMapleJobInputs.size())
    	{
    		ArrayList<String> copy = globalMapleJobInputs;
    		// all Maple tasks are done, call collector to resort based on key
    		collector = new MapleCollectorThread(localMapleIntermediatePrefix, copy);
    		collector.start();
    	}
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
		            if(inputLine == null || inputLine.split(":").length < 2) {
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
				            	
				            	// if it is the leader, add to the global list
				            	if(Application.getInstance().group.isLeader())
				            	{
				            		globalMapleJobInputs.add(commandParts[i]);
				            	}
				            	
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
		            	localMapleIntermediatePrefix = intermediateFilePrefix;
		            	String sourceFile = commandParts[3];
		            	MapleJuiceServer.localMapleJobInputs.add(sourceFile);
		            	MapleJuiceNode mjNode = new MapleJuiceNode(mapleExeSdfsKey,intermediateFilePrefix,sourceFile);
		            	MapleJuiceTask mapleTask = new MapleJuiceTask("maple", mjNode, mjNode.intermediateFilePrefix+"_OUTPUT_"+mjNode.sdfsSourceFile);
		            	mapleTaskThreads.add(mapleTask);
		            	new Thread(mapleTask).start();
		            } else if(command.equals("mapledone"))
		            {
		            	System.out.println("mapledone:"+commandParts);
		            	String intermediateFilePrefix = commandParts[1];
		            	String sdfsSourceFile = commandParts[2];
		            	globalMapleDoneMessages++;
		            	if(globalMapleDoneMessages > globalMapleJobInputs.size())
		            	{
		            		System.out.println("ERROR - incorrect file sent in");
		            	}
		            	
		            	if(globalMapleDoneMessages == MapleJuiceServer.globalMapleJobInputs.size())
		            	{
		            		ArrayList<String> copy = globalMapleJobInputs;
		            		// all Maple tasks are done, call collector to resort based on key
		            		collector = new MapleCollectorThread(intermediateFilePrefix, copy);
		            		collector.start();
		            	}
		            }else if(command.equals("maplecomplete"))
		            {
		            	System.out.println("maplecomplete:"+commandParts[1]);
						long end = System.currentTimeMillis();

						System.out.println("Maple Time: " + (end - Application.startMapleTime) + "ms");
		            	// send message to console to not block anymore
		            } else if(command.equals("getmaplejobs"))
		            {
		            	System.out.println("getmaplejobs");
		            	ArrayList<String> currJobs = MapleJuiceServer.localMapleJobInputs;
		            	for(String job : currJobs)
		            	{
		            		out.println(job);
		            	}
		            	out.println("<END>");
		            	
		            	for(String job : currJobs)
		            	{
		            		Application.getInstance().mapleClient.sendMapleDone(localMapleIntermediatePrefix, job);
		            	}
		            }else if(command.equals("wherejuice"))
		            {
		            	originatorIP = clientSocket.getInetAddress().getHostAddress();
		            	if(commandParts.length < 3)
		            	{
		            		System.out.println("wherejuice: called with too few commands.");
		            	}
		            	String[] inputTokens =  inputLine.split(":");
		            	String sdfs_juice_key = inputTokens[1];
			            String intermediate_file_key =inputTokens[2];
			            int num_juice = Integer.parseInt(inputTokens[3]);
			            destinationFile = inputTokens[4]; // Store for later use
			            
			            String[] juiceCommands = prepareJuiceCommands(num_juice, intermediate_file_key);
			            delegateJuiceTasks(sdfs_juice_key, intermediate_file_key,juiceCommands);
			            
			            //out.println("<END>");
		            } else if(command.equals("juice"))
		            {
		            	if(commandParts.length < 4)
		            	{
		            		System.out.println("ERROR: juice command has too few arguments ("+commandParts+")");
		            	}
		            	String juiceExeSdfsKey = commandParts[1];
		            	String intermediateFilePrefix = commandParts[2];
		            	System.out.println("RECEIVED_JUICE: "+commandParts);
		        		if (!Application.getInstance().dfsClient
		        				.hasFile(juiceExeSdfsKey)) {
		        			System.out.println("Does not contain the executable - getting it.");
		        			String localFileName = Application.getInstance().dfsClient
		        					.generateNewFileName("file.jar");
		        			Application.getInstance().dfsClient
		        					.get(juiceExeSdfsKey,localFileName);
		        		}
		        		
		        		// download sdfs files to process
		            	for(int i=3;i<commandParts.length;i++){
		            		String sourceFile = commandParts[i];
		            		System.out.println("download sdfs_file:"+sourceFile);
		            		
		            		if (!Application.getInstance().dfsClient
		            				.hasFile(sourceFile)) {
		            			String localFileName = Application.getInstance().dfsClient
		            					.generateNewFileName("file.scratch");
		            			Application.getInstance().dfsClient.get(
		            					sourceFile, localFileName);
		            		}
		            		try {
								Thread.sleep(200);
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
		            	}
		            	
		            	
		            	// start juice tasks
		            	for(int i=3;i<commandParts.length;i++){
		            		MapleJuiceNode mjNode = new MapleJuiceNode(juiceExeSdfsKey,intermediateFilePrefix,commandParts[i]);
			            	MapleJuiceTask juiceTask = new MapleJuiceTask("juice", mjNode,  mjNode.intermediateFilePrefix+"_DESTINATION_"+mjNode.sdfsSourceFile);
			            	mapleTaskThreads.add(juiceTask);
			            	executor.execute(juiceTask);
			            	//juiceTask.start();
		            	}
		            	
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
						long end = System.currentTimeMillis();

						System.out.println("Juice Time: " + (end - Application.startJuiceTime) + "ms");
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
		
		
		private String[] prepareJuiceCommands(int num_juices, String intermediate_file_key) {
            Set<String> keys = Application.getInstance().dfsServer.globalFileMap.keySet();
            String[] juiceCommands = new String[num_juices];
            
            for(int i=0;i<num_juices;i++)
            {
            	juiceCommands[i] = "";
            }
            
            int nodeIndex = 0;
            synchronized(Application.getInstance().group.list)
            {
	            for(String key : keys) {
	            	if (key.startsWith(intermediate_file_key) && !key.startsWith(intermediate_file_key+"_OUTPUT")) {
	            		System.out.println("Selected Key: " + key);
		            	jobFilesLeft.add(key);
		            	juiceCommands[nodeIndex] += key+":";
		            	nodeIndex = (nodeIndex + 1) % num_juices;
	            	}
	        	}
            }
            
            return juiceCommands;
		}
		
		/**
		 * Delegate maple command to a node
		 * 
		 * @param nodeToRunOn
		 * @param sdfsSourceFile
		 */
		private void delegateJuiceTasks(String sdfs_juice_key, String intermediate_prefix, String[] juiceCommands) {
            
            synchronized(Application.getInstance().group.list)
            {
            	for(int i=0;i<juiceCommands.length;i++)
            	{
            		if(juiceCommands[i].length() != 0)
            		{
	            		String nodeIP = Application.getInstance().group.list.get(i).getIP();
	            		String message = "juice:"+sdfs_juice_key+":"+intermediate_prefix+":"+juiceCommands[i];
	            		System.out.println("juice:"+sdfs_juice_key+":"+intermediate_prefix+":"+juiceCommands[i]);
	            		sendMessage(nodeIP, message);
            		}
            	}
            }
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
}
