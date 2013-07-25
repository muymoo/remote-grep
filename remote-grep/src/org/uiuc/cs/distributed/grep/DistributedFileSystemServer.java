package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class DistributedFileSystemServer extends Thread {
    public Map<String,Set<Node>> globalFileMap;  // sdfs key, list of nodes stored on
    private ServerSocket   serverSocket = null;
    private int            serverPort   = Application.TCP_SDFS_PORT;
    
    public DistributedFileSystemServer()
    {
        try {
			serverSocket = new ServerSocket(serverPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // TCP 
        globalFileMap = new HashMap<String,Set<Node>>();
    }
    /**
     * main class for processing distributed file system requests on the server
     * side
     */
    @Override
    public void run()
    {    	
        while(true) {
        	try {   	
	            Socket clientSocket = serverSocket.accept();
	            
	            // Setup our input and output streams
	            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	            
	            String inputLine = in.readLine();
	            
	            // make sure the input line is formatted correctly
	            if(inputLine.split(":").length < 2) {
	            	continue;
	            }
	            String command = inputLine.split(":")[0];
	            String sdfs_key = inputLine.split(":")[1];
	            System.out.println("DFSServer - command: "+command);
	            if(command.equals("whereis"))
	            {
	                String nodeIp = whereis(sdfs_key);
	    		    out.println(nodeIp);
	    		    System.out.println("whereis: "+nodeIp);
	            } 
	            else if(command.equals("get")) 
	            {
					byte[] buffer = new byte[65536];
					int number;
	                File sdfsFile =  new File(get(sdfs_key)); // get the sdfs file from local storage
	    			OutputStream socketOutputStream = null;
	    			InputStream fileInputStream = null;
					try {
						socketOutputStream = clientSocket.getOutputStream();
						fileInputStream = new FileInputStream(sdfsFile);
					} catch (IOException e2) {
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
	    			
					
	    	        try {
	                    System.out.println("reading in file: " + fileInputStream);
	                    while ((number = fileInputStream.read(buffer)) != -1) {
	                            try {
	                                    socketOutputStream.write(buffer, 0, number);
	                                    System.out.println("write: " + buffer.toString());
	                            } catch (IOException e) {
	                                    // TODO Auto-generated catch block
	                                    e.printStackTrace();
	                            }
	                    }
		            } catch (IOException e1) {
		                    // TODO Auto-generated catch block
		                    e1.printStackTrace();
		            }
	
		            try {
		                    socketOutputStream.close();
		                    fileInputStream.close();
		                    clientSocket.close();
		            } catch (IOException e) {
		                    // TODO Auto-generated catch block
		                    e.printStackTrace();
		            } 
	            } 
	            else if(command.equals("whereput")) 
	            {
	                // Where are we going to replicate this node since we've already stored it locally?
	                String nodeToPutIP = whereput(sdfs_key, clientSocket.getInetAddress().getHostAddress());
	                System.out.println("whereput: "+nodeToPutIP);
	                out.println(nodeToPutIP);
	            } 
	            else if(command.equals("put")) 
	            {
	                // We are going to store the put'd file on the local machine here (unique file name)
	            	String fileName = Application.getInstance().dfsClient.generateNewFileName("FILLER.data");
	                File localFile = new File(fileName);
	                
	    
					byte[] buffer = new byte[65536];
					int number;
					InputStream socketStream;
					socketStream = clientSocket.getInputStream();
					OutputStream fileStream = new FileOutputStream(localFile);
					
					// Read file from sender
					while ((number = socketStream.read(buffer)) != -1) {
						fileStream.write(buffer, 0, number);
					}

					fileStream.close();
	                socketStream.close();
	                clientSocket.close();
	                
	                // Update the file map on this node
	                Application.getInstance().dfsClient.updateFileMap(sdfs_key,fileName);
	                
	            }	            
	            else if(command.equals("wheredelete"))
	            {
	            	ArrayList<Node> nodesWithFile = wheredelete(sdfs_key, clientSocket.getInetAddress().getHostAddress());
	                for(Node node : nodesWithFile)
	                {
	                    out.println(node.getIP());
	                }
	                out.println("<END>");
	                
	        		// Remove the key from the global map
	                synchronized(globalFileMap)
	                {
	                	globalFileMap.remove(sdfs_key);
	                }
	            }
	            else if(command.equals("delete"))
	            {
	            	System.out.println("Deleting: " + sdfs_key);
	            	Application.getInstance().dfsClient.deleteSdfsLocalFile(sdfs_key);
	            }
	            else if(command.equals("getlist"))
	            {
	                Set<String> localSdfsKeys = getList();
	                for(String key : localSdfsKeys)
	                {
	                    out.println(key);
	                }
	                out.println("<END>");
	            }
	            else if(command.equals("replicate"))
	            {
	                Application.getInstance().dfsClient.put(sdfs_key);
	            }

        	} catch(IOException e)
        	{
        		e.printStackTrace();
        	}
        }
    }

    /**
     * returns the ip of a node where the file is stored on
     * @returns String - the ip of a node where the file is stored
     * */
    public String whereis(String sdfsKey)
    {
        // check to make sure the sdfsKey is in the file map and that
        // nodes exist within the list
        synchronized(globalFileMap)
        {
	        if(globalFileMap.containsKey(sdfsKey) &&
	        globalFileMap.get(sdfsKey).size() > 0)
	        {
	            //check the DistributedFileSystemClient map for the sdfs key
	            Iterator<Node> it = globalFileMap.get(sdfsKey).iterator();
	            Node node = it.next();
	            return node.getIP(); // node file is stored on
	        }
        }

        return "";
    }
    
    /**
     * returns the ip of a node to put the file
     * @returns String - ip of the node to put the file
     */
    public String whereput(String SDFS_Key, String puttingNodeIP)
    {   
    	synchronized(globalFileMap)
    	{
    		if(globalFileMap.containsKey(SDFS_Key))
    		{
    			if(globalFileMap.get(SDFS_Key).size() == 2)
	    		{
	    			System.out.println("whereput: key is already stored at two nodes");
	    			return "";
	    		}
    			Set<Node> nodes = globalFileMap.get(SDFS_Key);
    			nodes.add(new Node(puttingNodeIP,Application.TCP_SDFS_PORT));
    			globalFileMap.put(SDFS_Key, nodes);
    		} else {
    			// add to masterFileMap key:sdfs_key value: += requestingNode
    	    	Set<Node> nodes = new HashSet<Node>();
    	    	nodes.add(new Node(puttingNodeIP,Application.TCP_SDFS_PORT));
    	    	globalFileMap.put(SDFS_Key, nodes);
    		}
    	}
        //check the DistributedFileSystemClient map for the sdfs key
    	String ipToPut = "";
        synchronized(Application.getInstance().group.list)
        {
	        // Find a different node then the requesting node
	        for(Node node : Application.getInstance().group.list) {
	            if(!node.getIP().equals(puttingNodeIP))
	            {
	            	ipToPut = node.getIP(); // put it to this node
	            }
	        }
        }
        synchronized(globalFileMap)
        {
        	Set<Node> nodes = globalFileMap.get(SDFS_Key);
        	nodes.add(new Node(ipToPut,Application.TCP_SDFS_PORT));
        	globalFileMap.put(SDFS_Key, nodes);
        }
        return ipToPut;
    }
    
	/**
	 * Finds all nodes that need to remove the selected file.
	 * 
	 * @param sdfsKey
	 *            SDFS file key to delete
	 * @param ip
	 *            Node requesting the delete
	 * @return List of nodes that have the file to delete.
	 */
	public ArrayList<Node> wheredelete(String sdfsKey, String ip) {
		ArrayList<Node> result = new ArrayList<Node>();
		synchronized(globalFileMap)
		{
			for (Node node : globalFileMap.get(sdfsKey)) {
				// Since we've already deleted the file from the requesting ip, we
				// can skip it.
				if (!node.getIP().equals(ip)) {
					// add node to list of nodes to delete
					result.add(node);
				}
			}
		}
		return result;
	}

    /**
     * returns the local file path of the file to stream back to the client
     * @returns String - local file path of file to stream
     **/
    public String get(String sdfs_key) {
         return Application.getInstance().dfsClient.getFileLocation(sdfs_key);
    }
    
 
    //delete
    public Set<String> getList()
    {
        return Application.getInstance().dfsClient.getFilesOnNode(); 
    }
    
    void populateGlobalFileMap()
    {	
    	long start = new Date().getTime();
         // add contents of local file to global map
         for(String sdfs_key : Application.getInstance().dfsClient.getFilesOnNode()) {
             Set<Node> nodes = new HashSet<Node>();
             nodes.add(Application.getInstance().group.getSelfNode());
             synchronized(globalFileMap)
             {
            	 if(!globalFileMap.containsKey(sdfs_key))
            	 {
            		 globalFileMap.put(sdfs_key, nodes);
            	 } else {
            		 boolean alreadyAdded = false;
            		 Set<Node> currentNodes = globalFileMap.get(sdfs_key);
            		 for(Node node : currentNodes )
            		 {
            			 if(node.getIP().equals(Application.hostaddress))
            			 {
            				 alreadyAdded = true;
            			 }
            		 }
            		 if(!alreadyAdded)
            		 {
            			 currentNodes.add(Application.getInstance().group.getSelfNode());
            			 globalFileMap.put(sdfs_key, currentNodes);
            		 }
            	 }
             }
         }
         
		try {
			// contact all other nodes to get their maps, and add to global map
			List<Node> allOtherNodes = Application.getInstance().group
					.getOtherNodes();
			for (Node node : allOtherNodes) {
				Application.getInstance().group.getLeader().getIP();
				
				Socket cSocket = new Socket(node.getIP(),
						Application.TCP_SDFS_PORT);
				// Setup our input and output streams
				PrintWriter out = new PrintWriter(cSocket.getOutputStream(),
						true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						cSocket.getInputStream()));

				out.println("getlist:FILLERDATA");
				String inputLine = "";
				
				while (!(inputLine = in.readLine()).equals("<END>")) {
					String curr_sdfs_key = inputLine;
					synchronized(globalFileMap)
					{
						if (!globalFileMap.containsKey(curr_sdfs_key)) {
							Set<Node> nodes = new HashSet<Node>();
							nodes.add(node);
							globalFileMap.put(curr_sdfs_key, nodes);
						} else {
							Set<Node> nodes = globalFileMap.get(curr_sdfs_key);
							boolean foundNode = false;
							for(Node currNode : nodes)
							{
								if(currNode.getIP().equals(node.getIP()))
								{
									foundNode = true;
								}
							}
							if(!foundNode)
							{
								nodes.add(node);
								globalFileMap.put(curr_sdfs_key, nodes);
							}
						}
					}
				}
				out.close();
				in.close();
				cSocket.close();
				
				searchForReplication(true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		long time = new Date().getTime() - start;
    	System.out.println("Populated global file map took "+time+"ms");
    }
    
    public void searchForReplication(boolean doPut) throws IOException
    {

    	
        Map<String,String> ipMessagePairs = new HashMap<String,String>();
        synchronized(globalFileMap)
        {
	        // search for files that need to be replicated
	        for(String key : globalFileMap.keySet())
	        {
	            if(globalFileMap.get(key).size() == 0)
	            {
	                globalFileMap.remove(key);
	            }
	            else if(globalFileMap.get(key).size() == 1)
	            {
	                Iterator<Node> it = globalFileMap.get(key).iterator();
	    		    Node node = it.next();
	    		    System.out.println("Replicating Key"+key);
	    		    if(doPut)
	    		    {
	    		    	ipMessagePairs.put(key,node.getIP());
	    		    }
	                
	            } else if(globalFileMap.get(key).size() > 2)
	            {
	            	System.out.println("ERROR - dfs globalFileMap Entry has more than 2 copies");
	            }
	        }
        }
        
        
        for(String key : ipMessagePairs.keySet())
        {
            if(!Application.getInstance().group.getSelfNode().getIP().equals(ipMessagePairs.get(key)))
            {
	        	Socket cSocket = new Socket(ipMessagePairs.get(key),Application.TCP_SDFS_PORT);
                // Setup our input and output streams
                PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
                
                out.println("replicate:"+key);
                
                out.close();
                in.close();
                cSocket.close();
            } else {
                Application.getInstance().dfsClient.put(key);
            }

        }
    }
    
    public void removeFailedNodeEntries(Node nodeToRemove)
    {
    	try
    	{
	        synchronized(globalFileMap)
	        {
		        for(String key : globalFileMap.keySet())
		        {
		        	Iterator<Node> it = globalFileMap.get(key).iterator();
		            while(it.hasNext())
		            {
		            	Node node = it.next();
		                if(node.getIP().equals(nodeToRemove.getIP()))
		                {
		                	// remove node from map
		                	it.remove();
		                }
		            }
		        }
	        }
	        searchForReplication(true);
	        
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }

}
