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
    	if(Application.getInstance().group.isLeader())
    	{
    		populateGlobalFileMap();
    	}
    	
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
	                // add to masterFileMap key:sdfs_key value: += requestingNode
	            	Set<Node> nodes = new HashSet<Node>();
	            	nodes.add(new Node(clientSocket.getInetAddress().getHostAddress(),Application.TCP_SDFS_PORT));
	                globalFileMap.put(sdfs_key, nodes);
	                
	                // Where are we going to replicate this node since we've already stored it locally?
	                String nodeToPutIP = whereput(sdfs_key, clientSocket.getInetAddress().getHostAddress());
	                globalFileMap.get(sdfs_key).add(new Node(nodeToPutIP, Application.TCP_SDFS_PORT)); // Add replicated node
	                System.out.println("whereput: "+nodeToPutIP);
	                out.println(nodeToPutIP);
	            } 
	            else if(command.equals("put")) 
	            {
	                // We are going to store the put'd file on the local machine here (unique file name)
	                String fileName = "/home/ebowlin2/mp3/sdfs/files/data" + new Date().getTime() + ".data";
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
        System.out.println("globalFileMap.contains key: "+globalFileMap.containsKey(sdfsKey));
        System.out.println("globalFileMap.get key .size() "+globalFileMap.get(sdfsKey).size());
        if(globalFileMap.containsKey(sdfsKey) &&
        globalFileMap.get(sdfsKey).size() > 0)
        {
            //check the DistributedFileSystemClient map for the sdfs key
            Iterator<Node> it = globalFileMap.get(sdfsKey).iterator();
            Node node = it.next();
            return node.getIP(); // node file is stored on
        }

        return "";
    }
    
    /**
     * returns the ip of a node to put the file
     * @returns String - ip of the node to put the file
     */
    public String whereput(String SDFS_Key, String puttingNodeIP)
    {
        //check the DistributedFileSystemClient map for the sdfs key
        synchronized(Application.getInstance().group.list)
        {
	        // Find a different node then the requesting node
	        for(Node node : Application.getInstance().group.list) {
	            if(!node.getIP().equals(puttingNodeIP))
	            {
	                return node.getIP(); // put it to this node
	            }
	        }
	        System.out.println("didnt identify a node");
	        return "";
        }
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
         // add contents of local file to global map
         for(String sdfs_key : Application.getInstance().dfsClient.getFilesOnNode()) {
             Set<Node> nodes = new HashSet<Node>();
             nodes.add(Application.getInstance().group.getSelfNode());
             globalFileMap.put(sdfs_key, nodes);
         }
         
         try
         {
          // contact all other nodes to get their maps, and add to global map
          List<Node> allOtherNodes = Application.getInstance().group.getOtherNodes();
          for(Node node : allOtherNodes)
          {
            Application.getInstance().group.getLeader().getIP();
            
            Socket cSocket = new Socket(node.getIP(),Application.TCP_SDFS_PORT);
            
            // Setup our input and output streams
            PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
            
            out.println("getlist:FILLERDATA");
            String inputLine="";
            while((inputLine = in.readLine()) != "<END>")
            {
                String curr_sdfs_key = inputLine;
                if(!globalFileMap.containsKey(curr_sdfs_key))
                {
                    Set<Node> nodes = new HashSet<Node>();
                    nodes.add(node);
                    globalFileMap.put(curr_sdfs_key, nodes);
                } else {
                    globalFileMap.get(curr_sdfs_key).add(node);
                }
            }
          }
         } catch (IOException e) {
        	 e.printStackTrace();
         }
    }
    
    public void removeFailedNodeEntries(Node nodeToRemove)
    {
    	try
    	{
	        List<String> sdfsKeysToReplicate = new ArrayList<String>();
	        
	        for(String key : globalFileMap.keySet())
	        {
	            for(Node node : globalFileMap.get(key))
	            {
	                if(node.equals(nodeToRemove))
	                {
	                    //remove node from list
	                }
	            }
	        }
	        
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
	                if(!Application.getInstance().group.getSelfNode().equals(node))
	                {
	                	Socket cSocket = new Socket(node.getIP(),Application.TCP_SDFS_PORT);
	                    // Setup our input and output streams
	                    PrintWriter out = new PrintWriter(cSocket.getOutputStream(), true);
	                    BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
	                    
	                    out.println("replicate:"+key);
	                } else {
	                    Application.getInstance().dfsClient.put(key);
	                }
	            }
	        }
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }
}
