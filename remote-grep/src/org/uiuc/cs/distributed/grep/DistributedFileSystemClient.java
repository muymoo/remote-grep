package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class DistributedFileSystemClient {
    public Map<String,String> fileMap; // Key: SDFS Path, Value: local path
    private Socket clientSocket = null;
    
    public DistributedFileSystemClient()
    {
    	//socket = new ServerSocket(Application.TCP_SDFS_PORT);
    	fileMap = new HashMap<String,String>();
    }
    
    public String generateNewFileName(String localFileName)
    {
    	String[] parts = localFileName.split("[.]");
    	String fileExtension = ".data";
    	if(parts.length > 1)
    		fileExtension = "."+parts[parts.length - 1];
    	return Application.SDFS_DIR+File.separator+Application.hostaddress+"_"+ UUID.randomUUID().toString() + fileExtension;
    }
    
    public void get( String sdfsFilePath, String localFileName)
    {
    	System.out.println("DFSClient - starting get");
        // Save time by checking locally first
        if(fileMap.containsKey(sdfsFilePath))
        {
            // Copy file to userInputtedLocalFileName
            // https://gist.github.com/mrenouf/889747
            //return fileMap.get(sdfsFilePath);
        	System.out.println("can copy file locally");
        	try {
				copyFile(new File(fileMap.get(sdfsFilePath)), new File(localFileName));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        // Look on the network for the file
        else 
        {
        	try
        	{
    	        String ipToPlaceFile="";
    	        if(Application.getInstance().group.isLeader())
    	        {
    	        	ipToPlaceFile = Application.getInstance().dfsServer.whereis(sdfsFilePath);
    	        } else {
    	        	System.out.println("About to run whereis:");
		            clientSocket = new Socket(Application.getInstance().group.getLeader().getIP(),Application.TCP_SDFS_PORT);
		            
		            // Setup our input and output streams
		            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		            
		            out.println("whereis:"+sdfsFilePath);
		            ipToPlaceFile = in.readLine();
		            
		            
		            in.close();
		            out.close();
	            
    	        }
    	        System.out.println("whereis result: "+ipToPlaceFile);
    	        
    	        if(!ipToPlaceFile.equals(""))
    	        {
		            clientSocket = new Socket(ipToPlaceFile, Application.TCP_SDFS_PORT);
		            ServerSocket serverSocket = new ServerSocket(0);
		            
		            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
		            out.println("get:"+sdfsFilePath+":"+serverSocket.getLocalPort());
		            System.out.println("get:"+sdfsFilePath+":"+serverSocket.getLocalPort());
		            
		            Socket socket = serverSocket.accept();
		            
		            System.out.println("Started file receive.");
					// Setup our input/output streams
					byte[] buffer = new byte[65536];
					int number;
					InputStream socketStream = socket.getInputStream();
					File f = new File(localFileName);
					OutputStream fileStream = new FileOutputStream(f);
					
					// Read file from sender
					while ((number = socketStream.read(buffer)) != -1) {
						fileStream.write(buffer, 0, number);
					}
					socket.close();
					serverSocket.close();
		
					fileStream.close();
		            socketStream.close();
		            clientSocket.close();
		            
		            updateFileMap(sdfsFilePath, localFileName);
    	        } else {
    	        	System.out.println("File not found.");
    	        }
        	} catch(Exception e ) {
        		e.printStackTrace();
        	}
        }
    }
    
    public void put(String sdfsKey)
    {
    	try
    	{
	        String ipToPlaceFile="";
	        if(Application.getInstance().group.isLeader())
	        {
	            ipToPlaceFile = Application.getInstance().dfsServer.whereput(sdfsKey, Application.hostaddress);
	        } 
	        else 
	        {
	            clientSocket = new Socket(Application.getInstance().group.getLeader().getIP(),Application.TCP_SDFS_PORT);
	            
	            // Setup our input and output streams
	            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	            out.println("whereput:"+sdfsKey);
	            ipToPlaceFile = in.readLine();
	            
	            in.close();
	            out.close();
	            clientSocket.close();
	        }
	        
	        if(!ipToPlaceFile.equals(""))
	        {
		        Socket socket = new Socket(ipToPlaceFile,Application.TCP_SDFS_PORT);
		        ServerSocket serverSocket = new ServerSocket(0);
		        
		        // Setup our input and output streams
		        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		        out.println("put:"+sdfsKey+":"+serverSocket.getLocalPort());
		        System.out.println("put:"+sdfsKey+":"+serverSocket.getLocalPort());
		        
		        sendFileToNode(serverSocket, fileMap.get(sdfsKey));
		        socket.close();
		    }
	        
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
       
    }
    
    public void put(String localFile, String sdfsKey){
    	// Add node to local file system before sending to remote nodes for replication.
    	String fileName = generateNewFileName(localFile);
    	try {
			copyFile(new File(localFile), new File(fileName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	fileMap.put(sdfsKey, fileName);
    	// TODO: sendFileMapUpdateToMaster(sdfsKey, thisNode);
        
        put(sdfsKey);
    }
    
	/**
	 * Remove file from sdfs file system.
	 * 
	 * @param sdfsKey
	 *            SDFS Key of file to remove
	 */
	public void delete(String sdfsKey) {
		// If file is stored locally, delete
		if (fileMap.containsKey(sdfsKey)) {
			deleteSdfsLocalFile(sdfsKey);
		}

		// Send request to master to remove file
		try {
			ArrayList<Node> nodesToRemoveFileFrom = new ArrayList<Node>();
			
			// If we're on the master node, we can just ask the server for the list of nodes
			if (Application.getInstance().group.isLeader()) {
				nodesToRemoveFileFrom = Application.getInstance().dfsServer.wheredelete(sdfsKey,
						Application.getInstance().group.getSelfNode().getIP());
			}
			// Otherwise we need to send a message to the master to get a list of nodes
			else
			{
				// Connect to the leader (master)
	            clientSocket = new Socket(Application.getInstance().group.getLeader().getIP(),Application.TCP_SDFS_PORT);
	            
	            // Setup our input and output streams
	            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	            
	            // Ask master for a list of nodes to delete
	            out.println("wheredelete:" + sdfsKey);
	            
	            // Read in nodes from server
	            String nodeToDeleteFrom = "";
	            while( !(nodeToDeleteFrom = in.readLine()).equals("<END>"))
	            {
	            	System.out.println("wheredelete RESPONSE: "+nodeToDeleteFrom);
	            	nodesToRemoveFileFrom.add(new Node(nodeToDeleteFrom,0));
	            }
	            
	            
	            // Cleanup stuff
	            in.close();
	            out.close();
	            clientSocket.close();
			}

			// Now we have a list of all nodes with the file so we can tell them
			// to delete it
			for (Node node : nodesToRemoveFileFrom) {
				System.out.println("Removing file from node: " + node);
				clientSocket = new Socket(node.getIP(),
						Application.TCP_SDFS_PORT);

				// Setup our input and output streams
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				out.println("delete:" + sdfsKey);
			}
		} catch (Exception e) {
			System.out.println("DELETE: Fail");
			e.printStackTrace();
		}
	}

	/**
	 * Deletes file from local disk and file map
	 * @param sdfsKey
	 */
	public void deleteSdfsLocalFile(String sdfsKey) {
		// Get local filename
		String localFilePath = fileMap.get(sdfsKey);
		File fileToDelete = new File(localFilePath);

		System.out.println("Deleting local file: " + localFilePath);
		
		// Delete local file
		if(fileToDelete.delete())
		{
			System.out.println("File Deleted.");
		}
		else
		{
			System.out.println("Failed to remove file from disk. Will only remove from file map.");
		}
		
		// Remove deleted file + key from file map
		fileMap.remove(sdfsKey);
	}
    
    public synchronized void updateFileMap(String sdfs_key, String local_filepath)
    {
        fileMap.put(sdfs_key, local_filepath);
    }
    
    public synchronized Set<String> getFilesOnNode()
    {
        return fileMap.keySet();
    }
    
    public synchronized boolean hasFile(String sdfs_key)
    {
    	return fileMap.containsKey(sdfs_key);
    }
    
    public synchronized String getFileLocation(String sdfs_key)
    {
        return fileMap.get(sdfs_key);
    }

    private String sendFileToNode(ServerSocket socket, String localFileName)
    {
	    // Read in file
	    byte[] buffer = new byte[65536];
	    int number;
		OutputStream socketOutputStream = null;
		FileInputStream fileInputStream = null;
        
        try {
        	Socket clientSocket = socket.accept();
        	socketOutputStream = clientSocket.getOutputStream();
        	
        	fileInputStream = new FileInputStream(localFileName);
        	
            while ((number = fileInputStream.read(buffer)) != -1) {
                    try {
                            socketOutputStream.write(buffer, 0, number);
                    } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                    }
            }            
            
            socketOutputStream.close();
            fileInputStream.close();
            socket.close();
        } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
        }

        return null;
    }
    
    public static void copyFile(File sourceFile, File destFile) throws IOException {
    	  if (!destFile.exists()) {
    	    destFile.createNewFile();
    	  }
    	  FileInputStream fIn = null;
    	  FileOutputStream fOut = null;
    	  FileChannel source = null;
    	  FileChannel destination = null;
    	  try {
    	    fIn = new FileInputStream(sourceFile);
    	    source = fIn.getChannel();
    	    fOut = new FileOutputStream(destFile);
    	    destination = fOut.getChannel();
    	    long transfered = 0;
    	    long bytes = source.size();
    	    while (transfered < bytes) {
    	      transfered += destination.transferFrom(source, 0, source.size());
    	      destination.position(transfered);
    	    }
    	  } finally {
    	    if (source != null) {
    	      source.close();
    	    } else if (fIn != null) {
    	      fIn.close();
    	    }
    	    if (destination != null) {
    	      destination.close();
    	    } else if (fOut != null) {
    	      fOut.close();
    	    }
    	  }
    	}

}
