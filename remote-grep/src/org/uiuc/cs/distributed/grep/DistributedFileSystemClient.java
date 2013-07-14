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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DistributedFileSystemClient {
    private Map<String,String> fileMap; // Key: SDFS Path, Value: local path
    private Socket clientSocket = null;
    
    public DistributedFileSystemClient()
    {
    	//socket = new ServerSocket(Application.TCP_SDFS_PORT);
    	fileMap = new HashMap<String,String>();
    }
    
    public void get( String sdfsFilePath, String localFileName)
    {
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
	            clientSocket = new Socket(ipToPlaceFile, Application.TCP_SDFS_PORT);
	            
	            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	            out.println("get:"+sdfsFilePath);
	            System.out.println("Started file receive.");
				// Setup our input/output streams
				byte[] buffer = new byte[65536];
				int number;
				InputStream socketStream = clientSocket.getInputStream();
				File f = new File(localFileName);
				OutputStream fileStream = new FileOutputStream(f);
				
				// Read file from sender
				while ((number = socketStream.read(buffer)) != -1) {
					fileStream.write(buffer, 0, number);
				}
	
				fileStream.close();
	            socketStream.close();
	            clientSocket.close();
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
	            ipToPlaceFile = Application.getInstance().dfsServer.whereput(sdfsKey, Application.getInstance().group.getSelfNode().getIP());
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
	        
	        clientSocket = new Socket(ipToPlaceFile,Application.TCP_SDFS_PORT);
	        
	        // Setup our input and output streams
	        PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	        out.println("put:"+sdfsKey);
	        
	        sendFileToNode(fileMap.get(sdfsKey));
	        
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
       
    }
    
    public void put(String localFile, String sdfsKey){
    	// Add node to local file system before sending to remote nodes for replication.
    	String fileName = "/home/ebowlin2/mp3/sdfs/files/data" + new Date().getTime() + ".data";
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
    
    public synchronized void updateFileMap(String sdfs_key, String local_filepath)
    {
        fileMap.put(sdfs_key, local_filepath);
    }
    
    public synchronized Set<String> getFilesOnNode()
    {
        return fileMap.keySet();
    }
    
    public synchronized String getFileLocation(String sdfs_key)
    {
        return fileMap.get(sdfs_key);
    }
    private String sendFileToNode(String localFileName)
    {
	    // Read in file
	    byte[] buffer = new byte[65536];
	    int number;
		OutputStream socketOutputStream = null;
		FileInputStream fileInputStream = null;
	
	    // Setup our input and output streams
	    try {
			socketOutputStream = clientSocket.getOutputStream();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	    try {
			fileInputStream = new FileInputStream(localFileName);
		} catch (FileNotFoundException e2) {
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
