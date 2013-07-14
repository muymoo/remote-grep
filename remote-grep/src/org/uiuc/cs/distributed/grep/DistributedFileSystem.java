package org.uiuc.cs.distributed.grep;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class DistributedFileSystem {
	Map<String, String> fileMap;
	
	public DistributedFileSystem()
	{
		fileMap = new HashMap<String, HashMap<Node,String>>();
	}
	
	public void put(String localFileName, String remoteFileName) 
	{
		// TODO: Get hashed/random node to store node on
		Node remoteNodeToStoreFile = Application.getInstance().group.list.get(1);
		
		String remoteNodesLocalFilePath = sendFileToNode(localFileName, remoteNodeToStoreFile);
		
		HashMap<Node, String> remoteFileMap = new HashMap<Node, String>();
		remoteFileMap.put(remoteNodeToStoreFile, remoteNodesLocalFilePath);
		
		// Store file location key: the path to remote file, value: the remote machine + its local path
		fileMap.put(remoteFileName, remoteFileMap);
		
	}

	private String sendFileToNode(String localFileName,
			Node remoteNodeToStoreFile) {
		// Read in file
		byte[] buffer = new byte[65536];
		int number;
        Socket clientSocket = null;
        OutputStream socketOutputStream = null;
        FileInputStream fileInputStream = null;
		
        try {
        	System.out.println("Sending " + localFileName + " to " + remoteNodeToStoreFile);
			clientSocket = new Socket("130.126.112.117",
					Application.TCP_SDFS_PORT);
			System.out.println("Connected to socket: " + clientSocket);
			// Setup our input and output streams
			socketOutputStream = clientSocket.getOutputStream();
			fileInputStream = new FileInputStream("/tmp/momont2/test.txt");

		}
		catch (UnknownHostException e)
        {
            System.err.println("Don't know about host: " + remoteNodeToStoreFile.getIP());
            System.exit(1);
        }
        catch (IOException e)
        {
            System.err.println("Couldn't get I/O for the connection to: " + remoteNodeToStoreFile.getIP());
            System.exit(1);
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
	
	public String get()
	{
		return "";
	}
	
	public boolean delete(String remoteFileName)
	{
		return false;
	}
	
	private String hashLocalFileName(String localFileName)
	{
		return "";
	}
}
