package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class DistributedFileSystemGetThread extends Thread {
	private String serverIP;
	private int serverPort;
	private String key;

	public DistributedFileSystemGetThread(String _serverIP, int _serverPort, String sdfsKey) {
		super("GetThread");
		serverIP = _serverIP;
		serverPort = _serverPort;
		key = sdfsKey;
	}

	@Override
	public void run() {
		Socket socket = null;
		try {
			socket = new Socket(serverIP, serverPort);
		} catch (UnknownHostException e3) {
			e3.printStackTrace();
		} catch (IOException e3) {
			e3.printStackTrace();
		}
		byte[] buffer = new byte[65536];
		int number;
        File sdfsFile =  new File(Application.getInstance().dfsClient.getFileLocation(key)); // get the sdfs file from local storage
		OutputStream socketOutputStream = null;
		InputStream fileInputStream = null;
		try {
			socketOutputStream = socket.getOutputStream();
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
                socket.close();
        } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        } 
	}

}
